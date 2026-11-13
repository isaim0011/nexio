//! Completion slab — the data structure at the heart of cancellation safety.
//!
//! Every submitted SQE gets a `SlabKey` (u64, embedded in `user_data`).
//! The matching CQE carries that key back; we use it to look up the `OpState`
//! and either wake the pending `Future` or free an already-dropped (`Ignored`) op.
//!
//! # Invariants (targeted by Kani harnesses in tests/kani_slab.rs)
//!
//! 1. A `SlabKey` is live from `insert` until its matching CQE is processed.
//! 2. `Ignored` entries stay until their real CQE arrives, then buf is dropped.
//! 3. The slab index fits in a u32; high 32 bits carry a generation counter
//!    to detect stale CQEs from a recycled slot.

use std::task::Waker;

use crate::buf::IoBufMut;

// ── ErasedBuf ─────────────────────────────────────────────────────────────────

/// Type-erased owned buffer held in the slab for `Ignored` ops.
///
/// # Safety
/// Implementors must keep the buffer allocation live for their full lifetime.
pub trait ErasedBuf: Send {
    /// Callback executed when an ignored operation finishes in the kernel.
    ///
    /// By default this does nothing; dropping `self` releases allocated memory.
    /// Types managing resources like new file descriptors override this to clean them up.
    fn on_ignored_completion(&mut self, result: i32) {
        let _ = result;
    }
}

impl<B: IoBufMut + Send> ErasedBuf for B {}

/// Zero-size placeholder used for `Submitted`/`Complete` entries where the
/// real buffer lives in `Op::buf`, not the slab.
pub struct PlaceholderBuf;
unsafe impl Send for PlaceholderBuf {}
impl ErasedBuf for PlaceholderBuf {}

/// Internal wrapper type-erasing any `Send + 'static` buffer or resource.
pub(crate) struct ErasedBox<T: Send + 'static>(pub(crate) T);
unsafe impl<T: Send + 'static> Send for ErasedBox<T> {}
impl<T: Send + 'static> ErasedBuf for ErasedBox<T> {}

/// Guard that closes an accepted file descriptor if the accept operation was ignored/cancelled.
pub struct CloseOnDropFd;
unsafe impl Send for CloseOnDropFd {}
impl ErasedBuf for CloseOnDropFd {
    fn on_ignored_completion(&mut self, result: i32) {
        if result >= 0 {
            // Close the accepted socket to avoid descriptor leaks.
            unsafe {
                libc::close(result as libc::c_int);
            }
        }
    }
}

/// Heap-allocated storage for socket addresses during asynchronous connect.
pub struct SockAddrStorage {
    /// Raw address bytes
    pub storage: libc::sockaddr_storage,
    /// Address length in bytes
    pub len: libc::socklen_t,
}
unsafe impl Send for SockAddrStorage {}
impl ErasedBuf for SockAddrStorage {}

/// Heap-allocated `iovec` array kept alive while a vectored SQE is in flight.
pub(crate) struct IovecsStorage(pub(crate) Vec<libc::iovec>);
unsafe impl Send for IovecsStorage {}
impl ErasedBuf for IovecsStorage {}

/// Guard that holds path for openat and closes an opened file descriptor if the operation was ignored/cancelled.
pub struct OpenAtData {
    /// Path buffer kept alive until CQE
    pub path: std::ffi::CString,
}
unsafe impl Send for OpenAtData {}
impl ErasedBuf for OpenAtData {
    fn on_ignored_completion(&mut self, result: i32) {
        if result >= 0 {
            unsafe {
                libc::close(result as libc::c_int);
            }
        }
    }
}

/// Heap-allocated storage for msghdr, iovec, and sockaddr_storage for sendmsg/recvmsg.
pub struct MsgHdrData {
    /// Address storage for destination or source address
    pub storage: libc::sockaddr_storage,
    /// Address length
    pub namelen: libc::socklen_t,
    /// I/O vector referencing user buffer
    pub iov: libc::iovec,
    /// Message header passed to kernel
    pub msg: libc::msghdr,
}
unsafe impl Send for MsgHdrData {}
impl ErasedBuf for MsgHdrData {}

impl MsgHdrData {
    /// Create a `MsgHdrData` for sendmsg/send_to.
    pub fn new_send(
        storage: libc::sockaddr_storage,
        namelen: libc::socklen_t,
        ptr: *const u8,
        len: usize,
    ) -> Box<Self> {
        let mut data = Box::new(Self {
            storage,
            namelen,
            iov: libc::iovec {
                iov_base: ptr as *mut libc::c_void,
                iov_len: len,
            },
            msg: unsafe { std::mem::zeroed() },
        });
        if data.namelen > 0 {
            data.msg.msg_name = &mut data.storage as *mut _ as *mut libc::c_void;
            data.msg.msg_namelen = data.namelen;
        } else {
            data.msg.msg_name = std::ptr::null_mut();
            data.msg.msg_namelen = 0;
        }
        data.msg.msg_iov = &mut data.iov as *mut _;
        data.msg.msg_iovlen = 1;
        data
    }

    /// Create a `MsgHdrData` for recvmsg/recv_from.
    pub fn new_recv(ptr: *mut u8, len: usize) -> Box<Self> {
        let mut data = Box::new(Self {
            storage: unsafe { std::mem::zeroed() },
            namelen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            iov: libc::iovec {
                iov_base: ptr as *mut libc::c_void,
                iov_len: len,
            },
            msg: unsafe { std::mem::zeroed() },
        });
        data.msg.msg_name = &mut data.storage as *mut _ as *mut libc::c_void;
        data.msg.msg_namelen = data.namelen;
        data.msg.msg_iov = &mut data.iov as *mut _;
        data.msg.msg_iovlen = 1;
        data
    }

    /// Returns a raw pointer to `libc::msghdr`.
    pub fn as_msghdr_ptr(&self) -> *const libc::msghdr {
        &self.msg as *const _
    }

    /// Returns a mutable raw pointer to `libc::msghdr`.
    pub fn as_msghdr_mut_ptr(&mut self) -> *mut libc::msghdr {
        &mut self.msg as *mut _
    }
}

// ── OpState ───────────────────────────────────────────────────────────────────

/// Lifecycle of one in-flight io_uring operation.
///
/// ```text
/// Submitted -> Complete  (CQE arrived, Future woken)
/// Submitted -> Ignored   (Future dropped, ASYNC_CANCEL submitted)
/// Ignored   -> (freed)   (real CQE arrived, buf dropped)
/// ```
pub enum OpState {
    /// Op is in-flight. Waker is updated each time the Future is polled.
    /// `buf` is `None` for normal ops whose real buffer lives in `Op<B>::buf` / `LazyOp<B>::buf`,
    /// or `Some(...)` for ops with auxiliary heap state (e.g. `MsgHdrData`, `OpenAtData`, `SockAddrStorage`).
    /// For Ignored ops the real buf is moved into `Ignored`.
    Submitted {
        /// Optional waker registered when the future is polled.
        waker: Option<Waker>,
        /// Optional type-erased auxiliary metadata held while in flight.
        buf: Option<Box<dyn ErasedBuf>>,
    },
    /// CQE arrived; result waiting to be consumed.
    Complete {
        /// Raw result code from the CQE.
        result: i32,
    },
    /// Future was dropped. Real buf moved here; held until real CQE lands.
    Ignored {
        /// Type-erased buffer held until the kernel finishes with it.
        buf: Box<dyn ErasedBuf>,
        /// Optional auxiliary state (e.g. `MsgHdrData`) held alongside `buf`.
        aux: Option<Box<dyn ErasedBuf>>,
    },
}

// ── SlabKey ───────────────────────────────────────────────────────────────────

/// Opaque handle returned by `CompletionSlab::insert`; embedded in `user_data`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlabKey(u64);

impl SlabKey {
    /// Create a new `SlabKey` from an index and generation.
    pub fn new(index: usize, generation: u32) -> Self {
        assert!(index <= u32::MAX as usize, "slab index overflow");
        SlabKey(((generation as u64) << 32) | (index as u64))
    }

    /// Return the slot index in the slab.
    pub fn index(self) -> usize {
        (self.0 & 0xFFFF_FFFF) as usize
    }

    /// Return the generation counter for this key.
    pub fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Return the raw `u64` representation for use in io_uring `user_data`.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Reconstruct a `SlabKey` from a raw `u64` user data value.
    pub fn from_u64(v: u64) -> Self {
        SlabKey(v)
    }
}

// ── CompletionSlab ────────────────────────────────────────────────────────────

const NO_FREE: usize = usize::MAX;

enum SlotState {
    Vacant { next_free: usize },
    Occupied(OpState),
}

struct Slot {
    generation: u32,
    state: SlotState,
}

/// Completion slab tracking in-flight and completed operations.
pub struct CompletionSlab {
    slots: Vec<Slot>,
    free_head: usize,
    len: usize,
}

impl Default for CompletionSlab {
    fn default() -> Self {
        Self::new()
    }
}

impl CompletionSlab {
    /// Create a new `CompletionSlab` with hardware-adaptive initial capacity.
    pub fn new() -> Self {
        Self::with_capacity(crate::profile::HardwareProfile::get().slab_capacity())
    }

    /// Create a new `CompletionSlab` with a specified initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            free_head: NO_FREE,
            len: 0,
        }
    }

    #[inline]
    fn get_occupied_mut(&mut self, index: usize, generation: u32) -> Option<&mut OpState> {
        let slot = self.slots.get_mut(index)?;
        if slot.generation != generation {
            return None;
        }
        match &mut slot.state {
            SlotState::Occupied(op) => Some(op),
            SlotState::Vacant { .. } => None,
        }
    }

    #[inline]
    fn free_slot(&mut self, index: usize) {
        let slot = &mut self.slots[index];
        slot.generation = slot.generation.wrapping_add(1);
        slot.state = SlotState::Vacant {
            next_free: self.free_head,
        };
        self.free_head = index;
        self.len -= 1;
    }

    /// Insert a new in-flight op. Returns the key to embed in `user_data`.
    #[inline]
    pub fn insert(&mut self, state: OpState) -> SlabKey {
        self.len += 1;
        if self.free_head != NO_FREE {
            let index = self.free_head;
            let slot = &mut self.slots[index];
            let next_free = match slot.state {
                SlotState::Vacant { next_free } => next_free,
                SlotState::Occupied(_) => unreachable!("free list pointed to occupied slot"),
            };
            self.free_head = next_free;
            slot.state = SlotState::Occupied(state);
            SlabKey::new(index, slot.generation)
        } else {
            let index = self.slots.len();
            self.slots.push(Slot {
                generation: 0,
                state: SlotState::Occupied(state),
            });
            SlabKey::new(index, 0)
        }
    }

    /// Update (or set) the waker for a `Submitted` op.
    /// Called by `Op::poll` each time the future is polled.
    #[inline]
    pub fn update_waker(&mut self, key: SlabKey, waker: Waker) {
        if let Some(OpState::Submitted {
            waker: ref mut w, ..
        }) = self.get_occupied_mut(key.index(), key.generation())
        {
            *w = Some(waker);
        }
    }

    /// Poll for a result. Returns `Some(i32)` if the op is `Complete`,
    /// else registers `waker` (if changed) and returns `None`.
    #[inline]
    pub fn poll_result(&mut self, key: SlabKey, waker: &Waker) -> Option<i32> {
        let op = self.get_occupied_mut(key.index(), key.generation())?;
        match op {
            OpState::Complete { result } => Some(*result),
            OpState::Submitted {
                waker: ref mut w, ..
            } => {
                if !matches!(w, Some(existing) if existing.will_wake(waker)) {
                    *w = Some(waker.clone());
                }
                None
            }
            OpState::Ignored { .. } => None,
        }
    }

    /// Poll for a result and immediately consume/free the slot if `Complete`.
    #[inline]
    pub fn poll_and_consume(&mut self, key: SlabKey, waker: &Waker) -> Option<i32> {
        let index = key.index();
        let op = self.get_occupied_mut(index, key.generation())?;
        match op {
            OpState::Complete { result } => {
                let res = *result;
                self.free_slot(index);
                Some(res)
            }
            OpState::Submitted {
                waker: ref mut w, ..
            } => {
                if !matches!(w, Some(existing) if existing.will_wake(waker)) {
                    *w = Some(waker.clone());
                }
                None
            }
            OpState::Ignored { .. } => None,
        }
    }

    /// Called when a CQE arrives with the given `user_data`.
    ///
    /// Returns `Some(waker)` for `Submitted` ops (caller must wake it).
    /// Returns `None` for `Ignored` ops (buf dropped, slot freed).
    /// Returns `None` for stale/unknown keys (silently ignored).
    #[inline]
    pub fn complete(&mut self, user_data: u64, result: i32) -> Option<Waker> {
        let key = SlabKey::from_u64(user_data);
        let index = key.index();
        let op = self.get_occupied_mut(index, key.generation())?;

        // Swap state to a temporary so we can destructure without partial moves.
        let prev = std::mem::replace(op, OpState::Complete { result });

        match prev {
            OpState::Submitted { waker, buf: _ } => waker,
            OpState::Ignored { mut buf, mut aux } => {
                buf.on_ignored_completion(result);
                if let Some(ref mut a) = aux {
                    a.on_ignored_completion(result);
                }
                drop(buf);
                drop(aux);
                self.free_slot(index);
                None
            }
            OpState::Complete { .. } => None,
        }
    }

    /// Move a live op to `Ignored` after its Future is dropped.
    /// The buf is held here until the real CQE arrives in `complete`.
    ///
    /// Returns `true` if the op is in-flight and an `ASYNC_CANCEL` should be submitted.
    /// Returns `false` if the op had already completed (in which case it is cleaned up immediately).
    pub fn ignore(&mut self, key: SlabKey, buf: Box<dyn ErasedBuf>) -> bool {
        let index = key.index();
        if let Some(op) = self.get_occupied_mut(index, key.generation()) {
            match op {
                OpState::Submitted {
                    buf: ref mut prev_buf,
                    ..
                } => {
                    let old_buf = prev_buf.take();
                    *op = OpState::Ignored { buf, aux: old_buf };
                    return true;
                }
                OpState::Complete { .. } => {
                    drop(buf);
                    self.free_slot(index);
                    return false;
                }
                OpState::Ignored { .. } => {
                    drop(buf);
                    return false;
                }
            }
        }
        drop(buf);
        false
    }

    /// Move a live op to `Ignored` where the buffer was already stored in `Submitted`.
    /// Used for operations like Connect and SimpleOp.
    pub fn ignore_simple(&mut self, key: SlabKey) -> bool {
        self.ignore_simple_inner(key, false)
    }

    /// Move a live accept op to `Ignored`, closing an already-accepted fd if completed before drop.
    pub fn ignore_accept(&mut self, key: SlabKey) -> bool {
        self.ignore_simple_inner(key, true)
    }

    fn ignore_simple_inner(&mut self, key: SlabKey, close_fd_on_complete: bool) -> bool {
        let index = key.index();
        if let Some(op) = self.get_occupied_mut(index, key.generation()) {
            let state = std::mem::replace(op, OpState::Complete { result: 0 });
            match state {
                OpState::Submitted { buf, .. } => {
                    *op = OpState::Ignored {
                        buf: buf.unwrap_or_else(|| Box::new(PlaceholderBuf)),
                        aux: None,
                    };
                    return true;
                }
                OpState::Complete { result } => {
                    if close_fd_on_complete && result >= 0 {
                        unsafe {
                            libc::close(result as libc::c_int);
                        }
                    }
                    self.free_slot(index);
                    return false;
                }
                OpState::Ignored { buf, aux } => {
                    *op = OpState::Ignored { buf, aux };
                    return false;
                }
            }
        }
        false
    }

    /// Mark a `Complete` slot as consumed and free it.
    /// Called by `Op::poll` after extracting the result.
    #[inline]
    pub fn consume(&mut self, key: SlabKey) {
        let index = key.index();
        if let Some(OpState::Complete { .. }) = self.get_occupied_mut(index, key.generation()) {
            self.free_slot(index);
        }
    }

    /// Remove a slot that failed submission before being enqueued to kernel.
    pub(crate) fn remove_unsubmitted(&mut self, key: SlabKey) {
        let index = key.index();
        if let Some(OpState::Submitted { .. }) = self.get_occupied_mut(index, key.generation()) {
            self.free_slot(index);
        }
    }

    /// Returns the number of active entries in the slab.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the slab contains no active entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
