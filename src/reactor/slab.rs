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

use slab::Slab;
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
    /// buf is a PlaceholderBuf for normal ops (real buf is in `Op<B>::buf`).
    /// For Ignored ops the real buf is moved here.
    Submitted {
        /// Optional waker registered when the future is polled.
        waker: Option<Waker>,
        /// Type-erased buffer or metadata held while in flight.
        buf: Box<dyn ErasedBuf>,
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

struct SlabEntry {
    generation: u32,
    state: OpState,
}

/// Completion slab tracking in-flight and completed operations.
pub struct CompletionSlab {
    inner: Slab<SlabEntry>,
    generations: Vec<u32>,
}

impl Default for CompletionSlab {
    fn default() -> Self {
        Self::new()
    }
}

impl CompletionSlab {
    /// Create a new `CompletionSlab` with default capacity (256).
    pub fn new() -> Self {
        Self::with_capacity(256)
    }

    /// Create a new `CompletionSlab` with a specified initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Slab::with_capacity(capacity),
            generations: Vec::with_capacity(capacity),
        }
    }

    /// Insert a new in-flight op. Returns the key to embed in `user_data`.
    pub fn insert(&mut self, state: OpState) -> SlabKey {
        let entry = self.inner.vacant_entry();
        let index = entry.key();
        if index >= self.generations.len() {
            self.generations.resize(index + 1, 0);
        }
        let generation = self.generations[index];
        entry.insert(SlabEntry { generation, state });
        SlabKey::new(index, generation)
    }

    /// Update (or set) the waker for a `Submitted` op.
    /// Called by `Op::poll` each time the future is polled.
    pub fn update_waker(&mut self, key: SlabKey, waker: Waker) {
        if let Some(entry) = self.inner.get_mut(key.index()) {
            if entry.generation == key.generation() {
                if let OpState::Submitted {
                    waker: ref mut w, ..
                } = entry.state
                {
                    *w = Some(waker);
                }
            }
        }
    }

    /// Poll for a result. Returns `Some(i32)` if the op is `Complete`,
    /// else registers `waker` (if changed) and returns `None`.
    pub fn poll_result(&mut self, key: SlabKey, waker: &Waker) -> Option<i32> {
        let entry = self.inner.get_mut(key.index())?;
        if entry.generation != key.generation() {
            return None;
        }
        match &mut entry.state {
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
    pub fn poll_and_consume(&mut self, key: SlabKey, waker: &Waker) -> Option<i32> {
        let index = key.index();
        let entry = self.inner.get_mut(index)?;
        if entry.generation != key.generation() {
            return None;
        }
        match &mut entry.state {
            OpState::Complete { result } => {
                let res = *result;
                self.generations[index] = self.generations[index].wrapping_add(1);
                self.inner.remove(index);
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
    pub fn complete(&mut self, user_data: u64, result: i32) -> Option<Waker> {
        let key = SlabKey::from_u64(user_data);
        let index = key.index();

        let entry = self.inner.get_mut(index)?;

        if entry.generation != key.generation() {
            return None; // stale CQE from a recycled slot
        }

        // Swap state to a temporary so we can destructure without partial moves.
        let prev = std::mem::replace(&mut entry.state, OpState::Complete { result });

        match prev {
            OpState::Submitted { waker, buf: _ } => {
                // buf lives in Op<B>::buf, not here — PlaceholderBuf is dropped,
                // that's fine. State is now Complete; Future will consume it.
                waker
            }
            OpState::Ignored { mut buf, mut aux } => {
                // Future was dropped; now the real CQE arrived. Free everything.
                buf.on_ignored_completion(result);
                if let Some(ref mut a) = aux {
                    a.on_ignored_completion(result);
                }
                drop(buf);
                drop(aux);
                self.generations[index] = self.generations[index].wrapping_add(1);
                self.inner.remove(index);
                None
            }
            OpState::Complete { .. } => {
                // Double-completion: ignore duplicate CQE safely without panicking.
                None
            }
        }
    }

    /// Move a live op to `Ignored` after its Future is dropped.
    /// The buf is held here until the real CQE arrives in `complete`.
    ///
    /// Returns `true` if the op is in-flight and an `ASYNC_CANCEL` should be submitted.
    /// Returns `false` if the op had already completed (in which case it is cleaned up immediately).
    pub fn ignore(&mut self, key: SlabKey, buf: Box<dyn ErasedBuf>) -> bool {
        let index = key.index();
        if let Some(entry) = self.inner.get_mut(index) {
            if entry.generation == key.generation() {
                match &mut entry.state {
                    OpState::Submitted {
                        buf: ref mut prev_buf,
                        ..
                    } => {
                        let old_buf = std::mem::replace(prev_buf, Box::new(PlaceholderBuf));
                        entry.state = OpState::Ignored {
                            buf,
                            aux: Some(old_buf),
                        };
                        return true;
                    }
                    OpState::Complete { .. } => {
                        drop(buf);
                        self.generations[index] = self.generations[index].wrapping_add(1);
                        self.inner.remove(index);
                        return false;
                    }
                    OpState::Ignored { .. } => {
                        drop(buf);
                        return false;
                    }
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
        if let Some(entry) = self.inner.get_mut(index) {
            if entry.generation == key.generation() {
                let state = std::mem::replace(&mut entry.state, OpState::Complete { result: 0 });
                match state {
                    OpState::Submitted { buf, .. } => {
                        entry.state = OpState::Ignored { buf, aux: None };
                        return true;
                    }
                    OpState::Complete { result } => {
                        if close_fd_on_complete && result >= 0 {
                            unsafe {
                                libc::close(result as libc::c_int);
                            }
                        }
                        self.generations[index] = self.generations[index].wrapping_add(1);
                        self.inner.remove(index);
                        return false;
                    }
                    OpState::Ignored { buf, aux } => {
                        entry.state = OpState::Ignored { buf, aux };
                        return false;
                    }
                }
            }
        }
        false
    }

    /// Mark a `Complete` slot as consumed and free it.
    /// Called by `Op::poll` after extracting the result.
    pub fn consume(&mut self, key: SlabKey) {
        let index = key.index();
        if let Some(entry) = self.inner.get(index) {
            if entry.generation == key.generation()
                && matches!(entry.state, OpState::Complete { .. })
            {
                self.generations[index] = self.generations[index].wrapping_add(1);
                self.inner.remove(index);
            }
        }
    }

    /// Remove a slot that failed submission before being enqueued to kernel.
    pub(crate) fn remove_unsubmitted(&mut self, key: SlabKey) {
        let index = key.index();
        if let Some(entry) = self.inner.get(index) {
            if entry.generation == key.generation()
                && matches!(entry.state, OpState::Submitted { .. })
            {
                self.generations[index] = self.generations[index].wrapping_add(1);
                self.inner.remove(index);
            }
        }
    }

    /// Returns the number of active entries in the slab.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the slab contains no active entries.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
