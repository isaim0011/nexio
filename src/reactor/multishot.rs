//! Multishot io_uring operations (`AcceptMulti`, `RecvMulti`) and
//! kernel-provided buffer rings (`IORING_REGISTER_PBUF_RING`).

use std::{
    collections::VecDeque,
    future::poll_fn,
    io,
    os::unix::io::RawFd,
    sync::atomic::{AtomicU16, Ordering},
    task::{Context, Poll, Waker},
};

use io_uring::{cqueue, opcode, types};

use super::{try_with_uring, try_with_uring_mut, with_uring, with_uring_mut, INTERNAL_CANCEL_UD};

/// Base `user_data` tag for multishot operations.
///
/// Any `user_data` in `[MULTISHOT_UD_BASE, INTERNAL_TIMER_UD)` identifies an
/// entry in `Reactor::multishot_ops` rather than `CompletionSlab`.
pub(crate) const MULTISHOT_UD_BASE: u64 = 0xFFFF_FFFF_0000_0000;

/// Internal slot tracking an active or cancelled multishot operation.
pub(crate) struct MultishotSlot {
    events: VecDeque<(i32, u32)>,
    done: bool,
    waker: Option<Waker>,
    ignored: bool,
    close_fds_on_ignore: bool,
}

impl MultishotSlot {
    pub(crate) fn new(close_fds_on_ignore: bool) -> Self {
        Self {
            events: VecDeque::with_capacity(8),
            done: false,
            waker: None,
            ignored: false,
            close_fds_on_ignore,
        }
    }

    pub(crate) fn on_cqe(&mut self, result: i32, flags: u32) -> (Option<Waker>, bool) {
        let more = cqueue::more(flags);
        if self.ignored {
            if self.close_fds_on_ignore && result >= 0 {
                // SAFETY: `result` is a freshly accepted fd whose consumer was dropped.
                unsafe {
                    libc::close(result);
                }
            }
            return (None, !more);
        }
        self.events.push_back((result, flags));
        if !more {
            self.done = true;
        }
        (self.waker.take(), false)
    }

    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<(i32, u32)>> {
        if let Some(ev) = self.events.pop_front() {
            return Poll::Ready(Some(ev));
        }
        if self.done {
            return Poll::Ready(None);
        }
        if !matches!(self.waker, Some(ref w) if w.will_wake(cx.waker())) {
            self.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }

    /// Mark the slot as ignored/cancelled. Returns `true` if the slot is already
    /// finished (`done == true`) and can be removed immediately.
    pub(crate) fn ignore(&mut self) -> bool {
        while let Some((res, _)) = self.events.pop_front() {
            if self.close_fds_on_ignore && res >= 0 {
                // SAFETY: `res` is an unconsumed accepted fd.
                unsafe {
                    libc::close(res);
                }
            }
        }
        self.waker = None;
        self.ignored = true;
        self.done
    }
}

/// Safe wrapper around an `io_uring` provided buffer ring (`IORING_REGISTER_PBUF_RING`).
///
/// Used with [`MultishotRecv`] (`IORING_OP_RECV` with `IOSQE_BUFFER_SELECT` /
/// `RecvMulti`) so the kernel picks a buffer from the ring for each incoming
/// packet without per-recv allocations or copies.
pub struct ProvidedBufRing {
    bgid: u16,
    ring_entries: u16,
    buf_size: usize,
    br_base: *mut types::BufRingEntry,
    ring_bytes: usize,
    bufs: Vec<u8>,
    tail: u16,
}

impl ProvidedBufRing {
    /// Allocate and register a provided buffer ring on the current thread's [`Reactor`](super::Reactor).
    ///
    /// `ring_entries` must be a non-zero power of two (at most `32768`).
    /// `buf_size` is the byte capacity of each buffer in the ring.
    pub fn new(ring_entries: u16, buf_size: usize, bgid: u16) -> io::Result<Self> {
        if ring_entries == 0 || !ring_entries.is_power_of_two() || ring_entries > 32768 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ring_entries must be a power of two in 1..=32768",
            ));
        }
        if buf_size == 0 || buf_size > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buf_size must be in 1..=u32::MAX",
            ));
        }

        let ring_bytes = (ring_entries as usize) * std::mem::size_of::<types::BufRingEntry>();
        // SAFETY: anonymous private mapping for page-aligned `BufRingEntry` array.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                ring_bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let br_base = ptr as *mut types::BufRingEntry;

        let bufs = vec![0u8; (ring_entries as usize) * buf_size];

        let reg_res = with_uring(|r| r.register_buf_ring(br_base as u64, ring_entries, bgid));
        if let Err(e) = reg_res {
            // SAFETY: `br_base` was just mapped with `ring_bytes`.
            unsafe {
                libc::munmap(br_base as *mut libc::c_void, ring_bytes);
            }
            return Err(e);
        }

        for bid in 0..ring_entries {
            // SAFETY: `bid < ring_entries` is in bounds of both `br_base` and `bufs`.
            unsafe {
                let entry = &mut *br_base.add(bid as usize);
                entry.set_addr(bufs.as_ptr().add((bid as usize) * buf_size) as u64);
                entry.set_len(buf_size as u32);
                entry.set_bid(bid);
            }
        }
        let tail = ring_entries;
        // SAFETY: `BufRingEntry::tail(br_base)` points to the shared `tail` field in the ring header.
        unsafe {
            (*(types::BufRingEntry::tail(br_base) as *const AtomicU16))
                .store(tail, Ordering::Release);
        }

        Ok(Self {
            bgid,
            ring_entries,
            buf_size,
            br_base,
            ring_bytes,
            bufs,
            tail,
        })
    }

    /// Return the buffer group ID registered with the kernel.
    #[inline]
    pub fn bgid(&self) -> u16 {
        self.bgid
    }

    /// Return the byte size of each individual buffer slot.
    #[inline]
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    /// Return the total number of buffer entries in this ring.
    #[inline]
    pub fn ring_entries(&self) -> u16 {
        self.ring_entries
    }

    /// Borrow the slice corresponding to `bid` with `len` bytes read.
    ///
    /// Returns `None` if `bid >= self.ring_entries()`.
    #[inline]
    pub fn get(&self, bid: u16, len: usize) -> Option<&[u8]> {
        if bid >= self.ring_entries {
            return None;
        }
        let start = (bid as usize) * self.buf_size;
        let clamped = len.min(self.buf_size);
        Some(&self.bufs[start..start + clamped])
    }

    /// Return buffer `bid` back to the kernel ring so it can be reused for future completions.
    #[inline]
    pub fn recycle(&mut self, bid: u16) {
        if bid >= self.ring_entries {
            return;
        }
        let mask = (self.ring_entries as usize) - 1;
        let slot_idx = (self.tail as usize) & mask;
        // SAFETY: `slot_idx < ring_entries` and `bid < ring_entries`.
        unsafe {
            let entry = &mut *self.br_base.add(slot_idx);
            entry.set_addr(self.bufs.as_ptr().add((bid as usize) * self.buf_size) as u64);
            entry.set_len(self.buf_size as u32);
            entry.set_bid(bid);
            self.tail = self.tail.wrapping_add(1);
            (*(types::BufRingEntry::tail(self.br_base) as *const AtomicU16))
                .store(self.tail, Ordering::Release);
        }
    }
}

impl Drop for ProvidedBufRing {
    fn drop(&mut self) {
        let _ = try_with_uring(|r| {
            let _ = r.unregister_buf_ring(self.bgid);
        });
        // SAFETY: `self.br_base` was mapped via `mmap` with `self.ring_bytes`.
        unsafe {
            libc::munmap(self.br_base as *mut libc::c_void, self.ring_bytes);
        }
    }
}

/// Completion chunk yielded by [`MultishotRecv::next`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultishotChunk {
    /// Number of bytes received (`0` indicates EOF).
    pub len: usize,
    /// Provided buffer ID selected by the kernel (`IORING_CQE_F_BUFFER`), if any.
    pub bid: Option<u16>,
    /// Raw CQE flags.
    pub flags: u32,
}

/// Active multishot accept stream on a listening socket (`IORING_OP_ACCEPT` multishot).
pub struct MultishotAccept {
    slot_idx: usize,
}

impl MultishotAccept {
    pub(crate) fn from_slot(slot_idx: usize) -> Self {
        Self { slot_idx }
    }

    /// Poll for the next accepted raw file descriptor.
    pub fn poll_next_fd(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<RawFd>>> {
        with_uring_mut(|r| {
            let Some(slot) = r.multishot_ops.get_mut(self.slot_idx) else {
                return Poll::Ready(None);
            };
            match slot.poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some((res, _flags))) => {
                    if res < 0 {
                        Poll::Ready(Some(Err(io::Error::from_raw_os_error(-res))))
                    } else {
                        Poll::Ready(Some(Ok(res as RawFd)))
                    }
                }
            }
        })
    }

    /// Await the next accepted raw file descriptor, or `None` when the multishot op ends.
    pub async fn next_fd(&mut self) -> Option<io::Result<RawFd>> {
        poll_fn(|cx| self.poll_next_fd(cx)).await
    }
}

impl Drop for MultishotAccept {
    fn drop(&mut self) {
        let _ = try_with_uring_mut(|r| {
            let done = match r.multishot_ops.get_mut(self.slot_idx) {
                Some(slot) => slot.ignore(),
                None => return,
            };
            if done {
                r.multishot_ops.remove(self.slot_idx);
            } else {
                let ud = MULTISHOT_UD_BASE + self.slot_idx as u64;
                let cancel = opcode::AsyncCancel::new(ud)
                    .build()
                    .user_data(INTERNAL_CANCEL_UD);
                let _ = r.push_raw_sqe(cancel);
            }
        });
    }
}

/// Active multishot receive stream on a connected socket (`RecvMulti`).
pub struct MultishotRecv {
    slot_idx: usize,
}

impl MultishotRecv {
    pub(crate) fn from_slot(slot_idx: usize) -> Self {
        Self { slot_idx }
    }

    /// Poll for the next [`MultishotChunk`].
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<MultishotChunk>>> {
        with_uring_mut(|r| {
            let Some(slot) = r.multishot_ops.get_mut(self.slot_idx) else {
                return Poll::Ready(None);
            };
            match slot.poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some((res, flags))) => {
                    if res < 0 {
                        Poll::Ready(Some(Err(io::Error::from_raw_os_error(-res))))
                    } else {
                        Poll::Ready(Some(Ok(MultishotChunk {
                            len: res as usize,
                            bid: cqueue::buffer_select(flags),
                            flags,
                        })))
                    }
                }
            }
        })
    }

    /// Await the next [`MultishotChunk`], or `None` when the multishot recv finishes.
    pub async fn next(&mut self) -> Option<io::Result<MultishotChunk>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }
}

impl Drop for MultishotRecv {
    fn drop(&mut self) {
        let _ = try_with_uring_mut(|r| {
            let done = match r.multishot_ops.get_mut(self.slot_idx) {
                Some(slot) => slot.ignore(),
                None => return,
            };
            if done {
                r.multishot_ops.remove(self.slot_idx);
            } else {
                let ud = MULTISHOT_UD_BASE + self.slot_idx as u64;
                let cancel = opcode::AsyncCancel::new(ud)
                    .build()
                    .user_data(INTERNAL_CANCEL_UD);
                let _ = r.push_raw_sqe(cancel);
            }
        });
    }
}
