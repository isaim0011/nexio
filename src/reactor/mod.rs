//! Thread-local io_uring reactor.
//!
//! One `Reactor` per thread. No shared state; no locks.
//! Multi-core scale-out: N independent reactors + SO_REUSEPORT sharding.

pub mod builder;
/// Internal operation futures. Public so external verification harnesses can reach them;
/// **not covered by semver** and not part of nexio's supported API.
#[doc(hidden)]
pub mod op;
/// Internal completion slab. Public so the fuzz, Kani and Miri harnesses can reach it;
/// **not covered by semver** and not part of nexio's supported API.
#[doc(hidden)]
pub mod slab;

use std::{cell::RefCell, ffi::CStr, io, net::SocketAddr, os::unix::io::RawFd, time::Duration};

use io_uring::{opcode, types, IoUring};
use slab::{CompletionSlab, OpState, SlabKey};

pub use builder::ReactorBuilder;
#[doc(hidden)]
pub use op::{AcceptOp, ConnectOp, LazyOp, Op, SimpleOp};

// ── Thread-local handle ───────────────────────────────────────────────────────

thread_local! {
    pub(crate) static REACTOR: RefCell<Reactor> =
        RefCell::new(Reactor::new().expect("failed to initialise io_uring reactor"));
}

// ── Reactor ───────────────────────────────────────────────────────────────────

/// Thread-local io_uring driver and completion tracker.
pub struct Reactor {
    ring: IoUring,
    pub(crate) slab: CompletionSlab,
}

impl Reactor {
    /// Create a builder to configure and construct a `Reactor`.
    pub fn builder() -> ReactorBuilder {
        ReactorBuilder::new()
    }

    /// Create a `Reactor` with a custom queue depth using default flags.
    pub fn with_entries(entries: u32) -> io::Result<Self> {
        ReactorBuilder::new().entries(entries).build()
    }

    /// Create a new `Reactor` instance with a queue depth of 1024.
    ///
    /// Automatically enables `COOP_TASKRUN` / `DEFER_TASKRUN` / `SINGLE_ISSUER`
    /// on kernels that support them (Linux 6.1+), falling back cleanly on older kernels.
    pub fn new() -> io::Result<Self> {
        ReactorBuilder::new().build()
    }

    /// Temporarily install `self` as the current thread's active reactor for the
    /// duration of `f`, restoring the previous thread-local reactor on return or panic.
    ///
    /// This allows running `nexio::block_on`, `nexio::net`, `nexio::fs`, and
    /// `nexio::time` on a custom [`Reactor`] (e.g., from [`Reactor::builder`] or
    /// [`Reactor::with_entries`]).
    pub fn enter<R>(&mut self, f: impl FnOnce() -> R) -> R {
        struct RestoreGuard<'a>(&'a mut Reactor);
        impl Drop for RestoreGuard<'_> {
            fn drop(&mut self) {
                REACTOR.with(|r| std::mem::swap(&mut *r.borrow_mut(), self.0));
            }
        }
        REACTOR.with(|r| std::mem::swap(&mut *r.borrow_mut(), self));
        let guard = RestoreGuard(self);
        let out = f();
        drop(guard);
        out
    }

    /// Internal helper to push an SQE with retries on SQ full / EINTR.
    fn push_sqe(&mut self, sqe: io_uring::squeue::Entry, key: SlabKey) -> io::Result<()> {
        let sqe = sqe.user_data(key.as_u64());
        unsafe {
            if self.ring.submission().push(&sqe).is_err() {
                // SQ is full; flush pending SQEs to kernel to make room.
                loop {
                    match self.ring.submit() {
                        Ok(_) => break,
                        Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                        Err(e) => {
                            self.slab.remove_unsubmitted(key);
                            return Err(e);
                        }
                    }
                }
                if self.ring.submission().push(&sqe).is_err() {
                    self.slab.remove_unsubmitted(key);
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "submission queue is full",
                    ));
                }
            }
        }
        Ok(())
    }

    // ── Submission ────────────────────────────────────────────────────────────

    /// Submit a Read SQE. Returns the SlabKey to give to Op<B>.
    ///
    /// # Safety
    /// `buf.stable_ptr_mut()` must remain valid until the matching CQE arrives.
    /// The caller immediately wraps the returned key in an `Op<B>` which
    /// upholds this via `ManuallyDrop`.
    pub(crate) unsafe fn submit_read<B>(
        &mut self,
        fd: RawFd,
        buf: &mut B,
        offset: u64,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBufMut + Send + 'static,
    {
        unsafe { self.submit_read_with_waker(fd, buf, offset, None) }
    }

    /// Submit a Read SQE with an optional pre-registered waker.
    ///
    /// # Safety
    /// Same lifetime guarantees as `submit_read`.
    pub(crate) unsafe fn submit_read_with_waker<B>(
        &mut self,
        fd: RawFd,
        buf: &mut B,
        offset: u64,
        waker: Option<std::task::Waker>,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBufMut + Send + 'static,
    {
        let sqe = opcode::Read::new(
            types::Fd(fd),
            buf.stable_ptr_mut(),
            buf.bytes_total() as u32,
        )
        .offset(offset)
        .build();

        let key = self.slab.insert(OpState::Submitted {
            waker,
            buf: Box::new(slab::PlaceholderBuf),
        });

        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a Write SQE.
    ///
    /// # Safety
    /// Same lifetime guarantees as `submit_read`.
    pub(crate) unsafe fn submit_write<B>(
        &mut self,
        fd: RawFd,
        buf: &B,
        offset: u64,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBuf + Send + 'static,
    {
        unsafe { self.submit_write_with_waker(fd, buf, offset, None) }
    }

    /// Submit a Write SQE with an optional pre-registered waker.
    ///
    /// # Safety
    /// Same lifetime guarantees as `submit_read`.
    pub(crate) unsafe fn submit_write_with_waker<B>(
        &mut self,
        fd: RawFd,
        buf: &B,
        offset: u64,
        waker: Option<std::task::Waker>,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBuf + Send + 'static,
    {
        let sqe = opcode::Write::new(types::Fd(fd), buf.stable_ptr(), buf.bytes_init() as u32)
            .offset(offset)
            .build();

        let key = self.slab.insert(OpState::Submitted {
            waker,
            buf: Box::new(slab::PlaceholderBuf),
        });

        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a socket `Recv` SQE with an optional pre-registered waker.
    ///
    /// # Safety
    /// Same lifetime guarantees as `submit_read`.
    pub(crate) unsafe fn submit_recv_with_waker<B>(
        &mut self,
        fd: RawFd,
        buf: &mut B,
        waker: Option<std::task::Waker>,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBufMut + Send + 'static,
    {
        let sqe = opcode::Recv::new(
            types::Fd(fd),
            buf.stable_ptr_mut(),
            buf.bytes_total() as u32,
        )
        .build();

        let key = self.slab.insert(OpState::Submitted {
            waker,
            buf: Box::new(slab::PlaceholderBuf),
        });

        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a socket `Send` SQE with an optional pre-registered waker.
    ///
    /// # Safety
    /// Same lifetime guarantees as `submit_write`.
    pub(crate) unsafe fn submit_send_with_waker<B>(
        &mut self,
        fd: RawFd,
        buf: &B,
        waker: Option<std::task::Waker>,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBuf + Send + 'static,
    {
        let sqe =
            opcode::Send::new(types::Fd(fd), buf.stable_ptr(), buf.bytes_init() as u32).build();

        let key = self.slab.insert(OpState::Submitted {
            waker,
            buf: Box::new(slab::PlaceholderBuf),
        });

        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit an Accept SQE on `fd`.
    pub(crate) fn submit_accept(&mut self, fd: RawFd) -> io::Result<SlabKey> {
        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(slab::CloseOnDropFd),
        });

        let sqe =
            opcode::Accept::new(types::Fd(fd), std::ptr::null_mut(), std::ptr::null_mut()).build();

        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a Connect SQE. Holds the destination sockaddr buffer safely on the heap.
    pub(crate) fn submit_connect(&mut self, fd: RawFd, addr: SocketAddr) -> io::Result<SlabKey> {
        let (storage, len) = socket_addr_to_libc(addr);
        let storage_box = Box::new(slab::SockAddrStorage { storage, len });
        let ptr = &storage_box.storage as *const libc::sockaddr_storage as *const libc::sockaddr;

        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: storage_box,
        });

        let sqe = opcode::Connect::new(types::Fd(fd), ptr, len).build();
        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a ReadFixed SQE.
    pub(crate) unsafe fn submit_read_fixed<B>(
        &mut self,
        fd: RawFd,
        buf: &mut B,
        offset: u64,
        buf_index: u16,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBufMut + Send + 'static,
    {
        let sqe = opcode::ReadFixed::new(
            types::Fd(fd),
            buf.stable_ptr_mut(),
            buf.bytes_total() as u32,
            buf_index,
        )
        .offset(offset)
        .build();

        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(slab::PlaceholderBuf),
        });

        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a WriteFixed SQE.
    pub(crate) unsafe fn submit_write_fixed<B>(
        &mut self,
        fd: RawFd,
        buf: &B,
        offset: u64,
        buf_index: u16,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBuf + Send + 'static,
    {
        let sqe = opcode::WriteFixed::new(
            types::Fd(fd),
            buf.stable_ptr(),
            buf.bytes_init() as u32,
            buf_index,
        )
        .offset(offset)
        .build();

        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(slab::PlaceholderBuf),
        });

        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a SendMsg SQE. Holds msghdr and address buffers safely on the heap in the slab.
    pub(crate) unsafe fn submit_sendmsg<B>(
        &mut self,
        fd: RawFd,
        buf: &B,
        addr: Option<SocketAddr>,
        flags: u32,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBuf + Send + 'static,
    {
        let (storage, len) = match addr {
            Some(a) => socket_addr_to_libc(a),
            None => (unsafe { std::mem::zeroed() }, 0),
        };
        let msg_data = slab::MsgHdrData::new_send(storage, len, buf.stable_ptr(), buf.bytes_init());
        let ptr = msg_data.as_msghdr_ptr();
        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: msg_data,
        });

        let sqe = opcode::SendMsg::new(types::Fd(fd), ptr)
            .flags(flags)
            .build();
        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a RecvMsg SQE. Holds msghdr and address buffers safely on the heap in the slab.
    pub(crate) unsafe fn submit_recvmsg<B>(
        &mut self,
        fd: RawFd,
        buf: &mut B,
        flags: u32,
    ) -> io::Result<SlabKey>
    where
        B: crate::buf::IoBufMut + Send + 'static,
    {
        let mut msg_data = slab::MsgHdrData::new_recv(buf.stable_ptr_mut(), buf.bytes_total());
        let ptr = msg_data.as_msghdr_mut_ptr();
        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: msg_data,
        });

        let sqe = opcode::RecvMsg::new(types::Fd(fd), ptr)
            .flags(flags)
            .build();
        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a relative timeout SQE for `duration`.
    pub(crate) fn submit_sleep(&mut self, duration: Duration) -> io::Result<SlabKey> {
        let ts = types::Timespec::new()
            .sec(duration.as_secs())
            .nsec(duration.subsec_nanos());
        let ts_box = Box::new(slab::ErasedBox(ts));
        let ptr = &ts_box.0 as *const types::Timespec;
        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: ts_box,
        });

        let sqe = opcode::Timeout::new(ptr).build();
        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit an absolute timeout SQE at `timespec`.
    pub(crate) fn submit_timeout_at(&mut self, timespec: types::Timespec) -> io::Result<SlabKey> {
        let ts_box = Box::new(slab::ErasedBox(timespec));
        let ptr = &ts_box.0 as *const types::Timespec;
        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: ts_box,
        });

        let sqe = opcode::Timeout::new(ptr)
            .flags(types::TimeoutFlags::ABS)
            .build();
        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit an Fsync SQE.
    pub(crate) fn submit_fsync(&mut self, fd: RawFd, datasync: bool) -> io::Result<SlabKey> {
        let mut fsync = opcode::Fsync::new(types::Fd(fd));
        if datasync {
            fsync = fsync.flags(types::FsyncFlags::DATASYNC);
        }
        let sqe = fsync.build();
        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(slab::PlaceholderBuf),
        });
        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a one-shot `POLL_ADD` SQE: completes when `fd` reports any of `events`
    /// (poll flags such as `POLLIN`). Carries no buffer, so dropping it is trivially safe.
    pub(crate) fn submit_poll_add(&mut self, fd: RawFd, events: u32) -> io::Result<SlabKey> {
        let sqe = opcode::PollAdd::new(types::Fd(fd), events).build();
        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(slab::PlaceholderBuf),
        });
        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit an OpenAt SQE. Holds the CString safely on the heap in the slab.
    pub(crate) fn submit_openat(
        &mut self,
        dirfd: RawFd,
        path: &CStr,
        flags: i32,
        mode: u32,
    ) -> io::Result<SlabKey> {
        let open_data = Box::new(slab::OpenAtData {
            path: path.to_owned(),
        });
        let ptr = open_data.path.as_ptr();
        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: open_data,
        });

        let sqe = opcode::OpenAt::new(types::Fd(dirfd), ptr)
            .flags(flags)
            .mode(mode as libc::mode_t)
            .build();
        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a Close SQE.
    pub(crate) fn submit_close(&mut self, fd: RawFd) -> io::Result<SlabKey> {
        let sqe = opcode::Close::new(types::Fd(fd)).build();
        let key = self.slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(slab::PlaceholderBuf),
        });
        self.push_sqe(sqe, key)?;
        Ok(key)
    }

    /// Submit a best-effort ASYNC_CANCEL for a previously submitted op.
    pub(crate) fn submit_cancel(&mut self, key: SlabKey) -> io::Result<()> {
        let sqe = opcode::AsyncCancel::new(key.as_u64())
            .build()
            .user_data(u64::MAX); // sentinel: ignore this CQE
        unsafe {
            if self.ring.submission().push(&sqe).is_err() {
                // If SQ full, flush once and retry
                if self.ring.submit().is_ok() {
                    let _ = self.ring.submission().push(&sqe);
                }
            }
        }
        Ok(())
    }

    // ── Safe High-level Operation Constructors ────────────────────────────────

    /// Start an asynchronous read operation on `fd` into owned buffer `buf`.
    pub fn read<B>(&mut self, fd: RawFd, mut buf: B, offset: u64) -> io::Result<Op<B>>
    where
        B: crate::buf::IoBufMut + Send + 'static,
    {
        let key = unsafe { self.submit_read(fd, &mut buf, offset)? };
        Ok(unsafe { Op::new(key, buf) })
    }

    /// Start an asynchronous write operation on `fd` from owned buffer `buf`.
    pub fn write<B>(&mut self, fd: RawFd, buf: B, offset: u64) -> io::Result<Op<B>>
    where
        B: crate::buf::IoBuf + Send + 'static,
    {
        let key = unsafe { self.submit_write(fd, &buf, offset)? };
        Ok(unsafe { Op::new(key, buf) })
    }

    /// Start an asynchronous read operation on `fd` into owned buffer `buf` using registered fixed buffers.
    pub fn read_fixed<B>(
        &mut self,
        fd: RawFd,
        mut buf: B,
        offset: u64,
        buf_index: u16,
    ) -> io::Result<Op<B>>
    where
        B: crate::buf::IoBufMut + Send + 'static,
    {
        let key = unsafe { self.submit_read_fixed(fd, &mut buf, offset, buf_index)? };
        Ok(unsafe { Op::new(key, buf) })
    }

    /// Start an asynchronous write operation on `fd` from owned buffer `buf` using registered fixed buffers.
    pub fn write_fixed<B>(
        &mut self,
        fd: RawFd,
        buf: B,
        offset: u64,
        buf_index: u16,
    ) -> io::Result<Op<B>>
    where
        B: crate::buf::IoBuf + Send + 'static,
    {
        let key = unsafe { self.submit_write_fixed(fd, &buf, offset, buf_index)? };
        Ok(unsafe { Op::new(key, buf) })
    }

    /// Start an asynchronous accept operation on listening socket `fd`.
    pub fn accept(&mut self, fd: RawFd) -> io::Result<AcceptOp> {
        let key = self.submit_accept(fd)?;
        Ok(AcceptOp::new(key))
    }

    /// Start an asynchronous connect operation to `addr` on `fd`.
    pub fn connect(&mut self, fd: RawFd, addr: SocketAddr) -> io::Result<ConnectOp> {
        let key = self.submit_connect(fd, addr)?;
        Ok(ConnectOp::new(key))
    }

    /// Start an asynchronous sleep operation for `duration`.
    pub fn sleep(&mut self, duration: Duration) -> io::Result<SimpleOp> {
        let key = self.submit_sleep(duration)?;
        Ok(SimpleOp::new(key))
    }

    /// Start an asynchronous timeout operation until absolute `timespec`.
    pub fn timeout_at(&mut self, timespec: types::Timespec) -> io::Result<SimpleOp> {
        let key = self.submit_timeout_at(timespec)?;
        Ok(SimpleOp::new(key))
    }

    /// Wait until `fd` is readable (or hung up / in error).
    pub fn poll_readable(&mut self, fd: RawFd) -> io::Result<SimpleOp> {
        let key = self.submit_poll_add(fd, libc::POLLIN as u32)?;
        Ok(SimpleOp::new(key))
    }

    /// Wait until `fd` is writable (or in error).
    pub fn poll_writable(&mut self, fd: RawFd) -> io::Result<SimpleOp> {
        let key = self.submit_poll_add(fd, libc::POLLOUT as u32)?;
        Ok(SimpleOp::new(key))
    }

    /// Start an asynchronous fsync operation on `fd`.
    pub fn fsync(&mut self, fd: RawFd, datasync: bool) -> io::Result<SimpleOp> {
        let key = self.submit_fsync(fd, datasync)?;
        Ok(SimpleOp::new(key))
    }

    /// Start an asynchronous openat operation on `dirfd` and `path`.
    ///
    /// The returned `AcceptOp` will close the resulting file descriptor if dropped before completion.
    pub fn openat(
        &mut self,
        dirfd: RawFd,
        path: &CStr,
        flags: i32,
        mode: u32,
    ) -> io::Result<AcceptOp> {
        let key = self.submit_openat(dirfd, path, flags, mode)?;
        Ok(AcceptOp::new(key))
    }

    /// Start an asynchronous close operation on `fd`.
    pub fn close(&mut self, fd: RawFd) -> io::Result<SimpleOp> {
        let key = self.submit_close(fd)?;
        Ok(SimpleOp::new(key))
    }

    /// Start an asynchronous sendmsg operation on `fd`.
    pub fn sendmsg<B>(
        &mut self,
        fd: RawFd,
        buf: B,
        addr: Option<SocketAddr>,
        flags: u32,
    ) -> io::Result<Op<B>>
    where
        B: crate::buf::IoBuf + Send + 'static,
    {
        let key = unsafe { self.submit_sendmsg(fd, &buf, addr, flags)? };
        Ok(unsafe { Op::new(key, buf) })
    }

    /// Start an asynchronous recvmsg operation on `fd`.
    pub fn recvmsg<B>(&mut self, fd: RawFd, mut buf: B, flags: u32) -> io::Result<Op<B>>
    where
        B: crate::buf::IoBufMut + Send + 'static,
    {
        let key = unsafe { self.submit_recvmsg(fd, &mut buf, flags)? };
        Ok(unsafe { Op::new(key, buf) })
    }

    /// Start an asynchronous UDP send_to operation to `addr`.
    pub fn send_to<B>(&mut self, fd: RawFd, buf: B, addr: SocketAddr) -> io::Result<Op<B>>
    where
        B: crate::buf::IoBuf + Send + 'static,
    {
        self.sendmsg(fd, buf, Some(addr), 0)
    }

    /// Start an asynchronous UDP recv_from operation.
    pub fn recv_from<B>(&mut self, fd: RawFd, buf: B) -> io::Result<Op<B>>
    where
        B: crate::buf::IoBufMut + Send + 'static,
    {
        self.recvmsg(fd, buf, 0)
    }

    // ── Zero-Copy Registered Buffers and Files ────────────────────────────────

    /// Register fixed buffers for use with [`read_fixed`](Self::read_fixed) and [`write_fixed`](Self::write_fixed).
    ///
    /// # Safety
    /// The memory regions described by `bufs` must remain allocated and valid until unregistered
    /// or until the reactor is destroyed.
    pub unsafe fn register_buffers(&self, bufs: &[libc::iovec]) -> io::Result<()> {
        unsafe { self.ring.submitter().register_buffers(bufs) }
    }

    /// Unregister all previously registered buffers.
    pub fn unregister_buffers(&self) -> io::Result<()> {
        self.ring.submitter().unregister_buffers()
    }

    /// Register fixed files for direct descriptor operations.
    pub fn register_files(&self, fds: &[RawFd]) -> io::Result<()> {
        self.ring.submitter().register_files(fds)
    }

    /// Unregister all previously registered fixed files.
    pub fn unregister_files(&self) -> io::Result<()> {
        self.ring.submitter().unregister_files()
    }

    // ── Poll / park ───────────────────────────────────────────────────────────

    /// Drain already-ready CQEs from userspace without a syscall.
    pub fn drain_ready(&mut self) -> io::Result<usize> {
        self.drain_completions()
    }

    /// Hand every queued SQE to the kernel without waiting for completions.
    pub(crate) fn flush_queued(&mut self) {
        let _ = self.ring.submit();
    }

    /// Flush buffered SQEs and drain available CQEs without blocking.
    pub fn submit_and_poll(&mut self) -> io::Result<usize> {
        // Note: submit_and_wait(0) submits pending SQEs and runs deferred task_work
        // in one syscall without blocking. If SQ is empty and CQ already has items,
        // drain them first; otherwise issue a non-blocking enter(0) to flush SQ/task_work.
        loop {
            match self.ring.submit_and_wait(0) {
                Ok(_) => break,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e),
            }
        }
        self.drain_completions()
    }

    /// Block until at least one CQE is available, then drain all.
    pub fn park(&mut self) -> io::Result<usize> {
        loop {
            match self.ring.submit_and_wait(1) {
                Ok(_) => break,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e),
            }
        }
        self.drain_completions()
    }

    fn drain_completions(&mut self) -> io::Result<usize> {
        let mut count = 0;
        // Process each CQE exactly once
        for cqe in self.ring.completion() {
            let user_data = cqe.user_data();
            if user_data == u64::MAX {
                continue; // ASYNC_CANCEL completion — ignore
            }
            if let Some(waker) = self.slab.complete(user_data, cqe.result()) {
                waker.wake();
            }
            count += 1;
        }
        Ok(count)
    }
}

/// Close `fd` after handing any queued operations to the kernel.
///
/// An SQE names its file by number and the kernel resolves the number when the SQE is
/// *submitted*, not when it is queued. If a descriptor were closed while an operation on it
/// still sat in the submission queue, the number could be reused (by the next `socket()` or
/// `accept`) and the stale operation would run on the wrong file, for example stealing the
/// first bytes of an unrelated connection. Submitting first pins the right file.
pub(crate) fn close_fd(fd: std::os::fd::RawFd) {
    if fd < 0 {
        return;
    }
    let _ = REACTOR.try_with(|r| {
        if let Ok(mut r) = r.try_borrow_mut() {
            r.flush_queued();
        }
    });
    // SAFETY: the caller owns `fd` and never uses it again.
    unsafe { libc::close(fd) };
}

// ── Helper: SocketAddr -> (libc::sockaddr_storage, libc::socklen_t) ────────────

fn socket_addr_to_libc(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match addr {
        SocketAddr::V4(v4) => {
            let sin = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from_ne_bytes(v4.ip().octets()),
                },
                sin_zero: [0; 8],
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &sin as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of::<libc::sockaddr_in>(),
                );
            }
            (
                storage,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(v6) => {
            let sin6 = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                },
                sin6_scope_id: v6.scope_id(),
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &sin6 as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of::<libc::sockaddr_in6>(),
                );
            }
            (
                storage,
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct TestBuffer {
        data: Vec<u8>,
        drop_count: Arc<AtomicUsize>,
    }

    unsafe impl crate::buf::IoBufMut for TestBuffer {
        fn stable_ptr_mut(&mut self) -> *mut u8 {
            self.data.as_mut_ptr()
        }
        fn bytes_total(&self) -> usize {
            self.data.capacity()
        }
        unsafe fn set_init(&mut self, n: usize) {
            unsafe { self.data.set_len(n) }
        }
    }

    unsafe impl crate::buf::IoBuf for TestBuffer {
        fn stable_ptr(&self) -> *const u8 {
            self.data.as_ptr()
        }
        fn bytes_init(&self) -> usize {
            self.data.len()
        }
    }

    impl Drop for TestBuffer {
        fn drop(&mut self) {
            self.drop_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_slab_generation_recycling() {
        let mut slab = CompletionSlab::new();

        let key0 = slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(slab::PlaceholderBuf),
        });
        assert_eq!(key0.index(), 0);
        assert_eq!(key0.generation(), 0);

        // Complete and consume
        let _ = slab.complete(key0.as_u64(), 10);
        slab.consume(key0);
        assert_eq!(slab.len(), 0);

        // Next insert at slot 0 should have generation 1
        let key1 = slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(slab::PlaceholderBuf),
        });
        assert_eq!(key1.index(), 0);
        assert_eq!(key1.generation(), 1);

        // Old key0 with generation 0 must be rejected
        let stale = slab.complete(key0.as_u64(), 99);
        assert!(stale.is_none());
        assert_eq!(slab.len(), 1);

        // New key1 completes properly
        let valid = slab.complete(key1.as_u64(), 20);
        assert!(valid.is_none());
        slab.consume(key1);
        assert_eq!(slab.len(), 0);
    }

    #[test]
    fn test_accept_close_on_cancel() {
        let mut slab = CompletionSlab::new();

        // Simulate an accept operation submitted
        let key = slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(slab::CloseOnDropFd),
        });

        // Op dropped before completion -> ignored
        assert!(slab.ignore_simple(key));

        // Create a socketpair: pair[0] simulates the accepted socket
        let mut pair = [0 as libc::c_int; 2];
        let ret =
            unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, pair.as_mut_ptr()) };
        assert_eq!(ret, 0);

        // CQE arrives for the ignored accept
        let _ = slab.complete(key.as_u64(), pair[0]);
        assert_eq!(slab.len(), 0);

        // pair[0] should have been closed automatically by CloseOnDropFd.
        // If pair[0] was closed, sending on pair[1] must return EPIPE (peer shutdown).
        let byte = [1u8];
        let sent = unsafe { libc::send(pair[1], byte.as_ptr() as *const _, 1, libc::MSG_NOSIGNAL) };
        assert_eq!(
            sent, -1,
            "pair[0] should have been closed on ignored completion"
        );
        let err = io::Error::last_os_error();
        assert_eq!(err.raw_os_error(), Some(libc::EPIPE));

        unsafe {
            libc::close(pair[1]);
        }
    }

    #[test]
    fn test_reactor_real_pipe_io() {
        let mut fds = [0 as libc::c_int; 2];
        let ret =
            unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(ret, 0);

        let write_buf = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mut write_op = REACTOR.with(|r| {
            r.borrow_mut()
                .write(fds[0], write_buf, 0)
                .expect("submit write failed")
        });

        let read_buf = vec![0u8; 8];
        let mut read_op = REACTOR.with(|r| {
            r.borrow_mut()
                .read(fds[1], read_buf, 0)
                .expect("submit read failed")
        });

        let waker = std::task::Waker::from(std::sync::Arc::new(DummyWake));
        let mut cx = std::task::Context::from_waker(&waker);

        use std::future::Future;
        use std::pin::Pin;

        let mut write_done = false;
        let mut read_done = false;

        while !write_done || !read_done {
            REACTOR.with(|r| r.borrow_mut().submit_and_poll().unwrap());

            if !write_done {
                if let std::task::Poll::Ready((res, buf)) = Pin::new(&mut write_op).poll(&mut cx) {
                    assert_eq!(res.unwrap(), 8);
                    assert_eq!(buf, vec![1, 2, 3, 4, 5, 6, 7, 8]);
                    write_done = true;
                }
            }
            if !read_done {
                if let std::task::Poll::Ready((res, buf)) = Pin::new(&mut read_op).poll(&mut cx) {
                    assert_eq!(res.unwrap(), 8);
                    assert_eq!(buf, vec![1, 2, 3, 4, 5, 6, 7, 8]);
                    read_done = true;
                }
            }
        }

        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }

    #[test]
    fn test_reactor_with_entries_forcing_sq_full() {
        let mut fds = [0 as libc::c_int; 2];
        let ret =
            unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(ret, 0);

        let mut reactor = Reactor::with_entries(4).expect("failed to create 4-entry reactor");

        // Submit 6 writes to a ring with depth 4.
        // The 5th write encounters an SQ-full condition, which triggers an automatic
        // internal submit() inside push_sqe to flush pending SQEs to the kernel.
        let mut ops = Vec::new();
        for i in 0..6 {
            let op = reactor
                .write(fds[0], vec![i as u8; 4], 0)
                .expect("write submission failed");
            ops.push(op);
        }

        // Drain all 6 completions
        let mut drained = 0;
        while drained < 6 {
            drained += reactor.submit_and_poll().expect("submit_and_poll failed");
        }
        assert_eq!(drained, 6);

        // Verify in the slab that each write succeeded with 4 bytes
        let waker = std::task::Waker::from(std::sync::Arc::new(DummyWake));
        for op in &ops {
            let res = reactor.slab.poll_result(op.key(), &waker);
            assert_eq!(
                res,
                Some(4),
                "each write should have completed with 4 bytes"
            );
        }

        // Prevent Op::drop from querying the global REACTOR
        for op in ops {
            std::mem::forget(op);
        }

        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }

    #[test]
    fn test_reactor_builder_sqpoll() {
        match ReactorBuilder::new().sqpoll(Some(50)).build() {
            Ok(mut reactor) => {
                let mut fds = [0 as libc::c_int; 2];
                let ret = unsafe {
                    libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr())
                };
                assert_eq!(ret, 0);

                let write_buf = vec![42u8; 4];
                let _write_op = reactor
                    .write(fds[0], write_buf, 0)
                    .expect("write submit failed");
                let read_buf = vec![0u8; 4];
                let _read_op = reactor
                    .read(fds[1], read_buf, 0)
                    .expect("read submit failed");

                let mut ready = 0;
                for _ in 0..100 {
                    if let Ok(n) = reactor.submit_and_poll() {
                        ready += n;
                        if ready >= 2 {
                            break;
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }

                std::mem::forget(_write_op);
                std::mem::forget(_read_op);

                unsafe {
                    libc::close(fds[0]);
                    libc::close(fds[1]);
                }
            }
            Err(e)
                if e.raw_os_error() == Some(libc::EPERM)
                    || e.raw_os_error() == Some(libc::EACCES) =>
            {
                // Gracefully skipped: kernel or environment requires CAP_SYS_NICE / root for SQPOLL
                eprintln!("Skipping SQPOLL test: insufficient privileges ({:?})", e);
            }
            Err(e) => {
                panic!("Unexpected error building SQPOLL reactor: {:?}", e);
            }
        }
    }

    #[test]
    fn test_reactor_sleep_5ms() {
        let start = std::time::Instant::now();
        let mut sleep_op = REACTOR.with(|r| {
            r.borrow_mut()
                .sleep(std::time::Duration::from_millis(5))
                .expect("sleep submission failed")
        });

        let waker = std::task::Waker::from(std::sync::Arc::new(DummyWake));
        let mut cx = std::task::Context::from_waker(&waker);

        use std::future::Future;
        use std::pin::Pin;

        loop {
            if let std::task::Poll::Ready(res) = Pin::new(&mut sleep_op).poll(&mut cx) {
                assert!(res.is_ok(), "sleep should succeed: {:?}", res);
                break;
            }
            REACTOR.with(|r| r.borrow_mut().park().expect("park failed"));
        }

        assert!(start.elapsed() >= std::time::Duration::from_millis(4));
    }

    #[test]
    fn test_openat_read_fsync_close() {
        let tmp_path = format!("/tmp/nexio_test_openat_{}.tmp\0", std::process::id());
        let c_path = std::ffi::CStr::from_bytes_with_nul(tmp_path.as_bytes()).unwrap();

        let waker = std::task::Waker::from(std::sync::Arc::new(DummyWake));
        let mut cx = std::task::Context::from_waker(&waker);
        use std::future::Future;
        use std::pin::Pin;

        // 1. Open file with O_CREAT | O_RDWR | O_TRUNC
        let mut open_op = REACTOR.with(|r| {
            r.borrow_mut()
                .openat(
                    libc::AT_FDCWD,
                    c_path,
                    libc::O_CREAT | libc::O_RDWR | libc::O_TRUNC,
                    0o644,
                )
                .expect("openat submission failed")
        });

        let fd = loop {
            if let std::task::Poll::Ready(res) = Pin::new(&mut open_op).poll(&mut cx) {
                break res.expect("openat failed");
            }
            REACTOR.with(|r| r.borrow_mut().park().expect("park failed"));
        };
        assert!(fd >= 0);

        // 2. Write data to the open file
        let write_buf = b"hello io_uring".to_vec();
        let mut write_op = REACTOR.with(|r| {
            r.borrow_mut()
                .write(fd, write_buf, 0)
                .expect("write submission failed")
        });
        loop {
            if let std::task::Poll::Ready((res, _)) = Pin::new(&mut write_op).poll(&mut cx) {
                assert_eq!(res.unwrap(), 14);
                break;
            }
            REACTOR.with(|r| r.borrow_mut().park().expect("park failed"));
        }

        // 3. Read data back from offset 0
        let read_buf = vec![0u8; 14];
        let mut read_op = REACTOR.with(|r| {
            r.borrow_mut()
                .read(fd, read_buf, 0)
                .expect("read submission failed")
        });
        loop {
            if let std::task::Poll::Ready((res, buf)) = Pin::new(&mut read_op).poll(&mut cx) {
                assert_eq!(res.unwrap(), 14);
                assert_eq!(&buf[..], b"hello io_uring");
                break;
            }
            REACTOR.with(|r| r.borrow_mut().park().expect("park failed"));
        }

        // 4. Fsync
        let mut fsync_op = REACTOR.with(|r| {
            r.borrow_mut()
                .fsync(fd, false)
                .expect("fsync submission failed")
        });
        loop {
            if let std::task::Poll::Ready(res) = Pin::new(&mut fsync_op).poll(&mut cx) {
                assert!(res.is_ok(), "fsync failed: {:?}", res);
                break;
            }
            REACTOR.with(|r| r.borrow_mut().park().expect("park failed"));
        }

        // 5. Close
        let mut close_op =
            REACTOR.with(|r| r.borrow_mut().close(fd).expect("close submission failed"));
        loop {
            if let std::task::Poll::Ready(res) = Pin::new(&mut close_op).poll(&mut cx) {
                assert!(res.is_ok(), "close failed: {:?}", res);
                break;
            }
            REACTOR.with(|r| r.borrow_mut().park().expect("park failed"));
        }

        // 6. Verify fd is now closed
        unsafe {
            let ret = libc::close(fd);
            assert_eq!(ret, -1);
            assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
            libc::unlink(c_path.as_ptr());
        }
    }

    #[test]
    fn test_register_buffers_read_write_fixed() {
        let mut fds = [0 as libc::c_int; 2];
        let ret =
            unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(ret, 0);

        let mut buf1 = vec![0u8; 16];
        let mut buf2 = vec![0u8; 16];
        buf1[..8].copy_from_slice(&[10, 20, 30, 40, 50, 60, 70, 80]);

        let iovecs = [
            libc::iovec {
                iov_base: buf1.as_mut_ptr() as *mut libc::c_void,
                iov_len: buf1.len(),
            },
            libc::iovec {
                iov_base: buf2.as_mut_ptr() as *mut libc::c_void,
                iov_len: buf2.len(),
            },
        ];

        REACTOR.with(|r| unsafe {
            r.borrow()
                .register_buffers(&iovecs)
                .expect("register_buffers failed");
        });

        let waker = std::task::Waker::from(std::sync::Arc::new(DummyWake));
        let mut cx = std::task::Context::from_waker(&waker);
        use std::future::Future;
        use std::pin::Pin;

        let mut write_op = REACTOR.with(|r| {
            r.borrow_mut()
                .write_fixed(fds[0], buf1, 0, 0)
                .expect("write_fixed submission failed")
        });

        let mut read_op = REACTOR.with(|r| {
            r.borrow_mut()
                .read_fixed(fds[1], buf2, 0, 1)
                .expect("read_fixed submission failed")
        });

        let mut write_done = false;
        let mut read_done = false;

        while !write_done || !read_done {
            REACTOR.with(|r| r.borrow_mut().submit_and_poll().unwrap());

            if !write_done {
                if let std::task::Poll::Ready((res, _)) = Pin::new(&mut write_op).poll(&mut cx) {
                    assert_eq!(res.unwrap(), 16);
                    write_done = true;
                }
            }
            if !read_done {
                if let std::task::Poll::Ready((res, buf)) = Pin::new(&mut read_op).poll(&mut cx) {
                    assert_eq!(res.unwrap(), 16);
                    assert_eq!(&buf[..8], &[10, 20, 30, 40, 50, 60, 70, 80]);
                    read_done = true;
                }
            }
        }

        REACTOR.with(|r| {
            r.borrow()
                .unregister_buffers()
                .expect("unregister_buffers failed");
        });

        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }

    #[test]
    fn test_register_files() {
        let reactor = Reactor::new().expect("reactor creation failed");
        let mut fds = [0 as libc::c_int; 2];
        unsafe {
            assert_eq!(
                libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()),
                0
            );
        }
        reactor.register_files(&fds).expect("register_files failed");
        reactor.unregister_files().expect("unregister_files failed");
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }

    #[test]
    fn test_udp_send_to_recv_from() {
        let fd1 = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
        let fd2 = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
        assert!(fd1 >= 0 && fd2 >= 0);

        // Bind fd2 to 127.0.0.1:0
        let mut addr: libc::sockaddr_in = unsafe { std::mem::zeroed() };
        addr.sin_family = libc::AF_INET as libc::sa_family_t;
        addr.sin_addr.s_addr = libc::INADDR_LOOPBACK.to_be();
        addr.sin_port = 0;
        let ret = unsafe {
            libc::bind(
                fd2,
                &addr as *const _ as *const libc::sockaddr,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        };
        assert_eq!(ret, 0);

        // Get dynamically assigned port
        let mut len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        let ret =
            unsafe { libc::getsockname(fd2, &mut addr as *mut _ as *mut libc::sockaddr, &mut len) };
        assert_eq!(ret, 0);
        let target_port = u16::from_be(addr.sin_port);
        let target_addr: std::net::SocketAddr =
            format!("127.0.0.1:{}", target_port).parse().unwrap();

        let waker = std::task::Waker::from(std::sync::Arc::new(DummyWake));
        let mut cx = std::task::Context::from_waker(&waker);
        use std::future::Future;
        use std::pin::Pin;

        let write_buf = vec![1, 2, 3, 4, 9, 8, 7, 6];
        let mut send_op = REACTOR.with(|r| {
            r.borrow_mut()
                .send_to(fd1, write_buf, target_addr)
                .expect("send_to failed")
        });

        let read_buf = vec![0u8; 8];
        let mut recv_op = REACTOR.with(|r| {
            r.borrow_mut()
                .recv_from(fd2, read_buf)
                .expect("recv_from failed")
        });

        let mut send_done = false;
        let mut recv_done = false;

        while !send_done || !recv_done {
            REACTOR.with(|r| r.borrow_mut().submit_and_poll().unwrap());

            if !send_done {
                if let std::task::Poll::Ready((res, _)) = Pin::new(&mut send_op).poll(&mut cx) {
                    assert_eq!(res.unwrap(), 8);
                    send_done = true;
                }
            }
            if !recv_done {
                if let std::task::Poll::Ready((res, buf)) = Pin::new(&mut recv_op).poll(&mut cx) {
                    assert_eq!(res.unwrap(), 8);
                    assert_eq!(buf, vec![1, 2, 3, 4, 9, 8, 7, 6]);
                    recv_done = true;
                }
            }
        }

        unsafe {
            libc::close(fd1);
            libc::close(fd2);
        }
    }

    #[test]
    fn test_reactor_real_cancel_on_drop() {
        let mut fds = [0 as libc::c_int; 2];
        let ret =
            unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(ret, 0);

        let drop_count = Arc::new(AtomicUsize::new(0));
        let buf = TestBuffer {
            data: vec![0u8; 64],
            drop_count: Arc::clone(&drop_count),
        };

        // Scope to drop the read op while in flight
        {
            let op = REACTOR.with(|r| {
                let mut r = r.borrow_mut();
                r.read(fds[1], buf, 0).expect("read submission failed")
            });

            // Poll once so SQE is submitted to kernel
            REACTOR.with(|r| r.borrow_mut().submit_and_poll().unwrap());

            // Op is dropped here while in flight!
            drop(op);
        }

        // Drain reactor completions until the cancelled op's CQE lands.
        REACTOR.with(|r| {
            let mut r = r.borrow_mut();
            let _ = r.submit_and_poll();
            while drop_count.load(Ordering::SeqCst) == 0 {
                let _ = r.park();
            }
        });

        // Now buffer must be dropped exactly once
        assert_eq!(drop_count.load(Ordering::SeqCst), 1);

        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }

    struct DummyWake;
    impl std::task::Wake for DummyWake {
        fn wake(self: std::sync::Arc<Self>) {}
    }
}
