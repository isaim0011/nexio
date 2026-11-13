//! `Op<B>` and specialized futures representing in-flight io_uring operations.
//!
//! Ownership flow:
//!   - Buffer lives in `Op::buf` (ManuallyDrop) while in-flight.
//!   - On `Poll::Ready`: take buf, return to caller via BufResult.
//!   - On `Drop` while in-flight: take buf, move into slab as Ignored.
//!     Slab drops buf when the real CQE arrives.
//!
//! This is the "leak-until-real-completion" pattern. No double-free, no
//! blocking Drop, no use-after-free.

use std::{
    future::Future,
    io,
    mem::ManuallyDrop,
    os::unix::io::RawFd,
    pin::Pin,
    task::{Context, Poll},
};

use super::{
    slab::{ErasedBox, ErasedBuf, SlabKey},
    with_uring_mut,
};
use crate::buf::BufResult;

/// Lifecycle state for an in-flight operation future.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OpLifecycle {
    InFlight,
    Complete(i32),
}

/// A `Future` wrapping one in-flight io_uring operation with an owned buffer.
///
/// `!Send` by construction: reactor is thread-local and the buffer pointer
/// registered with the kernel must not migrate threads.
pub struct Op<B: Unpin + Send + 'static> {
    key: SlabKey,
    buf: ManuallyDrop<B>,
    lifecycle: OpLifecycle,
}

impl<B: Unpin + Send + 'static> Op<B> {
    /// Create a new in-flight operation from a submitted key and buffer.
    ///
    /// # Safety
    /// Caller must have submitted an SQE with `user_data = key.as_u64()` and
    /// transferred ownership of `buf`'s backing memory to the kernel.
    pub unsafe fn new(key: SlabKey, buf: B) -> Self {
        Self {
            key,
            buf: ManuallyDrop::new(buf),
            lifecycle: OpLifecycle::InFlight,
        }
    }

    /// Returns the key associated with this operation.
    pub fn key(&self) -> SlabKey {
        self.key
    }
}

impl<B: Unpin + Send + 'static> Future for Op<B> {
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let OpLifecycle::Complete(_) = self.lifecycle {
            panic!("Op polled after completion");
        }

        let key = self.key;
        // Check if reactor already completed this op and consume slot in one borrow.
        let result = with_uring_mut(|r| r.slab.poll_and_consume(key, cx.waker()));

        match result {
            None => Poll::Pending,
            Some(raw) => {
                let this = self.get_mut();
                // SAFETY: this branch runs exactly once; ManuallyDrop::take
                // leaves buf logically uninitialised, which is fine — Drop
                // won't run on it again because we're in the Complete branch.
                let buf = unsafe { ManuallyDrop::take(&mut this.buf) };
                this.lifecycle = OpLifecycle::Complete(raw);

                let io_result = if raw >= 0 {
                    Ok(raw as usize)
                } else {
                    Err(io::Error::from_raw_os_error(-raw))
                };

                Poll::Ready((io_result, buf))
            }
        }
    }
}

impl<B: Unpin + Send + 'static> Drop for Op<B> {
    fn drop(&mut self) {
        if self.lifecycle == OpLifecycle::InFlight {
            // Kernel still has a pointer into our buffer.
            // 1. Take buf out of ManuallyDrop.
            // 2. Move it into the slab as Ignored.
            // 3. Submit best-effort ASYNC_CANCEL if still in flight.
            let buf = unsafe { ManuallyDrop::take(&mut self.buf) };
            let boxed: Box<dyn ErasedBuf> = Box::new(ErasedBox(buf));

            with_uring_mut(|reactor| {
                if reactor.slab.ignore(self.key, boxed) {
                    let _ = reactor.submit_cancel(self.key);
                }
            });
        }
        // If Complete, buf was already taken in poll(). ManuallyDrop: no-op.
    }
}

enum LazyPhase<F> {
    Pending(Option<F>),
    InFlight(SlabKey),
    Complete,
}

/// A `Future` that submits an `io_uring` op and registers the waker in a single
/// reactor borrow on its first poll, preserving full cancel-on-drop safety.
pub struct LazyOp<B: Unpin + Send + 'static, F> {
    buf: ManuallyDrop<B>,
    phase: LazyPhase<F>,
}

impl<B, F> LazyOp<B, F>
where
    B: Unpin + Send + 'static,
    F: FnOnce(&mut super::Reactor, &mut B, std::task::Waker) -> io::Result<SlabKey> + Unpin,
{
    /// Create a new `LazyOp` owning `buf` and a submission closure `submit`.
    pub fn new(buf: B, submit: F) -> Self {
        Self {
            buf: ManuallyDrop::new(buf),
            phase: LazyPhase::Pending(Some(submit)),
        }
    }
}

impl<B, F> Future for LazyOp<B, F>
where
    B: Unpin + Send + 'static,
    F: FnOnce(&mut super::Reactor, &mut B, std::task::Waker) -> io::Result<SlabKey> + Unpin,
{
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.phase {
            LazyPhase::Pending(submit) => {
                let submit_fn = submit.take().expect("LazyOp missing submit closure");
                let buf_ref = &mut *this.buf;
                let res = with_uring_mut(|reactor| submit_fn(reactor, buf_ref, cx.waker().clone()));
                match res {
                    Ok(key) => {
                        this.phase = LazyPhase::InFlight(key);
                        Poll::Pending
                    }
                    Err(e) => {
                        let taken = unsafe { ManuallyDrop::take(&mut this.buf) };
                        this.phase = LazyPhase::Complete;
                        Poll::Ready((Err(e), taken))
                    }
                }
            }
            LazyPhase::InFlight(key) => {
                let k = *key;
                let result = with_uring_mut(|r| r.slab.poll_and_consume(k, cx.waker()));
                match result {
                    None => Poll::Pending,
                    Some(raw) => {
                        let taken = unsafe { ManuallyDrop::take(&mut this.buf) };
                        this.phase = LazyPhase::Complete;
                        let io_result = if raw >= 0 {
                            Ok(raw as usize)
                        } else {
                            Err(io::Error::from_raw_os_error(-raw))
                        };
                        Poll::Ready((io_result, taken))
                    }
                }
            }
            LazyPhase::Complete => panic!("LazyOp polled after completion"),
        }
    }
}

impl<B: Unpin + Send + 'static, F> Drop for LazyOp<B, F> {
    fn drop(&mut self) {
        match self.phase {
            LazyPhase::Pending(_) => {
                // Never submitted to kernel; drop buffer directly.
                unsafe { ManuallyDrop::drop(&mut self.buf) };
            }
            LazyPhase::InFlight(key) => {
                let taken = unsafe { ManuallyDrop::take(&mut self.buf) };
                let boxed: Box<dyn ErasedBuf> = Box::new(ErasedBox(taken));
                with_uring_mut(|reactor| {
                    if reactor.slab.ignore(key, boxed) {
                        let _ = reactor.submit_cancel(key);
                    }
                });
            }
            LazyPhase::Complete => {}
        }
    }
}

/// A future representing an in-flight operation returning a bare `i32` result.
pub struct SimpleOp {
    key: SlabKey,
    lifecycle: OpLifecycle,
}

impl SimpleOp {
    /// Create a new `SimpleOp` wrapping `key`.
    pub fn new(key: SlabKey) -> Self {
        Self {
            key,
            lifecycle: OpLifecycle::InFlight,
        }
    }

    /// Return the slab key.
    pub fn key(&self) -> SlabKey {
        self.key
    }
}

impl Future for SimpleOp {
    type Output = io::Result<i32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let OpLifecycle::Complete(_) = self.lifecycle {
            panic!("SimpleOp polled after completion");
        }

        let key = self.key;
        let this = self.get_mut();
        with_uring_mut(
            |reactor| match reactor.slab.poll_and_consume(key, cx.waker()) {
                None => Poll::Pending,
                Some(result) => {
                    this.lifecycle = OpLifecycle::Complete(result);
                    if result >= 0 {
                        Poll::Ready(Ok(result))
                    } else if result == -libc::ETIME {
                        // Timer expired without error
                        Poll::Ready(Ok(0))
                    } else {
                        Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
                    }
                }
            },
        )
    }
}

impl Drop for SimpleOp {
    fn drop(&mut self) {
        if self.lifecycle == OpLifecycle::InFlight {
            with_uring_mut(|reactor| {
                if reactor.slab.ignore_simple(self.key) {
                    let _ = reactor.submit_cancel(self.key);
                }
            });
        }
    }
}

/// A future representing an in-flight TCP Accept operation.
pub struct AcceptOp {
    key: SlabKey,
    lifecycle: OpLifecycle,
}

impl AcceptOp {
    /// Create a new `AcceptOp` wrapping `key`.
    pub fn new(key: SlabKey) -> Self {
        Self {
            key,
            lifecycle: OpLifecycle::InFlight,
        }
    }

    /// Return the slab key.
    pub fn key(&self) -> SlabKey {
        self.key
    }
}

impl Future for AcceptOp {
    type Output = io::Result<RawFd>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let OpLifecycle::Complete(_) = self.lifecycle {
            panic!("AcceptOp polled after completion");
        }

        let key = self.key;
        let this = self.get_mut();
        with_uring_mut(
            |reactor| match reactor.slab.poll_and_consume(key, cx.waker()) {
                None => Poll::Pending,
                Some(result) => {
                    this.lifecycle = OpLifecycle::Complete(result);
                    if result >= 0 {
                        Poll::Ready(Ok(result as RawFd))
                    } else {
                        Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
                    }
                }
            },
        )
    }
}

impl Drop for AcceptOp {
    fn drop(&mut self) {
        if self.lifecycle == OpLifecycle::InFlight {
            with_uring_mut(|reactor| {
                if reactor.slab.ignore_accept(self.key) {
                    let _ = reactor.submit_cancel(self.key);
                }
            });
        }
    }
}

/// A future representing an in-flight TCP Connect operation.
pub struct ConnectOp {
    key: SlabKey,
    lifecycle: OpLifecycle,
}

impl ConnectOp {
    /// Create a new `ConnectOp` wrapping `key`.
    pub fn new(key: SlabKey) -> Self {
        Self {
            key,
            lifecycle: OpLifecycle::InFlight,
        }
    }

    /// Return the slab key.
    pub fn key(&self) -> SlabKey {
        self.key
    }
}

impl Future for ConnectOp {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let OpLifecycle::Complete(_) = self.lifecycle {
            panic!("ConnectOp polled after completion");
        }

        let key = self.key;
        let this = self.get_mut();
        with_uring_mut(
            |reactor| match reactor.slab.poll_and_consume(key, cx.waker()) {
                None => Poll::Pending,
                Some(result) => {
                    this.lifecycle = OpLifecycle::Complete(result);
                    if result >= 0 {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
                    }
                }
            },
        )
    }
}

impl Drop for ConnectOp {
    fn drop(&mut self) {
        if self.lifecycle == OpLifecycle::InFlight {
            with_uring_mut(|reactor| {
                if reactor.slab.ignore_simple(self.key) {
                    let _ = reactor.submit_cancel(self.key);
                }
            });
        }
    }
}
