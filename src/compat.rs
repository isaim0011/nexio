//! Adapters between nexio's owned-buffer streams and the poll-based I/O traits the
//! wider ecosystem uses (`tokio::io::AsyncRead`/`AsyncWrite`, and hyper's `Read`/`Write`).
//!
//! nexio's `read(buf).await` takes ownership of a buffer and gives it back; the
//! ecosystem's `poll_read(&mut [u8])` borrows one. [`Compat`] bridges them with an
//! internal buffer:
//!
//! * **Reads** go into an internal buffer; `poll_read` hands out its contents, so a
//!   caller with a small destination never causes bytes to be dropped.
//! * **Writes** are copied into an internal buffer and sent in the background of the
//!   next call. `poll_write` reports the bytes accepted as soon as they are queued, and
//!   a failure of an earlier write surfaces on the following `poll_write`, `poll_flush`
//!   or `poll_shutdown`. Call `flush` (or `shutdown`) to wait until everything has
//!   actually been written, as with any buffered writer.
//! * Dropping the adapter, or a task that was polling it, cancels the in-flight
//!   operation through nexio's normal cancellation-safe path.
//!
//! The adapter is `!Send`, like the streams it wraps. Enable the `tokio-compat` feature
//! for the tokio traits; the `http` feature enables hyper's.

// Without an I/O-trait feature enabled, the adapter's poll methods have no caller.
#![cfg_attr(not(any(feature = "tokio-compat", feature = "http")), allow(dead_code))]

use std::{
    future::Future,
    io,
    mem::MaybeUninit,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

/// Bytes read from the stream per underlying read.
const MIN_READ: usize = 16 * 1024;
/// Largest adaptive read size. A connection only gets here by repeatedly filling its buffer.
const MAX_READ: usize = 256 * 1024;
/// Most bytes accepted into the write buffer per `poll_write`.
const WRITE_CHUNK: usize = 256 * 1024;

type BoxFut = Pin<Box<dyn Future<Output = (io::Result<usize>, Vec<u8>)>>>;
type ShutdownFut = Pin<Box<dyn Future<Output = io::Result<()>>>>;

/// A stream the adapter can drive: owned-buffer reads and writes plus a write-half shutdown.
///
/// Implemented for nexio's TCP and Unix streams (and TLS streams with the `tls`
/// feature). It is sealed: you do not implement it yourself.
pub trait Duplex: sealed::Sealed + Sized + 'static {
    #[doc(hidden)]
    fn read_owned(self: Rc<Self>, buf: Vec<u8>) -> BoxFut;
    #[doc(hidden)]
    fn write_all_owned(self: Rc<Self>, buf: Vec<u8>) -> BoxFut;
    #[doc(hidden)]
    fn shutdown_owned(self: Rc<Self>) -> ShutdownFut;
}

pub(crate) mod sealed {
    pub trait Sealed {}
}

macro_rules! impl_duplex {
    ($ty:ty) => {
        impl sealed::Sealed for $ty {}
        impl Duplex for $ty {
            fn read_owned(self: Rc<Self>, buf: Vec<u8>) -> BoxFut {
                Box::pin(async move { self.read(buf).await })
            }
            fn write_all_owned(self: Rc<Self>, buf: Vec<u8>) -> BoxFut {
                Box::pin(async move {
                    let len = buf.len();
                    let (res, buf) = self.write_all(buf).await;
                    (res.map(|()| len), buf)
                })
            }
            fn shutdown_owned(self: Rc<Self>) -> ShutdownFut {
                Box::pin(async move { self.shutdown(std::net::Shutdown::Write) })
            }
        }
    };
}

impl_duplex!(crate::net::TcpStream);
#[cfg(unix)]
impl_duplex!(crate::net::UnixStream);

enum ReadState {
    Idle(Option<Vec<u8>>),
    Pending(BoxFut),
    Buffered {
        buf: Vec<u8>,
        pos: usize,
        len: usize,
    },
}

enum WriteState {
    Idle(Option<Vec<u8>>),
    Pending(BoxFut),
}

/// Wraps a nexio stream so it implements the ecosystem's poll-based I/O traits.
pub struct Compat<S: Duplex> {
    stream: Rc<S>,
    read: ReadState,
    write: WriteState,
    /// A write that finished (with an error) while it was being started; reported by the next call.
    write_err: Option<io::Error>,
    shutdown: ShutdownState,
    /// Current read size: grows when reads keep filling the buffer, shrinks back when they stay small.
    read_hint: usize,
    small_reads: u8,
}

enum ShutdownState {
    NotStarted,
    Pending(ShutdownFut),
    Done,
}

impl<S: Duplex> Compat<S> {
    /// Wrap `stream`.
    pub fn new(stream: S) -> Self {
        Self {
            stream: Rc::new(stream),
            read: ReadState::Idle(None),
            write: WriteState::Idle(None),
            write_err: None,
            shutdown: ShutdownState::NotStarted,
            read_hint: MIN_READ,
            small_reads: 0,
        }
    }

    /// A shared reference to the wrapped stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Read into `dst`. Returns `Ready(Ok(0))` at end of stream.
    pub(crate) fn poll_read_uninit(
        &mut self,
        cx: &mut Context<'_>,
        dst: &mut [MaybeUninit<u8>],
    ) -> Poll<io::Result<usize>> {
        if dst.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            match &mut self.read {
                ReadState::Buffered { buf, pos, len } => {
                    let n = dst.len().min(*len - *pos);
                    // SAFETY: `n <= dst.len()` and `n <= len - pos <= buf.len() - pos`, so both ranges
                    // are in bounds; `u8` and `MaybeUninit<u8>` have the same layout, and the buffers
                    // are distinct allocations, so they cannot overlap.
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            buf.as_ptr().add(*pos),
                            dst.as_mut_ptr().cast::<u8>(),
                            n,
                        );
                    }
                    *pos += n;
                    if *pos == *len {
                        let spare = std::mem::take(buf);
                        self.read = ReadState::Idle(Some(spare));
                    }
                    return Poll::Ready(Ok(n));
                }
                ReadState::Idle(spare) => {
                    let mut buf = spare.take().unwrap_or_default();
                    buf.clear();
                    let want = self.read_hint;
                    if buf.capacity() > want * 2 {
                        buf = Vec::with_capacity(want);
                    } else {
                        buf.reserve(want);
                    }
                    self.read = ReadState::Pending(self.stream.clone().read_owned(buf));
                }
                ReadState::Pending(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready((res, buf)) => match res {
                        Ok(0) => {
                            self.read = ReadState::Idle(Some(buf));
                            return Poll::Ready(Ok(0));
                        }
                        Ok(len) => {
                            // A reader that keeps filling its buffer is handed a bigger one, so a
                            // burst of large messages arrives in few reads instead of many. The
                            // consumer then handles a whole burst before it next waits on the
                            // socket, which is what lets a server fan it out in one batch.
                            if len * 10 >= buf.capacity() * 9 {
                                self.read_hint = (self.read_hint * 2).min(MAX_READ);
                                self.small_reads = 0;
                            } else if len * 8 < self.read_hint {
                                self.small_reads += 1;
                                if self.small_reads >= 32 {
                                    self.read_hint = (self.read_hint / 2).max(MIN_READ);
                                    self.small_reads = 0;
                                }
                            } else {
                                self.small_reads = 0;
                            }
                            self.read = ReadState::Buffered { buf, pos: 0, len }
                        }
                        Err(e) => {
                            self.read = ReadState::Idle(Some(buf));
                            return Poll::Ready(Err(e));
                        }
                    },
                },
            }
        }
    }

    /// Drive an in-flight write to completion; `Ready(Ok)` when nothing is in flight.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(e) = self.write_err.take() {
            return Poll::Ready(Err(e));
        }
        if let WriteState::Pending(fut) = &mut self.write {
            match fut.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready((res, buf)) => {
                    self.write = WriteState::Idle(Some(buf));
                    res?;
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    pub(crate) fn poll_write_bytes(
        &mut self,
        cx: &mut Context<'_>,
        src: &[u8],
    ) -> Poll<io::Result<usize>> {
        if src.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.poll_drain(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        let WriteState::Idle(spare) = &mut self.write else {
            unreachable!("poll_drain leaves the write state idle");
        };
        let mut buf = spare.take().unwrap_or_default();
        buf.clear();
        let n = src.len().min(WRITE_CHUNK);
        buf.extend_from_slice(&src[..n]);
        let mut fut = self.stream.clone().write_all_owned(buf);
        // Start the write right away: a future that is never polled never submits its I/O, and
        // nothing would wake the caller to poll it later (hyper, for one, may not flush).
        match fut.as_mut().poll(cx) {
            Poll::Pending => self.write = WriteState::Pending(fut),
            Poll::Ready((res, buf)) => {
                self.write = WriteState::Idle(Some(buf));
                if let Err(e) = res {
                    self.write_err = Some(e);
                }
            }
        }
        Poll::Ready(Ok(n))
    }

    pub(crate) fn poll_flush_all(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_drain(cx)
    }

    pub(crate) fn poll_shutdown_write(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_drain(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        loop {
            match &mut self.shutdown {
                ShutdownState::Done => return Poll::Ready(Ok(())),
                ShutdownState::NotStarted => {
                    self.shutdown = ShutdownState::Pending(self.stream.clone().shutdown_owned());
                }
                ShutdownState::Pending(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(res) => {
                        self.shutdown = ShutdownState::Done;
                        return Poll::Ready(res);
                    }
                },
            }
        }
    }
}

#[cfg(feature = "tokio-compat")]
mod tokio_impl {
    use super::*;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    impl<S: Duplex> AsyncRead for Compat<S> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            // SAFETY: `poll_read_uninit` only writes initialized bytes into the slice and never
            // de-initializes any of it.
            let dst = unsafe { buf.unfilled_mut() };
            let n = match this.poll_read_uninit(cx, dst) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(n)) => n,
            };
            // SAFETY: `poll_read_uninit` initialized exactly the first `n` bytes of the slice.
            unsafe { buf.assume_init(n) };
            buf.advance(n);
            Poll::Ready(Ok(()))
        }
    }

    impl<S: Duplex> AsyncWrite for Compat<S> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().poll_write_bytes(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.get_mut().poll_flush_all(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.get_mut().poll_shutdown_write(cx)
        }
    }
}

#[cfg(feature = "http")]
mod hyper_impl {
    use super::*;

    impl<S: Duplex> hyper::rt::Read for Compat<S> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            mut buf: hyper::rt::ReadBufCursor<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            // SAFETY: the slice is only written through `MaybeUninit::write`, never read.
            let dst = unsafe { buf.as_mut() };
            let n = match this.poll_read_uninit(cx, dst) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(n)) => n,
            };
            // SAFETY: `poll_read_uninit` initialized exactly the first `n` bytes.
            unsafe { buf.advance(n) };
            Poll::Ready(Ok(()))
        }
    }

    impl<S: Duplex> hyper::rt::Write for Compat<S> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().poll_write_bytes(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.get_mut().poll_flush_all(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.get_mut().poll_shutdown_write(cx)
        }
    }
}
