//! Unix-domain sockets (Unix only), one implementation for both backends.
//!
//! The sockets are non-blocking `socket2` sockets. Each operation tries the
//! syscall and, when it would block, waits for readiness and tries again. The wait
//! is the only backend-specific part: the portable backend uses its reactor, the
//! io_uring backend uses a one-shot `POLL_ADD` (which holds no buffer, so dropping a
//! pending wait is trivially safe). Nothing here ever hands the kernel a pointer to a
//! caller's buffer, so cancelling an operation just abandons the wait.
//!
//! Addresses are filesystem paths. Abstract-namespace and unnamed sockets report
//! `None` for their path.

use std::{
    io,
    mem::MaybeUninit,
    net::Shutdown,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    time::Duration,
};

use socket2::{Domain, SockAddr, Socket, Type};

use crate::buf::{BufResult, IoBuf, IoBufMut};

#[cfg(nexio_poll)]
use super::sock::Io as Sock;

/// A non-blocking socket plus the ability to wait for readiness (io_uring backend).
#[cfg(nexio_uring)]
struct Sock {
    sock: Socket,
}

#[cfg(nexio_uring)]
impl Sock {
    fn new(sock: Socket) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        Ok(Self { sock })
    }

    async fn ready(&self, write: bool) -> io::Result<()> {
        let fd = self.sock.as_raw_fd();
        let op = crate::reactor::REACTOR.with(|r| {
            let mut r = r.borrow_mut();
            if write {
                r.poll_writable(fd)
            } else {
                r.poll_readable(fd)
            }
        })?;
        op.await.map(|_| ())
    }
}

fn path_of(addr: &SockAddr) -> Option<PathBuf> {
    addr.as_pathname().map(Path::to_path_buf)
}

fn would_block(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::WouldBlock
}

fn send_nosig(sock: &Socket, data: &[u8]) -> io::Result<usize> {
    #[cfg(not(target_vendor = "apple"))]
    {
        sock.send_with_flags(data, libc::MSG_NOSIGNAL)
    }
    #[cfg(target_vendor = "apple")]
    {
        sock.set_nosigpipe(true)?;
        sock.send(data)
    }
}

// ── UnixListener ─────────────────────────────────────────────────────────────

/// A Unix-domain stream listener.
pub struct UnixListener {
    io: Sock,
}

impl UnixListener {
    /// Bind to a filesystem path and start listening. The path must not already
    /// exist; remove a stale socket file first.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let sock = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        sock.bind(&SockAddr::unix(path)?)?;
        sock.listen(1024)?;
        Ok(Self {
            io: Sock::new(sock)?,
        })
    }

    /// Wrap a standard-library listener.
    pub fn from_std(listener: std::os::unix::net::UnixListener) -> io::Result<Self> {
        Ok(Self {
            io: Sock::new(Socket::from(listener))?,
        })
    }

    /// The path this listener is bound to, if it has one.
    pub fn local_path(&self) -> io::Result<Option<PathBuf>> {
        Ok(path_of(&self.io.sock.local_addr()?))
    }

    /// Accept the next connection.
    pub async fn accept(&self) -> io::Result<UnixStream> {
        loop {
            match self.io.sock.accept() {
                Ok((sock, _)) => {
                    return Ok(UnixStream {
                        io: Sock::new(sock)?,
                    })
                }
                Err(e) if would_block(&e) => self.io.ready(false).await?,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// Take the pending socket error, if any.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.io.sock.take_error()
    }

    /// The raw file descriptor.
    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.io.sock.as_raw_fd()
    }
}

// ── UnixStream ───────────────────────────────────────────────────────────────

/// A Unix-domain stream connection.
pub struct UnixStream {
    io: Sock,
}

impl UnixStream {
    /// Connect to the listener at `path`.
    ///
    /// A Unix-domain connect either succeeds at once or fails; when the listener's
    /// backlog is full it reports `WouldBlock` with no readiness signal to wait on, so
    /// this retries with a short, growing delay until the connection is accepted into
    /// the queue (or the future is dropped).
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let addr = SockAddr::unix(path)?;
        let io = Sock::new(Socket::new(Domain::UNIX, Type::STREAM, None)?)?;
        let mut delay = Duration::from_micros(200);
        loop {
            match io.sock.connect(&addr) {
                Ok(()) => return Ok(Self { io }),
                Err(e) if would_block(&e) => {
                    crate::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_millis(10));
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// A connected pair of streams.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (a, b) = Socket::pair(Domain::UNIX, Type::STREAM, None)?;
        Ok((Self { io: Sock::new(a)? }, Self { io: Sock::new(b)? }))
    }

    /// Wrap a standard-library stream.
    pub fn from_std(stream: std::os::unix::net::UnixStream) -> io::Result<Self> {
        Ok(Self {
            io: Sock::new(Socket::from(stream))?,
        })
    }

    /// The local path, if it has one.
    pub fn local_path(&self) -> io::Result<Option<PathBuf>> {
        Ok(path_of(&self.io.sock.local_addr()?))
    }

    /// The peer's path, if it has one.
    pub fn peer_path(&self) -> io::Result<Option<PathBuf>> {
        Ok(path_of(&self.io.sock.peer_addr()?))
    }

    /// Read into `buf`; the buffer's initialized length becomes the bytes read
    /// (0 means the peer closed).
    pub async fn read<B>(&self, mut buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        loop {
            let total = buf.bytes_total();
            let ptr = buf.stable_ptr_mut() as *mut MaybeUninit<u8>;
            // SAFETY: `[ptr, ptr + total)` is this buffer's allocation, owned exclusively
            // here; `MaybeUninit<u8>` makes no claim that the bytes are initialized.
            let spare = unsafe { std::slice::from_raw_parts_mut(ptr, total) };
            match self.io.sock.recv(spare) {
                Ok(n) => {
                    // SAFETY: recv wrote `n` bytes at the start of the buffer.
                    unsafe { buf.set_init(n) };
                    return (Ok(n), buf);
                }
                Err(e) if would_block(&e) => {
                    if let Err(e) = self.io.ready(false).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
        }
    }

    /// Read until `buf` is full; `UnexpectedEof` if the peer closes first.
    pub async fn read_exact<B>(&self, mut buf: B) -> BufResult<(), B>
    where
        B: IoBufMut + Send + 'static,
    {
        let total = buf.bytes_total();
        let mut got = 0usize;
        while got < total {
            // SAFETY: `got < total`, so this stays inside the buffer's allocation.
            let ptr = unsafe { buf.stable_ptr_mut().add(got) } as *mut MaybeUninit<u8>;
            // SAFETY: as in `read`.
            let spare = unsafe { std::slice::from_raw_parts_mut(ptr, total - got) };
            match self.io.sock.recv(spare) {
                Ok(0) => {
                    let e =
                        io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer");
                    return (Err(e), buf);
                }
                Ok(n) => got += n,
                Err(e) if would_block(&e) => {
                    if let Err(e) = self.io.ready(false).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
        }
        // SAFETY: `total` bytes have been written into the buffer.
        unsafe { buf.set_init(total) };
        (Ok(()), buf)
    }

    /// Write from `buf`, returning how many bytes were sent.
    pub async fn write<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        loop {
            // SAFETY: the buffer guarantees `bytes_init()` initialized bytes at `stable_ptr()`.
            let data = unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) };
            match send_nosig(&self.io.sock, data) {
                Ok(n) => return (Ok(n), buf),
                Err(e) if would_block(&e) => {
                    if let Err(e) = self.io.ready(true).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
        }
    }

    /// Write all of `buf`.
    pub async fn write_all<B>(&self, buf: B) -> BufResult<(), B>
    where
        B: IoBuf + Send + 'static,
    {
        let total = buf.bytes_init();
        let mut sent = 0usize;
        while sent < total {
            // SAFETY: `sent < total <= bytes_init()`, inside the initialized region.
            let data =
                unsafe { std::slice::from_raw_parts(buf.stable_ptr().add(sent), total - sent) };
            match send_nosig(&self.io.sock, data) {
                Ok(0) => {
                    let e =
                        io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer");
                    return (Err(e), buf);
                }
                Ok(n) => sent += n,
                Err(e) if would_block(&e) => {
                    if let Err(e) = self.io.ready(true).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
        }
        (Ok(()), buf)
    }

    /// Shut down the read half, write half, or both.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.io.sock.shutdown(how)
    }

    /// Take the pending socket error, if any.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.io.sock.take_error()
    }

    /// The raw file descriptor.
    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.io.sock.as_raw_fd()
    }
}

// ── UnixDatagram ─────────────────────────────────────────────────────────────

/// A Unix-domain datagram socket.
pub struct UnixDatagram {
    io: Sock,
}

impl UnixDatagram {
    /// Bind to a filesystem path. The path must not already exist.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let sock = Socket::new(Domain::UNIX, Type::DGRAM, None)?;
        sock.bind(&SockAddr::unix(path)?)?;
        Ok(Self {
            io: Sock::new(sock)?,
        })
    }

    /// A socket with no address; it can send but only receives replies if connected.
    pub fn unbound() -> io::Result<Self> {
        Ok(Self {
            io: Sock::new(Socket::new(Domain::UNIX, Type::DGRAM, None)?)?,
        })
    }

    /// A connected pair of datagram sockets.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (a, b) = Socket::pair(Domain::UNIX, Type::DGRAM, None)?;
        Ok((Self { io: Sock::new(a)? }, Self { io: Sock::new(b)? }))
    }

    /// Set the default peer for [`send`](Self::send) and [`recv`](Self::recv).
    pub fn connect(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.io.sock.connect(&SockAddr::unix(path)?)
    }

    /// The local path, if it has one.
    pub fn local_path(&self) -> io::Result<Option<PathBuf>> {
        Ok(path_of(&self.io.sock.local_addr()?))
    }

    /// Send `buf` to the socket bound at `path`.
    pub async fn send_to<B>(&self, buf: B, path: impl AsRef<Path>) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        let addr = match SockAddr::unix(path) {
            Ok(a) => a,
            Err(e) => return (Err(e), buf),
        };
        loop {
            // SAFETY: the buffer guarantees `bytes_init()` initialized bytes at `stable_ptr()`.
            let data = unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) };
            match self.io.sock.send_to(data, &addr) {
                Ok(n) => return (Ok(n), buf),
                Err(e) if would_block(&e) => {
                    if let Err(e) = self.io.ready(true).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
        }
    }

    /// Send `buf` to the connected peer.
    pub async fn send<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        loop {
            // SAFETY: as in `send_to`.
            let data = unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) };
            match self.io.sock.send(data) {
                Ok(n) => return (Ok(n), buf),
                Err(e) if would_block(&e) => {
                    if let Err(e) = self.io.ready(true).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
        }
    }

    /// Receive one datagram and report the sender's path, if it has one.
    pub async fn recv_from<B>(&self, mut buf: B) -> (io::Result<(usize, Option<PathBuf>)>, B)
    where
        B: IoBufMut + Send + 'static,
    {
        loop {
            let total = buf.bytes_total();
            let ptr = buf.stable_ptr_mut() as *mut MaybeUninit<u8>;
            // SAFETY: `[ptr, ptr + total)` is this buffer's allocation, owned exclusively here.
            let spare = unsafe { std::slice::from_raw_parts_mut(ptr, total) };
            match self.io.sock.recv_from(spare) {
                Ok((n, from)) => {
                    // SAFETY: recv_from wrote `n` bytes at the start of the buffer.
                    unsafe { buf.set_init(n) };
                    return (Ok((n, path_of(&from))), buf);
                }
                Err(e) if would_block(&e) => {
                    if let Err(e) = self.io.ready(false).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
        }
    }

    /// Receive one datagram from the connected peer.
    pub async fn recv<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        let (res, buf) = self.recv_from(buf).await;
        (res.map(|(n, _)| n), buf)
    }

    /// Take the pending socket error, if any.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.io.sock.take_error()
    }

    /// The raw file descriptor.
    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.io.sock.as_raw_fd()
    }
}
