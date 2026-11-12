//! TCP on the portable readiness backend.
//!
//! The API mirrors the io_uring backend (owned buffers in, buffers back out)
//! so code written against one compiles against the other. Underneath, each
//! operation tries a non-blocking syscall and, if it would block, waits for the
//! reactor to report readiness and tries again.
//!
//! Differences from the io_uring backend:
//! * `bind` and `connect` accept host names; they are resolved on the blocking
//!   pool (see [`lookup_host`]), never on the reactor thread.
//! * `SO_REUSEPORT` is set where it exists (Unix other than Solaris/illumos);
//!   `SO_REUSEADDR` only on Unix, because on Windows it permits port hijacking.
//! * Async files run on the blocking pool (see the `fs` module).

use std::{
    io,
    mem::MaybeUninit,
    net::{Shutdown, SocketAddr},
    time::Duration,
};

pub use crate::addr::{lookup_host, ToAddrs};
mod sock;
mod udp;
#[cfg(unix)]
#[path = "../unix.rs"]
pub mod unix;
#[cfg(unix)]
use sock::raw;
use sock::{is_in_progress, socket_addr, Io};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
pub use udp::UdpSocket;
#[cfg(unix)]
pub use unix::{UnixDatagram, UnixListener, UnixStream};

use crate::buf::{BufResult, IoBuf, IoBufMut};

// ── TcpListener ───────────────────────────────────────────────────────────────

/// A TCP listener.
pub struct TcpListener {
    io: Io,
}

impl TcpListener {
    /// Bind to `addr` and start listening. Hostnames are resolved with the
    /// blocking standard-library resolver.
    pub async fn bind(addr: impl crate::addr::ToAddrs) -> io::Result<Self> {
        let mut last = None;
        for sa in crate::addr::lookup_host(addr).await? {
            match Self::bind_addr(sa) {
                Ok(l) => return Ok(l),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            )
        }))
    }

    /// Bind directly to a resolved address.
    pub fn bind_addr(addr: SocketAddr) -> io::Result<Self> {
        let sock = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
        #[cfg(unix)]
        sock.set_reuse_address(true)?;
        #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
        sock.set_reuse_port(true)?;
        sock.bind(&SockAddr::from(addr))?;
        sock.listen(1024)?;
        Ok(Self { io: Io::new(sock)? })
    }

    /// Wrap a standard-library listener.
    pub fn from_std(listener: std::net::TcpListener) -> io::Result<Self> {
        Ok(Self {
            io: Io::new(Socket::from(listener))?,
        })
    }

    /// The address this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket_addr(self.io.sock.local_addr()?)
    }

    /// Accept the next connection.
    pub async fn accept(&self) -> io::Result<TcpStream> {
        Ok(self.accept_addr().await?.0)
    }

    /// Accept the next connection and report the peer address.
    pub async fn accept_addr(&self) -> io::Result<(TcpStream, SocketAddr)> {
        loop {
            match self.io.sock.accept() {
                Ok((sock, peer)) => {
                    return Ok((TcpStream { io: Io::new(sock)? }, socket_addr(peer)?));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => self.io.ready(false).await?,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// Set the IP time-to-live for packets sent from accepted sockets.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.sock.set_ttl(ttl)
    }

    /// The IP time-to-live option.
    pub fn ttl(&self) -> io::Result<u32> {
        self.io.sock.ttl()
    }

    /// Take the pending socket error, if any.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.io.sock.take_error()
    }

    /// The raw file descriptor.
    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        raw(&self.io.sock)
    }
}

// ── TcpStream ─────────────────────────────────────────────────────────────────

/// A TCP connection.
pub struct TcpStream {
    io: Io,
}

impl TcpStream {
    /// Connect to `addr`. Hostnames are resolved with the blocking
    /// standard-library resolver; the connect itself is asynchronous.
    pub async fn connect(addr: impl crate::addr::ToAddrs) -> io::Result<Self> {
        let mut last = None;
        for sa in crate::addr::lookup_host(addr).await? {
            match Self::connect_addr(sa).await {
                Ok(s) => return Ok(s),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            )
        }))
    }

    /// Connect to an already-resolved address.
    pub async fn connect_addr(addr: SocketAddr) -> io::Result<Self> {
        let sock = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
        let io = Io::new(sock)?;
        match io.sock.connect(&SockAddr::from(addr)) {
            Ok(()) => {}
            Err(e) if is_in_progress(&e) => loop {
                io.ready(true).await?;
                if let Some(e) = io.sock.take_error()? {
                    return Err(e);
                }
                match io.sock.peer_addr() {
                    Ok(_) => break,
                    Err(e) if e.kind() == io::ErrorKind::NotConnected => {}
                    Err(e) => return Err(e),
                }
            },
            Err(e) => return Err(e),
        }
        Ok(Self { io })
    }

    /// Wrap a standard-library stream.
    pub fn from_std(stream: std::net::TcpStream) -> io::Result<Self> {
        Ok(Self {
            io: Io::new(Socket::from(stream))?,
        })
    }

    /// The remote address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        socket_addr(self.io.sock.peer_addr()?)
    }

    /// The local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket_addr(self.io.sock.local_addr()?)
    }

    /// Read into `buf`. On success the buffer's initialized length is the
    /// number of bytes read (0 means the peer closed).
    pub async fn read<B>(&self, mut buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        loop {
            let total = buf.bytes_total();
            let ptr = buf.stable_ptr_mut() as *mut MaybeUninit<u8>;
            // SAFETY: `[ptr, ptr + total)` is this buffer's allocation, owned exclusively by us
            // here, and `MaybeUninit<u8>` makes no claim that the bytes are initialized.
            let spare = unsafe { std::slice::from_raw_parts_mut(ptr, total) };
            match self.io.sock.recv(spare) {
                Ok(n) => {
                    // SAFETY: recv wrote `n` bytes at the start of the buffer.
                    unsafe { buf.set_init(n) };
                    return (Ok(n), buf);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.io.ready(false).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return (Err(e), buf),
            }
        }
    }

    /// Read until `buf` is full, like [`std::io::Read::read_exact`]. Returns an
    /// `UnexpectedEof` error if the peer closes first.
    pub async fn read_exact<B>(&self, mut buf: B) -> BufResult<(), B>
    where
        B: IoBufMut + Send + 'static,
    {
        let total = buf.bytes_total();
        let mut got = 0usize;
        while got < total {
            let base = buf.stable_ptr_mut();
            // SAFETY: `got < total`, so this stays inside the buffer's allocation.
            let ptr = unsafe { base.add(got) } as *mut MaybeUninit<u8>;
            // SAFETY: as in `read`.
            let spare = unsafe { std::slice::from_raw_parts_mut(ptr, total - got) };
            match self.io.sock.recv(spare) {
                Ok(0) => {
                    let e =
                        io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer");
                    return (Err(e), buf);
                }
                Ok(n) => got += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
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
            match self.io.send_nosig(data) {
                Ok(n) => return (Ok(n), buf),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
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
            // SAFETY: `sent < total <= bytes_init()`, so the slice stays inside the initialized region.
            let data =
                unsafe { std::slice::from_raw_parts(buf.stable_ptr().add(sent), total - sent) };
            match self.io.send_nosig(data) {
                Ok(0) => {
                    let e =
                        io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer");
                    return (Err(e), buf);
                }
                Ok(n) => sent += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
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

    /// Set `TCP_NODELAY`.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.io.sock.set_nodelay(nodelay)
    }

    /// Whether `TCP_NODELAY` is set.
    pub fn nodelay(&self) -> io::Result<bool> {
        self.io.sock.nodelay()
    }

    /// Set `SO_KEEPALIVE`.
    pub fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        self.io.sock.set_keepalive(keepalive)
    }

    /// Whether `SO_KEEPALIVE` is set.
    pub fn keepalive(&self) -> io::Result<bool> {
        self.io.sock.keepalive()
    }

    /// Set `SO_LINGER`.
    pub fn set_linger(&self, dur: Option<Duration>) -> io::Result<()> {
        self.io.sock.set_linger(dur)
    }

    /// The `SO_LINGER` setting.
    pub fn linger(&self) -> io::Result<Option<Duration>> {
        self.io.sock.linger()
    }

    /// Set the kernel receive buffer size.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.io.sock.set_recv_buffer_size(size)
    }

    /// The kernel receive buffer size.
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        self.io.sock.recv_buffer_size()
    }

    /// Set the kernel send buffer size.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.io.sock.set_send_buffer_size(size)
    }

    /// The kernel send buffer size.
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        self.io.sock.send_buffer_size()
    }

    /// Set the IP time-to-live.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.sock.set_ttl(ttl)
    }

    /// The IP time-to-live.
    pub fn ttl(&self) -> io::Result<u32> {
        self.io.sock.ttl()
    }

    /// Take the pending socket error, if any.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.io.sock.take_error()
    }

    /// The raw file descriptor.
    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        raw(&self.io.sock)
    }
}
