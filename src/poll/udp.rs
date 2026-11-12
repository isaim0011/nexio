//! UDP on the portable readiness backend. The API mirrors the io_uring
//! backend's `UdpSocket`.

use std::{io, mem::MaybeUninit, net::SocketAddr};

use socket2::{Domain, Protocol, SockAddr, Socket, Type};

#[cfg(unix)]
use super::sock::raw;
use super::sock::{socket_addr, Io};
use crate::buf::{BufResult, IoBuf, IoBufMut};

/// A UDP socket.
pub struct UdpSocket {
    io: Io,
}

impl UdpSocket {
    /// Bind to `addr`. Host names are resolved on the blocking pool.
    pub async fn bind(addr: impl crate::addr::ToAddrs) -> io::Result<Self> {
        let mut last = None;
        for sa in crate::addr::lookup_host(addr).await? {
            match Self::bind_addr(sa) {
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

    /// Bind to an already-resolved address.
    pub fn bind_addr(addr: SocketAddr) -> io::Result<Self> {
        let sock = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
        #[cfg(unix)]
        sock.set_reuse_address(true)?;
        #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
        sock.set_reuse_port(true)?;
        sock.bind(&SockAddr::from(addr))?;
        Ok(Self { io: Io::new(sock)? })
    }

    /// Wrap a standard-library socket.
    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<Self> {
        Ok(Self {
            io: Io::new(Socket::from(socket))?,
        })
    }

    /// Set the default peer for [`send`](Self::send) and [`recv`](Self::recv).
    pub async fn connect(&self, addr: impl crate::addr::ToAddrs) -> io::Result<()> {
        let mut last = None;
        for sa in crate::addr::lookup_host(addr).await? {
            match self.connect_addr(sa) {
                Ok(()) => return Ok(()),
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

    /// Set the default peer to an already-resolved address.
    pub fn connect_addr(&self, addr: SocketAddr) -> io::Result<()> {
        self.io.sock.connect(&SockAddr::from(addr))
    }

    /// The local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket_addr(self.io.sock.local_addr()?)
    }

    /// The connected peer's address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        socket_addr(self.io.sock.peer_addr()?)
    }

    /// Send `buf` to the connected peer.
    pub async fn send<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        loop {
            match self.try_send(&buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.io.ready(true).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                other => return (other, buf),
            }
        }
    }

    /// Receive from the connected peer into `buf`.
    pub async fn recv<B>(&self, mut buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        loop {
            let (res, b) = self.try_recv(buf);
            buf = b;
            match res {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.io.ready(false).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                other => return (other, buf),
            }
        }
    }

    /// Send `buf` to `target`.
    pub async fn send_to<B>(&self, buf: B, target: SocketAddr) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        loop {
            match self.try_send_to(&buf, target) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.io.ready(true).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                other => return (other, buf),
            }
        }
    }

    /// Receive one datagram into `buf` and report who sent it.
    pub async fn recv_from<B>(&self, mut buf: B) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: IoBufMut + Send + 'static,
    {
        loop {
            let (res, b) = self.try_recv_from(buf);
            buf = b;
            match res {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.io.ready(false).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                other => return (other, buf),
            }
        }
    }

    /// Try to send once to `target`; fails with `WouldBlock` rather than waiting.
    pub fn try_send_to<B: IoBuf>(&self, buf: &B, target: SocketAddr) -> io::Result<usize> {
        // SAFETY: the buffer guarantees `bytes_init()` initialized bytes at `stable_ptr()`.
        let data = unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) };
        self.io.sock.send_to(data, &SockAddr::from(target))
    }

    /// Try to send once to the connected peer; fails with `WouldBlock` rather than waiting.
    pub fn try_send<B: IoBuf>(&self, buf: &B) -> io::Result<usize> {
        // SAFETY: as in `try_send_to`.
        let data = unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) };
        self.io.sock.send(data)
    }

    /// Try to receive once; fails with `WouldBlock` rather than waiting.
    pub fn try_recv_from<B: IoBufMut>(&self, mut buf: B) -> (io::Result<(usize, SocketAddr)>, B) {
        let total = buf.bytes_total();
        let ptr = buf.stable_ptr_mut() as *mut MaybeUninit<u8>;
        // SAFETY: `[ptr, ptr + total)` is this buffer's allocation, exclusively ours here.
        let spare = unsafe { std::slice::from_raw_parts_mut(ptr, total) };
        match self.io.sock.recv_from(spare) {
            Ok((n, from)) => {
                // SAFETY: recv_from wrote `n` bytes at the start of the buffer.
                unsafe { buf.set_init(n) };
                (socket_addr(from).map(|a| (n, a)), buf)
            }
            Err(e) => (Err(e), buf),
        }
    }

    /// Try to receive once from the connected peer; fails with `WouldBlock` rather than waiting.
    pub fn try_recv<B: IoBufMut>(&self, mut buf: B) -> BufResult<usize, B> {
        let total = buf.bytes_total();
        let ptr = buf.stable_ptr_mut() as *mut MaybeUninit<u8>;
        // SAFETY: as in `try_recv_from`.
        let spare = unsafe { std::slice::from_raw_parts_mut(ptr, total) };
        match self.io.sock.recv(spare) {
            Ok(n) => {
                // SAFETY: recv wrote `n` bytes at the start of the buffer.
                unsafe { buf.set_init(n) };
                (Ok(n), buf)
            }
            Err(e) => (Err(e), buf),
        }
    }

    /// Set `SO_BROADCAST`.
    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        self.io.sock.set_broadcast(on)
    }

    /// Whether `SO_BROADCAST` is set.
    pub fn broadcast(&self) -> io::Result<bool> {
        self.io.sock.broadcast()
    }

    /// Set `SO_REUSEADDR` (Unix only).
    #[cfg(unix)]
    pub fn set_reuseaddr(&self, on: bool) -> io::Result<()> {
        self.io.sock.set_reuse_address(on)
    }

    /// Whether `SO_REUSEADDR` is set (Unix only).
    #[cfg(unix)]
    pub fn reuseaddr(&self) -> io::Result<bool> {
        self.io.sock.reuse_address()
    }

    /// Set `SO_REUSEPORT` (where it exists).
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    pub fn set_reuseport(&self, on: bool) -> io::Result<()> {
        self.io.sock.set_reuse_port(on)
    }

    /// Whether `SO_REUSEPORT` is set (where it exists).
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    pub fn reuseport(&self) -> io::Result<bool> {
        self.io.sock.reuse_port()
    }

    /// Set the IP time-to-live.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.io.sock.set_ttl(ttl)
    }

    /// The IP time-to-live.
    pub fn ttl(&self) -> io::Result<u32> {
        self.io.sock.ttl()
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
