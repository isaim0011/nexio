//! Networking on top of the nexio reactor.
//!
//! Provides TCP and UDP networking using owned-buffer ops through io_uring.

pub mod udp;
pub use udp::UdpSocket;

use std::{
    io,
    net::SocketAddr,
    os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
};

use crate::{
    buf::{BufResult, IoBuf, IoBufMut, IoBufMutExt},
    reactor::REACTOR,
};

// ── Helpers for sockaddr conversion ──────────────────────────────────────────

pub(crate) fn socket_addr_to_storage(
    addr: SocketAddr,
) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match addr {
        SocketAddr::V4(v4) => {
            let sin = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from(*v4.ip()).to_be(),
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
                sin6_flowinfo: v6.flowinfo().to_be(),
                sin6_addr: libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                },
                sin6_scope_id: v6.scope_id().to_be(),
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

pub(crate) fn sockaddr_storage_to_addr(storage: &libc::sockaddr_storage) -> io::Result<SocketAddr> {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            let sin = unsafe { *(storage as *const _ as *const libc::sockaddr_in) };
            let ip = std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(std::net::SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            let sin6 = unsafe { *(storage as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            let flowinfo = u32::from_be(sin6.sin6_flowinfo);
            let scope_id = u32::from_be(sin6.sin6_scope_id);
            Ok(SocketAddr::V6(std::net::SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported address family",
        )),
    }
}

pub(crate) struct CloseOnDrop(pub(crate) RawFd);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if self.0 >= 0 {
            unsafe { libc::close(self.0) };
        }
    }
}

// ── TcpListener ───────────────────────────────────────────────────────────────

/// A TCP listener that accepts connections via io_uring.
pub struct TcpListener {
    fd: RawFd,
}

impl TcpListener {
    /// Bind to `addr` with `SO_REUSEPORT` and `SO_REUSEADDR` enabled, and start listening.
    pub async fn bind(addr: impl std::net::ToSocketAddrs) -> io::Result<Self> {
        let mut last_err = None;
        let addrs = addr.to_socket_addrs()?;
        for sa in addrs {
            match Self::bind_addr(sa) {
                Ok(listener) => return Ok(listener),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            )
        }))
    }

    /// Bind directly to a resolved `SocketAddr` with `SO_REUSEPORT` enabled.
    pub fn bind_addr(addr: SocketAddr) -> io::Result<Self> {
        let domain = match addr {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };

        let fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let mut guard = CloseOnDrop(fd);
        let optval: libc::c_int = 1;

        // Enable SO_REUSEADDR
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        // Enable SO_REUSEPORT
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        // Bind address
        let (storage, socklen) = socket_addr_to_storage(addr);
        let ret = unsafe { libc::bind(fd, &storage as *const _ as *const libc::sockaddr, socklen) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        // Start listening
        let ret = unsafe { libc::listen(fd, 1024) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        guard.0 = -1; // disarm guard
        Ok(Self { fd })
    }

    /// Create a `TcpListener` from a standard library `TcpListener`.
    pub fn from_std(std_listener: std::net::TcpListener) -> io::Result<Self> {
        std_listener.set_nonblocking(true)?;
        use std::os::unix::io::IntoRawFd;
        Ok(Self {
            fd: std_listener.into_raw_fd(),
        })
    }

    /// Enable `SO_REUSEPORT` for multi-core sharding.
    ///
    /// Note: [`TcpListener::bind`] already enables `SO_REUSEPORT` automatically.
    pub fn set_reuseport(&self) -> io::Result<()> {
        let optval: libc::c_int = 1;
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Gets the value of the `SO_REUSEPORT` socket option.
    pub fn reuseport(&self) -> io::Result<bool> {
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            Ok(optval != 0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Sets the `SO_REUSEADDR` socket option.
    pub fn set_reuseaddr(&self, reuseaddr: bool) -> io::Result<()> {
        let optval: libc::c_int = if reuseaddr { 1 } else { 0 };
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Gets the value of the `SO_REUSEADDR` socket option.
    pub fn reuseaddr(&self) -> io::Result<bool> {
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            Ok(optval != 0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Sets the time-to-live (TTL) or hop limit for packets sent from this socket.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        let addr = self.local_addr()?;
        let (level, name) = match addr {
            SocketAddr::V4(_) => (libc::IPPROTO_IP, libc::IP_TTL),
            SocketAddr::V6(_) => (libc::IPPROTO_IPV6, libc::IPV6_UNICAST_HOPS),
        };
        let optval: libc::c_int = ttl as libc::c_int;
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                level,
                name,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Gets the time-to-live (TTL) or hop limit for packets sent from this socket.
    pub fn ttl(&self) -> io::Result<u32> {
        let addr = self.local_addr()?;
        let (level, name) = match addr {
            SocketAddr::V4(_) => (libc::IPPROTO_IP, libc::IP_TTL),
            SocketAddr::V6(_) => (libc::IPPROTO_IPV6, libc::IPV6_UNICAST_HOPS),
        };
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                level,
                name,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            Ok(optval as u32)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Attempts to accept a connection without blocking or scheduling an io_uring operation.
    ///
    /// Returns `Ok((stream, addr))` if a connection is immediately available, or
    /// `Err(ErrorKind::WouldBlock)` if no incoming connections are queued.
    pub fn try_accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let new_fd = unsafe {
            libc::accept4(
                self.fd,
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut len,
                libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            )
        };
        if new_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let addr = sockaddr_storage_to_addr(&storage)?;
        Ok((TcpStream { fd: new_fd }, addr))
    }

    /// Get and clear the value of the `SO_ERROR` option on this socket.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            if optval == 0 {
                Ok(None)
            } else {
                Ok(Some(io::Error::from_raw_os_error(optval)))
            }
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Returns the local socket address of this listener.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let ret = unsafe {
            libc::getsockname(
                self.fd,
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        sockaddr_storage_to_addr(&storage)
    }

    /// Accept a new connection via io_uring.
    pub async fn accept(&self) -> io::Result<TcpStream> {
        let op = REACTOR.with(|r| r.borrow_mut().accept(self.fd))?;
        let raw_fd = op.await?;
        Ok(TcpStream { fd: raw_fd })
    }

    /// Accept a new connection via io_uring, returning both stream and peer address.
    pub async fn accept_addr(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let stream = self.accept().await?;
        let addr = stream.peer_addr()?;
        Ok((stream, addr))
    }

    /// Returns the raw file descriptor.
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
    }

    /// Consumes this listener, returning the raw file descriptor.
    pub fn into_raw_fd(self) -> RawFd {
        let fd = self.fd;
        std::mem::forget(self);
        fd
    }

    /// Construct a `TcpListener` from an existing raw file descriptor.
    ///
    /// # Safety
    /// The caller must ensure `fd` is a valid open listening socket.
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self { fd }
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl IntoRawFd for TcpListener {
    fn into_raw_fd(self) -> RawFd {
        self.into_raw_fd()
    }
}

impl FromRawFd for TcpListener {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { Self::from_raw_fd(fd) }
    }
}

// ── TcpStream ─────────────────────────────────────────────────────────────────

/// A connected TCP stream that reads and writes via io_uring.
pub struct TcpStream {
    fd: RawFd,
}

impl TcpStream {
    /// Connect to `addr` via io_uring.
    pub async fn connect(addr: impl std::net::ToSocketAddrs) -> io::Result<Self> {
        let mut last_err = None;
        let addrs = addr.to_socket_addrs()?;
        for sa in addrs {
            match Self::connect_addr(sa).await {
                Ok(stream) => return Ok(stream),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            )
        }))
    }

    /// Connect directly to a resolved `SocketAddr` via io_uring.
    pub async fn connect_addr(addr: SocketAddr) -> io::Result<Self> {
        let domain = match addr {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };

        let fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let mut guard = CloseOnDrop(fd);
        let op = REACTOR.with(|r| r.borrow_mut().connect(fd, addr))?;
        op.await?;

        guard.0 = -1; // disarm guard
        Ok(Self { fd })
    }

    /// Create a `TcpStream` from a standard library `TcpStream`.
    pub fn from_std(std_stream: std::net::TcpStream) -> io::Result<Self> {
        std_stream.set_nonblocking(true)?;
        use std::os::unix::io::IntoRawFd;
        Ok(Self {
            fd: std_stream.into_raw_fd(),
        })
    }

    /// Returns the remote peer address of this stream.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let ret = unsafe {
            libc::getpeername(
                self.fd,
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        sockaddr_storage_to_addr(&storage)
    }

    /// Returns the local socket address of this stream.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let ret = unsafe {
            libc::getsockname(
                self.fd,
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        sockaddr_storage_to_addr(&storage)
    }

    /// Read into `buf`. Returns `BufResult<usize, B>`.
    ///
    /// On success, `buf` is marked with the initialized length matching the bytes read.
    pub async fn read<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        let fd = self.fd;
        let (res, mut buf) = crate::reactor::LazyOp::new(buf, move |r, b, w| unsafe {
            r.submit_recv_with_waker(fd, b, Some(w))
        })
        .await;
        if let Ok(n) = res {
            // SAFETY: kernel reported writing n bytes into the buffer.
            unsafe { buf.set_init(n) };
        }
        (res, buf)
    }

    /// Read until `buf` is full (`bytes_total()` bytes), like [`std::io::Read::read_exact`].
    ///
    /// Returns `Ok(())` once the buffer is full, or an `UnexpectedEof` error if the peer closed
    /// first. The buffer is handed back either way.
    pub async fn read_exact<B>(&self, mut buf: B) -> BufResult<(), B>
    where
        B: IoBufMut + Send + 'static,
    {
        let total = buf.bytes_total();
        if total == 0 {
            return (Ok(()), buf);
        }

        // Fast path: avoid Slice wrapper on the common single-read completion.
        let (res, buf_out) = self.read(buf).await;
        buf = buf_out;
        let mut read_bytes = match res {
            Ok(0) => {
                return (
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    )),
                    buf,
                );
            }
            Ok(n) if n >= total => return (Ok(()), buf),
            Ok(n) => n,
            Err(e) => return (Err(e), buf),
        };

        while read_bytes < total {
            let slice = buf.slice(read_bytes..total);
            let (res, slice) = self.read(slice).await;
            buf = slice.into_inner();

            match res {
                Ok(0) => {
                    return (
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "failed to fill whole buffer",
                        )),
                        buf,
                    );
                }
                Ok(n) => {
                    read_bytes += n;
                }
                Err(e) => {
                    return (Err(e), buf);
                }
            }
        }

        // SAFETY: total bytes have been successfully read into the buffer.
        unsafe { buf.set_init(read_bytes) };
        (Ok(()), buf)
    }

    /// Write from `buf`. Returns `BufResult<usize, B>`.
    pub async fn write<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBuf + IoBufMut + Send + 'static,
    {
        let fd = self.fd;
        crate::reactor::LazyOp::new(buf, move |r, b, w| unsafe {
            r.submit_send_with_waker(fd, b, Some(w))
        })
        .await
    }

    /// Write all bytes from `buf` to the stream.
    ///
    /// Loops until the entire initialized length (`bytes_init()`) is written or an error occurs.
    pub async fn write_all<B>(&self, mut buf: B) -> BufResult<(), B>
    where
        B: IoBuf + IoBufMut + Send + 'static,
    {
        let total = buf.bytes_init();
        if total == 0 {
            return (Ok(()), buf);
        }

        // Fast path: avoid Slice wrapper when entire buffer writes in one call.
        let (res, buf_out) = self.write(buf).await;
        buf = buf_out;
        let mut written = match res {
            Ok(0) => {
                return (
                    Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    )),
                    buf,
                );
            }
            Ok(n) if n >= total => return (Ok(()), buf),
            Ok(n) => n,
            Err(e) => return (Err(e), buf),
        };

        while written < total {
            let slice = buf.slice(written..total);
            let (res, slice) = self.write(slice).await;
            buf = slice.into_inner();

            match res {
                Ok(0) => {
                    return (
                        Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write whole buffer",
                        )),
                        buf,
                    );
                }
                Ok(n) => {
                    written += n;
                }
                Err(e) => {
                    return (Err(e), buf);
                }
            }
        }

        (Ok(()), buf)
    }

    /// Sets the value of the `TCP_NODELAY` option on this socket.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        let optval: libc::c_int = if nodelay { 1 } else { 0 };
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Gets the value of the `TCP_NODELAY` option on this socket.
    pub fn nodelay(&self) -> io::Result<bool> {
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            Ok(optval != 0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Shuts down the read, write, or both halves of this connection.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        let how = match how {
            std::net::Shutdown::Read => libc::SHUT_RD,
            std::net::Shutdown::Write => libc::SHUT_WR,
            std::net::Shutdown::Both => libc::SHUT_RDWR,
        };
        let ret = unsafe { libc::shutdown(self.fd, how) };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Sets the `SO_KEEPALIVE` option on this socket.
    pub fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        let optval: libc::c_int = if keepalive { 1 } else { 0 };
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Gets the value of the `SO_KEEPALIVE` option on this socket.
    pub fn keepalive(&self) -> io::Result<bool> {
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            Ok(optval != 0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Sets the `SO_LINGER` option on this socket.
    pub fn set_linger(&self, dur: Option<std::time::Duration>) -> io::Result<()> {
        let linger = match dur {
            Some(d) => libc::linger {
                l_onoff: 1,
                l_linger: d.as_secs().min(libc::c_int::MAX as u64) as libc::c_int,
            },
            None => libc::linger {
                l_onoff: 0,
                l_linger: 0,
            },
        };
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                &linger as *const _ as *const libc::c_void,
                std::mem::size_of_val(&linger) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Gets the value of the `SO_LINGER` option on this socket.
    pub fn linger(&self) -> io::Result<Option<std::time::Duration>> {
        let mut linger: libc::linger = unsafe { std::mem::zeroed() };
        let mut optlen = std::mem::size_of_val(&linger) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                &mut linger as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            if linger.l_onoff != 0 {
                Ok(Some(std::time::Duration::from_secs(linger.l_linger as u64)))
            } else {
                Ok(None)
            }
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Sets the receive buffer size (`SO_RCVBUF`).
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        let optval: libc::c_int = size.min(libc::c_int::MAX as usize) as libc::c_int;
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Gets the receive buffer size (`SO_RCVBUF`).
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            Ok(optval as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Sets the send buffer size (`SO_SNDBUF`).
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        let optval: libc::c_int = size.min(libc::c_int::MAX as usize) as libc::c_int;
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Gets the send buffer size (`SO_SNDBUF`).
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            Ok(optval as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Sets the time-to-live (TTL) or hop limit for packets sent from this socket.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        let addr = self.local_addr()?;
        let (level, name) = match addr {
            SocketAddr::V4(_) => (libc::IPPROTO_IP, libc::IP_TTL),
            SocketAddr::V6(_) => (libc::IPPROTO_IPV6, libc::IPV6_UNICAST_HOPS),
        };
        let optval: libc::c_int = ttl as libc::c_int;
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                level,
                name,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Gets the time-to-live (TTL) or hop limit for packets sent from this socket.
    pub fn ttl(&self) -> io::Result<u32> {
        let addr = self.local_addr()?;
        let (level, name) = match addr {
            SocketAddr::V4(_) => (libc::IPPROTO_IP, libc::IP_TTL),
            SocketAddr::V6(_) => (libc::IPPROTO_IPV6, libc::IPV6_UNICAST_HOPS),
        };
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                level,
                name,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            Ok(optval as u32)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Get and clear the value of the `SO_ERROR` option on this socket.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                &mut optval as *mut _ as *mut libc::c_void,
                &mut optlen,
            )
        };
        if ret == 0 {
            if optval == 0 {
                Ok(None)
            } else {
                Ok(Some(io::Error::from_raw_os_error(optval)))
            }
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Returns the raw file descriptor.
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
    }

    /// Consumes this stream, returning the raw file descriptor.
    pub fn into_raw_fd(self) -> RawFd {
        let fd = self.fd;
        std::mem::forget(self);
        fd
    }

    /// Construct a `TcpStream` from an existing raw file descriptor.
    ///
    /// # Safety
    /// The caller must ensure `fd` is a valid open connected socket.
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self { fd }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl IntoRawFd for TcpStream {
    fn into_raw_fd(self) -> RawFd {
        self.into_raw_fd()
    }
}

impl FromRawFd for TcpStream {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { Self::from_raw_fd(fd) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::block_on;
    use std::thread;

    #[test]
    fn test_listener_bind_reuseport() {
        block_on(async {
            let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener1.local_addr().unwrap().port();

            // Binding a second listener on the same port should succeed thanks to SO_REUSEPORT
            let listener2 = TcpListener::bind(format!("127.0.0.1:{port}"))
                .await
                .unwrap();
            assert_eq!(listener2.local_addr().unwrap().port(), port);
        });
    }

    #[test]
    fn test_loopback_echo_nexio() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        // Server thread with its own reactor
        let server = thread::spawn(move || {
            block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = listener.local_addr().unwrap().port();
                ready_tx.send(port).unwrap();

                let (stream, _peer) = listener.accept_addr().await.unwrap();
                let buf = vec![0u8; 1024];
                let (res, buf) = stream.read(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"hello nexio echo");

                let slice = buf.slice(0..n);
                let (res, _) = stream.write_all(slice).await;
                res.unwrap();
            });
        });

        let port = ready_rx.recv().unwrap();

        // Client thread with its own reactor
        let client = thread::spawn(move || {
            block_on(async move {
                let stream = TcpStream::connect(format!("127.0.0.1:{port}"))
                    .await
                    .unwrap();
                let msg = b"hello nexio echo".to_vec();
                let (res, _) = stream.write_all(msg).await;
                res.unwrap();

                let buf = vec![0u8; 16];
                let (res, buf) = stream.read_exact(buf).await;
                res.unwrap();
                assert_eq!(&buf[..], b"hello nexio echo");
            });
        });

        server.join().unwrap();
        client.join().unwrap();
    }

    #[test]
    fn test_multi_round_large_payload_echo() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        const PAYLOAD_SIZE: usize = 64 * 1024; // 64 KiB
        const ROUNDS: usize = 5;

        let server = thread::spawn(move || {
            block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = listener.local_addr().unwrap().port();
                ready_tx.send(port).unwrap();

                let stream = listener.accept().await.unwrap();
                for _ in 0..ROUNDS {
                    let buf = vec![0u8; PAYLOAD_SIZE];
                    let (res, buf) = stream.read_exact(buf).await;
                    res.unwrap();
                    let (res, _) = stream.write_all(buf).await;
                    res.unwrap();
                }
            });
        });

        let port = ready_rx.recv().unwrap();

        let client = thread::spawn(move || {
            block_on(async move {
                let stream = TcpStream::connect(format!("127.0.0.1:{port}"))
                    .await
                    .unwrap();

                for r in 0..ROUNDS {
                    let mut send_data = vec![0u8; PAYLOAD_SIZE];
                    for (i, byte) in send_data.iter_mut().enumerate() {
                        *byte = ((i + r) % 256) as u8;
                    }

                    let (res, _) = stream.write_all(send_data.clone()).await;
                    res.unwrap();

                    let recv_buf = vec![0u8; PAYLOAD_SIZE];
                    let (res, recv_buf) = stream.read_exact(recv_buf).await;
                    res.unwrap();

                    assert_eq!(recv_buf, send_data);
                }
            });
        });

        server.join().unwrap();
        client.join().unwrap();
    }

    #[test]
    fn test_addrs_match() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        let server = thread::spawn(move || {
            block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let server_addr = listener.local_addr().unwrap();
                ready_tx.send(server_addr).unwrap();

                let (accepted, peer_addr) = listener.accept_addr().await.unwrap();
                assert_eq!(accepted.peer_addr().unwrap(), peer_addr);
                assert_eq!(accepted.local_addr().unwrap(), server_addr);

                let (res, _) = accepted.write_all(b"ok".to_vec()).await;
                res.unwrap();
            });
        });

        let server_addr = ready_rx.recv().unwrap();

        let client = thread::spawn(move || {
            block_on(async move {
                let stream = TcpStream::connect(server_addr).await.unwrap();
                assert_eq!(stream.peer_addr().unwrap(), server_addr);
                let client_local = stream.local_addr().unwrap();
                assert_ne!(client_local.port(), 0);

                let buf = vec![0u8; 2];
                let (res, buf) = stream.read_exact(buf).await;
                res.unwrap();
                assert_eq!(&buf[..], b"ok");
            });
        });

        server.join().unwrap();
        client.join().unwrap();
    }

    #[test]
    fn test_from_std() {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = std_listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            block_on(async move {
                let listener = TcpListener::from_std(std_listener).unwrap();
                let stream = listener.accept().await.unwrap();
                let (res, _) = stream.write_all(b"std_test".to_vec()).await;
                res.unwrap();
            });
        });

        let client = thread::spawn(move || {
            block_on(async move {
                let std_stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
                let stream = TcpStream::from_std(std_stream).unwrap();
                let buf = vec![0u8; 8];
                let (res, buf) = stream.read_exact(buf).await;
                res.unwrap();
                assert_eq!(&buf[..], b"std_test");
            });
        });

        server.join().unwrap();
        client.join().unwrap();
    }

    #[test]
    fn test_tcp_nodelay() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        let server = thread::spawn(move || {
            block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                ready_tx.send(listener.local_addr().unwrap()).unwrap();
                let (stream, _) = listener.accept_addr().await.unwrap();
                let (res, _) = stream.read(vec![0u8; 16]).await;
                res.unwrap();
            });
        });

        let addr = ready_rx.recv().unwrap();

        let client = thread::spawn(move || {
            block_on(async move {
                let stream = TcpStream::connect(addr).await.unwrap();
                stream.set_nodelay(true).unwrap();
                assert!(stream.nodelay().unwrap());
                stream.set_nodelay(false).unwrap();
                assert!(!stream.nodelay().unwrap());
                stream.set_nodelay(true).unwrap();
                assert!(stream.nodelay().unwrap());
                let (res, _) = stream.write(b"ping".to_vec()).await;
                res.unwrap();
            });
        });

        server.join().unwrap();
        client.join().unwrap();
    }

    #[test]
    fn test_tcp_shutdown() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        let server = thread::spawn(move || {
            block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                ready_tx.send(listener.local_addr().unwrap()).unwrap();
                let (stream, _) = listener.accept_addr().await.unwrap();

                // Read client's ping message
                let buf = vec![0u8; 16];
                let (res, buf) = stream.read(buf).await;
                let n = res.unwrap();
                assert_eq!(&buf[..n], b"ping");

                // Client called shutdown(Write), so next read must return 0 (EOF)
                let buf2 = vec![0u8; 16];
                let (res2, _) = stream.read(buf2).await;
                assert_eq!(res2.unwrap(), 0);

                // Server can still send data back (half-closed socket)
                let (res3, _) = stream.write_all(b"pong".to_vec()).await;
                res3.unwrap();
            });
        });

        let addr = ready_rx.recv().unwrap();

        let client = thread::spawn(move || {
            block_on(async move {
                let stream = TcpStream::connect(addr).await.unwrap();
                let (res, _) = stream.write_all(b"ping".to_vec()).await;
                res.unwrap();

                // Shut down writing half
                stream.shutdown(std::net::Shutdown::Write).unwrap();

                // Client can still read the server's reply
                let buf = vec![0u8; 4];
                let (res, buf) = stream.read_exact(buf).await;
                res.unwrap();
                assert_eq!(&buf[..], b"pong");
            });
        });

        server.join().unwrap();
        client.join().unwrap();
    }

    #[test]
    fn test_tcp_options_and_try_accept() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        let server = thread::spawn(move || {
            block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                listener.set_reuseaddr(true).unwrap();
                assert!(listener.reuseaddr().unwrap());
                assert!(listener.reuseport().unwrap());

                listener.set_ttl(64).unwrap();
                assert_eq!(listener.ttl().unwrap(), 64);
                assert!(listener.take_error().unwrap().is_none());

                // Before any client connects, try_accept returns WouldBlock
                let try_res = listener.try_accept();
                assert!(matches!(try_res, Err(ref e) if e.kind() == io::ErrorKind::WouldBlock));

                ready_tx.send(listener.local_addr().unwrap()).unwrap();

                // Wait for client to connect and retry try_accept
                let mut accepted = None;
                for _ in 0..100 {
                    match listener.try_accept() {
                        Ok(pair) => {
                            accepted = Some(pair);
                            break;
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                        Err(e) => panic!("unexpected error from try_accept: {:?}", e),
                    }
                }
                let (stream, peer) = accepted.expect("try_accept timed out waiting for connection");
                assert_eq!(stream.peer_addr().unwrap(), peer);

                // Test options on stream
                stream.set_keepalive(true).unwrap();
                assert!(stream.keepalive().unwrap());
                stream.set_keepalive(false).unwrap();
                assert!(!stream.keepalive().unwrap());

                stream
                    .set_linger(Some(std::time::Duration::from_secs(3)))
                    .unwrap();
                assert_eq!(
                    stream.linger().unwrap(),
                    Some(std::time::Duration::from_secs(3))
                );
                stream.set_linger(None).unwrap();
                assert_eq!(stream.linger().unwrap(), None);

                stream.set_recv_buffer_size(32768).unwrap();
                assert!(stream.recv_buffer_size().unwrap() >= 32768);

                stream.set_send_buffer_size(32768).unwrap();
                assert!(stream.send_buffer_size().unwrap() >= 32768);

                stream.set_ttl(48).unwrap();
                assert_eq!(stream.ttl().unwrap(), 48);

                assert!(stream.take_error().unwrap().is_none());
            });
        });

        let addr = ready_rx.recv().unwrap();

        let client = thread::spawn(move || {
            block_on(async move {
                let _stream = TcpStream::connect(addr).await.unwrap();
                std::thread::sleep(std::time::Duration::from_millis(100));
            });
        });

        server.join().unwrap();
        client.join().unwrap();
    }

    #[test]
    fn test_udp_connected_send_recv() {
        block_on(async {
            let socket_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr_a = socket_a.local_addr().unwrap();

            let socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr_b = socket_b.local_addr().unwrap();

            socket_a.connect(addr_b).await.unwrap();
            socket_b.connect(addr_a).await.unwrap();

            assert_eq!(socket_a.peer_addr().unwrap(), addr_b);
            assert_eq!(socket_b.peer_addr().unwrap(), addr_a);

            let msg = b"udp connected packet via io_uring".to_vec();
            let (send_res, _) = socket_a.send(msg).await;
            assert_eq!(send_res.unwrap(), 33);

            let buf = vec![0u8; 64];
            let (recv_res, buf) = socket_b.recv(buf).await;
            let n = recv_res.unwrap();
            assert_eq!(n, 33);
            assert_eq!(&buf[..n], b"udp connected packet via io_uring");
        });
    }

    #[test]
    fn test_udp_send_to_recv_from() {
        block_on(async {
            let socket_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr_a = socket_a.local_addr().unwrap();

            let socket_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr_b = socket_b.local_addr().unwrap();

            let msg = b"unconnected udp datagram payload".to_vec();
            let (send_res, _) = socket_a.send_to(msg, addr_b).await;
            assert_eq!(send_res.unwrap(), 32);

            std::thread::sleep(std::time::Duration::from_millis(10));

            let buf = vec![0u8; 64];
            let (recv_res, buf) = socket_b.recv_from(buf).await;
            let (n, from_addr) = recv_res.unwrap();
            assert_eq!(n, 32);
            assert_eq!(&buf[..n], b"unconnected udp datagram payload");
            assert_eq!(from_addr, addr_a);
        });
    }

    #[test]
    fn test_udp_options_and_from_std() {
        let std_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = std_socket.local_addr().unwrap().port();
        let nexio_socket = UdpSocket::from_std(std_socket).unwrap();
        assert_eq!(nexio_socket.local_addr().unwrap().port(), port);

        nexio_socket.set_broadcast(true).unwrap();
        assert!(nexio_socket.broadcast().unwrap());
        nexio_socket.set_broadcast(false).unwrap();
        assert!(!nexio_socket.broadcast().unwrap());

        nexio_socket.set_reuseaddr(true).unwrap();
        assert!(nexio_socket.reuseaddr().unwrap());

        nexio_socket.set_reuseport(true).unwrap();
        assert!(nexio_socket.reuseport().unwrap());

        nexio_socket.set_ttl(55).unwrap();
        assert_eq!(nexio_socket.ttl().unwrap(), 55);

        nexio_socket.set_recv_buffer_size(32768).unwrap();
        assert!(nexio_socket.recv_buffer_size().unwrap() >= 32768);

        nexio_socket.set_send_buffer_size(32768).unwrap();
        assert!(nexio_socket.send_buffer_size().unwrap() >= 32768);

        assert!(nexio_socket.take_error().unwrap().is_none());

        // Test connect & disconnect
        nexio_socket
            .connect_addr("127.0.0.1:9999".parse().unwrap())
            .unwrap();
        assert!(nexio_socket.peer_addr().is_ok());
        nexio_socket.disconnect().unwrap();
        assert!(nexio_socket.peer_addr().is_err());
    }
}
