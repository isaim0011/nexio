//! UDP networking on top of the nexio reactor.
//!
//! Provides datagram socket operations using owned buffers and io_uring.

use std::{
    io,
    net::SocketAddr,
    os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
};

use crate::{
    buf::{BufResult, IoBuf, IoBufMut},
    net::{sockaddr_storage_to_addr, socket_addr_to_storage, CloseOnDrop},
    reactor::REACTOR,
};

/// An asynchronous UDP datagram socket.
///
/// Datagram sockets support sending and receiving discrete packets. When connected
/// to a peer via [`connect`](Self::connect), datagrams can be sent and received
/// using the high-performance io_uring reactor via [`send`](Self::send) and
/// [`recv`](Self::recv).
pub struct UdpSocket {
    fd: RawFd,
}

impl UdpSocket {
    /// Bind to `addr` with `SO_REUSEPORT` and `SO_REUSEADDR` enabled.
    pub async fn bind(addr: impl crate::addr::ToAddrs) -> io::Result<Self> {
        let mut last_err = None;
        let addrs = crate::addr::lookup_host(addr).await?;
        for sa in addrs {
            match Self::bind_addr(sa) {
                Ok(socket) => return Ok(socket),
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

    /// Bind directly to a resolved `SocketAddr` with `SO_REUSEPORT` and `SO_REUSEADDR` enabled.
    pub fn bind_addr(addr: SocketAddr) -> io::Result<Self> {
        let domain = match addr {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };

        let fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_DGRAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
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

        guard.0 = -1; // disarm guard
        Ok(Self { fd })
    }

    /// Create a `UdpSocket` from a standard library `std::net::UdpSocket`.
    pub fn from_std(std_socket: std::net::UdpSocket) -> io::Result<Self> {
        std_socket.set_nonblocking(true)?;
        Ok(Self {
            fd: std_socket.into_raw_fd(),
        })
    }

    /// Connect this socket to a remote address.
    ///
    /// This sets the default peer address for subsequent [`send`](Self::send) and
    /// [`recv`](Self::recv) calls.
    pub async fn connect(&self, addr: impl crate::addr::ToAddrs) -> io::Result<()> {
        let mut last_err = None;
        let addrs = crate::addr::lookup_host(addr).await?;
        for sa in addrs {
            match self.connect_addr(sa) {
                Ok(()) => return Ok(()),
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

    /// Connect directly to a resolved `SocketAddr`.
    pub fn connect_addr(&self, addr: SocketAddr) -> io::Result<()> {
        let (storage, len) = socket_addr_to_storage(addr);
        let ret =
            unsafe { libc::connect(self.fd, &storage as *const _ as *const libc::sockaddr, len) };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Disconnect this socket from its remote peer.
    pub fn disconnect(&self) -> io::Result<()> {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        storage.ss_family = libc::AF_UNSPEC as libc::sa_family_t;
        let ret = unsafe {
            libc::connect(
                self.fd,
                &storage as *const _ as *const libc::sockaddr,
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Returns the local socket address of this socket.
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

    /// Returns the remote peer address if this socket is connected.
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

    /// Send a datagram from `buf` to the connected peer via io_uring.
    pub async fn send<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBuf + Send + Unpin + 'static,
    {
        let fd = self.fd;
        let mut buf = buf;
        loop {
            let (res, b) = crate::reactor::LazyOp::new(buf, move |r, b, w| unsafe {
                r.submit_send_with_waker(fd, b, Some(w))
            })
            .await;
            buf = b;
            match res {
                // A non-blocking socket reports a full send buffer as `WouldBlock`;
                // an async send waits for room instead of surfacing that.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.wait(true).await {
                        return (Err(e), buf);
                    }
                }
                other => return (other, buf),
            }
        }
    }

    /// Wait until the socket is ready for reading (or writing), using a one-shot
    /// `POLL_ADD`. Dropping the future cancels it; it holds no buffer.
    async fn wait(&self, write: bool) -> io::Result<()> {
        let op = REACTOR.with(|r| {
            let mut r = r.borrow_mut();
            if write {
                r.poll_writable(self.fd)
            } else {
                r.poll_readable(self.fd)
            }
        })?;
        op.await.map(|_| ())
    }

    /// Receive a datagram into `buf` via io_uring.
    ///
    /// On success, `buf` is marked with the initialized length matching the bytes received.
    pub async fn recv<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + Unpin + 'static,
    {
        let fd = self.fd;
        let mut buf = buf;
        loop {
            let (res, mut b) = crate::reactor::LazyOp::new(buf, move |r, b, w| unsafe {
                r.submit_recv_with_waker(fd, b, Some(w))
            })
            .await;
            match res {
                Ok(n) => {
                    // SAFETY: kernel reported writing n bytes into the buffer.
                    unsafe { b.set_init(n) };
                    return (Ok(n), b);
                }
                // The socket is non-blocking, so an empty queue comes back as
                // `WouldBlock`. An async receive waits for data instead.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    buf = b;
                    if let Err(e) = self.wait(false).await {
                        return (Err(e), buf);
                    }
                }
                Err(e) => return (Err(e), b),
            }
        }
    }

    /// Send a datagram from `buf` to `target` asynchronously via io_uring.
    pub async fn send_to<B>(&self, buf: B, target: SocketAddr) -> BufResult<usize, B>
    where
        B: IoBuf + Send + Unpin + 'static,
    {
        let mut buf = buf;
        loop {
            let key = match REACTOR.with(|r| unsafe {
                r.borrow_mut()
                    .submit_sendmsg(self.fd, &buf, Some(target), 0)
            }) {
                Ok(k) => k,
                Err(e) => return (Err(e), buf),
            };
            let (res, b) = unsafe { crate::reactor::Op::new(key, buf) }.await;
            buf = b;
            match res {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.wait(true).await {
                        return (Err(e), buf);
                    }
                }
                other => return (other, buf),
            }
        }
    }

    /// Attempt to send a datagram to `target` synchronously without blocking.
    pub fn try_send_to<B>(&self, buf: &B, target: SocketAddr) -> io::Result<usize>
    where
        B: IoBuf,
    {
        let (storage, len) = socket_addr_to_storage(target);
        let ret = unsafe {
            libc::sendto(
                self.fd,
                buf.stable_ptr() as *const libc::c_void,
                buf.bytes_init(),
                0,
                &storage as *const _ as *const libc::sockaddr,
                len,
            )
        };
        if ret >= 0 {
            Ok(ret as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Receive a datagram into `buf`, returning the number of bytes and source address.
    ///
    /// If the socket is connected to a peer, this reads asynchronously via io_uring
    /// and associates the peer address. Otherwise it receives with `recvfrom`, waiting for
    /// readability (a one-shot `POLL_ADD`) whenever no datagram is queued.
    pub async fn recv_from<B>(&self, buf: B) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: IoBufMut + Send + Unpin + 'static,
    {
        if let Ok(peer) = self.peer_addr() {
            let (res, buf) = self.recv(buf).await;
            match res {
                Ok(n) => (Ok((n, peer)), buf),
                Err(e) => (Err(e), buf),
            }
        } else {
            // Unconnected: the sender's address comes from `recvfrom`, so receive with
            // it and wait for readability whenever the queue is empty.
            let mut buf = buf;
            loop {
                let (res, b) = self.try_recv_from(buf);
                buf = b;
                match res {
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        if let Err(e) = self.wait(false).await {
                            return (Err(e), buf);
                        }
                    }
                    other => return (other, buf),
                }
            }
        }
    }

    /// Attempt to receive a datagram into `buf` synchronously without blocking.
    pub fn try_recv_from<B>(&self, mut buf: B) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: IoBufMut,
    {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let ret = unsafe {
            libc::recvfrom(
                self.fd,
                buf.stable_ptr_mut() as *mut libc::c_void,
                buf.bytes_total(),
                0,
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        };
        if ret >= 0 {
            let n = ret as usize;
            // SAFETY: kernel reported writing n bytes into the buffer.
            unsafe { buf.set_init(n) };
            match sockaddr_storage_to_addr(&storage) {
                Ok(addr) => (Ok((n, addr)), buf),
                Err(e) => (Err(e), buf),
            }
        } else {
            (Err(io::Error::last_os_error()), buf)
        }
    }

    /// Attempt to receive a datagram message on the socket without blocking.
    pub fn try_recv<B>(&self, mut buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut,
    {
        let ret = unsafe {
            libc::recv(
                self.fd,
                buf.stable_ptr_mut() as *mut libc::c_void,
                buf.bytes_total(),
                0,
            )
        };
        if ret >= 0 {
            let n = ret as usize;
            // SAFETY: kernel reported writing n bytes into the buffer.
            unsafe { buf.set_init(n) };
            (Ok(n), buf)
        } else {
            (Err(io::Error::last_os_error()), buf)
        }
    }

    /// Attempt to send a datagram message on the socket without blocking.
    pub fn try_send<B>(&self, buf: &B) -> io::Result<usize>
    where
        B: IoBuf,
    {
        let ret = unsafe {
            libc::send(
                self.fd,
                buf.stable_ptr() as *const libc::c_void,
                buf.bytes_init(),
                0,
            )
        };
        if ret >= 0 {
            Ok(ret as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Enable or disable `SO_BROADCAST`.
    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        let optval: libc::c_int = if on { 1 } else { 0 };
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_BROADCAST,
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

    /// Gets the value of the `SO_BROADCAST` option for this socket.
    pub fn broadcast(&self) -> io::Result<bool> {
        let mut optval: libc::c_int = 0;
        let mut optlen = std::mem::size_of_val(&optval) as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_BROADCAST,
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

    /// Enable or disable `SO_REUSEADDR`.
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

    /// Gets the value of the `SO_REUSEADDR` option for this socket.
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

    /// Enable or disable `SO_REUSEPORT`.
    pub fn set_reuseport(&self, reuseport: bool) -> io::Result<()> {
        let optval: libc::c_int = if reuseport { 1 } else { 0 };
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

    /// Gets the value of the `SO_REUSEPORT` option for this socket.
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

    /// Consumes this socket, returning the raw file descriptor.
    pub fn into_raw_fd(self) -> RawFd {
        let fd = self.fd;
        std::mem::forget(self);
        fd
    }

    /// Construct a `UdpSocket` from an existing raw file descriptor.
    ///
    /// # Safety
    /// The caller must ensure `fd` is a valid open UDP socket.
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self { fd }
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        crate::reactor::close_fd(self.fd);
    }
}

impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl IntoRawFd for UdpSocket {
    fn into_raw_fd(self) -> RawFd {
        self.into_raw_fd()
    }
}

impl FromRawFd for UdpSocket {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { Self::from_raw_fd(fd) }
    }
}
