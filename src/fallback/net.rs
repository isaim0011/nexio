//! Runtime-fallback networking module dispatching between `io_uring` and `poll`.

#[allow(unreachable_pub, unused_imports, dead_code)]
#[path = "../poll/net.rs"]
mod poll;
#[allow(unreachable_pub, unused_imports, dead_code)]
#[path = "../net/mod.rs"]
mod uring;

use std::{
    io,
    net::{Shutdown, SocketAddr},
    os::unix::io::{AsRawFd, RawFd},
    time::Duration,
};

pub use crate::addr::{lookup_host, ToAddrs};
use crate::buf::{BufResult, IoBuf, IoBufMut};
pub use uring::unix::{self, UnixDatagram, UnixListener, UnixStream};

/// Stream of accepted [`TcpStream`]s backed by io_uring multishot accept.
pub struct MultishotTcpAccept {
    inner: uring::MultishotTcpAccept,
}

impl MultishotTcpAccept {
    /// Await the next accepted [`TcpStream`], or `None` when the multishot operation terminates.
    pub async fn next(&mut self) -> Option<io::Result<TcpStream>> {
        self.inner.next().await.map(|r| r.map(TcpStream::Uring))
    }
}

/// A TCP socket server listening for connections.
pub enum TcpListener {
    /// Linux `io_uring` listener.
    Uring(uring::TcpListener),
    /// Portable `epoll` fallback listener.
    Poll(poll::TcpListener),
}

impl TcpListener {
    /// Bind to `addr` and start listening.
    pub async fn bind(addr: impl ToAddrs) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::TcpListener::bind(addr).await.map(TcpListener::Uring)
        } else {
            poll::TcpListener::bind(addr).await.map(TcpListener::Poll)
        }
    }

    /// Bind synchronously to a single [`SocketAddr`].
    pub fn bind_addr(addr: SocketAddr) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::TcpListener::bind_addr(addr).map(TcpListener::Uring)
        } else {
            poll::TcpListener::bind_addr(addr).map(TcpListener::Poll)
        }
    }

    /// Create a `TcpListener` from a standard library `TcpListener`.
    pub fn from_std(listener: std::net::TcpListener) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::TcpListener::from_std(listener).map(TcpListener::Uring)
        } else {
            poll::TcpListener::from_std(listener).map(TcpListener::Poll)
        }
    }

    /// Returns the local socket address of this listener.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            TcpListener::Uring(l) => l.local_addr(),
            TcpListener::Poll(l) => l.local_addr(),
        }
    }

    /// Accept a new connection.
    pub async fn accept(&self) -> io::Result<TcpStream> {
        match self {
            TcpListener::Uring(l) => l.accept().await.map(TcpStream::Uring),
            TcpListener::Poll(l) => l.accept().await.map(TcpStream::Poll),
        }
    }

    /// Accept a new connection, returning both stream and peer address.
    pub async fn accept_addr(&self) -> io::Result<(TcpStream, SocketAddr)> {
        match self {
            TcpListener::Uring(l) => l.accept_addr().await.map(|(s, a)| (TcpStream::Uring(s), a)),
            TcpListener::Poll(l) => l.accept_addr().await.map(|(s, a)| (TcpStream::Poll(s), a)),
        }
    }

    /// Start a multishot accept stream on this listener (`io_uring` only).
    pub fn accept_multishot(&self) -> io::Result<MultishotTcpAccept> {
        match self {
            TcpListener::Uring(l) => l
                .accept_multishot()
                .map(|inner| MultishotTcpAccept { inner }),
            TcpListener::Poll(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "accept_multishot requires io_uring",
            )),
        }
    }

    /// Enable or disable `SO_REUSEADDR`.
    pub fn set_reuseaddr(&self, reuse: bool) -> io::Result<()> {
        match self {
            TcpListener::Uring(l) => l.set_reuseaddr(reuse),
            TcpListener::Poll(l) => l.set_reuseaddr(reuse),
        }
    }

    /// Query `SO_REUSEADDR`.
    pub fn reuseaddr(&self) -> io::Result<bool> {
        match self {
            TcpListener::Uring(l) => l.reuseaddr(),
            TcpListener::Poll(l) => l.reuseaddr(),
        }
    }

    /// Enable `SO_REUSEPORT` (on `io_uring`, takes no argument; on `poll`, sets `true`).
    pub fn set_reuseport(&self) -> io::Result<()> {
        match self {
            TcpListener::Uring(l) => l.set_reuseport(),
            TcpListener::Poll(l) => l.set_reuseport(true),
        }
    }

    /// Query `SO_REUSEPORT`.
    pub fn reuseport(&self) -> io::Result<bool> {
        match self {
            TcpListener::Uring(l) => l.reuseport(),
            TcpListener::Poll(l) => l.reuseport(),
        }
    }

    /// Set `IP_TTL`.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        match self {
            TcpListener::Uring(l) => l.set_ttl(ttl),
            TcpListener::Poll(l) => l.set_ttl(ttl),
        }
    }

    /// Query `IP_TTL`.
    pub fn ttl(&self) -> io::Result<u32> {
        match self {
            TcpListener::Uring(l) => l.ttl(),
            TcpListener::Poll(l) => l.ttl(),
        }
    }

    /// Query and clear `SO_ERROR`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        match self {
            TcpListener::Uring(l) => l.take_error(),
            TcpListener::Poll(l) => l.take_error(),
        }
    }

    /// Return the raw file descriptor.
    pub fn as_raw_fd(&self) -> RawFd {
        match self {
            TcpListener::Uring(l) => l.as_raw_fd(),
            TcpListener::Poll(l) => l.as_raw_fd(),
        }
    }

    /// Construct a `TcpListener` from an existing raw file descriptor.
    ///
    /// # Safety
    /// The caller must ensure `fd` is a valid open listening socket.
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        if crate::reactor::is_uring() {
            TcpListener::Uring(unsafe { uring::TcpListener::from_raw_fd(fd) })
        } else {
            let std_listener =
                unsafe { <std::net::TcpListener as std::os::unix::io::FromRawFd>::from_raw_fd(fd) };
            TcpListener::Poll(
                poll::TcpListener::from_std(std_listener)
                    .expect("failed to register raw TcpListener on poll fallback"),
            )
        }
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        TcpListener::as_raw_fd(self)
    }
}

/// A TCP stream between a local and a remote socket.
pub enum TcpStream {
    /// Linux `io_uring` TCP stream.
    Uring(uring::TcpStream),
    /// Portable `epoll` fallback TCP stream.
    Poll(poll::TcpStream),
}

impl TcpStream {
    /// Connect to `addr`.
    pub async fn connect(addr: impl ToAddrs) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::TcpStream::connect(addr).await.map(TcpStream::Uring)
        } else {
            poll::TcpStream::connect(addr).await.map(TcpStream::Poll)
        }
    }

    /// Connect to a single [`SocketAddr`].
    pub async fn connect_addr(addr: SocketAddr) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::TcpStream::connect_addr(addr)
                .await
                .map(TcpStream::Uring)
        } else {
            poll::TcpStream::connect_addr(addr)
                .await
                .map(TcpStream::Poll)
        }
    }

    /// Create a `TcpStream` from a standard library `TcpStream`.
    pub fn from_std(stream: std::net::TcpStream) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::TcpStream::from_std(stream).map(TcpStream::Uring)
        } else {
            poll::TcpStream::from_std(stream).map(TcpStream::Poll)
        }
    }

    /// Returns the remote peer address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            TcpStream::Uring(s) => s.peer_addr(),
            TcpStream::Poll(s) => s.peer_addr(),
        }
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            TcpStream::Uring(s) => s.local_addr(),
            TcpStream::Poll(s) => s.local_addr(),
        }
    }

    /// Read into `buf`.
    pub async fn read<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        match self {
            TcpStream::Uring(s) => s.read(buf).await,
            TcpStream::Poll(s) => s.read(buf).await,
        }
    }

    /// Read until `buf` is full.
    pub async fn read_exact<B>(&self, buf: B) -> BufResult<(), B>
    where
        B: IoBufMut + Send + 'static,
    {
        match self {
            TcpStream::Uring(s) => s.read_exact(buf).await,
            TcpStream::Poll(s) => s.read_exact(buf).await,
        }
    }

    /// Write `buf` to the stream.
    pub async fn write<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        match self {
            TcpStream::Uring(s) => s.write(buf).await,
            TcpStream::Poll(s) => s.write(buf).await,
        }
    }

    /// Write all initialized bytes in `buf` to the stream.
    pub async fn write_all<B>(&self, buf: B) -> BufResult<(), B>
    where
        B: IoBuf + Send + 'static,
    {
        match self {
            TcpStream::Uring(s) => s.write_all(buf).await,
            TcpStream::Poll(s) => s.write_all(buf).await,
        }
    }

    /// Vectored read into multiple buffers.
    pub async fn read_vectored<B>(&self, bufs: Vec<B>) -> BufResult<usize, Vec<B>>
    where
        B: IoBufMut + Send + 'static,
    {
        match self {
            TcpStream::Uring(s) => s.read_vectored(bufs).await,
            TcpStream::Poll(s) => s.read_vectored(bufs).await,
        }
    }

    /// Vectored write from multiple buffers.
    pub async fn write_vectored<B>(&self, bufs: Vec<B>) -> BufResult<usize, Vec<B>>
    where
        B: IoBuf + Send + 'static,
    {
        match self {
            TcpStream::Uring(s) => s.write_vectored(bufs).await,
            TcpStream::Poll(s) => s.write_vectored(bufs).await,
        }
    }

    /// Write all initialized bytes from multiple buffers.
    pub async fn write_all_vectored<B>(&self, bufs: Vec<B>) -> BufResult<(), Vec<B>>
    where
        B: IoBuf + Send + 'static,
    {
        match self {
            TcpStream::Uring(s) => s.write_all_vectored(bufs).await,
            TcpStream::Poll(s) => s.write_all_vectored(bufs).await,
        }
    }

    /// Start a multishot receive stream (`io_uring` only).
    pub fn recv_multishot(&self, bgid: u16) -> io::Result<crate::reactor::MultishotRecv> {
        match self {
            TcpStream::Uring(s) => s.recv_multishot(bgid),
            TcpStream::Poll(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "recv_multishot requires io_uring",
            )),
        }
    }

    /// Shut down the read, write, or both halves of this connection.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match self {
            TcpStream::Uring(s) => s.shutdown(how),
            TcpStream::Poll(s) => s.shutdown(how),
        }
    }

    /// Enable or disable `TCP_NODELAY`.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        match self {
            TcpStream::Uring(s) => s.set_nodelay(nodelay),
            TcpStream::Poll(s) => s.set_nodelay(nodelay),
        }
    }

    /// Query `TCP_NODELAY`.
    pub fn nodelay(&self) -> io::Result<bool> {
        match self {
            TcpStream::Uring(s) => s.nodelay(),
            TcpStream::Poll(s) => s.nodelay(),
        }
    }

    /// Enable or disable `SO_KEEPALIVE`.
    pub fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        match self {
            TcpStream::Uring(s) => s.set_keepalive(keepalive),
            TcpStream::Poll(s) => s.set_keepalive(keepalive),
        }
    }

    /// Query `SO_KEEPALIVE`.
    pub fn keepalive(&self) -> io::Result<bool> {
        match self {
            TcpStream::Uring(s) => s.keepalive(),
            TcpStream::Poll(s) => s.keepalive(),
        }
    }

    /// Set `SO_LINGER`.
    pub fn set_linger(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            TcpStream::Uring(s) => s.set_linger(dur),
            TcpStream::Poll(s) => s.set_linger(dur),
        }
    }

    /// Query `SO_LINGER`.
    pub fn linger(&self) -> io::Result<Option<Duration>> {
        match self {
            TcpStream::Uring(s) => s.linger(),
            TcpStream::Poll(s) => s.linger(),
        }
    }

    /// Set `SO_RCVBUF`.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        match self {
            TcpStream::Uring(s) => s.set_recv_buffer_size(size),
            TcpStream::Poll(s) => s.set_recv_buffer_size(size),
        }
    }

    /// Query `SO_RCVBUF`.
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        match self {
            TcpStream::Uring(s) => s.recv_buffer_size(),
            TcpStream::Poll(s) => s.recv_buffer_size(),
        }
    }

    /// Set `SO_SNDBUF`.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        match self {
            TcpStream::Uring(s) => s.set_send_buffer_size(size),
            TcpStream::Poll(s) => s.set_send_buffer_size(size),
        }
    }

    /// Query `SO_SNDBUF`.
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        match self {
            TcpStream::Uring(s) => s.send_buffer_size(),
            TcpStream::Poll(s) => s.send_buffer_size(),
        }
    }

    /// Set `IP_TTL`.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        match self {
            TcpStream::Uring(s) => s.set_ttl(ttl),
            TcpStream::Poll(s) => s.set_ttl(ttl),
        }
    }

    /// Query `IP_TTL`.
    pub fn ttl(&self) -> io::Result<u32> {
        match self {
            TcpStream::Uring(s) => s.ttl(),
            TcpStream::Poll(s) => s.ttl(),
        }
    }

    /// Query and clear `SO_ERROR`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        match self {
            TcpStream::Uring(s) => s.take_error(),
            TcpStream::Poll(s) => s.take_error(),
        }
    }

    /// Return the raw file descriptor.
    pub fn as_raw_fd(&self) -> RawFd {
        match self {
            TcpStream::Uring(s) => s.as_raw_fd(),
            TcpStream::Poll(s) => s.as_raw_fd(),
        }
    }

    /// Construct a `TcpStream` from an existing raw file descriptor.
    ///
    /// # Safety
    /// The caller must ensure `fd` is a valid open TCP stream socket.
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        if crate::reactor::is_uring() {
            TcpStream::Uring(unsafe { uring::TcpStream::from_raw_fd(fd) })
        } else {
            let std_stream =
                unsafe { <std::net::TcpStream as std::os::unix::io::FromRawFd>::from_raw_fd(fd) };
            TcpStream::Poll(
                poll::TcpStream::from_std(std_stream)
                    .expect("failed to register raw TcpStream on poll fallback"),
            )
        }
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        TcpStream::as_raw_fd(self)
    }
}

define_stream_split!(TcpStream);

/// A UDP socket.
pub enum UdpSocket {
    /// Linux `io_uring` UDP socket.
    Uring(uring::UdpSocket),
    /// Portable `epoll` fallback UDP socket.
    Poll(poll::UdpSocket),
}

impl UdpSocket {
    /// Bind a UDP socket to `addr`.
    pub async fn bind(addr: impl ToAddrs) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::UdpSocket::bind(addr).await.map(UdpSocket::Uring)
        } else {
            poll::UdpSocket::bind(addr).await.map(UdpSocket::Poll)
        }
    }

    /// Bind synchronously to a single [`SocketAddr`].
    pub fn bind_addr(addr: SocketAddr) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::UdpSocket::bind_addr(addr).map(UdpSocket::Uring)
        } else {
            poll::UdpSocket::bind_addr(addr).map(UdpSocket::Poll)
        }
    }

    /// Wrap an existing standard library [`std::net::UdpSocket`].
    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::UdpSocket::from_std(socket).map(UdpSocket::Uring)
        } else {
            poll::UdpSocket::from_std(socket).map(UdpSocket::Poll)
        }
    }

    /// Connect this UDP socket to a remote address.
    pub async fn connect(&self, addr: impl ToAddrs) -> io::Result<()> {
        match self {
            UdpSocket::Uring(u) => u.connect(addr).await,
            UdpSocket::Poll(u) => u.connect(addr).await,
        }
    }

    /// Connect synchronously to a single [`SocketAddr`].
    pub fn connect_addr(&self, addr: SocketAddr) -> io::Result<()> {
        match self {
            UdpSocket::Uring(u) => u.connect_addr(addr),
            UdpSocket::Poll(u) => u.connect_addr(addr),
        }
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            UdpSocket::Uring(u) => u.local_addr(),
            UdpSocket::Poll(u) => u.local_addr(),
        }
    }

    /// Returns the connected remote peer address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            UdpSocket::Uring(u) => u.peer_addr(),
            UdpSocket::Poll(u) => u.peer_addr(),
        }
    }

    /// Send a datagram on a connected UDP socket.
    pub async fn send<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBuf + Send + Unpin + 'static,
    {
        match self {
            UdpSocket::Uring(u) => u.send(buf).await,
            UdpSocket::Poll(u) => u.send(buf).await,
        }
    }

    /// Receive a datagram on a connected UDP socket.
    pub async fn recv<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + Unpin + 'static,
    {
        match self {
            UdpSocket::Uring(u) => u.recv(buf).await,
            UdpSocket::Poll(u) => u.recv(buf).await,
        }
    }

    /// Send a datagram to `target`.
    pub async fn send_to<B>(&self, buf: B, target: SocketAddr) -> BufResult<usize, B>
    where
        B: IoBuf + Send + Unpin + 'static,
    {
        match self {
            UdpSocket::Uring(u) => u.send_to(buf, target).await,
            UdpSocket::Poll(u) => u.send_to(buf, target).await,
        }
    }

    /// Receive a datagram and its source address.
    pub async fn recv_from<B>(&self, buf: B) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: IoBufMut + Send + Unpin + 'static,
    {
        match self {
            UdpSocket::Uring(u) => u.recv_from(buf).await,
            UdpSocket::Poll(u) => u.recv_from(buf).await,
        }
    }

    /// Try sending a datagram to `target` without suspending.
    pub fn try_send_to<B: IoBuf>(&self, buf: &B, target: SocketAddr) -> io::Result<usize> {
        match self {
            UdpSocket::Uring(u) => u.try_send_to(buf, target),
            UdpSocket::Poll(u) => u.try_send_to(buf, target),
        }
    }

    /// Try sending a datagram on a connected socket without suspending.
    pub fn try_send<B: IoBuf>(&self, buf: &B) -> io::Result<usize> {
        match self {
            UdpSocket::Uring(u) => u.try_send(buf),
            UdpSocket::Poll(u) => u.try_send(buf),
        }
    }

    /// Try receiving a datagram and source address without suspending.
    pub fn try_recv_from<B: IoBufMut>(&self, buf: B) -> (io::Result<(usize, SocketAddr)>, B) {
        match self {
            UdpSocket::Uring(u) => u.try_recv_from(buf),
            UdpSocket::Poll(u) => u.try_recv_from(buf),
        }
    }

    /// Try receiving a datagram without suspending.
    pub fn try_recv<B: IoBufMut>(&self, buf: B) -> BufResult<usize, B> {
        match self {
            UdpSocket::Uring(u) => u.try_recv(buf),
            UdpSocket::Poll(u) => u.try_recv(buf),
        }
    }

    /// Set `SO_BROADCAST`.
    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        match self {
            UdpSocket::Uring(u) => u.set_broadcast(on),
            UdpSocket::Poll(u) => u.set_broadcast(on),
        }
    }

    /// Query `SO_BROADCAST`.
    pub fn broadcast(&self) -> io::Result<bool> {
        match self {
            UdpSocket::Uring(u) => u.broadcast(),
            UdpSocket::Poll(u) => u.broadcast(),
        }
    }

    /// Set `SO_REUSEADDR`.
    pub fn set_reuseaddr(&self, on: bool) -> io::Result<()> {
        match self {
            UdpSocket::Uring(u) => u.set_reuseaddr(on),
            UdpSocket::Poll(u) => u.set_reuseaddr(on),
        }
    }

    /// Query `SO_REUSEADDR`.
    pub fn reuseaddr(&self) -> io::Result<bool> {
        match self {
            UdpSocket::Uring(u) => u.reuseaddr(),
            UdpSocket::Poll(u) => u.reuseaddr(),
        }
    }

    /// Set `SO_REUSEPORT`.
    pub fn set_reuseport(&self, on: bool) -> io::Result<()> {
        match self {
            UdpSocket::Uring(u) => u.set_reuseport(on),
            UdpSocket::Poll(u) => u.set_reuseport(on),
        }
    }

    /// Query `SO_REUSEPORT`.
    pub fn reuseport(&self) -> io::Result<bool> {
        match self {
            UdpSocket::Uring(u) => u.reuseport(),
            UdpSocket::Poll(u) => u.reuseport(),
        }
    }

    /// Set `IP_TTL`.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        match self {
            UdpSocket::Uring(u) => u.set_ttl(ttl),
            UdpSocket::Poll(u) => u.set_ttl(ttl),
        }
    }

    /// Query `IP_TTL`.
    pub fn ttl(&self) -> io::Result<u32> {
        match self {
            UdpSocket::Uring(u) => u.ttl(),
            UdpSocket::Poll(u) => u.ttl(),
        }
    }

    /// Set `SO_RCVBUF`.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        match self {
            UdpSocket::Uring(u) => u.set_recv_buffer_size(size),
            UdpSocket::Poll(u) => u.set_recv_buffer_size(size),
        }
    }

    /// Query `SO_RCVBUF`.
    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        match self {
            UdpSocket::Uring(u) => u.recv_buffer_size(),
            UdpSocket::Poll(u) => u.recv_buffer_size(),
        }
    }

    /// Set `SO_SNDBUF`.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        match self {
            UdpSocket::Uring(u) => u.set_send_buffer_size(size),
            UdpSocket::Poll(u) => u.set_send_buffer_size(size),
        }
    }

    /// Query `SO_SNDBUF`.
    pub fn send_buffer_size(&self) -> io::Result<usize> {
        match self {
            UdpSocket::Uring(u) => u.send_buffer_size(),
            UdpSocket::Poll(u) => u.send_buffer_size(),
        }
    }

    /// Query and clear `SO_ERROR`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        match self {
            UdpSocket::Uring(u) => u.take_error(),
            UdpSocket::Poll(u) => u.take_error(),
        }
    }

    /// Return the raw file descriptor.
    pub fn as_raw_fd(&self) -> RawFd {
        match self {
            UdpSocket::Uring(u) => u.as_raw_fd(),
            UdpSocket::Poll(u) => u.as_raw_fd(),
        }
    }
}

impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        UdpSocket::as_raw_fd(self)
    }
}
