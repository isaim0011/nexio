//! Socket registration and readiness waiting shared by TCP and UDP on the
//! portable backend.

use std::{future::poll_fn, io, net::SocketAddr, task::Poll};

use socket2::{SockAddr, Socket};

use crate::reactor::{try_with_poll_mut, with_poll_mut};

#[cfg(unix)]
pub(super) fn raw(s: &Socket) -> crate::reactor::RawSource {
    use std::os::fd::AsRawFd;
    s.as_raw_fd()
}
#[cfg(windows)]
pub(super) fn raw(s: &Socket) -> crate::reactor::RawSource {
    use std::os::windows::io::AsRawSocket;
    s.as_raw_socket()
}

pub(super) fn is_in_progress(e: &io::Error) -> bool {
    if e.kind() == io::ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(unix)]
    if e.raw_os_error() == Some(libc::EINPROGRESS) {
        return true;
    }
    #[cfg(windows)]
    if e.raw_os_error() == Some(10036) {
        return true; // WSAEINPROGRESS
    }
    false
}

/// A non-blocking socket registered with this thread's reactor.
pub(super) struct Io {
    pub(super) sock: Socket,
    key: usize,
}

impl Io {
    pub(super) fn new(sock: Socket) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        #[cfg(target_vendor = "apple")]
        sock.set_nosigpipe(true)?;
        let key = with_poll_mut(|r| r.register(raw(&sock)))?;
        Ok(Self { sock, key })
    }

    /// Wait once for the socket to become ready in one direction. Callers retry
    /// their syscall in a loop, so a spurious wakeup only costs one more attempt.
    pub(super) async fn ready(&self, write: bool) -> io::Result<()> {
        let mut armed = false;
        poll_fn(|cx| {
            if armed {
                return Poll::Ready(Ok(()));
            }
            match with_poll_mut(|r| r.arm(self.key, &self.sock, write, cx.waker())) {
                Ok(()) => {
                    armed = true;
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        })
        .await
    }

    pub(super) fn send_nosig(&self, data: &[u8]) -> io::Result<usize> {
        #[cfg(all(unix, not(target_vendor = "apple")))]
        {
            self.sock.send_with_flags(data, libc::MSG_NOSIGNAL)
        }
        #[cfg(not(all(unix, not(target_vendor = "apple"))))]
        {
            self.sock.send(data)
        }
    }
}

impl Drop for Io {
    fn drop(&mut self) {
        let _ = try_with_poll_mut(|r| {
            r.deregister(self.key, &self.sock);
        });
    }
}

pub(super) fn socket_addr(a: SockAddr) -> io::Result<SocketAddr> {
    a.as_socket()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "not an IP socket address"))
}
