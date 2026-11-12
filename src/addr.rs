//! Asynchronous address resolution.
//!
//! `std::net::ToSocketAddrs` resolves host names with a blocking system call. Called
//! on a reactor thread that freezes every task on that thread, for as long as the
//! DNS server takes (seconds, if it is slow or down). [`ToAddrs`] fixes that: IP
//! literals resolve immediately with no thread hop, and only real host names are
//! sent to the blocking pool.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs},
};

/// What a resolvable value turned out to be.
#[doc(hidden)]
pub enum Resolution {
    /// Already numeric; no lookup needed.
    Done(Vec<SocketAddr>),
    /// A `"host:port"` string that needs the system resolver.
    Name(String),
    /// A host name and a port that need the system resolver.
    HostPort(String, u16),
}

mod sealed {
    pub trait Sealed {}
}

/// A value that names one or more socket addresses and can be resolved without
/// blocking the reactor.
///
/// Implemented for the same types as [`std::net::ToSocketAddrs`] (socket addresses,
/// `(ip, port)` tuples, `(&str, u16)`, `(String, u16)`, `&str`, `String`, and slices of
/// socket addresses). The trait is sealed.
pub trait ToAddrs: sealed::Sealed {
    #[doc(hidden)]
    fn into_resolution(self) -> Resolution;
}

macro_rules! impl_done {
    ($t:ty, |$s:ident| $e:expr) => {
        impl sealed::Sealed for $t {}
        impl ToAddrs for $t {
            fn into_resolution(self) -> Resolution {
                let $s = self;
                Resolution::Done(vec![$e])
            }
        }
    };
}

impl_done!(SocketAddr, |s| s);
impl_done!(SocketAddrV4, |s| SocketAddr::V4(s));
impl_done!(SocketAddrV6, |s| SocketAddr::V6(s));
impl_done!((IpAddr, u16), |s| SocketAddr::new(s.0, s.1));
impl_done!((Ipv4Addr, u16), |s| SocketAddr::new(IpAddr::V4(s.0), s.1));
impl_done!((Ipv6Addr, u16), |s| SocketAddr::new(IpAddr::V6(s.0), s.1));

impl sealed::Sealed for &[SocketAddr] {}
impl ToAddrs for &[SocketAddr] {
    fn into_resolution(self) -> Resolution {
        Resolution::Done(self.to_vec())
    }
}

impl sealed::Sealed for Vec<SocketAddr> {}
impl ToAddrs for Vec<SocketAddr> {
    fn into_resolution(self) -> Resolution {
        Resolution::Done(self)
    }
}

fn host_port(host: &str, port: u16) -> Resolution {
    // Bracketed or bare IPv6 literals and dotted IPv4 literals need no lookup.
    let trimmed = host.trim_start_matches('[').trim_end_matches(']');
    match trimmed.parse::<IpAddr>() {
        Ok(ip) => Resolution::Done(vec![SocketAddr::new(ip, port)]),
        Err(_) => Resolution::HostPort(host.to_owned(), port),
    }
}

fn host_string(s: &str) -> Resolution {
    match s.parse::<SocketAddr>() {
        Ok(a) => Resolution::Done(vec![a]),
        Err(_) => Resolution::Name(s.to_owned()),
    }
}

impl sealed::Sealed for (&str, u16) {}
impl ToAddrs for (&str, u16) {
    fn into_resolution(self) -> Resolution {
        host_port(self.0, self.1)
    }
}

impl sealed::Sealed for (String, u16) {}
impl ToAddrs for (String, u16) {
    fn into_resolution(self) -> Resolution {
        host_port(&self.0, self.1)
    }
}

impl sealed::Sealed for &str {}
impl ToAddrs for &str {
    fn into_resolution(self) -> Resolution {
        host_string(self)
    }
}

impl sealed::Sealed for String {}
impl ToAddrs for String {
    fn into_resolution(self) -> Resolution {
        host_string(&self)
    }
}

impl sealed::Sealed for &String {}
impl ToAddrs for &String {
    fn into_resolution(self) -> Resolution {
        host_string(self)
    }
}

/// Resolve `addr` to socket addresses without blocking the reactor.
///
/// IP literals and `SocketAddr` values return immediately. Host names are looked up
/// on the blocking pool with the system resolver (so `/etc/hosts`, search domains and
/// the platform's DNS configuration all apply). Must be called from inside
/// [`block_on`](crate::block_on).
pub async fn lookup_host(addr: impl ToAddrs) -> io::Result<Vec<SocketAddr>> {
    match addr.into_resolution() {
        Resolution::Done(v) => Ok(v),
        Resolution::Name(s) => {
            crate::spawn_blocking(move || s.to_socket_addrs().map(|it| it.collect::<Vec<_>>()))
                .await
                .map_err(join_to_io)?
        }
        Resolution::HostPort(h, p) => crate::spawn_blocking(move || {
            (h.as_str(), p)
                .to_socket_addrs()
                .map(|it| it.collect::<Vec<_>>())
        })
        .await
        .map_err(join_to_io)?,
    }
}

fn join_to_io(e: crate::executor::JoinError) -> io::Error {
    io::Error::other(format!("address lookup failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;

    fn done(r: Resolution) -> Option<Vec<SocketAddr>> {
        match r {
            Resolution::Done(v) => Some(v),
            _ => None,
        }
    }

    #[test]
    fn ip_literals_never_reach_the_resolver() {
        let want: SocketAddr = "127.0.0.1:80".parse().unwrap();
        assert_eq!(done(("127.0.0.1", 80).into_resolution()), Some(vec![want]));
        assert_eq!(done("127.0.0.1:80".into_resolution()), Some(vec![want]));
        assert_eq!(done(want.into_resolution()), Some(vec![want]));
        assert_eq!(
            done((Ipv4Addr::LOCALHOST, 80).into_resolution()),
            Some(vec![want])
        );
        let v6: SocketAddr = "[::1]:8080".parse().unwrap();
        assert_eq!(done("[::1]:8080".into_resolution()), Some(vec![v6]));
        assert_eq!(done(("::1", 8080).into_resolution()), Some(vec![v6]));
        assert_eq!(done(("[::1]", 8080).into_resolution()), Some(vec![v6]));
    }

    #[test]
    fn host_names_are_sent_to_the_resolver() {
        assert!(done(("example.invalid", 80).into_resolution()).is_none());
        assert!(done("example.invalid:80".into_resolution()).is_none());
        assert!(done(String::from("example.invalid:80").into_resolution()).is_none());
    }

    #[test]
    fn localhost_resolves_without_freezing_the_reactor() {
        block_on(async {
            let addrs = lookup_host(("localhost", 8080)).await.unwrap();
            assert!(!addrs.is_empty());
            assert!(addrs
                .iter()
                .all(|a| a.ip().is_loopback() && a.port() == 8080));
            let addrs = lookup_host("localhost:9").await.unwrap();
            assert!(addrs.iter().all(|a| a.ip().is_loopback() && a.port() == 9));
        });
    }

    #[test]
    fn unresolvable_names_return_an_error_not_a_hang() {
        block_on(async {
            let r = lookup_host(("nexio-no-such-host.invalid", 80)).await;
            assert!(r.is_err());
        });
    }

    #[test]
    fn a_slow_lookup_does_not_stall_timers() {
        // The resolver runs on the pool, so the reactor keeps servicing timers
        // while a lookup is outstanding.
        block_on(async {
            let lookup =
                crate::spawn(async { lookup_host(("nexio-no-such-host.invalid", 80)).await });
            let t = std::time::Instant::now();
            for _ in 0..3 {
                crate::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(t.elapsed() < std::time::Duration::from_millis(500));
            let _ = lookup.await;
        });
    }
}
