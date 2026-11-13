//! Runtime-fallback reactor: tries `io_uring` first and automatically falls
//! back to `polling` (`epoll`) when `io_uring_setup` is blocked (`EPERM`/`ENOSYS`/`EACCES`)
//! or `NEXIO_FORCE_POLL` is set.

#[allow(unreachable_pub, unused_imports, dead_code)]
#[path = "../poll/reactor.rs"]
pub(crate) mod poll;
#[allow(unreachable_pub, unused_imports, dead_code)]
#[path = "../reactor/mod.rs"]
pub(crate) mod uring;

use std::{cell::RefCell, io, task::Waker, time::Instant};

pub(crate) use poll::RawSource;
pub(crate) use uring::close_fd;
#[doc(hidden)]
pub use uring::{op, slab};
pub use uring::{
    LazyOp, MultishotAccept, MultishotChunk, MultishotRecv, Op, ProvidedBufRing, ReactorStats,
};

pub(crate) type TimerHandle = (Instant, u64);

/// Thread-local reactor that uses `io_uring` when permitted by the kernel,
/// or falls back to `epoll` (`polling`) at runtime.
#[allow(clippy::large_enum_variant)]
pub enum Reactor {
    /// Linux `io_uring` backend.
    Uring(uring::Reactor),
    /// Portable `epoll` readiness fallback.
    Poll(poll::Reactor),
}

thread_local! {
    /// The current thread's reactor, created on first use.
    pub static REACTOR: RefCell<Reactor> = RefCell::new(
        Reactor::new().expect("failed to initialise both io_uring and poll fallback reactors")
    );
}

#[inline]
pub(crate) fn with_uring_mut<R>(f: impl FnOnce(&mut uring::Reactor) -> R) -> R {
    with_uring_mut_impl(f)
}

#[inline]
pub(crate) fn try_with_uring_mut<R>(f: impl FnOnce(&mut uring::Reactor) -> R) -> Option<R> {
    try_with_uring_mut_impl(f)
}

#[inline]
pub(crate) fn with_poll_mut<R>(f: impl FnOnce(&mut poll::Reactor) -> R) -> R {
    with_poll_mut_impl(f)
}

#[inline]
pub(crate) fn try_with_poll_mut<R>(f: impl FnOnce(&mut poll::Reactor) -> R) -> Option<R> {
    try_with_poll_mut_impl(f)
}

#[inline]
fn force_poll() -> bool {
    std::env::var_os("NEXIO_FORCE_POLL").is_some()
}

#[inline]
fn is_uring_blocked(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EPERM | libc::ENOSYS | libc::EACCES)
    )
}

#[inline]
pub(crate) fn is_uring() -> bool {
    REACTOR.with(|r| matches!(&*r.borrow(), Reactor::Uring(_)))
}

#[inline]
pub(crate) fn with_uring_impl<R>(f: impl FnOnce(&uring::Reactor) -> R) -> R {
    REACTOR.with(|r| match &*r.borrow() {
        Reactor::Uring(u) => f(u),
        Reactor::Poll(_) => panic!("io_uring operation invoked while running on poll fallback"),
    })
}

#[inline]
pub(crate) fn with_uring_mut_impl<R>(f: impl FnOnce(&mut uring::Reactor) -> R) -> R {
    REACTOR.with(|r| match &mut *r.borrow_mut() {
        Reactor::Uring(u) => f(u),
        Reactor::Poll(_) => panic!("io_uring operation invoked while running on poll fallback"),
    })
}

#[inline]
pub(crate) fn try_with_uring_impl<R>(f: impl FnOnce(&uring::Reactor) -> R) -> Option<R> {
    REACTOR
        .try_with(|r| {
            r.try_borrow().ok().and_then(|r| match &*r {
                Reactor::Uring(u) => Some(f(u)),
                Reactor::Poll(_) => None,
            })
        })
        .ok()
        .flatten()
}

#[inline]
pub(crate) fn try_with_uring_mut_impl<R>(f: impl FnOnce(&mut uring::Reactor) -> R) -> Option<R> {
    REACTOR
        .try_with(|r| {
            r.try_borrow_mut().ok().and_then(|mut r| match &mut *r {
                Reactor::Uring(u) => Some(f(u)),
                Reactor::Poll(_) => None,
            })
        })
        .ok()
        .flatten()
}

#[inline]
pub(crate) fn with_poll_mut_impl<R>(f: impl FnOnce(&mut poll::Reactor) -> R) -> R {
    REACTOR.with(|r| match &mut *r.borrow_mut() {
        Reactor::Poll(p) => f(p),
        Reactor::Uring(_) => panic!("poll operation invoked while running on io_uring"),
    })
}

#[inline]
pub(crate) fn try_with_poll_mut_impl<R>(f: impl FnOnce(&mut poll::Reactor) -> R) -> Option<R> {
    REACTOR
        .try_with(|r| {
            r.try_borrow_mut().ok().and_then(|mut r| match &mut *r {
                Reactor::Poll(p) => Some(f(p)),
                Reactor::Uring(_) => None,
            })
        })
        .ok()
        .flatten()
}

/// Builder for [`Reactor`] with automatic fallback to `poll` when `io_uring_setup` is blocked.
#[derive(Debug, Clone)]
pub struct ReactorBuilder {
    inner: uring::ReactorBuilder,
}

impl Default for ReactorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReactorBuilder {
    /// Create a new `ReactorBuilder`.
    pub fn new() -> Self {
        Self {
            inner: uring::ReactorBuilder::new(),
        }
    }

    /// Set the submission queue depth.
    pub fn entries(mut self, entries: u32) -> Self {
        self.inner = self.inner.entries(entries);
        self
    }

    /// Enable or disable `COOP_TASKRUN` / `DEFER_TASKRUN` / `SINGLE_ISSUER`.
    pub fn coop_taskrun(mut self, enabled: bool) -> Self {
        self.inner = self.inner.coop_taskrun(enabled);
        self
    }

    /// Enable or disable kernel-side SQPOLL thread.
    pub fn sqpoll(mut self, idle_ms: Option<u32>) -> Self {
        self.inner = self.inner.sqpoll(idle_ms);
        self
    }

    /// Build the [`Reactor`], falling back to `poll` if `io_uring_setup` is blocked.
    pub fn build(&self) -> io::Result<Reactor> {
        if force_poll() {
            return poll::Reactor::new().map(Reactor::Poll);
        }
        match self.inner.build() {
            Ok(u) => Ok(Reactor::Uring(u)),
            Err(e) if is_uring_blocked(&e) => poll::Reactor::new().map(Reactor::Poll),
            Err(e) => Err(e),
        }
    }
}

/// Return [`ReactorStats`] for the current thread's reactor, if initialized.
pub fn current_stats() -> Option<ReactorStats> {
    REACTOR
        .try_with(|r| r.try_borrow().ok().map(|r| r.stats()))
        .ok()
        .flatten()
}

impl Reactor {
    /// Create a builder to configure and construct a `Reactor`.
    pub fn builder() -> ReactorBuilder {
        ReactorBuilder::new()
    }

    /// Create a `Reactor` with a custom queue depth.
    pub fn with_entries(entries: u32) -> io::Result<Self> {
        ReactorBuilder::new().entries(entries).build()
    }

    /// Create a new `Reactor`, falling back to `poll` if `io_uring_setup` is blocked.
    pub fn new() -> io::Result<Self> {
        ReactorBuilder::new().build()
    }

    /// Return `true` if `io_uring` is supported in the current environment.
    pub fn is_supported() -> bool {
        !force_poll() && uring::Reactor::is_supported()
    }

    /// Return the backend name (`"io_uring"` or `"poll"` depending on runtime availability).
    pub fn backend_name() -> &'static str {
        if crate::io_uring_available() {
            "io_uring"
        } else {
            "poll"
        }
    }

    /// Return a snapshot of this reactor's active operations, timers, and queued SQEs.
    pub fn stats(&self) -> ReactorStats {
        match self {
            Reactor::Uring(u) => u.stats(),
            Reactor::Poll(p) => {
                let s = p.stats();
                ReactorStats {
                    in_flight_ops: s.in_flight_ops,
                    pending_timers: s.pending_timers,
                    sq_pending: s.sq_pending,
                }
            }
        }
    }

    /// Temporarily install `self` as the current thread's active reactor for `f`.
    pub fn enter<R>(&mut self, f: impl FnOnce() -> R) -> R {
        struct RestoreGuard<'a>(&'a mut Reactor);
        impl Drop for RestoreGuard<'_> {
            fn drop(&mut self) {
                REACTOR.with(|r| std::mem::swap(&mut *r.borrow_mut(), self.0));
            }
        }
        REACTOR.with(|r| std::mem::swap(&mut *r.borrow_mut(), self));
        let guard = RestoreGuard(self);
        let out = f();
        drop(guard);
        out
    }

    pub(crate) fn add_timer(&mut self, deadline: Instant, waker: Waker) -> TimerHandle {
        match self {
            Reactor::Uring(u) => u.add_timer(deadline, waker),
            Reactor::Poll(p) => p.add_timer(deadline, waker),
        }
    }

    pub(crate) fn update_timer(&mut self, key: TimerHandle, waker: &Waker) {
        match self {
            Reactor::Uring(u) => u.update_timer(key, waker),
            Reactor::Poll(p) => p.update_timer(key, waker),
        }
    }

    pub(crate) fn cancel_timer(&mut self, key: TimerHandle) {
        match self {
            Reactor::Uring(u) => u.cancel_timer(key),
            Reactor::Poll(p) => p.cancel_timer(key),
        }
    }

    /// Flush buffered work and drain ready completions without blocking.
    pub fn submit_and_poll(&mut self) -> io::Result<usize> {
        match self {
            Reactor::Uring(u) => u.submit_and_poll(),
            Reactor::Poll(p) => p.submit_and_poll(),
        }
    }

    pub(crate) fn submit_and_poll_into(&mut self, wakers: &mut Vec<Waker>) -> io::Result<usize> {
        match self {
            Reactor::Uring(u) => u.submit_and_poll_into(wakers),
            Reactor::Poll(p) => p.submit_and_poll_into(wakers),
        }
    }

    /// Block until an event or timer expires, then drain ready completions.
    pub fn park(&mut self) -> io::Result<usize> {
        match self {
            Reactor::Uring(u) => u.park(),
            Reactor::Poll(p) => p.park(),
        }
    }

    pub(crate) fn park_into(&mut self, wakers: &mut Vec<Waker>) -> io::Result<usize> {
        match self {
            Reactor::Uring(u) => u.park_into(wakers),
            Reactor::Poll(p) => p.park_into(wakers),
        }
    }
}
