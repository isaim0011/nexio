//! Portable readiness reactor (epoll / kqueue / IOCP via the `polling` crate).
//!
//! One `Reactor` per thread, exactly like the io_uring backend, and it exposes
//! the two calls the executor makes (`submit_and_poll`, `park`). The model is
//! different, though: a readiness backend never hands the kernel a pointer to
//! the caller's buffer. An operation tries the syscall, and if the socket would
//! block it registers interest and retries when woken. Dropping a pending
//! future therefore just abandons the wait; there is nothing in flight that
//! could write into freed memory, which is why this backend needs no
//! completion slab.

use std::{
    cell::RefCell,
    collections::BTreeMap,
    io,
    sync::Arc,
    task::Waker,
    time::{Duration, Instant},
};

use polling::{Event, Events, Poller};
use slab::Slab;

#[cfg(unix)]
pub(crate) type RawSource = std::os::fd::RawFd;
#[cfg(windows)]
pub(crate) type RawSource = std::os::windows::io::RawSocket;

#[derive(Default)]
struct Source {
    raw: Option<RawSource>,
    read: Option<Waker>,
    write: Option<Waker>,
}

/// Handle to a registered timer, used to update or cancel it.
pub(crate) type TimerHandle = (Instant, u64);

/// Thread-local readiness reactor.
pub struct Reactor {
    poller: Arc<Poller>,
    mailbox: Arc<crate::blocking::Mailbox>,
    events: Events,
    /// Reused between waits so a busy reactor does not allocate per wakeup round.
    wake_scratch: Vec<Waker>,
    rearm_scratch: Vec<usize>,
    sources: Slab<Source>,
    timers: BTreeMap<TimerHandle, Waker>,
    next_timer: u64,
}

#[cfg(not(nexio_runtime_fallback))]
thread_local! {
    /// The current thread's reactor, created on first use.
    pub static REACTOR: RefCell<Reactor> =
        RefCell::new(Reactor::new().expect("failed to create the readiness poller"));
}

#[cfg(not(nexio_runtime_fallback))]
#[inline]
pub(crate) fn with_poll_mut<R>(f: impl FnOnce(&mut Reactor) -> R) -> R {
    REACTOR.with(|r| f(&mut r.borrow_mut()))
}

#[cfg(not(nexio_runtime_fallback))]
#[inline]
pub(crate) fn try_with_poll_mut<R>(f: impl FnOnce(&mut Reactor) -> R) -> Option<R> {
    REACTOR
        .try_with(|r| r.try_borrow_mut().ok().map(|mut r| f(&mut r)))
        .ok()
        .flatten()
}

#[cfg(nexio_runtime_fallback)]
#[inline]
pub(crate) fn with_poll_mut<R>(f: impl FnOnce(&mut Reactor) -> R) -> R {
    crate::reactor::with_poll_mut_impl(f)
}

#[cfg(nexio_runtime_fallback)]
#[inline]
pub(crate) fn try_with_poll_mut<R>(f: impl FnOnce(&mut Reactor) -> R) -> Option<R> {
    crate::reactor::try_with_poll_mut_impl(f)
}

/// Portable builder stub matching [`crate::reactor::ReactorBuilder`] so
/// cross-platform callers compile on both `io_uring` and `poll` backends.
#[derive(Debug, Clone, Default)]
pub struct ReactorBuilder {
    entries: Option<u32>,
}

impl ReactorBuilder {
    /// Create a new `ReactorBuilder`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Ignored on the readiness backend; retained for API parity with `io_uring`.
    pub fn entries(mut self, entries: u32) -> Self {
        self.entries = Some(entries);
        self
    }

    /// Ignored on the readiness backend; retained for API parity with `io_uring`.
    pub fn coop_taskrun(self, _enabled: bool) -> Self {
        self
    }

    /// Ignored on the readiness backend; retained for API parity with `io_uring`.
    pub fn sqpoll(self, _idle: Option<u32>) -> Self {
        self
    }

    /// Build the readiness [`Reactor`].
    pub fn build(&self) -> io::Result<Reactor> {
        Reactor::new()
    }
}

/// Snapshot of reactor queue and source metrics for production observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReactorStats {
    /// Number of currently registered I/O sources in the poller slab.
    pub in_flight_ops: usize,
    /// Number of pending timers in the reactor timer map.
    pub pending_timers: usize,
    /// Number of queued SQEs (`0` on the readiness poll backend).
    pub sq_pending: usize,
}

/// Return [`ReactorStats`] for the current thread's reactor, if initialized.
#[cfg(not(nexio_runtime_fallback))]
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

    /// Create a `Reactor` (the `entries` hint is accepted for parity with `io_uring`).
    pub fn with_entries(_entries: u32) -> io::Result<Self> {
        Self::new()
    }

    /// Create a reactor with its own poller.
    pub fn new() -> io::Result<Self> {
        let poller = Arc::new(Poller::new()?);
        let waker_poller = poller.clone();
        let mailbox = Arc::new(crate::blocking::Mailbox::new(Box::new(move || {
            let _ = waker_poller.notify();
        })));
        Ok(Self {
            poller,
            mailbox,
            events: Events::new(),
            wake_scratch: Vec::new(),
            rearm_scratch: Vec::new(),
            sources: Slab::new(),
            timers: BTreeMap::new(),
            next_timer: 0,
        })
    }

    /// Return `true` if the readiness reactor backend is supported on this host.
    pub fn is_supported() -> bool {
        true
    }

    /// Return the active reactor backend name (`"io_uring"` or `"poll"`).
    pub const fn backend_name() -> &'static str {
        "poll"
    }

    /// Return a snapshot of this reactor's registered sources and pending timers.
    pub fn stats(&self) -> ReactorStats {
        ReactorStats {
            in_flight_ops: self.sources.len(),
            pending_timers: self.timers.len(),
            sq_pending: 0,
        }
    }

    /// The mailbox pool threads use to hand results back to this thread.
    pub(crate) fn mailbox(&self) -> Arc<crate::blocking::Mailbox> {
        self.mailbox.clone()
    }

    /// Register a socket and return its key. No interest is armed yet.
    pub(crate) fn register(&mut self, raw: RawSource) -> io::Result<usize> {
        let key = self.sources.insert(Source {
            raw: Some(raw),
            ..Source::default()
        });
        // SAFETY: the caller keeps the socket alive until `deregister`.
        if let Err(e) = unsafe { self.poller.add(raw, Event::none(key)) } {
            self.sources.remove(key);
            return Err(e);
        }
        Ok(key)
    }

    /// Remove a socket from the poller. Safe to call twice.
    pub(crate) fn deregister(&mut self, key: usize, source: impl polling::AsSource) {
        if self.sources.try_remove(key).is_some() {
            let _ = self.poller.delete(source);
        }
    }

    /// Record `waker` as waiting for readiness in one direction and arm the
    /// poller for everything currently awaited on this socket.
    pub(crate) fn arm(
        &mut self,
        key: usize,
        source: impl polling::AsSource,
        write: bool,
        waker: &Waker,
    ) -> io::Result<()> {
        let Some(src) = self.sources.get_mut(key) else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "socket is deregistered",
            ));
        };
        let slot = if write { &mut src.write } else { &mut src.read };
        if !matches!(slot, Some(existing) if existing.will_wake(waker)) {
            *slot = Some(waker.clone());
        }
        let ev = Event::new(key, src.read.is_some(), src.write.is_some());
        self.poller.modify(source, ev)
    }

    // ── timers ───────────────────────────────────────────────────────────────

    pub(crate) fn add_timer(&mut self, deadline: Instant, waker: Waker) -> TimerHandle {
        self.next_timer += 1;
        let handle = (deadline, self.next_timer);
        self.timers.insert(handle, waker);
        handle
    }

    pub(crate) fn update_timer(&mut self, handle: TimerHandle, waker: &Waker) {
        if let Some(slot) = self.timers.get_mut(&handle) {
            if !slot.will_wake(waker) {
                *slot = waker.clone();
            }
        }
    }

    pub(crate) fn cancel_timer(&mut self, handle: TimerHandle) {
        self.timers.remove(&handle);
    }

    // ── the executor's two calls ─────────────────────────────────────────────

    /// Poll without blocking: deliver ready I/O and due timers. Returns how
    /// many wakers were woken.
    pub fn submit_and_poll(&mut self) -> io::Result<usize> {
        let mut wakers = std::mem::take(&mut self.wake_scratch);
        let res = self.submit_and_poll_into(&mut wakers);
        for w in wakers.drain(..) {
            w.wake();
        }
        self.wake_scratch = wakers;
        res
    }

    /// Poll without blocking, pushing I/O and timer wakers into `wakers` so
    /// the caller can wake them outside any `RefCell` borrow.
    pub(crate) fn submit_and_poll_into(&mut self, wakers: &mut Vec<Waker>) -> io::Result<usize> {
        self.wait_into(Some(Duration::ZERO), wakers)
    }

    /// Block until I/O is ready or the next timer is due, then deliver both.
    pub fn park(&mut self) -> io::Result<usize> {
        let mut wakers = std::mem::take(&mut self.wake_scratch);
        let res = self.park_into(&mut wakers);
        for w in wakers.drain(..) {
            w.wake();
        }
        self.wake_scratch = wakers;
        res
    }

    /// Block until I/O is ready or the next timer is due, pushing wakers into `wakers`.
    pub(crate) fn park_into(&mut self, wakers: &mut Vec<Waker>) -> io::Result<usize> {
        let timeout = self
            .timers
            .keys()
            .next()
            .map(|(deadline, _)| deadline.saturating_duration_since(Instant::now()));
        self.wait_into(timeout, wakers)
    }

    fn wait_into(
        &mut self,
        timeout: Option<Duration>,
        to_wake: &mut Vec<Waker>,
    ) -> io::Result<usize> {
        self.events.clear();
        match self.poller.wait(&mut self.events, timeout) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }

        let before = to_wake.len();
        let mut rearm = std::mem::take(&mut self.rearm_scratch);
        for ev in self.events.iter() {
            let Some(src) = self.sources.get_mut(ev.key) else {
                continue;
            };
            if ev.readable {
                to_wake.extend(src.read.take());
            }
            if ev.writable {
                to_wake.extend(src.write.take());
            }
            // The poller disarms a socket once it fires, so anything still
            // awaited in the other direction must be armed again.
            if src.read.is_some() || src.write.is_some() {
                rearm.push(ev.key);
            }
        }
        for key in rearm.drain(..) {
            if let Some(src) = self.sources.get(key) {
                if let Some(raw) = src.raw {
                    let ev = Event::new(key, src.read.is_some(), src.write.is_some());
                    // SAFETY: `raw` belongs to a socket that is still registered, so it is open.
                    let _ = self.poller.modify(unsafe { borrow(raw) }, ev);
                }
            }
        }

        let now = Instant::now();
        while let Some(entry) = self.timers.first_entry() {
            if entry.key().0 > now {
                break;
            }
            to_wake.push(entry.remove());
        }

        let woken = to_wake.len() - before;
        self.rearm_scratch = rearm;
        // Results handed back by blocking-pool threads.
        let finished = self.mailbox.drain();
        let blocking_woken = if finished.is_empty() {
            0
        } else {
            crate::blocking::deliver(finished)
        };
        let remote = self.mailbox.drain_wakes();
        let remote_woken = remote.len();
        crate::executor::deliver_remote_wakes(remote);
        Ok(woken + blocking_woken + remote_woken)
    }
}

#[cfg(unix)]
unsafe fn borrow<'a>(raw: RawSource) -> std::os::fd::BorrowedFd<'a> {
    unsafe { std::os::fd::BorrowedFd::borrow_raw(raw) }
}

#[cfg(windows)]
unsafe fn borrow<'a>(raw: RawSource) -> std::os::windows::io::BorrowedSocket<'a> {
    unsafe { std::os::windows::io::BorrowedSocket::borrow_raw(raw) }
}
