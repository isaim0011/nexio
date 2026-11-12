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
    poller: Poller,
    events: Events,
    sources: Slab<Source>,
    timers: BTreeMap<TimerHandle, Waker>,
    next_timer: u64,
}

thread_local! {
    /// The current thread's reactor, created on first use.
    pub static REACTOR: RefCell<Reactor> =
        RefCell::new(Reactor::new().expect("failed to create the readiness poller"));
}

impl Reactor {
    /// Create a reactor with its own poller.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            poller: Poller::new()?,
            events: Events::new(),
            sources: Slab::new(),
            timers: BTreeMap::new(),
            next_timer: 0,
        })
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
        if write {
            src.write = Some(waker.clone());
        } else {
            src.read = Some(waker.clone());
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

    pub(crate) fn update_timer(&mut self, handle: TimerHandle, waker: Waker) {
        if let Some(slot) = self.timers.get_mut(&handle) {
            *slot = waker;
        }
    }

    pub(crate) fn cancel_timer(&mut self, handle: TimerHandle) {
        self.timers.remove(&handle);
    }

    // ── the executor's two calls ─────────────────────────────────────────────

    /// Poll without blocking: deliver ready I/O and due timers. Returns how
    /// many wakers were woken.
    pub fn submit_and_poll(&mut self) -> io::Result<usize> {
        self.wait(Some(Duration::ZERO))
    }

    /// Block until I/O is ready or the next timer is due, then deliver both.
    pub fn park(&mut self) -> io::Result<usize> {
        let timeout = self
            .timers
            .keys()
            .next()
            .map(|(deadline, _)| deadline.saturating_duration_since(Instant::now()));
        self.wait(timeout)
    }

    fn wait(&mut self, timeout: Option<Duration>) -> io::Result<usize> {
        self.events.clear();
        match self.poller.wait(&mut self.events, timeout) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }

        let mut to_wake: Vec<Waker> = Vec::new();
        let mut rearm: Vec<usize> = Vec::new();
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
        for key in rearm {
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

        let woken = to_wake.len();
        for w in to_wake {
            w.wake();
        }
        Ok(woken)
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
