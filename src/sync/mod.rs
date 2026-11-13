//! Asynchronous synchronization primitives and channels.
//!
//! Includes single-producer/multi-producer channels ([`oneshot`], [`mpsc`]),
//! broadcast and watch channels ([`broadcast`], [`watch`]), locks ([`Mutex`],
//! [`RwLock`]), coordination ([`Notify`], [`Semaphore`]), and graceful-shutdown
//! [`CancellationToken`].

use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex as StdMutex,
    },
    task::{Context, Poll, Waker},
};

pub use crate::executor::channel::{mpsc, oneshot};

// ── Notify ────────────────────────────────────────────────────────────────────

struct NotifyState {
    notified_one: bool,
    epoch: u64,
    waiters: VecDeque<(u64, Waker)>,
    next_id: u64,
}

/// Notifies a single task or all waiting tasks to wake up.
#[derive(Clone)]
pub struct Notify {
    state: Arc<StdMutex<NotifyState>>,
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Notify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notify").finish_non_exhaustive()
    }
}

impl Notify {
    /// Create a new `Notify`.
    pub fn new() -> Self {
        Self {
            state: Arc::new(StdMutex::new(NotifyState {
                notified_one: false,
                epoch: 0,
                waiters: VecDeque::new(),
                next_id: 1,
            })),
        }
    }

    /// Notify one waiting task, or store a permit if none is waiting yet.
    pub fn notify_one(&self) {
        let waker = {
            let mut st = self.state.lock().unwrap();
            if let Some((_, w)) = st.waiters.pop_front() {
                Some(w)
            } else {
                st.notified_one = true;
                None
            }
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// Notify all currently waiting tasks.
    pub fn notify_waiters(&self) {
        let wakers: Vec<Waker> = {
            let mut st = self.state.lock().unwrap();
            st.epoch = st.epoch.wrapping_add(1);
            st.waiters.drain(..).map(|(_, w)| w).collect()
        };
        for w in wakers {
            w.wake();
        }
    }

    /// Wait for a notification.
    pub fn notified(&self) -> Notified<'_> {
        let epoch = self.state.lock().unwrap().epoch;
        Notified {
            notify: self,
            id: 0,
            epoch,
            done: false,
        }
    }
}

/// Future returned by [`Notify::notified`].
pub struct Notified<'a> {
    notify: &'a Notify,
    id: u64,
    epoch: u64,
    done: bool,
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.done {
            return Poll::Ready(());
        }
        let mut st = self.notify.state.lock().unwrap();
        if st.epoch != self.epoch {
            self.done = true;
            return Poll::Ready(());
        }
        if st.notified_one {
            st.notified_one = false;
            self.done = true;
            return Poll::Ready(());
        }
        if self.id == 0 {
            let id = st.next_id;
            st.next_id = st.next_id.wrapping_add(1).max(1);
            self.id = id;
            st.waiters.push_back((id, cx.waker().clone()));
        } else if let Some((_, w)) = st.waiters.iter_mut().find(|(id, _)| *id == self.id) {
            if !w.will_wake(cx.waker()) {
                *w = cx.waker().clone();
            }
        } else {
            // Popped by notify_one()
            self.done = true;
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

impl Drop for Notified<'_> {
    fn drop(&mut self) {
        if !self.done && self.id != 0 {
            let mut st = self.notify.state.lock().unwrap();
            let before = st.waiters.len();
            st.waiters.retain(|(id, _)| *id != self.id);
            if st.waiters.len() == before && st.epoch == self.epoch {
                // We were popped by notify_one() right before drop; pass permit on.
                drop(st);
                self.notify.notify_one();
            }
        }
    }
}

// ── Semaphore ─────────────────────────────────────────────────────────────────

struct SemaphoreState {
    permits: usize,
    waiters: VecDeque<(u64, usize, Waker)>,
    next_id: u64,
}

/// Counting semaphore for bounding concurrency.
pub struct Semaphore {
    state: StdMutex<SemaphoreState>,
}

impl fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Semaphore")
            .field("available_permits", &self.available_permits())
            .finish()
    }
}

/// Error returned when [`Semaphore::try_acquire`] cannot obtain permits immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TryAcquireError;

impl fmt::Display for TryAcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no permits available")
    }
}

impl std::error::Error for TryAcquireError {}

/// Permit guard returned by [`Semaphore::acquire`] / [`Semaphore::acquire_many`].
pub struct SemaphorePermit<'a> {
    sem: &'a Semaphore,
    permits: usize,
}

impl SemaphorePermit<'_> {
    /// Forget the permit without returning it to the semaphore.
    pub fn forget(mut self) {
        self.permits = 0;
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        if self.permits > 0 {
            self.sem.add_permits(self.permits);
        }
    }
}

impl Semaphore {
    /// Create a new semaphore with `permits` initial permits.
    pub fn new(permits: usize) -> Self {
        Self {
            state: StdMutex::new(SemaphoreState {
                permits,
                waiters: VecDeque::new(),
                next_id: 1,
            }),
        }
    }

    /// Return the current number of available permits.
    pub fn available_permits(&self) -> usize {
        self.state.lock().unwrap().permits
    }

    /// Add `n` permits to the semaphore and wake eligible waiters.
    pub fn add_permits(&self, n: usize) {
        let mut to_wake = Vec::new();
        {
            let mut st = self.state.lock().unwrap();
            st.permits = st.permits.saturating_add(n);
            let mut avail = st.permits;
            while let Some(&(_, needed, _)) = st.waiters.front() {
                if avail >= needed {
                    avail -= needed;
                    if let Some((_, _, w)) = st.waiters.pop_front() {
                        to_wake.push(w);
                    }
                } else {
                    break;
                }
            }
        }
        for w in to_wake {
            w.wake();
        }
    }

    /// Try to acquire one permit immediately.
    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        self.try_acquire_many(1)
    }

    /// Try to acquire `n` permits immediately.
    pub fn try_acquire_many(&self, n: usize) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        let mut st = self.state.lock().unwrap();
        if st.waiters.is_empty() && st.permits >= n {
            st.permits -= n;
            Ok(SemaphorePermit {
                sem: self,
                permits: n,
            })
        } else {
            Err(TryAcquireError)
        }
    }

    /// Acquire one permit asynchronously.
    pub fn acquire(&self) -> Acquire<'_> {
        self.acquire_many(1)
    }

    /// Acquire `n` permits asynchronously.
    pub fn acquire_many(&self, n: usize) -> Acquire<'_> {
        Acquire {
            sem: self,
            needed: n,
            id: 0,
            acquired: false,
        }
    }
}

/// Future returned by [`Semaphore::acquire`] and [`Semaphore::acquire_many`].
pub struct Acquire<'a> {
    sem: &'a Semaphore,
    needed: usize,
    id: u64,
    acquired: bool,
}

impl<'a> Future for Acquire<'a> {
    type Output = SemaphorePermit<'a>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.acquired {
            panic!("Acquire polled after completion");
        }
        let mut st = self.sem.state.lock().unwrap();
        let is_front_or_empty = st.waiters.front().map_or(true, |(id, _, _)| *id == self.id);
        if is_front_or_empty && st.permits >= self.needed {
            st.permits -= self.needed;
            if self.id != 0 {
                st.waiters.retain(|(id, _, _)| *id != self.id);
            }
            self.acquired = true;
            return Poll::Ready(SemaphorePermit {
                sem: self.sem,
                permits: self.needed,
            });
        }
        if self.id == 0 {
            let id = st.next_id;
            st.next_id = st.next_id.wrapping_add(1).max(1);
            self.id = id;
            st.waiters.push_back((id, self.needed, cx.waker().clone()));
        } else if let Some((_, _, w)) = st.waiters.iter_mut().find(|(id, _, _)| *id == self.id) {
            if !w.will_wake(cx.waker()) {
                *w = cx.waker().clone();
            }
        } else {
            // Re-queue at front if popped while another task grabbed permits before our poll.
            st.waiters
                .push_front((self.id, self.needed, cx.waker().clone()));
        }
        Poll::Pending
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        if !self.acquired && self.id != 0 {
            let mut st = self.sem.state.lock().unwrap();
            st.waiters.retain(|(id, _, _)| *id != self.id);
        }
    }
}

// ── Mutex ─────────────────────────────────────────────────────────────────────

/// Error returned by [`Mutex::try_lock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TryLockError;

impl fmt::Display for TryLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("lock is currently held")
    }
}

impl std::error::Error for TryLockError {}

/// An asynchronous mutual exclusion lock.
pub struct Mutex<T> {
    sem: Semaphore,
    data: std::cell::UnsafeCell<T>,
}

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

impl<T: fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.try_lock() {
            Ok(g) => f.debug_struct("Mutex").field("data", &*g).finish(),
            Err(_) => f.debug_struct("Mutex").field("data", &"<locked>").finish(),
        }
    }
}

/// RAII guard for [`Mutex`].
pub struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
    _permit: SemaphorePermit<'a>,
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T> Mutex<T> {
    /// Create a new unlocked `Mutex`.
    pub fn new(value: T) -> Self {
        Self {
            sem: Semaphore::new(1),
            data: std::cell::UnsafeCell::new(value),
        }
    }

    /// Acquire the lock asynchronously.
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        let permit = self.sem.acquire().await;
        MutexGuard {
            mutex: self,
            _permit: permit,
        }
    }

    /// Try to acquire the lock without waiting.
    pub fn try_lock(&self) -> Result<MutexGuard<'_, T>, TryLockError> {
        let permit = self.sem.try_acquire().map_err(|_| TryLockError)?;
        Ok(MutexGuard {
            mutex: self,
            _permit: permit,
        })
    }

    /// Get a mutable reference to the inner data without locking.
    pub fn get_mut(&mut self) -> &mut T {
        self.data.get_mut()
    }

    /// Consume the mutex and return the inner value.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

// ── RwLock ────────────────────────────────────────────────────────────────────

const RWLOCK_MAX_READERS: usize = isize::MAX as usize;

/// An asynchronous reader-writer lock.
pub struct RwLock<T> {
    sem: Semaphore,
    data: std::cell::UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send + Sync> Sync for RwLock<T> {}

impl<T: fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.try_read() {
            Ok(g) => f.debug_struct("RwLock").field("data", &*g).finish(),
            Err(_) => f.debug_struct("RwLock").field("data", &"<locked>").finish(),
        }
    }
}

/// Shared read guard for [`RwLock`].
pub struct RwLockReadGuard<'a, T> {
    lock: &'a RwLock<T>,
    _permit: SemaphorePermit<'a>,
}

impl<T> Deref for RwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

/// Exclusive write guard for [`RwLock`].
pub struct RwLockWriteGuard<'a, T> {
    lock: &'a RwLock<T>,
    _permit: SemaphorePermit<'a>,
}

impl<T> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> RwLock<T> {
    /// Create a new unlocked `RwLock`.
    pub fn new(value: T) -> Self {
        Self {
            sem: Semaphore::new(RWLOCK_MAX_READERS),
            data: std::cell::UnsafeCell::new(value),
        }
    }

    /// Acquire a shared read lock asynchronously.
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        let permit = self.sem.acquire().await;
        RwLockReadGuard {
            lock: self,
            _permit: permit,
        }
    }

    /// Try to acquire a shared read lock without waiting.
    pub fn try_read(&self) -> Result<RwLockReadGuard<'_, T>, TryLockError> {
        let permit = self.sem.try_acquire().map_err(|_| TryLockError)?;
        Ok(RwLockReadGuard {
            lock: self,
            _permit: permit,
        })
    }

    /// Acquire an exclusive write lock asynchronously.
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        let permit = self.sem.acquire_many(RWLOCK_MAX_READERS).await;
        RwLockWriteGuard {
            lock: self,
            _permit: permit,
        }
    }

    /// Try to acquire an exclusive write lock without waiting.
    pub fn try_write(&self) -> Result<RwLockWriteGuard<'_, T>, TryLockError> {
        let permit = self
            .sem
            .try_acquire_many(RWLOCK_MAX_READERS)
            .map_err(|_| TryLockError)?;
        Ok(RwLockWriteGuard {
            lock: self,
            _permit: permit,
        })
    }

    /// Get a mutable reference to the inner data without locking.
    pub fn get_mut(&mut self) -> &mut T {
        self.data.get_mut()
    }

    /// Consume the lock and return the inner value.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

// ── watch ─────────────────────────────────────────────────────────────────────

/// A single-producer, multi-consumer channel that retains the latest value.
pub mod watch {
    use super::*;
    use std::sync::{RwLock as StdRwLock, RwLockReadGuard as StdRwLockReadGuard};

    /// Error returned when a watch channel has been closed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RecvError;

    impl fmt::Display for RecvError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("watch channel closed")
        }
    }

    impl std::error::Error for RecvError {}

    /// Error returned by [`Sender::send`] when all receivers have been dropped.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SendError<T>(pub T);

    impl<T: fmt::Debug> fmt::Display for SendError<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("watch channel closed")
        }
    }

    impl<T: fmt::Debug> std::error::Error for SendError<T> {}

    struct Shared<T> {
        value: StdRwLock<T>,
        version: AtomicU64,
        closed: AtomicBool,
        notify: Notify,
        rx_count: AtomicUsize,
    }

    /// Borrowed reference to the latest value in a [`watch`] channel.
    pub struct Ref<'a, T> {
        guard: StdRwLockReadGuard<'a, T>,
    }

    impl<T> Deref for Ref<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.guard
        }
    }

    /// Sending half of a [`watch`] channel.
    pub struct Sender<T> {
        shared: Arc<Shared<T>>,
    }

    /// Receiving half of a [`watch`] channel.
    pub struct Receiver<T> {
        shared: Arc<Shared<T>>,
        seen_version: u64,
    }

    impl<T> Clone for Receiver<T> {
        fn clone(&self) -> Self {
            self.shared.rx_count.fetch_add(1, Ordering::Relaxed);
            Self {
                shared: Arc::clone(&self.shared),
                seen_version: self.seen_version,
            }
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            self.shared.rx_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            self.shared.closed.store(true, Ordering::Release);
            self.shared.notify.notify_waiters();
        }
    }

    /// Create a new [`watch`] channel initialized to `init`.
    pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
        let shared = Arc::new(Shared {
            value: StdRwLock::new(init),
            version: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            notify: Notify::new(),
            rx_count: AtomicUsize::new(1),
        });
        (
            Sender {
                shared: Arc::clone(&shared),
            },
            Receiver {
                shared,
                seen_version: 0,
            },
        )
    }

    impl<T> Sender<T> {
        /// Send a new value, notifying all receivers.
        pub fn send(&self, value: T) -> Result<(), SendError<T>> {
            if self.is_closed() {
                return Err(SendError(value));
            }
            *self.shared.value.write().unwrap() = value;
            self.shared.version.fetch_add(1, Ordering::Release);
            self.shared.notify.notify_waiters();
            Ok(())
        }

        /// Replace the current value even if no receivers exist, returning the old value.
        pub fn send_replace(&self, value: T) -> T {
            let old = std::mem::replace(&mut *self.shared.value.write().unwrap(), value);
            self.shared.version.fetch_add(1, Ordering::Release);
            self.shared.notify.notify_waiters();
            old
        }

        /// Borrow the current value.
        pub fn borrow(&self) -> Ref<'_, T> {
            Ref {
                guard: self.shared.value.read().unwrap(),
            }
        }

        /// Create a new receiver subscribed to future updates.
        pub fn subscribe(&self) -> Receiver<T> {
            self.shared.rx_count.fetch_add(1, Ordering::Relaxed);
            Receiver {
                shared: Arc::clone(&self.shared),
                seen_version: self.shared.version.load(Ordering::Acquire),
            }
        }

        /// Return `true` if all receivers have been dropped.
        pub fn is_closed(&self) -> bool {
            self.shared.rx_count.load(Ordering::Acquire) == 0
        }
    }

    impl<T> Receiver<T> {
        /// Return `true` if the value has changed since it was last seen.
        pub fn has_changed(&self) -> Result<bool, RecvError> {
            let v = self.shared.version.load(Ordering::Acquire);
            if v != self.seen_version {
                Ok(true)
            } else if self.shared.closed.load(Ordering::Acquire) {
                Err(RecvError)
            } else {
                Ok(false)
            }
        }

        /// Borrow the current value without marking it as seen.
        pub fn borrow(&self) -> Ref<'_, T> {
            Ref {
                guard: self.shared.value.read().unwrap(),
            }
        }

        /// Borrow the current value and mark it as seen.
        pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
            let v = self.shared.version.load(Ordering::Acquire);
            self.seen_version = v;
            Ref {
                guard: self.shared.value.read().unwrap(),
            }
        }

        /// Wait until a new value is sent or the sender is dropped.
        pub async fn changed(&mut self) -> Result<(), RecvError> {
            loop {
                let notified = self.shared.notify.notified();
                let v = self.shared.version.load(Ordering::Acquire);
                if v != self.seen_version {
                    self.seen_version = v;
                    return Ok(());
                }
                if self.shared.closed.load(Ordering::Acquire) {
                    return Err(RecvError);
                }
                notified.await;
            }
        }
    }
}

// ── broadcast ─────────────────────────────────────────────────────────────────

/// A multi-producer, multi-consumer bounded broadcast channel.
pub mod broadcast {
    use super::*;

    /// Error returned by [`Sender::send`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SendError<T>(pub T);

    impl<T: fmt::Debug> fmt::Display for SendError<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("broadcast channel has no active receivers")
        }
    }

    impl<T: fmt::Debug> std::error::Error for SendError<T> {}

    /// Error returned by [`Receiver::recv`] and [`Receiver::try_recv`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TryRecvError {
        /// No unread messages are currently available.
        Empty,
        /// All senders were dropped.
        Closed,
        /// This receiver fell behind and missed `u64` messages.
        Lagged(u64),
    }

    /// Error returned by [`Receiver::recv`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RecvError {
        /// All senders were dropped.
        Closed,
        /// This receiver fell behind and missed `u64` messages.
        Lagged(u64),
    }

    impl fmt::Display for RecvError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Closed => f.write_str("broadcast channel closed"),
                Self::Lagged(n) => write!(f, "broadcast receiver lagged by {n} messages"),
            }
        }
    }

    impl std::error::Error for RecvError {}

    struct Slot<T> {
        seq: u64,
        val: T,
    }

    struct Inner<T> {
        capacity: usize,
        buffer: VecDeque<Slot<T>>,
        tail_seq: u64,
        tx_count: usize,
        rx_count: usize,
    }

    struct Shared<T> {
        inner: StdMutex<Inner<T>>,
        notify: Notify,
    }

    /// Sending half of a [`broadcast`] channel.
    pub struct Sender<T> {
        shared: Arc<Shared<T>>,
    }

    /// Receiving half of a [`broadcast`] channel.
    pub struct Receiver<T> {
        shared: Arc<Shared<T>>,
        next_seq: u64,
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.shared.inner.lock().unwrap().tx_count += 1;
            Self {
                shared: Arc::clone(&self.shared),
            }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let last = {
                let mut inner = self.shared.inner.lock().unwrap();
                inner.tx_count -= 1;
                inner.tx_count == 0
            };
            if last {
                self.shared.notify.notify_waiters();
            }
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            let mut inner = self.shared.inner.lock().unwrap();
            inner.rx_count -= 1;
            if inner.rx_count == 0 {
                inner.buffer.clear();
            }
        }
    }

    /// Create a bounded [`broadcast`] channel with `capacity` slots.
    pub fn channel<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>) {
        assert!(capacity > 0, "broadcast capacity must be > 0");
        let shared = Arc::new(Shared {
            inner: StdMutex::new(Inner {
                capacity,
                buffer: VecDeque::with_capacity(capacity),
                tail_seq: 0,
                tx_count: 1,
                rx_count: 1,
            }),
            notify: Notify::new(),
        });
        (
            Sender {
                shared: Arc::clone(&shared),
            },
            Receiver {
                shared,
                next_seq: 0,
            },
        )
    }

    impl<T: Clone> Sender<T> {
        /// Broadcast `value` to all active receivers. Returns the receiver count on success.
        pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
            let rx_count = {
                let mut inner = self.shared.inner.lock().unwrap();
                if inner.rx_count == 0 {
                    return Err(SendError(value));
                }
                let seq = inner.tail_seq;
                inner.tail_seq += 1;
                if inner.buffer.len() == inner.capacity {
                    inner.buffer.pop_front();
                }
                inner.buffer.push_back(Slot { seq, val: value });
                inner.rx_count
            };
            self.shared.notify.notify_waiters();
            Ok(rx_count)
        }

        /// Subscribe a new [`Receiver`] starting at the current tail.
        pub fn subscribe(&self) -> Receiver<T> {
            let mut inner = self.shared.inner.lock().unwrap();
            inner.rx_count += 1;
            let next_seq = inner.tail_seq;
            Receiver {
                shared: Arc::clone(&self.shared),
                next_seq,
            }
        }

        /// Return the current number of active receivers.
        pub fn receiver_count(&self) -> usize {
            self.shared.inner.lock().unwrap().rx_count
        }
    }

    impl<T: Clone> Receiver<T> {
        /// Try to receive the next message without waiting.
        pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
            let inner = self.shared.inner.lock().unwrap();
            let oldest_seq = inner.buffer.front().map_or(inner.tail_seq, |slot| slot.seq);
            if self.next_seq < oldest_seq {
                let lagged = oldest_seq - self.next_seq;
                self.next_seq = oldest_seq;
                return Err(TryRecvError::Lagged(lagged));
            }
            let offset = (self.next_seq - oldest_seq) as usize;
            if let Some(slot) = inner.buffer.get(offset) {
                self.next_seq += 1;
                Ok(slot.val.clone())
            } else if inner.tx_count == 0 {
                Err(TryRecvError::Closed)
            } else {
                Err(TryRecvError::Empty)
            }
        }

        /// Receive the next message asynchronously.
        pub async fn recv(&mut self) -> Result<T, RecvError> {
            let shared = Arc::clone(&self.shared);
            loop {
                let notified = shared.notify.notified();
                match self.try_recv() {
                    Ok(v) => return Ok(v),
                    Err(TryRecvError::Closed) => return Err(RecvError::Closed),
                    Err(TryRecvError::Lagged(n)) => return Err(RecvError::Lagged(n)),
                    Err(TryRecvError::Empty) => notified.await,
                }
            }
        }

        /// Create a fresh receiver subscribed from the current tail.
        pub fn resubscribe(&self) -> Self {
            let mut inner = self.shared.inner.lock().unwrap();
            inner.rx_count += 1;
            let next_seq = inner.tail_seq;
            Self {
                shared: Arc::clone(&self.shared),
                next_seq,
            }
        }
    }
}

// ── CancellationToken ─────────────────────────────────────────────────────────

struct TokenInner {
    cancelled: AtomicBool,
    notify: Notify,
    children: StdMutex<Vec<CancellationToken>>,
}

/// Cooperative cancellation token for graceful shutdown.
#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<TokenInner>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("is_cancelled", &self.is_cancelled())
            .finish()
    }
}

impl CancellationToken {
    /// Create a new uncancelled token.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TokenInner {
                cancelled: AtomicBool::new(false),
                notify: Notify::new(),
                children: StdMutex::new(Vec::new()),
            }),
        }
    }

    /// Create a child token that is cancelled when this parent is cancelled.
    pub fn child_token(&self) -> Self {
        let child = Self::new();
        if self.is_cancelled() {
            child.cancel();
            return child;
        }
        self.inner.children.lock().unwrap().push(child.clone());
        if self.is_cancelled() {
            child.cancel();
        }
        child
    }

    /// Cancel this token and all its children.
    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::AcqRel) {
            self.inner.notify.notify_waiters();
            let children = std::mem::take(&mut *self.inner.children.lock().unwrap());
            for child in children {
                child.cancel();
            }
        }
    }

    /// Return `true` if cancellation has been triggered.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Wait until this token is cancelled.
    pub async fn cancelled(&self) {
        loop {
            let notified = self.inner.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}
