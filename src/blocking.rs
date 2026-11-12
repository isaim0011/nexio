//! A thread pool for work that would block the reactor thread.
//!
//! Some operations have no readiness or completion interface on every platform
//! (name resolution, file I/O on the portable backend, waiting for a child
//! process). They run on pool threads, and [`spawn_blocking`] returns a future
//! that completes on the reactor thread that asked for it.
//!
//! # How a result gets back to the right thread
//!
//! Executor wakers are thread-local and must not be called from a pool thread.
//! Instead each reactor thread owns a mailbox: a worker stores its result,
//! pushes the job id into the mailbox and pokes the reactor awake; the reactor
//! then drains the mailbox on its own thread and wakes the waiting tasks there.
//! The future registers its waker *before* it checks for the result, so a result
//! that lands in between is never missed.

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex, OnceLock,
    },
    task::{Context, Poll, Waker},
    thread,
    time::Duration,
};

use crate::executor::JoinError;

const MAX_THREADS: usize = 512;
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);

// ── mailbox ──────────────────────────────────────────────────────────────────

/// Where pool threads deposit finished job ids for one reactor thread.
pub(crate) struct Mailbox {
    done: Mutex<Vec<u64>>,
    notify: Box<dyn Fn() + Send + Sync>,
}

impl Mailbox {
    pub(crate) fn new(notify: Box<dyn Fn() + Send + Sync>) -> Self {
        Self {
            done: Mutex::new(Vec::new()),
            notify,
        }
    }

    fn complete(&self, id: u64) {
        self.done.lock().unwrap_or_else(|e| e.into_inner()).push(id);
        (self.notify)();
    }

    pub(crate) fn drain(&self) -> Vec<u64> {
        std::mem::take(&mut *self.done.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

thread_local! {
    /// Wakers of tasks awaiting a blocking job, keyed by job id. Reactor thread only.
    static WAKERS: RefCell<HashMap<u64, Waker>> = RefCell::new(HashMap::new());
}

/// Wake the tasks waiting on these finished jobs. Must run on the reactor thread.
/// Returns how many tasks were woken.
pub(crate) fn deliver(ids: Vec<u64>) -> usize {
    let wakers: Vec<Waker> = WAKERS.with(|w| {
        let mut w = w.borrow_mut();
        ids.iter().filter_map(|id| w.remove(id)).collect()
    });
    let n = wakers.len();
    for w in wakers {
        w.wake();
    }
    n
}

#[cfg(nexio_poll)]
fn current_mailbox() -> Arc<Mailbox> {
    crate::reactor::REACTOR.with(|r| r.borrow().mailbox())
}

#[cfg(nexio_uring)]
fn current_mailbox() -> Arc<Mailbox> {
    thread_local! {
        static MAILBOX: RefCell<Option<Arc<Mailbox>>> = const { RefCell::new(None) };
    }
    MAILBOX.with(|slot| {
        if let Some(m) = slot.borrow().as_ref() {
            return m.clone();
        }
        let m = start_uring_mailbox();
        *slot.borrow_mut() = Some(m.clone());
        m
    })
}

/// io_uring has no thread-safe "wake the reactor" call, so a background task on
/// the reactor reads an eventfd through the ordinary read path and drains the
/// mailbox whenever a pool thread writes to it.
#[cfg(nexio_uring)]
fn start_uring_mailbox() -> Arc<Mailbox> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    // SAFETY: eventfd has no pointer arguments; the result is checked below.
    let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
    assert!(
        raw >= 0,
        "eventfd failed: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: `raw` is a fresh descriptor that nothing else owns.
    let writer = unsafe { OwnedFd::from_raw_fd(raw) };
    let reader = writer.try_clone().expect("failed to duplicate the eventfd");
    let mailbox = Arc::new(Mailbox::new(Box::new(move || {
        let one: u64 = 1;
        // SAFETY: writing 8 bytes from a live u64 to an eventfd we own; a failed
        // write only means the counter is already saturated, which still wakes the reader.
        unsafe {
            libc::write(
                writer.as_raw_fd(),
                &one as *const u64 as *const libc::c_void,
                8,
            );
        }
    })));

    let for_task = mailbox.clone();
    let file = crate::fs::File::from_std(std::fs::File::from(reader));
    drop(crate::spawn(async move {
        let mut buf = vec![0u8; 8];
        loop {
            let (res, b) = file.read_exact_at(buf, 0).await;
            buf = b;
            if res.is_err() {
                break;
            }
            deliver(for_task.drain());
        }
    }));
    mailbox
}

// ── the pool ─────────────────────────────────────────────────────────────────

type Job = Box<dyn FnOnce() + Send + 'static>;

struct PoolState {
    queue: VecDeque<Job>,
    idle: usize,
    threads: usize,
}

struct Pool {
    state: Mutex<PoolState>,
    wake: Condvar,
}

fn pool() -> &'static Pool {
    static POOL: OnceLock<Pool> = OnceLock::new();
    POOL.get_or_init(|| Pool {
        state: Mutex::new(PoolState {
            queue: VecDeque::new(),
            idle: 0,
            threads: 0,
        }),
        wake: Condvar::new(),
    })
}

impl Pool {
    fn submit(&'static self, job: Job) {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.queue.push_back(job);
        if st.idle > 0 {
            self.wake.notify_one();
        } else if st.threads < MAX_THREADS {
            st.threads += 1;
            drop(st);
            let spawned = thread::Builder::new()
                .name("nexio-blocking".into())
                .spawn(move || self.worker());
            if spawned.is_err() {
                // Could not start a thread; an existing one will pick the job up,
                // or it stays queued until one frees up.
                self.state.lock().unwrap_or_else(|e| e.into_inner()).threads -= 1;
            }
        }
    }

    fn worker(&'static self) {
        loop {
            let job = {
                let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    if let Some(job) = st.queue.pop_front() {
                        break Some(job);
                    }
                    st.idle += 1;
                    let (guard, timeout) = self
                        .wake
                        .wait_timeout(st, IDLE_TIMEOUT)
                        .unwrap_or_else(|e| e.into_inner());
                    st = guard;
                    st.idle -= 1;
                    if timeout.timed_out() && st.queue.is_empty() {
                        st.threads -= 1;
                        break None;
                    }
                }
            };
            match job {
                Some(job) => job(),
                None => return,
            }
        }
    }
}

// ── the public future ────────────────────────────────────────────────────────

struct Shared<T> {
    result: Mutex<Option<thread::Result<T>>>,
}

/// A running blocking job. Await it for the result.
///
/// Dropping it does not cancel the job (a running closure cannot be stopped); the
/// job runs to completion and its result is discarded.
pub struct BlockingTask<T> {
    id: u64,
    shared: Arc<Shared<T>>,
}

impl<T> Unpin for BlockingTask<T> {}

impl<T> Future for BlockingTask<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Register first, then check: a result that arrives between the two is
        // seen by the check, and one that arrives later finds the waker.
        WAKERS.with(|w| w.borrow_mut().insert(self.id, cx.waker().clone()));
        let taken = self
            .shared
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        match taken {
            Some(res) => {
                WAKERS.with(|w| w.borrow_mut().remove(&self.id));
                Poll::Ready(res.map_err(|_| JoinError::Panicked))
            }
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for BlockingTask<T> {
    fn drop(&mut self) {
        let _ = WAKERS.try_with(|w| {
            if let Ok(mut w) = w.try_borrow_mut() {
                w.remove(&self.id);
            }
        });
    }
}

/// Run `f` on a pool thread and return a future for its result.
///
/// Use it for anything that blocks: a slow syscall, a CPU-heavy computation, a
/// library with no async interface. A panic in `f` is caught and reported as
/// [`JoinError::Panicked`]; the pool keeps working.
///
/// Must be called from inside [`block_on`](crate::block_on) (it needs the
/// reactor to deliver the result). The pool grows on demand up to 512 threads
/// and retires idle threads after 10 seconds.
pub fn spawn_blocking<F, R>(f: F) -> BlockingTask<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let shared = Arc::new(Shared {
        result: Mutex::new(None),
    });
    let mailbox = current_mailbox();
    let slot = shared.clone();
    pool().submit(Box::new(move || {
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        *slot.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(out);
        mailbox.complete(id);
    }));
    BlockingTask { id, shared }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block_on, spawn, time::sleep};
    use std::{sync::atomic::AtomicBool, time::Instant};

    #[test]
    fn returns_the_closures_result() {
        let v = block_on(async { spawn_blocking(|| 6 * 7).await.unwrap() });
        assert_eq!(v, 42);
    }

    #[test]
    fn runs_on_a_different_thread() {
        let main_id = thread::current().id();
        let job_id = block_on(async { spawn_blocking(|| thread::current().id()).await.unwrap() });
        assert_ne!(main_id, job_id);
    }

    #[test]
    fn jobs_run_in_parallel() {
        let t = Instant::now();
        block_on(async {
            let mut tasks = Vec::new();
            for _ in 0..100 {
                tasks.push(spawn_blocking(|| thread::sleep(Duration::from_millis(100))));
            }
            for t in tasks {
                t.await.unwrap();
            }
        });
        // Serially this would take 10 s.
        assert!(
            t.elapsed() < Duration::from_secs(3),
            "took {:?}",
            t.elapsed()
        );
    }

    #[test]
    fn a_panic_is_reported_and_the_pool_survives() {
        block_on(async {
            let err = spawn_blocking(|| -> u32 { panic!("boom") })
                .await
                .unwrap_err();
            assert!(err.is_panic());
            assert_eq!(spawn_blocking(|| 1u32).await.unwrap(), 1);
        });
    }

    #[test]
    fn a_dropped_task_does_not_hang_and_the_job_still_runs() {
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        block_on(async move {
            drop(spawn_blocking(move || {
                thread::sleep(Duration::from_millis(50));
                flag.store(true, Ordering::SeqCst);
            }));
            sleep(Duration::from_millis(200)).await;
        });
        assert!(ran.load(Ordering::SeqCst));
    }

    #[test]
    fn blocking_work_does_not_stall_other_tasks() {
        block_on(async {
            let blocker = spawn(async {
                spawn_blocking(|| thread::sleep(Duration::from_millis(300)))
                    .await
                    .unwrap()
            });
            let t = Instant::now();
            for _ in 0..5 {
                sleep(Duration::from_millis(20)).await;
            }
            // The reactor kept running timers while the pool thread slept.
            assert!(
                t.elapsed() < Duration::from_millis(250),
                "reactor stalled: {:?}",
                t.elapsed()
            );
            blocker.await.unwrap();
        });
    }

    #[test]
    fn many_sequential_jobs_never_lose_a_wakeup() {
        block_on(async {
            for i in 0..2000u32 {
                assert_eq!(spawn_blocking(move || i).await.unwrap(), i);
            }
        });
    }
}
