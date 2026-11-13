//! Single-threaded `!Send` task executor.
//!
//! Provides a local async runtime driving an `io_uring` reactor via its public API:
//!
//! - [`block_on`]: Run a `!Send` future to completion on the current thread.
//! - [`spawn`]: Spawn a concurrent `!Send` task returning a [`JoinHandle`].
//! - [`yield_now`]: Cooperative yield to other ready tasks.

pub mod channel;
pub mod join_set;
pub(crate) mod remote;
pub mod select;
pub mod task;
pub(crate) mod waker;

#[cfg(test)]
mod tests;

pub use channel::{mpsc, oneshot};
pub use join_set::JoinSet;
pub use select::{join2, select2, Either, Join2, Select2};
pub use task::{JoinError, JoinHandle, TaskId};

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    pin::{pin, Pin},
    rc::Rc,
    task::{Context, Poll, Waker},
};

use crate::reactor::REACTOR;
use task::{Task, TaskControl, TaskShared, TaskState};
use waker::{RootWaker, TaskWaker};

const NO_FREE: usize = usize::MAX;

struct Slot {
    generation: u32,
    occupied: bool,
    queued: bool,
    task: Option<Task>,
    next_free: usize,
}

struct ExecutorInner {
    slots: Vec<Slot>,
    free_head: usize,
    ready_queue: VecDeque<(usize, u32)>,
}

impl ExecutorInner {
    fn new() -> Self {
        let cap = crate::profile::HardwareProfile::get().executor_capacity();
        Self {
            slots: Vec::with_capacity(cap),
            free_head: NO_FREE,
            ready_queue: VecDeque::with_capacity(cap),
        }
    }

    /// Task id layout: generation (32 bits) | owning executor (10) | slot index (22).
    #[inline]
    fn pack_id(index: usize, generation: u32) -> TaskId {
        assert!(
            index < (1usize << remote::INDEX_BITS),
            "too many live tasks on one thread"
        );
        let exec = remote::current() as u64;
        TaskId(((generation as u64) << 32) | (exec << remote::INDEX_BITS) | (index as u64))
    }

    #[inline]
    fn unpack_id(id: TaskId) -> (u32, usize, u32) {
        let raw = id.0;
        let index = (raw & ((1 << remote::INDEX_BITS) - 1)) as usize;
        let exec = ((raw >> remote::INDEX_BITS) & ((1 << remote::EXEC_BITS) - 1)) as u32;
        (exec, index, (raw >> 32) as u32)
    }

    fn insert_task(
        &mut self,
        waker_fn: impl FnOnce(TaskId) -> Waker,
        make_task: impl FnOnce(TaskId, Waker) -> Task,
    ) -> TaskId {
        let (index, generation) = if self.free_head != NO_FREE {
            let idx = self.free_head;
            let slot = &mut self.slots[idx];
            self.free_head = slot.next_free;
            (idx, slot.generation)
        } else {
            let idx = self.slots.len();
            self.slots.push(Slot {
                generation: 0,
                occupied: false,
                queued: false,
                task: None,
                next_free: NO_FREE,
            });
            (idx, 0)
        };
        let id = Self::pack_id(index, generation);
        let waker = waker_fn(id);
        let task = make_task(id, waker);
        let slot = &mut self.slots[index];
        slot.occupied = true;
        slot.queued = true;
        slot.task = Some(task);
        slot.next_free = NO_FREE;
        self.ready_queue.push_back((index, generation));
        id
    }

    #[inline]
    fn remove_slot(&mut self, index: usize) {
        if let Some(slot) = self.slots.get_mut(index) {
            if slot.occupied {
                slot.occupied = false;
                slot.queued = false;
                slot.task = None;
                slot.generation = slot.generation.wrapping_add(1);
                slot.next_free = self.free_head;
                self.free_head = index;
            }
        }
    }

    fn clear(&mut self) {
        self.free_head = NO_FREE;
        for (index, slot) in self.slots.iter_mut().enumerate().rev() {
            if slot.occupied {
                slot.occupied = false;
                slot.queued = false;
                slot.task = None;
                slot.generation = slot.generation.wrapping_add(1);
            }
            slot.next_free = self.free_head;
            self.free_head = index;
        }
        self.ready_queue.clear();
    }
}

thread_local! {
    static EXECUTOR: RefCell<ExecutorInner> = RefCell::new(ExecutorInner::new());
    static IS_RUNNING: Cell<bool> = const { Cell::new(false) };
    /// Incremented every time `block_on` starts on this thread. Tasks spawned in one
    /// session are dropped when it ends, so anything that must keep a background task
    /// alive across sessions needs to know when a new one began.
    static SESSION: Cell<u64> = const { Cell::new(0) };
}

pub(crate) fn wake_task(id: TaskId) {
    let (exec, index, gen) = ExecutorInner::unpack_id(id);
    if exec != remote::current() {
        // A wake from another thread (a channel, a notifier): hand it to the owner.
        remote::wake_remote(exec, id.0);
        return;
    }
    EXECUTOR.with(|exec| {
        let mut inner = exec.borrow_mut();
        let should_queue = if let Some(slot) = inner.slots.get_mut(index) {
            if slot.occupied && slot.generation == gen && !slot.queued {
                slot.queued = true;
                true
            } else {
                false
            }
        } else {
            false
        };
        if should_queue {
            inner.ready_queue.push_back((index, gen));
        }
    });
}

/// Wake tasks (and possibly the root future) that other threads asked this thread to wake.
/// Runs on the executor's own thread, from its reactor.
pub(crate) fn deliver_remote_wakes(ids: Vec<u64>) {
    for raw in ids {
        if raw == remote::ROOT_WAKE {
            waker::RootWaker::mark_woken();
        } else {
            wake_task(TaskId(raw));
        }
    }
}

pub(crate) fn cancel_task(id: TaskId) {
    let (exec, index, gen) = ExecutorInner::unpack_id(id);
    if exec != remote::current() {
        return;
    }
    EXECUTOR.with(|exec| {
        let mut inner = exec.borrow_mut();
        if let Some(slot) = inner.slots.get(index) {
            if slot.occupied && slot.generation == gen {
                inner.remove_slot(index);
            }
        }
    });
}

/// Spawn a `!Send` task onto the current thread's executor.
///
/// Returns a [`JoinHandle`] that can be used to await the task's completion or cancel it.
/// If the handle is dropped, the task continues running detached in the background.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
{
    let shared = Rc::new(TaskShared {
        state: RefCell::new(TaskState::Running { join_waker: None }),
        is_cancelled: Cell::new(false),
    });
    let shared_task = Rc::clone(&shared);

    let wrapped = async move {
        if shared_task.is_cancelled.get() {
            return;
        }

        let output = future.await;

        if shared_task.is_cancelled.get() {
            return;
        }

        // If JoinHandle was already dropped, drop the output immediately.
        // Note: `Task` holds one `Rc<dyn TaskControl>` clone and `wrapped` holds `shared_task`,
        // so `strong_count == 2` means the caller dropped `JoinHandle`.
        if Rc::strong_count(&shared_task) == 2 {
            drop(output);
            return;
        }

        let mut state = shared_task.state.borrow_mut();
        if let TaskState::Running { join_waker } = &mut *state {
            let waker = join_waker.take();
            *state = TaskState::Completed(output);
            drop(state);
            if let Some(w) = waker {
                w.wake();
            }
        }
    };

    let control: Rc<dyn TaskControl> = Rc::clone(&shared) as Rc<dyn TaskControl>;
    let boxed = Box::pin(wrapped);
    let task_id = EXECUTOR.with(|exec| {
        exec.borrow_mut()
            .insert_task(TaskWaker::into_waker, |id, waker| {
                Task::new(id, waker, control, boxed)
            })
    });

    JoinHandle { task_id, shared }
}

/// Cooperatively yields execution back to the executor, rescheduling the current task
/// at the end of the ready queue.
pub async fn yield_now() {
    struct YieldNow(bool);
    impl Future for YieldNow {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    YieldNow(false).await
}

/// Run `future` to completion on the current thread, driving the io_uring reactor.
///
/// Runs concurrent tasks spawned with [`spawn`] and dispatches I/O completions
/// via the reactor.
///
/// # Panics
/// Panics if called recursively on the same thread.
pub fn block_on<F: Future>(future: F) -> F::Output {
    IS_RUNNING.with(|running| {
        if running.get() {
            panic!("cannot call block_on recursively on the same thread");
        }
        running.set(true);
        SESSION.with(|s| s.set(s.get() + 1));
    });
    // Other threads may wake our tasks: give this thread an executor id and publish its mailbox
    // (the id must exist before the first task, including the mailbox's own helper, is spawned).
    remote::ensure_id();
    remote::register(&crate::blocking::current_mailbox());

    struct ResetGuard;
    impl Drop for ResetGuard {
        fn drop(&mut self) {
            IS_RUNNING.with(|r| r.set(false));
            EXECUTOR.with(|exec| exec.borrow_mut().clear());
        }
    }
    let _guard = ResetGuard;

    let mut future = pin!(future);
    let waker = RootWaker::into_waker();
    let mut cx = Context::from_waker(&waker);
    let mut poll_root = true;
    let mut wakers: Vec<Waker> = Vec::with_capacity(64);

    loop {
        // 1. Poll the root future on initial entry or when woken.
        if poll_root {
            RootWaker::clear();
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
        }

        // 2. Run ready tasks in FIFO order (skip queue work if empty).
        let mut has_ready_tasks = false;
        let batch_size = EXECUTOR.with(|exec| exec.borrow().ready_queue.len());
        if batch_size > 0 {
            let min_batch = crate::profile::HardwareProfile::get().executor_min_batch();
            let max_steps = batch_size.max(min_batch);

            for _ in 0..max_steps {
                let popped = EXECUTOR.with(|exec| {
                    let mut inner = exec.borrow_mut();
                    while let Some((index, queued_gen)) = inner.ready_queue.pop_front() {
                        if let Some(slot) = inner.slots.get_mut(index) {
                            if slot.occupied && slot.generation == queued_gen {
                                slot.queued = false;
                                if let Some(task) = slot.task.take() {
                                    return Some((index, slot.generation, task));
                                }
                            }
                        }
                    }
                    None
                });

                let (index, gen, mut task) = match popped {
                    None => break,
                    Some(t) => t,
                };

                if task.is_cancelled() {
                    EXECUTOR.with(|exec| {
                        let mut inner = exec.borrow_mut();
                        if let Some(slot) = inner.slots.get(index) {
                            if slot.occupied && slot.generation == gen {
                                inner.remove_slot(index);
                            }
                        }
                    });
                    continue;
                }

                let poll_res = task.poll_own_waker();

                EXECUTOR.with(|exec| {
                    let mut inner = exec.borrow_mut();
                    if let Some(slot) = inner.slots.get_mut(index) {
                        if slot.occupied && slot.generation == gen {
                            if poll_res == Poll::Pending && !task.is_cancelled() {
                                slot.task = Some(task);
                            } else {
                                inner.remove_slot(index);
                            }
                        }
                    }
                });
            }

            // 3. Check if running tasks woke the root future.
            if RootWaker::is_woken() {
                RootWaker::clear();
                if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                    return output;
                }
            }

            has_ready_tasks = EXECUTOR.with(|exec| !exec.borrow().ready_queue.is_empty());
        }

        // 4. If no tasks/root are ready, submit SQEs and park in a single syscall.
        if has_ready_tasks || RootWaker::is_woken() {
            REACTOR.with(|r| {
                r.borrow_mut()
                    .submit_and_poll_into(&mut wakers)
                    .expect("reactor submit_and_poll failed")
            });
            for w in wakers.drain(..) {
                w.wake();
            }
            poll_root = RootWaker::is_woken();
        } else {
            REACTOR.with(|r| {
                r.borrow_mut()
                    .park_into(&mut wakers)
                    .expect("reactor park failed")
            });
            for w in wakers.drain(..) {
                w.wake();
            }
            poll_root = RootWaker::is_woken();
        }
    }
}

/// The id of the `block_on` session currently (or most recently) running on this thread.
#[cfg(nexio_uring)]
pub(crate) fn session_id() -> u64 {
    SESSION.with(|s| s.get())
}
