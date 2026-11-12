//! Single-threaded `!Send` task executor.
//!
//! Provides a local async runtime driving an `io_uring` reactor via its public API:
//!
//! - [`block_on`]: Run a `!Send` future to completion on the current thread.
//! - [`spawn`]: Spawn a concurrent `!Send` task returning a [`JoinHandle`].
//! - [`yield_now`]: Cooperative yield to other ready tasks.

pub mod channel;
pub mod join_set;
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

use slab::Slab;

use crate::reactor::REACTOR;
use task::{Task, TaskShared, TaskState};
use waker::{RootWaker, TaskWaker};

struct TaskSlot {
    generation: u32,
    task: Option<Task>,
    queued: bool,
}

struct ExecutorInner {
    tasks: Slab<TaskSlot>,
    generations: Vec<u32>,
    ready_queue: VecDeque<(usize, u32)>,
}

impl ExecutorInner {
    fn new() -> Self {
        Self {
            tasks: Slab::with_capacity(256),
            generations: Vec::with_capacity(256),
            ready_queue: VecDeque::with_capacity(256),
        }
    }

    #[inline]
    fn pack_id(index: usize, generation: u32) -> TaskId {
        TaskId(((generation as u64) << 32) | (index as u64))
    }

    #[inline]
    fn unpack_id(id: TaskId) -> (usize, u32) {
        let raw = id.0;
        ((raw & 0xFFFF_FFFF) as usize, (raw >> 32) as u32)
    }

    fn insert_task(
        &mut self,
        waker_fn: impl FnOnce(TaskId) -> Waker,
        make_task: impl FnOnce(TaskId, Waker) -> Task,
    ) -> TaskId {
        let entry = self.tasks.vacant_entry();
        let index = entry.key();
        if index >= self.generations.len() {
            self.generations.resize(index + 1, 0);
        }
        let generation = self.generations[index];
        let id = Self::pack_id(index, generation);
        let waker = waker_fn(id);
        let task = make_task(id, waker);
        entry.insert(TaskSlot {
            generation,
            task: Some(task),
            queued: true,
        });
        self.ready_queue.push_back((index, generation));
        id
    }

    fn remove_slot(&mut self, index: usize) {
        if self.tasks.contains(index) {
            self.generations[index] = self.generations[index].wrapping_add(1);
            self.tasks.remove(index);
        }
    }

    fn clear(&mut self) {
        self.tasks.clear();
        self.ready_queue.clear();
    }
}

thread_local! {
    static EXECUTOR: RefCell<ExecutorInner> = RefCell::new(ExecutorInner::new());
    static IS_RUNNING: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn wake_task(id: TaskId) {
    let (index, gen) = ExecutorInner::unpack_id(id);
    EXECUTOR.with(|exec| {
        let mut inner = exec.borrow_mut();
        let should_queue = if let Some(slot) = inner.tasks.get_mut(index) {
            if slot.generation == gen && !slot.queued {
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

pub(crate) fn cancel_task(id: TaskId) {
    let (index, gen) = ExecutorInner::unpack_id(id);
    EXECUTOR.with(|exec| {
        let mut inner = exec.borrow_mut();
        if let Some(slot) = inner.tasks.get(index) {
            if slot.generation == gen {
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
    });
    let shared_task = Rc::clone(&shared);
    let is_cancelled = Rc::new(Cell::new(false));
    let is_cancelled_task = Rc::clone(&is_cancelled);

    let wrapped = async move {
        if is_cancelled_task.get() {
            return;
        }

        let output = future.await;

        if is_cancelled_task.get() {
            return;
        }

        // If JoinHandle was already dropped, drop the output immediately.
        if Rc::strong_count(&shared_task) == 1 {
            drop(output);
            return;
        }

        let mut state = shared_task.state.borrow_mut();
        if let TaskState::Running { join_waker } = &mut *state {
            let waker = join_waker.take();
            *state = TaskState::Completed(output);
            if let Some(w) = waker {
                w.wake();
            }
        }
    };

    let shared_for_panic = Rc::clone(&shared);
    let on_panic = Box::new(move || {
        let mut state = shared_for_panic.state.borrow_mut();
        if let TaskState::Running { join_waker } = &mut *state {
            let waker = join_waker.take();
            *state = TaskState::Panicked;
            if let Some(w) = waker {
                w.wake();
            }
        }
    });

    let boxed = Box::pin(wrapped);
    let cancelled_clone = Rc::clone(&is_cancelled);
    let task_id = EXECUTOR.with(|exec| {
        exec.borrow_mut().insert_task(TaskWaker::into_waker, |id, waker| {
            Task::new(id, waker, cancelled_clone, boxed, on_panic)
        })
    });

    JoinHandle {
        task_id,
        shared,
        is_cancelled,
    }
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
    });

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
            let max_steps = batch_size.max(64);

            for _ in 0..max_steps {
                let popped = EXECUTOR.with(|exec| {
                    let mut inner = exec.borrow_mut();
                    while let Some((index, queued_gen)) = inner.ready_queue.pop_front() {
                        if let Some(slot) = inner.tasks.get_mut(index) {
                            if slot.generation == queued_gen {
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

                if task.is_cancelled.get() {
                    EXECUTOR.with(|exec| {
                        let mut inner = exec.borrow_mut();
                        if let Some(slot) = inner.tasks.get(index) {
                            if slot.generation == gen {
                                inner.remove_slot(index);
                            }
                        }
                    });
                    continue;
                }

                let poll_res = task.poll_own_waker();

                EXECUTOR.with(|exec| {
                    let mut inner = exec.borrow_mut();
                    if let Some(slot) = inner.tasks.get_mut(index) {
                        if slot.generation == gen {
                            if poll_res == Poll::Pending && !task.is_cancelled.get() {
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
                    .submit_and_poll()
                    .expect("reactor submit_and_poll failed")
            });
            poll_root = RootWaker::is_woken();
        } else {
            REACTOR.with(|r| {
                r.borrow_mut().park().expect("reactor park failed")
            });
            poll_root = RootWaker::is_woken();
        }
    }
}
