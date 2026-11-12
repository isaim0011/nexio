//! A set of tasks that are managed and joined dynamically.
//!
//! [`JoinSet`] allows spawning multiple tasks and awaiting their completion
//! in completion order. Dropping the set automatically cancels all remaining tasks.

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use slab::Slab;

use super::{spawn, JoinError, JoinHandle, TaskId};

struct JoinSetInner {
    completed_order: VecDeque<usize>,
    waker: Option<Waker>,
}

struct TaskGuard {
    key: usize,
    completed: Rc<Cell<bool>>,
    inner: Rc<RefCell<JoinSetInner>>,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if !self.completed.get() {
            let mut inner = self.inner.borrow_mut();
            inner.completed_order.push_back(self.key);
            if let Some(w) = inner.waker.take() {
                w.wake();
            }
        }
    }
}

/// A collection of concurrent tasks that can be awaited as they finish.
///
/// When a `JoinSet` is dropped, all remaining tasks are cancelled immediately,
/// dropping their futures and resources.
pub struct JoinSet<T: 'static> {
    tasks: Slab<JoinHandle<T>>,
    inner: Rc<RefCell<JoinSetInner>>,
}

impl<T: 'static> JoinSet<T> {
    /// Creates a new, empty [`JoinSet`].
    pub fn new() -> Self {
        Self {
            tasks: Slab::with_capacity(16),
            inner: Rc::new(RefCell::new(JoinSetInner {
                completed_order: VecDeque::with_capacity(16),
                waker: None,
            })),
        }
    }

    /// Returns the number of tasks currently managed by the set.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns `true` if the set contains no tasks.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Spawns a task onto the executor and adds it to the set.
    ///
    /// Returns the [`TaskId`] of the spawned task.
    pub fn spawn<F>(&mut self, future: F) -> TaskId
    where
        F: Future<Output = T> + 'static,
    {
        let entry = self.tasks.vacant_entry();
        let key = entry.key();

        let completed = Rc::new(Cell::new(false));
        let completed_clone = Rc::clone(&completed);
        let inner_clone = Rc::clone(&self.inner);

        let guard = TaskGuard {
            key,
            completed,
            inner: Rc::clone(&self.inner),
        };

        let handle = spawn(async move {
            let _guard = guard;
            let output = future.await;
            completed_clone.set(true);
            let mut inner = inner_clone.borrow_mut();
            inner.completed_order.push_back(key);
            if let Some(w) = inner.waker.take() {
                w.wake();
            }
            output
        });

        let task_id = handle.id();
        entry.insert(handle);
        task_id
    }

    /// Waits for the next task in the set to complete.
    ///
    /// Returns `Some(Ok(T))` if a task completes successfully, `Some(Err(JoinError))` if a
    /// task panicked or was cancelled, and `None` if the set is empty.
    ///
    /// Tasks are yielded in the order they complete, not the order they were spawned.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        struct JoinNext<'a, T: 'static> {
            set: &'a mut JoinSet<T>,
        }

        impl<'a, T: 'static> Future for JoinNext<'a, T> {
            type Output = Option<Result<T, JoinError>>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.set.poll_join_next(cx)
            }
        }

        JoinNext { set: self }.await
    }

    /// Polls for the next completed task in the set.
    pub fn poll_join_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<T, JoinError>>> {
        if self.tasks.is_empty() {
            return Poll::Ready(None);
        }

        let mut inner = self.inner.borrow_mut();
        while let Some(key) = inner.completed_order.pop_front() {
            if self.tasks.contains(key) {
                let mut handle = self.tasks.remove(key);
                drop(inner);
                let pin_handle = Pin::new(&mut handle);
                match pin_handle.poll(cx) {
                    Poll::Ready(res) => return Poll::Ready(Some(res)),
                    Poll::Pending => {
                        unreachable!("completed task was not ready");
                    }
                }
            }
        }

        inner.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// Aborts all tasks in the set.
    ///
    /// Any remaining tasks will produce [`JoinError::Cancelled`] when joined.
    pub fn abort_all(&mut self) {
        for (_, handle) in self.tasks.iter() {
            handle.abort();
        }
    }
}

impl<T: 'static> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: 'static> Drop for JoinSet<T> {
    fn drop(&mut self) {
        self.abort_all();
    }
}

impl<T: 'static> std::fmt::Debug for JoinSet<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinSet")
            .field("len", &self.len())
            .finish()
    }
}
