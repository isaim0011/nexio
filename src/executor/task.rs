//! Task types, identifiers, and handle abstractions for the executor.

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

/// Unique identifier for an executor task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(pub(crate) u64);

impl TaskId {
    /// Return the numeric representation of this task ID.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskId({})", self.0)
    }
}

/// Error returned when awaiting a [`JoinHandle`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinError {
    /// The task was cancelled before it completed.
    Cancelled,
    /// The task panicked during its execution.
    Panicked,
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => write!(f, "task was cancelled"),
            Self::Panicked => write!(f, "task panicked"),
        }
    }
}

impl std::error::Error for JoinError {}

impl JoinError {
    /// Returns true if the task panicked.
    pub fn is_panic(&self) -> bool {
        matches!(self, Self::Panicked)
    }

    /// Returns true if the task was cancelled.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

pub(crate) enum TaskState<T> {
    Running { join_waker: Option<Waker> },
    Completed(T),
    Panicked,
    Cancelled,
    Consumed,
}

pub(crate) struct TaskShared<T> {
    pub(crate) state: RefCell<TaskState<T>>,
    pub(crate) is_cancelled: std::cell::Cell<bool>,
}

pub(crate) trait TaskControl {
    fn is_cancelled(&self) -> bool;
    fn mark_panicked(&self);
}

impl<T> TaskControl for TaskShared<T> {
    #[inline]
    fn is_cancelled(&self) -> bool {
        self.is_cancelled.get()
    }

    fn mark_panicked(&self) {
        let mut state = self.state.borrow_mut();
        if let TaskState::Running { join_waker } = &mut *state {
            let waker = join_waker.take();
            *state = TaskState::Panicked;
            drop(state);
            if let Some(w) = waker {
                w.wake();
            }
        }
    }
}

/// An owned permission to join on a spawned task.
///
/// Dropping a `JoinHandle` detaches the task; it will continue running to completion
/// in the background, and its output value will be dropped when it finishes.
#[must_use = "tasks are detached if the handle is dropped"]
pub struct JoinHandle<T> {
    pub(crate) task_id: TaskId,
    pub(crate) shared: Rc<TaskShared<T>>,
}

impl<T> JoinHandle<T> {
    /// Returns the [`TaskId`] of the task associated with this handle.
    pub fn id(&self) -> TaskId {
        self.task_id
    }

    /// Checks if the task has completed, panicked, or been cancelled.
    pub fn is_finished(&self) -> bool {
        let state = self.shared.state.borrow();
        !matches!(&*state, TaskState::Running { .. })
    }

    /// Cancels the task if it is still running.
    ///
    /// The task's future will be dropped immediately if it has not yet completed.
    pub fn cancel(&self) {
        self.abort();
    }

    /// Aborts the task. Alias for [`cancel`](Self::cancel).
    pub fn abort(&self) {
        self.shared.is_cancelled.set(true);
        let mut state = self.shared.state.borrow_mut();
        if let TaskState::Running { join_waker } = &mut *state {
            let w = join_waker.take();
            *state = TaskState::Cancelled;
            drop(state);
            if let Some(w) = w {
                w.wake();
            }
            super::cancel_task(self.task_id);
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.state.borrow_mut();
        match &mut *state {
            TaskState::Running { join_waker } => {
                *join_waker = Some(cx.waker().clone());
                Poll::Pending
            }
            TaskState::Completed(_) => {
                if let TaskState::Completed(val) =
                    std::mem::replace(&mut *state, TaskState::Consumed)
                {
                    Poll::Ready(Ok(val))
                } else {
                    unreachable!()
                }
            }
            TaskState::Panicked => Poll::Ready(Err(JoinError::Panicked)),
            TaskState::Cancelled => Poll::Ready(Err(JoinError::Cancelled)),
            TaskState::Consumed => Poll::Ready(Err(JoinError::Cancelled)),
        }
    }
}

pub(crate) struct Task {
    #[allow(dead_code)]
    pub(crate) id: TaskId,
    pub(crate) waker: Waker,
    control: Rc<dyn TaskControl>,
    future: Pin<Box<dyn Future<Output = ()> + 'static>>,
}

impl Task {
    pub(crate) fn new(
        id: TaskId,
        waker: Waker,
        control: Rc<dyn TaskControl>,
        future: Pin<Box<dyn Future<Output = ()> + 'static>>,
    ) -> Self {
        Self {
            id,
            waker,
            control,
            future,
        }
    }

    #[inline]
    pub(crate) fn is_cancelled(&self) -> bool {
        self.control.is_cancelled()
    }

    /// Poll the task using its own stored waker without cloning the `Waker`.
    #[inline]
    pub(crate) fn poll_own_waker(&mut self) -> Poll<()> {
        let mut cx = Context::from_waker(&self.waker);
        let future = self.future.as_mut();
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.poll(&mut cx)));
        match res {
            Ok(poll) => poll,
            Err(_panic_payload) => {
                self.control.mark_panicked();
                Poll::Ready(())
            }
        }
    }
}
