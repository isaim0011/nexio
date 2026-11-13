//! Proper wakers for the single-threaded executor.
//!
//! [`TaskWaker`] encodes [`TaskId`] into a zero-allocation, zero-atomic [`RawWaker`]
//! on 64-bit targets (falling back to safe `Arc` on 32-bit targets).
//! [`RootWaker`] uses [`AtomicBool`] via [`std::task::Wake`].

use std::task::Waker;

use super::task::TaskId;

/// Zero-allocation, zero-atomic waker for spawned `!Send` tasks.
pub(crate) struct TaskWaker;

#[cfg(target_pointer_width = "64")]
mod raw_task_waker {
    use super::TaskId;
    use std::ptr::NonNull;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    #[inline]
    fn encode(task_id: TaskId) -> *const () {
        NonNull::<u8>::dangling()
            .as_ptr()
            .wrapping_add(task_id.0 as usize)
            .cast::<()>()
    }

    #[inline]
    fn decode(data: *const ()) -> TaskId {
        let base = NonNull::<u8>::dangling().as_ptr() as usize;
        TaskId((data as usize).wrapping_sub(base) as u64)
    }

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        RawWaker::new(data, &VTABLE)
    }

    unsafe fn wake_waker(data: *const ()) {
        super::super::wake_task(decode(data));
    }

    unsafe fn wake_by_ref_waker(data: *const ()) {
        super::super::wake_task(decode(data));
    }

    unsafe fn drop_waker(_data: *const ()) {}

    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref_waker, drop_waker);

    #[inline]
    pub(super) fn into_waker(task_id: TaskId) -> Waker {
        // SAFETY: VTABLE functions uphold RawWaker contract and never dereference `data`.
        unsafe { Waker::from_raw(RawWaker::new(encode(task_id), &VTABLE)) }
    }
}

#[cfg(not(target_pointer_width = "64"))]
mod raw_task_waker {
    use super::TaskId;
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    struct ArcTaskWaker(TaskId);

    impl Wake for ArcTaskWaker {
        fn wake(self: Arc<Self>) {
            super::super::wake_task(self.0);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            super::super::wake_task(self.0);
        }
    }

    pub(super) fn into_waker(task_id: TaskId) -> Waker {
        Waker::from(Arc::new(ArcTaskWaker(task_id)))
    }
}

impl TaskWaker {
    #[inline]
    pub(crate) fn into_waker(task_id: TaskId) -> Waker {
        raw_task_waker::into_waker(task_id)
    }
}

/// Waker for the root future executed by `block_on`.
///
/// Uses a thread-local flag with a zero-allocation, zero-atomic [`RawWaker`]
/// on the reactor thread.
pub(crate) struct RootWaker;

thread_local! {
    static ROOT_WOKEN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

mod raw_root_waker {
    use super::ROOT_WOKEN;
    use std::ptr::NonNull;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    unsafe fn clone_root(data: *const ()) -> RawWaker {
        RawWaker::new(data, &VTABLE)
    }

    /// The waker's data pointer encodes the executor id that owns the root future. A wake on
    /// that thread sets the flag directly; from any other thread it is queued for the owner.
    unsafe fn wake_root(data: *const ()) {
        let base = NonNull::<u8>::dangling().as_ptr() as usize;
        let exec = (data as usize).wrapping_sub(base) as u32;
        if exec == crate::executor::remote::current() {
            ROOT_WOKEN.with(|w| w.set(true));
        } else {
            crate::executor::remote::wake_remote(exec, crate::executor::remote::ROOT_WAKE);
        }
    }

    unsafe fn drop_root(_data: *const ()) {}

    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_root, wake_root, wake_root, drop_root);

    #[inline]
    pub(super) fn into_waker() -> Waker {
        // SAFETY: VTABLE functions uphold RawWaker contract and never dereference `data`.
        let exec = crate::executor::remote::current() as usize;
        unsafe {
            Waker::from_raw(RawWaker::new(
                NonNull::<u8>::dangling().as_ptr().wrapping_add(exec).cast(),
                &VTABLE,
            ))
        }
    }
}

impl RootWaker {
    #[inline]
    pub(crate) fn into_waker() -> Waker {
        raw_root_waker::into_waker()
    }

    #[inline]
    pub(crate) fn is_woken() -> bool {
        ROOT_WOKEN.with(|w| w.get())
    }

    /// Mark the root future woken. Called by the owning thread when it drains remote wakes.
    #[inline]
    pub(crate) fn mark_woken() {
        ROOT_WOKEN.with(|w| w.set(true));
    }

    #[inline]
    pub(crate) fn clear() {
        ROOT_WOKEN.with(|w| w.set(false));
    }
}
