//! Waking a task from a thread that is not the one running it.
//!
//! nexio's executors are thread-local, but the wider ecosystem is not: a `tokio::sync::mpsc`
//! sender, a `flume` channel or an `Arc<Notify>` on another thread will call the `Waker` of
//! a nexio task. Wakers must be callable from any thread, so every executor thread gets a small
//! id, baked into the ids of its tasks and wakers, and a mailbox registered under that id.
//!
//! A wake on the owning thread takes the fast local path (a thread-local compare, no atomics).
//! A wake from anywhere else looks up the owner's mailbox, queues the task id there and pokes
//! that thread's reactor awake; the owner drains the mailbox and wakes the task locally.
//! Waking a task whose executor is gone is a no-op, and a stale id can at worst wake an
//! unrelated task spuriously, which async code must tolerate anyway.

use std::{
    cell::{Cell, RefCell},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
};

use crate::blocking::Mailbox;

/// Bits of a task id that hold the owning executor's id.
pub(crate) const EXEC_BITS: u32 = 10;
/// Bits that hold the task's slot index (so up to ~4 million live tasks per executor).
pub(crate) const INDEX_BITS: u32 = 22;
const MAX_EXEC: u32 = (1 << EXEC_BITS) - 1;

/// A wake request that targets the root future of `block_on` rather than a spawned task.
pub(crate) const ROOT_WAKE: u64 = u64::MAX;

static REGISTRY: RwLock<Vec<Option<Weak<Mailbox>>>> = RwLock::new(Vec::new());
static FREE_IDS: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static NEXT_ID: AtomicU32 = AtomicU32::new(1);

/// Frees this thread's executor id when the thread ends.
struct IdGuard(u32);

impl Drop for IdGuard {
    fn drop(&mut self) {
        if let Ok(mut reg) = REGISTRY.write() {
            if let Some(slot) = reg.get_mut(self.0 as usize) {
                *slot = None;
            }
        }
        FREE_IDS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(self.0);
    }
}

thread_local! {
    static EXEC_ID: Cell<u32> = const { Cell::new(0) };
    static GUARD: RefCell<Option<IdGuard>> = const { RefCell::new(None) };
}

/// This thread's executor id, or 0 if it has never run an executor.
#[inline]
pub(crate) fn current() -> u32 {
    EXEC_ID.with(Cell::get)
}

/// Give this thread an executor id if it has none yet.
pub(crate) fn ensure_id() -> u32 {
    let existing = current();
    if existing != 0 {
        return existing;
    }
    let id = FREE_IDS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .pop()
        .unwrap_or_else(|| NEXT_ID.fetch_add(1, Ordering::Relaxed));
    assert!(
        id <= MAX_EXEC,
        "more than {MAX_EXEC} threads are running nexio executors at once"
    );
    EXEC_ID.with(|c| c.set(id));
    GUARD.with(|g| *g.borrow_mut() = Some(IdGuard(id)));
    id
}

/// Publish this thread's mailbox so other threads can wake its tasks.
pub(crate) fn register(mailbox: &Arc<Mailbox>) {
    let id = current() as usize;
    debug_assert!(id != 0, "register before ensure_id");
    let mut reg = REGISTRY.write().unwrap_or_else(|e| e.into_inner());
    if reg.len() <= id {
        reg.resize(id + 1, None);
    }
    reg[id] = Some(Arc::downgrade(mailbox));
}

/// Queue `raw` (a task id, or [`ROOT_WAKE`]) for the executor with id `exec`.
pub(crate) fn wake_remote(exec: u32, raw: u64) {
    let mailbox = {
        let reg = REGISTRY.read().unwrap_or_else(|e| e.into_inner());
        reg.get(exec as usize)
            .and_then(|s| s.as_ref())
            .and_then(Weak::upgrade)
    };
    if let Some(mailbox) = mailbox {
        mailbox.post_wake(raw);
    }
}
