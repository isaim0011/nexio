//! Cancel-on-drop regression tests.
//!
//! These tests mirror Tokio's own `complete_ops_on_drop` / `cancel_on_drop`
//! pattern — the exact bug class that produced a double-free and a
//! use-after-free in Ringbahn (Without Boats, "Two Memory Bugs From Ringbahn").
//!
//! What we're asserting:
//!
//! 1. Dropping an in-flight Op does not double-free the buffer.
//! 2. Dropping an in-flight Op does not leak the buffer when CQE arrives.
//! 3. Investigating buffer leak when CQE arrives before Drop.
//! 4. Investigating socket fd leak when TcpListener::accept is dropped.
//! 5. Investigating socket fd leak when TcpStream::connect is dropped.

use nexio::reactor::slab::{CompletionSlab, ErasedBuf, OpState, PlaceholderBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// A buffer that records its own drop count. Under Miri, any double-drop
/// will be caught as a use-after-free. We additionally assert the counter
/// == 1 at the end of each test.
struct TrackDrop {
    #[allow(dead_code)]
    data: Vec<u8>,
    drop_count: Arc<AtomicUsize>,
}

impl Drop for TrackDrop {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}

unsafe impl nexio::buf::IoBufMut for TrackDrop {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }
    fn bytes_total(&self) -> usize {
        self.data.capacity()
    }
    unsafe fn set_init(&mut self, n: usize) {
        unsafe { self.data.set_len(n) }
    }
}

unsafe impl Send for TrackDrop {}

// ── Test 1: slab-level drop-during-flight (no double-free, freed on CQE) ───────

#[test]
fn slab_ignore_then_complete_drops_buf_once() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let buf = TrackDrop {
        data: vec![0u8; 64],
        drop_count: Arc::clone(&drop_count),
    };
    let mut slab = CompletionSlab::new();

    // 1. Insert as Submitted
    let key = slab.insert(OpState::Submitted {
        waker: None,
        buf: Box::new(PlaceholderBuf),
    });

    // 2. Future dropped: move real buf into slab as Ignored
    let boxed: Box<dyn ErasedBuf> = Box::new(buf);
    slab.ignore(key, boxed);

    // Buffer must NOT be dropped yet
    assert_eq!(
        drop_count.load(Ordering::SeqCst),
        0,
        "buf dropped too early"
    );

    // 3. CQE arrives
    let waker = slab.complete(key.as_u64(), 0);
    assert!(waker.is_none(), "Ignored op should return no waker");

    // Buffer MUST be dropped now
    assert_eq!(
        drop_count.load(Ordering::SeqCst),
        1,
        "buf not dropped on CQE"
    );

    // Slot is now freed (subsequent CQE for same key returns None)
    assert!(
        slab.complete(key.as_u64(), 0).is_none(),
        "slot not freed after CQE"
    );
}

// ── Test 2: stale CQE is safely discarded ─────────────────────────────────────

#[test]
fn slab_stale_cqe_is_discarded() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let buf = TrackDrop {
        data: vec![0u8; 64],
        drop_count: Arc::clone(&drop_count),
    };
    let mut slab = CompletionSlab::new();

    let key = slab.insert(OpState::Submitted {
        waker: None,
        buf: Box::new(PlaceholderBuf),
    });

    // Complete normally
    let _waker = slab.complete(key.as_u64(), 42);
    slab.consume(key);

    // Stale CQE with old user_data
    let waker2 = slab.complete(key.as_u64(), 0);
    assert!(waker2.is_none(), "stale CQE should return no waker");

    drop(buf);
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
}

// ── Test 3: buffer & slot freed immediately when CQE arrives BEFORE Drop ──────

#[test]
fn slab_drop_after_cqe_arrived_frees_buf_immediately() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let buf = TrackDrop {
        data: vec![0u8; 64],
        drop_count: Arc::clone(&drop_count),
    };
    let mut slab = CompletionSlab::new();

    let key = slab.insert(OpState::Submitted {
        waker: None,
        buf: Box::new(PlaceholderBuf),
    });

    // CQE arrives before Op is polled
    let _waker = slab.complete(key.as_u64(), 64);

    // Now the Op is dropped without being polled; Op::drop moves buf into slab.ignore.
    // Because the operation already completed, slab.ignore must drop buf and free the slot immediately.
    let boxed: Box<dyn ErasedBuf> = Box::new(buf);
    let still_in_flight = slab.ignore(key, boxed);
    assert!(
        !still_in_flight,
        "completed op should not need ASYNC_CANCEL"
    );
    assert_eq!(
        drop_count.load(Ordering::SeqCst),
        1,
        "buf must be dropped immediately when CQE arrived before drop"
    );
    assert!(slab.is_empty(), "slab slot must be freed immediately");
}

// ── Test 4: multiple concurrent in-flight drops (no double-free) ───────────────

#[test]
fn slab_concurrent_in_flight_drops_no_double_free() {
    let mut drop_counts = Vec::new();
    let mut keys = Vec::new();
    let mut slab = CompletionSlab::new();

    for _ in 0..32 {
        let dc = Arc::new(AtomicUsize::new(0));
        let buf = TrackDrop {
            data: vec![0u8; 16],
            drop_count: Arc::clone(&dc),
        };
        drop_counts.push(dc);

        let key = slab.insert(OpState::Submitted {
            waker: None,
            buf: Box::new(PlaceholderBuf),
        });
        keys.push(key);

        let boxed: Box<dyn ErasedBuf> = Box::new(buf);
        slab.ignore(key, boxed);
    }

    // None dropped yet
    for dc in &drop_counts {
        assert_eq!(dc.load(Ordering::SeqCst), 0);
    }

    // Deliver CQEs in permuted order (odd then even)
    for i in (1..32).step_by(2) {
        slab.complete(keys[i].as_u64(), 0);
    }
    for i in (0..32).step_by(2) {
        slab.complete(keys[i].as_u64(), 0);
    }

    // Every buffer must be dropped exactly once
    for dc in &drop_counts {
        assert_eq!(dc.load(Ordering::SeqCst), 1);
    }
}

// ── Helper: count open file descriptors in /proc/self/fd ──────────────────────

#[cfg(not(miri))]
static FD_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(not(miri))]
fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("read /proc/self/fd")
        .count()
}

// ── Test 5: cancel-on-drop fd leak regression in TcpListener::accept ──────────

#[cfg(not(miri))]
#[test]
fn test_cancel_accept_fd_leak() {
    let _guard = FD_TEST_LOCK.lock().unwrap();
    let baseline = open_fd_count();

    std::thread::spawn(|| {
        nexio::block_on(async {
            let listener = nexio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            for _ in 0..50 {
                let accept_fut = listener.accept();
                drop(accept_fut);
            }

            // Drop an accept future *after* polling it (so SQE is in flight)
            // while a client concurrently connects and drops.
            let mut accept_fut = Box::pin(listener.accept());
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            let _ = std::future::Future::poll(accept_fut.as_mut(), &mut cx);
            drop(accept_fut);

            let _ = std::net::TcpStream::connect(addr);
            nexio::time::sleep(std::time::Duration::from_millis(25)).await;
        });
    })
    .join()
    .unwrap();

    let after = open_fd_count();
    assert_eq!(
        after, baseline,
        "fd leak detected after accept cancel-on-drop: baseline={baseline}, after={after}"
    );
}

// ── Test 6: cancel-on-drop fd leak regression in TcpStream::connect ─────────

#[cfg(not(miri))]
#[test]
fn test_cancel_connect_fd_leak() {
    let _guard = FD_TEST_LOCK.lock().unwrap();
    let baseline = open_fd_count();

    std::thread::spawn(|| {
        nexio::block_on(async {
            use std::future::Future;
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);

            for _ in 0..50 {
                let mut connect_fut = Box::pin(nexio::net::TcpStream::connect("127.0.0.1:1"));
                let _ = connect_fut.as_mut().poll(&mut cx);
                drop(connect_fut);
            }

            nexio::time::sleep(std::time::Duration::from_millis(25)).await;
        });
    })
    .join()
    .unwrap();

    let after = open_fd_count();
    assert_eq!(
        after, baseline,
        "fd leak detected after connect cancel-on-drop: baseline={baseline}, after={after}"
    );
}
