//! Slab invariant and adversarial CQE tests.
//!
//! Invariants under test (from src/reactor/slab.rs):
//! 1. A `SlabKey` is live from `insert` until its matching CQE is processed.
//! 2. `Ignored` entries stay until their real CQE arrives, then buf is dropped.
//! 3. The slab index fits in a u32; high 32 bits carry a generation counter
//!    to detect stale CQEs from a recycled slot.
//!
//! Run with: cargo test --test slab_invariants
//! Run with Miri: cargo +nightly miri test --test slab_invariants

#![cfg(nexio_uring)]

use nexio::reactor::slab::{CompletionSlab, OpState, SlabKey};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Buffer tracker for drop count verification.
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

// ── Invariant 3: SlabKey bit packing / unpacking arithmetic ───────────────────

#[test]
fn test_slab_key_arithmetic() {
    let test_cases: &[(usize, u32)] = &[
        (0, 0),
        (0, 1),
        (1, 0),
        (1, 1),
        (42, 99),
        (65535, 65535),
        (u32::MAX as usize, 0),
        (0, u32::MAX),
        (u32::MAX as usize, u32::MAX),
        (123456789, 987654321),
    ];

    for &(idx, gen) in test_cases {
        let raw = ((gen as u64) << 32) | (idx as u64);
        let key = SlabKey::from_u64(raw);
        assert_eq!(key.index(), idx, "Index mismatch for idx={idx}, gen={gen}");
        assert_eq!(
            key.generation(),
            gen,
            "Generation mismatch for idx={idx}, gen={gen}"
        );
        assert_eq!(key.as_u64(), raw, "Raw u64 roundtrip mismatch");
    }
}

// ── Invariant 3: Recycled slot bumps generation & discards stale CQE ──────────

#[test]
fn test_recycled_slot_stale_cqe_discarded() {
    let mut slab = CompletionSlab::new();

    // 1. Insert first op
    let key1 = slab.insert(OpState::Submitted {
        waker: None,
        buf: None,
    });
    assert_eq!(key1.index(), 0);
    assert_eq!(key1.generation(), 0);

    // Complete and consume first op -> slot 0 is freed and generation increments
    let _ = slab.complete(key1.as_u64(), 100);
    slab.consume(key1);

    // 2. Insert second op -> Slab reuses index 0 with bumped generation
    let key2 = slab.insert(OpState::Submitted {
        waker: None,
        buf: None,
    });
    assert_eq!(key2.index(), 0);
    assert_eq!(
        key2.generation(),
        1,
        "Recycled slot must bump generation counter"
    );
    assert_ne!(key1.as_u64(), key2.as_u64());

    // A stale CQE from op 1 arrives now: must be silently discarded!
    let waker = slab.complete(key1.as_u64(), 999);
    assert!(waker.is_none(), "Stale CQE must be discarded");

    // Verify key2 is still Submitted and completes cleanly with its real key
    let waker_noop = std::task::Waker::noop();
    assert!(slab.poll_result(key2, waker_noop).is_none());
    let _ = slab.complete(key2.as_u64(), 42);
    assert_eq!(slab.poll_result(key2, waker_noop), Some(42));
    slab.consume(key2);
}

// ── Adversarial CQE: Out of bounds and vacant slot indices ────────────────────

#[test]
fn test_adversarial_out_of_bounds_cqes() {
    let mut slab = CompletionSlab::new();

    // Empty slab: arbitrary user_data should return None without panicking
    let adversarial_keys: &[u64] = &[
        0,
        1,
        255,
        256,
        1000,
        1_000_000,
        u32::MAX as u64,
        (1u64 << 32),
        (u32::MAX as u64) << 32,
        u64::MAX,
        u64::MAX - 1,
        0xDEAD_BEEF_CAFE_BABE,
    ];

    for &raw in adversarial_keys {
        let waker = slab.complete(raw, 0);
        assert!(
            waker.is_none(),
            "Expected None for adversarial key {raw:#x}"
        );
    }

    // Insert one valid op
    let valid_key = slab.insert(OpState::Submitted {
        waker: None,
        buf: None,
    });

    // Feed out-of-bounds keys with valid op present
    for &raw in adversarial_keys {
        if raw == valid_key.as_u64() {
            continue;
        }
        let waker = slab.complete(raw, -1);
        assert!(
            waker.is_none(),
            "Adversarial key should not affect valid op"
        );
    }

    // Valid op is still intact
    let w = slab.complete(valid_key.as_u64(), 42);
    assert!(w.is_none());
    slab.consume(valid_key);
}

// ── Adversarial CQE: Generation mismatch on occupied slot ─────────────────────

#[test]
fn test_adversarial_generation_mismatch() {
    let mut slab = CompletionSlab::new();

    let key = slab.insert(OpState::Submitted {
        waker: None,
        buf: None,
    });

    let index = key.index() as u64;

    // Test multiple wrong generations on the correct index
    for wrong_gen in [1u32, 2, 42, 1000, u32::MAX] {
        let stale_raw = ((wrong_gen as u64) << 32) | index;
        let w = slab.complete(stale_raw, 0);
        assert!(
            w.is_none(),
            "Stale generation {wrong_gen} should be discarded"
        );
    }

    // Original entry must still be valid and completeable
    let w = slab.complete(key.as_u64(), 10);
    assert!(w.is_none());
    slab.consume(key);
}

// ── Property-style Randomized Adversarial Fuzzing ─────────────────────────────

#[test]
fn test_adversarial_property_fuzz() {
    // Deterministic pseudo-random generator (LCG) for reproducible fuzzing
    struct SimpleRng(u64);
    impl SimpleRng {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            self.0
        }
        fn next_range(&mut self, max: usize) -> usize {
            (self.next_u64() % (max as u64)) as usize
        }
    }

    let mut rng = SimpleRng(0x1234_5678_9ABC_DEF0);
    let mut slab = CompletionSlab::new();

    struct ActiveOp {
        key: SlabKey,
        drop_count: Arc<AtomicUsize>,
        ignored: bool,
        completed: bool,
    }

    let mut ops: Vec<ActiveOp> = Vec::new();

    for _ in 0..2000 {
        let action = rng.next_range(5);
        match action {
            0 => {
                // Insert a new op
                let dc = Arc::new(AtomicUsize::new(0));
                let key = slab.insert(OpState::Submitted {
                    waker: None,
                    buf: None,
                });
                ops.push(ActiveOp {
                    key,
                    drop_count: dc,
                    ignored: false,
                    completed: false,
                });
            }
            1 => {
                // Ignore (cancel) an active submitted op
                if let Some(pos) = ops.iter().position(|op| !op.ignored && !op.completed) {
                    let op = &mut ops[pos];
                    let buf = TrackDrop {
                        data: vec![0u8; 8],
                        drop_count: Arc::clone(&op.drop_count),
                    };
                    slab.ignore(op.key, Box::new(buf));
                    op.ignored = true;
                    assert_eq!(op.drop_count.load(Ordering::SeqCst), 0);
                }
            }
            2 => {
                // Complete an active op
                if !ops.is_empty() {
                    let idx = rng.next_range(ops.len());
                    let op = &mut ops[idx];
                    if !op.completed {
                        let _ = slab.complete(op.key.as_u64(), 0);
                        op.completed = true;
                        if op.ignored {
                            // Ignored op: buffer should be dropped upon completion
                            assert_eq!(op.drop_count.load(Ordering::SeqCst), 1);
                        } else {
                            // Submitted op: consumed by future
                            slab.consume(op.key);
                        }
                    }
                }
            }
            3 => {
                // Send adversarial CQE with random user_data
                let raw = rng.next_u64();
                let res = (rng.next_u64() as i32) % 1024;
                let _ = slab.complete(raw, res);
            }
            4 => {
                // Send adversarial CQE with existing index but random generation
                if !ops.is_empty() {
                    let idx = rng.next_range(ops.len());
                    let fake_gen = (rng.next_u64() >> 32) as u32;
                    let target_key = ops[idx].key;
                    if fake_gen != target_key.generation() {
                        let fake_user_data =
                            ((fake_gen as u64) << 32) | (target_key.index() as u64);
                        let w = slab.complete(fake_user_data, 0);
                        assert!(w.is_none(), "Mismatched gen must return None");
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    // Clean up all remaining active ops
    for op in &mut ops {
        if !op.completed {
            let _ = slab.complete(op.key.as_u64(), 0);
            op.completed = true;
            if op.ignored {
                assert_eq!(op.drop_count.load(Ordering::SeqCst), 1);
            } else {
                slab.consume(op.key);
            }
        }
    }
}
