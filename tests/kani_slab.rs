//! Kani bounded model-checking harnesses for CompletionSlab.
//!
//! These harnesses use `kani::any()` to generate adversarial CQE orderings and
//! indices — the same approach AWS used for Firecracker's VirtIO queue parser,
//! which found 5 real bugs including a quota-bypass rounding error.
//!
//! Run with:  cargo kani --tests --harness <name>
//!
//! Each harness is guarded by `#[cfg(kani)]` so it compiles only when Kani
//! is the driver; they're invisible to `cargo test` and don't affect CI
//! unless Kani is explicitly invoked.
//!
//! Target invariants:
//!
//! 1. `SlabKey` packing/unpacking is bijective across all valid u32 indices and generations.
//! 2. `complete()` with an out-of-range index never panics.
//! 3. `complete()` with a generation mismatch never accesses freed memory.
//! 4. A slot visited by `ignore` then `complete` drops its buffer exactly once.
//! 5. No slab slot is freed more than once regardless of CQE ordering.
//! 6. `Complete -> ignore` frees the buffer and slot immediately.
//! 7. `poll_and_consume` frees the slot and bumps generation on `Complete`.

#![cfg(nexio_uring)]
#![cfg_attr(not(kani), allow(unexpected_cfgs))]

#[cfg(kani)]
mod kani_harnesses {
    use nexio::reactor::slab::{
        CloseOnDropFd, CompletionSlab, ErasedBuf, OpState, PlaceholderBuf, SlabKey,
    };

    // ── Harness 1: SlabKey index and generation bit arithmetic ───────────

    #[kani::proof]
    fn harness_key_roundtrip() {
        let index: u32 = kani::any();
        let gen: u32 = kani::any();

        let raw = ((gen as u64) << 32) | (index as u64);
        let key = SlabKey::from_u64(raw);

        kani::assert(key.index() == index as usize, "index bit extraction failed");
        kani::assert(key.generation() == gen, "generation bit extraction failed");
        kani::assert(key.as_u64() == raw, "raw roundtrip failed");
    }

    #[kani::proof]
    fn harness_key_bits_disjoint() {
        let raw: u64 = kani::any();
        let key = SlabKey::from_u64(raw);

        let recovered_index = (key.index() & 0xFFFF_FFFF) as u64;
        let recovered_gen = (key.generation() as u64) << 32;

        kani::assert((recovered_index & recovered_gen) == 0, "bits overlap");
        kani::assert(
            (recovered_index | recovered_gen) == raw,
            "bits reconstruction mismatch",
        );
    }

    // ── Harness 2: out-of-range user_data never panics ────────────────────

    #[kani::proof]
    #[kani::unwind(6)]
    fn harness_oob_index_no_panic() {
        let mut slab = CompletionSlab::with_capacity(4);

        let _key = slab.insert(OpState::Submitted {
            waker: None,
            buf: None,
        });

        let adversarial: u64 = kani::any();
        let _waker = slab.complete(adversarial, kani::any());
    }

    // ── Harness 3: generation mismatch is always discarded ────────────────

    #[kani::proof]
    #[kani::unwind(6)]
    fn harness_generation_mismatch_discarded() {
        let mut slab = CompletionSlab::with_capacity(4);

        let key = slab.insert(OpState::Submitted {
            waker: None,
            buf: None,
        });

        let index = key.index() as u64;
        let wrong_gen: u32 = kani::any();
        kani::assume(wrong_gen != key.generation());

        let stale_user_data = ((wrong_gen as u64) << 32) | index;
        let waker = slab.complete(stale_user_data, 0);
        kani::assert(waker.is_none(), "stale CQE returned a waker");

        let normal_waker = slab.complete(key.as_u64(), 42);
        kani::assert(
            normal_waker.is_none(),
            "slot was prematurely consumed by stale CQE",
        );
    }

    // ── Harness 4: ignore → complete frees slot exactly once ─────────────

    struct KaniDropCounter {
        count: *mut u32,
    }
    unsafe impl Send for KaniDropCounter {}
    impl ErasedBuf for KaniDropCounter {}
    impl Drop for KaniDropCounter {
        fn drop(&mut self) {
            unsafe { *self.count += 1 };
        }
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn harness_ignore_then_complete_drops_once() {
        let mut drop_count: u32 = 0;
        let counter = KaniDropCounter {
            count: &mut drop_count,
        };

        let mut slab = CompletionSlab::with_capacity(4);
        let key = slab.insert(OpState::Submitted {
            waker: None,
            buf: None,
        });

        let still_in_flight = slab.ignore(key, Box::new(counter));
        kani::assert(still_in_flight, "submitted op must remain in flight");
        kani::assert(drop_count == 0, "buf dropped at ignore time");

        let _ = slab.complete(key.as_u64(), 0);
        kani::assert(drop_count == 1, "buf not dropped exactly once at CQE time");

        let second = slab.complete(key.as_u64(), 0);
        kani::assert(second.is_none(), "slot was not freed after completion");
    }

    // ── Harness 5: two CQEs for the same slot — no double-free ───────────

    #[kani::proof]
    #[kani::unwind(6)]
    fn harness_double_cqe_no_double_free() {
        let mut slab = CompletionSlab::with_capacity(4);
        let key = slab.insert(OpState::Submitted {
            waker: None,
            buf: None,
        });

        let _w1 = slab.complete(key.as_u64(), 0);
        slab.consume(key);

        let w2 = slab.complete(key.as_u64(), 0);
        kani::assert(w2.is_none(), "double CQE returned a waker");
    }

    // ── Harness 6: CQE before ignore drops buffer and frees slot immediately ─

    #[kani::proof]
    #[kani::unwind(6)]
    fn harness_complete_then_ignore_frees_immediately() {
        let mut drop_count: u32 = 0;
        let counter = KaniDropCounter {
            count: &mut drop_count,
        };

        let mut slab = CompletionSlab::with_capacity(4);
        let key = slab.insert(OpState::Submitted {
            waker: None,
            buf: None,
        });

        let res: i32 = kani::any();
        let _ = slab.complete(key.as_u64(), res);

        let still_in_flight = slab.ignore(key, Box::new(counter));
        kani::assert(
            !still_in_flight,
            "already-completed op must not stay in flight",
        );
        kani::assert(drop_count == 1, "buf must be dropped immediately");
        kani::assert(slab.is_empty(), "slab must be empty");
    }

    // ── Harness 7: poll_and_consume + slot recycling bumps generation ─────

    #[kani::proof]
    #[kani::unwind(6)]
    fn harness_poll_and_consume_bumps_generation() {
        let mut slab = CompletionSlab::with_capacity(4);
        let key1 = slab.insert(OpState::Submitted {
            waker: None,
            buf: None,
        });

        let waker = std::task::Waker::noop();
        kani::assert(
            slab.poll_and_consume(key1, waker).is_none(),
            "not yet complete",
        );

        let cqe_res: i32 = kani::any();
        let _ = slab.complete(key1.as_u64(), cqe_res);
        let polled = slab.poll_and_consume(key1, waker);
        kani::assert(polled == Some(cqe_res), "poll_and_consume mismatch");
        kani::assert(slab.is_empty(), "slot not freed");

        let key2 = slab.insert(OpState::Submitted {
            waker: None,
            buf: None,
        });
        kani::assert(key2.index() == key1.index(), "slot not reused");
        kani::assert(
            key2.generation() == key1.generation().wrapping_add(1),
            "gen not bumped",
        );

        // Stale CQE for key1 must not complete key2
        let _ = slab.complete(key1.as_u64(), 0);
        kani::assert(
            slab.poll_and_consume(key2, waker).is_none(),
            "stale CQE affected key2",
        );
    }

    // ── Harness 8: ignore_accept state transitions ────────────────────────

    #[kani::proof]
    #[kani::unwind(6)]
    fn harness_ignore_accept_cleans_slot() {
        let mut slab = CompletionSlab::with_capacity(4);
        let key = slab.insert(OpState::Submitted {
            waker: None,
            buf: Some(Box::new(CloseOnDropFd)),
        });

        let still_in_flight = slab.ignore_accept(key);
        kani::assert(still_in_flight, "submitted accept should stay in flight");

        // Negative result CQE does not close an invalid fd and frees slot
        let neg: i32 = kani::any();
        kani::assume(neg < 0);
        let _ = slab.complete(key.as_u64(), neg);
        kani::assert(slab.is_empty(), "ignored accept slot not freed");
    }
}
