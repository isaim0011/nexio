//! Coverage-guided state-machine fuzzer for `CompletionSlab`.
//!
//! The fuzzer's bytes drive arbitrary interleavings of: inserting an op,
//! delivering CQEs (including stale, recycled-slot and completely random
//! `user_data`), dropping the "future" that owns an op, and consuming a
//! completed result. A small model tracks what must happen to each tracked
//! buffer, and the target asserts after every step that
//!
//!   * a buffer is never dropped before the event that releases it,
//!   * a buffer is never dropped twice,
//!   * `CompletionSlab::len` always equals the number of live ops, and
//!   * garbage or stale CQEs change nothing,
//!
//! and at the end that every buffer was dropped exactly once (no leak).
//!
//! Two op shapes are modelled because the runtime uses both: kind A keeps
//! its buffer in the future and moves it into the slab on drop (`ignore`);
//! kind B keeps it in the slab from the start (`ignore_simple`).

#![no_main]

use libfuzzer_sys::fuzz_target;
use nexio::reactor::slab::{CompletionSlab, ErasedBuf, OpState, PlaceholderBuf, SlabKey};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{RawWaker, RawWakerVTable, Waker},
};

struct Tracker(Arc<AtomicUsize>);
impl Drop for Tracker {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl ErasedBuf for Tracker {}

fn noop_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: every vtable function ignores its data pointer.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum St {
    Submitted,
    Complete,
    Ignored,
    Gone,
}

struct Op {
    key: SlabKey,
    st: St,
    kind_b: bool,
    drops: Arc<AtomicUsize>,
    /// Kind A only: the buffer while the future still owns it.
    held: Option<Tracker>,
    future_alive: bool,
    /// Whether the model says the buffer must have been dropped by now.
    expect_dropped: bool,
}

struct Cursor<'a>(&'a [u8], usize);
impl Cursor<'_> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.0.get(self.1)?;
        self.1 += 1;
        Some(b)
    }
    fn u64(&mut self) -> Option<u64> {
        let mut v = 0u64;
        for _ in 0..8 {
            v = (v << 8) | self.byte()? as u64;
        }
        Some(v)
    }
}

fn check(ops: &[Op], slab: &CompletionSlab) {
    let live = ops.iter().filter(|o| o.st != St::Gone).count();
    assert_eq!(slab.len(), live, "slab length disagrees with the model");
    for o in ops {
        let d = o.drops.load(Ordering::SeqCst);
        assert!(d <= 1, "buffer dropped {d} times (double free)");
        assert_eq!(
            d, o.expect_dropped as usize,
            "buffer drop state wrong: st={:?} kind_b={} future_alive={}",
            o.st, o.kind_b, o.future_alive
        );
    }
}

fn deliver_cqe(ops: &mut [Op], slab: &mut CompletionSlab, raw: u64, result: i32) {
    let k = SlabKey::from_u64(raw);
    let hit = ops.iter().position(|o| {
        o.st != St::Gone && o.key.index() == k.index() && o.key.generation() == k.generation()
    });
    let _ = slab.complete(raw, result);
    if let Some(i) = hit {
        let o = &mut ops[i];
        match o.st {
            St::Submitted => {
                o.st = St::Complete;
                if o.kind_b {
                    o.expect_dropped = true; // buffer lived in the slab's Submitted entry
                }
            }
            St::Ignored => {
                o.st = St::Gone;
                o.expect_dropped = true; // real CQE finally releases it
            }
            St::Complete | St::Gone => {}
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let mut c = Cursor(data, 0);
    let mut slab = CompletionSlab::new();
    let mut ops: Vec<Op> = Vec::new();
    let waker = noop_waker();

    while let Some(action) = c.byte() {
        match action % 8 {
            // insert
            0 | 1 => {
                if ops.len() >= 64 {
                    continue;
                }
                let kind_b = action & 0x10 != 0;
                let drops = Arc::new(AtomicUsize::new(0));
                let (buf, held): (Box<dyn ErasedBuf>, Option<Tracker>) = if kind_b {
                    (Box::new(Tracker(drops.clone())), None)
                } else {
                    (Box::new(PlaceholderBuf), Some(Tracker(drops.clone())))
                };
                let key = slab.insert(OpState::Submitted { waker: None, buf: Some(buf) });
                ops.push(Op {
                    key,
                    st: St::Submitted,
                    kind_b,
                    drops,
                    held,
                    future_alive: true,
                    expect_dropped: false,
                });
            }
            // CQE for a known op (possibly already finished: duplicate or stale)
            2 | 3 => {
                let (Some(i), Some(r)) = (c.byte(), c.byte()) else {
                    break;
                };
                if ops.is_empty() {
                    continue;
                }
                let raw = ops[i as usize % ops.len()].key.as_u64();
                deliver_cqe(&mut ops, &mut slab, raw, r as i8 as i32);
            }
            // CQE with arbitrary user_data
            4 => {
                let (Some(raw), Some(r)) = (c.u64(), c.byte()) else {
                    break;
                };
                deliver_cqe(&mut ops, &mut slab, raw, r as i8 as i32);
            }
            // the owning future is dropped
            5 => {
                let Some(i) = c.byte() else { break };
                if ops.is_empty() {
                    continue;
                }
                let n = ops.len();
                let o = &mut ops[i as usize % n];
                if !o.future_alive {
                    continue;
                }
                o.future_alive = false;
                if o.kind_b {
                    let cancel = slab.ignore_simple(o.key);
                    assert_eq!(cancel, o.st == St::Submitted, "ignore_simple return value");
                    match o.st {
                        St::Submitted => o.st = St::Ignored,
                        St::Complete => o.st = St::Gone, // buffer was dropped at completion already
                        _ => {}
                    }
                } else {
                    let tracker = o.held.take().expect("kind A future holds the buffer");
                    let cancel = slab.ignore(o.key, Box::new(tracker));
                    assert_eq!(cancel, o.st == St::Submitted, "ignore return value");
                    match o.st {
                        St::Submitted => o.st = St::Ignored, // moved into the slab, still alive
                        St::Complete => {
                            o.st = St::Gone;
                            o.expect_dropped = true; // dropped immediately
                        }
                        _ => {}
                    }
                }
            }
            // the future polls and consumes a finished result
            6 => {
                let Some(i) = c.byte() else { break };
                if ops.is_empty() {
                    continue;
                }
                let n = ops.len();
                let o = &mut ops[i as usize % n];
                if !o.future_alive {
                    continue;
                }
                let got = slab.poll_and_consume(o.key, &waker);
                assert_eq!(
                    got.is_some(),
                    o.st == St::Complete,
                    "poll_and_consume result"
                );
                if o.st == St::Complete {
                    o.st = St::Gone;
                    o.future_alive = false;
                    if !o.kind_b {
                        o.held = None; // the future drops its buffer when it finishes
                        o.expect_dropped = true;
                    }
                }
            }
            // read-only poll: never changes anything
            _ => {
                let Some(i) = c.byte() else { break };
                if ops.is_empty() {
                    continue;
                }
                let n = ops.len();
                let o = &ops[i as usize % n];
                if o.future_alive {
                    let got = slab.poll_result(o.key, &waker);
                    assert_eq!(got.is_some(), o.st == St::Complete, "poll_result");
                }
            }
        }
        check(&ops, &slab);
    }

    // Teardown: nothing may leak and nothing may be freed twice.
    drop(slab);
    for o in &mut ops {
        o.held = None;
        assert_eq!(
            o.drops.load(Ordering::SeqCst),
            1,
            "buffer leaked or double-dropped at teardown"
        );
    }
});
