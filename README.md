# nexio

A verified, single-core io_uring runtime for Linux.

**Status: Phase 0, Phase 1 (Miri + Kani + high-QD tests), Phase 2, and Linux Phase 3 core complete.**

## Design in one paragraph

The kernel owns the buffer from SQE submission until the matching CQE arrives.
Dropped futures submit a best-effort `ASYNC_CANCEL` and park their buffer in a
reactor slab; the slab frees it when the real CQE lands — never before.
No borrowed `&mut [u8]`, no blocking `Drop`, no work-stealing.
One reactor per thread; `SO_REUSEPORT` sharding for multi-core.
Miri + Kani on the pure-Rust layer, load tests at high queue depth for the rest.

## Why this architecture

Built from a research pass over tokio-uring, compio, monoio, and glommio.
Two findings drove the design:

1. **Parity, not a win, at low core counts.** A tokio-rs/io-uring maintainer
   issue (#291) found a naive io_uring receiver losing to multi-threaded Tokio
   2x — until `SO_REUSEPORT` sharding was added, at which point they *converged
   exactly*. We don't chase a performance story the evidence doesn't support.

2. **The real differentiator is verification.** tokio-uring, compio, and monoio
   run zero Miri, zero Kani, zero loom in CI — despite Tokio's own (non-io_uring)
   core running both. AWS's Kani team applied the same technique to Firecracker's
   VirtIO queue parsing and found 5 real bugs. Nobody has done this for a Rust
   io_uring runtime. We will.

## Building

```
# Requires Linux, Rust stable ≥ 1.75, kernel ≥ 5.11
cargo build
cargo test
cargo test --test cancel_on_drop   # the most important test

# Miri (requires nightly):
bash scripts/ci_miri.sh

# Kani (requires kani toolchain):
cargo kani --tests

# Benchmark (5-way honest comparison — tokio unsharded / sharded / nexio / compio / monoio):
bash bench/run.sh --compare-modes --markdown
cargo bench --bench udp
cargo bench --bench file_io
```

## Reports & Decisions

- Architecture, optimization & verification decisions: [`reports/architecture_and_decisions.md`](reports/architecture_and_decisions.md)
- Benchmark suite results (TCP echo, UDP, Disk/File I/O): [`reports/phase2_benchmark_results.md`](reports/phase2_benchmark_results.md)
- Initial io_uring design research: [`reports/io_uring runtime design tradeoffs.md`](reports/io_uring%20runtime%20design%20tradeoffs.md)

## Roadmap

See [`ROADMAP.md`](ROADMAP.md).

## Crate structure

```
src/
  lib.rs          public API surface
  buf/
    mod.rs        BufResult<T,B>, IoBuf, IoBufMut, Slice
  reactor/
    mod.rs        Reactor: thread-local IoUring, submit/poll/park loop, fixed bufs/files
    builder.rs    ReactorBuilder (entries, coop_taskrun, opt-in sqpoll)
    slab.rs       CompletionSlab: generation-tagged, Miri + Kani verified
    op.rs         Op<B>, LazyOp<B,F>, AcceptOp, ConnectOp, SimpleOp: ManuallyDrop cancel safety
  executor/
    mod.rs        block_on + spawn + yield_now, single-threaded O(1) Slab queue
    task.rs       TaskId, JoinHandle, JoinError
    waker.rs      Zero-alloc RawWaker TaskWaker & RootWaker (strict-provenance clean)
    select.rs     select2, join2, Either
    join_set.rs   JoinSet<T>
    channel.rs    !Send oneshot + bounded/unbounded mpsc channels
  net/
    mod.rs        TcpListener, TcpStream
    udp.rs        UdpSocket
  fs/
    mod.rs        File, OpenOptions (openat, read_at, write_at, fsync, close)
  time/
    mod.rs        sleep, timeout, interval
tests/
  cancel_on_drop.rs      no double-free, no leaked fd — Miri target
  slab_invariants.rs     adversarial CQEs + 2,000-step fuzzer — Miri target
  high_qd_exhaustion.rs  SQ-full + -ECANCELED race storm + error paths
  kani_slab.rs           9 bounded model-checking harnesses (Kani)
benches/
  echo.rs                5-runtime TCP echo benchmark (Turbine methodology)
  udp.rs                 4-runtime UDP ping-pong & 32-pair burst benchmark
  file_io.rs             4-runtime disk/file I/O benchmark (seq/rand reads + fsync)
```

## What "beating" tokio/compio/monoio means here

In order:

1. **Correctness, verified** — Miri + Kani on the cancellation-safety surface
   none of the three incumbents currently run in CI.
2. **Honest performance numbers** — every benchmark reported with hardware,
   core count, and methodology. If nexio doesn't match sharded Tokio, we say so.
3. **Performance where the evidence supports it** — parity at 1–4 cores is the
   realistic v0 bar; real gains expected only at higher core counts and
   connection counts, where thread-per-core stops being overhead and starts
   being an advantage.
