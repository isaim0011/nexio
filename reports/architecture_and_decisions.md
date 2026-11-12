# nexio — Architecture, Optimization & Verification Decisions

This document records the engineering decisions, tradeoffs, verification findings, and measured performance outcomes behind `nexio`.

---

## 1. Core Soundness & Architecture Decisions

1. **Owned Buffers Only (`IoBuf` / `IoBufMut` / `BufResult<T, B>`)**
   - Under Linux `io_uring`, the kernel holds raw buffer pointers asynchronously from SQE submission until the matching CQE lands. Borrowing `&mut [u8]` across `.await` cannot prevent use-after-free or data races when a `Future` is dropped or forgotten.
   - Ownership transfer into `Op<B>` / `LazyOp<B, F>` (stored inside `ManuallyDrop<B>`) ensures the backing allocation cannot move or be freed while the kernel holds the pointer.

2. **Leak-Until-Real-Completion Cancellation (`OpState::Ignored` + `ASYNC_CANCEL`)**
   - `IORING_OP_ASYNC_CANCEL` is best-effort in Linux; the kernel can still complete the original operation after cancel submission.
   - Dropping an in-flight future transfers `B` into `CompletionSlab` under `OpState::Ignored { buf, aux }` and submits `ASYNC_CANCEL` (`user_data = u64::MAX`). Cleanup runs **only** when the real CQE arrives.
   - **Edge cases found and fixed during verification:**
     - *CQE arrives before `Drop`*: `CompletionSlab::ignore` immediately drops `buf` and frees the slot rather than transitioning to `Ignored` (which would leak permanently because no second CQE will arrive).
     - *Accepted fd before `Drop`*: `AcceptOp::drop` uses `ignore_accept` so any already-completed fd in `OpState::Complete { result }` is closed immediately via `libc::close`, and in-flight cancelled accepts close via `CloseOnDropFd::on_ignored_completion`.
     - *Duplicate CQEs*: `CompletionSlab::complete` safely ignores duplicate CQEs on `Complete` slots without panicking.

3. **Thread-Per-Core, `!Send` Tasks, `SO_REUSEPORT` Sharding**
   - One `Reactor` (`IoUring`, depth `1024`) and one single-threaded executor per core (`thread_local!`), eliminating cross-thread ring locks and work-stealing synchronization overhead.
   - Multi-core scaling uses `SO_REUSEPORT` + `SO_REUSEADDR` set *prior* to `bind()` (`TcpListener::bind_addr` / `UdpSocket::bind_addr`).
   - `Reactor::enter(&mut self, f)` enables running `block_on`, `net`, `fs`, and `time` on custom `ReactorBuilder` instances (custom queue depths or opt-in `SQPOLL`).

---

## 2. Verification-First Engineering (Miri + Kani + High-QD Tests)

| Layer | Tooling | Outcome |
|---|---|---|
| Pure-Rust Slab & State Machine | **Miri** (`-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check`) | `5/5` `slab_invariants` + `4/4` `cancel_on_drop` pure-Rust tests passed (`0 UB`, `0` leaks) |
| Slab Bit Packing, OOB CQEs, Lifecycle | **Kani `0.68.0` / CBMC `6.11.0` / CaDiCaL `3.0.0`** | `9/9` proof harnesses `VERIFICATION:- SUCCESSFUL` |
| Kernel Exhaustion & FD Leaks | **Integration Tests** (`cargo test`) | `76/76` passed (`62` unit + `6` `cancel_on_drop` with exact `/proc/self/fd` baseline equality + `3` `high_qd_exhaustion` forcing SQ-full on depth-4 ring & 200-op `-ECANCELED` race storms + `5` `slab_invariants`) |

**Key Kani Design Fix (`CombinedBuf` → `aux: Option<Box<dyn ErasedBuf>>`):**
Originally `CompletionSlab::ignore` combined `Submitted.buf` (e.g., `MsgHdrData`) and the user's `Op<B>` buffer in a `CombinedBuf` wrapper implementing `ErasedBuf`. Because `CombinedBuf` held `Box<dyn ErasedBuf>`, CBMC encountered recursive `drop_glue::<dyn ErasedBuf>` unwinding. Replacing `CombinedBuf` with `aux: Option<Box<dyn ErasedBuf>>` inside `OpState::Ignored` removed the extra heap allocation and made `drop_glue` strictly acyclic so all 9 Kani proofs solved cleanly.

---

## 3. Post-Optimization Decisions & Measured Impact

Measured on Oracle Cloud VPS (`2 vCPUs`, `AMD EPYC 9J14`, Linux `6.12.0-207.111.5.1.el9uek.x86_64`):

1. **`IORING_SETUP_COOP_TASKRUN` + `DEFER_TASKRUN` + `SINGLE_ISSUER` (with kernel fallback)**
   - Enabled by default in `ReactorBuilder` (falling back to `IoUring::new` on pre-6.1 kernels), paired with userspace CQE draining and single-syscall `park()`.
2. **Socket-Native `IORING_OP_RECV` / `IORING_OP_SEND`**
   - Switched `TcpStream::read` / `write` from `IORING_OP_READ` / `WRITE` to `IORING_OP_RECV` / `SEND`.
3. **O(1) Generational `Slab<TaskSlot>` Ready Queue + Zero-Alloc `RawWaker` (`TaskWaker` & `RootWaker`)**
   - Replaced `HashMap`/`HashSet` ready bookkeeping with `slab::Slab` + generation-tagged `VecDeque<(usize, u32)>`.
   - Replaced `Arc`-backed wakers with zero-allocation, zero-atomic `RawWaker`s (`NonNull::dangling()` offset encoding on 64-bit, passing Miri `-Zmiri-strict-provenance`) and `Task::poll_own_waker()` to eliminate per-poll `Waker::clone()`.
4. **Single-Borrow `LazyOp` + `read_exact`/`write_all` Fast Paths**
   - `LazyOp` submits the SQE and registers the task's `Waker` in a single `REACTOR.with` borrow on first poll (cutting `RefCell` + TLS overhead in half vs eager submit + initial poll).
   - `TcpStream::read_exact`, `TcpStream::write_all`, and `fs::File::read_exact_at` skip `Slice` wrapping when the operation completes in one call.

### Summary of Final Measured Standings (`reports/phase2_benchmark_results.md` §5)

- **UDP (`benches/udp.rs`)**: `nexio` ranked **#1 in 2 of 2** (`89,498 msg/s` ping-pong, `106,588 msg/s` 32-pair burst; `+35.7%` to `+43.5%` over `tokio`, `+8.5%` over `monoio` ping-pong).
- **Disk / File I/O (`benches/file_io.rs`)**: `nexio` ranked **#1** on `seq_read_64k` (`184,500 IOPS`, `5.7×` over `tokio`) and reached `999,307 IOPS` on `rand_read_4k_qd32` (`10.4×` over `tokio`, beating `compio`, `3.5%` behind `monoio` at `1,035,832`).
- **TCP Echo (`benches/echo.rs`)**: `nexio_sharded` ranked **#1 in throughput in 5 of 6** configurations (up to `92,329 msg/s` @ 100×1024 B and `88,605 msg/s` @ 500×1024 B) and **#1 in p50 latency in 4 of 6** (`compio` edged `100×128 B` by `0.9%`, while `tokio_sharded` kept tighter `p99` tail bounds).

---

## 4. Known Limits & Deferred Scope

- **Linux-only**: Windows IOCP and macOS kqueue fallback (Phase 3 cross-platform) is not implemented.
- **Multishot Accept / Recv & Provided Buffer Rings**: Deferred; opt-in `register_buffers` / `read_fixed` / `write_fixed`, `register_files`, and `SQPOLL` are implemented and unit-tested.
- **CI**: [.github/workflows/ci.yml](file:///c:/Users/Bimo/Desktop/nexio/.github/workflows/ci.yml) runs `cargo test --all-targets`, `cargo clippy --all-targets -- -D warnings`, Miri (`scripts/ci_miri.sh`), Kani (`tests/kani_slab.rs`), and the release benchmark smoke check.
