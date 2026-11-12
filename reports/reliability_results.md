# Reliability results: soak, fuzzing, and kernel matrix

Date: 2026-11-12. Host: Oracle Cloud VM, 2 vCPU, Linux 6.12, rustc 1.99. Everything here was run, not
assumed. Raw commands are in the repository (`examples/soak.rs`, `fuzz/`, `scripts/kernel_matrix/`).

## 1. Soak test (`examples/soak.rs`)

One reactor hosts an echo server plus N client loops that, at random: complete a byte-verified
ping-pong; connect and drop immediately; cancel an in-flight read with a timeout (dropping the future
with its buffer still owned by the kernel); or write 64 KiB and vanish while the server is still
echoing. It samples open file descriptors and resident memory, and passes only if there are no echo
mismatches, no unexpected connect errors, and descriptors and memory return to their post-warmup
baseline after the load stops.

| Run | Duration | Clients | Verified ping-pongs | Cancelled in-flight reads | Abandoned writes | Mismatches | FDs under load | FDs after | RSS |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Host 6.12 | 600 s | 128 | 2,019,673 | 337,101 | 673,220 | 0 | 141-145 (flat) | 6 | 4,496 KiB (flat) |
| Host 6.12 | 20 s | 64 | 106,595 | 17,869 | 35,661 | 0 | 76-80 | 6 | 2.6 MB (flat) |
| Debian 5.10 guest | 20 s | 64 | 104,900 | 17,579 | 35,109 | 0 | ~80 | 7 | flat |
| Ubuntu 5.15 guest | 20 s | 64 | 50,609 | 8,470 | 17,011 | 0 | ~80 | 7 | flat |
| Debian 6.1 guest | 20 s | 64 | 73,234 | 12,160 | 24,514 | 0 | ~81 | 7 | flat |

All runs: PASS. No leak signal: descriptor count is flat under load and returns to the idle baseline
(listener, ring, and standard descriptors) after the clients stop. Guest throughput differs because the
guests shared two vCPUs with other jobs; the numbers are not a performance comparison.

Limit: a leak that grows slower than the resolution of this test (a few bytes per million operations)
would not be visible in 10 minutes. This is a smoke test for gross leaks, not proof of absence.

## 2. Fuzzing (`fuzz/fuzz_targets/slab_ops.rs`)

A coverage-guided libFuzzer target drives arbitrary interleavings through the real `CompletionSlab`:
insert, deliver CQEs (known, duplicate, stale, recycled-slot, and fully random `user_data`), drop the
owning future (`ignore` and `ignore_simple`), and consume a result. A model tracks the expected fate
of every tracked buffer and the target asserts after each step that a buffer is never dropped before
its releasing event, never dropped twice, that `len()` equals the number of live operations, and that
garbage CQEs change nothing; at teardown every buffer must have been dropped exactly once.

- Campaign: 3,498,512 executions in 421 s, about 11,700 executions/s, 346+ corpus entries, **no crashes**.
- **Harness sanity check:** a deliberate bug (leaking the buffer in `ignore()` when the operation had
  already completed) was injected into a throwaway copy of the slab. The fuzzer failed within seconds
  and wrote a crash artifact. The original file was restored and confirmed byte-identical.

Limit: this fuzzes the pure-Rust bookkeeping, not the kernel interaction, and the model is only as
complete as the invariants written into it. CI runs a 60-second smoke run on every push.

## 3. Kernel matrix (`scripts/kernel_matrix/run.sh`)

Each kernel boots under QEMU/KVM with only a tiny init that mounts the basic filesystems, brings up
loopback, and runs the statically linked (musl) unit and integration tests plus the 20-second soak.
Kernel images are the distributions' unsigned kernel packages, so these are **vendor kernels with
their own io_uring backports**, not mainline releases.

| Kernel | Source | Tests | Soak |
|:---|:---|:---|:---|
| 5.10.0-32 | Debian 11 | 79 passed, 0 failed | PASS |
| 5.15.0-204 | Ubuntu 22.04 LTS | 79 passed, 0 failed | PASS |
| 6.1.0-50 | Debian 12 | 79 passed, 0 failed | PASS |
| 6.12.0 | Oracle Linux (host) | 79 passed, 0 failed | PASS |

79 = 65 unit + 6 cancel-on-drop + 3 high-queue-depth + 5 slab-invariant tests (Kani harnesses are not
run by `cargo test`; they are verified separately). Kernels older than 6.1 exercise the fallback path
that runs without `COOP_TASKRUN`/`DEFER_TASKRUN`; 6.1 and newer exercise the fast path.

Not tested: mainline 5.11 to 5.14, other distributions' kernels, ARM, or kernels with `io_uring`
restricted by seccomp or `kernel.io_uring_disabled`.

## 4. API review (first pass)

- Internal `reactor::op` and `reactor::slab` were fully public, which would have committed their
  internals to semver. They are now `#[doc(hidden)]` with an explicit "not covered by semver" note
  (they remain reachable so the fuzz, Kani and Miri harnesses keep working).
- `TcpStream::read_exact` returned the byte count while `File::read_exact_at` returned `()`. Both now
  return `()`, matching `std::io::Read::read_exact`; nothing in the repository depended on the count.
- Confirmed already correct: dropping a `JoinHandle` detaches the task (documented on `spawn`), and
  `Elapsed` implements `Display` and `Error`.
- Left open on purpose: deciding the stability promise for 1.0, and the `Send` bound on I/O buffers
  (needed by the type-erased slab today, a restriction users may notice).
