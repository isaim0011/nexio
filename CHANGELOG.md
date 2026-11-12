# Changelog

## 0.1.0

First release. Linux only; pre-1.0, so the API may still change.

### What is in it
- Thread-per-core `io_uring` runtime: one reactor and one single-threaded executor per thread.
- Owned-buffer I/O (`TcpListener`, `TcpStream`, `UdpSocket`, `fs::File`) where a buffer is handed to an
  operation and returned with its result.
- Cancellation safety: dropping a future with an operation in flight keeps its buffer alive until the
  kernel's completion arrives. Accepted descriptors from cancelled accepts are closed, not leaked.
- Timers (`sleep`, `timeout`, `interval`), `select2`/`join2`, `JoinSet`, `!Send` oneshot and mpsc
  channels, and `run_sharded` for one pinned reactor per core.
- `COOP_TASKRUN`, `DEFER_TASKRUN` and `SINGLE_ISSUER` enabled automatically on kernels that support
  them, with a plain-ring fallback (`no-coop-taskrun` feature turns them off).

### How it was checked
- Kani: 9 harnesses on the completion slab. Miri (strict provenance) on the pure-Rust slab layer.
- Coverage-guided fuzzing of the slab against a model: 3.5M executions, no failures.
- 10-minute soak with random cancellation: 2.0M byte-verified echoes, no mismatches, flat memory,
  descriptors back to baseline.
- Full test suite and soak on vendor kernels 5.10 (Debian), 5.15 (Ubuntu 22.04) and 6.1 (Debian), and 6.12.

### Known limits
- Linux only; compiling for other targets is a deliberate error.
- No TLS, async DNS (hostname resolution uses the blocking std resolver), Unix-domain sockets, signals
  or process spawning, and no tokio `AsyncRead`/`AsyncWrite` compatibility.
- Miri and Kani cannot execute `io_uring` syscalls, so they cover the pure-Rust bookkeeping only.
- Mainline kernels 5.11 to 5.14 and distributions other than those above are untested.
- Performance is competitive with other io_uring runtimes (about 5-12% less server CPU per message on a
  TCP echo test), not dramatically faster; see `reports/` in the repository for methodology and every
  result, including the ideas that did not help.

### Bugs found and fixed while preparing this release
- A cargo feature declaration did not match the code, leaving `COOP_TASKRUN` impossible to disable.
- CI had been failing on every push (test job ran standalone benchmark programs); fixed and now green.
- `TcpStream::read_exact` now returns `()` like `File::read_exact_at` and the standard library.
