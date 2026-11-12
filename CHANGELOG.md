# Changelog

## Unreleased

### Fixed (affects the published 0.1.0)
- **`UdpSocket::recv_from` on the io_uring backend did not wait.** On an unconnected socket it made one
  non-blocking `recvfrom` call and returned `WouldBlock` if no datagram was queued, so
  `recv_from(buf).await` in a UDP server failed spuriously. The connected `recv`/`send` and `send_to`
  could surface `WouldBlock` the same way on a full buffer. They now wait (a one-shot `POLL_ADD`, which
  holds no buffer, so it is trivially cancellation-safe) and retry. Found by running one shared API test
  file against both backends; regression tests cover data arriving after the receive starts.

- **Portable backend** (epoll / kqueue / IOCP via `polling` and `socket2`) for Windows, macOS and other Unix,
  selected automatically off Linux or with `--features poll` on Linux. Covers TCP, timers and everything in
  the executor; UDP and async file I/O are not implemented on it yet. Same API, verified by a shared test file
  that passes on io_uring, epoll and Windows.
- Unix-domain sockets (`UnixListener`, `UnixStream`, `UnixDatagram`) on Unix, one implementation shared by both
  backends; writing to a closed peer returns `BrokenPipe` instead of raising `SIGPIPE`.
- `nexio::signal`: `ctrl_c()` everywhere and `signal(kind)` on Unix, delivered across any number of reactor
  threads. Signals coalesce, like the OS. Forbidden signals return an error instead of panicking.
- `nexio::process`: `Command`/`Child` with async `wait`, `output`, and piped stdin/stdout/stderr. Pipes never
  lose data when an operation is cancelled, output is bounded to 1 MiB per pipe, dropped children are reaped.
- `spawn_blocking`: a blocking thread pool whose results are delivered to the requesting reactor thread.
- `lookup_host` and the `ToAddrs` trait: `bind`/`connect` no longer run the blocking system resolver on the
  reactor thread; IP literals resolve instantly and only host names use the pool.
- Portable backend gains `UdpSocket` and async `fs::File` / `OpenOptions` (files run on the blocking pool).
- Backend selection moved to a build script (`nexio_uring` / `nexio_poll` cfgs).
- `TcpStream::read_exact` returns `()` instead of the byte count (breaking, matches `File::read_exact_at`).
- Internal `reactor::op` and `reactor::slab` hidden from the docs and excluded from semver.

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
