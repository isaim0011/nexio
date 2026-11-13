# Changelog

## 0.2.5 - 2026-11-14

### Added
- **Procedural macros (`nexio-macros`, default `macros` feature):** `#[nexio::main]` (with `shards = N` / `shards = "auto"`) and `#[nexio::test]`.
- **Stream splitting:** `split()`, `into_split()`, and `reunite()` (`ReuniteError`) on `TcpStream` and `UnixStream`.
- **Filesystem helpers (`nexio::fs`):** `read`, `write`, `metadata`, `symlink_metadata`, `remove_file`, `remove_dir`, `remove_dir_all`, `rename`, `create_dir`, `create_dir_all`, `copy`, `set_permissions`, `read_dir` (`ReadDir`, `DirEntry`), plus `File::{metadata, set_len, set_permissions}`.
- **Vectored I/O:** `TcpStream::{read_vectored, write_vectored, write_all_vectored}` and `File::{read_vectored_at, write_vectored_at, write_all_vectored_at}` across `io_uring` and `poll`.
- **Async sync primitives (`nexio::sync`):** `Notify`, `Semaphore`, `Mutex`, `RwLock`, `watch`, `broadcast`, `CancellationToken`, and re-exports of `oneshot` and `mpsc`.
- **HTTP/2 & Server ergonomics (`nexio::http`):** Enabled `hyper` `http2` support and added `LocalExec`, `serve_http2_connection`, and `Server::{bind, from_listener, local_addr, serve, serve_with_shutdown}`.
- **Timer ergonomics (`nexio::time`):** Public `Sleep` (`deadline()`, `is_elapsed()`, `reset()`), `timeout_at`, `interval_at`, and `MissedTickBehavior::{Burst, Delay, Skip}`.
- **Runtime `io_uring` availability & fallback:** Added `nexio::io_uring_available()` and optional `runtime-fallback` feature so Linux binaries can automatically fall back to `epoll` (`polling`) when `io_uring_setup` is blocked (`EPERM`/`ENOSYS`/`EACCES`).
- **Multishot I/O & provided buffer rings (`io_uring`):** Added `ProvidedBufRing` (`IORING_REGISTER_PBUF_RING`), `TcpListener::accept_multishot`, and `TcpStream::recv_multishot`.
- **Observability:** Added `ReactorStats`, `Reactor::stats()`, `Reactor::backend_name()`, and `current_stats()`.

### Changed
- Replaced the `no-coop-taskrun` Cargo feature with `coop-taskrun` to match `ReactorBuilder::coop_taskrun`.

## 0.2.4 - 2026-11-13

### Changed
- `compat::Compat` reads adaptively: it starts at 16 KiB and doubles (up to 256 KiB) while reads keep
  filling the buffer, shrinking back after a run of small reads, and accepts up to 256 KiB per write
  (was 64 KiB). A consumer sitting on a stream of large messages (a WebSocket server receiving 8 KiB
  publishes, say) now sees a burst in a few reads instead of many, so it handles the whole burst before it
  next waits on the socket and can fan it out in one batch. Measured with Turbine's fan-out benchmark: TCP
  segments per delivery at 8 KiB went from 1.02 to 0.57 and server CPU per delivery from 6.0 to 5.2 us;
  small payloads are unchanged.

## 0.2.3 - 2026-11-13

### Fixed
- **Wakers called from another thread were lost.** A nexio task's `Waker` only worked on the thread
  that ran it, so waking it from a `tokio::sync::mpsc` sender, a `oneshot`, a `Notify` or any other
  channel on a different thread silently did nothing and the task hung (the same for the future passed
  to `block_on`). Task ids now carry their executor's id, and a wake from a foreign thread is queued
  in the owner's mailbox and wakes its reactor. Same-thread wakes keep the zero-atomic fast path
  (spawn, yield and channel costs measured unchanged). New `tests/cross_thread_wake.rs` runs on every
  backend and hangs without the fix.
- Limits that follow: up to 1023 threads may run nexio executors at once, and up to ~4 million live
  tasks per thread; both panic with a clear message instead of misbehaving.

## 0.2.2 - 2026-11-13

### Added
- `OpenOptions::noatime(true)`: open with `O_NOATIME` (Linux) so reads do not update access times;
  falls back to a normal open when the kernel refuses it. About +6% IOPS on page-cache random reads.
- `benches/tasks.rs` scheduler micro-benchmark and `scripts/profile_echo.sh`;
  `reports/optimization_pass.md` with the profile and measured results.

### Changed
- Bounded channels track blocked senders in an ordered map: O(log n) wake-up registration and
  cancellation instead of scanning every blocked sender.
- The compat adapter copies read data in bulk; TLS reuses its scratch buffers; the portable reactor reuses
  its wake lists and skips redundant waker clones.

## 0.2.1 - 2026-11-13

Documentation only: new README, CONTRIBUTING and SECURITY files, updated crates.io keywords.

## 0.2.0 - 2026-11-13

### Added
- `compat` (feature `tokio-compat`): tokio `AsyncRead`/`AsyncWrite` adapter for nexio streams.
- `tls` (feature `tls`): TLS client/server over `rustls`, with truncation detection and ALPN.
- `http` (feature `http`): HTTP/1.1 client and server on `hyper`, plain or over TLS.

### Fixed (affects the published 0.1.0)
- **A closed descriptor could be reused before a queued `io_uring` operation on it was submitted.** nexio
  batches submissions, and the kernel resolves an SQE's file number at submit time. Dropping a stream
  with a read still in the submission queue closed the number, a new socket took it, and the stale read
  then ran on the new connection and swallowed its first bytes (the request simply never arrived).
  Every close now submits queued operations first. Found by the HTTP concurrency test (5+ parallel
  connections hung deterministically); passes on both backends.
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
- No HTTP/2 (hyper needs `Send` tasks). The portable backend has had no soak/fuzz campaign.
- Miri and Kani cannot execute `io_uring` syscalls, so they cover the pure-Rust bookkeeping only.
- Mainline kernels 5.11 to 5.14 and distributions other than those above are untested.
- Performance is competitive with other io_uring runtimes (about 5-12% less server CPU per message on a
  TCP echo test), not dramatically faster; see `reports/` in the repository for methodology and every
  result, including the ideas that did not help.

### Bugs found and fixed while preparing this release
- A cargo feature declaration did not match the code, leaving `COOP_TASKRUN` impossible to disable.
- CI had been failing on every push (test job ran standalone benchmark programs); fixed and now green.
- `TcpStream::read_exact` now returns `()` like `File::read_exact_at` and the standard library.
