---
name: nexio-maintainer
description: >-
  Complete top-to-bottom engineering, architecture, cross-platform, verification,
  MCP, and release skill for the nexio async runtime repository. Activate when
  developing, auditing, testing, documenting, or releasing any part of nexio.
---

# `nexio` Maintainer & Engineering Skill

`nexio` is a thread-per-core async runtime with owned-buffer I/O: `io_uring` on Linux, and a portable readiness backend (`epoll` / `kqueue` / `IOCP` via `polling`) everywhere else, behind one unified API.

---

## 1. Core Architectural Invariants (Non-Negotiable)

1. **Owned-buffer contract:** Callers move buffers into I/O operations and receive `(io::Result<T>, Buf)` back. Never expose borrowed-buffer async ops where a dropped future could leave kernel pointers dangling.
2. **Cancellation-on-drop safety (`CompletionSlab`):** Dropping an in-flight future transitions its slab slot from `OpState::Submitted` to `OpState::Cancelled`, retaining `Option<Box<dyn ErasedBuf>>` until the kernel CQE arrives. Never free a buffer before completion.
3. **Thread-per-core (`!Send` tasks):** One reactor per thread, no work-stealing, zero locks on the hot path. Scale across cores with `run_sharded` (`SO_REUSEPORT` on Unix) or `#[nexio::main(shards = "auto")]`.
4. **Backend parity:** Every public API must compile and behave identically across:
   - Linux `io_uring` (`src/reactor/`, `src/net/`, `src/fs/`)
   - Portable `polling` (`src/poll/` — Windows IOCP, macOS kqueue, BSDs, Linux `--features poll`)
   - Runtime fallback (`src/fallback/` — Linux `--features runtime-fallback`, probing `io_uring_available()`)

---

## 2. Repository & Crate Map

- **Root crate (`nexio`):** `Cargo.toml`, `build.rs`, `src/`
- **Proc-macro crate (`nexio-macros`):** `nexio-macros/` (`#[nexio::main]`, `#[nexio::test]`, enabled by default via `macros` feature)
- **Public modules in `src/`:**
  - `reactor`: `Reactor`, `ReactorStats`, `current_stats()`, `ProvidedBufRing`, `MultishotAccept`, `MultishotRecv`
  - `net`: `TcpListener`, `TcpStream`, `UdpSocket`, `split()` / `into_split()` / `reunite()`, vectored I/O, `accept_multishot`, `recv_multishot`, `lookup_host`
  - `unix` (`cfg(unix)`): `UnixListener`, `UnixStream` (`split` / `into_split` / `reunite`), `UnixDatagram`
  - `fs`: `File`, `OpenOptions`, positional & vectored `*_at` ops, `read`, `write`, `copy`, `rename`, `metadata`, `symlink_metadata`, `create_dir`, `create_dir_all`, `remove_file`, `remove_dir`, `remove_dir_all`, `set_permissions`, `read_dir`
  - `sync`: `Notify`, `Semaphore`, `Mutex`, `RwLock`, `watch`, `broadcast`, `CancellationToken`, `oneshot`, `mpsc`
  - `time`: `sleep`, `sleep_until`, `timeout`, `timeout_at`, `interval`, `interval_at`, `Sleep` (`deadline`, `is_elapsed`, `reset`), `Interval`, `MissedTickBehavior` (`Burst`, `Delay`, `Skip`)
  - `process` & `signal`: `Command`, `ctrl_c()`, Unix `signal()`
  - `http` (`feature = "http"`): `Server` (`bind`, `from_listener`, `local_addr`, `serve`, `serve_with_shutdown`), `serve_connection` (HTTP/1.1), `serve_http2_connection` (HTTP/2), `LocalExec`, `connect`
  - `tls` (`feature = "tls"`): `rustls` `TlsAcceptor` / `TlsConnector`
  - `compat` (`feature = "tokio-compat"` or `"http"`): `Compat` (`AsyncRead` / `AsyncWrite`)
  - `profile`: OS capability/limit probing (`memlock`, `nofile`, RAM)
- **Agent MCP server:** `examples/mcp_server.rs` + `.mcp.json` (`nexio_runtime_info`, `nexio_platform_matrix`, `nexio_api_reference`, `nexio_check_workspace`)
- **Verification & scripts:** `tests/`, `fuzz/`, `scripts/ci_miri.sh`, `.github/workflows/ci.yml`

---

## 3. Cross-Platform Rules (Linux / Windows / macOS)

- **Linux (`io_uring` / `epoll`):**
  - Default `io_uring`; `coop-taskrun` opts into `COOP_TASKRUN` / `DEFER_TASKRUN`.
  - `poll` forces `epoll`; `runtime-fallback` probes `io_uring_available()` at runtime and falls back to `epoll` on `EPERM`/`ENOSYS`/`EACCES`.
- **macOS (`kqueue`) & non-glibc Unix:**
  - Never use glibc-specific `libc` types unconditionally (e.g. use `libc::c_int` instead of `libc::__rlimit_resource_t` outside `all(target_os = "linux", target_env = "gnu")`).
- **Windows (`IOCP`):**
  - Gate Unix-only items (`UnixStream`, `UnixDatagram`, `signal::unix`, `SO_REUSEPORT` assumptions) behind `#[cfg(unix)]`.
- **No-default-features targets (`Miri` and `Fuzz`):**
  - `scripts/ci_miri.sh` runs with `--no-default-features` and keeps `TMPDIR` outside the repo root so Miri's temporary sysroot crate does not conflict with `[workspace]`.
  - `fuzz/Cargo.toml` depends on `nexio` with `default-features = false`.

---

## 4. Mandatory Verification Gate (Run Before Every Push)

1. **Formatting & Docs:**
   ```bash
   cargo fmt --check
   RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --features http,tls,tokio-compat
   ```
2. **Clippy:**
   ```bash
   cargo clippy --lib --tests --examples --features http,tls,tokio-compat -- -D warnings
   ```
3. **Tests:**
   ```bash
   cargo test --features http,tls,tokio-compat --workspace
   ```
   On Linux with `runtime-fallback`, also verify both `io_uring` and forced fallback:
   ```bash
   cargo test --features runtime-fallback,http,tls,tokio-compat --workspace
   NEXIO_FORCE_POLL=1 cargo test --features runtime-fallback,http,tls,tokio-compat --workspace
   ```
4. **Formal / UB / Fuzz (Linux):**
   ```bash
   bash scripts/ci_miri.sh
   cargo kani --tests
   cargo +nightly fuzz run slab_ops -- -max_total_time=60
   ```
5. **MCP Smoke Check:**
   Verify `cargo run --quiet --example mcp_server` responds cleanly to `initialize`, `tools/list`, `nexio_runtime_info`, `nexio_platform_matrix`, `nexio_api_reference`, and `nexio_check_workspace`.

---

## 5. Release & Publishing Runbook

1. Bump version in `Cargo.toml` (and `nexio-macros/Cargo.toml` + `nexio-macros` dependency version if macro code changed).
2. Update `CHANGELOG.md` and `README.md` (examples, feature table, platform notes, MCP docs).
3. Run the full Verification Gate above.
4. Commit, tag (`git tag vX.Y.Z`), and push (`git push origin main vX.Y.Z`).
5. Watch GitHub Actions CI until **all 9 jobs** pass (`Test, fmt, docs`, `Clippy All Targets`, `Portable backend (Linux/Windows/macOS)`, `Miri Verification`, `Kani Model Checking`, `Fuzz smoke`, `Release Benchmark Smoke Check`).
6. Publish to `crates.io` (`cargo publish -p nexio-macros` if updated, then `cargo publish -p nexio`).
7. Create the GitHub Release (`gh release create vX.Y.Z ...`).
