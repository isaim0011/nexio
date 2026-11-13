<div align="center">

# nexio

**A thread-per-core async runtime with owned-buffer I/O.**<br>
`io_uring` on Linux. Epoll, kqueue and IOCP everywhere else. One API.

[![Crates.io](https://img.shields.io/crates/v/nexio.svg)](https://crates.io/crates/nexio)
[![Docs.rs](https://img.shields.io/docsrs/nexio)](https://docs.rs/nexio)
[![CI](https://github.com/isaim0011/nexio/actions/workflows/ci.yml/badge.svg)](https://github.com/isaim0011/nexio/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
![Platforms](https://img.shields.io/badge/platforms-Linux%20%7C%20Windows%20%7C%20macOS-informational)

[Quick start](#quick-start) ·
[Features](#features) ·
[Platforms](#platforms) ·
[Performance](#performance-honestly) ·
[Verification](#verification) ·
[Roadmap](ROADMAP.md) ·
[Changelog](CHANGELOG.md)

</div>

---

nexio is built around three ideas:

- **Owned buffers.** You hand a buffer to an operation and get it back with the result, so the kernel never
  writes into memory Rust thinks you still own.
- **Cancellation that cannot free memory early.** Dropping a future with an operation in flight does not free
  its buffer; the runtime holds it until the kernel's completion arrives. This layer is checked with Miri,
  Kani and a fuzzer.
- **One reactor per thread, no work-stealing.** Tasks are `!Send`, there are no locks on the hot path, and
  you scale across cores by running one independent reactor per core.

It ships with the pieces a real service needs, on every platform: TCP, UDP, Unix sockets, files, timers,
channels, signals, child processes, async DNS, and (behind features) TLS and HTTP/1.1.

## Quick start

```toml
[dependencies]
nexio = "0.2"
```

An echo server:

```rust
use nexio::net::TcpListener;

fn main() -> std::io::Result<()> {
    nexio::block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 7878)).await?;
        loop {
            let stream = listener.accept().await?;
            drop(nexio::spawn(async move {
                let mut buf = Vec::with_capacity(4096);
                loop {
                    let (res, b) = stream.read(buf).await;   // the buffer goes in and comes back
                    buf = b;
                    match res {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let (res, b) = stream.write_all(buf).await;
                            buf = b;
                            if res.is_err() { break; }
                        }
                    }
                }
            }));
        }
    })
}
```

### Use every core

```rust
nexio::run_sharded(std::thread::available_parallelism()?.get(), |_shard| async move {
    let listener = nexio::net::TcpListener::bind(("0.0.0.0", 7878)).await.unwrap();
    // ... the same accept loop
});
```

Each shard owns its ring and executor, pinned to its own CPU. `TcpListener::bind` sets `SO_REUSEPORT`, so the
kernel balances new connections across shards.

### HTTP and TLS

```toml
nexio = { version = "0.2", features = ["http", "tls"] }
```

```rust
use nexio::http;
use hyper::{service::service_fn, Response, body::Bytes};
use http_body_util::Full;

// One task per connection; works over a TcpStream or a TlsStream.
let stream = listener.accept().await?;
drop(nexio::spawn(async move {
    let _ = http::serve_connection(stream, service_fn(|_req| async {
        Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from("hello"))))
    })).await;
}));
```

More in [`examples/`](examples): `echo_server`, `echo_server_multicore`, `file_and_timeout`, `soak`.

## Features

| Area | What you get |
|:---|:---|
| **Networking** | `TcpListener`/`TcpStream`, `UdpSocket`, Unix-domain stream and datagram sockets, async host-name resolution |
| **Files** | positional `read_at`/`write_at`, `OpenOptions` |
| **Runtime** | `block_on`, `spawn`, `JoinSet`, `join2`, `select2`, timers and timeouts, bounded and unbounded channels, `spawn_blocking` |
| **Processes and signals** | spawn children with piped stdio, `ctrl_c`, Unix `signal()` |
| **TLS** (`tls`) | `rustls` client and server; a connection cut without `close_notify` is an `UnexpectedEof` error, never a clean end; ALPN |
| **HTTP/1.1** (`http`) | client and server on `hyper`, plain or over TLS |
| **Ecosystem** (`tokio-compat`) | `nexio::compat::Compat` exposes nexio streams as tokio `AsyncRead`/`AsyncWrite` (no tokio runtime needed) |

nexio contains no cryptography and no HTTP parser of its own: TLS is `rustls`, HTTP is `hyper`, wired to nexio's
sockets.

### Cargo features

| Feature | Effect |
|:---|:---|
| `tls` | `nexio::tls` over `rustls` (ring provider; you choose the trust roots) |
| `http` | `nexio::http` over `hyper` (HTTP/1.1) |
| `tokio-compat` | the tokio `AsyncRead`/`AsyncWrite` adapter |
| `poll` | force the portable backend on Linux (useful where `io_uring` is blocked) |
| `no-coop-taskrun` | do not request `COOP_TASKRUN`/`DEFER_TASKRUN` on `io_uring` |

## Platforms

The backend is chosen at build time; the public API is the same, and the shared API tests run unchanged against
every backend.

| | `io_uring` backend | Portable backend |
|:---|:---|:---|
| **Used on** | Linux (default) | Windows, macOS, other Unix, and Linux with `--features poll` |
| **Mechanism** | completion-based `io_uring`; the kernel holds your buffer | readiness polling (epoll / kqueue / IOCP) over non-blocking sockets |
| TCP, UDP, timers, channels, `spawn` | yes | yes |
| Files | native `io_uring` | blocking thread pool |
| Unix sockets, signals, processes, DNS | yes | yes (Unix sockets and Unix signals on Unix) |
| `run_sharded` with `SO_REUSEPORT` | yes | Unix only |
| TLS, HTTP/1.1, tokio compat | yes | yes |
| **Verification** | Miri, Kani, fuzzing, soak, kernel matrix, CI | CI on Windows, macOS and Linux epoll |

**Requirements.** On Linux, a kernel with `io_uring` (tested on vendor kernels 5.10, 5.15, 6.1 and 6.12; newer
features such as `DEFER_TASKRUN` are used when present). Some container runtimes block `io_uring` with
seccomp; use `--features poll` there. Rust 1.75 or newer is declared; it is built and tested on current stable.

## Performance, honestly

Measured on one 2-vCPU Oracle Cloud VM (Linux 6.12), loopback. Methodology, raw numbers and every correction are
in [`reports/split_benchmark_results.md`](reports/split_benchmark_results.md).

| Benchmark | nexio | Comparison |
|:---|:---|:---|
| TCP echo, server CPU per message | **6.75 µs** | compio / monoio 7.1 to 7.2 µs, tokio 7.6 to 7.85 µs |
| File I/O | 5 to 10x tokio | level with compio and monoio |
| Batched random reads (1M, page cache) | **1.46M IOPS** (1.42M typical; `noatime` adds ~6%) | compio 1.27 to 1.34M, monoio 1.36 to 1.46M |
| Scheduler: spawn + await a task | **~110 ns** | tokio current-thread ~168 ns |
| Scheduler: bounded channel send + recv | **~15.5 ns** | tokio ~39 ns |

Read this as: **nexio is competitive with the other `io_uring` runtimes and slightly ahead on these tests, not
dramatically faster.** On plain TCP about 93% of server CPU is the kernel's TCP stack, which no user-space
runtime can remove. We tried registered buffers, multishot operations, zero-copy sends and batching; the
results, including those that did not help, are in the reports, and a profile of where the time goes is in [`reports/optimization_pass.md`](reports/optimization_pass.md). Other hardware, real NICs and more cores may
give different numbers.

## Verification

The cancellation layer is where memory-safety bugs would hide, so it gets the heaviest checking:

- **Kani** model-checks the completion slab's index arithmetic (9 harnesses): `cargo kani --tests`.
- **Miri** runs the pure-Rust slab and cancellation tests under strict provenance: `bash scripts/ci_miri.sh`.
- **Integration tests** force submission-queue exhaustion, `-ECANCELED` races, descriptor-leak and
  descriptor-reuse scenarios against a real kernel.
- **Fuzzing** drives random insert/complete/drop/garbage-completion sequences through the slab against a model:
  `cargo +nightly fuzz run slab_ops`.
- **Soak:** connection churn with random cancellation, byte-verified, flat memory, descriptors back to baseline:
  `cargo run --release --example soak -- 600 128`.
- **Kernel matrix:** tests plus soak inside real 5.10, 5.15 and 6.1 kernels: `scripts/kernel_matrix/run.sh`.
- **CI** runs tests, clippy, rustdoc, Miri, Kani and a fuzz smoke test on Linux, plus the portable backend on
  Windows, macOS and Linux with every optional feature enabled.

Details and limits: [`reports/reliability_results.md`](reports/reliability_results.md). Miri and Kani cannot
execute `io_uring` syscalls, so they cover the bookkeeping, not the kernel interaction. The portable backend has
not had the soak, fuzz and kernel-matrix campaigns the `io_uring` backend has had.

## Not included

HTTP/2 (hyper's HTTP/2 needs `Send` tasks; nexio's are thread-local by design), HTTP/3, WebSockets, and a default
TLS trust store (choose roots yourself, for example with `webpki-roots`). See [ROADMAP.md](ROADMAP.md).

## Building and testing

```bash
cargo test --features http,tls,tokio-compat   # any OS
cargo test --features poll                    # force the portable backend on Linux
cargo kani --tests                            # needs the Kani toolchain
bash scripts/ci_miri.sh                       # needs nightly and Miri
bash bench/split.sh                           # pinned server/client echo benchmark
```

## Contributing

Issues and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md). Security reports: see
[SECURITY.md](SECURITY.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your
option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in nexio
by you shall be dual licensed as above, without any additional terms or conditions.
