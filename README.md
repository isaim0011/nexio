# nexio

A thread-per-core async runtime for Linux, built directly on `io_uring`.

- **Owned-buffer I/O.** You hand a buffer to an operation and get it back with the result, so the
  kernel never writes into memory Rust thinks it still owns.
- **Cancellation-safe by construction.** Dropping a future that has an operation in flight does not
  free its buffer. The runtime keeps it until the kernel's completion arrives, so there is no
  use-after-free window. This layer is checked with Miri and Kani (see [Verification](#verification)).
- **One reactor per thread, no work-stealing.** Tasks are `!Send`. Scale across cores with
  [`run_sharded`](#use-every-core): one independent reactor per core, connections spread by
  `SO_REUSEPORT`.
- **Small surface:** TCP, UDP, files, timers, channels, `select`/`join`, `JoinSet`.

**Status: 0.1, pre-release.** It works and is tested, but the API may still change. It is Linux-only.

## Quick start

```toml
[dependencies]
nexio = { git = "https://github.com/isaim0011/nexio" }
```

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
                    let (res, b) = stream.read(buf).await;   // buffer goes in, comes back
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

More in [`examples/`](examples): `echo_server`, `echo_server_multicore`, `file_and_timeout`.

### Use every core

```rust
nexio::run_sharded(std::thread::available_parallelism()?.get(), |_shard| async move {
    let listener = nexio::net::TcpListener::bind(("0.0.0.0", 7878)).await.unwrap();
    // ... accept loop, exactly as above
});
```

Each shard gets its own io_uring and its own executor, pinned to its own CPU. `TcpListener::bind` enables
`SO_REUSEPORT`, so the kernel balances new connections across shards.

## Requirements

- Linux with `io_uring`. **Tested only on kernel 6.12.** The code targets 5.11+ and enables
  `COOP_TASKRUN`, `DEFER_TASKRUN` and `SINGLE_ISSUER` (6.1+) when available, falling back to a plain
  ring otherwise, but older kernels have not been run yet (tracked in the roadmap).
- Some container runtimes block `io_uring` with seccomp; if ring setup fails, that is why.
- Declared MSRV is Rust 1.75; it has only been built and tested on current stable.
- Other operating systems fail to compile with an explicit error. There is no fallback backend yet.

## Performance, honestly

Measured on one 2-vCPU Oracle Cloud VM (Linux 6.12), loopback, one machine. Full methodology, raw numbers
and every correction we made are in [`reports/split_benchmark_results.md`](reports/split_benchmark_results.md).

- **TCP echo** (server pinned to one core, byte-verifying client on another): nexio uses about
  **6.75 µs of server CPU per message** versus 7.1–7.2 (compio, monoio) and 7.6–7.85 (tokio). That is
  roughly 5% cheaper than compio/monoio and 12% cheaper than tokio.
- **File I/O:** about 5–10x tokio (which uses a blocking thread pool); level with compio and monoio.
- **Batched random reads** (1M, page cache): about 1.46M IOPS versus 1.27–1.34M (compio) and 1.36–1.46M (monoio).

Please read these as: **nexio is competitive with the other io_uring runtimes and slightly ahead on
these tests, not dramatically faster.** On plain TCP, about 93% of server CPU is the kernel's TCP stack,
which no user-space runtime can remove. We tried registered buffers, multishot accept/recv, zero-copy
sends and batching; the results, including the ones that did not help, are in the reports. Results on
other hardware, real NICs and higher core counts may differ.

## Verification

The cancellation-safety layer is the part most likely to hide memory-safety bugs, so it is the part
that is checked the hardest:

- **Kani** model-checks the completion slab's index arithmetic: 9 harnesses, run with `cargo kani --tests`.
- **Miri** (strict provenance) runs the pure-Rust slab and cancellation tests: `bash scripts/ci_miri.sh`.
- **Integration tests** force submission-queue exhaustion, `-ECANCELED` races and fd-leak scenarios on a
  real kernel: `cargo test`.

Limits: Miri and Kani cannot execute `io_uring` syscalls, so they cover the pure-Rust bookkeeping, not
the kernel interaction. Cancellation of an in-flight `io_uring` operation is best-effort at the kernel
level; nexio's guarantee is that memory stays valid until the kernel is done with it.

## What is not here (yet)

No TLS, HTTP, DNS (hostname resolution in `bind`/`connect` uses the standard library's blocking
resolver), Unix-domain sockets, signal handling, or process spawning. No `AsyncRead`/`AsyncWrite`
compatibility with the tokio ecosystem: the owned-buffer API is different on purpose. No Windows or
macOS backend. See [`ROADMAP.md`](ROADMAP.md).

## Building and testing

```bash
cargo test                       # Linux only
cargo kani --tests               # needs the Kani toolchain
bash scripts/ci_miri.sh          # needs nightly + Miri
bash bench/split.sh              # pinned server/client echo benchmark
```

## License

MIT OR Apache-2.0.
