# nexio roadmap

Built from a real research pass (`reports/io_uring runtime design
tradeoffs.md`) on tokio-uring, compio, monoio, and glommio — not from
guessing. Two things that research changed before any code was
written:

1. **The honest performance goal is parity, not a clean win, at low
   core counts.** A tokio-rs/io-uring maintainer issue (#291) found a
   naive io_uring receiver losing to multi-threaded tokio 2x — until
   `SO_REUSEPORT` sharding was added, at which point the two
   *converged exactly*. That matches what we found empirically in
   Turbine's own compio-vs-tokio benchmark on a 2-vCPU VPS. Nexio
   should not chase a speed story the evidence doesn't support at
   small scale.
2. **The real, available differentiator is correctness tooling, not
   raw throughput.** tokio-uring and compio's CI run no Miri and no
   loom; monoio's README mentions neither. None of the three run a
   bounded model checker (Kani) on their completion-slab index
   arithmetic, despite AWS's Kani/Firecracker precedent being a near-
   perfect template for exactly that code. This is genuine white
   space a new project can occupy that the incumbents currently don't.

So: **beat them on verified correctness first, chase performance
second, and only with evidence.**

## Phase 0 — v0: a sound single-core reactor (Linux only)

The non-negotiable architecture, because the whole ecosystem converged
on it independently for cancellation-safety reasons, not style:

- [x] `BufResult<T, B>` — owned-buffer result type (`src/buf.rs`)
- [x] `Reactor`: thread-local `io_uring::IoUring` instance, a slab of
      pending completions, submit/poll/park loop
- [x] Owned-buffer `Read`/`Write`/`Accept`/`Connect` ops — never a
      borrowed `&mut [u8]`, per Without Boats' "Notes on io-uring":
      the kernel can still be touching the buffer after a `Future` is
      dropped, so ownership transfer is the only sound model
- [x] **Leak-until-real-completion cancellation**: on drop of an
      in-flight op, submit a best-effort `ASYNC_CANCEL`, move the
      buffer into the reactor's slab (not freed), and only release it
      when the real CQE eventually lands — the exact pattern
      tokio-uring (`Ignored` state), compio, and monoio all use,
      because `IORING_OP_ASYNC_CANCEL` is itself best-effort and
      cannot guarantee the kernel has stopped touching the buffer
- [x] `!Send` tasks, single-threaded executor per reactor — no
      work-stealing, no cross-thread shared ring state (eliminates a
      whole class of synchronization bugs the other runtimes accept
      lock-based solutions for)
- [x] Minimal `TcpListener`/`TcpStream` on top of the above
- [x] Re-run Turbine's honest 3-way echo benchmark (unsharded tokio /
      sharded tokio / nexio sharded) on the same Oracle VPS — if nexio
      doesn't at least match sharded tokio, that's a real finding to
      report, not something to hide or re-benchmark until it looks
      better

**Explicitly deferred, with the evidence for why:**
- SQPOLL — needs a dedicated kernel thread *per ring*; one per core
  under thread-per-core multiplies kernel CPU cost linearly with core
  count, and no benchmark found shows it's worth that on either
  compio or monoio
- Registered buffers/files, multishot accept/recv — real resource-
  lifecycle complexity (registration, fixed-slot allocation, buffer-
  ring refill to dodge `-ENOBUFS`) for a benefit neither inspected
  runtime's driver code actually leans on by default
- Cross-platform fallback (Windows IOCP / macOS kqueue) — real work,
  explicitly out of scope until Phase 0's Linux core is sound and
  benchmarked; a Linux-only v0 mirrors monoio's own scoping choice

## Phase 1 — verification (the actual differentiator)

Do what the incumbents conspicuously don't:

- [x] Split the unsafe surface in two: a pure-Rust layer (slab
      allocation/indexing, `Op` state-machine transitions, waker
      bookkeeping) versus the syscall-adjacent SQE/CQE submission path
- [x] Run **Miri** on the pure-Rust layer against *mocked* completions
      — the same split Tokio's own CI already uses for its epoll core
      (Miri cannot execute real syscalls, so this is the only part it
      can validate)
- [x] A dedicated cancel-on-drop regression test (mirroring Tokio's
      own `complete_ops_on_drop` test) asserting no double-free and no
      leaked fd across a drop-during-flight path — this exact bug
      class produced a real double-free *and* a real use-after-free in
      Without Boats' own Ringbahn prototype, so "we wrote the pattern
      correctly" needs a test, not just a design doc
- [x] Apply **Kani** to the completion-slab index arithmetic under
      adversarial CQE ordering — feed `kani::any()`-generated indices/
      orderings through it the way AWS's Kani team did for
      Firecracker's VirtIO queue parsing and I/O rate limiter (which
      found 5 real bugs, including a quota-bypass rounding error).
      Nobody has published doing this for an io_uring Rust runtime —
      genuine first-mover territory, not catching up
- [x] Integration tests at high queue depth designed to *force*
      exhaustion paths real kernels only reveal under load:
      submission-queue-full, `-ENOBUFS`, `-ECANCELED` races
- [x] Loom: explicitly deferred until/unless any shared state crosses
      a thread boundary (e.g. a future cross-core wake mechanism) —
      not useful for a genuinely single-threaded reactor, and loom's
      own maintainers point to Miri as the complementary tool instead

## Phase 2 — cheap, evidence-backed performance

- [x] `IORING_SETUP_COOP_TASKRUN` / `IORING_SETUP_DEFER_TASKRUN`
      (kernel 6.1+) — Jens Axboe's own guidance says this "has shown
      to yield very nice benefits" for a redesigned completion loop,
      and it costs a setup flag, not an architecture change
- [x] Re-benchmark after each change on the same VPS, same 3-way
      comparison methodology Turbine established — no claim ships
      without a number next to it from this machine, on this day

## Phase 3 — only with profiling evidence, not before

- [x] Registered buffers/files and SQPOLL exist as opt-in `ReactorBuilder` options (`register_buffers`,
      `read_fixed`, `write_fixed`, `register_files`, SQPOLL); none is on by default
- [x] Multishot accept/recv: **measured and rejected** (see below); not implemented in nexio
- [x] Cross-platform fallback: see Phase 6

## Measured and rejected (details and raw numbers in `reports/split_benchmark_results.md`)

Every item was tried against the same Oracle VM with a byte-verifying client and reported whether it
helped or not. None of these reached 2x; TCP cost is ~93% kernel.

- **Registered buffers (single region):** ~3% *worse* than plain buffers on TCP echo
- **Multishot accept + recv with a provided-buffer ring:** CPU tie, throughput ~2-3% lower
- **Zero-copy send (`SendZc`):** slower at every size tested up to 256 KB on loopback
- **Batched sends (one `io_uring_enter` per N):** ~10% less CPU per delivery when fanning out to 1,000 sockets
- **`COOP_TASKRUN` + `DEFER_TASKRUN`:** ~5% more throughput; kept on by default
- **Kernel bypass (AF_XDP), raw UDP, both sides:** 3.4x packets/s on a virtual NIC in copy mode, not
  transferable to TCP; a user-space TCP stack on top (`utcp/`, experimental) was byte-correct against
  Linux TCP but not cheaper per message than nexio. Not part of nexio.

## Phase 4 — usable by other people

Done:
- [x] User-facing README with an honest performance section and an explicit limits section
- [x] Runnable examples (`echo_server`, `echo_server_multicore`, `file_and_timeout`)
- [x] `run_sharded`: one pinned reactor per core in one call
- [x] Package metadata, source-file excludes, `cargo publish --dry-run` clean
- [x] Clear compile error on non-Linux targets
- [x] Repo cleanup (stale scaffold removed, duplicate scripts removed)

Also done in this phase:
- [x] **Kernel matrix:** full suite and soak pass on vendor kernels 5.10, 5.15 and 6.1 (KVM guests) and
      6.12 (host); reproducible with `scripts/kernel_matrix/run.sh`. Mainline 5.11-5.14 untested
- [x] **Fuzzing:** coverage-guided state-machine fuzzer for the completion slab; 3.5M runs, no crashes;
      harness validated by injecting a real bug; 60-second smoke job in CI
- [x] **Soak test:** 10 minutes, 128 clients, 2.0M verified echoes, 337k cancelled in-flight reads; no
      mismatches, memory flat, descriptors back to baseline (`examples/soak.rs`)
- [x] **API review, first pass:** internal modules hidden from docs and excluded from semver;
      `TcpStream::read_exact` made consistent with `File::read_exact_at`

Open, in priority order:
- [ ] Decide and document the 1.0 stability promise; revisit the `Send` bound on I/O buffers
- [ ] Longer soak (hours) and a leak check at finer resolution than "does not grow in 10 minutes"
- [ ] Mainline kernel coverage (5.11-5.14) and kernels with io_uring restricted by seccomp or sysctl
- [ ] Publish to crates.io and make the repository public (needs the owner's explicit go-ahead)

## Phase 6 — the whole ecosystem, not only Linux

Decision: keep `io_uring` as the Linux backend, untouched and still verified, and add a **portable readiness
backend** built on the `polling` crate (epoll on Linux, kqueue on macOS/BSD, IOCP on Windows) and `socket2`.
Only the reactor and the socket layer differ; the executor, channels, `select`/`join` and `JoinSet` are shared.
A readiness backend never gives the kernel a pointer to a caller's buffer, so cancellation safety is simpler
there (dropping a pending future abandons a wait) and needs no completion slab.

Done:
- [x] Build-time backend selection (`build.rs`); `--features poll` forces the portable backend on Linux
- [x] Portable reactor: per-direction wakers, re-arming when one direction fires, timer map
- [x] Portable `TcpListener` / `TcpStream`, timers (`sleep`, `timeout`, `interval`)
- [x] Backend-neutral API test file (`tests/portable_api.rs`): echo, 4 MiB with simultaneous read and write on
      one socket, refused connect, EOF, cancelled read and accept, 200 concurrent clients, timer ordering.
      Passes on io_uring, Linux epoll and Windows
- [x] `spawn_blocking` pool, async host-name resolution, portable `UdpSocket` and async `File`
- [x] CI matrix: Windows, macOS, Linux epoll; all green on first run (macOS: 42 unit + 9 API tests and clippy)

Open, in priority order:
- [ ] Soak and fuzz-style leak checks on the portable backend (the existing soak example already runs on it)
- [ ] Runtime fallback: try `io_uring`, fall back to epoll on failure (today it is a build-time choice)
- [ ] Windows multi-core story (`SO_REUSEPORT` does not exist there)
- [ ] Native IOCP backend only if measurements show the readiness layer is a bottleneck on Windows

## Phase 5 — features users will ask for

None started. Listed so nobody assumes they exist:
- [ ] Async hostname resolution (today `bind`/`connect` with a hostname block on the std resolver)
- [ ] Unix-domain sockets
- [ ] Signal handling, process spawning
- [ ] TLS integration story (an adapter crate, not part of the core)
- [ ] An optional compatibility layer for tokio-style `AsyncRead`/`AsyncWrite`
- [ ] Cross-platform fallback backend (see Phase 3)

## What "beating" tokio/compio/monoio actually means here

Not a benchmark number pulled out of context. In order:

1. **Correctness, verified, not asserted** — Miri + Kani coverage on
   the cancellation-safety surface that none of the three incumbents
   currently run in CI.
2. **Honesty about performance** — every number reported with its
   hardware, core count, and methodology, the same discipline applied
   to Turbine's compio/tokio findings (including the correction when
   the first comparison turned out unfair).
3. **Performance, where the evidence supports it** — parity with
   sharded tokio is the realistic v0 bar per the research; real gains
   are expected only at higher core counts or higher-connection-count
   workloads, and should be chased there, not assumed here.
