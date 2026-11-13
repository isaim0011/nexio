# Optimization pass (0.3 development)

Goal: find out where nexio's time actually goes and make what can be faster, faster. Everything below was
measured on the same 2-vCPU Oracle Cloud VM (Linux 6.12), pinned with `taskset`, interleaved runs.
Nothing here is a projection.

## 1. Where the time goes (perf, frame pointers on)

**TCP echo, server pinned to one core** (`scripts/profile_echo.sh`, 6.7 us of server CPU per message):

| Share of server CPU | What |
|---|---|
| ~94% | kernel: TCP stack, netfilter/conntrack (`nft_do_chain` alone is 8% on this VM), `io_uring` submission |
| ~3.7% | everything compiled into the binary, nexio included |
| ~2% | libc `syscall` wrapper (`io_uring_enter`) |

nexio's own code (executor, slab, wakers, reactor) is on the order of 0.25 us per message. Removing *all* of
it would make echo ~4% cheaper. There is no runtime-only route to a large echo speedup, which matches the
earlier finding (~93% kernel). The same netfilter cost applies to every runtime on this VM.

**Batched random reads, page cache** (4 KiB, queue depth 64, ~0.74 us per read):

| Share | What |
|---|---|
| 20.6% | `_copy_to_iter` (the kernel copying page-cache data into the buffer) |
| ~25% | filesystem read path (`filemap_read`, XFS, ...) |
| ~7% | **atime handling** (`atime_needs_update`, `touch_atime`, `current_time`) |
| ~3% | SELinux file permission hook |
| ~5% | nexio (slab insert 2.3%, `push_sqe` 1%) |

## 2. Changes and measured effect

| Change | Measured effect |
|---|---|
| `OpenOptions::noatime(true)` (`O_NOATIME`, falls back when not the owner) | batched 4 KiB reads: **1.34M -> 1.42M IOPS (+6%)**, CPU per read 0.74 -> 0.70 us (5 interleaved A/B runs, every pair favored it) |
| Bounded-channel blocked senders: `VecDeque` scan -> ordered map (`O(n)` -> `O(log n)` per poll and per cancel, FIFO kept) | removes a quadratic case with many blocked senders; no change at low contention |
| Compat adapter: byte-by-byte copy -> one bulk copy | fewer instructions per `poll_read` with large buffers; not separately benchmarked |
| TLS: reuse the write and decrypt scratch buffers | no allocation per record in steady state; not separately benchmarked |
| Portable reactor: reuse wake/re-arm vectors, skip waker clones when the same task re-arms | fewer allocations per readiness round; not separately benchmarked |
| TCP echo | unchanged (6.65-6.75 us before and after) |

The slab/`PlaceholderBuf` allocation suggested in an outside review turned out not to exist: it is a
zero-sized type, and boxing a zero-sized type does not allocate. It was not changed.

## 3. Scheduler micro-benchmark (`benches/tasks.rs`, one thread, pinned)

nexio versus tokio's current-thread runtime (`LocalSet`), 2M items, three runs each, results stable to ~2%:

| Case | nexio | tokio | ratio |
|---|---|---|---|
| spawn a trivial task and await it | 108-113 ns | 166-171 ns | 1.5x |
| `yield_now` round trip | 35 ns | 47-49 ns | 1.35x |
| bounded channel send+recv | 15.5 ns | 38.5-39.4 ns | 2.5x |
| zero-length `sleep` | 505 ns | ~1 ms (tokio rounds to its 1 ms timer tick) | not comparable |

These are scheduler and channel costs only. They matter for task-heavy services; for I/O-bound ones the
kernel dominates, as in section 1.

## 3b. What is left

- Echo and other kernel-bound paths cannot get meaningfully faster from user space. The remaining levers
  (multishot receive, registered buffers, zero-copy send, SQPOLL) were measured earlier and did not help on
  this hardware; see `split_benchmark_results.md`.
- `noatime` only helps workloads that read many files you own. It is opt-in because it changes semantics
  (access times stop updating).
