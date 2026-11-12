# Split-process benchmark and where 2x is (and isn't) possible

**Date:** 2026-11-12. **Host:** Oracle Linux 9 VPS, 2 vCPU, kernel 6.12, rustc 1.99.1, release + thin LTO.
**Why this exists:** the first benchmark (`phase2_benchmark_results.md`) ran server shards and the
load generator on the same two cores, so a saturated client capped every runtime at roughly 80-90k
msg/s and hid real differences. This one runs the server and the load generator as separate
processes pinned to different cores (`bench/split.sh`, server on core 0, client on core 1) and
reads the server's CPU time from `/proc/<pid>/stat`, giving server CPU microseconds per message.

## TCP echo (100 connections x 2000 messages, 128 B, 3 runs each, run-to-run spread under 1%)

| Backend | Throughput (msg/s) | Server CPU per message | p50 latency |
|:---|---:|---:|---:|
| tokio_unsharded | ~88,400 | 7.60-7.65 us | ~1,142 us |
| tokio_sharded | ~88,500 | 7.65-7.85 us | ~1,138 us |
| **nexio_sharded** | **~94,000** | **6.75 us** | **~1,071 us** |
| compio_sharded | ~87,200 | 7.15 us | ~1,157 us |
| monoio_sharded | ~87,600 | 7.05-7.15 us | ~1,149 us |

Nexio is about 12% cheaper per message than tokio and about 5% cheaper than compio and monoio.
Throughput is still limited by the client core, which is why CPU per message is the primary metric.

### Why 2x on TCP echo is not reachable by any runtime

Server CPU split over 300,000 messages (100 ticks/s):

| Backend | user ticks | kernel ticks | user share |
|:---|---:|---:|---:|
| nexio_sharded | 13 | 187 | 6.5% |
| tokio_sharded | 36 | 193 | 15.7% |

93.5% of nexio's server cost is the kernel TCP stack, which every runtime pays equally. Nexio's
user-space cost is already 0.43 us per message. Driving it to zero would save about 7%. Halving the
total would need the kernel to do half the work, which no runtime controls.

## File I/O (page-cache resident, so this measures syscall and scheduling overhead, not disk speed)

| Workload | nexio | compio | monoio | tokio |
|:---|---:|---:|---:|---:|
| rand_read_4k_qd32 (IOPS) | 1,020,886 | 1,004,831 | 1,046,351 | 104,118 |
| seq_read_64k (IOPS) | 146,505 | 175,196 | 181,012 | 29,939 |
| seq_write_4k + fsync (IOPS) | 26,120 | 26,513 | 26,570 | 20,491 |

- Versus tokio, nexio is about 9.8x on random reads and 4.9x on 64K sequential reads. That is the
  known cost of tokio doing file I/O through a blocking thread pool, not a nexio-specific win.
- Versus compio and monoio, nexio is level on random reads and writes, and 16-19% slower on 64K
  sequential reads. That gap is the concrete optimization target.
- Runs are short (a few ms for the read workloads), so treat differences under about 5% as noise.

## Correction to the earlier report

`phase2_benchmark_results.md` states that io_uring runtimes "pull decisively ahead" at high
concurrency and reports nexio +16% over tokio_sharded. Its own table shows compio_sharded
(88,638 msg/s) and monoio_sharded (87,101 msg/s) beating nexio (79,873 msg/s) in that case, and
the +16% compares against the weakest tokio run (tokio_unsharded was 77,713). Those throughput
numbers were also client-limited. It also labels the VPS as Frankfurt; the VCN is in
ap-singapore-1. Use this report's numbers instead.
