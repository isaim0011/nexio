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
- Versus compio and monoio, nexio is level on every workload. An earlier single run showed nexio
  16-19% behind on 64K sequential reads; six repeat runs showed that was noise. Range over six
  runs, in IOPS: nexio 131k-194k (mean ~166k), compio 130k-181k (mean ~162k), monoio 132k-191k
  (mean ~175k). The ranges overlap almost completely, so there is nothing to optimize there.
- Each read workload lasts only a few milliseconds, so a single run cannot resolve differences
  under roughly 30%. Compare means over repeated runs, not one table.

## Correction to the earlier report

`phase2_benchmark_results.md` states that io_uring runtimes "pull decisively ahead" at high
concurrency and reports nexio +16% over tokio_sharded. Its own table shows compio_sharded
(88,638 msg/s) and monoio_sharded (87,101 msg/s) beating nexio (79,873 msg/s) in that case, and
the +16% compares against the weakest tokio run (tokio_unsharded was 77,713). Those throughput
numbers were also client-limited. It also labels the VPS as Frankfurt; the VCN is in
ap-singapore-1. Use this report's numbers instead.

## Batched small ops (`benches/batched.rs`)

Page-cache resident 16 MiB file, 1,000,000 random reads, one process per backend pinned to a single
core, 4 runs each. Queue depth is the number of concurrent tasks issuing reads, so submissions can be
batched per `io_uring_enter`.

| Config | nexio | monoio | compio |
|:---|---:|---:|---:|
| QD 128, 512 B reads | 1.455-1.479M IOPS (0.67-0.68 us/op) | 1.357-1.465M (0.68-0.73 us/op) | 1.268-1.338M (0.74-0.79 us/op) |
| QD 512, 64 B reads | 1.438-1.471M IOPS (0.68-0.69 us/op) | 1.407-1.438M (0.69-0.71 us/op) | 1.282-1.307M (0.76-0.78 us/op) |

Nexio is about 12% ahead of compio and level with or slightly ahead of monoio (0-7%). The per-op
cost sits near 0.68 us for every runtime, which is mostly the kernel read path, so batching
cannot produce 2x here either. The honest summary across all workloads measured: nexio is at the
front of the io_uring pack by single-digit to low-double-digit percentages, and far ahead of tokio
only where tokio falls back to a blocking thread pool (file I/O).

## Zero-copy sends (`benches/zc.rs`, standalone, not integrated into nexio)

Plain `Send` versus `SendZc` (IORING_OP_SEND_ZC) straight on the `io-uring` crate, loopback TCP,
sender on core 0 and a blocking receiver on core 1, queue depth 1, 400 MB per run, 3 runs each
(runs agree within 2%).

| Message size | Send MB/s | SendZc MB/s | Send CPU/msg | SendZc CPU/msg |
|---:|---:|---:|---:|---:|
| 4 KB | 436-478 | 208 | 5.6-6.1 us | 7.5 us |
| 16 KB | ~1,640 | ~750 | 6.6 us | 7.7 us |
| 64 KB | ~3,000 | ~2,225 | 11.2-11.3 us | 12.0 us |
| 256 KB | ~4,950 | 3,160-3,250 | 35.0 us | 26.1-27.3 us |

Zero-copy is slower in throughput at every size and costs more sender CPU up to 64 KB. Only at 256 KB
does it save sender CPU (about 25%), and even there throughput is lower. It is nowhere near 2x.

Limits of this test: the kernel copies zero-copy data on loopback instead of pinning pages, so this
host cannot show the benefit a real NIC might give; queue depth 1 serializes on the extra
buffer-release notification CQE, so deeper pipelining could narrow the throughput gap. Supporting
SendZc in nexio would also require changes to the verified completion slab (two CQEs per send).
Not worth doing on this evidence.

## Fan-out: one payload to N sockets (`benches/fanout.rs`, standalone)

The core operation of a rooms/pub-sub server. 1,000 loopback connections, 300 rounds (300,000
deliveries), publisher on core 0, a single epoll receiver thread on core 1, 3 runs each.
`sync` is N blocking `send(2)` calls per round; `uring` queues N Send SQEs and issues one
`io_uring_enter` per round.

| Message size | sync CPU per delivery | uring CPU per delivery | Saving |
|---:|---:|---:|---:|
| 128 B | 4.51-4.58 us | 4.03-4.11 us | ~10-12% |
| 1 KB | 4.55-4.75 us | 4.04-4.31 us | ~5-15% |

Batching all N sends into one syscall saves roughly 10%. The remaining ~4 us per delivery is the
kernel TCP send path, which no submission strategy removes. Deliveries per second were bounded by
the single receiver core (~125-135k/s), so CPU per delivery is the metric to read, not throughput.
