//! Fair Disk / File I/O benchmark comparing:
//!   - `nexio`       (`nexio::fs::File` over `io_uring`)
//!   - `compio`      (`compio::fs::File` over `io_uring`)
//!   - `monoio`      (`monoio::fs::File` over `io_uring`)
//!   - `tokio`       (`tokio::fs::File` / `spawn_blocking`)
//!
//! Workloads:
//!   1. Sequential 4 KiB Writes (4,096 blocks = 16 MiB) + `sync_data`
//!   2. Concurrent Random 4 KiB Reads (QD=32, 8,192 reads across 16 MiB file)
//!   3. Sequential 64 KiB Reads (1,024 blocks = 64 MiB)

use std::{
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

struct BenchRow {
    backend: &'static str,
    workload: &'static str,
    ops: usize,
    bytes: usize,
    elapsed: Duration,
}

impl BenchRow {
    fn iops(&self) -> f64 {
        self.ops as f64 / self.elapsed.as_secs_f64()
    }
    fn mb_s(&self) -> f64 {
        (self.bytes as f64 / (1024.0 * 1024.0)) / self.elapsed.as_secs_f64()
    }
}

fn temp_file(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("nexio_bench_{}_{}.dat", name, std::process::id()))
}

fn make_offsets(count: usize, file_blocks: usize, block_size: usize) -> Vec<u64> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let blk = ((state >> 32) as usize) % file_blocks;
        out.push((blk * block_size) as u64);
    }
    out
}

// ── Nexio ─────────────────────────────────────────────────────────────────────

fn bench_nexio(path: &Path, offsets: &[u64]) -> Vec<BenchRow> {
    let mut rows = Vec::new();
    const SEQ_WRITES: usize = 4096;
    const BLK4: usize = 4096;
    const SEQ64_CNT: usize = 1024;
    const BLK64: usize = 65536;

    nexio::block_on(async {
        // 1. Sequential 4 KiB write + sync_data
        let _ = std::fs::remove_file(path);
        let file = nexio::fs::File::create(path).await.unwrap();
        let mut buf = vec![0xA5u8; BLK4];
        let t0 = Instant::now();
        for i in 0..SEQ_WRITES {
            let (res, b) = file.write_all_at(buf, (i * BLK4) as u64).await;
            res.unwrap();
            buf = b;
        }
        file.sync_data().await.unwrap();
        let elapsed = t0.elapsed();
        file.close().await.unwrap();
        rows.push(BenchRow {
            backend: "nexio",
            workload: "seq_write_4k+fsync",
            ops: SEQ_WRITES,
            bytes: SEQ_WRITES * BLK4,
            elapsed,
        });

        // 2. Concurrent Random 4 KiB Reads (QD=32)
        let file = Rc::new(nexio::fs::File::open(path).await.unwrap());
        let qd = 32usize;
        let per_task = offsets.len() / qd;
        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(qd);
        for w in 0..qd {
            let f = Rc::clone(&file);
            let slice = offsets[w * per_task..(w + 1) * per_task].to_vec();
            handles.push(nexio::spawn(async move {
                let mut b = vec![0u8; BLK4];
                for off in slice {
                    let (res, out) = f.read_exact_at(b, off).await;
                    res.unwrap();
                    b = out;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        rows.push(BenchRow {
            backend: "nexio",
            workload: "rand_read_4k_qd32",
            ops: qd * per_task,
            bytes: qd * per_task * BLK4,
            elapsed,
        });
        drop(file);

        // 3. Sequential 64 KiB Reads (across 16 MiB wrapping)
        let file = nexio::fs::File::open(path).await.unwrap();
        let mut buf = vec![0u8; BLK64];
        let t0 = Instant::now();
        for i in 0..SEQ64_CNT {
            let off = ((i % 256) * BLK64) as u64;
            let (res, b) = file.read_exact_at(buf, off).await;
            res.unwrap();
            buf = b;
        }
        let elapsed = t0.elapsed();
        file.close().await.unwrap();
        rows.push(BenchRow {
            backend: "nexio",
            workload: "seq_read_64k",
            ops: SEQ64_CNT,
            bytes: SEQ64_CNT * BLK64,
            elapsed,
        });
    });

    rows
}

// ── Compio ────────────────────────────────────────────────────────────────────

fn bench_compio(path: &Path, offsets: &[u64]) -> Vec<BenchRow> {
    use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
    let mut rows = Vec::new();
    const SEQ_WRITES: usize = 4096;
    const BLK4: usize = 4096;
    const SEQ64_CNT: usize = 1024;
    const BLK64: usize = 65536;

    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let _ = std::fs::remove_file(path);
        let mut file = compio::fs::File::create(path).await.unwrap();
        let mut buf = vec![0xA5u8; BLK4];
        let t0 = Instant::now();
        for i in 0..SEQ_WRITES {
            let compio::BufResult(res, b) = file.write_all_at(buf, (i * BLK4) as u64).await;
            res.unwrap();
            buf = b;
        }
        file.sync_data().await.unwrap();
        let elapsed = t0.elapsed();
        file.close().await.unwrap();
        rows.push(BenchRow {
            backend: "compio",
            workload: "seq_write_4k+fsync",
            ops: SEQ_WRITES,
            bytes: SEQ_WRITES * BLK4,
            elapsed,
        });

        let file = Rc::new(compio::fs::File::open(path).await.unwrap());
        let qd = 32usize;
        let per_task = offsets.len() / qd;
        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(qd);
        for w in 0..qd {
            let f = Rc::clone(&file);
            let slice = offsets[w * per_task..(w + 1) * per_task].to_vec();
            handles.push(compio::runtime::spawn(async move {
                let mut b = vec![0u8; BLK4];
                for off in slice {
                    let compio::BufResult(res, out) = f.read_exact_at(b, off).await;
                    res.unwrap();
                    b = out;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        rows.push(BenchRow {
            backend: "compio",
            workload: "rand_read_4k_qd32",
            ops: qd * per_task,
            bytes: qd * per_task * BLK4,
            elapsed,
        });
        drop(file);

        let file = compio::fs::File::open(path).await.unwrap();
        let mut buf = vec![0u8; BLK64];
        let t0 = Instant::now();
        for i in 0..SEQ64_CNT {
            let off = ((i % 256) * BLK64) as u64;
            let compio::BufResult(res, b) = file.read_exact_at(buf, off).await;
            res.unwrap();
            buf = b;
        }
        let elapsed = t0.elapsed();
        file.close().await.unwrap();
        rows.push(BenchRow {
            backend: "compio",
            workload: "seq_read_64k",
            ops: SEQ64_CNT,
            bytes: SEQ64_CNT * BLK64,
            elapsed,
        });
    });

    rows
}

// ── Monoio ────────────────────────────────────────────────────────────────────

fn bench_monoio(path: &Path, offsets: &[u64]) -> Vec<BenchRow> {
    let mut rows = Vec::new();
    const SEQ_WRITES: usize = 4096;
    const BLK4: usize = 4096;
    const SEQ64_CNT: usize = 1024;
    const BLK64: usize = 65536;

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let _ = std::fs::remove_file(path);
        let file = monoio::fs::File::create(path).await.unwrap();
        let mut buf = vec![0xA5u8; BLK4];
        let t0 = Instant::now();
        for i in 0..SEQ_WRITES {
            let (res, b) = file.write_all_at(buf, (i * BLK4) as u64).await;
            res.unwrap();
            buf = b;
        }
        file.sync_data().await.unwrap();
        let elapsed = t0.elapsed();
        file.close().await.unwrap();
        rows.push(BenchRow {
            backend: "monoio",
            workload: "seq_write_4k+fsync",
            ops: SEQ_WRITES,
            bytes: SEQ_WRITES * BLK4,
            elapsed,
        });

        let file = Rc::new(monoio::fs::File::open(path).await.unwrap());
        let qd = 32usize;
        let per_task = offsets.len() / qd;
        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(qd);
        for w in 0..qd {
            let f = Rc::clone(&file);
            let slice = offsets[w * per_task..(w + 1) * per_task].to_vec();
            handles.push(monoio::spawn(async move {
                let mut b = vec![0u8; BLK4];
                for off in slice {
                    let (res, out) = f.read_exact_at(b, off).await;
                    res.unwrap();
                    b = out;
                }
            }));
        }
        for h in handles {
            h.await;
        }
        let elapsed = t0.elapsed();
        rows.push(BenchRow {
            backend: "monoio",
            workload: "rand_read_4k_qd32",
            ops: qd * per_task,
            bytes: qd * per_task * BLK4,
            elapsed,
        });
        drop(file);

        let file = monoio::fs::File::open(path).await.unwrap();
        let mut buf = vec![0u8; BLK64];
        let t0 = Instant::now();
        for i in 0..SEQ64_CNT {
            let off = ((i % 256) * BLK64) as u64;
            let (res, b) = file.read_exact_at(buf, off).await;
            res.unwrap();
            buf = b;
        }
        let elapsed = t0.elapsed();
        file.close().await.unwrap();
        rows.push(BenchRow {
            backend: "monoio",
            workload: "seq_read_64k",
            ops: SEQ64_CNT,
            bytes: SEQ64_CNT * BLK64,
            elapsed,
        });
    });

    rows
}

// ── Tokio (current-thread runtime + tokio::fs spawn_blocking pool) ────────────

fn bench_tokio(path: &Path, offsets: &[u64]) -> Vec<BenchRow> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    let mut rows = Vec::new();
    const SEQ_WRITES: usize = 4096;
    const BLK4: usize = 4096;
    const SEQ64_CNT: usize = 1024;
    const BLK64: usize = 65536;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let _ = std::fs::remove_file(path);
        let mut file = tokio::fs::File::create(path).await.unwrap();
        let buf = vec![0xA5u8; BLK4];
        let t0 = Instant::now();
        for _ in 0..SEQ_WRITES {
            file.write_all(&buf).await.unwrap();
        }
        file.sync_data().await.unwrap();
        let elapsed = t0.elapsed();
        drop(file);
        rows.push(BenchRow {
            backend: "tokio",
            workload: "seq_write_4k+fsync",
            ops: SEQ_WRITES,
            bytes: SEQ_WRITES * BLK4,
            elapsed,
        });

        let qd = 32usize;
        let per_task = offsets.len() / qd;
        let p = Arc::new(path.to_path_buf());
        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(qd);
        for w in 0..qd {
            let path_clone = Arc::clone(&p);
            let slice = offsets[w * per_task..(w + 1) * per_task].to_vec();
            handles.push(tokio::spawn(async move {
                let std_file = std::fs::File::open(&*path_clone).unwrap();
                let mut f = tokio::fs::File::from_std(std_file);
                let mut b = vec![0u8; BLK4];
                for off in slice {
                    f.seek(std::io::SeekFrom::Start(off)).await.unwrap();
                    f.read_exact(&mut b).await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = t0.elapsed();
        rows.push(BenchRow {
            backend: "tokio",
            workload: "rand_read_4k_qd32",
            ops: qd * per_task,
            bytes: qd * per_task * BLK4,
            elapsed,
        });

        let mut file = tokio::fs::File::open(path).await.unwrap();
        let mut buf = vec![0u8; BLK64];
        let t0 = Instant::now();
        for i in 0..SEQ64_CNT {
            let off = ((i % 256) * BLK64) as u64;
            file.seek(std::io::SeekFrom::Start(off)).await.unwrap();
            file.read_exact(&mut buf).await.unwrap();
        }
        let elapsed = t0.elapsed();
        drop(file);
        rows.push(BenchRow {
            backend: "tokio",
            workload: "seq_read_64k",
            ops: SEQ64_CNT,
            bytes: SEQ64_CNT * BLK64,
            elapsed,
        });
    });

    rows
}

fn main() {
    let path = temp_file("file_io");
    let offsets = make_offsets(8192, 4096, 4096);

    let mut all = Vec::new();
    all.extend(bench_nexio(&path, &offsets));
    all.extend(bench_compio(&path, &offsets));
    all.extend(bench_monoio(&path, &offsets));
    all.extend(bench_tokio(&path, &offsets));

    let _ = std::fs::remove_file(&path);

    println!("| Backend | Workload | Ops | Time (ms) | Throughput (IOPS) | Bandwidth (MB/s) |");
    println!("|:---|:---|---:|---:|---:|---:|");
    for r in &all {
        println!(
            "| `{}` | {} | {} | {:.2} | {:.0} | {:.2} |",
            r.backend,
            r.workload,
            r.ops,
            r.elapsed.as_secs_f64() * 1000.0,
            r.iops(),
            r.mb_s()
        );
    }
}
