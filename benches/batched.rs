//! Batched small-op benchmark: the workload where per-op syscall and
//! scheduling overhead should matter most, so any real submission-batching
//! advantage would show up here.
//!
//! One backend per process (so CPU time is attributable), page-cache
//! resident 16 MiB file, random reads.
//!
//! Usage: batched <nexio|compio|monoio> <queue_depth> <read_size> <total_ops>
//! Output: one line `backend qd size ops secs iops cpu_us_per_op`.

use std::{
    path::PathBuf,
    rc::Rc,
    time::{Duration, Instant},
};

const FILE_BYTES: usize = 16 * 1024 * 1024;

fn cpu_time() -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

fn offsets(count: usize, size: usize) -> Vec<u64> {
    let blocks = FILE_BYTES / size;
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..count)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            ((x as usize % blocks) * size) as u64
        })
        .collect()
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 5 {
        eprintln!("usage: batched <nexio|compio|monoio> <qd> <size> <total_ops>");
        std::process::exit(2);
    }
    let backend = a[1].clone();
    let qd: usize = a[2].parse().unwrap();
    let size: usize = a[3].parse().unwrap();
    let total: usize = a[4].parse().unwrap();
    let per_task = total / qd;
    let total = per_task * qd;

    let path: PathBuf =
        std::env::temp_dir().join(format!("nexio_batched_{}.dat", std::process::id()));
    std::fs::write(&path, vec![0xA5u8; FILE_BYTES]).unwrap();
    let offs = offsets(total, size);

    let cpu0 = cpu_time();
    let t0 = Instant::now();
    match backend.as_str() {
        "nexio" => nexio::block_on(async {
            let noatime = std::env::var_os("NEXIO_NOATIME").is_some();
            let file = Rc::new(
                nexio::fs::OpenOptions::new()
                    .read(true)
                    .noatime(noatime)
                    .open(&path)
                    .await
                    .unwrap(),
            );
            let mut hs = Vec::with_capacity(qd);
            for w in 0..qd {
                let f = Rc::clone(&file);
                let slice = offs[w * per_task..(w + 1) * per_task].to_vec();
                hs.push(nexio::spawn(async move {
                    let mut b = vec![0u8; size];
                    for off in slice {
                        let (r, o) = f.read_exact_at(b, off).await;
                        r.unwrap();
                        b = o;
                    }
                }));
            }
            for h in hs {
                h.await.unwrap();
            }
        }),
        "compio" => compio::runtime::Runtime::new().unwrap().block_on(async {
            use compio::io::AsyncReadAtExt;
            let file = Rc::new(compio::fs::File::open(&path).await.unwrap());
            let mut hs = Vec::with_capacity(qd);
            for w in 0..qd {
                let f = Rc::clone(&file);
                let slice = offs[w * per_task..(w + 1) * per_task].to_vec();
                hs.push(compio::runtime::spawn(async move {
                    let mut b = vec![0u8; size];
                    for off in slice {
                        let compio::BufResult(r, o) = f.read_exact_at(b, off).await;
                        r.unwrap();
                        b = o;
                    }
                }));
            }
            for h in hs {
                h.await.unwrap();
            }
        }),
        "monoio" => monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let file = Rc::new(monoio::fs::File::open(&path).await.unwrap());
                let mut hs = Vec::with_capacity(qd);
                for w in 0..qd {
                    let f = Rc::clone(&file);
                    let slice = offs[w * per_task..(w + 1) * per_task].to_vec();
                    hs.push(monoio::spawn(async move {
                        let mut b = vec![0u8; size];
                        for off in slice {
                            let (r, o) = f.read_exact_at(b, off).await;
                            r.unwrap();
                            b = o;
                        }
                    }));
                }
                for h in hs {
                    h.await;
                }
            }),
        other => {
            eprintln!("unknown backend {other}");
            std::process::exit(2);
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    let cpu = (cpu_time() - cpu0).as_secs_f64();
    let _ = std::fs::remove_file(&path);
    println!(
        "{backend} qd={qd} size={size} ops={total} secs={secs:.3} iops={:.0} cpu_us_per_op={:.3}",
        total as f64 / secs,
        cpu * 1e6 / total as f64
    );
}
