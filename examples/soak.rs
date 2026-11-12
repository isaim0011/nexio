//! Soak test: sustained connection churn with random cancellation, watching
//! for leaked file descriptors and growing memory.
//!
//! One reactor hosts an echo server and many client loops that, at random,
//! complete a verified ping-pong, drop a connection immediately, cancel an
//! in-flight read via a timeout (dropping the future with its buffer
//! in flight), or write a large payload and vanish without reading it.
//!
//!     cargo run --release --example soak -- <seconds> [clients]
//!
//! Exit status 0 only if no echo mismatched, no connection operation failed
//! unexpectedly, and file descriptors and resident memory returned to
//! (near) their post-warmup baseline after the load stopped.

use nexio::{net::TcpListener, net::TcpStream, time};
use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

fn rss_kb() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            s.split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<usize>().ok())
        })
        .map(|pages| pages * 4)
        .unwrap_or(0)
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Default)]
struct Stats {
    pingpong: Cell<u64>,
    dropped_immediately: Cell<u64>,
    cancelled_reads: Cell<u64>,
    completed_reads: Cell<u64>,
    abandoned_writes: Cell<u64>,
    mismatches: Cell<u64>,
    connect_errors: Cell<u64>,
}

fn bump(c: &Cell<u64>) {
    c.set(c.get() + 1);
}

fn pattern(seed: u64, n: usize) -> Vec<u8> {
    (0..n)
        .map(|i| (seed as u8).wrapping_add((i as u8).wrapping_mul(13)))
        .collect()
}

fn main() {
    let secs: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let clients: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    let ok = nexio::block_on(async move {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().unwrap();

        // Echo server: handlers are detached; each ends when its peer goes away.
        drop(nexio::spawn(async move {
            loop {
                let Ok(stream) = listener.accept().await else {
                    break;
                };
                drop(nexio::spawn(async move {
                    let mut buf = Vec::with_capacity(4096);
                    loop {
                        let (res, b) = stream.read(buf).await;
                        buf = b;
                        if matches!(res, Ok(0) | Err(_)) {
                            break;
                        }
                        let (res, b) = stream.write_all(buf).await;
                        buf = b;
                        if res.is_err() {
                            break;
                        }
                    }
                }));
            }
        }));

        let stats = Rc::new(Stats::default());
        let stop = Rc::new(Cell::new(false));
        let mut handles = Vec::new();
        for c in 0..clients {
            let stats = stats.clone();
            let stop = stop.clone();
            handles.push(nexio::spawn(async move {
                let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ ((c as u64 + 1) * 0xBF58_476D_1CE4_E5B9));
                while !stop.get() {
                    let stream = match TcpStream::connect_addr(addr).await {
                        Ok(s) => s,
                        Err(_) => {
                            bump(&stats.connect_errors);
                            continue;
                        }
                    };
                    match rng.below(10) {
                        0..=5 => {
                            for _ in 0..(1 + rng.below(8)) {
                                let n = 1 + rng.below(200) as usize;
                                let msg = pattern(rng.next(), n);
                                let (res, _) = stream.write_all(msg.clone()).await;
                                if res.is_err() {
                                    break;
                                }
                                let (res, echo) = stream.read_exact(vec![0u8; n]).await;
                                if res.is_err() {
                                    break;
                                }
                                if echo != msg {
                                    bump(&stats.mismatches);
                                }
                            }
                            bump(&stats.pingpong);
                        }
                        6 => bump(&stats.dropped_immediately),
                        7 => {
                            let (res, _) = stream.write_all(pattern(1, 32)).await;
                            if res.is_ok() {
                                let wait = Duration::from_micros(50 + rng.below(3000));
                                match time::timeout(wait, stream.read(Vec::with_capacity(64))).await
                                {
                                    Ok(_) => bump(&stats.completed_reads),
                                    Err(_) => bump(&stats.cancelled_reads),
                                }
                            }
                        }
                        _ => {
                            // Large write, then vanish while the server is still echoing.
                            let _ = time::timeout(
                                Duration::from_millis(2),
                                stream.write_all(pattern(2, 64 * 1024)),
                            )
                            .await;
                            bump(&stats.abandoned_writes);
                        }
                    }
                    // `stream` is dropped here, possibly with operations still in flight.
                }
            }));
        }

        let start = Instant::now();
        let mut baseline: Option<(usize, usize)> = None;
        let mut last_report = Instant::now();
        while start.elapsed() < Duration::from_secs(secs) {
            time::sleep(Duration::from_millis(250)).await;
            if baseline.is_none() && start.elapsed() > Duration::from_secs(3) {
                baseline = Some((fd_count(), rss_kb()));
                println!(
                    "baseline after warmup: fds={} rss={} KiB",
                    baseline.unwrap().0,
                    baseline.unwrap().1
                );
            }
            if last_report.elapsed() >= Duration::from_secs(5) {
                last_report = Instant::now();
                println!(
                    "t={:>4}s fds={:<5} rss={:<7}KiB pingpong={} cancelled_reads={} completed_reads={} abandoned={} mismatches={}",
                    start.elapsed().as_secs(),
                    fd_count(),
                    rss_kb(),
                    stats.pingpong.get(),
                    stats.cancelled_reads.get(),
                    stats.completed_reads.get(),
                    stats.abandoned_writes.get(),
                    stats.mismatches.get(),
                );
            }
        }

        stop.set(true);
        for h in handles {
            let _ = h.await;
        }
        // Let the server drain its handlers and the kernel finish cancelled operations.
        time::sleep(Duration::from_secs(2)).await;

        let (fds0, rss0) = baseline.unwrap_or((0, 0));
        let (fds1, rss1) = (fd_count(), rss_kb());
        println!(
            "after load stopped: fds={fds1} (baseline {fds0})  rss={rss1} KiB (baseline {rss0} KiB)"
        );
        println!(
            "totals: pingpong={} dropped_immediately={} cancelled_reads={} completed_reads={} abandoned_writes={} connect_errors={} mismatches={}",
            stats.pingpong.get(),
            stats.dropped_immediately.get(),
            stats.cancelled_reads.get(),
            stats.completed_reads.get(),
            stats.abandoned_writes.get(),
            stats.connect_errors.get(),
            stats.mismatches.get()
        );

        let mut ok = true;
        if stats.mismatches.get() != 0 {
            println!("FAIL: echo mismatches");
            ok = false;
        }
        if stats.connect_errors.get() != 0 {
            println!("FAIL: unexpected connect errors");
            ok = false;
        }
        // Allow a few fds of slack (listener, ring, a straggler); a leak would grow with load.
        if fds1 > fds0 + 8 {
            println!(
                "FAIL: file descriptors grew by {}",
                fds1 as isize - fds0 as isize
            );
            ok = false;
        }
        // Allow 25% + 8 MiB for allocator behaviour; a leak grows without bound.
        if rss1 > rss0 + rss0 / 4 + 8 * 1024 {
            println!("FAIL: resident memory grew from {rss0} to {rss1} KiB");
            ok = false;
        }
        println!("{}", if ok { "PASS" } else { "FAIL" });
        ok
    });
    std::process::exit(if ok { 0 } else { 1 });
}
