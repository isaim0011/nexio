//! Scheduler micro-benchmarks: the runtime overhead that does not involve the kernel.
//!
//! Compares nexio with tokio's current-thread runtime on identical work, one thread each:
//!   spawn   - spawn N trivial tasks and await every handle
//!   yield   - N tasks that each yield K times (scheduler round trips)
//!   channel - one producer and one consumer exchanging N messages through a bounded channel
//!   timer   - zero-length sleeps awaited one after another (tokio rounds up to its 1 ms tick,
//!             so its count is much smaller; compare ns_per_item, not the totals)
//!
//! Usage: tasks <nexio|tokio> <n>
//! Output: one line per case: `backend case n secs ns_per_item`.

use std::time::Instant;

fn report(backend: &str, case: &str, n: usize, secs: f64) {
    println!(
        "{backend} {case} n={n} secs={secs:.3} ns_per_item={:.1}",
        secs * 1e9 / n as f64
    );
}

fn nexio_cases(n: usize) {
    use nexio::{block_on, spawn, yield_now};

    let t = Instant::now();
    block_on(async {
        let mut hs = Vec::with_capacity(1024);
        for i in 0..n {
            hs.push(spawn(async move { i }));
            if hs.len() == 1024 {
                for h in hs.drain(..) {
                    h.await.unwrap();
                }
            }
        }
        for h in hs {
            h.await.unwrap();
        }
    });
    report("nexio", "spawn", n, t.elapsed().as_secs_f64());

    let tasks = 64usize;
    let per = n / tasks;
    let t = Instant::now();
    block_on(async {
        let mut hs = Vec::new();
        for _ in 0..tasks {
            hs.push(spawn(async move {
                for _ in 0..per {
                    yield_now().await;
                }
            }));
        }
        for h in hs {
            h.await.unwrap();
        }
    });
    report("nexio", "yield", per * tasks, t.elapsed().as_secs_f64());

    let t = Instant::now();
    block_on(async {
        let (tx, mut rx) = nexio::executor::channel::mpsc::channel::<usize>(64);
        let prod = spawn(async move {
            for i in 0..n {
                tx.send(i).await.unwrap();
            }
        });
        let mut sum = 0usize;
        while let Some(v) = rx.recv().await {
            sum = sum.wrapping_add(v);
        }
        prod.await.unwrap();
        std::hint::black_box(sum);
    });
    report("nexio", "channel", n, t.elapsed().as_secs_f64());

    let m = n / 10;
    let t = Instant::now();
    block_on(async {
        for _ in 0..m {
            nexio::time::sleep(std::time::Duration::ZERO).await;
        }
    });
    report("nexio", "timer", m, t.elapsed().as_secs_f64());
}

fn tokio_cases(n: usize) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();

    let t = Instant::now();
    local.block_on(&rt, async {
        let mut hs = Vec::with_capacity(1024);
        for i in 0..n {
            hs.push(tokio::task::spawn_local(async move { i }));
            if hs.len() == 1024 {
                for h in hs.drain(..) {
                    h.await.unwrap();
                }
            }
        }
        for h in hs {
            h.await.unwrap();
        }
    });
    report("tokio", "spawn", n, t.elapsed().as_secs_f64());

    let tasks = 64usize;
    let per = n / tasks;
    let t = Instant::now();
    local.block_on(&rt, async {
        let mut hs = Vec::new();
        for _ in 0..tasks {
            hs.push(tokio::task::spawn_local(async move {
                for _ in 0..per {
                    tokio::task::yield_now().await;
                }
            }));
        }
        for h in hs {
            h.await.unwrap();
        }
    });
    report("tokio", "yield", per * tasks, t.elapsed().as_secs_f64());

    let t = Instant::now();
    local.block_on(&rt, async {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<usize>(64);
        let prod = tokio::task::spawn_local(async move {
            for i in 0..n {
                tx.send(i).await.unwrap();
            }
        });
        let mut sum = 0usize;
        while let Some(v) = rx.recv().await {
            sum = sum.wrapping_add(v);
        }
        prod.await.unwrap();
        std::hint::black_box(sum);
    });
    report("tokio", "channel", n, t.elapsed().as_secs_f64());

    // tokio's timer wheel has 1 ms granularity, so even a zero-length sleep waits for the next
    // tick; keep this case small enough to finish.
    let m = (n / 4000).max(1);
    let t = Instant::now();
    local.block_on(&rt, async {
        for _ in 0..m {
            tokio::time::sleep(std::time::Duration::ZERO).await;
        }
    });
    report("tokio", "timer", m, t.elapsed().as_secs_f64());
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 3 {
        eprintln!("usage: tasks <nexio|tokio> <n>");
        std::process::exit(2);
    }
    let n: usize = a[2].parse().unwrap();
    match a[1].as_str() {
        "nexio" => nexio_cases(n),
        "tokio" => tokio_cases(n),
        other => {
            eprintln!("unknown backend {other}");
            std::process::exit(2);
        }
    }
}
