//! Honest 3-way (and multi-runtime) TCP echo benchmark.
//!
//! From ROADMAP.md:
//! > A tokio-rs/io-uring maintainer issue (#291) found a naive single-ring
//! > io_uring receiver losing to multi-threaded Tokio by 2x until
//! > SO_REUSEPORT sharding was added, at which point the two converged exactly.
//! > Nexio should not chase a speed story the evidence doesn't support at small scale.
//! > Beat them on verified correctness first, chase performance second, and only with evidence.
//!
//! Variants evaluated:
//!   1. `tokio_unsharded` — multi-threaded Tokio runtime, 1 listening socket
//!   2. `tokio_sharded`   — N single-threaded Tokio runtimes with SO_REUSEPORT (N = num_cpus)
//!   3. `nexio_sharded`   — N single-threaded nexio reactors with SO_REUSEPORT (N = num_cpus)
//!   4. `compio_sharded`  — N compio io_uring runtimes with SO_REUSEPORT (reference)
//!   5. `monoio_sharded`  — N monoio io_uring runtimes with SO_REUSEPORT (reference)
//!
//! All servers are subjected to a 100% identical, fair load generator:
//!   - High-performance multi-threaded client connecting C concurrent connections
//!   - M request-response round-trips per connection of S bytes
//!   - TCP_NODELAY enabled on all sockets to prevent Nagle distortion
//!   - High-resolution per-request latency recording (nanosecond precision, reported in µs)
//!   - Computes throughput (msg/s, MB/s) and latency distribution (min, p50, p90, p95, p99, p99.9, max, mean)
//!
//! Run with:
//!   cargo bench --bench echo
//!   cargo run --release --bench echo -- [OPTIONS]
//!
//! Options:
//!   --connections <N>   Concurrent connections (default: 100)
//!   --messages <N>      Messages per connection (default: 500)
//!   --msg-size <N>      Message size in bytes (default: 128, 1024, 8192)
//!   --shards <N>        Number of server shards (default: num_cpus)
//!   --backends <list>   Comma-separated backends (default: 3way)
//!   --quick             Fast sanity run for CI (20 conns, 100 msgs)
//!   --compare-modes     Evaluate and compare Moderate (100 conns) vs High-Concurrency (500 conns)
//!   --json              Emit JSON results
//!   --markdown          Emit Markdown summary table

use std::{
    fs,
    net::TcpStream as StdTcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

// ── Metrics & Configuration ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BenchMetrics {
    pub backend: String,
    pub mode: String,
    pub msg_size: usize,
    pub connections: usize,
    pub messages_per_conn: usize,
    pub total_messages: usize,
    pub wall_time: Duration,
    pub throughput_msgs_sec: f64,
    pub bandwidth_mb_sec: f64,
    pub min_us: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
}

#[derive(Debug, Clone)]
struct BenchConfig {
    connections: usize,
    messages_per_conn: usize,
    msg_sizes: Vec<usize>,
    shards: usize,
    backends: Vec<String>,
    quick: bool,
    json: bool,
    markdown: bool,
    compare_modes: bool,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            connections: 100,
            messages_per_conn: 500,
            msg_sizes: vec![128, 1024, 8192],
            shards: num_cpus::get(),
            backends: vec![
                "tokio_unsharded".into(),
                "tokio_sharded".into(),
                "nexio_sharded".into(),
                "compio_sharded".into(),
                "monoio_sharded".into(),
            ],
            quick: false,
            json: false,
            markdown: false,
            compare_modes: false,
        }
    }
}

#[derive(Debug, Clone)]
struct ConcurrencyMode {
    name: &'static str,
    connections: usize,
    messages_per_conn: usize,
}

// ── Hardware & Environment Metadata ──────────────────────────────────────────

fn print_hw_metadata() {
    let kernel = fs::read_to_string("/proc/sys/kernel/osrelease")
        .unwrap_or_else(|_| "unknown".into())
        .trim()
        .to_string();

    let cpu_model = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|info| {
            for line in info.lines() {
                if line.starts_with("model name") {
                    if let Some((_, model)) = line.split_once(':') {
                        return Some(model.trim().to_string());
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| "Unknown CPU".into());

    println!("================================================================================");
    println!(" nexio Honest 3-Way Echo Benchmark");
    println!(
        " System: {} vCPUs | CPU: {} | Linux kernel {}",
        num_cpus::get(),
        cpu_model,
        kernel
    );
    println!("================================================================================");
}

#[cfg(unix)]
fn raise_fd_limit() {
    unsafe {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            let target = 65536.min(rlim.rlim_max);
            if rlim.rlim_cur < target {
                rlim.rlim_cur = target;
                let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
            }
        }
    }
}

#[cfg(not(unix))]
fn raise_fd_limit() {}

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral port bind");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

fn create_reuseport_socket(port: u16) -> std::io::Result<socket2::Socket> {
    use socket2::{Domain, Socket, Type};
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None)?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    socket.bind(&addr.into())?;
    socket.listen(2048)?;
    Ok(socket)
}

// ── 100% Identical Load Generator ─────────────────────────────────────────────

fn run_load_generator(
    backend_name: &str,
    mode_name: &str,
    port: u16,
    connections: usize,
    messages_per_conn: usize,
    msg_size: usize,
) -> Result<BenchMetrics, Box<dyn std::error::Error + Send + Sync>> {
    let client_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    client_rt.block_on(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // Warmup: prime buffers & connections
        if let Ok(mut warmup) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = warmup.set_nodelay(true);
            let payload = vec![0x42u8; msg_size];
            let mut buf = vec![0u8; msg_size];
            for _ in 0..10 {
                if warmup.write_all(&payload).await.is_err()
                    || warmup.read_exact(&mut buf).await.is_err()
                {
                    break;
                }
            }
        }

        let start = Instant::now();
        let mut handles = Vec::with_capacity(connections);

        for _ in 0..connections {
            handles.push(tokio::spawn(async move {
                let mut stream = None;
                for attempt in 0..5 {
                    match TcpStream::connect(("127.0.0.1", port)).await {
                        Ok(s) => {
                            stream = Some(s);
                            break;
                        }
                        Err(_) if attempt < 4 => {
                            tokio::time::sleep(Duration::from_millis(15)).await;
                        }
                        Err(e) => return Err(e),
                    }
                }
                let mut stream = stream.expect("connected stream");
                stream.set_nodelay(true)?;
                let payload = vec![0x42u8; msg_size];
                let mut buf = vec![0u8; msg_size];
                let mut lats = Vec::with_capacity(messages_per_conn);

                for _ in 0..messages_per_conn {
                    let t0 = Instant::now();
                    stream.write_all(&payload).await?;
                    stream.read_exact(&mut buf).await?;
                    let rtt_us = t0.elapsed().as_nanos() as f64 / 1000.0;
                    lats.push(rtt_us);
                }
                Ok::<Vec<f64>, std::io::Error>(lats)
            }));
        }

        let mut all_latencies = Vec::with_capacity(connections * messages_per_conn);
        for h in handles {
            let task_lats = h.await??;
            all_latencies.extend(task_lats);
        }

        let wall_time = start.elapsed();
        let total_messages = all_latencies.len();
        let throughput_msgs_sec = total_messages as f64 / wall_time.as_secs_f64();
        let bandwidth_mb_sec =
            (total_messages * msg_size) as f64 / (1024.0 * 1024.0 * wall_time.as_secs_f64());

        all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = all_latencies.len();
        let min_us = all_latencies.first().copied().unwrap_or(0.0);
        let max_us = all_latencies.last().copied().unwrap_or(0.0);
        let p50_us = all_latencies.get(n * 50 / 100).copied().unwrap_or(0.0);
        let p90_us = all_latencies.get(n * 90 / 100).copied().unwrap_or(0.0);
        let p95_us = all_latencies.get(n * 95 / 100).copied().unwrap_or(0.0);
        let p99_us = all_latencies.get(n * 99 / 100).copied().unwrap_or(0.0);
        let p999_us = all_latencies.get(n * 999 / 1000).copied().unwrap_or(0.0);
        let mean_us = if n > 0 {
            all_latencies.iter().sum::<f64>() / n as f64
        } else {
            0.0
        };

        Ok(BenchMetrics {
            backend: backend_name.to_string(),
            mode: mode_name.to_string(),
            msg_size,
            connections,
            messages_per_conn,
            total_messages,
            wall_time,
            throughput_msgs_sec,
            bandwidth_mb_sec,
            min_us,
            p50_us,
            p90_us,
            p95_us,
            p99_us,
            p999_us,
            max_us,
            mean_us,
        })
    })
}

// ── Backend 1: Tokio Unsharded ────────────────────────────────────────────────

fn start_tokio_unsharded(
    port: u16,
    shards: usize,
    msg_size: usize,
    ready_tx: mpsc::Sender<Result<(), String>>,
) -> (thread::JoinHandle<()>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);

    let handle = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(shards)
            .enable_all()
            .build()
            .expect("tokio multi-thread rt");

        rt.block_on(async move {
            let listener = match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
                Ok(l) => l,
                Err(e) => {
                    let _ = ready_tx.send(Err(format!("tokio_unsharded bind failed: {e}")));
                    return;
                }
            };
            let _ = ready_tx.send(Ok(()));

            while !stop_clone.load(Ordering::Relaxed) {
                tokio::select! {
                    res = listener.accept() => {
                        match res {
                            Ok((stream, _)) => {
                                let _ = stream.set_nodelay(true);
                                tokio::spawn(async move {
                                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                    let mut stream = stream;
                                    let mut buf = vec![0u8; msg_size];
                                    while stream.read_exact(&mut buf).await.is_ok() {
                                        if stream.write_all(&buf).await.is_err() {
                                            break;
                                        }
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
            }
        });
    });

    (handle, stop)
}

// ── Backend 2: Tokio Sharded (SO_REUSEPORT) ───────────────────────────────────

fn start_tokio_sharded(
    port: u16,
    shards: usize,
    msg_size: usize,
    ready_tx: mpsc::Sender<Result<(), String>>,
) -> (Vec<thread::JoinHandle<()>>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let mut handles = Vec::with_capacity(shards);

    for shard in 0..shards {
        let stop_clone = Arc::clone(&stop);
        let ready_tx = ready_tx.clone();
        let core_id = if !core_ids.is_empty() {
            Some(core_ids[shard % core_ids.len()])
        } else {
            None
        };

        handles.push(thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio current-thread rt");

            rt.block_on(async move {
                let socket = match create_reuseport_socket(port) {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("tokio_sharded socket failed: {e}")));
                        return;
                    }
                };
                let std_listener: std::net::TcpListener = socket.into();
                let listener = match tokio::net::TcpListener::from_std(std_listener) {
                    Ok(l) => l,
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("tokio_sharded from_std failed: {e}")));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));

                while !stop_clone.load(Ordering::Relaxed) {
                    tokio::select! {
                        res = listener.accept() => {
                            match res {
                                Ok((stream, _)) => {
                                    let _ = stream.set_nodelay(true);
                                    tokio::spawn(async move {
                                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                        let mut stream = stream;
                                        let mut buf = vec![0u8; msg_size];
                                        while stream.read_exact(&mut buf).await.is_ok() {
                                            if stream.write_all(&buf).await.is_err() {
                                                break;
                                            }
                                        }
                                    });
                                }
                                Err(_) => break,
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                    }
                }
            });
        }));
    }

    (handles, stop)
}

// ── Backend 3: nexio Sharded (SO_REUSEPORT) ───────────────────────────────────

fn start_nexio_sharded(
    port: u16,
    shards: usize,
    msg_size: usize,
    ready_tx: mpsc::Sender<Result<(), String>>,
) -> (Vec<thread::JoinHandle<()>>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let mut handles = Vec::with_capacity(shards);

    for shard in 0..shards {
        let stop_clone = Arc::clone(&stop);
        let ready_tx = ready_tx.clone();
        let ready_tx_fail = ready_tx.clone();
        let core_id = if !core_ids.is_empty() {
            Some(core_ids[shard % core_ids.len()])
        } else {
            None
        };

        handles.push(thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                nexio::block_on(async move {
                    let listener = match nexio::net::TcpListener::bind(("127.0.0.1", port)).await {
                        Ok(l) => l,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("nexio_sharded bind error: {e}")));
                            return;
                        }
                    };
                    let _ = listener.set_reuseport();
                    let _ = ready_tx.send(Ok(()));

                    while !stop_clone.load(Ordering::Relaxed) {
                        let stream = match listener.accept().await {
                            Ok(s) => s,
                            Err(_) => break,
                        };
                        unsafe {
                            let optval: libc::c_int = 1;
                            libc::setsockopt(
                                stream.as_raw_fd(),
                                libc::IPPROTO_TCP,
                                libc::TCP_NODELAY,
                                &optval as *const _ as *const libc::c_void,
                                std::mem::size_of_val(&optval) as libc::socklen_t,
                            );
                        }

                        drop(nexio::executor::spawn(async move {
                            let mut buf = vec![0u8; msg_size];
                            loop {
                                let (res, b) = stream.read_exact(buf).await;
                                buf = b;
                                if res.is_err() {
                                    break;
                                }
                                let (wres, b) = stream.write_all(buf).await;
                                buf = b;
                                if wres.is_err() {
                                    break;
                                }
                            }
                        }));
                    }
                });
            }));

            if let Err(e) = res {
                let _ = ready_tx_fail.send(Err(format!("nexio_sharded panicked: {:?}", e)));
            }
        }));
    }

    (handles, stop)
}

// ── Backend 4: compio Sharded (Reference) ─────────────────────────────────────

fn start_compio_sharded(
    port: u16,
    shards: usize,
    msg_size: usize,
    ready_tx: mpsc::Sender<Result<(), String>>,
) -> (Vec<thread::JoinHandle<()>>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let mut handles = Vec::with_capacity(shards);

    for shard in 0..shards {
        let stop_clone = Arc::clone(&stop);
        let ready_tx = ready_tx.clone();
        let ready_tx_fail = ready_tx.clone();
        let core_id = if !core_ids.is_empty() {
            Some(core_ids[shard % core_ids.len()])
        } else {
            None
        };

        handles.push(thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let rt = match compio::runtime::Runtime::new() {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("compio runtime new failed: {e}")));
                        return;
                    }
                };

                rt.block_on(async move {
                    let socket = match create_reuseport_socket(port) {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("compio socket bind failed: {e}")));
                            return;
                        }
                    };
                    let std_listener: std::net::TcpListener = socket.into();
                    let listener = match compio::net::TcpListener::from_std(std_listener) {
                        Ok(l) => l,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("compio from_std failed: {e}")));
                            return;
                        }
                    };
                    let _ = ready_tx.send(Ok(()));

                    while !stop_clone.load(Ordering::Relaxed) {
                        let Ok((stream, _)) = listener.accept().await else {
                            break;
                        };
                        compio::runtime::spawn(async move {
                            use compio::buf::BufResult;
                            use compio::io::{AsyncReadExt, AsyncWriteExt};
                            let mut stream = stream;
                            let mut buf = vec![0u8; msg_size];
                            loop {
                                let BufResult(res, b) = stream.read_exact(buf).await;
                                buf = b;
                                if res.is_err() {
                                    break;
                                }
                                let BufResult(res, b) = stream.write_all(buf).await;
                                buf = b;
                                if res.is_err() {
                                    break;
                                }
                            }
                        })
                        .detach();
                    }
                });
            }));

            if let Err(e) = res {
                let _ = ready_tx_fail.send(Err(format!("compio_sharded panicked: {:?}", e)));
            }
        }));
    }

    (handles, stop)
}

// ── Backend 5: monoio Sharded (Reference) ─────────────────────────────────────

fn start_monoio_sharded(
    port: u16,
    shards: usize,
    msg_size: usize,
    ready_tx: mpsc::Sender<Result<(), String>>,
) -> (Vec<thread::JoinHandle<()>>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let mut handles = Vec::with_capacity(shards);

    for shard in 0..shards {
        let stop_clone = Arc::clone(&stop);
        let ready_tx = ready_tx.clone();
        let ready_tx_fail = ready_tx.clone();
        let core_id = if !core_ids.is_empty() {
            Some(core_ids[shard % core_ids.len()])
        } else {
            None
        };

        handles.push(thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut rt = match monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .enable_all()
                    .build()
                {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("monoio runtime build failed: {e}")));
                        return;
                    }
                };

                rt.block_on(async move {
                    let socket = match create_reuseport_socket(port) {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("monoio socket bind failed: {e}")));
                            return;
                        }
                    };
                    let std_listener: std::net::TcpListener = socket.into();
                    let listener = match monoio::net::TcpListener::from_std(std_listener) {
                        Ok(l) => l,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("monoio from_std failed: {e}")));
                            return;
                        }
                    };
                    let _ = ready_tx.send(Ok(()));

                    while !stop_clone.load(Ordering::Relaxed) {
                        let Ok((stream, _)) = listener.accept().await else {
                            break;
                        };
                        monoio::spawn(async move {
                            use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
                            let mut stream = stream;
                            let mut buf = vec![0u8; msg_size];
                            loop {
                                let (res, b) = stream.read_exact(buf).await;
                                buf = b;
                                if res.is_err() {
                                    break;
                                }
                                let (res, b) = stream.write_all(buf).await;
                                buf = b;
                                if res.is_err() {
                                    break;
                                }
                            }
                        });
                    }
                });
            }));

            if let Err(e) = res {
                let _ = ready_tx_fail.send(Err(format!("monoio_sharded panicked: {:?}", e)));
            }
        }));
    }

    (handles, stop)
}

// ── Orchestrator ─────────────────────────────────────────────────────────────

fn run_single_benchmark(
    backend: &str,
    mode_name: &str,
    connections: usize,
    messages_per_conn: usize,
    msg_size: usize,
    cfg: &BenchConfig,
) -> Option<BenchMetrics> {
    let port = get_free_port();
    let (ready_tx, ready_rx) = mpsc::channel();

    let (stop_flag, handles_count) = match backend {
        "tokio_unsharded" => {
            let (_handle, stop) = start_tokio_unsharded(port, cfg.shards, msg_size, ready_tx);
            (stop, 1)
        }
        "tokio_sharded" => {
            let (_handles, stop) = start_tokio_sharded(port, cfg.shards, msg_size, ready_tx);
            (stop, cfg.shards)
        }
        "nexio_sharded" => {
            let (_handles, stop) = start_nexio_sharded(port, cfg.shards, msg_size, ready_tx);
            (stop, cfg.shards)
        }
        "compio_sharded" => {
            let (_handles, stop) = start_compio_sharded(port, cfg.shards, msg_size, ready_tx);
            (stop, cfg.shards)
        }
        "monoio_sharded" => {
            let (_handles, stop) = start_monoio_sharded(port, cfg.shards, msg_size, ready_tx);
            (stop, cfg.shards)
        }
        unknown => {
            eprintln!("Unknown backend: {unknown}");
            return None;
        }
    };

    // Await all shard readiness signals
    for _ in 0..handles_count {
        match ready_rx.recv_timeout(Duration::from_secs(3)) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                eprintln!("[WARN] {backend} (msg_size={msg_size}) failed readiness: {e}");
                stop_flag.store(true, Ordering::Relaxed);
                return None;
            }
            Err(_) => {
                eprintln!(
                    "[WARN] {backend} (msg_size={msg_size}) timed out waiting for server startup"
                );
                stop_flag.store(true, Ordering::Relaxed);
                return None;
            }
        }
    }

    // Brief settling delay before load generator fires
    thread::sleep(Duration::from_millis(25));

    let result = run_load_generator(
        backend,
        mode_name,
        port,
        connections,
        messages_per_conn,
        msg_size,
    );

    // Stop servers
    stop_flag.store(true, Ordering::Relaxed);
    // Unblock any pending accept by connecting dummy streams to each shard
    for _ in 0..(cfg.shards * 2) {
        let _ = StdTcpStream::connect(("127.0.0.1", port));
    }
    thread::sleep(Duration::from_millis(50));

    match result {
        Ok(metrics) => Some(metrics),
        Err(e) => {
            eprintln!("[ERROR] {backend} load generation failed: {e}");
            None
        }
    }
}

// ── Output Formatting ────────────────────────────────────────────────────────

fn print_results_table(metrics: &[BenchMetrics]) {
    println!("\n+--------------------+-------------------------+----------+-------+------------+--------------------+------------------+----------+----------+----------+----------+");
    println!("| Backend            | Mode                    | Msg Size | Conns | Total Msgs | Throughput (msg/s) | Bandwidth (MB/s) | p50 (µs) | p90 (µs) | p99 (µs) | Max (µs) |");
    println!("+--------------------+-------------------------+----------+-------+------------+--------------------+------------------+----------+----------+----------+----------+");

    for m in metrics {
        let short_mode = if m.mode.len() > 23 {
            format!("{:.20}...", m.mode)
        } else {
            m.mode.clone()
        };
        println!(
            "| {:<18} | {:<23} | {:>8} | {:>5} | {:>10} | {:>18.0} | {:>16.2} | {:>8.1} | {:>8.1} | {:>8.1} | {:>8.1} |",
            m.backend,
            short_mode,
            m.msg_size,
            m.connections,
            m.total_messages,
            m.throughput_msgs_sec,
            m.bandwidth_mb_sec,
            m.p50_us,
            m.p90_us,
            m.p99_us,
            m.max_us,
        );
    }
    println!("+--------------------+-------------------------+----------+-------+------------+--------------------+------------------+----------+----------+----------+----------+");
}

fn print_markdown_table(metrics: &[BenchMetrics]) {
    println!("\n### Benchmark Results\n");
    println!("| Backend | Mode | Msg Size | Connections | Total Msgs | Throughput (msg/s) | Bandwidth (MB/s) | p50 (µs) | p90 (µs) | p99 (µs) | Max (µs) |");
    println!("|:---|:---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for m in metrics {
        println!(
            "| `{}` | {} | {} B | {} | {} | **{:.0}** | {:.2} | {:.1} | {:.1} | {:.1} | {:.1} |",
            m.backend,
            m.mode,
            m.msg_size,
            m.connections,
            m.total_messages,
            m.throughput_msgs_sec,
            m.bandwidth_mb_sec,
            m.p50_us,
            m.p90_us,
            m.p99_us,
            m.max_us,
        );
    }
}

fn print_json_results(metrics: &[BenchMetrics]) {
    println!("\nJSON_RESULTS_START");
    print!("[");
    for (i, m) in metrics.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!(
            r#"{{"backend":"{}","mode":"{}","msg_size":{},"connections":{},"messages_per_conn":{},"total_messages":{},"wall_time_ms":{:.2},"throughput_msgs_sec":{:.2},"bandwidth_mb_sec":{:.2},"min_us":{:.2},"p50_us":{:.2},"p90_us":{:.2},"p95_us":{:.2},"p99_us":{:.2},"p999_us":{:.2},"max_us":{:.2},"mean_us":{:.2}}}"#,
            m.backend,
            m.mode,
            m.msg_size,
            m.connections,
            m.messages_per_conn,
            m.total_messages,
            m.wall_time.as_secs_f64() * 1000.0,
            m.throughput_msgs_sec,
            m.bandwidth_mb_sec,
            m.min_us,
            m.p50_us,
            m.p90_us,
            m.p95_us,
            m.p99_us,
            m.p999_us,
            m.max_us,
            m.mean_us
        );
    }
    println!("]");
    println!("JSON_RESULTS_END");
}

fn print_analysis(metrics: &[BenchMetrics]) {
    println!("\n=== Honest Comparative Analysis ===");

    // Group by (msg_size, connections)
    let mut combinations: Vec<(usize, usize)> = metrics
        .iter()
        .map(|m| (m.msg_size, m.connections))
        .collect();
    combinations.sort_unstable();
    combinations.dedup();

    for (msg_size, connections) in combinations {
        let size_metrics: Vec<&BenchMetrics> = metrics
            .iter()
            .filter(|m| m.msg_size == msg_size && m.connections == connections)
            .collect();
        if size_metrics.is_empty() {
            continue;
        }
        println!(
            "\n--- Message Size: {} Bytes | Connections: {} ---",
            msg_size, connections
        );
        let tokio_unsharded = size_metrics.iter().find(|m| m.backend == "tokio_unsharded");
        let tokio_sharded = size_metrics.iter().find(|m| m.backend == "tokio_sharded");

        for m in &size_metrics {
            let vs_unsharded = if let Some(tu) = tokio_unsharded {
                format!(
                    "{:.2}x vs tokio_unsharded",
                    m.throughput_msgs_sec / tu.throughput_msgs_sec
                )
            } else {
                "-".into()
            };
            let vs_sharded = if let Some(ts) = tokio_sharded {
                format!(
                    "{:.2}x vs tokio_sharded",
                    m.throughput_msgs_sec / ts.throughput_msgs_sec
                )
            } else {
                "-".into()
            };
            println!(
                "  • {:<16}: {:>9.0} msg/s  | p50: {:>6.1} µs | p99: {:>7.1} µs | {}, {}",
                m.backend, m.throughput_msgs_sec, m.p50_us, m.p99_us, vs_unsharded, vs_sharded
            );
        }
    }

    // High-concurrency scaling comparison if multiple connection levels were tested
    let mut conn_levels: Vec<usize> = metrics.iter().map(|m| m.connections).collect();
    conn_levels.sort_unstable();
    conn_levels.dedup();

    if conn_levels.len() > 1 {
        let low_c = conn_levels[0];
        let high_c = conn_levels[conn_levels.len() - 1];
        println!(
            "\n================================================================================"
        );
        println!(
            " High-Concurrency Scaling Impact ({} conns -> {} conns)",
            low_c, high_c
        );
        println!(
            "================================================================================"
        );

        let mut msg_sizes: Vec<usize> = metrics.iter().map(|m| m.msg_size).collect();
        msg_sizes.sort_unstable();
        msg_sizes.dedup();

        for &msg_size in &msg_sizes {
            println!("\n--- Message Size: {} Bytes Scaling Factor ---", msg_size);
            let mut backends: Vec<String> = metrics.iter().map(|m| m.backend.clone()).collect();
            backends.sort();
            backends.dedup();

            for backend in &backends {
                let m_low = metrics.iter().find(|m| {
                    m.msg_size == msg_size && m.connections == low_c && &m.backend == backend
                });
                let m_high = metrics.iter().find(|m| {
                    m.msg_size == msg_size && m.connections == high_c && &m.backend == backend
                });

                if let (Some(l), Some(h)) = (m_low, m_high) {
                    let tput_ratio = h.throughput_msgs_sec / l.throughput_msgs_sec;
                    let p50_ratio = h.p50_us / l.p50_us.max(1.0);
                    let p99_ratio = h.p99_us / l.p99_us.max(1.0);
                    println!(
                        "  • {:<16}: {:>8.0} -> {:>8.0} msg/s ({:.2}x) | p50: {:>6.1} -> {:>6.1} µs ({:.1}x) | p99: {:>7.1} -> {:>7.1} µs ({:.1}x)",
                        backend, l.throughput_msgs_sec, h.throughput_msgs_sec, tput_ratio, l.p50_us, h.p50_us, p50_ratio, l.p99_us, h.p99_us, p99_ratio
                    );
                }
            }
        }
    }
}

// ── CLI Parsing & Entry Point ────────────────────────────────────────────────

fn parse_cli_args() -> BenchConfig {
    let mut cfg = BenchConfig::default();
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--connections" => {
                if let Some(val) = args.get(i + 1).and_then(|s| s.parse().ok()) {
                    cfg.connections = val;
                    i += 1;
                }
            }
            "--messages" => {
                if let Some(val) = args.get(i + 1).and_then(|s| s.parse().ok()) {
                    cfg.messages_per_conn = val;
                    i += 1;
                }
            }
            "--msg-size" => {
                if let Some(val) = args.get(i + 1).and_then(|s| s.parse().ok()) {
                    cfg.msg_sizes = vec![val];
                    i += 1;
                }
            }
            "--shards" => {
                if let Some(val) = args.get(i + 1).and_then(|s| s.parse().ok()) {
                    cfg.shards = val;
                    i += 1;
                }
            }
            "--backends" => {
                if let Some(val) = args.get(i + 1) {
                    if val == "3way" {
                        cfg.backends = vec![
                            "tokio_unsharded".into(),
                            "tokio_sharded".into(),
                            "nexio_sharded".into(),
                        ];
                    } else if val == "all" {
                        cfg.backends = vec![
                            "tokio_unsharded".into(),
                            "tokio_sharded".into(),
                            "nexio_sharded".into(),
                            "compio_sharded".into(),
                            "monoio_sharded".into(),
                        ];
                    } else {
                        cfg.backends = val.split(',').map(|s| s.trim().to_string()).collect();
                    }
                    i += 1;
                }
            }
            "--quick" => {
                cfg.quick = true;
                cfg.connections = 20;
                cfg.messages_per_conn = 100;
                cfg.msg_sizes = vec![128, 1024];
            }
            "--compare-modes" => {
                cfg.compare_modes = true;
            }
            "--json" => {
                cfg.json = true;
            }
            "--markdown" => {
                cfg.markdown = true;
            }
            _ => {}
        }
        i += 1;
    }
    cfg
}

/// Separate-process modes. The in-process bench runs server shards and the
/// load generator on the same cores, so a saturated client masks server
/// differences. Here the driver script pins `--server-only` and
/// `--client-only` to different cores and reads the server's CPU time from
/// /proc, giving CPU-seconds-per-message independent of who saturates first.
fn run_split_mode(args: &[String]) -> bool {
    let get = |flag: &str| -> Option<String> {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1).cloned())
    };
    let port: u16 = get("--port").and_then(|s| s.parse().ok()).unwrap_or(0);
    let msg_size: usize = get("--msg-size")
        .and_then(|s| s.parse().ok())
        .unwrap_or(128);
    let shards: usize = get("--shards").and_then(|s| s.parse().ok()).unwrap_or(1);

    if let Some(backend) = get("--server-only") {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (_keep, _stop) = match backend.as_str() {
            "tokio_unsharded" => {
                let (h, s) = start_tokio_unsharded(port, shards, msg_size, ready_tx);
                (vec![h], s)
            }
            "tokio_sharded" => start_tokio_sharded(port, shards, msg_size, ready_tx),
            "nexio_sharded" => start_nexio_sharded(port, shards, msg_size, ready_tx),
            "compio_sharded" => start_compio_sharded(port, shards, msg_size, ready_tx),
            "monoio_sharded" => start_monoio_sharded(port, shards, msg_size, ready_tx),
            other => {
                eprintln!("unknown backend {other}");
                std::process::exit(2);
            }
        };
        let n = if backend == "tokio_unsharded" {
            1
        } else {
            shards
        };
        for _ in 0..n {
            match ready_rx.recv_timeout(Duration::from_secs(5)) {
                Ok(Ok(())) => {}
                other => {
                    eprintln!("server failed to start: {other:?}");
                    std::process::exit(3);
                }
            }
        }
        println!("READY {}", std::process::id());
        loop {
            thread::sleep(Duration::from_secs(3600));
        }
    }

    if args.iter().any(|a| a == "--client-only") {
        let conns: usize = get("--connections")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let msgs: usize = get("--messages")
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);
        match run_load_generator("split", "split", port, conns, msgs, msg_size) {
            Ok(m) => println!(
                "RESULT total={} wall_s={:.4} throughput={:.0} p50_us={:.1} p99_us={:.1}",
                m.total_messages,
                m.wall_time.as_secs_f64(),
                m.throughput_msgs_sec,
                m.p50_us,
                m.p99_us
            ),
            Err(e) => {
                eprintln!("load generation failed: {e}");
                std::process::exit(4);
            }
        }
        return true;
    }
    false
}

fn main() {
    raise_fd_limit();
    let raw_args: Vec<String> = std::env::args().collect();
    if run_split_mode(&raw_args) {
        return;
    }
    let cfg = parse_cli_args();
    print_hw_metadata();

    let modes: Vec<ConcurrencyMode> = if cfg.compare_modes {
        if cfg.quick {
            vec![
                ConcurrencyMode {
                    name: "Standard Concurrency (quick: 20c x 100m)",
                    connections: 20,
                    messages_per_conn: 100,
                },
                ConcurrencyMode {
                    name: "High Concurrency (quick: 50c x 40m)",
                    connections: 50,
                    messages_per_conn: 40,
                },
            ]
        } else {
            vec![
                ConcurrencyMode {
                    name: "Standard Concurrency (100 conns x 500 msgs)",
                    connections: 100,
                    messages_per_conn: 500,
                },
                ConcurrencyMode {
                    name: "High Concurrency (500 conns x 200 msgs)",
                    connections: 500,
                    messages_per_conn: 200,
                },
            ]
        }
    } else {
        let name: &'static str = if cfg.connections == 500 && cfg.messages_per_conn == 200 {
            "High Concurrency (500 conns x 200 msgs)"
        } else if cfg.connections == 100 && cfg.messages_per_conn == 500 {
            "Standard Concurrency (100 conns x 500 msgs)"
        } else if cfg.quick {
            "Quick Sanity (20 conns x 100 msgs)"
        } else {
            "Custom Concurrency"
        };
        vec![ConcurrencyMode {
            name,
            connections: cfg.connections,
            messages_per_conn: cfg.messages_per_conn,
        }]
    };

    println!(
        "Configuration: compare_modes={} shards={} backends={:?}",
        cfg.compare_modes, cfg.shards, cfg.backends
    );

    let mut all_results = Vec::new();

    for mode in &modes {
        println!(
            "\n################################################################################"
        );
        println!(">>> Mode: {} <<<", mode.name);
        println!(
            "    Connections: {} | Messages/Conn: {} | Total Messages/Backend: {}",
            mode.connections,
            mode.messages_per_conn,
            mode.connections * mode.messages_per_conn
        );
        println!(
            "################################################################################"
        );

        for &msg_size in &cfg.msg_sizes {
            println!("\n>>> Benchmarking message size: {} bytes <<<", msg_size);
            for backend in &cfg.backends {
                print!("  Running {:<18} ... ", backend);
                std::io::Write::flush(&mut std::io::stdout()).unwrap();

                if let Some(metrics) = run_single_benchmark(
                    backend,
                    mode.name,
                    mode.connections,
                    mode.messages_per_conn,
                    msg_size,
                    &cfg,
                ) {
                    println!(
                        "{:>8.0} msg/s | p50: {:>6.1} µs | p99: {:>6.1} µs",
                        metrics.throughput_msgs_sec, metrics.p50_us, metrics.p99_us
                    );
                    all_results.push(metrics);
                } else {
                    println!("SKIPPED / FAILED (see diagnostics)");
                }
            }
        }
    }

    if !all_results.is_empty() {
        print_results_table(&all_results);
        print_analysis(&all_results);

        if cfg.markdown {
            print_markdown_table(&all_results);
        }
        if cfg.json {
            print_json_results(&all_results);
        }
    } else {
        eprintln!("\nNo benchmarks completed successfully.");
    }
}
