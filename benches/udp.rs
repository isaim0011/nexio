//! Fair UDP datagram benchmark comparing:
//!   - `nexio`  (`nexio::net::UdpSocket` over `io_uring`)
//!   - `compio` (`compio::net::UdpSocket` over `io_uring`)
//!   - `monoio` (`monoio::net::UdpSocket` over `io_uring`)
//!   - `tokio`  (`tokio::net::UdpSocket` over `epoll`)
//!
//! Workloads:
//!   1. Connected UDP Ping-Pong (128 B, 20,000 sequential round-trips)
//!   2. Concurrent UDP Fan-In/Echo (32 socket pairs, 32 x 2,000 = 64,000 datagrams)

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

struct UdpRow {
    backend: &'static str,
    workload: &'static str,
    msgs: usize,
    elapsed: Duration,
}

impl UdpRow {
    fn msg_s(&self) -> f64 {
        self.msgs as f64 / self.elapsed.as_secs_f64()
    }
}

fn bench_nexio() -> Vec<UdpRow> {
    use nexio::buf::IoBufMutExt;
    let mut rows = Vec::new();
    const PING_CNT: usize = 20_000;
    const CONNS: usize = 32;
    const PER_CONN: usize = 2_000;

    nexio::block_on(async {
        let srv = nexio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let cli = nexio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let srv_addr: SocketAddr = srv.local_addr().unwrap();
        let cli_addr: SocketAddr = cli.local_addr().unwrap();
        srv.connect(cli_addr).await.unwrap();
        cli.connect(srv_addr).await.unwrap();

        let srv_task = nexio::spawn(async move {
            let mut buf = vec![0u8; 128];
            for _ in 0..PING_CNT {
                let (res, b) = srv.recv(buf).await;
                let n = res.unwrap();
                buf = b;
                let (wres, sl) = srv.send(buf.slice(0..n)).await;
                wres.unwrap();
                buf = sl.into_inner();
            }
        });

        let mut cbuf = vec![0x77u8; 128];
        let mut rbuf = vec![0u8; 128];
        let t0 = Instant::now();
        for _ in 0..PING_CNT {
            let (wres, b) = cli.send(cbuf).await;
            wres.unwrap();
            cbuf = b;
            let (rres, b) = cli.recv(rbuf).await;
            rres.unwrap();
            rbuf = b;
        }
        let elapsed = t0.elapsed();
        srv_task.await.unwrap();
        rows.push(UdpRow {
            backend: "nexio",
            workload: "udp_pingpong_128b",
            msgs: PING_CNT,
            elapsed,
        });

        // Concurrent 32-pair fan-in/echo
        let mut srv_handles = Vec::with_capacity(CONNS);
        let mut cli_sockets = Vec::with_capacity(CONNS);
        for _ in 0..CONNS {
            let s = nexio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let c = nexio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let sa = s.local_addr().unwrap();
            let ca = c.local_addr().unwrap();
            s.connect(ca).await.unwrap();
            c.connect(sa).await.unwrap();
            srv_handles.push(nexio::spawn(async move {
                let mut buf = vec![0u8; 128];
                for _ in 0..PER_CONN {
                    let (r, b) = s.recv(buf).await;
                    let n = r.unwrap();
                    buf = b;
                    let (w, sl) = s.send(buf.slice(0..n)).await;
                    w.unwrap();
                    buf = sl.into_inner();
                }
            }));
            cli_sockets.push(c);
        }

        let t0 = Instant::now();
        let mut cli_handles = Vec::with_capacity(CONNS);
        for c in cli_sockets {
            cli_handles.push(nexio::spawn(async move {
                let mut sb = vec![0x55u8; 128];
                let mut rb = vec![0u8; 128];
                for _ in 0..PER_CONN {
                    let (w, b) = c.send(sb).await;
                    w.unwrap();
                    sb = b;
                    let (r, b) = c.recv(rb).await;
                    r.unwrap();
                    rb = b;
                }
            }));
        }
        for h in cli_handles {
            h.await.unwrap();
        }
        for h in srv_handles {
            h.await.unwrap();
        }
        rows.push(UdpRow {
            backend: "nexio",
            workload: "udp_32pairs_128b",
            msgs: CONNS * PER_CONN,
            elapsed: t0.elapsed(),
        });
    });

    rows
}

fn bench_compio() -> Vec<UdpRow> {
    use compio::buf::{IntoInner, IoBuf};
    let mut rows = Vec::new();
    const PING_CNT: usize = 20_000;
    const CONNS: usize = 32;
    const PER_CONN: usize = 2_000;

    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let srv = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let cli = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let srv_addr = srv.local_addr().unwrap();
        let cli_addr = cli.local_addr().unwrap();
        srv.connect(cli_addr).await.unwrap();
        cli.connect(srv_addr).await.unwrap();

        let srv_task = compio::runtime::spawn(async move {
            let mut buf = vec![0u8; 128];
            for _ in 0..PING_CNT {
                let compio::BufResult(r, b) = srv.recv(buf).await;
                let n = r.unwrap();
                buf = b;
                let compio::BufResult(w, sl) = srv.send(buf.slice(0..n)).await;
                w.unwrap();
                buf = sl.into_inner();
            }
        });

        let mut cbuf = vec![0x77u8; 128];
        let mut rbuf = vec![0u8; 128];
        let t0 = Instant::now();
        for _ in 0..PING_CNT {
            let compio::BufResult(w, b) = cli.send(cbuf).await;
            w.unwrap();
            cbuf = b;
            let compio::BufResult(r, b) = cli.recv(rbuf).await;
            r.unwrap();
            rbuf = b;
        }
        let elapsed = t0.elapsed();
        srv_task.await.unwrap();
        rows.push(UdpRow {
            backend: "compio",
            workload: "udp_pingpong_128b",
            msgs: PING_CNT,
            elapsed,
        });

        let mut srv_handles = Vec::with_capacity(CONNS);
        let mut cli_sockets = Vec::with_capacity(CONNS);
        for _ in 0..CONNS {
            let s = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let c = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let sa = s.local_addr().unwrap();
            let ca = c.local_addr().unwrap();
            s.connect(ca).await.unwrap();
            c.connect(sa).await.unwrap();
            srv_handles.push(compio::runtime::spawn(async move {
                let mut buf = vec![0u8; 128];
                for _ in 0..PER_CONN {
                    let compio::BufResult(r, b) = s.recv(buf).await;
                    let n = r.unwrap();
                    buf = b;
                    let compio::BufResult(w, sl) = s.send(buf.slice(0..n)).await;
                    w.unwrap();
                    buf = sl.into_inner();
                }
            }));
            cli_sockets.push(c);
        }

        let t0 = Instant::now();
        let mut cli_handles = Vec::with_capacity(CONNS);
        for c in cli_sockets {
            cli_handles.push(compio::runtime::spawn(async move {
                let mut sb = vec![0x55u8; 128];
                let mut rb = vec![0u8; 128];
                for _ in 0..PER_CONN {
                    let compio::BufResult(w, b) = c.send(sb).await;
                    w.unwrap();
                    sb = b;
                    let compio::BufResult(r, b) = c.recv(rb).await;
                    r.unwrap();
                    rb = b;
                }
            }));
        }
        for h in cli_handles {
            h.await.unwrap();
        }
        for h in srv_handles {
            h.await.unwrap();
        }
        rows.push(UdpRow {
            backend: "compio",
            workload: "udp_32pairs_128b",
            msgs: CONNS * PER_CONN,
            elapsed: t0.elapsed(),
        });
    });

    rows
}

fn bench_monoio() -> Vec<UdpRow> {
    use monoio::buf::IoBuf;
    let mut rows = Vec::new();
    const PING_CNT: usize = 20_000;
    const CONNS: usize = 32;
    const PER_CONN: usize = 2_000;

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let srv = monoio::net::udp::UdpSocket::bind("127.0.0.1:0").unwrap();
        let cli = monoio::net::udp::UdpSocket::bind("127.0.0.1:0").unwrap();
        let srv_addr = srv.local_addr().unwrap();
        let cli_addr = cli.local_addr().unwrap();
        srv.connect(cli_addr).await.unwrap();
        cli.connect(srv_addr).await.unwrap();

        let srv_task = monoio::spawn(async move {
            let mut buf = vec![0u8; 128];
            for _ in 0..PING_CNT {
                let (r, b) = srv.recv(buf).await;
                let n = r.unwrap();
                buf = b;
                let (w, sl) = srv.send(buf.slice(0..n)).await;
                w.unwrap();
                buf = sl.into_inner();
            }
        });

        let mut cbuf = vec![0x77u8; 128];
        let mut rbuf = vec![0u8; 128];
        let t0 = Instant::now();
        for _ in 0..PING_CNT {
            let (w, b) = cli.send(cbuf).await;
            w.unwrap();
            cbuf = b;
            let (r, b) = cli.recv(rbuf).await;
            r.unwrap();
            rbuf = b;
        }
        let elapsed = t0.elapsed();
        srv_task.await;
        rows.push(UdpRow {
            backend: "monoio",
            workload: "udp_pingpong_128b",
            msgs: PING_CNT,
            elapsed,
        });

        let mut srv_handles = Vec::with_capacity(CONNS);
        let mut cli_sockets = Vec::with_capacity(CONNS);
        for _ in 0..CONNS {
            let s = monoio::net::udp::UdpSocket::bind("127.0.0.1:0").unwrap();
            let c = monoio::net::udp::UdpSocket::bind("127.0.0.1:0").unwrap();
            let sa = s.local_addr().unwrap();
            let ca = c.local_addr().unwrap();
            s.connect(ca).await.unwrap();
            c.connect(sa).await.unwrap();
            srv_handles.push(monoio::spawn(async move {
                let mut buf = vec![0u8; 128];
                for _ in 0..PER_CONN {
                    let (r, b) = s.recv(buf).await;
                    let n = r.unwrap();
                    buf = b;
                    let (w, sl) = s.send(buf.slice(0..n)).await;
                    w.unwrap();
                    buf = sl.into_inner();
                }
            }));
            cli_sockets.push(c);
        }

        let t0 = Instant::now();
        let mut cli_handles = Vec::with_capacity(CONNS);
        for c in cli_sockets {
            cli_handles.push(monoio::spawn(async move {
                let mut sb = vec![0x55u8; 128];
                let mut rb = vec![0u8; 128];
                for _ in 0..PER_CONN {
                    let (w, b) = c.send(sb).await;
                    w.unwrap();
                    sb = b;
                    let (r, b) = c.recv(rb).await;
                    r.unwrap();
                    rb = b;
                }
            }));
        }
        for h in cli_handles {
            h.await;
        }
        for h in srv_handles {
            h.await;
        }
        rows.push(UdpRow {
            backend: "monoio",
            workload: "udp_32pairs_128b",
            msgs: CONNS * PER_CONN,
            elapsed: t0.elapsed(),
        });
    });

    rows
}

fn bench_tokio() -> Vec<UdpRow> {
    let mut rows = Vec::new();
    const PING_CNT: usize = 20_000;
    const CONNS: usize = 32;
    const PER_CONN: usize = 2_000;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let srv = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let cli = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let srv_addr = srv.local_addr().unwrap();
        let cli_addr = cli.local_addr().unwrap();
        srv.connect(cli_addr).await.unwrap();
        cli.connect(srv_addr).await.unwrap();

        let srv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 128];
            for _ in 0..PING_CNT {
                let n = srv.recv(&mut buf).await.unwrap();
                srv.send(&buf[..n]).await.unwrap();
            }
        });

        let cbuf = vec![0x77u8; 128];
        let mut rbuf = vec![0u8; 128];
        let t0 = Instant::now();
        for _ in 0..PING_CNT {
            cli.send(&cbuf).await.unwrap();
            cli.recv(&mut rbuf).await.unwrap();
        }
        let elapsed = t0.elapsed();
        srv_task.await.unwrap();
        rows.push(UdpRow {
            backend: "tokio",
            workload: "udp_pingpong_128b",
            msgs: PING_CNT,
            elapsed,
        });

        let mut srv_handles = Vec::with_capacity(CONNS);
        let mut cli_sockets = Vec::with_capacity(CONNS);
        for _ in 0..CONNS {
            let s = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let c = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let sa = s.local_addr().unwrap();
            let ca = c.local_addr().unwrap();
            s.connect(ca).await.unwrap();
            c.connect(sa).await.unwrap();
            srv_handles.push(tokio::spawn(async move {
                let mut buf = vec![0u8; 128];
                for _ in 0..PER_CONN {
                    let n = s.recv(&mut buf).await.unwrap();
                    s.send(&buf[..n]).await.unwrap();
                }
            }));
            cli_sockets.push(c);
        }

        let t0 = Instant::now();
        let mut cli_handles = Vec::with_capacity(CONNS);
        for c in cli_sockets {
            cli_handles.push(tokio::spawn(async move {
                let sb = vec![0x55u8; 128];
                let mut rb = vec![0u8; 128];
                for _ in 0..PER_CONN {
                    c.send(&sb).await.unwrap();
                    c.recv(&mut rb).await.unwrap();
                }
            }));
        }
        for h in cli_handles {
            h.await.unwrap();
        }
        for h in srv_handles {
            h.await.unwrap();
        }
        rows.push(UdpRow {
            backend: "tokio",
            workload: "udp_32pairs_128b",
            msgs: CONNS * PER_CONN,
            elapsed: t0.elapsed(),
        });
    });

    rows
}

fn main() {
    let mut all = Vec::new();
    all.extend(bench_nexio());
    all.extend(bench_compio());
    all.extend(bench_monoio());
    all.extend(bench_tokio());

    println!("| Backend | Workload | Round-trips | Time (ms) | Throughput (msg/s) |");
    println!("|:---|:---|---:|---:|---:|");
    for r in &all {
        println!(
            "| `{}` | {} | {} | {:.2} | {:.0} |",
            r.backend,
            r.workload,
            r.msgs,
            r.elapsed.as_secs_f64() * 1000.0,
            r.msg_s()
        );
    }
}
