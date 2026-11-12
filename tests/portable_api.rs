//! Backend-neutral tests of the public API. They run unchanged on whichever
//! backend is selected (io_uring on Linux, the portable readiness backend
//! elsewhere or with `--features poll`), so passing on both is the evidence that
//! the two backends behave alike.

use nexio::{
    block_on,
    net::{TcpListener, TcpStream},
    spawn,
    time::{self, Elapsed},
};
use std::{io, rc::Rc, time::Duration};

async fn echo_server(listener: TcpListener) {
    loop {
        let Ok(stream) = listener.accept().await else {
            break;
        };
        drop(spawn(async move {
            let mut buf = Vec::with_capacity(16 * 1024);
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
}

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
}

#[test]
fn tcp_echo_roundtrip() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(spawn(echo_server(listener)));

        let client = TcpStream::connect(addr).await.unwrap();
        assert_eq!(client.peer_addr().unwrap(), addr);
        for i in 0..50u8 {
            let msg = pattern(1 + i as usize * 7, i);
            let (res, _) = client.write_all(msg.clone()).await;
            res.unwrap();
            let (res, echo) = client.read_exact(vec![0u8; msg.len()]).await;
            res.unwrap();
            assert_eq!(echo, msg);
        }
    });
}

#[test]
fn four_mebibytes_with_simultaneous_read_and_write_on_one_socket() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(spawn(echo_server(listener)));

        const TOTAL: usize = 4 * 1024 * 1024;
        const CHUNK: usize = 64 * 1024;
        let client = Rc::new(TcpStream::connect(addr).await.unwrap());

        // The writer and the reader wait on the same socket at the same time, in
        // opposite directions: the server's echo cannot drain until we read, and we
        // cannot finish writing until it drains.
        let writer = {
            let client = client.clone();
            spawn(async move {
                let mut sent = 0;
                while sent < TOTAL {
                    let (res, _) = client.write_all(pattern(CHUNK, (sent / CHUNK) as u8)).await;
                    res.unwrap();
                    sent += CHUNK;
                }
            })
        };
        let mut received = 0;
        while received < TOTAL {
            let (res, got) = client.read_exact(vec![0u8; CHUNK]).await;
            res.unwrap();
            assert_eq!(
                got,
                pattern(CHUNK, (received / CHUNK) as u8),
                "corruption at {received}"
            );
            received += CHUNK;
        }
        writer.await.unwrap();
    });
}

#[test]
fn connecting_to_a_closed_port_is_refused() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let err = TcpStream::connect(addr)
            .await
            .err()
            .expect("connect must fail");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused, "{err:?}");
    });
}

#[test]
fn peer_close_reads_zero_and_read_exact_reports_eof() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(spawn(async move {
            let s = listener.accept().await.unwrap();
            let (res, _) = s.write_all(b"abc".to_vec()).await;
            res.unwrap();
            // dropped: closes the connection
        }));
        let client = TcpStream::connect(addr).await.unwrap();
        let (res, got) = client.read_exact(vec![0u8; 3]).await;
        res.unwrap();
        assert_eq!(got, b"abc");
        let (res, _) = client.read(Vec::with_capacity(8)).await;
        assert_eq!(res.unwrap(), 0, "a closed peer reads as 0 bytes");
        let (res, _) = client.read_exact(vec![0u8; 4]).await;
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    });
}

#[test]
fn a_cancelled_read_leaves_the_connection_usable() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = nexio::executor::oneshot::channel::<()>();
        drop(spawn(async move {
            let s = listener.accept().await.unwrap();
            rx.await.ok();
            let (res, _) = s.write_all(b"late".to_vec()).await;
            res.unwrap();
            time::sleep(Duration::from_millis(200)).await;
        }));
        let client = TcpStream::connect(addr).await.unwrap();

        // Nothing to read yet: the timeout drops the pending read, buffer and all.
        let r = time::timeout(Duration::from_millis(30), client.read(vec![0u8; 16])).await;
        assert!(matches!(r, Err(Elapsed)), "read should have timed out");

        tx.send(()).ok();
        let (res, got) = client.read_exact(vec![0u8; 4]).await;
        res.unwrap();
        assert_eq!(got, b"late");
    });
}

#[test]
fn a_cancelled_accept_does_not_lose_the_next_connection() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let r = time::timeout(Duration::from_millis(30), listener.accept()).await;
        assert!(r.is_err(), "no one is connecting yet");

        let client = TcpStream::connect(addr).await.unwrap();
        let server_side = listener.accept().await.unwrap();
        let (res, _) = client.write_all(b"hi".to_vec()).await;
        res.unwrap();
        let (res, got) = server_side.read_exact(vec![0u8; 2]).await;
        res.unwrap();
        assert_eq!(got, b"hi");
    });
}

#[test]
fn two_hundred_concurrent_clients() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(spawn(echo_server(listener)));

        let mut handles = Vec::new();
        for c in 0..200u8 {
            handles.push(spawn(async move {
                let s = TcpStream::connect(addr).await.unwrap();
                for m in 0..10u8 {
                    let msg = pattern(100 + m as usize, c ^ m);
                    let (res, _) = s.write_all(msg.clone()).await;
                    res.unwrap();
                    let (res, echo) = s.read_exact(vec![0u8; msg.len()]).await;
                    res.unwrap();
                    assert_eq!(echo, msg);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
}

#[test]
fn sleeps_complete_in_deadline_order() {
    block_on(async {
        let order = Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut handles = Vec::new();
        for (name, ms) in [("c", 60u64), ("a", 10), ("b", 35)] {
            let order = order.clone();
            handles.push(spawn(async move {
                time::sleep(Duration::from_millis(ms)).await;
                order.borrow_mut().push(name);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(*order.borrow(), vec!["a", "b", "c"]);
    });
}

#[test]
fn sleep_waits_at_least_its_duration() {
    block_on(async {
        let t = std::time::Instant::now();
        time::sleep(Duration::from_millis(50)).await;
        assert!(t.elapsed() >= Duration::from_millis(50));
        assert!(
            t.elapsed() < Duration::from_millis(500),
            "took {:?}",
            t.elapsed()
        );
    });
}

#[test]
fn host_names_work_for_bind_and_connect() {
    block_on(async {
        // `localhost` may resolve to ::1 first; connect must fall through to 127.0.0.1.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(spawn(echo_server(listener)));
        let client = TcpStream::connect(("localhost", port)).await.unwrap();
        let (res, _) = client.write_all(b"ping".to_vec()).await;
        res.unwrap();
        let (res, got) = client.read_exact(vec![0u8; 4]).await;
        res.unwrap();
        assert_eq!(got, b"ping");

        // Binding by name, and by an owned/borrowed string, both resolve.
        let by_name = TcpListener::bind(("localhost", 0)).await.unwrap();
        assert!(by_name.local_addr().unwrap().ip().is_loopback());
        let text = String::from("127.0.0.1:0");
        let by_text = TcpListener::bind(&text).await.unwrap();
        assert!(by_text.local_addr().unwrap().ip().is_loopback());
    });
}

#[test]
fn an_unresolvable_host_is_an_error_not_a_hang() {
    block_on(async {
        let r = TcpStream::connect(("nexio-no-such-host.invalid", 80)).await;
        assert!(r.is_err());
    });
}
