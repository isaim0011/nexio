//! Unix-domain socket tests. They run on both backends (io_uring and portable) on
//! Linux and macOS.
#![cfg(unix)]

use nexio::{
    block_on,
    net::{UnixDatagram, UnixListener, UnixStream},
    spawn, time,
};
use std::{
    io,
    path::PathBuf,
    rc::Rc,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

fn sock_path(tag: &str) -> PathBuf {
    static N: AtomicU32 = AtomicU32::new(0);
    // Short names: Unix socket paths are limited to ~100 bytes.
    std::env::temp_dir().join(format!(
        "nx{}_{}_{}.sock",
        std::process::id(),
        tag,
        N.fetch_add(1, Ordering::Relaxed)
    ))
}

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
}

async fn echo_server(listener: UnixListener) {
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

#[test]
fn stream_echo_roundtrip() {
    let path = sock_path("echo");
    block_on(async {
        let listener = UnixListener::bind(&path).unwrap();
        assert_eq!(
            listener.local_path().unwrap().as_deref(),
            Some(path.as_path())
        );
        drop(spawn(echo_server(listener)));

        let client = UnixStream::connect(&path).await.unwrap();
        assert_eq!(client.peer_path().unwrap().as_deref(), Some(path.as_path()));
        for i in 0..50u8 {
            let msg = pattern(1 + i as usize * 9, i);
            let (res, _) = client.write_all(msg.clone()).await;
            res.unwrap();
            let (res, echo) = client.read_exact(vec![0u8; msg.len()]).await;
            res.unwrap();
            assert_eq!(echo, msg);
        }
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn a_connected_pair_talks_both_ways() {
    block_on(async {
        let (a, b) = UnixStream::pair().unwrap();
        let (res, _) = a.write_all(b"to b".to_vec()).await;
        res.unwrap();
        let (res, got) = b.read_exact(vec![0u8; 4]).await;
        res.unwrap();
        assert_eq!(got, b"to b");
        let (res, _) = b.write_all(b"to a".to_vec()).await;
        res.unwrap();
        let (res, got) = a.read_exact(vec![0u8; 4]).await;
        res.unwrap();
        assert_eq!(got, b"to a");
    });
}

#[test]
fn four_mebibytes_with_simultaneous_read_and_write() {
    let path = sock_path("big");
    block_on(async {
        let listener = UnixListener::bind(&path).unwrap();
        drop(spawn(echo_server(listener)));
        const TOTAL: usize = 4 * 1024 * 1024;
        const CHUNK: usize = 64 * 1024;
        let client = Rc::new(UnixStream::connect(&path).await.unwrap());
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
                "at {received}"
            );
            received += CHUNK;
        }
        writer.await.unwrap();
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn peer_close_reads_zero_and_read_exact_reports_eof() {
    block_on(async {
        let (a, b) = UnixStream::pair().unwrap();
        let (res, _) = a.write_all(b"abc".to_vec()).await;
        res.unwrap();
        drop(a);
        let (res, got) = b.read_exact(vec![0u8; 3]).await;
        res.unwrap();
        assert_eq!(got, b"abc");
        let (res, _) = b.read(Vec::with_capacity(8)).await;
        assert_eq!(res.unwrap(), 0);
        let (res, _) = b.read_exact(vec![0u8; 4]).await;
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    });
}

#[test]
fn writing_to_a_closed_peer_is_an_error_not_a_killed_process() {
    // Without MSG_NOSIGNAL / SO_NOSIGPIPE this would raise SIGPIPE and end the test process.
    block_on(async {
        let (a, b) = UnixStream::pair().unwrap();
        drop(b);
        let mut failed = false;
        for _ in 0..20 {
            let (res, _) = a.write_all(vec![0u8; 1024]).await;
            if let Err(e) = res {
                assert_eq!(e.kind(), io::ErrorKind::BrokenPipe, "{e:?}");
                failed = true;
                break;
            }
        }
        assert!(failed, "writes to a closed peer must eventually fail");
    });
}

#[test]
fn connecting_without_a_listener_fails_cleanly() {
    let missing = sock_path("missing");
    let stale = sock_path("stale");
    block_on(async {
        let err = UnixStream::connect(&missing)
            .await
            .err()
            .expect("must fail");
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err:?}");

        // A socket file left behind by a dead listener refuses connections.
        let l = UnixListener::bind(&stale).unwrap();
        drop(l);
        let err = UnixStream::connect(&stale).await.err().expect("must fail");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused, "{err:?}");
    });
    let _ = std::fs::remove_file(&stale);
}

#[test]
fn binding_over_an_existing_path_fails() {
    let path = sock_path("dup");
    let first = UnixListener::bind(&path).unwrap();
    let err = UnixListener::bind(&path).err().expect("must fail");
    assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err:?}");
    drop(first);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn a_cancelled_accept_does_not_lose_the_next_connection() {
    let path = sock_path("acc");
    block_on(async {
        let listener = UnixListener::bind(&path).unwrap();
        let r = time::timeout(Duration::from_millis(30), listener.accept()).await;
        assert!(r.is_err(), "no one is connecting yet");

        let client = UnixStream::connect(&path).await.unwrap();
        let server_side = listener.accept().await.unwrap();
        let (res, _) = client.write_all(b"hi".to_vec()).await;
        res.unwrap();
        let (res, got) = server_side.read_exact(vec![0u8; 2]).await;
        res.unwrap();
        assert_eq!(got, b"hi");
    });
    let _ = std::fs::remove_file(&path);
}

#[test]
fn a_cancelled_read_leaves_the_connection_usable() {
    block_on(async {
        let (a, b) = UnixStream::pair().unwrap();
        let r = time::timeout(Duration::from_millis(30), a.read(vec![0u8; 16])).await;
        assert!(r.is_err(), "nothing was sent yet");
        let (res, _) = b.write_all(b"late".to_vec()).await;
        res.unwrap();
        let (res, got) = a.read_exact(vec![0u8; 4]).await;
        res.unwrap();
        assert_eq!(got, b"late");
    });
}

#[test]
fn read_waits_for_data_that_arrives_later() {
    block_on(async {
        let (a, b) = UnixStream::pair().unwrap();
        let sender = spawn(async move {
            time::sleep(Duration::from_millis(60)).await;
            let (res, _) = b.write_all(b"later".to_vec()).await;
            res.unwrap();
        });
        let t = std::time::Instant::now();
        let (res, got) = a.read(vec![0u8; 16]).await;
        let n = res.expect("read must wait, not fail with WouldBlock");
        assert_eq!(&got[..n], b"later");
        assert!(t.elapsed() >= Duration::from_millis(40));
        sender.await.unwrap();
    });
}

#[test]
fn two_hundred_concurrent_clients() {
    let path = sock_path("many");
    block_on(async {
        let listener = UnixListener::bind(&path).unwrap();
        drop(spawn(echo_server(listener)));
        let mut handles = Vec::new();
        for c in 0..200u8 {
            let path = path.clone();
            handles.push(spawn(async move {
                let s = UnixStream::connect(&path).await.unwrap();
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
    let _ = std::fs::remove_file(&path);
}

#[test]
fn datagrams_report_the_sender_path_and_keep_boundaries() {
    let (pa, pb) = (sock_path("da"), sock_path("db"));
    block_on(async {
        let a = UnixDatagram::bind(&pa).unwrap();
        let b = UnixDatagram::bind(&pb).unwrap();
        for i in 0..50u8 {
            let (res, _) = a.send_to(pattern(1 + i as usize, i), &pb).await;
            res.unwrap();
        }
        for i in 0..50u8 {
            let (res, buf) = b.recv_from(vec![0u8; 1024]).await;
            let (n, from) = res.unwrap();
            assert_eq!(buf[..n], pattern(1 + i as usize, i)[..]);
            assert_eq!(from.as_deref(), Some(pa.as_path()));
        }
    });
    let _ = std::fs::remove_file(&pa);
    let _ = std::fs::remove_file(&pb);
}

#[test]
fn datagram_recv_waits_and_cancellation_loses_nothing() {
    block_on(async {
        let (a, b) = UnixDatagram::pair().unwrap();
        let r = time::timeout(Duration::from_millis(30), b.recv(vec![0u8; 64])).await;
        assert!(r.is_err(), "nothing sent yet");

        let sender = spawn(async move {
            time::sleep(Duration::from_millis(50)).await;
            let (res, _) = a.send(b"after".to_vec()).await;
            res.unwrap();
        });
        let (res, buf) = b.recv(vec![0u8; 64]).await;
        let n = res.expect("recv must wait for the datagram");
        assert_eq!(&buf[..n], b"after");
        sender.await.unwrap();
    });
}
