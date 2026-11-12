//! nexio streams used through tokio's `AsyncRead`/`AsyncWrite` via `nexio::compat::Compat`.
//! (No tokio runtime is involved: only tokio's traits and extension methods.)
#![cfg(feature = "tokio-compat")]

use nexio::{
    block_on,
    compat::Compat,
    net::{TcpListener, TcpStream},
    spawn, time,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
}

async fn connected_pair() -> (Compat<TcpStream>, Compat<TcpStream>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).await.unwrap();
    let server = listener.accept().await.unwrap();
    // Request/response traffic made of several small writes needs TCP_NODELAY, as in any real
    // server; without it Nagle's algorithm and delayed ACKs stall each round trip by ~40 ms.
    client.set_nodelay(true).unwrap();
    server.set_nodelay(true).unwrap();
    (Compat::new(client), Compat::new(server))
}

#[test]
fn echo_with_tokio_extension_methods() {
    block_on(async {
        let (mut client, mut server) = connected_pair().await;
        let echo = spawn(async move {
            let mut buf = [0u8; 100];
            loop {
                let n = server.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                server.write_all(&buf[..n]).await.unwrap();
                server.flush().await.unwrap();
            }
        });
        for i in 0..50u8 {
            let msg = pattern(1 + i as usize * 11, i);
            client.write_all(&msg).await.unwrap();
            client.flush().await.unwrap();
            let mut got = vec![0u8; msg.len()];
            client.read_exact(&mut got).await.unwrap();
            assert_eq!(got, msg);
        }
        client.shutdown().await.unwrap();
        echo.await.unwrap();
    });
}

#[test]
fn a_tiny_destination_buffer_never_drops_bytes() {
    block_on(async {
        let (mut client, mut server) = connected_pair().await;
        let data = pattern(5000, 9);
        server.write_all(&data).await.unwrap();
        server.shutdown().await.unwrap();

        // Read 7 bytes at a time: each underlying read returns far more than that.
        let mut got = Vec::new();
        let mut small = [0u8; 7];
        loop {
            let n = client.read(&mut small).await.unwrap();
            if n == 0 {
                break;
            }
            got.extend_from_slice(&small[..n]);
        }
        assert_eq!(got, data);
    });
}

#[test]
fn four_mebibytes_both_ways_through_split() {
    block_on(async {
        let (client, server) = connected_pair().await;
        const TOTAL: usize = 4 * 1024 * 1024;
        let data = pattern(TOTAL, 3);
        let expect = data.clone();

        // The server echoes everything it receives, concurrently with the client writing.
        let echo = spawn(async move {
            let (mut r, mut w) = tokio::io::split(server);
            tokio::io::copy(&mut r, &mut w).await.unwrap();
            w.shutdown().await.unwrap();
        });

        let (mut r, mut w) = tokio::io::split(client);
        let writer = spawn(async move {
            w.write_all(&data).await.unwrap();
            w.shutdown().await.unwrap();
        });
        let mut got = Vec::new();
        r.read_to_end(&mut got).await.unwrap();
        writer.await.unwrap();
        echo.await.unwrap();
        assert_eq!(got.len(), expect.len());
        assert!(got == expect, "echoed bytes differ");
    });
}

#[test]
fn a_cancelled_read_loses_nothing() {
    block_on(async {
        let (mut client, mut server) = connected_pair().await;
        let mut buf = [0u8; 64];
        let r = time::timeout(Duration::from_millis(30), client.read(&mut buf)).await;
        assert!(r.is_err(), "nothing was sent yet");

        server.write_all(b"after the timeout").await.unwrap();
        server.flush().await.unwrap();
        let mut got = vec![0u8; 17];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(got, b"after the timeout");
    });
}

#[test]
fn half_close_lets_the_peer_reply_after_end_of_input() {
    block_on(async {
        let (mut client, mut server) = connected_pair().await;
        let responder = spawn(async move {
            let mut request = Vec::new();
            server.read_to_end(&mut request).await.unwrap();
            let reply = format!("got {} bytes", request.len());
            server.write_all(reply.as_bytes()).await.unwrap();
            server.shutdown().await.unwrap();
        });
        client.write_all(&pattern(1234, 1)).await.unwrap();
        client.shutdown().await.unwrap();
        let mut reply = String::new();
        client.read_to_string(&mut reply).await.unwrap();
        assert_eq!(reply, "got 1234 bytes");
        responder.await.unwrap();
    });
}

#[test]
fn eof_reads_zero_and_a_write_to_a_closed_peer_errors() {
    block_on(async {
        let (mut client, server) = connected_pair().await;
        drop(server);
        let mut buf = [0u8; 8];
        assert_eq!(client.read(&mut buf).await.unwrap(), 0);

        // The first writes may be accepted into the buffer; an error must surface soon after.
        let mut failed = false;
        for _ in 0..50 {
            if client.write_all(&[0u8; 4096]).await.is_err() || client.flush().await.is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed, "writing to a closed peer must eventually fail");
    });
}

#[cfg(unix)]
#[test]
fn unix_streams_work_the_same_way() {
    block_on(async {
        let (a, b) = nexio::net::UnixStream::pair().unwrap();
        let (mut a, mut b) = (Compat::new(a), Compat::new(b));
        a.write_all(b"over a unix socket").await.unwrap();
        a.flush().await.unwrap();
        let mut got = vec![0u8; 18];
        b.read_exact(&mut got).await.unwrap();
        assert_eq!(got, b"over a unix socket");
    });
}

#[test]
fn many_concurrent_adapter_connections() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        const N: usize = 30;
        let server = spawn(async move {
            for _ in 0..N {
                let stream = listener.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                drop(spawn(async move {
                    let mut s = Compat::new(stream);
                    let mut buf = [0u8; 64];
                    let n = s.read(&mut buf).await.unwrap();
                    s.write_all(&buf[..n]).await.unwrap();
                    s.flush().await.unwrap();
                    // Wait for the client to close, like a keep-alive server would.
                    let _ = s.read(&mut buf).await;
                }));
            }
        });
        let mut clients = Vec::new();
        for i in 0..N {
            clients.push(spawn(async move {
                let tcp = TcpStream::connect(addr).await.unwrap();
                tcp.set_nodelay(true).unwrap();
                let mut c = Compat::new(tcp);
                let msg = format!("hello {i}");
                c.write_all(msg.as_bytes()).await.unwrap();
                c.flush().await.unwrap();
                let mut got = vec![0u8; msg.len()];
                c.read_exact(&mut got).await.unwrap();
                assert_eq!(got, msg.as_bytes());
            }));
        }
        for c in clients {
            c.await.unwrap();
        }
        server.await.unwrap();
    });
}
