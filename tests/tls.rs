//! TLS tests over real loopback sockets, with certificates generated on the fly.
#![cfg(feature = "tls")]

use std::{sync::Arc, time::Duration};

use nexio::{
    block_on,
    net::{TcpListener, TcpStream},
    spawn, time,
    tls::{
        rustls::{
            pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
            ClientConfig, RootCertStore, ServerConfig,
        },
        TlsAcceptor, TlsConnector,
    },
};

fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
}

struct Identity {
    cert: CertificateDer<'static>,
    key: Vec<u8>,
}

fn identity(name: &str) -> Identity {
    let ck = rcgen::generate_simple_self_signed(vec![name.to_string()]).unwrap();
    Identity {
        cert: ck.cert.der().clone(),
        key: ck.key_pair.serialize_der(),
    }
}

fn server_config(id: &Identity) -> Arc<ServerConfig> {
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(id.key.clone()));
    Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![id.cert.clone()], key)
            .unwrap(),
    )
}

fn client_config(trusted: &Identity) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(trusted.cert.clone()).unwrap();
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

fn name(s: &'static str) -> ServerName<'static> {
    ServerName::try_from(s).unwrap()
}

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = l.local_addr().unwrap();
    let c = TcpStream::connect(addr).await.unwrap();
    let s = l.accept().await.unwrap();
    c.set_nodelay(true).unwrap();
    s.set_nodelay(true).unwrap();
    (c, s)
}

#[test]
fn echo_over_tls() {
    block_on(async {
        let id = identity("localhost");
        let acceptor = TlsAcceptor::new(server_config(&id));
        let connector = TlsConnector::new(client_config(&id));
        let (c, s) = tcp_pair().await;

        let server = spawn(async move {
            let tls = acceptor.accept(s).await.unwrap();
            loop {
                let (res, buf) = tls.read(vec![0u8; 512]).await;
                let n = res.unwrap();
                if n == 0 {
                    break;
                }
                tls.write_all(buf[..n].to_vec()).await.0.unwrap();
            }
            tls.shutdown().await.unwrap();
        });

        let tls = connector.connect(name("localhost"), c).await.unwrap();
        for i in 0..30u8 {
            let msg = pattern(1 + i as usize * 17, i);
            tls.write_all(msg.clone()).await.0.unwrap();
            let (res, got) = tls.read_exact(vec![0u8; msg.len()]).await;
            res.unwrap();
            assert_eq!(got, msg);
        }
        tls.shutdown().await.unwrap();
        // The server's close_notify reads as a clean end of stream.
        let (res, _) = tls.read(vec![0u8; 8]).await;
        assert_eq!(res.unwrap(), 0);
        server.await.unwrap();
    });
}

#[test]
fn four_mebibytes_each_way() {
    block_on(async {
        let id = identity("localhost");
        let acceptor = TlsAcceptor::new(server_config(&id));
        let connector = TlsConnector::new(client_config(&id));
        let (c, s) = tcp_pair().await;
        const TOTAL: usize = 4 * 1024 * 1024;
        let data = pattern(TOTAL, 5);
        let expect = data.clone();

        let server = spawn(async move {
            let tls = std::rc::Rc::new(acceptor.accept(s).await.unwrap());
            let mut got = Vec::with_capacity(TOTAL);
            while got.len() < TOTAL {
                let (res, buf) = tls.read(vec![0u8; 32 * 1024]).await;
                let n = res.unwrap();
                assert!(n > 0, "unexpected end of stream");
                got.extend_from_slice(&buf[..n]);
            }
            tls.write_all(got).await.0.unwrap();
            tls.shutdown().await.unwrap();
        });

        let tls = std::rc::Rc::new(connector.connect(name("localhost"), c).await.unwrap());
        for chunk in data.chunks(64 * 1024) {
            tls.write_all(chunk.to_vec()).await.0.unwrap();
        }
        let mut got = Vec::with_capacity(TOTAL);
        while got.len() < TOTAL {
            let (res, buf) = tls.read(vec![0u8; 32 * 1024]).await;
            let n = res.unwrap();
            assert!(n > 0);
            got.extend_from_slice(&buf[..n]);
        }
        assert!(got == expect, "echoed bytes differ");
        server.await.unwrap();
    });
}

#[test]
fn a_certificate_for_another_name_is_rejected() {
    block_on(async {
        let id = identity("localhost");
        let acceptor = TlsAcceptor::new(server_config(&id));
        let connector = TlsConnector::new(client_config(&id));
        let (c, s) = tcp_pair().await;
        let server = spawn(async move { acceptor.accept(s).await.is_err() });
        let err = connector
            .connect(name("not-localhost.example"), c)
            .await
            .err()
            .expect("name mismatch must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err:?}");
        assert!(
            server.await.unwrap(),
            "the server sees the failed handshake"
        );
    });
}

#[test]
fn an_untrusted_certificate_is_rejected() {
    block_on(async {
        let server_id = identity("localhost");
        let other = identity("localhost");
        let acceptor = TlsAcceptor::new(server_config(&server_id));
        // The client trusts a different certificate than the one the server presents.
        let connector = TlsConnector::new(client_config(&other));
        let (c, s) = tcp_pair().await;
        let server = spawn(async move { acceptor.accept(s).await.is_err() });
        assert!(connector.connect(name("localhost"), c).await.is_err());
        assert!(server.await.unwrap());
    });
}

#[test]
fn a_cut_connection_is_an_error_not_a_clean_end() {
    block_on(async {
        let id = identity("localhost");
        let acceptor = TlsAcceptor::new(server_config(&id));
        let connector = TlsConnector::new(client_config(&id));
        let (c, s) = tcp_pair().await;
        let server = spawn(async move {
            let tls = acceptor.accept(s).await.unwrap();
            tls.write_all(b"partial".to_vec()).await.0.unwrap();
            // Drop the stream without sending close_notify.
            drop(tls);
        });
        let tls = connector.connect(name("localhost"), c).await.unwrap();
        server.await.unwrap();
        let (res, buf) = tls.read_exact(vec![0u8; 7]).await;
        res.unwrap();
        assert_eq!(buf, b"partial");
        let (res, _) = tls.read(vec![0u8; 8]).await;
        let err = res.expect_err("truncation must not look like a clean close");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof, "{err:?}");
    });
}

#[test]
fn a_cancelled_read_loses_nothing() {
    block_on(async {
        let id = identity("localhost");
        let acceptor = TlsAcceptor::new(server_config(&id));
        let connector = TlsConnector::new(client_config(&id));
        let (c, s) = tcp_pair().await;
        let server = spawn(async move {
            let tls = acceptor.accept(s).await.unwrap();
            time::sleep(Duration::from_millis(200)).await;
            tls.write_all(b"late data".to_vec()).await.0.unwrap();
            tls
        });
        let tls = connector.connect(name("localhost"), c).await.unwrap();
        let r = time::timeout(Duration::from_millis(30), tls.read(vec![0u8; 32])).await;
        assert!(r.is_err(), "nothing has been sent yet");
        let (res, buf) = tls.read_exact(vec![0u8; 9]).await;
        res.unwrap();
        assert_eq!(buf, b"late data");
        let _keep = server.await.unwrap();
    });
}

#[test]
fn alpn_is_negotiated() {
    block_on(async {
        let id = identity("localhost");
        let mut sc = Arc::try_unwrap(server_config(&id)).ok().unwrap();
        sc.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let mut roots = RootCertStore::empty();
        roots.add(id.cert.clone()).unwrap();
        let mut cc = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        cc.alpn_protocols = vec![b"http/1.1".to_vec()];
        let acceptor = TlsAcceptor::new(Arc::new(sc));
        let connector = TlsConnector::new(Arc::new(cc));
        let (c, s) = tcp_pair().await;
        let server = spawn(async move { acceptor.accept(s).await.unwrap().alpn_protocol() });
        let tls = connector.connect(name("localhost"), c).await.unwrap();
        assert_eq!(tls.alpn_protocol().as_deref(), Some(&b"http/1.1"[..]));
        assert_eq!(server.await.unwrap().as_deref(), Some(&b"http/1.1"[..]));
    });
}

#[cfg(feature = "tokio-compat")]
#[test]
fn tls_through_the_tokio_adapter() {
    use nexio::compat::Compat;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    block_on(async {
        let id = identity("localhost");
        let acceptor = TlsAcceptor::new(server_config(&id));
        let connector = TlsConnector::new(client_config(&id));
        let (c, s) = tcp_pair().await;
        let server = spawn(async move {
            let mut tls = Compat::new(acceptor.accept(s).await.unwrap());
            let mut req = Vec::new();
            tls.read_to_end(&mut req).await.unwrap();
            tls.write_all(format!("{} bytes", req.len()).as_bytes())
                .await
                .unwrap();
            tls.shutdown().await.unwrap();
        });
        let mut tls = Compat::new(connector.connect(name("localhost"), c).await.unwrap());
        tls.write_all(&pattern(10_000, 1)).await.unwrap();
        tls.shutdown().await.unwrap();
        let mut reply = String::new();
        tls.read_to_string(&mut reply).await.unwrap();
        assert_eq!(reply, "10000 bytes");
        server.await.unwrap();
    });
}
