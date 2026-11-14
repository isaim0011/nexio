//! HTTP/1.1 client and server over loopback, plain and (with `tls`) encrypted.
#![cfg(feature = "http")]

use std::convert::Infallible;

use http_body_util::{BodyExt, Empty, Full};
use hyper::{body::Bytes, service::service_fn, Method, Request, Response};
use nexio::{
    block_on, http,
    net::{TcpListener, TcpStream},
    spawn,
};

fn http_pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

#[test]
fn get_and_post_with_keep_alive() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = spawn(async move {
            let stream = listener.accept().await.unwrap();
            stream.set_nodelay(true).unwrap();
            http::serve_connection(
                stream,
                service_fn(|req: Request<hyper::body::Incoming>| async move {
                    let reply = match (req.method(), req.uri().path()) {
                        (&Method::GET, "/hello") => Bytes::from("hello"),
                        (&Method::POST, "/len") => {
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            Bytes::from(format!("{}", body.len()))
                        }
                        _ => Bytes::from("?"),
                    };
                    Ok::<_, Infallible>(Response::new(Full::new(reply)))
                }),
            )
            .await
            .unwrap();
        });

        let tcp = TcpStream::connect(addr).await.unwrap();
        tcp.set_nodelay(true).unwrap();
        let mut client = http::connect::<_, Full<Bytes>>(tcp).await.unwrap();

        // Three requests on one connection.
        let req = Request::get("/hello")
            .header("host", "localhost")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = client.send(req).await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.into_body().collect().await.unwrap().to_bytes(),
            "hello"
        );

        let payload = http_pattern(300_000);
        let req = Request::post("/len")
            .header("host", "localhost")
            .body(Full::new(Bytes::from(payload)))
            .unwrap();
        let resp = client.send(req).await.unwrap();
        assert_eq!(
            resp.into_body().collect().await.unwrap().to_bytes(),
            "300000"
        );

        let req = Request::get("/nope")
            .header("host", "localhost")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = client.send(req).await.unwrap();
        assert_eq!(resp.into_body().collect().await.unwrap().to_bytes(), "?");

        drop(client);
        server.await.unwrap();
    });
}

#[test]
fn many_concurrent_connections() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        const N: usize = 40;
        let server = spawn(async move {
            for _ in 0..N {
                let stream = listener.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                drop(spawn(async move {
                    let _ = http::serve_connection(
                        stream,
                        service_fn(|req: Request<hyper::body::Incoming>| async move {
                            let p = req.uri().path().to_string();
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(p))))
                        }),
                    )
                    .await;
                }));
            }
        });
        let mut tasks = Vec::new();
        for i in 0..N {
            tasks.push(spawn(async move {
                let tcp = TcpStream::connect(addr).await.unwrap();
                tcp.set_nodelay(true).unwrap();
                let mut client = http::connect::<_, Empty<Bytes>>(tcp).await.unwrap();
                let req = Request::get(format!("/c{i}"))
                    .header("host", "localhost")
                    .body(Empty::new())
                    .unwrap();
                let resp = client.send(req).await.unwrap();
                let body = resp.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(body, format!("/c{i}").as_bytes());
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        server.await.unwrap();
    });
}

#[cfg(feature = "tls")]
#[test]
fn https_end_to_end() {
    use nexio::tls::{
        rustls::{
            pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
            ClientConfig, RootCertStore, ServerConfig,
        },
        TlsAcceptor, TlsConnector,
    };
    use std::sync::Arc;

    block_on(async {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert = ck.cert.der().clone();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(ck.key_pair.serialize_der()));
        let sc = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key)
            .unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(cert).unwrap();
        let cc = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let acceptor = TlsAcceptor::new(Arc::new(sc));
        let connector = TlsConnector::new(Arc::new(cc));

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = spawn(async move {
            let tcp = listener.accept().await.unwrap();
            tcp.set_nodelay(true).unwrap();
            let tls = acceptor.accept(tcp).await.unwrap();
            let _ = http::serve_connection(
                tls,
                service_fn(|_req: Request<hyper::body::Incoming>| async {
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from("secure"))))
                }),
            )
            .await;
        });

        let tcp = TcpStream::connect(addr).await.unwrap();
        tcp.set_nodelay(true).unwrap();
        let tls = connector
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        let mut client = http::connect::<_, Empty<Bytes>>(tls).await.unwrap();
        let req = Request::get("/")
            .header("host", "localhost")
            .body(Empty::new())
            .unwrap();
        let resp = client.send(req).await.unwrap();
        assert_eq!(
            resp.into_body().collect().await.unwrap().to_bytes(),
            "secure"
        );
        drop(client);
        server.await.unwrap();
    });
}

#[test]
fn server_bind_and_serve_with_shutdown() {
    block_on(async {
        let server = http::Server::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = server.local_addr().unwrap();
        let (tx, rx) = nexio::sync::oneshot::channel::<()>();

        let srv_task = spawn(async move {
            server
                .serve_with_shutdown(
                    |_stream| {
                        service_fn(|_req: Request<hyper::body::Incoming>| async {
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from("server-ok"))))
                        })
                    },
                    async move {
                        let _ = rx.await;
                    },
                )
                .await
                .unwrap();
        });

        let tcp = TcpStream::connect(addr).await.unwrap();
        tcp.set_nodelay(true).unwrap();
        let mut client = http::connect::<_, Empty<Bytes>>(tcp).await.unwrap();
        let req = Request::get("/")
            .header("host", "localhost")
            .body(Empty::new())
            .unwrap();
        let resp = client.send(req).await.unwrap();
        assert_eq!(
            resp.into_body().collect().await.unwrap().to_bytes(),
            "server-ok"
        );
        drop(client);
        let _ = tx.send(());
        srv_task.await.unwrap();
    });
}

#[test]
fn http2_end_to_end() {
    block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = spawn(async move {
            let tcp = listener.accept().await.unwrap();
            tcp.set_nodelay(true).unwrap();
            let _ = http::serve_http2_connection(
                tcp,
                service_fn(|req: Request<hyper::body::Incoming>| async move {
                    let path = req.uri().path().to_owned();
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(format!("h2:{path}")))))
                }),
            )
            .await;
        });

        let tcp = TcpStream::connect(addr).await.unwrap();
        tcp.set_nodelay(true).unwrap();
        let (mut sender, conn) =
            hyper::client::conn::http2::handshake(http::LocalExec, nexio::compat::Compat::new(tcp))
                .await
                .unwrap();
        drop(spawn(async move {
            let _ = conn.await;
        }));

        let r1 = sender
            .send_request(Request::get("/alpha").body(Empty::<Bytes>::new()).unwrap())
            .await
            .unwrap();
        let r2 = sender
            .send_request(Request::get("/beta").body(Empty::<Bytes>::new()).unwrap())
            .await
            .unwrap();

        assert_eq!(
            r1.into_body().collect().await.unwrap().to_bytes(),
            "h2:/alpha"
        );
        assert_eq!(
            r2.into_body().collect().await.unwrap().to_bytes(),
            "h2:/beta"
        );
        drop(sender);
        server.await.unwrap();
    });
}
