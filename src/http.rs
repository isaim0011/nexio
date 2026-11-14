//! HTTP/1.1 and HTTP/2 client and server connections, built on [`hyper`] over nexio streams.
//!
//! nexio contains no HTTP parser of its own: hyper does the protocol work, and this
//! module connects it to nexio's sockets (through [`Compat`]),
//! so requests and responses are the standard `http` types. Any [`Duplex`] stream
//! works: a plain [`TcpStream`], or a
//! [`TlsStream`](crate::tls::TlsStream) for HTTPS (with the `tls` feature).
//!
//! ```ignore
//! // High-level server:
//! nexio::http::Server::bind(("127.0.0.1", 8080)).await?.serve(|_stream| {
//!     hyper::service::service_fn(|_req| async {
//!         Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from("hi"))))
//!     })
//! }).await?;
//!
//! // Client:
//! let mut client = nexio::http::connect(TcpStream::connect(addr).await?).await?;
//! let resp = client.send(Request::get("/").header("host", "x").body(Empty::<Bytes>::new())?).await?;
//! ```

use std::{error::Error as StdError, future::Future, io, net::SocketAddr};

pub use http_body_util;
pub use hyper;
use hyper::{
    body::{Body, Incoming},
    client::conn::http1::SendRequest,
    server::conn::{http1, http2},
    Request, Response,
};

use crate::{
    addr::ToAddrs,
    compat::{Compat, Duplex},
    executor::Either,
    net::{TcpListener, TcpStream},
    select2, spawn,
};

type BoxError = Box<dyn StdError + Send + Sync>;

/// Single-threaded [`hyper::rt::Executor`] that spawns tasks onto the current nexio shard.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalExec;

impl<F> hyper::rt::Executor<F> for LocalExec
where
    F: Future + 'static,
{
    fn execute(&self, fut: F) {
        drop(spawn(fut));
    }
}

/// Serve HTTP/1.1 requests arriving on `io` until the connection ends, answering each
/// with `service` (for example one made with `hyper::service::service_fn`).
///
/// Keep-alive is on. Run each connection in its own task with [`spawn`].
pub async fn serve_connection<S, Svc, B>(io: S, service: Svc) -> hyper::Result<()>
where
    S: Duplex,
    Svc: hyper::service::Service<Request<Incoming>, Response = Response<B>>,
    Svc::Error: Into<BoxError>,
    Svc::Future: 'static,
    B: Body + 'static,
    B::Error: Into<BoxError>,
{
    http1::Builder::new()
        .serve_connection(Compat::new(io), service)
        .await
}

/// Serve HTTP/2 requests arriving on `io` until the connection ends.
pub async fn serve_http2_connection<S, Svc, B>(io: S, service: Svc) -> hyper::Result<()>
where
    S: Duplex,
    Svc: hyper::service::Service<Request<Incoming>, Response = Response<B>>,
    Svc::Error: Into<BoxError>,
    Svc::Future: 'static,
    B: Body + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    http2::Builder::new(LocalExec)
        .serve_connection(Compat::new(io), service)
        .await
}

/// High-level HTTP/1.1 server bound to a [`TcpListener`].
pub struct Server {
    listener: TcpListener,
}

impl Server {
    /// Bind an HTTP server to `addr`.
    pub async fn bind(addr: impl ToAddrs) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener })
    }

    /// Wrap an existing [`TcpListener`].
    pub fn from_listener(listener: TcpListener) -> Self {
        Self { listener }
    }

    /// Return the local bound address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept connections and serve each with a service returned by `make_service`.
    pub async fn serve<M, Svc, B>(self, mut make_service: M) -> io::Result<()>
    where
        M: FnMut(&TcpStream) -> Svc,
        Svc: hyper::service::Service<Request<Incoming>, Response = Response<B>> + 'static,
        Svc::Error: Into<BoxError>,
        Svc::Future: 'static,
        B: Body + 'static,
        B::Error: Into<BoxError>,
    {
        loop {
            let stream = self.listener.accept().await?;
            let _ = stream.set_nodelay(true);
            let svc = make_service(&stream);
            drop(spawn(async move {
                let _ = serve_connection(stream, svc).await;
            }));
        }
    }

    /// Accept connections until `shutdown` resolves, then stop accepting.
    pub async fn serve_with_shutdown<M, Svc, B, Sh>(
        self,
        mut make_service: M,
        shutdown: Sh,
    ) -> io::Result<()>
    where
        M: FnMut(&TcpStream) -> Svc,
        Svc: hyper::service::Service<Request<Incoming>, Response = Response<B>> + 'static,
        Svc::Error: Into<BoxError>,
        Svc::Future: 'static,
        B: Body + 'static,
        B::Error: Into<BoxError>,
        Sh: Future<Output = ()>,
    {
        let accept_loop = async {
            loop {
                let stream = self.listener.accept().await?;
                let _ = stream.set_nodelay(true);
                let svc = make_service(&stream);
                drop(spawn(async move {
                    let _ = serve_connection(stream, svc).await;
                }));
            }
            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        };
        match select2(accept_loop, shutdown).await {
            Either::Left(res) => res,
            Either::Right(()) => Ok(()),
        }
    }
}

/// An HTTP/1.1 client connection. Requests are sent one at a time, in order.
pub struct Client<B> {
    sender: SendRequest<B>,
}

/// Run the client side of HTTP/1.1 over `io`. The connection's driver runs in a task
/// spawned on the current nexio runtime and ends when the client is dropped or the
/// peer closes.
pub async fn connect<S, B>(io: S) -> hyper::Result<Client<B>>
where
    S: Duplex,
    B: Body + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    let (sender, conn) = hyper::client::conn::http1::handshake(Compat::new(io)).await?;
    // Detached on purpose: the driver ends when the client is dropped.
    drop(spawn(async move {
        let _ = conn.await;
    }));
    Ok(Client { sender })
}

impl<B> Client<B>
where
    B: Body + 'static,
{
    /// Send `req` and return the response once its headers arrive; the body streams in
    /// as you read it. Set a `Host` header (HTTP/1.1 requires one).
    pub async fn send(&mut self, req: Request<B>) -> hyper::Result<Response<Incoming>> {
        self.sender.ready().await?;
        self.sender.send_request(req).await
    }

    /// Whether the connection can still be used.
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}
