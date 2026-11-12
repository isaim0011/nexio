//! HTTP/1.1 client and server connections, built on [`hyper`] over nexio streams.
//!
//! nexio contains no HTTP parser of its own: hyper does the protocol work, and this
//! module connects it to nexio's sockets (through [`Compat`]),
//! so requests and responses are the standard `http` types. Any [`Duplex`] stream
//! works: a plain [`TcpStream`](crate::net::TcpStream), or a
//! [`TlsStream`](crate::tls::TlsStream) for HTTPS (with the `tls` feature).
//!
//! ```ignore
//! // Server: one task per connection.
//! let stream = listener.accept().await?;
//! nexio::spawn(async move {
//!     let _ = nexio::http::serve_connection(stream, service_fn(|req| async {
//!         Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from("hi"))))
//!     })).await;
//! });
//!
//! // Client.
//! let mut client = nexio::http::connect(TcpStream::connect(addr).await?).await?;
//! let resp = client.send(Request::get("/").header("host", "x").body(Empty::<Bytes>::new())?).await?;
//! ```
//!
//! Only HTTP/1.1 is provided. Futures here are not `Send`, like everything in nexio, which
//! rules out hyper's HTTP/2 (it requires `Send` tasks).

use std::error::Error as StdError;

pub use http_body_util;
pub use hyper;
use hyper::{
    body::{Body, Incoming},
    client::conn::http1::SendRequest,
    server::conn::http1,
    Request, Response,
};

use crate::{
    compat::{Compat, Duplex},
    spawn,
};

type BoxError = Box<dyn StdError + Send + Sync>;

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
