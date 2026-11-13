//! TLS over nexio streams, using [`rustls`].
//!
//! nexio does not implement any cryptography or protocol logic itself: this module
//! only moves bytes between a nexio stream and rustls's state machine. Certificate
//! verification, protocol versions, cipher suites and ALPN are all configured through
//! the standard rustls [`ClientConfig`] and
//! [`ServerConfig`] you pass in. There is no default trust
//! store: build your `ClientConfig` with the roots you intend to trust (for example the
//! `webpki-roots` crate, or `rustls-platform-verifier`).
//!
//! ```ignore
//! let connector = nexio::tls::TlsConnector::new(Arc::new(client_config));
//! let tcp = nexio::net::TcpStream::connect(("example.com", 443)).await?;
//! let tls = connector.connect("example.com".try_into()?, tcp).await?;
//! tls.write_all(b"hello".to_vec()).await.0?;
//! ```
//!
//! # Guarantees
//!
//! * **Truncation is an error.** If the connection ends without the peer's
//!   `close_notify`, `read` fails with `UnexpectedEof`; only a proper close reads as
//!   end-of-stream (`Ok(0)`). An attacker cannot silently cut your data short.
//! * **A cancelled `read` loses nothing.** Ciphertext fed to rustls stays inside
//!   rustls, so dropping a pending `read` leaves the stream usable.
//! * Concurrent use is fine: one task can read while another writes.
//!
//! The crypto provider is `ring`.
//!
//! [`ClientConfig`]: rustls::ClientConfig
//! [`ServerConfig`]: rustls::ServerConfig

use std::{
    cell::{Cell, RefCell},
    future::Future,
    io::{self, Read, Write},
    pin::Pin,
    rc::Rc,
    sync::Arc,
};

pub use rustls;
use rustls::{
    pki_types::ServerName, ClientConfig, ClientConnection, Connection, ServerConfig,
    ServerConnection,
};

use crate::{
    buf::{BufResult, IoBuf, IoBufMut},
    compat::{sealed, Duplex},
};

/// Bytes of ciphertext read from the transport per underlying read.
const READ_CHUNK: usize = 16 * 1024;

fn tls_err(e: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// A TLS session over a nexio stream `S` (usually a TCP stream).
pub struct TlsStream<S: Duplex> {
    io: Rc<S>,
    conn: RefCell<Connection>,
    /// The transport reported end-of-stream.
    eof: Cell<bool>,
    /// Buffers kept between calls so steady-state reads and writes do not allocate.
    out_spare: Cell<Vec<u8>>,
    read_spare: Cell<Vec<u8>>,
}

impl<S: Duplex> TlsStream<S> {
    fn new(io: S, conn: Connection) -> Self {
        Self {
            io: Rc::new(io),
            conn: RefCell::new(conn),
            eof: Cell::new(false),
            out_spare: Cell::new(Vec::new()),
            read_spare: Cell::new(Vec::new()),
        }
    }

    /// Write out everything rustls has queued for the peer.
    async fn flush_tls(&self) -> io::Result<()> {
        loop {
            let mut out = self.out_spare.take();
            out.clear();
            {
                let mut c = self.conn.borrow_mut();
                if !c.wants_write() {
                    self.out_spare.set(out);
                    return Ok(());
                }
                while c.wants_write() {
                    c.write_tls(&mut out)?;
                }
            }
            let (res, mut out) = self.io.clone().write_all_owned(out).await;
            out.clear();
            self.out_spare.set(out);
            res?;
        }
    }

    /// Read some ciphertext from the transport and hand it to rustls.
    async fn fill_tls(&self) -> io::Result<()> {
        let mut buf = self.read_spare.take();
        buf.clear();
        if buf.capacity() < READ_CHUNK {
            buf.reserve(READ_CHUNK - buf.len());
        }
        let (res, mut buf) = self.io.clone().read_owned(buf).await;
        let n = match res {
            Ok(n) => n,
            Err(e) => {
                buf.clear();
                self.read_spare.set(buf);
                return Err(e);
            }
        };
        let mut io_err: Option<io::Error> = None;
        let outcome: Result<(), rustls::Error> = {
            let mut c = self.conn.borrow_mut();
            if n == 0 {
                self.eof.set(true);
                // An empty read tells rustls the transport ended, so it can report a
                // truncated stream instead of waiting forever.
                let _ = c.read_tls(&mut io::empty());
                c.process_new_packets().map(|_| ())
            } else {
                let mut rest: &[u8] = &buf[..n];
                let mut result = Ok(());
                while !rest.is_empty() {
                    match c.read_tls(&mut rest) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(e) => {
                            io_err = Some(e);
                            break;
                        }
                    }
                    if let Err(e) = c.process_new_packets() {
                        result = Err(e);
                        break;
                    }
                }
                result
            }
        };
        buf.clear();
        self.read_spare.set(buf);
        if let Some(e) = io_err {
            return Err(e);
        }
        match outcome {
            Ok(()) => Ok(()),
            Err(e) => {
                // Send the alert rustls queued before reporting the failure.
                let _ = self.flush_tls().await;
                Err(tls_err(e))
            }
        }
    }

    async fn handshake(&self) -> io::Result<()> {
        loop {
            self.flush_tls().await?;
            if !self.conn.borrow().is_handshaking() {
                return Ok(());
            }
            if self.conn.borrow().wants_read() {
                self.fill_tls().await?;
                if self.eof.get() && self.conn.borrow().is_handshaking() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed during the TLS handshake",
                    ));
                }
            }
        }
    }

    /// Read decrypted data into `buf`. `Ok(0)` means the peer closed the connection
    /// cleanly (`close_notify`); a connection cut without it is an `UnexpectedEof` error.
    pub async fn read<B>(&self, mut buf: B) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        let total = buf.bytes_total();
        if total == 0 {
            return (Ok(0), buf);
        }
        let want = total.min(READ_CHUNK);
        let mut tmp = self.read_spare.take();
        tmp.resize(want, 0);
        loop {
            let r = self.conn.borrow_mut().reader().read(&mut tmp);
            match r {
                Ok(n) => {
                    // SAFETY: `tmp[..n]` is initialized, `n <= tmp.len() <= total`, and the
                    // two allocations are distinct.
                    unsafe {
                        std::ptr::copy_nonoverlapping(tmp.as_ptr(), buf.stable_ptr_mut(), n);
                        buf.set_init(n);
                    }
                    self.read_spare.set(tmp);
                    return (Ok(n), buf);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.fill_tls().await {
                        return (Err(e), buf);
                    }
                    // Answer anything rustls queued while processing (key updates, alerts).
                    if let Err(e) = self.flush_tls().await {
                        return (Err(e), buf);
                    }
                }
                Err(e) => return (Err(e), buf),
            }
        }
    }

    /// Read until `buf` is full; `UnexpectedEof` if the stream ends first.
    pub async fn read_exact<B>(&self, mut buf: B) -> BufResult<(), B>
    where
        B: IoBufMut + Send + 'static,
    {
        let total = buf.bytes_total();
        let mut got = 0usize;
        while got < total {
            // Read through a slice view so each call fills the next part of the buffer.
            // SAFETY: `got < total`, so the pointer stays inside the buffer's allocation.
            let tail = unsafe { buf.stable_ptr_mut().add(got) };
            let (res, _) = self
                .read(TailBuf {
                    ptr: tail,
                    len: total - got,
                })
                .await;
            match res {
                Ok(0) => {
                    let e =
                        io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer");
                    return (Err(e), buf);
                }
                Ok(n) => got += n,
                Err(e) => return (Err(e), buf),
            }
        }
        // SAFETY: `total` bytes were written into the buffer.
        unsafe { buf.set_init(total) };
        (Ok(()), buf)
    }

    /// Encrypt and send all of `buf`.
    pub async fn write_all<B>(&self, buf: B) -> BufResult<(), B>
    where
        B: IoBuf + Send + 'static,
    {
        let total = buf.bytes_init();
        let mut off = 0usize;
        while off < total {
            // SAFETY: `off < total <= bytes_init()`, inside the initialized region.
            let data =
                unsafe { std::slice::from_raw_parts(buf.stable_ptr().add(off), total - off) };
            let wrote = self.conn.borrow_mut().writer().write(data);
            match wrote {
                Ok(n) => off += n,
                Err(e) => return (Err(e), buf),
            }
            if let Err(e) = self.flush_tls().await {
                return (Err(e), buf);
            }
        }
        (Ok(()), buf)
    }

    /// Encrypt and send `buf`, returning how many bytes were sent (all of them).
    pub async fn write<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        let len = buf.bytes_init();
        let (res, buf) = self.write_all(buf).await;
        (res.map(|()| len), buf)
    }

    /// Send anything still queued.
    pub async fn flush(&self) -> io::Result<()> {
        self.flush_tls().await
    }

    /// Send `close_notify` and shut down the write side of the transport. Data from the
    /// peer can still be read afterwards.
    pub async fn shutdown(&self) -> io::Result<()> {
        self.conn.borrow_mut().send_close_notify();
        self.flush_tls().await?;
        self.io.clone().shutdown_owned().await
    }

    /// The ALPN protocol agreed during the handshake, if any (for example `b"http/1.1"`).
    pub fn alpn_protocol(&self) -> Option<Vec<u8>> {
        self.conn.borrow().alpn_protocol().map(<[u8]>::to_vec)
    }

    /// The wrapped transport.
    pub fn get_ref(&self) -> &S {
        &self.io
    }
}

/// A window onto the unfilled tail of a buffer, so `read_exact` can reuse `read`.
struct TailBuf {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: a `TailBuf` is built from, and used only within, a single `read_exact` call that owns
// the underlying buffer, and it never outlives that call.
unsafe impl IoBufMut for TailBuf {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.ptr
    }
    fn bytes_total(&self) -> usize {
        self.len
    }
    unsafe fn set_init(&mut self, _n: usize) {}
}
// `read` requires `Send + 'static`; a `TailBuf` never crosses a thread and borrows nothing.
unsafe impl Send for TailBuf {}
impl Unpin for TailBuf {}

impl<S: Duplex> sealed::Sealed for TlsStream<S> {}

#[allow(clippy::manual_async_fn)]
impl<S: Duplex> Duplex for TlsStream<S> {
    fn read_owned(
        self: Rc<Self>,
        buf: Vec<u8>,
    ) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + 'static {
        async move { self.read(buf).await }
    }

    fn write_all_owned(
        self: Rc<Self>,
        buf: Vec<u8>,
    ) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + 'static {
        async move { self.write(buf).await }
    }

    fn shutdown_owned(self: Rc<Self>) -> Pin<Box<dyn Future<Output = io::Result<()>>>> {
        Box::pin(async move { self.shutdown().await })
    }
}

/// Starts TLS client sessions with a shared configuration.
#[derive(Clone)]
pub struct TlsConnector {
    config: Arc<ClientConfig>,
}

impl TlsConnector {
    /// A connector using `config`.
    pub fn new(config: Arc<ClientConfig>) -> Self {
        Self { config }
    }

    /// Run the client handshake over `io`, verifying the server as `server_name`.
    pub async fn connect<S: Duplex>(
        &self,
        server_name: ServerName<'static>,
        io: S,
    ) -> io::Result<TlsStream<S>> {
        let conn = ClientConnection::new(self.config.clone(), server_name).map_err(tls_err)?;
        let stream = TlsStream::new(io, Connection::Client(conn));
        stream.handshake().await?;
        Ok(stream)
    }
}

/// Accepts TLS sessions with a shared configuration.
#[derive(Clone)]
pub struct TlsAcceptor {
    config: Arc<ServerConfig>,
}

impl TlsAcceptor {
    /// An acceptor using `config`.
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self { config }
    }

    /// Run the server handshake over `io`.
    pub async fn accept<S: Duplex>(&self, io: S) -> io::Result<TlsStream<S>> {
        let conn = ServerConnection::new(self.config.clone()).map_err(tls_err)?;
        let stream = TlsStream::new(io, Connection::Server(conn));
        stream.handshake().await?;
        Ok(stream)
    }
}
