//! A TCP echo server on one reactor.
//!
//! Run it, then in another terminal: `nc 127.0.0.1 7878`
//!
//!     cargo run --release --example echo_server

use nexio::net::TcpListener;

fn main() -> std::io::Result<()> {
    nexio::block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 7878)).await?;
        println!("echo server listening on {}", listener.local_addr()?);
        loop {
            let stream = listener.accept().await?;
            // Dropping the JoinHandle detaches the task; it keeps running.
            drop(nexio::spawn(async move {
                // Buffers are owned and handed to the kernel for the duration of
                // each operation, then returned alongside the result.
                let mut buf = Vec::with_capacity(4096);
                loop {
                    let (res, b) = stream.read(buf).await;
                    buf = b;
                    match res {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let (res, b) = stream.write_all(buf).await;
                            buf = b;
                            if res.is_err() {
                                break;
                            }
                        }
                    }
                }
            }));
        }
    })
}
