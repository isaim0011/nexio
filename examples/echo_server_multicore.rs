//! The same echo server on every core: one reactor per thread, with the kernel
//! spreading connections across them via SO_REUSEPORT.
//!
//!     cargo run --release --example echo_server_multicore

use nexio::net::TcpListener;

fn main() {
    let shards = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    println!("starting {shards} shard(s) on 127.0.0.1:7879");
    nexio::run_sharded(shards, |_shard| async move {
        let listener = TcpListener::bind(("127.0.0.1", 7879)).await.expect("bind");
        loop {
            let Ok(stream) = listener.accept().await else {
                break;
            };
            drop(nexio::spawn(async move {
                let mut buf = Vec::with_capacity(4096);
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
    });
}
