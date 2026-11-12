//! Async file I/O and timeouts: write a file, read it back, and show that a
//! slow operation can be abandoned safely.
//!
//!     cargo run --release --example file_and_timeout

use nexio::{fs::File, time};
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let path = std::env::temp_dir().join("nexio_example.txt");
    nexio::block_on(async {
        let file = File::create(&path).await?;
        let (res, _) = file.write_all_at(b"hello from nexio".to_vec(), 0).await;
        res?;
        file.sync_all().await?;
        file.close().await?;

        let file = File::open(&path).await?;
        let (res, buf) = file.read_exact_at(vec![0u8; 16], 0).await;
        res?;
        println!("read back: {}", String::from_utf8_lossy(&buf));
        file.close().await?;

        // A future that loses the race is dropped; nexio cancels the in-flight
        // operation and keeps its buffer alive until the kernel is done with it.
        match time::timeout(
            Duration::from_millis(50),
            time::sleep(Duration::from_secs(10)),
        )
        .await
        {
            Ok(()) => println!("sleep finished"),
            Err(_) => println!("timed out after 50ms, as expected"),
        }
        std::fs::remove_file(&path)
    })
}
