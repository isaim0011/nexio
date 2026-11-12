//! Multi-core helper: one reactor per thread, each pinned to its own core.

use std::future::Future;

/// Run `factory(i)` to completion on `threads` independent reactors, one OS
/// thread each, and return every result in shard order.
///
/// Thread `i` is pinned to CPU `i % available_cpus` (best effort: pinning
/// failures are ignored). Each shard has its own io_uring instance and its own
/// single-threaded executor, so futures never need to be `Send` and shards
/// share no state. Combine with [`TcpListener::bind`](crate::net::TcpListener::bind),
/// which enables `SO_REUSEPORT`, to let the kernel spread incoming connections
/// across shards:
///
/// ```no_run
/// nexio::run_sharded(4, |shard| async move {
///     let listener = nexio::net::TcpListener::bind(("0.0.0.0", 7878)).await.unwrap();
///     loop {
///         let stream = listener.accept().await.unwrap();
///         nexio::spawn(async move {
///             let mut buf = Vec::with_capacity(4096);
///             loop {
///                 let (res, b) = stream.read(buf).await;
///                 buf = b;
///                 match res {
///                     Ok(0) | Err(_) => break,
///                     Ok(_) => {
///                         let (res, b) = stream.write_all(buf).await;
///                         buf = b;
///                         if res.is_err() {
///                             break;
///                         }
///                     }
///                 }
///             }
///         });
///         let _ = shard;
///     }
/// });
/// ```
///
/// # Panics
///
/// Panics if `threads` is zero, or if any shard thread panics.
pub fn run_sharded<F, Fut>(threads: usize, factory: F) -> Vec<Fut::Output>
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future + 'static,
    Fut::Output: Send + 'static,
{
    assert!(threads > 0, "run_sharded needs at least one thread");
    let factory = std::sync::Arc::new(factory);
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let handles: Vec<_> = (0..threads)
        .map(|i| {
            let factory = std::sync::Arc::clone(&factory);
            std::thread::Builder::new()
                .name(format!("nexio-shard-{i}"))
                .spawn(move || {
                    pin_to_cpu(i % cpus);
                    crate::block_on(factory(i))
                })
                .expect("failed to spawn shard thread")
        })
        .collect();
    handles
        .into_iter()
        .map(|h| {
            h.join()
                .unwrap_or_else(|_| panic!("a nexio shard thread panicked"))
        })
        .collect()
}

fn pin_to_cpu(cpu: usize) {
    // SAFETY: cpu_set_t is plain data; sched_setaffinity only reads it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        let _ = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(test)]
mod tests {
    use super::run_sharded;

    #[test]
    fn runs_every_shard_and_returns_results_in_order() {
        let out = run_sharded(3, |i| async move { i * 10 });
        assert_eq!(out, vec![0, 10, 20]);
    }

    #[test]
    fn shards_have_independent_reactors() {
        let out = run_sharded(2, |i| async move {
            crate::time::sleep(std::time::Duration::from_millis(5)).await;
            i
        });
        assert_eq!(out, vec![0, 1]);
    }

    #[test]
    #[should_panic(expected = "at least one thread")]
    fn zero_threads_is_rejected() {
        run_sharded(0, |_| async {});
    }
}
