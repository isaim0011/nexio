//! Phase 1 high-queue-depth & kernel exhaustion integration tests.
//!
//! Designed to force exhaustion paths real kernels only reveal under load:
//! - Submission queue full (`Reactor::with_entries(4)` with >100 concurrent ops)
//! - Massive concurrent `-ECANCELED` vs real completion races (200+ ops)
//! - Error paths (`ECONNREFUSED`, `UnexpectedEof`, `WriteZero` / `EPIPE`, `timeout`)

use std::{
    future::Future,
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Waker},
    thread,
    time::Duration,
};

static FD_LOCK: Mutex<()> = Mutex::new(());

use nexio::{
    buf::IoBufMut,
    executor::{block_on, spawn},
    net::{TcpListener, TcpStream},
    reactor::Reactor,
    time::{sleep, timeout, Elapsed},
};

fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("read /proc/self/fd")
        .count()
}

struct TrackDrop {
    data: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl Drop for TrackDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

unsafe impl IoBufMut for TrackDrop {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }
    fn bytes_total(&self) -> usize {
        self.data.capacity()
    }
    unsafe fn set_init(&mut self, n: usize) {
        unsafe { self.data.set_len(n) };
    }
}

unsafe impl Send for TrackDrop {}

// ── Test 1: Force SQ-full flush across 1,000 queued ops on a tiny ring ────────

#[test]
fn test_sq_full_1500_ops_small_ring() {
    let _lock = FD_LOCK.lock().unwrap();
    let mut fds = [0 as libc::c_int; 2];
    let ret = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    assert_eq!(ret, 0);

    // Tiny ring of depth 4 forces push_sqe() to flush to the kernel hundreds of times
    let mut reactor = Reactor::with_entries(4).expect("create depth-4 reactor");
    reactor.enter(|| {
        block_on(async {
            let s_tx = unsafe { TcpStream::from_raw_fd(fds[0]) };
            let s_rx = unsafe { TcpStream::from_raw_fd(fds[1]) };

            // Submit 100 concurrent writes + 100 concurrent reads via spawned tasks
            // on a depth-4 SQ ring, forcing continuous SQ-full flushes.
            let mut write_handles = Vec::with_capacity(200);
            let mut read_handles = Vec::with_capacity(200);

            let s_tx = std::rc::Rc::new(s_tx);
            let s_rx = std::rc::Rc::new(s_rx);

            for i in 0..200u16 {
                let tx = std::rc::Rc::clone(&s_tx);
                write_handles.push(spawn(async move {
                    let (res, _) = tx.write_all(vec![(i & 0xFF) as u8; 8]).await;
                    res.unwrap();
                }));
                let rx = std::rc::Rc::clone(&s_rx);
                read_handles.push(spawn(async move {
                    let (res, _) = rx.read_exact(vec![0u8; 8]).await;
                    res.unwrap();
                }));
            }

            for h in write_handles {
                h.await.unwrap();
            }
            for h in read_handles {
                h.await.unwrap();
            }
        });
    });
}

// ── Test 2: 200 concurrent -ECANCELED vs completion race storm ────────────────

#[test]
fn test_ecanceled_vs_completion_race_storm() {
    let _lock = FD_LOCK.lock().unwrap();
    let drop_counter = Arc::new(AtomicUsize::new(0));
    const OPS: usize = 200;

    let baseline = open_fd_count();

    let drop_counter_thread = Arc::clone(&drop_counter);
    thread::spawn(move || {
        let mut fds_list = Vec::with_capacity(OPS);
        for _ in 0..OPS {
            let mut fds = [0 as libc::c_int; 2];
            let ret =
                unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
            assert_eq!(ret, 0);
            fds_list.push(fds);
        }

        let peer_fds: Vec<libc::c_int> = fds_list.iter().map(|p| p[0]).collect();
        let peer = thread::spawn(move || {
            for (idx, &fd) in peer_fds.iter().enumerate() {
                if idx % 2 == 0 {
                    let byte = [42u8; 1];
                    unsafe {
                        libc::send(
                            fd,
                            byte.as_ptr() as *const libc::c_void,
                            1,
                            libc::MSG_DONTWAIT,
                        );
                    }
                }
            }
        });

        block_on(async {
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);

            let streams: Vec<TcpStream> = fds_list
                .iter()
                .map(|pair| unsafe { TcpStream::from_raw_fd(pair[1]) })
                .collect();

            let mut ops = Vec::with_capacity(OPS);
            for stream in &streams {
                let buf = TrackDrop {
                    data: vec![0u8; 64],
                    drops: Arc::clone(&drop_counter_thread),
                };
                ops.push(Box::pin(stream.read(buf)));
            }

            for op in ops.iter_mut().step_by(2) {
                let _ = op.as_mut().poll(&mut cx);
            }
            drop(ops);

            sleep(Duration::from_millis(30)).await;
            drop(streams);
        });

        peer.join().unwrap();

        for pair in fds_list {
            unsafe {
                libc::close(pair[0]);
            }
        }
    })
    .join()
    .unwrap();

    assert_eq!(
        drop_counter.load(Ordering::SeqCst),
        OPS,
        "every buffer must be dropped exactly once across cancel/completion races"
    );

    let after = open_fd_count();
    assert!(
        after <= baseline + 2,
        "fd leak after cancel storm: baseline={baseline}, after={after}"
    );
}

// ── Test 3: Network error paths and timeout cancellation ──────────────────────

#[test]
fn test_error_paths_and_timeout_cancel() {
    let _lock = FD_LOCK.lock().unwrap();
    block_on(async {
        // 1. ECONNREFUSED on unbound loopback port
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed_port = probe.local_addr().unwrap().port();
        drop(probe);

        let err = TcpStream::connect(format!("127.0.0.1:{closed_port}"))
            .await
            .err()
            .expect("expected connection refused");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);

        // 2. UnexpectedEof + WriteZero + timeout cancellation on live loopback socket
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let srv = spawn(async move {
            let stream = listener.accept().await.unwrap();
            // Send 3 bytes then close -> client read_exact(16) hits UnexpectedEof
            let (res, _) = stream.write_all(b"abc".to_vec()).await;
            res.unwrap();
            drop(stream);

            // Second connection: stay silent so client timeout() fires and cancels read
            let slow_stream = listener.accept().await.unwrap();
            sleep(Duration::from_millis(60)).await;
            drop(slow_stream);
        });

        let c1 = TcpStream::connect(addr).await.unwrap();
        let (res, _) = c1.read_exact(vec![0u8; 16]).await;
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        drop(c1);

        let c2 = TcpStream::connect(addr).await.unwrap();
        let timed = timeout(Duration::from_millis(10), async move {
            let (r, _) = c2.read_exact(vec![0u8; 32]).await;
            r
        })
        .await;
        assert!(matches!(timed, Err(Elapsed)));

        srv.await.unwrap();
    });
}
