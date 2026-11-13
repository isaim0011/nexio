#![cfg(feature = "macros")]

//! Integration tests for nexio-macros, sync primitives, time ergonomics,
//! filesystem helpers, stream split/into_split, vectored I/O, and reactor stats.

use std::{
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use nexio::{
    fs::{self, File},
    net::{TcpListener, TcpStream},
    reactor::{current_stats, Reactor},
    sync::{broadcast, mpsc, oneshot, watch, CancellationToken, Mutex, Notify, RwLock, Semaphore},
    time::{interval_at, sleep, timeout_at, MissedTickBehavior},
};

#[nexio::test]
async fn macro_test_runs_on_nexio_runtime() {
    let val = nexio::spawn(async { 21 + 21 }).await.unwrap();
    assert_eq!(val, 42);
    assert!(matches!(Reactor::backend_name(), "io_uring" | "poll"));
    assert!(current_stats().is_some());
}

#[nexio::test]
async fn sync_notify_semaphore_mutex_rwlock() {
    let notify = Arc::new(Notify::new());
    let n2 = Arc::clone(&notify);
    let h = nexio::spawn(async move {
        n2.notified().await;
        7u32
    });
    nexio::yield_now().await;
    notify.notify_one();
    assert_eq!(h.await.unwrap(), 7);

    let sem = Rc::new(Semaphore::new(2));
    let p1 = sem.acquire().await;
    let p2 = sem.acquire().await;
    assert!(sem.try_acquire().is_err());
    drop(p1);
    assert_eq!(sem.available_permits(), 1);
    drop(p2);
    assert_eq!(sem.available_permits(), 2);

    let mtx = Rc::new(Mutex::new(10));
    {
        let mut g = mtx.lock().await;
        *g += 5;
    }
    assert_eq!(*mtx.lock().await, 15);

    let rw = Rc::new(RwLock::new(vec![1, 2]));
    {
        let r1 = rw.read().await;
        let r2 = rw.read().await;
        assert_eq!(r1.len() + r2.len(), 4);
    }
    {
        let mut w = rw.write().await;
        w.push(3);
    }
    assert_eq!(&*rw.read().await, &[1, 2, 3]);
}

#[nexio::test]
async fn sync_watch_broadcast_and_cancellation_token() {
    let (wtx, mut wrx) = watch::channel(1u32);
    let mut wrx2 = wrx.clone();
    wtx.send(2).unwrap();
    wrx.changed().await.unwrap();
    assert_eq!(*wrx.borrow(), 2);
    assert!(wrx2.has_changed().unwrap());
    assert_eq!(*wrx2.borrow_and_update(), 2);

    let (btx, mut brx1) = broadcast::channel::<u32>(4);
    let mut brx2 = btx.subscribe();
    btx.send(10).unwrap();
    btx.send(20).unwrap();
    assert_eq!(brx1.recv().await.unwrap(), 10);
    assert_eq!(brx1.recv().await.unwrap(), 20);
    assert_eq!(brx2.recv().await.unwrap(), 10);

    let parent = CancellationToken::new();
    let child = parent.child_token();
    let c2 = child.clone();
    let h = nexio::spawn(async move {
        c2.cancelled().await;
        true
    });
    parent.cancel();
    assert!(child.is_cancelled());
    assert!(h.await.unwrap());

    // Ensure oneshot/mpsc re-exports work through nexio::sync
    let (otx, orx) = oneshot::channel();
    otx.send(99).unwrap();
    assert_eq!(orx.await.unwrap(), 99);
    let (mtx, mut mrx) = mpsc::channel(2);
    mtx.send(123).await.unwrap();
    assert_eq!(mrx.recv().await, Some(123));
}

#[nexio::test]
async fn time_sleep_reset_timeout_at_and_interval() {
    let mut s = sleep(Duration::from_millis(5));
    let first_dl = s.deadline();
    assert!(!s.is_elapsed());
    s.reset(Instant::now() + Duration::from_millis(12));
    assert!(s.deadline() >= first_dl);
    s.await;

    let res = timeout_at(Instant::now() + Duration::from_millis(50), async { 55 }).await;
    assert_eq!(res, Ok(55));

    let mut iv = interval_at(
        Instant::now() + Duration::from_millis(2),
        Duration::from_millis(4),
    );
    iv.set_missed_tick_behavior(MissedTickBehavior::Delay);
    assert_eq!(iv.missed_tick_behavior(), MissedTickBehavior::Delay);
    iv.tick().await;
}

#[nexio::test]
async fn fs_helpers_and_vectored_file_io() {
    let base = std::env::temp_dir().join(format!("nexio_fs_ops_{}", std::process::id()));
    let _ = fs::remove_dir_all(&base).await;
    fs::create_dir_all(base.join("sub")).await.unwrap();

    let p1 = base.join("a.txt");
    let p2 = base.join("sub/b.txt");
    fs::write(&p1, b"hello world!").await.unwrap();
    assert_eq!(fs::read(&p1).await.unwrap(), b"hello world!");
    assert_eq!(fs::metadata(&p1).await.unwrap().len(), 12);
    assert_eq!(fs::symlink_metadata(&p1).await.unwrap().len(), 12);

    fs::copy(&p1, &p2).await.unwrap();
    let p3 = base.join("sub/c.txt");
    fs::rename(&p2, &p3).await.unwrap();

    let mut rd = fs::read_dir(base.join("sub")).await.unwrap();
    let mut names = Vec::new();
    while let Some(ent) = rd.next_entry().await.unwrap() {
        names.push(ent.file_name().to_string_lossy().into_owned());
    }
    assert_eq!(names, vec!["c.txt"]);

    // Vectored file I/O + File::metadata + File::set_len
    let vpath = base.join("vec.bin");
    let f = File::create(&vpath).await.unwrap();
    let (res, _) = f
        .write_all_vectored_at(vec![b"ABCD".to_vec(), b"EFGH".to_vec()], 0)
        .await;
    res.unwrap();
    f.sync_data().await.unwrap();
    assert_eq!(f.metadata().await.unwrap().len(), 8);
    f.set_len(6).await.unwrap();
    assert_eq!(f.metadata().await.unwrap().len(), 6);
    f.close().await.unwrap();

    let f = File::open(&vpath).await.unwrap();
    let (res, bufs) = f
        .read_vectored_at(vec![vec![0u8; 3], vec![0u8; 3]], 0)
        .await;
    assert_eq!(res.unwrap(), 6);
    assert_eq!(&bufs[0], b"ABC");
    assert_eq!(&bufs[1], b"DEF");
    f.close().await.unwrap();

    fs::remove_file(&p3).await.unwrap();
    fs::remove_dir(base.join("sub")).await.unwrap();
    fs::remove_dir_all(&base).await.unwrap();
}

#[nexio::test]
async fn tcp_split_into_split_and_vectored_io() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = nexio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let (rd, wr) = stream.into_split();
        let (res, bufs) = rd.read(vec![0u8; 10]).await;
        let n = res.unwrap();
        let (wres, _) = wr.write_all(bufs[..n].to_vec()).await;
        wres.unwrap();
        let reunited = rd.reunite(wr).unwrap();
        reunited.shutdown(std::net::Shutdown::Both).unwrap();
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let (res, _) = client
        .write_all_vectored(vec![b"hello".to_vec(), b"world".to_vec()])
        .await;
    res.unwrap();

    let (rd, _wr) = client.split();
    let mut got = vec![0u8; 10];
    let (rres, out) = rd.read_exact(got).await;
    rres.unwrap();
    got = out;
    assert_eq!(&got, b"helloworld");

    server.await.unwrap();
}

#[nexio::test]
async fn io_uring_available_matches_backend() {
    let avail = nexio::io_uring_available();
    if avail {
        assert_eq!(nexio::Reactor::backend_name(), "io_uring");
    } else {
        // On non-Linux or poll/forced-poll builds, io_uring_available() is false.
        let _ = nexio::Reactor::backend_name();
    }
}

#[cfg(all(target_os = "linux", not(feature = "poll")))]
#[nexio::test]
async fn multishot_accept_and_provided_buf_ring_roundtrip() {
    if !nexio::io_uring_available() {
        return;
    }
    let Ok(mut ring) = nexio::reactor::ProvidedBufRing::new(16, 1024, 42) else {
        // Kernel < 5.19 without IORING_REGISTER_PBUF_RING.
        return;
    };
    let listener = nexio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let Ok(mut accept) = listener.accept_multishot() else {
        return;
    };

    let client_task = nexio::spawn(async move {
        let stream = nexio::net::TcpStream::connect(addr).await.unwrap();
        let (res, _) = stream.write_all(b"multishot-ping".to_vec()).await;
        res.unwrap();
    });

    match accept.next().await {
        Some(Ok(conn)) => {
            if let Ok(mut rx) = conn.recv_multishot(42) {
                if let Some(Ok(chunk)) = rx.next().await {
                    if chunk.len > 0 {
                        let bid = chunk.bid.expect("kernel should select buffer id");
                        let slice = ring.get(bid, chunk.len).expect("valid bid");
                        assert_eq!(slice, b"multishot-ping");
                        ring.recycle(bid);
                    }
                }
            }
        }
        Some(Err(e)) if e.raw_os_error() == Some(libc::EINVAL) => {}
        other => panic!("unexpected multishot accept outcome: {:?}", other.is_some()),
    }
    client_task.await.unwrap();
}
