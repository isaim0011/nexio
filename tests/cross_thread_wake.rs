//! Wakers must work when another thread calls them: channels from the wider ecosystem
//! (here tokio's, which are runtime-independent) wake nexio tasks from whatever thread sends.
//! Every test runs on every backend.

use std::{sync::Arc, thread, time::Duration};

use nexio::{block_on, spawn, time};

#[test]
fn a_task_awaiting_a_foreign_channel_is_woken_by_another_thread() {
    block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
        let consumer = spawn(async move {
            let mut got = Vec::new();
            while let Some(v) = rx.recv().await {
                got.push(v);
            }
            got
        });
        let sender = thread::spawn(move || {
            for i in 0..100 {
                tx.send(i).unwrap();
                if i % 10 == 0 {
                    thread::sleep(Duration::from_millis(2));
                }
            }
        });
        let got = consumer.await.unwrap();
        sender.join().unwrap();
        assert_eq!(got, (0..100).collect::<Vec<_>>());
    });
}

#[test]
fn the_root_future_is_woken_from_another_thread() {
    // No spawned task at all: the future passed to block_on itself waits on the other thread.
    block_on(async {
        let (tx, rx) = tokio::sync::oneshot::channel::<&'static str>();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            let _ = tx.send("hello from another thread");
        });
        assert_eq!(rx.await.unwrap(), "hello from another thread");
    });
}

#[test]
fn many_tasks_woken_by_many_threads_at_once() {
    block_on(async {
        const TASKS: usize = 64;
        let mut senders = Vec::new();
        let mut handles = Vec::new();
        for i in 0..TASKS {
            let (tx, rx) = tokio::sync::oneshot::channel::<usize>();
            senders.push(tx);
            handles.push(spawn(async move { rx.await.unwrap() + i }));
        }
        // Each sender is fired from its own thread, all roughly together.
        let barrier = Arc::new(std::sync::Barrier::new(TASKS));
        let threads: Vec<_> = senders
            .into_iter()
            .enumerate()
            .map(|(i, tx)| {
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let _ = tx.send(i * 1000);
                })
            })
            .collect();
        for (i, h) in handles.into_iter().enumerate() {
            assert_eq!(h.await.unwrap(), i * 1000 + i);
        }
        for t in threads {
            t.join().unwrap();
        }
    });
}

#[test]
fn a_wake_while_the_reactor_is_busy_is_not_lost() {
    // The executor stays busy with timers while a foreign thread wakes a waiting task.
    block_on(async {
        let (tx, rx) = tokio::sync::oneshot::channel::<u8>();
        let waiter = spawn(async move { rx.await.unwrap() });
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(15));
            let _ = tx.send(7);
        });
        for _ in 0..40 {
            time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(waiter.await.unwrap(), 7);
    });
}

#[test]
fn each_thread_runs_its_own_executor_and_they_wake_each_other() {
    // Two executors on two threads, exchanging messages through tokio channels both ways.
    let (to_b_tx, mut to_b_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (to_a_tx, mut to_a_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let b = thread::spawn(move || {
        block_on(async move {
            while let Some(v) = to_b_rx.recv().await {
                if to_a_tx.send(v + 1).is_err() {
                    break;
                }
            }
        });
    });
    block_on(async move {
        let mut v = 0u32;
        for _ in 0..200 {
            to_b_tx.send(v).unwrap();
            v = to_a_rx.recv().await.unwrap();
        }
        assert_eq!(v, 200);
        drop(to_b_tx);
    });
    b.join().unwrap();
}

#[test]
fn waking_after_the_executor_is_gone_does_not_crash() {
    let waker_cell = Arc::new(std::sync::Mutex::new(None));
    let cell = waker_cell.clone();
    thread::spawn(move || {
        block_on(async move {
            std::future::poll_fn(move |cx| {
                *cell.lock().unwrap() = Some(cx.waker().clone());
                std::task::Poll::Ready(())
            })
            .await;
        });
    })
    .join()
    .unwrap();
    // The thread (and its executor) has ended; waking its saved waker is a harmless no-op.
    let w = waker_cell.lock().unwrap().take().unwrap();
    w.wake();
}
