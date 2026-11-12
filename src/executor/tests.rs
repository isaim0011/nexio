//! Unit tests for executor, covering wake ordering, drop behavior, JoinHandle,
//! cancellation, panic handling, and reactor integration.

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use super::{
    block_on, channel::mpsc, channel::oneshot, join2, select2, spawn, yield_now, Either,
    JoinError, JoinSet,
};

/// Helper struct that increments a counter when dropped.
#[derive(Clone)]
struct DropTracker {
    counter: Arc<AtomicUsize>,
}

impl DropTracker {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

impl Drop for DropTracker {
    fn drop(&mut self) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

/// A future that records when it was polled and allows external waking.
struct SteppedFuture {
    id: usize,
    poll_log: Rc<RefCell<Vec<usize>>>,
    waker_slot: Rc<RefCell<Option<Waker>>>,
    polls_remaining: usize,
}

impl Future for SteppedFuture {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_log.borrow_mut().push(self.id);
        if self.polls_remaining == 0 {
            Poll::Ready(self.id)
        } else {
            self.polls_remaining -= 1;
            *self.waker_slot.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ── Test: Basic spawn and join ───────────────────────────────────────────────

#[test]
fn test_spawn_and_join() {
    let result = block_on(async {
        let handle = spawn(async {
            40 + 2
        });
        handle.await.unwrap()
    });
    assert_eq!(result, 42);
}

#[test]
fn test_spawn_multiple_and_join() {
    let sum = block_on(async {
        let mut handles = Vec::new();
        for i in 1..=5 {
            handles.push(spawn(async move { i * 10 }));
        }
        let mut total = 0;
        for h in handles {
            total += h.await.unwrap();
        }
        total
    });
    assert_eq!(sum, 150);
}

// ── Test: Wake ordering ──────────────────────────────────────────────────────

#[test]
fn test_wake_ordering_fifo() {
    // We spawn 3 tasks that pause on first poll.
    // Then we wake them in order: Task 3, Task 1, Task 2.
    // We assert that the executor polls them in that exact FIFO order.
    let log = Rc::new(RefCell::new(Vec::new()));
    let w1 = Rc::new(RefCell::new(None));
    let w2 = Rc::new(RefCell::new(None));
    let w3 = Rc::new(RefCell::new(None));

    let log_clone = Rc::clone(&log);
    let (w1_c, w2_c, w3_c) = (Rc::clone(&w1), Rc::clone(&w2), Rc::clone(&w3));

    block_on(async move {
        let h1 = spawn(SteppedFuture {
            id: 1,
            poll_log: Rc::clone(&log_clone),
            waker_slot: w1_c,
            polls_remaining: 1,
        });
        let h2 = spawn(SteppedFuture {
            id: 2,
            poll_log: Rc::clone(&log_clone),
            waker_slot: w2_c,
            polls_remaining: 1,
        });
        let h3 = spawn(SteppedFuture {
            id: 3,
            poll_log: Rc::clone(&log_clone),
            waker_slot: w3_c,
            polls_remaining: 1,
        });

        // Let all three tasks run their first poll (log: [1, 2, 3]).
        yield_now().await;

        assert_eq!(*log_clone.borrow(), vec![1, 2, 3]);

        // Clear log for testing the wake order.
        log_clone.borrow_mut().clear();

        // Wake in specific order: 3, then 1, then 2.
        w3.borrow().as_ref().unwrap().wake_by_ref();
        w1.borrow().as_ref().unwrap().wake_by_ref();
        w2.borrow().as_ref().unwrap().wake_by_ref();

        // Await all.
        assert_eq!(h1.await.unwrap(), 1);
        assert_eq!(h2.await.unwrap(), 2);
        assert_eq!(h3.await.unwrap(), 3);

        // The second polls MUST be in exact wake order: [3, 1, 2]!
        assert_eq!(*log_clone.borrow(), vec![3, 1, 2]);
    });
}

#[test]
fn test_wake_ordering_yield_interleaved() {
    let trace = Rc::new(RefCell::new(Vec::new()));
    let t1 = Rc::clone(&trace);
    let t2 = Rc::clone(&trace);

    block_on(async move {
        let h1 = spawn(async move {
            for _ in 0..3 {
                t1.borrow_mut().push("A");
                yield_now().await;
            }
        });

        let h2 = spawn(async move {
            for _ in 0..3 {
                t2.borrow_mut().push("B");
                yield_now().await;
            }
        });

        h1.await.unwrap();
        h2.await.unwrap();
    });

    assert_eq!(*trace.borrow(), vec!["A", "B", "A", "B", "A", "B"]);
}

// ── Test: Drop semantics ─────────────────────────────────────────────────────

#[test]
fn test_drop_on_normal_completion() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let tracker = DropTracker::new(Arc::clone(&drop_count));

    block_on(async move {
        let handle = spawn(async move {
            let _captured = tracker;
            yield_now().await;
            100
        });

        assert_eq!(handle.await.unwrap(), 100);
    });

    // Captured variables in the task's future must be dropped upon completion.
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_drop_join_handle_detaches_task_and_drops_output() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let tracker = DropTracker::new(Arc::clone(&drop_count));
    let done_flag = Rc::new(RefCell::new(false));
    let done_clone = Rc::clone(&done_flag);

    block_on(async move {
        let handle = spawn(async move {
            yield_now().await;
            *done_clone.borrow_mut() = true;
            tracker
        });

        // Drop the JoinHandle immediately before the task completes!
        drop(handle);

        // Yield until the detached task finishes.
        while !*done_flag.borrow() {
            yield_now().await;
        }
    });

    // The output value must have been dropped when the task completed!
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_drop_join_handle_after_completion_drops_output() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let tracker = DropTracker::new(Arc::clone(&drop_count));

    block_on(async move {
        let handle = spawn(async move {
            tracker
        });

        // Let the task run to completion without awaiting the handle.
        yield_now().await;
        assert!(handle.is_finished());

        // Output is still held inside Completed(...) in the shared state.
        assert_eq!(drop_count.load(Ordering::SeqCst), 0);

        // Now drop the JoinHandle.
        drop(handle);

        // Output MUST be dropped now!
        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn test_join_handle_consumes_output() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let tracker = DropTracker::new(Arc::clone(&drop_count));

    block_on(async move {
        let handle = spawn(async move {
            tracker
        });

        let received = handle.await.unwrap();
        assert_eq!(drop_count.load(Ordering::SeqCst), 0);

        drop(received);
        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn test_cancel_task_drops_future_and_resources() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let tracker = DropTracker::new(Arc::clone(&drop_count));

    block_on(async move {
        let handle = spawn(async move {
            let _held = tracker;
            loop {
                yield_now().await;
            }
        });

        // Let task start.
        yield_now().await;
        assert_eq!(drop_count.load(Ordering::SeqCst), 0);

        // Cancel the task.
        handle.cancel();

        // The future and its captured variables must be dropped immediately!
        assert_eq!(drop_count.load(Ordering::SeqCst), 1);

        // Awaiting the cancelled task must return Err(JoinError::Cancelled).
        let res = handle.await;
        assert_eq!(res, Err(JoinError::Cancelled));
    });
}

#[test]
fn test_executor_cleanup_drops_unfinished_tasks() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let tracker = DropTracker::new(Arc::clone(&drop_count));

    block_on(async move {
        drop(spawn(async move {
            let _held = tracker;
            loop {
                yield_now().await;
            }
        }));
        // Root future finishes immediately.
    });

    // When block_on exits, executor cleanup must drop the unfinished task!
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
}

// ── Test: Panic handling ─────────────────────────────────────────────────────

#[test]
fn test_task_panic_is_caught_and_reported() {
    block_on(async {
        let handle = spawn(async {
            panic!("test panic inside spawned task");
        });

        let res = handle.await;
        assert!(res.is_err());
        assert!(res.unwrap_err().is_panic());
    });
}

// ── Test: Nested spawns ──────────────────────────────────────────────────────

#[test]
fn test_nested_spawns() {
    let sum = block_on(async {
        let h1 = spawn(async {
            let h2 = spawn(async {
                let h3 = spawn(async { 7 });
                h3.await.unwrap() * 2
            });
            h2.await.unwrap() + 1
        });
        h1.await.unwrap()
    });
    assert_eq!(sum, 15);
}

// ── Test: Nested block_on panics ─────────────────────────────────────────────

#[test]
#[should_panic(expected = "cannot call block_on recursively on the same thread")]
fn test_nested_block_on_panics() {
    block_on(async {
        block_on(async {});
    });
}

// ── Test: Reactor integration via submit_and_poll ────────────────────────────

#[test]
fn test_reactor_integration_poll() {
    let result = block_on(async {
        let count = crate::reactor::REACTOR.with(|r| {
            r.borrow_mut().submit_and_poll().unwrap()
        });
        assert_eq!(count, 0);
        42
    });
    assert_eq!(result, 42);
}

// ── Test: join2 ─────────────────────────────────────────────────────────────

#[test]
fn test_join2_basic() {
    let result = block_on(async {
        join2(async { 10 }, async { 20 }).await
    });
    assert_eq!(result, (10, 20));
}

#[test]
fn test_join2_async_interleaved() {
    let result = block_on(async {
        let f1 = async {
            yield_now().await;
            yield_now().await;
            100
        };
        let f2 = async {
            yield_now().await;
            200
        };
        join2(f1, f2).await
    });
    assert_eq!(result, (100, 200));
}

#[test]
fn test_join2_drop_semantics() {
    let drop1 = Arc::new(AtomicUsize::new(0));
    let drop2 = Arc::new(AtomicUsize::new(0));
    let t1 = DropTracker::new(Arc::clone(&drop1));
    let t2 = DropTracker::new(Arc::clone(&drop2));

    block_on(async move {
        let (r1, r2) = join2(
            async move {
                let _x = t1;
                yield_now().await;
                1
            },
            async move {
                let _y = t2;
                2
            },
        )
        .await;
        assert_eq!((r1, r2), (1, 2));
    });

    assert_eq!(drop1.load(Ordering::SeqCst), 1);
    assert_eq!(drop2.load(Ordering::SeqCst), 1);
}

// ── Test: select2 ────────────────────────────────────────────────────────────

#[test]
fn test_select2_left_wins() {
    let result = block_on(async {
        let fast = async { 42 };
        let slow = async {
            yield_now().await;
            99
        };
        select2(fast, slow).await
    });
    assert_eq!(result, Either::Left(42));
    assert!(result.is_left());
    assert_eq!(result.left(), Some(42));
    assert_eq!(result.right(), None);
}

#[test]
fn test_select2_right_wins() {
    let result = block_on(async {
        let slow = async {
            yield_now().await;
            10
        };
        let fast = async { 20 };
        select2(slow, fast).await
    });
    assert_eq!(result, Either::Right(20));
    assert!(result.is_right());
    assert_eq!(result.right(), Some(20));
    assert_eq!(result.left(), None);
}

#[test]
fn test_select2_drop_cancellation_of_losing_future() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let tracker = DropTracker::new(Arc::clone(&drop_count));

    block_on(async move {
        let fast = async {
            yield_now().await;
            "fast"
        };
        let slow = async move {
            let _captured = tracker;
            loop {
                yield_now().await;
            }
            #[allow(unreachable_code)]
            "slow"
        };

        let result = select2(fast, slow).await;
        assert_eq!(result, Either::Left("fast"));
    });

    // The losing future must have been dropped and cancelled immediately!
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_select2_drop_cancellation_right_loser() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let tracker = DropTracker::new(Arc::clone(&drop_count));

    block_on(async move {
        let slow = async move {
            let _captured = tracker;
            loop {
                yield_now().await;
            }
            #[allow(unreachable_code)]
            1
        };
        let fast = async {
            yield_now().await;
            2
        };

        let result = select2(slow, fast).await;
        assert_eq!(result, Either::Right(2));
    });

    // Slow future on the left was dropped upon the right future winning!
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
}

// ── Test: JoinSet ───────────────────────────────────────────────────────────

#[test]
fn test_join_set_basic() {
    block_on(async {
        let mut set = JoinSet::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);

        for i in 1..=4 {
            set.spawn(async move { i * 10 });
        }
        assert_eq!(set.len(), 4);
        assert!(!set.is_empty());

        let mut results = Vec::new();
        while let Some(res) = set.join_next().await {
            results.push(res.unwrap());
        }

        assert_eq!(set.len(), 0);
        assert!(set.is_empty());
        assert_eq!(set.join_next().await, None);

        results.sort();
        assert_eq!(results, vec![10, 20, 30, 40]);
    });
}

#[test]
fn test_join_set_completion_order() {
    // Task A takes 3 yields
    // Task B takes 1 yield
    // Task C takes 2 yields
    // Expected join_next order: B, C, A!
    block_on(async {
        let mut set = JoinSet::new();

        set.spawn(async {
            yield_now().await;
            yield_now().await;
            yield_now().await;
            "A"
        });

        set.spawn(async {
            yield_now().await;
            "B"
        });

        set.spawn(async {
            yield_now().await;
            yield_now().await;
            "C"
        });

        let mut order = Vec::new();
        while let Some(res) = set.join_next().await {
            order.push(res.unwrap());
        }

        assert_eq!(order, vec!["B", "C", "A"]);
    });
}

#[test]
fn test_join_set_abort_all() {
    block_on(async {
        let mut set = JoinSet::new();

        for _ in 0..3 {
            set.spawn(async {
                loop {
                    yield_now().await;
                }
            });
        }

        assert_eq!(set.len(), 3);
        set.abort_all();
        assert_eq!(set.len(), 3);

        let mut cancelled_count = 0;
        while let Some(res) = set.join_next().await {
            assert_eq!(res, Err(JoinError::Cancelled));
            cancelled_count += 1;
        }

        assert_eq!(cancelled_count, 3);
        assert_eq!(set.len(), 0);
        assert!(set.is_empty());
    });
}

#[test]
fn test_join_set_drop_cleanup() {
    let drop_count = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let mut set = JoinSet::new();

        for _ in 0..3 {
            let tracker = DropTracker::new(Arc::clone(&drop_count));
            set.spawn(async move {
                let _captured = tracker;
                loop {
                    yield_now().await;
                }
            });
        }

        // Allow tasks to run their first step
        yield_now().await;
        assert_eq!(drop_count.load(Ordering::SeqCst), 0);

        // Dropping JoinSet must cancel and drop all 3 tasks immediately!
        drop(set);
    });

    assert_eq!(drop_count.load(Ordering::SeqCst), 3);
}

// ── Test: oneshot channel ───────────────────────────────────────────────────

#[test]
fn test_oneshot_send_recv() {
    block_on(async {
        let (tx, rx) = oneshot::channel();
        tx.send("hello").unwrap();
        let val = rx.await.unwrap();
        assert_eq!(val, "hello");
    });
}

#[test]
fn test_oneshot_sender_drop() {
    block_on(async {
        let (tx, rx) = oneshot::channel::<i32>();
        drop(tx);
        let res = rx.await;
        assert_eq!(res, Err(oneshot::RecvError));
    });
}

#[test]
fn test_oneshot_receiver_drop() {
    let (tx, rx) = oneshot::channel::<i32>();
    drop(rx);
    assert!(tx.is_closed());
    let err = tx.send(42);
    assert_eq!(err, Err(42));
}

#[test]
fn test_oneshot_try_recv() {
    let (tx, mut rx) = oneshot::channel::<i32>();
    assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Empty));
    tx.send(123).unwrap();
    assert_eq!(rx.try_recv(), Ok(123));
}

// ── Test: mpsc channel ──────────────────────────────────────────────────────

#[test]
fn test_mpsc_bounded_basic() {
    block_on(async {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    });
}

#[test]
fn test_mpsc_backpressure() {
    block_on(async {
        // Channel with capacity 2
        let (tx, mut rx) = mpsc::channel(2);

        // Fill buffer to capacity
        tx.send(10).await.unwrap();
        tx.send(20).await.unwrap();

        let tx_clone = tx.clone();
        let sent_flag = Rc::new(RefCell::new(false));
        let sent_clone = Rc::clone(&sent_flag);

        // Spawning task that attempts to send a 3rd item (must be blocked by backpressure)
        let handle = spawn(async move {
            tx_clone.send(30).await.unwrap();
            *sent_clone.borrow_mut() = true;
        });

        // Yield: task runs, finds buffer full, registers waker, stays blocked
        yield_now().await;
        assert!(!*sent_flag.borrow());

        // Receive one item: frees a slot in buffer
        assert_eq!(rx.recv().await, Some(10));

        // Waiting for the blocked task ensures it is woken and completes sending
        handle.await.unwrap();
        assert!(*sent_flag.borrow());

        // Drain remaining items
        assert_eq!(rx.recv().await, Some(20));
        assert_eq!(rx.recv().await, Some(30));
    });
}

#[test]
fn test_mpsc_sender_drop() {
    block_on(async {
        let (tx, mut rx) = mpsc::channel(2);
        tx.send(1).await.unwrap();
        drop(tx);

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, None);
    });
}

#[test]
fn test_mpsc_receiver_drop_wakes_blocked_sender() {
    block_on(async {
        let (tx, rx) = mpsc::channel(1);
        tx.send(1).await.unwrap();

        let tx_clone = tx.clone();
        let handle = spawn(async move {
            tx_clone.send(2).await
        });

        yield_now().await;
        // Drop the receiver while task is blocked waiting for space
        drop(rx);

        // The blocked sender must be woken up and return SendError
        let res = handle.await.unwrap();
        assert_eq!(res, Err(mpsc::SendError(2)));
    });
}

#[test]
fn test_mpsc_waker_cleanup_on_send_drop() {
    block_on(async {
        let (tx, _rx) = mpsc::channel(1);
        tx.send(1).await.unwrap();

        // Attempt send(2) in a select with a faster future:
        let result = select2(tx.send(2), async {
            yield_now().await;
            "winner"
        })
        .await;

        assert_eq!(result, Either::Right("winner"));
    });
}

#[test]
fn test_mpsc_unbounded() {
    block_on(async {
        let (tx, mut rx) = mpsc::unbounded_channel();
        for i in 1..=5 {
            tx.send(i).unwrap();
        }
        drop(tx);

        let mut received = Vec::new();
        while let Some(val) = rx.recv().await {
            received.push(val);
        }
        assert_eq!(received, vec![1, 2, 3, 4, 5]);
    });
}
