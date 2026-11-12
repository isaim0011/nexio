//! Asynchronous time utilities: `io_uring` timeouts on the io_uring backend, the
//! reactor's timer map on the portable backend.

use std::{fmt, future::Future, time::Duration};

use crate::{
    executor::{select2, Either},
    reactor::REACTOR,
};

/// Error returned when a [`timeout`] expires before the inner future completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Sleep for at least `duration` using `io_uring` `IORING_OP_TIMEOUT`.
///
/// Dropping the returned future cancels the pending kernel timeout cleanly.
#[cfg(nexio_uring)]
pub async fn sleep(duration: Duration) {
    if duration.is_zero() {
        crate::executor::yield_now().await;
        return;
    }
    if let Ok(op) = REACTOR.with(|r| r.borrow_mut().sleep(duration)) {
        let _ = op.await;
    }
}

/// Sleep for at least `duration` using the reactor's timer map.
///
/// Dropping the returned future cancels the timer.
#[cfg(nexio_poll)]
pub async fn sleep(duration: Duration) {
    if duration.is_zero() {
        crate::executor::yield_now().await;
        return;
    }
    Sleep {
        deadline: std::time::Instant::now() + duration,
        handle: None,
    }
    .await
}

#[cfg(nexio_poll)]
struct Sleep {
    deadline: std::time::Instant,
    handle: Option<(std::time::Instant, u64)>,
}

#[cfg(nexio_poll)]
impl Future for Sleep {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if std::time::Instant::now() >= self.deadline {
            return std::task::Poll::Ready(());
        }
        let (deadline, waker) = (self.deadline, cx.waker().clone());
        let existing = self.handle;
        let handle = REACTOR.with(|r| {
            let mut r = r.borrow_mut();
            match existing {
                Some(h) => {
                    r.update_timer(h, waker);
                    h
                }
                None => r.add_timer(deadline, waker),
            }
        });
        self.handle = Some(handle);
        std::task::Poll::Pending
    }
}

#[cfg(nexio_poll)]
impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(h) = self.handle {
            let _ = REACTOR.try_with(|r| {
                if let Ok(mut r) = r.try_borrow_mut() {
                    r.cancel_timer(h);
                }
            });
        }
    }
}

/// Require a future to complete within `duration`.
///
/// If the deadline elapses first, the inner future is dropped immediately
/// (triggering cancellation safety for any in-flight `io_uring` ops) and
/// `Err(Elapsed)` is returned.
pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    match select2(future, sleep(duration)).await {
        Either::Left(output) => Ok(output),
        Either::Right(()) => Err(Elapsed),
    }
}

/// A repeating timer that ticks at a fixed interval.
pub struct Interval {
    period: Duration,
}

impl Interval {
    /// Wait until the next tick elapses.
    pub async fn tick(&mut self) {
        sleep(self.period).await;
    }

    /// Return the interval period.
    pub fn period(&self) -> Duration {
        self.period
    }
}

/// Create an [`Interval`] that yields every `period`.
pub fn interval(period: Duration) -> Interval {
    Interval { period }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::block_on;
    use std::time::Instant;

    #[test]
    fn test_sleep_duration() {
        block_on(async {
            let start = Instant::now();
            sleep(Duration::from_millis(15)).await;
            assert!(start.elapsed() >= Duration::from_millis(10));
        });
    }

    #[test]
    fn test_timeout_completes_before_deadline() {
        block_on(async {
            let res = timeout(Duration::from_millis(200), async {
                sleep(Duration::from_millis(2)).await;
                42
            })
            .await;
            assert_eq!(res, Ok(42));
        });
    }

    #[test]
    fn test_timeout_elapsed_drops_inner() {
        block_on(async {
            let res = timeout(Duration::from_millis(10), async {
                sleep(Duration::from_millis(500)).await;
                99
            })
            .await;
            assert_eq!(res, Err(Elapsed));
        });
    }

    #[test]
    fn test_interval_ticks() {
        block_on(async {
            let mut iv = interval(Duration::from_millis(5));
            let start = Instant::now();
            iv.tick().await;
            iv.tick().await;
            assert!(start.elapsed() >= Duration::from_millis(5));
        });
    }
}
