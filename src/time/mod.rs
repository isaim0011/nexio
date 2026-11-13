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

/// Sleep for at least `duration` using the reactor's userspace timer map.
///
/// Dropping the returned [`Sleep`] future cancels the timer cleanly with zero kernel SQEs.
pub fn sleep(duration: Duration) -> Sleep {
    if duration.is_zero() {
        return Sleep {
            deadline: std::time::Instant::now(),
            handle: None,
            yield_once: true,
        };
    }
    sleep_until(std::time::Instant::now() + duration)
}

/// Sleep until `deadline` using the reactor's userspace timer map.
pub fn sleep_until(deadline: std::time::Instant) -> Sleep {
    Sleep {
        deadline,
        handle: None,
        yield_once: false,
    }
}

/// Future returned by [`sleep`] and [`sleep_until`].
pub struct Sleep {
    deadline: std::time::Instant,
    handle: Option<(std::time::Instant, u64)>,
    yield_once: bool,
}

impl Sleep {
    /// Return the instant at which this `Sleep` is scheduled to complete.
    pub fn deadline(&self) -> std::time::Instant {
        self.deadline
    }

    /// Return `true` if the deadline has already elapsed.
    pub fn is_elapsed(&self) -> bool {
        std::time::Instant::now() >= self.deadline
    }

    /// Reset this `Sleep` to a new `deadline`, cancelling any previously registered timer.
    pub fn reset(&mut self, deadline: std::time::Instant) {
        if let Some(h) = self.handle.take() {
            let _ = REACTOR.try_with(|r| {
                if let Ok(mut r) = r.try_borrow_mut() {
                    r.cancel_timer(h);
                }
            });
        }
        self.deadline = deadline;
        self.yield_once = false;
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.yield_once {
            self.yield_once = false;
            cx.waker().wake_by_ref();
            return std::task::Poll::Pending;
        }
        if std::time::Instant::now() >= self.deadline {
            return std::task::Poll::Ready(());
        }
        let deadline = self.deadline;
        let existing = self.handle;
        let handle = REACTOR.with(|r| {
            let mut r = r.borrow_mut();
            match existing {
                Some(h) => {
                    r.update_timer(h, cx.waker());
                    h
                }
                None => r.add_timer(deadline, cx.waker().clone()),
            }
        });
        self.handle = Some(handle);
        std::task::Poll::Pending
    }
}

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

/// Require a future to complete before `deadline`.
pub async fn timeout_at<F>(deadline: std::time::Instant, future: F) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    match select2(future, sleep_until(deadline)).await {
        Either::Left(output) => Ok(output),
        Either::Right(()) => Err(Elapsed),
    }
}

/// Strategy used by [`Interval`] when ticks are missed due to slow processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MissedTickBehavior {
    /// Catch up immediately by firing missed ticks in a burst.
    Burst,
    /// Shift the schedule to `now + period`.
    Delay,
    /// Skip missed ticks and align to the next multiple of `period`.
    #[default]
    Skip,
}

/// A repeating timer that ticks at a fixed interval without cumulative drift.
pub struct Interval {
    period: Duration,
    next_tick: std::time::Instant,
    missed_tick_behavior: MissedTickBehavior,
}

impl Interval {
    /// Wait until the next tick elapses.
    pub async fn tick(&mut self) {
        let now = std::time::Instant::now();
        if self.next_tick > now {
            sleep_until(self.next_tick).await;
        } else {
            crate::executor::yield_now().await;
        }
        let after = std::time::Instant::now();
        match self.missed_tick_behavior {
            MissedTickBehavior::Burst => {
                self.next_tick += self.period;
            }
            MissedTickBehavior::Delay => {
                self.next_tick = after + self.period;
            }
            MissedTickBehavior::Skip => {
                self.next_tick += self.period;
                if self.next_tick <= after {
                    let missed = (after.duration_since(self.next_tick).as_nanos()
                        / self.period.as_nanos().max(1)) as u32
                        + 1;
                    self.next_tick += self.period * missed;
                }
            }
        }
    }

    /// Reset the interval so the next tick occurs `period` from now.
    pub fn reset(&mut self) {
        self.next_tick = std::time::Instant::now() + self.period;
    }

    /// Return the interval period.
    pub fn period(&self) -> Duration {
        self.period
    }

    /// Set the [`MissedTickBehavior`] for this interval.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.missed_tick_behavior = behavior;
    }

    /// Return the current [`MissedTickBehavior`].
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.missed_tick_behavior
    }
}

/// Create an [`Interval`] that yields every `period`.
pub fn interval(period: Duration) -> Interval {
    let period = if period.is_zero() {
        Duration::from_nanos(1)
    } else {
        period
    };
    interval_at(std::time::Instant::now() + period, period)
}

/// Create an [`Interval`] starting at `start` that yields every `period`.
pub fn interval_at(start: std::time::Instant, period: Duration) -> Interval {
    let period = if period.is_zero() {
        Duration::from_nanos(1)
    } else {
        period
    };
    Interval {
        period,
        next_tick: start,
        missed_tick_behavior: MissedTickBehavior::default(),
    }
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
