//! Combinators for awaiting multiple futures concurrently.
//!
//! - [`join2`]: Waits for two futures to complete and returns their outputs as a tuple.
//! - [`select2`]: Waits for the first of two futures to complete and drops the other.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

/// Result of [`select2`], indicating which future completed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Either<A, B> {
    /// The first future finished first.
    Left(A),
    /// The second future finished first.
    Right(B),
}

impl<A, B> Either<A, B> {
    /// Returns `true` if this is [`Either::Left`].
    pub fn is_left(&self) -> bool {
        matches!(self, Self::Left(_))
    }

    /// Returns `true` if this is [`Either::Right`].
    pub fn is_right(&self) -> bool {
        matches!(self, Self::Right(_))
    }

    /// Returns `true` if this is the first (left) variant.
    pub fn is_first(&self) -> bool {
        self.is_left()
    }

    /// Returns `true` if this is the second (right) variant.
    pub fn is_second(&self) -> bool {
        self.is_right()
    }

    /// Converts into `Option<A>` if this is [`Either::Left`].
    pub fn left(self) -> Option<A> {
        match self {
            Self::Left(a) => Some(a),
            Self::Right(_) => None,
        }
    }

    /// Converts into `Option<B>` if this is [`Either::Right`].
    pub fn right(self) -> Option<B> {
        match self {
            Self::Left(_) => None,
            Self::Right(b) => Some(b),
        }
    }

    /// Converts into `Option<A>` if this is the first (left) variant.
    pub fn first(self) -> Option<A> {
        self.left()
    }

    /// Converts into `Option<B>` if this is the second (right) variant.
    pub fn second(self) -> Option<B> {
        self.right()
    }
}

pin_project! {
    /// Future returned by [`join2`].
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Join2<F1, F2>
    where
        F1: Future,
        F2: Future,
    {
        #[pin]
        fut1: Option<F1>,
        #[pin]
        fut2: Option<F2>,
        out1: Option<F1::Output>,
        out2: Option<F2::Output>,
    }
}

impl<F1, F2> Future for Join2<F1, F2>
where
    F1: Future,
    F2: Future,
{
    type Output = (F1::Output, F2::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if this.out1.is_none() {
            if let Some(fut) = this.fut1.as_mut().as_pin_mut() {
                if let Poll::Ready(out) = fut.poll(cx) {
                    *this.out1 = Some(out);
                    this.fut1.set(None);
                }
            }
        }

        if this.out2.is_none() {
            if let Some(fut) = this.fut2.as_mut().as_pin_mut() {
                if let Poll::Ready(out) = fut.poll(cx) {
                    *this.out2 = Some(out);
                    this.fut2.set(None);
                }
            }
        }

        if this.out1.is_some() && this.out2.is_some() {
            Poll::Ready((this.out1.take().unwrap(), this.out2.take().unwrap()))
        } else {
            Poll::Pending
        }
    }
}

/// Waits for both `fut1` and `fut2` to complete concurrently.
///
/// Returns a tuple containing the results of both futures once both have completed.
pub fn join2<F1, F2>(fut1: F1, fut2: F2) -> Join2<F1, F2>
where
    F1: Future,
    F2: Future,
{
    Join2 {
        fut1: Some(fut1),
        fut2: Some(fut2),
        out1: None,
        out2: None,
    }
}

pin_project! {
    /// Future returned by [`select2`].
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Select2<F1, F2> {
        #[pin]
        fut1: Option<F1>,
        #[pin]
        fut2: Option<F2>,
    }
}

impl<F1, F2> Future for Select2<F1, F2>
where
    F1: Future,
    F2: Future,
{
    type Output = Either<F1::Output, F2::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if let Some(fut) = this.fut1.as_mut().as_pin_mut() {
            if let Poll::Ready(out) = fut.poll(cx) {
                this.fut1.set(None);
                this.fut2.set(None);
                return Poll::Ready(Either::Left(out));
            }
        }

        if let Some(fut) = this.fut2.as_mut().as_pin_mut() {
            if let Poll::Ready(out) = fut.poll(cx) {
                this.fut1.set(None);
                this.fut2.set(None);
                return Poll::Ready(Either::Right(out));
            }
        }

        Poll::Pending
    }
}

/// Waits for either `fut1` or `fut2` to complete.
///
/// Returns [`Either::Left`] if `fut1` completes first, or [`Either::Right`] if `fut2`
/// completes first. The other future is immediately dropped upon completion.
pub fn select2<F1, F2>(fut1: F1, fut2: F2) -> Select2<F1, F2>
where
    F1: Future,
    F2: Future,
{
    Select2 {
        fut1: Some(fut1),
        fut2: Some(fut2),
    }
}
