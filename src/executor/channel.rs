//! Fast single-threaded asynchronous channels.
//!
//! Provides two channel types designed for `!Send` tasks on a local executor:
//!
//! - [`oneshot`]: A channel for sending a single value from one producer to one consumer.
//! - [`mpsc`]: A multi-producer, single-consumer FIFO channel supporting both bounded and unbounded buffers.
//!
//! Both implementations use `Rc<RefCell<...>>` without atomics or locks.

pub mod oneshot {
    //! Single-producer, single-consumer channel for sending a single value.

    use std::{
        cell::RefCell,
        future::Future,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    struct OneshotState<T> {
        value: Option<T>,
        waker: Option<Waker>,
        sender_dropped: bool,
        receiver_dropped: bool,
    }

    /// Error returned when the oneshot sender is dropped without sending a value.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RecvError;

    impl std::fmt::Display for RecvError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "oneshot sender dropped without sending a value")
        }
    }

    impl std::error::Error for RecvError {}

    /// Error returned by [`Receiver::try_recv`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TryRecvError {
        /// No value has been sent yet.
        Empty,
        /// The sender was dropped without sending a value.
        Closed,
    }

    impl std::fmt::Display for TryRecvError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Empty => write!(f, "oneshot channel is empty"),
                Self::Closed => write!(f, "oneshot sender was dropped"),
            }
        }
    }

    impl std::error::Error for TryRecvError {}

    /// The sending half of a oneshot channel.
    pub struct Sender<T> {
        state: Rc<RefCell<OneshotState<T>>>,
    }

    impl<T> std::fmt::Debug for Sender<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Sender")
                .field("is_closed", &self.is_closed())
                .finish()
        }
    }

    impl<T> Sender<T> {
        /// Attempts to send a value to the receiver.
        ///
        /// Returns `Ok(())` on success, or `Err(value)` if the receiver was already dropped.
        pub fn send(self, value: T) -> Result<(), T> {
            let mut state = self.state.borrow_mut();
            if state.receiver_dropped {
                return Err(value);
            }
            state.value = Some(value);
            if let Some(w) = state.waker.take() {
                w.wake();
            }
            Ok(())
        }

        /// Returns `true` if the receiver has been dropped.
        pub fn is_closed(&self) -> bool {
            self.state.borrow().receiver_dropped
        }

        /// Returns `true` if the receiver has been dropped. Alias for [`is_closed`](Self::is_closed).
        pub fn is_canceled(&self) -> bool {
            self.is_closed()
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let mut state = self.state.borrow_mut();
            state.sender_dropped = true;
            if state.value.is_none() {
                if let Some(w) = state.waker.take() {
                    w.wake();
                }
            }
        }
    }

    /// The receiving half of a oneshot channel.
    pub struct Receiver<T> {
        state: Rc<RefCell<OneshotState<T>>>,
    }

    impl<T> std::fmt::Debug for Receiver<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Receiver").finish()
        }
    }

    impl<T> Receiver<T> {
        /// Attempts to receive the value without waiting.
        pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
            let mut state = self.state.borrow_mut();
            if let Some(val) = state.value.take() {
                Ok(val)
            } else if state.sender_dropped {
                Err(TryRecvError::Closed)
            } else {
                Err(TryRecvError::Empty)
            }
        }

        /// Closes the receiving half of the channel, discarding any pending value.
        pub fn close(&mut self) {
            let mut state = self.state.borrow_mut();
            state.receiver_dropped = true;
            state.waker = None;
            state.value = None;
        }
    }

    impl<T> Future for Receiver<T> {
        type Output = Result<T, RecvError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut state = self.state.borrow_mut();
            if let Some(val) = state.value.take() {
                Poll::Ready(Ok(val))
            } else if state.sender_dropped {
                Poll::Ready(Err(RecvError))
            } else {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            let mut state = self.state.borrow_mut();
            state.receiver_dropped = true;
            state.waker = None;
            state.value = None;
        }
    }

    /// Creates a new oneshot channel, returning a [`Sender`] and [`Receiver`].
    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let state = Rc::new(RefCell::new(OneshotState {
            value: None,
            waker: None,
            sender_dropped: false,
            receiver_dropped: false,
        }));
        (
            Sender {
                state: Rc::clone(&state),
            },
            Receiver { state },
        )
    }
}

pub mod mpsc {
    //! Multi-producer, single-consumer channel supporting bounded backpressure and unbounded buffering.

    use std::{
        cell::RefCell,
        collections::VecDeque,
        future::Future,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    struct ChannelState<T> {
        queue: VecDeque<T>,
        capacity: usize,
        receiver_waker: Option<Waker>,
        sender_wakers: VecDeque<(u64, Waker)>,
        next_waiter_id: u64,
        sender_count: usize,
        receiver_count: usize,
    }

    /// Error returned by [`Sender::send`] when the receiver has been dropped.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SendError<T>(pub T);

    impl<T> std::fmt::Display for SendError<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "channel closed")
        }
    }

    impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

    /// Error returned by [`Sender::try_send`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TrySendError<T> {
        /// The channel buffer is currently full.
        Full(T),
        /// The receiver has been dropped.
        Closed(T),
    }

    impl<T> std::fmt::Display for TrySendError<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Full(_) => write!(f, "channel buffer is full"),
                Self::Closed(_) => write!(f, "channel closed"),
            }
        }
    }

    impl<T: std::fmt::Debug> std::error::Error for TrySendError<T> {}

    /// Error returned by [`Receiver::try_recv`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TryRecvError {
        /// The channel is currently empty.
        Empty,
        /// All senders have been dropped and the channel is empty.
        Disconnected,
    }

    impl std::fmt::Display for TryRecvError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Empty => write!(f, "channel is empty"),
                Self::Disconnected => write!(f, "channel is disconnected"),
            }
        }
    }

    impl std::error::Error for TryRecvError {}

    /// The sending half of a bounded mpsc channel.
    pub struct Sender<T> {
        state: Rc<RefCell<ChannelState<T>>>,
    }

    impl<T> std::fmt::Debug for Sender<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Sender")
                .field("capacity", &self.capacity())
                .field("is_closed", &self.is_closed())
                .finish()
        }
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.state.borrow_mut().sender_count += 1;
            Self {
                state: Rc::clone(&self.state),
            }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let mut state = self.state.borrow_mut();
            state.sender_count = state.sender_count.saturating_sub(1);
            if state.sender_count == 0 {
                if let Some(w) = state.receiver_waker.take() {
                    w.wake();
                }
            }
        }
    }

    impl<T> Sender<T> {
        /// Sends a value asynchronously, waiting for space if the channel buffer is full.
        pub fn send(&self, value: T) -> Send<'_, T> {
            Send {
                sender: self,
                value: Some(value),
                waiter_id: None,
            }
        }

        /// Attempts to send a value without waiting.
        pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
            let mut state = self.state.borrow_mut();
            if state.receiver_count == 0 {
                return Err(TrySendError::Closed(value));
            }
            if state.queue.len() >= state.capacity || !state.sender_wakers.is_empty() {
                return Err(TrySendError::Full(value));
            }
            state.queue.push_back(value);
            if let Some(w) = state.receiver_waker.take() {
                w.wake();
            }
            Ok(())
        }

        /// Returns `true` if the receiver has been dropped.
        pub fn is_closed(&self) -> bool {
            self.state.borrow().receiver_count == 0
        }

        /// Returns the maximum capacity of the channel.
        pub fn capacity(&self) -> usize {
            self.state.borrow().capacity
        }
    }

    /// Future returned by [`Sender::send`].
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Send<'a, T> {
        sender: &'a Sender<T>,
        value: Option<T>,
        waiter_id: Option<u64>,
    }

    impl<'a, T> std::fmt::Debug for Send<'a, T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Send")
                .field("has_value", &self.value.is_some())
                .finish()
        }
    }

    impl<'a, T: Unpin> Future for Send<'a, T> {
        type Output = Result<(), SendError<T>>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let mut state = this.sender.state.borrow_mut();

            if state.receiver_count == 0 {
                let val = this.value.take().expect("polled after completion");
                return Poll::Ready(Err(SendError(val)));
            }

            if state.queue.len() < state.capacity {
                let val = this.value.take().expect("polled after completion");
                state.queue.push_back(val);
                if let Some(w) = state.receiver_waker.take() {
                    w.wake();
                }
                return Poll::Ready(Ok(()));
            }

            // Buffer is full: register waker
            let waiter_id = match this.waiter_id {
                Some(id) => id,
                None => {
                    let id = state.next_waiter_id;
                    state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
                    this.waiter_id = Some(id);
                    id
                }
            };

            if let Some(entry) = state
                .sender_wakers
                .iter_mut()
                .find(|(id, _)| *id == waiter_id)
            {
                entry.1 = cx.waker().clone();
            } else {
                state
                    .sender_wakers
                    .push_back((waiter_id, cx.waker().clone()));
            }

            Poll::Pending
        }
    }

    impl<'a, T> Drop for Send<'a, T> {
        fn drop(&mut self) {
            if let Some(id) = self.waiter_id {
                let mut state = self.sender.state.borrow_mut();
                if let Some(pos) = state.sender_wakers.iter().position(|(w_id, _)| *w_id == id) {
                    state.sender_wakers.remove(pos);
                }
            }
        }
    }

    /// The receiving half of a bounded mpsc channel.
    pub struct Receiver<T> {
        state: Rc<RefCell<ChannelState<T>>>,
    }

    impl<T> std::fmt::Debug for Receiver<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Receiver").finish()
        }
    }

    impl<T> Receiver<T> {
        /// Receives the next value from the channel.
        ///
        /// Returns `None` once all senders are dropped and the buffer is drained.
        pub async fn recv(&mut self) -> Option<T> {
            struct Recv<'a, T> {
                receiver: &'a mut Receiver<T>,
            }

            impl<'a, T> Future for Recv<'a, T> {
                type Output = Option<T>;

                fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                    self.receiver.poll_recv(cx)
                }
            }

            impl<'a, T> Drop for Recv<'a, T> {
                fn drop(&mut self) {
                    let mut state = self.receiver.state.borrow_mut();
                    state.receiver_waker = None;
                }
            }

            Recv { receiver: self }.await
        }

        /// Polls for the next value from the channel.
        pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
            let mut state = self.state.borrow_mut();
            if let Some(val) = state.queue.pop_front() {
                if let Some((_id, waker)) = state.sender_wakers.pop_front() {
                    waker.wake();
                }
                Poll::Ready(Some(val))
            } else if state.sender_count == 0 {
                Poll::Ready(None)
            } else {
                state.receiver_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }

        /// Attempts to receive a value without waiting.
        pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
            let mut state = self.state.borrow_mut();
            if let Some(val) = state.queue.pop_front() {
                if let Some((_id, waker)) = state.sender_wakers.pop_front() {
                    waker.wake();
                }
                Ok(val)
            } else if state.sender_count == 0 {
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            }
        }

        /// Closes the receiver, discarding buffered items and waking pending senders.
        pub fn close(&mut self) {
            let mut state = self.state.borrow_mut();
            state.receiver_count = 0;
            state.receiver_waker = None;
            state.queue.clear();
            while let Some((_id, waker)) = state.sender_wakers.pop_front() {
                waker.wake();
            }
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            let mut state = self.state.borrow_mut();
            state.receiver_count = 0;
            state.receiver_waker = None;
            state.queue.clear();
            while let Some((_id, waker)) = state.sender_wakers.pop_front() {
                waker.wake();
            }
        }
    }

    /// Creates a bounded multi-producer, single-consumer channel.
    ///
    /// # Panics
    /// Panics if `buffer == 0`.
    pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
        assert!(buffer > 0, "mpsc channel capacity must be greater than 0");
        let state = Rc::new(RefCell::new(ChannelState {
            queue: VecDeque::with_capacity(buffer),
            capacity: buffer,
            receiver_waker: None,
            sender_wakers: VecDeque::new(),
            next_waiter_id: 0,
            sender_count: 1,
            receiver_count: 1,
        }));
        (
            Sender {
                state: Rc::clone(&state),
            },
            Receiver { state },
        )
    }

    /// The sending half of an unbounded mpsc channel.
    pub struct UnboundedSender<T> {
        state: Rc<RefCell<ChannelState<T>>>,
    }

    impl<T> std::fmt::Debug for UnboundedSender<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("UnboundedSender")
                .field("is_closed", &self.is_closed())
                .finish()
        }
    }

    impl<T> Clone for UnboundedSender<T> {
        fn clone(&self) -> Self {
            self.state.borrow_mut().sender_count += 1;
            Self {
                state: Rc::clone(&self.state),
            }
        }
    }

    impl<T> Drop for UnboundedSender<T> {
        fn drop(&mut self) {
            let mut state = self.state.borrow_mut();
            state.sender_count = state.sender_count.saturating_sub(1);
            if state.sender_count == 0 {
                if let Some(w) = state.receiver_waker.take() {
                    w.wake();
                }
            }
        }
    }

    impl<T> UnboundedSender<T> {
        /// Sends a value synchronously into the channel.
        ///
        /// Returns `Err(SendError(value))` if the receiver has been dropped.
        pub fn send(&self, value: T) -> Result<(), SendError<T>> {
            let mut state = self.state.borrow_mut();
            if state.receiver_count == 0 {
                return Err(SendError(value));
            }
            state.queue.push_back(value);
            if let Some(w) = state.receiver_waker.take() {
                w.wake();
            }
            Ok(())
        }

        /// Returns `true` if the receiver has been dropped.
        pub fn is_closed(&self) -> bool {
            self.state.borrow().receiver_count == 0
        }
    }

    /// The receiving half of an unbounded mpsc channel.
    pub struct UnboundedReceiver<T> {
        state: Rc<RefCell<ChannelState<T>>>,
    }

    impl<T> std::fmt::Debug for UnboundedReceiver<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("UnboundedReceiver").finish()
        }
    }

    impl<T> UnboundedReceiver<T> {
        /// Receives the next value from the channel asynchronously.
        pub async fn recv(&mut self) -> Option<T> {
            struct UnboundedRecv<'a, T> {
                receiver: &'a mut UnboundedReceiver<T>,
            }

            impl<'a, T> Future for UnboundedRecv<'a, T> {
                type Output = Option<T>;

                fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                    self.receiver.poll_recv(cx)
                }
            }

            impl<'a, T> Drop for UnboundedRecv<'a, T> {
                fn drop(&mut self) {
                    let mut state = self.receiver.state.borrow_mut();
                    state.receiver_waker = None;
                }
            }

            UnboundedRecv { receiver: self }.await
        }

        /// Polls for the next value from the channel.
        pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
            let mut state = self.state.borrow_mut();
            if let Some(val) = state.queue.pop_front() {
                Poll::Ready(Some(val))
            } else if state.sender_count == 0 {
                Poll::Ready(None)
            } else {
                state.receiver_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }

        /// Attempts to receive a value without waiting.
        pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
            let mut state = self.state.borrow_mut();
            if let Some(val) = state.queue.pop_front() {
                Ok(val)
            } else if state.sender_count == 0 {
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            }
        }

        /// Closes the channel receiver.
        pub fn close(&mut self) {
            let mut state = self.state.borrow_mut();
            state.receiver_count = 0;
            state.receiver_waker = None;
            state.queue.clear();
        }
    }

    impl<T> Drop for UnboundedReceiver<T> {
        fn drop(&mut self) {
            let mut state = self.state.borrow_mut();
            state.receiver_count = 0;
            state.receiver_waker = None;
            state.queue.clear();
        }
    }

    /// Creates an unbounded multi-producer, single-consumer channel.
    pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
        let state = Rc::new(RefCell::new(ChannelState {
            queue: VecDeque::new(),
            capacity: usize::MAX,
            receiver_waker: None,
            sender_wakers: VecDeque::new(),
            next_waiter_id: 0,
            sender_count: 1,
            receiver_count: 1,
        }));
        (
            UnboundedSender {
                state: Rc::clone(&state),
            },
            UnboundedReceiver { state },
        )
    }
}
