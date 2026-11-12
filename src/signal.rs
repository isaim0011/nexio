//! Signal handling.
//!
//! * [`ctrl_c`] works on every platform.
//! * On Unix, `signal` returns a `Signal` stream for any catchable signal
//!   (`SIGTERM`, `SIGHUP`, `SIGUSR1`, ...).
//!
//! # How delivery works
//!
//! A signal handler may only do async-signal-safe things, so it does exactly one: it
//! writes the signal number to a socket. A dedicated thread reads that socket and,
//! for every subscriber to that signal, sets a flag and pokes the subscriber's reactor
//! through the same mailbox that [`spawn_blocking`](crate::spawn_blocking) uses. Any
//! number of reactor threads can wait for the same signal, and each is woken on its
//! own thread.
//!
//! # Semantics
//!
//! * Signals are **coalesced**: if the same signal arrives several times before your
//!   task runs, `Signal::recv` returns once. That is how the OS itself behaves.
//! * Registering a handler replaces the signal's default action. After the first
//!   call to [`ctrl_c`] or `signal` for `SIGINT`/`SIGTERM`, the process no longer
//!   exits by itself on that signal: your code decides.
//! * Signals that arrive before you subscribe are not remembered.
//! * Handlers stay installed for the life of the process, even after every
//!   `Signal` is dropped.

use std::{
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::{Context, Poll},
};

use crate::blocking::{current_mailbox, next_id, register_waker, unregister_waker, Mailbox};

/// One subscriber: a flag, plus the mailbox of the reactor thread to wake.
struct Sub {
    id: u64,
    pending: AtomicBool,
    mailbox: Arc<Mailbox>,
}

struct Registry {
    subs: Mutex<HashMap<i32, Vec<Arc<Sub>>>>,
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| Registry {
        subs: Mutex::new(HashMap::new()),
    })
}

/// Mark every subscriber of `signo` and wake its reactor. Runs on a normal thread.
fn dispatch(signo: i32) {
    let subs: Vec<Arc<Sub>> = registry()
        .subs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&signo)
        .cloned()
        .unwrap_or_default();
    for s in subs {
        s.pending.store(true, Ordering::SeqCst);
        s.mailbox.complete(s.id);
    }
}

fn subscribe(signo: i32) -> Arc<Sub> {
    let sub = Arc::new(Sub {
        id: next_id(),
        pending: AtomicBool::new(false),
        mailbox: current_mailbox(),
    });
    registry()
        .subs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(signo)
        .or_default()
        .push(sub.clone());
    sub
}

fn unsubscribe(signo: i32, sub: &Arc<Sub>) {
    if let Some(list) = registry()
        .subs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_mut(&signo)
    {
        list.retain(|s| !Arc::ptr_eq(s, sub));
    }
}

/// Wait for the next notification on `sub`.
fn poll_sub(sub: &Sub, cx: &mut Context<'_>) -> Poll<()> {
    // Register the waker first and check second, so a signal that lands between the two
    // is seen by the check and one that lands later finds the waker.
    register_waker(sub.id, cx.waker());
    if sub.pending.swap(false, Ordering::SeqCst) {
        unregister_waker(sub.id);
        Poll::Ready(())
    } else {
        Poll::Pending
    }
}

// ── Unix ─────────────────────────────────────────────────────────────────────

#[cfg(unix)]
mod sys {
    use super::*;
    use std::{
        os::{fd::AsRawFd, unix::net::UnixStream as StdUnixStream},
        sync::atomic::AtomicI32,
    };

    /// Write end of the socket the signal handler uses; set once.
    static WRITE_FD: AtomicI32 = AtomicI32::new(-1);

    /// Start the thread that turns bytes from handlers into dispatches. Idempotent.
    pub(super) fn ensure_thread() -> io::Result<()> {
        static STARTED: OnceLock<io::Result<()>> = OnceLock::new();
        let r = STARTED.get_or_init(|| {
            let (read, write) = StdUnixStream::pair()?;
            // The handler must never block: if the pipe is full a signal is already pending.
            write.set_nonblocking(true)?;
            WRITE_FD.store(write.as_raw_fd(), Ordering::SeqCst);
            // The write end is used from signal handlers for the life of the process.
            std::mem::forget(write);
            std::thread::Builder::new()
                .name("nexio-signals".into())
                .spawn(move || {
                    use std::io::Read;
                    let mut read = read;
                    let mut byte = [0u8; 1];
                    loop {
                        match read.read(&mut byte) {
                            Ok(1) => dispatch(byte[0] as i32),
                            Ok(_) => break,
                            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                            Err(_) => break,
                        }
                    }
                })?;
            Ok(())
        });
        match r {
            Ok(()) => Ok(()),
            Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
        }
    }

    /// Install the handler for `signo` once per process.
    pub(super) fn install(signo: i32) -> io::Result<()> {
        ensure_thread()?;
        static INSTALLED: OnceLock<Mutex<std::collections::HashSet<i32>>> = OnceLock::new();
        let installed = INSTALLED.get_or_init(|| Mutex::new(Default::default()));
        let mut set = installed.lock().unwrap_or_else(|e| e.into_inner());
        if set.contains(&signo) {
            return Ok(());
        }
        // SAFETY: the action only calls `write(2)` on a descriptor that is never closed,
        // which is async-signal-safe, and touches no other state.
        unsafe {
            signal_hook_registry::register(signo, move || {
                let fd = WRITE_FD.load(Ordering::SeqCst);
                let byte = signo as u8;
                libc::write(fd, &byte as *const u8 as *const libc::c_void, 1);
            })?;
        }
        set.insert(signo);
        Ok(())
    }
}

/// Which signal to wait for (Unix).
#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SignalKind(i32);

#[cfg(unix)]
impl SignalKind {
    /// `SIGHUP`.
    pub const fn hangup() -> Self {
        Self(libc::SIGHUP)
    }
    /// `SIGINT` (Ctrl-C in a terminal).
    pub const fn interrupt() -> Self {
        Self(libc::SIGINT)
    }
    /// `SIGQUIT`.
    pub const fn quit() -> Self {
        Self(libc::SIGQUIT)
    }
    /// `SIGTERM`, the usual "please shut down".
    pub const fn terminate() -> Self {
        Self(libc::SIGTERM)
    }
    /// `SIGALRM`.
    pub const fn alarm() -> Self {
        Self(libc::SIGALRM)
    }
    /// `SIGCHLD`.
    pub const fn child() -> Self {
        Self(libc::SIGCHLD)
    }
    /// `SIGUSR1`.
    pub const fn user_defined1() -> Self {
        Self(libc::SIGUSR1)
    }
    /// `SIGUSR2`.
    pub const fn user_defined2() -> Self {
        Self(libc::SIGUSR2)
    }
    /// `SIGWINCH`.
    pub const fn window_change() -> Self {
        Self(libc::SIGWINCH)
    }
    /// Any signal by number. Fails when subscribing if the signal cannot be caught.
    pub const fn from_raw(signo: i32) -> Self {
        Self(signo)
    }
    /// The signal number.
    pub const fn as_raw_value(self) -> i32 {
        self.0
    }
}

/// A stream of one kind of signal (Unix).
#[cfg(unix)]
pub struct Signal {
    signo: i32,
    sub: Arc<Sub>,
}

/// Start listening for `kind`.
///
/// Fails with `InvalidInput` for signals that cannot be caught (`SIGKILL`, `SIGSTOP`)
/// or that the runtime must not touch (`SIGSEGV`, `SIGBUS`, `SIGFPE`, `SIGILL`).
/// Must be called from inside [`block_on`](crate::block_on).
#[cfg(unix)]
pub fn signal(kind: SignalKind) -> io::Result<Signal> {
    let signo = kind.0;
    if signo <= 0 || signo >= 128 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid signal number {signo}"),
        ));
    }
    // The registry crate panics on these instead of returning an error, so reject them here.
    const FORBIDDEN: [i32; 6] = [
        libc::SIGKILL,
        libc::SIGSTOP,
        libc::SIGILL,
        libc::SIGFPE,
        libc::SIGSEGV,
        libc::SIGBUS,
    ];
    if FORBIDDEN.contains(&signo) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("signal {signo} cannot be caught"),
        ));
    }
    sys::install(signo)?;
    Ok(Signal {
        signo,
        sub: subscribe(signo),
    })
}

#[cfg(unix)]
impl Signal {
    /// Wait for the next delivery of this signal. Coalesces repeats; never returns `None`
    /// while the process runs (the type is `Option` to leave room for shutdown).
    pub async fn recv(&mut self) -> Option<()> {
        std::future::poll_fn(|cx| poll_sub(&self.sub, cx).map(Some)).await
    }
}

#[cfg(unix)]
impl Drop for Signal {
    fn drop(&mut self) {
        unregister_waker(self.sub.id);
        unsubscribe(self.signo, &self.sub);
    }
}

// ── Windows ──────────────────────────────────────────────────────────────────

/// Key under which Ctrl-C subscribers are filed (the numeric value of `SIGINT`).
const CTRL_C_KEY: i32 = 2;

#[cfg(windows)]
mod sys {
    use super::*;
    use windows_sys::Win32::{
        Foundation::{BOOL, FALSE, TRUE},
        System::Console::{SetConsoleCtrlHandler, CTRL_BREAK_EVENT, CTRL_C_EVENT},
    };

    unsafe extern "system" fn handler(ctrl_type: u32) -> BOOL {
        if ctrl_type == CTRL_C_EVENT || ctrl_type == CTRL_BREAK_EVENT {
            dispatch(CTRL_C_KEY);
            TRUE
        } else {
            FALSE
        }
    }

    pub(super) fn install() -> io::Result<()> {
        static INSTALLED: OnceLock<io::Result<()>> = OnceLock::new();
        let r = INSTALLED.get_or_init(|| {
            // SAFETY: `handler` is a valid console control handler for the process lifetime.
            if unsafe { SetConsoleCtrlHandler(Some(handler), TRUE) } == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
        match r {
            Ok(()) => Ok(()),
            Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
        }
    }
}

// ── ctrl_c, everywhere ───────────────────────────────────────────────────────

/// Wait for Ctrl-C (`SIGINT` on Unix, the console control event on Windows).
///
/// The handler is installed the first time this is polled, which replaces the default
/// "exit the process" action. A Ctrl-C that arrives before then is handled by the
/// default action. Must be called from inside [`block_on`](crate::block_on).
pub async fn ctrl_c() -> io::Result<()> {
    #[cfg(unix)]
    {
        sys::install(CTRL_C_KEY)?;
    }
    #[cfg(windows)]
    {
        sys::install()?;
    }
    let sub = subscribe(CTRL_C_KEY);
    let result = std::future::poll_fn(|cx| poll_sub(&sub, cx)).await;
    unregister_waker(sub.id);
    unsubscribe(CTRL_C_KEY, &sub);
    let () = result;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block_on, spawn, time};
    use std::time::Duration;

    /// Deliver a signal to this process (Unix) or simulate the console event (Windows).
    fn raise(_kind_or_key: i32) {
        #[cfg(unix)]
        // SAFETY: raising a signal that this test has installed a handler for.
        unsafe {
            libc::raise(_kind_or_key);
        }
        #[cfg(windows)]
        dispatch(CTRL_C_KEY);
    }

    #[cfg(unix)]
    #[test]
    fn a_signal_wakes_a_waiting_task() {
        block_on(async {
            let mut sig = signal(SignalKind::user_defined1()).unwrap();
            let waiter = spawn(async move { sig.recv().await });
            time::sleep(Duration::from_millis(20)).await;
            raise(libc::SIGUSR1);
            assert_eq!(waiter.await.unwrap(), Some(()));
        });
    }

    #[cfg(unix)]
    #[test]
    fn repeated_signals_before_recv_coalesce_into_one() {
        block_on(async {
            let mut sig = signal(SignalKind::user_defined2()).unwrap();
            for _ in 0..5 {
                raise(libc::SIGUSR2);
            }
            // Give the signal thread time to dispatch all five.
            time::sleep(Duration::from_millis(100)).await;
            assert_eq!(sig.recv().await, Some(()));
            let again = time::timeout(Duration::from_millis(100), sig.recv()).await;
            assert!(again.is_err(), "the five signals were coalesced into one");
        });
    }

    #[cfg(unix)]
    #[test]
    fn dropping_a_pending_recv_loses_nothing() {
        block_on(async {
            let mut sig = signal(SignalKind::hangup()).unwrap();
            let r = time::timeout(Duration::from_millis(30), sig.recv()).await;
            assert!(r.is_err(), "no signal yet");
            raise(libc::SIGHUP);
            assert_eq!(sig.recv().await, Some(()));
        });
    }

    #[cfg(unix)]
    #[test]
    fn uncatchable_and_invalid_signals_are_rejected() {
        block_on(async {
            for raw in [libc::SIGKILL, libc::SIGSTOP, 0, -3, 500] {
                let err = signal(SignalKind::from_raw(raw)).err().expect("must fail");
                assert_eq!(
                    err.kind(),
                    io::ErrorKind::InvalidInput,
                    "signal {raw}: {err:?}"
                );
            }
        });
    }

    #[cfg(unix)]
    #[test]
    fn every_reactor_thread_waiting_for_a_signal_is_woken() {
        // Two reactors on two threads subscribe to the same signal; one delivery wakes both,
        // each on its own thread.
        let ready = Arc::new(std::sync::Barrier::new(3));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let ready = ready.clone();
            threads.push(std::thread::spawn(move || {
                block_on(async move {
                    let mut sig = signal(SignalKind::user_defined1()).unwrap();
                    ready.wait();
                    sig.recv().await
                })
            }));
        }
        ready.wait();
        std::thread::sleep(Duration::from_millis(50));
        raise(libc::SIGUSR1);
        for t in threads {
            assert_eq!(t.join().unwrap(), Some(()));
        }
    }

    #[test]
    fn ctrl_c_resolves_when_interrupted() {
        block_on(async {
            let waiter = spawn(async { ctrl_c().await });
            time::sleep(Duration::from_millis(30)).await;
            #[cfg(unix)]
            raise(libc::SIGINT);
            #[cfg(windows)]
            raise(CTRL_C_KEY);
            waiter.await.unwrap().unwrap();
        });
    }
}
