//! Spawning and talking to child processes.
//!
//! The API follows [`std::process`], with `wait`, `output` and the stdio pipes
//! made asynchronous.
//!
//! # How it stays reliable
//!
//! * **Pipes never lose data when an operation is cancelled.** One small thread per
//!   pipe copies bytes between the pipe and a bounded in-memory queue; `read` takes
//!   from that queue and `write` adds to it. Dropping a pending `read` (a timeout,
//!   say) leaves the data in the queue for the next one. The queue is capped at 1 MiB
//!   per pipe, so a child that writes faster than you read is slowed down by the
//!   pipe's own back-pressure rather than buffered without limit.
//! * **`wait` runs on the blocking pool** (it polls `try_wait` with a short, growing
//!   delay), so waiting on a child never stalls the reactor and works the same on
//!   every platform. Dropping a pending `wait` does not stop the poll; it ends when
//!   the child does.
//! * **Children are reaped.** Dropping a [`Child`] that has not been waited for hands
//!   it to a background thread that waits for it, so it does not stay a zombie.
//!   With [`Command::kill_on_drop`] it is killed first.
//!
//! `write` to a [`ChildStdin`] completes when the bytes are accepted into the queue,
//! not when the child has read them; use [`ChildStdin::flush`] or
//! [`ChildStdin::shutdown`] to wait for delivery.

use std::{
    collections::VecDeque,
    ffi::OsStr,
    future::poll_fn,
    io::{self, Read, Write},
    path::Path,
    process::Child as StdChild,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
    time::Duration,
};

pub use std::process::{ExitStatus, Output, Stdio};

use crate::{
    blocking::{
        current_mailbox, next_id, register_waker, run_detached, spawn_blocking, unregister_waker,
        Mailbox,
    },
    buf::{BufResult, IoBuf, IoBufMut},
};

/// Most bytes queued per pipe before the pump thread stops reading (or `write` waits).
const MAX_BUFFERED: usize = 1 << 20;
const CHUNK: usize = 16 * 1024;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// ── pipes from the child (stdout, stderr) ────────────────────────────────────

struct InState {
    chunks: VecDeque<Vec<u8>>,
    front_off: usize,
    buffered: usize,
    eof: bool,
    err: Option<io::Error>,
    /// The consumer dropped its end; the pump thread should stop.
    abandoned: bool,
}

struct PipeIn {
    state: Mutex<InState>,
    space: Condvar,
    id: u64,
    mailbox: Arc<Mailbox>,
}

impl PipeIn {
    fn spawn(mut src: impl Read + Send + 'static) -> Arc<Self> {
        let pipe = Arc::new(Self {
            state: Mutex::new(InState {
                chunks: VecDeque::new(),
                front_off: 0,
                buffered: 0,
                eof: false,
                err: None,
                abandoned: false,
            }),
            space: Condvar::new(),
            id: next_id(),
            mailbox: current_mailbox(),
        });
        let p = pipe.clone();
        let spawned = std::thread::Builder::new()
            .name("nexio-pipe-in".into())
            .spawn(move || {
                let mut buf = vec![0u8; CHUNK];
                loop {
                    {
                        let mut st = lock(&p.state);
                        while st.buffered >= MAX_BUFFERED && !st.abandoned {
                            st = p.space.wait(st).unwrap_or_else(|e| e.into_inner());
                        }
                        if st.abandoned {
                            return;
                        }
                    }
                    match src.read(&mut buf) {
                        Ok(0) => {
                            lock(&p.state).eof = true;
                            p.mailbox.complete(p.id);
                            return;
                        }
                        Ok(n) => {
                            let mut st = lock(&p.state);
                            st.chunks.push_back(buf[..n].to_vec());
                            st.buffered += n;
                            drop(st);
                            p.mailbox.complete(p.id);
                        }
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(e) => {
                            let mut st = lock(&p.state);
                            st.err = Some(e);
                            st.eof = true;
                            drop(st);
                            p.mailbox.complete(p.id);
                            return;
                        }
                    }
                }
            });
        if let Err(e) = spawned {
            let mut st = lock(&pipe.state);
            st.err = Some(e);
            st.eof = true;
        }
        pipe
    }

    fn poll_read<B: IoBufMut>(&self, buf: &mut B, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let total = buf.bytes_total();
        if total == 0 {
            return Poll::Ready(Ok(0));
        }
        // Register first, check second: a chunk that lands in between is seen by the check.
        register_waker(self.id, cx.waker());
        let mut st = lock(&self.state);
        if let Some(front) = st.chunks.front() {
            let off = st.front_off;
            let n = (front.len() - off).min(total);
            // SAFETY: `front[off..off + n]` is in bounds, the destination holds `total >= n`
            // bytes, and the two allocations are distinct.
            unsafe {
                std::ptr::copy_nonoverlapping(front[off..].as_ptr(), buf.stable_ptr_mut(), n);
                buf.set_init(n);
            }
            st.front_off += n;
            st.buffered -= n;
            if st.front_off == st.chunks.front().map_or(0, Vec::len) {
                st.chunks.pop_front();
                st.front_off = 0;
            }
            drop(st);
            self.space.notify_one();
            unregister_waker(self.id);
            return Poll::Ready(Ok(n));
        }
        if let Some(e) = st.err.take() {
            unregister_waker(self.id);
            return Poll::Ready(Err(e));
        }
        if st.eof {
            unregister_waker(self.id);
            return Poll::Ready(Ok(0));
        }
        Poll::Pending
    }
}

macro_rules! child_reader {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        pub struct $name {
            pipe: Arc<PipeIn>,
        }

        impl $name {
            /// Read into `buf`; the buffer's initialized length becomes the bytes read
            /// (0 means the child closed its end).
            ///
            /// Cancelling a pending read loses nothing: the data stays queued.
            pub async fn read<B>(&self, mut buf: B) -> BufResult<usize, B>
            where
                B: IoBufMut + Send + 'static,
            {
                let res = poll_fn(|cx| self.pipe.poll_read(&mut buf, cx)).await;
                (res, buf)
            }

            /// Read everything until the child closes its end, appending to `out`.
            pub async fn read_to_end(&self, out: &mut Vec<u8>) -> io::Result<usize> {
                let mut total = 0;
                let mut chunk = Vec::with_capacity(CHUNK);
                loop {
                    let (res, mut returned) = self.read(chunk).await;
                    match res? {
                        0 => return Ok(total),
                        n => {
                            out.extend_from_slice(&returned[..n]);
                            total += n;
                            let cap = returned.capacity();
                            returned.clear();
                            if n * 10 >= cap * 9 && cap < (CHUNK * 16) {
                                returned.reserve((cap * 2).min(CHUNK * 16));
                            }
                            chunk = returned;
                        }
                    }
                }
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                unregister_waker(self.pipe.id);
                lock(&self.pipe.state).abandoned = true;
                self.pipe.space.notify_all();
            }
        }
    };
}

child_reader!(
    /// The child's standard output.
    ChildStdout
);
child_reader!(
    /// The child's standard error.
    ChildStderr
);

// ── pipe to the child (stdin) ────────────────────────────────────────────────

struct OutState {
    chunks: VecDeque<Vec<u8>>,
    buffered: usize,
    /// A chunk is currently being written by the pump thread.
    writing: bool,
    closing: bool,
    closed: bool,
    err: Option<(io::ErrorKind, String)>,
}

struct PipeOut {
    state: Mutex<OutState>,
    work: Condvar,
    id: u64,
    mailbox: Arc<Mailbox>,
}

impl PipeOut {
    fn spawn(mut dst: impl Write + Send + 'static) -> Arc<Self> {
        let pipe = Arc::new(Self {
            state: Mutex::new(OutState {
                chunks: VecDeque::new(),
                buffered: 0,
                writing: false,
                closing: false,
                closed: false,
                err: None,
            }),
            work: Condvar::new(),
            id: next_id(),
            mailbox: current_mailbox(),
        });
        let p = pipe.clone();
        let spawned = std::thread::Builder::new()
            .name("nexio-pipe-out".into())
            .spawn(move || loop {
                let chunk = {
                    let mut st = lock(&p.state);
                    loop {
                        if let Some(c) = st.chunks.pop_front() {
                            st.writing = true;
                            break Some(c);
                        }
                        if st.closing {
                            break None;
                        }
                        st = p.work.wait(st).unwrap_or_else(|e| e.into_inner());
                    }
                };
                let Some(chunk) = chunk else {
                    drop(dst);
                    lock(&p.state).closed = true;
                    p.mailbox.complete(p.id);
                    return;
                };
                let res = dst.write_all(&chunk).and_then(|()| dst.flush());
                let mut st = lock(&p.state);
                st.writing = false;
                st.buffered -= chunk.len();
                if let Err(e) = res {
                    st.err = Some((e.kind(), e.to_string()));
                    st.chunks.clear();
                    st.buffered = 0;
                    st.closed = true;
                    drop(st);
                    p.mailbox.complete(p.id);
                    return;
                }
                drop(st);
                p.mailbox.complete(p.id);
            });
        if let Err(e) = spawned {
            let mut st = lock(&pipe.state);
            st.err = Some((e.kind(), e.to_string()));
            st.closed = true;
        }
        pipe
    }

    fn broken(st: &OutState) -> Option<io::Error> {
        if let Some((kind, msg)) = &st.err {
            return Some(io::Error::new(*kind, msg.clone()));
        }
        if st.closing || st.closed {
            return Some(io::Error::new(io::ErrorKind::BrokenPipe, "stdin is closed"));
        }
        None
    }

    fn poll_write(&self, data: &[u8], cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        register_waker(self.id, cx.waker());
        let mut st = lock(&self.state);
        if let Some(e) = Self::broken(&st) {
            unregister_waker(self.id);
            return Poll::Ready(Err(e));
        }
        if data.is_empty() {
            unregister_waker(self.id);
            return Poll::Ready(Ok(0));
        }
        if st.buffered >= MAX_BUFFERED {
            return Poll::Pending;
        }
        st.chunks.push_back(data.to_vec());
        st.buffered += data.len();
        drop(st);
        self.work.notify_one();
        unregister_waker(self.id);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        register_waker(self.id, cx.waker());
        let st = lock(&self.state);
        if let Some((kind, msg)) = &st.err {
            unregister_waker(self.id);
            return Poll::Ready(Err(io::Error::new(*kind, msg.clone())));
        }
        if st.chunks.is_empty() && !st.writing {
            unregister_waker(self.id);
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }

    fn start_close(&self) {
        lock(&self.state).closing = true;
        self.work.notify_one();
    }

    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        register_waker(self.id, cx.waker());
        if lock(&self.state).closed {
            unregister_waker(self.id);
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

/// The child's standard input.
pub struct ChildStdin {
    pipe: Arc<PipeOut>,
}

impl ChildStdin {
    /// Queue `buf` for the child, returning how many bytes were accepted (all of them,
    /// unless the queue is full, in which case this waits for room).
    pub async fn write<B>(&self, buf: B) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        // SAFETY: the buffer guarantees `bytes_init()` initialized bytes at `stable_ptr()`.
        let data = unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) };
        let res = poll_fn(|cx| self.pipe.poll_write(data, cx)).await;
        (res, buf)
    }

    /// Queue all of `buf` for the child.
    pub async fn write_all<B>(&self, buf: B) -> BufResult<(), B>
    where
        B: IoBuf + Send + 'static,
    {
        let (res, buf) = self.write(buf).await;
        (res.map(|_| ()), buf)
    }

    /// Wait until everything queued so far has been written to the child.
    pub async fn flush(&self) -> io::Result<()> {
        poll_fn(|cx| self.pipe.poll_flush(cx)).await
    }

    /// Flush, then close stdin so the child sees end-of-file.
    pub async fn shutdown(&self) -> io::Result<()> {
        let flushed = self.flush().await;
        self.pipe.start_close();
        poll_fn(|cx| self.pipe.poll_closed(cx)).await;
        flushed
    }
}

impl Drop for ChildStdin {
    fn drop(&mut self) {
        unregister_waker(self.pipe.id);
        // Everything queued is still written, then the pipe closes.
        self.pipe.start_close();
    }
}

// ── Command and Child ────────────────────────────────────────────────────────

/// A process builder. See [`std::process::Command`].
pub struct Command {
    inner: std::process::Command,
    kill_on_drop: bool,
}

impl Command {
    /// A builder for running `program`.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            inner: std::process::Command::new(program),
            kill_on_drop: false,
        }
    }

    /// Add an argument.
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.inner.arg(arg);
        self
    }

    /// Add several arguments.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.inner.args(args);
        self
    }

    /// Set an environment variable for the child.
    pub fn env(&mut self, key: impl AsRef<OsStr>, val: impl AsRef<OsStr>) -> &mut Self {
        self.inner.env(key, val);
        self
    }

    /// Set several environment variables.
    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.inner.envs(vars);
        self
    }

    /// Remove an environment variable from the child.
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        self.inner.env_remove(key);
        self
    }

    /// Clear the child's environment entirely.
    pub fn env_clear(&mut self) -> &mut Self {
        self.inner.env_clear();
        self
    }

    /// Set the child's working directory.
    pub fn current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Self {
        self.inner.current_dir(dir);
        self
    }

    /// Configure the child's stdin.
    pub fn stdin(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.inner.stdin(cfg);
        self
    }

    /// Configure the child's stdout.
    pub fn stdout(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.inner.stdout(cfg);
        self
    }

    /// Configure the child's stderr.
    pub fn stderr(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.inner.stderr(cfg);
        self
    }

    /// Kill the child if its [`Child`] is dropped before it was waited for.
    pub fn kill_on_drop(&mut self, kill: bool) -> &mut Self {
        self.kill_on_drop = kill;
        self
    }

    /// Start the process. Must be called from inside [`block_on`](crate::block_on) when
    /// any stream is piped. The `fork`/`exec` itself is synchronous and quick.
    pub fn spawn(&mut self) -> io::Result<Child> {
        let mut child = self.inner.spawn()?;
        let stdin = child.stdin.take().map(|s| ChildStdin {
            pipe: PipeOut::spawn(s),
        });
        let stdout = child.stdout.take().map(|s| ChildStdout {
            pipe: PipeIn::spawn(s),
        });
        let stderr = child.stderr.take().map(|s| ChildStderr {
            pipe: PipeIn::spawn(s),
        });
        Ok(Child {
            inner: Arc::new(Mutex::new(child)),
            stdin,
            stdout,
            stderr,
            kill_on_drop: self.kill_on_drop,
            status: None,
        })
    }

    /// Run to completion and capture stdout and stderr; stdin is closed.
    pub async fn output(&mut self) -> io::Result<Output> {
        self.inner
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = self.spawn()?;
        let out = child.stdout.take().expect("stdout was piped");
        let err = child.stderr.take().expect("stderr was piped");
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        // Drain both pipes at once so the child can never block on a full one.
        let (a, b) = crate::join2(out.read_to_end(&mut stdout), err.read_to_end(&mut stderr)).await;
        a?;
        b?;
        let status = child.wait().await?;
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }

    /// Run to completion with stdio inherited from this process and return the exit status.
    pub async fn status(&mut self) -> io::Result<ExitStatus> {
        self.spawn()?.wait().await
    }
}

/// A running child process.
pub struct Child {
    inner: Arc<Mutex<StdChild>>,
    /// The child's stdin, if it was piped. Take it to hand it to another task.
    pub stdin: Option<ChildStdin>,
    /// The child's stdout, if it was piped.
    pub stdout: Option<ChildStdout>,
    /// The child's stderr, if it was piped.
    pub stderr: Option<ChildStderr>,
    kill_on_drop: bool,
    status: Option<ExitStatus>,
}

impl Child {
    /// The operating-system process id.
    pub fn id(&self) -> u32 {
        lock(&self.inner).id()
    }

    /// Wait for the child to exit. Closes our end of its stdin first so a child that
    /// reads to end-of-file can finish.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(s) = self.status {
            return Ok(s);
        }
        self.stdin.take();
        let inner = self.inner.clone();
        let status = spawn_blocking(move || {
            let mut delay = Duration::from_micros(500);
            loop {
                match lock(&inner).try_wait() {
                    Ok(Some(s)) => return Ok(s),
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
                std::thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_millis(20));
            }
        })
        .await
        .map_err(|e| io::Error::other(format!("waiting for the child failed: {e}")))??;
        self.status = Some(status);
        Ok(status)
    }

    /// Check whether the child has exited, without waiting.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(s) = self.status {
            return Ok(Some(s));
        }
        let status = lock(&self.inner).try_wait()?;
        self.status = status;
        Ok(status)
    }

    /// Ask the operating system to kill the child, without waiting for it to die.
    pub fn start_kill(&mut self) -> io::Result<()> {
        if self.status.is_some() {
            return Ok(());
        }
        lock(&self.inner).kill()
    }

    /// Kill the child and wait for it to exit.
    pub async fn kill(&mut self) -> io::Result<()> {
        self.start_kill()?;
        self.wait().await.map(|_| ())
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if self.status.is_some() {
            return;
        }
        if self.kill_on_drop {
            let _ = lock(&self.inner).kill();
        }
        // Reap it so it does not linger as a zombie, whatever happens to the caller.
        let inner = self.inner.clone();
        run_detached(move || {
            let _ = lock(&inner).wait();
        });
    }
}
