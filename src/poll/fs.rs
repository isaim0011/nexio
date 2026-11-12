//! Async files on the portable backend.
//!
//! Regular files have no readiness interface on any platform, so every
//! operation runs on the blocking pool ([`spawn_blocking`]).
//! The API matches the io_uring backend's `File`: the buffer goes in and comes
//! back with the result.
//!
//! Dropping a pending operation does not cancel it, because a syscall already
//! running cannot be stopped. The job owns the buffer, so it can never be freed
//! underneath the syscall; the result is simply discarded.

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    blocking::spawn_blocking,
    buf::{BufResult, IoBuf, IoBufMut},
};

fn join_err(e: crate::executor::JoinError) -> io::Error {
    io::Error::other(format!("file operation failed: {e}"))
}

/// An asynchronously accessed file.
pub struct File {
    file: Arc<std::fs::File>,
}

/// One positional read into the buffer's spare capacity. Returns bytes read.
fn pread(file: &std::fs::File, buf: &mut impl IoBufMut, pos: u64) -> io::Result<usize> {
    let total = buf.bytes_total();
    let ptr = buf.stable_ptr_mut();
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: `[ptr, ptr + total)` is the buffer's allocation, owned by this job; pread
        // writes at most `total` bytes and never reads from it.
        let n = unsafe {
            libc::pread(
                file.as_raw_fd(),
                ptr as *mut libc::c_void,
                total,
                pos as libc::off_t,
            )
        };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        // The std API needs initialized bytes, so clear the spare capacity first.
        // SAFETY: `[ptr, ptr + total)` is the buffer's allocation, owned by this job.
        let slice = unsafe {
            std::ptr::write_bytes(ptr, 0, total);
            std::slice::from_raw_parts_mut(ptr, total)
        };
        file.seek_read(slice, pos)
    }
}

fn pwrite(file: &std::fs::File, data: &[u8], pos: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_at(data, pos)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        file.seek_write(data, pos)
    }
}

impl File {
    /// Open an existing file for reading.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Create a file for writing, truncating it if it exists.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    /// Wrap a standard-library file.
    pub fn from_std(file: std::fs::File) -> Self {
        Self {
            file: Arc::new(file),
        }
    }

    /// Read at `pos` into `buf`, returning the number of bytes read.
    pub async fn read_at<B>(&self, mut buf: B, pos: u64) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        let file = self.file.clone();
        let job = spawn_blocking(move || {
            let res = pread(&file, &mut buf, pos);
            if let Ok(n) = res {
                // SAFETY: pread wrote `n` bytes at the start of the buffer.
                unsafe { buf.set_init(n) };
            }
            (res, buf)
        });
        match job.await {
            Ok(out) => out,
            // The buffer was lost with the panicked job; nothing can be returned.
            Err(e) => panic!("{}", join_err(e)),
        }
    }

    /// Read until `buf` is full, starting at `pos`.
    pub async fn read_exact_at<B>(&self, mut buf: B, pos: u64) -> BufResult<(), B>
    where
        B: IoBufMut + Send + 'static,
    {
        let file = self.file.clone();
        let job = spawn_blocking(move || {
            let total = buf.bytes_total();
            let mut got = 0usize;
            while got < total {
                // SAFETY: `got < total`, so the pointer stays inside the buffer.
                let tail = unsafe { buf.stable_ptr_mut().add(got) };
                let mut view = RawTail {
                    ptr: tail,
                    len: total - got,
                };
                match pread(&file, &mut view, pos + got as u64) {
                    Ok(0) => {
                        let e = io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "failed to fill whole buffer",
                        );
                        return (Err(e), buf);
                    }
                    Ok(n) => got += n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return (Err(e), buf),
                }
            }
            // SAFETY: `total` bytes have been written into the buffer.
            unsafe { buf.set_init(total) };
            (Ok(()), buf)
        });
        match job.await {
            Ok(out) => out,
            Err(e) => panic!("{}", join_err(e)),
        }
    }

    /// Write `buf` at `pos`, returning the number of bytes written.
    pub async fn write_at<B>(&self, buf: B, pos: u64) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        let file = self.file.clone();
        let job = spawn_blocking(move || {
            // SAFETY: the buffer guarantees `bytes_init()` initialized bytes at `stable_ptr()`.
            let data = unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) };
            let res = pwrite(&file, data, pos);
            (res, buf)
        });
        match job.await {
            Ok(out) => out,
            Err(e) => panic!("{}", join_err(e)),
        }
    }

    /// Write all of `buf` at `pos`.
    pub async fn write_all_at<B>(&self, buf: B, pos: u64) -> BufResult<(), B>
    where
        B: IoBuf + Send + 'static,
    {
        let file = self.file.clone();
        let job = spawn_blocking(move || {
            let total = buf.bytes_init();
            let mut sent = 0usize;
            while sent < total {
                // SAFETY: `sent < total <= bytes_init()`, inside the initialized region.
                let data =
                    unsafe { std::slice::from_raw_parts(buf.stable_ptr().add(sent), total - sent) };
                match pwrite(&file, data, pos + sent as u64) {
                    Ok(0) => {
                        let e = io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write whole buffer",
                        );
                        return (Err(e), buf);
                    }
                    Ok(n) => sent += n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return (Err(e), buf),
                }
            }
            (Ok(()), buf)
        });
        match job.await {
            Ok(out) => out,
            Err(e) => panic!("{}", join_err(e)),
        }
    }

    /// Flush data and metadata to disk.
    pub async fn sync_all(&self) -> io::Result<()> {
        let file = self.file.clone();
        spawn_blocking(move || file.sync_all())
            .await
            .map_err(join_err)?
    }

    /// Flush data (not necessarily metadata) to disk.
    pub async fn sync_data(&self) -> io::Result<()> {
        let file = self.file.clone();
        spawn_blocking(move || file.sync_data())
            .await
            .map_err(join_err)?
    }

    /// Close the file. Operations still running on the pool finish first; the
    /// descriptor is released when the last of them completes.
    pub async fn close(self) -> io::Result<()> {
        drop(self);
        Ok(())
    }
}

/// A raw window onto the unfilled tail of a buffer, used to read at an offset
/// without creating a reference to uninitialized memory.
struct RawTail {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: `RawTail` is only built inside one blocking job from a pointer into a buffer
// that job exclusively owns, and never leaves it.
unsafe impl IoBufMut for RawTail {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.ptr
    }
    fn bytes_total(&self) -> usize {
        self.len
    }
    unsafe fn set_init(&mut self, _n: usize) {}
}

/// Options for opening a [`File`].
#[derive(Clone, Debug)]
pub struct OpenOptions {
    inner: std::fs::OpenOptions,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    /// A new set of options with everything off.
    pub fn new() -> Self {
        Self {
            inner: std::fs::OpenOptions::new(),
        }
    }

    /// Open for reading.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.inner.read(read);
        self
    }

    /// Open for writing.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.inner.write(write);
        self
    }

    /// Open for appending.
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.inner.append(append);
        self
    }

    /// Truncate an existing file to zero length.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.inner.truncate(truncate);
        self
    }

    /// Create the file if it does not exist.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.inner.create(create);
        self
    }

    /// Create the file, failing if it already exists.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.inner.create_new(create_new);
        self
    }

    /// Set the permission bits for a newly created file (Unix only).
    #[cfg(unix)]
    pub fn mode(&mut self, mode: u32) -> &mut Self {
        use std::os::unix::fs::OpenOptionsExt;
        self.inner.mode(mode);
        self
    }

    /// Open the file at `path` with these options.
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let opts = self.inner.clone();
        let path: PathBuf = path.as_ref().to_path_buf();
        let file = spawn_blocking(move || opts.open(path))
            .await
            .map_err(join_err)??;
        Ok(File::from_std(file))
    }
}
