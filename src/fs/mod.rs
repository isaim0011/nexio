//! Asynchronous file I/O backed by `io_uring` using owned buffers.

use std::{
    ffi::CString,
    io,
    os::unix::{
        ffi::OsStrExt,
        io::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    },
    path::Path,
};

use crate::{
    buf::{BufResult, IoBuf, IoBufMut, IoBufMutExt},
    reactor::{with_uring_mut, LazyOp},
};

pub use crate::fs_ops::*;

/// An asynchronously accessed file handle backed by `io_uring`.
pub struct File {
    fd: RawFd,
}

impl File {
    /// Open an existing file in read-only mode.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Create or truncate a file in write-only mode.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    /// Create a [`File`] from a standard library `std::fs::File`.
    pub fn from_std(file: std::fs::File) -> Self {
        Self {
            fd: file.into_raw_fd(),
        }
    }

    /// Read bytes starting at byte offset `pos` into owned buffer `buf`.
    pub async fn read_at<B>(&self, buf: B, pos: u64) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        let fd = self.fd;
        let (res, mut buf) = LazyOp::new(buf, move |r, b, w| unsafe {
            r.submit_read_with_waker(fd, b, pos, Some(w))
        })
        .await;
        if let Ok(n) = res {
            // SAFETY: kernel wrote n initialized bytes into buf.
            unsafe { buf.set_init(n) };
        }
        (res, buf)
    }

    /// Read exact bytes to fill `buf` capacity starting at byte offset `pos`.
    pub async fn read_exact_at<B>(&self, mut buf: B, pos: u64) -> BufResult<(), B>
    where
        B: IoBufMut + Send + 'static,
    {
        let total = buf.bytes_total();
        if total == 0 {
            return (Ok(()), buf);
        }

        let (res, b) = self.read_at(buf, pos).await;
        buf = b;
        let mut read_bytes = match res {
            Ok(0) => {
                return (
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    )),
                    buf,
                );
            }
            Ok(n) if n >= total => return (Ok(()), buf),
            Ok(n) => n,
            Err(e) => return (Err(e), buf),
        };

        while read_bytes < total {
            let slice = buf.slice(read_bytes..total);
            let (res, slice) = self.read_at(slice, pos + read_bytes as u64).await;
            buf = slice.into_inner();
            match res {
                Ok(0) => {
                    return (
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "failed to fill whole buffer",
                        )),
                        buf,
                    );
                }
                Ok(n) => read_bytes += n,
                Err(e) => return (Err(e), buf),
            }
        }

        unsafe { buf.set_init(read_bytes) };
        (Ok(()), buf)
    }

    /// Write bytes from owned buffer `buf` starting at byte offset `pos`.
    pub async fn write_at<B>(&self, buf: B, pos: u64) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        let fd = self.fd;
        LazyOp::new(buf, move |r, b, w| unsafe {
            r.submit_write_with_waker(fd, b, pos, Some(w))
        })
        .await
    }

    /// Write all initialized bytes of `buf` starting at byte offset `pos`.
    pub async fn write_all_at<B>(&self, mut buf: B, pos: u64) -> BufResult<(), B>
    where
        B: IoBuf + IoBufMut + Send + 'static,
    {
        let total = buf.bytes_init();
        let mut written = 0;

        while written < total {
            let slice = buf.slice(written..total);
            let (res, slice) = self.write_at(slice, pos + written as u64).await;
            buf = slice.into_inner();

            match res {
                Ok(0) => {
                    return (
                        Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write whole buffer",
                        )),
                        buf,
                    );
                }
                Ok(n) => written += n,
                Err(e) => return (Err(e), buf),
            }
        }

        (Ok(()), buf)
    }

    /// Read into multiple buffers at `pos` (`preadv`).
    pub async fn read_vectored_at<B>(&self, bufs: Vec<B>, pos: u64) -> BufResult<usize, Vec<B>>
    where
        B: IoBufMut + Send + 'static,
    {
        if bufs.is_empty() {
            return (Ok(0), bufs);
        }
        let fd = self.fd;
        let (res, mut bufs) = LazyOp::new(bufs, move |r, bs, w| unsafe {
            r.submit_readv_with_waker(fd, bs, pos, Some(w))
        })
        .await;
        if let Ok(n) = res {
            let mut rem = n;
            for b in &mut bufs {
                let take = rem.min(b.bytes_total());
                unsafe { b.set_init(take) };
                rem -= take;
                if rem == 0 {
                    break;
                }
            }
        }
        (res, bufs)
    }

    /// Write from multiple buffers at `pos` (`pwritev`).
    pub async fn write_vectored_at<B>(&self, bufs: Vec<B>, pos: u64) -> BufResult<usize, Vec<B>>
    where
        B: IoBuf + Send + 'static,
    {
        if bufs.is_empty() {
            return (Ok(0), bufs);
        }
        let fd = self.fd;
        LazyOp::new(bufs, move |r, bs, w| unsafe {
            r.submit_writev_with_waker(fd, bs, 0, pos, Some(w))
        })
        .await
    }

    /// Write all initialized bytes from multiple buffers at `pos` (`pwritev`).
    pub async fn write_all_vectored_at<B>(
        &self,
        mut bufs: Vec<B>,
        pos: u64,
    ) -> BufResult<(), Vec<B>>
    where
        B: IoBuf + Send + 'static,
    {
        let fd = self.fd;
        let mut idx = 0usize;
        let mut off = 0usize;
        let mut file_off = pos;
        while idx < bufs.len() {
            let init_len = bufs[idx].bytes_init();
            if off >= init_len {
                idx += 1;
                off = 0;
                continue;
            }
            let (res, out) = LazyOp::new(bufs, move |r, bs, w| unsafe {
                r.submit_writev_with_waker(fd, &bs[idx..], off, file_off, Some(w))
            })
            .await;
            bufs = out;
            match res {
                Ok(0) => {
                    return (
                        Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write whole vectored buffer",
                        )),
                        bufs,
                    );
                }
                Ok(mut n) => {
                    file_off += n as u64;
                    while n > 0 && idx < bufs.len() {
                        let rem = bufs[idx].bytes_init().saturating_sub(off);
                        if n >= rem {
                            n -= rem;
                            idx += 1;
                            off = 0;
                        } else {
                            off += n;
                            n = 0;
                        }
                    }
                }
                Err(e) => return (Err(e), bufs),
            }
        }
        (Ok(()), bufs)
    }

    /// Flush all modified file data and metadata to disk (`fsync`).
    pub async fn sync_all(&self) -> io::Result<()> {
        let op = with_uring_mut(|r| r.fsync(self.fd, false))?;
        op.await.map(|_| ())
    }

    /// Flush all modified file data (without unnecessary metadata) to disk (`fdatasync`).
    pub async fn sync_data(&self) -> io::Result<()> {
        let op = with_uring_mut(|r| r.fsync(self.fd, true))?;
        op.await.map(|_| ())
    }

    fn dup_std_file(&self) -> io::Result<std::fs::File> {
        let dup_fd = unsafe { libc::fcntl(self.fd, libc::F_DUPFD_CLOEXEC, 0) };
        if dup_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { std::fs::File::from_raw_fd(dup_fd) })
    }

    /// Query metadata for this open file.
    pub async fn metadata(&self) -> io::Result<std::fs::Metadata> {
        let std_file = self.dup_std_file()?;
        crate::blocking::spawn_blocking(move || std_file.metadata())
            .await
            .map_err(|e| io::Error::other(format!("file metadata failed: {e}")))?
    }

    /// Truncate or extend the underlying file to `size` bytes.
    pub async fn set_len(&self, size: u64) -> io::Result<()> {
        let std_file = self.dup_std_file()?;
        crate::blocking::spawn_blocking(move || std_file.set_len(size))
            .await
            .map_err(|e| io::Error::other(format!("file set_len failed: {e}")))?
    }

    /// Change the permissions of the underlying file.
    pub async fn set_permissions(&self, perm: std::fs::Permissions) -> io::Result<()> {
        let std_file = self.dup_std_file()?;
        crate::blocking::spawn_blocking(move || std_file.set_permissions(perm))
            .await
            .map_err(|e| io::Error::other(format!("file set_permissions failed: {e}")))?
    }

    /// Asynchronously close the file descriptor via `io_uring`.
    pub async fn close(self) -> io::Result<()> {
        let fd = self.into_raw_fd();
        let op = with_uring_mut(|r| r.close(fd))?;
        op.await.map(|_| ())
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl IntoRawFd for File {
    fn into_raw_fd(self) -> RawFd {
        let fd = self.fd;
        std::mem::forget(self);
        fd
    }
}

impl FromRawFd for File {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self { fd }
    }
}

impl Drop for File {
    fn drop(&mut self) {
        crate::reactor::close_fd(self.fd);
    }
}

/// Builder for configuring how a [`File`] is opened.
#[derive(Debug, Clone)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
    noatime: bool,
    mode: u32,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    /// Create a blank new set of options ready for configuration.
    pub fn new() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            noatime: false,
            mode: 0o666,
        }
    }

    /// Set read access.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    /// Set write access.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    /// Set append mode.
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    /// Truncate existing file on open.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    /// Create file if it does not exist.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    /// Atomically create a new file, failing if it already exists.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    /// Do not update the file's access time on reads (`O_NOATIME`).
    ///
    /// Skips a metadata update on every read, which is measurable on page-cache-heavy
    /// workloads. The kernel only allows it for the file's owner (or with `CAP_FOWNER`); when it
    /// is refused, the file is opened normally instead.
    pub fn noatime(&mut self, noatime: bool) -> &mut Self {
        self.noatime = noatime;
        self
    }

    /// Set Unix permission mode bits for newly created files.
    pub fn mode(&mut self, mode: u32) -> &mut Self {
        self.mode = mode;
        self
    }

    /// Open the file at `path` asynchronously via `io_uring` `openat`.
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let cpath = CString::new(path.as_ref().as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))?;

        let access_mode = match (self.read, self.write || self.append) {
            (true, false) => libc::O_RDONLY,
            (false, true) => libc::O_WRONLY,
            (true, true) => libc::O_RDWR,
            (false, false) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "must specify read or write access",
                ));
            }
        };

        let mut flags = access_mode | libc::O_CLOEXEC;
        if self.append {
            flags |= libc::O_APPEND;
        }
        if self.truncate {
            flags |= libc::O_TRUNC;
        }
        if self.create_new {
            flags |= libc::O_CREAT | libc::O_EXCL;
        } else if self.create {
            flags |= libc::O_CREAT;
        }

        if self.noatime {
            let op = with_uring_mut(|r| {
                r.openat(libc::AT_FDCWD, &cpath, flags | libc::O_NOATIME, self.mode)
            })?;
            match op.await {
                Ok(fd) => return Ok(File { fd }),
                // Not the owner: fall back to a normal open.
                Err(e) if e.raw_os_error() == Some(libc::EPERM) => {}
                Err(e) => return Err(e),
            }
        }
        let op = with_uring_mut(|r| r.openat(libc::AT_FDCWD, &cpath, flags, self.mode))?;
        let fd = op.await?;
        Ok(File { fd })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::block_on;

    #[test]
    fn test_file_write_read_sync_close() {
        block_on(async {
            let path =
                std::env::temp_dir().join(format!("nexio_fs_test_{}.bin", std::process::id()));
            let _ = std::fs::remove_file(&path);

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .await
                .unwrap();

            let payload = b"nexio io_uring file payload".to_vec();
            let (wres, payload) = file.write_all_at(payload, 0).await;
            wres.unwrap();
            file.sync_data().await.unwrap();
            file.sync_all().await.unwrap();

            let read_buf = vec![0u8; payload.len()];
            let (rres, read_buf) = file.read_at(read_buf, 0).await;
            assert_eq!(rres.unwrap(), payload.len());
            assert_eq!(read_buf, payload);

            file.close().await.unwrap();
            let _ = std::fs::remove_file(&path);
        });
    }
}
