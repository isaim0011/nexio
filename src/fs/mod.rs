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
    reactor::{LazyOp, REACTOR},
};

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

    /// Flush all modified file data and metadata to disk (`fsync`).
    pub async fn sync_all(&self) -> io::Result<()> {
        let op = REACTOR.with(|r| r.borrow_mut().fsync(self.fd, false))?;
        op.await.map(|_| ())
    }

    /// Flush all modified file data (without unnecessary metadata) to disk (`fdatasync`).
    pub async fn sync_data(&self) -> io::Result<()> {
        let op = REACTOR.with(|r| r.borrow_mut().fsync(self.fd, true))?;
        op.await.map(|_| ())
    }

    /// Asynchronously close the file descriptor via `io_uring`.
    pub async fn close(self) -> io::Result<()> {
        let fd = self.into_raw_fd();
        let op = REACTOR.with(|r| r.borrow_mut().close(fd))?;
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
        if self.fd >= 0 {
            unsafe { libc::close(self.fd) };
        }
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

        let op = REACTOR.with(|r| {
            r.borrow_mut()
                .openat(libc::AT_FDCWD, &cpath, flags, self.mode)
        })?;
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
