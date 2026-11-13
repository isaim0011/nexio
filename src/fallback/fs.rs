//! Runtime-fallback filesystem module dispatching between `io_uring` and `poll`.

#[allow(unreachable_pub, unused_imports, dead_code)]
#[path = "../poll/fs.rs"]
mod poll;
#[allow(unreachable_pub, unused_imports, dead_code)]
#[path = "../fs/mod.rs"]
mod uring;

use std::{io, path::Path};

use crate::buf::{BufResult, IoBuf, IoBufMut};
pub use crate::fs_ops::*;

/// An asynchronously accessed file.
pub enum File {
    /// Linux `io_uring` file handle.
    Uring(uring::File),
    /// Portable `spawn_blocking` fallback file handle.
    Poll(poll::File),
}

impl File {
    /// Open an existing file in read-only mode.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::File::open(path).await.map(File::Uring)
        } else {
            poll::File::open(path).await.map(File::Poll)
        }
    }

    /// Create or truncate a file in write-only mode.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        if crate::reactor::is_uring() {
            uring::File::create(path).await.map(File::Uring)
        } else {
            poll::File::create(path).await.map(File::Poll)
        }
    }

    /// Wrap an existing standard library [`std::fs::File`].
    pub fn from_std(file: std::fs::File) -> Self {
        if crate::reactor::is_uring() {
            File::Uring(uring::File::from_std(file))
        } else {
            File::Poll(poll::File::from_std(file))
        }
    }

    /// Read into `buf` at file offset `pos`.
    pub async fn read_at<B>(&self, buf: B, pos: u64) -> BufResult<usize, B>
    where
        B: IoBufMut + Send + 'static,
    {
        match self {
            File::Uring(f) => f.read_at(buf, pos).await,
            File::Poll(f) => f.read_at(buf, pos).await,
        }
    }

    /// Read until `buf` is full at file offset `pos`.
    pub async fn read_exact_at<B>(&self, buf: B, pos: u64) -> BufResult<(), B>
    where
        B: IoBufMut + Send + 'static,
    {
        match self {
            File::Uring(f) => f.read_exact_at(buf, pos).await,
            File::Poll(f) => f.read_exact_at(buf, pos).await,
        }
    }

    /// Write `buf` at file offset `pos`.
    pub async fn write_at<B>(&self, buf: B, pos: u64) -> BufResult<usize, B>
    where
        B: IoBuf + Send + 'static,
    {
        match self {
            File::Uring(f) => f.write_at(buf, pos).await,
            File::Poll(f) => f.write_at(buf, pos).await,
        }
    }

    /// Write all initialized bytes of `buf` at file offset `pos`.
    pub async fn write_all_at<B>(&self, buf: B, pos: u64) -> BufResult<(), B>
    where
        B: IoBuf + IoBufMut + Send + 'static,
    {
        match self {
            File::Uring(f) => f.write_all_at(buf, pos).await,
            File::Poll(f) => f.write_all_at(buf, pos).await,
        }
    }

    /// Vectored positional read into multiple buffers at `pos`.
    pub async fn read_vectored_at<B>(&self, bufs: Vec<B>, pos: u64) -> BufResult<usize, Vec<B>>
    where
        B: IoBufMut + Send + 'static,
    {
        match self {
            File::Uring(f) => f.read_vectored_at(bufs, pos).await,
            File::Poll(f) => f.read_vectored_at(bufs, pos).await,
        }
    }

    /// Vectored positional write from multiple buffers at `pos`.
    pub async fn write_vectored_at<B>(&self, bufs: Vec<B>, pos: u64) -> BufResult<usize, Vec<B>>
    where
        B: IoBuf + Send + 'static,
    {
        match self {
            File::Uring(f) => f.write_vectored_at(bufs, pos).await,
            File::Poll(f) => f.write_vectored_at(bufs, pos).await,
        }
    }

    /// Write all initialized bytes from multiple buffers at `pos`.
    pub async fn write_all_vectored_at<B>(&self, bufs: Vec<B>, pos: u64) -> BufResult<(), Vec<B>>
    where
        B: IoBuf + Send + 'static,
    {
        match self {
            File::Uring(f) => f.write_all_vectored_at(bufs, pos).await,
            File::Poll(f) => f.write_all_vectored_at(bufs, pos).await,
        }
    }

    /// Flush all modified file data and metadata to disk (`fsync`).
    pub async fn sync_all(&self) -> io::Result<()> {
        match self {
            File::Uring(f) => f.sync_all().await,
            File::Poll(f) => f.sync_all().await,
        }
    }

    /// Flush all modified file data to disk (`fdatasync`).
    pub async fn sync_data(&self) -> io::Result<()> {
        match self {
            File::Uring(f) => f.sync_data().await,
            File::Poll(f) => f.sync_data().await,
        }
    }

    /// Query metadata for this open file.
    pub async fn metadata(&self) -> io::Result<std::fs::Metadata> {
        match self {
            File::Uring(f) => f.metadata().await,
            File::Poll(f) => f.metadata().await,
        }
    }

    /// Truncate or extend the underlying file to `size` bytes.
    pub async fn set_len(&self, size: u64) -> io::Result<()> {
        match self {
            File::Uring(f) => f.set_len(size).await,
            File::Poll(f) => f.set_len(size).await,
        }
    }

    /// Change the permissions of the underlying file.
    pub async fn set_permissions(&self, perm: std::fs::Permissions) -> io::Result<()> {
        match self {
            File::Uring(f) => f.set_permissions(perm).await,
            File::Poll(f) => f.set_permissions(perm).await,
        }
    }

    /// Asynchronously close the file.
    pub async fn close(self) -> io::Result<()> {
        match self {
            File::Uring(f) => f.close().await,
            File::Poll(f) => f.close().await,
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
    noatime: bool,
    mode: u32,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    /// Create a blank new set of options.
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
    pub fn noatime(&mut self, noatime: bool) -> &mut Self {
        self.noatime = noatime;
        self
    }

    /// Set Unix permission mode bits for newly created files.
    pub fn mode(&mut self, mode: u32) -> &mut Self {
        self.mode = mode;
        self
    }

    /// Open a file at `path` with the configured options.
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        if crate::reactor::is_uring() {
            let mut opts = uring::OpenOptions::new();
            opts.read(self.read)
                .write(self.write)
                .append(self.append)
                .truncate(self.truncate)
                .create(self.create)
                .create_new(self.create_new)
                .noatime(self.noatime)
                .mode(self.mode);
            opts.open(path).await.map(File::Uring)
        } else {
            let mut opts = poll::OpenOptions::new();
            opts.read(self.read)
                .write(self.write)
                .append(self.append)
                .truncate(self.truncate)
                .create(self.create)
                .create_new(self.create_new)
                .noatime(self.noatime)
                .mode(self.mode);
            opts.open(path).await.map(File::Poll)
        }
    }
}
