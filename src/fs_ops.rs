//! Shared asynchronous filesystem helpers (portable across backends).

use std::{
    ffi::OsString,
    fs::{Metadata, Permissions},
    io,
    path::{Path, PathBuf},
};

use crate::blocking::spawn_blocking;

fn join_err(e: crate::executor::JoinError) -> io::Error {
    io::Error::other(format!("filesystem operation failed: {e}"))
}

/// Read the entire contents of a file into a `Vec<u8>`.
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let path = path.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::read(path))
        .await
        .map_err(join_err)?
}

/// Write a slice as the entire contents of a file, creating or truncating it.
pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    let data = contents.as_ref().to_vec();
    spawn_blocking(move || std::fs::write(path, data))
        .await
        .map_err(join_err)?
}

/// Query metadata for `path`, following symlinks.
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let path = path.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::metadata(path))
        .await
        .map_err(join_err)?
}

/// Query metadata for `path` without following symlinks.
pub async fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let path = path.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::symlink_metadata(path))
        .await
        .map_err(join_err)?
}

/// Remove a file from the filesystem.
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::remove_file(path))
        .await
        .map_err(join_err)?
}

/// Remove an empty directory.
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::remove_dir(path))
        .await
        .map_err(join_err)?
}

/// Remove a directory and all its contents recursively.
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::remove_dir_all(path))
        .await
        .map_err(join_err)?
}

/// Rename a file or directory to a new name, replacing the original if `to` already exists.
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    let from = from.as_ref().to_path_buf();
    let to = to.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::rename(from, to))
        .await
        .map_err(join_err)?
}

/// Create a new empty directory at `path`.
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::create_dir(path))
        .await
        .map_err(join_err)?
}

/// Recursively create a directory and all of its missing parent components.
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::create_dir_all(path))
        .await
        .map_err(join_err)?
}

/// Copy the contents of one file to another, returning the number of bytes copied.
pub async fn copy(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<u64> {
    let from = from.as_ref().to_path_buf();
    let to = to.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::copy(from, to))
        .await
        .map_err(join_err)?
}

/// Change the permissions of the file or directory at `path`.
pub async fn set_permissions(path: impl AsRef<Path>, perm: Permissions) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    spawn_blocking(move || std::fs::set_permissions(path, perm))
        .await
        .map_err(join_err)?
}

/// A single entry yielded by [`ReadDir::next_entry`].
#[derive(Debug, Clone)]
pub struct DirEntry {
    path: PathBuf,
    file_name: OsString,
    file_type: std::fs::FileType,
}

impl DirEntry {
    /// Return the full path to this entry.
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    /// Return the bare file name of this entry.
    pub fn file_name(&self) -> OsString {
        self.file_name.clone()
    }

    /// Return the file type of this entry.
    pub fn file_type(&self) -> std::fs::FileType {
        self.file_type
    }

    /// Query metadata for this entry without following symlinks.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let p = self.path.clone();
        spawn_blocking(move || std::fs::symlink_metadata(p))
            .await
            .map_err(join_err)?
    }
}

/// Stream of directory entries returned by [`read_dir`].
pub struct ReadDir {
    entries: std::vec::IntoIter<io::Result<DirEntry>>,
}

impl ReadDir {
    /// Yield the next directory entry, or `Ok(None)` at the end.
    pub async fn next_entry(&mut self) -> io::Result<Option<DirEntry>> {
        match self.entries.next() {
            Some(Ok(entry)) => Ok(Some(entry)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

/// Read the entries of a directory asynchronously.
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    let path = path.as_ref().to_path_buf();
    let entries = spawn_blocking(move || -> io::Result<Vec<io::Result<DirEntry>>> {
        let rd = std::fs::read_dir(path)?;
        let mut out = Vec::new();
        for res in rd {
            out.push(res.and_then(|e| {
                let ft = e.file_type()?;
                Ok(DirEntry {
                    path: e.path(),
                    file_name: e.file_name(),
                    file_type: ft,
                })
            }));
        }
        Ok(out)
    })
    .await
    .map_err(join_err)??;
    Ok(ReadDir {
        entries: entries.into_iter(),
    })
}
