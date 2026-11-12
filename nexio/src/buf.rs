//! Owned-buffer completion I/O result. io_uring is completion-based: you
//! hand the kernel a pointer and it writes into it *after* your call
//! returns, so a borrowed `&mut [u8]` isn't safe — the kernel could
//! still be writing into it after the borrow's lifetime ends. The fix
//! every io_uring runtime uses (tokio-uring, compio, monoio) is the
//! same one here: the caller gives up *ownership* of the buffer for
//! the duration of the operation, and gets it back in the result,
//! whether the op succeeded or failed.

/// Result of an owned-buffer operation: `T` is the normal I/O result
/// (bytes read/written), `B` is the buffer, handed back either way so
/// the caller can reuse its allocation without a new one.
#[derive(Debug)]
pub struct BufResult<T, B>(pub std::io::Result<T>, pub B);
