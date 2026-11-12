//! Owned buffer types.
//!
//! The fundamental rule (Without Boats, "Notes on io-uring", 2020):
//!
//! > The kernel must own the buffer — Rust's type system has no other way
//! > to model an external, uncancellable actor touching memory on its own
//! > schedule.
//!
//! `BufResult<T, B>` pairs an operation result with the returned buffer so
//! callers can reclaim ownership after completion, without any intermediate
//! borrow.

use std::ops::{Deref, DerefMut};

// ── BufResult ────────────────────────────────────────────────────────────────

/// The return type of every owned-buffer operation: the I/O result paired
/// with the buffer the kernel has now finished with.
///
/// Modelled after tokio-uring's identical type.  The buffer is returned
/// regardless of whether the operation succeeded so the caller can inspect
/// or reuse it.
pub type BufResult<T, B> = (std::io::Result<T>, B);

// ── IoBuf / IoBufMut ─────────────────────────────────────────────────────────

/// Marker trait for buffers that may be passed to the kernel for reading
/// (i.e. the kernel *writes* into them).
///
/// # Safety
///
/// The implementor guarantees:
/// - `stable_ptr()` returns a pointer that remains valid and at the same
///   address for the entire lifetime of `Self` while ownership is held by
///   the driver slab.
/// - The memory region `[ptr, ptr + bytes_total())` is fully allocated and
///   does not alias any other live Rust reference.
pub unsafe trait IoBufMut: Unpin + 'static {
    /// Returns a raw mutable pointer to the start of the buffer.
    fn stable_ptr_mut(&mut self) -> *mut u8;

    /// Total capacity the kernel may write into, in bytes.
    fn bytes_total(&self) -> usize;

    /// Marks the first `n` bytes as initialised after a successful read.
    ///
    /// # Safety
    /// Caller guarantees the kernel has written at least `n` bytes.
    unsafe fn set_init(&mut self, n: usize);
}

/// Marker trait for buffers the kernel reads from (i.e. Write / Send ops).
///
/// # Safety
///
/// Same lifetime and aliasing guarantees as [`IoBufMut`].
pub unsafe trait IoBuf: Unpin + 'static {
    /// Returns a raw const pointer to the start of the buffer.
    fn stable_ptr(&self) -> *const u8;

    /// Number of bytes the kernel should read.
    fn bytes_init(&self) -> usize;
}

// ── Vec<u8> impls ─────────────────────────────────────────────────────────────

// SAFETY: Vec<u8> owns its heap allocation; the pointer is stable as long as
// the Vec is not resized.  The driver slab holds the Vec — not a borrow of it
// — so no resize can occur while the kernel holds the pointer.
unsafe impl IoBufMut for Vec<u8> {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }

    unsafe fn set_init(&mut self, n: usize) {
        // SAFETY: forwarded from caller guarantee.
        unsafe { self.set_len(n) }
    }
}

unsafe impl IoBuf for Vec<u8> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

// ── Slice — a fixed-capacity view into a Vec<u8> ─────────────────────────────

/// A fixed-capacity buffer slice that participates in ownership transfer.
///
/// Created via [`IoBufMutExt::slice`] from a `Vec<u8>`.  The original `Vec`
/// is recoverable after the operation completes via [`Slice::into_inner`].
pub struct Slice<B> {
    buf: B,
    begin: usize,
    end: usize,
}

impl<B: IoBufMut> Slice<B> {
    /// Wraps `buf`, exposing only the range `begin..end` to the kernel.
    pub fn new(buf: B, begin: usize, end: usize) -> Self {
        assert!(begin <= end);
        Self { buf, begin, end }
    }

    /// Recovers the underlying buffer after the operation completes.
    pub fn into_inner(self) -> B {
        self.buf
    }

    /// Current begin offset.
    pub fn begin(&self) -> usize {
        self.begin
    }

    /// Current end offset.
    pub fn end(&self) -> usize {
        self.end
    }
}

impl<B: IoBuf + IoBufMut> Deref for Slice<B> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: begin..end is a valid subrange of the allocated buffer.
        unsafe {
            std::slice::from_raw_parts(self.buf.stable_ptr().add(self.begin), self.end - self.begin)
        }
    }
}

impl<B: IoBuf + IoBufMut> DerefMut for Slice<B> {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.buf.stable_ptr_mut().add(self.begin),
                self.end - self.begin,
            )
        }
    }
}

// SAFETY: delegates stable_ptr_mut to inner buf; begin offset is always
// within allocated capacity (checked in Slice::new).
unsafe impl<B: IoBufMut> IoBufMut for Slice<B> {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        unsafe { self.buf.stable_ptr_mut().add(self.begin) }
    }

    fn bytes_total(&self) -> usize {
        self.end - self.begin
    }

    unsafe fn set_init(&mut self, n: usize) {
        unsafe { self.buf.set_init(self.begin + n) }
    }
}

unsafe impl<B: IoBuf> IoBuf for Slice<B> {
    fn stable_ptr(&self) -> *const u8 {
        unsafe { self.buf.stable_ptr().add(self.begin) }
    }

    fn bytes_init(&self) -> usize {
        self.end - self.begin
    }
}

// ── Extension trait for ergonomic slicing ────────────────────────────────────

/// Convenience extension for constructing a [`Slice`] from any `IoBufMut`.
pub trait IoBufMutExt: IoBufMut + Sized {
    /// Returns a [`Slice`] covering `range` within this buffer.
    fn slice(self, range: impl std::ops::RangeBounds<usize>) -> Slice<Self> {
        use std::ops::Bound::*;
        let begin = match range.start_bound() {
            Included(&n) => n,
            Excluded(&n) => n + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            Unbounded => self.bytes_total(),
        };
        Slice::new(self, begin, end)
    }
}

impl<B: IoBufMut> IoBufMutExt for B {}
