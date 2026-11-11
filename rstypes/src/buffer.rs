//! Owned buffers, and the leases and pool buffers that share their backing.
//!
//! I/O takes a buffer by value and gives it back when the operation ends. While the operation is
//! in progress, the driver owns the buffer. Thus a dropped or forgotten future cannot free memory
//! that the kernel still uses.
//!
//! - [`Buffer`] is the contract for the bytes that a send reads. [`BufferMut`] adds the writable
//!   capacity that a receive fills. Their address stays the same when the buffer moves.
//! - `Vec<u8>` and `&'static [u8]` obey the contract directly. A [`Backing`] holds each other
//!   region, for example a `Box<[u8]>`, a pool slot, or memory that a library protects. It frees
//!   or returns the region through its one [`Release`] hook.
//! - A [`WriteLoan`] holds a writable buffer during a receive. It records the exact extent that
//!   the receive filled, and it makes only those bytes readable.
//!
//! A buffer gives four separate facts: the writable capacity, the initialized length, the
//! allocation size, and the alignment. An inline array, for example `[u8; 4096]`, does not obey
//! the contract, because its bytes move when the array moves.
//!
//! ```
//! # extern crate rstypes as rsasync;
//! use rsasync::buffer::WriteLoan;
//!
//! // Before the receive, the caller owns the buffer.
//! let mut buf = Vec::with_capacity(64);
//! buf.extend_from_slice(b"GET ");
//!
//! // During the receive, the loan owns the buffer. The backend writes into the capacity after the
//! // initialized bytes. Here, a copy does the work of the backend.
//! let mut loan = WriteLoan::new(buf);
//! assert_eq!(loan.fill_from(b"/index.html"), 11);
//! assert_eq!(loan.filled(), b"/index.html");
//!
//! // After the receive, the caller owns the buffer again, with exactly the filled bytes.
//! let buf = loan.into_inner();
//! assert_eq!(buf, b"GET /index.html");
//! ```

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt;
use core::mem::ManuallyDrop;
use core::ptr::{self, NonNull};

/// Bytes whose address stays the same when the value that owns them moves.
///
/// A send reads the initialized bytes of a `Buffer`. The driver keeps the value until the
/// operation releases its backing, and only then gives it back or drops it.
///
/// # Safety
///
/// An implementation guarantees these conditions from when the value is made until it drops:
///
/// - [`as_ptr`](Self::as_ptr) gives an address that is not null and that is valid for reads of
///   [`initialized`](Self::initialized) bytes, and these bytes are initialized.
/// - A move of the value, also as a part of a different value that moves, does not change the
///   address, the initialized length, or the bytes. A pointer from `as_ptr` stays valid for reads
///   after the move. Thus the value holds its region through a raw pointer or a shared reference.
///   It does not hold it through a `Box` or a `&mut` reference, because a move of one of these
///   asserts unique access to the region.
/// - Only a method that takes `&mut self` changes the address, the initialized length, or the
///   bytes.
#[expect(
    unsafe_code,
    reason = "drivers rely on the stable address that this unsafe trait guarantees"
)]
pub unsafe trait Buffer: 'static {
    /// The address of the first byte.
    fn as_ptr(&self) -> *const u8;

    /// The number of initialized bytes, from the first byte.
    fn initialized(&self) -> usize;

    /// The size in bytes of the allocation that this value keeps, and that its release frees or
    /// returns.
    ///
    /// The allocation size can be larger than the capacity, for example when a pool rounds a slot
    /// up to a page. A region that no release frees, for example a static region, gives zero.
    fn allocation_size(&self) -> usize;

    /// The largest power of two that divides the address of the first byte.
    ///
    /// Compare it with the alignment that an operation needs, for example the logical block size
    /// of a file that is open for direct I/O.
    fn alignment(&self) -> usize {
        let address = self.as_ptr().addr();
        address.isolate_lowest_one()
    }

    /// The initialized bytes.
    #[expect(
        unsafe_code,
        reason = "the trait contract makes the initialized bytes valid for reads"
    )]
    fn bytes(&self) -> &[u8] {
        // SAFETY: the trait contract makes `as_ptr` non-null and valid for reads of `initialized`
        // initialized bytes in one region. Only `&mut self` changes them, so they stay unchanged
        // while `self` is borrowed.
        unsafe { core::slice::from_raw_parts(self.as_ptr(), self.initialized()) }
    }
}

/// A [`Buffer`] with writable capacity after its initialized bytes.
///
/// A receive fills the capacity after the initialized bytes through a [`WriteLoan`]. To receive
/// into the full capacity again, first clear the buffer, for example with `Vec::clear`.
///
/// # Safety
///
/// An implementation guarantees the conditions of [`Buffer`], and also these conditions:
///
/// - [`as_mut_ptr`](Self::as_mut_ptr) gives the address of `as_ptr`. It is valid for reads and
///   writes of [`capacity`](Self::capacity) bytes, also after a move of the value.
/// - The capacity is at least the initialized length. A move of the value does not change it.
/// - [`set_initialized`](Self::set_initialized) with `len` makes `initialized` give `len`, and it
///   changes no other fact.
/// - While the value lives, no other code reads or writes its region, except through a pointer
///   from `as_ptr` or `as_mut_ptr`.
#[expect(
    unsafe_code,
    reason = "a receive writes through the address that this unsafe trait guarantees"
)]
pub unsafe trait BufferMut: Buffer {
    /// The address of the first byte, for writes.
    fn as_mut_ptr(&mut self) -> *mut u8;

    /// The number of writable bytes, from the first byte.
    fn capacity(&self) -> usize;

    /// Records that the first `len` bytes are initialized.
    ///
    /// # Safety
    ///
    /// `len` is at most [`capacity`](Self::capacity), and the first `len` bytes are initialized.
    unsafe fn set_initialized(&mut self, len: usize);
}

#[expect(unsafe_code, reason = "a Vec keeps its heap block at one address while it moves")]
// SAFETY: the `Vec` documentation, "Guarantees", gives each condition. A `Vec` does not keep its
// elements inline, because "the contents of a `Vec` wouldn't have a stable address if it were only
// moved". Its heap block holds `len` initialized elements and then `capacity - len` uninitialized
// elements, and only a method that takes `&mut self` reallocates it. `Vec::as_ptr` and
// `Vec::as_mut_ptr` do "not materialize a reference to the underlying slice", so a pointer from
// them stays valid while the `Vec` moves. "Using `unsafe` code to write to the excess capacity,
// and then increasing the length to match, is always valid."
unsafe impl Buffer for Vec<u8> {
    fn as_ptr(&self) -> *const u8 {
        Vec::as_ptr(self)
    }

    fn initialized(&self) -> usize {
        self.len()
    }

    fn allocation_size(&self) -> usize {
        Vec::capacity(self)
    }
}

#[expect(
    unsafe_code,
    reason = "a Vec is the only owner of its heap block and of its capacity"
)]
// SAFETY: as for `Buffer`: a `Vec` is the only owner of its heap block, and its capacity after
// `len` is writable.
unsafe impl BufferMut for Vec<u8> {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        Vec::as_mut_ptr(self)
    }

    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }

    unsafe fn set_initialized(&mut self, len: usize) {
        // SAFETY: the caller makes `len` at most the capacity, with the first `len` bytes
        // initialized. This is the contract of `Vec::set_len`.
        unsafe { self.set_len(len) };
    }
}

#[expect(unsafe_code, reason = "a static region keeps its address for the whole program")]
// SAFETY: a shared reference to a static slice is a copy of an address and a length. The bytes
// live for the whole program, nothing frees them, and nothing writes them while a shared reference
// to them exists.
unsafe impl Buffer for &'static [u8] {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn initialized(&self) -> usize {
        self.len()
    }

    fn allocation_size(&self) -> usize {
        0
    }
}

/// The release hook of a backing: the one function that frees or returns its region.
///
/// The hook is a function and one data pointer. A [`Backing`] calls `release(data, ptr,
/// capacity)` one time, with its own region, when it drops. The hook can free the region, return
/// it to a pool, or give it back to the library that owns it. It runs on the thread that drops
/// the backing, which can be any thread, so it must not block.
#[derive(Debug)]
pub struct Release {
    data: *const (),
    release: unsafe fn(*const (), NonNull<u8>, usize),
}

impl Release {
    /// Makes a hook that calls `release(data, ptr, capacity)` with the region of its backing.
    pub const fn new(data: *const (), release: unsafe fn(*const (), NonNull<u8>, usize)) -> Self {
        Self { data, release }
    }
}

/// An owned writable region that one [`Release`] hook frees or returns.
///
/// A `Backing` holds a writable region from any source: a `Vec<u8>`, a `Box<[u8]>`, a pool slot, a
/// buffer that the kernel selected, or memory that a library protects. Thus all of them are one
/// type. A `Backing` is the only owner of its region. When it drops, it calls its hook one time, on
/// the thread that drops it. A `Backing` made from a `Vec<u8>` or a `Box<[u8]>` keeps the same
/// bytes: the conversion does not copy or allocate.
///
/// A `Backing` is `Send` and `Sync`: [`from_raw_parts`](Self::from_raw_parts) accepts only a
/// region and a hook that each thread can use.
pub struct Backing {
    ptr: NonNull<u8>,
    capacity: usize,
    initialized: usize,
    allocation_size: usize,
    release: Release,
}

impl Backing {
    /// Makes a backing from a region and its release hook.
    ///
    /// The region is `capacity` bytes at `ptr`, and its first `initialized` bytes are initialized.
    /// `allocation_size` is the size of the allocation that the hook frees or returns. When the
    /// backing drops, it calls the hook with `ptr` and `capacity`, on the thread that drops it.
    ///
    /// ```
    /// # extern crate rstypes as rsasync;
    /// use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
    /// use std::ptr::{self, NonNull};
    ///
    /// use rsasync::buffer::{Backing, Buffer, BufferMut, Release};
    ///
    /// /// One page, aligned to the page.
    /// #[repr(C, align(4096))]
    /// struct Page([u8; 4096]);
    /// const PAGE: Layout = Layout::new::<Page>();
    ///
    /// /// The release hook: frees the page. A library can also clear or unlock its memory here.
    /// ///
    /// /// # Safety
    /// ///
    /// /// `ptr` is from `page`, and nothing uses it after this call.
    /// unsafe fn free_page(_: *const (), ptr: NonNull<u8>, _: usize) {
    ///     // SAFETY: `page` allocated `ptr` with `PAGE`.
    ///     unsafe { dealloc(ptr.as_ptr(), PAGE) };
    /// }
    ///
    /// /// A backing of 1000 writable bytes in one page.
    /// fn page() -> Backing {
    ///     // SAFETY: the size of `PAGE` is not zero.
    ///     let Some(ptr) = NonNull::new(unsafe { alloc(PAGE) }) else { handle_alloc_error(PAGE) };
    ///     // SAFETY: the page is valid for reads and writes of 4096 bytes. Only the backing uses
    ///     // it, and `free_page` frees it on any thread.
    ///     unsafe { Backing::from_raw_parts(ptr, 1000, 0, PAGE.size(), Release::new(ptr::null(), free_page)) }
    /// }
    ///
    /// let buf = page();
    /// assert_eq!((buf.capacity(), buf.allocation_size()), (1000, 4096));
    /// assert!(buf.alignment() >= 4096);
    /// drop(buf); // calls `free_page` one time
    /// ```
    ///
    /// # Safety
    ///
    /// - `ptr` is valid for reads and writes of `capacity` bytes in one allocation, until the hook
    ///   runs. `initialized` is at most `capacity`, and the first `initialized` bytes are
    ///   initialized.
    /// - Until the hook runs, no other code reads, writes, or frees the region.
    /// - A call of the hook with `ptr` and `capacity`, on any thread, releases the region. Each
    ///   thread can read and write the region and call the hook.
    #[expect(
        unsafe_code,
        reason = "the caller gives the region and the hook that the backing owns"
    )]
    pub const unsafe fn from_raw_parts(
        ptr: NonNull<u8>,
        capacity: usize,
        initialized: usize,
        allocation_size: usize,
        release: Release,
    ) -> Self {
        Self {
            ptr,
            capacity,
            initialized,
            allocation_size,
            release,
        }
    }
}

/// Frees the heap block of a `Vec<u8>` that became a [`Backing`].
///
/// # Safety
///
/// `ptr` and `capacity` are from a `Vec<u8>` that nothing dropped, and nothing uses them after
/// this call.
#[expect(
    unsafe_code,
    reason = "the Vec form frees its block with the pointer and the capacity of that Vec"
)]
unsafe fn release_vec(_: *const (), ptr: NonNull<u8>, capacity: usize) {
    // SAFETY: the caller gives the pointer and the capacity of a `Vec<u8>` that nothing dropped.
    // The `Vec` documentation says that its reported capacity "can even be used to manually free
    // the memory allocated by a `Vec`". A length of zero drops no element.
    drop(unsafe { Vec::from_raw_parts(ptr.as_ptr(), 0, capacity) });
}

impl From<Vec<u8>> for Backing {
    #[expect(
        unsafe_code,
        reason = "the backing takes the heap block of a Vec that it does not drop"
    )]
    fn from(vec: Vec<u8>) -> Self {
        let mut vec = ManuallyDrop::new(vec);
        // SAFETY: `Vec::as_mut_ptr` is never null. Without an allocation it gives a dangling
        // pointer, which is not null.
        let ptr = unsafe { NonNull::new_unchecked(vec.as_mut_ptr()) };
        // SAFETY: the `Vec` owns `capacity` bytes at `ptr`, and the first `len` of them are
        // initialized (the `Vec` documentation, "Guarantees"). `ManuallyDrop` keeps the `Vec` from
        // freeing them, so only the backing uses them. `release_vec` frees them with the pointer
        // and the capacity of that `Vec`, and the global allocator allows this on any thread.
        unsafe {
            Self::from_raw_parts(
                ptr,
                vec.capacity(),
                vec.len(),
                vec.capacity(),
                Release::new(ptr::null(), release_vec),
            )
        }
    }
}

impl From<Box<[u8]>> for Backing {
    /// Keeps the same bytes: a `Box<[u8]>` becomes a `Vec<u8>` "without clones or allocation"
    /// (`<[T]>::into_vec`), and the `Vec` becomes the backing.
    ///
    /// A `Box<[u8]>` is not a [`Buffer`] itself, because a pointer from a `Box` is not valid after
    /// the `Box` moves (the `Box` documentation, "Considerations for unsafe code").
    fn from(boxed: Box<[u8]>) -> Self {
        Self::from(boxed.into_vec())
    }
}

impl Drop for Backing {
    #[expect(unsafe_code, reason = "the drop is the one call of the release hook")]
    fn drop(&mut self) {
        // SAFETY: `from_raw_parts` requires that a call of the hook with `ptr` and `capacity`
        // releases the region on any thread. This drop is the only call, and nothing uses the
        // region after it.
        unsafe { (self.release.release)(self.release.data, self.ptr, self.capacity) };
    }
}

#[expect(
    unsafe_code,
    reason = "a backing accepts only a region and a hook that each thread can use"
)]
#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "`from_raw_parts` requires a hook and a data pointer that each thread can use"
)]
// SAFETY: `from_raw_parts` requires that each thread can read and write the region and call the
// hook, so the backing can move to a different thread and drop there.
unsafe impl Send for Backing {}

#[expect(unsafe_code, reason = "a shared backing gives only reads that each thread can make")]
// SAFETY: a shared `Backing` gives only reads of its initialized bytes, which `from_raw_parts`
// allows from each thread. Nothing writes them while a shared reference exists.
unsafe impl Sync for Backing {}

#[expect(
    unsafe_code,
    reason = "a backing holds its region through a raw pointer that a move does not change"
)]
// SAFETY: `from_raw_parts` makes the region valid for reads and writes, with its first
// `initialized` bytes initialized, until the drop calls the hook. The backing holds the region
// through a raw pointer, so a move asserts no unique access. Only `set_initialized` changes a
// fact, and it takes `&mut self`.
unsafe impl Buffer for Backing {
    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr().cast_const()
    }

    fn initialized(&self) -> usize {
        self.initialized
    }

    fn allocation_size(&self) -> usize {
        self.allocation_size
    }
}

#[expect(unsafe_code, reason = "a backing is the only owner of its writable region")]
// SAFETY: as for `Buffer`: `from_raw_parts` makes the backing the only user of its writable
// region.
unsafe impl BufferMut for Backing {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    unsafe fn set_initialized(&mut self, len: usize) {
        self.initialized = len;
    }
}

impl fmt::Debug for Backing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backing")
            .field("ptr", &self.ptr)
            .field("capacity", &self.capacity)
            .field("initialized", &self.initialized)
            .field("allocation_size", &self.allocation_size)
            .finish_non_exhaustive()
    }
}

/// An exclusive writable loan over the capacity after the initialized bytes of a buffer.
///
/// A receive holds the loan while the backend writes. The loan owns the buffer, so no other code
/// reads or writes the region until the backend gives it back. The loan then records the exact
/// extent that the backend filled, and only those bytes become readable. A loan that the backend
/// did not fill gives back the buffer with its initialized length unchanged.
#[derive(Debug)]
pub struct WriteLoan<B> {
    buffer: B,
    start: usize,
}

impl<B: BufferMut> WriteLoan<B> {
    /// Lends the capacity after the initialized bytes of `buffer`.
    pub fn new(buffer: B) -> Self {
        let start = buffer.initialized();
        Self { buffer, start }
    }

    /// The address of the first byte of the loan, for the backend.
    ///
    /// The address stays the same when the loan moves.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        // The `BufferMut` contract keeps `start` in the capacity, so the address is in the region.
        self.buffer.as_mut_ptr().wrapping_add(self.start)
    }

    /// The number of bytes that the backend can write.
    pub fn capacity(&self) -> usize {
        self.buffer.capacity() - self.start
    }

    /// Records that the backend filled the first `len` bytes of the loan.
    ///
    /// # Safety
    ///
    /// `len` is at most [`capacity`](Self::capacity). The backend initialized the first `len`
    /// bytes of the loan, and it does not write them again.
    #[expect(unsafe_code, reason = "the backend tells how many bytes of the loan it initialized")]
    pub unsafe fn set_filled(&mut self, len: usize) {
        debug_assert!(len <= self.capacity(), "a fill is larger than its loan");
        // SAFETY: the bytes before `start` were initialized when the loan started, and nothing
        // changed them since then. The caller initialized the next `len` bytes, and `start + len`
        // is at most the capacity of the buffer.
        unsafe { self.buffer.set_initialized(self.start + len) };
    }

    /// Copies the most bytes of `bytes` that fit after the filled bytes, and records them. Gives
    /// the number of bytes that it copied.
    ///
    /// A driver uses it to give data that it holds already, with no call to the backend.
    #[expect(unsafe_code, reason = "the copy writes only in the loan, after its filled bytes")]
    pub fn fill_from(&mut self, bytes: &[u8]) -> usize {
        let filled = self.filled().len();
        let count = bytes.len().min(self.capacity() - filled);
        // SAFETY: the loan is valid for writes of `capacity` bytes, and `filled + count` is at
        // most `capacity`. `bytes` is a shared slice, so it is not in the region that the loan
        // owns exclusively. After the copy, the first `filled + count` bytes of the loan are
        // initialized.
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), self.as_mut_ptr().add(filled), count);
            self.set_filled(filled + count);
        }
        count
    }

    /// The bytes that the backend filled: exactly the recorded extent, and no uninitialized
    /// capacity.
    pub fn filled(&self) -> &[u8] {
        &self.buffer.bytes()[self.start..]
    }

    /// Ends the loan and gives back the buffer, with the filled bytes after its earlier
    /// initialized bytes.
    pub fn into_inner(self) -> B {
        self.buffer
    }
}

#[cfg(test)]
mod tests;
