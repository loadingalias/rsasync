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
//! - A [`Lease`] freezes the initialized bytes of a buffer, or lends the bytes of a different
//!   owner, for example a `bytes::Bytes`. Its views share the backing with no copy, and the last
//!   lease or view that drops releases the backing. [`Backing::freeze_prefix`] freezes only the
//!   first bytes of a backing, and the rest of its region stays writable for the next receive.
//! - A [`Pool`] gives slots of one size as backings, with no allocation, and charges all of them
//!   to a budget. The last backing, lease, or view of a slot returns it to the pool, on any thread.
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
use core::mem::{ManuallyDrop, MaybeUninit};
use core::ptr::{self, NonNull};
use core::slice::{self, SliceIndex};

use crate::sync::Arc;
use crate::sync::atomic::{AtomicPtr, Ordering};

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
/// into the full capacity again, first call [`truncate(0)`](Self::truncate).
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

    /// The initialized bytes, for a change in place, for example a decryption.
    #[expect(
        unsafe_code,
        reason = "the trait contract makes the initialized bytes valid for writes"
    )]
    fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: the trait contract makes `as_mut_ptr` valid for reads and writes of `capacity`
        // bytes in one region, with the first `initialized` of them initialized. No other code
        // reads or writes them while `self` is borrowed mutably.
        unsafe { core::slice::from_raw_parts_mut(self.as_mut_ptr(), self.initialized()) }
    }

    /// Shortens the initialized bytes to their first `len` bytes. A `len` that is not shorter
    /// changes nothing.
    ///
    /// After `truncate(0)`, a receive can fill the full capacity again.
    #[expect(unsafe_code, reason = "a prefix of the initialized bytes is initialized")]
    fn truncate(&mut self, len: usize) {
        if len < self.initialized() {
            // SAFETY: `len` is less than the initialized length, which is at most the capacity, and
            // the first `len` bytes are a prefix of the initialized bytes.
            unsafe { self.set_initialized(len) };
        }
    }
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
pub struct Release(Hook);

/// What the drop of a backing does.
#[derive(Debug)]
enum Hook {
    /// Calls the hook of the caller, from [`Release::new`], with the region of the backing.
    Caller {
        data: *const (),
        release: unsafe fn(*const (), NonNull<u8>, usize),
    },
    /// Gives back one count of the owner that the parts of the region share: a pool slot, from
    /// [`Pool::acquire`], or the `Arc` of the lease of a split, from [`Backing::freeze_prefix`].
    /// The count names its owner, not the address of the region, so an empty tail at the end of a
    /// slot keeps that slot. A later lease or split of the backing moves or copies the count, with
    /// no allocation.
    Shared(Shared),
}

impl Release {
    /// Makes a hook that calls `release(data, ptr, capacity)` with the region of its backing.
    pub const fn new(data: *const (), release: unsafe fn(*const (), NonNull<u8>, usize)) -> Self {
        Self(Hook::Caller { data, release })
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

    /// Freezes the first `len` initialized bytes into a [`Lease`], with no copy, and gives the rest
    /// of the region as a writable backing of the same allocation: the initialized bytes after
    /// `len`, and then the capacity after them. A receive can fill the rest while the views of the
    /// lease go to their owners.
    ///
    /// The allocation is released once, after the last view of the lease and the last backing of
    /// the rest drop, on any thread. Each part reports the allocation size of the whole. A backing
    /// from a [`Pool`] splits with no allocation, because its slot counts each part. Another backing
    /// allocates one reference count at its first split, and each later split of its rest shares it.
    ///
    /// Gives the backing back, unchanged, when `len` is more than its initialized bytes.
    ///
    /// ```
    /// # extern crate rstypes as rsasync;
    /// use rsasync::buffer::{Backing, Buffer, Lease, WriteLoan};
    ///
    /// // A receive filled one record and the start of the next one.
    /// let mut loan = WriteLoan::new(Backing::from(Vec::with_capacity(64)));
    /// loan.fill_from(b"RECORD|NE");
    /// let (record, rest) = loan.into_inner().freeze_prefix(7).unwrap();
    /// assert_eq!(record.bytes(), b"RECORD|");
    ///
    /// // The next receive fills the rest of the same allocation while the record is shared.
    /// let mut loan = WriteLoan::new(rest);
    /// loan.fill_from(b"XT");
    /// let next = Lease::from(loan.into_inner());
    /// assert_eq!(next.bytes(), b"NEXT");
    /// assert_eq!(next.as_ptr(), record.as_ptr().wrapping_add(7));
    /// ```
    #[expect(
        unsafe_code,
        reason = "the rest of the region starts after the frozen prefix, and only the new backing uses it"
    )]
    pub fn freeze_prefix(mut self, len: usize) -> Result<(Lease, Self), Self> {
        let Some(rest) = self.initialized.checked_sub(len) else {
            return Err(self);
        };
        let (ptr, capacity) = (self.ptr, self.capacity);
        // The lease covers exactly the prefix, so no shared reference covers the rest.
        self.initialized = len;
        let shared = Shared::from(self);
        // SAFETY: `len` is at most the initialized bytes, which are at most the capacity, so the rest
        // starts in the region or at its end.
        let start = unsafe { ptr.add(len) };
        // SAFETY: the rest is in the allocation of `shared`, after the prefix that the lease covers.
        // The backing that owned it is now `shared`, which writes nothing, so only the new backing
        // uses the rest.
        let tail = unsafe { shared.tail(start, capacity - len, rest) };
        Ok((Lease::of(shared, ptr.as_ptr(), len), tail))
    }
}

/// The owner that the parts of a backing share after it becomes a lease or splits: the slot of a
/// pool backing, or the reference count of another backing. Each part holds one count of it. The
/// last count to drop returns the slot, or drops the backing, whose own hook releases the region.
#[derive(Clone, Debug)]
enum Shared {
    Slot(pool::SlotRef),
    Heap(Arc<Backing>),
}

impl From<Backing> for Shared {
    /// Takes the count that `backing` holds of a slot or of the `Arc` of a split. Another backing
    /// moves into a new `Arc`, the one allocation.
    #[expect(unsafe_code, reason = "the count moves out of a backing that does not drop")]
    fn from(backing: Backing) -> Self {
        // The count moves to the result, so the backing does not drop.
        let backing = ManuallyDrop::new(backing);
        // SAFETY: the backing does not drop, so the read hook is the one owner of its count.
        match unsafe { ptr::read(&raw const backing.release.0) } {
            Hook::Shared(shared) => shared,
            // The read hook of the caller is two plain values, so the backing keeps its hook.
            Hook::Caller { .. } => Self::Heap(Arc::new(ManuallyDrop::into_inner(backing))),
        }
    }
}

impl Shared {
    /// The address of the first byte of the owner.
    fn base(&self) -> *const u8 {
        match self {
            Self::Slot(slot) => slot.as_ptr(),
            Self::Heap(backing) => backing.as_ptr(),
        }
    }

    /// The size of the allocation that the owner keeps.
    fn allocation_size(&self) -> usize {
        match self {
            Self::Slot(slot) => slot.allocation_size(),
            Self::Heap(backing) => backing.allocation_size(),
        }
    }

    /// A backing of the `capacity` bytes at `ptr`, with the first `initialized` of them
    /// initialized, which holds one more count of this owner. Its drop gives the count back.
    ///
    /// # Safety
    ///
    /// The region is in the allocation of the owner, after each byte that a lease of the owner
    /// covers, and only the new backing uses it.
    #[expect(
        unsafe_code,
        reason = "the new backing holds one more count of the owner of its region"
    )]
    unsafe fn tail(&self, ptr: NonNull<u8>, capacity: usize, initialized: usize) -> Backing {
        let release = Release(Hook::Shared(self.clone()));
        // SAFETY: the count keeps the allocation alive, and the caller gives a region of it that
        // only the new backing uses. The count drops on any thread.
        unsafe { Backing::from_raw_parts(ptr, capacity, initialized, self.allocation_size(), release) }
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
    drop(unsafe { Vec::from_parts(ptr, 0, capacity) });
}

impl From<Vec<u8>> for Backing {
    #[expect(
        unsafe_code,
        reason = "the backing takes the heap block of a Vec that it does not drop"
    )]
    fn from(vec: Vec<u8>) -> Self {
        let (ptr, len, capacity) = vec.into_parts();
        // SAFETY: the `Vec` owned `capacity` bytes at `ptr`, and the first `len` of them are
        // initialized (the `Vec` documentation, "Guarantees"). `into_parts` gave them up, so only
        // the backing uses them. `release_vec` frees them with the pointer and the capacity of that
        // `Vec`, and the global allocator allows this on any thread.
        unsafe { Self::from_raw_parts(ptr, capacity, len, capacity, Release::new(ptr::null(), release_vec)) }
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

impl AsRef<[u8]> for Backing {
    /// The initialized bytes. With it, `bytes::Bytes::from_owner(backing)` makes a `Bytes` of the
    /// same bytes with no copy.
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

impl Drop for Backing {
    #[expect(unsafe_code, reason = "the drop is the one call of the release hook")]
    fn drop(&mut self) {
        // A shared count drops with the field, after this call.
        if let Hook::Caller { data, release } = self.release.0 {
            // SAFETY: `from_raw_parts` requires that a call of the hook with `ptr` and `capacity`
            // releases the region on any thread. This drop is the only call, and nothing uses the
            // region after it.
            unsafe { release(data, self.ptr, self.capacity) };
        }
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

/// An immutable owning lease over initialized bytes: all of the bytes of a backing, or a view of a
/// part of them.
///
/// A lease keeps its backing alive, also after the buffer, the first lease, and the operation that
/// made them drop. Its bytes do not change, and their address stays the same while any lease or
/// view of the backing lives. [`view`](Self::view) gives a lease of a sub-range with no copy, and
/// a clone gives one more lease of the same bytes. The last lease or view that drops releases the
/// backing through its [`Release`] hook, one time, on the thread that drops it.
///
/// A small view keeps the whole allocation: its [`allocation_size`](Buffer::allocation_size) is
/// that of the backing, not its own length. To free most of a large allocation, copy the small
/// part into a new buffer and drop the view.
///
/// A lease is `Send` and `Sync`, because each backing is.
///
/// ```
/// # extern crate rstypes as rsasync;
/// use rsasync::buffer::{Backing, Buffer, BufferMut, Lease};
///
/// // A received record, changed in place, as a decryption does.
/// let mut received = Vec::with_capacity(4096);
/// received.extend_from_slice(b"HEAD:BODY");
/// let mut record = Backing::from(received);
/// record.bytes_mut().make_ascii_lowercase();
///
/// // Freeze the record, then give its parts to two owners, with no copy.
/// let lease = Lease::from(record);
/// let head = lease.view(..4).unwrap();
/// let body = lease.view(5..).unwrap();
/// drop(lease);
/// assert_eq!(head.bytes(), b"head");
///
/// // The view of 4 bytes keeps the whole allocation of 4096 bytes.
/// assert!(body.allocation_size() >= 4096);
///
/// // A view can move to a different thread. The last view that drops releases the backing.
/// let owner = std::thread::spawn(move || body.bytes() == b"body");
/// assert!(owner.join().unwrap());
/// drop(head);
/// ```
#[derive(Clone, Debug)]
pub struct Lease {
    owner: Owner,
    /// The position of the first byte, from the first byte of the owner.
    offset: usize,
    len: usize,
}

/// What keeps the bytes of a lease alive.
#[derive(Clone, Debug)]
enum Owner {
    /// A backing or a pool slot, which the last lease, view, or tail releases or returns.
    Shared(Shared),
    /// An owner from [`Lease::from_owner`], which the last lease or view drops. Its count is a
    /// standard `Arc` in each form, because the loom `Arc` cannot hold a trait object. The count
    /// only drops the owner.
    Foreign(alloc::sync::Arc<dyn ForeignBytes>),
    /// A static region, which nothing releases.
    Static(&'static [u8]),
}

/// The bytes of an owner from [`Lease::from_owner`], without the type of the owner.
trait ForeignBytes: Send + Sync {
    /// The address that `as_ref` gave one time, when the owner was at its final address.
    fn as_ptr(&self) -> *const u8;

    /// The allocation size that the caller gave.
    fn allocation_size(&self) -> usize;
}

/// An owner from [`Lease::from_owner`], and the facts of its bytes.
struct Foreign<T> {
    /// Set one time, after the owner is in its `Arc`. It is atomic so that `from_owner` can set it
    /// through a shared reference: a `&mut` would assert unique access to the bytes of an owner
    /// that holds them inline.
    ptr: AtomicPtr<u8>,
    allocation_size: usize,
    owner: T,
}

impl<T: Send + Sync> ForeignBytes for Foreign<T> {
    fn as_ptr(&self) -> *const u8 {
        self.ptr.load(Ordering::Relaxed).cast_const()
    }

    fn allocation_size(&self) -> usize {
        self.allocation_size
    }
}

impl fmt::Debug for dyn ForeignBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Foreign")
            .field("allocation_size", &self.allocation_size())
            .finish_non_exhaustive()
    }
}

impl Lease {
    /// A lease of the bytes at `range` of this lease, for example `4..9` or `..n`, with no copy.
    /// It keeps the whole backing alive. Gives `None` if `range` is not in this lease.
    pub fn view<R: SliceIndex<[u8], Output = [u8]>>(&self, range: R) -> Option<Self> {
        let view = self.bytes().get(range)?;
        let offset = self.offset + (view.as_ptr().addr() - self.as_ptr().addr());
        Some(Self {
            owner: self.owner.clone(),
            offset,
            len: view.len(),
        })
    }

    /// Lends the bytes that `owner` gives through `AsRef<[u8]>`, with no copy, for example the bytes
    /// of a `String`, a `bytes::Bytes`, or a read-only memory map. The last lease or view that drops
    /// drops `owner` one time, on its thread.
    ///
    /// `allocation_size` is the size of the allocation that `owner` keeps, which the leases report.
    /// Give the true size: a smaller value makes a budget charge too little for the memory that the
    /// lease keeps. The length of the bytes is the minimum.
    ///
    /// This costs one allocation: the reference count, with `owner` in it.
    ///
    /// ```
    /// # extern crate rstypes as rsasync;
    /// use rsasync::buffer::{Buffer, Lease};
    ///
    /// let mut text = String::with_capacity(64);
    /// text.push_str("key=value");
    /// let (address, capacity) = (text.as_ptr(), text.capacity());
    ///
    /// let lease = Lease::from_owner(text, capacity);
    /// let value = lease.view(4..).unwrap();
    /// assert_eq!(value.bytes(), b"value");
    /// assert_eq!(value.as_ptr(), address.wrapping_add(4)); // no copy
    /// assert_eq!(value.allocation_size(), capacity);
    /// ```
    pub fn from_owner<T>(owner: T, allocation_size: usize) -> Self
    where
        T: AsRef<[u8]> + Send + Sync + 'static,
    {
        let foreign = alloc::sync::Arc::new(Foreign {
            ptr: AtomicPtr::new(ptr::null_mut()),
            allocation_size,
            owner,
        });
        // The owner is at its final address: the `Arc` does not move it, and only its last drop
        // borrows it mutably. Thus `as_ref` runs one time, here, and the lease keeps its address.
        let bytes = foreign.owner.as_ref();
        foreign
            .ptr
            .store(ptr::from_ref(bytes).cast::<u8>().cast_mut(), Ordering::Relaxed);
        Self {
            len: bytes.len(),
            owner: Owner::Foreign(foreign),
            offset: 0,
        }
    }
}

impl AsRef<[u8]> for Lease {
    /// The bytes of the lease. With it, `bytes::Bytes::from_owner(lease)` makes a `Bytes` of the
    /// same bytes with no copy.
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

impl From<Backing> for Lease {
    /// Freezes the initialized bytes of `backing`, with no copy. The capacity after them stays
    /// in the allocation until the release.
    ///
    /// This is the one allocation of a lease: its reference count. Views and clones add none. A
    /// backing from a [`Pool`] needs no allocation, because its slot counts the leases. The rest of
    /// a split backing needs none either: it shares the count of its first split.
    fn from(backing: Backing) -> Self {
        let (ptr, len) = (backing.as_ptr(), backing.initialized());
        Self::of(Shared::from(backing), ptr, len)
    }
}

impl Lease {
    /// A lease of the `len` bytes at `ptr`, in the region of `shared`.
    fn of(shared: Shared, ptr: *const u8, len: usize) -> Self {
        Self {
            offset: ptr.addr() - shared.base().addr(),
            owner: Owner::Shared(shared),
            len,
        }
    }
}

impl From<Vec<u8>> for Lease {
    /// Freezes the initialized bytes of `vec`, with no copy, through [`Backing::from`].
    fn from(vec: Vec<u8>) -> Self {
        Self::from(Backing::from(vec))
    }
}

impl From<&'static [u8]> for Lease {
    /// Lends a static region. It has no reference count and no release.
    fn from(region: &'static [u8]) -> Self {
        Self {
            owner: Owner::Static(region),
            offset: 0,
            len: region.len(),
        }
    }
}

#[expect(
    unsafe_code,
    reason = "a lease keeps its owner alive and gives only reads of its bytes"
)]
// SAFETY: the owner of a lease is a static region, a shared backing, a foreign owner, or a pool
// slot, and the lease keeps it alive. A shared backing is never borrowed mutably: no code takes
// `&mut` from its `Arc`, so its address, its initialized length, and its bytes stay the same until
// the last lease drops it. The rest of a split backing writes only after the initialized bytes of
// the shared backing, in the same allocation, and a lease covers bytes of that rest only after the
// rest froze them. A foreign owner gave its bytes through `as_ref` one time, at its final
// address in its `Arc`. After that, no code moves it, calls it, or borrows it mutably until its
// last drop. Two shared borrows of a type can overlap, so a sound type cannot change bytes that it
// gave through `&self` while it gives a shared borrow: only a `&mut` borrow or the drop can change
// them. A pool slot stays in use while its count is above zero, so its pool does not give it again,
// and its address in the arena of the pool is fixed. Its writers are the backing that became the
// lease and the rests of its splits, and each writes only bytes that no lease covers until it froze
// them. A writable rest can hold the last count of the slot, and then the slot returns only after
// the rest drops. So nothing writes the bytes that a lease covers while it lives, and a rest
// writes only its own bytes. Each lease covers `len` bytes from `offset`, in bytes of its owner
// that a backing froze: `From` and `Backing::freeze_prefix` take them, and `view` takes a
// sub-slice of them. A move of a lease copies the `Arc` pointer, the raw pointer of the pool and
// the index of the slot, or the shared reference, so the region does not move.
unsafe impl Buffer for Lease {
    fn as_ptr(&self) -> *const u8 {
        let base = match &self.owner {
            Owner::Shared(shared) => shared.base(),
            Owner::Foreign(foreign) => foreign.as_ptr(),
            Owner::Static(region) => region.as_ptr(),
        };
        base.wrapping_add(self.offset)
    }

    fn initialized(&self) -> usize {
        self.len
    }

    fn allocation_size(&self) -> usize {
        match &self.owner {
            Owner::Shared(shared) => shared.allocation_size(),
            Owner::Foreign(foreign) => foreign.allocation_size(),
            Owner::Static(_) => 0,
        }
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

    /// Lends the capacity after the filled bytes to `init` as uninitialized bytes, and records the
    /// bytes that `init` initialized. Gives the number of bytes that it recorded.
    ///
    /// `init` gives back the slice that it initialized, for example the result of
    /// [`write_copy_of_slice`](slice::write_copy_of_slice), or of a cipher that encrypts into
    /// uninitialized memory. The loan records the slice only when it starts at the first byte that
    /// `init` got. For each other slice, the loan records no byte and gives 0.
    ///
    /// ```
    /// # extern crate rstypes as rsasync;
    /// use rsasync::buffer::WriteLoan;
    ///
    /// let mut loan = WriteLoan::new(Vec::with_capacity(64));
    /// let count = loan.fill_with(|spare| spare[..4].write_copy_of_slice(b"DATA"));
    /// assert_eq!(count, 4);
    /// assert_eq!(loan.filled(), b"DATA");
    /// ```
    #[must_use = "a slice that is not at the start of the spare capacity fills nothing"]
    #[expect(
        unsafe_code,
        reason = "the loan lends its spare capacity, and records only an initialized slice at its start"
    )]
    pub fn fill_with(&mut self, init: impl FnOnce(&mut [MaybeUninit<u8>]) -> &mut [u8]) -> usize {
        let filled = self.filled().len();
        let len = self.capacity() - filled;
        // SAFETY: the loan is valid for reads and writes of `capacity` bytes, and while it holds
        // the buffer, no other code reaches them. The `len` bytes after the `filled` bytes are in
        // that region. `MaybeUninit` makes no claim about their values.
        let spare = unsafe { slice::from_raw_parts_mut(self.as_mut_ptr().add(filled).cast::<MaybeUninit<u8>>(), len) };
        let start = spare.as_ptr().cast::<u8>();
        let initialized = init(spare);
        // A `&mut [u8]` holds only initialized bytes. It is in the spare capacity only when it
        // starts at its first byte and is not longer: another region can start at its end.
        if initialized.as_ptr() != start || initialized.len() > len {
            return 0;
        }
        let count = initialized.len();
        // SAFETY: the first `filled` bytes of the loan were initialized before, and `init`
        // initialized the next `count` bytes. `filled + count` is at most `capacity`.
        unsafe { self.set_filled(filled + count) };
        count
    }

    /// Copies the most bytes of `bytes` that fit after the filled bytes, and records them. Gives
    /// the number of bytes that it copied.
    ///
    /// A driver uses it to give data that it holds already, with no call to the backend.
    pub fn fill_from(&mut self, bytes: &[u8]) -> usize {
        self.fill_with(|spare| {
            let count = bytes.len().min(spare.len());
            spare[..count].write_copy_of_slice(&bytes[..count])
        })
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

mod pool;

pub use pool::{Pool, PoolEmpty, PoolRefused};

#[cfg(test)]
mod tests;

#[cfg(all(test, loom))]
mod explore;
