extern crate std;

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::alloc::Layout;
use core::hint::black_box;
use core::ptr::{self, NonNull};
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread::{self, ThreadId};

use super::{Backing, Buffer, BufferMut, Release, WriteLoan};

/// One page, aligned to the page: memory that a library allocates on its own.
#[repr(C, align(4096))]
struct Page([u8; 4096]);
const PAGE: Layout = Layout::new::<Page>();

/// What the release hook of a page backing saw.
#[derive(Default)]
struct Releases {
    count: AtomicUsize,
    thread: Mutex<Option<ThreadId>>,
}

/// The release hook of a page backing: records the call, then frees the page.
///
/// # Safety
///
/// `data` is from `Arc::into_raw` of a `Releases`, `ptr` is from `alloc(PAGE)`, and nothing uses
/// them after this call.
#[expect(
    unsafe_code,
    reason = "the hook takes back the record and the page that `page` gave the backing"
)]
unsafe fn release_page(data: *const (), ptr: NonNull<u8>, _: usize) {
    // SAFETY: `page` made `data` with `Arc::into_raw`, and this call is its only use.
    let releases = unsafe { Arc::from_raw(data.cast::<Releases>()) };
    releases.count.fetch_add(1, Ordering::Relaxed);
    *releases.thread.lock().unwrap() = Some(thread::current().id());
    // SAFETY: `page` allocated `ptr` with `PAGE`, and nothing uses it after this call.
    unsafe { std::alloc::dealloc(ptr.as_ptr(), PAGE) };
}

/// A custom backing: 1000 writable bytes, not initialized, in one page.
#[expect(
    unsafe_code,
    reason = "the test maps its own memory into a backing, as a library does"
)]
fn page(releases: &Arc<Releases>) -> Backing {
    // SAFETY: the size of `PAGE` is not zero.
    let ptr = NonNull::new(unsafe { std::alloc::alloc(PAGE) }).unwrap();
    let data = Arc::into_raw(Arc::clone(releases)).cast::<()>();
    // SAFETY: the page is valid for reads and writes of 4096 bytes, and only the backing uses it.
    // `release_page` frees it, and it can run on any thread.
    unsafe { Backing::from_raw_parts(ptr, 1000, 0, PAGE.size(), Release::new(data, release_page)) }
}

/// Does the work of a backend: writes `bytes` at the address of the loan, then records the fill.
#[expect(unsafe_code, reason = "a backend writes through the address of the loan")]
fn backend_fill<B: BufferMut>(loan: &mut WriteLoan<B>, bytes: &[u8]) {
    assert!(bytes.len() <= loan.capacity());
    // SAFETY: the loan is valid for writes of `capacity` bytes, and the copy initializes the first
    // `bytes.len()` of them.
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), loan.as_mut_ptr(), bytes.len());
        loan.set_filled(bytes.len());
    }
}

/// The address, the initialized length, and the capacity, through the buffer contract.
fn facts<B: BufferMut>(buffer: &B) -> (*const u8, usize, usize) {
    (buffer.as_ptr(), buffer.initialized(), buffer.capacity())
}

#[test]
fn the_address_and_the_length_stay_the_same_after_moves() {
    let mut vec = Vec::with_capacity(64);
    vec.extend_from_slice(b"head");
    // The expected facts come from `Vec` itself.
    let (address, capacity) = (vec.as_ptr(), vec.capacity());

    let vec = black_box(Box::new(vec));
    assert_eq!(facts(&*vec), (address, 4, capacity));

    // Inside a moved owner: a loan, then a backing.
    let mut loan = black_box(Box::new(WriteLoan::new(*vec)));
    assert_eq!(loan.as_mut_ptr().cast_const(), address.wrapping_add(4));
    let backing = black_box(Box::new(Backing::from(loan.into_inner())));
    assert_eq!(facts(&*backing), (address, 4, capacity));

    let boxed = Box::<[u8]>::from(&b"boxed"[..]);
    let address = boxed.as_ptr();
    let backing = black_box(Box::new(Backing::from(boxed)));
    assert_eq!(facts(&*backing), (address, 5, 5));

    let region: &'static [u8] = b"static";
    let moved = black_box(Box::new(region));
    assert_eq!((Buffer::as_ptr(&*moved), moved.initialized()), (region.as_ptr(), 6));
}

#[test]
fn a_loan_exposes_exactly_its_filled_extent() {
    let mut vec = Vec::with_capacity(64);
    vec.extend_from_slice(b"head");
    let capacity = vec.capacity();

    // A short fill: 4 bytes of the capacity after the initialized bytes.
    let mut loan = WriteLoan::new(vec);
    assert_eq!(loan.capacity(), capacity - 4);
    backend_fill(&mut loan, b"tail");
    assert_eq!(loan.filled(), b"tail");
    let mut vec = loan.into_inner();
    assert_eq!(vec, b"headtail");

    // A receive that fails fills nothing: the buffer keeps only its earlier bytes.
    vec.truncate(4);
    assert_eq!(WriteLoan::new(vec).into_inner(), b"head");

    // The capacity of a custom backing is not initialized: only the fill becomes readable.
    let releases = Arc::default();
    let mut loan = WriteLoan::new(page(&releases));
    backend_fill(&mut loan, b"short");
    assert_eq!(loan.filled(), b"short");
    assert_eq!(loan.into_inner().bytes(), b"short");
}

#[test]
fn a_copy_into_a_loan_stops_at_its_capacity() {
    let mut loan = WriteLoan::new(Vec::<u8>::with_capacity(4));
    let capacity = loan.capacity();
    let bytes: Vec<u8> = (0..=u8::MAX).collect();

    assert_eq!(loan.fill_from(&bytes[..1]), 1);
    assert_eq!(loan.fill_from(&bytes[1..]), capacity - 1);
    assert_eq!(loan.fill_from(&bytes), 0);
    assert_eq!(loan.into_inner(), bytes[..capacity]);
}

#[test]
fn the_allocation_size_and_the_alignment_are_separate_from_the_capacity() {
    let releases = Arc::default();
    let backing = page(&releases);

    assert_eq!(backing.capacity(), 1000);
    assert_eq!(backing.allocation_size(), PAGE.size());
    assert!(backing.alignment().is_power_of_two());
    assert!(backing.alignment() >= PAGE.align());
}

// The WebAssembly targets of the tests have no threads.
#[cfg(not(target_family = "wasm"))]
#[test]
fn the_release_hook_runs_once_on_the_thread_of_the_last_drop() {
    let releases = Arc::<Releases>::default();
    let loan = WriteLoan::new(page(&releases));

    let dropper = thread::spawn(move || {
        let backing = loan.into_inner();
        drop(backing);
        thread::current().id()
    })
    .join()
    .unwrap();

    assert_eq!(releases.count.load(Ordering::Relaxed), 1);
    assert_eq!(*releases.thread.lock().unwrap(), Some(dropper));
}
