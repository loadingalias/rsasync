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

use super::{Backing, Buffer, BufferMut, Lease, Release, WriteLoan};

/// One page, aligned to the page: memory that a library allocates on its own.
#[repr(C, align(4096))]
struct Page([u8; 4096]);
const PAGE: Layout = Layout::new::<Page>();

/// The address of a page backing, as the allocator gave it, and what its release hook saw.
#[derive(Default)]
struct Releases {
    address: AtomicUsize,
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
    releases.address.store(ptr.addr().get(), Ordering::Relaxed);
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

#[test]
fn truncate_shortens_the_initialized_bytes_and_never_lengthens_them() {
    let releases = Arc::default();
    let mut loan = WriteLoan::new(page(&releases));
    backend_fill(&mut loan, b"first");
    let mut backing = loan.into_inner();

    // A longer length would expose capacity that is not initialized.
    backing.truncate(backing.capacity());
    assert_eq!(backing.bytes(), b"first");

    // After `truncate(0)`, a second receive fills the full capacity again.
    backing.truncate(0);
    let mut loan = WriteLoan::new(backing);
    assert_eq!(loan.capacity(), 1000);
    backend_fill(&mut loan, b"second");
    assert_eq!(loan.into_inner().bytes(), b"second");
}

#[test]
fn a_view_keeps_the_backing_address_and_reports_the_allocation_size_of_its_parent() {
    let mut vec = Vec::with_capacity(64);
    vec.extend_from_slice(b"header payload");
    let (address, capacity) = (vec.as_ptr(), vec.capacity());

    let lease = Lease::from(vec);
    let view = lease.view(7..).unwrap();
    assert_eq!(
        (view.as_ptr(), view.bytes()),
        (address.wrapping_add(7), &b"payload"[..])
    );
    // A view of a view keeps the same backing.
    assert_eq!(view.view(1..4).unwrap().as_ptr(), address.wrapping_add(8));
    // The view keeps the whole allocation, so it reports the allocation of its parent.
    assert_eq!(view.allocation_size(), capacity);

    let region: &'static [u8] = b"static";
    let view = Lease::from(region).view(1..).unwrap();
    assert_eq!(
        (view.as_ptr(), view.allocation_size()),
        (region.as_ptr().wrapping_add(1), 0)
    );
}

#[test]
fn a_view_keeps_its_backing_alive_after_the_first_lease_drops() {
    let releases = Arc::<Releases>::default();
    let mut loan = WriteLoan::new(page(&releases));
    backend_fill(&mut loan, b"received");
    let lease = Lease::from(loan.into_inner());
    // The request of an operation keeps one more lease of the same bytes.
    let request = lease.clone();
    let view = lease.view(2..).unwrap();

    drop(lease);
    drop(request);
    assert_eq!(releases.count.load(Ordering::Relaxed), 0);
    assert_eq!(view.bytes(), b"ceived");

    drop(view);
    assert_eq!(releases.count.load(Ordering::Relaxed), 1);
}

#[test]
fn a_view_outside_its_lease_is_refused() {
    let lease = Lease::from(b"bytes".to_vec());
    let view = lease.view(1..4).unwrap();

    // `2..4` is in the parent, but not in the view of 3 bytes.
    assert!(view.view(2..4).is_none());
    assert_eq!(view.view(1..3).unwrap().bytes(), b"te");
}

// The WebAssembly targets of the tests have no threads.
#[cfg(not(target_family = "wasm"))]
#[test]
fn a_region_changed_in_place_splits_into_views_that_release_it_once_on_the_last_thread() {
    let releases = Arc::<Releases>::default();
    let mut loan = WriteLoan::new(page(&releases));
    backend_fill(&mut loan, b"HEADBODY");
    let mut backing = loan.into_inner();
    // A change in place, as a decryption makes it.
    backing.bytes_mut().make_ascii_lowercase();

    let lease = Lease::from(backing);
    let (head, body) = (lease.view(..4).unwrap(), lease.view(4..).unwrap());
    drop(lease);
    let address = releases.address.load(Ordering::Relaxed);
    assert_eq!((head.as_ptr().addr(), body.as_ptr().addr()), (address, address + 4));

    // Two owners keep the views on two threads.
    let head_owner = thread::spawn(move || assert_eq!(head.bytes(), b"head"));
    let body = thread::spawn(move || {
        assert_eq!(body.bytes(), b"body");
        body
    })
    .join()
    .unwrap();
    head_owner.join().unwrap();
    assert_eq!(releases.count.load(Ordering::Relaxed), 0);

    // The last view drops on a third thread, which releases the backing.
    let last = thread::spawn(move || {
        drop(body);
        thread::current().id()
    })
    .join()
    .unwrap();
    assert_eq!(releases.count.load(Ordering::Relaxed), 1);
    assert_eq!(*releases.thread.lock().unwrap(), Some(last));
}

/// An owner of immutable bytes that records its drop, as a `bytes::Bytes` or a memory map owns
/// its bytes.
struct Recorded {
    bytes: Box<[u8]>,
    releases: Arc<Releases>,
}

impl AsRef<[u8]> for Recorded {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Recorded {
    fn drop(&mut self) {
        self.releases.count.fetch_add(1, Ordering::Relaxed);
        *self.releases.thread.lock().unwrap() = Some(thread::current().id());
    }
}

#[test]
fn a_lease_from_an_owner_keeps_its_address_and_reports_the_given_allocation_size() {
    let releases = Arc::default();
    let bytes = Box::<[u8]>::from(&b"key=value"[..]);
    let address = bytes.as_ptr();

    let lease = Lease::from_owner(Recorded { bytes, releases }, 4096);
    let value = lease.view(4..).unwrap();
    assert_eq!(
        (value.as_ptr(), value.bytes()),
        (address.wrapping_add(4), &b"value"[..])
    );
    assert_eq!(value.allocation_size(), 4096);
}

#[test]
fn an_owner_with_inline_bytes_keeps_them_after_the_lease_moves() {
    // The bytes of an array are in the owner itself, so they move when the owner moves.
    let lease = Lease::from_owner(*b"inline", 6);
    let view = black_box(Box::new(lease.view(2..).unwrap()));
    let lease = black_box(Box::new(lease));

    assert_eq!(lease.bytes(), b"inline");
    assert_eq!(view.bytes(), b"line");
    assert_eq!(view.as_ptr(), lease.as_ptr().wrapping_add(2));
}

// The WebAssembly targets of the tests have no threads.
#[cfg(not(target_family = "wasm"))]
#[test]
fn the_owner_drops_once_at_the_last_lease_or_view_on_its_thread() {
    let releases = Arc::<Releases>::default();
    let owner = Recorded {
        bytes: Box::from(&b"owned"[..]),
        releases: Arc::clone(&releases),
    };
    let lease = Lease::from_owner(owner, 5);
    let (clone, view) = (lease.clone(), lease.view(1..).unwrap());
    drop(lease);
    drop(clone);
    assert_eq!(releases.count.load(Ordering::Relaxed), 0);

    let last = thread::spawn(move || {
        assert_eq!(view.bytes(), b"wned");
        drop(view);
        thread::current().id()
    })
    .join()
    .unwrap();
    assert_eq!(releases.count.load(Ordering::Relaxed), 1);
    assert_eq!(*releases.thread.lock().unwrap(), Some(last));
}

#[test]
fn a_frozen_prefix_keeps_its_address_and_bytes_while_the_rest_receives() {
    let mut loan = WriteLoan::new(Backing::from(Vec::with_capacity(64)));
    backend_fill(&mut loan, b"RECORD|NE");
    let backing = loan.into_inner();
    let (address, capacity) = (backing.as_ptr(), backing.capacity());
    let (record, rest) = backing.freeze_prefix(7).unwrap();
    assert_eq!((record.as_ptr(), record.bytes()), (address, &b"RECORD|"[..]));
    assert_eq!(facts(&rest), (address.wrapping_add(7), 2, capacity - 7));
    assert_eq!(rest.allocation_size(), record.allocation_size());

    // The next receive fills the rest while the record is shared.
    let mut loan = WriteLoan::new(rest);
    backend_fill(&mut loan, b"XT");
    let next = Lease::from(loan.into_inner());
    assert_eq!((record.as_ptr(), record.bytes()), (address, &b"RECORD|"[..]));
    assert_eq!((next.as_ptr(), next.bytes()), (address.wrapping_add(7), &b"NEXT"[..]));
}

// The WebAssembly targets of the tests have no threads.
#[cfg(not(target_family = "wasm"))]
#[test]
fn a_split_backing_is_released_once_after_its_last_part_in_each_order() {
    // The parts: the first lease, a view of it, the lease of a second split, and its rest.
    for order in [[0, 1, 2, 3], [3, 2, 1, 0], [1, 3, 0, 2]] {
        let releases = Arc::new(Releases::default());
        let mut loan = WriteLoan::new(page(&releases));
        backend_fill(&mut loan, b"HEADBODYTAIL");
        let (head, rest) = loan.into_inner().freeze_prefix(4).unwrap();
        let view = head.view(1..).unwrap();
        let (body, tail) = rest.freeze_prefix(4).unwrap();
        assert_eq!((view.bytes(), body.bytes()), (&b"EAD"[..], &b"BODY"[..]));
        let mut parts: [Option<Box<dyn Send>>; 4] = [
            Some(Box::new(head)),
            Some(Box::new(view)),
            Some(Box::new(body)),
            Some(Box::new(tail)),
        ];
        for &part in &order[..3] {
            drop(parts[part].take());
            assert_eq!(releases.count.load(Ordering::Relaxed), 0, "{order:?}");
        }
        let last = parts[order[3]].take();
        let thread = thread::spawn(move || drop(last));
        let id = thread.thread().id();
        thread.join().unwrap();
        assert_eq!(releases.count.load(Ordering::Relaxed), 1, "{order:?}");
        assert_eq!(*releases.thread.lock().unwrap(), Some(id));
    }
}

#[test]
fn a_prefix_longer_than_the_initialized_bytes_gives_the_backing_back_unchanged() {
    let backing = Backing::from(b"ABC".to_vec());
    let before = facts(&backing);
    let back = backing.freeze_prefix(4).unwrap_err();
    assert_eq!(facts(&back), before);
}
