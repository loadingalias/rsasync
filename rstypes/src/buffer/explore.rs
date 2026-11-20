//! The named risk: the parts of a split backing that drop on two threads release its allocation
//! twice, or while the other part still reads it.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::alloc::Layout;
use core::num::{NonZeroU32, NonZeroU64};
use core::ptr::NonNull;

use super::{Backing, Buffer, Lease, Pool, PoolEmpty, Release, WriteLoan};
use crate::capacity::Budget;
use crate::sync::Arc;
use crate::sync::atomic::{AtomicUsize, Ordering};

/// Checks `f` in each thread order within a preemption bound of 3.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

/// The release hook of a counted backing: counts the call, then frees the region.
///
/// # Safety
///
/// `data` is from `Arc::into_raw` of the counter, `ptr` and `len` are from the boxed slice of
/// `counted`, and nothing uses them after this call.
#[expect(
    unsafe_code,
    reason = "the hook takes back the counter and the region that `counted` gave"
)]
unsafe fn release_counted(data: *const (), ptr: NonNull<u8>, len: usize) {
    // SAFETY: `counted` made `data` with `Arc::into_raw`, and this call is its only use.
    let releases = unsafe { Arc::from_raw(data.cast::<AtomicUsize>()) };
    releases.fetch_add(1, Ordering::Relaxed);
    // SAFETY: `counted` leaked this boxed slice of `len` bytes.
    drop(unsafe { Box::from_raw(core::ptr::slice_from_raw_parts_mut(ptr.as_ptr(), len)) });
}

/// A backing of `bytes` whose release hook counts its calls in `releases`.
#[expect(
    unsafe_code,
    reason = "the test maps a boxed slice into a backing with a counting hook"
)]
fn counted(releases: &Arc<AtomicUsize>, bytes: &[u8]) -> Backing {
    let len = bytes.len();
    let region = NonNull::new(Box::into_raw(Vec::from(bytes).into_boxed_slice()).cast::<u8>()).unwrap();
    let data = Arc::into_raw(Arc::clone(releases)).cast::<()>();
    // SAFETY: the boxed slice is valid for reads and writes of `len` initialized bytes, only the
    // backing uses it, and `release_counted` frees it on any thread.
    unsafe { Backing::from_raw_parts(region, len, len, len, Release::new(data, release_counted)) }
}

#[test]
fn the_last_part_of_a_split_backing_releases_it_once_on_either_thread() {
    model(|| {
        let releases = Arc::new(AtomicUsize::new(0));
        let (head, rest) = counted(&releases, b"HEADBODY").freeze_prefix(4).unwrap();
        let body = Lease::from(rest);
        let drops_head = loom::thread::spawn(move || drop(head));
        assert_eq!(body.bytes(), b"BODY");
        drop(body);
        drops_head.join().unwrap();
        assert_eq!(releases.load(Ordering::Relaxed), 1);
    });
}

#[test]
fn the_last_part_of_a_split_slot_returns_it_once_on_either_thread() {
    model(|| {
        let budget = Budget::new(NonZeroU64::new(64).unwrap());
        let mut pool = Pool::new(
            &budget,
            NonZeroU32::new(1).unwrap(),
            Layout::from_size_align(8, 8).unwrap(),
        )
        .unwrap();
        let mut loan = WriteLoan::new(pool.acquire().unwrap());
        loan.fill_from(b"HEADBODY");
        let (head, rest) = loan.into_inner().freeze_prefix(4).unwrap();
        let drops_rest = loom::thread::spawn(move || drop(rest));
        assert_eq!(head.bytes(), b"HEAD");
        drop(head);
        drops_rest.join().unwrap();

        let slot = pool.acquire().unwrap();
        assert_eq!(pool.acquire().unwrap_err(), PoolEmpty);
        drop(slot);
    });
}
