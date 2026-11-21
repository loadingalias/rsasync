//! A value that its owner sets one time and detaches one time, which other threads read in
//! between: the ring of the home of an owner, which each wake from another thread reads.
//!
//! - The set publishes the value with `Release`, and a read loads it with `Acquire`. So a read that
//!   finds the value also sees each write that made it, also when the read has no other order with
//!   the set.
//! - One word, `state`, holds the detach bit and the count of the reads that run. A read adds itself
//!   to the count, and the detach sets the bit, each with one read-modify-write of that word, so
//!   the order of the word has one of them first. A read that came first is in the count, and the
//!   detach waits until the count is zero. A read that came later finds the bit, and it does not
//!   load the value. The last read releases its count with `Release`, and the detach loads the
//!   count with `Acquire`, so each read ends before the detach frees the value.
//!
//! So a kept waker of an owner that ended holds no value of the cell: the value, such as the
//! descriptors of a ring, closes at the detach.

use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::ptr;

use crate::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
use crate::sync::{Arc, hint};

/// In `state`: the owner detached the cell. The other bits count the reads that run.
const DETACHED: u32 = 1 << 31;

/// What a read found.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Read<R> {
    /// The owner set no value yet.
    Empty,
    /// The value, given to the closure, which gave this.
    Value(R),
    /// The owner detached the cell: nothing reads its value again.
    Detached,
}

pub(super) struct Late<T> {
    /// The value, from `Arc::into_raw`, or null.
    value: AtomicPtr<T>,
    /// The detach bit, and the count of the reads that run.
    state: AtomicU32,
    /// The cell owns one count of an `Arc<T>`, and each thread that reads gets `&T`: it is `Send`
    /// and `Sync` as that `Arc` is.
    _value: PhantomData<Arc<T>>,
}

impl<T> Late<T> {
    /// A cell with `value`. It keeps the count, so it allocates nothing.
    pub(super) fn new(value: Option<Arc<T>>) -> Self {
        Self {
            value: AtomicPtr::new(value.map_or(ptr::null_mut(), |value| Arc::into_raw(value).cast_mut())),
            state: AtomicU32::new(0),
            _value: PhantomData,
        }
    }

    /// Sets the value. Only the owner sets it, one time, before the detach.
    pub(super) fn set(&self, value: Arc<T>) {
        // Ordering: `Release` publishes the value to each read that loads it.
        let before = self.value.swap(Arc::into_raw(value).cast_mut(), Ordering::Release);
        assert!(before.is_null(), "an owner sets its value one time");
    }

    /// Calls `f` with the value, if the owner set it and did not detach the cell. `f` can clone the
    /// `Arc` to keep the value after the read.
    #[expect(unsafe_code, reason = "the count of reads keeps the value until the read ends")]
    pub(super) fn with<R>(&self, f: impl FnOnce(&Arc<T>) -> R) -> Read<R> {
        // Ordering: `Relaxed`: the order of the word alone puts this read before or after the
        // detach. The load of the value below reads the release of `set`.
        let before = self.state.fetch_add(1, Ordering::Relaxed);
        assert!(before & !DETACHED != !DETACHED, "fewer reads run than the count holds");
        let read = if before & DETACHED != 0 {
            Read::Detached
        } else {
            // Ordering: `Acquire` reads the release of `set`.
            let value = self.value.load(Ordering::Acquire);
            if value.is_null() {
                Read::Empty
            } else {
                // SAFETY: the pointer came from `Arc::into_raw`, and the cell holds its count. This
                // read is in the count before the detach, so the detach drops that count only after
                // it ends. The `ManuallyDrop` lends that count, and it does not drop it.
                let value = ManuallyDrop::new(unsafe { Arc::from_raw(value) });
                Read::Value(f(&value))
            }
        };
        // Ordering: `Release` orders this read before the free of the detach, which loads the count
        // with `Acquire`.
        self.state.fetch_sub(1, Ordering::Release);
        read
    }

    /// Detaches the cell: each later read does nothing. Waits until each read that runs now ends,
    /// and then frees the value. Only the owner detaches, one time. Each such read runs a few
    /// steps, so the wait is short.
    pub(super) fn detach(&self) {
        // Ordering: `Relaxed`: the order of the word puts each read before or after this change.
        let before = self.state.fetch_or(DETACHED, Ordering::Relaxed);
        assert!(before & DETACHED == 0, "an owner detaches its value one time");
        // Ordering: `Acquire` reads the release of the last read that ends.
        while self.state.load(Ordering::Acquire) != DETACHED {
            hint::spin_loop();
        }
        // Ordering: `Relaxed`: each read that could load the value ended before the load above.
        free(self.value.swap(ptr::null_mut(), Ordering::Relaxed));
    }
}

/// Drops the count of a value of `Late`, if there is one.
#[expect(unsafe_code, reason = "the pointer owns the count of `Arc::into_raw`")]
fn free<T>(value: *mut T) {
    if !value.is_null() {
        // SAFETY: the pointer came from `Arc::into_raw`, and no read and no other free can use it:
        // the caller took it out of the cell.
        drop(unsafe { Arc::from_raw(value) });
    }
}

impl<T> Drop for Late<T> {
    fn drop(&mut self) {
        // Ordering: `Relaxed`: the drop has the cell alone.
        free(self.value.swap(ptr::null_mut(), Ordering::Relaxed));
    }
}

#[cfg(all(test, loom))]
mod explore;
