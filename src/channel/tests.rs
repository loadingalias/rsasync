//! Helpers of the channel tests. They drive the code under test, so they use the native atomics.

use alloc::sync::Arc;
use core::future::Future;
use core::num::NonZeroU64;
use core::pin::Pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::{Context, Poll, Waker};

use crate::capacity::Budget;

pub(super) const BOUND: u64 = 1 << 20;

pub(super) fn budget() -> Budget {
    Budget::new(NonZeroU64::new(BOUND).unwrap())
}

pub(super) fn poll<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(waker))
}

/// A value that counts the drops of each value with the same counter.
#[derive(Debug)]
pub(super) struct Counted(pub(super) u32, pub(super) Arc<AtomicU32>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

/// The drop counter, and a function that makes counted values.
pub(super) fn counted() -> (Arc<AtomicU32>, impl Fn(u32) -> Counted) {
    let drops = Arc::new(AtomicU32::new(0));
    let make = {
        let drops = Arc::clone(&drops);
        move |id| Counted(id, Arc::clone(&drops))
    };
    (drops, make)
}

/// Gives the producer and the number of each value that a test sends.
#[cfg(feature = "std")]
pub(super) fn tag(producer: u32, number: u32) -> u32 {
    (producer << 16) | number
}

/// Checks that the values of each producer are in the order of their numbers.
#[cfg(feature = "std")]
pub(super) fn in_producer_order(values: &[u32]) -> bool {
    let mut last = std::collections::HashMap::new();
    values.iter().all(|value| {
        let (producer, number) = (value >> 16, value & 0xffff);
        last.insert(producer, number).is_none_or(|previous| previous < number)
    })
}

/// The charge of a channel equals the bytes that its construction allocates. A charge that omits
/// the two counts of the `Arc` and their padding fails.
#[cfg(feature = "std")]
#[test]
fn each_channel_charges_each_byte_that_it_allocates() {
    use crate::channel::{mpmc, mpsc, oneshot, spsc};
    use crate::table::tests::allocations::bytes;

    /// Makes a channel with `make`, and gives its charge and the bytes that `make` allocated.
    fn measure<R>(budget: &Budget, make: impl FnOnce(&Budget) -> R) -> (u64, u64) {
        let before = bytes();
        let endpoints = make(budget);
        let allocated = bytes() - before;
        let charge = BOUND - budget.available();
        drop(endpoints);
        (charge, allocated)
    }

    let budget = budget();
    let (charge, allocated) = measure(&budget, |budget| spsc::channel::<u64>(budget, 8).unwrap());
    assert_eq!(charge, allocated, "spsc");
    let (charge, allocated) = measure(&budget, |budget| mpsc::channel::<u64>(budget, 8, 3).unwrap());
    assert_eq!(charge, allocated, "mpsc");
    let (charge, allocated) = measure(&budget, |budget| mpmc::channel::<u64>(budget, 8, 3, 2).unwrap());
    assert_eq!(charge, allocated, "mpmc");
    let (charge, allocated) = measure(&budget, |budget| oneshot::channel::<u64>(budget).unwrap());
    assert_eq!(charge, allocated, "oneshot");
}

/// Each refusal of the allocator gives `Allocation`, and keeps no charge and no memory. A channel
/// whose last allocation aborts the process fails.
#[cfg(feature = "std")]
#[test]
fn a_channel_that_the_allocator_refuses_keeps_no_charge_and_no_memory() {
    use crate::channel::{Refused, mpmc, mpsc, oneshot, spsc};
    use crate::table::tests::allocations::each_refusal;

    let budget = budget();
    let refusals = [
        each_refusal(|| spsc::channel::<u64>(&budget, 8).map(drop)),
        each_refusal(|| mpsc::channel::<u64>(&budget, 8, 3).map(drop)),
        each_refusal(|| mpmc::channel::<u64>(&budget, 8, 3, 2).map(drop)),
        each_refusal(|| oneshot::channel::<u64>(&budget).map(drop)),
    ];
    for refusals in refusals {
        assert!(!refusals.is_empty());
        assert!(refusals.iter().all(|refusal| *refusal == (Refused::Allocation, 0)));
    }
    assert_eq!(budget.available(), BOUND);
}
