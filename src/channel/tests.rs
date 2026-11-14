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
pub(super) fn counted() -> (Arc<AtomicU32>, impl Fn(u32) -> Counted + Clone) {
    let drops = Arc::new(AtomicU32::new(0));
    let make = {
        let drops = Arc::clone(&drops);
        move |id| Counted(id, Arc::clone(&drops))
    };
    (drops, make)
}
