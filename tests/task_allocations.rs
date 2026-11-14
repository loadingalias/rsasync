//! A refused spawn allocates nothing: the spawn claims its slot and its charge before it
//! allocates, and a refusal gives the future back. A spawn that boxes the future before it claims
//! its slot fails this test. It is a test binary of its own, because a binary has one global
//! allocator, and the unit tests of the crate have theirs.
#![cfg(feature = "std")]

use core::alloc::{GlobalAlloc, Layout};
use core::cell::Cell;
use core::future::pending;
use core::num::NonZeroU64;
use std::alloc::System;

use rsasync::capacity::Budget;
use rsasync::task::{LocalOwner, Reason, spawn_local};

/// Counts the allocations of each thread.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

#[expect(
    unsafe_code,
    reason = "the test allocator counts each allocation and forwards it to the system allocator"
)]
// SAFETY: each method forwards its arguments to the system allocator, which meets the contract.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
        // SAFETY: the caller meets the contract of `alloc`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller meets the contract of `dealloc`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

#[test]
fn a_refused_spawn_allocates_nothing() {
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let mut owner = LocalOwner::new(&budget, 2).unwrap();
    owner.block_on(async {
        let _first = spawn_local(pending::<()>()).unwrap();

        // Too few free units for the task.
        let rest = budget.reserve(budget.available()).unwrap();
        let before = ALLOCATIONS.get();
        let refused = spawn_local(pending::<()>()).unwrap_err();
        assert_eq!(ALLOCATIONS.get(), before);
        assert!(matches!(refused.reason(), Reason::Budget(_)));
        drop(rest);

        // No free slot.
        let _second = spawn_local(pending::<()>()).unwrap();
        let before = ALLOCATIONS.get();
        let refused = spawn_local(pending::<()>()).unwrap_err();
        assert_eq!(ALLOCATIONS.get(), before);
        assert_eq!(refused.reason(), Reason::NoSlot);
    });
}
