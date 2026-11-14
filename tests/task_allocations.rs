//! A refused spawn allocates nothing: the spawn claims its slot and its charge before it
//! allocates, and a refusal gives the future back. A spawn that boxes the future before it claims
//! its slot fails this test. A wake from another thread allocates nothing: the remote-ready list
//! has one entry for each slot. A wake path that pushes a new node for each wake fails this test.
//! A refusal and its report complete under an allocator that refuses each request.
//! It is a test binary of its own, because a binary has one global allocator, and the unit tests
//! of the crate have theirs.
#![cfg(feature = "std")]

use core::alloc::{GlobalAlloc, Layout};
use core::cell::Cell;
use core::fmt::{self, Write as _};
use core::future::{pending, poll_fn};
use core::num::NonZeroU64;
use core::task::Poll;
use std::alloc::System;
use std::thread;

use rsasync::capacity::Budget;
use rsasync::task::{LocalOwner, Outcome, Reason, Refused, spawn_local};

/// Counts the allocations of each thread, and refuses each one while `REFUSE` is set.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static REFUSE: Cell<bool> = const { Cell::new(false) };
}

#[expect(
    unsafe_code,
    reason = "the test allocator counts each allocation and forwards it to the system allocator"
)]
// SAFETY: each method forwards its arguments to the system allocator, which meets the contract.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
        if REFUSE.get() {
            return core::ptr::null_mut();
        }
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

#[test]
fn a_wake_from_another_thread_allocates_nothing() {
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    owner.block_on(async {
        let mut other = None;
        // The task gives its waker to a new thread at its first poll. At its second poll, after
        // the wake, it gives the allocations of that thread during the wake.
        let task = spawn_local(poll_fn(move |cx| {
            if let Some(other) = other.take() {
                return Poll::Ready(thread::JoinHandle::join(other).unwrap());
            }
            let waker = cx.waker().clone();
            other = Some(thread::spawn(move || {
                let before = ALLOCATIONS.get();
                waker.wake_by_ref();
                ALLOCATIONS.get() - before
            }));
            Poll::Pending
        }))
        .unwrap();
        assert!(matches!(task.join().await, Outcome::Completed(0)));
    });
}

/// Text in a fixed buffer, so that a report needs no allocation.
struct Text {
    bytes: [u8; 512],
    len: usize,
}

impl fmt::Write for Text {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let end = self
            .len
            .checked_add(text.len())
            .filter(|end| *end <= self.bytes.len())
            .ok_or(fmt::Error)?;
        self.bytes[self.len..end].copy_from_slice(text.as_bytes());
        self.len = end;
        Ok(())
    }
}

#[test]
fn a_refusal_report_completes_under_an_allocator_that_refuses_each_request() {
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let mut text = Text {
        bytes: [0; 512],
        len: 0,
    };
    REFUSE.set(true);
    let made = LocalOwner::new(&budget, 4);
    let written = made.as_ref().err().map(|refused| write!(text, "{refused} {refused:?}"));
    REFUSE.set(false);
    assert!(matches!(made, Err(Refused::Allocation)));
    assert_eq!(written, Some(Ok(())));

    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    owner.block_on(async {
        let _task = spawn_local(pending::<()>()).unwrap();
        let mut text = Text {
            bytes: [0; 512],
            len: 0,
        };
        REFUSE.set(true);
        let spawned = spawn_local(pending::<()>());
        let written = spawned
            .as_ref()
            .err()
            .map(|refused| write!(text, "{refused} {refused:?} {}", refused.reason()));
        REFUSE.set(false);
        assert!(matches!(&spawned, Err(refused) if refused.reason() == Reason::NoSlot));
        assert_eq!(written, Some(Ok(())));
    });
}
