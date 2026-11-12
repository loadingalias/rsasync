extern crate std;

use core::alloc::{GlobalAlloc, Layout};
use core::cell::Cell;
use core::num::{NonZeroU32, NonZeroU64};
use std::alloc::System;

use super::{Pool, PoolEmpty, PoolRefused};
use alloc::vec::Vec;

use crate::buffer::{Backing, Buffer, BufferMut, Lease, WriteLoan};
use crate::capacity::{Budget, Exhausted};

/// Counts the allocations of each thread, so that a test can show that an operation makes none.
struct Counting;

std::thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

#[expect(unsafe_code, reason = "the test allocator gives each call to the system allocator")]
// SAFETY: each method gives its call to `System` with the same arguments, and only counts it.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the caller gives a layout that `GlobalAlloc::alloc` accepts.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `alloc` gave `ptr` from `System`, with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `f`, and gives its result and the number of allocations that it made on this thread.
fn allocations<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let before = ALLOCATIONS.with(Cell::get);
    let result = f();
    (result, ALLOCATIONS.with(Cell::get) - before)
}

const MIB: u64 = 1024 * 1024;

fn budget() -> Budget {
    Budget::new(NonZeroU64::new(MIB).unwrap())
}

/// 4000 bytes aligned to 4096 bytes: the rounded slot size is 4096 bytes.
fn slot() -> Layout {
    Layout::from_size_align(4000, 4096).unwrap()
}

fn pool(budget: &Budget, slots: u32) -> Pool {
    Pool::new(budget, NonZeroU32::new(slots).unwrap(), slot()).unwrap()
}

#[test]
fn a_slot_returns_once_when_its_last_lease_or_view_drops() {
    let budget = budget();
    let mut pool = pool(&budget, 1);
    let mut loan = WriteLoan::new(pool.acquire().unwrap());
    assert_eq!(loan.fill_from(b"HEAD:BODY"), 9);
    let lease = Lease::from(loan.into_inner());
    let (head, body) = (lease.view(..4).unwrap(), lease.view(5..).unwrap());
    let clone = lease.clone();
    drop((lease, head, clone));

    // The view keeps its slot: the pool gives no slot, and the bytes stay.
    assert_eq!(pool.acquire().unwrap_err(), PoolEmpty);
    assert_eq!(body.bytes(), b"BODY");

    // The slot returns one time: the pool gives it one time, and then no slot.
    drop(body);
    let again = pool.acquire().unwrap();
    assert_eq!(pool.acquire().unwrap_err(), PoolEmpty);
    drop(again);
}

#[test]
fn the_pool_charge_is_the_rounded_backing_of_all_slots() {
    let budget = budget();
    let mut pool = pool(&budget, 3);
    // Three rounded slots, also when they are idle.
    assert_eq!(budget.available(), MIB - 3 * 4096);

    let buffer = pool.acquire().unwrap();
    assert_eq!((buffer.capacity(), buffer.allocation_size()), (4000, 4096));
    assert_eq!(budget.available(), MIB - 3 * 4096);

    // The charge ends when the memory is freed: after the pool and its last slot drop.
    drop(pool);
    assert_eq!(budget.available(), MIB - 3 * 4096);
    drop(buffer);
    assert_eq!(budget.available(), MIB);

    // A budget with fewer free units refuses the pool and keeps nothing.
    let small = Budget::new(NonZeroU64::new(3 * 4096 - 1).unwrap());
    let refused = Pool::new(&small, NonZeroU32::new(3).unwrap(), slot()).unwrap_err();
    assert_eq!(
        refused,
        PoolRefused::Budget(Exhausted {
            requested: 3 * 4096,
            available: 3 * 4096 - 1
        })
    );
    assert_eq!(small.available(), 3 * 4096 - 1);
}

#[test]
fn a_slot_size_of_zero_is_refused() {
    let budget = budget();
    let refused = Pool::new(&budget, NonZeroU32::new(1).unwrap(), Layout::new::<()>()).unwrap_err();
    assert_eq!(refused, PoolRefused::Layout);
    assert_eq!(budget.available(), MIB);
}

#[test]
fn an_empty_pool_refuses_and_allocates_nothing() {
    let budget = budget();
    let mut pool = pool(&budget, 2);

    let ([first, second, third], made) = allocations(|| [pool.acquire(), pool.acquire(), pool.acquire()]);
    assert_eq!(made, 0);
    assert_eq!(third.unwrap_err(), PoolEmpty);
    drop((first.unwrap(), second.unwrap()));
}

#[test]
fn a_lease_of_a_slot_and_its_views_allocate_nothing() {
    let budget = budget();
    let mut pool = pool(&budget, 1);
    let mut backing = pool.acquire().unwrap();
    backing.truncate(0);

    let ((), made) = allocations(|| {
        let lease = Lease::from(backing);
        let view = lease.view(..).unwrap();
        let clone = view.clone();
        drop((lease, view, clone));
    });
    assert_eq!(made, 0);
    // The last drop returned the slot.
    drop(pool.acquire().unwrap());

    // A lease of a backing that is not a slot allocates its count: the counter sees it.
    let (lease, made) = allocations(|| Lease::from(Backing::from(Vec::new())));
    assert_eq!(made, 1);
    drop(lease);
}

// The WebAssembly targets of the tests have no threads.
#[cfg(not(target_family = "wasm"))]
#[test]
fn a_view_dropped_on_another_thread_returns_its_slot_with_no_allocation() {
    let budget = budget();
    let mut pool = pool(&budget, 1);
    let backing = pool.acquire().unwrap();
    let address = backing.as_ptr().addr();
    let lease = Lease::from(backing);
    let view = lease.view(..).unwrap();
    drop(lease);

    let made = std::thread::spawn(move || allocations(|| drop(view)).1).join().unwrap();
    assert_eq!(made, 0);
    // The next acquire at the home gets the slot from the return list.
    assert_eq!(pool.acquire().unwrap().as_ptr().addr(), address);
}
