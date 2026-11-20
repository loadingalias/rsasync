extern crate std;

use core::alloc::{GlobalAlloc, Layout};
use core::cell::Cell;
use core::num::{NonZeroU32, NonZeroU64};
use std::alloc::System;

use super::{Pool, PoolEmpty, PoolRefused};
use alloc::vec::Vec;

use crate::buffer::{Backing, Buffer, BufferMut, Lease, WriteLoan};
use crate::capacity::{Budget, Exhausted};

/// Counts the allocations of each thread and their bytes, so that a test can show that an
/// operation makes none, or that a charge covers each byte. It refuses each allocation from the
/// count in `REFUSE_FROM` on.
struct Counting;

std::thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
    static REFUSE_FROM: Cell<usize> = const { Cell::new(usize::MAX) };
    /// The bytes that this thread allocated, less the bytes that it freed.
    static HELD: Cell<isize> = const { Cell::new(0) };
}

#[expect(unsafe_code, reason = "the test allocator gives each call to the system allocator")]
// SAFETY: each method gives its call to `System` with the same arguments, and only counts it.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let count = ALLOCATIONS.with(|count| count.replace(count.get() + 1));
        if count >= REFUSE_FROM.with(Cell::get) {
            return core::ptr::null_mut();
        }
        BYTES.with(|bytes| bytes.update(|bytes| bytes + layout.size()));
        HELD.with(|held| held.update(|held| held + layout.size().cast_signed()));
        // SAFETY: the caller gives a layout that `GlobalAlloc::alloc` accepts.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        HELD.with(|held| held.update(|held| held - layout.size().cast_signed()));
        // SAFETY: `alloc` gave `ptr` from `System`, with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `f`, and gives its result and the number of allocations that it made on this thread.
pub(in crate::buffer) fn allocations<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let before = ALLOCATIONS.with(Cell::get);
    let result = f();
    (result, ALLOCATIONS.with(Cell::get) - before)
}

/// Runs `f`, and gives its result and the number of bytes that it allocated on this thread.
fn allocated<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let before = BYTES.with(Cell::get);
    let result = f();
    (result, (BYTES.with(Cell::get) - before) as u64)
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
fn the_pool_charges_each_byte_that_it_allocates() {
    let budget = budget();
    let (mut pool, bytes) = allocated(|| pool(&budget, 3));
    // Each allocation of the pool, with three rounded slots, also when they are idle.
    assert!(bytes > 3 * 4096);
    assert_eq!(budget.available(), MIB - bytes);

    let buffer = pool.acquire().unwrap();
    assert_eq!((buffer.capacity(), buffer.allocation_size()), (4000, 4096));
    assert_eq!(budget.available(), MIB - bytes);

    // The charge ends when the memory is freed: after the pool and its last slot drop.
    drop(pool);
    assert_eq!(budget.available(), MIB - bytes);
    drop(buffer);
    assert_eq!(budget.available(), MIB);

    // A budget with fewer free units refuses the pool and keeps nothing.
    let small = Budget::new(NonZeroU64::new(bytes - 1).unwrap());
    let refused = Pool::new(&small, NonZeroU32::new(3).unwrap(), slot()).unwrap_err();
    assert_eq!(
        refused,
        PoolRefused::Budget(Exhausted {
            requested: bytes,
            available: bytes - 1
        })
    );
    assert_eq!(small.available(), bytes - 1);
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

#[test]
fn a_split_slot_allocates_nothing_and_returns_after_its_last_part() {
    let budget = budget();
    let mut pool = pool(&budget, 1);
    let mut loan = WriteLoan::new(pool.acquire().unwrap());
    loan.fill_from(b"HEADBODY");
    let backing = loan.into_inner();
    let ((head, rest), split) = allocations(|| backing.freeze_prefix(4).unwrap());
    let (body, frozen) = allocations(|| Lease::from(rest));
    assert_eq!((split, frozen), (0, 0));
    // The lease of the rest gives the bytes of the rest, at their place in the slot.
    assert_eq!((head.bytes(), body.bytes()), (&b"HEAD"[..], &b"BODY"[..]));
    assert_eq!(body.as_ptr(), head.as_ptr().wrapping_add(4));

    drop(head);
    assert_eq!(pool.acquire().unwrap_err(), PoolEmpty);
    drop(body);
    pool.acquire().unwrap();
}

#[test]
fn a_later_split_of_a_heap_backing_allocates_nothing() {
    let backing = Backing::from(b"ABCDEF".to_vec());
    let ((first, rest), one) = allocations(|| backing.freeze_prefix(2).unwrap());
    let ((second, rest), later) = allocations(|| rest.freeze_prefix(2).unwrap());
    let (third, last) = allocations(|| Lease::from(rest));
    // The first split allocates the shared count. Each later part shares it.
    assert_eq!((one, later, last), (1, 0, 0));
    assert_eq!(
        (first.bytes(), second.bytes(), third.bytes()),
        (&b"AB"[..], &b"CD"[..], &b"EF"[..])
    );
}

#[test]
fn a_split_that_fills_its_slot_returns_that_slot_and_no_other() {
    let budget = budget();
    // No padding: the end of the first slot is the start of the second.
    let slot = Layout::from_size_align(4096, 4096).unwrap();
    let mut pool = Pool::new(&budget, NonZeroU32::new(2).unwrap(), slot).unwrap();
    let (first, second) = (pool.acquire().unwrap(), pool.acquire().unwrap());
    let address = first.as_ptr().addr();
    let mut loan = WriteLoan::new(first);
    loan.fill_from(&[7; 4096]);
    // The prefix takes the whole slot, so the empty rest starts at the end of the slot. A split of
    // the rest and a lease of it also keep the first slot.
    let (whole, rest) = loan.into_inner().freeze_prefix(4096).unwrap();
    let (empty, rest) = rest.freeze_prefix(0).unwrap();
    drop((whole, empty, rest));

    // Only the first slot returns. The second stays with its backing.
    let again = pool.acquire().unwrap();
    assert_eq!(again.as_ptr().addr(), address);
    assert_eq!(pool.acquire().unwrap_err(), PoolEmpty);
    drop((again, second));
}

/// Each refusal of the allocator gives `Allocation`, and keeps no charge and no memory. Run `n`
/// refuses allocation `n`, counted from 0, and each later one, until a run fails no allocation. A
/// pool whose last allocation aborts the process fails.
#[test]
fn a_pool_that_the_allocator_refuses_keeps_no_charge_and_no_memory() {
    let budget = budget();
    let mut refused = 0;
    loop {
        let held = HELD.with(Cell::get);
        REFUSE_FROM.with(|from| from.set(ALLOCATIONS.with(Cell::get) + refused));
        let made = Pool::new(&budget, NonZeroU32::new(2).unwrap(), slot());
        REFUSE_FROM.with(|from| from.set(usize::MAX));
        let Err(reason) = made else { break };
        let kept = HELD.with(Cell::get) - held;
        assert_eq!((reason, kept, budget.available()), (PoolRefused::Allocation, 0, MIB));
        refused += 1;
        assert!(refused < 16, "a pool makes a few allocations");
    }
    assert!(refused > 0);
}
