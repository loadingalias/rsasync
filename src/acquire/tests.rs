use core::assert_matches;
use core::future::Future;
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::pin;
use core::task::{Context, Poll};

use super::{AboveBound, Acquirer, TooManyWaiters, budget, memory};
use crate::capacity::Budget;
use crate::wait::tests::counter;

/// A budget of `bound` units with `places` waiter places, and the memory budget of its places.
fn waiting(bound: u64, places: u32) -> (Budget, Budget, Acquirer) {
    let memory = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let (budget, acquirer) = budget(
        &memory,
        NonZeroU64::new(bound).unwrap(),
        NonZeroU32::new(places).unwrap(),
    )
    .unwrap();
    (memory, budget, acquirer)
}

#[test]
fn a_release_wakes_the_waiters_in_the_order_of_their_waits() {
    let (_memory, budget, mut first) = waiting(10, 3);
    let (mut second, mut third) = (first.try_clone().unwrap(), first.try_clone().unwrap());
    let mut held = budget.reserve(10).unwrap();

    let (a, waker_a) = counter();
    let (b, waker_b) = counter();
    let (c, waker_c) = counter();
    let mut wait_a = pin!(first.acquire(5).unwrap());
    let mut wait_b = pin!(second.acquire(1).unwrap());
    let mut wait_c = pin!(third.acquire(5).unwrap());
    assert!(wait_a.as_mut().poll(&mut Context::from_waker(&waker_a)).is_pending());
    assert!(wait_b.as_mut().poll(&mut Context::from_waker(&waker_b)).is_pending());
    assert!(wait_c.as_mut().poll(&mut Context::from_waker(&waker_c)).is_pending());

    // One free unit is enough for the second waiter, but the first waits for 5, so it stays
    // first, and the second does not take the unit.
    held.split(1).unwrap().release();
    assert_eq!((a.get(), b.get(), c.get()), (1, 0, 0));
    assert!(wait_a.as_mut().poll(&mut Context::from_waker(&waker_a)).is_pending());
    assert!(wait_b.as_mut().poll(&mut Context::from_waker(&waker_b)).is_pending());
    assert_eq!(budget.available(), 1);

    // Each waiter that gets its units wakes the next one.
    drop(held);
    assert_eq!((a.get(), b.get(), c.get()), (2, 0, 0));
    let Poll::Ready(got_a) = wait_a.as_mut().poll(&mut Context::from_waker(&waker_a)) else {
        panic!("the first waiter gets its units")
    };
    assert_eq!((a.get(), b.get(), c.get()), (2, 1, 0));
    let Poll::Ready(got_b) = wait_b.as_mut().poll(&mut Context::from_waker(&waker_b)) else {
        panic!("the second waiter gets its unit")
    };
    assert_eq!((got_a.amount(), got_b.amount(), c.get()), (5, 1, 1));
    // 4 units are free, and the third waiter waits for 5.
    assert!(wait_c.as_mut().poll(&mut Context::from_waker(&waker_c)).is_pending());
    drop(got_b);
    assert!(wait_c.as_mut().poll(&mut Context::from_waker(&waker_c)).is_ready());
}

#[test]
fn a_dropped_wait_leaves_the_budget_total_unchanged_and_wakes_the_next_waiter() {
    let (_memory, budget, mut first) = waiting(10, 3);
    let (mut second, mut third) = (first.try_clone().unwrap(), first.try_clone().unwrap());
    // 4 units are free.
    let held = budget.reserve(6).unwrap();

    let (a, waker_a) = counter();
    let (b, waker_b) = counter();
    let mut wait_b = pin!(second.acquire(4).unwrap());
    {
        let mut wait_a = pin!(first.acquire(5).unwrap());
        assert!(wait_a.as_mut().poll(&mut Context::from_waker(&waker_a)).is_pending());

        // A later wait goes behind the first waiter, also when its own units are free. It holds
        // none of them, so its drop changes no total.
        {
            let (_, waker_c) = counter();
            let mut wait_c = pin!(third.acquire(2).unwrap());
            assert!(wait_c.as_mut().poll(&mut Context::from_waker(&waker_c)).is_pending());
            assert_eq!(budget.available(), 4);
        }
        assert_eq!(budget.available(), 4);
        assert!(wait_b.as_mut().poll(&mut Context::from_waker(&waker_b)).is_pending());

        // The release wakes the first waiter, and it drops with no poll.
        drop(held);
        assert_eq!((a.get(), b.get()), (1, 0));
    }
    // The drop held no units, and it passed the wake to the next waiter.
    assert_eq!((budget.available(), b.get()), (10, 1));
    let Poll::Ready(permit) = wait_b.as_mut().poll(&mut Context::from_waker(&waker_b)) else {
        panic!("the next waiter gets the units")
    };
    assert_eq!((permit.amount(), budget.available()), (4, 6));
}

#[test]
fn a_wait_for_zero_units_ends_at_once_and_a_wait_above_the_bound_is_refused() {
    let (_memory, budget, mut first) = waiting(10, 2);
    let mut second = first.try_clone().unwrap();
    let _held = budget.reserve(10).unwrap();
    let (_, waker) = counter();
    let mut cx = Context::from_waker(&waker);

    let mut wait = pin!(first.acquire(1).unwrap());
    assert!(wait.as_mut().poll(&mut cx).is_pending());
    // A waiter is first, and no unit is free. A wait for zero units does not wait behind it.
    let zero = pin!(second.acquire(0).unwrap()).poll(&mut cx);
    assert_matches!(zero, Poll::Ready(permit) if permit.amount() == 0);
    assert_eq!(
        second.acquire(11).unwrap_err(),
        AboveBound {
            requested: 11,
            bound: 10
        }
    );
}

#[test]
fn the_places_are_bounded_charged_and_free_again_after_a_drop() {
    let heap = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let places = NonZeroU32::new(2).unwrap();
    let (budget, first) = budget(&heap, NonZeroU64::new(10).unwrap(), places).unwrap();
    assert_eq!(heap.available(), (1 << 20) - memory(places));

    let second = first.try_clone().unwrap();
    assert_eq!(second.try_clone().unwrap_err(), TooManyWaiters);
    drop(first);
    let third = second.try_clone().unwrap();

    // The charge ends when the budget and each acquirer have dropped.
    drop((second, third));
    assert_eq!(heap.available(), (1 << 20) - memory(places));
    drop(budget);
    assert_eq!(heap.available(), 1 << 20);
}

/// Each refusal of the allocator gives `Allocation`, and keeps no charge and no memory. A budget
/// whose allocation aborts the process fails.
#[cfg(feature = "std")]
#[test]
fn a_budget_that_the_allocator_refuses_keeps_no_charge_and_no_memory() {
    use super::Refused;
    use crate::table::tests::allocations::each_refusal;

    let heap = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let (bound, places) = (NonZeroU64::new(8).unwrap(), NonZeroU32::new(4).unwrap());
    // The budget of the last run drops after the refusals: on macOS, its drop takes the lock of the
    // places for the first time, and the standard library allocates that lock then.
    let refusals = each_refusal(|| budget(&heap, bound, places));
    assert!(!refusals.is_empty());
    assert!(refusals.iter().all(|refusal| *refusal == (Refused::Allocation, 0)));
    assert_eq!(heap.available(), 1 << 20);
}

/// The charge of a budget equals the bytes that its construction allocates: its waiter places
/// and its own budget. A charge that omits the allocation of that budget fails.
#[cfg(feature = "std")]
#[test]
fn a_budget_charges_each_byte_that_it_allocates() {
    use crate::table::tests::allocations::bytes;

    let heap = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let before = bytes();
    let made = budget(&heap, NonZeroU64::new(8).unwrap(), NonZeroU32::new(4).unwrap()).unwrap();
    let allocated = bytes() - before;
    assert_eq!((1 << 20) - heap.available(), allocated);
    drop(made);
}
