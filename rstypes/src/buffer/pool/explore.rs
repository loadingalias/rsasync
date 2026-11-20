//! The named risk: a concurrent return and acquire issues one slot twice. A slot that the pool
//! gives two times, or loses, shows as a wrong set of slots at the end of a model.

use alloc::vec::Vec;
use core::alloc::Layout;
use core::num::{NonZeroU32, NonZeroU64};

use super::{Pool, PoolEmpty};
use crate::buffer::{Buffer, Lease};
use crate::capacity::Budget;

fn pool(slots: u32) -> Pool {
    let budget = Budget::new(NonZeroU64::new(4096).unwrap());
    let slot = Layout::from_size_align(8, 8).unwrap();
    Pool::new(&budget, NonZeroU32::new(slots).unwrap(), slot).unwrap()
}

/// Checks `f` in each thread order within a preemption bound of 3.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

#[test]
fn a_concurrent_return_and_acquire_never_issue_one_slot_twice() {
    model(|| {
        let mut pool = pool(2);
        let first = pool.acquire().unwrap();
        let second = Lease::from(pool.acquire().unwrap());
        let view = second.view(..).unwrap();
        let mut expected = [first.as_ptr().addr(), second.as_ptr().addr()];
        expected.sort_unstable();

        let returns_first = loom::thread::spawn(move || drop(first));
        let returns_view = loom::thread::spawn(move || drop(view));
        drop(second);
        // The home acquires while the slots return.
        let early = pool.acquire().ok();
        returns_first.join().unwrap();
        returns_view.join().unwrap();

        // Each slot comes back exactly one time.
        let late: Vec<_> = core::iter::from_fn(|| pool.acquire().ok()).collect();
        let mut issued: Vec<_> = early.iter().chain(&late).map(|slot| slot.as_ptr().addr()).collect();
        issued.sort_unstable();
        assert_eq!(issued, expected);
    });
}

#[test]
fn the_last_of_two_concurrent_views_returns_the_slot_once() {
    model(|| {
        let mut pool = pool(1);
        let lease = Lease::from(pool.acquire().unwrap());
        let (one, two) = (lease.view(..).unwrap(), lease.view(..).unwrap());
        drop(lease);

        let drops_one = loom::thread::spawn(move || drop(one));
        drop(two);
        drops_one.join().unwrap();

        let slot = pool.acquire().unwrap();
        assert_eq!(pool.acquire().unwrap_err(), PoolEmpty);
        drop(slot);
    });
}
