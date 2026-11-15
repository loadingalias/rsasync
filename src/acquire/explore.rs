//! The named risks: a release that races a new wait, a cancel, or a change of the first waiter
//! does not reach the first waiter (a lost wake), and a cancelled wait keeps units (a ghost
//! reservation). A lost wake shows as a waiter that has no units after its recheck and whose waker
//! no wake reached. A ghost reservation shows as a budget total below the bound.

use core::future::Future;
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::pin;
use core::task::Context;

use super::{Acquirer, budget};
use crate::capacity::Budget;
use crate::wait::tests::counter;

/// Checks `f` in each thread order within a preemption bound of 3.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

/// A budget of `bound` units with two waiter places, its two acquirers, and the memory budget.
fn waiting(bound: u64) -> (Budget, Budget, Acquirer, Acquirer) {
    let memory = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let (budget, first) = budget(&memory, NonZeroU64::new(bound).unwrap(), NonZeroU32::new(2).unwrap()).unwrap();
    let second = first.try_clone().unwrap();
    (memory, budget, first, second)
}

#[test]
fn a_release_that_races_a_new_wait_reaches_it() {
    model(|| {
        let (_memory, budget, mut acquirer, _second) = waiting(1);
        let held = budget.reserve(1).unwrap();
        let releaser = loom::thread::spawn(move || drop(held));

        let (count, waker) = counter();
        let mut cx = Context::from_waker(&waker);
        let mut wait = pin!(acquirer.acquire(1).unwrap());
        let ready = wait.as_mut().poll(&mut cx).is_ready();
        releaser.join().unwrap();
        // The wait got the units, or the release woke it, and then it gets them.
        assert!(ready || (count.get() == 1 && wait.as_mut().poll(&mut cx).is_ready()));
    });
}

#[test]
fn a_release_that_races_a_cancel_of_the_first_waiter_reaches_the_next() {
    model(|| {
        let (_memory, budget, mut first, mut second) = waiting(1);
        let held = budget.reserve(1).unwrap();

        let (_, waker_a) = counter();
        let (b, waker_b) = counter();
        let mut wait_b = pin!(second.acquire(1).unwrap());
        {
            let mut wait_a = pin!(first.acquire(1).unwrap());
            assert!(wait_a.as_mut().poll(&mut Context::from_waker(&waker_a)).is_pending());
            assert!(wait_b.as_mut().poll(&mut Context::from_waker(&waker_b)).is_pending());

            let releaser = loom::thread::spawn(move || drop(held));
            drop(wait_a);
            releaser.join().unwrap();
        }
        // The cancelled wait kept no unit, and the next waiter got a wake.
        assert_eq!((budget.available(), b.get() >= 1), (1, true));
        assert!(wait_b.as_mut().poll(&mut Context::from_waker(&waker_b)).is_ready());
    });
}

#[test]
fn a_release_that_races_a_change_of_the_first_waiter_reaches_the_new_one() {
    model(|| {
        let (_memory, budget, mut first, mut second) = waiting(2);
        let mut held = budget.reserve(2).unwrap();
        let other = held.split(1).unwrap();

        let (_, waker_a) = counter();
        let (b, waker_b) = counter();
        let mut wait_a = pin!(first.acquire(1).unwrap());
        let mut wait_b = pin!(second.acquire(1).unwrap());
        assert!(wait_a.as_mut().poll(&mut Context::from_waker(&waker_a)).is_pending());
        assert!(wait_b.as_mut().poll(&mut Context::from_waker(&waker_b)).is_pending());

        // The first waiter takes one unit and leaves the order, while a second unit returns.
        let releaser = loom::thread::spawn(move || drop(other));
        drop(held);
        let got_a = wait_a.as_mut().poll(&mut Context::from_waker(&waker_a));
        releaser.join().unwrap();

        assert!(got_a.is_ready() && b.get() >= 1);
        let got_b = wait_b.as_mut().poll(&mut Context::from_waker(&waker_b));
        assert_eq!((got_b.is_ready(), budget.available()), (true, 0));
    });
}
