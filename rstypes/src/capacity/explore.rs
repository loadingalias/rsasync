//! The named risk: a double release in a concurrent split and release. A double release shows as
//! a total above the bound at the end of a model.

use core::num::NonZeroU64;

use super::Budget;

fn budget(bound: u64) -> Budget {
    Budget::new(NonZeroU64::new(bound).unwrap())
}

/// Checks `f` in each thread order within a preemption bound of 3.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

#[test]
fn a_concurrent_split_and_release_never_releases_twice() {
    model(|| {
        let budget = budget(4);
        let mut whole = budget.reserve(4).unwrap();
        let mut moved = whole.split(2).unwrap();

        let splitter = loom::thread::spawn(move || {
            let part = moved.split(1).unwrap();
            part.release();
            drop(moved);
        });
        let other = budget.clone();
        let reserver = loom::thread::spawn(move || {
            // It races with the releases, so it gets its units only if they are free.
            if let Ok(permit) = other.reserve(3) {
                assert_eq!(permit.amount(), 3);
            }
        });
        drop(whole);
        splitter.join().unwrap();
        reserver.join().unwrap();

        assert_eq!(budget.available(), 4);
    });
}

#[test]
fn a_credit_batch_and_a_reservation_never_take_the_budget_above_its_bound() {
    model(|| {
        let budget = budget(4);
        let mut credit = budget.credit(1);

        let other = budget.clone();
        let reserver = loom::thread::spawn(move || {
            if let Ok(permit) = other.reserve(2) {
                permit.release();
            }
        });
        if credit.charge(2).is_ok() {
            credit.refund(2).unwrap();
            assert_eq!(credit.idle(), 1);
        }
        drop(credit);
        reserver.join().unwrap();

        assert_eq!(budget.available(), 4);
    });
}
