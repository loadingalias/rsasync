extern crate std;

use core::num::NonZeroU64;

use super::{Budget, Exhausted, NotCharged};

fn budget(bound: u64) -> Budget {
    Budget::new(NonZeroU64::new(bound).unwrap())
}

#[test]
fn a_reservation_above_the_free_units_is_refused_and_changes_nothing() {
    let budget = budget(100);
    let held = budget.reserve(60).unwrap();

    let refused = budget.reserve(41).unwrap_err();
    assert_eq!(
        refused,
        Exhausted {
            requested: 41,
            available: 40
        }
    );
    assert_eq!(budget.available(), 40);
    // The refusal took nothing: all of the free units are still there.
    let rest = budget.reserve(40).unwrap();
    assert_eq!(budget.available(), 0);

    drop((held, rest));
    assert_eq!(budget.available(), 100);
}

#[test]
fn split_parts_sum_to_the_original_and_each_part_releases_once() {
    let budget = budget(100);
    let mut whole = budget.reserve(100).unwrap();

    let part = whole.split(30).unwrap();
    assert_eq!((whole.amount(), part.amount()), (70, 30));
    assert_eq!(budget.available(), 0);
    assert_eq!(
        whole.split(71).unwrap_err(),
        Exhausted {
            requested: 71,
            available: 70
        }
    );

    // An explicit release, then a drop: the total comes back to the bound, and not above it.
    part.release();
    assert_eq!(budget.available(), 30);
    drop(whole);
    assert_eq!(budget.available(), 100);
}

#[test]
fn a_child_permit_releases_after_its_parent_drops() {
    let budget = budget(100);

    // A part split from a permit.
    let mut parent = budget.reserve(50).unwrap();
    let child = parent.split(20).unwrap();
    drop(parent);
    assert_eq!(budget.available(), 80);
    drop(child);
    assert_eq!(budget.available(), 100);

    // A permit split from a credit.
    let mut credit = budget.credit(10);
    let child = credit.split(15).unwrap();
    drop(credit);
    assert_eq!(budget.available(), 85);
    drop(child);
    assert_eq!(budget.available(), 100);
}

#[test]
fn credit_keeps_its_idle_units_counted_and_never_takes_the_budget_above_its_bound() {
    let budget = budget(100);
    let mut credit = budget.credit(30);
    // Each charged and idle unit stays reserved in the budget.
    let held = |credit: &super::Credit| credit.charged() + credit.idle();

    // The first charge takes a batch: what it needs, and 30 idle units more.
    credit.charge(10).unwrap();
    assert_eq!((credit.charged(), credit.idle(), budget.available()), (10, 30, 60));
    // A charge from the idle units does not change the budget.
    credit.charge(25).unwrap();
    assert_eq!((credit.charged(), credit.idle(), budget.available()), (35, 5, 60));

    // A refund keeps at most 30 idle units and gives the rest back.
    credit.refund(35).unwrap();
    assert_eq!((credit.charged(), credit.idle(), budget.available()), (0, 30, 70));
    assert_eq!(
        credit.refund(1).unwrap_err(),
        NotCharged {
            refunded: 1,
            charged: 0
        }
    );

    // When the budget has less than a full batch, the charge takes what it needs, and no more.
    let other = budget.reserve(60).unwrap();
    credit.charge(35).unwrap();
    assert_eq!((credit.charged(), credit.idle(), budget.available()), (35, 5, 0));
    assert_eq!(
        credit.charge(6).unwrap_err(),
        Exhausted {
            requested: 6,
            available: 5
        }
    );
    assert_eq!(budget.available() + held(&credit) + other.amount(), 100);

    drop((credit, other));
    assert_eq!(budget.available(), 100);
}

#[test]
fn zero_units_reserve_nothing_and_never_refuse() {
    let budget = budget(10);
    let all = budget.reserve(10).unwrap();

    // A budget with no free unit still gives an empty permit, and an empty part.
    let mut empty = budget.reserve(0).unwrap();
    assert_eq!(empty.split(0).unwrap().amount(), 0);
    // A credit with an idle bound of zero keeps no idle unit: each charge takes exactly its
    // units, and each refund gives them back.
    drop(all);
    let mut credit = budget.credit(0);
    credit.charge(4).unwrap();
    assert_eq!((credit.idle(), budget.available()), (0, 6));
    credit.refund(4).unwrap();
    assert_eq!((credit.idle(), budget.available()), (0, 10));
}

#[test]
fn totals_above_32_bits_stay_exact() {
    let budget = budget(1 << 40);
    let mut large = budget.reserve((1 << 32) + 1).unwrap();
    let part = large.split(1 << 32).unwrap();

    assert_eq!(budget.available(), (1 << 40) - (1 << 32) - 1);
    assert_eq!((large.amount(), part.amount()), (1, 1 << 32));
    drop((large, part));
    assert_eq!(budget.available(), 1 << 40);
}

// The WebAssembly targets of the tests have no threads.
#[cfg(not(target_family = "wasm"))]
#[test]
fn a_permit_that_moves_to_another_thread_releases_once_there() {
    let budget = budget(100);
    let permit = budget.reserve(40).unwrap();

    let owner = budget.clone();
    std::thread::spawn(move || {
        // The move did not release the units.
        assert_eq!(owner.available(), 60);
        drop(permit);
    })
    .join()
    .unwrap();
    assert_eq!(budget.available(), 100);
}
