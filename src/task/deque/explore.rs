//! The named risk: a pop of the owner and a steal claim the same entry, or an entry gets no taker,
//! when they race each other or a push.

use alloc::vec::Vec;

use super::Deque;
use crate::sync::Arc;

/// Checks `f` in each thread order within 3 preemptions.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

/// Steals once from `queue` on another thread, and gives each entry that the steal took.
fn thief(queue: &Arc<Deque>) -> loom::thread::JoinHandle<Vec<u32>> {
    let queue = Arc::clone(queue);
    loom::thread::spawn(move || {
        let into = Deque::new().unwrap();
        let mut taken = Vec::new();
        if let Some(first) = queue.steal_into(&into) {
            taken.push(first);
            taken.extend(core::iter::from_fn(|| into.pop()));
        }
        taken
    })
}

#[test]
fn a_pop_and_a_steal_take_each_entry_once() {
    model(|| {
        let queue = Arc::new(Deque::new().unwrap());
        for slot in 1..=3 {
            queue.push(slot).unwrap();
        }
        let thief = thief(&queue);
        let mut taken: Vec<u32> = core::iter::from_fn(|| queue.pop()).collect();
        let stolen = thief.join().unwrap();
        // The steal takes the older entries, in their order.
        assert!(stolen.is_sorted());
        taken.extend(stolen);
        taken.sort_unstable();
        assert_eq!(taken, [1, 2, 3]);
    });
}

#[test]
fn a_push_during_a_steal_keeps_its_entry() {
    model(|| {
        let queue = Arc::new(Deque::new().unwrap());
        queue.push(1).unwrap();
        queue.push(2).unwrap();
        let thief = thief(&queue);
        queue.push(3).unwrap();
        let mut taken: Vec<u32> = core::iter::from_fn(|| queue.pop()).collect();
        taken.extend(thief.join().unwrap());
        taken.sort_unstable();
        assert_eq!(taken, [1, 2, 3]);
    });
}

#[test]
fn two_thieves_and_the_owner_take_each_entry_once() {
    let mut model = loom::model::Builder::new();
    // Three threads: 2 preemptions, as the other models with three threads.
    model.preemption_bound = Some(2);
    model.check(|| {
        let queue = Arc::new(Deque::new().unwrap());
        for slot in 1..=4 {
            queue.push(slot).unwrap();
        }
        let first = thief(&queue);
        let second = thief(&queue);
        let mut taken: Vec<u32> = core::iter::from_fn(|| queue.pop()).collect();
        taken.extend(first.join().unwrap());
        taken.extend(second.join().unwrap());
        taken.sort_unstable();
        assert_eq!(taken, [1, 2, 3, 4]);
    });
}

#[test]
fn a_steal_into_a_queue_that_a_running_steal_holds_takes_nothing_and_keeps_each_entry() {
    let mut model = loom::model::Builder::new();
    // One preemption: the thief stops after its claim, and the owner then takes its other entries.
    model.preemption_bound = Some(1);
    // Each of the 129 pushes, the 64 pops, and the copy of 65 entries is a branch of loom.
    model.max_branches = 10_000;
    model.check(|| {
        // The thief claims 65 of 129 entries. While it copies, the queue of the owner looks empty,
        // and 129 places stay held: more than the room for half of a queue.
        let queue = Arc::new(Deque::new().unwrap());
        for slot in 0..129 {
            queue.push(slot).unwrap();
        }
        let thief = thief(&queue);
        while queue.len() > 64 {
            loom::thread::yield_now();
        }
        let mut taken: Vec<u32> = core::iter::from_fn(|| queue.pop()).collect();
        let other = Deque::new().unwrap();
        other.push(129).unwrap();
        match other.steal_into(&queue) {
            Some(first) => taken.push(first),
            None => taken.extend(core::iter::from_fn(|| other.pop())),
        }
        taken.extend(thief.join().unwrap());
        taken.sort_unstable();
        assert_eq!(taken, (0..130).collect::<Vec<_>>());
    });
}
