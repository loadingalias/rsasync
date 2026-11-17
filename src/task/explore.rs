//! The named risk: a wake or a cancel from another thread races the start or the end of a poll,
//! and is lost, or drops the future during its poll; a stale wake schedules a task in a reused
//! slot; a push to the remote-ready list races the harvest and the park of the owner, and its
//! unpark is lost. A lost wake or unpark shows as an owner that parks forever, which loom reports
//! as a deadlock.
//!
//! In each model, the owner runs in a spawned thread, and the main thread only joins: see the loom
//! join note in `src/wait/explore.rs`.

use core::assert_matches;
use core::cell::Cell;
use core::future::{pending, poll_fn};
use core::num::NonZeroU64;
use core::task::{Poll, Waker};

use alloc::rc::Rc;

use super::{CancelHandle, LocalOwner, Outcome, spawn_local};
use crate::capacity::Budget;
use crate::channel::oneshot;

/// Checks `f` in each thread order within `bound` preemptions: 3 for two threads, 2 for three.
fn model(bound: usize, f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(bound);
    model.check(f);
}

fn budget() -> Budget {
    Budget::new(NonZeroU64::new(1 << 20).unwrap())
}

#[test]
fn a_wake_from_another_thread_reaches_a_local_task() {
    model(3, || {
        let budget = budget();
        let (sender, mut receiver) = oneshot::channel::<u32>(&budget).unwrap();
        let owner = loom::thread::spawn(move || {
            let mut owner = LocalOwner::new(&budget, 1).unwrap();
            owner.block_on(async move {
                let task = spawn_local(async move { receiver.recv().await.unwrap() }).unwrap();
                matches!(task.join().await, Outcome::Completed(5))
            })
        });
        sender.send(5).unwrap();
        assert!(owner.join().unwrap());
    });
}

#[test]
fn a_wake_of_the_root_from_another_thread_is_not_lost() {
    model(3, || {
        let budget = budget();
        let (sender, mut receiver) = oneshot::channel::<u32>(&budget).unwrap();
        let owner = loom::thread::spawn(move || {
            let mut owner = LocalOwner::new(&budget, 1).unwrap();
            owner.block_on(async move { receiver.recv().await })
        });
        sender.send(5).unwrap();
        assert_eq!(owner.join().unwrap(), Ok(5));
    });
}

#[test]
fn a_cancel_from_another_thread_ends_the_task_at_a_poll_boundary() {
    model(2, || {
        let budget = budget();
        let (handle_sender, mut handle_receiver) = oneshot::channel::<CancelHandle>(&budget).unwrap();
        let owner = loom::thread::spawn(move || {
            let mut owner = LocalOwner::new(&budget, 1).unwrap();
            owner.block_on(async move {
                let in_poll = Rc::new(Cell::new(false));
                let probe = Rc::clone(&in_poll);
                let mut polls = 0;
                let task = spawn_local(poll_fn(move |cx| {
                    probe.set(true);
                    polls += 1;
                    // The cancel can come here, during the poll.
                    loom::thread::yield_now();
                    probe.set(false);
                    if polls == 1 {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Poll::Ready(())
                }))
                .unwrap();
                handle_sender.send(task.cancel_handle()).unwrap();
                let outcome = task.join().await;
                // The future dropped at a poll boundary, never during its poll.
                assert!(!in_poll.get());
                matches!(outcome, Outcome::Cancelled | Outcome::Completed(()))
            })
        });
        let canceller = loom::thread::spawn(move || handle_receiver.recv_blocking().unwrap().cancel());
        canceller.join().unwrap();
        assert!(owner.join().unwrap());
    });
}

#[test]
fn a_stale_wake_from_another_thread_does_not_poll_a_reused_slot() {
    model(2, || {
        let budget = budget();
        let (waker_sender, mut waker_receiver) = oneshot::channel::<Waker>(&budget).unwrap();
        let owner = loom::thread::spawn(move || {
            let mut owner = LocalOwner::new(&budget, 1).unwrap();
            let polls = Rc::new(Cell::new(0));
            let count = Rc::clone(&polls);
            owner.block_on(async move {
                let mut sender = Some(waker_sender);
                let first = spawn_local(poll_fn(move |cx| {
                    if let Some(sender) = sender.take() {
                        sender.send(cx.waker().clone()).unwrap();
                    }
                    Poll::Ready(())
                }))
                .unwrap();
                assert_matches!(first.join().await, Outcome::Completed(()));
                // The second task takes the one slot, with the next generation.
                let _second = spawn_local(async move {
                    count.update(|count| count + 1);
                    pending::<()>().await;
                })
                .unwrap();
                super::tests::yield_once().await;
            });
            // The end of `block_on` drops the second task with no poll.
            polls.get()
        });
        let waker = loom::thread::spawn(move || waker_receiver.recv_blocking().unwrap().wake());
        waker.join().unwrap();
        assert_eq!(owner.join().unwrap(), 1);
    });
}
