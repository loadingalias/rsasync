use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::num::NonZeroU64;
use core::task::{Poll, Waker};
use core::time::Duration;
use std::sync::mpsc;
use std::thread;

use super::RingOwner;
use crate::blocking::{self, Service};
use crate::capacity::Budget;
use crate::driver::{Config, Driver};
use crate::table::tests::allocations;
use crate::task::tests::yield_once;
use crate::task::{Outcome, spawn_local};

fn owner(slots: usize) -> RingOwner {
    let slots = u32::try_from(slots).unwrap();
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let driver = Driver::new(Config::new()).expect("the test host allows io_uring");
    RingOwner::new(&budget, slots, driver).unwrap()
}

/// Gives its waker to `wakers` at its first poll, and ends at its next poll.
fn pend_once(wakers: mpsc::Sender<Waker>) -> impl Future<Output = ()> {
    let mut sent = false;
    poll_fn(move |cx| {
        if sent {
            return Poll::Ready(());
        }
        sent = true;
        wakers.send(cx.waker().clone()).unwrap();
        Poll::Pending
    })
}

/// Only the wake that finds the remote-ready list empty signals the home, so a burst of wakes
/// from many threads with no ring writes the interrupt descriptor one time for each batch. No wake
/// allocates.
#[test]
fn a_burst_of_wakes_from_threads_with_no_ring_signals_once_for_each_batch() {
    const TASKS: usize = 16;
    const THREADS: usize = 4;
    let mut owner = owner(TASKS);
    let home = owner.driver.home();
    owner
        .block_on(async move {
            for _batch in 0..2 {
                let (sender, receiver) = mpsc::channel();
                let tasks: Vec<_> = (0..TASKS)
                    .map(|_| spawn_local(pend_once(sender.clone())).unwrap())
                    .collect();
                yield_once().await;
                let wakers: Vec<Waker> = receiver.try_iter().collect();
                assert_eq!(wakers.len(), TASKS);
                // The owner stays in this poll of the root during the burst, so it takes the
                // whole burst as one batch.
                let allocations = thread::scope(|scope| {
                    let threads: Vec<_> = wakers
                        .chunks(TASKS / THREADS)
                        .map(|wakers| {
                            scope.spawn(move || {
                                let before = allocations::count();
                                wakers.iter().for_each(Waker::wake_by_ref);
                                allocations::count() - before
                            })
                        })
                        .collect();
                    threads.into_iter().map(|thread| thread.join().unwrap()).sum::<u64>()
                });
                assert_eq!(allocations, 0);
                assert_eq!(home.take_interrupts(), 1);
                for task in tasks {
                    assert!(matches!(task.join().await, Outcome::Completed(())));
                }
            }
        })
        .unwrap();
}

/// A wake from a thread whose owner has a ring sends a message from that ring, and the message
/// alone ends the wait of the home in its ring: the interrupt descriptor of the home gets no
/// write. The wakes, the message, and its submit allocate nothing.
#[test]
fn a_wake_from_a_thread_with_a_ring_ends_the_wait_of_its_home_by_a_message() {
    const TASKS: usize = 8;
    let (wakers, received) = mpsc::channel::<Vec<Waker>>();
    let target = thread::spawn(move || {
        let mut owner = owner(TASKS);
        let home = owner.driver.home();
        owner
            .block_on(async move {
                let (sender, receiver) = mpsc::channel();
                let tasks: Vec<_> = (0..TASKS)
                    .map(|_| spawn_local(pend_once(sender.clone())).unwrap())
                    .collect();
                yield_once().await;
                wakers.send(receiver.try_iter().collect()).unwrap();
                // The owner waits in its ring until the wakes come.
                for task in tasks {
                    assert!(matches!(task.join().await, Outcome::Completed(())));
                }
                home.take_interrupts()
            })
            .unwrap()
    });
    let wakers = received.recv().unwrap();
    assert_eq!(wakers.len(), TASKS);
    // The target then waits in its ring with near certainty. If it does not, it takes the wakes
    // before its wait, and the test still holds.
    thread::sleep(Duration::from_millis(50));
    let mut sender = owner(1);
    let allocations = sender
        .block_on(async move {
            let before = allocations::count();
            wakers.iter().for_each(Waker::wake_by_ref);
            // The owner sends the message before its next turn of the driver.
            yield_once().await;
            allocations::count() - before
        })
        .unwrap();
    assert_eq!(allocations, 0);
    assert_eq!(target.join().unwrap(), 0);
}

/// A job of the blocking service wakes its local task from a thread with no ring, and the wake
/// reaches the home, which waits in its ring.
#[test]
fn a_wake_from_a_blocking_service_thread_reaches_its_home() {
    let budget = Budget::new(NonZeroU64::new(4 << 20).unwrap());
    let config = blocking::Config::new().threads(1).jobs(1).stack(256 << 10);
    let service = Service::new(&budget, config).unwrap();
    let submitter = service.submitter();
    let mut owner = owner(1);
    let outcome = owner
        .block_on(async move {
            let task = spawn_local(async move {
                let mut waiter = submitter
                    .submit(|| {
                        // The home then waits in its ring with near certainty.
                        thread::sleep(Duration::from_millis(20));
                        5
                    })
                    .unwrap();
                waiter.wait().await
            })
            .unwrap();
            task.join().await
        })
        .unwrap();
    assert!(matches!(outcome, Outcome::Completed(Ok(5))));
    service.teardown();
}
