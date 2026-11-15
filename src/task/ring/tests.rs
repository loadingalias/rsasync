use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::Cell;
use core::future::{Future, poll_fn};
use core::num::NonZeroU64;
use core::task::{Poll, Waker};
use core::time::Duration;
use std::io::{self, Write as _};
use std::os::fd::OwnedFd;
use std::sync::mpsc;
use std::thread;

use super::RingOwner;
use crate::blocking::{self, Service};
use crate::buffer::{Backing, Buffer as _};
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

/// A task that is always ready keeps the owner from its wait in the ring, which runs the deferred
/// work of the kernel. A turn that sees the task-run flag enters the kernel to run that work, so
/// the receives complete while the task still runs. A short burst completes its receive with no
/// wait for a full buffer, and a close of the peer completes its receive at the end of the stream.
#[test]
fn a_busy_owner_completes_a_burst_and_a_close_while_its_task_runs() {
    /// The rounds of the busy task, far more than the receives need.
    const ROUNDS: u32 = 1_000_000;
    let mut owner = owner(2);
    let receive = |owner: &mut RingOwner| {
        let (read, write) = io::pipe().expect("the test host makes a pipe");
        let reader = owner
            .driver
            .adopt_reader(OwnedFd::from(read))
            .expect("the test driver has room");
        // The future borrows its reader for the rest of the test.
        let reader = Box::leak(Box::new(reader));
        let future = owner
            .driver
            .recv(reader, Backing::from(Vec::with_capacity(64)))
            .expect("the test driver has room");
        (future, write)
    };
    let (burst, mut burst_writer) = receive(&mut owner);
    let (close, close_writer) = receive(&mut owner);
    let (received, busy_rounds) = owner
        .block_on(async move {
            let (done, rounds) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(0)));
            let busy = spawn_local({
                let (done, rounds) = (Rc::clone(&done), Rc::clone(&rounds));
                async move {
                    while !done.get() && rounds.get() < ROUNDS {
                        rounds.set(rounds.get() + 1);
                        yield_once().await;
                    }
                }
            })
            .unwrap();
            // The turns of the yield submit the two receives, which then wait for data.
            yield_once().await;
            burst_writer.write_all(b"abc").unwrap();
            drop(close_writer);
            let (burst, close) = (burst.await.unwrap(), close.await.unwrap());
            let at = rounds.get();
            done.set(true);
            assert!(matches!(busy.join().await, Outcome::Completed(())));
            ((burst.0.unwrap(), burst.1.bytes().to_vec(), close.0.unwrap()), at)
        })
        .unwrap();
    assert_eq!(received, (3, b"abc".to_vec(), 0));
    // The receives completed while the task still ran.
    assert!(busy_rounds < ROUNDS, "{busy_rounds}");
}

/// On a home with a ring, a parked owner wakes from its wait in the ring for the timer of a task
/// when no other work comes, and the timer never fires before its deadline. The check of lateness
/// is only for a missed wake: the runtime does not bound the wake delay of the system.
#[test]
fn a_timer_never_fires_early_and_wakes_its_owner_from_its_ring() {
    let resolution = core::time::Duration::from_millis(1);
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let (clock, timers) = crate::task::tests::timed(&budget, 4, resolution);
    let mut owner = owner(1);
    owner.use_timers(timers, clock).unwrap();
    let lateness = owner
        .block_on(crate::task::tests::wake_for_timers(clock, resolution, Rc::default()))
        .unwrap();
    for late in lateness {
        assert!(late < core::time::Duration::from_secs(1), "{late:?}");
    }
}
