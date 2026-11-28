//! The named risk: a send that races a drop of the receiver loses the value or gives it two
//! owners, or a send or a drop of the sender that races a waiting receive loses its wake. A lost
//! value shows as no drop of it, two owners as two drops, and a lost wake as a future that is
//! still pending after the sender ended, with no wake of its waker. Also: a sender that touches
//! the channel after a receive can see the value holds the charge after that receive returned.

use core::sync::atomic::Ordering;
use core::task::Poll;

use super::channel;
use crate::channel::tests::{BOUND, budget, counted, poll};
use crate::channel::{Closed, SendError, TryRecvError};
use crate::wait::tests::counter;

/// Checks `f` in each thread order within a preemption bound of 3.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

#[test]
fn a_send_that_races_a_receiver_drop_has_one_owner() {
    model(|| {
        let budget = budget();
        let (drops, make) = counted();
        let (sender, mut receiver) = channel(&budget).unwrap();
        let (_count, waker) = counter();
        let producer = loom::thread::spawn(move || sender.send(make(1)).err().map(|SendError(value)| value));
        // A poll stores a waker, so the drop can come while the sender holds the cell. A value
        // that the poll takes drops with its result.
        drop(poll(&mut receiver.recv(), &waker));
        drop(receiver);
        if let Some(value) = producer.join().unwrap() {
            // The receiver was gone first: the value came back, and nothing dropped it.
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            drop(value);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        // One endpoint freed the channel.
        assert_eq!(budget.available(), BOUND);
    });
}

/// A join future that moves to another task polls with a new waker. A send that races that poll
/// wakes the new waker, or the poll sees the value.
#[test]
fn a_send_that_races_a_poll_with_a_new_waker_wakes_that_waker() {
    model(|| {
        let budget = budget();
        let (sender, mut receiver) = channel(&budget).unwrap();
        let (_first, waker) = counter();
        let (count, moved) = counter();
        let mut recv = receiver.recv();
        assert!(poll(&mut recv, &waker).is_pending());
        let producer = loom::thread::spawn(move || sender.send(1).unwrap());
        let mut polled = poll(&mut recv, &moved);
        producer.join().unwrap();
        if polled.is_pending() {
            assert_eq!(count.get(), 1);
            polled = poll(&mut recv, &moved);
        }
        assert_eq!(polled, Poll::Ready(Ok(1)));
    });
}

#[test]
fn a_send_that_races_a_pending_receive_is_not_lost() {
    model(|| {
        let budget = budget();
        let (sender, mut receiver) = channel(&budget).unwrap();
        let (count, waker) = counter();
        let producer = loom::thread::spawn(move || sender.send(1).unwrap());
        let mut recv = receiver.recv();
        let mut polled = poll(&mut recv, &waker);
        producer.join().unwrap();
        if polled.is_pending() {
            assert_eq!(count.get(), 1);
            polled = poll(&mut recv, &waker);
        }
        assert_eq!(polled, Poll::Ready(Ok(1)));
    });
}

/// A user meets the risk as a budget that is low, or a spawn that the budget refuses, right after a
/// join.
#[cfg(feature = "std")]
#[test]
fn a_receive_that_takes_the_value_ends_the_charge_before_it_returns() {
    model(|| {
        let budget = budget();
        let (sender, mut receiver) = channel(&budget).unwrap();
        // The receiving thread is not the one that joins: the loom join breaks on an unpark that
        // the receive did not use, which the standard join ignores.
        let receives = {
            let budget = budget.clone();
            loom::thread::spawn(move || (receiver.recv_blocking(), budget.available()))
        };
        sender.send(1).unwrap();
        assert_eq!(receives.join().unwrap(), (Ok(1), BOUND));
    });
}

#[test]
fn a_sender_drop_that_races_a_pending_receive_closes_it() {
    model(|| {
        let budget = budget();
        let (sender, mut receiver) = channel::<u32>(&budget).unwrap();
        let (count, waker) = counter();
        let producer = loom::thread::spawn(move || drop(sender));
        let mut recv = receiver.recv();
        let mut polled = poll(&mut recv, &waker);
        producer.join().unwrap();
        if polled.is_pending() {
            assert_eq!(count.get(), 1);
            polled = poll(&mut recv, &waker);
        }
        assert_eq!(polled, Poll::Ready(Err(Closed)));
    });
}

#[test]
fn a_send_that_races_a_dropped_receive_future_keeps_the_value() {
    model(|| {
        let budget = budget();
        let (sender, mut receiver) = channel(&budget).unwrap();
        let (_count, waker) = counter();
        let producer = loom::thread::spawn(move || sender.send(1).unwrap());
        let mut recv = receiver.recv();
        let polled = poll(&mut recv, &waker);
        drop(recv);
        producer.join().unwrap();
        if polled.is_pending() {
            assert_eq!(receiver.try_recv(), Ok(1));
        } else {
            assert_eq!(polled, Poll::Ready(Ok(1)));
            assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
        }
    });
}
