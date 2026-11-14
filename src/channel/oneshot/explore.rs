//! The named risk: a send that races a drop of the receiver loses the value or gives it two
//! owners, or a send or a drop of the sender that races a waiting receive loses its wake. A lost
//! value shows as no drop of it, two owners as two drops, and a lost wake as a future that is
//! still pending after the sender ended, with no wake of its waker.

use core::sync::atomic::Ordering;
use core::task::Poll;

use super::channel;
use crate::channel::tests::{budget, counted, poll};
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
        let (sender, receiver) = channel(&budget).unwrap();
        let producer = loom::thread::spawn(move || sender.send(make(1)).err().map(|SendError(value)| value));
        drop(receiver);
        if let Some(value) = producer.join().unwrap() {
            // The receiver was gone first: the value came back, and nothing dropped it.
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            drop(value);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
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
