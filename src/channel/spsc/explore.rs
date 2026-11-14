//! The named risk: a send, a receive, a close, or an abort that races a waiting endpoint loses
//! its wake, or a value that races an abort has no owner or two owners. A lost wake shows as a
//! future that is still pending after the other thread ended, with no wake of its latest waker,
//! or as a blocking wait that loom reports as a deadlock.

use core::sync::atomic::Ordering;
use core::task::Poll;

use super::channel;
use super::tests::{budget, counted, poll};
use crate::channel::{Closed, SendError, TryRecvError, TrySendError};
use crate::wait::tests::counter;

/// Checks `f` in each thread order within a preemption bound of 3.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

#[test]
fn a_send_that_races_a_pending_receive_is_not_lost() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
        let (count, waker) = counter();
        let producer = loom::thread::spawn(move || sender.try_send(1).unwrap());
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
fn a_receive_that_races_a_pending_send_is_not_lost() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
        sender.try_send(1).unwrap();
        let (count, waker) = counter();
        let consumer = loom::thread::spawn(move || {
            assert_eq!(receiver.try_recv(), Ok(1));
            receiver
        });
        let mut send = sender.send(2);
        let polled = poll(&mut send, &waker);
        let mut receiver = consumer.join().unwrap();
        if polled.is_pending() {
            assert_eq!(count.get(), 1);
            assert_eq!(poll(&mut send, &waker), Poll::Ready(Ok(())));
        }
        assert_eq!(receiver.try_recv(), Ok(2));
    });
}

#[test]
fn a_close_that_races_a_pending_send_ends_it_with_its_value() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
        sender.try_send(1).unwrap();
        let (count, waker) = counter();
        let closer = loom::thread::spawn(move || {
            receiver.close();
            receiver
        });
        let mut send = sender.send(2);
        let mut polled = poll(&mut send, &waker);
        let mut receiver = closer.join().unwrap();
        if polled.is_pending() {
            assert_eq!(count.get(), 1);
            polled = poll(&mut send, &waker);
        }
        assert_eq!(polled, Poll::Ready(Err(SendError(2))));
        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    });
}

#[test]
fn a_sender_drop_that_races_a_pending_receive_ends_it_after_the_last_value() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
        let (count, waker) = counter();
        let mut producer = Some(loom::thread::spawn(move || {
            sender.try_send(1).unwrap();
            drop(sender);
        }));
        // Each receive that is pending when the producer has ended must have its wake.
        let mut receive = || {
            let woken = count.get();
            let mut recv = receiver.recv();
            let polled = poll(&mut recv, &waker);
            if polled.is_ready() {
                return polled;
            }
            if let Some(producer) = producer.take() {
                producer.join().unwrap();
            }
            assert_eq!(count.get(), woken + 1);
            poll(&mut recv, &waker)
        };
        assert_eq!(receive(), Poll::Ready(Ok(1)));
        assert_eq!(receive(), Poll::Ready(Err(Closed)));
    });
}

#[test]
fn a_drain_that_races_a_reservation_gets_its_value_before_the_end() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
        let (count, waker) = counter();
        let producer = loom::thread::spawn(move || match sender.try_reserve(1) {
            Ok(mut reservation) => {
                reservation.push(7).unwrap();
                reservation.publish().unwrap();
                true
            }
            Err(_) => false,
        });
        receiver.close();
        let mut recv = receiver.recv();
        let mut polled = poll(&mut recv, &waker);
        let accepted = producer.join().unwrap();
        if polled.is_pending() {
            assert_eq!(count.get(), 1);
            polled = poll(&mut recv, &waker);
        }
        drop(recv);
        if accepted {
            assert_eq!(polled, Poll::Ready(Ok(7)));
            assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
        } else {
            assert_eq!(polled, Poll::Ready(Err(Closed)));
        }
    });
}

#[test]
fn an_abort_that_races_a_send_and_a_publication_drops_each_value_once() {
    model(|| {
        let budget = budget();
        let (drops, make) = counted();
        let (mut sender, receiver) = channel(&budget, 2).unwrap();
        let producer = loom::thread::spawn(move || {
            assert!(!matches!(sender.try_send(make(1)), Err(TrySendError::Full(_))));
            let Ok(mut reservation) = sender.try_reserve(1) else {
                return 1;
            };
            reservation.push(make(2)).unwrap();
            // A refused publication drops the value.
            let _published_or_refused = reservation.publish();
            2
        });
        drop(receiver);
        // The producer and the receiver are gone, so each value that was made is dropped, one
        // time. A value that an abort let in would stay in a slot.
        let made = producer.join().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), made);
    });
}

#[test]
fn a_blocking_receive_returns_after_a_send_that_races_it() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
        // The waiting thread does not join: see the loom join note in `src/wait/explore.rs`.
        let consumer = loom::thread::spawn(move || receiver.recv_blocking());
        sender.try_send(1).unwrap();
        assert_eq!(consumer.join().unwrap(), Ok(1));
    });
}

#[test]
fn a_blocking_send_returns_after_a_close_that_races_it() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
        sender.try_send(1).unwrap();
        // The waiting thread does not join: see the loom join note in `src/wait/explore.rs`.
        let producer = loom::thread::spawn(move || sender.send_blocking(2));
        receiver.close();
        assert_eq!(producer.join().unwrap(), Err(SendError(2)));
    });
}
