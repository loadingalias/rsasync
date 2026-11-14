//! The named risk: two claims race and lose, double, or reorder a slot, or pass the bound; a
//! close or an abort races a claim or a publication, and loses a wake, or leaves a value with no
//! owner or two owners; a clone or a drop races a waiting sender or a wake of one sender, and
//! leaks a place or loses a wake, also when a dropped wait passes its wake on.

use core::sync::atomic::Ordering;
use core::task::Poll;

use super::channel;
use crate::channel::tests::{budget, counted, poll};
use crate::channel::{Closed, SendError, TooManySenders, TryRecvError, TrySendError};
use crate::wait::tests::counter;

/// Checks `f` in each thread order within a preemption bound of 3.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

#[test]
fn two_claims_that_race_hold_the_bound_and_the_order_of_each_sender() {
    model(|| {
        let budget = budget();
        let (mut first, mut receiver) = channel(&budget, 2, 2).unwrap();
        let mut second = first.try_clone().unwrap();
        let other = loom::thread::spawn(move || [20, 21].map(|value| second.try_send(value).is_ok()));
        let mine = [10, 11].map(|value| first.try_send(value).is_ok());
        let theirs = other.join().unwrap();
        let sent = mine.iter().chain(&theirs).filter(|sent| **sent).count();
        assert_eq!(sent, 2);
        let values = [receiver.try_recv().unwrap(), receiver.try_recv().unwrap()];
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        // A sender that sent two values sent them in order.
        assert!(values != [11, 10] && values != [21, 20]);
    });
}

#[test]
fn a_publication_that_races_a_pending_receive_is_not_lost() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1).unwrap();
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
fn a_receive_that_races_a_waiting_send_wakes_it() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1).unwrap();
        sender.try_send(1).unwrap();
        let (count, waker) = counter();
        let consumer = loom::thread::spawn(move || {
            assert_eq!(receiver.try_recv(), Ok(1));
            receiver
        });
        let mut send = sender.send(2);
        let mut polled = poll(&mut send, &waker);
        let mut receiver = consumer.join().unwrap();
        if polled.is_pending() {
            assert_eq!(count.get(), 1);
            polled = poll(&mut send, &waker);
        }
        assert_eq!(polled, Poll::Ready(Ok(())));
        drop(send);
        assert_eq!(receiver.try_recv(), Ok(2));
    });
}

#[test]
fn a_dropped_wait_that_races_a_receive_passes_its_wake_on() {
    model(|| {
        let budget = budget();
        let (mut first, mut receiver) = channel(&budget, 1, 2).unwrap();
        let mut second = first.try_clone().unwrap();
        first.try_send(1).unwrap();
        let (_, waker_a) = counter();
        let (b, waker_b) = counter();
        let mut send_a = first.send(2);
        let mut send_b = second.send(3);
        assert!(poll(&mut send_a, &waker_a).is_pending());
        assert!(poll(&mut send_b, &waker_b).is_pending());
        let consumer = loom::thread::spawn(move || {
            assert_eq!(receiver.try_recv(), Ok(1));
            receiver
        });
        // The first sender gives up while the receive frees the slot: the wake reaches the second.
        drop(send_a);
        let mut receiver = consumer.join().unwrap();
        assert_eq!(b.get(), 1);
        assert_eq!(poll(&mut send_b, &waker_b), Poll::Ready(Ok(())));
        drop(send_b);
        assert_eq!(receiver.try_recv(), Ok(3));
    });
}

#[test]
fn a_close_that_races_a_waiting_send_ends_it_with_its_value() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1).unwrap();
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
fn a_drain_that_races_a_reservation_gets_its_value_before_the_end() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1).unwrap();
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
        let (mut sender, receiver) = channel(&budget, 2, 1).unwrap();
        let producer = loom::thread::spawn(move || {
            assert!(!matches!(sender.try_send(make(1)), Err(TrySendError::Full(_))));
            let Ok(mut reservation) = sender.try_reserve(1) else {
                return 1;
            };
            reservation.push(make(2)).unwrap();
            // A refused publication drops the value.
            let _published_or_aborted = reservation.publish();
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
fn the_drop_of_the_last_sender_that_races_a_pending_receive_ends_it_after_the_last_value() {
    model(|| {
        let budget = budget();
        let (first, mut receiver) = channel(&budget, 1, 2).unwrap();
        let mut second = first.try_clone().unwrap();
        drop(first);
        let (count, waker) = counter();
        let mut producer = Some(loom::thread::spawn(move || {
            second.try_send(1).unwrap();
            drop(second);
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
fn a_clone_and_a_drop_that_race_use_each_place_once() {
    model(|| {
        let budget = budget();
        let (first, _receiver) = channel::<u32>(&budget, 1, 2).unwrap();
        let second = first.try_clone().unwrap();
        let dropper = loom::thread::spawn(move || drop(second));
        let third = first.try_clone();
        dropper.join().unwrap();
        // Two places: a clone that took the freed place leaves none; a clone that came first
        // leaves the freed one.
        match third {
            Ok(third) => assert_eq!(third.try_clone().unwrap_err(), TooManySenders),
            Err(TooManySenders) => {
                let fourth = first.try_clone().unwrap();
                assert_eq!(fourth.try_clone().unwrap_err(), TooManySenders);
            }
        }
    });
}

// In the two blocking models, the waiting thread is not the one that joins: see the loom join
// note in `src/wait/explore.rs`.

#[test]
fn a_blocking_send_returns_after_a_receive_that_races_it() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1).unwrap();
        sender.try_send(1).unwrap();
        let producer = loom::thread::spawn(move || sender.send_blocking(2));
        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(producer.join().unwrap(), Ok(()));
        assert_eq!(receiver.try_recv(), Ok(2));
    });
}

#[test]
fn a_blocking_receive_returns_after_a_send_that_races_it() {
    model(|| {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1).unwrap();
        let consumer = loom::thread::spawn(move || receiver.recv_blocking());
        sender.try_send(1).unwrap();
        assert_eq!(consumer.join().unwrap(), Ok(1));
    });
}
