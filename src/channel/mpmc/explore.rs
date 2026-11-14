//! The named risk: competing receives lose or double a value; a close or the drop of the last
//! sender races waiting receivers and loses a wake; a publication with two waiting receivers
//! loses the second wake, or a dropped woken wait does not pass its wake on; an abort races a
//! publication and leaves a value with no owner or two owners; a clone or a drop of a receiver
//! races the place table.

use core::sync::atomic::Ordering;
use core::task::Poll;

use super::channel;
use crate::channel::tests::{budget, counted, poll};
use crate::channel::{Closed, SendError, TooManyReceivers, TryRecvError, TrySendError};
use crate::wait::tests::counter;

/// Checks `f` in each thread order within `bound` preemptions: 3 for two threads, 2 for three.
fn model(bound: usize, f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(bound);
    model.check(f);
}

#[test]
fn two_receivers_that_compete_take_each_value_once() {
    model(3, || {
        let budget = budget();
        let (mut sender, mut first) = channel(&budget, 2, 1, 2).unwrap();
        let mut second = first.try_clone().unwrap();
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        drop(sender);
        let other = loom::thread::spawn(move || [second.try_recv(), second.try_recv()]);
        let mine = [first.try_recv(), first.try_recv()];
        let theirs = other.join().unwrap();
        let mut taken: alloc::vec::Vec<u32> = mine.iter().chain(&theirs).filter_map(|result| result.ok()).collect();
        taken.sort_unstable();
        assert_eq!(taken, [1, 2]);
        assert_eq!(first.try_recv(), Err(TryRecvError::Closed));
    });
}

#[test]
fn a_publication_that_races_a_waiting_receive_is_not_lost() {
    model(3, || {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1, 1).unwrap();
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
fn the_drop_of_the_last_sender_wakes_two_waiting_receivers() {
    model(3, || {
        let budget = budget();
        let (sender, mut first) = channel::<u32>(&budget, 1, 1, 2).unwrap();
        let mut second = first.try_clone().unwrap();
        // The waiting thread does not join: see the loom join note in `src/wait/explore.rs`.
        let other = loom::thread::spawn(move || second.recv_blocking());
        let (count, waker) = counter();
        let mut recv = first.recv();
        let mut polled = poll(&mut recv, &waker);
        drop(sender);
        if polled.is_pending() {
            assert_eq!(count.get(), 1);
            polled = poll(&mut recv, &waker);
        }
        assert_eq!(polled, Poll::Ready(Err(Closed)));
        assert_eq!(other.join().unwrap(), Err(Closed));
    });
}

#[test]
fn a_dropped_woken_receive_passes_its_wake_on() {
    model(3, || {
        let budget = budget();
        let (mut sender, mut first) = channel(&budget, 1, 1, 2).unwrap();
        let mut second = first.try_clone().unwrap();
        let (_, waker_a) = counter();
        let (b, waker_b) = counter();
        let mut recv_a = first.recv();
        let mut recv_b = second.recv();
        assert!(poll(&mut recv_a, &waker_a).is_pending());
        assert!(poll(&mut recv_b, &waker_b).is_pending());
        let producer = loom::thread::spawn(move || sender.try_send(1).unwrap());
        // The first receiver gives up while the value comes: the wake reaches the second.
        drop(recv_a);
        producer.join().unwrap();
        assert_eq!(b.get(), 1);
        assert_eq!(poll(&mut recv_b, &waker_b), Poll::Ready(Ok(1)));
    });
}

#[test]
fn two_publications_reach_two_waiting_receivers() {
    model(3, || {
        let budget = budget();
        let (mut sender, mut first) = channel(&budget, 2, 1, 2).unwrap();
        let mut second = first.try_clone().unwrap();
        let (a, waker_a) = counter();
        let (b, waker_b) = counter();
        let mut recv_a = first.recv();
        let mut recv_b = second.recv();
        assert!(poll(&mut recv_a, &waker_a).is_pending());
        assert!(poll(&mut recv_b, &waker_b).is_pending());
        let producer = loom::thread::spawn(move || {
            sender.try_send(1).unwrap();
            sender.try_send(2).unwrap();
        });
        producer.join().unwrap();
        // The receivers run as an executor does: each wake leads to a poll. Each round of polls
        // gives a value, or a waiting receiver missed its wake.
        let mut results = [None, None];
        let mut seen = [0, 0];
        while results.contains(&None) {
            let mut polled = false;
            if results[0].is_none() && a.get() > seen[0] {
                seen[0] = a.get();
                polled = true;
                if let Poll::Ready(value) = poll(&mut recv_a, &waker_a) {
                    results[0] = Some(value);
                }
            }
            if results[1].is_none() && b.get() > seen[1] {
                seen[1] = b.get();
                polled = true;
                if let Poll::Ready(value) = poll(&mut recv_b, &waker_b) {
                    results[1] = Some(value);
                }
            }
            assert!(polled, "a waiting receiver missed its wake");
        }
        let mut values = results.map(|result| result.unwrap().unwrap());
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
    });
}

#[test]
fn a_drain_that_races_a_reservation_gets_its_value_before_the_end() {
    model(3, || {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1, 1).unwrap();
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
    model(3, || {
        let budget = budget();
        let (drops, make) = counted();
        let (mut sender, receiver) = channel(&budget, 2, 1, 1).unwrap();
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
        // The producer and the receivers are gone, so each value that was made is dropped, one
        // time. A value that an abort let in would stay in a slot.
        let made = producer.join().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), made);
    });
}

#[test]
fn a_receive_that_races_a_waiting_send_wakes_it() {
    model(3, || {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1, 1).unwrap();
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
fn a_close_that_races_a_waiting_send_ends_it_with_its_value() {
    model(3, || {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1, 1).unwrap();
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
fn a_receiver_clone_and_drop_that_race_use_each_place_once() {
    model(3, || {
        let budget = budget();
        let (_sender, first) = channel::<u32>(&budget, 1, 1, 2).unwrap();
        let second = first.try_clone().unwrap();
        let dropper = loom::thread::spawn(move || drop(second));
        let third = first.try_clone();
        dropper.join().unwrap();
        match third {
            Ok(third) => assert_eq!(third.try_clone().unwrap_err(), TooManyReceivers),
            Err(TooManyReceivers) => {
                let fourth = first.try_clone().unwrap();
                assert_eq!(fourth.try_clone().unwrap_err(), TooManyReceivers);
            }
        }
    });
}

// In the blocking model, the waiting thread is not the one that joins: see the loom join note in
// `src/wait/explore.rs`.

#[test]
fn a_blocking_receive_returns_after_a_send_that_races_it() {
    model(3, || {
        let budget = budget();
        let (mut sender, mut receiver) = channel(&budget, 1, 1, 1).unwrap();
        let consumer = loom::thread::spawn(move || receiver.recv_blocking());
        sender.try_send(1).unwrap();
        assert_eq!(consumer.join().unwrap(), Ok(1));
    });
}
