use core::assert_matches;
use core::sync::atomic::Ordering;
use core::task::{Poll, Waker};

use super::channel;
use crate::channel::tests::{BOUND, Counted, budget, counted, poll};
#[cfg(feature = "std")]
use crate::channel::tests::{in_producer_order, tag};
use crate::channel::{Aborted, Closed, Refused, ReserveError, SendError, TooManySenders, TryRecvError, TrySendError};
use crate::wait::tests::counter;

#[test]
fn construction_refuses_with_a_typed_reason_and_keeps_no_charge() {
    let budget = budget();
    assert_eq!(channel::<u64>(&budget, 0, 1).unwrap_err(), Refused::Zero);
    assert_eq!(channel::<u64>(&budget, 1, 0).unwrap_err(), Refused::Zero);
    assert_eq!(
        channel::<u64>(&budget, (1 << 27) + 1, 1).unwrap_err(),
        Refused::TooLarge
    );
    assert_eq!(
        channel::<u64>(&budget, 1, (1 << 16) + 1).unwrap_err(),
        Refused::TooLarge
    );
    // 2^17 slots of 16 bytes need more than the 2^20 units of the budget.
    assert_matches!(channel::<u64>(&budget, 1 << 17, 1).unwrap_err(), Refused::Budget(_));
    assert_eq!(budget.available(), BOUND);
}

#[test]
fn the_charge_ends_when_the_receiver_and_each_sender_drop() {
    let budget = budget();
    let (sender, receiver) = channel::<u64>(&budget, 1000, 4).unwrap();
    let clone = sender.try_clone().unwrap();
    assert!(budget.available() <= BOUND - 8000);
    drop((sender, receiver));
    assert!(budget.available() <= BOUND - 8000);
    drop(clone);
    assert_eq!(budget.available(), BOUND);
}

#[test]
fn a_clone_takes_a_free_place_and_a_drop_frees_it() {
    let budget = budget();
    let (first, _receiver) = channel::<u32>(&budget, 4, 2).unwrap();
    let second = first.try_clone().unwrap();
    assert_eq!(second.try_clone().unwrap_err(), TooManySenders);
    drop(first);
    let third = second.try_clone().unwrap();
    assert_eq!(third.try_clone().unwrap_err(), TooManySenders);
}

#[test]
fn only_the_drop_of_the_last_sender_closes_the_channel() {
    let budget = budget();
    let (mut first, mut receiver) = channel(&budget, 4, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    first.try_send(1).unwrap();
    drop(first);
    second.try_send(2).unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    drop(second);
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
}

#[test]
fn a_full_or_closed_send_returns_the_value() {
    let budget = budget();
    let (mut first, mut receiver) = channel(&budget, 1, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    first.try_send(1).unwrap();
    // The bound is shared: the slot that the first sender holds is not free for the second.
    assert_eq!(second.try_send(2), Err(TrySendError::Full(2)));
    receiver.close();
    assert_eq!(second.try_send(3), Err(TrySendError::Closed(3)));
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    let mut send = first.send(4);
    assert_eq!(poll(&mut send, Waker::noop()), Poll::Ready(Err(SendError(4))));
}

#[test]
fn a_dropped_send_future_drops_its_value_and_never_reports_it_sent() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut sender, mut receiver) = channel(&budget, 1, 1).unwrap();
    sender.try_send(make(1)).unwrap();
    let mut send = sender.send(make(2));
    assert!(poll(&mut send, Waker::noop()).is_pending());
    // A slot is free when the future drops, so a drop that sends would succeed.
    assert_eq!(receiver.try_recv().unwrap().0, 1);
    drop(send);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);
}

#[test]
fn an_explicit_cancellation_returns_the_value_only_if_it_was_not_sent() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 1, 1).unwrap();
    sender.try_send(1).unwrap();
    let mut send = sender.send(2);
    assert!(poll(&mut send, Waker::noop()).is_pending());
    assert_eq!(send.cancel(), Some(2));
    assert_eq!(receiver.try_recv(), Ok(1));
    let mut send = sender.send(3);
    assert_eq!(poll(&mut send, Waker::noop()), Poll::Ready(Ok(())));
    assert_eq!(send.cancel(), None);
    assert_eq!(receiver.try_recv(), Ok(3));
}

#[test]
fn a_freed_slot_wakes_one_waiting_sender_and_a_dropped_wait_passes_the_wake_on() {
    let budget = budget();
    let (mut first, mut receiver) = channel(&budget, 1, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    first.try_send(1).unwrap();
    let (a, waker_a) = counter();
    let (b, waker_b) = counter();
    let mut send_a = first.send(2);
    let mut send_b = second.send(3);
    assert!(poll(&mut send_a, &waker_a).is_pending());
    assert!(poll(&mut send_b, &waker_b).is_pending());

    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(a.get() + b.get(), 1);
    // The woken sender gives up, so the wake goes to the other one.
    let (woken, mut other, other_waker) = if a.get() == 1 {
        (send_a, send_b, &waker_b)
    } else {
        (send_b, send_a, &waker_a)
    };
    drop(woken);
    assert_eq!((a.get(), b.get()), (1, 1));
    assert_eq!(poll(&mut other, other_waker), Poll::Ready(Ok(())));
}

#[test]
fn a_close_wakes_each_waiting_sender_with_its_value() {
    let budget = budget();
    let (mut first, mut receiver) = channel(&budget, 1, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    first.try_send(1).unwrap();
    let (count, waker) = counter();
    let mut send_a = first.send(2);
    let mut send_b = second.send(3);
    assert!(poll(&mut send_a, &waker).is_pending());
    assert!(poll(&mut send_b, &waker).is_pending());
    receiver.close();
    assert_eq!(count.get(), 2);
    assert_eq!(poll(&mut send_a, &waker), Poll::Ready(Err(SendError(2))));
    assert_eq!(poll(&mut send_b, &waker), Poll::Ready(Err(SendError(3))));
}

#[test]
fn a_partial_reservation_publishes_its_prefix_and_the_receiver_passes_the_rest() {
    let budget = budget();
    let (mut first, mut receiver) = channel(&budget, 4, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    let mut reservation = first.try_reserve(3).unwrap();
    reservation.push(1).unwrap();
    reservation.push(2).unwrap();
    second.try_send(3).unwrap();
    // The value of the second sender waits behind the reservation, which holds its place.
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    reservation.publish().unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Ok(3));
    // The slot that was not written is free again.
    assert_eq!(first.try_reserve(5).unwrap_err(), ReserveError::TooLarge);
    first.try_reserve(4).unwrap().publish().unwrap();
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn a_dropped_reservation_drops_its_values_and_publishes_none() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut sender, mut receiver) = channel(&budget, 2, 1).unwrap();
    let mut reservation = sender.try_reserve(2).unwrap();
    reservation.push(make(1)).unwrap();
    drop(reservation);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);
    sender.try_reserve(2).unwrap();
}

#[test]
fn a_drain_does_not_end_while_an_accepted_reservation_can_publish() {
    let budget = budget();
    let (mut first, mut receiver) = channel(&budget, 4, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    second.try_send(1).unwrap();
    let mut reservation = first.try_reserve(2).unwrap();
    reservation.push(2).unwrap();
    receiver.close();
    assert_eq!(second.try_send(9), Err(TrySendError::Closed(9)));
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    reservation.publish().unwrap();
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
}

#[test]
fn an_abort_drops_the_values_and_a_later_publication_fails_and_drops_its_values_once() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut first, receiver) = channel(&budget, 4, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    second.try_send(make(1)).unwrap();
    let mut reservation = first.try_reserve(3).unwrap();
    reservation.push(make(2)).unwrap();
    reservation.push(make(3)).unwrap();
    drop(receiver);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(reservation.publish(), Err(Aborted));
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    assert_matches!(second.try_send(make(4)), Err(TrySendError::Closed(Counted(4, _))));
    drop((first, second));
    assert_eq!(drops.load(Ordering::Relaxed), 4);
}

#[test]
fn a_pending_receive_is_woken_by_a_send_and_by_the_last_sender_drop() {
    let budget = budget();
    let (mut first, mut receiver) = channel(&budget, 1, 2).unwrap();
    let second = first.try_clone().unwrap();
    let (count, waker) = counter();
    let mut recv = receiver.recv();
    assert!(poll(&mut recv, &waker).is_pending());
    first.try_send(1).unwrap();
    assert_eq!(count.get(), 1);
    assert_eq!(poll(&mut recv, &waker), Poll::Ready(Ok(1)));
    drop(recv);

    let mut recv = receiver.recv();
    assert!(poll(&mut recv, &waker).is_pending());
    drop(first);
    assert_eq!(count.get(), 1);
    drop(second);
    assert_eq!(count.get(), 2);
    assert_eq!(poll(&mut recv, &waker), Poll::Ready(Err(Closed)));
}

#[cfg(feature = "std")]
#[test]
fn producers_that_clone_reserve_and_drop_never_hold_more_than_the_bound() {
    use alloc::vec::Vec;
    use core::sync::atomic::AtomicU32;
    use std::sync::Arc;

    // Not a power of two: a bound that a ring rounds up shows here.
    const CAPACITY: u32 = 6;
    let budget = budget();
    for _ in 0..50 {
        let (sender, mut receiver) = channel(&budget, CAPACITY, 16).unwrap();
        // The claimed slots: each value, and each slot of a reservation, published or not. No
        // receive runs while the producers work, so no slot becomes free again.
        let held = Arc::new(AtomicU32::new(0));
        let sent = Arc::new(AtomicU32::new(0));
        let producers = Arc::new(AtomicU32::new(0));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let mut sender = sender.try_clone().unwrap();
                let counts = [&held, &sent, &producers].map(Arc::clone);
                std::thread::spawn(move || {
                    let [held, sent, producers] = counts;
                    for step in 0..8 {
                        let mut clone = sender.try_clone().unwrap();
                        let id = producers.fetch_add(1, Ordering::Relaxed);
                        let mut number = 0;
                        if clone.try_send(tag(id, number)).is_ok() {
                            held.fetch_add(1, Ordering::Relaxed);
                            sent.fetch_add(1, Ordering::Relaxed);
                            number += 1;
                        }
                        if let Ok(mut reservation) = clone.try_reserve(2) {
                            held.fetch_add(2, Ordering::Relaxed);
                            reservation.push(tag(id, number)).unwrap();
                            if step % 2 == 0 {
                                reservation.publish().unwrap();
                                sent.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    let id = producers.fetch_add(1, Ordering::Relaxed);
                    if sender.try_send(tag(id, 0)).is_ok() {
                        held.fetch_add(1, Ordering::Relaxed);
                        sent.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        drop(sender);
        for thread in threads {
            thread.join().unwrap();
        }
        let held = held.load(Ordering::Relaxed);
        assert!(held <= CAPACITY, "the senders hold {held} slots of {CAPACITY}");
        let values: Vec<u32> = core::iter::from_fn(|| receiver.try_recv().ok()).collect();
        assert_eq!(values.len(), sent.load(Ordering::Relaxed) as usize);
        assert!(in_producer_order(&values));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    }
}

#[cfg(feature = "std")]
#[test]
fn blocking_waits_keep_the_order_of_each_sender_between_threads() {
    use alloc::vec::Vec;

    let budget = budget();
    let (sender, mut receiver) = channel(&budget, 2, 4).unwrap();
    let threads: Vec<_> = (0..3)
        .map(|producer| {
            let mut sender = sender.try_clone().unwrap();
            std::thread::spawn(move || {
                for number in 0..300 {
                    sender.send_blocking(tag(producer, number)).unwrap();
                }
            })
        })
        .collect();
    drop(sender);
    let mut values = Vec::new();
    while let Ok(value) = receiver.recv_blocking() {
        values.push(value);
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(values.len(), 900);
    assert!(in_producer_order(&values));
}
