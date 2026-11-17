use core::assert_matches;
use core::sync::atomic::Ordering;
use core::task::{Poll, Waker};

use super::channel;
use crate::channel::tests::{BOUND, Counted, budget, counted, poll};
#[cfg(feature = "std")]
use crate::channel::tests::{in_producer_order, tag};
use crate::channel::{Aborted, Closed, Refused, ReserveError, SendError, TooManyReceivers, TryRecvError, TrySendError};
use crate::wait::tests::counter;

#[test]
fn construction_refuses_with_a_typed_reason_and_keeps_no_charge() {
    let budget = budget();
    for (capacity, senders, receivers) in [(0, 1, 1), (1, 0, 1), (1, 1, 0)] {
        let refused = channel::<u64>(&budget, capacity, senders, receivers).unwrap_err();
        assert_eq!(refused, Refused::Zero);
    }
    assert_eq!(
        channel::<u64>(&budget, (1 << 27) + 1, 1, 1).unwrap_err(),
        Refused::TooLarge
    );
    assert_eq!(
        channel::<u64>(&budget, 1, 1, (1 << 16) + 1).unwrap_err(),
        Refused::TooLarge
    );
    // 2^17 slots of 16 bytes need more than the 2^20 units of the budget.
    assert_matches!(channel::<u64>(&budget, 1 << 17, 1, 1).unwrap_err(), Refused::Budget(_));
    assert_eq!(budget.available(), BOUND);
}

#[test]
fn the_charge_ends_when_each_endpoint_drops() {
    let budget = budget();
    let (sender, receiver) = channel::<u64>(&budget, 1000, 2, 2).unwrap();
    let clone = receiver.try_clone().unwrap();
    assert!(budget.available() <= BOUND - 16000);
    drop((sender, receiver));
    assert!(budget.available() <= BOUND - 16000);
    drop(clone);
    assert_eq!(budget.available(), BOUND);
}

#[test]
fn a_receiver_clone_takes_a_free_place_and_a_drop_frees_it() {
    let budget = budget();
    let (_sender, first) = channel::<u32>(&budget, 4, 1, 2).unwrap();
    let second = first.try_clone().unwrap();
    assert_eq!(second.try_clone().unwrap_err(), TooManyReceivers);
    drop(first);
    let third = second.try_clone().unwrap();
    assert_eq!(third.try_clone().unwrap_err(), TooManyReceivers);
}

#[test]
fn only_the_drop_of_the_last_receiver_aborts() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut sender, first) = channel(&budget, 4, 1, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    sender.try_send(make(1)).unwrap();
    sender.try_send(make(2)).unwrap();
    drop(first);
    assert_eq!(second.try_recv().unwrap().0, 1);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(second);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    assert_matches!(sender.try_send(make(3)), Err(TrySendError::Closed(Counted(3, _))));
}

#[test]
fn a_full_or_closed_send_returns_the_value() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 1, 1, 1).unwrap();
    sender.try_send(1).unwrap();
    assert_eq!(sender.try_send(2), Err(TrySendError::Full(2)));
    receiver.close();
    assert_eq!(sender.try_send(3), Err(TrySendError::Closed(3)));
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    let mut send = sender.send(4);
    assert_eq!(poll(&mut send, Waker::noop()), Poll::Ready(Err(SendError(4))));
}

#[test]
fn a_dropped_send_future_drops_its_value_and_never_reports_it_sent() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut sender, mut receiver) = channel(&budget, 1, 1, 1).unwrap();
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
fn a_free_slot_wakes_a_waiting_sender() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 1, 1, 1).unwrap();
    sender.try_send(1).unwrap();
    let (count, waker) = counter();
    let mut send = sender.send(2);
    assert!(poll(&mut send, &waker).is_pending());
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(count.get(), 1);
    assert_eq!(poll(&mut send, &waker), Poll::Ready(Ok(())));
    drop(send);
    assert_eq!(receiver.try_recv(), Ok(2));
}

#[test]
fn a_value_wakes_one_waiting_receiver_and_a_dropped_wait_passes_the_wake_on() {
    let budget = budget();
    let (mut sender, mut first) = channel(&budget, 2, 1, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    let (a, waker_a) = counter();
    let (b, waker_b) = counter();
    let mut recv_a = first.recv();
    let mut recv_b = second.recv();
    assert!(poll(&mut recv_a, &waker_a).is_pending());
    assert!(poll(&mut recv_b, &waker_b).is_pending());
    sender.try_send(1).unwrap();
    assert_eq!(a.get() + b.get(), 1);
    // The woken receiver gives up, so the wake goes to the other one.
    let (woken, mut other, other_waker) = if a.get() == 1 {
        (recv_a, recv_b, &waker_b)
    } else {
        (recv_b, recv_a, &waker_a)
    };
    drop(woken);
    assert_eq!((a.get(), b.get()), (1, 1));
    assert_eq!(poll(&mut other, other_waker), Poll::Ready(Ok(1)));
}

#[test]
fn two_values_reach_two_waiting_receivers() {
    let budget = budget();
    let (mut sender, mut first) = channel(&budget, 2, 1, 2).unwrap();
    let mut second = first.try_clone().unwrap();
    let (a, waker_a) = counter();
    let (b, waker_b) = counter();
    let mut recv_a = first.recv();
    let mut recv_b = second.recv();
    assert!(poll(&mut recv_a, &waker_a).is_pending());
    assert!(poll(&mut recv_b, &waker_b).is_pending());
    sender.try_send(1).unwrap();
    sender.try_send(2).unwrap();
    // The first value wakes one receiver. Its take wakes the other for the second value.
    let (mut woken, woken_waker, mut other, other_count, other_waker) = if a.get() == 1 {
        (recv_a, &waker_a, recv_b, &b, &waker_b)
    } else {
        (recv_b, &waker_b, recv_a, &a, &waker_a)
    };
    assert_eq!(other_count.get(), 0);
    assert_eq!(poll(&mut woken, woken_waker), Poll::Ready(Ok(1)));
    assert_eq!(other_count.get(), 1);
    assert_eq!(poll(&mut other, other_waker), Poll::Ready(Ok(2)));
}

#[test]
fn a_close_wakes_each_waiting_receiver() {
    let budget = budget();
    let (_sender, mut first) = channel::<u32>(&budget, 1, 1, 3).unwrap();
    let mut second = first.try_clone().unwrap();
    let mut closer = first.try_clone().unwrap();
    let (count, waker) = counter();
    let mut recv_a = first.recv();
    let mut recv_b = second.recv();
    assert!(poll(&mut recv_a, &waker).is_pending());
    assert!(poll(&mut recv_b, &waker).is_pending());
    closer.close();
    assert_eq!(count.get(), 2);
    assert_eq!(poll(&mut recv_a, &waker), Poll::Ready(Err(Closed)));
    assert_eq!(poll(&mut recv_b, &waker), Poll::Ready(Err(Closed)));
}

#[test]
fn a_close_wakes_each_waiting_sender_with_its_value() {
    let budget = budget();
    let (mut first, mut receiver) = channel(&budget, 1, 2, 1).unwrap();
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
fn a_partial_reservation_publishes_its_prefix_and_the_receivers_pass_the_rest() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 4, 1, 1).unwrap();
    let mut reservation = sender.try_reserve(3).unwrap();
    reservation.push(1).unwrap();
    reservation.push(2).unwrap();
    reservation.publish().unwrap();
    sender.try_send(3).unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Ok(3));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    // The slot that was not written is free again.
    assert_eq!(sender.try_reserve(5).unwrap_err(), ReserveError::TooLarge);
    sender.try_reserve(4).unwrap().publish().unwrap();
}

#[test]
fn a_drain_does_not_end_while_an_accepted_reservation_can_publish() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 4, 1, 1).unwrap();
    sender.try_send(1).unwrap();
    let mut reservation = sender.try_reserve(2).unwrap();
    reservation.push(2).unwrap();
    receiver.close();
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    reservation.publish().unwrap();
    assert_eq!(sender.try_send(9), Err(TrySendError::Closed(9)));
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
}

#[test]
fn an_abort_drops_the_values_and_a_later_publication_fails_and_drops_its_values_once() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut sender, receiver) = channel(&budget, 4, 1, 1).unwrap();
    sender.try_send(make(1)).unwrap();
    let mut reservation = sender.try_reserve(3).unwrap();
    reservation.push(make(2)).unwrap();
    reservation.push(make(3)).unwrap();
    drop(receiver);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(reservation.publish(), Err(Aborted));
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    drop(sender);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
}

/// A claim takes each slot of a reservation only when the slot is free for its lap. A claim that
/// checks only the first slot takes a slot that still holds a value, and holds more than the bound.
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
        let (sender, mut receiver) = channel(&budget, CAPACITY, 16, 1).unwrap();
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
fn each_value_goes_to_exactly_one_receiver_also_after_the_close() {
    use alloc::vec::Vec;
    use std::sync::{Arc, Barrier};

    const VALUES: u32 = 300;
    let budget = budget();
    // Room for each value, so the senders end, and so close the channel, before any receive.
    let (sender, receiver) = channel(&budget, 3 * VALUES, 4, 4).unwrap();
    let start = Arc::new(Barrier::new(4));
    let receivers: Vec<_> = (0..3)
        .map(|_| {
            let mut receiver = receiver.try_clone().unwrap();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                let mut values = Vec::new();
                while let Ok(value) = receiver.recv_blocking() {
                    values.push(value);
                }
                values
            })
        })
        .collect();
    drop(receiver);
    let senders: Vec<_> = (0..3)
        .map(|producer| {
            let mut sender = sender.try_clone().unwrap();
            std::thread::spawn(move || {
                for number in 0..VALUES {
                    sender.try_send(producer * VALUES + number).unwrap();
                }
            })
        })
        .collect();
    drop(sender);
    for sender in senders {
        sender.join().unwrap();
    }
    // The channel is closed and full. The receivers compete for each value.
    start.wait();
    let mut values: Vec<u32> = receivers
        .into_iter()
        .flat_map(|receiver| receiver.join().unwrap())
        .collect();
    values.sort_unstable();
    assert_eq!(values, (0..3 * VALUES).collect::<Vec<_>>());
}
