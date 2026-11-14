use alloc::sync::Arc;
use core::future::Future;
use core::num::NonZeroU64;
use core::pin::Pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::{Context, Poll, Waker};

use super::channel;
use crate::capacity::Budget;
use crate::channel::{Aborted, Closed, Refused, ReserveError, SendError, TryRecvError, TrySendError};
use crate::wait::tests::counter;

const BOUND: u64 = 1 << 20;

pub(super) fn budget() -> Budget {
    Budget::new(NonZeroU64::new(BOUND).unwrap())
}

pub(super) fn poll<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(waker))
}

/// A value that counts the drops of each value with the same counter. It drives the code under
/// test, so it uses the native atomics.
#[derive(Debug)]
pub(super) struct Counted(pub(super) u32, Arc<AtomicU32>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

/// The drop counter, and a function that makes counted values.
pub(super) fn counted() -> (Arc<AtomicU32>, impl Fn(u32) -> Counted) {
    let drops = Arc::new(AtomicU32::new(0));
    let make = {
        let drops = Arc::clone(&drops);
        move |id| Counted(id, Arc::clone(&drops))
    };
    (drops, make)
}

#[test]
fn construction_refuses_with_a_typed_reason_and_keeps_no_charge() {
    let budget = budget();
    assert_eq!(channel::<u64>(&budget, 0).unwrap_err(), Refused::Zero);
    assert_eq!(channel::<u64>(&budget, (1 << 27) + 1).unwrap_err(), Refused::TooLarge);
    // 2^17 slots of 8 bytes need more than the 2^20 units of the budget.
    assert!(matches!(
        channel::<u64>(&budget, 1 << 17).unwrap_err(),
        Refused::Budget(_)
    ));
    assert_eq!(budget.available(), BOUND);
}

#[test]
fn the_slots_stay_charged_until_both_endpoints_drop() {
    let budget = budget();
    let (sender, receiver) = channel::<u64>(&budget, 1000).unwrap();
    assert!(budget.available() <= BOUND - 8000);
    drop(sender);
    assert!(budget.available() <= BOUND - 8000);
    drop(receiver);
    assert_eq!(budget.available(), BOUND);
}

#[test]
fn a_full_or_closed_send_returns_the_value() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
    sender.try_send(1).unwrap();
    assert_eq!(sender.try_send(2), Err(TrySendError::Full(2)));
    receiver.close();
    assert_eq!(sender.try_send(3), Err(TrySendError::Closed(3)));
    // The drain still gives the value that the channel accepted before the close.
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    assert_eq!(sender.try_send(4), Err(TrySendError::Closed(4)));
    let mut send = sender.send(5);
    assert_eq!(poll(&mut send, Waker::noop()), Poll::Ready(Err(SendError(5))));
}

#[test]
fn a_close_ends_a_pending_send_with_its_value() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
    sender.try_send(1).unwrap();
    let (count, waker) = counter();
    let mut send = sender.send(2);
    assert!(poll(&mut send, &waker).is_pending());
    receiver.close();
    assert_eq!(count.get(), 1);
    assert_eq!(poll(&mut send, &waker), Poll::Ready(Err(SendError(2))));
}

#[test]
fn a_dropped_send_future_drops_its_value_and_never_reports_it_sent() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
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
    let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
    sender.try_send(1).unwrap();
    let mut send = sender.send(2);
    assert!(poll(&mut send, Waker::noop()).is_pending());
    assert_eq!(send.cancel(), Some(2));
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

    let mut send = sender.send(3);
    assert_eq!(poll(&mut send, Waker::noop()), Poll::Ready(Ok(())));
    assert_eq!(send.cancel(), None);
    assert_eq!(receiver.try_recv(), Ok(3));
}

#[test]
fn a_partial_reservation_publishes_only_its_initialized_prefix() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 4).unwrap();
    let mut reservation = sender.try_reserve(3).unwrap();
    reservation.push(1).unwrap();
    reservation.push(2).unwrap();
    assert_eq!(reservation.remaining(), 1);
    reservation.publish().unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    // The slot that was not written is free again.
    assert_eq!(sender.try_reserve(5).unwrap_err(), ReserveError::TooLarge);
    sender.try_reserve(4).unwrap().publish().unwrap();
}

#[test]
fn a_dropped_reservation_drops_its_values_and_publishes_none() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut sender, mut receiver) = channel(&budget, 2).unwrap();
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
    let (mut sender, mut receiver) = channel(&budget, 4).unwrap();
    sender.try_send(1).unwrap();
    let mut reservation = sender.try_reserve(2).unwrap();
    reservation.push(2).unwrap();
    receiver.close();
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    reservation.publish().unwrap();
    assert_eq!(sender.try_reserve(1).unwrap_err(), ReserveError::Closed);
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));

    // A reservation that drops instead also ends the drain.
    let (mut sender, mut receiver) = channel::<u32>(&budget, 4).unwrap();
    let reservation = sender.try_reserve(1).unwrap();
    receiver.close();
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    drop(reservation);
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
}

#[test]
fn an_abort_drops_the_values_in_the_channel() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut sender, receiver) = channel(&budget, 4).unwrap();
    sender.try_send(make(1)).unwrap();
    sender.try_send(make(2)).unwrap();
    drop(receiver);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    assert!(matches!(
        sender.try_send(make(3)),
        Err(TrySendError::Closed(Counted(3, _)))
    ));
}

#[test]
fn a_publication_after_an_abort_fails_and_drops_each_written_value_once() {
    let budget = budget();
    let (drops, make) = counted();
    let (mut sender, receiver) = channel(&budget, 4).unwrap();
    let mut reservation = sender.try_reserve(3).unwrap();
    reservation.push(make(1)).unwrap();
    reservation.push(make(2)).unwrap();
    drop(receiver);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(reservation.publish(), Err(Aborted));
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(sender);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn a_pending_receive_is_woken_by_a_send_and_by_a_close() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 1).unwrap();
    let (count, waker) = counter();
    let mut recv = receiver.recv();
    assert!(poll(&mut recv, &waker).is_pending());
    sender.try_send(1).unwrap();
    assert_eq!(count.get(), 1);
    assert_eq!(poll(&mut recv, &waker), Poll::Ready(Ok(1)));
    drop(recv);

    let mut recv = receiver.recv();
    assert!(poll(&mut recv, &waker).is_pending());
    drop(sender);
    assert_eq!(count.get(), 2);
    assert_eq!(poll(&mut recv, &waker), Poll::Ready(Err(Closed)));
}

#[cfg(feature = "std")]
#[test]
fn blocking_waits_move_each_value_in_order_between_threads() {
    let budget = budget();
    let (mut sender, mut receiver) = channel(&budget, 2).unwrap();
    let producer = std::thread::spawn(move || {
        for value in 0..1000_u32 {
            sender.send_blocking(value).unwrap();
        }
    });
    for value in 0..1000_u32 {
        assert_eq!(receiver.recv_blocking(), Ok(value));
    }
    assert_eq!(receiver.recv_blocking(), Err(Closed));
    producer.join().unwrap();
}
