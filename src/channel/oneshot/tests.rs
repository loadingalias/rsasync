use core::sync::atomic::Ordering;
use core::task::{Poll, Waker};

use super::channel;
use crate::channel::tests::{BOUND, budget, counted, poll};
use crate::channel::{Closed, SendError, TryRecvError};
use crate::wait::tests::counter;

#[test]
fn a_sent_value_reaches_the_receiver_once_by_each_receive() {
    let budget = budget();
    let (sender, mut receiver) = channel(&budget).unwrap();
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    sender.send(1).unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));

    let (sender, mut receiver) = channel(&budget).unwrap();
    let (count, waker) = counter();
    let mut recv = receiver.recv();
    assert!(poll(&mut recv, &waker).is_pending());
    sender.send(2).unwrap();
    assert_eq!(count.get(), 1);
    assert_eq!(poll(&mut recv, &waker), Poll::Ready(Ok(2)));
    drop(recv);
    assert_eq!(poll(&mut receiver.recv(), &waker), Poll::Ready(Err(Closed)));

    #[cfg(feature = "std")]
    {
        let (sender, mut receiver) = channel(&budget).unwrap();
        let worker = std::thread::spawn(move || sender.send(3));
        assert_eq!(receiver.recv_blocking(), Ok(3));
        assert_eq!(worker.join().unwrap(), Ok(()));
        assert_eq!(receiver.recv_blocking(), Err(Closed));
    }
}

/// A send after the receiver dropped gives back the same value, which nothing dropped.
#[test]
fn a_send_after_the_receiver_dropped_returns_its_value() {
    let budget = budget();
    let (drops, make) = counted();
    let (sender, receiver) = channel(&budget).unwrap();
    drop(receiver);
    let SendError(value) = sender.send(make(7)).unwrap_err();
    assert_eq!((value.0, drops.load(Ordering::Relaxed)), (7, 0));
    drop(value);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn a_dropped_sender_closes_the_channel_and_wakes_the_receive() {
    let budget = budget();
    let (sender, mut receiver) = channel::<u32>(&budget).unwrap();
    drop(sender);
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));

    let (sender, mut receiver) = channel::<u32>(&budget).unwrap();
    let (count, waker) = counter();
    let mut recv = receiver.recv();
    assert!(poll(&mut recv, &waker).is_pending());
    drop(sender);
    assert_eq!(count.get(), 1);
    assert_eq!(poll(&mut recv, &waker), Poll::Ready(Err(Closed)));
}

/// A receive future that drops while it waits takes no value: the value of a later send stays in
/// the channel for the next receive.
#[test]
fn a_dropped_receive_future_takes_no_value() {
    let budget = budget();
    let (sender, mut receiver) = channel(&budget).unwrap();
    let mut recv = receiver.recv();
    assert!(poll(&mut recv, Waker::noop()).is_pending());
    drop(recv);
    sender.send(5).unwrap();
    assert_eq!(receiver.try_recv(), Ok(5));
}

/// A sent value that no receive takes drops once, with the receiver, and the charge of the channel
/// ends when both endpoints are gone.
#[test]
fn a_value_that_no_receive_takes_drops_once_with_the_receiver() {
    let budget = budget();
    let (drops, make) = counted();
    let (sender, receiver) = channel(&budget).unwrap();
    assert!(budget.available() < BOUND);
    sender.send(make(1)).unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(receiver);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(budget.available(), BOUND);
}
