//! Bounded channels, which move values from producers to consumers with no scheduler or driver.
//!
//! [`spsc`] has one sender and one receiver. [`mpsc`] has many senders and one receiver.
//! [`oneshot`] moves one value, such as a reply or a startup outcome. Each send, receive, and
//! reservation has a nonblocking form, which is the kernel. The asynchronous waits, and with the
//! `std` feature the blocking waits, use the same publication and close rules. The outcome types in
//! this module state who owns the value after each result.
//!
//! A channel charges its slots and its shared state to a [`Budget`](crate::capacity::Budget)
//! before it allocates. A value that holds memory of its own carries its own charge, for example
//! a [`Permit`](crate::capacity::Permit) in the value: the channel does not examine its values.

use core::fmt;

use crate::capacity::Exhausted;

pub mod mpsc;
pub mod oneshot;
pub mod spsc;
#[cfg(test)]
mod tests;

/// A refusal to make a channel. A refusal keeps no charge and no memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The capacity is zero. A channel has at least one slot: a capacity of zero is not a
    /// rendezvous and not an unbounded channel.
    Zero,
    /// The capacity is larger than the topology permits, or its slots are too large for one
    /// allocation.
    TooLarge,
    /// The budget has fewer free units than the slots and the shared state of the channel.
    Budget(Exhausted),
    /// The allocator did not give the memory.
    Allocation,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => f.write_str("a channel cannot have a capacity of zero"),
            Self::TooLarge => f.write_str("the capacity of the channel is too large"),
            Self::Budget(refused) => write!(f, "the budget cannot charge the channel: {refused}"),
            Self::Allocation => f.write_str("the allocator did not give the memory of the channel"),
        }
    }
}

impl core::error::Error for Refused {}

/// A refusal of a nonblocking send. The value comes back.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// No slot is free. A later send can succeed.
    Full(T),
    /// The channel is closed. No later send can succeed.
    Closed(T),
}

// Not derived: the value need not be `Debug`, and the message does not show it.
impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("Full(..)"),
            Self::Closed(_) => f.write_str("Closed(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("no slot of the channel is free"),
            Self::Closed(_) => f.write_str("the channel is closed"),
        }
    }
}

impl<T> core::error::Error for TrySendError<T> {}

/// A refusal of a waiting send: the channel is closed. The value comes back.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the channel is closed")
    }
}

impl<T> core::error::Error for SendError<T> {}

/// A refusal of a nonblocking receive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// No value is ready. A later receive can succeed.
    Empty,
    /// The channel is closed, and no value is left or can still come. This result is terminal.
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("no value of the channel is ready"),
            Self::Closed => f.write_str("the channel is closed and empty"),
        }
    }
}

impl core::error::Error for TryRecvError {}

/// A refusal of a waiting receive: the channel is closed, and no value is left or can still come.
/// This result is terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the channel is closed and empty")
    }
}

impl core::error::Error for Closed {}

/// A failed send: the channel accepted a reservation, and then the receiver aborted the channel
/// before the publication. The channel dropped the written values of the reservation, as the
/// abort drops the values that are in the channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Aborted;

impl fmt::Display for Aborted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the receiver aborted the channel, so the channel dropped the values")
    }
}

impl core::error::Error for Aborted {}

/// A refusal of a reservation. A refusal reserves no slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReserveError {
    /// Fewer slots are free than the reservation asks for. A later reservation can succeed.
    Full,
    /// The channel is closed. No later reservation can succeed.
    Closed,
    /// The reservation asks for more slots than the capacity of the channel, so no reservation of
    /// that size can succeed.
    TooLarge,
}

impl fmt::Display for ReserveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("too few slots of the channel are free"),
            Self::Closed => f.write_str("the channel is closed"),
            Self::TooLarge => f.write_str("the reservation is larger than the channel"),
        }
    }
}

impl core::error::Error for ReserveError {}

/// A refusal to clone a sender: each sender place of the channel is in use. A later clone can
/// succeed after a sender drops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooManySenders;

impl fmt::Display for TooManySenders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("each sender place of the channel is in use")
    }
}

impl core::error::Error for TooManySenders {}
