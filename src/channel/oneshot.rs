//! The one-shot channel: one value from one sender to one receiver, such as a reply, a join
//! result, or a startup outcome.
//!
//! [`channel`] makes a [`Sender`] and a [`Receiver`]. [`Sender::send`] takes the sender, so it
//! sends at most one value, and it never waits: the channel has room for the one value.
//!
//! ```
//! use core::num::NonZeroU64;
//! use std::thread;
//!
//! use rsasync::capacity::Budget;
//! use rsasync::channel::{Closed, oneshot};
//!
//! let budget = Budget::new(NonZeroU64::new(4096).unwrap());
//! let (sender, mut receiver) = oneshot::channel::<u32>(&budget)?;
//!
//! let worker = thread::spawn(move || sender.send(7));
//! assert_eq!(receiver.recv_blocking(), Ok(7));
//! assert!(worker.join().expect("the worker does not panic").is_ok());
//! // The one value is taken, and no other can come.
//! assert_eq!(receiver.recv_blocking(), Err(Closed));
//! # Ok::<(), rsasync::channel::Refused>(())
//! ```
//!
//! # Value ownership
//!
//! | Outcome | Owner of the value |
//! | --- | --- |
//! | [`Sender::send`] succeeds | The channel, until the receiver takes it or the receiver drops |
//! | [`Sender::send`] finds the receiver gone | The caller: [`SendError`] gives it back |
//! | The receiver drops before it takes a sent value | The receiver drops the value |
//! | A [`RecvFuture`] drops | The channel keeps the value: a later receive takes it |
//!
//! A drop of the sender with no send closes the channel: a receive then gives [`Closed`]. After
//! the receiver took the value, a receive gives [`Closed`] too.
//!
//! # Orderings
//!
//! One word, `state`, holds three flags: `VALUE`, `SENDER_GONE`, and `RECEIVER_GONE`. Each change
//! of the word is a read-modify-write, so the changes have one total order, and each change reads
//! the change before it. Thus the change that comes first in a race decides the owner of the
//! value:
//!
//! - A send against a drop of the receiver: the send writes the slot and then sets `VALUE` and
//!   `SENDER_GONE`, and the drop sets `RECEIVER_GONE`. If the drop comes first, the send reads
//!   `RECEIVER_GONE`, so no receiver saw the value, and the send takes it back. If the send comes
//!   first, the drop reads `VALUE`, so the receiver owns the value and drops it.
//! - The send sets the flags with `Release`, and the receiver reads them with `Acquire`, so the
//!   write of the slot happens before its read. The drop of the receiver also reads with
//!   `Acquire`. A send that takes its value back needs no `Acquire`: a drop of the receiver that
//!   comes first reads no slot, and the send reads only its own write.
//! - A send or a drop of the sender against a waiting receive: the sender first changes the word
//!   and then wakes the waiter of the receiver. The wait handshake (`crate::wait`) registers the
//!   waker before it rechecks the word, so the recheck sees the change, or the wake comes.

use core::alloc::AllocError;
use core::fmt;
use core::future::Future;
use core::mem::MaybeUninit;
use core::pin::Pin;
#[cfg(feature = "local")]
use core::ptr::NonNull;
use core::task::{Context, Poll};

use super::{Closed, Refused, SendError, TryRecvError};
use crate::capacity::{Budget, Permit};
use crate::sync::{
    Arc, UnsafeCell, arc_size,
    atomic::{AtomicU32, Ordering},
    try_arc,
};
use crate::wait::Waiter;

/// The slot holds the sent value.
const VALUE: u32 = 1;
/// The sender is gone: it sent its value, or it dropped with no send.
const SENDER_GONE: u32 = 1 << 1;
/// The receiver is gone.
const RECEIVER_GONE: u32 = 1 << 2;

struct Shared<T> {
    state: AtomicU32,
    /// The waiter of the receiver. The sender never waits.
    waiter: Waiter,
    slot: UnsafeCell<MaybeUninit<T>>,
    _charge: Permit,
}

impl<T> Shared<T> {
    /// Moves `value` into the slot.
    ///
    /// # Safety
    ///
    /// The slot is empty, and only the caller has access to it.
    #[expect(unsafe_code, reason = "the caller has the only access to the empty slot")]
    unsafe fn write(&self, value: T) {
        // SAFETY: only the caller has access to the slot.
        self.slot.with_mut(|slot| unsafe { (*slot).write(value) });
    }

    /// Moves the value out of the slot. The slot is then empty.
    ///
    /// # Safety
    ///
    /// The slot holds the value, and only the caller has access to it.
    #[expect(unsafe_code, reason = "the caller has the only access to the written slot")]
    unsafe fn take(&self) -> T {
        // SAFETY: the slot holds the value, and only the caller has access to it.
        self.slot.with(|slot| unsafe { (*slot).assume_init_read() })
    }

    /// Shows the written value to the receiver and wakes it, and ends the sender. Gives the value
    /// back when the receiver is gone.
    ///
    /// # Safety
    ///
    /// The sender wrote the slot, and it did not deliver it yet.
    #[expect(
        unsafe_code,
        reason = "before the delivery sets `VALUE`, and after a drop of the receiver, only the sender has the slot"
    )]
    unsafe fn deliver(&self) -> Option<T> {
        // Ordering: `Release` orders the write of the slot before the read of the receiver. The
        // read below needs no `Acquire`: a drop of the receiver that comes first reads no slot.
        if self.state.fetch_or(VALUE | SENDER_GONE, Ordering::Release) & RECEIVER_GONE != 0 {
            // SAFETY: the receiver was gone before `VALUE`, so it never reads the slot.
            return Some(unsafe { self.take() });
        }
        self.waiter.wake();
        None
    }
}

/// A value that a sender wrote to its channel, and that the receiver does not see yet. The drop
/// shows the value to the receiver and wakes it, or drops the value when the receiver is gone, so
/// the sender frees other resources first. The owner of a task stages its outcome so, and a join
/// sees the outcome only after the memory, the charge, and the slot of the task are free.
#[cfg(feature = "local")]
pub(crate) struct Staged {
    /// The shared state of the channel, from `Arc::into_raw`, with no type.
    shared: NonNull<()>,
    /// [`deliver_staged`] of the type of the value.
    deliver: unsafe fn(NonNull<()>),
}

/// Delivers the written value of the shared state `shared` of a channel of `T`, as
/// [`Shared::deliver`] does, drops it when the receiver is gone, and drops the reference of the
/// sender.
///
/// # Safety
///
/// `shared` is from `Arc::into_raw` of a channel of `T` whose slot the sender wrote, and this is the
/// one delivery of it.
#[cfg(feature = "local")]
#[expect(
    unsafe_code,
    reason = "the caller gives the reference of a sender that wrote its slot"
)]
unsafe fn deliver_staged<T>(shared: NonNull<()>) {
    // SAFETY: see the contract: the reference is the sender's, of a channel of `T`.
    let shared = unsafe { Arc::from_raw(shared.cast::<Shared<T>>().as_ptr().cast_const()) };
    // SAFETY: the slot is written, and this is its one delivery.
    drop(unsafe { shared.deliver() });
}

#[cfg(feature = "local")]
impl Drop for Staged {
    #[expect(unsafe_code, reason = "a `Staged` delivers its written value once, at its drop")]
    fn drop(&mut self) {
        // SAFETY: `stage` made `shared` and `deliver` from one channel whose slot it wrote, and
        // only this drop delivers it.
        unsafe { (self.deliver)(self.shared) };
    }
}

#[cfg(feature = "local")]
impl fmt::Debug for Staged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Staged").finish_non_exhaustive()
    }
}

#[expect(unsafe_code, reason = "the state word gives the slot to one endpoint at a time")]
// SAFETY: the slot is the sender's until the send sets `VALUE`, and then the receiver's. If the
// receiver was gone first, it stays the sender's (module docs). The value moves between threads,
// so `T` is `Send`.
unsafe impl<T: Send> Sync for Shared<T> {}

/// Makes a one-shot channel. It charges the shared state to `budget`, at its size in memory, before
/// it allocates. The charge ends when both endpoints drop.
///
/// # Errors
///
/// [`Refused::Budget`] when the budget has fewer free units than the size of the shared state, and
/// [`Refused::Allocation`] when the allocator does not give it.
pub fn channel<T>(budget: &Budget) -> Result<(Sender<T>, Receiver<T>), Refused> {
    let charge = u64::try_from(size::<T>()).map_err(|_| Refused::TooLarge)?;
    let charge = budget.reserve(charge).map_err(Refused::Budget)?;
    with_charge(charge).map_err(|_| Refused::Allocation)
}

/// The size of the shared state of a one-shot channel of `T`, which its charge pays for.
pub(crate) fn size<T>() -> usize {
    arc_size::<Shared<T>>()
}

/// Makes a one-shot channel whose shared state `charge` pays for. The caller reserved at least
/// [`size`] units for it. A refusal of the allocator ends the charge.
pub(crate) fn with_charge<T>(charge: Permit) -> Result<(Sender<T>, Receiver<T>), AllocError> {
    let shared = try_arc(Shared {
        state: AtomicU32::new(0),
        waiter: Waiter::new(),
        slot: UnsafeCell::new(MaybeUninit::uninit()),
        _charge: charge,
    })?;
    let sender = Sender {
        shared: Some(Arc::clone(&shared)),
    };
    let receiver = Receiver { shared, taken: false };
    Ok((sender, receiver))
}

/// The sending endpoint. Its drop with no send closes the channel: a receive then gives
/// [`Closed`].
pub struct Sender<T> {
    /// The shared state, which the send takes, so that the drop after a send does nothing.
    shared: Option<Arc<Shared<T>>>,
}

impl<T> Sender<T> {
    /// Sends `value`, and ends the sender. It never waits.
    ///
    /// # Errors
    ///
    /// [`SendError`] with the value when the receiver is gone.
    #[expect(
        unsafe_code,
        reason = "before the send sets `VALUE`, and after a drop of the receiver, only the sender has the slot"
    )]
    pub fn send(mut self, value: T) -> Result<(), SendError<T>> {
        let Some(shared) = self.shared.take() else {
            return Err(SendError(value));
        };
        // SAFETY: the slot is empty, and no receiver reads it before `VALUE`.
        unsafe { shared.write(value) };
        // SAFETY: the slot is written, and this is its one delivery.
        match unsafe { shared.deliver() } {
            Some(value) => Err(SendError(value)),
            None => Ok(()),
        }
    }
}

// `T: 'static`: a `Staged` names no lifetime, so it must not outlive data that the value borrows.
#[cfg(feature = "local")]
impl<T: 'static> Sender<T> {
    /// Writes `value` to the slot, and ends the sender. The receiver sees the value at the drop of
    /// the [`Staged`]. Gives the value back when the sender has no channel.
    #[expect(
        unsafe_code,
        reason = "before the delivery sets `VALUE`, only the sender has the slot"
    )]
    pub(crate) fn stage(mut self, value: T) -> Result<Staged, T> {
        let Some(shared) = self.shared.take() else {
            return Err(value);
        };
        // SAFETY: the slot is empty, and no receiver reads it before `VALUE`.
        unsafe { shared.write(value) };
        // SAFETY: `Arc::into_raw` gives a pointer that is not null.
        let shared = unsafe { NonNull::new_unchecked(Arc::into_raw(shared).cast_mut().cast()) };
        Ok(Staged {
            shared,
            deliver: deliver_staged::<T>,
        })
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            // Ordering: `Relaxed`: the change publishes no data, and the wake handshake orders it
            // before the recheck of a waiting receiver.
            shared.state.fetch_or(SENDER_GONE, Ordering::Relaxed);
            shared.waiter.wake();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

/// The receiving endpoint. Its drop drops a sent value that it did not take, and a later send
/// gives its value back.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The receiver took the value.
    taken: bool,
}

impl<T> Receiver<T> {
    /// Takes the value, with no wait.
    ///
    /// # Errors
    ///
    /// [`TryRecvError::Empty`] when no value is sent yet, and [`TryRecvError::Closed`] when the
    /// sender dropped with no send, or the receiver took the value.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        receive(&self.shared, &mut self.taken)
    }

    /// Gives a future that waits for the value. It gives [`Closed`] when the sender dropped with
    /// no send, or the receiver took the value.
    pub fn recv(&mut self) -> RecvFuture<'_, T> {
        RecvFuture {
            receiver: self,
            waits: false,
        }
    }

    /// Waits on this thread for the value. It gives [`Closed`] when the sender dropped with no
    /// send, or the receiver took the value.
    #[cfg(feature = "std")]
    pub fn recv_blocking(&mut self) -> Result<T, Closed> {
        let Self { shared, taken } = self;
        shared.waiter.wait(|| ready(shared, taken))
    }

    /// The handshake of a waiting receive, for a future that holds the receiver. After a
    /// `Pending`, the future calls [`Receiver::unwait`] if it drops before its result.
    pub(crate) fn poll_recv(&mut self, cx: &Context<'_>) -> Poll<Result<T, Closed>> {
        let Self { shared, taken } = self;
        shared.waiter.poll_wait(cx, || ready(shared, taken))
    }

    /// Ends the interest of a waiting receive that gave `Pending`.
    pub(crate) fn unwait(&self) {
        self.shared.waiter.cancel();
    }
}

/// Takes the value from `shared` for a receiver that took no value before, as `taken` tells.
#[expect(
    unsafe_code,
    reason = "after `VALUE`, the slot holds the value, which only the receiver takes"
)]
fn receive<T>(shared: &Shared<T>, taken: &mut bool) -> Result<T, TryRecvError> {
    if *taken {
        return Err(TryRecvError::Closed);
    }
    // Ordering: `Acquire` reads the release of the send, so the write of the slot happens before
    // its read.
    let state = shared.state.load(Ordering::Acquire);
    if state & VALUE != 0 {
        *taken = true;
        // SAFETY: the slot holds the sent value, and only the receiver takes it.
        return Ok(unsafe { shared.take() });
    }
    Err(if state & SENDER_GONE != 0 {
        TryRecvError::Closed
    } else {
        TryRecvError::Empty
    })
}

/// The result of a waiting receive, or `None` while no value is sent and the sender lives.
fn ready<T>(shared: &Shared<T>, taken: &mut bool) -> Option<Result<T, Closed>> {
    match receive(shared, taken) {
        Ok(value) => Some(Ok(value)),
        Err(TryRecvError::Closed) => Some(Err(Closed)),
        Err(TryRecvError::Empty) => None,
    }
}

impl<T> Drop for Receiver<T> {
    #[expect(
        unsafe_code,
        reason = "after a drop of the receiver, no receive follows, and a send reads the flag"
    )]
    fn drop(&mut self) {
        // Ordering: `Acquire` reads the release of the send, so the write of the slot happens
        // before its drop here.
        let state = self.shared.state.fetch_or(RECEIVER_GONE, Ordering::Acquire);
        if state & VALUE != 0 && !self.taken {
            // SAFETY: the send set `VALUE` before this change, so the slot holds the value, and
            // the receiver owns it.
            drop(unsafe { self.shared.take() });
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

/// A waiting receive, from [`Receiver::recv`]. A drop of the future takes no value: a later
/// receive takes it.
#[must_use = "a future does nothing unless you poll it"]
pub struct RecvFuture<'a, T> {
    receiver: &'a mut Receiver<T>,
    /// The future registered its waker, and has no result yet.
    waits: bool,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Result<T, Closed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        crate::coop::poll(cx, |cx| {
            let polled = this.receiver.poll_recv(cx);
            this.waits = polled.is_pending();
            polled
        })
    }
}

impl<T> Drop for RecvFuture<'_, T> {
    fn drop(&mut self) {
        if self.waits {
            self.receiver.unwait();
        }
    }
}

impl<T> fmt::Debug for RecvFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvFuture").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, loom))]
mod explore;
