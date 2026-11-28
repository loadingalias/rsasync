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
//! // The receive freed the channel before it returned, so the charge has ended.
//! assert_eq!(budget.available(), 4096);
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
//! A receive that gives the value or [`Closed`] frees the shared state of the channel before it
//! returns, so the charge has ended then. Thus a join of a task finds the budget free.
//!
//! # Orderings
//!
//! One word, `state`, holds four flags: `VALUE`, `ENDED`, `STORED`, and `RECEIVER_GONE`. Each
//! change of the word is a read-modify-write, so the changes have one total order, and each change
//! reads the change before it. The endpoint that makes the last access frees the shared state, on
//! its own thread:
//!
//! - The sender ends with one change that sets `ENDED`, and `VALUE` after it wrote the slot. When
//!   `STORED` is clear, that change is its last access.
//! - When `STORED` is set, that change gives the waker cell to the sender. The sender takes the
//!   waker and clears `STORED` with its last access. Then it wakes the waker, which it now holds.
//! - The sender has left when `ENDED` is set and `STORED` is clear. A receive that sees it makes
//!   the last access: it takes the value and frees the shared state.
//! - The receiver holds the cell while `ENDED` and `STORED` are clear. It writes its waker, and
//!   then sets `STORED` with a compare and exchange from the word that it read. The exchange fails
//!   only on an end of the sender, which did not touch the cell. To change a stored waker, the
//!   receiver first clears `STORED` with a compare and exchange.
//! - A receive while the sender holds the cell wakes its own waker and tries again, because the
//!   sender wakes the waker of an earlier poll. The sender holds the cell for a few instructions,
//!   and it runs no waker in that time.
//! - The drop of the receiver sets `RECEIVER_GONE`, and it frees when the sender has left. Else the
//!   last change of the sender reads the flag, and the sender frees. A send then gives back the
//!   value, which no receive saw.
//! - The changes use `AcqRel` and the loads use `Acquire`. Thus each access of one endpoint
//!   happens before a free by the other, the write of the slot before its read, and the write of
//!   the waker before its take. The exchange that takes a stored waker back needs no order: only
//!   the receiver wrote the cell, and the sender did not take it.

use alloc::boxed::Box;
use core::alloc::AllocError;
use core::fmt;
use core::future::Future;
use core::mem::{ManuallyDrop, MaybeUninit};
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::{Context, Poll};

use super::{Closed, Refused, SendError, TryRecvError};
use crate::capacity::{Budget, Permit};
#[cfg(feature = "std")]
use crate::sync::thread;
use crate::sync::{
    UnsafeCell,
    atomic::{AtomicU32, Ordering},
    hint,
};
use crate::wait::Wake;

/// The slot holds the sent value. The sender sets it with `ENDED`.
const VALUE: u32 = 1;
/// The sender ended: it sent its value, or it dropped with no send. While `STORED` is set too, it
/// still takes the waker.
const ENDED: u32 = 1 << 1;
/// The receiver is gone.
const RECEIVER_GONE: u32 = 1 << 2;
/// The cell holds the waker of a waiting receive. The change that sets `ENDED` gives the cell to
/// the sender.
const STORED: u32 = 1 << 3;

/// The sender made its last access in `state`: it ended, and it holds no waker.
const fn left(state: u32) -> bool {
    state & (ENDED | STORED) == ENDED
}

struct Shared<T> {
    state: AtomicU32,
    /// The waker of a waiting receive. The flags of `state` give the cell to one endpoint at a
    /// time.
    waker: UnsafeCell<Option<Wake>>,
    slot: UnsafeCell<MaybeUninit<T>>,
    _charge: Permit,
}

/// What a waiting receive found when it stored its waker.
enum Look {
    /// The sender left, with this state.
    Left(u32),
    /// The cell holds the waker.
    Stored,
    /// The sender holds the cell, and wakes the waker of an earlier poll.
    Held,
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

    /// Gives `f` the waker cell.
    ///
    /// # Safety
    ///
    /// The flags of `state` give the cell to the caller.
    #[expect(
        unsafe_code,
        reason = "the flags of the state word give the caller the only access to the cell"
    )]
    unsafe fn with_waker<R>(&self, f: impl FnOnce(&mut Option<Wake>) -> R) -> R {
        // SAFETY: the caller holds the cell, so no other access to it runs.
        self.waker.with_mut(|cell| f(unsafe { &mut *cell }))
    }

    /// Stores the waker that `put` writes to the cell, for a waiting receive. Gives what the
    /// receive found.
    #[expect(unsafe_code, reason = "with `ENDED` and `STORED` clear, the cell is the receiver's")]
    fn store(&self, put: impl FnOnce(&mut Option<Wake>)) -> Look {
        // Ordering: `Acquire` reads the last access of the sender (module docs).
        let mut state = self.state.load(Ordering::Acquire);
        if state & (ENDED | STORED) == STORED {
            // Take the cell back. Ordering: `Relaxed` on success (module docs). A failure reads an
            // end of the sender, as the load does.
            state = match self
                .state
                .compare_exchange(state, state & !STORED, Ordering::Relaxed, Ordering::Acquire)
            {
                Ok(_) => state & !STORED,
                Err(now) => now,
            };
        }
        if state & ENDED != 0 {
            return if left(state) { Look::Left(state) } else { Look::Held };
        }
        // SAFETY: `ENDED` and `STORED` are clear, so the cell is the receiver's.
        unsafe { self.with_waker(put) };
        // Ordering: `Release` gives the waker to the end of the sender. The exchange fails only on
        // an end of the sender that did not take the cell, and `Acquire` reads it.
        match self
            .state
            .compare_exchange(state, state | STORED, Ordering::Release, Ordering::Acquire)
        {
            Ok(_) => Look::Stored,
            Err(now) => Look::Left(now),
        }
    }
}

/// Ends the sender of `shared` with `ended`: `ENDED`, and `VALUE` when the slot holds a sent
/// value. Wakes a stored waker after the last access. When the receiver is gone, frees the shared
/// state, and gives back the value, which no receive saw.
///
/// # Safety
///
/// `shared` is the shared state of a sender that did not end, and the slot holds a value exactly
/// when `ended` has `VALUE`.
#[expect(
    unsafe_code,
    reason = "the state word gives the cell, and the last access, to one endpoint at a time"
)]
unsafe fn end<T>(shared: NonNull<Shared<T>>, ended: u32) -> Option<T> {
    // SAFETY: the sender did not end, so the shared state lives.
    let channel = unsafe { shared.as_ref() };
    // Ordering: `AcqRel` (module docs).
    let mut state = channel.state.fetch_or(ended, Ordering::AcqRel);
    let mut waker = None;
    if state & STORED != 0 {
        // SAFETY: the change set `ENDED` while `STORED` was set, so it gave the cell to the sender.
        waker = unsafe { channel.with_waker(Option::take) };
        // Ordering: `AcqRel`, as above.
        state = channel.state.fetch_and(!STORED, Ordering::AcqRel);
    }
    if state & RECEIVER_GONE == 0 {
        // That change was the last access: the receiver frees the shared state.
        if let Some(waker) = waker {
            waker.wake();
        }
        return None;
    }
    // The receiver dropped before the sender left, so it left the shared state to the sender.
    drop(waker);
    // SAFETY: the receiver is gone, and `VALUE` says that the slot holds the value.
    let value = (ended & VALUE != 0).then(|| unsafe { channel.take() });
    // SAFETY: the receiver is gone, and the slot is empty.
    unsafe { free(shared) };
    value
}

/// Frees the shared state `shared`, and so ends its charge. A waker in the cell drops with it.
///
/// # Safety
///
/// `shared` is from [`with_charge`], each other access to it ended, and the slot is empty.
#[expect(unsafe_code, reason = "the caller makes the last access")]
unsafe fn free<T>(shared: NonNull<Shared<T>>) {
    // SAFETY: `with_charge` made `shared` with `Box::leak`, and no access follows.
    drop(unsafe { Box::from_raw(shared.as_ptr()) });
}

/// A value that a sender wrote to its channel, and that the receiver does not see yet. The drop
/// shows the value to the receiver and wakes it, or drops the value when the receiver is gone, so
/// the sender frees other resources first. The owner of a task stages its outcome so, and a join
/// sees the outcome only after the memory, the charge, and the slot of the task are free.
#[cfg(feature = "local")]
pub(crate) struct Staged {
    /// The shared state of the channel, with no type.
    shared: NonNull<()>,
    /// [`deliver_staged`] of the type of the value.
    deliver: unsafe fn(NonNull<()>),
}

/// Ends the sender of the shared state `shared` of a channel of `T`, as a send does, and drops
/// the value when the receiver is gone.
///
/// # Safety
///
/// `shared` is the shared state of a channel of `T` whose sender wrote its slot and did not end.
#[cfg(feature = "local")]
#[expect(
    unsafe_code,
    reason = "the caller gives the shared state of a sender that wrote its slot"
)]
unsafe fn deliver_staged<T>(shared: NonNull<()>) {
    // SAFETY: see the contract.
    drop(unsafe { end(shared.cast::<Shared<T>>(), ENDED | VALUE) });
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

/// Makes a one-shot channel. It charges the shared state to `budget`, at its size in memory, before
/// it allocates. The charge ends when a receive gives the value or [`Closed`], or when both
/// endpoints drop.
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
pub(crate) const fn size<T>() -> usize {
    size_of::<Shared<T>>()
}

/// Makes a one-shot channel whose shared state `charge` pays for. The caller reserved at least
/// [`size`] units for it. A refusal of the allocator ends the charge.
pub(crate) fn with_charge<T>(charge: Permit) -> Result<(Sender<T>, Receiver<T>), AllocError> {
    let shared = NonNull::from(Box::leak(Box::try_new(Shared {
        state: AtomicU32::new(0),
        waker: UnsafeCell::new(None),
        slot: UnsafeCell::new(MaybeUninit::uninit()),
        _charge: charge,
    })?));
    Ok((Sender { shared }, Receiver { shared: Some(shared) }))
}

/// The sending endpoint. Its drop with no send closes the channel: a receive then gives
/// [`Closed`].
pub struct Sender<T> {
    /// The shared state. A send or a stage ends the sender with no drop.
    shared: NonNull<Shared<T>>,
}

impl<T> Sender<T> {
    /// Sends `value`, and ends the sender. It never waits.
    ///
    /// # Errors
    ///
    /// [`SendError`] with the value when the receiver is gone.
    #[expect(unsafe_code, reason = "before `ENDED`, only the sender has the slot")]
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        let shared = ManuallyDrop::new(self).shared;
        // SAFETY: the sender did not end, so the shared state lives. The slot is empty, and no
        // receive reads it before `ENDED`.
        unsafe { shared.as_ref().write(value) };
        // SAFETY: the sender did not end, and the slot holds the value.
        match unsafe { end(shared, ENDED | VALUE) } {
            Some(value) => Err(SendError(value)),
            None => Ok(()),
        }
    }
}

// `T: 'static`: a `Staged` names no lifetime, so it must not outlive data that the value borrows.
#[cfg(feature = "local")]
impl<T: 'static> Sender<T> {
    /// Writes `value` to the slot, and ends the sender. The receiver sees the value at the drop of
    /// the [`Staged`].
    #[expect(unsafe_code, reason = "before `ENDED`, only the sender has the slot")]
    pub(crate) fn stage(self, value: T) -> Staged {
        let shared = ManuallyDrop::new(self).shared;
        // SAFETY: as in `send`.
        unsafe { shared.as_ref().write(value) };
        Staged {
            shared: shared.cast(),
            deliver: deliver_staged::<T>,
        }
    }
}

impl<T> Drop for Sender<T> {
    #[expect(unsafe_code, reason = "a sender that drops did not end")]
    fn drop(&mut self) {
        // SAFETY: a send and a stage end the sender with no drop, so it did not end, and the slot
        // is empty.
        unsafe { end(self.shared, ENDED) };
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
    /// The shared state, until the receiver frees it: when it sees that the sender left, or at its
    /// drop.
    shared: Option<NonNull<Shared<T>>>,
}

impl<T> Receiver<T> {
    /// Takes the value, with no wait.
    ///
    /// # Errors
    ///
    /// [`TryRecvError::Empty`] when no value is sent yet, and [`TryRecvError::Closed`] when the
    /// sender dropped with no send, or the receiver took the value.
    #[expect(unsafe_code, reason = "the receiver did not free the shared state")]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let Some(shared) = self.shared else {
            return Err(TryRecvError::Closed);
        };
        // SAFETY: the receiver did not free the shared state. Ordering: `Acquire` reads the last
        // access of the sender (module docs).
        let state = unsafe { shared.as_ref() }.state.load(Ordering::Acquire);
        if left(state) {
            return self.finish(shared, state).map_err(|Closed| TryRecvError::Closed);
        }
        Err(TryRecvError::Empty)
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
    #[expect(unsafe_code, reason = "the receiver did not free the shared state")]
    pub fn recv_blocking(&mut self) -> Result<T, Closed> {
        let Some(shared) = self.shared else {
            return Err(Closed);
        };
        loop {
            // SAFETY: the receiver did not free the shared state.
            match unsafe { shared.as_ref() }.store(|cell| *cell = Some(Wake::Thread(thread::current()))) {
                Look::Left(state) => return self.finish(shared, state),
                // A wake can come before the park: the park then returns at once.
                Look::Stored => thread::park(),
                // The sender ends in a few instructions.
                Look::Held => hint::spin_loop(),
            }
        }
    }

    /// The handshake of a waiting receive, for a future that holds the receiver. After a
    /// `Pending`, the future calls [`Receiver::unwait`] if it drops before its result.
    #[expect(unsafe_code, reason = "the receiver did not free the shared state")]
    pub(crate) fn poll_recv(&mut self, cx: &Context<'_>) -> Poll<Result<T, Closed>> {
        let Some(shared) = self.shared else {
            return Poll::Ready(Err(Closed));
        };
        let waker = cx.waker();
        // SAFETY: the receiver did not free the shared state.
        let look = unsafe { shared.as_ref() }.store(|cell| match cell {
            Some(Wake::Task(stored)) if stored.will_wake(waker) => {}
            _ => *cell = Some(Wake::Task(waker.clone())),
        });
        match look {
            Look::Left(state) => Poll::Ready(self.finish(shared, state)),
            Look::Stored => Poll::Pending,
            // The sender wakes the waker of an earlier poll, which can be of another task. Wake
            // this one too, and give the sender time to end.
            Look::Held => {
                waker.wake_by_ref();
                hint::spin_loop();
                Poll::Pending
            }
        }
    }

    /// Ends the interest of a waiting receive that gave `Pending`: takes its waker back.
    #[expect(
        unsafe_code,
        reason = "the exchange that clears `STORED` gives the cell back to the receiver"
    )]
    pub(crate) fn unwait(&mut self) {
        let Some(shared) = self.shared else {
            return;
        };
        // SAFETY: the receiver did not free the shared state.
        let channel = unsafe { shared.as_ref() };
        // While the receiver lives and the sender did not end, the word holds `STORED` alone.
        // Ordering: `Relaxed`, as for each take back (module docs). A failure takes nothing.
        if channel
            .state
            .compare_exchange(STORED, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            // SAFETY: the exchange cleared `STORED` with `ENDED` clear, so the cell is the
            // receiver's.
            drop(unsafe { channel.with_waker(Option::take) });
        }
    }

    /// Takes the value that the sender left in `state`, or gives [`Closed`] when it left none,
    /// and frees the shared state, which ends its charge.
    #[expect(
        unsafe_code,
        reason = "after the sender left, only the receiver has the shared state"
    )]
    fn finish(&mut self, shared: NonNull<Shared<T>>, state: u32) -> Result<T, Closed> {
        self.shared = None;
        let value = if state & VALUE != 0 {
            // SAFETY: the sender left with `VALUE`, so the slot holds the value, and only the
            // receiver has it.
            Ok(unsafe { shared.as_ref().take() })
        } else {
            Err(Closed)
        };
        // SAFETY: the sender left, this receiver makes no access after it, and the slot is empty.
        unsafe { free(shared) };
        value
    }
}

impl<T> Drop for Receiver<T> {
    #[expect(unsafe_code, reason = "the receiver did not free the shared state")]
    fn drop(&mut self) {
        let Some(shared) = self.shared else {
            return;
        };
        // SAFETY: the receiver did not free the shared state. Ordering: `AcqRel` (module docs).
        let state = unsafe { shared.as_ref() }
            .state
            .fetch_or(RECEIVER_GONE, Ordering::AcqRel);
        if left(state) {
            // The sender left, so the receiver frees the shared state, with a sent value that it
            // did not take.
            drop(self.finish(shared, state));
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

#[expect(
    unsafe_code,
    reason = "the state word gives the slot and the cell to one endpoint at a time"
)]
// SAFETY: the endpoints move the value and a waker between threads, so `T` is `Send`. A shared
// endpoint gives no access to the shared state.
unsafe impl<T: Send> Send for Sender<T> {}

#[expect(unsafe_code, reason = "a shared sender gives no access to the shared state")]
// SAFETY: as for `Send`.
unsafe impl<T: Send> Sync for Sender<T> {}

#[expect(
    unsafe_code,
    reason = "the state word gives the slot and the cell to one endpoint at a time"
)]
// SAFETY: as for the sender.
unsafe impl<T: Send> Send for Receiver<T> {}

#[expect(unsafe_code, reason = "a shared receiver gives no access to the shared state")]
// SAFETY: as for the sender.
unsafe impl<T: Send> Sync for Receiver<T> {}

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
