//! The bounded channel with one sender and one receiver.
//!
//! [`channel`] makes a [`Sender`] and a [`Receiver`]. Neither endpoint can be cloned, and each
//! operation borrows its endpoint mutably, so each endpoint has at most one operation at a time.
//! The receiver gets the values in the order that the sender published them.
//!
//! ```
//! use core::num::NonZeroU64;
//! use std::thread;
//!
//! use rsasync::capacity::Budget;
//! use rsasync::channel::spsc;
//!
//! type Error = Box<dyn core::error::Error + Send + Sync>;
//!
//! let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
//! let (mut sender, mut receiver) = spsc::channel::<u32>(&budget, 8)?;
//!
//! let producer = thread::spawn(move || {
//!     // One value: wait for a free slot, then send.
//!     sender.send_blocking(1)?;
//!     // A batch: reserve 4 slots, write 2, and publish only the 2 written values.
//!     let mut batch = sender.try_reserve(4)?;
//!     for value in [2, 3] {
//!         batch.push(value).expect("the reservation has 4 slots");
//!     }
//!     batch.publish()?;
//!     // The sender drops here, which closes the channel.
//!     Ok::<(), Error>(())
//! });
//!
//! // Each receive waits for the next value. After the last value, it gives `Closed`.
//! let mut values = Vec::new();
//! while let Ok(value) = receiver.recv_blocking() {
//!     values.push(value);
//! }
//! assert_eq!(values, [1, 2, 3]);
//! producer.join().expect("the producer does not panic")?;
//! # Ok::<(), Error>(())
//! ```
//!
//! # Value ownership
//!
//! | Outcome | Owner of the value |
//! | --- | --- |
//! | A send succeeds | The channel, until the receiver takes it or the receiver drops |
//! | [`Sender::try_send`] finds the channel full or closed | The caller: [`TrySendError`] gives it back |
//! | A waiting send finds the channel closed | The caller: [`SendError`] gives it back |
//! | A pending [`SendFuture`] drops | The future drops it. It was never sent |
//! | [`SendFuture::cancel`] | The caller: the value comes back |
//! | A value is written into a [`Reservation`] | The reservation, until [`Reservation::publish`] |
//! | A reservation drops with no publication | The reservation drops its values. They were never sent |
//! | The receiver drops | The receiver drops each value that is still in the channel |
//! | A failed send: the publication of an accepted reservation comes after the abort | The channel drops the written values, as the abort drops the values in the channel. [`Reservation::publish`] gives [`Aborted`] |
//!
//! A reservation larger than the capacity is a refusal, [`ReserveError::TooLarge`], and moves no
//! value.
//!
//! # Close
//!
//! - A drop of the sender closes the channel. The receiver gets each value that is still in the
//!   channel, and then [`Closed`].
//! - [`Receiver::close`] is the drain. Later sends and reservations fail, and a pending send ends
//!   with its value. An accepted reservation can still publish its values. The receiver gets
//!   [`Closed`] only when the channel is empty and no accepted reservation can still publish.
//! - A drop of the receiver is the abort. The receiver drops the values that are in the channel.
//!   Later sends fail with their value. The publication of an accepted reservation fails with
//!   [`Aborted`], and the channel drops its written values.
//!
//! # Orderings
//!
//! Two words hold the state. The sender advances the written position in the `written` word,
//! and the receiver advances the read position in the `read` word. Each change of a word is a
//! read-modify-write, so the changes of each word have one total order:
//!
//! - The `written` word also holds the flags of close, abort, and an accepted reservation, and
//!   the bit of a waiting receiver. A send changes the position only if the word has no close
//!   flag, in the same change. Thus each send is before or after the close, also for the drain
//!   and the abort, and a drain sees each accepted reservation.
//! - The `read` word also holds the bit of a waiting sender.
//! - A publication of each word is `Release`, and the other endpoint loads it with `Acquire`.
//!   Thus the write of a slot happens before its read, and its read before its next write.
//! - A wait registers its waker in the waiter of its endpoint, and then sets its bit with an
//!   `AcqRel` change, which also rechecks the position. The publication that clears the bit
//!   reads that change with `Acquire`, so it wakes the new waker. A publication before the bit
//!   is set is in the value that the change reads.
//! - A close changes the `written` word and then wakes the waiter of the sender. The wait
//!   handshake of the waiter (`crate::wait`) orders the close before the recheck of the sender.
//!
//! rtrb publishes each position with a plain store, but it has no close and no wait. A wait needs
//! a store-load order on the publication path: the publisher stores the position and then loads
//! the waiting bit. One read-modify-write takes the place of the store, a fence, and the load.

use core::alloc::Layout;
use core::fmt;
use core::future::Future;
use core::mem::{ManuallyDrop, MaybeUninit};
use core::pin::Pin;
use core::task::{Context, Poll};

use alloc::boxed::Box;
use alloc::vec::Vec;

use super::{Aborted, Closed, Refused, ReserveError, SendError, TryRecvError, TrySendError};
use crate::capacity::{Budget, Permit};
#[cfg(feature = "std")]
use crate::sync::thread;
use crate::sync::{
    Arc, UnsafeCell,
    atomic::{AtomicU32, Ordering},
};
use crate::wait::Waiter;

/// The bits of a position in each word. A position is below two times the capacity.
const POSITION: u32 = (1 << 28) - 1;
/// In the `written` word: the receiver waits for a value or for the end of a reservation.
const RECEIVER_WAITS: u32 = 1 << 28;
/// In the `written` word: an accepted reservation can still publish.
const RESERVED: u32 = 1 << 29;
/// In the `written` word: the channel accepts no new send or reservation.
const CLOSED: u32 = 1 << 30;
/// In the `written` word: the receiver is gone, so no value can be published.
const ABORTED: u32 = 1 << 31;
/// In the `read` word: the sender waits for a free slot.
const SENDER_WAITS: u32 = 1 << 28;
/// The largest capacity: two times it is the largest position, which needs 28 bits.
const MAX_CAPACITY: u32 = 1 << 27;

type Slot<T> = UnsafeCell<MaybeUninit<T>>;

/// One word with the waiter that its publications wake. The alignment puts each on its own cache
/// line, also on targets with lines of 128 bytes, so the endpoints do not share a line.
#[repr(align(128))]
struct End {
    word: AtomicU32,
    waiter: Waiter,
}

impl End {
    fn new() -> Self {
        Self {
            word: AtomicU32::new(0),
            waiter: Waiter::new(),
        }
    }
}

/// Changes `word` to `change` of its value, with `order` on success, unless `change` gives `None`.
/// Gives the value before the change. A refusal reads no data, so it has no order.
fn update(word: &AtomicU32, order: Ordering, change: impl Fn(u32) -> Option<u32>) -> Option<u32> {
    let mut current = word.load(Ordering::Relaxed);
    loop {
        let next = change(current)?;
        match word.compare_exchange_weak(current, next, order, Ordering::Relaxed) {
            Ok(_) => return Some(current),
            Err(actual) => current = actual,
        }
    }
}

struct Shared<T> {
    /// The written position and the flags, with the waiter of the receiver.
    written: End,
    /// The read position and the bit of a waiting sender, with the waiter of the sender.
    read: End,
    slots: Box<[Slot<T>]>,
    capacity: u32,
    _charge: Permit,
}

impl<T> Shared<T> {
    /// The position `count` slots after `position`.
    fn advance(&self, position: u32, count: u32) -> u32 {
        // No overflow: the position is below 2^28 and the count is at most 2^27.
        let next = position + count;
        let wrap = 2 * self.capacity;
        if next >= wrap { next - wrap } else { next }
    }

    /// The number of values from the read position to the written position.
    fn len(&self, read: u32, written: u32) -> u32 {
        if written >= read {
            written - read
        } else {
            written + 2 * self.capacity - read
        }
    }

    fn slot(&self, position: u32) -> &Slot<T> {
        let index = if position < self.capacity {
            position
        } else {
            position - self.capacity
        };
        &self.slots[index as usize]
    }

    /// Moves `value` into the slot of `position`.
    ///
    /// # Safety
    ///
    /// The slot is free, and only the caller has access to it.
    #[expect(unsafe_code, reason = "the caller has the only access to the free slot")]
    unsafe fn write(&self, position: u32, value: T) {
        // SAFETY: only the caller has access to the slot.
        self.slot(position).with_mut(|slot| unsafe { (*slot).write(value) });
    }

    /// Moves the value out of the slot of `position`. The slot is then free.
    ///
    /// # Safety
    ///
    /// The slot holds a value, and only the caller has access to it.
    #[expect(unsafe_code, reason = "the caller has the only access to the written slot")]
    unsafe fn take(&self, position: u32) -> T {
        // SAFETY: the slot holds a value, and only the caller has access to it.
        self.slot(position).with(|slot| unsafe { (*slot).assume_init_read() })
    }

    /// Drops the values of `count` slots from `position`.
    ///
    /// # Safety
    ///
    /// Each of the slots holds a value, and only the caller has access to them.
    #[expect(unsafe_code, reason = "the caller has the only access to the written slots")]
    unsafe fn drop_values(&self, position: u32, count: u32) {
        if !core::mem::needs_drop::<T>() {
            return;
        }
        for offset in 0..count {
            let slot = self.slot(self.advance(position, offset));
            // SAFETY: the slot holds a value, and only the caller has access to it.
            slot.with_mut(|slot| unsafe { (*slot).assume_init_drop() });
        }
    }
}

#[expect(unsafe_code, reason = "the positions give each slot to one endpoint at a time")]
// SAFETY: a slot is the sender's while it is free and the receiver's while it holds a published
// value. The changes of the two words order each handover (module docs). The values move between
// threads, so `T` is `Send`.
unsafe impl<T: Send> Sync for Shared<T> {}

/// Makes a channel of `capacity` slots. It charges the slots and the shared state to `budget`,
/// at their size in memory, before it allocates. The charge ends when both endpoints drop.
///
/// The capacity is at least 1 and at most 2^27.
///
/// ```
/// use core::num::NonZeroU64;
///
/// use rsasync::capacity::Budget;
/// use rsasync::channel::{Refused, TryRecvError, spsc};
///
/// let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
/// assert_eq!(spsc::channel::<u64>(&budget, 0).unwrap_err(), Refused::Zero);
///
/// let (mut sender, mut receiver) = spsc::channel(&budget, 2)?;
/// sender.try_send(1)?;
/// sender.try_send(2)?;
/// // The channel is full: the value comes back.
/// assert_eq!(sender.try_send(3).unwrap_err().to_string(), "no slot of the channel is free");
///
/// drop(sender);
/// assert_eq!(receiver.try_recv(), Ok(1));
/// assert_eq!(receiver.try_recv(), Ok(2));
/// assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
/// # Ok::<(), Box<dyn core::error::Error>>(())
/// ```
pub fn channel<T>(budget: &Budget, capacity: u32) -> Result<(Sender<T>, Receiver<T>), Refused> {
    if capacity == 0 {
        return Err(Refused::Zero);
    }
    if capacity > MAX_CAPACITY {
        return Err(Refused::TooLarge);
    }
    let slots = Layout::array::<Slot<T>>(capacity as usize).map_err(|_| Refused::TooLarge)?;
    let charge = slots
        .size()
        .checked_add(size_of::<Shared<T>>())
        .and_then(|size| u64::try_from(size).ok())
        .ok_or(Refused::TooLarge)?;
    let charge = budget.reserve(charge).map_err(Refused::Budget)?;

    let mut slots = Vec::new();
    slots
        .try_reserve_exact(capacity as usize)
        .map_err(|_| Refused::Allocation)?;
    slots.extend((0..capacity).map(|_| UnsafeCell::new(MaybeUninit::uninit())));
    let shared = Arc::new(Shared {
        written: End::new(),
        read: End::new(),
        slots: slots.into_boxed_slice(),
        capacity,
        _charge: charge,
    });
    let sender = Sender {
        shared: Arc::clone(&shared),
        written: 0,
        read: 0,
    };
    let receiver = Receiver {
        shared,
        read: 0,
        written: 0,
    };
    Ok((sender, receiver))
}

/// The sending endpoint. Its drop closes the channel: the receiver gets the values that are left,
/// and then [`Closed`].
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    /// The written position. Only the sender changes it.
    written: u32,
    /// The read position at the last load. The receiver can only advance it.
    read: u32,
}

impl<T> Sender<T> {
    /// Sends `value` if a slot is free and the channel is open. Otherwise the value comes back.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        if self.free(1) == 0 {
            return Err(if self.closed() {
                TrySendError::Closed(value)
            } else {
                TrySendError::Full(value)
            });
        }
        self.send_free(value).map_err(TrySendError::Closed)
    }

    /// Gives a future that waits for a free slot and then sends `value`. The future holds the
    /// value until the send: a drop of the future drops it, and [`SendFuture::cancel`] gives it
    /// back. A closed channel gives the value back in [`SendError`].
    pub fn send(&mut self, value: T) -> SendFuture<'_, T> {
        SendFuture {
            sender: self,
            value: Some(value),
            waits: false,
        }
    }

    /// Waits on this thread for a free slot, and then sends `value`. A closed channel gives the
    /// value back in [`SendError`].
    #[cfg(feature = "std")]
    pub fn send_blocking(&mut self, value: T) -> Result<(), SendError<T>> {
        let mut waits = false;
        let mut value = value;
        loop {
            match self.send_step(value, &mut waits, Waiter::register_thread) {
                Ok(result) => return result,
                Err(back) => value = back,
            }
            // A wake can come before the park: the park then returns at once.
            thread::park();
        }
    }

    /// Reserves `count` free slots, which the [`Reservation`] fills and then publishes. An
    /// accepted reservation can publish also after [`Receiver::close`].
    pub fn try_reserve(&mut self, count: u32) -> Result<Reservation<'_, T>, ReserveError> {
        if count > self.shared.capacity {
            return Err(ReserveError::TooLarge);
        }
        if self.free(count) < count {
            return Err(if self.closed() {
                ReserveError::Closed
            } else {
                ReserveError::Full
            });
        }
        // Ordering: `Relaxed`: the acceptance publishes no data. The total order of the word puts
        // it before or after a close, and a close after it reads the flag.
        update(&self.shared.written.word, Ordering::Relaxed, |word| {
            (word & CLOSED == 0).then_some(word | RESERVED)
        })
        .ok_or(ReserveError::Closed)?;
        Ok(Reservation {
            sender: self,
            count,
            written: 0,
        })
    }

    /// The number of free slots, at least as many as `count` if that many are free. It loads the
    /// read position only when the last load shows fewer.
    fn free(&mut self, count: u32) -> u32 {
        let shared = &*self.shared;
        let mut free = shared.capacity - shared.len(self.read, self.written);
        if free < count {
            // Ordering: `Acquire` reads the release of each receive, so the read of each slot
            // happens before the sender writes it again.
            self.read = shared.read.word.load(Ordering::Acquire) & POSITION;
            free = shared.capacity - shared.len(self.read, self.written);
        }
        free
    }

    fn closed(&self) -> bool {
        // Ordering: `Relaxed`: the flag publishes no data. A wait sees it by the order of the
        // waiter (module docs).
        self.shared.written.word.load(Ordering::Relaxed) & CLOSED != 0
    }

    /// Writes `value` into the next slot and publishes it. Gives the value back when the channel
    /// is closed. The caller saw that the slot is free.
    #[expect(
        unsafe_code,
        reason = "the slot at the written position is free, and only the sender writes it"
    )]
    fn send_free(&mut self, value: T) -> Result<(), T> {
        // SAFETY: the slot is free, and only the sender has access to a free slot.
        unsafe { self.shared.write(self.written, value) };
        if self.publish(1, CLOSED) {
            Ok(())
        } else {
            // SAFETY: the publication failed, so the slot is still the sender's, with the value.
            Err(unsafe { self.shared.take(self.written) })
        }
    }

    /// Publishes the `count` written slots after the written position, unless the word has a
    /// flag of `refused`. Clears the flag of a reservation, and wakes a waiting receiver.
    fn publish(&mut self, count: u32, refused: u32) -> bool {
        let shared = &*self.shared;
        let next = shared.advance(self.written, count);
        // Ordering: `Release` orders the writes of the slots before the load of the receiver.
        // `Acquire` reads the release of a waiting receiver, so its waker is registered. A refusal
        // reads no data, so `Relaxed`.
        let changed = update(&shared.written.word, Ordering::AcqRel, |word| {
            (word & refused == 0).then_some(next | (word & (CLOSED | ABORTED)))
        });
        let Some(word) = changed else {
            return false;
        };
        self.written = next;
        if word & RECEIVER_WAITS != 0 {
            shared.written.waiter.wake();
        }
        true
    }

    /// One step of a waiting send: sends `value`, or registers with `register`, rechecks, and
    /// gives the value back to wait for a wake.
    fn send_step(
        &mut self,
        value: T,
        waits: &mut bool,
        register: impl FnOnce(&Waiter),
    ) -> Result<Result<(), SendError<T>>, T> {
        if self.free(1) == 0 && !self.closed() {
            register(&self.shared.read.waiter);
            *waits = true;
            // Ordering: `Release` orders the registration before the receive that reads the bit.
            // `Acquire` reads the release of each receive before it, as in `free`.
            self.read = self.shared.read.word.fetch_or(SENDER_WAITS, Ordering::AcqRel) & POSITION;
            if self.free(1) == 0 && !self.closed() {
                return Err(value);
            }
        }
        if *waits {
            self.unwait();
            *waits = false;
        }
        Ok(if self.free(1) == 0 {
            Err(SendError(value))
        } else {
            self.send_free(value).map_err(SendError)
        })
    }

    /// Ends the interest of a wait. A receive that cleared the bit wakes the waiter itself.
    fn unwait(&self) {
        let shared = &*self.shared;
        // Ordering: `Relaxed`: only the sender sets the bit, and the change publishes no data.
        if shared.read.word.fetch_and(!SENDER_WAITS, Ordering::Relaxed) & SENDER_WAITS != 0 {
            shared.read.waiter.cancel();
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let shared = &*self.shared;
        // Ordering: `Acquire` reads the release of a waiting receiver, as in `publish`. The close
        // publishes no data.
        if shared.written.word.fetch_or(CLOSED, Ordering::Acquire) & RECEIVER_WAITS != 0 {
            shared.written.waiter.wake();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

/// A waiting send, from [`Sender::send`]. It gives `Ok(())` when the receiver can take the value,
/// or [`SendError`] with the value when the channel is closed. A poll after that gives `Pending`.
#[must_use = "a future does nothing unless you poll it"]
pub struct SendFuture<'a, T> {
    sender: &'a mut Sender<T>,
    value: Option<T>,
    waits: bool,
}

impl<T> SendFuture<'_, T> {
    /// Ends the send, and gives back the value if the send did not move it into the channel.
    pub fn cancel(mut self) -> Option<T> {
        self.value.take()
    }
}

// The future never pins its value: it moves the value into a slot.
impl<T> Unpin for SendFuture<'_, T> {}

impl<T> Future for SendFuture<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(value) = this.value.take() else {
            return Poll::Pending;
        };
        match this
            .sender
            .send_step(value, &mut this.waits, |waiter| waiter.register(cx.waker()))
        {
            Ok(result) => Poll::Ready(result),
            Err(value) => {
                this.value = Some(value);
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for SendFuture<'_, T> {
    fn drop(&mut self) {
        if self.waits {
            self.sender.unwait();
        }
    }
}

impl<T> fmt::Debug for SendFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendFuture").finish_non_exhaustive()
    }
}

/// Free slots that the sender fills, and then publishes in one step. Only the written prefix is
/// published. A drop with no publication drops the written values, and frees the slots.
pub struct Reservation<'a, T> {
    sender: &'a mut Sender<T>,
    count: u32,
    written: u32,
}

impl<T> Reservation<'_, T> {
    /// The number of slots that are not written yet.
    pub fn remaining(&self) -> u32 {
        self.count - self.written
    }

    /// Writes `value` into the next slot of the reservation, or gives it back when each slot is
    /// written.
    #[expect(
        unsafe_code,
        reason = "the slot is free and in the reservation, which only the sender writes"
    )]
    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.written == self.count {
            return Err(value);
        }
        let shared = &*self.sender.shared;
        let position = shared.advance(self.sender.written, self.written);
        // SAFETY: the slot is in the reservation and not written yet, so it is free, and only the
        // sender has access to a free slot.
        unsafe { shared.write(position, value) };
        self.written += 1;
        Ok(())
    }

    /// Publishes the written values, in order, and frees the slots that are not written. The
    /// publication can come after [`Receiver::close`]. If the receiver aborted the channel, the
    /// send fails: the channel drops the written values and gives [`Aborted`].
    pub fn publish(self) -> Result<(), Aborted> {
        let mut this = ManuallyDrop::new(self);
        if this.settle(true) { Ok(()) } else { Err(Aborted) }
    }

    /// Ends the reservation: publishes the written values if `publish` is true, and otherwise
    /// drops them. Gives `true` if the values are published, or if `publish` is false and the
    /// receiver is not gone.
    #[expect(
        unsafe_code,
        reason = "the slots of a reservation that is not published stay the sender's"
    )]
    fn settle(&mut self, publish: bool) -> bool {
        let count = if publish { self.written } else { 0 };
        let settled = self.sender.publish(count, ABORTED);
        if !(publish && settled) {
            // The values were not published, so their slots are still the sender's. The drop
            // comes after the change of the word, so a drop that panics does not keep the flag.
            // SAFETY: the reservation wrote the slots from the written position.
            unsafe { self.sender.shared.drop_values(self.sender.written, self.written) };
        }
        settled
    }
}

impl<T> Drop for Reservation<'_, T> {
    fn drop(&mut self) {
        self.settle(false);
    }
}

impl<T> fmt::Debug for Reservation<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reservation")
            .field("count", &self.count)
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

/// The receiving endpoint. Its drop is the abort: it drops the values that are in the channel,
/// and later sends fail.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The read position. Only the receiver changes it.
    read: u32,
    /// The `written` word at the last load.
    written: u32,
}

impl<T> Receiver<T> {
    /// Takes the next value, if one is ready.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if self.read == self.written & POSITION {
            // Ordering: `Acquire` reads the release of each publication, so the write of each
            // slot happens before its read.
            self.written = self.shared.written.word.load(Ordering::Acquire);
        }
        self.take()
    }

    /// Gives a future that waits for the next value. It gives [`Closed`] when the channel is
    /// closed, and no value is left or can still come.
    pub fn recv(&mut self) -> RecvFuture<'_, T> {
        RecvFuture {
            receiver: self,
            waits: false,
        }
    }

    /// Waits on this thread for the next value. It gives [`Closed`] when the channel is closed,
    /// and no value is left or can still come.
    #[cfg(feature = "std")]
    pub fn recv_blocking(&mut self) -> Result<T, Closed> {
        let mut waits = false;
        loop {
            if let Some(result) = self.recv_step(&mut waits, Waiter::register_thread) {
                return result;
            }
            // A wake can come before the park: the park then returns at once.
            thread::park();
        }
    }

    /// The drain: later sends and reservations fail, and a pending send ends with its value. The
    /// receiver still gets each value that is in the channel or in an accepted reservation.
    pub fn close(&mut self) {
        let shared = &*self.shared;
        // Ordering: `Relaxed`: the close publishes no data. The total order of the word puts each
        // send before or after it, and the waiter orders it before the recheck of a waiting
        // sender.
        if shared.written.word.fetch_or(CLOSED, Ordering::Relaxed) & CLOSED == 0 {
            shared.read.waiter.wake();
        }
    }

    /// Takes the next value by the `written` word of the last load.
    #[expect(
        unsafe_code,
        reason = "a slot before the written position holds a published value, which only the receiver takes"
    )]
    fn take(&mut self) -> Result<T, TryRecvError> {
        let shared = &*self.shared;
        if self.read == self.written & POSITION {
            return Err(if self.written & (CLOSED | RESERVED) == CLOSED {
                TryRecvError::Closed
            } else {
                TryRecvError::Empty
            });
        }
        // SAFETY: the slot is before the written position, so it holds a published value, and
        // only the receiver has access to a published value.
        let value = unsafe { shared.take(self.read) };
        self.read = shared.advance(self.read, 1);
        // Ordering: `Release` orders the read of the slot before the next write of the sender.
        // `Acquire` reads the release of a waiting sender, so its waker is registered.
        if shared.read.word.swap(self.read, Ordering::AcqRel) & SENDER_WAITS != 0 {
            shared.read.waiter.wake();
        }
        Ok(value)
    }

    /// One step of a waiting receive: takes a value, or registers with `register` and rechecks.
    /// Gives `None` to wait for a wake.
    fn recv_step(&mut self, waits: &mut bool, register: impl FnOnce(&Waiter)) -> Option<Result<T, Closed>> {
        let result = match self.try_recv() {
            Err(TryRecvError::Empty) => {
                register(&self.shared.written.waiter);
                *waits = true;
                // Ordering: `Release` orders the registration before the publication that reads
                // the bit. `Acquire` reads the release of each publication before it, as in
                // `try_recv`.
                self.written = self.shared.written.word.fetch_or(RECEIVER_WAITS, Ordering::AcqRel);
                match self.take() {
                    Err(TryRecvError::Empty) => return None,
                    result => result,
                }
            }
            result => result,
        };
        if *waits {
            self.unwait();
            *waits = false;
        }
        Some(result.map_err(|_| Closed))
    }

    /// Ends the interest of a wait. A publication that cleared the bit wakes the waiter itself.
    fn unwait(&self) {
        let shared = &*self.shared;
        // Ordering: `Relaxed`: only the receiver sets the bit, and the change publishes no data.
        if shared.written.word.fetch_and(!RECEIVER_WAITS, Ordering::Relaxed) & RECEIVER_WAITS != 0 {
            shared.written.waiter.cancel();
        }
    }
}

impl<T> Drop for Receiver<T> {
    #[expect(
        unsafe_code,
        reason = "after the abort, no publication follows, so the receiver has each published slot"
    )]
    fn drop(&mut self) {
        let shared = &*self.shared;
        // Ordering: `Acquire` reads the release of each publication, so the write of each slot
        // happens before its drop. After this change, no publication can change the position.
        let word = shared.written.word.fetch_or(CLOSED | ABORTED, Ordering::Acquire);
        if word & CLOSED == 0 {
            shared.read.waiter.wake();
        }
        let count = shared.len(self.read, word & POSITION);
        // SAFETY: the slots from the read position to the last written position hold published
        // values, and only the receiver has access to them.
        unsafe { shared.drop_values(self.read, count) };
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

/// A waiting receive, from [`Receiver::recv`]. A drop of the future takes no value.
#[must_use = "a future does nothing unless you poll it"]
pub struct RecvFuture<'a, T> {
    receiver: &'a mut Receiver<T>,
    waits: bool,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Result<T, Closed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this
            .receiver
            .recv_step(&mut this.waits, |waiter| waiter.register(cx.waker()))
        {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
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
