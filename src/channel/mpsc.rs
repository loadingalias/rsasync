//! The bounded channel with many senders and one receiver.
//!
//! [`channel`] makes a [`Sender`] and a [`Receiver`]. [`Sender::try_clone`] makes another sender,
//! up to the number of sender places that the channel has. Each operation borrows its endpoint
//! mutably, so each endpoint has at most one operation at a time.
//!
//! ```
//! use core::num::NonZeroU64;
//! use std::thread;
//!
//! use rsasync::capacity::Budget;
//! use rsasync::channel::mpsc;
//!
//! type Error = Box<dyn core::error::Error + Send + Sync>;
//!
//! let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
//! // 4 slots for all senders, and 3 sender places.
//! let (sender, mut receiver) = mpsc::channel::<(u32, u32)>(&budget, 4, 3)?;
//!
//! let producers: Vec<_> = (0..2)
//!     .map(|producer| {
//!         let mut sender = sender.try_clone()?;
//!         Ok(thread::spawn(move || {
//!             for number in 0..10 {
//!                 // Wait for a free slot, then send.
//!                 sender.send_blocking((producer, number))?;
//!             }
//!             Ok::<(), Error>(())
//!         }))
//!     })
//!     .collect::<Result<_, Error>>()?;
//! // The channel closes when the last sender drops.
//! drop(sender);
//!
//! let mut next = [0, 0];
//! while let Ok((producer, number)) = receiver.recv_blocking() {
//!     // The values of each sender come in the order of its sends.
//!     assert_eq!(number, next[producer as usize]);
//!     next[producer as usize] += 1;
//! }
//! assert_eq!(next, [10, 10]);
//! for producer in producers {
//!     producer.join().expect("a producer does not panic")?;
//! }
//! # Ok::<(), Error>(())
//! ```
//!
//! # Bound
//!
//! All senders share one ring of exactly `capacity` slots. A slot is in use from the send or the
//! reservation that claims it until the receiver takes the value or passes the slot. A clone adds
//! no slot, so the values in the channel and the slots of the reservations together never exceed
//! the capacity, with any number of senders.
//!
//! # Order and fairness
//!
//! The values of one sender reach the receiver in the order of its sends and publications. The
//! channel states no order between the values of different senders.
//!
//! When the ring is full, a waiting send waits for a free slot. For each slot that it frees, the
//! receiver wakes one waiting sender, in rotating order of the sender places. A sender that does
//! not wait can take a free slot first. Thus a woken sender can find the ring full and wait again.
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
//! - The drop of the last sender closes the channel. The receiver gets each value that is still
//!   in the channel, and then [`Closed`].
//! - [`Receiver::close`] is the drain. Later sends and reservations fail, and each pending send
//!   ends with its value. An accepted reservation can still publish its values. The receiver gets
//!   [`Closed`] only when the channel is empty and no accepted reservation can still publish.
//! - A drop of the receiver is the abort. The receiver drops the values that are in the channel.
//!   A send that finds the abort fails with its value. The publication of an accepted reservation
//!   fails with [`Aborted`], and the channel drops its written values.
//!
//! # Orderings
//!
//! The `tail` word holds the claimed position and the flags of close and abort. The `head` word
//! holds the read position and the bit of a waiting sender. Each slot has a state word. A position
//! has a lap above the bits of the next power of two of the capacity, and the index of a slot
//! below them, so a position wraps only after 2^62 positions.
//!
//! - A claim is a compare-exchange of `tail`, so the claims and the close have one total order. A
//!   claim loads `head` before `tail`. Thus the length that it computes is never smaller than the
//!   true length, and no claim passes the capacity.
//! - A publication is a swap of the state of its slot, `Release` for the value. The receiver loads
//!   the state with `Acquire`, takes the value, and then swaps `head` with `Release`. A claim loads
//!   `head` with `Acquire`, so the take happens before the next write of the slot.
//! - The receiver waits on the state of the slot at its read position. It registers its waker, and
//!   then sets the bit of a waiting receiver with an `AcqRel` change, which also rechecks the
//!   state. The publication that swaps the state reads the bit, and wakes the receiver.
//! - A waiting sender registers its waker in its place, sets its waiting bit, and then changes
//!   `head` with an `AcqRel` read-modify-write that also sets the bit of a waiting sender. The
//!   receiver swaps `head` after each take. The two changes of `head` have one order: the receiver
//!   reads the bit and wakes a waiting sender, or the sender reads the new read position. The
//!   places of the senders are the shared ones of `super::ring`.
//! - A close changes `tail` and then wakes the waiting senders (`super::ring`).

use core::alloc::Layout;
use core::fmt;
use core::future::Future;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::task::{Context, Poll};

use alloc::boxed::Box;

use super::ring::{ABORTED, CLOSED, Line, MAX_CAPACITY, MAX_PLACES, POSITION, Places, Ring, items};
use super::{Aborted, Closed, Refused, ReserveError, SendError, TooManySenders, TryRecvError, TrySendError};
use crate::capacity::{Budget, Permit};
#[cfg(feature = "std")]
use crate::sync::thread;
use crate::sync::{
    Arc, UnsafeCell,
    atomic::{AtomicU32, AtomicU64, Ordering},
};
use crate::wait::Waiter;

/// In the `head` word: a sender waits for a free slot.
const SENDERS_WAIT: u64 = 1 << 62;

/// The state of a slot: free, or claimed and not published yet.
const EMPTY: u32 = 0;
/// The state of a slot: it holds a published value.
const FULL: u32 = 1;
/// The state of a slot: its claim ended with no value.
const SKIP: u32 = 2;
/// The state of a slot: the receiver is gone, and the claim of the slot cannot publish.
const ABANDONED: u32 = 3;
/// The bits of the state of a slot, below the bit of a waiting receiver.
const STATE: u32 = 0b11;
/// In the state of a slot: the receiver waits for the publication of this slot.
const RECEIVER_WAITS: u32 = 0b100;

struct Slot<T> {
    state: AtomicU32,
    value: UnsafeCell<MaybeUninit<T>>,
}

struct Shared<T> {
    /// The claimed position and the flags `CLOSED` and `ABORTED`.
    tail: Line<AtomicU64>,
    /// The read position, which only the receiver advances, and `SENDERS_WAIT`.
    head: Line<AtomicU64>,
    receiver: Waiter,
    slots: Box<[Slot<T>]>,
    ring: Ring,
    senders: Places,
    _charge: Permit,
}

impl<T> Shared<T> {
    fn slot(&self, position: u64) -> &Slot<T> {
        &self.slots[self.ring.index(position)]
    }

    /// Moves `value` into the slot of `position`.
    ///
    /// # Safety
    ///
    /// The caller claimed the slot, and did not publish it.
    #[expect(unsafe_code, reason = "the claim gives the caller the only access to the slot")]
    unsafe fn write(&self, position: u64, value: T) {
        // SAFETY: only the caller has access to the slot.
        self.slot(position)
            .value
            .with_mut(|slot| unsafe { (*slot).write(value) });
    }

    /// Moves the value out of the slot of `position`.
    ///
    /// # Safety
    ///
    /// The slot holds a value, and only the caller has access to it.
    #[expect(unsafe_code, reason = "the caller has the only access to the written slot")]
    unsafe fn take(&self, position: u64) -> T {
        // SAFETY: the slot holds a value, and only the caller has access to it.
        self.slot(position)
            .value
            .with(|slot| unsafe { (*slot).assume_init_read() })
    }

    /// Publishes the slot of `position` with `state`, and wakes a receiver that waits for it.
    /// Gives `ABANDONED` if the receiver is gone, and `EMPTY` otherwise.
    fn publish(&self, position: u64, state: u32) -> u32 {
        // Ordering: `Release` orders the write of the value before the `Acquire` load of the
        // receiver. `Acquire` reads the release of a waiting receiver, so its waker is registered,
        // and of an abort, so the caller has the value again.
        let old = self.slot(position).state.swap(state, Ordering::AcqRel);
        if old & RECEIVER_WAITS != 0 {
            self.receiver.wake();
        }
        old & STATE
    }
}

#[expect(
    unsafe_code,
    reason = "a claim or the read position gives each slot to one endpoint at a time"
)]
// SAFETY: a slot is the sender's from its claim to its publication, and the receiver's from the
// publication to the next claim. The changes of `tail`, of `head`, and of the state of the slot
// order each handover (module docs). The values move between threads, so `T` is `Send`.
unsafe impl<T: Send> Sync for Shared<T> {}

/// Makes a channel of `capacity` slots and `senders` sender places. It charges the slots, the
/// places, and the shared state to `budget`, at their size in memory, before it allocates. The
/// charge ends when the receiver and each sender drop.
///
/// The capacity is at least 1 and at most 2^27. The number of sender places is at least 1 and at
/// most 2^16.
///
/// ```
/// use core::num::NonZeroU64;
///
/// use rsasync::capacity::Budget;
/// use rsasync::channel::{Refused, TooManySenders, TryRecvError, mpsc};
///
/// let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
/// assert_eq!(mpsc::channel::<u64>(&budget, 4, 0).unwrap_err(), Refused::Zero);
///
/// let (mut first, mut receiver) = mpsc::channel(&budget, 4, 2)?;
/// let mut second = first.try_clone()?;
/// // Each of the 2 sender places is in use.
/// assert_eq!(first.try_clone().unwrap_err(), TooManySenders);
///
/// first.try_send(1)?;
/// second.try_send(2)?;
/// drop((first, second));
/// assert_eq!(receiver.try_recv(), Ok(1));
/// assert_eq!(receiver.try_recv(), Ok(2));
/// assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
/// # Ok::<(), Box<dyn core::error::Error>>(())
/// ```
pub fn channel<T>(budget: &Budget, capacity: u32, senders: u32) -> Result<(Sender<T>, Receiver<T>), Refused> {
    if capacity == 0 || senders == 0 {
        return Err(Refused::Zero);
    }
    if capacity > MAX_CAPACITY || senders > MAX_PLACES {
        return Err(Refused::TooLarge);
    }
    let size = Layout::array::<Slot<T>>(capacity as usize)
        .ok()
        .and_then(|slots| slots.size().checked_add(Places::size(senders)?))
        .and_then(|size| size.checked_add(size_of::<Shared<T>>()))
        .and_then(|size| u64::try_from(size).ok())
        .ok_or(Refused::TooLarge)?;
    let charge = budget.reserve(size).map_err(Refused::Budget)?;

    let slots = items(capacity, |_| Slot {
        state: AtomicU32::new(EMPTY),
        value: UnsafeCell::new(MaybeUninit::uninit()),
    })?;
    // The first sender has place 0.
    let senders = Places::new(senders)?;
    let shared = Arc::new(Shared {
        tail: Line(AtomicU64::new(0)),
        head: Line(AtomicU64::new(0)),
        receiver: Waiter::new(),
        slots,
        ring: Ring::new(capacity),
        senders,
        _charge: charge,
    });
    let sender = Sender {
        shared: Arc::clone(&shared),
        place: 0,
    };
    let receiver = Receiver { shared, head: 0 };
    Ok((sender, receiver))
}

/// Why a claim fails.
enum Refusal {
    Full,
    Closed,
}

/// A sending endpoint. It has one sender place of the channel. The drop of the last sender closes
/// the channel: the receiver gets the values that are left, and then [`Closed`].
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    place: u32,
}

impl<T> Sender<T> {
    /// Makes another sender of the channel, which takes a free sender place. A clone adds no slot
    /// to the channel.
    pub fn try_clone(&self) -> Result<Self, TooManySenders> {
        let place = self.shared.senders.take().ok_or(TooManySenders)?;
        Ok(Self {
            shared: Arc::clone(&self.shared),
            place,
        })
    }

    /// Sends `value` if a slot is free and the channel is open. Otherwise the value comes back.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        self.send_now(value).map_err(|(refusal, value)| match refusal {
            Refusal::Full => TrySendError::Full(value),
            Refusal::Closed => TrySendError::Closed(value),
        })
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
    ///
    /// The reservation holds its place in the order of the channel: the receiver cannot pass its
    /// slots until it ends. Thus one slow reservation stops the receiver for every sender. Fill
    /// and publish a reservation with no wait between this call and [`Reservation::publish`].
    pub fn try_reserve(&mut self, count: u32) -> Result<Reservation<'_, T>, ReserveError> {
        if u64::from(count) > self.shared.ring.capacity {
            return Err(ReserveError::TooLarge);
        }
        match self.claim(count) {
            Ok(position) => Ok(Reservation {
                sender: self,
                position,
                count,
                written: 0,
                settled: 0,
            }),
            Err(Refusal::Full) => Err(ReserveError::Full),
            Err(Refusal::Closed) => Err(ReserveError::Closed),
        }
    }

    /// Claims `count` free slots, and gives the position of the first.
    fn claim(&self, count: u32) -> Result<u64, Refusal> {
        let shared = &*self.shared;
        let count = u64::from(count);
        // Ordering: `Acquire` reads the release of each receive, so the take of each slot happens
        // before a sender writes it again. `head` comes before `tail`, so the read position is not
        // after the claimed position (module docs).
        let mut head = shared.head.0.load(Ordering::Acquire) & POSITION;
        // Ordering: `Relaxed`: a claim publishes no data.
        let mut tail = shared.tail.0.load(Ordering::Relaxed);
        loop {
            if tail & CLOSED != 0 {
                return Err(Refusal::Closed);
            }
            if shared.ring.free(head, tail & POSITION) < count {
                // Ordering: as above.
                let fresh = shared.head.0.load(Ordering::Acquire) & POSITION;
                if fresh == head {
                    return Err(Refusal::Full);
                }
                head = fresh;
                tail = shared.tail.0.load(Ordering::Relaxed);
                continue;
            }
            let next = shared.ring.advance(tail & POSITION, count);
            // Ordering: `Relaxed`, as above. The total order of `tail` puts the claim before or
            // after a close.
            match shared
                .tail
                .0
                .compare_exchange_weak(tail, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Ok(tail & POSITION),
                Err(actual) => tail = actual,
            }
        }
    }

    /// Claims a slot, writes `value` into it, and publishes it.
    fn send_now(&self, value: T) -> Result<(), (Refusal, T)> {
        match self.claim(1) {
            Ok(position) => self
                .send_claimed(position, value)
                .map_err(|value| (Refusal::Closed, value)),
            Err(refusal) => Err((refusal, value)),
        }
    }

    /// Writes `value` into the claimed slot of `position`, and publishes it. Gives the value back
    /// when the receiver is gone.
    #[expect(unsafe_code, reason = "the claim gives the sender the slot until its publication")]
    fn send_claimed(&self, position: u64, value: T) -> Result<(), T> {
        let shared = &*self.shared;
        // SAFETY: the sender claimed the slot, and did not publish it.
        unsafe { shared.write(position, value) };
        if shared.publish(position, FULL) == ABANDONED {
            // SAFETY: the receiver is gone, and no claim follows the abort, so the sender has the
            // only access to the slot again.
            Err(unsafe { shared.take(position) })
        } else {
            Ok(())
        }
    }

    /// One step of a waiting send: sends `value`, or registers with `register`, rechecks, and
    /// gives the value back to wait for a wake.
    fn send_step(
        &mut self,
        value: T,
        waits: &mut bool,
        register: impl FnOnce(&Waiter),
    ) -> Result<Result<(), SendError<T>>, T> {
        let result = match self.send_now(value) {
            Err((Refusal::Full, value)) => {
                let shared = &*self.shared;
                register(shared.senders.waiter(self.place));
                *waits = true;
                shared.senders.wait(self.place);
                // Ordering: `AcqRel`: the next receive reads this change, and then sees the bit, or
                // this change reads that receive, and the retry sees the free slot (module docs).
                shared.head.0.fetch_or(SENDERS_WAIT, Ordering::AcqRel);
                match self.send_now(value) {
                    Err((Refusal::Full, value)) => return Err(value),
                    result => result,
                }
            }
            result => result,
        };
        if *waits {
            self.shared.senders.unwait(self.place, false);
            *waits = false;
        }
        Ok(result.map_err(|(_, value)| SendError(value)))
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let shared = &*self.shared;
        if shared.senders.release(self.place) {
            // Ordering: `Relaxed`: the close publishes no data. The waiter of the receiver orders
            // it before the recheck of a waiting receiver.
            shared.tail.0.fetch_or(CLOSED, Ordering::Relaxed);
            shared.receiver.wake();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("place", &self.place)
            .finish_non_exhaustive()
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
            self.sender.shared.senders.unwait(self.sender.place, true);
        }
    }
}

impl<T> fmt::Debug for SendFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendFuture").finish_non_exhaustive()
    }
}

/// Claimed slots that the sender fills, and then publishes in one step. Only the written prefix is
/// published. A drop with no publication drops the written values, and the receiver passes the
/// slots.
pub struct Reservation<'a, T> {
    sender: &'a mut Sender<T>,
    position: u64,
    count: u32,
    written: u32,
    /// The slots that this reservation ended already.
    settled: u32,
}

impl<T> Reservation<'_, T> {
    /// The number of slots that are not written yet.
    pub fn remaining(&self) -> u32 {
        self.count - self.written
    }

    /// Writes `value` into the next slot of the reservation, or gives it back when each slot is
    /// written.
    #[expect(unsafe_code, reason = "the reservation claimed the slot and did not publish it")]
    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.written == self.count {
            return Err(value);
        }
        let shared = &*self.sender.shared;
        let position = shared.ring.advance(self.position, u64::from(self.written));
        // SAFETY: the reservation claimed the slot, and did not publish it.
        unsafe { shared.write(position, value) };
        self.written += 1;
        Ok(())
    }

    /// Publishes the written values, in order, and lets the receiver pass the slots that are not
    /// written. The publication can come after [`Receiver::close`]. If the receiver aborted the
    /// channel, the send fails: the channel drops the written values and gives [`Aborted`].
    pub fn publish(mut self) -> Result<(), Aborted> {
        if self.settle(true) { Ok(()) } else { Err(Aborted) }
    }

    /// Ends each slot that is not ended yet: publishes the written values if `publish` is true,
    /// and otherwise drops them. Gives `false` if the receiver is gone. A slot ends before the
    /// drop of its value, so a drop that panics leaves the rest to the drop of the reservation.
    #[expect(
        unsafe_code,
        reason = "the slots of a reservation are the sender's until each publication"
    )]
    fn settle(&mut self, publish: bool) -> bool {
        let shared = &*self.sender.shared;
        let mut received = true;
        while self.settled < self.count {
            let offset = self.settled;
            let position = shared.ring.advance(self.position, u64::from(offset));
            self.settled += 1;
            let written = offset < self.written;
            if written && publish {
                if shared.publish(position, FULL) == ABANDONED {
                    received = false;
                    // SAFETY: the receiver is gone, and no claim follows the abort, so the sender
                    // has the only access to the slot again.
                    drop(unsafe { shared.take(position) });
                }
            } else {
                // The value leaves the slot before the slot ends: after the end, the receiver can
                // pass the slot, and another claim can write it.
                // SAFETY: the reservation wrote the slot and did not publish it.
                let value = written.then(|| unsafe { shared.take(position) });
                if shared.publish(position, SKIP) == ABANDONED {
                    received = false;
                }
                drop(value);
            }
        }
        received
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
    head: u64,
}

impl<T> Receiver<T> {
    /// Takes the next value, if one is ready.
    #[expect(
        unsafe_code,
        reason = "a published slot at the read position is the receiver's until it moves on"
    )]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        loop {
            let shared = &*self.shared;
            let slot = shared.slot(self.head);
            // Ordering: `Acquire` reads the release of the publication, so the write of the value
            // happens before its read.
            match slot.state.load(Ordering::Acquire) & STATE {
                FULL => {
                    // SAFETY: the slot holds a published value, and only the receiver has access
                    // to it until the read position moves on.
                    let value = unsafe { shared.take(self.head) };
                    self.pass();
                    return Ok(value);
                }
                SKIP => self.pass(),
                _ => {
                    return Err(if self.ended() {
                        TryRecvError::Closed
                    } else {
                        TryRecvError::Empty
                    });
                }
            }
        }
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

    /// The drain: later sends and reservations fail, and each pending send ends with its value.
    /// The receiver still gets each value that is in the channel or in an accepted reservation.
    pub fn close(&mut self) {
        let shared = &*self.shared;
        // Ordering: `Relaxed`: the close publishes no data. The total order of `tail` puts each
        // claim before or after it, and the waiting words order it before the recheck of each
        // waiting sender.
        if shared.tail.0.fetch_or(CLOSED, Ordering::Relaxed) & CLOSED == 0 {
            shared.senders.wake_all();
        }
    }

    /// Frees the slot at the read position, moves on, and wakes a waiting sender.
    fn pass(&mut self) {
        let shared = &*self.shared;
        // Ordering: `Relaxed`: the swap of `head` below orders it before the next claim.
        shared.slot(self.head).state.store(EMPTY, Ordering::Relaxed);
        self.head = shared.ring.advance(self.head, 1);
        // Ordering: `Release` orders the take of the slot before the next claim, which loads
        // `head` with `Acquire`. `Acquire` reads the release of a waiting sender (module docs).
        if shared.head.0.swap(self.head, Ordering::AcqRel) & SENDERS_WAIT != 0 {
            shared.senders.wake_one();
            if shared.senders.waiting() {
                // Ordering: `Relaxed`: the receiver reads this bit at its next swap, in this
                // thread.
                shared.head.0.fetch_or(SENDERS_WAIT, Ordering::Relaxed);
            }
        }
    }

    /// The channel is closed, and each claim is passed.
    fn ended(&self) -> bool {
        // Ordering: `Relaxed`: the claimed position publishes no data. A slot before it that is
        // not published yet keeps the channel open.
        let tail = self.shared.tail.0.load(Ordering::Relaxed);
        tail & CLOSED != 0 && tail & POSITION == self.head
    }

    /// One step of a waiting receive: takes a value, or registers with `register` and rechecks.
    /// Gives `None` to wait for a wake.
    fn recv_step(&mut self, waits: &mut bool, register: impl Fn(&Waiter)) -> Option<Result<T, Closed>> {
        // The interest of an earlier poll is on the slot at the read position, which has not
        // moved since then.
        if *waits {
            self.unwait();
            *waits = false;
        }
        loop {
            match self.try_recv() {
                Err(TryRecvError::Empty) => {}
                result => return Some(result.map_err(|_| Closed)),
            }
            register(&self.shared.receiver);
            // Ordering: `Release` orders the registration before the publication that reads the
            // bit. `Acquire` reads the release of a publication before it.
            let state = self
                .shared
                .slot(self.head)
                .state
                .fetch_or(RECEIVER_WAITS, Ordering::AcqRel);
            if state & STATE == EMPTY && !self.ended() {
                *waits = true;
                return None;
            }
            self.unwait();
        }
    }

    /// Ends the interest of a wait on the slot at the read position. A publication that cleared
    /// the bit wakes the waiter itself.
    fn unwait(&self) {
        let shared = &*self.shared;
        // Ordering: `Relaxed`: only the receiver sets the bit, and the change publishes no data.
        let state = shared
            .slot(self.head)
            .state
            .fetch_and(!RECEIVER_WAITS, Ordering::Relaxed);
        if state & RECEIVER_WAITS != 0 {
            shared.receiver.cancel();
        }
    }
}

impl<T> Drop for Receiver<T> {
    #[expect(
        unsafe_code,
        reason = "after the abort, a published slot is the receiver's, and no claim follows"
    )]
    fn drop(&mut self) {
        let shared = &*self.shared;
        // Ordering: `Relaxed`: the abort publishes no data. The swap of each slot below orders it
        // against the publication of the slot.
        let tail = shared.tail.0.fetch_or(CLOSED | ABORTED, Ordering::Relaxed);
        if tail & CLOSED == 0 {
            shared.senders.wake_all();
        }
        let mut position = self.head;
        while position != tail & POSITION {
            // Ordering: `Acquire` reads the release of a publication, so the write of the value
            // happens before its drop. `Release` orders the abort before a later publication,
            // which then has its value again.
            if shared.slot(position).state.swap(ABANDONED, Ordering::AcqRel) & STATE == FULL {
                // SAFETY: the slot holds a published value, and the sender is done with it.
                drop(unsafe { shared.take(position) });
            }
            position = shared.ring.advance(position, 1);
        }
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
