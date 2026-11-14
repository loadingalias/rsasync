//! The bounded channel with many senders and many receivers that compete for the values.
//!
//! [`channel`] makes a [`Sender`] and a [`Receiver`]. [`Sender::try_clone`] and
//! [`Receiver::try_clone`] make more endpoints, up to the number of places that the channel has
//! for each kind. Each operation borrows its endpoint mutably, so each endpoint has at most one
//! operation at a time.
//!
//! ```
//! use core::num::NonZeroU64;
//! use std::thread;
//!
//! use rsasync::capacity::Budget;
//! use rsasync::channel::mpmc;
//!
//! type Error = Box<dyn core::error::Error + Send + Sync>;
//!
//! let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
//! // 8 slots, 1 sender place, and 3 receiver places: one for each worker.
//! let (mut jobs, receiver) = mpmc::channel::<u32>(&budget, 8, 1, 3)?;
//!
//! let receivers = [receiver.try_clone()?, receiver.try_clone()?, receiver];
//! let workers: Vec<_> = receivers
//!     .into_iter()
//!     .map(|mut receiver| {
//!         thread::spawn(move || {
//!             // Each worker takes the next job, until the channel is closed and empty.
//!             let mut total = 0;
//!             while let Ok(job) = receiver.recv_blocking() {
//!                 total += job;
//!             }
//!             total
//!         })
//!     })
//!     .collect();
//!
//! for job in 1..=100 {
//!     jobs.send_blocking(job)?;
//! }
//! // The drop of the last sender closes the channel. The workers take the rest, and then end.
//! drop(jobs);
//!
//! let total: u32 = workers
//!     .into_iter()
//!     .map(|worker| worker.join().expect("a worker does not panic"))
//!     .sum();
//! // Each job went to exactly one worker.
//! assert_eq!(total, (1..=100).sum());
//! # Ok::<(), Error>(())
//! ```
//!
//! # When to choose it
//!
//! Choose this channel only when the receivers compete for the same values, as the workers of a
//! job queue do. With one receiver, choose [`spsc`](super::spsc) or [`mpsc`](super::mpsc): their
//! receive costs less, and their receiver gets the values in order. To give each consumer its own
//! stream of values, give each consumer its own channel.
//!
//! # Order and fairness
//!
//! Each value goes to exactly one receiver. The receivers take the values of one sender in the
//! order of its sends, but each receiver works on its own, so the channel states no order in which
//! the receivers handle the values.
//!
//! A waiting receiver waits for a value, and a waiting sender waits for a free slot. Each value
//! and each free slot wakes one waiting endpoint, in rotating order of the places. An endpoint that
//! does not wait can take the value or the slot first, so a woken endpoint can wait again.
//!
//! # Value ownership
//!
//! The ownership of a value is as in the [`mpsc`](super::mpsc) channel, with one more rule: a
//! receive that took a value gives it to its caller, also when a close comes after the take. The
//! drop of the last receiver is the abort.
//!
//! # Close
//!
//! - The drop of the last sender closes the channel. The receivers get each value that is still in
//!   the channel, and then [`Closed`].
//! - [`Receiver::close`], from any receiver, is the drain. Later sends and reservations fail, and
//!   each pending send ends with its value. An accepted reservation can still publish its values.
//! - Each close wakes each waiting receiver and each waiting sender. A receive gives [`Closed`]
//!   only when the channel is closed, and each claimed slot is taken.
//! - The drop of the last receiver is the abort. It drops the values that are in the channel. A
//!   send that finds the abort fails with its value. The publication of an accepted reservation
//!   fails with [`Aborted`], and the channel drops its written values.
//!
//! # Orderings
//!
//! The `tail` word holds the position that the senders claim, and the flags of close and abort.
//! The `head` word holds the position that the receivers claim. Each slot has a stamp: the lap of
//! the position that it serves, its state, and a wait bit for each side.
//!
//! - A sender claims a position with a compare-exchange of `tail`, only if the stamp of each slot
//!   is free for the lap of its position. It loads each stamp with `Acquire`, which reads the
//!   release of the receive that freed the slot. Thus that read happens before the next write.
//! - A publication swaps the stamp with `Release` for the value. A receiver loads the stamp with
//!   `Acquire`, claims the position with a compare-exchange of `head`, takes the value, and frees
//!   the slot with a swap of the stamp for the next lap.
//! - A wait registers its waker in its place (`super::ring`), and then sets the wait bit of its side
//!   in the stamp of the slot at its claim position, with an `AcqRel` change that also rechecks the
//!   stamp. Each swap of a stamp reads the bits that it clears, and wakes one waiting endpoint of
//!   the side of each bit. A woken endpoint that takes a value or claims a slot sets the bit again
//!   on the next slot if more endpoints of its side wait, so each waiting endpoint gets a wake.
//! - A close changes `tail` and then wakes the waiting endpoints of both sides (`super::ring`).

use core::alloc::Layout;
use core::fmt;
use core::future::Future;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::task::{Context, Poll};

use alloc::boxed::Box;

use super::ring::{ABORTED, CLOSED, Line, MAX_CAPACITY, MAX_PLACES, POSITION, Places, Ring, items};
use super::{
    Aborted, Closed, Refused, ReserveError, SendError, TooManyReceivers, TooManySenders, TryRecvError, TrySendError,
};
use crate::capacity::{Budget, Permit};
#[cfg(feature = "std")]
use crate::sync::thread;
use crate::sync::{
    Arc, UnsafeCell,
    atomic::{AtomicU64, Ordering},
};
use crate::wait::Waiter;

/// The state of a slot: free for the claim of its lap, or claimed and not published yet.
const FREE: u64 = 0;
/// The state of a slot: it holds a published value.
const FULL: u64 = 1;
/// The state of a slot: its claim ended with no value.
const SKIP: u64 = 2;
/// The state of a slot: the receivers are gone, and the claim of the slot cannot publish.
const ABANDONED: u64 = 3;
/// The bits of the state in a stamp.
const STATE: u64 = 0b11;
/// In a stamp: a receiver waits for the publication of this slot.
const RECEIVER_WAITS: u64 = 0b100;
/// In a stamp: a sender waits for this slot to be free.
const SENDER_WAITS: u64 = 0b1000;
/// The lap in a stamp is above these bits. The stamp keeps the lap modulo 2^60.
const LAP_SHIFT: u32 = 4;

/// The stamp of `state` for the lap `lap`.
fn stamp(lap: u64, state: u64) -> u64 {
    (lap << LAP_SHIFT) | state
}

/// The lap of a stamp, against the lap of a position.
#[derive(PartialEq, Eq)]
enum Turn {
    Same,
    Previous,
    Later,
}

fn turn(stamp: u64, lap: u64) -> Turn {
    let difference = (stamp >> LAP_SHIFT).wrapping_sub(lap) & (u64::MAX >> LAP_SHIFT);
    match difference {
        0 => Turn::Same,
        difference if difference == u64::MAX >> LAP_SHIFT => Turn::Previous,
        _ => Turn::Later,
    }
}

struct Slot<T> {
    stamp: AtomicU64,
    value: UnsafeCell<MaybeUninit<T>>,
}

struct Shared<T> {
    /// The position that the senders claim, and the flags `CLOSED` and `ABORTED`.
    tail: Line<AtomicU64>,
    /// The position that the receivers claim.
    head: Line<AtomicU64>,
    slots: Box<[Slot<T>]>,
    ring: Ring,
    senders: Places,
    receivers: Places,
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

    /// Swaps the stamp of the slot of `position` to `new`, and wakes one waiting endpoint for each
    /// wait bit that the swap clears. Gives the stamp before the swap.
    fn change(&self, position: u64, new: u64) -> u64 {
        // Ordering: `Release` orders the write or the read of the value before the next user of
        // the slot, which loads the stamp with `Acquire`. `Acquire` reads the release of each wait
        // that set a bit, so its waker is registered, and of an abort.
        let old = self.slot(position).stamp.swap(new, Ordering::AcqRel);
        if old & RECEIVER_WAITS != 0 {
            self.receivers.wake_one();
        }
        if old & SENDER_WAITS != 0 {
            self.senders.wake_one();
        }
        old
    }

    /// Publishes the claimed slot of `position` with `state`. Gives `false` if the receivers are
    /// gone.
    fn publish(&self, position: u64, state: u64) -> bool {
        let old = self.change(position, stamp(self.ring.lap(position), state));
        old & STATE != ABANDONED
    }

    /// Frees the slot of `position` for the claim of its next lap.
    fn free(&self, position: u64) {
        self.change(position, stamp(self.ring.lap(position) + 1, FREE));
    }

    /// If endpoints of the side of `bit` wait, sets `bit` in the stamp of the slot of `position`,
    /// and wakes one of them at once if that slot is `ready` for its lap already.
    fn pass_on(&self, places: &Places, bit: u64, position: u64, ready: impl Fn(u64) -> bool) {
        if !places.waiting() {
            return;
        }
        // Ordering: `AcqRel`, as for the bit of a wait (module docs).
        let old = self.slot(position).stamp.fetch_or(bit, Ordering::AcqRel);
        if turn(old, self.ring.lap(position)) == Turn::Same && ready(old & STATE) {
            places.wake_one();
        }
    }

    /// The channel is closed, and each claimed slot from `head` on is taken.
    fn ended(&self, head: u64) -> bool {
        // Ordering: `Relaxed`: the claimed position publishes no data. A slot before it that is not
        // taken yet keeps the channel open.
        let tail = self.tail.0.load(Ordering::Relaxed);
        tail & CLOSED != 0 && tail & POSITION == head
    }

    /// Closes the channel, and wakes each waiting endpoint. Gives `false` if it was closed.
    fn close(&self, flags: u64) -> bool {
        // Ordering: `Relaxed`: the close publishes no data. The total order of `tail` puts each
        // claim before or after it, and the waiting words order it before the recheck of each wait
        // (`super::ring`).
        let first = self.tail.0.fetch_or(flags, Ordering::Relaxed) & CLOSED == 0;
        self.senders.wake_all();
        self.receivers.wake_all();
        first
    }
}

#[expect(unsafe_code, reason = "a claim gives each slot to one endpoint at a time")]
// SAFETY: a slot is its sender's from the claim to the publication, and its receiver's from the
// claim of the read to the free. The stamp and the compare-exchanges of `tail` and `head` order
// each handover (module docs). The values move between threads, so `T` is `Send`.
unsafe impl<T: Send> Sync for Shared<T> {}

/// Makes a channel of `capacity` slots, `senders` sender places, and `receivers` receiver places.
/// It charges the slots, the places, and the shared state to `budget`, at their size in memory,
/// before it allocates. The charge ends when each endpoint drops.
///
/// The capacity is at least 1 and at most 2^27. Each number of places is at least 1 and at most
/// 2^16.
///
/// ```
/// use core::num::NonZeroU64;
///
/// use rsasync::capacity::Budget;
/// use rsasync::channel::{Refused, TooManyReceivers, TryRecvError, mpmc};
///
/// let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
/// assert_eq!(mpmc::channel::<u64>(&budget, 4, 1, 0).unwrap_err(), Refused::Zero);
///
/// let (mut sender, mut first) = mpmc::channel(&budget, 4, 1, 2)?;
/// let mut second = first.try_clone()?;
/// assert_eq!(first.try_clone().unwrap_err(), TooManyReceivers);
///
/// sender.try_send(1)?;
/// sender.try_send(2)?;
/// drop(sender);
/// // Each value goes to one receiver.
/// assert_eq!(first.try_recv(), Ok(1));
/// assert_eq!(second.try_recv(), Ok(2));
/// assert_eq!(first.try_recv(), Err(TryRecvError::Closed));
/// # Ok::<(), Box<dyn core::error::Error>>(())
/// ```
pub fn channel<T>(
    budget: &Budget,
    capacity: u32,
    senders: u32,
    receivers: u32,
) -> Result<(Sender<T>, Receiver<T>), Refused> {
    if capacity == 0 || senders == 0 || receivers == 0 {
        return Err(Refused::Zero);
    }
    if capacity > MAX_CAPACITY || senders > MAX_PLACES || receivers > MAX_PLACES {
        return Err(Refused::TooLarge);
    }
    let size = Layout::array::<Slot<T>>(capacity as usize)
        .ok()
        .and_then(|slots| slots.size().checked_add(Places::size(senders)?))
        .and_then(|size| size.checked_add(Places::size(receivers)?))
        .and_then(|size| size.checked_add(size_of::<Shared<T>>()))
        .and_then(|size| u64::try_from(size).ok())
        .ok_or(Refused::TooLarge)?;
    let charge = budget.reserve(size).map_err(Refused::Budget)?;

    // Each slot is free for the claim of lap 0.
    let slots = items(capacity, |_| Slot {
        stamp: AtomicU64::new(stamp(0, FREE)),
        value: UnsafeCell::new(MaybeUninit::uninit()),
    })?;
    // The first sender and the first receiver have place 0.
    let shared = Arc::new(Shared {
        tail: Line(AtomicU64::new(0)),
        head: Line(AtomicU64::new(0)),
        slots,
        ring: Ring::new(capacity),
        senders: Places::new(senders)?,
        receivers: Places::new(receivers)?,
        _charge: charge,
    });
    let sender = Sender {
        shared: Arc::clone(&shared),
        place: 0,
    };
    let receiver = Receiver { shared, place: 0 };
    Ok((sender, receiver))
}

/// Why a claim fails.
enum Refusal {
    Full,
    Closed,
}

/// A sending endpoint. It has one sender place of the channel. The drop of the last sender closes
/// the channel: the receivers get the values that are left, and then [`Closed`].
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    place: u32,
}

impl<T> Sender<T> {
    /// Makes another sender of the channel, which takes a free sender place.
    pub fn try_clone(&self) -> Result<Self, TooManySenders> {
        let place = self.shared.senders.take().ok_or(TooManySenders)?;
        Ok(Self {
            shared: Arc::clone(&self.shared),
            place,
        })
    }

    /// Sends `value` if a slot is free and the channel is open. Otherwise the value comes back.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        self.try_send_shared(value)
    }

    /// [`Self::try_send`] through a shared borrow. It never waits and uses no sender place, so
    /// several threads can send through one sender at the same time.
    pub(crate) fn try_send_shared(&self, value: T) -> Result<(), TrySendError<T>> {
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
    /// The reservation holds its place in the order of the channel: no receiver can pass its
    /// slots until it ends. Thus one slow reservation stops each receiver for every sender. Fill
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
        let ring = shared.ring;
        // Ordering: `Relaxed`: a claim publishes no data. The stamps order the slots.
        let mut tail = shared.tail.0.load(Ordering::Relaxed);
        'claim: loop {
            if tail & CLOSED != 0 {
                return Err(Refusal::Closed);
            }
            let position = tail & POSITION;
            for offset in 0..u64::from(count) {
                let slot = ring.advance(position, offset);
                // Ordering: `Acquire` reads the release of the receive that freed the slot, so its
                // read of the value happens before the write of this claim.
                let stamp = shared.slot(slot).stamp.load(Ordering::Acquire);
                if turn(stamp, ring.lap(slot)) != Turn::Same || stamp & STATE != FREE {
                    // The slot holds a value of the lap before, or another claim took it.
                    let fresh = shared.tail.0.load(Ordering::Relaxed);
                    if fresh == tail {
                        return Err(Refusal::Full);
                    }
                    tail = fresh;
                    continue 'claim;
                }
            }
            let next = ring.advance(position, u64::from(count));
            // Ordering: `Relaxed`, as above. The total order of `tail` puts the claim before or
            // after a close.
            match shared
                .tail
                .0
                .compare_exchange_weak(tail, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    shared.pass_on(&shared.senders, SENDER_WAITS, next, |state| state == FREE);
                    return Ok(position);
                }
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
    /// when the receivers are gone.
    #[expect(unsafe_code, reason = "the claim gives the sender the slot until its publication")]
    fn send_claimed(&self, position: u64, value: T) -> Result<(), T> {
        let shared = &*self.shared;
        // SAFETY: the sender claimed the slot, and did not publish it.
        unsafe { shared.write(position, value) };
        if shared.publish(position, FULL) {
            Ok(())
        } else {
            // SAFETY: the receivers are gone, and no claim follows the abort, so the sender has
            // the only access to the slot again.
            Err(unsafe { shared.take(position) })
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
        let shared = &*self.shared;
        let result = match self.send_now(value) {
            Err((Refusal::Full, value)) => {
                register(shared.senders.waiter(self.place));
                *waits = true;
                shared.senders.wait(self.place);
                // Ordering: `Relaxed`, as in `claim`.
                let tail = shared.tail.0.load(Ordering::Relaxed);
                let position = tail & POSITION;
                // Ordering: `AcqRel`, as for the bit of a wait (module docs).
                let stamp = shared.slot(position).stamp.fetch_or(SENDER_WAITS, Ordering::AcqRel);
                // The slot still holds the lap before: its free wakes a waiting sender.
                if tail & CLOSED == 0 && turn(stamp, shared.ring.lap(position)) == Turn::Previous {
                    return Err(value);
                }
                match self.send_now(value) {
                    // Another sender took the free slot. Its publication reads the bit that this
                    // wait set, and wakes a waiting sender.
                    Err((Refusal::Full, value)) => return Err(value),
                    result => result,
                }
            }
            result => result,
        };
        if *waits {
            shared.senders.unwait(self.place, false);
            *waits = false;
        }
        Ok(result.map_err(|(_, value)| SendError(value)))
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let shared = &*self.shared;
        if shared.senders.release(self.place) {
            shared.close(CLOSED);
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

/// A waiting send, from [`Sender::send`]. It gives `Ok(())` when a receiver can take the value,
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
/// published. A drop with no publication drops the written values, and the receivers pass the
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

    /// Publishes the written values, in order, and lets the receivers pass the slots that are not
    /// written. The publication can come after [`Receiver::close`]. If the receivers aborted the
    /// channel, the send fails: the channel drops the written values and gives [`Aborted`].
    pub fn publish(mut self) -> Result<(), Aborted> {
        if self.settle(true) { Ok(()) } else { Err(Aborted) }
    }

    /// Ends each slot that is not ended yet: publishes the written values if `publish` is true,
    /// and otherwise drops them. Gives `false` if the receivers are gone. A slot ends before the
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
                if !shared.publish(position, FULL) {
                    received = false;
                    // SAFETY: the receivers are gone, and no claim follows the abort, so the
                    // sender has the only access to the slot again.
                    drop(unsafe { shared.take(position) });
                }
            } else {
                // The value leaves the slot before the slot ends: after the end, a receiver can
                // pass the slot, and another claim can write it.
                // SAFETY: the reservation wrote the slot and did not publish it.
                let value = written.then(|| unsafe { shared.take(position) });
                if !shared.publish(position, SKIP) {
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

/// A receiving endpoint. It has one receiver place of the channel. The drop of the last receiver
/// is the abort: it drops the values that are in the channel, and later sends fail.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    place: u32,
}

impl<T> Receiver<T> {
    /// Makes another receiver of the channel, which takes a free receiver place.
    pub fn try_clone(&self) -> Result<Self, TooManyReceivers> {
        let place = self.shared.receivers.take().ok_or(TooManyReceivers)?;
        Ok(Self {
            shared: Arc::clone(&self.shared),
            place,
        })
    }

    /// Takes the next value, if one is ready. Each value goes to exactly one receiver.
    #[expect(
        unsafe_code,
        reason = "the claim of a published slot gives it to this receiver until it frees it"
    )]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let shared = &*self.shared;
        let ring = shared.ring;
        loop {
            // Ordering: `Relaxed`: the stamp below orders the value.
            let head = shared.head.0.load(Ordering::Relaxed);
            // Ordering: `Acquire` reads the release of the publication, so the write of the value
            // happens before its read.
            let stamp = shared.slot(head).stamp.load(Ordering::Acquire);
            match turn(stamp, ring.lap(head)) {
                Turn::Same if stamp & STATE == FULL || stamp & STATE == SKIP => {
                    let next = ring.advance(head, 1);
                    // Ordering: `Relaxed`: the stamp orders the value, and the claim publishes no
                    // data.
                    if shared
                        .head
                        .0
                        .compare_exchange_weak(head, next, Ordering::Relaxed, Ordering::Relaxed)
                        .is_err()
                    {
                        continue;
                    }
                    if stamp & STATE == SKIP {
                        shared.free(head);
                        continue;
                    }
                    // SAFETY: the claim of the published slot gives it to this receiver, until
                    // the free below.
                    let value = unsafe { shared.take(head) };
                    shared.free(head);
                    shared.pass_on(&shared.receivers, RECEIVER_WAITS, next, |state| {
                        state == FULL || state == SKIP
                    });
                    return Ok(value);
                }
                // Not published yet, or the slot still holds the lap before.
                Turn::Same | Turn::Previous => {
                    return Err(if shared.ended(head) {
                        TryRecvError::Closed
                    } else {
                        TryRecvError::Empty
                    });
                }
                // Another receiver took the slot.
                Turn::Later => {}
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
    /// The receivers still get each value that is in the channel or in an accepted reservation.
    pub fn close(&mut self) {
        self.shared.close(CLOSED);
    }

    /// One step of a waiting receive: takes a value, or registers with `register` and rechecks.
    /// Gives `None` to wait for a wake.
    fn recv_step(&mut self, waits: &mut bool, register: impl Fn(&Waiter)) -> Option<Result<T, Closed>> {
        if *waits {
            self.shared.receivers.unwait(self.place, false);
            *waits = false;
        }
        loop {
            match self.try_recv() {
                Err(TryRecvError::Empty) => {}
                result => return Some(result.map_err(|_| Closed)),
            }
            let shared = &*self.shared;
            register(shared.receivers.waiter(self.place));
            shared.receivers.wait(self.place);
            // Ordering: `Relaxed`, as in `try_recv`.
            let head = shared.head.0.load(Ordering::Relaxed);
            // Ordering: `AcqRel`, as for the bit of a wait (module docs).
            let stamp = shared.slot(head).stamp.fetch_or(RECEIVER_WAITS, Ordering::AcqRel);
            let pending = match turn(stamp, shared.ring.lap(head)) {
                Turn::Same => stamp & STATE == FREE,
                // Its free wakes a waiting receiver, which then waits on the lap of `head`.
                Turn::Previous => true,
                Turn::Later => false,
            };
            if pending && !shared.ended(head) {
                *waits = true;
                return None;
            }
            shared.receivers.unwait(self.place, false);
        }
    }
}

impl<T> Drop for Receiver<T> {
    #[expect(
        unsafe_code,
        reason = "after the abort, a published slot is the last receiver's, and no claim follows"
    )]
    fn drop(&mut self) {
        let shared = &*self.shared;
        if !shared.receivers.release(self.place) {
            return;
        }
        shared.close(CLOSED | ABORTED);
        // Ordering: `Relaxed`: the release of each receiver place orders the claims of the other
        // receivers before this drop. After the abort, no claim changes `tail`.
        let head = shared.head.0.load(Ordering::Relaxed);
        let tail = shared.tail.0.load(Ordering::Relaxed) & POSITION;
        let mut position = head;
        while position != tail {
            let old = shared.change(position, stamp(0, ABANDONED));
            if old & STATE == FULL {
                // SAFETY: the slot holds a published value, the sender is done with it, and no
                // receiver is left.
                drop(unsafe { shared.take(position) });
            }
            position = shared.ring.advance(position, 1);
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("place", &self.place)
            .finish_non_exhaustive()
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
            self.receiver.shared.receivers.unwait(self.receiver.place, true);
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
