//! Waiting reservations: a future that waits until a budget has the units that it asks for.
//!
//! [`budget`] makes a [`Budget`] and an [`Acquirer`] of it. Give clones of the budget to the
//! runtime, to each library, and to the application, as for a budget from [`Budget::new`].
//! [`Acquirer::acquire`] gives a future that waits until the budget has the units, and then gives
//! their [`Permit`]. The wait has no spin and no timed retry: the budget wakes a waiter when units
//! return to it.
//!
//! Each acquirer has one waiter place of the budget, and [`Acquirer::try_clone`] takes another
//! place. The number of places is fixed when the budget is made. A future borrows its acquirer
//! mutably, so a place has at most one wait at a time.
//!
//! ```
//! use core::num::{NonZeroU32, NonZeroU64};
//! use core::pin::pin;
//! use core::task::{Context, Poll, Waker};
//!
//! use rsasync::acquire;
//! use rsasync::capacity::Budget;
//!
//! // The memory of the waiter places comes from a memory budget.
//! let memory = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
//! // A budget of 8 connections, with 2 waiter places.
//! let places = NonZeroU32::new(2).unwrap();
//! let (connections, mut acquirer) = acquire::budget(&memory, NonZeroU64::new(8).unwrap(), places)?;
//!
//! let held = connections.reserve(8)?;
//! let mut cx = Context::from_waker(Waker::noop());
//! let mut wait = pin!(acquirer.acquire(2)?);
//! assert!(wait.as_mut().poll(&mut cx).is_pending());
//!
//! // The release wakes the waiter, and its next poll gives the permit.
//! drop(held);
//! let Poll::Ready(permit) = wait.as_mut().poll(&mut cx) else { panic!("the units are free") };
//! assert_eq!((permit.amount(), connections.available()), (2, 6));
//! # Ok::<(), Box<dyn core::error::Error>>(())
//! ```
//!
//! # Order
//!
//! 1. Waiters are served first in, first out: in the order in which their waits started. Only the
//!    first waiter takes units. When units return to the budget, the budget wakes the first
//!    waiter. When the first waiter gets its units or stops its wait, it wakes the next one.
//! 2. A new wait goes behind each waiter in the order, also when its units are free. Thus smaller
//!    requests do not pass a large one. A new wait takes free units at once only when no waiter is
//!    in the order.
//! 3. A reservation that does not wait, such as [`Budget::reserve`] or a charge of a credit, takes
//!    free units at once, also when waiters are in the order. The budget wakes the first waiter
//!    after the units return, so such a reservation can take them before the poll of the waiter.
//!    The woken waiter then finds too few units and waits again at its place. Thus a stream of
//!    small reservations that do not wait can delay a large waiter for as long as the stream
//!    continues.
//!
//! A wait for zero units ends at once, also when waiters are in the order.
//!
//! # Cancellation
//!
//! A drop of a future stops its wait. A future holds no units before it is ready, so a drop
//! releases nothing and changes no total. When the future was the first waiter, the drop wakes the
//! next one, also when a release woke the dropped future first.
//!
//! A forgotten future keeps its place in the order. The next wait of its acquirer continues at that
//! place, and a drop of the acquirer removes it.
//!
//! # Orderings
//!
//! The order of the waiting places is a list under a short lock. The word `head` holds the first
//! place, or `NONE`. Each change of `head` is a read-modify-write, so the changes have one total
//! order, and each reads the change before it. The holder of the lock writes `head` each time
//! that the first place changes, and wakes the new first place after it frees the lock. A release
//! does not take the lock.
//!
//! - A release gives its units back with a `Release` change of the counter of the budget, and then
//!   calls [`ReleaseListener::released`]. That changes `head` with a `Release` read-modify-write,
//!   which reads the latest first place, and wakes the waiter of that place.
//! - A first place that `head` gets after that change: the change that writes it is `Acquire`, so it
//!   reads the release. Thus the release happens before the wake of the new first place, and
//!   before its recheck.
//! - A first place that the release reads: the wake goes through the handshake of its waiter
//!   (`crate::wait`). The waiter sees the units at its recheck, or the wake reaches its waker.
//!
//! Thus a release that comes before, during, or after a wait, a cancel, or a change of the first
//! place reaches the first waiter. A plain load of `head` in the release would not do: the release
//! and a new first waiter each write one word and then read the other, and on a weak memory model
//! both reads can miss the other write. The first waiter reads `head` with `Relaxed`. It wrote
//! `head` itself, or the waiter before it wrote it and then woke it, so the read sees its place.
//!
//! The exploration tests check these orders for the words of this module. The counter of the budget
//! is in `rstypes`, and its exploration tests check its own orders.

use alloc::boxed::Box;
use core::fmt;
use core::future::Future;
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::Pin;
use core::task::{Context, Poll};

use crate::capacity::{Budget, Exhausted, Permit, ReleaseListener};
use crate::sync::{
    Mutex,
    atomic::{AtomicU32, Ordering},
};
use crate::wait::Waiter;

/// The largest number of waiter places.
pub const MAX_PLACES: u32 = 1 << 16;

/// No place: the end of a list, or an order with no waiter.
const NONE: u32 = u32::MAX;
/// In `Order::prev`: the place does not wait.
const OUT: u32 = u32::MAX - 1;

/// Why [`budget`] made no budget. A refusal changes nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refused {
    /// The number of places is above [`MAX_PLACES`].
    TooManyPlaces,
    /// The memory budget has fewer free units than [`memory`].
    Memory(Exhausted),
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyPlaces => write!(f, "a budget has at most {MAX_PLACES} waiter places"),
            Self::Memory(exhausted) => write!(f, "the memory of the waiter places: {exhausted}"),
        }
    }
}

impl core::error::Error for Refused {}

/// A refusal to clone an acquirer: each waiter place of the budget is in use. A later clone can
/// succeed after an acquirer drops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooManyWaiters;

impl fmt::Display for TooManyWaiters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("each waiter place of the budget is in use")
    }
}

impl core::error::Error for TooManyWaiters {}

/// A refusal of a wait: it asks for more units than the bound of the budget, so it could never end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AboveBound {
    /// The units that the wait asked for.
    pub requested: u64,
    /// The bound of the budget.
    pub bound: u64,
}

impl fmt::Display for AboveBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the wait asks for {} capacity units, but the bound of the budget is {}",
            self.requested, self.bound
        )
    }
}

impl core::error::Error for AboveBound {}

/// The bytes of `places` waiter places, which [`budget`] charges to its memory budget.
#[must_use]
pub fn memory(places: NonZeroU32) -> u64 {
    // The queue, with the two counts of its `Arc`.
    let fixed = size_of::<Queue>() + 2 * size_of::<usize>();
    let each = size_of::<Waiter>() + 2 * size_of::<u32>();
    // No overflow: each part is at most a few hundred bytes, and a count has 32 bits.
    fixed as u64 + u64::from(places.get()) * each as u64
}

/// Makes a budget of `bound` units whose waits use `places` waiter places, and the acquirer of the
/// first place. It charges [`memory`] bytes to `memory` before it allocates, and the charge ends
/// when the budget and each acquirer have dropped.
///
/// # Errors
///
/// [`Refused`] gives the reason, and nothing changes.
pub fn budget(memory: &Budget, bound: NonZeroU64, places: NonZeroU32) -> Result<(Budget, Acquirer), Refused> {
    if places.get() > MAX_PLACES {
        return Err(Refused::TooManyPlaces);
    }
    let charge = memory.reserve(self::memory(places)).map_err(Refused::Memory)?;
    let count = places.get();
    let queue = alloc::sync::Arc::new(Queue {
        head: AtomicU32::new(NONE),
        waiters: (0..count).map(|_| Waiter::new()).collect(),
        order: Mutex::new(Order {
            // Place 0 is in use. The free list holds the others, in order.
            next: (0..count)
                .map(|place| {
                    if place == 0 || place + 1 == count {
                        NONE
                    } else {
                        place + 1
                    }
                })
                .collect(),
            prev: (0..count).map(|_| OUT).collect(),
            first: NONE,
            last: NONE,
            free: if count > 1 { 1 } else { NONE },
        }),
        _charge: charge,
    });
    let budget = Budget::with_listener(bound, queue.clone());
    let acquirer = Acquirer {
        budget: budget.clone(),
        queue,
        place: 0,
    };
    Ok((budget, acquirer))
}

/// The waiter places of a budget, and their order. The budget holds it as its release listener.
struct Queue {
    /// The first place of the order, or `NONE`. Each change is a read-modify-write (module docs).
    head: AtomicU32,
    waiters: Box<[Waiter]>,
    order: Mutex<Order>,
    /// The memory of the places. It releases when the budget and each acquirer have dropped.
    _charge: Permit,
}

/// The list of the waiting places, in the order of their waits, and the list of the free places.
struct Order {
    /// For a waiting place, the next place in the order. For a free place, the next free place.
    next: Box<[u32]>,
    /// For a waiting place, the place before it, or `NONE` for the first. `OUT` for a place that
    /// does not wait.
    prev: Box<[u32]>,
    first: u32,
    last: u32,
    /// The first free place, or `NONE`.
    free: u32,
}

/// The index of a place in the lists.
fn at(place: u32) -> usize {
    place as usize
}

impl Order {
    /// Adds `place` at the end of the order, if it does not wait already.
    fn push(&mut self, place: u32) {
        if self.prev[at(place)] != OUT {
            // A forgotten future left it in the order: the new wait continues at its place.
            return;
        }
        self.prev[at(place)] = self.last;
        self.next[at(place)] = NONE;
        match self.last {
            NONE => self.first = place,
            last => self.next[at(last)] = place,
        }
        self.last = place;
    }

    /// Removes `place` from the order, if it waits.
    fn remove(&mut self, place: u32) {
        let (prev, next) = (self.prev[at(place)], self.next[at(place)]);
        if prev == OUT {
            return;
        }
        match prev {
            NONE => self.first = next,
            prev => self.next[at(prev)] = next,
        }
        match next {
            NONE => self.last = prev,
            next => self.prev[at(next)] = prev,
        }
        self.prev[at(place)] = OUT;
        self.next[at(place)] = NONE;
    }
}

impl Queue {
    fn waiter(&self, place: u32) -> &Waiter {
        &self.waiters[at(place)]
    }

    /// Whether `place` is the first place of the order.
    fn is_first(&self, place: u32) -> bool {
        // Ordering: `Relaxed`: the place wrote `head` itself, or the place before it wrote it and
        // then woke it (module docs).
        self.head.load(Ordering::Relaxed) == place
    }

    /// Changes the order with `change` under the lock. When the first place changes, it writes
    /// `head` and gives the new first place. Otherwise it gives `NONE`.
    fn change(&self, change: impl FnOnce(&mut Order)) -> u32 {
        let mut order = self.order.lock();
        let before = order.first;
        change(&mut order);
        if order.first == before {
            return NONE;
        }
        // Ordering: `Acquire` reads each release before it, so a release happens before the wake
        // of the new first place and its recheck. No reader of `head` acquires, so the change needs
        // no `Release`: the lock orders the changes, and a new first place gets its wake.
        self.head.swap(order.first, Ordering::Acquire);
        order.first
    }

    /// Removes a place from the order with `change`, and wakes the new first place after the lock
    /// is free.
    fn leave(&self, change: impl FnOnce(&mut Order)) {
        let first = self.change(change);
        if first != NONE {
            self.waiter(first).wake();
        }
    }
}

impl ReleaseListener for Queue {
    fn released(&self) {
        // Ordering: `Release` orders the release of the units before a later change of `head`.
        // The read-modify-write reads the latest first place (module docs).
        let first = self.head.fetch_or(0, Ordering::Release);
        if first != NONE {
            self.waiter(first).wake();
        }
    }
}

/// The waiter place of a budget from [`budget`]. Its futures wait for units of that budget.
///
/// A drop frees the place.
pub struct Acquirer {
    budget: Budget,
    queue: alloc::sync::Arc<Queue>,
    place: u32,
}

impl Acquirer {
    /// Makes an acquirer of the same budget, which takes a free waiter place.
    pub fn try_clone(&self) -> Result<Self, TooManyWaiters> {
        let mut order = self.queue.order.lock();
        let place = order.free;
        if place == NONE {
            return Err(TooManyWaiters);
        }
        order.free = order.next[at(place)];
        order.next[at(place)] = NONE;
        drop(order);
        Ok(Self {
            budget: self.budget.clone(),
            queue: self.queue.clone(),
            place,
        })
    }

    /// Gives a future that waits until the budget has `amount` free units, in the order of the
    /// waits, and then gives a permit of them. A wait for zero units ends at once.
    ///
    /// ```
    /// use core::num::{NonZeroU32, NonZeroU64};
    ///
    /// use rsasync::acquire;
    /// use rsasync::capacity::Budget;
    /// use rsasync::task::{LocalOwner, Outcome, spawn_local};
    ///
    /// // The memory of the waiter places and of the tasks comes from a memory budget.
    /// let memory = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
    /// // A budget of 4 connections, with 2 waiter places.
    /// let places = NonZeroU32::new(2).unwrap();
    /// let (connections, mut acquirer) = acquire::budget(&memory, NonZeroU64::new(4).unwrap(), places)?;
    /// let mut owner = LocalOwner::new(&memory, 4)?;
    ///
    /// let all = connections.reserve(4)?;
    /// let amount = owner.block_on(async move {
    ///     let waiter = spawn_local(async move {
    ///         // When no unit is free, the task waits until a release wakes it.
    ///         let permit = acquirer.acquire(1).expect("1 is not above the bound").await;
    ///         permit.amount()
    ///     })
    ///     .expect("the owner has a free slot");
    ///     // A second task releases the units.
    ///     let _releaser = spawn_local(async move { drop(all) }).expect("the owner has a free slot");
    ///     match waiter.join().await {
    ///         Outcome::Completed(amount) => amount,
    ///         Outcome::Cancelled | Outcome::Panicked(_) | Outcome::OwnerFailed(_) => 0,
    ///     }
    /// });
    /// assert_eq!((amount, connections.available()), (1, 4));
    /// # Ok::<(), Box<dyn core::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// [`AboveBound`] when `amount` is above the bound of the budget: no release can make the
    /// wait end.
    pub fn acquire(&mut self, amount: u64) -> Result<Acquire<'_>, AboveBound> {
        let bound = self.budget.bound();
        if amount > bound {
            return Err(AboveBound {
                requested: amount,
                bound,
            });
        }
        Ok(Acquire {
            acquirer: self,
            amount,
            waits: false,
        })
    }

    /// Ends the wait of this place: removes its waker, and removes it from the order. When it was
    /// the first place, the next place gets the wake.
    fn stop(&self) {
        self.queue.waiter(self.place).cancel();
        self.queue.leave(|order| order.remove(self.place));
    }
}

impl Drop for Acquirer {
    fn drop(&mut self) {
        let place = self.place;
        self.queue.leave(|order| {
            order.remove(place);
            order.next[at(place)] = order.free;
            order.free = place;
        });
    }
}

impl fmt::Debug for Acquirer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Acquirer")
            .field("budget", &self.budget)
            .field("place", &self.place)
            .finish()
    }
}

/// A future that waits for units of a budget, from [`Acquirer::acquire`]. It gives their permit.
///
/// A drop stops the wait. The future holds no units before it is ready, so the drop releases none.
#[must_use = "a future does nothing unless you poll it"]
pub struct Acquire<'a> {
    acquirer: &'a mut Acquirer,
    amount: u64,
    /// The place of the acquirer is in the order.
    waits: bool,
}

impl Future for Acquire<'_> {
    type Output = Permit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Permit> {
        let this = self.get_mut();
        crate::coop::poll(cx, |cx| {
            let (acquirer, amount) = (&*this.acquirer, this.amount);
            let (queue, place) = (&acquirer.queue, acquirer.place);
            if !this.waits {
                // With no waiter in the order, take the units now if they are free.
                // Ordering: `Relaxed`: an old value only selects the order, which a waiter then
                // rechecks.
                if (amount == 0 || queue.head.load(Ordering::Relaxed) == NONE)
                    && let Ok(permit) = acquirer.budget.reserve(amount)
                {
                    return Poll::Ready(permit);
                }
                // The new first place of a push can only be this place, so it needs no wake.
                queue.change(|order| order.push(place));
                this.waits = true;
            }
            let polled = queue.waiter(place).poll_wait(cx, || {
                if queue.is_first(place) {
                    acquirer.budget.reserve(amount).ok()
                } else {
                    None
                }
            });
            if polled.is_ready() {
                this.waits = false;
                acquirer.stop();
            }
            polled
        })
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        if self.waits {
            self.acquirer.stop();
        }
    }
}

impl fmt::Debug for Acquire<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Acquire")
            .field("amount", &self.amount)
            .field("waits", &self.waits)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, loom))]
mod explore;
