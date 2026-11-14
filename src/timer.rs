//! Timer storage: bounded timers that fire against the instants that the caller gives.
//!
//! A [`Timers`] storage holds a fixed number of timers on one clock domain. Its owner, the home,
//! inserts each timer, and drives the storage with [`Timers::turn`] and an instant of that domain.
//! The storage reads no clock, so a test gives it the instants of an injected clock. No scheduler
//! and no driver is necessary.
//!
//! A [`Timer`] is a future. It gives `Ok(())` at its expiry, and [`Closed`] when the storage
//! closes first. The future can move to another thread while the storage stays at its home. A
//! drop of the future cancels the timer, and [`Timer::cancel`] also tells what ended it first.
//!
//! - Resolution: the storage counts time in ticks of [`Timers::resolution`]. A deadline goes up to
//!   the start of the next tick, so a timer never fires before its deadline. A timer fires at the
//!   first turn at or after the start of its tick. When the home turns at [`Timers::next_turn`],
//!   and no more timers are due than the limit of the turn, each timer fires at most
//!   [`Timers::max_coarsening`] after its deadline.
//! - Limit: a turn fires at most the limit that the home gives it. When more timers are due, they
//!   fire in deadline order at the timer resolution in the turns that follow, and none fires
//!   early.
//! - Range: a deadline is at most `2^36 - 1` ticks after the current tick of the storage, about
//!   795 days at a resolution of 1 ms. [`Timers::latest`] gives the latest deadline that the
//!   storage takes now. A later deadline refuses with [`Refused::Range`], and it never wraps.
//! - A past deadline: a deadline at or before the instant of the last turn is due at the next
//!   turn.
//! - Capacity: a full storage refuses with [`Refused::Full`]. A dropped timer frees its place.
//! - Memory: the storage allocates one time, when it starts, and charges [`Timers::memory`] bytes
//!   to a caller's budget before it allocates.
//!
//! ```
//! use core::{future::Future, num::NonZeroU32, num::NonZeroU64, pin::pin, task::Context};
//! use core::{task::Poll, task::Waker, time::Duration};
//!
//! use rsasync::capacity::Budget;
//! use rsasync::time::{ClockDomain, Deadline, Instant, Suspend};
//! use rsasync::timer::Timers;
//!
//! let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
//! let domain = ClockDomain::new(1, Suspend::Pauses);
//! let at = |nanos| Instant::new(domain, nanos);
//! let tick = Duration::from_millis(1);
//! let mut timers = Timers::new(NonZeroU32::new(64).unwrap(), tick, at(0), &budget)?;
//!
//! // A deadline of 2.5 ms goes up to the start of the third tick, at 3 ms.
//! let mut timer = pin!(timers.insert(Deadline::at(at(2_500_000)))?);
//! assert_eq!(timers.next_turn(), Some(at(3_000_000)));
//!
//! let mut cx = Context::from_waker(Waker::noop());
//! // Each turn fires at most 32 timers. `false`: no other timer is due.
//! assert!(!timers.turn(at(2_999_999), 32)?);
//! assert_eq!(timer.as_mut().poll(&mut cx), Poll::Pending);
//! assert!(!timers.turn(at(3_000_000), 32)?);
//! assert_eq!(timer.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
//! # Ok::<(), Box<dyn core::error::Error>>(())
//! ```

mod wheel;

#[cfg(all(test, loom))]
mod explore;
#[cfg(test)]
mod tests;

use alloc::boxed::Box;
use core::fmt;
use core::future::Future;
use core::num::NonZeroU32;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use crate::capacity::{Budget, Exhausted, Permit};
use crate::sync::Arc;
use crate::sync::atomic::{AtomicU32, Ordering};
use crate::time::{ClockDomain, Deadline, Instant, TimeError};
use crate::wait::Waiter;
use wheel::{RANGE, Wheel};

/// The state of a timer that waits.
const ARMED: u32 = 0;
const EXPIRED: u32 = 1;
const CLOSED: u32 = 2;

/// The end of the list of released entries.
const NIL: u32 = u32::MAX;

/// Why [`Timers::new`] made no storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Invalid {
    /// The resolution is zero, or longer than `u64::MAX` nanoseconds.
    Resolution,
    /// The memory of the capacity is larger than the address space of the target.
    Capacity,
    /// The budget has less free capacity than [`Timers::memory`].
    Exhausted(Exhausted),
}

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolution => f.write_str("the timer resolution is zero or too long"),
            Self::Capacity => f.write_str("the memory of the timer capacity does not fit the target"),
            Self::Exhausted(exhausted) => write!(f, "the memory of the timers: {exhausted}"),
        }
    }
}

impl core::error::Error for Invalid {}

/// Why [`Timers::insert`] made no timer. A refusal changes nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refused {
    /// Each place of the storage holds a timer.
    Full,
    /// The deadline is after [`Timers::latest`].
    Range,
    /// The storage is closed.
    Closed,
    /// The deadline is on a different clock domain than the storage.
    Domain,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Full => "the timer storage is full",
            Self::Range => "the deadline is after the range of the timer storage",
            Self::Closed => "the timer storage is closed",
            Self::Domain => "the deadline is on a different clock domain than the timer storage",
        })
    }
}

impl core::error::Error for Refused {}

/// The storage of a timer closed before the timer expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the timer storage closed before the timer expired")
    }
}

impl core::error::Error for Closed {}

/// What ended a timer first, as [`Timer::cancel`] gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The timer expired.
    Expired,
    /// The cancel: the timer did not expire, and it never expires.
    Cancelled,
    /// The storage closed.
    Closed,
}

/// The part of one timer that its consumer and the home share.
///
/// Only the home writes `state`, and it writes it before it wakes the waiter. The state carries no
/// other data, and the state word of the waiter orders the wake handshake (`crate::wait`), so each
/// access of `state` is `Relaxed`. A new timer of the entry gets `ARMED` before the home gives it
/// to its consumer, and the move of the timer to the consumer orders that store.
struct Entry {
    state: AtomicU32,
    waiter: Waiter,
    /// The next entry in the list of released entries.
    next: AtomicU32,
}

/// The part of a storage that its timers share.
///
/// A timer that drops releases its entry: it pushes the entry to the list at `released`, with a
/// `Release` exchange, after its last access to the entry. The home takes the whole list with an
/// `Acquire` swap, so each access of the consumer happens before the home uses the entry again.
/// The home takes only the whole list, so the list has no ABA problem. An entry is in the list at
/// most one time, because only the drop of its timer pushes it, and the home gives the entry to a
/// new timer only after it takes the entry from the list.
struct Shared {
    entries: Box<[Entry]>,
    released: AtomicU32,
    /// The memory of the storage. It releases when the last timer and the storage have dropped.
    _charge: Permit,
}

/// The storage of a fixed number of timers on one clock domain. It stays at its home, which
/// inserts, turns, and closes.
///
/// A drop of the storage closes it.
pub struct Timers {
    shared: Arc<Shared>,
    wheel: Wheel,
    capacity: NonZeroU32,
    domain: ClockDomain,
    /// The tick, in nanoseconds. It is at least 1.
    resolution: u64,
    /// The instant of the last turn, in nanoseconds.
    now: u64,
    closed: bool,
}

impl Timers {
    /// A storage of `capacity` timers, with ticks of `resolution`, on the domain of `now`. Its
    /// first tick is the tick of `now`. It reserves [`Timers::memory`] bytes from `budget`, and
    /// then allocates one time.
    ///
    /// # Errors
    ///
    /// [`Invalid`] gives the reason, and nothing changes.
    pub fn new(capacity: NonZeroU32, resolution: Duration, now: Instant, budget: &Budget) -> Result<Self, Invalid> {
        let resolution = u64::try_from(resolution.as_nanos())
            .ok()
            .filter(|&nanos| nanos > 0)
            .ok_or(Invalid::Resolution)?;
        let memory = Self::memory(capacity);
        if isize::try_from(memory).is_err() {
            return Err(Invalid::Capacity);
        }
        let charge = budget.reserve(memory).map_err(Invalid::Exhausted)?;
        let shared = Arc::new(Shared {
            entries: (0..capacity.get())
                .map(|_| Entry {
                    state: AtomicU32::new(ARMED),
                    waiter: Waiter::new(),
                    next: AtomicU32::new(NIL),
                })
                .collect(),
            released: AtomicU32::new(NIL),
            _charge: charge,
        });
        Ok(Self {
            shared,
            wheel: Wheel::new(capacity.get(), now.as_nanos() / resolution),
            capacity,
            domain: now.domain(),
            resolution,
            now: now.as_nanos(),
            closed: false,
        })
    }

    /// The bytes that a storage of `capacity` timers allocates, and charges to its budget.
    #[must_use]
    pub fn memory(capacity: NonZeroU32) -> u64 {
        // The shared part, with the two counts of its `Arc`.
        let fixed = size_of::<Shared>() + 2 * size_of::<usize>();
        let each = size_of::<Entry>() + Wheel::entry_size();
        // No overflow: each part is at most a few hundred bytes, and a capacity has 32 bits.
        fixed as u64 + u64::from(capacity.get()) * each as u64
    }

    /// The number of timers that the storage holds when it is full.
    #[must_use]
    pub fn capacity(&self) -> NonZeroU32 {
        self.capacity
    }

    /// The tick of the storage. A deadline goes up to the start of the next tick.
    #[must_use]
    pub fn resolution(&self) -> Duration {
        Duration::from_nanos(self.resolution)
    }

    /// The most that a timer fires after its deadline, when the home turns at
    /// [`Timers::next_turn`] and no more timers are due than the limit of the turn: the
    /// resolution less one nanosecond.
    #[must_use]
    pub fn max_coarsening(&self) -> Duration {
        Duration::from_nanos(self.resolution - 1)
    }

    /// The latest deadline that [`Timers::insert`] takes until the next turn: `2^36 - 1` ticks
    /// after the current tick, or the last tick of the domain if that is earlier. The current tick
    /// is the tick of the last turn, or an earlier tick when that turn stopped at its limit.
    #[must_use]
    pub fn latest(&self) -> Instant {
        // No overflow: the tick is at most `u64::MAX / resolution`.
        Instant::new(self.domain, self.latest_tick() * self.resolution)
    }

    /// Inserts a timer that expires at `deadline`. A deadline at or before the instant of the last
    /// turn is due at the next turn.
    ///
    /// # Errors
    ///
    /// [`Refused`] gives the reason, and nothing changes.
    pub fn insert(&mut self, deadline: Deadline) -> Result<Timer, Refused> {
        if self.closed {
            return Err(Refused::Closed);
        }
        let deadline = deadline.instant();
        if deadline.domain() != self.domain {
            return Err(Refused::Domain);
        }
        // Up, never down: a tick that starts before the deadline would fire early.
        let tick = deadline.as_nanos().div_ceil(self.resolution);
        let tick = if deadline.as_nanos() <= self.now {
            // The clock passed the deadline, so each later turn is on time. The tick of the last
            // turn makes the timer due at the next turn. A turn that stopped at its limit left the
            // current tick earlier: the tick keeps the order of the due timers, and each timer of
            // the wheel is at most at the latest tick.
            tick.min(self.now / self.resolution)
                .clamp(self.wheel.elapsed(), self.latest_tick())
        } else if tick > self.latest_tick() {
            return Err(Refused::Range);
        } else {
            tick
        };
        let index = match self.wheel.alloc() {
            Some(index) => index,
            None => {
                self.reclaim();
                self.wheel.alloc().ok_or(Refused::Full)?
            }
        };
        self.wheel.insert(index, tick);
        Ok(Timer {
            shared: Arc::clone(&self.shared),
            index,
        })
    }

    /// Fires at most `limit` timers whose ticks `now` reached, and wakes their consumers. Gives
    /// `true` when more timers are due: turn again before the home sleeps. The due timers fire in
    /// deadline order at the timer resolution, in this turn and in the turns that follow: by tick,
    /// and in no stated order inside one tick. A timer that was inserted with a deadline before
    /// the current tick of the storage has that current tick. The
    /// turn also frees the place of each dropped timer. An instant before the last turn fires only
    /// the timers that are due.
    ///
    /// The work is at most a fixed number of steps for each timer that the turn frees, fires, or
    /// moves to a finer level of the wheel. A tick with no timer costs nothing.
    ///
    /// # Errors
    ///
    /// [`TimeError::DomainMismatch`] when `now` is on a different domain than the storage. Nothing
    /// changes.
    pub fn turn(&mut self, now: Instant, limit: u32) -> Result<bool, TimeError> {
        if now.domain() != self.domain {
            return Err(TimeError::DomainMismatch);
        }
        self.reclaim();
        self.now = self.now.max(now.as_nanos());
        let entries = &self.shared.entries;
        Ok(self.wheel.advance(self.now / self.resolution, limit, |index| {
            end(&entries[index as usize], EXPIRED);
        }))
    }

    /// The instant of the next turn that has work. When a timer is due, it is at or before the
    /// last turn, so the home turns at once. Else it is the start of the tick of the next timer, or
    /// of the next move of timers to a finer level. `None` when the storage holds no waiting
    /// timer. A home that sleeps until this instant and then turns fires each timer within
    /// [`Timers::max_coarsening`], when no more timers are due than the limit of the turn.
    #[must_use]
    pub fn next_turn(&self) -> Option<Instant> {
        // No overflow: a tick of the wheel is at most `u64::MAX / resolution`.
        self.wheel
            .next()
            .map(|tick| Instant::new(self.domain, tick * self.resolution))
    }

    /// Closes the storage: each waiting timer gives [`Closed`] and wakes its consumer, and each
    /// later insert refuses with [`Refused::Closed`].
    pub fn close(&mut self) {
        self.closed = true;
        let entries = &self.shared.entries;
        self.wheel.clear(|index| end(&entries[index as usize], CLOSED));
    }

    /// The latest tick of a deadline that the storage takes until the next turn.
    fn latest_tick(&self) -> u64 {
        self.wheel
            .elapsed()
            .saturating_add(RANGE)
            .min(u64::MAX / self.resolution)
    }

    /// Frees the entry of each dropped timer.
    fn reclaim(&mut self) {
        // Ordering: see `Shared`.
        let mut index = self.shared.released.swap(NIL, Ordering::Acquire);
        while let Some(entry) = self.shared.entries.get(index as usize) {
            let next = entry.next.load(Ordering::Relaxed);
            entry.state.store(ARMED, Ordering::Relaxed);
            self.wheel.free(index);
            index = next;
        }
    }
}

impl Drop for Timers {
    fn drop(&mut self) {
        self.close();
    }
}

impl fmt::Debug for Timers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timers")
            .field("capacity", &self.capacity())
            .field("resolution", &self.resolution())
            .field("domain", &self.domain)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

/// Gives `state` to the timer of `entry`, which waits, and wakes its consumer.
fn end(entry: &Entry, state: u32) {
    entry.state.store(state, Ordering::Relaxed);
    entry.waiter.wake();
}

/// One timer of a [`Timers`] storage. As a future, it gives `Ok(())` at its expiry and
/// [`Closed`] when the storage closes first.
///
/// The timer can move to another thread, and its storage stays at its home. A drop cancels the
/// timer: the next turn or insert of the home frees its place.
pub struct Timer {
    shared: Arc<Shared>,
    index: u32,
}

impl Timer {
    /// Cancels the timer, and tells what ended it first. [`Outcome::Cancelled`] tells that the
    /// timer did not expire, and that it never expires.
    pub fn cancel(self) -> Outcome {
        match self.entry().state.load(Ordering::Relaxed) {
            ARMED => Outcome::Cancelled,
            EXPIRED => Outcome::Expired,
            _ => Outcome::Closed,
        }
    }

    fn entry(&self) -> &Entry {
        &self.shared.entries[self.index as usize]
    }
}

impl Future for Timer {
    type Output = Result<(), Closed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let entry = self.entry();
        entry
            .waiter
            .poll_wait(cx, || match entry.state.load(Ordering::Relaxed) {
                ARMED => None,
                EXPIRED => Some(Ok(())),
                _ => Some(Err(Closed)),
            })
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let entry = self.entry();
        // A wake that the home gives at this time takes the waker itself.
        entry.waiter.cancel();
        let released = &self.shared.released;
        let mut head = released.load(Ordering::Relaxed);
        loop {
            entry.next.store(head, Ordering::Relaxed);
            // Ordering: see `Shared`.
            match released.compare_exchange_weak(head, self.index, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => break,
                Err(now) => head = now,
            }
        }
    }
}

impl fmt::Debug for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timer")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}
