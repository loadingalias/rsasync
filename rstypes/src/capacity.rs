//! Capacity permits, and the caller-owned budget that contains them.
//!
//! A [`Budget`] holds a fixed number of capacity units, for example bytes of memory. The caller
//! makes it and gives clones of it to the runtime, to each library, and to its own code. Thus all of
//! their charges come from one total. Each charge is a [`Permit`], which releases its units one
//! time: when it drops, or when [`Permit::release`] ends it. A permit can split, and its parts move
//! to other owners and threads. A [`Credit`] lets one worker or session charge many small amounts
//! with no update of the shared budget, and it returns its unused units in batches.
//!
//! Units are `u64` on every target, also on 32-bit targets, so a total never depends on the pointer
//! width.
//!
//! ```
//! # extern crate rstypes as rsasync;
//! use core::num::NonZeroU64;
//!
//! use rsasync::capacity::{Budget, Exhausted, Permit};
//!
//! /// A library that charges the memory of each connection that it accepts.
//! struct Server {
//!     budget: Budget,
//! }
//!
//! impl Server {
//!     fn accept(&self) -> Result<Permit, Exhausted> {
//!         self.budget.reserve(16 * 1024)
//!     }
//! }
//!
//! // The application makes the one budget, and gives a clone to each library.
//! let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
//! let server = Server { budget: budget.clone() };
//! let cache = budget.clone(); // a second library
//!
//! let connection = server.accept()?;
//! let entry = cache.reserve(40 * 1024)?;
//! assert_eq!(budget.available(), 8 * 1024);
//!
//! // The next connection does not fit. The refusal tells what is free, and nothing changes.
//! let refused = server.accept().unwrap_err();
//! assert_eq!((refused.requested, refused.available), (16 * 1024, 8 * 1024));
//!
//! // A release makes the units free again, one time.
//! drop(entry);
//! connection.release();
//! assert_eq!(budget.available(), 64 * 1024);
//! # Ok::<(), Exhausted>(())
//! ```

use core::fmt;
use core::num::NonZeroU64;

use crate::sync::Arc;
use crate::sync::atomic::{AtomicU64, Ordering};

/// A caller-owned budget of capacity units. Each clone shares the same units.
///
/// The bound is not zero. A reservation of zero units gives an empty permit, also when no unit is
/// free.
#[derive(Clone)]
pub struct Budget {
    inner: Arc<Inner>,
}

struct Inner {
    bound: u64,
    /// The bound minus the units that permits and credits hold.
    available: AtomicU64,
}

/// A refusal: the source has fewer units than the request. A refusal changes nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Exhausted {
    /// The units that the request asked for.
    pub requested: u64,
    /// The units that were free in the source when it refused.
    pub available: u64,
}

impl fmt::Display for Exhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the request needs {} capacity units, but only {} are free",
            self.requested, self.available
        )
    }
}

impl core::error::Error for Exhausted {}

impl Budget {
    /// Makes a budget of `bound` units, all of them free.
    pub fn new(bound: NonZeroU64) -> Self {
        Self {
            inner: Arc::new(Inner {
                bound: bound.get(),
                available: AtomicU64::new(bound.get()),
            }),
        }
    }

    /// The number of units in the budget.
    pub fn bound(&self) -> u64 {
        self.inner.bound
    }

    /// The number of free units now. Other owners can change it at any time.
    pub fn available(&self) -> u64 {
        self.inner.available.load(Ordering::Relaxed)
    }

    /// Reserves `amount` units, or refuses if fewer are free.
    pub fn reserve(&self, amount: u64) -> Result<Permit, Exhausted> {
        let amount = self.take(amount, amount)?;
        Ok(Permit {
            budget: self.clone(),
            amount,
        })
    }

    /// Makes an empty credit that keeps at most `idle_bound` idle units.
    pub fn credit(&self, idle_bound: u64) -> Credit {
        Credit {
            held: Permit {
                budget: self.clone(),
                amount: 0,
            },
            charged: 0,
            idle_bound,
        }
    }

    /// Takes as many free units as possible, up to `max`, but at least `min`. Gives the number
    /// that it took, or refuses and takes none.
    fn take(&self, min: u64, max: u64) -> Result<u64, Exhausted> {
        if max == 0 {
            return Ok(0);
        }
        // Ordering: the compare and exchange is one atomic update, so two owners cannot take the
        // same units. `Acquire` on success pairs with the `Release` of `give`: each use of units
        // before their release happens before each use after a new reservation of them.
        let mut available = self.inner.available.load(Ordering::Relaxed);
        loop {
            if available < min {
                return Err(Exhausted {
                    requested: min,
                    available,
                });
            }
            let taken = available.min(max);
            match self.inner.available.compare_exchange_weak(
                available,
                available - taken,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(taken),
                Err(now) => available = now,
            }
        }
    }

    /// Gives back `amount` units that a permit or a credit took.
    fn give(&self, amount: u64) {
        if amount > 0 {
            // Ordering: see `take`.
            self.inner.available.fetch_add(amount, Ordering::Release);
        }
    }
}

impl fmt::Debug for Budget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Budget")
            .field("bound", &self.bound())
            .field("available", &self.available())
            .finish()
    }
}

/// Units that one owner holds from a budget.
///
/// A permit releases its units one time, when it drops or when [`release`](Self::release) ends
/// it. It can move to a different owner or thread, and it then releases there. A part that
/// [`split`](Self::split) gives is a separate permit: it releases its own units, also after the
/// permit that it came from drops.
#[derive(Debug)]
pub struct Permit {
    budget: Budget,
    amount: u64,
}

impl Permit {
    /// The number of units that the permit holds.
    pub fn amount(&self) -> u64 {
        self.amount
    }

    /// Moves `amount` of the units into a new permit, or refuses if the permit holds fewer.
    pub fn split(&mut self, amount: u64) -> Result<Self, Exhausted> {
        let Some(rest) = self.amount.checked_sub(amount) else {
            return Err(Exhausted {
                requested: amount,
                available: self.amount,
            });
        };
        self.amount = rest;
        Ok(Self {
            budget: self.budget.clone(),
            amount,
        })
    }

    /// Releases the units now. A drop does the same.
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.budget.give(self.amount);
    }
}

/// Credit of one worker or session: units that it holds from a budget and charges with no update
/// of the budget.
///
/// A charge takes units from the idle units of the credit. When too few are idle, the credit takes
/// more from the budget in one batch: what the charge needs, and more up to its idle bound if the
/// budget has them. A refund makes charged units idle again, and the credit gives the idle units
/// above its idle bound back to the budget. Thus the credit never keeps more idle units than its
/// bound, and its idle units stay reserved in the budget.
///
/// Charges end with the credit: when it drops, it releases its charged and idle units. A charge
/// that can outlive the credit takes a permit with [`split`](Self::split) instead.
///
/// ```
/// # extern crate rstypes as rsasync;
/// use core::num::NonZeroU64;
///
/// use rsasync::capacity::Budget;
///
/// let budget = Budget::new(NonZeroU64::new(1024 * 1024).unwrap());
///
/// // A session charges each frame to its credit, and keeps at most 64 KiB idle.
/// let mut session = budget.credit(64 * 1024);
/// for _ in 0..100 {
///     session.charge(512)?; // only the first charge takes units from the budget
/// }
/// assert_eq!(session.charged(), 100 * 512);
///
/// // The refund keeps 64 KiB idle, and they stay reserved in the budget.
/// session.refund(100 * 512)?;
/// assert_eq!(session.idle(), 64 * 1024);
/// assert_eq!(budget.available(), 1024 * 1024 - 64 * 1024);
///
/// drop(session);
/// assert_eq!(budget.available(), 1024 * 1024);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct Credit {
    /// All of the units of the credit: the charged units, and then the idle units.
    held: Permit,
    charged: u64,
    idle_bound: u64,
}

/// A refusal of a refund: the credit has fewer charged units than the refund. A refusal changes
/// nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotCharged {
    /// The units that the refund gave back.
    pub refunded: u64,
    /// The units that the credit had charged.
    pub charged: u64,
}

impl fmt::Display for NotCharged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the refund gives back {} capacity units, but only {} are charged",
            self.refunded, self.charged
        )
    }
}

impl core::error::Error for NotCharged {}

impl Credit {
    /// The number of charged units.
    pub fn charged(&self) -> u64 {
        self.charged
    }

    /// The number of idle units, which the credit holds and has not charged.
    pub fn idle(&self) -> u64 {
        self.held.amount - self.charged
    }

    /// Charges `amount` units, or refuses if the idle units and the budget together have fewer.
    pub fn charge(&mut self, amount: u64) -> Result<(), Exhausted> {
        let idle = self.idle();
        if let Some(need) = amount.checked_sub(idle).filter(|&need| need > 0) {
            let taken = self
                .held
                .budget
                .take(need, need.saturating_add(self.idle_bound))
                .map_err(|refused| Exhausted {
                    requested: amount,
                    available: idle + refused.available,
                })?;
            self.held.amount += taken;
        }
        self.charged += amount;
        Ok(())
    }

    /// Makes `amount` charged units idle again, and gives the idle units above the idle bound back
    /// to the budget. Refuses if fewer units are charged.
    pub fn refund(&mut self, amount: u64) -> Result<(), NotCharged> {
        let Some(charged) = self.charged.checked_sub(amount) else {
            return Err(NotCharged {
                refunded: amount,
                charged: self.charged,
            });
        };
        self.charged = charged;
        if let Some(excess) = self.idle().checked_sub(self.idle_bound).filter(|&excess| excess > 0) {
            self.held.amount -= excess;
            self.held.budget.give(excess);
        }
        Ok(())
    }

    /// Takes `amount` units into a separate permit, as [`charge`](Self::charge) takes them. The
    /// permit releases them to the budget, also after the credit drops.
    pub fn split(&mut self, amount: u64) -> Result<Permit, Exhausted> {
        self.charge(amount)?;
        self.charged -= amount;
        self.held.amount -= amount;
        Ok(Permit {
            budget: self.held.budget.clone(),
            amount,
        })
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, loom))]
mod explore;
