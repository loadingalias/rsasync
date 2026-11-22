//! Clock sources: the native monotonic clock of each clock domain, and an injected clock.
//!
//! A clock gives the instants of one [`ClockDomain`]. Each domain states whether system suspend
//! advances it, so a caller selects the clock by what a deadline must measure:
//!
//! - [`Suspend::Advances`](crate::time::Suspend::Advances): the time also passes while the system is suspended. Use it for a
//!   deadline that the world sets, for example a lease or a protocol timeout.
//! - [`Suspend::Pauses`](crate::time::Suspend::Pauses): the time stops while the system is suspended. Use it for the time that
//!   the program runs, for example a retry interval.
//!
//! [`NativeClock`] reads the clock of the operating system, with the `std` feature. Its two
//! domains have the identifier [`NATIVE`]. [`InjectedClock`] moves only when its owner advances it,
//! so a test controls the time. It works without `std`.
//!
//! The runtime and its driver read the time only through a clock of this module: each deadline,
//! wait, and backoff, too. So the time of a run enters it at one transition.
//!
//! ```
//! use core::time::Duration;
//!
//! use rsasync::clock::InjectedClock;
//! use rsasync::time::{ClockDomain, Instant, Suspend};
//!
//! let clock = InjectedClock::new(Instant::new(ClockDomain::new(1, Suspend::Pauses), 0));
//! let start = clock.now();
//! assert_eq!(clock.now(), start);
//! let later = clock.advance(Duration::from_millis(5))?;
//! assert_eq!(later.duration_since(start)?, Duration::from_millis(5));
//! # Ok::<(), rsasync::time::TimeError>(())
//! ```

#[cfg(feature = "std")]
mod native;
#[cfg(feature = "std")]
mod physical;
#[cfg(test)]
mod tests;

use core::fmt;
use core::time::Duration;

use crate::sync::Arc;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::time::{ClockDomain, Instant, TimeError};

#[cfg(feature = "std")]
pub use native::NativeClock;
#[cfg(feature = "std")]
pub use physical::PhysicalClock;

/// The identifier of the two native clock domains. They differ by their
/// [`Suspend`](crate::time::Suspend) value. An
/// injected clock uses another identifier.
pub const NATIVE: u32 = u32::MAX;

/// The target has no native clock for the requested domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unsupported;

impl fmt::Display for Unsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the target has no native clock for this clock domain")
    }
}

impl core::error::Error for Unsupported {}

/// A clock that moves only when its owner advances it. Each clone shares the same time.
///
/// The time is one atomic count of nanoseconds. Each access is `Relaxed`: the clock publishes no
/// other data, and the reads of one location see its changes in one order, so the clock never
/// goes back for a reader.
#[derive(Clone)]
pub struct InjectedClock(Arc<Shared>);

struct Shared {
    domain: ClockDomain,
    nanos: AtomicU64,
}

impl InjectedClock {
    /// A clock at `start`, on the domain of `start`.
    #[must_use]
    pub fn new(start: Instant) -> Self {
        Self(Arc::new(Shared {
            domain: start.domain(),
            nanos: AtomicU64::new(start.as_nanos()),
        }))
    }

    /// The domain of the clock.
    #[must_use]
    pub fn domain(&self) -> ClockDomain {
        self.0.domain
    }

    /// The current instant of the clock.
    #[must_use]
    pub fn now(&self) -> Instant {
        Instant::new(self.0.domain, self.0.nanos.load(Ordering::Relaxed))
    }

    /// Moves the clock forward by `by`, and gives the new instant.
    ///
    /// # Errors
    ///
    /// [`TimeError::Overflow`] when the new instant is after the last instant of the domain. The
    /// clock does not move.
    pub fn advance(&self, by: Duration) -> Result<Instant, TimeError> {
        let by = u64::try_from(by.as_nanos()).map_err(|_| TimeError::Overflow)?;
        let mut nanos = self.0.nanos.load(Ordering::Relaxed);
        loop {
            let later = nanos.checked_add(by).ok_or(TimeError::Overflow)?;
            match self
                .0
                .nanos
                .compare_exchange_weak(nanos, later, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Ok(Instant::new(self.0.domain, later)),
                Err(now) => nanos = now,
            }
        }
    }
}

impl fmt::Debug for InjectedClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InjectedClock").field("now", &self.now()).finish()
    }
}

/// A deadline and the clock that reads it: the end of a timed wait of an owner or a driver.
#[cfg(feature = "local")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Until {
    pub(crate) instant: Instant,
    clock: NativeClock,
}

#[cfg(feature = "local")]
impl Until {
    /// The deadline `instant`, which `clock` reads.
    pub(crate) fn new(instant: Instant, clock: NativeClock) -> Self {
        assert!(
            instant.domain() == clock.domain(),
            "a deadline is on the domain of its clock"
        );
        Self { instant, clock }
    }

    /// The time from now until the deadline, or zero when it passed.
    pub(crate) fn remaining(self) -> Duration {
        self.instant.duration_since(self.clock.now()).unwrap_or(Duration::ZERO)
    }
}
