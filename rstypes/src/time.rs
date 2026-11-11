//! Clock domains, instants and deadlines, and physical-time readings.
//!
//! A [`ClockDomain`] identifies one monotonic clock and states whether system suspend advances
//! it. An [`Instant`] is a point on one domain. A [`Deadline`] is an absolute instant: it keeps
//! its domain and its instant, so a delay before a poll does not move it. Values of two domains
//! do not compare, and arithmetic that overflows returns an error.
//!
//! A [`Reading`] is physical time: an estimate, an uncertainty bound or "unknown", and its source.
//! A reading is separate from the clock domains, so a correction of the civil clock never moves a
//! deadline.
//!
//! This module has no clock. A clock source gives the instants of its domain and the readings.
//!
//! ```
//! # extern crate rstypes as rsasync;
//! use core::time::Duration;
//! use rsasync::time::{ClockDomain, Deadline, Instant, Suspend, TimeError};
//!
//! let domain = ClockDomain::new(1, Suspend::Pauses);
//! let start = Instant::new(domain, 1_000_000);
//! let deadline = Deadline::at(start.checked_add(Duration::from_millis(5))?);
//!
//! // A late poll reads a later instant. The deadline does not move.
//! assert!(!deadline.is_reached(Instant::new(domain, 5_999_999))?);
//! assert!(deadline.is_reached(Instant::new(domain, 90_000_000))?);
//!
//! // An instant of a different domain does not compare.
//! let other = Instant::new(ClockDomain::new(2, Suspend::Advances), 1_000_000);
//! assert_eq!(start.compare(other), Err(TimeError::DomainMismatch));
//! # Ok::<(), TimeError>(())
//! ```

use core::{cmp::Ordering, error, fmt, time::Duration};

/// Whether system suspend advances a clock domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Suspend {
    /// The clock counts the time while the system is suspended.
    Advances,
    /// The clock stops while the system is suspended.
    Pauses,
}

/// The identity of one monotonic clock domain.
///
/// Two domains are the same domain only when their identifiers and their suspend behaviors are
/// equal. Each clock source gives its domain an identifier that no other domain uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ClockDomain {
    id: u32,
    suspend: Suspend,
}

impl ClockDomain {
    /// Returns the domain with the identifier `id` and the suspend behavior `suspend`.
    pub const fn new(id: u32, suspend: Suspend) -> Self {
        Self { id, suspend }
    }

    /// Returns the identifier of the domain.
    pub const fn id(self) -> u32 {
        self.id
    }

    /// Returns whether system suspend advances the domain.
    pub const fn suspend(self) -> Suspend {
        self.suspend
    }
}

/// A point on one clock domain, in nanoseconds since the origin of the domain.
///
/// An instant compares only with an instant of the same domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Instant {
    domain: ClockDomain,
    nanos: u64,
}

impl Instant {
    /// Returns the instant `nanos` nanoseconds after the origin of `domain`.
    pub const fn new(domain: ClockDomain, nanos: u64) -> Self {
        Self { domain, nanos }
    }

    /// Returns the domain of the instant.
    pub const fn domain(self) -> ClockDomain {
        self.domain
    }

    /// Returns the nanoseconds since the origin of the domain.
    pub const fn as_nanos(self) -> u64 {
        self.nanos
    }

    /// Compares this instant with `other`.
    ///
    /// # Errors
    ///
    /// [`TimeError::DomainMismatch`] when `other` is on a different domain.
    pub fn compare(self, other: Self) -> Result<Ordering, TimeError> {
        self.same_domain(other)?;
        Ok(self.nanos.cmp(&other.nanos))
    }

    /// Returns the instant `duration` after this instant, on the same domain.
    ///
    /// # Errors
    ///
    /// [`TimeError::Overflow`] when the result is after the last instant of the domain.
    pub fn checked_add(self, duration: Duration) -> Result<Self, TimeError> {
        let nanos = self.nanos.checked_add(nanos(duration)?).ok_or(TimeError::Overflow)?;
        Ok(Self { nanos, ..self })
    }

    /// Returns the instant `duration` before this instant, on the same domain.
    ///
    /// # Errors
    ///
    /// [`TimeError::Overflow`] when the result is before the origin of the domain.
    pub fn checked_sub(self, duration: Duration) -> Result<Self, TimeError> {
        let nanos = self.nanos.checked_sub(nanos(duration)?).ok_or(TimeError::Overflow)?;
        Ok(Self { nanos, ..self })
    }

    /// Returns the time from `earlier` to this instant.
    ///
    /// # Errors
    ///
    /// [`TimeError::DomainMismatch`] when `earlier` is on a different domain, and
    /// [`TimeError::Overflow`] when `earlier` is after this instant.
    pub fn duration_since(self, earlier: Self) -> Result<Duration, TimeError> {
        self.same_domain(earlier)?;
        let nanos = self.nanos.checked_sub(earlier.nanos).ok_or(TimeError::Overflow)?;
        Ok(Duration::from_nanos(nanos))
    }

    fn same_domain(self, other: Self) -> Result<(), TimeError> {
        if self.domain == other.domain {
            Ok(())
        } else {
            Err(TimeError::DomainMismatch)
        }
    }
}

/// Returns `duration` in nanoseconds, or [`TimeError::Overflow`] when a `u64` cannot hold it.
fn nanos(duration: Duration) -> Result<u64, TimeError> {
    u64::try_from(duration.as_nanos()).map_err(|_| TimeError::Overflow)
}

/// An absolute deadline: an instant on one clock domain.
///
/// A deadline keeps its domain and its instant. A delay before a poll does not move it, so it
/// never becomes a new duration budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Deadline(Instant);

impl Deadline {
    /// Returns the deadline at `instant`.
    pub const fn at(instant: Instant) -> Self {
        Self(instant)
    }

    /// Returns the instant of the deadline.
    pub const fn instant(self) -> Instant {
        self.0
    }

    /// Returns whether `now` is at or after the deadline.
    ///
    /// # Errors
    ///
    /// [`TimeError::DomainMismatch`] when `now` is on a different domain.
    pub fn is_reached(self, now: Instant) -> Result<bool, TimeError> {
        Ok(now.compare(self.0)?.is_ge())
    }
}

/// The source of a reading and of its uncertainty bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ReadingSource {
    /// The system clock gave the estimate and the bound, for example the maximum error that the
    /// kernel reports. When the system clock has no bound, the bound is unknown.
    SystemClock,
    /// The caller supplied the bound.
    Caller,
}

/// A physical-time reading: an estimate, an uncertainty bound or "unknown", and its source.
///
/// The estimate is the time since the Unix epoch, 1970-01-01 00:00:00 UTC, with no leap seconds.
/// The true time is between the estimate minus the bound and the estimate plus the bound. A
/// reading gives no distributed order, database visibility, or authorization.
///
/// ```
/// # extern crate rstypes as rsasync;
/// use core::time::Duration;
/// use rsasync::time::{Reading, ReadingSource, TimeError};
///
/// let estimate = Duration::from_secs(1_800_000_000);
/// let expiry = Duration::from_secs(1_800_000_060);
/// let reading = Reading::new(estimate, Some(Duration::from_millis(20)), ReadingSource::Caller);
/// // The latest time that the reading permits is before the expiry.
/// assert!(reading.latest()? < expiry);
///
/// // A check that needs a bound refuses an unknown bound.
/// let unknown = Reading::new(estimate, None, ReadingSource::SystemClock);
/// assert_eq!(unknown.latest(), Err(TimeError::UnknownBound));
/// # Ok::<(), TimeError>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Reading {
    estimate: Duration,
    bound: Option<Duration>,
    source: ReadingSource,
}

impl Reading {
    /// Returns a reading. A `bound` of `None` is an unknown bound.
    pub const fn new(estimate: Duration, bound: Option<Duration>, source: ReadingSource) -> Self {
        Self {
            estimate,
            bound,
            source,
        }
    }

    /// Returns the estimate: the time since the Unix epoch.
    pub const fn estimate(self) -> Duration {
        self.estimate
    }

    /// Returns the uncertainty bound, or `None` when the bound is unknown.
    pub const fn bound(self) -> Option<Duration> {
        self.bound
    }

    /// Returns the source of the reading.
    pub const fn source(self) -> ReadingSource {
        self.source
    }

    /// Returns the earliest time that the reading permits: the estimate minus the bound.
    ///
    /// # Errors
    ///
    /// [`TimeError::UnknownBound`] when the bound is unknown, and [`TimeError::Overflow`] when the
    /// result is before the Unix epoch.
    pub fn earliest(self) -> Result<Duration, TimeError> {
        let bound = self.bound.ok_or(TimeError::UnknownBound)?;
        self.estimate.checked_sub(bound).ok_or(TimeError::Overflow)
    }

    /// Returns the latest time that the reading permits: the estimate plus the bound.
    ///
    /// # Errors
    ///
    /// [`TimeError::UnknownBound`] when the bound is unknown, and [`TimeError::Overflow`] when a
    /// [`Duration`] cannot hold the result.
    pub fn latest(self) -> Result<Duration, TimeError> {
        let bound = self.bound.ok_or(TimeError::UnknownBound)?;
        self.estimate.checked_add(bound).ok_or(TimeError::Overflow)
    }
}

/// The reason that a time operation refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TimeError {
    /// The values are on different clock domains.
    DomainMismatch,
    /// The result is out of the range of its type, for example a negative duration.
    Overflow,
    /// The operation needs an uncertainty bound, and the bound of the reading is unknown.
    UnknownBound,
}

impl fmt::Display for TimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DomainMismatch => "the values are on different clock domains",
            Self::Overflow => "the result is out of range",
            Self::UnknownBound => "the uncertainty bound of the reading is unknown",
        })
    }
}

impl error::Error for TimeError {}

#[cfg(test)]
mod tests {
    use core::{cmp::Ordering, time::Duration};

    use super::{ClockDomain, Deadline, Instant, Reading, ReadingSource, Suspend, TimeError};

    const DOMAIN: ClockDomain = ClockDomain::new(1, Suspend::Pauses);

    #[test]
    fn instants_of_two_domains_refuse_to_compare() {
        let here = Instant::new(DOMAIN, 10);
        // Another identifier, and the same identifier with another suspend behavior.
        for domain in [
            ClockDomain::new(2, Suspend::Pauses),
            ClockDomain::new(1, Suspend::Advances),
        ] {
            let there = Instant::new(domain, 10);
            assert_eq!(here.compare(there), Err(TimeError::DomainMismatch));
            assert_eq!(there.duration_since(here), Err(TimeError::DomainMismatch));
            assert_eq!(Deadline::at(here).is_reached(there), Err(TimeError::DomainMismatch));
        }
        assert_eq!(here.compare(Instant::new(DOMAIN, 11)), Ok(Ordering::Less));
    }

    #[test]
    fn arithmetic_that_overflows_returns_an_error() {
        let one = Duration::from_nanos(1);
        let first = Instant::new(DOMAIN, 0);
        let last = Instant::new(DOMAIN, u64::MAX);
        assert_eq!(Instant::new(DOMAIN, u64::MAX - 1).checked_add(one), Ok(last));
        assert_eq!(last.checked_add(one), Err(TimeError::Overflow));
        assert_eq!(first.checked_sub(one), Err(TimeError::Overflow));
        // More nanoseconds than a `u64` holds: a truncating conversion would give a small value.
        assert_eq!(
            first.checked_add(Duration::from_secs(u64::MAX)),
            Err(TimeError::Overflow)
        );
        assert_eq!(
            last.checked_sub(Duration::from_secs(u64::MAX)),
            Err(TimeError::Overflow)
        );
        assert_eq!(last.duration_since(first), Ok(Duration::from_nanos(u64::MAX)));
        assert_eq!(first.duration_since(Instant::new(DOMAIN, 1)), Err(TimeError::Overflow));

        let reading = |estimate, bound| Reading::new(estimate, Some(bound), ReadingSource::Caller);
        assert_eq!(reading(Duration::MAX, one).latest(), Err(TimeError::Overflow));
        // The earliest time would be before the Unix epoch.
        assert_eq!(reading(one, 2 * one).earliest(), Err(TimeError::Overflow));
    }

    #[test]
    fn deadline_is_reached_at_its_instant_after_any_delay() {
        let start = Instant::new(DOMAIN, 1_000);
        let deadline = Deadline::at(start.checked_add(Duration::from_millis(5)).unwrap());
        for delay in [
            0,
            1,
            4_999_999,
            5_000_000,
            5_000_001,
            3_600_000_000_000,
            u64::MAX - 1_000,
        ] {
            let now = Instant::new(DOMAIN, 1_000 + delay);
            assert_eq!(deadline.is_reached(now), Ok(delay >= 5_000_000), "delay of {delay} ns");
        }
    }

    #[test]
    fn unknown_bound_is_reported_and_refused() {
        let estimate = Duration::from_secs(1_800_000_000);
        let unknown = Reading::new(estimate, None, ReadingSource::SystemClock);
        assert_eq!(unknown.bound(), None);
        assert_eq!(unknown.earliest(), Err(TimeError::UnknownBound));
        assert_eq!(unknown.latest(), Err(TimeError::UnknownBound));

        // A zero bound is known, not unknown.
        let exact = Reading::new(estimate, Some(Duration::ZERO), ReadingSource::Caller);
        assert_eq!((exact.earliest(), exact.latest()), (Ok(estimate), Ok(estimate)));
        let bounded = Reading::new(estimate, Some(Duration::from_millis(250)), ReadingSource::Caller);
        assert_eq!(bounded.earliest(), Ok(Duration::new(1_799_999_999, 750_000_000)));
        assert_eq!(bounded.latest(), Ok(Duration::new(1_800_000_000, 250_000_000)));
    }
}
