use core::time::Duration;

use super::InjectedClock;
use crate::time::{ClockDomain, Instant, Suspend, TimeError};

#[test]
fn an_injected_clock_moves_only_when_its_owner_advances_it() {
    let domain = ClockDomain::new(1, Suspend::Pauses);
    let clock = InjectedClock::new(Instant::new(domain, 7));
    let shared = clock.clone();
    assert_eq!(
        (clock.now(), clock.now()),
        (Instant::new(domain, 7), Instant::new(domain, 7))
    );
    assert_eq!(shared.advance(Duration::from_nanos(5)), Ok(Instant::new(domain, 12)));
    assert_eq!(clock.now(), Instant::new(domain, 12));
    // An advance past the last instant of the domain refuses, and the clock stays.
    assert_eq!(
        clock.advance(Duration::from_nanos(u64::MAX - 11)),
        Err(TimeError::Overflow)
    );
    assert_eq!(shared.now(), Instant::new(domain, 12));
}

/// Four threads read the clock. Each read must be at or after the latest read that another thread
/// published before it, so the clock is monotonic across the processors.
#[cfg(feature = "std")]
#[test]
fn each_native_clock_is_monotonic_across_threads_and_reports_its_resolution() {
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    use super::NativeClock;

    const THREADS: usize = 4;
    const READS: usize = 250_000;

    for suspend in [Suspend::Advances, Suspend::Pauses] {
        let clock = NativeClock::new(suspend).unwrap();
        assert!(!clock.resolution().is_zero());
        std::println!("{suspend:?}: resolution {:?}", clock.resolution());
        let first = clock.now().as_nanos();
        let latest = AtomicU64::new(first);
        thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    for _ in 0..READS {
                        let published = latest.load(Ordering::Acquire);
                        let now = clock.now().as_nanos();
                        assert!(now >= published, "{suspend:?}: {now} after {published}");
                        latest.fetch_max(now, Ordering::Release);
                    }
                });
            }
        });
        assert!(latest.into_inner() > first, "{suspend:?}: the clock did not move");
    }
    // The clock that suspend advances counts each suspend that the other one does not count.
    let pauses = NativeClock::new(Suspend::Pauses).unwrap().now();
    let advances = NativeClock::new(Suspend::Advances).unwrap().now();
    assert!(advances.as_nanos() >= pauses.as_nanos());
}
