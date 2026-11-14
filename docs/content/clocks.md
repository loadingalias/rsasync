# Clocks

`rsasync::clock` gives the instants of a clock domain. `NativeClock` reads the monotonic clock of
the operating system. `InjectedClock` moves only when its owner advances it, so a test controls the
time. rsasync is in development, and its alpha releases are on crates.io.

## Select a clock domain

A clock domain states whether system suspend advances it (see [Shared types](shared-types.md#time)).
Select the domain by what a deadline must measure:

| Domain | The time while the system is suspended | Use it for |
| --- | --- | --- |
| `Suspend::Advances` | Counts | A deadline that the world sets, for example a lease or a protocol timeout |
| `Suspend::Pauses` | Does not count | The time that the program runs, for example a retry interval |

An instant of one domain does not compare with an instant of the other domain.

## Read the native clock

`NativeClock::new(suspend)` gives the native clock of the domain with `suspend`, or `Unsupported`
when the target has no native clock for that domain. It needs the `std` feature, which is on by
default.

- `NativeClock::now()` gives the current instant. Each read is one read of the clock of the operating
  system. An instant is never before an instant that an earlier read gave, on any thread.
- `NativeClock::resolution()` gives the resolution that the operating system reports for the clock.
- `NativeClock::domain()` gives the domain. The two native domains have the identifier
  `clock::NATIVE`, which is `u32::MAX`, and they differ by their `Suspend` value.

```rust
use rsasync::clock::NativeClock;
use rsasync::time::Suspend;

let clock = NativeClock::new(Suspend::Pauses)?;
let start = clock.now();
let end = clock.now();
assert!(end.compare(start)?.is_ge());
assert!(!clock.resolution().is_zero());
```

An instant is the nanoseconds since the origin of the clock, for example the start of the system. A
count above `u64::MAX` nanoseconds, about 584 years, stays at the last instant of the domain.

## Native clock of each target

| Target | `Suspend::Advances` | `Suspend::Pauses` | Resolution |
| --- | --- | --- | --- |
| Linux, x86_64 and aarch64, glibc and musl | `CLOCK_BOOTTIME` | `CLOCK_MONOTONIC` | `clock_getres` of the clock |
| macOS | `CLOCK_MONOTONIC_RAW` | `CLOCK_UPTIME_RAW` | `clock_getres` of the clock |
| Windows 10 and later, x86_64 and aarch64 | `QueryInterruptTimePrecise` | `QueryUnbiasedInterruptTimePrecise` | 100 ns, the unit of the count |
| Each other target | None | None | `NativeClock::new` gives `Unsupported` |

- Linux: `CLOCK_BOOTTIME` and `CLOCK_MONOTONIC` are the clocks of the kernel timers. An NTP
  correction can change their rate a little, but they never step.
- macOS: `CLOCK_UPTIME_RAW` is the clock of `mach_absolute_time`, and `CLOCK_MONOTONIC_RAW` adds the
  time of sleep. No frequency or time adjustment changes them.
- Windows: the interrupt time counts sleep and hibernation, and the unbiased interrupt time does
  not.

## Inject the time in a test

`InjectedClock::new(start)` gives a clock at `start`, on the domain of `start`. Its time moves only
when `InjectedClock::advance(by)` moves it. Each clone shares the same time, so a test can give
clones to other threads. An advance past the last instant of the domain gives `TimeError::Overflow`,
and the clock does not move. The injected clock works without `std`, also on WebAssembly.

```rust
use core::time::Duration;

use rsasync::clock::InjectedClock;
use rsasync::time::{ClockDomain, Instant, Suspend};

let clock = InjectedClock::new(Instant::new(ClockDomain::new(1, Suspend::Pauses), 0));
let start = clock.now();
assert_eq!(clock.now(), start);
let later = clock.advance(Duration::from_millis(5))?;
assert_eq!(later.duration_since(start)?, Duration::from_millis(5));
```

Give an injected clock an identifier other than `clock::NATIVE`. A timer storage takes the instants
of any clock: see [Timers](timers.md).

## Evidence

The test `clock::tests::each_native_clock_is_monotonic_across_threads_and_reports_its_resolution`
reads each native clock on four threads, 250,000 times on each thread. Each read must be at or after
the latest read that another thread published before it.

| Target | Host | Result |
| --- | --- | --- |
| Linux, x86_64, glibc | EC2 c8i.4xlarge, Ubuntu 26.04, Linux 7.0.0-1016-aws | Both clocks pass. `clock_getres` reports 1 ns for each clock |
| Linux, aarch64, glibc | EC2 c8g.4xlarge, Ubuntu 26.04, Linux 7.0.0-1016-aws | Both clocks pass. `clock_getres` reports 1 ns for each clock |
| macOS, aarch64 | This Mac, Apple M1 Pro, Darwin 27.2.0 | Both clocks pass. `clock_getres` reports 42 ns for each clock |
| Windows | No run yet. The clocks build for the two Windows targets | |
