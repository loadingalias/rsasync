# Physical time

`rsasync::clock::PhysicalClock` reads the civil time of the system as a `Reading`: an estimate, an
uncertainty bound or "unknown", and the source of the bound. Use it where a decision needs the time
of the world, for example the expiry of a certificate or a token, and needs to know how wrong the
time can be. rsasync is in development, and its alpha releases are on crates.io.

A reading gives no distributed order, database visibility, or authorization. A deadline uses a
monotonic clock of [Clocks](clocks.md), not a reading.

## Read the physical time

`PhysicalClock::new()` gives the physical-time source of the system, or `Unsupported` when the
target has none. It needs the `std` feature, which is on by default. `PhysicalClock::read()` gives a
`Reading`:

- `Reading::estimate()` is the time since the Unix epoch, 1970-01-01 00:00:00 UTC, with no leap
  seconds.
- `Reading::bound()` is the uncertainty bound, or `None` when the bound is unknown. The true time is
  between the estimate minus the bound and the estimate plus the bound.
- `Reading::source()` is `ReadingSource::SystemClock` for the bound of the system, and
  `ReadingSource::Caller` for a bound that you supply.

A check that needs a bound uses `Reading::earliest()` or `Reading::latest()`. Each one refuses an
unknown bound with `TimeError::UnknownBound`, so the check cannot pass on a guess.

```rust
use core::time::Duration;

use rsasync::clock::PhysicalClock;
use rsasync::time::TimeError;

let expiry = Duration::from_secs(1_900_000_000);
let reading = PhysicalClock::new()?.read();
match reading.latest() {
    Ok(latest) if latest < expiry => { /* valid at each time that the reading permits */ }
    Ok(_) => { /* expired, or too close to the expiry for the bound */ }
    Err(TimeError::UnknownBound) => { /* refuse, or get a bound from another source */ }
    Err(_) => { /* the bound is too large for a `Duration` */ }
}
```

## Bound of each target

| Target | Estimate | Bound of the system |
| --- | --- | --- |
| Linux, x86_64 and aarch64, glibc and musl | `adjtimex` | The maximum error of the kernel while the clock is synchronized. Else unknown |
| macOS | `std::time::SystemTime` | Unknown |
| Windows, x86_64 and aarch64 | `std::time::SystemTime` | Unknown |
| Each other target | None: `PhysicalClock::new` gives `Unsupported` | |

- Linux: one `adjtimex` call gives the time and the maximum error together, and each user can make
  it. A time daemon such as chrony sets the maximum error. When the kernel reports the clock as not
  synchronized, the bound is unknown: the kernel then keeps a maximum error of 16 s, which is not a
  bound. When the kernel gives the time in microseconds, the bound adds 1 µs for the truncation.
  When the call fails, for example in a sandbox that refuses it, the reading has the time of
  `std::time::SystemTime` and an unknown bound.
- The bound covers the error that the time daemon knows. A time service that smears a leap second
  differs from UTC by up to 0.5 s near the leap second, and that difference is not in the bound.
- An estimate before the Unix epoch shows a wrong clock. The reading then has an estimate of 0 and an
  unknown bound, also with a bound that you supply.

## Supply a bound

When you know the error of the clock from another source, for example your own time service, give it
with `PhysicalClock::with_bound(bound)`. Each reading of that clock has your bound and
`ReadingSource::Caller`, on each target. Your bound replaces the bound of the system.

```rust
use core::time::Duration;

use rsasync::clock::PhysicalClock;
use rsasync::time::ReadingSource;

let reading = PhysicalClock::new()?.with_bound(Duration::from_millis(20)).read();
assert_eq!(reading.bound(), Some(Duration::from_millis(20)));
assert_eq!(reading.source(), ReadingSource::Caller);
```

## Steps of the civil clock

A correction or a step of the civil clock changes the next reading. It never changes an instant of a
`NativeClock`, so it never moves a deadline or a timer. A set of the civil clock on Linux also
clears the synchronized state of the kernel, so the readings have an unknown bound until the time
daemon synchronizes the clock again.

## Evidence

On EC2 c8i.4xlarge, Ubuntu 26.04, Linux 7.0.0-1016-aws, x86_64, with chrony synchronized to the
Amazon Time Sync Service:

- A reading had a bound of 5.712 ms and `ReadingSource::SystemClock`.
- The test `a_step_of_the_civil_clock_moves_no_monotonic_timer`, which needs `CAP_SYS_TIME`, stepped
  the civil clock forward by 60 s. The next reading had an unknown bound. A timer on the
  `Suspend::Pauses` native clock fired at its deadline, 1 s after its insert, and not before. The
  test then stepped the civil clock back.

On EC2 c8g.4xlarge, Ubuntu 26.04, Linux 7.0.0-1016-aws, aarch64, with chrony synchronized to the
same service, a reading had a bound of 1.111 ms and `ReadingSource::SystemClock`.

On macOS, a reading has an unknown bound. No Windows run has occurred yet.
