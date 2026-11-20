# Shared types

rsasync shares three groups of types with networking libraries and with applications:

| Module | Contents |
| --- | --- |
| `rsasync::buffer` | Owned buffers, and the leases and pool buffers that share their backing. See [Buffer ownership](buffer-ownership.md) |
| `rsasync::capacity` | Capacity permits, and the caller-owned budget that contains them. See [Capacity](capacity.md) |
| `rsasync::time` | Clock domains, instants and deadlines, and physical-time readings |

rsasync is in development, and its alpha releases are on crates.io.

## Use the types through `rsasync`

```rust
use rsasync::{buffer, capacity, time};
```

The modules come from the `rstypes` crate, and `rsasync` re-exports them. Do not add `rstypes` to
your dependencies, and do not name it in your code. A library that uses these types in its public
API re-exports them, so you use them through that library or through `rsasync`. Each path gives
the same type, so a value goes from a library to the runtime with no conversion.

## Time

`rsasync::time` holds time values. It has no clock: a clock of [Clocks](clocks.md) gives the
instants of its domain. You can also make each value directly, for example in a test. To wait for a
deadline, use a timer: see [Timers](timers.md).

- A `ClockDomain` identifies one monotonic clock. It has a `u32` identifier and a `Suspend` value:
  `Advances` when the clock counts the time while the system is suspended, and `Pauses` when it
  stops. Two domains are equal only when their identifiers and their `Suspend` values are equal.
  Each clock source gives its domain an identifier that no other domain uses.
- An `Instant` is a `u64` count of nanoseconds since the origin of its domain.
- A `Deadline` is an absolute instant. It keeps its domain and its instant, so a late poll does
  not move it. Make it from a duration budget one time, at the start of the work. A deadline has
  no operation that gives a new duration budget.
- A `Reading` is physical time: an estimate, an uncertainty bound or "unknown", and its source.
  The estimate is a `Duration` since the Unix epoch, 1970-01-01 00:00:00 UTC, with no leap
  seconds. A bound of `None` is unknown. The source is `ReadingSource::SystemClock` or
  `ReadingSource::Caller`, when the caller supplied the bound. [Physical time](physical-time.md)
  gives the readings of the system.

Instants have no `<` operator, because instants of two domains have no order. Use `compare`. A
reading is separate from the clock domains, so a correction of the civil clock never moves a
deadline. A reading gives no distributed order, database visibility, or authorization.

```rust
use core::time::Duration;
use rsasync::time::{ClockDomain, Deadline, Instant, Reading, ReadingSource, Suspend, TimeError};

fn main() -> Result<(), TimeError> {
    let domain = ClockDomain::new(1, Suspend::Pauses);
    let start = Instant::new(domain, 1_000_000);
    let deadline = Deadline::at(start.checked_add(Duration::from_millis(5))?);

    // A late poll reads a later instant. The deadline does not move.
    assert!(!deadline.is_reached(Instant::new(domain, 5_999_999))?);
    assert!(deadline.is_reached(Instant::new(domain, 90_000_000))?);

    // An instant of a different domain does not compare.
    let other = Instant::new(ClockDomain::new(2, Suspend::Advances), 1_000_000);
    assert_eq!(start.compare(other), Err(TimeError::DomainMismatch));

    // A check that needs a bound refuses an unknown bound.
    let estimate = Duration::from_secs(1_800_000_000);
    let reading = Reading::new(estimate, Some(Duration::from_millis(20)), ReadingSource::Caller);
    assert!(reading.latest()? < Duration::from_secs(1_800_000_060));
    let unknown = Reading::new(estimate, None, ReadingSource::SystemClock);
    assert_eq!(unknown.latest(), Err(TimeError::UnknownBound));
    Ok(())
}
```

| Operation | Result | Refusal |
| --- | --- | --- |
| `Instant::compare(other)` | The `Ordering` of the two instants | `DomainMismatch` |
| `Instant::checked_add(duration)` and `checked_sub(duration)` | The instant on the same domain | `Overflow` before the origin or after `u64::MAX` nanoseconds |
| `Instant::duration_since(earlier)` | The `Duration` from `earlier` | `DomainMismatch`, and `Overflow` when `earlier` is later |
| `Deadline::is_reached(now)` | `true` when `now` is at or after the deadline | `DomainMismatch` |
| `Reading::earliest()` and `latest()` | The estimate minus or plus the bound | `UnknownBound`, and `Overflow` before the Unix epoch or past `Duration::MAX` |

Each refusal is a `TimeError`:

- `DomainMismatch`: the values come from two clocks. Use instants of one domain.
- `Overflow`: the result is out of range for its type.
- `UnknownBound`: the reading has no bound. Refuse the check, or get a reading with a bound, for
  example a bound that the caller supplies.

## Why the types are in a separate crate

- A library, for example a protocol implementation, can use these types on a different runtime.
  It depends on `rstypes`, not on rsasync.
- `rstypes` has its own version. It changes only when a shared type changes, so different releases
  of rsasync and of a library can use the same types.

## How a shared type changes

The shared types of `rstypes` change only through the path below. Three families use them:
rsasync (the runtime), rsnetwork (networking), and rsauthnz (auth). One set of types for all three
lets a value go between the families with no conversion.

1. Make the change as a reviewed change. Name each family that uses the changed type: rsasync and
   rsnetwork for `buffer` and `capacity`, and also rsauthnz for `time`.
2. Test the uses of the changed type in each named family again, on the changed revision.
3. Merge the change only when each of these uses passes.

A release that contains the change has a new version of `rstypes`. This path covers only
`rstypes`. The other APIs of rsasync can change in each alpha.

## Build without `std`

`rstypes` has a `std` feature, which is on by default. Without it, the crate needs only `core` and
`alloc`. `rsasync` has a `std` feature too, which is also on by default. It turns on the feature of
`rstypes`, and without it `rsasync` also needs only `core` and `alloc`.

`cargo xtask check` and `cargo xtask fix`, with no arguments, lint `rstypes` and `rsasync` without
`std`: the library for `wasm32v1-none`, and all targets, the tests too, for the host. That target
has no `std`, so each use of `std` outside the `std` feature fails the check; the host lint compiles
the tests without the feature, so a test that uses `std` with no gate fails it.
`rust-toolchain.toml` names the target. To run only this build, from the repository root:

```sh
rustup target add wasm32v1-none   # when rustup did not install it with the toolchain
cargo build --locked -p rstypes --no-default-features --target wasm32v1-none
```

The build of `rstypes` without `std` also compiles for `wasm32-unknown-unknown`, and its tests run on
`wasm32-wasip1` in Wasmtime. [WebAssembly](platform-support.md#webassembly) gives the commands and
the limits.
