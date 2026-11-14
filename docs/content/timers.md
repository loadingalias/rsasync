# Timers

`rsasync::timer` holds bounded timers. A `Timers` storage holds a fixed number of timers on one
clock domain. A `Timer` is a future that is ready at its deadline. The storage works with no
scheduler and no driver, also without `std`. rsasync is in development, and its alpha releases are
on crates.io.

The storage reads no clock. Its owner, the home, gives it the instants of its clock domain, for
example from a native clock, and a test can give it the instants of an injected clock (see
[Clocks](clocks.md)).

## Drive a timer storage

1. Make the storage with `Timers::new(capacity, resolution, now, &budget)`. `now` is an instant of
   the clock domain of the storage. The storage charges its memory to the budget (see
   [Capacity](capacity.md)).
2. Insert a timer with `Timers::insert(deadline)`. Give the `Timer` to the task that waits.
3. Sleep until `Timers::next_turn()`, read the clock, and call `Timers::turn(now, limit)`. The turn
   fires at most `limit` timers whose ticks `now` reached, and wakes the task of each one. When it
   gives `true`, more timers are due: turn again before you sleep. When `next_turn` gives `None`,
   no timer waits.
4. Close the storage with `Timers::close()`, or drop it, when the home stops.

```rust
use core::{future::Future, num::NonZeroU32, num::NonZeroU64, pin::pin, task::Context};
use core::{task::Poll, task::Waker, time::Duration};

use rsasync::capacity::Budget;
use rsasync::time::{ClockDomain, Deadline, Instant, Suspend};
use rsasync::timer::Timers;

let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
let domain = ClockDomain::new(1, Suspend::Pauses);
let at = |nanos| Instant::new(domain, nanos);
let tick = Duration::from_millis(1);
let mut timers = Timers::new(NonZeroU32::new(64).unwrap(), tick, at(0), &budget)?;

// A deadline of 2.5 ms goes up to the start of the third tick, at 3 ms.
let mut timer = pin!(timers.insert(Deadline::at(at(2_500_000)))?);
assert_eq!(timers.next_turn(), Some(at(3_000_000)));

let mut cx = Context::from_waker(Waker::noop());
// Each turn fires at most 32 timers. `false`: no other timer is due.
assert!(!timers.turn(at(2_999_999), 32)?);
assert_eq!(timer.as_mut().poll(&mut cx), Poll::Pending);
assert!(!timers.turn(at(3_000_000), 32)?);
assert_eq!(timer.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
```

A turn never blocks. Its work is a fixed number of steps for each timer that it fires, frees, or
moves to a finer level of the storage. A tick with no timer costs nothing, so the cost of a turn
depends on its timers, not on the time since the last turn.

## Fire many due timers

The limit of a turn bounds its fires, also when many timers are due at one time. When more timers
are due than the limit, they fire in deadline order at the timer resolution in the turns that
follow, and none fires early. Thus a timer of an earlier tick fires first, and timers of one tick
fire in no stated order. A timer that you insert with a deadline before the current tick of the
storage has that current tick. While due
timers remain, `next_turn` gives an instant at or before the last turn.

## Resolution, coarsening, and range

The storage counts time in ticks of its resolution, which you give to `Timers::new`.

- A timer never fires before its deadline. The storage moves each deadline up to the start of the
  next tick, never down.
- A timer fires at the first turn at or after the start of its tick. When the home turns at
  `next_turn`, and no more timers are due than the limit of the turn, each timer fires at most
  `Timers::max_coarsening()` after its deadline. The maximum coarsening is the resolution less
  one nanosecond.
- A deadline at or before the instant of the last turn is due at the next turn.
- A deadline can be at most `2^36 - 1` ticks after the current tick of the storage. The current
  tick is the tick of the last turn, or an earlier tick when that turn stopped at its limit.
  `Timers::latest()` gives the latest deadline that `insert` takes now. A later deadline refuses
  with `Refused::Range`. The storage never wraps a deadline into an earlier tick. The last tick of
  the domain also ends the range.

| Resolution | Range |
| --- | --- |
| 1 ns | about 68.7 s |
| 1 µs | about 19.1 hours |
| 100 µs | about 79.5 days |
| 1 ms | about 795 days |

A finer resolution gives a smaller coarsening and a shorter range. The resolution does not change
the memory of the storage.

## Outcomes

Each refusal changes nothing.

| Operation | Result | Meaning |
| --- | --- | --- |
| `Timers::new` | `Invalid::Resolution` | The resolution is zero, or longer than `u64::MAX` nanoseconds |
| `Timers::new` | `Invalid::Capacity` | The memory of the capacity is larger than the address space of the target |
| `Timers::new` | `Invalid::Exhausted` | The budget has less free capacity than `Timers::memory(capacity)` |
| `Timers::insert` | `Refused::Full` | Each place of the storage holds a timer |
| `Timers::insert` | `Refused::Range` | The deadline is after `Timers::latest()` |
| `Timers::insert` | `Refused::Closed` | The storage is closed |
| `Timers::insert` | `Refused::Domain` | The deadline is on a different clock domain than the storage |
| `Timers::turn` | `Ok(true)` | More timers are due than the limit. Turn again before you sleep |
| `Timers::turn` | `TimeError::DomainMismatch` | The instant is on a different clock domain than the storage |
| `Timer` (await) | `Ok(())` | The timer expired |
| `Timer` (await) | `Err(Closed)` | The storage closed or dropped before the timer expired |
| `Timer::cancel` | `Outcome::Cancelled` | The timer did not expire, and it never expires |
| `Timer::cancel` | `Outcome::Expired` | The timer expired before the cancel |
| `Timer::cancel` | `Outcome::Closed` | The storage closed before the cancel |

A drop of a `Timer` also cancels it. `cancel` is for a caller that must know what came first.

## Capacity and memory

- The capacity is a `NonZeroU32`. Each `Timer` holds one place until it drops, also after it
  expires or after the storage closes.
- A dropped timer frees its place at the next turn or insert of the home. An insert into a full
  storage first frees the places of the dropped timers, so `Refused::Full` tells that `capacity`
  timers were live when the insert ran.
- The storage allocates one time, in `Timers::new`. It reserves `Timers::memory(capacity)` bytes
  from the budget before it allocates. On a 64-bit target such as aarch64 macOS, this is 56 bytes
  and 64 bytes for each timer.
- The charge stays until the storage and each of its timers have dropped, because the timers
  share that memory.

## Move a timer to another thread

A `Timer` is `Send`. It can move to another thread, and a task can await it there. The
storage stays at its home, and only the home inserts, turns, and closes. A poll on another thread
registers the waker of that poll, and the turn of the home wakes it. A drop on another thread
tells the home, and the home frees the place at its next turn or insert. The storage takes no lock,
and it allocates nothing after `Timers::new`.

## Check the timers

The example on this page is a doctest of `rsasync::timer`. From the repository root,
`cargo xtask test` runs it with the other tests. `cargo xtask explore -p rsasync` checks a fire
that races a poll on another thread, and a drop on another thread that races the reuse of its
place, in each thread order within the preemption bound (see
[Explore concurrent code](concurrency-exploration.md)).

The tests of the timers also pass without `std` on `wasm32-wasip1`, in Wasmtime 50.0.1 on an
aarch64 macOS host:

```sh
cargo xtask test -p rsasync --target wasm32-wasip1 --no-default-features -E 'test(/^timer::/)'
```
