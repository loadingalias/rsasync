# Capacity

`rsasync::capacity` counts the capacity that the runtime, the libraries, and the application
reserve, for example bytes of memory. The application makes one budget and gives it to each of
them, so all of their charges come from one total. rsasync is in development and has no release.
The waiting reservation, which waits until capacity is free, is not available yet.

## One budget for the runtime, the libraries, and the application

A `Budget` has a fixed bound of units, which is not zero. Each clone of a budget shares the same
units, so give a clone to each library that charges memory. A library keeps the clone and reserves
from it. It does not make its own budget.

`Budget::reserve(amount)` gives a `Permit` of `amount` units, or a refusal:

- The refusal is an `Exhausted` value with the requested units and the units that were free. A
  refusal changes nothing, so the caller can wait, drop work, or try a smaller amount.
- The permit holds its units until it drops, or until `Permit::release` ends it. Then the units are
  free again, one time.
- A permit can move to a different owner or thread. It then releases there.

This example gives one budget to two libraries and to the application:

```rust
use core::num::NonZeroU64;

use rsasync::capacity::{Budget, Exhausted, Permit};

/// A library that charges the memory of each connection that it accepts.
struct Server {
    budget: Budget,
}

impl Server {
    fn accept(&self) -> Result<Permit, Exhausted> {
        self.budget.reserve(16 * 1024)
    }
}

// The application makes the one budget, and gives a clone to each library.
let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
let server = Server { budget: budget.clone() };
let cache = budget.clone(); // a second library

let connection = server.accept()?;
let entry = cache.reserve(40 * 1024)?;
assert_eq!(budget.available(), 8 * 1024);

// The next connection does not fit. The refusal tells what is free, and nothing changes.
let refused = server.accept().unwrap_err();
assert_eq!((refused.requested, refused.available), (16 * 1024, 8 * 1024));

// A release makes the units free again, one time.
drop(entry);
connection.release();
assert_eq!(budget.available(), 64 * 1024);
```

`Budget::available` is the number of free units at the time of the call. Other owners can change
it at any time, so use the result of `reserve`, not `available`, to know if units are yours.

## Split a permit

`Permit::split(amount)` moves `amount` units into a new permit. The two parts together hold the
units of the original permit. Each part releases only its own units, also after the other part
drops. Thus a part can outlive the permit that it came from: for example, a stream keeps its part
after its connection closes. `split` refuses with `Exhausted` if the permit holds fewer units.

## Charge small amounts from a credit

Each reservation and each release updates the budget that all of its owners share. A worker or a
session that makes many small charges uses a `Credit` from `Budget::credit(idle_bound)` instead:

- `Credit::charge(amount)` takes the units from the idle units of the credit, with no update of the
  budget. When too few are idle, the credit takes one batch from the budget: what the charge needs,
  and more, up to `idle_bound`, if the budget has them. If the idle units and the budget together
  have too few, the charge refuses with `Exhausted` and changes nothing.
- `Credit::refund(amount)` makes charged units idle again. The credit keeps at most `idle_bound`
  idle units and gives the others back to the budget. A refund of more units than are charged
  refuses with `NotCharged` and changes nothing.
- Idle units stay reserved in the budget. Thus a credit never makes the budget give more than its
  bound, and the units that it keeps idle are at most its idle bound.
- `Credit::split(amount)` charges `amount` units as a separate permit. Use it for a charge that can
  outlive the credit.
- When the credit drops, it releases its charged and its idle units. Thus each charge ends with the
  credit.

```rust
use core::num::NonZeroU64;

use rsasync::capacity::Budget;

let budget = Budget::new(NonZeroU64::new(1024 * 1024).unwrap());

// A session charges each frame to its credit, and keeps at most 64 KiB idle.
let mut session = budget.credit(64 * 1024);
for _ in 0..100 {
    session.charge(512)?; // only the first charge takes units from the budget
}
assert_eq!(session.charged(), 100 * 512);

// The refund keeps 64 KiB idle, and they stay reserved in the budget.
session.refund(100 * 512)?;
assert_eq!(session.idle(), 64 * 1024);
assert_eq!(budget.available(), 1024 * 1024 - 64 * 1024);

drop(session);
assert_eq!(budget.available(), 1024 * 1024);
```

## Zero and large totals

- The bound of a budget is a `NonZeroU64`, so a budget always has units.
- A reservation, a split, or a charge of zero units always succeeds and changes no total, also when
  no unit is free.
- A credit with an idle bound of zero keeps no idle unit. Each charge then takes exactly its units
  from the budget, and each refund gives them back.
- Units are `u64` on every target, also on 32-bit targets such as `wasm32`. Thus a total never
  depends on the pointer width.

## Check the examples and the concurrent code

The examples on this page are doctests of `rsasync::capacity`. From the repository root,
`cargo xtask test` runs them with the other tests. `cargo xtask explore -p rstypes` checks a
concurrent split, release, and credit batch in each thread order within its preemption bound (see
[Explore concurrent code](concurrency-exploration.md)).
