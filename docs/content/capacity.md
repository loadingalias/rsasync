# Capacity

`rsasync::capacity` counts the capacity that the runtime, the libraries, and the application
reserve, for example bytes of memory. The application makes one budget and gives it to each of
them, so all of their charges come from one total. `rsasync::acquire` adds the waiting
reservation, which waits until the units are free. rsasync is in development, and its alpha
releases are on crates.io. The waiting reservation is not in a published release yet.

## One budget for the runtime, the libraries, and the application

A `Budget` has a fixed bound of units, which is not zero. Each clone of a budget shares the same
units, so give a clone to each library that charges memory. A library keeps the clone and reserves
from it. It does not make its own budget.

`Budget::reserve(amount)` gives a `Permit` of `amount` units, or a refusal:

- The refusal is an `Exhausted` value with the requested units and the units that were free. A
  refusal changes nothing, so the caller can wait (see
  [Wait for free units](#wait-for-free-units)), drop work, or try a smaller amount.
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

A `buffer::Pool` charges its slots, their entries, and its shared state to a budget when it is
made (see
[Receive into pool slots](buffer-ownership.md#receive-into-pool-slots)).

`Budget::available` is the number of free units at the time of the call. Other owners can change
it at any time, so use the result of `reserve`, not `available`, to know if units are yours.

## Wait for free units

A budget from `rsasync::acquire::budget` lets a future wait until the units that it asks for are
free. The wait has no spin and no timed retry: the budget wakes the waiter when units return.

1. Call `acquire::budget(&memory, bound, places)`. It gives a `Budget` of `bound` units and an
   `Acquirer`. Give clones of the budget to the runtime, to each library, and to the application,
   as for a budget from `Budget::new`.
2. In the code that waits, call `acquirer.acquire(amount)`. It gives an `Acquire` future, or
   `AboveBound` when `amount` is above the bound, because such a wait could never end.
3. Await the future. It gives a `Permit` of `amount` units, which releases as each permit does.

Each acquirer has one waiter place, and a future borrows its acquirer mutably. Thus each waiter
place has at most one wait at a time. `Acquirer::try_clone` takes another free place, or refuses
with `TooManyWaiters` when each place is in use. A drop of an acquirer frees its place.

- Places: from 1 to `acquire::MAX_PLACES` (2^16). `acquire::budget` refuses more with
  `Refused::TooManyPlaces`.
- Memory: `acquire::budget` charges `acquire::memory(places)` bytes to `memory` before it allocates,
  or refuses with `Refused::Memory` and the `Exhausted` value. The charge ends when the budget and
  each acquirer have dropped.
- Order:
  1. The waiters get units first in, first out, in the order in which their waits started. Only
     the first waiter takes units. When units return, the budget wakes the first waiter. When the
     first waiter gets its units or stops its wait, it wakes the next one.
  2. A new wait goes behind each waiter in the order, also when its units are free. Thus smaller
     requests do not pass a large one. A new wait takes free units at once only when no waiter is
     in the order.
  3. A reservation that does not wait, such as `Budget::reserve` or `Credit::charge`, takes free
     units at once, also when waiters are in the order. It can take returned units before the poll
     of the woken first waiter, which then waits again at its place. Thus a stream of small
     reservations that do not wait can delay a large waiter for as long as the stream continues.
- Cancellation: a drop of the future stops the wait. The future holds no units before it is ready,
  so the drop releases nothing and changes no total. When it was the first waiter, the drop wakes
  the next one.
- A wait for zero units ends at once, also when waiters are in the order.
- A forgotten future keeps its place in the order. The next wait of its acquirer continues at that
  place, and a drop of the acquirer removes it.

This example has a task that waits for a unit and a second task that releases the units. It uses
the local tasks of [Tasks](tasks.md):

```rust
use core::num::{NonZeroU32, NonZeroU64};

use rsasync::acquire;
use rsasync::capacity::Budget;
use rsasync::task::{LocalOwner, Outcome, spawn_local};

// The memory of the waiter places and of the tasks comes from a memory budget.
let memory = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
// A budget of 4 connections, with 2 waiter places.
let places = NonZeroU32::new(2).unwrap();
let (connections, mut acquirer) = acquire::budget(&memory, NonZeroU64::new(4).unwrap(), places)?;
let mut owner = LocalOwner::new(&memory, 4)?;

let all = connections.reserve(4)?;
let amount = owner.block_on(async move {
    let waiter = spawn_local(async move {
        // When no unit is free, the task waits until a release wakes it.
        let permit = acquirer.acquire(1).expect("1 is not above the bound").await;
        permit.amount()
    })
    .expect("the owner has a free slot");
    // A second task releases the units.
    let _releaser = spawn_local(async move { drop(all) }).expect("the owner has a free slot");
    match waiter.join().await {
        Outcome::Completed(amount) => amount,
        Outcome::Cancelled | Outcome::Panicked(_) | Outcome::OwnerFailed(_) => 0,
    }
});
assert_eq!((amount, connections.available()), (1, 4));
```

The waits need no scheduler: a poll with any waker works, and the release wakes that waker. The
module documentation of `rsasync::acquire` has an example that polls the future by hand.

### Get a call when units return

`Budget::with_listener(bound, listener)` makes a budget that calls `listener.released()` after units
return to it. `acquire::budget` uses it. Use it directly only for a waiting mechanism of your own:

- The budget calls it on the thread that gives the units back, in the drop of a permit or a credit,
  or in a refund of a credit. The units are free when it runs.
- A refusal, a reservation, and a release of zero units do not call it.
- Keep it short, and do not wait in it, because it runs in the drop of a permit.
- A budget from `Budget::new` has no listener. Its release then costs one branch for the check.

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
- A wait for zero units ends at once.
- Units are `u64` on every target, also on 32-bit targets such as `wasm32`. Thus a total never
  depends on the pointer width.

## Check the examples and the concurrent code

The examples on this page are doctests of `rsasync::capacity` and `rsasync::acquire`. From the
repository root, `cargo xtask test` runs them with the other tests. `cargo xtask explore -p rstypes`
checks a concurrent split, release, and credit batch in each thread order within its preemption
bound. `cargo xtask explore -p rsasync` checks a release that races a new wait, a cancel of the
first waiter, and a change of the first waiter (see
[Explore concurrent code](concurrency-exploration.md)).
