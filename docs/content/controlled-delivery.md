# Test your code under controlled completions

A driver under controlled delivery gives each completion of its backend one turn later, and your
test can hold, fail, or repeat a completion. So each run of a test gets an order that a kernel
gives only by chance. Examples are a drop or a cancel before a completion, a late completion, and a
cancel that finds its operation running. The operation table, the harvest, and the rules of each resource run
as with native delivery. The control chooses only when each completion reaches the driver, and
with what result. It is a part of the [driver-only surface](driver-only.md), on each backend.
rsasync is in development, and this seam is not in a published release yet.

## Start a controlled driver

Give `Delivery::Controlled` to `Config::delivery`. `Driver::control()` then gives the `Control` of
the driver, and under native delivery, the default, it gives `None`.

<!-- The same text as the doctest of `Config::delivery`. Edit both. -->
```rust
use rsasync::driver::{Config, Delivery, Driver};

let mut driver = Driver::new(Config::new().delivery(Delivery::Controlled))?;
assert!(driver.control().is_some());
```

Under controlled delivery:

- Each completion is due at the turn after the turn that took it from the backend. The requests
  of that next turn come first: a drop, a cancel, or a close, also from another thread.
- `Turn::completed()` counts the completions that the turn gave to their operations.
- `Turn::more()` is true, and `Driver::wait` returns at once, while a completion is due.
- On IOCP, no socket skips the packet of a call that ends at once ([skip-on-success](iocp.md#skip-on-success)),
  so each completion comes through the port.
- The control keeps at most the completion room of the driver: two times the larger of
  `Config::operations` and `Config::submissions`, and at most 65536. A completion beyond that room
  stays in the backend for a later turn.
- `Driver::teardown()` ends each hold first, so it gets each terminal event.

## Hold a completion, then drop its future

This example shows that a receive whose future drops after its bytes came leaves the bytes to the
next receive. With native delivery, the kernel can complete the receive before or after the drop.
The hold makes the drop come first on each run.

<!-- The same text as the doctest of `Control`. Edit both. -->
```rust
use std::io::Write as _;
use std::os::fd::OwnedFd;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use rsasync::buffer::{Backing, Buffer as _};
use rsasync::driver::{Config, Delivery, Driver};

let mut driver = Driver::new(Config::new().delivery(Delivery::Controlled))?;
let (read, mut write) = std::io::pipe()?;
let mut reader = driver.adopt_reader(OwnedFd::from(read))?;
let receive = driver.recv(&mut reader, Backing::from(Vec::with_capacity(64)))?;
let id = receive.id();
driver.control().ok_or("native delivery")?.hold(id)?;
write.write_all(b"hello")?;

// A turn takes the completion into the control, so look after each turn and before each wait.
loop {
    let more = driver.turn()?.more();
    if driver.control().ok_or("native delivery")?.posted(id)?.next().is_some() {
        break;
    }
    if !more {
        driver.wait()?;
    }
}
// The receive completed in the kernel, and its completion waits in the control. Its drop comes
// first, so the driver applies the rule of an abandoned receive: the stream keeps the bytes.
drop(receive);
driver.control().ok_or("native delivery")?.deliver(id)?;

let mut next = pin!(driver.recv(&mut reader, Backing::from(Vec::with_capacity(64)))?);
let (count, buffer) = loop {
    let more = driver.turn()?.more();
    if let Poll::Ready(output) = next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        break output?;
    }
    if !more {
        driver.wait()?;
    }
};
assert_eq!((count?, buffer.bytes()), (5, &b"hello"[..]));
// The next receive gave the kept bytes with one counted copy.
assert_eq!(driver.copied(), 5);
driver.teardown()?;
```

The example uses a pipe, so it runs on Linux and macOS. On Windows, hold the receive of a TCP
stream in the same way.

## Control calls

Each call names an operation by its `OperationId`, which `Operation::id()` gives. An identity names
the operation and its driver, so the control of another driver refuses it.

| Call | What it does |
| --- | --- |
| `Control::hold(id)` | Keeps each completion of `id` in the control, also one that it took already, until `deliver`. The operation runs on, and its future stays pending |
| `Control::deliver(id)` | Ends the hold of `id`. The next turn gives its kept completions, in the order that the backend posted them |
| `Control::posted(id)` | The results of the completions of `id` that the control keeps: a count, or the `io::Error` of the system |
| `Control::fail(id, &error)` | The next completion of `id` that the driver gives has `error` in place of its result. The error needs a code of the system (`io::Error::raw_os_error`) |
| `Control::cancel_finds_running(id)` | The result of the next cancel of `id` says that the operation runs (`EALREADY`, or `WSAEALREADY` on Windows). The operation ends only at its own completion. Hold the operation to make its completion come after the cancel |
| `Control::repeat(id)` | Gives the last completion of `id` again at the next turn: a late completion. After a later operation took the slot of `id`, the driver finds the identity stale, and the completion changes nothing |
| `Control::fail_next_registration()` | The registration of the next socket or pipe that the driver takes fails, as a refusal of epoll, kqueue, or the port would. Its adoption refuses with `Reason::Register` and gives the descriptor back, and an accept gives `OutOfMemory` and keeps its connection for the next accept |

A failed call changes nothing and gives a `ControlError`:

| Error | When | Recovery |
| --- | --- | --- |
| `ControlError::OtherDriver` | The identity is of an operation of another driver | Use the control of the driver of the operation |
| `ControlError::NoCompletion` | `repeat` found no completion of the operation: none reached its future yet, or a later operation of its slot got a completion since | Repeat after the completion, and before the next operation of the slot ends |
| `ControlError::Full` | `repeat` found the control at its completion room | Turn the driver, or deliver a held operation, then repeat |
| `ControlError::NoCode` | `fail` got an error with no code of the system | Make the error with `io::Error::from_raw_os_error` |
| `ControlError::NoRegistration` | `fail_next_registration` on io_uring, which registers no resource | Run the test on epoll (`Config::backend(Backend::Epoll)`), kqueue, or IOCP |

## Limits

- The workers of `rsasync::runtime` run native delivery. Controlled delivery is a setting of a
  driver that you own.
- Controlled delivery changes when each completion arrives and its result, not what the kernel
  does. Tests with native delivery show the behavior of each kernel and backend.
- A hold that you never end keeps its operation running until `teardown`. While the held
  completions fill the completion room, the driver takes no other completion from the backend.
