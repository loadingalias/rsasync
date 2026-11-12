# Drive the I/O driver yourself

An rsasync I/O driver can run with no scheduler: your code owns it and drives it. This page tells
how to start a driver, drive it from your own loop or from the reactor of another host, and end it.
The type is `rsasync::driver::Driver`. rsasync is in development and has no release. The driver
has its full drive and teardown contract, but no public I/O operation yet: the operations, such
as TCP, come in later changes and use this contract.

The driver needs Linux on x86_64 or aarch64, the `std` feature, and io_uring. It needs no helper
thread: `Driver::helper_threads` gives 0.

## The contract

The driver is the home of each operation that it accepts, from the submission to the terminal
event of the operation. Only the terminal event releases the buffer and the charge of the
operation. You keep the driver driven until that event, also after you drop or forget the future of
the operation. The future can move to another thread; the driver stays on the thread that created
it.

| Call | What it does |
| --- | --- |
| `Driver::new(Config)` | Starts the driver and allocates its bounded memory one time. It gives `Error::Refused` if the kernel or its policy refuses io_uring, for example in a container with the default seccomp profile |
| `Driver::turn()` | One bounded turn that never blocks. It takes the requests of other threads, submits, enters the kernel only when there is work for it, and gives up to `Config::completions_per_turn` completions to their futures |
| `Turn::more()` | Work remains. Turn again before you wait |
| The wake descriptor (`AsFd` of the driver) | Becomes readable when work comes: a completion, deferred work of the kernel, or a request or a wake from another thread |
| `Driver::wait()` | Blocks until the wake descriptor is readable |
| `Driver::wake_handle()` | A `WakeHandle` that any thread can use to make the wake descriptor readable |
| `Driver::teardown()` | Cancels each running operation, drives until each terminal event, closes each resource, and reports the cancelled operations |

## Drive it from your own loop

Turn while work remains, and wait when no work remains:

```rust
use rsasync::driver::{Config, Driver};

let mut driver = Driver::new(Config::new())?;
// Submit operations here, and keep their futures.
loop {
    let turn = driver.turn()?;
    if !turn.more() {
        driver.wait()?;
    }
    // Stop when your work is done.
}
```

A turn stops at its bound. If more completions are ready, `Turn::more()` gives `true`, and the next
turn gives them. So a turn always returns soon, and your loop can do other work between turns.

## Drive it from another reactor

Register the wake descriptor (`driver.as_fd()`) for readability with the reactor of your host, for
example epoll, kqueue, or the reactor of another runtime. When it reports the descriptor readable,
turn the driver, and turn it again while `Turn::more()` gives `true`. The turn clears the
descriptor, so the reactor reports it again only for new work.

## Limits that you set

`Config` sets each limit before the driver starts:

| Setting | Default | Meaning |
| --- | --- | --- |
| `submissions` | 64 | Entries of the submission queue. The kernel rounds up to a power of two |
| `operations` | 256 | Operations that can run at one time. A submission beyond them is refused with the reason, and its resources come back |
| `resources` | 256 | Descriptors that the driver can own at one time |
| `completions_per_turn` | 64 | The most completions that one turn gives to their futures |

A limit of 0 gives `Error::Config`.

## End the driver

Call `Driver::teardown()`. It cancels each operation that still runs, by its own identity, and it
waits until the kernel ends each one, so it releases no buffer that the kernel can still use. Then
it closes each resource. `Teardown::cancelled()` gives the number of cancelled operations. A future
that did not take its result before teardown then ends with no result.

If you drop the driver without teardown, its futures end with no result, and the driver keeps the
buffers of the operations that still run: it leaks them, because the kernel can still use them.
