# Drive the I/O driver yourself

An rsasync I/O driver can run with no scheduler: your code owns it and drives it. This page tells
how to start a driver, drive it from your own loop or from the reactor of another host, use TCP on
it, and end it. The type is `rsasync::driver::Driver`. rsasync is in development and has no
release. The driver has TCP accept, receive, and send; the other operations come in later changes.

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
| `Driver::wait()` | Blocks in the kernel until a completion is ready, or a request or a wake from another thread comes. The kernel runs its deferred work during the wait |
| The wake descriptor (`AsFd` of the driver) | Becomes readable when work comes: a completion, deferred work of the kernel, or a request or a wake from another thread. Some kernels signal deferred work only one time; see [Drive it from another reactor](#drive-it-from-another-reactor) |
| `Driver::wake_handle()` | A `WakeHandle` that any thread can use to make the wake descriptor readable and to end a wait |
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
example epoll or the reactor of another runtime. When it reports the descriptor readable,
turn the driver, and turn it again while `Turn::more()` gives `true`. The turn clears the
descriptor, so the reactor reports it again only for new work.

Some kernels signal the wake descriptor for deferred work of the kernel only one time in the life
of the driver. In Linux, the defect is in a kernel that has the io_uring change `cd305ee3633a`
("defer eventfd signaling when queued from a wakeup handler") and not `04fe9aeb4f3c` ("reset
deferred signal state"). The driver tests saw it on Ubuntu 26.04 with `7.0.0-1016-aws`. On such a
kernel, a reactor that waits only for the descriptor can miss an operation that waited for
readiness, for example a receive that waited for its data, and the operation does not complete.
`Driver::wait` does not depend on that signal, so use your own loop with `Driver::wait` on such a
kernel.

## Accept, receive, and send on TCP

The driver owns each socket that it makes. `Driver::bind` binds a nonblocking socket and listens on
it. `Driver::accept` gives the future of a connection: a `TcpStream` and the address of its peer.
Each operation gives an `Operation` future. You poll it, and you turn the driver until it is ready.
This helper does both:

```rust
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use rsasync::driver::Driver;

/// Polls `future` while the driver turns, until it gives its output.
fn run<F: Future>(driver: &mut Driver, future: F) -> std::io::Result<F::Output> {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return Ok(output);
        }
        if !driver.turn()?.more() {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return Ok(output);
            }
            driver.wait()?;
        }
    }
}
```

A receive takes an owned buffer by value, and the kernel writes into its capacity after its
initialized bytes. The future gives the buffer back with the received bytes initialized, also
after a failure. A send takes a lease, and the kernel reads its bytes. The future gives the lease
back. Neither operation copies the bytes:

```rust
use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::driver::{Config, Driver};

let mut driver = Driver::new(Config::new())?;
let listener = driver.bind("127.0.0.1:8080".parse()?)?;
let accept = driver.accept(&listener)?;
let (stream, peer) = run(&mut driver, accept)???;

let receive = driver.recv(&stream, Backing::from(Vec::with_capacity(4096)))?;
let (received, request) = run(&mut driver, receive)??;
let count = received?;
assert_eq!(request.bytes().len(), count);

let send = driver.send(&stream, Lease::from(b"HTTP/1.1 200 OK\r\n\r\n".to_vec()))?;
let (sent, _response) = run(&mut driver, send)??;
sent?;
driver.close(stream)?;
```

The outer `Result` of each future is `Err(Ended)` only if the driver ended before the operation
gave its result. A refused operation, for example when each operation slot is in use, gives its
`Reason` and the buffer or lease back.

### Receive with no buffer while you wait

`Driver::provide` gives the slots of an `rsasync::buffer::Pool` to the kernel. Then
`Driver::recv_selected` waits with no buffer, and the kernel takes a free slot only when the data
comes. The future gives a `Lease` of the received bytes in that slot. When the last lease or view of
the slot drops, on any thread, the slot goes back to the pool, and the next turn gives it to the
kernel again:

```rust
use core::alloc::Layout;
use core::num::{NonZeroU32, NonZeroU64};

use rsasync::buffer::Pool;
use rsasync::capacity::Budget;

// The pool charges its memory to a budget: 64 slots of 4096 bytes.
let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
let slot = Layout::from_size_align(4096, 4096)?;
driver.provide(Pool::new(&budget, NonZeroU32::new(64).unwrap(), slot)?)?;
let receive = driver.recv_selected(&stream)?;
let request = run(&mut driver, receive)???;
```

If no slot is free when the kernel tries the receive, the receive ends with the error `ENOBUFS`
(`io::Error::raw_os_error`). The data stays in the socket, so receive again after a slot comes
back.

`Driver::close` closes a stream, and `Driver::close_listener` closes a listener. Each one refuses
with `Reason::Busy` while an operation runs on the socket. An operation runs until the kernel
ends it, also after you drop its future, so turn the driver until then.

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
