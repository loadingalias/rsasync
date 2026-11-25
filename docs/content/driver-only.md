# Drive the I/O driver yourself

An rsasync I/O driver can run with no scheduler: your code owns it and drives it. This page tells
how to start a driver, drive it from your own loop or from the reactor of another host, use TCP on
it, and end it. The type is `rsasync::driver::Driver`. rsasync is in development, and its alpha
releases are on crates.io.

The driver has TCP bind, connect, accept, receive, send, half-close, and close. It also has UDP
datagrams with their metadata, IPC (inter-process communication) streams at a path, the receive, send, and close of pipe ends,
files, and child processes. The other operations come in later changes. [TCP on the driver](tcp.md),
[UDP on the driver](udp.md), [IPC streams on the driver](ipc-streams.md),
[Pipes on the driver](pipes.md), [Files on the driver](files.md), and
[Child processes on the driver](processes.md) are the references for each operation and its
results.

The driver needs the `std` feature, and Linux on x86_64 or aarch64, macOS on aarch64, or Windows on
x86_64. On Linux, it runs on io_uring, and on [epoll](epoll.md) where the kernel refuses io_uring,
with the same operations. On macOS, it runs TCP, UDP, and files on [kqueue](kqueue.md), and on
Windows TCP, UDP, files, and IPC streams (named pipes) on an [I/O completion port](iocp.md), with
the reads, writes, and syncs of files on an [I/O ring](iocp.md#ioring-files) where the system has
one. The other operations of this page
have no macOS or Windows form yet. It does not need
the `local` feature, which holds the local tasks of `rsasync::task`, a scheduler. It starts no helper
thread. Before Linux 6.9, a change of the length of a file runs on your
[blocking service](blocking-service.md), on epoll and kqueue each file call does, on IOCP each
file call with no overlapped form does, and on IoRing each file call that the ring does not have
does. `Driver::report` gives the features of the kernel and the
route of each such call before the first operation: see the [capability report](capability-report.md)
and [Length and space](files.md#length-and-space).

## Run the example

The program in `examples/driver_only` of the repository uses only the public API of the driver. Its
loop polls a future again only after the future woke its waker. It prints one line for each of
these outcomes:

- The capability report: 0 helper threads, and the route of `set_file_len`. The program gives the
  driver a [blocking service](blocking-service.md). The driver uses it only for a call whose route is
  `Route::BlockingService`.
- File, TCP, and UDP I/O on owned buffers. Each operation takes its buffer, and its future gives the
  buffer back, also after a failure.
- A receive gives its completion to the waker of its latest poll, not to an earlier waker.
- Partial progress: an exact read that ends at the end of the file, a TCP receive into a buffer that
  is smaller than the data, and a datagram that a small buffer truncates. The same buffer then takes
  the next bytes.
- `Operation::cancel` on a waiting receive: the future gives `ECANCELED` and the buffer, and the next
  receive uses that buffer.
- A dropped receive and a dropped accept. A drop ends the interest, and the driver keeps the buffer
  until the kernel ends the operation. The next receive gets the bytes.
- Close of each resource. Then teardown cancels 0 operations, because no operation of the driver
  still runs.

Its manifest takes rsasync with no scheduler:

```toml
[target.'cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))'.dependencies]
rsasync = { path = "../..", default-features = false, features = ["std"] }
```

Run it on Linux, from the root of the repository:

```sh
cargo run -p driver_only
```

On Linux 7.0, it prints this text:

<!-- The same text as `EXPECTED` in examples/driver_only/src/main.rs. Edit both. -->
```text
report: 0 helper threads; set_file_len route: Ring
file: wrote 11 bytes and made them durable
file: an exact read of 16 bytes stopped at the end of the file after 11 bytes: "hello world"
file: set the length to 5, and the length is 5
file: the same buffer then read 5 bytes: "hello"
file: closed and removed the file
tcp: the receive woke its current waiter 1 time and its earlier waiter 0 times
tcp: the receive gave 8 of the 11 sent bytes: "hello wo"
tcp: the same buffer then received the rest: "rld"
tcp: the cancelled receive gave ECANCELED and its buffer back
tcp: the same buffer then received "again"
tcp: after a dropped receive, the next receive gave "late"
tcp: closed the listener after a dropped accept
tcp: closed both streams; Closed::sent() gives [Settled, Settled]
udp: a receive into 4 bytes gave "ping", truncated: true
udp: the same buffer then received "pong", truncated: false
udp: after a dropped receive, the next receive gave "late"
udp: closed both sockets
driver: teardown cancelled 0 operations and kept 0 children
blocking service: teardown found 0 running jobs and dropped 0 jobs
```

Before Linux 6.9, the first line ends with `set_file_len route: BlockingService`, because the kernel
has no ring operation for the change of the length. The service then runs it, and each other line
is the same. On another target, the program ends with an error, because the driver needs Linux on
x86_64 or aarch64. On Linux, `cargo xtask test` runs the steps of the program and compares their
output with this text.

`cargo tree -p driver_only -e features` shows `rsasync feature "std"` and no `local` feature.
A build of the whole workspace unifies the features of its packages, so the build with no scheduler
is the build of the package alone: `cargo build -p driver_only` or `cargo run -p driver_only`.
`cargo xtask check` lints that build alone, on the host.

## The contract

The driver is the home of each operation that it accepts, from the submission to the terminal
event of the operation. Only the terminal event releases the buffer and the charge of the
operation. You keep the driver driven until that event, also after you drop or forget the future of
the operation. The future can move to another thread; the driver stays on the thread that created
it.

Each resource and each child belongs to the driver that made it, also after it moves to another
thread. Use it only with that driver. Another driver refuses it and changes nothing: an operation
and `Driver::close_child` give `Refused` with `Reason::Resource` and the value, before each other
refusal, a call that gives an `io::Result` gives an error of the kind `NotFound`, and a signal of a
child gives `SignalChildError::OtherDriver`.

| Call | What it does |
| --- | --- |
| `Driver::new(Config)` | Starts the driver and allocates its bounded memory one time. When the allocator refuses, it gives `Error::System` of the kind `OutOfMemory`. If the kernel or its policy refuses io_uring, for example in a container with the default seccomp profile, it runs [epoll](epoll.md), or it gives `Error::Refused` when `Config::backend` requires io_uring |
| `Driver::report()` | What the driver found and applied when it started: the [capability report](capability-report.md) |
| `Driver::turn()` | One bounded turn that never blocks. It takes the requests of other threads, submits, enters the kernel only when there is work for it, and takes up to `Config::completions_per_turn` completions from the ring. It gives the completion of each operation to its future. The bound and `Turn::completed()` also count the completions of the driver itself, such as cancel results |
| `Turn::more()` | Work remains. Turn again before you wait |
| `Driver::wait()` | Blocks in the kernel until a completion is ready, or a request or a wake from another thread comes. A handled signal also ends the wait, with no completion. The kernel runs its deferred work during the wait. On io_uring, while a file operation runs, the wait enters the kernel again after each 250 µs, but it still returns only for these reasons. See [Wake latency of a wait](#wake-latency-of-a-wait) |
| `Driver::wait_until(deadline, clock)` | Blocks as `wait()` does, but at most until `deadline`, an instant of the `NativeClock` `clock`. See [Wait until a deadline](#wait-until-a-deadline) |
| The wake descriptor (`AsFd` of the driver) | Becomes readable when work comes: a completion, deferred work of the kernel, or a request or a wake from another thread. On io_uring, the first `as_fd()` registers it. Some kernels signal deferred work only one time; see [Drive it from another reactor](#drive-it-from-another-reactor) |
| `Driver::wake_handle()` | A `WakeHandle` that any thread can use to make the wake descriptor readable and to end a wait. A turn takes a wake that came before it |
| `Driver::teardown()` | Cancels each running operation, drives until each terminal event, closes each resource, and reports the cancelled operations |

## Drive it from your own loop

Turn while work remains, and wait when no work remains:

<!-- The same text as the doctest of `rsasync::driver`. Edit both. -->
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

A loop that only waits in the driver pays nothing for the wake descriptor. On io_uring, the driver
registers that descriptor only when you ask for it with `as_fd()`. Without it, the kernel signals
no descriptor at a completion, and no turn reads one.

A wake of another thread, and a handled signal, write that one descriptor. So a wake reaches a
host reactor at once, and it ends a wait. Select one way to wait for a driver. A loop that uses
`as_fd()` and also calls `wait()` still works, but the kernel then signals the descriptor at each
completion, so a wait can end one time more with no new work.

### Wait until a deadline

A loop with timers waits for its I/O and for its next timer in one call. `Driver::wait_until` ends
at a completion, at a request or a wake from another thread, at a handled signal, or when the
deadline passed. The kernel ends the wait, so the driver starts no timer thread.

```rust
use core::time::Duration;

use rsasync::clock::NativeClock;
use rsasync::driver::{Config, Driver};
use rsasync::time::{Deadline, Suspend};

let clock = NativeClock::new(Suspend::Pauses)?;
let mut driver = Driver::new(Config::new())?;
loop {
    let turn = driver.turn()?;
    // 1. Poll the futures of your operations.
    // 2. Take the work that other threads gave to this loop.
    // 3. Run each timer that is due, and find the deadline of the next one.
    let next_timer = Deadline::at(clock.now().checked_add(Duration::from_millis(250))?);
    if !turn.more() {
        driver.wait_until(next_timer, clock)?;
    }
    // Stop when your work is done.
}
```

| Case | What `wait_until` does |
| --- | --- |
| No work before the deadline | It returns when the deadline passed |
| The deadline passed before the call | It takes the pushed entries and returns, with no block |
| A completion comes first | It returns then. The next turn gives the completion to its future |
| Another thread calls `WakeHandle::wake` | It returns then, also when the wake came before the wait and after the last turn |
| A handled signal comes | It returns then. Call it again with the same deadline: the time does not start again |
| The deadline is not an instant of the domain of `clock` | It gives an error of the kind `InvalidInput`, and it does not wait |

Rules for the loop:

- Read the clock after each return. The wait can end before the deadline with no work for you.
- Take the work of other threads after the turn and before the wait. A turn takes each wake that
  came before it. A wake that comes after the turn ends the wait.
- Use a clock of `Suspend::Pauses`. The kernel measures the wait on a clock that pauses while the
  system is suspended. With a clock of `Suspend::Advances`, a suspend during the wait can end the
  wait later than the deadline.
- On epoll and IOCP, the kernel counts the wait in milliseconds, so the driver rounds the time up.

The timed wait has the same form on io_uring, epoll, kqueue, and IOCP.

### Wake latency of a wait

While a thread waits, the kernel can put its CPU in a deep idle state. A deep state takes longer
to wake: C6 of an Intel Xeon server takes 190 µs. The kernel selects the state from its estimate
of the sleep, and the next timer of the CPU limits that estimate. So a wait that ends at a device
completion can end later than the device.

On io_uring, the driver prevents this while a file operation runs. Each entry of the kernel in
`wait()` or `wait_until()` ends after 250 µs at most, and the wait enters again. With this timer,
the kernel selects no idle state that needs a longer sleep. Most file operations end before the
timer, so they cost no other wakeup. A longer operation costs one more entry for each 250 µs. A
wait while no file operation runs, for example while only TCP receives wait, sleeps until its
wake, so an idle driver does not wake its CPU.

A receive on a socket can come after any time, so the driver does not limit that wait. On a host
that needs a low latency for network requests, limit the wake latency of the CPUs of your loops.
This command keeps CPU 1 out of each idle state that takes more than 20 µs to wake:

```sh
echo 20 | sudo tee /sys/devices/system/cpu/cpu1/power/pm_qos_resume_latency_us
```

The CPU then uses more power while it is idle. `0` removes the limit. To see the idle states of a
CPU and their wake latencies in microseconds:

```sh
cd /sys/devices/system/cpu/cpu1/cpuidle && grep . state*/name state*/latency
```

## Drive it from another reactor

On Linux and macOS, register the wake descriptor (`driver.as_fd()`) for readability with the
reactor of your host, for example epoll, kqueue, or the reactor of another runtime. When it reports the descriptor readable,
turn the driver, and turn it again while `Turn::more()` gives `true`. The turn clears the
descriptor, so the reactor reports it again only for new work.

On io_uring, the first `as_fd()` registers the descriptor with the ring. The descriptor is readable
then, one time, so that your first turn takes the work that came before the registration. If the
registration fails, the next `Driver::turn` gives its error. Ask for the descriptor only when a
reactor waits for it: a loop that waits in `Driver::wait` or `Driver::wait_until` does not need it.

The descriptor becomes readable for each completion and for each deferred work of the kernel, on
each supported kernel, so a reactor that waits only for it misses no operation.

One kernel defect needs a workaround in the driver. A Linux kernel with the io_uring change
`cd305ee3633a` ("defer eventfd signaling when queued from a wakeup handler") and not
`04fe9aeb4f3c` ("reset deferred signal state") signals the descriptor for deferred work only one
time in each registration, for example Ubuntu 26.04 with `7.0.0-1016-aws`. The first `as_fd()`
of a process on io_uring finds such a kernel with a probe of two deferred signals, which takes up
to about 2 s. Each later driver of the process uses that result and makes no probe. The workaround goes when the
supported kernels have `04fe9aeb4f3c`.

On such a kernel, the driver registers the descriptor again in each turn that takes a wake: two
system calls in that turn. The driver also does this when a signal of the probe comes late, for
example when a busy kernel delays it. A late signal does not prove that the kernel has no defect.
A kernel with no defect that signals in time makes no extra system call.
`driver.report().rearms_wake()` gives `true` when the driver registers again (see the
[capability report](capability-report.md)).

## Accept, receive, and send on TCP

The driver owns each socket that it makes. `Driver::bind` binds a nonblocking socket and listens on
it, and `Driver::connect` connects one. `Driver::accept` gives the future of a connection: a
`TcpStream` and the address of its peer.
Each operation gives an `Operation` future. You poll it, and you turn the driver until it is ready.
This helper does both:

<!-- The same text as the doctest of `rsasync::driver::Operation`. Edit both. -->
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

<!-- The same text as the doctest of `rsasync::driver::TcpStream`. Edit both. -->
```rust
use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::driver::{Config, Driver, TcpOptions};

let mut driver = Driver::new(Config::new())?;
let bind = driver.bind("127.0.0.1:8080".parse()?, TcpOptions::default())?;
let mut listener = run(&mut driver, bind)???;
let accept = driver.accept(&mut listener)?;
let (mut stream, peer) = run(&mut driver, accept)???;

let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(4096)))?;
let (received, request) = run(&mut driver, receive)??;
let count = received?;
assert_eq!(request.bytes().len(), count);

let send = driver.send(&mut stream, Lease::from(b"HTTP/1.1 200 OK\r\n\r\n".to_vec()))?;
let (sent, _response) = run(&mut driver, send)??;
sent?;
let close = driver.close(stream)?;
run(&mut driver, close)??;
```

The outer `Result` of each future is `Err(Ended)` only if the driver ended before the operation
gave its result. A poll of a future after it gave its output panics, as for the futures of the
standard library. A refused operation, for example when each operation slot is in use, gives its
`Reason` and the buffer or lease back.

Each future borrows the direction of its stream (`&mut`), so each direction runs one operation at
a time. `TcpStream::split` gives a read half and a write half, so that a receive and a send can run
at one time. A dropped or forgotten future loses no byte and no connection: [TCP on the
driver](tcp.md#dropped-forgotten-and-cancelled-operations) gives the rules.

### Receive with no buffer while you wait

`Driver::provide` gives the slots of an `rsasync::buffer::Pool` to the kernel. Then
`Driver::recv_selected` waits with no buffer, and the kernel takes a free slot only when the data
comes. The future gives that slot as an exclusive `Backing` with the received bytes, so you can
change them in place, for example to decrypt them. `Lease::from` then freezes them at the same
address, with no allocation. When the backing, or the last lease or view of the slot, drops, on any
thread, the slot goes back to the pool, and the next turn gives it to the kernel again:

<!-- The same text as the doctest of `rsasync::driver::Driver::provide`. Edit both. -->
```rust
use core::alloc::Layout;
use core::num::{NonZeroU32, NonZeroU64};

use rsasync::buffer::Pool;
use rsasync::capacity::Budget;

// The pool charges its memory to a budget: 64 slots of 4096 bytes.
let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
let slot = Layout::from_size_align(4096, 4096)?;
driver.provide(Pool::new(&budget, NonZeroU32::new(64).unwrap(), slot)?)?;
let receive = driver.recv_selected(&mut stream)?;
let request = run(&mut driver, receive)???;
```

If no slot is free when the kernel tries the receive, the receive ends with the error `ENOBUFS`
(`io::Error::raw_os_error`). The data stays in the socket, so receive again after a slot comes
back.

`Driver::close` closes a stream, and `Driver::close_listener` closes a listener. Each one cancels the
operations that run on the socket, and its future is ready after the kernel ends each one: see
[Close](tcp.md#close).

## Limits that you set

`Config` sets each limit before the driver starts:

| Setting | Default | Meaning |
| --- | --- | --- |
| `submissions` | 64 | Entries of the submission queue. The kernel rounds up to a power of two |
| `operations` | 256 | Operations that can run at one time. A submission beyond them is refused with the reason, and its resources come back. The driver allocates one slot for each when it starts. On a 64-bit target, the state of an operation in its slot takes at most 576 bytes, not counting its buffers |
| `resources` | 256 | Descriptors that the driver can own at one time |
| `children` | 64 | Child processes that the driver can own at one time. Each child also holds an operation slot until its status is taken or its handle drops. 0 refuses each spawn |
| `completions_per_turn` | 64 | The most completions that one turn gives to their futures |
| `delivery` | `Delivery::Native` | How the driver gives each completion to its operation. Under `Delivery::Controlled`, `Driver::control()` lets a test hold, fail, or repeat a completion: see [Test your code under controlled completions](controlled-delivery.md) |
| `files` | `Files::Plain` | The descriptor that each operation of a resource names. See [Registered files](#registered-files) |

A limit of 0, other than `children`, gives `Error::Config`.

## Registered files

With `Config::files(Files::Registered)`, an io_uring driver registers a file table at its start
(Linux 5.19), with one place for each resource slot. Each socket and pipe that the driver adopts
gets the place of its slot, and each receive, send, accept, and half-close names that place, so the
kernel takes no reference of the file for the operation. The close empties the place before it
closes the descriptor, so the socket closes as with a plain descriptor.

- `Report::files()` gives the table in use. It is `Files::Plain` on epoll, and when the kernel
  refuses the table.
- A resource whose registration fails keeps its plain descriptor, and its operations run on it, with
  no refusal. Under controlled delivery, `Control::fail_next_registration()` makes that failure.
- The operations of a file, and the operations of a movable resource, name the plain descriptor.

The default is `Files::Plain` until a measurement on the thread-per-core TCP cell selects the
default.

## End the driver

Call `Driver::teardown()`. It cancels each operation that still runs, by its own identity, and it
waits until the kernel ends each one, so it releases no buffer that the kernel can still use. Then
it closes each resource. `Teardown::cancelled()` gives the number of cancelled operations. A future
that did not take its result before teardown then ends with no result. Teardown also applies the end
policy of each child process: it kills a child with `End::Kill` and waits for its reap, and it
leaves a child with `End::Keep` running. `Teardown::kept()` counts those children (see
[Child processes on the driver](processes.md#end-policy-and-teardown)).

If you drop the driver without teardown, its futures end with no result, and the driver keeps the
buffers of the operations that still run: it leaks them, because the kernel can still use them.
