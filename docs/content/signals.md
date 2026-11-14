# Signal streams

A signal stream reports that a Unix signal arrived, for example `SIGTERM` from a service manager or
`SIGINT` from Ctrl-C. Its home is an I/O driver that you own and turn (see
[Drive the I/O driver yourself](driver-only.md)). The type is `rsasync::driver::SignalStream`.
rsasync is in development, and its alpha releases are on crates.io. Signal streams need Linux on
x86_64 or aarch64, and the `std` feature.

rsasync never connects a signal to the end of your program by itself. Your code receives the
signal and starts its own shutdown.

## Receive a signal

`Driver::signal(signal, escalation)` gives a stream of `signal`, whose home is that driver.
`SignalStream::recv().await` gives `Ok(())` when the signal arrived since the last report, and
`Err(Ended)` when the driver dropped. `SignalStream::poll_recv(cx)` is the same step for a caller
that polls by hand.

This loop drives a driver until an interrupt arrives, and then starts the shutdown of the program:

<!-- The same text as the doctest of `rsasync::driver::SignalStream`. Edit both. -->
```rust
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

use rsasync::driver::{Config, Driver, Escalation, Signal};

let mut driver = Driver::new(Config::new())?;
let mut interrupt = driver.signal(Signal::INTERRUPT, Escalation::Never)?;
let mut cx = Context::from_waker(Waker::noop());
loop {
    let more = driver.turn()?.more();
    if let Poll::Ready(result) = pin!(interrupt.recv()).poll(&mut cx) {
        result?;
        break;
    }
    if !more {
        driver.wait()?;
    }
}
// Start the shutdown of your program here. For the driver, `teardown` ends each operation.
drop(interrupt);
let report = driver.teardown()?;
```

An application with an executor starts its own shutdown at the same point, where the stream reports
the interrupt.

## One report for many signals

A stream reports that its signal arrived, not how many times. Several signals before one poll give
one report. Each stream of a signal reports it, also streams on different drivers: the signal
wakes the home of each one.

## Escalation for a stuck program

`Escalation::At(n)` states that from the `n`-th signal after the registration on, the handler
restores the default action of the signal and raises the signal again. With `Escalation::At(2)` on
`SIGINT`, the first Ctrl-C reaches the stream, so your program can start a clean shutdown. A second
Ctrl-C ends the program by the default action, also when the shutdown is stuck and nothing polls the
stream.

- When streams of one signal state different escalations, the lowest one applies.
- An escalation on a signal whose default action does not end the process, such as `SIGWINCH`,
  refuses with `SignalError::Escalation`.

## The handler and the disposition

The first stream of a signal installs a handler for that signal. When the last stream of the signal
closes, the disposition that the first stream replaced returns, for example the default action or a
handler of another library. While a stream lives, its handler replaces the earlier one and does not
call it. rsasync changes the disposition of no other signal.

The handler does only async-signal-safe work: it counts the signal, writes the wake descriptors of
the homes of its streams, and at a stated escalation restores the default action and raises the
signal again. It takes no lock, allocates nothing, and keeps `errno` of the code that it interrupts.
A system call that the handler interrupts restarts, because the handler uses `SA_RESTART`.

## Refusals

`Driver::signal` refuses with a `SignalError` and changes nothing:

| Refusal | Cause |
| --- | --- |
| `SignalError::Fault` | `SIGSEGV`, `SIGBUS`, `SIGFPE`, `SIGILL`, `SIGTRAP`, and `SIGSYS` report a fault of the thread that caused it. A stream cannot handle that fault |
| `SignalError::Uncatchable` | No handler can take `SIGKILL` or `SIGSTOP` |
| `SignalError::Invalid` | The number is not a signal of 1 to 64, or the C library keeps the signal for itself, such as 32 |
| `SignalError::Full` | The process has 64 signal streams, of all signals and drivers |
| `SignalError::Escalation` | The registration states `Escalation::At`, and the default action of the signal does not end the process: `SIGCHLD`, `SIGCONT`, `SIGURG`, or `SIGWINCH` |

`Signal` names the common signals, such as `Signal::TERMINATE` and `Signal::HANGUP`.
`Signal::from_raw(number)` gives each other signal, such as a real-time signal.
