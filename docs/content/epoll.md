# The epoll backend

The driver runs on epoll where the kernel refuses io_uring, for example under the default seccomp
profile of a container or with `kernel.io_uring_disabled`. Each operation of the driver
has the same API and the same ownership rules on both backends: the terminal event of an operation
releases its buffer and its charge, and a dropped or forgotten future ends only the interest. This
page tells when the driver selects epoll, how each operation runs on it, and what you give it.
rsasync is in development, and the epoll backend is not in a published release yet.

## Select the backend

| `Config` | Kernel refuses io_uring (`EPERM` or `ENOSYS` at ring creation) | Kernel allows io_uring |
| --- | --- | --- |
| `Config::new()`, the permissive selection | epoll, and `report().fallback()` gives the error number | io_uring |
| `Config::new().backend(Backend::IoUring)` | `Driver::new` gives `Error::Refused`, and no driver exists | io_uring |
| `Config::new().backend(Backend::Epoll)` | epoll | epoll |

The [capability report](capability-report.md) states the selection:

```rust
use rsasync::driver::{Backend, Config, Driver};

let driver = Driver::new(Config::new())?;
let report = driver.report();
if report.backend() == Backend::Epoll {
    // `fallback()` is `Some(1)` (`EPERM`) under `kernel.io_uring_disabled` or a seccomp profile.
    println!("io_uring refused with {:?}: the driver runs epoll", report.fallback());
}
```

A feature of `Config::require` is a feature of io_uring, so an epoll driver with a required feature
refuses with `Error::Unsupported`.

## How each operation runs

| Operations | On epoll |
| --- | --- |
| TCP, [local streams](local-streams.md), and pipes: accept, connect, receive, send, vectored send, half-close, close | One nonblocking call on the thread of the driver when epoll reports the descriptor ready. A local bind and a local connect are system calls, ready at once, as on io_uring. A receive into a selected buffer ([`Driver::provide`](driver-only.md)) takes its buffer only when its data comes |
| UDP: one datagram, with its metadata, segments, and truncation | One nonblocking `sendmsg` or `recvmsg`, with the control messages of [datagram I/O](datagram-io.md) |
| UDP batches: `send_batch` and `recv_batch` | One `sendmmsg` or `recvmmsg` for the batch. A send that went only in part sends the rest in the next call |
| Bind and listen (`Call::Bind`, `Call::BindUdp`) | System calls on the thread of the driver (`Route::SystemCall`) |
| The exit of a child (`Call::WaitChild`) | epoll waits for the process descriptor, then one nonblocking `waitid` (`Route::Poll`). A poll that finds no status, for a traced child, waits for the timer of the children of the driver ([Wait for the exit](processes.md#wait-for-the-exit)) |
| Files and directories (`Call::File`, `Call::SetFileLen`) | A job of your [blocking service](blocking-service.md), with positional calls (`preadv2`, `pwritev2`, and the others), because epoll cannot wait on a regular file (`Route::BlockingService`) |
| Spawn of a child | A job of your blocking service, as on io_uring |
| [Signals](signals.md) | The same streams as on io_uring. The handler writes the wake descriptor that epoll watches |

The terminal event of each readiness operation is the return of its nonblocking call, and the
terminal event of a file operation is the return of its job. After that return, the kernel keeps no
reference to the buffer. A cancel finds no readiness operation running: the operation waits for its
descriptor, and it ends at once with `ECANCELED`, or it already ended. A file job cannot be
cancelled, so its operation ends when the job returns.

## Helper threads

The driver starts no thread on either backend, and `report().helper_threads()` gives 0. On epoll,
each file operation runs on a thread of the blocking service that you give with
`Driver::use_blocking`, and you set the count of its threads (`blocking::Config::threads`). With no service, a file operation refuses with `Reason::Blocking` before it
starts, so no file call blocks the thread of the driver. Size the service for the file operations
that run at one time: a full service refuses with `Reason::Blocking`, and the caller keeps its
buffer.

```rust
use core::num::NonZeroU64;

use rsasync::blocking::{self, Service};
use rsasync::capacity::Budget;
use rsasync::driver::{Backend, Config, Driver};

let budget = Budget::new(NonZeroU64::new(16 << 20).unwrap());
let service = Service::new(&budget, blocking::Config::new().threads(4).jobs(64))?;
let mut driver = Driver::new(Config::new().backend(Backend::Epoll))?;
driver.use_blocking(service.submitter());
// Open, read, and write files on the driver here.
let teardown = driver.teardown()?;
let service = service.teardown();
```

## Registrations

The driver registers each socket and pipe end in its epoll instance when it takes the descriptor,
and it removes the registration before the close. A pipe end gets `O_NONBLOCK` at the registration.
When epoll does not take a descriptor, for example at the limit of `fs.epoll.max_user_watches`, the
adoption refuses with `Reason::Register` and gives the descriptor back, open: `adopt_reader` and
`adopt_writer` give it in `Refused::into_inner`. An accepted connection that the driver cannot
register stays with the listener, and the accept gives `OutOfMemory`, as with no free resource
slot.

## Drive it

`turn`, `wait`, and `teardown` work as on io_uring ([driver-only use](driver-only.md)). Each turn
takes the events of epoll with no wait. The wake descriptor (`AsFd` of the driver) is the epoll
descriptor: it is readable when an event of a resource waits or another thread wakes the driver, so
a host reactor watches it as on io_uring.

## Limits

- No zero-copy send yet, on either backend.
- The kernel probes of the capability report run only on io_uring, so `report().probed()` is empty on
  epoll.
