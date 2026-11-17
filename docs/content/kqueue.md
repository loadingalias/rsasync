# The kqueue backend

On macOS, the driver runs on kqueue. TCP and UDP have the same API and the same ownership rules as
on io_uring, epoll, and IOCP: the terminal event of an operation releases its buffer and its charge,
and a dropped or forgotten future ends only the interest. This page tells how each operation runs
on kqueue, what you give the driver, and what differs from Linux. rsasync is in development, and the
kqueue backend is not in a published release yet.

## Select the backend

kqueue is the one backend of the driver on macOS on aarch64 (`aarch64-apple-darwin`).
`Config::new()` selects it, and `report().backend()` gives `Backend::Kqueue`.
`Config::new().backend(Backend::IoUring)`, `Backend::Epoll`, or `Backend::Iocp` makes `Driver::new`
refuse with `Error::Refused`, with the kind `Unsupported`. A feature of `Config::require` is a
feature of io_uring, so a macOS driver with a required feature refuses with `Error::Unsupported`.

```rust
use rsasync::driver::{Backend, Config, Driver};

let driver = Driver::new(Config::new())?;
assert_eq!(driver.report().backend(), Backend::Kqueue);
```

## How each operation runs

The kqueue backend is the readiness poller of [the epoll backend](epoll.md), with kqueue in place
of epoll. Each operation runs as one nonblocking system call on the thread of the driver when
kqueue reports its socket ready.

| Operations | On kqueue |
| --- | --- |
| TCP: accept, connect, receive, send, vectored send, half-close, close | One nonblocking call when the socket is ready. A receive into a selected buffer ([`Driver::provide`](driver-only.md)) takes its buffer only when its data comes. A send to a closed peer gives `EPIPE` and raises no `SIGPIPE` (`MSG_NOSIGNAL`) |
| UDP: one datagram, with its metadata and truncation | One nonblocking `sendmsg` or `recvmsg`, with the control messages of [datagram I/O](datagram-io.md) |
| UDP batches: `send_batch` and `recv_batch` | One `sendmsg` or `recvmsg` for each datagram, in order. A batch stops at the first datagram that the kernel does not take, and its report counts the datagrams before it |
| Bind and listen (`Call::Bind`, `Call::BindUdp`) | System calls on the thread of the driver (`Route::SystemCall`) |

macOS has no flags of `socket` and `accept` for a close at `exec` and for no blocking. So the driver
sets `FD_CLOEXEC` and `O_NONBLOCK` with `fcntl` on each socket that it makes or accepts, after the
call. Another thread of your process that forks between the two calls can give the socket to its
child, as for the sockets of the standard library on macOS.

The terminal event of each operation is the return of its nonblocking call. After that return, the
kernel keeps no reference to the buffer. A cancel finds no operation running: the operation waits
for its socket, and it ends at once with `ECANCELED`, or it already ended.

## UDP has no segmentation offload

macOS has no segmentation offload and no coalesced receive. A caller that checks each path with
`datagram::Probe` learns it at the first step on macOS: the send of the probe gives
`Refused::NoSegmentation` ([validate segmentation offload](datagram-io.md#validate-segmentation-offload)).
That is the check of each other system too. On the driver:

- `send_to` of a `Datagram` with a `segment_size` sends nothing, and its future gives
  `datagram::Refused::NoSegmentation`.
- `send_batch` sends the datagrams before its first datagram with a segment size, and its report
  counts them. That datagram and the datagrams after it stay unsent. The next batch, which starts
  with that datagram, gives `NoSegmentation`. Send its contents as one datagram for each segment.
- `bind_udp` with `UdpOptions::gro` gives `Unsupported` (`ENOTSUP`), and the driver binds nothing.

The driver does not use the private batch calls `sendmsg_x` and `recvmsg_x` of
[datagram I/O](datagram-io.md#macos). A batch is one call for each datagram.

## Helper threads

The driver starts no thread, and `report().helper_threads()` gives 0. On macOS, no operation of the
driver runs on the [blocking service](blocking-service.md): files, pipes, local streams, child
processes, and signals have no kqueue form yet.

## Registrations

The driver registers each socket in its kqueue when it takes the socket: one edge-triggered
registration (`EV_CLEAR`) for reads (`EVFILT_READ`) and one for writes (`EVFILT_WRITE`). It
removes both before the close. When kqueue refuses a registration, for example with no kernel
memory, the operation gives `OutOfMemory`, as with no free resource slot, and an accepted connection
stays with its listener for the next accept.

## Drive it

`turn`, `wait`, and `teardown` work as on io_uring ([driver-only use](driver-only.md)). Each turn
takes the events of the kqueue with no wait. A wake from another thread, and each request of a
future on another thread, triggers a user event of the kqueue (`EVFILT_USER`), which ends a wait.
The wake descriptor (`AsFd` of the driver) is the kqueue: it is readable when an event of a socket
waits or another thread wakes the driver, so a host reactor watches it, for example with `poll`.

## Limits

- Only TCP and UDP. Files, pipes, local streams, child processes, and signals have no macOS form
  yet.
- No zero-copy send yet.
- The kernel probes of the capability report run only on io_uring, so `report().probed()` is empty on
  kqueue.
- x86_64 macOS has no driver yet.
