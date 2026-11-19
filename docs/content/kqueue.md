# The kqueue backend

On macOS, the driver runs on kqueue. TCP, UDP, local streams, pipes, child processes, signal
streams, and files have the same API and the same ownership rules as on io_uring and epoll: the terminal event of an operation releases its buffer and
its charge, and a dropped or forgotten future ends only the interest. This page tells how each
operation runs on kqueue, what you give the driver, and what differs from Linux. rsasync is in development, and the
kqueue backend is not in a published release yet.

## Select the backend

kqueue is the one backend of the driver on macOS on aarch64 (`aarch64-apple-darwin`).
`Config::new()` selects it, and `report().backend()` gives `Backend::Kqueue`.
`Config::new().backend(Backend::IoUring)`, `Backend::Epoll`, `Backend::Iocp`, or `Backend::IoRing` makes `Driver::new`
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
| TCP, [local streams](local-streams.md), and [pipes](pipes.md): accept, connect, receive, send, vectored send, half-close, close | One nonblocking call when the socket or the pipe end is ready. A local bind and a local connect are system calls, ready at once. A receive into a selected buffer ([`Driver::provide`](driver-only.md)) takes its buffer only when its data comes. A send to a closed peer gives `EPIPE` and raises no `SIGPIPE` (`MSG_NOSIGNAL`) |
| UDP: one datagram, with its metadata and truncation | One nonblocking `sendmsg` or `recvmsg`, with the control messages of [datagram I/O](datagram-io.md) |
| UDP batches: `send_batch` and `recv_batch` | One `sendmsg` or `recvmsg` for each datagram, in order. A batch stops at the first datagram that the kernel does not take, and its report counts the datagrams before it |
| Bind and listen (`Call::Bind`, `Call::BindUdp`, and `Driver::bind_local`) | System calls on the thread of the driver (`Route::SystemCall`) |
| Files and directories (`Call::File`, `Call::SetFileLen`) | A job of your [blocking service](blocking-service.md), with positional calls, because kqueue cannot wait on a regular file (`Route::BlockingService`). See [Files](#files) |
| Spawn of a [child](processes.md) | A job of your blocking service, as on Linux |
| The exit of a child (`Call::WaitChild`) | kqueue waits for the exit event of the process (`EVFILT_PROC` with `NOTE_EXIT`), then one nonblocking `waitid` (`Route::Poll`). A registration that finds no process is the exit event. When the status is not ready yet, a one-shot kqueue timer waits from 100 µs, doubling up to 10 ms, and the wait repeats each 10 ms until the status comes ([Wait for the exit](processes.md#wait-for-the-exit)) |
| [Signals](signals.md) | The handler writes the wake pipe of the home |

macOS has no flags of `socket` and `accept` for a close at `exec` and for no blocking. So the driver
sets `FD_CLOEXEC` and `O_NONBLOCK` with `fcntl` on each socket that it makes or accepts, after the
call. Another thread of your process that forks between the two calls can give the socket to its
child, as for the sockets of the standard library on macOS.

The terminal event of each socket operation is the return of its nonblocking call, and the terminal
event of a file operation is the return of its job. After that return, the kernel keeps no
reference to the buffer. A cancel finds no socket operation running: the operation waits for its
socket, and it ends at once with `ECANCELED`, or it already ended. A file job cannot be cancelled,
so its operation ends when the job returns.

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

The driver starts no thread, and `report().helper_threads()` gives 0. Each file operation runs on a
thread of the [blocking service](blocking-service.md) that you give with `Driver::use_blocking`, as
on [epoll](epoll.md#helper-threads), and you set the count of its threads
(`blocking::Config::threads`). With no service, a file operation refuses with `Reason::Blocking`
before it starts, so no file call blocks the thread of the driver. A full service also refuses with
`Reason::Blocking`, and the caller keeps its buffer.

```rust
use core::num::NonZeroU64;

use rsasync::blocking::{self, Service};
use rsasync::capacity::Budget;
use rsasync::driver::{Config, Driver};

let budget = Budget::new(NonZeroU64::new(16 << 20).unwrap());
let service = Service::new(&budget, blocking::Config::new().threads(4).jobs(64))?;
let mut driver = Driver::new(Config::new())?;
driver.use_blocking(service.submitter());
// Open, read, and write files on the driver here.
let teardown = driver.teardown()?;
let service = service.teardown();
```

## Files

The file operations of [Files on the driver](files.md) run on kqueue with the same API. Each one is
one job of the blocking service, which makes the macOS call with a duplicate of the descriptor:

| Operation | macOS call |
| --- | --- |
| `open_file`, `open_directory` | `openat`. `Cache::Direct` then turns the data cache of the file off (`F_NOCACHE`) |
| `read_at`, `read_vectored_at` | `pread`, `preadv` |
| `write_at`, `write_vectored_at` | `pwrite`, `pwritev`. `WriteMode::DataSync` adds `F_FULLFSYNC` after the write, in the same job |
| `file_len`, `set_file_len` | `fstat`, `ftruncate` |
| `sync`, `sync_directory` | `F_FULLFSYNC`, for `SyncMode::Data` and `SyncMode::All` |
| `allocate` | `F_PREALLOCATE`, then `ftruncate` for `Length::Extend`. See [Preallocation](#preallocation) |
| `punch_hole` | `F_PUNCHHOLE`, and zeros in the partial blocks. See [Hole punching](#hole-punching) |
| `rename`, `unlink` | `renameat`, `unlinkat` |

A read or a write moves at most `INT_MAX` bytes in one call, because macOS refuses a larger count.
The count of the result then tells where to go on, as on Linux.

### Durability

Durable contents on macOS need `F_FULLFSYNC`: a plain `fsync` leaves the bytes in the cache of the
device, where a power loss can lose them. So `Driver::sync` with each `SyncMode`,
`Driver::sync_directory`, and each write with `WriteMode::DataSync` use `F_FULLFSYNC`. It writes
the cache of the device to its media, so it costs more than a sync on Linux. A file system with no
`F_FULLFSYNC` makes the operation give `Unsupported`. The driver never makes a weaker sync in its
place. The tests run `F_FULLFSYNC` on APFS, and a probe ran it on exFAT and FAT32 on 2026-11-17.

### Direct I/O

macOS has no `O_DIRECT`. A `Cache::Direct` open turns the data cache of the file off with
`F_NOCACHE`, and `File::alignment` gives one page for the memory and the offset: 16 KiB on Apple
silicon. A read or write with less alignment refuses with `Reason::Alignment`, as on Linux.

### Preallocation

`F_PREALLOCATE` reserves blocks only after the physical end of the file. So `Driver::allocate`
reads the bytes that the file has allocated, and asks for the blocks from there to the end of the
range, all of them or none. No free space gives `StorageFull`. Then `Length::Extend` changes the
length with `ftruncate` when the range ends after it.

- A hole inside the file stays a hole: the job reserves no block for it, so a later write into the
  hole can still fail for space. On Linux, `fallocate` fills the hole.
- The preallocation and the length change are two calls. So an allocation with `Length::Extend`
  runs alone on its file: it starts when each other operation of the file ended, and each later
  operation of the file waits until it ends. Thus a write that ends after the range keeps each of
  its bytes. Another process that writes the file at the same time is outside that order, as for
  each `ftruncate`.

### Hole punching

`F_PUNCHHOLE` frees only whole blocks of the file system, with the block size of `fstatfs`
(4096 bytes on APFS). So `Driver::punch_hole` frees the whole blocks of the range, and then writes
zeros into the bytes of the range in its first and last block, before the end of the file. The
whole range reads as zeros, the bytes outside it stay, and the length stays, as on Linux. On exFAT
and FAT32, macOS has no hole punching: the operation gives `Unsupported` before it changes a
byte.

## Registrations

The driver registers each socket and pipe end in its kqueue when it takes the descriptor: one
edge-triggered registration (`EV_CLEAR`) for reads (`EVFILT_READ`) and one for writes
(`EVFILT_WRITE`). It removes both before the close. A pipe end gets `O_NONBLOCK` at the
registration. The exit of a child and the timer after it have one-shot registrations. A file has no registration. When kqueue refuses a registration, for
example with no kernel memory, the operation gives `OutOfMemory`, as with no free resource slot, and
an accepted connection stays with its listener for the next accept.

## Drive it

`turn`, `wait`, and `teardown` work as on io_uring ([driver-only use](driver-only.md)). Each turn
takes the events of the kqueue with no wait. A wake from another thread, each request of a future
on another thread, and the handler of a [signal stream](signals.md) write the wake pipe of the
driver. kqueue watches its read end, level-triggered, so the write ends a wait, and each turn
takes the bytes. A pipe, because a signal handler can call `write` and not `kevent`. Each driver
thus holds two more descriptors. Both ends stay open while a `WakeHandle` or a signal stream of
the driver lives, so a write after the drop of the driver raises no `SIGPIPE`. The wake descriptor (`AsFd` of the driver) is the
kqueue: it is readable when an event waits or another thread wakes the driver, so a host reactor
watches it, for example with `poll`.

## Limits

- A hole inside a file stays a hole after a preallocation, and another process can write a file
  during an allocation with `Length::Extend` (see [Preallocation](#preallocation)).
- No zero-copy send yet.
- The kernel probes of the capability report run only on io_uring, so `report().probed()` is empty on
  kqueue.
- x86_64 macOS has no driver yet.
