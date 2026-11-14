# Pipes on the driver

This page is the reference for pipe ends on the io_uring driver, `rsasync::driver::Driver`: how
to give a pipe end to the driver, each operation, and its result. A pipe end follows the stream
rules of [TCP on the driver](tcp.md): one operation for each direction, the rules for a dropped,
forgotten, or cancelled operation, and close. [Drive the I/O driver yourself](driver-only.md)
tells how to start, turn, and end the driver.

## Give a pipe end to the driver

`Driver::adopt_reader(fd)` takes the read end of a pipe or a FIFO and gives a `PipeReader`.
`Driver::adopt_writer(fd)` takes the write end and gives a `PipeWriter`. The descriptor is an
`OwnedFd`, for example from `std::io::pipe()`, or from the standard output of a child:
`OwnedFd::from(child.stdout.take().unwrap())`. A child of the driver has its piped streams as pipe
ends already (see [Child processes on the driver](processes.md#standard-streams)).

The driver clears `O_NONBLOCK` of the open file description of the end, because a nonblocking
read or write ends at once instead of waiting. Another descriptor of the same open file
description, for example a duplicate, then blocks too.

An adoption refuses, gives the descriptor back in its `Refused`, and changes nothing:

| `Refused::reason()` | Cause |
| --- | --- |
| `Reason::NotPipe` | The descriptor is not a pipe or a FIFO, for example a socket |
| `Reason::Direction` | A write end to `adopt_reader`, or a read end to `adopt_writer` |
| `Reason::Resources` | Each resource slot of the driver is in use (`Config::resources`) |

## Operations

Each operation gives an `Operation` future. Turn the driver until the future is ready. The outer
`Result` of the output is `Err(Ended)` only if the driver ended first.

| Call | Borrows | Output | Refusal |
| --- | --- | --- | --- |
| `Driver::recv(&mut reader, backing)` | The reader | `(io::Result<usize>, Backing)`: the count, and the buffer with the bytes. A count of 0 is the end of the stream | `Refused<Backing>`, with the buffer |
| `Driver::recv_selected(&mut reader)` | The reader | `io::Result<Backing>`: the buffer that the kernel selected from the pool of `Driver::provide`, with the bytes | `Refused<()>`, also with `Reason::Buffers` when the driver has no pool |
| `Driver::send(&mut writer, lease)` | The writer | `(io::Result<usize>, Lease)`: the count, and the lease | `Refused<Lease>`, with the lease |
| `Driver::send_vectored(&mut writer, regions)` | The writer | `(io::Result<usize>, Regions)`: the count across the regions, and the regions | `Refused<Regions>`, with the regions |
| `Driver::shutdown_write(&mut writer)` | Nothing | None: a pipe has no half-close | `Refused<()>` with `Reason::Unsupported`, at once. Close the writer instead |
| `Driver::close_reader(reader)` | Takes the reader | `Closed` | `Refused<PipeReader>`, with the reader |
| `Driver::close_writer(writer)` | Takes the writer | `Closed`, with the progress that dropped sends owe | `Refused<PipeWriter>`, with the writer |

A receive on an idle reader waits for data. A selecting receive holds no buffer while it waits:
the kernel takes a buffer from the pool only when the data comes.

A send of more bytes than the pipe has room for depends on the kernel. On Linux 7.0 it ends with
the count of the bytes that fit. On Linux 6.1 it waits until the reader takes the rest, or until a
cancel or a close, which then gives the count of the bytes that went. Each count is exact.

This example sends through a pipe, closes the writer, and receives until the end of the stream,
with `run` as in `Operation`:

<!-- The same text as the doctest of `rsasync::driver::PipeReader`. Edit both. -->
```rust
use std::os::fd::OwnedFd;

use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::driver::{Config, Driver};

let mut driver = Driver::new(Config::new())?;
let (read, write) = std::io::pipe()?;
let mut reader = driver.adopt_reader(OwnedFd::from(read))?;
let mut writer = driver.adopt_writer(OwnedFd::from(write))?;

let send = driver.send(&mut writer, Lease::from(b"hello".to_vec()))?;
let (sent, _lease) = run(&mut driver, send)??;
assert_eq!(sent?, 5);
let close = driver.close_writer(writer)?;
run(&mut driver, close)??;

let receive = driver.recv(&mut reader, Backing::from(Vec::with_capacity(64)))?;
let (received, buffer) = run(&mut driver, receive)??;
assert_eq!((received?, buffer.bytes()), (5, &b"hello"[..]));
let receive = driver.recv(&mut reader, Backing::from(Vec::with_capacity(64)))?;
let (end, _buffer) = run(&mut driver, receive)??;
assert_eq!(end?, 0, "the end of the stream");
```

## End of the stream and a closed reader

- The close of the writer, and of each other descriptor of the write end, is the end of the
  stream: the next receive after the last byte gives 0.
- A send after the reader closed gives an error of kind `io::ErrorKind::BrokenPipe` (`EPIPE`),
  where the process ignores `SIGPIPE`, as a Rust program does by default. Where the process does
  not ignore it, the signal ends the process. The driver changes no disposition (see
  [Signal streams](signals.md)).

## Dropped, forgotten, and cancelled operations

The rules of [TCP on the driver](tcp.md#dropped-forgotten-and-cancelled-operations) apply with no
change:

- A dropped receive asks for a cancel. Bytes that it took go to the next receive, before it
  submits, in order.
- A dropped send still runs. A dropped send that fails or sends only part of its bytes fails the
  write direction, and the next send gives `WriteFailed` with the sent count.
- An explicit cancel gives the buffer back with its exact progress.

## Close

`Driver::close_reader` and `Driver::close_writer` cancel the operation that runs on the end, a
dropped send too, wait until it ends, and close the descriptor. A close does not wait for the other
end: it returns also when the other end neither reads nor writes. The `Closed` result of a writer
reports the progress that its dropped sends owe, for example `Some(0)` for a send that waited for
room in a full pipe.
