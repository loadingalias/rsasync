# Local streams on the driver

This page is the reference for local streams on the io_uring driver, `rsasync::driver::Driver`:
Unix-domain stream sockets at a path, for processes on one host. It gives each operation, its
result, the permission rule of the socket file, and the identity of the peer. A local stream
follows the stream rules of [TCP on the driver](tcp.md): one operation for each direction, the
rules for a dropped, forgotten, or cancelled operation, and close.
[Drive the I/O driver yourself](driver-only.md) tells how to start, turn, and end the driver.

## Operations

Each operation gives an `Operation` future. Turn the driver until the future is ready. The outer
`Result` of the output is `Err(Ended)` only if the driver ended first.

| Call | Borrows | Output | Refusal |
| --- | --- | --- | --- |
| `Driver::bind_local(path, stale)` | Nothing | `io::Result<LocalListener>`: a listener at the socket file `path`. See [Bind at a path](#bind-at-a-path) | `Refused<()>` |
| `Driver::connect_local(path)` | Nothing | `io::Result<LocalStream>`: a stream to the listener at `path` | `Refused<()>` |
| `Driver::accept_local(&mut listener)` | The listener | `io::Result<LocalStream>`: the stream. See [The identity of the peer](#the-identity-of-the-peer) | `Refused<()>` |
| `Driver::recv`, `Driver::recv_selected`, `Driver::send`, `Driver::send_vectored`, `Driver::shutdown_write` | The read half or the write half | As for a TCP stream: see [TCP on the driver](tcp.md#operations) | As for a TCP stream |
| `Driver::local_peer(&stream)` | Nothing | `io::Result<Peer>`, at once | None |
| `Driver::close_local(stream)` | Takes the stream | `Closed` | `Refused<LocalStream>`, with the stream |
| `Driver::close_local_listener(listener)` | Takes the listener | `Closed` | `Refused<LocalListener>`, with the listener |

`LocalStream::split` gives the read half and the write half, so that a receive and a send can run
at one time. A half-close ends the receive of the peer after the bytes that went before it, as on
TCP.

The future of `bind_local` and `connect_local` gives these errors:

| Error kind | Cause |
| --- | --- |
| `InvalidInput` | The path is empty, holds a NUL byte, or has more than 107 bytes. The driver makes no socket. An abstract name, which has no file, is not available |
| `AddrInUse` | A bind found a file at the path. See [Bind at a path](#bind-at-a-path) |
| `PermissionDenied` | A connect to a socket file that the user of the process cannot write, for example one of another user. A bind with `Stale::Replace` on a socket file of another user |
| `NotFound` | A connect to a path with no file |
| `ConnectionRefused` | A connect to a socket file with no listener, for example the file of a closed listener |
| `OutOfMemory` | Each resource slot of the driver is in use (`Config::resources`) |

A refusal gives a `Reason`, as on TCP: `Operations`, `Queue`, `Resource`, or `Buffers`.

## Bind at a path

`Driver::bind_local` makes a Unix-domain stream socket, binds it to `path`, and listens on it. The
socket file has mode 0600 from the moment it exists: the driver sets the mode of the socket before
the bind, and Linux makes the file with that mode. So only the user that owns the file can connect.
The umask of the process can only remove more permissions. To let other users connect, change the
mode of the file yourself after the bind.

Where the kernel has the bind and listen operations of io_uring (Linux 6.11 and later), the ring
runs them. On an earlier kernel, the driver makes the system calls, and the future is ready at
once.

When a file is at the path, `stale` selects what the bind does:

| `Stale` | A live listener | A socket file with no listener | A socket file of another user | A file that is not a socket |
| --- | --- | --- | --- | --- |
| `Stale::Refuse` | `AddrInUse` | `AddrInUse` | `AddrInUse` | `AddrInUse` |
| `Stale::Replace` | `AddrInUse` | The bind removes the file and binds | `PermissionDenied` | `AddrInUse` |

With `Stale::Replace`, the driver connects to the socket file before it removes it. A refused
connect shows that no listener uses the file. A live listener gets this connection at its next
accept, and the connection has already ended. The driver removes no other file. These checks are
system calls before the bind.

## The identity of the peer

`Driver::local_peer(&stream)` gives the OS identity of the peer of a stream, from `SO_PEERCRED`:
`Peer::user()`, `Peer::group()`, and `Peer::process()`. For an accepted stream, it is the process
that connected. For a connected stream, it is the process that listened. The kernel recorded the
identity at the connect, so it does not change when the peer changes its user later.

Check the identity before you trust a request, for example after you change the mode of the
socket file so that other users can connect:

```rust
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;

use rsasync::driver::{Driver, LocalStream};

/// Whether the peer of `stream` runs as the user that owns the socket file at `path`.
fn peer_owns_socket(driver: &Driver, stream: &LocalStream, path: &Path) -> std::io::Result<bool> {
    Ok(driver.local_peer(stream)?.user() == std::fs::metadata(path)?.uid())
}
```

## Dropped, forgotten, and cancelled operations

The rules of [TCP on the driver](tcp.md#dropped-forgotten-and-cancelled-operations) apply with no
change:

- A dropped receive asks for a cancel. Bytes that it took go to the next receive, before it
  submits, in order.
- A dropped send still runs. A dropped send that fails or sends only part of its bytes fails the
  write direction, and the next send gives `WriteFailed` with the sent count.
- A dropped accept asks for a cancel. A connection that it took goes to the next accept.
- A dropped connect asks for a cancel. The driver closes a connection that completes after the
  drop, so its peer reads the end of the stream.
- An explicit cancel gives the buffer back with its exact progress.

## Close

`Driver::close_local` closes a stream as `Driver::close` closes a TCP stream: it cancels each
operation, waits until each one ends, releases the bytes that no receive took, and closes the
socket. `Driver::close_local_listener` cancels the accept that runs, closes each connection that no
accept took, and closes the socket.

The close of a listener does not remove its socket file, as in `std` and Tokio. A next bind at the
path then needs `Stale::Replace`, or remove the file yourself.
