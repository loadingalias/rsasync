# Local streams on the driver

This page is the reference for local streams on the driver, `rsasync::driver::Driver`:
Unix-domain stream sockets at a path, for processes on one host. Local streams run on Linux, on
io_uring and on [epoll](epoll.md), on macOS, on [kqueue](kqueue.md), and on Windows as byte-mode
named pipes, on the [I/O completion port](iocp.md) (see [Windows](#windows)). This page gives each
operation, its result, the permission rule of the socket file, the lock of the path, and the
identity of the peer. A local stream follows the stream rules of
[TCP on the driver](tcp.md): one operation for each direction, the rules for a dropped, forgotten,
or cancelled operation, and close. [Drive the I/O driver yourself](driver-only.md) tells how to
start, turn, and end the driver.

## Operations

Each operation gives an `Operation` future. Turn the driver until the future is ready. The outer
`Result` of the output is `Err(Ended)` only if the driver ended first.

| Call | Borrows | Output | Refusal |
| --- | --- | --- | --- |
| `Driver::bind_local(path, stale)` | Nothing | `io::Result<LocalListener>`, at once: a listener at the socket file `path`. See [Bind at a path](#bind-at-a-path) | `Refused<()>` |
| `Driver::connect_local(path)` | Nothing | `io::Result<LocalStream>`, at once: a stream to the listener at `path` | `Refused<()>` |
| `Driver::accept_local(&mut listener)` | The listener | `io::Result<LocalStream>`: the stream. See [The identity of the peer](#the-identity-of-the-peer) | `Refused<()>` |
| `Driver::recv`, `Driver::recv_selected`, `Driver::send`, `Driver::send_vectored`, `Driver::shutdown_write` | The read half or the write half | As for a TCP stream: see [TCP on the driver](tcp.md#operations) | As for a TCP stream |
| `Driver::local_peer(&stream)` | Nothing | `io::Result<Peer>`, at once | None |
| `Driver::close_local(stream)` | Takes the stream | `Closed` | `Refused<LocalStream>`, with the stream |
| `Driver::close_local_listener(listener)` | Takes the listener | `Closed` | `Refused<LocalListener>`, with the listener |

`LocalStream::split` gives the read half and the write half, so that a receive and a send can run
at one time. A half-close ends the receive of the peer after the bytes that went before it, as on
TCP.

The bind and the connect are system calls on the thread of the driver, on each backend, so their
futures are ready at once. A Unix-domain connect does not wait in the kernel: it connects, or it
fails at once. The future of `bind_local` and `connect_local` gives these errors:

| Error kind | Cause |
| --- | --- |
| `InvalidInput` | The path is empty, holds a NUL byte, or has more than 107 bytes on Linux or 103 bytes on macOS. The driver makes no socket. An abstract name, which has no file, is not available |
| `AddrInUse` | A bind found a file at the path, or a listener holds the lock of the path. See [Bind at a path](#bind-at-a-path) |
| `PermissionDenied` | A connect to a socket file that the user of the process cannot write, for example one of another user. A bind with a lock file of another user, or with `Stale::Replace` on a socket file of another user |
| `NotFound` | A connect to a path with no file |
| `ConnectionRefused` | A connect to a socket file with no listener, for example the file of a closed listener |
| `ResourceBusy` | A connect to a live listener whose queue of connections that wait for an accept is full. Connect again later. See [A busy listener](#a-busy-listener) |
| `OutOfMemory` | Each resource slot of the driver is in use (`Config::resources`). On Windows, also when the allocator refuses the state of a listener |

Windows gives other causes for some of these kinds: see [Windows](#windows).

A refusal gives a `Reason`, as on TCP: `Operations`, `Queue`, `Resource`, or `Buffers`.

## Bind at a path

`Driver::bind_local` makes a Unix-domain stream socket, binds it to `path`, and listens on it. Only
the user that owns the socket file can connect, because the file has mode 0600 before a connect can
succeed:

- On Linux, the driver sets the mode of the socket before the bind, and Linux makes the file with
  that mode.
- On macOS, the system makes the file with the mode 0777 less the umask, and it refuses a mode
  change of a socket. So the driver changes the mode of the file after the bind and before the
  listen. Until the listen, each connect gets `ConnectionRefused`.

The umask of the process can only remove more permissions. To let other users connect, change the
mode of the file yourself after the bind. Keep the directory of the path closed to writes of other
users: a user who can write the directory can remove or replace the socket file.

### The lock of the path

The bind takes a lock on the lock file of the path: the path with `.lock` after it, for example
`service.sock.lock` beside `service.sock`. The bind makes the lock file with mode 0600, and it takes
the lock with `flock` before it looks at the path. The listener holds the lock until its socket
closes: at its close, at the teardown of the driver, or at the end of its process. While a listener
holds the lock, each bind of the path gives `AddrInUse`, also after someone removed the socket
file.

The close keeps the lock file, as it keeps the socket file. A removal at the close would let two
binds lock two different files of one path: a bind that opened the old file before the removal,
and a bind that made a new file after it. Then each one could remove the socket file of the other.
A lock file that stays has one identity, so one lock decides.

### A path that exists

When a file is at the path and no listener holds the lock, `stale` selects what the bind does:

| `Stale` | A live listener of another program | A socket file with no listener | A socket file of another user | A file that is not a socket |
| --- | --- | --- | --- | --- |
| `Stale::Refuse` | `AddrInUse` | `AddrInUse` | `AddrInUse` | `AddrInUse` |
| `Stale::Replace` | `AddrInUse` | The bind removes the file and binds | `PermissionDenied` | `AddrInUse` |

A listener of another program, for example a `std` `UnixListener`, holds no lock. So with
`Stale::Replace`, the driver also connects to the socket file before it removes it. A refused
connect shows that no listener uses the file. A live listener gets this connection at its next
accept, and the connection has already ended. The driver removes no other file. These checks are
system calls before the bind.

Limit: on macOS, a listener whose queue is full refuses a connect, as a socket file with no
listener does. So `Stale::Replace` removes the socket file of a busy listener of another program,
which holds no lock. A listener of the driver holds the lock, so this limit does not apply to it.

### A busy listener

A live listener takes a limited number of connections that wait for its accept. When that queue is
full, a connect gives `ResourceBusy`, and a later connect can succeed. Linux gives `EAGAIN` for
such a connect. macOS refuses it with `ECONNREFUSED`, as for a socket file with no listener, so the
driver then reads the lock of the path: while a listener holds it, the connect gives
`ResourceBusy`, and else `ConnectionRefused`. The read takes no lock, so a bind at the same time
does not fail.

## The identity of the peer

`Driver::local_peer(&stream)` gives the OS identity of the peer of a stream: `Peer::user()`,
`Peer::group()`, and `Peer::process()`, which is an `Option<u32>`. For an accepted stream, it is the
process that connected. For a connected stream, it is the process that listened.

| System | Source | User and group | Process |
| --- | --- | --- | --- |
| Linux | `SO_PEERCRED` | Recorded at the connect | Recorded at the connect. `None` for a peer in a process namespace that this process cannot see |
| macOS | `getpeereid` and `LOCAL_PEERPID` | Recorded at the connect | The last process that used the socket of the peer. `None` after the peer closed its socket, also before the accept |

The user and the group do not change when the peer changes its user later, and they stay after the
peer closes its socket. On macOS, the process changes when the peer gives its socket to another
process, for example to a child after a fork, and it can name a process that ended. So use the user
and the group to authorize a peer, and use the process only as information.

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
- A connect is ready at once. A drop of its future before it gives the stream closes the stream,
  so its peer reads the end of the stream.
- An explicit cancel gives the buffer back with its exact progress.

## Close

`Driver::close_local` closes a stream as `Driver::close` closes a TCP stream: it cancels each
operation, waits until each one ends, releases the bytes that no receive took, and closes the
socket. `Driver::close_local_listener` cancels the accept that runs, closes each connection that no
accept took, and closes the socket.

The close of a listener does not remove its socket file, as in `std` and Tokio, and it keeps the
lock file (see [The lock of the path](#the-lock-of-the-path)). A next bind at the path then needs
`Stale::Replace`, or remove the socket file yourself.

## Backends

| Backend | Bind and connect | Accept, receive, send, half-close, and close |
| --- | --- | --- |
| io_uring (Linux) | System calls on the thread of the driver | Operations of the ring, as for TCP |
| [epoll](epoll.md) (Linux) | System calls on the thread of the driver | One nonblocking call when epoll reports the socket ready, as for TCP |
| [kqueue](kqueue.md) (macOS) | System calls on the thread of the driver | One nonblocking call when kqueue reports the socket ready, as for TCP |

| [IOCP](iocp.md) (Windows) | `CreateNamedPipeW` and `CreateFileW` on the thread of the driver | Overlapped `ConnectNamedPipe`, `ReadFile`, and `WriteFile` on the port. No half-close |

The results and the errors are the same on each backend, but for the differences of the systems
that this page states: the path length, the source of the identity of the peer, the busy
listener of another program on macOS, and the named pipes of Windows.

## Windows

On Windows, a local stream is a byte-mode named pipe. The path of `bind_local` and `connect_local`
is a pipe name, `\\.\pipe\<name>`, with at most 256 units in all. Each other path gives
`InvalidInput`. A pipe name has no file, and the name ends when the last instance of the pipe
closes.

The bind makes the first instance of the pipe:

- `FILE_FLAG_FIRST_PIPE_INSTANCE`: the bind gives `AddrInUse` while an instance of the name
  exists, also an instance that another process made first. So no process can take the clients of
  the listener by a pipe of the same name that it makes before the bind. No name is stale, so
  `Stale::Replace` binds as `Stale::Refuse`.
- `PIPE_REJECT_REMOTE_CLIENTS`: a client on another computer cannot connect.
- The security descriptor `O:<user>D:P(A;;GA;;;<user>)`: only the user of the process of the
  listener can open the pipe, and an open of another user gives `PermissionDenied`.

The listener holds 64 instances that wait for clients, as the queue of a listening socket holds
connections: a burst of 64 clients connects before an accept. A client takes the first waiting
instance, in the order in which the listener made them. An accept is one overlapped
`ConnectNamedPipe` on the first instance. When it has its client, the stream takes that instance,
and the listener makes a new instance after the others. A client that connects while each of the
64 instances has a client that no accept took gets `ResourceBusy`, the Windows form of a full
backlog: connect again later. A dropped accept leaves its client in the first instance, and the
next accept gives it at once. Each waiting instance is one kernel object and one handle on the
port. The 64 KiB buffer of each direction is a quota that only bytes in the pipe use, and an
instance takes no resource slot of the driver before its accept.

`connect_local` opens the pipe with `SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION`: the server
can read the identity of the client, and cannot act as the client. A connect to a pipe name with no
instance gives `NotFound`.

A named pipe has no half-close: `shutdown_write` refuses with `Reason::Unsupported`, and the close
of one end is the end of the stream for the other end, after each byte that went before it.
`recv_selected` on a named pipe gives `Unsupported`.

`Driver::local_peer` gives the identity of the peer, as on Linux and macOS: of the client for an
accepted stream, and of the server for a connected stream.

| Stream | `Peer::process()` | `Peer::user()` |
| --- | --- | --- |
| Accepted | `GetNamedPipeClientProcessId`: the process that connected, which the system recorded at the connect | The SID of the user of the token of the client. The thread of the driver impersonates the client, at the identification level, reads its token, and goes back to its own identity |
| Connected | `GetNamedPipeServerProcessId`: the process that made the pipe, which the system recorded at the bind | The owner of the pipe (`GetSecurityInfo`), which its server set. A process with no privilege to restore files or to take ownership cannot set another user as an owner, and a listener of the driver sets the user of its process |

`Peer::process()` is always `Some` on Windows. `Peer::user()` is a `Sid`, whose `Display` form is
the string form, for example `S-1-5-21-…-500`.

Windows has the user of a client only when it has the security context of the client. The driver
reads the user one time and keeps it for the life of the stream, as Unix records it at the
connect, so a later write of the client under another identity does not change it:

- A client of the driver opens the pipe with static tracking, so the system takes its context at
  the open, and the driver reads its user at the accept.
- A client that opens the pipe with no security quality of service, the default of `CreateFileW`,
  of `std::fs`, and of .NET, has dynamic tracking: the system takes its context at its first write.
  The driver reads its user at the first receive that takes its data. Until then, `local_peer`
  gives an `io::Error` whose `raw_os_error()` is `ERROR_CANNOT_IMPERSONATE` (1368). Receive the
  first bytes, then check the user, before you act on them.

The driver never reads the user from the process of the client, because the system can give its
process ID to another process after the client ends. Windows has no group in the peer. A client checks
the user of its server before it sends a secret, so it does not talk to a process of another user
that took the name. A listener authorizes a client by its user, which the security descriptor
already limits to the user of the listener.
