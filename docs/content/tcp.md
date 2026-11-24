# TCP on the driver

This page is the reference for TCP on the io_uring driver, `rsasync::driver::Driver`: each
operation, its result, and what happens when you drop, forget, or cancel its future.
[Drive the I/O driver yourself](driver-only.md) tells how to start, turn, and end the driver. The
same operations run on macOS, on the [kqueue backend](kqueue.md), and on Windows, on the
[IOCP backend](iocp.md). Each backend page states its differences.

## Operations

Each operation gives an `Operation` future. Turn the driver until the future is ready. The outer
`Result` of the output is `Err(Ended)` only if the driver ended first.

| Call | Borrows | Output | Refusal |
| --- | --- | --- | --- |
| `Driver::bind(address, options)` | Nothing | `io::Result<TcpListener>`: a nonblocking socket that listens. See [Bind and listen](#bind-and-listen) | `Refused<()>` |
| `Driver::connect(address, options)` | Nothing | `io::Result<TcpStream>`: a nonblocking stream to `address` | `Refused<()>` |
| `Driver::accept(&mut listener)` | The listener | `io::Result<(TcpStream, SocketAddr)>`: the stream and the address of its peer | `Refused<()>` |
| `Driver::recv(&mut stream, backing)` | The read half | `(io::Result<usize>, Backing)`: the count, and the buffer with the bytes after its earlier bytes | `Refused<Backing>`, with the buffer |
| `Driver::recv_selected(&mut stream)` | The read half | `io::Result<Backing>`: the buffer that the kernel selected from the pool of `Driver::provide`, with the bytes | `Refused<()>`, also with `Reason::Buffers` when the driver has no pool |
| `Driver::send(&mut stream, lease)` | The write half | `(io::Result<usize>, Lease)`: the count, and the lease | `Refused<Lease>`, with the lease |
| `Driver::send_vectored(&mut stream, regions)` | The write half | `(io::Result<usize>, Regions)`: the count across the regions, and the regions. See [Send several regions](#send-several-regions) | `Refused<Regions>`, with the regions |
| `Driver::shutdown_write(&mut stream)` | The write half | `io::Result<()>`. See [Half-close](#half-close) | `Refused<()>` |
| `Driver::close(stream)` | Takes the stream | `Closed` | `Refused<TcpStream>`, with the stream |
| `Driver::close_listener(listener)` | Takes the listener | `Closed` | `Refused<TcpListener>`, with the listener |
| `Operation::cancel(&self)` | Nothing | The future gives its output later, as usual | None |
| `Driver::set_options(&socket, options)` | Nothing | `io::Result<()>`, at once. See [Socket options](#socket-options) | None |
| `Driver::options(&socket)` | Nothing | `io::Result<TcpOptions>`, at once | None |

The future of `bind` and `connect` gives an error of the system, for example `ConnectionRefused`
when no listener has the port, or `OutOfMemory` when each resource slot of the driver is in use.

A refusal gives a `Reason`: `Operations` (each operation slot is in use), `Queue` (the submission
queue stays full), `Resource` (the driver does not own the socket), or `Buffers`.

`TcpStream::split` gives the read half and the write half, so that a receive and a send can run at
one time. A receive and a send take the stream or its half: `driver.recv(&mut stream, ...)` and
`driver.recv(reader, ...)` are the same operation.

## Bind and listen

`Driver::bind` makes a nonblocking socket, sets its options, binds it, and listens on it. The
listener reuses its address (`SO_REUSEADDR`), as `std` does. Its backlog is the most that the
kernel takes (`net.core.somaxconn`). The listener gives its local address with
`TcpListener::local_addr`, for example the port of a bind to port 0.

Where the kernel has the bind and listen operations of io_uring (Linux 6.11 and later), the ring
runs them, and the future is ready after the next turn. On an earlier kernel, the driver makes
the system calls, and the future is ready at once. The driver finds the operations with a probe
of the ring when it starts, not from the kernel version.

## Socket options

`TcpOptions` has one field for each option. `None` keeps the value of the system.
`Driver::bind` and `Driver::connect` set the options before the bind or the connect. Set
`reuse_port` and `only_v6` there, because the kernel uses them at the bind.
`Driver::set_options` sets the options of a stream or a listener later, and `Driver::options`
reads each one back, with the value of the kernel.

| Field | Option | Effect |
| --- | --- | --- |
| `no_delay` | `TCP_NODELAY` | Send small segments at once, with no Nagle delay |
| `keepalive` | `SO_KEEPALIVE` | Send keepalive probes on an idle connection |
| `only_v6` | `IPV6_V6ONLY` | An IPv6 socket takes no IPv4 traffic. `options` gives `None` for an IPv4 socket. Linux sets it on a socket that binds one IPv6 address, for example `::1` |
| `send_buffer` | `SO_SNDBUF` | The send buffer size in bytes |
| `receive_buffer` | `SO_RCVBUF` | The receive buffer size in bytes |
| `reuse_port` | `SO_REUSEPORT` | Sockets of one user can bind the same address, and the kernel spreads the connections between them |

Linux takes a buffer size of at most `net.core.wmem_max` (send) or `net.core.rmem_max`
(receive), and it doubles the value for its own bookkeeping. So `options` gives twice the value
that you set, when that value is not above the limit. Other kernels may not double the value; the
driver runs only on Linux now.

## Send several regions

`Driver::send_vectored` sends the bytes of several leases, in order, in one operation, with no copy
that joins them. `Regions` owns the leases and the array that names them for the kernel. Make it
one time and use it again, so that each send allocates nothing.

The count can end inside a region. `Regions::advance` then drops each region that went and makes
the region that went in part a view of its rest, with no copy, so the next send continues the
stream:

<!-- The same text as the doctest of `rsasync::driver::WriteFailed`. Edit both. -->
```rust
use rsasync::buffer::Lease;
use rsasync::driver::Regions;

let mut regions = Regions::with_capacity(3);
regions.push(Lease::from(b"HTTP/1.1 200 OK\r\n".to_vec()));
regions.push(Lease::from(b"content-length: 5\r\n\r\n".to_vec()));
regions.push(Lease::from(b"hello".to_vec()));
while !regions.is_empty() {
    let send = driver.send_vectored(&mut stream, regions)?;
    let (sent, back) = run(&mut driver, send)??;
    regions = back;
    regions.advance(sent?);
}
```

A vectored send is a send of the write direction: it waits for the sends before it, and the rules
for a dropped send apply to it.

## Half-close

`Driver::shutdown_write` ends the write side of the stream (`SHUT_WR`) after the sends before it.
The peer reads the bytes of those sends, and then the end of the stream. Receives on this side go
on, so you can read the reply of the peer. A dropped half-close still runs, as a send does.

On a failed write direction (see [A failed write direction](#a-failed-write-direction)), the
half-close gives `WriteFailed` and does nothing. So the peer never reads a clean end of the stream
after a message that was cut.

## One operation for each direction

The future of an operation borrows its direction exclusively: the read half, the write half, or
the listener. A half has no `Clone`. So when you start an operation, no future of an earlier
operation in that direction exists: you dropped it, or you forgot it with `mem::forget`.

A dropped or forgotten future ends your interest, not the operation. The direction stays busy until
the kernel ends the operation. An operation that you start before then waits, and the driver
starts it after that end, so the bytes stay in order. A receive or an accept that waited and whose
future dropped never ran, so it ends with no effect, and the next one takes its place. A send is
not cancelled: a send that waits and whose future dropped still runs. A send that you start after
it waits behind it, and the sends run in the order that you started them. Each waiting operation
holds its own operation slot, so the operation table (`Config::operations`) bounds the waiting
operations. A full table gives `Reason::Operations`, and no other refusal comes from this rule.

## Dropped, forgotten, and cancelled operations

No byte and no connection is lost. These rules apply:

| Operation | Its future drops or is forgotten | `Operation::cancel` |
| --- | --- | --- |
| Receive | The driver asks the kernel to cancel. The bytes that the receive got before the cancel took effect stay with the stream, in the receive's own buffer. The next receive gives them before it submits: `recv` copies as many as fit its buffer, and the stream keeps the rest; `recv_selected` gives that buffer. An error of the receive also stays, and the next receive gives it. Close releases the bytes that no receive took | The future gives the original buffer with the bytes that came before the cancel took effect, with no copy, or `ECANCELED` |
| Send, vectored send, and half-close | The operation is not cancelled. If a send sends all its bytes, it reports nothing. If it fails, sends only part of its bytes, or was cancelled, the write direction fails: see below | The future gives the lease or the regions with the exact count, or `ECANCELED`. The direction stays usable. On Windows, a cancel of a send that runs in the kernel aborts the connection: see [IOCP backend](iocp.md#differences-from-linux) |
| Accept | The driver asks the kernel to cancel. A connection that the accept took before the cancel took effect stays with the listener, and the next accept gives it, with its peer address, before it submits. Close of the listener closes it | The future gives the connection that the accept took before the cancel took effect, or `ECANCELED` |
| Connect | The driver asks the kernel to cancel. A connection that completes after the drop is closed at its end, so no descriptor stays open, and the peer reads the end of the stream | The future gives the stream that the connect made before the cancel took effect, or `ECANCELED` |
| Bind | The driver asks the kernel to cancel, and it closes the listener that the bind made | The future gives the listener that the bind made before the cancel took effect, or `ECANCELED` |

`Driver::copied()` gives the total of the bytes that receives copied from bytes that a dropped or
forgotten receive left. Each other byte of a receive goes from the kernel into its buffer with no
copy.

### A failed write direction

After a dropped send fails or sends only part of its bytes, you no longer know the framing of your
stream. The next send does not submit. It gives an `io::Error` whose payload is `WriteFailed`, with
`WriteFailed::sent()`: a `SendCount`, the count of the bytes that the dropped sends sent, from the
first byte of the first send whose result no future took. Each later send gives `WriteFailed` with
`SendCount::Settled`. Close the stream.

| `SendCount` | Meaning for you |
| --- | --- |
| `Settled` | No count is owed: each dropped send sent all its bytes, or an earlier result gave the count |
| `Sent(n)` | The dropped sends sent `n` bytes, and the peer can have read them |
| `Unknown` | The kernel gave no count, so you cannot know which bytes reached the peer. Only on [IOCP](iocp.md), after a cancel of a send that ran aborted the connection |

A next send that succeeds shows that each earlier dropped send sent all its bytes.

<!-- The same text as the doctest of `rsasync::driver::Regions`. Edit both. -->
```rust
use rsasync::buffer::Lease;
use rsasync::driver::WriteFailed;

let next = driver.send(&mut stream, Lease::from(b"next".to_vec()))?;
let (result, _lease) = run(&mut driver, next)??;
match result {
    Ok(_) => {} // Each earlier dropped send sent all its bytes.
    Err(error) => match WriteFailed::of(&error) {
        // `failed.sent()` gives the count of the bytes of the dropped sends. Close the stream.
        Some(failed) => eprintln!("{failed}"),
        None => return Err(error.into()),
    },
}
```

`run` is the helper of [Accept, receive, and send on TCP](driver-only.md#accept-receive-and-send-on-tcp).

## Close

`Driver::close` takes the stream, so no future of its operations exists. The close then:

1. Asks the kernel to cancel each operation that runs on the stream, a dropped send too, with one
   cancel request for each operation, by the operation's own identity. An operation that waits
   behind it never ran, so the close ends it at once.
2. Waits until the kernel ends each operation. It does not wait for the peer, so the close returns
   while the peer neither reads nor sends.
3. Releases the bytes that no receive took, and closes the socket.

Its future gives `Closed`. `Closed::sent()` gives the `SendCount` of the dropped sends:
`SendCount::Sent(n)` when a send that the close cancelled, or a failed send, owes that count, and
`SendCount::Unknown` when the close aborted a send on IOCP. It gives `SendCount::Settled` when no
send owes a count, also when a send already gave the count with `WriteFailed`.

`Driver::close_listener` cancels the accept that runs, waits until it ends, closes each connection
that no accept took, and closes the socket.

## Use TCP from a task

A task of a [runtime](runtime.md) owner uses `rsasync::local::TcpListener` and
`rsasync::local::TcpStream` (Linux). Each call takes no driver argument: it submits its operation
to the driver of the owner of its thread, and that driver is the home of the resource that it
makes. The operations, their outputs, the `Operation` future, and the rules of this page are the
same as on the driver.

| Local call | Driver call |
| --- | --- |
| `TcpListener::bind(address, options)` | `Driver::bind` |
| `listener.accept()` | `Driver::accept` |
| `TcpStream::connect(address, options)` | `Driver::connect` |
| `stream.recv(backing)`, `stream.send(lease)`, `stream.send_vectored(regions)` | `Driver::recv`, `Driver::send`, `Driver::send_vectored` |
| `stream.shutdown_write()` | `Driver::shutdown_write` |
| `options()`, `set_options(options)` | `Driver::options`, `Driver::set_options` |
| `stream.close()`, `listener.close()` | `Driver::close`, `Driver::close_listener` |

`stream.split()` gives a `rsasync::local::ReadHalf` and a `WriteHalf`, so a receive and a
send run at one time. A receive into a buffer that the kernel selects is not a local call yet: it
needs a pool on each worker.

```rust
use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::driver::TcpOptions;
use rsasync::local::{TcpListener, TcpStream};
use rsasync::runtime::Runtime;

let runtime = Runtime::builder().build()?;
runtime.block_on(async {
    let mut listener = TcpListener::bind("127.0.0.1:0".parse()?, TcpOptions::default())?.await??;
    let connect = TcpStream::connect(listener.local_addr(), TcpOptions::default())?;
    let (mut server, _peer) = listener.accept()?.await??;
    let mut client = connect.await??;

    let (sent, _lease) = client.send(Lease::from(b"ping".to_vec()))?.await?;
    assert_eq!(sent?, 4);
    let (received, buffer) = server.recv(Backing::from(Vec::with_capacity(64)))?.await?;
    assert_eq!(&buffer.bytes()[..received?], b"ping");

    server.close()?.await?;
    client.close()?.await?;
    Ok::<(), Box<dyn std::error::Error>>(())
})??;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The local types differ from the driver types in two points:

- They are not `Send`, so a resource stays on the thread of its home. The future of a bind, a
  connect, or an accept is not `Send` either, so the resource starts on that thread. A local call
  on another owner refuses with `Reason::Resource`, also after the `block_on` that made the
  resource ended. A local call on a thread with no driver, or in a destructor that a turn of the
  driver runs, refuses with `Reason::NoDriver`. Each refusal gives back what you gave, such as the
  buffer. An option call gives an `io::Result` instead: with no driver, or on another owner, it
  gives an error of the kind `NotFound`.
- A drop of a listener or a stream closes it, as `close` does, but with no future. The drop needs
  no free operation slot. The driver ends each operation of the resource and closes its socket.
  When the drop cannot use the driver at once, the next turn of the driver starts the close. For
  example, the drop runs in a destructor that a turn of the driver runs. A drop after the end of
  the `block_on` of the resource does nothing, because that `block_on` closed the socket.

For one listener on each worker, see [Bind one socket on each worker](runtime.md#bind-one-socket-on-each-worker).

## Use TCP from a movable task

A [movable task](runtime.md#spawn-movable-work) uses `rsasync::movable::TcpListener` and
`rsasync::movable::TcpStream` (Linux, on io_uring). They are `Send`, so the task keeps them on each
worker that polls it. A movable resource has no home. It owns its socket, and only its operations
have a home:

- A call submits its operation to the driver of the worker that polls the task at that time. That
  driver is the home of the operation until its terminal event.
- When an idle worker steals the task, an operation that runs stays at its home, which stays
  driven. Its future gives the result on the worker that polls the task then.
- The next operation goes to the driver of that worker, with no message to another thread.
- A cancel, a drop, or a close sends a request to the home of each operation that runs. Only that
  home cancels the operation in its ring.

| Movable call | Same meaning as |
| --- | --- |
| `TcpListener::bind(address, options)`, `listener.local_addr()` | `Driver::bind`, `TcpListener::local_addr` |
| `listener.accept()` | `Driver::accept` |
| `TcpStream::connect(address, options)` | `Driver::connect` |
| `stream.recv(backing)`, `stream.send(lease)` | `Driver::recv`, `Driver::send` |
| `stream.close()`, `listener.close()` | `Driver::close`, `Driver::close_listener` |

Each call gives a `rsasync::movable::Operation` future, with `cancel()` and the same outputs as
on the driver. The halves of a stream, a vectored send, a half-close, the option calls, and the
other kinds of resource are not movable calls yet.

```rust
use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::driver::TcpOptions;
use rsasync::movable::{TcpListener, TcpStream};
use rsasync::runtime::Runtime;
use rsasync::task::{self, Outcome};

let runtime = Runtime::builder().movable_workers(2).build()?;
runtime.block_on(async {
    let mut listener = TcpListener::bind("127.0.0.1:0".parse()?, TcpOptions::default())?.await??;
    let address = listener.local_addr();
    // The listener and its stream go with the task to each worker that polls it.
    let server = task::spawn_movable(async move {
        let (mut stream, _peer) = listener.accept().ok()?.await.ok()?.ok()?;
        let (received, buffer) = stream.recv(Backing::from(Vec::with_capacity(64))).ok()?.await.ok()?;
        let (sent, _lease) = stream.send(Lease::from(buffer)).ok()?.await.ok()?;
        stream.close().ok()?.await.ok()?;
        Some((received.ok()?, sent.ok()?))
    });

    let mut client = TcpStream::connect(address, TcpOptions::default())?.await??;
    let (sent, _lease) = client.send(Lease::from(b"ping".to_vec()))?.await?;
    assert_eq!(sent?, 4);
    let (received, buffer) = client.recv(Backing::from(Vec::with_capacity(64)))?.await?;
    assert_eq!(&buffer.bytes()[..received?], b"ping");
    client.close()?.await?;

    let Ok(server) = server else {
        return Err("the runtime has no movable worker".into());
    };
    assert!(matches!(server.join().await, Outcome::Completed(Some((4, 4)))));
    Ok::<(), Box<dyn std::error::Error>>(())
})??;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The rules of this page apply, with these differences from the local types:

- Each direction still runs one operation at a time, and a dropped receive leaves its bytes to
  the next receive. The stream keeps a dropped operation, not a driver. An operation that starts
  while a dropped one of its direction still runs gets its slot at once, at the driver of its own
  worker. That driver waits for the end of the dropped operation, takes what it left, and then
  starts the new one. So an accepted operation never refuses later, a dropped send that waits
  still runs, and the bytes of the stream keep their order when the two operations have different
  homes.
- A cancel of an operation that still waits for a dropped one ends it at once, as on the driver.
- A call never refuses with `Reason::Resource`, because no driver owns the resource. A call on a
  thread with no driver refuses with `Reason::NoDriver`. A call when the allocator gives no
  memory for the wait refuses with `Reason::Allocation`.
- A call on a worker that runs on [epoll](epoll.md) refuses with `Reason::Unsupported`. Only
  io_uring takes an operation for a descriptor that its driver does not own.
- A drop of a listener or a stream cancels each of its operations, a dropped send too. It needs
  no driver, so it also works after the task moved or on a thread with no driver. The socket
  closes when each operation ended.
- The memory of a movable resource has no charge to the budget of the runtime yet.

A completion waits while the worker of its home is in a poll, also when other workers are idle:
only the worker that owns a ring takes its completions. Keep each poll short, or give long work
to the blocking service.
