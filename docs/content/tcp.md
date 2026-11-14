# TCP on the driver

This page is the reference for TCP on the io_uring driver, `rsasync::driver::Driver`: each
operation, its result, and what happens when you drop, forget, or cancel its future.
[Drive the I/O driver yourself](driver-only.md) tells how to start, turn, and end the driver.

## Operations

Each operation gives an `Operation` future. Turn the driver until the future is ready. The outer
`Result` of the output is `Err(Ended)` only if the driver ended first.

| Call | Borrows | Output | Refusal |
| --- | --- | --- | --- |
| `Driver::bind(address)` | Nothing | `io::Result<TcpListener>`, a nonblocking socket that listens | An error of the system, or `OutOfMemory` when each resource slot is in use |
| `Driver::accept(&mut listener)` | The listener | `io::Result<(TcpStream, SocketAddr)>`: the stream and the address of its peer | `Refused<()>` |
| `Driver::recv(&mut stream, backing)` | The read half | `(io::Result<usize>, Backing)`: the count, and the buffer with the bytes after its earlier bytes | `Refused<Backing>`, with the buffer |
| `Driver::recv_selected(&mut stream)` | The read half | `io::Result<Backing>`: the buffer that the kernel selected from the pool of `Driver::provide`, with the bytes | `Refused<()>`, also with `Reason::Buffers` when the driver has no pool |
| `Driver::send(&mut stream, lease)` | The write half | `(io::Result<usize>, Lease)`: the count, and the lease | `Refused<Lease>`, with the lease |
| `Driver::close(stream)` | Takes the stream | `Closed` | `Refused<TcpStream>`, with the stream |
| `Driver::close_listener(listener)` | Takes the listener | `Closed` | `Refused<TcpListener>`, with the listener |
| `Operation::cancel(&self)` | Nothing | The future gives its output later, as usual | None |

A refusal gives a `Reason`: `Operations` (each operation slot is in use), `Queue` (the submission
queue stays full), `Resource` (the driver does not own the socket), or `Buffers`.

`TcpStream::split` gives the read half and the write half, so that a receive and a send can run at
one time. A receive and a send take the stream or its half: `driver.recv(&mut stream, ...)` and
`driver.recv(reader, ...)` are the same operation.

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
| Send | The send is not cancelled. If it sends all its bytes, it reports nothing. If it fails, sends only part of its bytes, or was cancelled, the write direction fails: see below | The future gives the lease with its exact count, or `ECANCELED`. The direction stays usable |
| Accept | The driver asks the kernel to cancel. A connection that the accept took before the cancel took effect stays with the listener, and the next accept gives it, with its peer address, before it submits. Close of the listener closes it | The future gives the connection that the accept took before the cancel took effect, or `ECANCELED` |

`Driver::copied()` gives the total of the bytes that receives copied from bytes that a dropped or
forgotten receive left. Each other byte of a receive goes from the kernel into its buffer with no
copy.

### A failed write direction

After a dropped send fails or sends only part of its bytes, you no longer know the framing of your
stream. The next send does not submit. It gives an `io::Error` whose payload is `WriteFailed`, with
`WriteFailed::sent()`: the bytes that the dropped sends sent, from the first byte of the first send
whose result no future took. Each later send gives `WriteFailed` with `sent()` of `None`. Close the
stream.

A next send that succeeds shows that each earlier dropped send sent all its bytes.

```rust
use rsasync::buffer::Lease;
use rsasync::driver::WriteFailed;

let next = driver.send(&mut stream, Lease::from(b"next".to_vec()))?;
let (result, _lease) = run(&mut driver, next)??;
match result {
    Ok(_) => {} // Each earlier dropped send sent all its bytes.
    Err(error) => match WriteFailed::of(&error) {
        // `failed.sent()` bytes of the dropped sends went to the peer. Close the stream.
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

Its future gives `Closed`. `Closed::sent()` gives the bytes that the dropped sends sent, from the
first byte of the first send whose result no future took, when a send that the close cancelled, or
a failed send, owes that count. It gives `None` when no send owes it, also when a send already gave
the count with `WriteFailed`.

`Driver::close_listener` cancels the accept that runs, waits until it ends, closes each connection
that no accept took, and closes the socket.
