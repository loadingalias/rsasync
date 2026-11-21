# UDP on the driver

This page is the reference for UDP on the io_uring driver, `rsasync::driver::Driver`: each
operation, its result, the batch report, the truncation rule, and what happens when you drop or
forget a future. [Drive the I/O driver yourself](driver-only.md) tells how to start, turn, and end
the driver. For datagram I/O on a socket of another runtime, with no driver, see
[Datagram I/O on any socket](datagram-io.md): both use the same metadata and the same control
messages. The same operations run on macOS, on the [kqueue backend](kqueue.md), which has no
segmentation offload, and on Windows, on the [IOCP backend](iocp.md#udp).

## Operations

Each operation gives an `Operation` future. Turn the driver until the future is ready.

| Call | Borrows | Output | Refusal |
| --- | --- | --- | --- |
| `Driver::bind_udp(address, options)` | Nothing | `io::Result<UdpSocket>`. See [Bind and options](#bind-and-options) | `Refused<()>` |
| `Driver::send_to(&mut socket, datagram)` | The write half | `(io::Result<usize>, Datagram)`: the datagrams that went, all or none | `Refused<Datagram>` |
| `Driver::send_batch(&mut socket, datagrams)` | The write half | `(io::Result<usize>, Datagrams)`: the datagrams that went, from the first | `Refused<Datagrams>` |
| `Driver::recv_from(&mut socket, backing)` | The read half | `(io::Result<Received>, Backing)`: the facts of the datagram, and the buffer with its bytes | `Refused<Backing>` |
| `Driver::recv_selected_from(&mut socket)` | The read half | `io::Result<(Received, Backing)>`: the facts, and the buffer that the kernel selected from the pool of `Driver::provide` | `Refused<()>`, also with `Reason::Buffers` when the driver has no pool |
| `Driver::recv_batch(&mut socket, arrivals)` | The read half | `(io::Result<usize>, Arrivals)`: the datagrams that came, each in a selected buffer with its facts | `Refused<Arrivals>`, also with `Reason::Buffers` when the driver has no pool |
| `Driver::close_udp(socket)` | Takes the socket | `Closed` | `Refused<UdpSocket>` |

`UdpSocket::split` gives the read half and the write half, so that a receive and a send can run at
one time. Each direction runs one operation at a time, as on a TCP stream.

`rsasync::datagram` is the `rsnetwork-udp` crate, whose types the UDP operations use: `Ecn`,
`Received`, `Metadata`, and `Refused`. `Received` gives `len`, the peer address `source`,
`truncated`, and `metadata` with the ECN codepoint, the local destination address, and the
segment size of a coalesced receive.

A datagram with its ECN codepoint, from one socket to another, with the `run` helper of
[Accept, receive, and send on TCP](driver-only.md#accept-receive-and-send-on-tcp):

<!-- The same text as the doctest of `rsasync::driver::UdpSocket`. Edit both. -->
```rust
use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::datagram::Ecn;
use rsasync::driver::{Config, Datagram, Driver, UdpOptions};

let mut driver = Driver::new(Config::new())?;
// The receiver reports the ECN codepoint and the local address of each datagram.
let options = UdpOptions { metadata: true, ..UdpOptions::default() };
let bind = driver.bind_udp("127.0.0.1:0".parse()?, options)?;
let mut receiver = run(&mut driver, bind)???;
let bind = driver.bind_udp("127.0.0.1:0".parse()?, UdpOptions::default())?;
let mut sender = run(&mut driver, bind)???;

let datagram = Datagram {
    ecn: Some(Ecn::Ect0),
    ..Datagram::new(Lease::from(b"hello".to_vec()), receiver.local_addr())
};
let send = driver.send_to(&mut sender, datagram)?;
let (sent, _datagram) = run(&mut driver, send)??;
assert_eq!(sent?, 1);

let receive = driver.recv_from(&mut receiver, Backing::from(Vec::with_capacity(1500)))?;
let (received, buffer) = run(&mut driver, receive)??;
let received = received?;
assert_eq!(buffer.bytes(), b"hello");
assert_eq!((received.truncated, received.metadata.ecn), (false, Some(Ecn::Ect0)));
```

## Bind and options

`Driver::bind_udp` makes a socket, sets `UdpOptions`, and binds it. The bind runs on the ring where
the kernel has the bind operation (Linux 6.11 and later), and as a system call elsewhere.

| Field | Effect |
| --- | --- |
| `only_v6`, `send_buffer`, `receive_buffer`, `reuse_port` | As in `TcpOptions`. Linux doubles the buffer sizes, and macOS and Windows keep them |
| `metadata` | Each receive reports the ECN codepoint and the local destination address |
| `dont_fragment` | A datagram above the path MTU is refused with `Refused::TooLarge`, which path MTU discovery needs |
| `gro` | One receive can give several datagrams of one peer, with their segment size. macOS has no coalesced receive, so there the bind gives `Unsupported` |

## Send a datagram

A `Datagram` owns its bytes (`contents`, a `Lease`) and its metadata: the `destination`, the
`ecn` codepoint, and the local `source` address. The kernel reads the bytes in place.

With a `segment_size`, the datagram is a batch of segments: the kernel or the device sends one
datagram for each segment, the last one can be shorter. The send is accepted or refused as a
whole, so the count is each segment or an error. A path that cannot segment gives
`Refused::NoSegmentation`, and none of the segments went: send them as separate datagrams with
`Driver::send_batch`, and use no segment size on that path again. macOS has no segmentation offload,
so there each send with a segment size gives `NoSegmentation`, and a batch ends before its first
datagram with a segment size ([the kqueue backend](kqueue.md#udp-has-no-segmentation-offload)).

## Batches

`Driver::send_batch` sends the datagrams of `Datagrams`, each with its own metadata, in one kernel
entry: one linked send for each, in order. A datagram that the kernel refuses ends the batch. The
report is the count of the datagrams that went, from the first; the datagrams after the refused one
did not go. `Datagrams::advance` then keeps the rest, and the next batch sends them. A rest never
joins another datagram.

`Driver::recv_batch` receives up to `Arrivals::capacity` datagrams in one kernel entry, each into a
buffer that the kernel selects: the first receive waits for a datagram, and the next ones take only
the datagrams that wait. So the wait holds no buffer. The report is the count of the datagrams,
and `Arrivals::drain` gives each one with its facts.

Make `Datagrams` and `Arrivals` one time and use them again, so that each batch allocates nothing.

A batch needs one entry of the submission queue for each datagram, at one time. A batch that does
not fit in the free entries refuses with `Reason::Queue` and gives its value back, with each
datagram that it held from an earlier batch. An accepted batch drops those datagrams before it
receives. A batch of more
datagrams than the queue has entries (`Config::submissions`, which the kernel rounds up to a power
of two) never fits, so it always refuses: use smaller batches.

## Receive and truncation

A receive gives one datagram, with its facts. If the datagram does not fit the buffer, the receive
gives the bytes that fit, and `truncated` is `true`: the kernel drops the rest of that datagram.
With the `gro` option, one receive can give several datagrams of one peer in one buffer, with
`metadata.segment_size`: each segment is one datagram, the last one can be shorter.

## Dropped, forgotten, and cancelled operations

| Operation | Its future drops or is forgotten |
| --- | --- |
| Receive | The driver asks the kernel to cancel. A datagram that the receive took before the cancel took effect stays with the socket, with its facts: the peer, the metadata, and its truncation result. The next receive gives it before it submits. `recv_from` copies it into its buffer, and if it does not fit, the rest drops and `truncated` is `true`; `Driver::copied` counts the copy. `recv_selected_from` and `recv_batch` give its buffer, with no copy. The datagrams of a dropped batch stay in order. An error of the receive also stays, and the next receive gives it. Close releases a datagram that no receive took |
| Send | The send is not cancelled. Its result is lost, as a datagram can be lost, and it fails no direction, because datagram boundaries are not at risk |

`Operation::cancel` keeps the interest: a receive gives the original buffer with the datagram that
came before the cancel took effect, with no copy, or `ECANCELED`.

## Close

`Driver::close_udp` cancels each operation that runs on the socket, waits until each one ends,
releases the datagrams that no receive took, and closes the socket.

A UDP socket has no half-close. `Driver::shutdown_write` on it refuses with `Reason::Unsupported`
before any submission, and the socket does not change.

## Use UDP from a task

A task of a [runtime](runtime.md) owner uses `rsasync::local::UdpSocket` (Linux), with no driver
argument, as [TCP from a task](tcp.md#use-tcp-from-a-task) does. `UdpSocket::bind(address,
options)`, `send_to`, `send_batch`, `recv_from`, `recv_batch`, and `close` are the driver calls
of this page. `socket.split()` gives a `rsasync::local::udp::ReadHalf` and a `WriteHalf`, so a
receive and a send run at one time. The socket is not `Send`, its refusals give back what you gave,
and a drop closes it. A receive into a buffer that the kernel selects is not a local call yet.

For one socket on each worker, see [Bind one socket on each worker](runtime.md#bind-one-socket-on-each-worker).
