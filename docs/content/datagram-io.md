# Datagram I/O on any socket

`rsnetwork-udp` sends and receives UDP datagrams on a socket that you own, with no wait, and with
no scheduler, driver, or runtime. It works on a socket of Tokio, of another runtime, or of your own
event loop. Each call is one system call. It is the datagram layer that the rsasync runtime and
the networking family share, and it takes the place of quinn-udp.

The crate is in development, and the calls on this page are not in a published release yet. It
supports Linux on x86_64 and aarch64, with glibc or musl. macOS and Windows come later; on other
targets the crate has no item. The calls need the `std` feature, which is on by default.

## Calls

Each call takes a `BorrowedFd` of a UDP socket and does not block: when the socket is not ready,
it gives an `io::Error` of the kind `WouldBlock`.

| Call | What it does |
| --- | --- |
| `send(socket, &transmit)` | Sends one datagram, or one batch of segments (see below). Gives the number of datagrams. The kernel sends all of them or none |
| `send_batch(socket, &transmits)` | Sends up to `BATCH` (32) transmits in one call (`sendmmsg`), each with its own metadata. Gives how many the kernel sent, from the first. Send the rest again |
| `recv(socket, &mut buffer)` | Receives one datagram, or one coalesced receive, as a `Received` |
| `recv_batch(socket, &mut buffers, &mut received)` | Receives up to 32 datagrams in one call (`recvmmsg`), one into each buffer, and gives their number |
| `configure(socket)` | Asks the kernel to report the ECN codepoint and the local address of each received datagram. On a dual-stack IPv6 socket, also for IPv4 datagrams |
| `set_gro(socket, on)` | Turns coalesced receive (GRO) on or off |
| `set_dont_fragment(socket)` | Turns on the don't-fragment option, which path MTU discovery needs |

## Send metadata

A `Transmit` is one send:

- `destination`: the peer. A dual-stack IPv6 socket sends to an IPv4 peer at its IPv4-mapped
  address.
- `contents`: the bytes.
- `segment_size`: `None` for one datagram. With `Some(size)`, the kernel or the device sends the
  bytes as one datagram for each `size` bytes (GSO); the last datagram can be shorter. Each
  datagram has the same metadata.
- `ecn`: the ECN codepoint (`Ect0`, `Ect1`, or `Ce`), or `None` for Not-ECT.
- `source`: the local source address, or `None` to let the route choose it.

## Receive metadata

A `Received` gives:

- `len`: the number of bytes in the buffer.
- `source`: the peer. A dual-stack IPv6 socket gives an IPv4 peer as its IPv4-mapped address.
- `truncated`: the datagram did not fit in the buffer, and the kernel dropped the rest of it.
- `metadata.segment_size`: with GRO, the size of each datagram of a coalesced receive; the last one
  can be shorter. `None` for one datagram.
- `metadata.ecn` and `metadata.destination`: the ECN codepoint and the local address that the
  datagram came to, after `configure`. On a dual-stack socket, an IPv4 datagram gives its IPv4
  address.

## Refusals

A send that the kernel refuses for one of these reasons gives an `io::Error` with a `Refused`
payload. Read it with `Refused::of(&error)`:

| Refusal | Cause | What to do |
| --- | --- | --- |
| `Refused::TooLarge` | The datagram is larger than the path MTU, and the don't-fragment option is on (`EMSGSIZE`) | Send smaller datagrams |
| `Refused::NoSegmentation` | The device cannot segment the batch (`EIO` on a send with a segment size). The kernel sent none of it | Send the segments as separate datagrams, for example with `send_batch`, and use no segment size on this path again |

Each other failure is the `io::Error` of the system call. The crate never drops a datagram
silently: a send reports what the kernel accepted, and a receive reports truncation.

## Use it with Tokio

Call each function in `UdpSocket::try_io`, which clears the readiness of the socket when the call
gives `WouldBlock`. The example needs Tokio with its `net` and `rt` features. It sends three
segments in one call, and receives them as three datagrams with their ECN codepoint:

```rust
use std::io;
use std::os::fd::AsFd;

use rsnetwork_udp::{Ecn, Transmit, configure, recv, send};
use tokio::io::Interest;
use tokio::net::UdpSocket;

let runtime = tokio::runtime::Builder::new_current_thread().enable_io().build()?;
runtime.block_on(async {
    let sender = UdpSocket::bind("127.0.0.1:0").await?;
    let receiver = UdpSocket::bind("127.0.0.1:0").await?;
    // The receiver reports the ECN codepoint and the local address of each datagram.
    configure(receiver.as_fd())?;

    // One send of three segments of 1000 bytes: three datagrams.
    let contents = vec![7; 3000];
    let transmit = Transmit {
        destination: receiver.local_addr()?,
        contents: &contents,
        segment_size: Some(1000),
        ecn: Some(Ecn::Ect0),
        source: None,
    };
    loop {
        sender.writable().await?;
        match sender.try_io(Interest::WRITABLE, || send(sender.as_fd(), &transmit)) {
            Ok(datagrams) => break assert_eq!(datagrams, 3),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }

    let mut buffer = [0; 1500];
    for _ in 0..3 {
        let received = loop {
            receiver.readable().await?;
            match receiver.try_io(Interest::READABLE, || recv(receiver.as_fd(), &mut buffer)) {
                Ok(received) => break received,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error),
            }
        };
        assert_eq!((received.len, received.metadata.ecn), (1000, Some(Ecn::Ect0)));
    }
    Ok(())
})
```

The crate depends on no other crate: `cargo tree -p rsnetwork-udp -e normal,build` shows only
`rsnetwork-udp`. Tokio is only a development dependency of its tests, so a plain `cargo tree` in
this repository also lists it.

## Declarations for other backends

`rsnetwork_udp::sys` declares the message headers (`MsgHdr`, `MmsgHdr`, `IoVec`), the packet
information, and the constants of the calls and of the control messages. `Control` is the one
encoding of the control messages: `Control::encode` writes those of a `Transmit`, and
`Control::decode` reads those of a received datagram. They need only `core`, so a completion
backend, such as the io_uring driver of rsasync, builds its messages with them. The calls need the
`std` feature, which is on by default.

## Limits

- An IPv4 path MTU on the loopback interface cannot be lowered without privilege. The tests check
  the don't-fragment refusal on IPv6, where a socket can lower its own MTU.
- `Refused::NoSegmentation` comes from a device that cannot segment, for example one with no
  checksum offload. No test host has such a device, so that path has no run evidence yet.
