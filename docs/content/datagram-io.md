# Datagram I/O on any socket

`rsnetwork-udp` sends and receives UDP datagrams on a socket that you own, with no wait, and with
no scheduler, driver, or runtime. It works on a socket of Tokio, of another runtime, or of your own
event loop. Each call is one system call. It is the datagram layer that the rsasync runtime and
the networking family share, and it takes the place of quinn-udp.

The crate is in development, and the calls on this page are not in a published release yet. It
supports Linux on x86_64 and aarch64, with glibc or musl, and macOS on aarch64. Windows comes
later; on other targets the crate has no item. The calls need the `std` feature, which is on by
default.

## Calls

Each call takes a `BorrowedFd` of a UDP socket and does not block: when the socket is not ready,
it gives an `io::Error` of the kind `WouldBlock`.

| Call | What it does |
| --- | --- |
| `send(socket, &transmit)` | Sends one datagram, or on Linux one batch of segments (see below). Gives the number of datagrams. The kernel sends all of them or none |
| `send_batch(socket, &transmits)` | Sends up to `BATCH` (32) transmits, each with its own metadata: in one call on Linux (`sendmmsg`), and with one `sendmsg` for each transmit on macOS. Gives how many the kernel sent, from the first. Send the rest again |
| `recv(socket, &mut buffer)` | Receives one datagram, or on Linux one coalesced receive, as a `Received` |
| `recv_batch(socket, &mut buffers, &mut received)` | Receives up to 32 datagrams, one into each buffer, and gives their number: in one call on Linux (`recvmmsg`), and with one `recvmsg` for each datagram on macOS |
| `configure(socket)` | Asks the kernel to report the ECN codepoint and the local address of each received datagram. On a dual-stack IPv6 socket, also for IPv4 datagrams |
| `set_gro(socket, on)` | Linux only. Turns coalesced receive (GRO) on or off |
| `set_dont_fragment(socket)` | Turns on the don't-fragment option, which path MTU discovery needs |

A batch stops at the first datagram that the kernel refuses. The datagrams before it go out, and
the count names only them. The refused datagram and the rest stay unsent, so the next call with
the rest gives the refusal.

## Send metadata

A `Transmit` is one send:

- `destination`: the peer, or `None` for the peer of a connected socket. A dual-stack IPv6 socket
  sends to an IPv4 peer at its IPv4-mapped address. On a connected socket, the socket fixes the
  peer of each datagram, so `None` names that peer, and the send gives the kernel no address. With
  `None`, the ECN codepoint goes in both the IPv4 and the IPv6 message, because the family of the
  peer is not known; each kernel skips the message of the other family.
- `contents`: the bytes.
- `segment_size`: `None` for one datagram. With `Some(size)` on Linux, the kernel or the device
  sends the bytes as one datagram for each `size` bytes (GSO); the last datagram can be shorter.
  Each datagram has the same metadata. macOS has no segmentation offload, so there each segment
  size gives `Refused::NoSegmentation`.
- `ecn`: the ECN codepoint (`Ect0`, `Ect1`, or `Ce`), or `None` for Not-ECT.
- `source`: the local source address, or `None` to let the route choose it.

## Receive metadata

A `Received` gives:

- `len`: the number of bytes in the buffer.
- `source`: the peer. A dual-stack IPv6 socket gives an IPv4 peer as its IPv4-mapped address.
- `truncated`: the datagram did not fit in the buffer, and the kernel dropped the rest of it.
- `metadata.segment_size`: with GRO on Linux, the size of each datagram of a coalesced receive; the
  last one can be shorter. `None` for one datagram, and always `None` on macOS.
- `metadata.ecn` and `metadata.destination`: the ECN codepoint and the local address that the
  datagram came to, after `configure`. On a dual-stack socket, an IPv4 datagram gives its IPv4
  address.

## Refusals

A send that the kernel refuses for one of these reasons gives an `io::Error` with a `Refused`
payload. Read it with `Refused::of(&error)`:

| Refusal | Cause | What to do |
| --- | --- | --- |
| `Refused::TooLarge` | The datagram is larger than the MTU, and the don't-fragment option is on (`EMSGSIZE`). On macOS, also a datagram that is larger than the send buffer of the socket, 9,216 bytes by default | Send smaller datagrams |
| `Refused::NoSegmentation` | The path cannot segment the batch. On Linux, the device cannot (`EIO` on a send with a segment size). macOS has no segmentation offload. No datagram of it was sent | Send the segments as separate datagrams, for example with `send_batch`, and use no segment size on this path again |

Each other failure is the `io::Error` of the system call. The crate never drops a datagram
silently: a send reports what the kernel accepted, and a receive reports truncation.

## Use it with Tokio

Call each function in `UdpSocket::try_io`, which clears the readiness of the socket when the call
gives `WouldBlock`. The example needs Tokio with its `net` and `rt` features. It sends three
datagrams in one batch, and receives them with their ECN codepoint. It runs on Linux and macOS:

```rust
use std::io;
use std::os::fd::AsFd;

use rsnetwork_udp::{Ecn, Transmit, configure, recv, send_batch};
use tokio::io::Interest;
use tokio::net::UdpSocket;

let runtime = tokio::runtime::Builder::new_current_thread().enable_io().build()?;
runtime.block_on(async {
    let sender = UdpSocket::bind("127.0.0.1:0").await?;
    let receiver = UdpSocket::bind("127.0.0.1:0").await?;
    // The receiver reports the ECN codepoint and the local address of each datagram.
    configure(receiver.as_fd())?;

    // One batch of three datagrams of 1000 bytes. A batch can send only its first datagrams:
    // send the rest again.
    let contents = [7; 1000];
    let transmits = [Transmit {
        destination: Some(receiver.local_addr()?),
        contents: &contents,
        segment_size: None,
        ecn: Some(Ecn::Ect0),
        source: None,
    }; 3];
    let mut sent = 0;
    while sent < transmits.len() {
        sender.writable().await?;
        match sender.try_io(Interest::WRITABLE, || send_batch(sender.as_fd(), &transmits[sent..])) {
            Ok(count) => sent += count,
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

The example is also the example of the crate documentation, which `cargo xtask test` runs.

The crate depends on no other crate: `cargo tree -p rsnetwork-udp -e normal,build` shows only
`rsnetwork-udp`. Tokio is only a development dependency of its tests, so a plain `cargo tree` in
this repository also lists it.

## macOS

macOS has no segmentation offload and no coalesced receive. Thus `send` refuses each segment size
with `Refused::NoSegmentation`, a received datagram has no segment size, and `set_gro` does not
exist. A batch is one `sendmsg` or `recvmsg` for each datagram.

The `macos-private-batch` feature makes `send_batch` and `recv_batch` use one system call for the
batch: `sendmsg_x` and `recvmsg_x`. These are private calls of macOS, so a release of macOS can
change them, and an app that uses private calls can fail App Store review. The feature is off by
default, and it has no effect on another target, so an iOS build never uses the calls. The calls
are not yet measured against one call for each datagram, so this page makes no speed claim for
them. The measurement program is `rsnetwork-udp/benches/macos_batch.rs`.

With the feature, these limits also apply:

- `recvmsg_x` does not report truncation. Thus a datagram that fills its buffer reports
  `truncated`, also when it fits exactly. Give each buffer more room than the largest datagram.
- In the published xnu source, a `sendmsg_x` that refuses a datagram after it sent others gives
  the count of the sent ones only when the refusal is `EMSGSIZE`, `ENOBUFS`, `EWOULDBLOCK`,
  `EINTR`, or `ERESTART`. For each other error it gives only the error, so a send of the batch
  again can repeat datagrams that went out.

## Declarations for other backends

`rsnetwork_udp::sys` declares the message headers (`MsgHdr` and `IoVec`, with `MmsgHdr` on Linux
and `MsgHdrX` on macOS), the packet information, and the constants of the calls and of the control
messages, each with the values and the layout of the target system. `Control` is the one
encoding of the control messages: `Control::encode` writes those of a `Transmit`, and
`Control::decode` reads those of a received datagram. They need only `core`, so a completion
backend, such as the io_uring driver of rsasync, builds its messages with them. The calls need the
`std` feature, which is on by default.

## Limits

- On Linux, the don't-fragment option refuses a datagram above the path MTU. An IPv4 path MTU on
  the loopback interface cannot be lowered without privilege, so the tests check the refusal on
  IPv6, where a socket can lower its own MTU.
- On macOS, the don't-fragment option refuses a datagram above the MTU of the interface, 16,384
  bytes on loopback. A datagram above a smaller path MTU goes out unfragmented, and the path can
  drop it. The tests check the refusal on IPv4 and IPv6.
- On a connected socket, send with no destination. macOS refuses a destination there
  (`EISCONN`).
- `Refused::NoSegmentation` from Linux comes from a device that cannot segment, for example one
  with no checksum offload. No test host has such a device, so that path has no run evidence yet.
