//! Datagram I/O on any UDP socket that the caller owns, and the system declarations that the
//! networking family and its runtimes share, so each declaration exists once.
//!
//! The calls send and receive with no wait, on a socket of any runtime or of no runtime, for
//! example a Tokio socket through `UdpSocket::try_io`. Each call is one system call:
//!
//! - `send` sends one datagram, or a batch of segments that the kernel or the device sends as one
//!   datagram for each segment (GSO). `send_batch` sends several in one call (`sendmmsg`).
//! - `recv` receives one datagram, or with `set_gro` several coalesced ones with their segment
//!   size (GRO). `recv_batch` receives several in one call (`recvmmsg`).
//! - A send can set the ECN codepoint and the source address, and a receive reports them, after
//!   `configure`. A receive reports truncation. `set_dont_fragment` turns on the
//!   don't-fragment option that path MTU discovery needs.
//!
//! The declarations of the calls, their message headers, and their control messages are in
//! `sys`, and `Control` is the one encoding of the control messages, which a completion
//! backend also uses. They need only `core`. The calls need the `std` feature, which is on by
//! default.
//!
//! Each item agrees with the Linux 6.1 headers that its module names, on x86_64 and aarch64.
//! Other targets get no item yet.
//!
//! With Tokio, each call runs in `UdpSocket::try_io`, which clears the readiness of the socket
//! when the call gives `WouldBlock`:
//!
//! ```
//! # #[cfg(target_os = "linux")]
//! # fn main() -> std::io::Result<()> {
//! use std::io;
//! use std::os::fd::AsFd;
//!
//! use rsnetwork_udp::{Ecn, Transmit, configure, recv, send};
//! use tokio::io::Interest;
//! use tokio::net::UdpSocket;
//!
//! let runtime = tokio::runtime::Builder::new_current_thread().enable_io().build()?;
//! runtime.block_on(async {
//!     let sender = UdpSocket::bind("127.0.0.1:0").await?;
//!     let receiver = UdpSocket::bind("127.0.0.1:0").await?;
//!     // The receiver reports the ECN codepoint and the local address of each datagram.
//!     configure(receiver.as_fd())?;
//!
//!     // One send of three segments of 1000 bytes: three datagrams.
//!     let contents = vec![7; 3000];
//!     let transmit = Transmit {
//!         destination: receiver.local_addr()?,
//!         contents: &contents,
//!         segment_size: Some(1000),
//!         ecn: Some(Ecn::Ect0),
//!         source: None,
//!     };
//!     loop {
//!         sender.writable().await?;
//!         match sender.try_io(Interest::WRITABLE, || send(sender.as_fd(), &transmit)) {
//!             Ok(datagrams) => break assert_eq!(datagrams, 3),
//!             Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
//!             Err(error) => return Err(error),
//!         }
//!     }
//!
//!     let mut buffer = [0; 1500];
//!     for _ in 0..3 {
//!         let received = loop {
//!             receiver.readable().await?;
//!             match receiver.try_io(Interest::READABLE, || recv(receiver.as_fd(), &mut buffer)) {
//!                 Ok(received) => break received,
//!                 Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
//!                 Err(error) => return Err(error),
//!             }
//!         };
//!         assert_eq!((received.len, received.metadata.ecn), (1000, Some(Ecn::Ect0)));
//!     }
//!     Ok(())
//! })
//! # }
//! # #[cfg(not(target_os = "linux"))]
//! # fn main() {}
//! ```

#![no_std]

#[cfg(feature = "std")]
extern crate std;

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod linux;

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub use linux::{AF_INET, AF_INET6, Control, Ecn, Metadata, SockAddrIn, SockAddrIn6, SockAddrStorage, Transmit, sys};

#[cfg(all(
    feature = "std",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use linux::io::{
    BATCH, Received, Refused, configure, recv, recv_batch, send, send_batch, set_dont_fragment, set_gro,
};
