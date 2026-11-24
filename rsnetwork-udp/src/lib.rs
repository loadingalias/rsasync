//! Datagram I/O on any UDP socket that the caller owns, and the system declarations that the
//! networking family and its runtimes share, so each declaration exists once.
//!
//! The calls send and receive with no wait, on a socket of any runtime or of no runtime, for
//! example a Tokio socket through `UdpSocket::try_io`. Each call is one system call, except a batch
//! on macOS and Windows:
//!
//! - `send` sends one datagram, or on Linux and Windows a batch of segments that the stack or the
//!   device sends as one datagram for each segment (GSO, USO). macOS has no segmentation offload,
//!   so there a segment size gives `Refused::NoSegmentation`.
//! - `send_batch` sends several datagrams: in one call on Linux (`sendmmsg`), and with one call
//!   for each datagram on macOS and Windows. `recv_batch` receives several in the same way
//!   (`recvmmsg` on Linux).
//! - `recv` receives one datagram, or several coalesced ones with their segment size: on Linux
//!   after `set_gro` (GRO), and on Windows after `set_uro` (URO).
//! - A `Probe` validates the segmentation offload of a path whose two ends the caller controls, and
//!   names the fault of a path that must fall back to plain sends and receives.
//! - A send can set the ECN codepoint and the source address, and a receive reports them, after
//!   `configure`. A receive reports truncation. `set_dont_fragment` turns on the
//!   don't-fragment option that path MTU discovery needs.
//!
//! The `macos-private-batch` feature, which is off by default, makes `send_batch` and
//! `recv_batch` of macOS use one call for the batch: the private system calls `sendmsg_x` and
//! `recvmsg_x`. A release of macOS can change them. The feature has no effect on another target.
//!
//! The declarations of the calls, their message headers, and their control messages are in
//! `sys`, and `Control` is the one encoding of the control messages, which a completion
//! backend also uses. They need only `core`. The calls need the `std` feature, which is on by
//! default.
//!
//! Each item agrees with the Linux 6.1 headers on x86_64 and aarch64, with the macOS 27.2 SDK on
//! aarch64, and with the Windows 11 SDK on x86_64, as the `sys` module names them.
//! The macOS declarations also compile and link for x86_64. The Windows declarations also compile for aarch64.
//!
//! With Tokio, each call runs in `UdpSocket::try_io`, which clears the readiness of the socket
//! when the call gives `WouldBlock`:
//!
//! ```
//! # #[cfg(any(target_os = "linux", target_os = "macos"))]
//! # fn main() -> std::io::Result<()> {
//! use std::io;
//! use std::os::fd::AsFd;
//!
//! use rsnetwork_udp::{Ecn, Transmit, configure, recv, send_batch};
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
//!     // One batch of three datagrams of 1000 bytes. A batch can send only its first datagrams:
//!     // send the rest again.
//!     let contents = [7; 1000];
//!     let transmits = [Transmit {
//!         destination: Some(receiver.local_addr()?),
//!         contents: &contents,
//!         segment_size: None,
//!         ecn: Some(Ecn::Ect0),
//!         source: None,
//!     }; 3];
//!     let mut sent = 0;
//!     while sent < transmits.len() {
//!         sender.writable().await?;
//!         match sender.try_io(Interest::WRITABLE, || send_batch(sender.as_fd(), &transmits[sent..])) {
//!             Ok(count) => sent += count,
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
//! # #[cfg(not(any(target_os = "linux", target_os = "macos")))]
//! # fn main() {}
//! ```

#![no_std]

#[cfg(feature = "std")]
extern crate std;

#[cfg(all(
    any(target_os = "linux", target_os = "macos", target_os = "windows"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod addr;
#[cfg(all(
    any(target_os = "linux", target_os = "macos", target_os = "windows"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod datagram;
#[cfg(all(
    feature = "std",
    any(target_os = "linux", target_os = "macos", target_os = "windows"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod probe;
#[cfg(all(
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod unix;
#[cfg(all(target_os = "windows", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod windows;

#[cfg(all(
    any(target_os = "linux", target_os = "macos", target_os = "windows"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use addr::{AF_INET, AF_INET6, SaFamily, SockAddrIn, SockAddrIn6, SockAddrStorage};
#[cfg(all(
    any(target_os = "linux", target_os = "macos", target_os = "windows"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use datagram::{Ecn, Metadata, Transmit};
#[cfg(all(
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use unix::{Control, sys};
#[cfg(all(target_os = "windows", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub use windows::{Control, sys};

#[cfg(all(
    feature = "std",
    any(target_os = "linux", target_os = "macos", target_os = "windows"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use datagram::{BATCH, Received, Refused};
#[cfg(all(
    feature = "std",
    any(target_os = "linux", target_os = "macos", target_os = "windows"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use probe::{Probe, Reason, Verdict};
#[cfg(all(
    feature = "std",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use unix::io::{configure, recv, recv_batch, send, send_batch, set_dont_fragment};
#[cfg(all(
    feature = "std",
    target_os = "windows",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use windows::io::{configure, recv, recv_batch, send, send_batch, set_dont_fragment, set_uro};

#[cfg(all(
    feature = "std",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use unix::io::set_gro;

/// The shared suite of the datagram calls, through sockets that Tokio owns.
#[cfg(test)]
#[cfg(all(
    feature = "std",
    any(target_os = "linux", target_os = "macos", target_os = "windows"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod tests;
