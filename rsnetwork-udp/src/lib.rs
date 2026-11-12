//! The system declarations of socket addresses, datagram calls, and control messages that the
//! networking family and its runtimes share, so each declaration exists once.
//!
//! This release has only the socket addresses that the TCP path of the rsasync driver uses: the
//! address that an accept writes, and its decode to a `core::net::SocketAddr`. It needs only
//! `core`.
//!
//! Each item agrees with `include/uapi/linux/socket.h`, `in.h`, and `in6.h`, and with
//! `include/linux/socket.h`, of Linux 6.1, on x86_64 and aarch64. Other targets get no item.

#![no_std]

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod linux;

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub use linux::{AF_INET, AF_INET6, SockAddrIn, SockAddrIn6, SockAddrStorage};
