//! The nonblocking datagram calls on a socket that the caller owns.

use core::ffi::{c_int, c_void};
use core::ptr;
use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd};

use super::control::Control;
use super::sys::{
    self, EINTR, EMSGSIZE, IP_PKTINFO, IP_RECVTOS, IPPROTO_IP, IPPROTO_IPV6, IPV6_DONTFRAG, IPV6_RECVPKTINFO,
    IPV6_RECVTCLASS, IPV6_V6ONLY, IoVec, MSG_DONTWAIT, MSG_TRUNC, MsgHdr,
};
use crate::addr::{AF_INET, AF_INET6, SockAddrStorage};
use crate::datagram::{Received, Refused, Transmit};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{recv_batch, send_batch, set_gro};

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use macos::{recv_batch, send_batch};

/// Asks the kernel to deliver the ECN codepoint and the local address with each received
/// datagram. A dual-stack IPv6 socket gets them for IPv4 datagrams too.
pub fn configure(socket: BorrowedFd<'_>) -> io::Result<()> {
    let (ipv4, ipv6) = families(socket)?;
    // Linux gives the metadata of the IPv4 datagrams of a dual-stack socket through its IPv4
    // options. macOS gives them through its IPv6 options.
    if ipv4 && (cfg!(target_os = "linux") || !ipv6) {
        set(socket, IPPROTO_IP, IP_RECVTOS, 1)?;
        set(socket, IPPROTO_IP, IP_PKTINFO, 1)?;
    }
    if ipv6 {
        set(socket, IPPROTO_IPV6, IPV6_RECVTCLASS, 1)?;
        set(socket, IPPROTO_IPV6, IPV6_RECVPKTINFO, 1)?;
    }
    Ok(())
}

/// Turns the don't-fragment option on, which path MTU discovery needs: a datagram above the path
/// MTU is refused with [`Refused::TooLarge`].
pub fn set_dont_fragment(socket: BorrowedFd<'_>) -> io::Result<()> {
    let (ipv4, ipv6) = families(socket)?;
    #[cfg(target_os = "linux")]
    if ipv4 {
        set(socket, IPPROTO_IP, sys::IP_MTU_DISCOVER, sys::IP_PMTUDISC_DO)?;
    }
    #[cfg(target_os = "macos")]
    if ipv4 && !ipv6 {
        set(socket, IPPROTO_IP, sys::IP_DONTFRAG, 1)?;
    }
    if ipv6 {
        #[cfg(target_os = "linux")]
        set(socket, IPPROTO_IPV6, sys::IPV6_MTU_DISCOVER, sys::IPV6_PMTUDISC_DO)?;
        set(socket, IPPROTO_IPV6, IPV6_DONTFRAG, 1)?;
    }
    Ok(())
}

/// Sends one datagram, or one batch of segments, with no wait. Gives the number of datagrams,
/// which is each datagram of `transmit`: the kernel sends all of them or none.
///
/// # Errors
///
/// `WouldBlock` when the socket has no room, a [`Refused`] payload, or another error of
/// `sendmsg`. On macOS, each segment size gives [`Refused::NoSegmentation`].
#[expect(unsafe_code, reason = "the message header names memory that lives during the call")]
pub fn send(socket: BorrowedFd<'_>, transmit: &Transmit<'_>) -> io::Result<usize> {
    #[cfg(target_os = "macos")]
    if transmit.segment_size.is_some() {
        return Err(io::Error::new(io::ErrorKind::Unsupported, Refused::NoSegmentation));
    }
    let (mut name, name_len) = name(transmit);
    let mut region = region(transmit.contents);
    let mut control = Control::new();
    let control_len = control.encode(transmit);
    let header = header(&mut name, name_len, &mut region, &mut control, control_len);
    loop {
        // SAFETY: the header names the address, the region of `transmit.contents`, and the control
        // messages, which live during the call, and the kernel only reads them.
        if unsafe { sys::sendmsg(socket.as_raw_fd(), &raw const header, MSG_DONTWAIT) } >= 0 {
            return Ok(transmit.datagrams());
        }
        match io::Error::last_os_error() {
            error if error.raw_os_error() == Some(EINTR) => {}
            error => return Err(Refused::of_send(error, transmit.segment_size.is_some())),
        }
    }
}

/// Receives one datagram into `buffer`, with no wait.
///
/// # Errors
///
/// `WouldBlock` when no datagram is ready, or another error of `recvmsg`.
#[expect(unsafe_code, reason = "the message header names memory that lives during the call")]
pub fn recv(socket: BorrowedFd<'_>, buffer: &mut [u8]) -> io::Result<Received> {
    let mut name = SockAddrStorage::EMPTY;
    let mut region = IoVec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    let mut control = Control::new();
    let mut header = header(&mut name, SockAddrStorage::LEN, &mut region, &mut control, Control::LEN);
    loop {
        // SAFETY: the header names the address storage, `buffer`, and the control storage, which
        // live during the call. The kernel writes at most their lengths.
        let len = unsafe { sys::recvmsg(socket.as_raw_fd(), &raw mut header, MSG_DONTWAIT) };
        if let Ok(len) = usize::try_from(len) {
            #[cfg(target_os = "linux")]
            let control_len = header.msg_controllen;
            #[cfg(target_os = "macos")]
            let control_len = usize::try_from(header.msg_controllen).unwrap_or(usize::MAX);
            return Received::from_message(&name, header.msg_namelen, header.msg_flags, &control, control_len, len);
        }
        match io::Error::last_os_error() {
            error if error.raw_os_error() == Some(EINTR) => {}
            error => return Err(error),
        }
    }
}

/// The address of the destination of `transmit`, and its length. A send to the peer of a connected
/// socket has no address, and its length is 0.
fn name(transmit: &Transmit<'_>) -> (SockAddrStorage, u32) {
    transmit
        .destination
        .map_or((SockAddrStorage::EMPTY, 0), SockAddrStorage::new)
}

/// The region of `bytes`, which the kernel only reads.
fn region(bytes: &[u8]) -> IoVec {
    IoVec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    }
}

/// A header that names one address of `name_len` bytes, or none for a length of 0, one region, and
/// `control_len` bytes of control messages.
fn header(
    name: &mut SockAddrStorage,
    name_len: u32,
    region: &mut IoVec,
    control: &mut Control,
    control_len: usize,
) -> MsgHdr {
    MsgHdr {
        msg_name: if name_len == 0 {
            ptr::null_mut()
        } else {
            ptr::from_mut(name).cast()
        },
        msg_namelen: name_len,
        msg_iov: region,
        msg_iovlen: 1,
        msg_control: if control_len == 0 {
            ptr::null_mut()
        } else {
            control.as_mut_ptr().cast()
        },
        #[cfg(target_os = "linux")]
        msg_controllen: control_len,
        #[cfg(target_os = "macos")]
        msg_controllen: c_uint_len(control_len),
        msg_flags: 0,
    }
}

impl Received {
    /// The datagram that a receive wrote, from its message header: `len` bytes, the peer address
    /// of `name_len` bytes in `name`, the received `flags`, and `control_len` bytes of control
    /// messages in `control`. A completion backend that makes its own receive gives the same
    /// result as [`recv`].
    ///
    /// # Errors
    ///
    /// `InvalidData` when the peer address is not an IP address.
    pub fn from_message(
        name: &SockAddrStorage,
        name_len: u32,
        flags: c_int,
        control: &Control,
        control_len: usize,
        len: usize,
    ) -> io::Result<Self> {
        let source = name
            .to_socket_addr(name_len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "the datagram has no IP peer address"))?;
        Ok(Self {
            len,
            source,
            truncated: flags & MSG_TRUNC != 0,
            metadata: control.decode(control_len),
        })
    }
}

impl Refused {
    /// The error of a send, with its [`Refused`] payload when it has one. `segmented` is whether
    /// the send had a segment size. A completion backend that makes its own send gives the same
    /// error as [`send`].
    #[cfg_attr(
        target_os = "macos",
        expect(unused_variables, reason = "macOS refuses a segment size before the call")
    )]
    #[must_use]
    pub fn of_send(error: io::Error, segmented: bool) -> io::Error {
        match error.raw_os_error() {
            Some(EMSGSIZE) => io::Error::new(io::ErrorKind::InvalidInput, Self::TooLarge),
            #[cfg(target_os = "linux")]
            Some(sys::EIO) if segmented => io::Error::new(io::ErrorKind::Unsupported, Self::NoSegmentation),
            _ => error,
        }
    }
}

/// The families that `socket` takes: IPv4 for an IPv4 socket, IPv6 for an IPv6 socket, and both
/// for a dual-stack IPv6 socket.
#[expect(
    unsafe_code,
    reason = "the kernel writes the local address and an option into owned storage"
)]
fn families(socket: BorrowedFd<'_>) -> io::Result<(bool, bool)> {
    let mut name = SockAddrStorage::EMPTY;
    let mut len = SockAddrStorage::LEN;
    // SAFETY: the storage and its length live during the call, and the kernel writes at most `len`.
    if unsafe { sys::getsockname(socket.as_raw_fd(), ptr::from_mut(&mut name).cast(), &raw mut len) } < 0 {
        return Err(io::Error::last_os_error());
    }
    match name.ss_family {
        AF_INET => Ok((true, false)),
        AF_INET6 => {
            let mut only: c_int = 0;
            let mut only_len = c_uint_len(size_of::<c_int>());
            // SAFETY: the option and its length live during the call, and the kernel writes at most
            // `only_len`.
            let result = unsafe {
                sys::getsockopt(
                    socket.as_raw_fd(),
                    IPPROTO_IPV6,
                    IPV6_V6ONLY,
                    ptr::from_mut(&mut only).cast::<c_void>(),
                    &raw mut only_len,
                )
            };
            if result < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok((only == 0, true))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the socket is not an IP socket",
        )),
    }
}

/// Sets the `int` option `name` of `level`.
#[expect(unsafe_code, reason = "the kernel reads an option from owned storage")]
pub(crate) fn set(socket: BorrowedFd<'_>, level: c_int, name: c_int, value: c_int) -> io::Result<()> {
    // SAFETY: the option lives during the call, and the kernel reads `size_of::<c_int>()` bytes.
    let result = unsafe {
        sys::setsockopt(
            socket.as_raw_fd(),
            level,
            name,
            ptr::from_ref(&value).cast::<c_void>(),
            c_uint_len(size_of::<c_int>()),
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// A length of at most [`BATCH`](crate::BATCH) or a small structure, as a `c_uint`.
fn c_uint_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}
