//! The nonblocking datagram calls on a socket that the caller owns.

use core::ffi::{c_int, c_void};
use core::fmt;
use core::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use core::ptr;
use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd};

use super::control::{Control, Metadata, Transmit};
use super::sys::{
    self, EINTR, EIO, EMSGSIZE, IP_MTU_DISCOVER, IP_PKTINFO, IP_PMTUDISC_DO, IP_RECVTOS, IPPROTO_IP, IPPROTO_IPV6,
    IPV6_DONTFRAG, IPV6_MTU_DISCOVER, IPV6_PMTUDISC_DO, IPV6_RECVPKTINFO, IPV6_RECVTCLASS, IPV6_V6ONLY, IoVec,
    MSG_DONTWAIT, MSG_TRUNC, MmsgHdr, MsgHdr, SOL_UDP, UDP_GRO,
};
use super::{AF_INET, AF_INET6, SockAddrStorage};

/// The most datagrams of one [`send_batch`] or [`recv_batch`].
pub const BATCH: usize = 32;

/// A send that the kernel refused. It is the payload of the `io::Error` of [`send`] and
/// [`send_batch`]: get it with [`Refused::of`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// The datagram is larger than the path MTU, and the don't-fragment option is on
    /// (`EMSGSIZE`). Send smaller datagrams.
    TooLarge,
    /// The device cannot segment the batch (`EIO` on a send with a segment size). The kernel sent
    /// none of it. Send its segments as separate datagrams, for example with [`send_batch`], and
    /// send no segment size on this path again.
    NoSegmentation,
}

impl Refused {
    /// The refusal that `error` carries, or `None` for each other error.
    #[must_use]
    pub fn of(error: &io::Error) -> Option<Self> {
        error.get_ref()?.downcast_ref().copied()
    }
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TooLarge => "the datagram is larger than the path MTU, and it cannot be fragmented",
            Self::NoSegmentation => "the device cannot segment the batch; send its segments as separate datagrams",
        })
    }
}

impl core::error::Error for Refused {}

/// A received datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Received {
    /// The number of bytes in the buffer.
    pub len: usize,
    /// The peer. A dual-stack IPv6 socket gives an IPv4 peer as its IPv4-mapped address.
    pub source: SocketAddr,
    /// The datagram did not fit in the buffer, and the kernel dropped its rest.
    pub truncated: bool,
    /// The segment size, the ECN codepoint, and the local address.
    pub metadata: Metadata,
}

impl Default for Received {
    fn default() -> Self {
        Self {
            len: 0,
            source: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            truncated: false,
            metadata: Metadata::default(),
        }
    }
}

/// Asks the kernel to deliver the ECN codepoint and the local address with each received
/// datagram. A dual-stack IPv6 socket gets them for IPv4 datagrams too.
pub fn configure(socket: BorrowedFd<'_>) -> io::Result<()> {
    let (ipv4, ipv6) = families(socket)?;
    if ipv4 {
        set(socket, IPPROTO_IP, IP_RECVTOS, 1)?;
        set(socket, IPPROTO_IP, IP_PKTINFO, 1)?;
    }
    if ipv6 {
        set(socket, IPPROTO_IPV6, IPV6_RECVTCLASS, 1)?;
        set(socket, IPPROTO_IPV6, IPV6_RECVPKTINFO, 1)?;
    }
    Ok(())
}

/// Turns coalesced receive (GRO) on or off. A coalesced receive gives several datagrams of one
/// peer in one buffer, with their segment size in [`Metadata::segment_size`].
pub fn set_gro(socket: BorrowedFd<'_>, on: bool) -> io::Result<()> {
    set(socket, SOL_UDP, UDP_GRO, c_int::from(on))
}

/// Turns the don't-fragment option on, which path MTU discovery needs: a datagram above the path
/// MTU is refused with [`Refused::TooLarge`].
pub fn set_dont_fragment(socket: BorrowedFd<'_>) -> io::Result<()> {
    let (ipv4, ipv6) = families(socket)?;
    if ipv4 {
        set(socket, IPPROTO_IP, IP_MTU_DISCOVER, IP_PMTUDISC_DO)?;
    }
    if ipv6 {
        set(socket, IPPROTO_IPV6, IPV6_MTU_DISCOVER, IPV6_PMTUDISC_DO)?;
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
/// `sendmsg`.
#[expect(unsafe_code, reason = "the message header names memory that lives during the call")]
pub fn send(socket: BorrowedFd<'_>, transmit: &Transmit<'_>) -> io::Result<usize> {
    let (mut name, name_len) = SockAddrStorage::new(transmit.destination);
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
            error => return Err(refusal(error, transmit.segment_size.is_some())),
        }
    }
}

/// Sends up to [`BATCH`] datagrams or batches of segments in one call (`sendmmsg`), each with its
/// own metadata, with no wait. Gives the number of `transmits` that the kernel sent, from the
/// first. Send the rest again; their bytes never join another datagram.
///
/// # Errors
///
/// As for [`send`], when the kernel sent none of them.
#[expect(unsafe_code, reason = "the message headers name memory that lives during the call")]
pub fn send_batch(socket: BorrowedFd<'_>, transmits: &[Transmit<'_>]) -> io::Result<usize> {
    let transmits = &transmits[..transmits.len().min(BATCH)];
    let mut names = [SockAddrStorage::EMPTY; BATCH];
    let mut regions = [region(&[]); BATCH];
    let mut controls = [Control::new(); BATCH];
    let mut headers = [MmsgHdr {
        msg_hdr: header_empty(),
        msg_len: 0,
    }; BATCH];
    let mut segmented = false;
    for (index, transmit) in transmits.iter().enumerate() {
        let name_len;
        (names[index], name_len) = SockAddrStorage::new(transmit.destination);
        regions[index] = region(transmit.contents);
        let control_len = controls[index].encode(transmit);
        headers[index].msg_hdr = header(
            &mut names[index],
            name_len,
            &mut regions[index],
            &mut controls[index],
            control_len,
        );
        segmented |= transmit.segment_size.is_some();
    }
    let count = c_uint_len(transmits.len());
    loop {
        // SAFETY: the first `count` headers name the addresses, the regions, and the control
        // messages of `transmits`, which live during the call. The kernel reads them, and writes
        // only the `msg_len` of each header.
        let sent = unsafe { sys::sendmmsg(socket.as_raw_fd(), headers.as_mut_ptr(), count, MSG_DONTWAIT) };
        if let Ok(sent) = usize::try_from(sent) {
            return Ok(sent);
        }
        match io::Error::last_os_error() {
            error if error.raw_os_error() == Some(EINTR) => {}
            error => return Err(refusal(error, segmented)),
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
            return finish(&name, &header, &control, len);
        }
        match io::Error::last_os_error() {
            error if error.raw_os_error() == Some(EINTR) => {}
            error => return Err(error),
        }
    }
}

/// Receives up to [`BATCH`] datagrams in one call (`recvmmsg`), one into each of `buffers`, with
/// no wait. Writes each into `received`, from the first, and gives the number of datagrams.
///
/// # Errors
///
/// `WouldBlock` when no datagram is ready, or another error of `recvmmsg`.
#[expect(unsafe_code, reason = "the message headers name memory that lives during the call")]
pub fn recv_batch(socket: BorrowedFd<'_>, buffers: &mut [&mut [u8]], received: &mut [Received]) -> io::Result<usize> {
    let count = buffers.len().min(received.len()).min(BATCH);
    let mut names = [SockAddrStorage::EMPTY; BATCH];
    let mut regions = [region(&[]); BATCH];
    let mut controls = [Control::new(); BATCH];
    let mut headers = [MmsgHdr {
        msg_hdr: header_empty(),
        msg_len: 0,
    }; BATCH];
    for (index, buffer) in buffers.iter_mut().take(count).enumerate() {
        regions[index] = IoVec {
            iov_base: buffer.as_mut_ptr().cast(),
            iov_len: buffer.len(),
        };
        headers[index].msg_hdr = header(
            &mut names[index],
            SockAddrStorage::LEN,
            &mut regions[index],
            &mut controls[index],
            Control::LEN,
        );
    }
    loop {
        // SAFETY: the first `count` headers name address storage, the buffers, and control storage,
        // which live during the call. The kernel writes at most their lengths.
        let done = unsafe {
            sys::recvmmsg(
                socket.as_raw_fd(),
                headers.as_mut_ptr(),
                c_uint_len(count),
                MSG_DONTWAIT,
                ptr::null_mut(),
            )
        };
        if let Ok(done) = usize::try_from(done) {
            for (index, slot) in received.iter_mut().take(done).enumerate() {
                let len = usize::try_from(headers[index].msg_len).unwrap_or(usize::MAX);
                *slot = finish(&names[index], &headers[index].msg_hdr, &controls[index], len)?;
            }
            return Ok(done);
        }
        match io::Error::last_os_error() {
            error if error.raw_os_error() == Some(EINTR) => {}
            error => return Err(error),
        }
    }
}

/// The region of `bytes`, which the kernel only reads.
fn region(bytes: &[u8]) -> IoVec {
    IoVec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    }
}

/// A header that names one address, one region, and `control_len` bytes of control messages.
fn header(
    name: &mut SockAddrStorage,
    name_len: u32,
    region: &mut IoVec,
    control: &mut Control,
    control_len: usize,
) -> MsgHdr {
    MsgHdr {
        msg_name: ptr::from_mut(name).cast(),
        msg_namelen: name_len,
        msg_iov: region,
        msg_iovlen: 1,
        msg_control: if control_len == 0 {
            ptr::null_mut()
        } else {
            control.as_mut_ptr().cast()
        },
        msg_controllen: control_len,
        msg_flags: 0,
    }
}

/// A header that names nothing.
const fn header_empty() -> MsgHdr {
    MsgHdr {
        msg_name: ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: ptr::null_mut(),
        msg_iovlen: 0,
        msg_control: ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    }
}

/// The datagram that a receive wrote, from its header and its storage.
fn finish(name: &SockAddrStorage, header: &MsgHdr, control: &Control, len: usize) -> io::Result<Received> {
    let source = name
        .to_socket_addr(header.msg_namelen)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "the datagram has no IP peer address"))?;
    Ok(Received {
        len,
        source,
        truncated: header.msg_flags & MSG_TRUNC != 0,
        metadata: control.decode(header.msg_controllen),
    })
}

/// A send error with its [`Refused`] payload when it has one.
fn refusal(error: io::Error, segmented: bool) -> io::Error {
    match error.raw_os_error() {
        Some(EMSGSIZE) => io::Error::new(io::ErrorKind::InvalidInput, Refused::TooLarge),
        Some(EIO) if segmented => io::Error::new(io::ErrorKind::Unsupported, Refused::NoSegmentation),
        _ => error,
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
fn set(socket: BorrowedFd<'_>, level: c_int, name: c_int, value: c_int) -> io::Result<()> {
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

/// A length of at most [`BATCH`] or a small structure, as a `c_uint`.
fn c_uint_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests;
