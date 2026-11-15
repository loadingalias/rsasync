//! The nonblocking datagram calls on a socket that the caller owns, on Windows.

use core::ffi::{c_char, c_int};
use core::net::SocketAddr;
use core::ptr;
use std::io;
use std::os::windows::io::{AsRawSocket as _, BorrowedSocket};
use std::sync::OnceLock;

use super::control::Control;
use super::sys::{
    self, Guid, IP_DONTFRAGMENT, IP_ECN, IP_PKTINFO, IPPROTO_IP, IPPROTO_IPV6, IPPROTO_UDP, IPV6_DONTFRAG, IPV6_ECN,
    IPV6_PKTINFO, IPV6_V6ONLY, MSG_TRUNC, SIO_GET_EXTENSION_FUNCTION_POINTER, SOCKET_ERROR, Socket,
    UDP_RECV_MAX_COALESCED_SIZE, WSAEINVAL, WSAEMSGSIZE, WSAEOPNOTSUPP, WSAID_WSARECVMSG, WsaBuf, WsaMsg, WsaRecvMsg,
};
use crate::addr::{AF_INET, AF_INET6, SockAddrStorage};
use crate::datagram::{BATCH, Received, Refused, Transmit};

/// The most bytes of a coalesced receive that [`set_uro`] asks for.
const COALESCED: u32 = 65_535;

/// Asks the stack to deliver the ECN codepoint and the local address with each received datagram. A
/// dual-stack IPv6 socket gets them for IPv4 datagrams too.
pub fn configure(socket: BorrowedSocket<'_>) -> io::Result<()> {
    let (ipv4, ipv6) = families(socket)?;
    if ipv4 {
        set(socket, IPPROTO_IP, IP_ECN, 1)?;
        set(socket, IPPROTO_IP, IP_PKTINFO, 1)?;
    }
    if ipv6 {
        set(socket, IPPROTO_IPV6, IPV6_ECN, 1)?;
        set(socket, IPPROTO_IPV6, IPV6_PKTINFO, 1)?;
    }
    Ok(())
}

/// Turns the don't-fragment option on, which path MTU discovery needs: a datagram above the path
/// MTU is refused with [`Refused::TooLarge`].
pub fn set_dont_fragment(socket: BorrowedSocket<'_>) -> io::Result<()> {
    let (ipv4, ipv6) = families(socket)?;
    if ipv4 {
        set(socket, IPPROTO_IP, IP_DONTFRAGMENT, 1)?;
    }
    if ipv6 {
        set(socket, IPPROTO_IPV6, IPV6_DONTFRAG, 1)?;
    }
    Ok(())
}

/// Turns coalesced receive (URO) on or off. A coalesced receive gives several datagrams of one
/// peer in one buffer, with their segment size in [`Metadata::segment_size`](crate::Metadata::segment_size).
/// A path whose coalesced data comes with no segment size fails a [`Probe`](crate::Probe): turn
/// URO off there.
pub fn set_uro(socket: BorrowedSocket<'_>, on: bool) -> io::Result<()> {
    let bytes = if on { COALESCED } else { 0 };
    set(
        socket,
        IPPROTO_UDP,
        UDP_RECV_MAX_COALESCED_SIZE,
        c_int::try_from(bytes).unwrap_or(c_int::MAX),
    )
}

/// Sends one datagram, or one batch of segments that the stack or the device sends as one datagram
/// for each segment (USO), with no wait. Gives the number of datagrams, which is each datagram of
/// `transmit`: the stack sends all of them or none. With no destination and an ECN codepoint, it
/// first reads the peer of the connected socket (`getpeername`), because the ECN message must have
/// the family of the peer.
///
/// # Errors
///
/// `WouldBlock` when the socket has no room, a [`Refused`] payload, or another error of
/// `WSASendMsg`, for example `WSAEINVAL` for the CE codepoint, which the stack does not send.
#[expect(unsafe_code, reason = "the message names memory that lives during the call")]
pub fn send(socket: BorrowedSocket<'_>, transmit: &Transmit<'_>) -> io::Result<usize> {
    // The stack refuses the ECN message of the other family, so a send with no destination and an
    // ECN codepoint names the peer of the connected socket, which gives the family.
    let connected;
    let transmit = if transmit.destination.is_none() && transmit.ecn.is_some() {
        connected = Transmit {
            destination: Some(peer(socket)?),
            ..*transmit
        };
        &connected
    } else {
        transmit
    };
    let len = u32::try_from(transmit.contents.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "the datagram is larger than 4 GiB"))?;
    let (mut name, name_len) = transmit
        .destination
        .map_or((SockAddrStorage::EMPTY, 0), SockAddrStorage::new);
    let mut region = WsaBuf {
        len,
        buf: transmit.contents.as_ptr().cast_mut().cast(),
    };
    let mut control = Control::new();
    let control_len = control.encode(transmit);
    let message = message(&mut name, name_len, &mut region, &mut control, control_len);
    let mut sent = 0;
    // SAFETY: the message names the address, the region of `transmit.contents`, and the control
    // messages, which live during the call, and the stack only reads them. The call is not
    // overlapped, so it ends before it returns.
    let result = unsafe {
        sys::WSASendMsg(
            socket.as_raw_socket(),
            &raw const message,
            0,
            &raw mut sent,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    if result == SOCKET_ERROR {
        return Err(refusal(last_error(), transmit.segment_size.is_some()));
    }
    Ok(transmit.datagrams())
}

/// Receives one datagram, or one coalesced receive, into `buffer`, with no wait.
///
/// # Errors
///
/// `WouldBlock` when no datagram is ready, or another error of `WSARecvMsg`.
#[expect(unsafe_code, reason = "the message names memory that lives during the call")]
pub fn recv(socket: BorrowedSocket<'_>, buffer: &mut [u8]) -> io::Result<Received> {
    let receive = wsa_recv_msg(socket.as_raw_socket())?;
    let mut name = SockAddrStorage::EMPTY;
    let mut region = WsaBuf {
        len: u32::try_from(buffer.len()).unwrap_or(u32::MAX),
        buf: buffer.as_mut_ptr().cast(),
    };
    let mut control = Control::new();
    let mut message = message(&mut name, SockAddrStorage::LEN, &mut region, &mut control, Control::LEN);
    let mut received = 0;
    // SAFETY: the message names the address storage, `buffer`, and the control storage, which live
    // during the call, and the stack writes at most their lengths. The call is not overlapped, so it
    // ends before it returns.
    let result = unsafe {
        receive(
            socket.as_raw_socket(),
            &raw mut message,
            &raw mut received,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    let (len, truncated) = if result == SOCKET_ERROR {
        let error = last_error();
        // A datagram larger than the buffer fills the buffer and gives `WSAEMSGSIZE`.
        if error.raw_os_error() != Some(WSAEMSGSIZE) {
            return Err(error);
        }
        (usize::try_from(region.len).unwrap_or(usize::MAX), true)
    } else {
        (
            usize::try_from(received).unwrap_or(usize::MAX),
            message.flags & MSG_TRUNC != 0,
        )
    };
    let source = u32::try_from(message.namelen)
        .ok()
        .and_then(|len| name.to_socket_addr(len))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "the datagram has no IP peer address"))?;
    Ok(Received {
        len,
        source,
        truncated,
        metadata: control.decode(usize::try_from(message.control.len).unwrap_or(usize::MAX)),
    })
}

/// Sends up to [`BATCH`] datagrams or batches of segments in turn, each with its own metadata, with
/// no wait: one `WSASendMsg` for each. Gives the number of `transmits` that the stack sent, from the
/// first. It stops at the first one that the stack refuses, so the rest never goes out: send it
/// again.
///
/// # Errors
///
/// As for [`send`], when the stack sent none of them.
pub fn send_batch(socket: BorrowedSocket<'_>, transmits: &[Transmit<'_>]) -> io::Result<usize> {
    let transmits = &transmits[..transmits.len().min(BATCH)];
    for (sent, transmit) in transmits.iter().enumerate() {
        if let Err(error) = send(socket, transmit) {
            return if sent == 0 { Err(error) } else { Ok(sent) };
        }
    }
    Ok(transmits.len())
}

/// Receives up to [`BATCH`] datagrams in turn, one into each of `buffers`, with no wait: one
/// `WSARecvMsg` for each. Writes each into `received`, from the first, and gives the number of
/// datagrams. It stops when no datagram is ready.
///
/// # Errors
///
/// `WouldBlock` when no datagram is ready, or another error of `WSARecvMsg`, when it received none.
pub fn recv_batch(
    socket: BorrowedSocket<'_>,
    buffers: &mut [&mut [u8]],
    received: &mut [Received],
) -> io::Result<usize> {
    let count = buffers.len().min(received.len()).min(BATCH);
    for (index, (buffer, slot)) in buffers.iter_mut().zip(received.iter_mut()).take(count).enumerate() {
        match recv(socket, buffer) {
            Ok(datagram) => *slot = datagram,
            Err(error) => return if index == 0 { Err(error) } else { Ok(index) },
        }
    }
    Ok(count)
}

/// A message that names one address of `name_len` bytes, or none for a length of 0, one region, and
/// `control_len` bytes of control messages.
fn message(
    name: &mut SockAddrStorage,
    name_len: u32,
    region: &mut WsaBuf,
    control: &mut Control,
    control_len: usize,
) -> WsaMsg {
    WsaMsg {
        name: if name_len == 0 {
            ptr::null_mut()
        } else {
            ptr::from_mut(name).cast()
        },
        namelen: c_int::try_from(name_len).unwrap_or(0),
        buffers: region,
        buffer_count: 1,
        control: WsaBuf {
            len: u32::try_from(control_len).unwrap_or(0),
            buf: if control_len == 0 {
                ptr::null_mut()
            } else {
                control.as_mut_ptr().cast()
            },
        },
        flags: 0,
    }
}

/// `WSARecvMsg`, which `ws2_32.dll` gives only as a pointer. The pointer is the same for each
/// socket of the process, so the first call keeps it.
#[expect(unsafe_code, reason = "the stack writes the function pointer into owned storage")]
fn wsa_recv_msg(socket: Socket) -> io::Result<WsaRecvMsg> {
    static POINTER: OnceLock<WsaRecvMsg> = OnceLock::new();
    if let Some(&receive) = POINTER.get() {
        return Ok(receive);
    }
    let mut pointer: Option<WsaRecvMsg> = None;
    let mut returned = 0;
    // SAFETY: the identifier and the storage of the pointer live during the call, and the stack
    // writes at most `size_of::<Option<WsaRecvMsg>>()` bytes: one function pointer, or null.
    let result = unsafe {
        sys::WSAIoctl(
            socket,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            ptr::from_ref(&WSAID_WSARECVMSG).cast(),
            u32_len(size_of::<Guid>()),
            ptr::from_mut(&mut pointer).cast(),
            u32_len(size_of::<Option<WsaRecvMsg>>()),
            &raw mut returned,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    if result == SOCKET_ERROR {
        return Err(last_error());
    }
    let receive = pointer.ok_or_else(|| io::Error::other("the socket provider has no WSARecvMsg"))?;
    Ok(*POINTER.get_or_init(|| receive))
}

/// A send error with its [`Refused`] payload when it has one.
fn refusal(error: io::Error, segmented: bool) -> io::Error {
    match error.raw_os_error() {
        Some(WSAEMSGSIZE) => io::Error::new(io::ErrorKind::InvalidInput, Refused::TooLarge),
        Some(WSAEINVAL | WSAEOPNOTSUPP) if segmented => {
            io::Error::new(io::ErrorKind::Unsupported, Refused::NoSegmentation)
        }
        _ => error,
    }
}

/// The last error of a Winsock call on this thread.
#[expect(unsafe_code, reason = "WSAGetLastError reads the error of this thread")]
fn last_error() -> io::Error {
    // SAFETY: the function takes no argument and reads thread-local state.
    io::Error::from_raw_os_error(unsafe { sys::WSAGetLastError() })
}

/// The peer of a connected socket.
#[expect(unsafe_code, reason = "the stack writes the peer address into owned storage")]
fn peer(socket: BorrowedSocket<'_>) -> io::Result<SocketAddr> {
    let mut name = SockAddrStorage::EMPTY;
    let mut len = c_int::try_from(SockAddrStorage::LEN).unwrap_or(0);
    // SAFETY: the storage and its length live during the call, and the stack writes at most `len`.
    if unsafe { sys::getpeername(socket.as_raw_socket(), ptr::from_mut(&mut name).cast(), &raw mut len) }
        == SOCKET_ERROR
    {
        return Err(last_error());
    }
    u32::try_from(len)
        .ok()
        .and_then(|len| name.to_socket_addr(len))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "the peer is not an IP address"))
}

/// The families that `socket` takes: IPv4 for an IPv4 socket, IPv6 for an IPv6 socket, and both
/// for a dual-stack IPv6 socket.
#[expect(
    unsafe_code,
    reason = "the stack writes the local address and an option into owned storage"
)]
fn families(socket: BorrowedSocket<'_>) -> io::Result<(bool, bool)> {
    let mut name = SockAddrStorage::EMPTY;
    let mut len = c_int::try_from(SockAddrStorage::LEN).unwrap_or(0);
    // SAFETY: the storage and its length live during the call, and the stack writes at most `len`.
    if unsafe { sys::getsockname(socket.as_raw_socket(), ptr::from_mut(&mut name).cast(), &raw mut len) }
        == SOCKET_ERROR
    {
        return Err(last_error());
    }
    match name.ss_family {
        AF_INET => Ok((true, false)),
        AF_INET6 => {
            let mut only: c_int = 0;
            let mut only_len = c_int::try_from(size_of::<c_int>()).unwrap_or(0);
            // SAFETY: the option and its length live during the call, and the stack writes at most
            // `only_len`.
            let result = unsafe {
                sys::getsockopt(
                    socket.as_raw_socket(),
                    IPPROTO_IPV6,
                    IPV6_V6ONLY,
                    ptr::from_mut(&mut only).cast::<c_char>(),
                    &raw mut only_len,
                )
            };
            if result == SOCKET_ERROR {
                return Err(last_error());
            }
            Ok((only == 0, true))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the socket is not an IP socket",
        )),
    }
}

/// Sets the `int` (`DWORD`) option `name` of `level`.
#[expect(unsafe_code, reason = "the stack reads an option from owned storage")]
pub(crate) fn set(socket: BorrowedSocket<'_>, level: c_int, name: c_int, value: c_int) -> io::Result<()> {
    // SAFETY: the option lives during the call, and the stack reads `size_of::<c_int>()` bytes.
    let result = unsafe {
        sys::setsockopt(
            socket.as_raw_socket(),
            level,
            name,
            ptr::from_ref(&value).cast::<c_char>(),
            c_int::try_from(size_of::<c_int>()).unwrap_or(0),
        )
    };
    if result == SOCKET_ERROR {
        return Err(last_error());
    }
    Ok(())
}

/// The size of a small structure, as a `DWORD`.
fn u32_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}
