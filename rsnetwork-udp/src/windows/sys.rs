//! The declarations of the datagram calls, their message headers, and their control messages on
//! Windows.
//!
//! Each item agrees with the Windows 11 SDK that the Visual Studio Build Tools 18.11.1 install, on
//! x86_64: `ws2def.h` (`WSAMSG`, `WSABUF`, `WSACMSGHDR`, the message flags), `ws2ipdef.h` (the IP,
//! IPv6, and UDP options, `IN_PKTINFO`, `IN6_PKTINFO`), `mswsock.h` (`WSAID_WSARECVMSG`,
//! `LPFN_WSARECVMSG`), `winsock2.h` (the functions and `SIO_GET_EXTENSION_FUNCTION_POINTER`), and
//! `winerror.h` (the error codes). A `SOCKET` is a `UINT_PTR`, 64 bits on x86_64, which is the
//! `RawSocket` of `std`. `ws2_32.dll` gives the functions; `WSARecvMsg` comes only as a pointer
//! from `WSAIoctl`.

use core::ffi::{c_char, c_int, c_void};
use core::mem::{offset_of, size_of};

/// `SOCKET`: a socket handle, the `RawSocket` of `std`.
pub type Socket = u64;

/// `WSABUF`: one region of a datagram, or the control messages of a `WSAMSG`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WsaBuf {
    /// The length of the region (`ULONG`).
    pub len: u32,
    /// The address of the region.
    pub buf: *mut c_char,
}

/// `WSAMSG`: one datagram, its address, and its control messages.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WsaMsg {
    /// The socket address of the peer, or null.
    pub name: *mut c_void,
    /// The length of the address (`INT`).
    pub namelen: c_int,
    /// The regions of the datagram.
    pub buffers: *mut WsaBuf,
    /// The number of regions (`ULONG`).
    pub buffer_count: u32,
    /// The control messages.
    pub control: WsaBuf,
    /// The flags of a received datagram, for example [`MSG_TRUNC`] (`ULONG`).
    pub flags: u32,
}

/// The size of `WSACMSGHDR`: a `SIZE_T` length, a level, and a type. Its data follows it at
/// `WSA_CMSGDATA_ALIGN`, a multiple of 8 bytes, and each message starts at a multiple of 8.
pub const CMSG_HEADER: usize = 16;

/// `IN_PKTINFO`: the address and the interface of an IPv4 datagram.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct InPktInfo {
    /// On send, the source address; on receive, the destination address.
    pub ipi_addr: [u8; 4],
    /// The interface index, or 0 (`ULONG`).
    pub ipi_ifindex: u32,
}

/// `IN6_PKTINFO`: the address and the interface of an IPv6 datagram.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct In6PktInfo {
    /// On send, the source address; on receive, the destination address.
    pub ipi6_addr: [u8; 16],
    /// The interface index, or 0 (`ULONG`).
    pub ipi6_ifindex: u32,
}

/// `GUID`: the identifier of an extension function.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Guid {
    /// The first 4 bytes.
    pub data1: u32,
    /// The next 2 bytes.
    pub data2: u16,
    /// The next 2 bytes.
    pub data3: u16,
    /// The last 8 bytes.
    pub data4: [u8; 8],
}

const _: () = {
    assert!(size_of::<WsaBuf>() == 16);
    assert!(offset_of!(WsaBuf, buf) == 8);
    assert!(size_of::<WsaMsg>() == 56);
    assert!(offset_of!(WsaMsg, namelen) == 8);
    assert!(offset_of!(WsaMsg, buffers) == 16);
    assert!(offset_of!(WsaMsg, buffer_count) == 24);
    assert!(offset_of!(WsaMsg, control) == 32);
    assert!(offset_of!(WsaMsg, flags) == 48);
    assert!(size_of::<InPktInfo>() == 8);
    assert!(offset_of!(InPktInfo, ipi_ifindex) == 4);
    assert!(size_of::<In6PktInfo>() == 20);
    assert!(offset_of!(In6PktInfo, ipi6_ifindex) == 16);
    assert!(size_of::<Guid>() == 16);
};

/// `WSAID_WSARECVMSG`: the identifier of `WSARecvMsg`.
pub const WSAID_WSARECVMSG: Guid = Guid {
    data1: 0xf689_d7c8,
    data2: 0x6f1f,
    data3: 0x436b,
    data4: [0x8a, 0x53, 0xe5, 0x4f, 0xe3, 0x51, 0xc3, 0x22],
};

/// `SIO_GET_EXTENSION_FUNCTION_POINTER`: `_WSAIORW(IOC_WS2, 6)`.
pub const SIO_GET_EXTENSION_FUNCTION_POINTER: u32 = 0xc800_0006;

/// `IPPROTO_IP`, the level of the IPv4 options.
pub const IPPROTO_IP: c_int = 0;
/// `IPPROTO_UDP`, the level of the UDP options.
pub const IPPROTO_UDP: c_int = 17;
/// `IPPROTO_IPV6`, the level of the IPv6 options.
pub const IPPROTO_IPV6: c_int = 41;

/// `IP_DONTFRAGMENT`: set the don't-fragment bit, and refuse an IPv4 datagram above the path MTU.
pub const IP_DONTFRAGMENT: c_int = 14;
/// `IP_PKTINFO`: the packet information of IPv4, on send and, as an option, on receive.
pub const IP_PKTINFO: c_int = 19;
/// `IP_ECN`: the ECN codepoint of IPv4, as an `int` control message. As the option `IP_RECVECN`, it
/// delivers the codepoint with each received datagram.
pub const IP_ECN: c_int = 50;

/// `IPV6_DONTFRAG`: refuse an IPv6 datagram above the path MTU.
pub const IPV6_DONTFRAG: c_int = 14;
/// `IPV6_PKTINFO`: the packet information of IPv6.
pub const IPV6_PKTINFO: c_int = 19;
/// `IPV6_V6ONLY`: the socket takes no IPv4 traffic.
pub const IPV6_V6ONLY: c_int = 27;
/// `IPV6_ECN`: the ECN codepoint of IPv6, as an `int` control message. As the option
/// `IPV6_RECVECN`, it delivers the codepoint with each received datagram.
pub const IPV6_ECN: c_int = 50;

/// `UDP_SEND_MSG_SIZE`: the segment size of a send that the stack or the device segments (USO), as a
/// `DWORD` control message.
pub const UDP_SEND_MSG_SIZE: c_int = 2;
/// `UDP_RECV_MAX_COALESCED_SIZE`: the most bytes of a coalesced receive (URO), as a `DWORD` option;
/// 0 turns coalescing off.
pub const UDP_RECV_MAX_COALESCED_SIZE: c_int = 3;
/// `UDP_COALESCED_INFO`: the segment size of a coalesced receive, as a `DWORD` control message.
pub const UDP_COALESCED_INFO: c_int = 3;

/// `MSG_TRUNC`: the datagram did not fit, and the stack dropped its rest.
pub const MSG_TRUNC: u32 = 0x100;
/// `MSG_CTRUNC`: the control messages did not fit.
pub const MSG_CTRUNC: u32 = 0x200;

/// `WSAEINVAL`: an argument is not valid, for example a segment size that the path cannot send.
pub const WSAEINVAL: c_int = 10022;
/// `WSAEWOULDBLOCK`: the call would wait.
pub const WSAEWOULDBLOCK: c_int = 10035;
/// `WSAEMSGSIZE`: the datagram is larger than the path MTU or the buffer.
pub const WSAEMSGSIZE: c_int = 10040;
/// `WSAEOPNOTSUPP`: the operation is not supported, for example a segmented send.
pub const WSAEOPNOTSUPP: c_int = 10045;

/// `SOCKET_ERROR`: the result of a failed call.
pub const SOCKET_ERROR: c_int = -1;

/// `SOCK_DGRAM`: datagrams, such as UDP.
pub const SOCK_DGRAM: c_int = 2;
/// `INVALID_SOCKET`: the result of a `socket` call that failed.
#[cfg(test)]
pub(crate) const INVALID_SOCKET: Socket = !0;
/// `IP_USER_MTU`: the upper bound of the IPv4 path MTU that the socket sets, with no privilege.
#[cfg(test)]
pub(crate) const IP_USER_MTU: c_int = 76;

/// `LPFN_WSARECVMSG`: `INT WSARecvMsg(SOCKET s, LPWSAMSG lpMsg, LPDWORD lpdwNumberOfBytesRecvd,
/// LPWSAOVERLAPPED lpOverlapped, LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine)`.
pub type WsaRecvMsg = unsafe extern "system" fn(
    s: Socket,
    msg: *mut WsaMsg,
    received: *mut u32,
    overlapped: *mut c_void,
    completion: *mut c_void,
) -> c_int;

#[cfg(feature = "std")]
#[expect(unsafe_code, reason = "ws2_32.dll gives these functions")]
#[link(name = "ws2_32")]
unsafe extern "system" {
    /// `int WSASendMsg(SOCKET Handle, LPWSAMSG lpMsg, DWORD dwFlags, LPDWORD lpNumberOfBytesSent,
    /// LPWSAOVERLAPPED lpOverlapped, LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine)`.
    pub(crate) fn WSASendMsg(
        handle: Socket,
        msg: *const WsaMsg,
        flags: u32,
        sent: *mut u32,
        overlapped: *mut c_void,
        completion: *mut c_void,
    ) -> c_int;
    /// `int WSAIoctl(SOCKET s, DWORD dwIoControlCode, LPVOID lpvInBuffer, DWORD cbInBuffer, LPVOID
    /// lpvOutBuffer, DWORD cbOutBuffer, LPDWORD lpcbBytesReturned, LPWSAOVERLAPPED lpOverlapped,
    /// LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine)`.
    pub fn WSAIoctl(
        s: Socket,
        code: u32,
        input: *const c_void,
        input_len: u32,
        output: *mut c_void,
        output_len: u32,
        returned: *mut u32,
        overlapped: *mut c_void,
        completion: *mut c_void,
    ) -> c_int;
    /// `int setsockopt(SOCKET s, int level, int optname, const char *optval, int optlen)`.
    pub fn setsockopt(s: Socket, level: c_int, name: c_int, value: *const c_char, len: c_int) -> c_int;
    /// `int getsockopt(SOCKET s, int level, int optname, char *optval, int *optlen)`.
    pub fn getsockopt(s: Socket, level: c_int, name: c_int, value: *mut c_char, len: *mut c_int) -> c_int;
    /// `int getsockname(SOCKET s, struct sockaddr *name, int *namelen)`.
    pub fn getsockname(s: Socket, name: *mut c_void, len: *mut c_int) -> c_int;
    /// `int getpeername(SOCKET s, struct sockaddr *name, int *namelen)`.
    pub fn getpeername(s: Socket, name: *mut c_void, len: *mut c_int) -> c_int;
    /// `SOCKET socket(int af, int type, int protocol)`. Only the tests make a socket: a dual-stack
    /// one, whose `IPV6_V6ONLY` must be off before the bind.
    #[cfg(test)]
    pub(crate) fn socket(af: c_int, kind: c_int, protocol: c_int) -> Socket;
    /// `int bind(SOCKET s, const struct sockaddr *name, int namelen)`.
    pub fn bind(s: Socket, name: *const c_void, len: c_int) -> c_int;
    /// `int WSAGetLastError(void)`.
    pub(crate) fn WSAGetLastError() -> c_int;
}
