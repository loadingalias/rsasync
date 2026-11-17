//! The declarations of the datagram calls, their message headers, and their control messages on macOS.
//!
//! Each item agrees with the macOS 27.2 SDK (`MacOSX27.2.sdk`) on aarch64: `usr/include/sys/socket.h`,
//! `sys/_types/_iovec_t.h`, `netinet/in.h`, `netinet/tcp.h`, `netinet6/in6.h` with `__APPLE_USE_RFC_3542`, and
//! `sys/errno.h`.
//! The SDK does not declare the private calls `sendmsg_x` and `recvmsg_x` and their `struct msghdr_x`. They agree
//! with `bsd/sys/socket_private.h` of xnu-12377.121.6, the newest published xnu source, and
//! `usr/lib/system/libsystem_kernel.tbd` of the SDK exports both calls.

use core::ffi::{c_int, c_uint, c_void};
use core::mem::{offset_of, size_of};

/// `struct iovec`: one region of a datagram.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct IoVec {
    /// The address of the region.
    pub iov_base: *mut c_void,
    /// The length of the region.
    pub iov_len: usize,
}

/// `struct msghdr`: one datagram, its address, and its control messages.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MsgHdr {
    /// The socket address of the peer, or null.
    pub msg_name: *mut c_void,
    /// The length of the address.
    pub msg_namelen: u32,
    /// The regions of the datagram.
    pub msg_iov: *mut IoVec,
    /// The number of regions.
    pub msg_iovlen: c_int,
    /// The control messages, or null.
    pub msg_control: *mut c_void,
    /// The length of the control messages.
    pub msg_controllen: u32,
    /// The flags of a received datagram, for example [`MSG_TRUNC`].
    pub msg_flags: c_int,
}

/// The size of `struct cmsghdr`: a `socklen_t` length, a level, and a type. Its data follows it, at a multiple of
/// 4 bytes (`__DARWIN_ALIGN32`).
pub const CMSG_HEADER: usize = 12;

/// `struct in_pktinfo`: the interface and the addresses of an IPv4 datagram.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct InPktInfo {
    /// The interface index, or 0.
    pub ipi_ifindex: c_uint,
    /// On send, the source address; on receive, the local address of the route.
    pub ipi_spec_dst: [u8; 4],
    /// On receive, the destination address of the datagram.
    pub ipi_addr: [u8; 4],
}

/// `struct in6_pktinfo`: the address and the interface of an IPv6 datagram.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct In6PktInfo {
    /// On send, the source address; on receive, the destination address.
    pub ipi6_addr: [u8; 16],
    /// The interface index, or 0.
    pub ipi6_ifindex: c_uint,
}

const _: () = {
    assert!(size_of::<IoVec>() == 16);
    assert!(offset_of!(IoVec, iov_len) == 8);
    assert!(size_of::<MsgHdr>() == 48);
    assert!(offset_of!(MsgHdr, msg_namelen) == 8);
    assert!(offset_of!(MsgHdr, msg_iov) == 16);
    assert!(offset_of!(MsgHdr, msg_iovlen) == 24);
    assert!(offset_of!(MsgHdr, msg_control) == 32);
    assert!(offset_of!(MsgHdr, msg_controllen) == 40);
    assert!(offset_of!(MsgHdr, msg_flags) == 44);
    assert!(size_of::<InPktInfo>() == 12);
    assert!(offset_of!(InPktInfo, ipi_spec_dst) == 4);
    assert!(offset_of!(InPktInfo, ipi_addr) == 8);
    assert!(size_of::<In6PktInfo>() == 20);
    assert!(offset_of!(In6PktInfo, ipi6_ifindex) == 16);
};

/// `SOCK_STREAM`: a byte stream, such as TCP.
pub const SOCK_STREAM: c_int = 1;
/// `SOCK_DGRAM`: datagrams, such as UDP.
pub const SOCK_DGRAM: c_int = 2;
/// `SHUT_WR`: end the write side of a stream; the peer reads the end of the stream after the bytes that came
/// before.
pub const SHUT_WR: c_int = 1;

/// `SOL_SOCKET`, the level of the socket options.
pub const SOL_SOCKET: c_int = 0xffff;
/// `SO_REUSEADDR`: a listener can bind an address whose earlier connections still wait.
pub const SO_REUSEADDR: c_int = 0x0004;
/// `SO_KEEPALIVE`: the stack probes an idle connection.
pub const SO_KEEPALIVE: c_int = 0x0008;
/// `SO_REUSEPORT`: sockets can bind the same address and port.
pub const SO_REUSEPORT: c_int = 0x0200;
/// `SO_SNDBUF`: the send buffer size.
pub const SO_SNDBUF: c_int = 0x1001;
/// `SO_RCVBUF`: the receive buffer size.
pub const SO_RCVBUF: c_int = 0x1002;
/// `IPPROTO_TCP`, the level of the TCP options (`netinet/in.h`).
pub const IPPROTO_TCP: c_int = 6;
/// `TCP_NODELAY`: send small segments at once, with no wait to coalesce them (`netinet/tcp.h`).
pub const TCP_NODELAY: c_int = 0x01;

/// `IPPROTO_IP`, the level of the IPv4 options.
pub const IPPROTO_IP: c_int = 0;
/// `IPPROTO_IPV6`, the level of the IPv6 options.
pub const IPPROTO_IPV6: c_int = 41;

/// `IP_TOS`: the traffic class byte of IPv4, with the ECN codepoint in its low 2 bits. A send takes it as an
/// `int`.
pub const IP_TOS: c_int = 3;
/// `IP_PKTINFO`: the packet information of IPv4. As a socket option it is also `IP_RECVPKTINFO`: deliver the packet
/// information with each received datagram.
pub const IP_PKTINFO: c_int = 26;
/// `IP_RECVTOS`: deliver the traffic class byte with each received datagram, in a control message of this type.
pub const IP_RECVTOS: c_int = 27;
/// `IP_DONTFRAG`: refuse an IPv4 datagram above the path MTU.
pub const IP_DONTFRAG: c_int = 28;

/// `IPV6_V6ONLY`: the socket takes no IPv4 traffic.
pub const IPV6_V6ONLY: c_int = 27;
/// `IPV6_RECVTCLASS`: deliver [`IPV6_TCLASS`] with each received datagram.
pub const IPV6_RECVTCLASS: c_int = 35;
/// `IPV6_TCLASS`: the traffic class of IPv6, with the ECN codepoint in its low 2 bits.
pub const IPV6_TCLASS: c_int = 36;
/// `IPV6_USE_MIN_MTU`: send at the minimum IPv6 MTU, 1280 bytes.
pub const IPV6_USE_MIN_MTU: c_int = 42;
/// `IPV6_PKTINFO` (`IPV6_3542PKTINFO`): the packet information of IPv6.
pub const IPV6_PKTINFO: c_int = 46;
/// `IPV6_RECVPKTINFO`: deliver [`IPV6_PKTINFO`] with each received datagram.
pub const IPV6_RECVPKTINFO: c_int = 61;
/// `IPV6_DONTFRAG`: refuse an IPv6 datagram above the path MTU.
pub const IPV6_DONTFRAG: c_int = 62;

/// `MSG_TRUNC`: the datagram did not fit, and the kernel dropped its rest.
pub const MSG_TRUNC: c_int = 0x10;
/// `MSG_CTRUNC`: the control messages did not fit.
pub const MSG_CTRUNC: c_int = 0x20;
/// `MSG_DONTWAIT`: the call does not block.
pub const MSG_DONTWAIT: c_int = 0x80;

/// `EINTR`.
pub const EINTR: c_int = 4;
/// `EMSGSIZE`: the datagram is larger than the path MTU with the don't-fragment option on, or larger than the
/// send buffer of the socket.
pub const EMSGSIZE: c_int = 40;

#[cfg(feature = "std")]
#[expect(unsafe_code, reason = "the C library of macOS gives these functions")]
unsafe extern "C" {
    /// `ssize_t sendmsg(int, const struct msghdr *, int)`.
    pub fn sendmsg(sockfd: c_int, msg: *const MsgHdr, flags: c_int) -> isize;
    /// `ssize_t recvmsg(int, struct msghdr *, int)`.
    pub fn recvmsg(sockfd: c_int, msg: *mut MsgHdr, flags: c_int) -> isize;
    /// `int setsockopt(int, int, int, const void *, socklen_t)`.
    pub fn setsockopt(sockfd: c_int, level: c_int, optname: c_int, optval: *const c_void, optlen: u32) -> c_int;
    /// `int getsockopt(int, int, int, void *restrict, socklen_t *restrict)`.
    pub fn getsockopt(sockfd: c_int, level: c_int, optname: c_int, optval: *mut c_void, optlen: *mut u32) -> c_int;
    /// `int getsockname(int, struct sockaddr *restrict, socklen_t *restrict)`.
    pub fn getsockname(sockfd: c_int, addr: *mut c_void, addrlen: *mut u32) -> c_int;
    /// `int socket(int, int, int)`.
    pub fn socket(domain: c_int, kind: c_int, protocol: c_int) -> c_int;
    /// `int bind(int, const struct sockaddr *, socklen_t)`.
    pub fn bind(sockfd: c_int, addr: *const c_void, addrlen: u32) -> c_int;
    /// `int listen(int, int)`.
    pub fn listen(sockfd: c_int, backlog: c_int) -> c_int;
}

/// `struct msghdr_x`: one datagram of a batch of `sendmsg_x` or `recvmsg_x`. A receive writes the length of
/// the datagram in `msg_datalen`.
#[cfg(feature = "macos-private-batch")]
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MsgHdrX {
    /// The socket address of the peer, or null.
    pub msg_name: *mut c_void,
    /// The length of the address.
    pub msg_namelen: u32,
    /// The regions of the datagram.
    pub msg_iov: *mut IoVec,
    /// The number of regions.
    pub msg_iovlen: c_int,
    /// The control messages, or null.
    pub msg_control: *mut c_void,
    /// The length of the control messages.
    pub msg_controllen: u32,
    /// The flags of a received datagram.
    pub msg_flags: c_int,
    /// The length of a received datagram. A send ignores it.
    pub msg_datalen: usize,
}

#[cfg(feature = "macos-private-batch")]
const _: () = {
    assert!(size_of::<MsgHdrX>() == 56);
    assert!(offset_of!(MsgHdrX, msg_flags) == 44);
    assert!(offset_of!(MsgHdrX, msg_datalen) == 48);
};

#[cfg(all(feature = "std", feature = "macos-private-batch"))]
#[expect(unsafe_code, reason = "libsystem_kernel of macOS gives these private calls")]
unsafe extern "C" {
    /// `ssize_t sendmsg_x(int s, const struct msghdr_x *msgp, u_int cnt, int flags)`: sends the `cnt` datagrams of
    /// `msgp`, and gives the number that it sent from the first.
    pub(crate) fn sendmsg_x(s: c_int, msgp: *const MsgHdrX, cnt: c_uint, flags: c_int) -> isize;
    /// `ssize_t recvmsg_x(int s, const struct msghdr_x *msgp, u_int cnt, int flags)`: receives up to `cnt`
    /// datagrams, and gives their number. The kernel writes the received fields of each header, also through the
    /// `const` pointer.
    pub(crate) fn recvmsg_x(s: c_int, msgp: *const MsgHdrX, cnt: c_uint, flags: c_int) -> isize;
}
