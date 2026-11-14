//! The declarations of the datagram calls, their message headers, and their control messages on
//! Linux, and of the socket calls and options that the networking family and its runtimes share.
//!
//! Each item agrees with `include/uapi/linux/socket.h`, `in.h`, `in6.h`, `udp.h`, `tcp.h`, `uio.h`,
//! `include/uapi/asm-generic/socket.h`, `include/linux/socket.h`, `include/linux/net.h`, and
//! `include/uapi/asm-generic/errno.h` of Linux 6.1, on x86_64 and aarch64. The structures have the layout of the kernel. glibc and musl give the functions; musl
//! declares the `size_t` length fields as an `int` and padding, which is the same memory on these
//! little-endian targets.

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

/// `struct msghdr` (`struct user_msghdr` of the kernel): one datagram, its address, and its control
/// messages.
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
    pub msg_iovlen: usize,
    /// The control messages, or null.
    pub msg_control: *mut c_void,
    /// The length of the control messages.
    pub msg_controllen: usize,
    /// The flags of a received datagram, for example [`MSG_TRUNC`].
    pub msg_flags: c_int,
}

/// `struct mmsghdr`: one datagram of a batch, and the number of bytes that the kernel sent or
/// received for it.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MmsgHdr {
    /// The datagram.
    pub msg_hdr: MsgHdr,
    /// The number of bytes.
    pub msg_len: c_uint,
}

/// The size of `struct cmsghdr`: a `size_t` length, a level, and a type. Its data follows it.
pub const CMSG_HEADER: usize = 16;

/// `struct in_pktinfo`: the interface and the addresses of an IPv4 datagram.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct InPktInfo {
    /// The interface index, or 0.
    pub ipi_ifindex: c_int,
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
    pub ipi6_ifindex: c_int,
}

const _: () = {
    assert!(size_of::<IoVec>() == 16);
    assert!(size_of::<MsgHdr>() == 56);
    assert!(offset_of!(MsgHdr, msg_iov) == 16);
    assert!(offset_of!(MsgHdr, msg_iovlen) == 24);
    assert!(offset_of!(MsgHdr, msg_control) == 32);
    assert!(offset_of!(MsgHdr, msg_controllen) == 40);
    assert!(offset_of!(MsgHdr, msg_flags) == 48);
    assert!(size_of::<MmsgHdr>() == 64);
    assert!(offset_of!(MmsgHdr, msg_len) == 56);
    assert!(size_of::<InPktInfo>() == 12);
    assert!(size_of::<In6PktInfo>() == 20);
};

/// `SOCK_STREAM` (`include/linux/net.h`): a byte stream, such as TCP.
pub const SOCK_STREAM: c_int = 1;
/// `SOCK_DGRAM` (`include/linux/net.h`): datagrams, such as UDP.
pub const SOCK_DGRAM: c_int = 2;
/// `SOCK_NONBLOCK` (`include/linux/net.h`, `O_NONBLOCK`): the new descriptor does not block.
pub const SOCK_NONBLOCK: c_int = 0o4_000;
/// `SOCK_CLOEXEC` (`include/linux/net.h`, `O_CLOEXEC`): the new descriptor closes at an exec.
pub const SOCK_CLOEXEC: c_int = 0o2_000_000;
/// `SHUT_WR` (`include/linux/net.h`): end the write side of a stream; the peer reads the end of
/// the stream after the bytes that came before.
pub const SHUT_WR: c_int = 1;

/// `SOL_SOCKET` (`include/uapi/asm-generic/socket.h`), the level of the socket options.
pub const SOL_SOCKET: c_int = 1;
/// `SO_REUSEADDR`: a listener can bind an address whose earlier connections still wait.
pub const SO_REUSEADDR: c_int = 2;
/// `SO_SNDBUF`: the send buffer size. Linux doubles the value that it takes, for its own
/// bookkeeping, and gives the doubled value back.
pub const SO_SNDBUF: c_int = 7;
/// `SO_RCVBUF`: the receive buffer size, which Linux doubles as it does [`SO_SNDBUF`].
pub const SO_RCVBUF: c_int = 8;
/// `SO_KEEPALIVE`: send keepalive probes on an idle connection.
pub const SO_KEEPALIVE: c_int = 9;
/// `SO_REUSEPORT`: sockets of one user can bind the same address, and the kernel spreads the
/// connections or datagrams between them.
pub const SO_REUSEPORT: c_int = 15;
/// `IPPROTO_TCP` (`include/uapi/linux/in.h`), the level of the TCP options.
pub const IPPROTO_TCP: c_int = 6;
/// `TCP_NODELAY` (`include/uapi/linux/tcp.h`): send small segments at once, with no Nagle delay.
pub const TCP_NODELAY: c_int = 1;

/// `IPPROTO_IP`, the level of the IPv4 options.
pub const IPPROTO_IP: c_int = 0;
/// `IPPROTO_IPV6`, the level of the IPv6 options.
pub const IPPROTO_IPV6: c_int = 41;
/// `SOL_UDP` (`IPPROTO_UDP`), the level of the UDP options.
pub const SOL_UDP: c_int = 17;

/// `IP_TOS`: the traffic class byte of IPv4, with the ECN codepoint in its low 2 bits.
pub const IP_TOS: c_int = 1;
/// `IP_PKTINFO`: the packet information of IPv4, on send and, as an option, on receive.
pub const IP_PKTINFO: c_int = 8;
/// `IP_MTU_DISCOVER`: the path MTU discovery mode of IPv4.
pub const IP_MTU_DISCOVER: c_int = 10;
/// `IP_RECVTOS`: deliver [`IP_TOS`] with each received datagram.
pub const IP_RECVTOS: c_int = 13;
/// `IP_PMTUDISC_DO`: set the don't-fragment bit, and refuse a datagram above the path MTU.
pub const IP_PMTUDISC_DO: c_int = 2;

/// `IPV6_MTU_DISCOVER`: the path MTU discovery mode of IPv6.
pub const IPV6_MTU_DISCOVER: c_int = 23;
/// `IPV6_MTU`: the MTU of the socket, at most the path MTU.
pub const IPV6_MTU: c_int = 24;
/// `IPV6_V6ONLY`: the socket takes no IPv4 traffic.
pub const IPV6_V6ONLY: c_int = 26;
/// `IPV6_RECVPKTINFO`: deliver [`IPV6_PKTINFO`] with each received datagram.
pub const IPV6_RECVPKTINFO: c_int = 49;
/// `IPV6_PKTINFO`: the packet information of IPv6.
pub const IPV6_PKTINFO: c_int = 50;
/// `IPV6_DONTFRAG`: refuse a datagram above the path MTU.
pub const IPV6_DONTFRAG: c_int = 62;
/// `IPV6_RECVTCLASS`: deliver [`IPV6_TCLASS`] with each received datagram.
pub const IPV6_RECVTCLASS: c_int = 66;
/// `IPV6_TCLASS`: the traffic class of IPv6, with the ECN codepoint in its low 2 bits.
pub const IPV6_TCLASS: c_int = 67;
/// `IPV6_PMTUDISC_DO`: refuse a datagram above the path MTU.
pub const IPV6_PMTUDISC_DO: c_int = 2;

/// `UDP_SEGMENT`: the segment size of a send that the kernel or the device segments (GSO).
pub const UDP_SEGMENT: c_int = 103;
/// `UDP_GRO`: deliver coalesced datagrams, with their segment size (GRO).
pub const UDP_GRO: c_int = 104;

/// `MSG_CTRUNC`: the control messages did not fit.
pub const MSG_CTRUNC: c_int = 0x8;
/// `MSG_TRUNC`: the datagram did not fit, and the kernel dropped its rest.
pub const MSG_TRUNC: c_int = 0x20;
/// `MSG_DONTWAIT`: the call does not block.
pub const MSG_DONTWAIT: c_int = 0x40;

/// `EINTR`.
pub const EINTR: c_int = 4;
/// `EIO`: on a send with a segment size, the device cannot segment it.
pub const EIO: c_int = 5;
/// `EMSGSIZE`: the datagram is larger than the path MTU, and it cannot be fragmented.
pub const EMSGSIZE: c_int = 90;

#[cfg(feature = "std")]
#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    /// `ssize_t sendmsg(int sockfd, const struct msghdr *msg, int flags)`.
    pub(crate) fn sendmsg(sockfd: c_int, msg: *const MsgHdr, flags: c_int) -> isize;
    /// `ssize_t recvmsg(int sockfd, struct msghdr *msg, int flags)`.
    pub(crate) fn recvmsg(sockfd: c_int, msg: *mut MsgHdr, flags: c_int) -> isize;
    /// `int sendmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen, int flags)`.
    pub(crate) fn sendmmsg(sockfd: c_int, msgvec: *mut MmsgHdr, vlen: c_uint, flags: c_int) -> c_int;
    /// `int recvmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen, int flags,
    /// struct timespec *timeout)`.
    pub(crate) fn recvmmsg(
        sockfd: c_int,
        msgvec: *mut MmsgHdr,
        vlen: c_uint,
        flags: c_int,
        timeout: *mut c_void,
    ) -> c_int;
    /// `int setsockopt(int sockfd, int level, int optname, const void *optval, socklen_t optlen)`.
    pub fn setsockopt(sockfd: c_int, level: c_int, optname: c_int, optval: *const c_void, optlen: u32) -> c_int;
    /// `int getsockopt(int sockfd, int level, int optname, void *optval, socklen_t *optlen)`.
    pub fn getsockopt(sockfd: c_int, level: c_int, optname: c_int, optval: *mut c_void, optlen: *mut u32) -> c_int;
    /// `int getsockname(int sockfd, struct sockaddr *addr, socklen_t *addrlen)`.
    pub fn getsockname(sockfd: c_int, addr: *mut c_void, addrlen: *mut u32) -> c_int;
    /// `int socket(int domain, int type, int protocol)`.
    pub fn socket(domain: c_int, kind: c_int, protocol: c_int) -> c_int;
    /// `int bind(int sockfd, const struct sockaddr *addr, socklen_t addrlen)`.
    pub fn bind(sockfd: c_int, addr: *const c_void, addrlen: u32) -> c_int;
    /// `int listen(int sockfd, int backlog)`.
    pub fn listen(sockfd: c_int, backlog: c_int) -> c_int;
}
