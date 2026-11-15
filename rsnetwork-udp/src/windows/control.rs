//! The one encoding of the control messages of a datagram on Windows: the segment size (USO), the
//! ECN codepoint, and the local address on send; the segment size of a coalesced receive (URO), the
//! ECN codepoint, and the local address on receive.
//!
//! A `WSACMSGHDR` has a `SIZE_T` length, a level, and a type. Its data starts 16 bytes after it, and
//! each message starts at a multiple of 8 bytes, as on Linux.

use core::ffi::c_int;
use core::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use super::sys::{
    CMSG_HEADER, IP_ECN, IP_PKTINFO, IPPROTO_IP, IPPROTO_IPV6, IPPROTO_UDP, IPV6_ECN, IPV6_PKTINFO, UDP_COALESCED_INFO,
    UDP_SEND_MSG_SIZE,
};
use crate::datagram::{Ecn, Metadata, Transmit};

/// Storage for the control messages of one datagram, with the alignment of a `WSACMSGHDR`. It has
/// room for each control message of this crate.
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy)]
pub struct Control {
    bytes: [u8; Control::LEN],
}

impl Control {
    /// The size of the storage.
    pub const LEN: usize = 128;

    /// Storage with no control message.
    #[must_use]
    pub const fn new() -> Self {
        Self { bytes: [0; Self::LEN] }
    }

    /// The address of the storage, for the control buffer of a `WSAMSG`.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr()
    }

    /// Writes the control messages of `transmit`: the segment size, the ECN codepoint, and the
    /// source address. Gives their length, for the control buffer. An IPv4 destination, also an
    /// IPv4-mapped one, takes the IPv4 ECN message, and an IPv6 destination the IPv6 one. With no
    /// destination, the ECN codepoint goes in both messages, which the stack refuses
    /// (`WSAEINVAL`): [`send`](crate::send) gives the peer of a connected socket as the
    /// destination. The source address takes the message of its own family.
    pub fn encode(&mut self, transmit: &Transmit<'_>) -> usize {
        // Whether the destination is IPv4, or `None` when the socket fixes it.
        let ipv4 = transmit.destination.map(|destination| match destination {
            SocketAddr::V4(_) => true,
            SocketAddr::V6(address) => address.ip().to_ipv4_mapped().is_some(),
        });
        let mut len = 0;
        if let Some(size) = transmit.segment_size {
            len = self.push(len, IPPROTO_UDP, UDP_SEND_MSG_SIZE, &u32::from(size).to_ne_bytes());
        }
        if let Some(ecn) = transmit.ecn {
            let value = c_int::from(ecn as u8).to_ne_bytes();
            if ipv4 != Some(false) {
                len = self.push(len, IPPROTO_IP, IP_ECN, &value);
            }
            if ipv4 != Some(true) {
                len = self.push(len, IPPROTO_IPV6, IPV6_ECN, &value);
            }
        }
        match transmit.source.map(canonical) {
            Some(IpAddr::V4(source)) => {
                // `IN_PKTINFO` with no interface: the address comes first.
                let mut info = [0; 8];
                info[..4].copy_from_slice(&source.octets());
                len = self.push(len, IPPROTO_IP, IP_PKTINFO, &info);
            }
            Some(IpAddr::V6(source)) => {
                // `IN6_PKTINFO` with no interface.
                let mut info = [0; 20];
                info[..16].copy_from_slice(&source.octets());
                len = self.push(len, IPPROTO_IPV6, IPV6_PKTINFO, &info);
            }
            None => {}
        }
        len
    }

    /// Reads the control messages of a received datagram: the first `len` bytes, from the control
    /// buffer. A message that is not complete, or that this crate does not know, is skipped.
    #[must_use]
    pub fn decode(&self, len: usize) -> Metadata {
        let mut metadata = Metadata::default();
        let end = len.min(Self::LEN);
        let mut start = 0;
        while start + CMSG_HEADER <= end {
            let message_len = usize::from_ne_bytes(array(&self.bytes[start..start + 8]));
            let Some(stop) = start
                .checked_add(message_len)
                .filter(|&stop| message_len >= CMSG_HEADER && stop <= end)
            else {
                break;
            };
            let level = c_int::from_ne_bytes(array(&self.bytes[start + 8..start + 12]));
            let kind = c_int::from_ne_bytes(array(&self.bytes[start + 12..start + CMSG_HEADER]));
            let data = &self.bytes[start + CMSG_HEADER..stop];
            match (level, kind) {
                (IPPROTO_UDP, UDP_COALESCED_INFO) if data.len() >= 4 => {
                    metadata.segment_size = usize::try_from(u32::from_ne_bytes(array(data))).ok();
                }
                (IPPROTO_IP, IP_ECN) | (IPPROTO_IPV6, IPV6_ECN) if data.len() >= 4 => {
                    let bits = c_int::from_ne_bytes(array(data));
                    metadata.ecn = u8::try_from(bits & 0b11).ok().and_then(Ecn::from_bits);
                }
                (IPPROTO_IP, IP_PKTINFO) if data.len() >= 8 => {
                    metadata.destination = Some(IpAddr::V4(Ipv4Addr::from(array::<4>(data))));
                }
                // A dual-stack socket can give an IPv4 datagram here, at its IPv4-mapped address.
                (IPPROTO_IPV6, IPV6_PKTINFO) if data.len() >= 20 => {
                    metadata.destination = Some(canonical(IpAddr::V6(Ipv6Addr::from(array::<16>(data)))));
                }
                _ => {}
            }
            start += message_len.next_multiple_of(8);
        }
        metadata
    }

    /// Writes one control message at `start`, and gives the start of the next one.
    fn push(&mut self, start: usize, level: c_int, kind: c_int, data: &[u8]) -> usize {
        let message_len = CMSG_HEADER + data.len();
        self.bytes[start..start + 8].copy_from_slice(&message_len.to_ne_bytes());
        self.bytes[start + 8..start + 12].copy_from_slice(&level.to_ne_bytes());
        self.bytes[start + 12..start + CMSG_HEADER].copy_from_slice(&kind.to_ne_bytes());
        self.bytes[start + CMSG_HEADER..start + message_len].copy_from_slice(data);
        start + message_len.next_multiple_of(8)
    }
}

impl Default for Control {
    fn default() -> Self {
        Self::new()
    }
}

/// An IPv4-mapped IPv6 address as its IPv4 address; each other address as it is.
fn canonical(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(address, IpAddr::V4),
        IpAddr::V4(_) => address,
    }
}

/// The first `N` bytes of `bytes`, which has at least `N`.
fn array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut array = [0; N];
    array.copy_from_slice(&bytes[..N]);
    array
}

const _: () = {
    // The largest encoding: a segment size, an ECN codepoint in each family, and IPv6 packet
    // information.
    assert!((CMSG_HEADER + 4).next_multiple_of(8) * 3 + (CMSG_HEADER + 20).next_multiple_of(8) <= Control::LEN);
};
