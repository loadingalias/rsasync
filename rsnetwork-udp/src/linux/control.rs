//! The one encoding of the control messages of a datagram: the segment size, the ECN codepoint,
//! and the local address on send; the segment size, the ECN codepoint, and the local address on
//! receive.

use core::ffi::c_int;
use core::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use super::sys::{
    CMSG_HEADER, IP_PKTINFO, IP_TOS, IPPROTO_IP, IPPROTO_IPV6, IPV6_PKTINFO, IPV6_TCLASS, SOL_UDP, UDP_GRO, UDP_SEGMENT,
};

/// An ECN codepoint other than Not-ECT (RFC 3168), in the low 2 bits of the traffic class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ecn {
    /// ECT(1).
    Ect1 = 0b01,
    /// ECT(0).
    Ect0 = 0b10,
    /// CE: congestion experienced.
    Ce = 0b11,
}

impl Ecn {
    /// The codepoint in the low 2 bits of a traffic class, or `None` for Not-ECT.
    #[must_use]
    pub const fn from_bits(bits: u8) -> Option<Self> {
        match bits & 0b11 {
            0b01 => Some(Self::Ect1),
            0b10 => Some(Self::Ect0),
            0b11 => Some(Self::Ce),
            _ => None,
        }
    }
}

/// One send: one datagram, or one batch of segments of `segment_size` bytes that the kernel or the
/// device sends as one datagram for each segment (GSO). The last segment can be shorter.
#[derive(Debug, Clone, Copy)]
pub struct Transmit<'a> {
    /// The peer. A dual-stack IPv6 socket sends to an IPv4 peer at its IPv4-mapped address.
    pub destination: SocketAddr,
    /// The bytes of the datagram or of the segments.
    pub contents: &'a [u8],
    /// The size of each segment, or `None` for one datagram.
    pub segment_size: Option<u16>,
    /// The ECN codepoint, or `None` for Not-ECT.
    pub ecn: Option<Ecn>,
    /// The local source address, or `None` to let the route choose it.
    pub source: Option<IpAddr>,
}

impl Transmit<'_> {
    /// The number of datagrams that the send makes.
    #[must_use]
    pub const fn datagrams(&self) -> usize {
        match self.segment_size {
            Some(size) if size > 0 => self.contents.len().div_ceil(size as usize),
            _ => 1,
        }
    }
}

/// The control messages of a received datagram.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Metadata {
    /// The size of each segment of a coalesced receive (GRO), or `None` for one datagram. The
    /// last segment can be shorter.
    pub segment_size: Option<usize>,
    /// The ECN codepoint, or `None` for Not-ECT or for a socket that does not report it.
    pub ecn: Option<Ecn>,
    /// The local address that the datagram came to, or `None` for a socket that does not report
    /// it. On a dual-stack socket, an IPv4 datagram gives its IPv4 address.
    pub destination: Option<IpAddr>,
}

/// Storage for the control messages of one datagram, with the alignment of a `cmsghdr`. It has
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

    /// The address of the storage, for `msg_control`.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr()
    }

    /// Writes the control messages of `transmit`: the segment size, the ECN codepoint, and the
    /// source address. Gives their length, for `msg_controllen`. An IPv4 destination, also an
    /// IPv4-mapped one, takes the IPv4 messages.
    pub fn encode(&mut self, transmit: &Transmit<'_>) -> usize {
        let ipv4 = match transmit.destination {
            SocketAddr::V4(_) => true,
            SocketAddr::V6(address) => address.ip().to_ipv4_mapped().is_some(),
        };
        let mut len = 0;
        if let Some(size) = transmit.segment_size {
            len = self.push(len, SOL_UDP, UDP_SEGMENT, &size.to_ne_bytes());
        }
        if let Some(ecn) = transmit.ecn {
            let value = c_int::from(ecn as u8).to_ne_bytes();
            len = if ipv4 {
                self.push(len, IPPROTO_IP, IP_TOS, &value)
            } else {
                self.push(len, IPPROTO_IPV6, IPV6_TCLASS, &value)
            };
        }
        match transmit.source.map(canonical) {
            Some(IpAddr::V4(source)) => {
                // `struct in_pktinfo` with no interface: `ipi_spec_dst` is the source.
                let mut info = [0; 12];
                info[4..8].copy_from_slice(&source.octets());
                len = self.push(len, IPPROTO_IP, IP_PKTINFO, &info);
            }
            Some(IpAddr::V6(source)) => {
                // `struct in6_pktinfo` with no interface.
                let mut info = [0; 20];
                info[..16].copy_from_slice(&source.octets());
                len = self.push(len, IPPROTO_IPV6, IPV6_PKTINFO, &info);
            }
            None => {}
        }
        len
    }

    /// Reads the control messages of a received datagram: the first `len` bytes, from
    /// `msg_controllen`. A message that is not complete, or that this crate does not know, is
    /// skipped.
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
            let kind = c_int::from_ne_bytes(array(&self.bytes[start + 12..start + 16]));
            let data = &self.bytes[start + CMSG_HEADER..stop];
            match (level, kind) {
                (SOL_UDP, UDP_GRO) if data.len() >= 4 => {
                    metadata.segment_size = usize::try_from(c_int::from_ne_bytes(array(data))).ok();
                }
                // The kernel delivers `IP_TOS` as one byte, and `IPV6_TCLASS` as an `int`.
                (IPPROTO_IP, IP_TOS) if !data.is_empty() => metadata.ecn = Ecn::from_bits(data[0]),
                (IPPROTO_IPV6, IPV6_TCLASS) if data.len() >= 4 => {
                    let class = c_int::from_ne_bytes(array(data));
                    metadata.ecn = u8::try_from(class & 0b11).ok().and_then(Ecn::from_bits);
                }
                (IPPROTO_IP, IP_PKTINFO) if data.len() >= 12 => {
                    metadata.destination = Some(IpAddr::V4(Ipv4Addr::from(array::<4>(&data[8..12]))));
                }
                (IPPROTO_IPV6, IPV6_PKTINFO) if data.len() >= 20 => {
                    metadata.destination = Some(IpAddr::V6(Ipv6Addr::from(array::<16>(data))));
                }
                _ => {}
            }
            start += aligned(message_len);
        }
        metadata
    }

    /// Writes one control message at `start`, and gives the start of the next one.
    fn push(&mut self, start: usize, level: c_int, kind: c_int, data: &[u8]) -> usize {
        let message_len = CMSG_HEADER + data.len();
        self.bytes[start..start + 8].copy_from_slice(&message_len.to_ne_bytes());
        self.bytes[start + 8..start + 12].copy_from_slice(&level.to_ne_bytes());
        self.bytes[start + 12..start + 16].copy_from_slice(&kind.to_ne_bytes());
        self.bytes[start + CMSG_HEADER..start + message_len].copy_from_slice(data);
        start + aligned(message_len)
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

/// `CMSG_ALIGN`: the length rounded up to the alignment of a `size_t`.
const fn aligned(len: usize) -> usize {
    len.next_multiple_of(8)
}

/// The first `N` bytes of `bytes`, which has at least `N`.
fn array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut array = [0; N];
    array.copy_from_slice(&bytes[..N]);
    array
}

const _: () = {
    // The largest encoding: a segment size, an ECN codepoint, and IPv6 packet information.
    assert!(aligned(CMSG_HEADER + 2) + aligned(CMSG_HEADER + 4) + aligned(CMSG_HEADER + 20) <= Control::LEN);
};
