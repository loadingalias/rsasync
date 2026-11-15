//! The datagram types of each target: the send and its metadata, and the received datagram and the
//! refusals of a send.

use core::net::{IpAddr, SocketAddr};
#[cfg(feature = "std")]
use core::{
    fmt,
    net::{Ipv4Addr, SocketAddrV4},
};
#[cfg(feature = "std")]
use std::io;

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
/// device sends as one datagram for each segment (GSO, or USO on Windows). The last segment can be shorter. macOS has no
/// segmentation offload, so a send there refuses each segment size.
#[derive(Debug, Clone, Copy)]
pub struct Transmit<'a> {
    /// The peer, or `None` for the peer of a connected socket. A dual-stack IPv6 socket sends to an
    /// IPv4 peer at its IPv4-mapped address. On a connected socket, the socket fixes the peer of
    /// each datagram, so `None` names that peer, and the send gives the kernel no address. macOS
    /// refuses an address on a connected socket (`EISCONN`).
    pub destination: Option<SocketAddr>,
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
    /// The size of each segment of a coalesced receive (GRO, or URO on Windows), or `None` for one
    /// datagram. The last segment can be shorter. It is always `None` on macOS, which has no GRO.
    pub segment_size: Option<usize>,
    /// The ECN codepoint, or `None` for Not-ECT or for a socket that does not report it.
    pub ecn: Option<Ecn>,
    /// The local address that the datagram came to, or `None` for a socket that does not report
    /// it. On a dual-stack socket, an IPv4 datagram gives its IPv4 address.
    pub destination: Option<IpAddr>,
}

/// The most datagrams of one [`send_batch`](crate::send_batch) or [`recv_batch`](crate::recv_batch).
#[cfg(feature = "std")]
pub const BATCH: usize = 32;

/// A send that the kernel refused. It is the payload of the `io::Error` of [`send`](crate::send) and
/// [`send_batch`](crate::send_batch): get it with [`Refused::of`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(feature = "std")]
pub enum Refused {
    /// The datagram is larger than the path MTU, and the don't-fragment option is on
    /// (`EMSGSIZE`, `WSAEMSGSIZE` on Windows). Send smaller datagrams. macOS also gives it for a datagram that is larger
    /// than the send buffer of the socket.
    TooLarge,
    /// The path cannot segment the batch. On Linux, the device cannot (`EIO` on a send with a
    /// segment size). On Windows, the stack refuses the segment size (`WSAEINVAL` or
    /// `WSAEOPNOTSUPP`). macOS has no segmentation offload. No datagram of it was sent. Send its
    /// segments as separate datagrams, for example with [`send_batch`](crate::send_batch), and send no segment size
    /// on this path again.
    NoSegmentation,
}

#[cfg(feature = "std")]
impl Refused {
    /// The refusal that `error` carries, or `None` for each other error.
    #[must_use]
    pub fn of(error: &io::Error) -> Option<Self> {
        error.get_ref()?.downcast_ref().copied()
    }
}

#[cfg(feature = "std")]
impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TooLarge => "the datagram is larger than the path MTU, and it cannot be fragmented",
            Self::NoSegmentation => "the path cannot segment the batch; send its segments as separate datagrams",
        })
    }
}

#[cfg(feature = "std")]
impl core::error::Error for Refused {}

/// A received datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(feature = "std")]
pub struct Received {
    /// The number of bytes in the buffer.
    pub len: usize,
    /// The peer. A dual-stack IPv6 socket gives an IPv4 peer as its IPv4-mapped address.
    pub source: SocketAddr,
    /// The datagram did not fit in the buffer, and the kernel dropped its rest. With the
    /// `macos-private-batch` feature, [`recv_batch`](crate::recv_batch) of macOS cannot tell a datagram that fits its
    /// buffer exactly from a longer one, and it reports both as truncated.
    pub truncated: bool,
    /// The segment size, the ECN codepoint, and the local address.
    pub metadata: Metadata,
}

#[cfg(feature = "std")]
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
