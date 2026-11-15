//! The run-time validation of segmentation offload on one socket path.

use core::net::SocketAddr;
use core::num::{NonZeroU8, NonZeroU16};
use std::vec;
use std::vec::Vec;

use crate::datagram::{Received, Transmit};

/// A probe of the segmentation offload of one socket path: a send with a segment size (GSO, USO)
/// and a receive that can coalesce (GRO, URO).
///
/// The probe sends a burst of segments of equal size, each filled with its own index, and checks
/// what the receiver gets: each segment once, with its bytes, either as its own datagram or in a
/// coalesced receive that reports the segment size. A receiver alone cannot tell coalesced data
/// with no segment size from one large datagram, so the known pattern is what makes the check
/// possible.
///
/// The probe validates a path whose two ends the caller controls, for example loopback or a paired
/// endpoint. A transport with its own acknowledgements, such as QUIC, can detect the loss of
/// offloaded datagrams itself, and then makes the same fallback. The probe has no clock: the caller
/// gives up at its own deadline with [`Probe::expired`].
///
/// On [`Verdict::Fallback`], the caller stops giving segment sizes on the path and turns coalesced
/// receive off, so later sends and receives are plain. The [`Reason`] names the fault.
#[derive(Debug, Clone)]
pub struct Probe {
    size: u16,
    contents: Vec<u8>,
    /// The segments that arrived, by index.
    seen: Vec<bool>,
    verdict: Option<Verdict>,
}

/// The result of a [`Probe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Each segment arrived once, with its bytes and its boundaries.
    Valid,
    /// The path failed the probe: send no segment size on it, and turn coalesced receive off.
    Fallback(Reason),
}

/// The fault of a path that failed a [`Probe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// A receive gave more bytes than one segment, with no segment size: the boundaries of the
    /// coalesced datagrams are lost.
    NoSegmentSize,
    /// A segment did not arrive before the deadline of the caller.
    Lost,
    /// A segment arrived with other bytes, another length, another segment size, a truncation, or
    /// a second time.
    Altered,
}

impl Probe {
    /// A probe of `segments` segments of `size` bytes each.
    #[must_use]
    pub fn new(segments: NonZeroU8, size: NonZeroU16) -> Self {
        let (segments, size) = (segments.get(), size.get());
        let mut contents = Vec::with_capacity(usize::from(segments) * usize::from(size));
        for index in 0..segments {
            contents.extend(core::iter::repeat_n(index, usize::from(size)));
        }
        Self {
            size,
            contents,
            seen: vec![false; usize::from(segments)],
            verdict: None,
        }
    }

    /// The send of the burst to `destination`: one transmit with the segment size of the probe.
    #[must_use]
    pub fn transmit(&self, destination: Option<SocketAddr>) -> Transmit<'_> {
        Transmit {
            destination,
            contents: &self.contents,
            segment_size: Some(self.size),
            ecn: None,
            source: None,
        }
    }

    /// Checks one receive: `received` and its `bytes`, the first `received.len` bytes of its
    /// buffer. Gives the verdict when the probe has one: [`Verdict::Valid`] when each segment
    /// arrived, or a fallback at the first fault.
    pub fn observe(&mut self, received: &Received, bytes: &[u8]) -> Option<Verdict> {
        if self.verdict.is_none() {
            self.verdict = self.check(received, bytes).err().map(Verdict::Fallback);
            if self.verdict.is_none() && self.seen.iter().all(|&seen| seen) {
                self.verdict = Some(Verdict::Valid);
            }
        }
        self.verdict
    }

    /// The verdict at the deadline of the caller: [`Reason::Lost`] while a segment is missing.
    pub fn expired(&mut self) -> Verdict {
        *self.verdict.get_or_insert(Verdict::Fallback(Reason::Lost))
    }

    fn check(&mut self, received: &Received, bytes: &[u8]) -> Result<(), Reason> {
        let size = usize::from(self.size);
        if received.truncated || bytes.len() != received.len {
            return Err(Reason::Altered);
        }
        match received.metadata.segment_size {
            Some(segment) if segment != size => return Err(Reason::Altered),
            None if bytes.len() > size => return Err(Reason::NoSegmentSize),
            _ => {}
        }
        for segment in bytes.chunks(size) {
            let index = usize::from(segment[0]);
            let fresh = self.seen.get(index).is_some_and(|&seen| !seen);
            if segment.len() != size || !fresh || segment.iter().any(|&byte| usize::from(byte) != index) {
                return Err(Reason::Altered);
            }
            self.seen[index] = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
