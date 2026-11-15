use core::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use core::num::{NonZeroU8, NonZeroU16};
use std::vec::Vec;

use super::{Probe, Reason, Verdict};
use crate::{Metadata, Received};

const SIZE: usize = 100;

fn three_segments() -> Probe {
    Probe::new(NonZeroU8::new(3).unwrap(), NonZeroU16::new(100).unwrap())
}

/// A receive of the `segments`, each `SIZE` bytes of its index, with `segment_size` in its metadata.
fn arrival(segments: &[u8], segment_size: Option<usize>) -> (Received, Vec<u8>) {
    let bytes: Vec<u8> = segments.iter().flat_map(|&index| [index; SIZE]).collect();
    let received = Received {
        len: bytes.len(),
        source: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1)),
        truncated: false,
        metadata: Metadata {
            segment_size,
            ..Metadata::default()
        },
    };
    (received, bytes)
}

/// Each segment arrives once, as its own datagram in any order, or in a coalesced receive that
/// reports the segment size: the path is valid.
#[test]
fn each_segment_once_with_its_boundaries_is_valid() {
    let mut probe = three_segments();
    for (index, verdict) in [(2, None), (0, None), (1, Some(Verdict::Valid))] {
        let (received, bytes) = arrival(&[index], None);
        assert_eq!(probe.observe(&received, &bytes), verdict);
    }
    let mut probe = three_segments();
    let (received, bytes) = arrival(&[0, 1], Some(SIZE));
    assert_eq!(probe.observe(&received, &bytes), None);
    let (received, bytes) = arrival(&[2], None);
    assert_eq!(probe.observe(&received, &bytes), Some(Verdict::Valid));
}

/// Coalesced data with no segment size is not one datagram: its boundaries are lost, and the path
/// falls back with that reason.
#[test]
fn coalesced_data_with_no_segment_size_falls_back() {
    let mut probe = three_segments();
    let (received, bytes) = arrival(&[0, 1, 2], None);
    assert_eq!(
        probe.observe(&received, &bytes),
        Some(Verdict::Fallback(Reason::NoSegmentSize))
    );
}

/// A segment that does not arrive before the deadline is lost, and a segment with other bytes, a
/// truncation, or a second arrival is altered.
#[test]
fn a_lost_or_altered_segment_falls_back() {
    let mut probe = three_segments();
    let (received, bytes) = arrival(&[0, 1], Some(SIZE));
    assert_eq!(probe.observe(&received, &bytes), None);
    assert_eq!(probe.expired(), Verdict::Fallback(Reason::Lost));

    let mut probe = three_segments();
    let (received, mut bytes) = arrival(&[0], None);
    bytes[SIZE - 1] = 7;
    assert_eq!(
        probe.observe(&received, &bytes),
        Some(Verdict::Fallback(Reason::Altered))
    );

    let mut probe = three_segments();
    let (received, bytes) = arrival(&[0], None);
    assert_eq!(probe.observe(&received, &bytes), None);
    assert_eq!(
        probe.observe(&received, &bytes),
        Some(Verdict::Fallback(Reason::Altered))
    );

    let mut probe = three_segments();
    let (mut received, bytes) = arrival(&[0], None);
    received.truncated = true;
    assert_eq!(
        probe.observe(&received, &bytes),
        Some(Verdict::Fallback(Reason::Altered))
    );
}
