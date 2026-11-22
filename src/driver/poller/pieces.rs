//! The stream sends of macOS, in calls that XNU runs whole or not at all.
//!
//! XNU `sosend` (`bsd/kern/uipc_socket.c`) gives the bytes of one call to the protocol in chunks of
//! at most `sosendmaxchain` (65536) bytes. A failed allocation of a later chunk returns `ENOBUFS`,
//! and `sosendcheck` can return `ENOMEM` before one. `sendit` (`bsd/kern/uipc_syscalls.c`) then
//! gives the error and no count, although the earlier chunks are in the stream. So the poller sends
//! a stream in calls of at most one chunk, and it sets the send low-water mark to that size. Then
//! `sosendcheck` gives `EWOULDBLOCK` unless the whole call fits, and each call sends all of its bytes
//! or none: a refusal of memory means that no byte of the call went.
//!
//! The calls also keep the send buffer as full as one large call does. XNU `tcp_output`
//! (`bsd/netinet/tcp_output.c`) grows the send buffer of TCP only while it is at least 7/8 full, so
//! a call is at most 1/8 of the default buffer (`net.inet.tcp.sendspace`, 128 KiB). With 64 KiB
//! calls, the buffer stays at 128 KiB.

use core::ffi::{c_int, c_void};
use core::num::NonZeroUsize;
use core::ptr;
use std::os::fd::RawFd;

use rsnetwork_udp::sys::{IoVec, MSG_DONTWAIT, MsgHdr, SOCK_STREAM, SOL_SOCKET, getsockopt, sendmsg, setsockopt};

use super::{counted, errno, pointer};
use crate::uring::Sqe;
use crate::uring::sys as ring;

/// The most bytes of one call: 1/8 of the default send buffer of TCP, below the 64 KiB of
/// `sosendmaxchain`.
const PIECE: NonZeroUsize = NonZeroUsize::new(16 << 10).expect("the piece is not zero");

/// The most regions of one call. A window of more regions sends fewer bytes in its call.
const REGIONS: usize = 64;

/// The bytes after which one send step ends: 8 MiB, the largest send buffer of macOS
/// (`kern.ipc.maxsockbuf`). So a peer that reads as fast as the calls write stops no turn.
pub(super) const STEP_BYTES: usize = 8 << 20;

/// `SO_SNDLOWAT` and `SO_TYPE` of `sys/socket.h` of the macOS 27.2 SDK.
const SO_SNDLOWAT: c_int = 0x1003;
const SO_TYPE: c_int = 0x1008;

/// How the poller runs the sends of one resource.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) enum Sends {
    /// The first send probes the socket.
    #[default]
    Unknown,
    /// One call for each send: a datagram is whole in its call.
    Whole,
    /// Calls of at most this many bytes: the send low-water mark of a stream socket.
    Pieces(NonZeroUsize),
}

/// How the sends of the socket `fd` run. For a stream socket, it first sets the send low-water mark
/// to [`PIECE`]. The kernel keeps at most the send buffer, and the mark that it kept is the size of
/// each call. A mark of 0 has no such rule, so its sends stay whole.
pub(super) fn probe(fd: RawFd) -> Result<Sends, i32> {
    if option(fd, SO_TYPE)? != SOCK_STREAM {
        return Ok(Sends::Whole);
    }
    set(fd, SO_SNDLOWAT, c_int::try_from(PIECE.get()).unwrap_or(c_int::MAX))?;
    let mark = usize::try_from(option(fd, SO_SNDLOWAT)?)
        .ok()
        .and_then(NonZeroUsize::new);
    Ok(mark.map_or(Sends::Whole, |mark| Sends::Pieces(mark.min(PIECE))))
}

/// The `int` option `name` of the socket level of `fd`.
#[expect(unsafe_code, reason = "the kernel writes one `int` into owned storage")]
fn option(fd: RawFd, name: c_int) -> Result<c_int, i32> {
    let mut value: c_int = 0;
    let mut len = u32::try_from(size_of::<c_int>()).unwrap_or(4);
    // SAFETY: `value` and `len` live during the call, and the kernel writes at most `len` bytes.
    let result = unsafe { getsockopt(fd, SOL_SOCKET, name, (&raw mut value).cast::<c_void>(), &raw mut len) };
    if result < 0 { Err(errno()) } else { Ok(value) }
}

/// Sets the `int` option `name` of the socket level of `fd`.
#[expect(unsafe_code, reason = "the kernel reads one `int` from owned storage")]
fn set(fd: RawFd, name: c_int, value: c_int) -> Result<(), i32> {
    let len = u32::try_from(size_of::<c_int>()).unwrap_or(4);
    // SAFETY: `value` lives during the call, and the kernel reads `len` bytes.
    let result = unsafe { setsockopt(fd, SOL_SOCKET, name, ptr::from_ref(&value).cast::<c_void>(), len) };
    if result < 0 { Err(errno()) } else { Ok(()) }
}

/// The regions of a stream send entry, and the place of its next call.
pub(super) struct Pieces<'a> {
    fd: RawFd,
    flags: c_int,
    regions: Regions<'a>,
    /// The region, and the byte in it, where the next call starts.
    region: usize,
    within: usize,
    /// The bytes that no call sent.
    rest: usize,
}

/// The one region of `IORING_OP_SEND`, or the regions of the message of `IORING_OP_SENDMSG`.
enum Regions<'a> {
    One(IoVec),
    Many(&'a [IoVec]),
}

impl<'a> Pieces<'a> {
    /// The regions of the send entry `sqe`.
    ///
    /// # Safety
    ///
    /// The memory that `sqe` names lives, and nothing writes it, for `'a`.
    #[expect(unsafe_code, reason = "the entry names a message header and its regions")]
    pub(super) unsafe fn new(sqe: &Sqe) -> Self {
        let regions = if sqe.opcode == ring::IORING_OP_SEND {
            Regions::One(IoVec {
                iov_base: pointer(sqe.addr),
                iov_len: sqe.len as usize,
            })
        } else {
            let header = pointer::<MsgHdr>(sqe.addr).cast_const();
            // SAFETY: the caller keeps the header and its regions for `'a`.
            let (base, count) = unsafe { ((*header).msg_iov.cast_const(), (*header).msg_iovlen) };
            let count = usize::try_from(count).unwrap_or(0);
            Regions::Many(if base.is_null() || count == 0 {
                &[]
            } else {
                // SAFETY: the header names `count` regions at `base`, which live for `'a`.
                unsafe { core::slice::from_raw_parts(base, count) }
            })
        };
        let rest = match &regions {
            Regions::One(region) => region.iov_len,
            Regions::Many(regions) => regions
                .iter()
                .fold(0_usize, |rest, region| rest.saturating_add(region.iov_len)),
        };
        Self {
            fd: sqe.fd,
            flags: sqe.rw_flags.cast_signed() | MSG_DONTWAIT,
            regions,
            region: 0,
            within: 0,
            rest,
        }
    }

    /// The bytes that no call sent.
    pub(super) const fn rest(&self) -> usize {
        self.rest
    }

    /// Sends the next bytes, at most `piece` of them in at most [`REGIONS`] regions, in one
    /// nonblocking `sendmsg`. Gives the bytes of the call and the count that the kernel took. A send
    /// of no bytes makes its one call too, so that it gives the error of the socket.
    #[expect(unsafe_code, reason = "the kernel reads the regions of the entry")]
    pub(super) fn call(&mut self, piece: NonZeroUsize) -> Result<(usize, usize), i32> {
        let mut window = [IoVec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        }; REGIONS];
        let regions = match &self.regions {
            Regions::One(region) => core::slice::from_ref(region),
            Regions::Many(regions) => regions,
        };
        let (mut count, mut len, mut within) = (0, 0, self.within);
        for region in regions.iter().skip(self.region) {
            if count == REGIONS || len == piece.get() {
                break;
            }
            let take = region.iov_len.saturating_sub(within).min(piece.get() - len);
            if take > 0 {
                window[count] = IoVec {
                    iov_base: region.iov_base.cast::<u8>().wrapping_add(within).cast(),
                    iov_len: take,
                };
                count += 1;
                len += take;
            }
            within = 0;
        }
        let header = MsgHdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: window.as_mut_ptr(),
            msg_iovlen: c_int::try_from(count).unwrap_or(0),
            msg_control: ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };
        // SAFETY: the window names `len` bytes of the regions of the entry, which live during the
        // call, and the kernel only reads them.
        let taken = counted(unsafe { sendmsg(self.fd, &raw const header, self.flags) })?;
        let taken = usize::try_from(taken).unwrap_or(0).min(len);
        self.advance(regions.len(), taken);
        Ok((len, taken))
    }

    /// Moves the place of the next call `sent` bytes on, over the regions of the entry.
    fn advance(&mut self, regions: usize, sent: usize) {
        self.rest -= sent;
        let mut sent = sent;
        while self.region < regions {
            let len = match &self.regions {
                Regions::One(region) => region.iov_len,
                Regions::Many(many) => many[self.region].iov_len,
            };
            let left = len - self.within;
            if sent < left {
                self.within += sent;
                return;
            }
            sent -= left;
            self.region += 1;
            self.within = 0;
        }
    }
}
