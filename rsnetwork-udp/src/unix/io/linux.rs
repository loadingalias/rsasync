//! The calls of Linux only: batches of one system call (`sendmmsg` and `recvmmsg`), and coalesced
//! receive (GRO).

use core::ffi::c_int;
use core::ptr;
use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd};

use super::{c_uint_len, finish, header, name, refusal, region, set};
use crate::sys::{self, EINTR, MSG_DONTWAIT, MmsgHdr, MsgHdr, SOL_UDP, UDP_GRO};
use crate::{BATCH, Control, Received, SockAddrStorage, Transmit};

/// Turns coalesced receive (GRO) on or off. A coalesced receive gives several datagrams of one
/// peer in one buffer, with their segment size in [`Metadata::segment_size`](crate::Metadata::segment_size).
pub fn set_gro(socket: BorrowedFd<'_>, on: bool) -> io::Result<()> {
    set(socket, SOL_UDP, UDP_GRO, c_int::from(on))
}

/// Sends up to [`BATCH`] datagrams or batches of segments in one call (`sendmmsg`), each with its
/// own metadata, with no wait. Gives the number of `transmits` that the kernel sent, from the
/// first. Send the rest again; their bytes never join another datagram.
///
/// # Errors
///
/// As for [`send`](crate::send), when the kernel sent none of them.
#[expect(unsafe_code, reason = "the message headers name memory that lives during the call")]
pub fn send_batch(socket: BorrowedFd<'_>, transmits: &[Transmit<'_>]) -> io::Result<usize> {
    let transmits = &transmits[..transmits.len().min(BATCH)];
    let mut names = [SockAddrStorage::EMPTY; BATCH];
    let mut regions = [region(&[]); BATCH];
    let mut controls = [Control::new(); BATCH];
    let mut headers = [MmsgHdr {
        msg_hdr: header_empty(),
        msg_len: 0,
    }; BATCH];
    let mut segmented = false;
    for (index, transmit) in transmits.iter().enumerate() {
        let name_len;
        (names[index], name_len) = name(transmit);
        regions[index] = region(transmit.contents);
        let control_len = controls[index].encode(transmit);
        headers[index].msg_hdr = header(
            &mut names[index],
            name_len,
            &mut regions[index],
            &mut controls[index],
            control_len,
        );
        segmented |= transmit.segment_size.is_some();
    }
    let count = c_uint_len(transmits.len());
    loop {
        // SAFETY: the first `count` headers name the addresses, the regions, and the control
        // messages of `transmits`, which live during the call. The kernel reads them, and writes
        // only the `msg_len` of each header.
        let sent = unsafe { sys::sendmmsg(socket.as_raw_fd(), headers.as_mut_ptr(), count, MSG_DONTWAIT) };
        if let Ok(sent) = usize::try_from(sent) {
            return Ok(sent);
        }
        match io::Error::last_os_error() {
            error if error.raw_os_error() == Some(EINTR) => {}
            error => return Err(refusal(error, segmented)),
        }
    }
}

/// Receives up to [`BATCH`] datagrams in one call (`recvmmsg`), one into each of `buffers`, with
/// no wait. Writes each into `received`, from the first, and gives the number of datagrams.
///
/// # Errors
///
/// `WouldBlock` when no datagram is ready, or another error of `recvmmsg`.
#[expect(unsafe_code, reason = "the message headers name memory that lives during the call")]
pub fn recv_batch(socket: BorrowedFd<'_>, buffers: &mut [&mut [u8]], received: &mut [Received]) -> io::Result<usize> {
    let count = buffers.len().min(received.len()).min(BATCH);
    let mut names = [SockAddrStorage::EMPTY; BATCH];
    let mut regions = [region(&[]); BATCH];
    let mut controls = [Control::new(); BATCH];
    let mut headers = [MmsgHdr {
        msg_hdr: header_empty(),
        msg_len: 0,
    }; BATCH];
    for (index, buffer) in buffers.iter_mut().take(count).enumerate() {
        regions[index] = sys::IoVec {
            iov_base: buffer.as_mut_ptr().cast(),
            iov_len: buffer.len(),
        };
        headers[index].msg_hdr = header(
            &mut names[index],
            SockAddrStorage::LEN,
            &mut regions[index],
            &mut controls[index],
            Control::LEN,
        );
    }
    loop {
        // SAFETY: the first `count` headers name address storage, the buffers, and control storage,
        // which live during the call. The kernel writes at most their lengths.
        let done = unsafe {
            sys::recvmmsg(
                socket.as_raw_fd(),
                headers.as_mut_ptr(),
                c_uint_len(count),
                MSG_DONTWAIT,
                ptr::null_mut(),
            )
        };
        if let Ok(done) = usize::try_from(done) {
            for (index, slot) in received.iter_mut().take(done).enumerate() {
                let len = usize::try_from(headers[index].msg_len).unwrap_or(usize::MAX);
                let header = &headers[index].msg_hdr;
                *slot = finish(
                    &names[index],
                    header.msg_namelen,
                    header.msg_flags,
                    &controls[index],
                    header.msg_controllen,
                    len,
                )?;
            }
            return Ok(done);
        }
        match io::Error::last_os_error() {
            error if error.raw_os_error() == Some(EINTR) => {}
            error => return Err(error),
        }
    }
}

/// A header that names nothing.
const fn header_empty() -> MsgHdr {
    MsgHdr {
        msg_name: ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: ptr::null_mut(),
        msg_iovlen: 0,
        msg_control: ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    }
}
