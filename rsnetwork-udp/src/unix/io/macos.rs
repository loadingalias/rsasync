//! The batches of macOS: one system call for each datagram (`sendmsg` and `recvmsg`), or, with the
//! `macos-private-batch` feature, one for the batch (the private `sendmsg_x` and `recvmsg_x`).

#[cfg(not(feature = "macos-private-batch"))]
pub use each::{recv_batch, send_batch};
#[cfg(feature = "macos-private-batch")]
pub use private::{recv_batch, send_batch};

/// The batches of one system call for each datagram.
#[cfg(not(feature = "macos-private-batch"))]
mod each {
    use std::io;
    use std::os::fd::BorrowedFd;

    use crate::{BATCH, Received, Transmit, recv, send};

    /// Sends up to [`BATCH`] datagrams in turn, each with its own metadata, with no wait: one
    /// `sendmsg` for each datagram. Gives the number of `transmits` that the kernel sent, from the
    /// first. It stops at the first datagram that the kernel refuses, so the rest never goes out:
    /// send it again.
    ///
    /// # Errors
    ///
    /// As for [`send`](crate::send), when the kernel sent none of them.
    pub fn send_batch(socket: BorrowedFd<'_>, transmits: &[Transmit<'_>]) -> io::Result<usize> {
        let transmits = &transmits[..transmits.len().min(BATCH)];
        for (sent, transmit) in transmits.iter().enumerate() {
            if let Err(error) = send(socket, transmit) {
                return if sent == 0 { Err(error) } else { Ok(sent) };
            }
        }
        Ok(transmits.len())
    }

    /// Receives up to [`BATCH`] datagrams in turn, one into each of `buffers`, with no wait: one
    /// `recvmsg` for each datagram. Writes each into `received`, from the first, and gives the number
    /// of datagrams. It stops when no datagram is ready.
    ///
    /// # Errors
    ///
    /// `WouldBlock` when no datagram is ready, or another error of `recvmsg`, when it received none.
    pub fn recv_batch(
        socket: BorrowedFd<'_>,
        buffers: &mut [&mut [u8]],
        received: &mut [Received],
    ) -> io::Result<usize> {
        let count = buffers.len().min(received.len()).min(BATCH);
        for (index, (buffer, slot)) in buffers.iter_mut().zip(received.iter_mut()).take(count).enumerate() {
            match recv(socket, buffer) {
                Ok(datagram) => *slot = datagram,
                Err(error) => return if index == 0 { Err(error) } else { Ok(index) },
            }
        }
        Ok(count)
    }
}

/// The batches of one system call. `sendmsg_x` and `recvmsg_x` are private calls of macOS, which
/// can change in a release of the system. Each `msghdr_x` has the address and the control messages
/// of its datagram.
#[cfg(feature = "macos-private-batch")]
mod private {
    use core::ptr;
    use std::io;
    use std::os::fd::{AsRawFd as _, BorrowedFd};

    use crate::sys::{self, EINTR, IoVec, MSG_DONTWAIT, MSG_TRUNC, MsgHdr, MsgHdrX};
    use crate::unix::io::{c_uint_len, header, name, region};
    use crate::{BATCH, Control, Received, Refused, SockAddrStorage, Transmit};

    /// Sends up to [`BATCH`] datagrams in one call (`sendmsg_x`), each with its own metadata, with
    /// no wait. Gives the number of `transmits` that the kernel sent, from the first. The kernel
    /// stops at the first datagram that it refuses, so the rest never goes out: send it again.
    ///
    /// # Errors
    ///
    /// As for [`send`](crate::send), when the kernel sent none of them.
    #[expect(unsafe_code, reason = "the message headers name memory that lives during the call")]
    pub fn send_batch(socket: BorrowedFd<'_>, transmits: &[Transmit<'_>]) -> io::Result<usize> {
        // The batch has no segment size either: it ends before the first transmit with one.
        let count = transmits
            .iter()
            .take(BATCH)
            .take_while(|transmit| transmit.segment_size.is_none())
            .count();
        if count == 0 {
            return if transmits.is_empty() {
                Ok(0)
            } else {
                Err(io::Error::new(io::ErrorKind::Unsupported, Refused::NoSegmentation))
            };
        }
        let mut names = [SockAddrStorage::EMPTY; BATCH];
        let mut regions = [region(&[]); BATCH];
        let mut controls = [Control::new(); BATCH];
        let mut headers = [EMPTY; BATCH];
        for (index, transmit) in transmits.iter().take(count).enumerate() {
            let name_len;
            (names[index], name_len) = name(transmit);
            regions[index] = region(transmit.contents);
            let control_len = controls[index].encode(transmit);
            headers[index] = extended(header(
                &mut names[index],
                name_len,
                &mut regions[index],
                &mut controls[index],
                control_len,
            ));
        }
        loop {
            // SAFETY: the first `count` headers name the addresses, the regions, and the control
            // messages of `transmits`, which live during the call, and the kernel only reads them.
            let sent = unsafe { sys::sendmsg_x(socket.as_raw_fd(), headers.as_ptr(), c_uint_len(count), MSG_DONTWAIT) };
            if let Ok(sent) = usize::try_from(sent) {
                return Ok(sent);
            }
            match io::Error::last_os_error() {
                error if error.raw_os_error() == Some(EINTR) => {}
                error => return Err(Refused::of_send(error, false)),
            }
        }
    }

    /// Receives up to [`BATCH`] datagrams in one call (`recvmsg_x`), one into each of `buffers`,
    /// with no wait. Writes each into `received`, from the first, and gives the number of
    /// datagrams.
    ///
    /// `recvmsg_x` does not report truncation: macOS 27.2 sets no `MSG_TRUNC` in the header of a
    /// datagram that did not fit. Thus a datagram that fills its buffer reports
    /// [`Received::truncated`], also when it fits exactly.
    ///
    /// # Errors
    ///
    /// `WouldBlock` when no datagram is ready, or another error of `recvmsg_x`.
    #[expect(unsafe_code, reason = "the message headers name memory that lives during the call")]
    pub fn recv_batch(
        socket: BorrowedFd<'_>,
        buffers: &mut [&mut [u8]],
        received: &mut [Received],
    ) -> io::Result<usize> {
        let count = buffers.len().min(received.len()).min(BATCH);
        if count == 0 {
            return Ok(0);
        }
        let mut names = [SockAddrStorage::EMPTY; BATCH];
        let mut regions = [region(&[]); BATCH];
        let mut controls = [Control::new(); BATCH];
        let mut headers = [EMPTY; BATCH];
        for (index, buffer) in buffers.iter_mut().take(count).enumerate() {
            regions[index] = IoVec {
                iov_base: buffer.as_mut_ptr().cast(),
                iov_len: buffer.len(),
            };
            headers[index] = extended(header(
                &mut names[index],
                SockAddrStorage::LEN,
                &mut regions[index],
                &mut controls[index],
                Control::LEN,
            ));
        }
        loop {
            // SAFETY: the first `count` headers name address storage, the buffers, and control
            // storage, which live during the call. The kernel writes at most their lengths, and the
            // received fields of each header, through a pointer that can write.
            let done = unsafe {
                sys::recvmsg_x(
                    socket.as_raw_fd(),
                    headers.as_mut_ptr().cast_const(),
                    c_uint_len(count),
                    MSG_DONTWAIT,
                )
            };
            if let Ok(done) = usize::try_from(done) {
                for (index, slot) in received.iter_mut().take(done).enumerate() {
                    let header = &headers[index];
                    let filled = header.msg_datalen >= regions[index].iov_len;
                    *slot = Received::from_message(
                        &names[index],
                        header.msg_namelen,
                        if filled {
                            header.msg_flags | MSG_TRUNC
                        } else {
                            header.msg_flags
                        },
                        &controls[index],
                        usize::try_from(header.msg_controllen).unwrap_or(usize::MAX),
                        header.msg_datalen,
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

    /// The header of one datagram of a batch.
    const fn extended(header: MsgHdr) -> MsgHdrX {
        MsgHdrX {
            msg_name: header.msg_name,
            msg_namelen: header.msg_namelen,
            msg_iov: header.msg_iov,
            msg_iovlen: header.msg_iovlen,
            msg_control: header.msg_control,
            msg_controllen: header.msg_controllen,
            msg_flags: 0,
            msg_datalen: 0,
        }
    }

    /// A header that names nothing.
    const EMPTY: MsgHdrX = MsgHdrX {
        msg_name: ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: ptr::null_mut(),
        msg_iovlen: 0,
        msg_control: ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
        msg_datalen: 0,
    };
}
