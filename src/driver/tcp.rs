//! TCP on the driver: bind and listen, accept, receive into an owned buffer or into a buffer that
//! the kernel selects, send from a lease, and close. Each socket is nonblocking, and the driver
//! owns its descriptor.

use core::net::SocketAddr;
use std::io;
use std::os::fd::OwnedFd;

use rsnetwork_udp::SockAddrStorage;

use super::{Driver, Finish, Held, Operation, Reason, Refused, Resource};
use crate::buffer::{Backing, Buffer as _, BufferMut as _, Lease};
use crate::uring::Sqe;

/// A listening TCP socket that a driver owns.
#[derive(Debug)]
pub struct TcpListener {
    resource: Resource,
    local: SocketAddr,
}

impl TcpListener {
    /// The local address of the socket.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }
}

/// A connected TCP socket that a driver owns.
#[derive(Debug)]
pub struct TcpStream {
    resource: Resource,
}

/// The result of an operation as a count of bytes, or the error of the kernel.
fn count(result: i32) -> io::Result<usize> {
    usize::try_from(result).map_err(|_| io::Error::from_raw_os_error(result.saturating_neg()))
}

impl Driver {
    /// Binds a TCP socket to `address` and listens on it. The socket is nonblocking.
    ///
    /// # Errors
    ///
    /// An error of the system, or `OutOfMemory` when each resource slot of the driver is in use.
    pub fn bind(&mut self, address: SocketAddr) -> io::Result<TcpListener> {
        let listener = std::net::TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let local = listener.local_addr()?;
        let resource = self
            .adopt(OwnedFd::from(listener))
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        Ok(TcpListener { resource, local })
    }

    /// Accepts a connection on `listener`. The future gives the stream and the address of its
    /// peer. A drop of the future asks for a cancel.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation.
    #[expect(unsafe_code, reason = "the accept names the address in its own resources")]
    pub fn accept(
        &mut self,
        listener: &TcpListener,
    ) -> Result<Operation<io::Result<(TcpStream, SocketAddr)>>, Refused<()>> {
        let fd = self.fd(listener.resource).ok_or(Refused {
            reason: Reason::Resource,
            value: (),
        })?;
        let held = |()| Held {
            finish: Finish::Accepted,
            peer_len: SockAddrStorage::LEN,
            ..Held::default()
        };
        let entry = |held: &mut Held| Sqe::accept(fd, (&raw mut held.peer).cast(), &raw mut held.peer_len);
        // SAFETY: the entry names the peer address in the resources, and the listener.
        let completion = unsafe { self.submit((), Some(listener.resource), held, entry, true) }?;
        Ok(Operation::new(completion, |result, held| {
            Some(count(result).and_then(|_| {
                let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
                let peer = held
                    .peer
                    .to_socket_addr(held.peer_len)
                    .ok_or(io::ErrorKind::InvalidData)?;
                Ok((TcpStream { resource }, peer))
            }))
        }))
    }

    /// Receives into the capacity of `buffer` after its initialized bytes. The future gives the
    /// count of received bytes, and the buffer with them initialized after its earlier bytes. The
    /// kernel writes into the buffer itself, with no copy. A drop of the future asks for a cancel.
    ///
    /// # Errors
    ///
    /// A refusal, with the buffer, when the driver has no room for the operation.
    #[expect(unsafe_code, reason = "the receive names the capacity of its own buffer")]
    pub fn recv(
        &mut self,
        stream: &TcpStream,
        mut buffer: Backing,
    ) -> Result<Operation<(io::Result<usize>, Backing)>, Refused<Backing>> {
        let Some(fd) = self.fd(stream.resource) else {
            return Err(Refused {
                reason: Reason::Resource,
                value: buffer,
            });
        };
        let start = buffer.initialized();
        let len = u32::try_from(buffer.capacity() - start).unwrap_or(u32::MAX);
        // The address of the buffer stays the same when the buffer moves into the resources.
        let entry = Sqe::recv(fd, buffer.as_mut_ptr().wrapping_add(start), len);
        let held = |buffer| Held {
            buffer: Some(buffer),
            finish: Finish::Received,
            ..Held::default()
        };
        // SAFETY: the entry names the capacity of the buffer, which the resources keep, and the
        // stream.
        let completion = unsafe { self.submit(buffer, Some(stream.resource), held, |_| entry, true) }?;
        Ok(Operation::new(completion, |result, held| {
            Some((count(result), held.buffer?))
        }))
    }

    /// Receives into a buffer that the kernel selects from the buffers of [`Driver::provide`] when
    /// the data comes, so the wait holds no buffer. The future gives a lease of the received bytes
    /// in that buffer. When its last lease or view drops, on any thread, the buffer goes back to
    /// the kernel. A drop of the future asks for a cancel.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no provided buffers or no room for the operation.
    #[expect(unsafe_code, reason = "a selecting receive names no memory of its own")]
    pub fn recv_selected(&mut self, stream: &TcpStream) -> Result<Operation<io::Result<Lease>>, Refused<()>> {
        let Some(group) = self.selected.as_ref().map(|selected| selected.ring.group()) else {
            return Err(Refused {
                reason: Reason::Buffers,
                value: (),
            });
        };
        let fd = self.fd(stream.resource).ok_or(Refused {
            reason: Reason::Resource,
            value: (),
        })?;
        let held = |()| Held {
            finish: Finish::Selected,
            ..Held::default()
        };
        // SAFETY: the entry names the stream, and the kernel takes a buffer of the provided ring,
        // which the driver keeps until the completion gives it.
        let completion =
            unsafe { self.submit((), Some(stream.resource), held, |_| Sqe::recv_selected(fd, group), true) }?;
        Ok(Operation::new(completion, |result, held| {
            Some(count(result).map(|_| held.lease.unwrap_or_else(|| Lease::from(&[][..]))))
        }))
    }

    /// Sends the bytes of `lease`. The future gives the count of sent bytes, and the lease. The
    /// kernel reads the lease itself, with no copy. A drop of the future does not cancel the send.
    ///
    /// # Errors
    ///
    /// A refusal, with the lease, when the driver has no room for the operation.
    #[expect(unsafe_code, reason = "the send names the bytes of its own lease")]
    pub fn send(
        &mut self,
        stream: &TcpStream,
        lease: Lease,
    ) -> Result<Operation<(io::Result<usize>, Lease)>, Refused<Lease>> {
        let Some(fd) = self.fd(stream.resource) else {
            return Err(Refused {
                reason: Reason::Resource,
                value: lease,
            });
        };
        let len = u32::try_from(lease.initialized()).unwrap_or(u32::MAX);
        // The address of the lease stays the same when the lease moves into the resources.
        let entry = Sqe::send(fd, lease.as_ptr(), len);
        let held = |lease| Held {
            lease: Some(lease),
            ..Held::default()
        };
        // SAFETY: the entry names the bytes of the lease, which the resources keep, and the
        // stream.
        let completion = unsafe { self.submit(lease, Some(stream.resource), held, |_| entry, false) }?;
        Ok(Operation::new(completion, |result, held| {
            Some((count(result), held.lease?))
        }))
    }

    /// Closes `stream`. Refuses with the stream while an operation runs on it.
    ///
    /// # Errors
    ///
    /// A refusal, with the stream, while an operation runs on it.
    pub fn close(&mut self, stream: TcpStream) -> Result<(), Refused<TcpStream>> {
        self.resources
            .close(stream.resource)
            .map_err(|reason| Refused { reason, value: stream })
    }

    /// Closes `listener`. Refuses with the listener while an accept runs on it.
    ///
    /// # Errors
    ///
    /// A refusal, with the listener, while an accept runs on it.
    pub fn close_listener(&mut self, listener: TcpListener) -> Result<(), Refused<TcpListener>> {
        self.resources.close(listener.resource).map_err(|reason| Refused {
            reason,
            value: listener,
        })
    }
}
