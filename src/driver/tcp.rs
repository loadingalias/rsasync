//! TCP on the driver: bind and listen, connect, accept, receive into an owned buffer or into a
//! buffer that the kernel selects, send from a lease or from several, half-close, the socket
//! options, and close. Each socket is nonblocking, and the driver owns its descriptor.
//!
//! Each direction of a stream, and the accepts of a listener, runs one operation at a time. The
//! home keeps the state of each direction: the operation that runs, one operation that waits for
//! it, what an abandoned receive or accept left, and the progress that dropped sends owe.

use alloc::vec::Vec;
use core::ffi::{c_int, c_void};
use core::net::SocketAddr;
use core::{fmt, ptr};
use std::io;
use std::os::fd::{AsFd as _, AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd};

use rsnetwork_udp::sys::{self, IoVec, MsgHdr};
use rsnetwork_udp::{AF_INET6, Received, SockAddrStorage};

use super::udp::{self, Arrivals, Batch};
use super::{Completion, Driver, ECANCELED, Held, Kind, Medium, Operation, Reason, Refused, Resource, Resources, Tag};
use crate::buffer::{Backing, Buffer as _, BufferMut as _, Lease, WriteLoan};
use crate::sync::Arc;
use crate::table::OpId;
use crate::uring::Sqe;

/// `ENOBUFS` (`include/uapi/asm-generic/errno.h`): a selecting receive found no free buffer, and
/// the data stays in the socket.
const ENOBUFS: i32 = 105;
/// `EAGAIN`: a receive with no wait found no datagram.
const EAGAIN: i32 = 11;
/// `ENOMEM`: each resource slot of the driver is in use. It reads as `OutOfMemory`.
const ENOMEM: i32 = 12;
/// `EINVAL`: an option value that the kernel cannot take.
const EINVAL: i32 = 22;

/// The backlog of a listener: the most that the kernel takes, `net.core.somaxconn`.
const BACKLOG: c_int = c_int::MAX;

/// A listening TCP socket that a driver owns.
///
/// An accept borrows the listener exclusively, so one accept runs at a time. When an accept future
/// drops, the driver asks the kernel to cancel the accept. A connection that the accept took before
/// the cancel took effect stays with the listener, and the next accept gives it before it
/// submits. A close of the listener closes it.
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

/// A connected TCP socket that a driver owns: its read half and its write half.
///
/// A receive borrows the read half exclusively, and a send borrows the write half exclusively,
/// through the stream or through [`TcpStream::split`]. So each direction runs one operation at a
/// time, and when an operation starts, no future of an earlier operation in its direction exists.
/// A half has no `Clone`.
///
/// A dropped or forgotten future ends the interest, not the operation. Its direction stays busy
/// until the operation ends: an operation that starts before then waits, and the driver starts it
/// after that end. A receive or an accept that waited and whose future dropped never ran, so it
/// ends with no effect. A send that waited and whose future dropped still runs, in start order.
/// No byte is lost or reordered:
///
/// - A dropped or forgotten receive asks the driver to cancel. The bytes that it received before
///   the cancel took effect stay with the stream, in its own buffer. The next receive gives them
///   before it submits: a receive into a buffer copies as many as fit, and the stream keeps the
///   rest; a selecting receive gives that buffer. [`Driver::copied`] counts these copies. An error
///   of an abandoned receive also stays, and the next receive gives it.
/// - A send is not cancelled. If a dropped send fails, sends only part of its bytes, or was
///   cancelled, the write direction fails: the next send gives an error with [`WriteFailed`] and
///   the count of the bytes that went, and each later send gives [`WriteFailed`] with no count. A
///   dropped send that sends all its bytes reports nothing, so a next send that succeeds shows
///   that each earlier dropped send sent all its bytes.
/// - [`Operation::cancel`] keeps the interest: the future gives the original buffer with its exact
///   progress, with no copy, and the direction stays usable.
///
/// [`Driver::close`] cancels each operation that runs, waits until each one ends, and reports the
/// progress that dropped sends owe.
///
/// An accept, a receive, a send, and a close, with `run` as in [`Operation`]:
///
/// ```no_run
/// # fn run<F: std::future::Future>(driver: &mut rsasync::driver::Driver, future: F) -> std::io::Result<F::Output> {
/// #     use std::future::Future as _;
/// #     let mut future = std::pin::pin!(future);
/// #     let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
/// #     loop {
/// #         if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #             return Ok(output);
/// #         }
/// #         if !driver.turn()?.more() {
/// #             if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #                 return Ok(output);
/// #             }
/// #             driver.wait()?;
/// #         }
/// #     }
/// # }
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use rsasync::buffer::{Backing, Buffer as _, Lease};
/// use rsasync::driver::{Config, Driver, TcpOptions};
///
/// let mut driver = Driver::new(Config::new())?;
/// let bind = driver.bind("127.0.0.1:8080".parse()?, TcpOptions::default())?;
/// let mut listener = run(&mut driver, bind)???;
/// let accept = driver.accept(&mut listener)?;
/// let (mut stream, peer) = run(&mut driver, accept)???;
///
/// let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(4096)))?;
/// let (received, request) = run(&mut driver, receive)??;
/// let count = received?;
/// assert_eq!(request.bytes().len(), count);
///
/// let send = driver.send(&mut stream, Lease::from(b"HTTP/1.1 200 OK\r\n\r\n".to_vec()))?;
/// let (sent, _response) = run(&mut driver, send)??;
/// sent?;
/// let close = driver.close(stream)?;
/// run(&mut driver, close)??;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct TcpStream {
    read: ReadHalf,
    write: WriteHalf,
}

impl TcpStream {
    fn new(resource: Resource) -> Self {
        Self {
            read: ReadHalf { resource },
            write: WriteHalf { resource },
        }
    }

    /// The read half and the write half, so that a receive and a send can run at one time.
    pub fn split(&mut self) -> (&mut ReadHalf, &mut WriteHalf) {
        (&mut self.read, &mut self.write)
    }
}

/// The read half of a [`TcpStream`], a [`UdpSocket`](super::UdpSocket), or a
/// [`PipeReader`](super::PipeReader): its receives borrow it.
#[derive(Debug)]
pub struct ReadHalf {
    pub(super) resource: Resource,
}

/// The write half of a [`TcpStream`], a [`UdpSocket`](super::UdpSocket), or a
/// [`PipeWriter`](super::PipeWriter): its sends borrow it.
#[derive(Debug)]
pub struct WriteHalf {
    pub(super) resource: Resource,
}

impl AsMut<ReadHalf> for TcpStream {
    fn as_mut(&mut self) -> &mut ReadHalf {
        &mut self.read
    }
}

impl AsMut<ReadHalf> for ReadHalf {
    fn as_mut(&mut self) -> &mut ReadHalf {
        self
    }
}

impl AsMut<WriteHalf> for TcpStream {
    fn as_mut(&mut self) -> &mut WriteHalf {
        &mut self.write
    }
}

impl AsMut<WriteHalf> for WriteHalf {
    fn as_mut(&mut self) -> &mut WriteHalf {
        self
    }
}

/// A TCP socket of the driver, a stream or a listener, whose options [`Driver::options`] reads and
/// [`Driver::set_options`] sets.
pub trait Socket: sealed::Sealed {}

impl Socket for TcpStream {}
impl Socket for TcpListener {}

mod sealed {
    /// The resource of a socket of the driver.
    #[derive(Debug, Clone, Copy)]
    pub struct Id(pub(in crate::driver) super::Resource);

    /// Only the sockets of the driver are sockets.
    pub trait Sealed {
        fn id(&self) -> Id;
    }

    impl Sealed for super::TcpStream {
        fn id(&self) -> Id {
            Id(self.read.resource)
        }
    }

    impl Sealed for super::TcpListener {
        fn id(&self) -> Id {
            Id(self.resource)
        }
    }
}

/// The options of a TCP socket. `None` keeps the value of the system.
///
/// [`Driver::bind`] and [`Driver::connect`] set them before the bind or the connect, as
/// `reuse_port` and `only_v6` need. [`Driver::set_options`] sets them on a socket, and
/// [`Driver::options`] reads each one back, with the value of the kernel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TcpOptions {
    /// Send small segments at once, with no Nagle delay (`TCP_NODELAY`).
    pub no_delay: Option<bool>,
    /// Send keepalive probes on an idle connection (`SO_KEEPALIVE`).
    pub keepalive: Option<bool>,
    /// An IPv6 socket takes no IPv4 traffic (`IPV6_V6ONLY`). [`Driver::options`] gives `None`
    /// for an IPv4 socket.
    pub only_v6: Option<bool>,
    /// The send buffer size in bytes (`SO_SNDBUF`). Linux takes at most `net.core.wmem_max`, and
    /// it doubles the value, for its own bookkeeping: [`Driver::options`] gives the doubled value.
    pub send_buffer: Option<u32>,
    /// The receive buffer size in bytes (`SO_RCVBUF`). Linux takes at most `net.core.rmem_max`,
    /// and it doubles the value as for `send_buffer`.
    pub receive_buffer: Option<u32>,
    /// Sockets of one user can bind the same address, and the kernel spreads the connections
    /// between them (`SO_REUSEPORT`).
    pub reuse_port: Option<bool>,
}

impl TcpOptions {
    /// Sets each option that is not `None` on `socket`.
    pub(super) fn apply(self, socket: BorrowedFd<'_>) -> io::Result<()> {
        let flags = [
            (self.no_delay, sys::IPPROTO_TCP, sys::TCP_NODELAY),
            (self.keepalive, sys::SOL_SOCKET, sys::SO_KEEPALIVE),
            (self.only_v6, sys::IPPROTO_IPV6, sys::IPV6_V6ONLY),
            (self.reuse_port, sys::SOL_SOCKET, sys::SO_REUSEPORT),
        ];
        for (value, level, name) in flags {
            if let Some(value) = value {
                set_int(socket, level, name, c_int::from(value))?;
            }
        }
        let sizes = [
            (self.send_buffer, sys::SO_SNDBUF),
            (self.receive_buffer, sys::SO_RCVBUF),
        ];
        for (value, name) in sizes {
            if let Some(value) = value {
                let value = c_int::try_from(value).map_err(|_| io::Error::from_raw_os_error(EINVAL))?;
                set_int(socket, sys::SOL_SOCKET, name, value)?;
            }
        }
        Ok(())
    }

    /// Reads each option of `socket`.
    fn read(socket: BorrowedFd<'_>) -> io::Result<Self> {
        let flag = |level, name| int(socket, level, name).map(|value| Some(value != 0));
        let size = |name| {
            let value = int(socket, sys::SOL_SOCKET, name)?;
            u32::try_from(value)
                .map(Some)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
        };
        let ipv6 = local_address(socket).is_some_and(|(address, _)| address.ss_family == AF_INET6);
        Ok(Self {
            no_delay: flag(sys::IPPROTO_TCP, sys::TCP_NODELAY)?,
            keepalive: flag(sys::SOL_SOCKET, sys::SO_KEEPALIVE)?,
            only_v6: if ipv6 {
                flag(sys::IPPROTO_IPV6, sys::IPV6_V6ONLY)?
            } else {
                None
            },
            send_buffer: size(sys::SO_SNDBUF)?,
            receive_buffer: size(sys::SO_RCVBUF)?,
            reuse_port: flag(sys::SOL_SOCKET, sys::SO_REUSEPORT)?,
        })
    }
}

/// The owned regions of a vectored send, in order, and the array that names them for the kernel.
///
/// Make it one time with [`Regions::with_capacity`], push the leases of a message, and send it
/// with [`Driver::send_vectored`]. The future gives it back, so a `Regions` that you use again
/// allocates nothing for each send. After a send that sent only part of the bytes,
/// [`Regions::advance`] drops the regions that went and keeps the rest of the partial one, so that
/// the next send continues the stream.
///
/// For example, with `run` as in [`Operation`]:
///
/// ```no_run
/// # fn run<F: std::future::Future>(driver: &mut rsasync::driver::Driver, future: F) -> std::io::Result<F::Output> {
/// #     use std::future::Future as _;
/// #     let mut future = std::pin::pin!(future);
/// #     let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
/// #     loop {
/// #         if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #             return Ok(output);
/// #         }
/// #         if !driver.turn()?.more() {
/// #             if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #                 return Ok(output);
/// #             }
/// #             driver.wait()?;
/// #         }
/// #     }
/// # }
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut driver = rsasync::driver::Driver::new(rsasync::driver::Config::new())?;
/// # let bind = driver.bind("127.0.0.1:8080".parse()?, rsasync::driver::TcpOptions::default())?;
/// # let mut listener = run(&mut driver, bind)???;
/// # let accept = driver.accept(&mut listener)?;
/// # let (mut stream, _) = run(&mut driver, accept)???;
/// use rsasync::buffer::Lease;
/// use rsasync::driver::WriteFailed;
///
/// let next = driver.send(&mut stream, Lease::from(b"next".to_vec()))?;
/// let (result, _lease) = run(&mut driver, next)??;
/// match result {
///     Ok(_) => {} // Each earlier dropped send sent all its bytes.
///     Err(error) => match WriteFailed::of(&error) {
///         // `failed.sent()` bytes of the dropped sends went to the peer. Close the stream.
///         Some(failed) => eprintln!("{failed}"),
///         None => return Err(error.into()),
///     },
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Regions {
    leases: Vec<Lease>,
    /// The iovec array of the leases, which the home writes before each submission. Its capacity
    /// is at least the number of leases.
    vectors: Vec<IoVec>,
    /// The message header that names `vectors`, which the home writes before each submission.
    header: MsgHdr,
}

/// A message header with no address, no region, and no control message.
const EMPTY_HEADER: MsgHdr = MsgHdr {
    msg_name: ptr::null_mut(),
    msg_namelen: 0,
    msg_iov: ptr::null_mut(),
    msg_iovlen: 0,
    msg_control: ptr::null_mut(),
    msg_controllen: 0,
    msg_flags: 0,
};

#[expect(
    unsafe_code,
    reason = "the array and the header hold only addresses of memory that the regions own"
)]
#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "the raw pointers name only the leases and the array of this value"
)]
// SAFETY: `vectors` and `header` hold only addresses of the bytes of `leases` and of `vectors`,
// which this value owns, and only the kernel reads through them, during a send whose resources the
// home keeps. A move to another thread moves that memory with them, and a lease is `Send`.
unsafe impl Send for Regions {}

impl Regions {
    /// No regions, with room for `regions` of them.
    #[must_use]
    pub fn with_capacity(regions: usize) -> Self {
        Self {
            leases: Vec::with_capacity(regions),
            vectors: Vec::with_capacity(regions),
            header: EMPTY_HEADER,
        }
    }

    /// Adds `lease` after the other regions. It allocates only when the regions have no room.
    pub fn push(&mut self, lease: Lease) {
        self.leases.push(lease);
        self.vectors
            .reserve(self.leases.len().saturating_sub(self.vectors.len()));
    }

    /// The regions, in order.
    #[must_use]
    pub fn leases(&self) -> &[Lease] {
        &self.leases
    }

    /// Whether there is no region.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }

    /// The number of bytes of all of the regions.
    #[must_use]
    pub fn total(&self) -> usize {
        self.leases
            .iter()
            .fold(0, |total: usize, lease| total.saturating_add(lease.initialized()))
    }

    /// Takes away the first `sent` bytes, for example the count of a send that sent only part of
    /// them: each region that went drops, and a region that went only in part becomes a view of
    /// its rest, with no copy.
    pub fn advance(&mut self, sent: usize) {
        let mut rest = sent;
        let mut done = 0;
        for lease in &mut self.leases {
            let len = lease.initialized();
            if rest < len {
                if let Some(view) = lease.view(rest..) {
                    *lease = view;
                }
                break;
            }
            rest -= len;
            done += 1;
        }
        self.leases.drain(..done);
    }

    /// Takes away each region.
    pub fn clear(&mut self) {
        self.leases.clear();
    }

    /// The regions, in order.
    #[must_use]
    pub fn into_leases(self) -> Vec<Lease> {
        self.leases
    }

    /// The header of a message of each region, for a submission. Its capacity holds the array, so
    /// it allocates nothing.
    fn message(&mut self) -> *const MsgHdr {
        self.vectors();
        self.header = MsgHdr {
            msg_iov: self.vectors.as_mut_ptr(),
            msg_iovlen: self.vectors.len(),
            ..EMPTY_HEADER
        };
        &raw const self.header
    }

    /// The iovec array of each region, and its length, for a submission. Its capacity holds the
    /// array, so it allocates nothing.
    pub(super) fn vectors(&mut self) -> (*const IoVec, u32) {
        self.vectors.clear();
        self.vectors.extend(self.leases.iter().map(|lease| IoVec {
            iov_base: lease.as_ptr().cast_mut().cast::<c_void>(),
            iov_len: lease.initialized(),
        }));
        (
            self.vectors.as_ptr(),
            u32::try_from(self.vectors.len()).unwrap_or(u32::MAX),
        )
    }
}

impl From<Vec<Lease>> for Regions {
    /// The regions of `leases`, in order. It allocates the array that names them.
    fn from(leases: Vec<Lease>) -> Self {
        Self {
            vectors: Vec::with_capacity(leases.len()),
            leases,
            header: EMPTY_HEADER,
        }
    }
}

/// The write direction of a stream failed, because a send whose future dropped failed, sent only
/// part of its bytes, or was cancelled. The caller no longer knows the framing of its stream. It
/// is the payload of the `io::Error` of a send: get it with [`WriteFailed::of`].
///
/// For example, with `run` as in [`Operation`]:
///
/// ```no_run
/// # fn run<F: std::future::Future>(driver: &mut rsasync::driver::Driver, future: F) -> std::io::Result<F::Output> {
/// #     use std::future::Future as _;
/// #     let mut future = std::pin::pin!(future);
/// #     let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
/// #     loop {
/// #         if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #             return Ok(output);
/// #         }
/// #         if !driver.turn()?.more() {
/// #             if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #                 return Ok(output);
/// #             }
/// #             driver.wait()?;
/// #         }
/// #     }
/// # }
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut driver = rsasync::driver::Driver::new(rsasync::driver::Config::new())?;
/// # let bind = driver.bind("127.0.0.1:8080".parse()?, rsasync::driver::TcpOptions::default())?;
/// # let mut listener = run(&mut driver, bind)???;
/// # let accept = driver.accept(&mut listener)?;
/// # let (mut stream, _) = run(&mut driver, accept)???;
/// use rsasync::buffer::Lease;
/// use rsasync::driver::Regions;
///
/// let mut regions = Regions::with_capacity(3);
/// regions.push(Lease::from(b"HTTP/1.1 200 OK\r\n".to_vec()));
/// regions.push(Lease::from(b"content-length: 5\r\n\r\n".to_vec()));
/// regions.push(Lease::from(b"hello".to_vec()));
/// while !regions.is_empty() {
///     let send = driver.send_vectored(&mut stream, regions)?;
///     let (sent, back) = run(&mut driver, send)??;
///     regions = back;
///     regions.advance(sent?);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteFailed {
    sent: Option<usize>,
}

impl WriteFailed {
    /// The bytes that the dropped sends sent, from the first byte of the first send whose result
    /// no future took. The first send after the failure gives the count, and each later send gives
    /// `None`.
    #[must_use]
    pub const fn sent(self) -> Option<usize> {
        self.sent
    }

    /// The failure that `error` carries, or `None` for each other error.
    #[must_use]
    pub fn of(error: &io::Error) -> Option<Self> {
        error.get_ref()?.downcast_ref().copied()
    }
}

impl fmt::Display for WriteFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.sent {
            Some(sent) => write!(
                f,
                "the write direction failed after a dropped send, which sent {sent} bytes"
            ),
            None => f.write_str("the write direction failed after a dropped send"),
        }
    }
}

impl core::error::Error for WriteFailed {}

/// What a close did, after each operation of its socket ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    sent: Option<usize>,
}

impl Closed {
    /// The bytes that the dropped sends sent, from the first byte of the first send whose result
    /// no future took, when a send that the close cancelled, or a failed send, owes that count.
    /// `None` when no send owes it, also when a send already gave it with [`WriteFailed`].
    #[must_use]
    pub const fn sent(self) -> Option<usize> {
        self.sent
    }
}

/// One direction of a stream, or the accepts of a listener: the operation that runs or ran last,
/// and the operations that wait for it, in start order, linked through `Held::next`.
#[derive(Debug, Default)]
struct Direction {
    current: Option<OpId>,
    first: Option<OpId>,
    last: Option<OpId>,
}

/// What an abandoned receive or accept left for the next one.
#[derive(Debug)]
enum Kept {
    /// Received bytes: from `start` to the end of the initialized bytes of `buffer`.
    Bytes { buffer: Backing, start: usize },
    /// The error of a receive.
    Error(i32),
    /// A datagram that a receive took, with its facts.
    Datagram { buffer: Backing, received: Received },
    /// The datagrams that a batch receive took, in order.
    Arrivals(Arrivals),
    /// An accepted connection, and the address of its peer.
    Connection {
        fd: OwnedFd,
        peer: SockAddrStorage,
        peer_len: u32,
    },
}

/// The write direction after dropped sends.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Sent {
    /// Each dropped send sent all its bytes.
    #[default]
    Ok,
    /// The direction failed, and the next send or the close gives the count.
    Owe,
    /// The direction failed, and a send gave the count.
    Reported,
}

/// The directions of a stream or a listener, at the home.
#[derive(Debug, Default)]
pub(super) struct Directions {
    read: Direction,
    write: Direction,
    kept: Option<Kept>,
    sent: Sent,
    /// The bytes that dropped sends sent, since the end of the last send whose result a future
    /// took, or since the last report.
    unreported: usize,
    /// The close that waits for the operations of the resource.
    closing: Option<OpId>,
    /// The resource is in the pending list of the driver.
    pending: bool,
}

impl Directions {
    /// The direction of an operation of `kind`.
    fn direction(&mut self, kind: Kind) -> Option<&mut Direction> {
        match kind {
            Kind::Recv | Kind::Selected | Kind::Accept | Kind::RecvFrom | Kind::RecvSelectedFrom | Kind::RecvBatch => {
                Some(&mut self.read)
            }
            Kind::Send | Kind::Vectored | Kind::Shutdown | Kind::SendTo | Kind::SendBatch => Some(&mut self.write),
            Kind::Other | Kind::Close | Kind::Connect | Kind::Bind | Kind::File(_) => None,
        }
    }

    /// The listener keeps a connection that no accept took.
    pub(super) fn keep_connection(&mut self, fd: OwnedFd, peer: SockAddrStorage, peer_len: u32) {
        self.kept = Some(Kept::Connection { fd, peer, peer_len });
    }

    /// Whether an operation of `kind` has its result here, with no submission.
    fn has_result(&self, kind: Kind) -> bool {
        match kind {
            Kind::Recv | Kind::Selected => matches!(self.kept, Some(Kept::Bytes { .. } | Kept::Error(_))),
            Kind::Accept => matches!(self.kept, Some(Kept::Connection { .. })),
            Kind::Send | Kind::Vectored | Kind::Shutdown => self.sent != Sent::Ok,
            Kind::RecvFrom | Kind::RecvSelectedFrom | Kind::RecvBatch => matches!(
                self.kept,
                Some(Kept::Datagram { .. } | Kept::Arrivals(_) | Kept::Error(_))
            ),
            Kind::Other | Kind::Close | Kind::Connect | Kind::Bind | Kind::SendTo | Kind::SendBatch | Kind::File(_) => {
                false
            }
        }
    }
}

/// The result of an operation as a count of bytes, or the error of the kernel.
pub(super) fn count(result: i32) -> io::Result<usize> {
    usize::try_from(result).map_err(|_| io::Error::from_raw_os_error(result.saturating_neg()))
}

/// The abandoned rule: the home applies it to the result of an operation of `tag` whose consumer
/// ended its interest. Received bytes, the error of a receive, and an accepted connection stay
/// for the next operation of the direction. A send that failed, sent only part of its bytes, or
/// was cancelled makes the write direction fail.
pub(super) fn abandon(resources: &mut Resources, tag: Tag, result: i32, held: Held) {
    if matches!(tag.kind, Kind::Connect | Kind::Bind | Kind::File(_)) {
        // No caller holds the socket or the file, so the home closes it: the peer of a connection
        // reads the end of the stream.
        if let Some(socket) = held.accepted {
            drop(resources.release(socket));
        }
        return;
    }
    let Some(resource) = tag.resource else {
        return;
    };
    if tag.kind == Kind::Accept {
        // A stream that the home gave to a future that dropped before it took it goes back to a
        // descriptor.
        let connection = held
            .connection
            .or_else(|| held.accepted.and_then(|stream| resources.release(stream)));
        if let (Some(fd), Some(listener)) = (connection, resources.get(resource)) {
            listener.directions.keep_connection(fd, held.peer, held.peer_len);
        }
        return;
    }
    let Some(owned) = resources.get(resource) else {
        return;
    };
    let directions = &mut owned.directions;
    match tag.kind {
        Kind::Recv | Kind::Selected => {
            let buffer = held.loan.map(WriteLoan::into_inner);
            if held.copied > 0
                && let Some(Kept::Bytes { start, .. }) = &mut directions.kept
            {
                // The receive copied its bytes from the kept buffer, which still holds them.
                *start -= held.copied;
            } else if let (Ok(received), Some(buffer)) = (usize::try_from(result), buffer)
                && received > 0
            {
                let start = buffer.initialized() - received;
                directions.kept = Some(Kept::Bytes { buffer, start });
            } else if result < 0 && result != -ECANCELED && result != -ENOBUFS {
                directions.kept = Some(Kept::Error(result));
            }
        }
        Kind::Send | Kind::Vectored | Kind::Shutdown => {
            if let Some(failed) = held.failed {
                // The future did not take the failure: the next send gives it.
                if let Some(sent) = failed.sent {
                    directions.unreported = directions.unreported.saturating_add(sent);
                    directions.sent = Sent::Owe;
                }
                return;
            }
            if tag.kind == Kind::Shutdown {
                return;
            }
            let sent = usize::try_from(result).unwrap_or(0);
            let len = match (&held.lease, &held.regions) {
                (Some(lease), _) => lease.initialized(),
                (None, Some(regions)) => regions.total(),
                (None, None) => 0,
            };
            directions.unreported = directions.unreported.saturating_add(sent);
            if (result < 0 || sent < len || held.cancelled) && directions.sent == Sent::Ok {
                directions.sent = Sent::Owe;
            }
        }
        Kind::RecvFrom | Kind::RecvSelectedFrom => match (held.loan, held.received) {
            (Some(loan), Some(received)) if result >= 0 => {
                directions.kept = Some(Kept::Datagram {
                    buffer: loan.into_inner(),
                    received,
                });
            }
            _ if result < 0 && result != -ECANCELED && result != -ENOBUFS => {
                directions.kept = Some(Kept::Error(result));
            }
            _ => {}
        },
        Kind::RecvBatch => match held.batch {
            Some(Batch::Receive(arrivals)) if !arrivals.is_empty() => {
                directions.kept = Some(Kept::Arrivals(arrivals));
            }
            _ if result < 0 && result != -ECANCELED && result != -ENOBUFS && result != -EAGAIN => {
                directions.kept = Some(Kept::Error(result));
            }
            _ => {}
        },
        // A dropped datagram send fails no direction: its result is lost, as a datagram can be.
        Kind::Accept
        | Kind::Other
        | Kind::Close
        | Kind::Connect
        | Kind::Bind
        | Kind::SendTo
        | Kind::SendBatch
        | Kind::File(_) => {}
    }
}

/// The consumer of an operation of `tag` took its result. A send result that a future took
/// reports each byte up to its end.
pub(super) fn taken(resources: &mut Resources, tag: Tag) {
    if matches!(tag.kind, Kind::Send | Kind::Vectored)
        && let Some(owned) = tag.resource.and_then(|resource| resources.get(resource))
        && owned.directions.sent == Sent::Ok
    {
        owned.directions.unreported = 0;
    }
}

/// An operation of `tag` ended: its resource needs the home when an operation or a close waits
/// there.
pub(super) fn after(resources: &mut Resources, pending: &mut Vec<Resource>, tag: Tag) {
    let Some(resource) = tag.resource else {
        return;
    };
    let Some(owned) = resources.get(resource) else {
        return;
    };
    let directions = &mut owned.directions;
    let waits = directions.read.first.is_some() || directions.write.first.is_some() || directions.closing.is_some();
    if waits && !directions.pending {
        directions.pending = true;
        // At most one entry for each resource, so the capacity of the list holds it.
        pending.push(resource);
    }
}

impl Driver {
    /// Binds a nonblocking TCP socket to `address` with `options`, and listens on it. The
    /// listener reuses its address, as `std` does, and its backlog is the most that the kernel
    /// takes (`net.core.somaxconn`). The future gives the listener and its local address.
    ///
    /// Where the kernel has `IORING_OP_BIND` and `IORING_OP_LISTEN` (Linux 6.11), the ring runs
    /// the bind and the listen; on an earlier kernel, the driver makes the system calls, and the
    /// future is ready at once.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system, or `OutOfMemory` when each resource slot of the driver is in use.
    pub fn bind(
        &mut self,
        address: SocketAddr,
        options: TcpOptions,
    ) -> Result<Operation<'static, io::Result<TcpListener>>, Refused<()>> {
        let setup = |socket: BorrowedFd<'_>| {
            set_int(socket, sys::SOL_SOCKET, sys::SO_REUSEADDR, 1)?;
            options.apply(socket)
        };
        let completion = self.bind_socket(address, true, setup)?;
        Ok(Operation::new(completion, |result, held| {
            Some(count(result).and_then(|_| {
                let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
                let local = held
                    .peer
                    .to_socket_addr(held.peer_len)
                    .ok_or(io::ErrorKind::InvalidData)?;
                Ok(TcpListener { resource, local })
            }))
        }))
    }

    /// Makes a socket for `address`, sets it up with `setup`, binds it, and with `listen` listens
    /// on it: a TCP listener, or a UDP socket with no `listen`. The ring runs the bind, and the
    /// listen linked to it, where the kernel has those operations; elsewhere the driver makes the
    /// system calls, and the operation ends at once. Its resources then hold the socket and its
    /// local address.
    pub(super) fn bind_socket(
        &mut self,
        address: SocketAddr,
        listen: bool,
        setup: impl FnOnce(BorrowedFd<'_>) -> io::Result<()>,
    ) -> Result<Completion, Refused<()>> {
        let ring = self.features.bind && (self.features.listen || !listen);
        // On the ring, the bind and the listen are two linked entries, so the queue needs room for
        // both. A bind with system calls pushes no entry.
        let entries = match (ring, listen) {
            (false, _) => 0,
            (true, false) => 1,
            (true, true) => 2,
        };
        let (storage, len) = SockAddrStorage::new(address);
        let tag = Tag {
            resource: None,
            kind: Kind::Bind,
        };
        let held = |()| Held {
            peer: storage,
            peer_len: len,
            ..Held::default()
        };
        let id = self
            .open(tag, entries, (), held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        let completion = Completion::new(Arc::clone(&self.shared), id, true);
        let kind = if listen { sys::SOCK_STREAM } else { sys::SOCK_DGRAM };
        match self.socket(id, address, kind, setup) {
            Err(error) => self.complete_now(id, errno(&error)),
            Ok((_, fd)) if ring => {
                // SAFETY: the bind names the address in the resources of `id`, which the home keeps
                // until its terminal event, and the socket, which `id` holds. The queue has room for
                // each entry, so the bind links only to the listen.
                #[expect(unsafe_code, reason = "the kernel reads the address in the resources of the bind")]
                unsafe {
                    if listen {
                        self.push(id, |held| {
                            Sqe::bind(fd, (&raw const held.peer).cast(), held.peer_len).linked_quiet()
                        });
                        self.push(id, |_| Sqe::listen(fd, BACKLOG.cast_unsigned()));
                    } else {
                        self.push(id, |held| Sqe::bind(fd, (&raw const held.peer).cast(), held.peer_len));
                    }
                }
            }
            Ok((socket, _)) => {
                let done = self
                    .resources
                    .borrow(socket)
                    .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
                    .and_then(|fd| {
                        bind_now(fd, &storage, len, listen)?;
                        local_address(fd).ok_or_else(io::Error::last_os_error)
                    });
                match done {
                    Ok(local) => {
                        self.hold(id, |held| (held.peer, held.peer_len) = local);
                        self.complete_now(id, 0);
                    }
                    Err(error) => {
                        drop(self.resources.release(socket));
                        self.hold(id, |held| held.accepted = None);
                        self.complete_now(id, errno(&error));
                    }
                }
            }
        }
        Ok(completion)
    }

    /// Connects a new nonblocking TCP socket with `options` to `address`. The future gives the
    /// stream. A drop of the future asks for a cancel, and the driver closes a connection that
    /// completes after the drop, so its peer reads the end of the stream.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives the error of the
    /// system, for example `ConnectionRefused` for a port with no listener, or `OutOfMemory` when
    /// each resource slot of the driver is in use.
    pub fn connect(
        &mut self,
        address: SocketAddr,
        options: TcpOptions,
    ) -> Result<Operation<'static, io::Result<TcpStream>>, Refused<()>> {
        let (storage, len) = SockAddrStorage::new(address);
        let tag = Tag {
            resource: None,
            kind: Kind::Connect,
        };
        let held = |()| Held {
            peer: storage,
            peer_len: len,
            ..Held::default()
        };
        let id = self
            .open(tag, 1, (), held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        let completion = Completion::new(Arc::clone(&self.shared), id, true);
        match self.socket(id, address, sys::SOCK_STREAM, |socket| options.apply(socket)) {
            Err(error) => self.complete_now(id, errno(&error)),
            Ok((_, fd)) => {
                // SAFETY: the connect names the address in the resources of `id`, which the home
                // keeps until its terminal event, and the socket, which `id` holds. The queue has
                // room.
                #[expect(unsafe_code, reason = "the kernel reads the address in the resources of the connect")]
                unsafe {
                    self.push(id, |held| {
                        Sqe::connect(fd, (&raw const held.peer).cast(), held.peer_len)
                    });
                }
            }
        }
        Ok(Operation::new(completion, |result, held| {
            Some(count(result).and_then(|_| Ok(TcpStream::new(held.accepted.ok_or(io::ErrorKind::OutOfMemory)?))))
        }))
    }

    /// Sends the bytes of `regions`, in order, in one operation, with no copy that joins them. The
    /// future gives the count of sent bytes, which can end inside a region, and the regions:
    /// [`Regions::advance`] then keeps the bytes that did not go. A drop of the future does not
    /// cancel the send, and the rules of [`Driver::send`] apply.
    ///
    /// # Errors
    ///
    /// A refusal, with the regions, when the driver has no room for the operation.
    pub fn send_vectored<'a, W: AsMut<WriteHalf>>(
        &mut self,
        writer: &'a mut W,
        regions: Regions,
    ) -> Result<Operation<'a, (io::Result<usize>, Regions)>, Refused<Regions>> {
        let held = |regions| Held {
            regions: Some(regions),
            ..Held::default()
        };
        let completion = self.begin(writer.as_mut().resource, Kind::Vectored, regions, held)?;
        Ok(Operation::new(completion, |result, held| {
            let sent = held
                .failed
                .map_or_else(|| count(result), |failed| Err(io::Error::other(failed)));
            Some((sent, held.regions?))
        }))
    }

    /// Ends the write side of the stream after the sends before it (`SHUT_WR`): the peer reads
    /// the end of the stream after their bytes, and the receives of this side go on. A drop of the
    /// future does not cancel it. On a failed write direction it gives [`WriteFailed`] and sends
    /// nothing, so the peer never reads a clean end after a cut message.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, and [`Reason::Unsupported`] for a
    /// resource that is not a TCP stream: a [`PipeWriter`](super::PipeWriter), which you close
    /// instead, or a [`UdpSocket`](super::UdpSocket). A refusal changes nothing.
    pub fn shutdown_write<'a, W: AsMut<WriteHalf>>(
        &mut self,
        writer: &'a mut W,
    ) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        let resource = writer.as_mut().resource;
        // Only a TCP stream has a half-close. The close of a pipe is the end of its stream, and a
        // datagram socket has no stream.
        if self
            .resources
            .get(resource)
            .is_some_and(|owned| owned.medium != Medium::Tcp)
        {
            return Err(Refused {
                reason: Reason::Unsupported,
                value: (),
            });
        }
        let completion = self.begin(resource, Kind::Shutdown, (), |()| Held::default())?;
        Ok(Operation::new(completion, |result, held| {
            Some(match held.failed {
                Some(failed) => Err(io::Error::other(failed)),
                None => count(result).map(|_| ()),
            })
        }))
    }

    /// Sets each option of `options` that is not `None` on `socket`.
    ///
    /// # Errors
    ///
    /// An error of the system, for example for `only_v6` on an IPv4 socket, or `InvalidInput` for a
    /// buffer size above `i32::MAX`.
    pub fn set_options(&mut self, socket: &impl Socket, options: TcpOptions) -> io::Result<()> {
        let fd = self.resources.borrow(socket.id().0).ok_or(io::ErrorKind::NotFound)?;
        options.apply(fd)
    }

    /// Reads each option of `socket`, with the value of the kernel.
    ///
    /// # Errors
    ///
    /// An error of the system.
    pub fn options(&self, socket: &impl Socket) -> io::Result<TcpOptions> {
        let fd = self.resources.borrow(socket.id().0).ok_or(io::ErrorKind::NotFound)?;
        TcpOptions::read(fd)
    }

    /// Makes the socket of the connect or the bind `id`: a new nonblocking socket of `kind` for
    /// `address`, which `setup` sets up before the bind or the connect, as a resource that `id`
    /// holds. Gives the resource and its descriptor.
    fn socket(
        &mut self,
        id: OpId,
        address: SocketAddr,
        kind: c_int,
        setup: impl FnOnce(BorrowedFd<'_>) -> io::Result<()>,
    ) -> io::Result<(Resource, std::os::fd::RawFd)> {
        let socket = new_socket(address, kind)?;
        setup(socket.as_fd())?;
        let fd = socket.as_raw_fd();
        let medium = if kind == sys::SOCK_DGRAM {
            Medium::Udp
        } else {
            Medium::Tcp
        };
        let resource = self
            .adopt(socket, medium)
            .map_err(|_| io::Error::from_raw_os_error(ENOMEM))?;
        self.hold(id, |held| held.accepted = Some(resource));
        Ok((resource, fd))
    }

    /// Changes the resources of the running operation `id`, at the home.
    fn hold(&self, id: OpId, change: impl FnOnce(&mut Held)) {
        if let Some(slot) = self.shared.entries.get(id.index() as usize) {
            slot.update(id, |held| {
                if let Some(held) = held {
                    change(held);
                }
            });
        }
    }

    /// Accepts a connection on `listener`. The future gives the stream and the address of its
    /// peer. A drop of the future asks for a cancel, and a connection that the accept took stays
    /// with the listener for the next accept.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation.
    pub fn accept<'a>(
        &mut self,
        listener: &'a mut TcpListener,
    ) -> Result<Operation<'a, io::Result<(TcpStream, SocketAddr)>>, Refused<()>> {
        let held = |()| Held {
            peer_len: SockAddrStorage::LEN,
            ..Held::default()
        };
        let completion = self.begin(listener.resource, Kind::Accept, (), held)?;
        Ok(Operation::new(completion, |result, held| {
            Some(count(result).and_then(|_| {
                let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
                let peer = held
                    .peer
                    .to_socket_addr(held.peer_len)
                    .ok_or(io::ErrorKind::InvalidData)?;
                Ok((TcpStream::new(resource), peer))
            }))
        }))
    }

    /// Receives into the capacity of `buffer` after its initialized bytes. The future gives the
    /// count of received bytes, and the buffer with them initialized after its earlier bytes. The
    /// kernel writes into the buffer itself, with no copy, except for the bytes that a dropped
    /// receive left (see [`TcpStream`]). A drop of the future asks for a cancel.
    ///
    /// # Errors
    ///
    /// A refusal, with the buffer, when the driver has no room for the operation.
    pub fn recv<'a, R: AsMut<ReadHalf>>(
        &mut self,
        reader: &'a mut R,
        buffer: Backing,
    ) -> Result<Operation<'a, (io::Result<usize>, Backing)>, Refused<Backing>> {
        let held = |buffer| Held {
            loan: Some(WriteLoan::new(buffer)),
            ..Held::default()
        };
        let completion = self.begin(reader.as_mut().resource, Kind::Recv, buffer, held)?;
        Ok(Operation::new(completion, |result, held| {
            Some((count(result), held.loan?.into_inner()))
        }))
    }

    /// Receives into a buffer that the kernel selects from the buffers of [`Driver::provide`] when
    /// the data comes, so the wait holds no buffer. The future gives that buffer as an exclusive
    /// backing with the received bytes initialized, so the caller can change them in place, for
    /// example to decrypt them, and then freeze them with `Lease::from`, with no allocation. When
    /// the backing, or its last lease or view, drops, on any thread, the buffer goes back to the
    /// kernel. A drop of the future asks for a cancel.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no provided buffers or no room for the operation.
    pub fn recv_selected<'a, R: AsMut<ReadHalf>>(
        &mut self,
        reader: &'a mut R,
    ) -> Result<Operation<'a, io::Result<Backing>>, Refused<()>> {
        if self.selected.is_none() {
            return Err(Refused {
                reason: Reason::Buffers,
                value: (),
            });
        }
        let completion = self.begin(reader.as_mut().resource, Kind::Selected, (), |()| Held::default())?;
        Ok(Operation::new(completion, |result, held| {
            // With no selected buffer, for example at the end of the stream, the backing is empty,
            // and an empty `Vec` allocates nothing.
            Some(count(result).map(|_| {
                held.loan
                    .map_or_else(|| Backing::from(Vec::new()), WriteLoan::into_inner)
            }))
        }))
    }

    /// Sends the bytes of `lease`. The future gives the count of sent bytes, and the lease. The
    /// kernel reads the lease itself, with no copy. A drop of the future does not cancel the send
    /// (see [`TcpStream`]).
    ///
    /// # Errors
    ///
    /// A refusal, with the lease, when the driver has no room for the operation.
    pub fn send<'a, W: AsMut<WriteHalf>>(
        &mut self,
        writer: &'a mut W,
        lease: Lease,
    ) -> Result<Operation<'a, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let held = |lease| Held {
            lease: Some(lease),
            ..Held::default()
        };
        let completion = self.begin(writer.as_mut().resource, Kind::Send, lease, held)?;
        Ok(Operation::new(completion, |result, held| {
            let sent = held
                .failed
                .map_or_else(|| count(result), |failed| Err(io::Error::other(failed)));
            Some((sent, held.lease?))
        }))
    }

    /// Closes `stream`. The close cancels each operation that runs on the stream, a dropped send
    /// too, waits until each one ends, releases the bytes that no receive took, and closes the
    /// socket. The future gives the progress that dropped sends owe.
    ///
    /// # Errors
    ///
    /// A refusal, with the stream, when the driver has no room for the operation.
    pub fn close(&mut self, stream: TcpStream) -> Result<Operation<'static, Closed>, Refused<TcpStream>> {
        let resource = stream.read.resource;
        let completion = self.close_resource(resource, stream)?;
        Ok(Operation::new(completion, |_, held| held.closed))
    }

    /// Closes `listener`. The close cancels the accept that runs, waits until it ends, closes each
    /// connection that no accept took, and closes the socket.
    ///
    /// # Errors
    ///
    /// A refusal, with the listener, when the driver has no room for the operation.
    pub fn close_listener(
        &mut self,
        listener: TcpListener,
    ) -> Result<Operation<'static, Closed>, Refused<TcpListener>> {
        let resource = listener.resource;
        let completion = self.close_resource(resource, listener)?;
        Ok(Operation::new(completion, |_, held| held.closed))
    }

    /// Starts an operation of `kind` in its direction of `resource`, with the resources that
    /// `held` makes from `value`. The caller holds the direction exclusively, so no future of an
    /// earlier operation of the direction exists: their interest ends first. The operation waits
    /// while the earlier one runs, and starts at its end.
    pub(super) fn begin<P>(
        &mut self,
        resource: Resource,
        kind: Kind,
        value: P,
        held: impl FnOnce(P) -> Held,
    ) -> Result<Completion, Refused<P>> {
        self.begin_entries(resource, kind, 1, value, held)
    }

    /// [`Driver::begin`] for an operation that pushes `entries` submission entries at one time,
    /// such as a batch. It refuses with [`Reason::Queue`] when the queue has fewer entries, because
    /// the operation can then never start.
    pub(super) fn begin_entries<P>(
        &mut self,
        resource: Resource,
        kind: Kind,
        entries: u32,
        value: P,
        held: impl FnOnce(P) -> Held,
    ) -> Result<Completion, Refused<P>> {
        if entries > self.ring.entries() {
            return Err(Refused {
                reason: Reason::Queue,
                value,
            });
        }
        let Some((current, first)) = self
            .direction(resource, kind)
            .map(|direction| (direction.current, direction.first))
        else {
            return Err(Refused {
                reason: Reason::Resource,
                value,
            });
        };
        if let Some(current) = current {
            self.forgo(current);
        }
        let mut next = first;
        while let Some(id) = next {
            // The link comes first: a receive or an accept leaves the queue in `forgo`.
            next = self.next(id);
            self.forgo(id);
        }
        let (table, resources) = (&self.table, &mut self.resources);
        let Some(owned) = resources.get(resource) else {
            return Err(Refused {
                reason: Reason::Resource,
                value,
            });
        };
        let has_result = owned.directions.has_result(kind);
        let Some(direction) = owned.directions.direction(kind) else {
            return Err(Refused {
                reason: Reason::Resource,
                value,
            });
        };
        direction.current = direction.current.filter(|&id| table.runs(id));
        let now = direction.current.is_none() && direction.first.is_none();
        let tag = Tag {
            resource: Some(resource),
            kind,
        };
        // An operation that waits, or that has its result here, pushes no entry now.
        let pushed = if now && !has_result { entries } else { 0 };
        let id = self
            .open(tag, pushed, value, held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        if now {
            if let Some(direction) = self.direction(resource, kind) {
                direction.current = Some(id);
            }
            // `open` made room for its entries, or the operation has its result here. A start that
            // finds no room waits first, and the next turn starts it.
            if !self.start(id, resource, kind) {
                self.wait_first(resource, kind, id);
            }
        } else {
            self.enqueue(resource, kind, id);
        }
        Ok(Completion::new(Arc::clone(&self.shared), id, !kind.writes()))
    }

    /// The direction of `kind` of `resource`.
    fn direction(&mut self, resource: Resource, kind: Kind) -> Option<&mut Direction> {
        self.resources.get(resource)?.directions.direction(kind)
    }

    /// The operation that waits after `id` in its direction.
    fn next(&self, id: OpId) -> Option<OpId> {
        let mut next = None;
        if let Some(slot) = self.shared.entries.get(id.index() as usize) {
            slot.update(id, |held| next = held.as_ref().and_then(|held| held.next));
        }
        next
    }

    /// Links `next` after the waiting operation `id`.
    fn link(&self, id: OpId, next: Option<OpId>) {
        if let Some(slot) = self.shared.entries.get(id.index() as usize) {
            slot.update(id, |held| {
                if let Some(held) = held {
                    held.next = next;
                }
            });
        }
    }

    /// Puts `id` at the end of the operations that wait in its direction.
    fn enqueue(&mut self, resource: Resource, kind: Kind, id: OpId) {
        let Some(last) = self.direction(resource, kind).map(|direction| direction.last) else {
            return;
        };
        match last {
            Some(last) => self.link(last, Some(id)),
            None => {
                if let Some(direction) = self.direction(resource, kind) {
                    direction.first = Some(id);
                }
            }
        }
        if let Some(direction) = self.direction(resource, kind) {
            direction.last = Some(id);
        }
    }

    /// Takes the first operation that waits in the direction of `kind`.
    pub(super) fn dequeue(&mut self, resource: Resource, kind: Kind) -> Option<OpId> {
        let id = self.direction(resource, kind)?.first?;
        let next = self.next(id);
        let direction = self.direction(resource, kind)?;
        direction.first = next;
        if next.is_none() {
            direction.last = None;
        }
        Some(id)
    }

    /// Takes `id` out of the operations that wait in its direction, if it waits there. It never
    /// waits again. Gives whether it waited.
    pub(super) fn unqueue(&mut self, tag: Tag, id: OpId) -> bool {
        let Some(resource) = tag.resource else {
            return false;
        };
        let Some(first) = self.direction(resource, tag.kind).map(|direction| direction.first) else {
            return false;
        };
        let (mut before, mut at) = (None, first);
        while let Some(op) = at {
            let next = self.next(op);
            if op == id {
                match before {
                    Some(before) => self.link(before, next),
                    None => {
                        if let Some(direction) = self.direction(resource, tag.kind) {
                            direction.first = next;
                        }
                    }
                }
                if let Some(direction) = self.direction(resource, tag.kind)
                    && direction.last == Some(id)
                {
                    direction.last = before;
                }
                return true;
            }
            (before, at) = (Some(op), next);
        }
        false
    }

    /// The operation `id` has no future any more: the caller started the next operation in its
    /// direction. Its interest ends, as at a drop of its future.
    fn forgo(&mut self, id: OpId) {
        match self.table.end_interest(id) {
            Some(ended) => self.claimed(id, ended),
            // A receive and an accept that run or wait ask to cancel, as at a drop.
            None => {
                if let Some(&tag) = self.table.resources(id)
                    && !tag.kind.writes()
                {
                    self.cancel(id);
                }
            }
        }
    }

    /// Starts the operation `id`: it takes the result that its direction holds, or the home
    /// submits it. Gives `false`, and changes nothing, when it needs a submission and the queue has
    /// no room.
    fn start(&mut self, id: OpId, resource: Resource, kind: Kind) -> bool {
        let Some(owned) = self.resources.get(resource) else {
            return true;
        };
        let Some(fd) = owned.fd.as_ref().map(OwnedFd::as_raw_fd) else {
            return true;
        };
        let pipe = owned.medium == Medium::Pipe;
        let Some(slot) = self.shared.entries.get(id.index() as usize) else {
            return true;
        };
        let directions = &mut owned.directions;
        let result = match (kind, directions.kept.take()) {
            (Kind::Recv, Some(Kept::Bytes { buffer, mut start })) => {
                let mut copied = 0;
                slot.update(id, |held| {
                    if let Some(held) = held
                        && let Some(loan) = held.loan.as_mut()
                    {
                        copied = loan.fill_from(buffer.bytes().get(start..).unwrap_or_default());
                        held.copied = copied;
                    }
                });
                start += copied;
                if start < buffer.initialized() {
                    directions.kept = Some(Kept::Bytes { buffer, start });
                }
                self.copied = self.copied.saturating_add(copied as u64);
                // The kept bytes came from one kernel result, so their count fits an `i32`.
                Some(i32::try_from(copied).unwrap_or(i32::MAX))
            }
            (Kind::Selected, Some(Kept::Bytes { mut buffer, start })) => {
                let len = buffer.initialized() - start;
                if start > 0 {
                    buffer.bytes_mut().copy_within(start.., 0);
                    buffer.truncate(len);
                    self.copied = self.copied.saturating_add(len as u64);
                }
                slot.update(id, |held| {
                    if let Some(held) = held {
                        held.copied = if start > 0 { len } else { 0 };
                        held.loan = Some(WriteLoan::new(buffer));
                    }
                });
                Some(i32::try_from(len).unwrap_or(i32::MAX))
            }
            (Kind::Recv | Kind::Selected, Some(Kept::Error(error))) => Some(error),
            (Kind::Accept, Some(Kept::Connection { fd, peer, peer_len })) => {
                match self.resources.adopt(fd, Medium::Tcp) {
                    Ok(stream) => slot.update(id, |held| {
                        if let Some(held) = held {
                            held.accepted = Some(stream);
                            held.peer = peer;
                            held.peer_len = peer_len;
                        }
                    }),
                    // With no free resource slot, the listener keeps the connection, and this
                    // accept gives `OutOfMemory`.
                    Err(fd) => {
                        if let Some(listener) = self.resources.get(resource) {
                            listener.directions.keep_connection(fd, peer, peer_len);
                        }
                    }
                }
                Some(0)
            }
            (Kind::RecvFrom, Some(Kept::Datagram { buffer, received })) => {
                let (result, copied) = deliver_copy(slot, id, &buffer, received);
                self.copied = self.copied.saturating_add(copied as u64);
                Some(result)
            }
            (Kind::RecvFrom, Some(Kept::Arrivals(mut arrivals))) => {
                let first = arrivals.take_first();
                directions.kept = udp::keep_arrivals(arrivals).map(Kept::Arrivals);
                first.map(|(buffer, received)| {
                    let (result, copied) = deliver_copy(slot, id, &buffer, received);
                    self.copied = self.copied.saturating_add(copied as u64);
                    result
                })
            }
            (Kind::RecvSelectedFrom, Some(Kept::Datagram { buffer, received })) => {
                Some(deliver(slot, id, buffer, received))
            }
            (Kind::RecvSelectedFrom, Some(Kept::Arrivals(mut arrivals))) => {
                let first = arrivals.take_first();
                directions.kept = udp::keep_arrivals(arrivals).map(Kept::Arrivals);
                first.map(|(buffer, received)| deliver(slot, id, buffer, received))
            }
            (Kind::RecvBatch, Some(Kept::Datagram { buffer, received })) => {
                let mut taken = Some((buffer, received));
                slot.update(id, |held| {
                    if let Some(Held {
                        batch: Some(Batch::Receive(arrivals)),
                        ..
                    }) = held
                        && let Some((buffer, received)) = taken.take()
                    {
                        taken = arrivals.add(buffer, received);
                    }
                });
                directions.kept = taken.map(|(buffer, received)| Kept::Datagram { buffer, received });
                Some(1)
            }
            (Kind::RecvBatch, Some(Kept::Arrivals(mut kept))) => {
                let mut count = 0;
                slot.update(id, |held| {
                    if let Some(Held {
                        batch: Some(Batch::Receive(arrivals)),
                        ..
                    }) = held
                    {
                        while arrivals.len() < arrivals.capacity() {
                            let Some((buffer, received)) = kept.take_first() else {
                                break;
                            };
                            if arrivals.add(buffer, received).is_none() {
                                count += 1;
                            }
                        }
                    }
                });
                directions.kept = udp::keep_arrivals(kept).map(Kept::Arrivals);
                Some(count)
            }
            (Kind::RecvFrom | Kind::RecvSelectedFrom | Kind::RecvBatch, Some(Kept::Error(error))) => Some(error),
            (Kind::Send | Kind::Vectored | Kind::Shutdown, kept) => {
                directions.kept = kept;
                match directions.sent {
                    Sent::Ok => None,
                    // A dropped send that waited never ran, so it gives no report.
                    Sent::Owe if !self.table.interest(id) => Some(-ECANCELED),
                    sent => {
                        let failed = WriteFailed {
                            sent: (sent == Sent::Owe).then_some(directions.unreported),
                        };
                        directions.sent = Sent::Reported;
                        directions.unreported = 0;
                        slot.update(id, |held| {
                            if let Some(held) = held {
                                held.failed = Some(failed);
                            }
                        });
                        Some(0)
                    }
                }
            }
            (_, kept) => {
                directions.kept = kept;
                None
            }
        };
        if let Some(result) = result {
            self.complete_now(id, result);
            return true;
        }
        if kind.batch() {
            return self.start_batch(id, fd);
        }
        if !self.room(1) {
            return false;
        }
        let group = self.selected.as_ref().map_or(0, |selected| selected.ring.group());
        let slot_len = self.selected.as_ref().map_or(0, |selected| selected.len as usize);
        let entry = move |held: &mut Held| match kind {
            Kind::Recv => match held.loan.as_mut() {
                Some(loan) => {
                    let (buffer, len) = (loan.as_mut_ptr(), u32::try_from(loan.capacity()).unwrap_or(u32::MAX));
                    if pipe {
                        Sqe::read(fd, buffer, len)
                    } else {
                        Sqe::recv(fd, buffer, len)
                    }
                }
                // A receive always has its loan.
                None => Sqe::nop(),
            },
            Kind::Selected if pipe => Sqe::read_selected(fd, group),
            Kind::Selected => Sqe::recv_selected(fd, group),
            Kind::Send => match held.lease.as_ref() {
                Some(lease) => {
                    let (bytes, len) = (lease.as_ptr(), u32::try_from(lease.initialized()).unwrap_or(u32::MAX));
                    if pipe {
                        Sqe::write(fd, bytes, len)
                    } else {
                        Sqe::send(fd, bytes, len)
                    }
                }
                // A send always has its lease.
                None => Sqe::nop(),
            },
            Kind::Vectored => match held.regions.as_mut() {
                Some(regions) if pipe => {
                    let (vectors, count) = regions.vectors();
                    Sqe::writev(fd, vectors, count)
                }
                Some(regions) => Sqe::send_message(fd, regions.message()),
                // A vectored send always has its regions.
                None => Sqe::nop(),
            },
            Kind::Shutdown => Sqe::shutdown(fd, sys::SHUT_WR),
            Kind::RecvFrom => match held.loan.as_mut() {
                Some(loan) => {
                    let (region, len) = (loan.as_mut_ptr(), loan.capacity());
                    Sqe::recv_message(fd, held.message.receive(region, len), 0)
                }
                // A receive always has its loan.
                None => Sqe::nop(),
            },
            Kind::RecvSelectedFrom => {
                Sqe::recv_message_selected(fd, held.message.receive(ptr::null_mut(), slot_len), 0, group)
            }
            Kind::SendTo => match held.datagram.as_ref() {
                Some(datagram) => Sqe::send_message(fd, held.message.send(datagram)),
                // A datagram send always has its datagram.
                None => Sqe::nop(),
            },
            // A batch submits in `start_batch`.
            Kind::SendBatch | Kind::RecvBatch => Sqe::nop(),
            Kind::Accept | Kind::Other | Kind::Close | Kind::Connect | Kind::Bind | Kind::File(_) => {
                Sqe::accept(fd, (&raw mut held.peer).cast(), &raw mut held.peer_len)
            }
        };
        // SAFETY: the entry names the loan, the lease, the message, or the peer address in the
        // resources of `id`, which the home keeps until its terminal event, and the descriptor of
        // `resource`, which stays open until its close, after each of its operations ends. The
        // queue has room.
        #[expect(unsafe_code, reason = "the kernel uses the memory of the resources of the operation")]
        unsafe {
            self.push(id, entry);
        }
        true
    }

    /// Submits the batch `id` on the socket `fd`: one entry for each datagram, each linked to the
    /// next, in one kernel entry. A batch receive waits only in its first entry. Gives `false`, and
    /// changes nothing, when the queue has no room for each entry.
    fn start_batch(&mut self, id: OpId, fd: std::os::fd::RawFd) -> bool {
        let mut entries = 0;
        self.hold(id, |held| {
            entries = match &held.batch {
                Some(Batch::Send(datagrams)) => datagrams.len(),
                Some(Batch::Receive(arrivals)) => arrivals.capacity(),
                None => 0,
            };
        });
        if entries == 0 {
            self.complete_now(id, 0);
            return true;
        }
        if !self.room(u32::try_from(entries).unwrap_or(u32::MAX)) {
            return false;
        }
        let group = self.selected.as_ref().map_or(0, |selected| selected.ring.group());
        let slot_len = self.selected.as_ref().map_or(0, |selected| selected.len as usize);
        for index in 0..entries {
            let last = index + 1 == entries;
            let entry = move |held: &mut Held| {
                let sqe = match held.batch.as_mut() {
                    Some(Batch::Send(datagrams)) => datagrams
                        .message(index)
                        .map_or_else(Sqe::nop, |message| Sqe::send_message(fd, message)),
                    Some(Batch::Receive(arrivals)) => {
                        // The first receive waits for a datagram; the next ones take only the
                        // datagrams that wait.
                        let flags = if index == 0 {
                            0
                        } else {
                            rsnetwork_udp::sys::MSG_DONTWAIT
                        };
                        arrivals.message(index, slot_len).map_or_else(Sqe::nop, |message| {
                            Sqe::recv_message_selected(fd, message, flags, group)
                        })
                    }
                    None => Sqe::nop(),
                };
                if last { sqe } else { sqe.linked() }
            };
            // SAFETY: each entry names a message of the batch in the resources of `id`, in the heap
            // array of the batch, which the home keeps until the last completion, and the socket,
            // which stays open until its close. The queue has room for each entry, so the links
            // join only these entries.
            #[expect(unsafe_code, reason = "the kernel uses the messages of the batch")]
            unsafe {
                self.push(id, entry);
            }
        }
        true
    }

    /// Starts each operation that waits for an operation that ended, and ends each close whose
    /// resource has no running operation.
    pub(super) fn attend(&mut self) {
        while let Some(resource) = self.pending.pop() {
            let Some(owned) = self.resources.get(resource) else {
                continue;
            };
            owned.directions.pending = false;
            for kind in [Kind::Recv, Kind::Send] {
                let table = &self.table;
                let runs = self
                    .resources
                    .get(resource)
                    .and_then(|owned| owned.directions.direction(kind))
                    .and_then(|direction| direction.current)
                    .is_some_and(|current| table.runs(current));
                if runs {
                    continue;
                }
                while let Some(id) = self.dequeue(resource, kind) {
                    if !self.table.runs(id) {
                        continue;
                    }
                    if let Some(direction) = self.direction(resource, kind) {
                        direction.current = Some(id);
                    }
                    let kind = self.table.resources(id).map_or(kind, |tag| tag.kind);
                    if self.start(id, resource, kind) {
                        break;
                    }
                    self.wait_first(resource, kind, id);
                    return;
                }
            }
            self.close_if_idle(resource);
        }
    }

    /// Makes `id` wait first again in its direction of `resource`, because the queue had no room
    /// for it. The next turn starts it.
    fn wait_first(&mut self, resource: Resource, kind: Kind, id: OpId) {
        let first = self.direction(resource, kind).and_then(|direction| direction.first);
        self.link(id, first);
        if let Some(direction) = self.direction(resource, kind) {
            direction.current = None;
            direction.first = Some(id);
            if direction.last.is_none() {
                direction.last = Some(id);
            }
        }
        if let Some(owned) = self.resources.get(resource)
            && !owned.directions.pending
        {
            owned.directions.pending = true;
            self.pending.push(resource);
        }
    }

    /// Cancels each operation of `resource`, and closes it after each one ends.
    pub(super) fn close_resource<P>(&mut self, resource: Resource, value: P) -> Result<Completion, Refused<P>> {
        if self.resources.get(resource).is_none() {
            return Err(Refused {
                reason: Reason::Resource,
                value,
            });
        }
        let tag = Tag {
            resource: Some(resource),
            kind: Kind::Close,
        };
        let id = self
            .open(tag, 0, value, |_| Held::default())
            .map_err(|(reason, value)| Refused { reason, value })?;
        if let Some(owned) = self.resources.get(resource) {
            owned.directions.closing = Some(id);
        }
        for kind in [Kind::Recv, Kind::Send] {
            // An operation that waits never ran, so it ends now; a send that waits too.
            while let Some(operation) = self.dequeue(resource, kind) {
                if self.table.end_interest(operation).is_none() {
                    self.complete_now(operation, -ECANCELED);
                }
            }
            let current = self.direction(resource, kind).and_then(|direction| direction.current);
            if let Some(operation) = current {
                match self.table.end_interest(operation) {
                    Some(ended) => self.claimed(operation, ended),
                    // The close cancels each operation that runs, a dropped send too.
                    None => self.cancel(operation),
                }
            }
        }
        self.close_if_idle(resource);
        // A close can release a resource that the pending list holds, so the list is empty
        // before a new resource takes that slot.
        self.attend();
        Ok(Completion::new(Arc::clone(&self.shared), id, false))
    }

    /// Ends the close of `resource` when no operation of it runs: releases what its directions
    /// keep, closes its descriptor, and gives the report.
    fn close_if_idle(&mut self, resource: Resource) {
        let table = &self.table;
        let Some(owned) = self.resources.get(resource) else {
            return;
        };
        let directions = &owned.directions;
        let Some(id) = directions.closing else {
            return;
        };
        let runs = [directions.read.current, directions.write.current]
            .into_iter()
            .flatten()
            .any(|operation| table.runs(operation))
            || directions.read.first.is_some()
            || directions.write.first.is_some()
            || owned.running > 0;
        if runs {
            return;
        }
        let closed = Closed {
            sent: (directions.sent == Sent::Owe).then_some(directions.unreported),
        };
        // The release drops the kept bytes and connections, and the descriptor.
        drop(self.resources.release(resource));
        if let Some(slot) = self.shared.entries.get(id.index() as usize) {
            slot.update(id, |held| {
                if let Some(held) = held {
                    held.closed = Some(closed);
                }
            });
        }
        self.complete_now(id, 0);
    }
}

/// Gives the kept datagram of `buffer` with `received` to the receive `id`, copied into its
/// buffer: one copy, which [`Driver::copied`] counts. Gives the result of the receive and the
/// bytes that it copied.
fn deliver_copy(slot: &crate::completion::Entry<Held>, id: OpId, buffer: &Backing, received: Received) -> (i32, usize) {
    let mut copied = 0;
    slot.update(id, |held| {
        if let Some(held) = held
            && let Some(loan) = held.loan.as_mut()
        {
            let (facts, count) = udp::copy_kept(buffer, received, loan);
            held.received = Some(facts);
            copied = count;
        }
    });
    // The kept bytes came from one kernel result, so their count fits an `i32`.
    (i32::try_from(copied).unwrap_or(i32::MAX), copied)
}

/// Gives the kept datagram of `buffer` with `received` to the selecting receive `id`, with no
/// copy. Gives the result of the receive.
fn deliver(slot: &crate::completion::Entry<Held>, id: OpId, buffer: Backing, received: Received) -> i32 {
    let len = i32::try_from(received.len).unwrap_or(i32::MAX);
    slot.update(id, |held| {
        if let Some(held) = held {
            held.loan = Some(WriteLoan::new(buffer));
            held.received = Some(received);
        }
    });
    len
}

/// The negative errno of `error`, as a kernel result gives it.
fn errno(error: &io::Error) -> i32 {
    error.raw_os_error().unwrap_or(EINVAL).saturating_neg()
}

/// The size of an `int`, as a `socklen_t`.
const INT_LEN: u32 = 4;

const _: () = assert!(size_of::<c_int>() == 4);

/// A new nonblocking socket of `kind`, `SOCK_STREAM` or `SOCK_DGRAM`, of the family of `address`.
#[expect(unsafe_code, reason = "the C library gives a new descriptor")]
fn new_socket(address: SocketAddr, kind: c_int) -> io::Result<OwnedFd> {
    let family = if address.is_ipv4() {
        rsnetwork_udp::AF_INET
    } else {
        AF_INET6
    };
    let kind = kind | sys::SOCK_NONBLOCK | sys::SOCK_CLOEXEC;
    // SAFETY: the call takes no pointer.
    let fd = unsafe { sys::socket(c_int::from(family), kind, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the C library gave a new descriptor that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Sets the option `name` of `level` of `socket` to the `int` `value`.
#[expect(unsafe_code, reason = "the call reads one `int` that lives during the call")]
fn set_int(socket: BorrowedFd<'_>, level: c_int, name: c_int, value: c_int) -> io::Result<()> {
    // SAFETY: `value` is an `int` that lives during the call, and `INT_LEN` is its size.
    let result = unsafe { sys::setsockopt(socket.as_raw_fd(), level, name, (&raw const value).cast(), INT_LEN) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// The `int` option `name` of `level` of `socket`.
#[expect(unsafe_code, reason = "the kernel writes one `int` that lives during the call")]
fn int(socket: BorrowedFd<'_>, level: c_int, name: c_int) -> io::Result<c_int> {
    let (mut value, mut len): (c_int, u32) = (0, INT_LEN);
    // SAFETY: the kernel writes at most `len` bytes into `value`, which is an `int` that lives
    // during the call, and the length into `len`.
    let result = unsafe { sys::getsockopt(socket.as_raw_fd(), level, name, (&raw mut value).cast(), &raw mut len) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

/// The local address of `socket`, and its length.
#[expect(
    unsafe_code,
    reason = "the kernel writes the address into storage that lives during the call"
)]
pub(super) fn local_address(socket: impl std::os::fd::AsFd) -> Option<(SockAddrStorage, u32)> {
    let (mut address, mut len) = (SockAddrStorage::EMPTY, SockAddrStorage::LEN);
    // SAFETY: the kernel writes at most `len` bytes into `address`, which has room for `LEN` bytes
    // and lives during the call, and the length into `len`.
    let result = unsafe { sys::getsockname(socket.as_fd().as_raw_fd(), (&raw mut address).cast(), &raw mut len) };
    (result == 0).then_some((address, len))
}

/// Binds `socket` to the address of `len` bytes in `address`, and with `listen` listens on it,
/// with system calls.
#[expect(unsafe_code, reason = "the call reads the address, which lives during the call")]
fn bind_now(socket: BorrowedFd<'_>, address: &SockAddrStorage, len: u32, listen: bool) -> io::Result<()> {
    // SAFETY: `address` holds an address of `len` bytes during the call.
    if unsafe { sys::bind(socket.as_raw_fd(), ptr::from_ref(address).cast(), len) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the call takes no pointer.
    if listen && unsafe { sys::listen(socket.as_raw_fd(), BACKLOG) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
