//! TCP on the driver: bind and listen, connect, accept, receive into an owned buffer or into a
//! buffer that the kernel selects, send from a lease or from several, half-close, the socket
//! options, and close. Each socket is nonblocking, and the driver owns its descriptor.
//!
//! Each direction of a stream, and the accepts of a listener, runs one operation at a time. The
//! home keeps the state of each direction: the operation that runs, one operation that waits for
//! it, what an abandoned receive or accept left, and the progress that dropped sends owe.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::ffi::c_int;
#[cfg(unix)]
use core::ffi::c_void;
use core::net::SocketAddr;
use core::{fmt, ptr};
use std::io;
#[cfg(unix)]
use std::os::fd::{AsRawFd as _, FromRawFd as _};

#[cfg(unix)]
use rsnetwork_udp::sys::{self, IoVec, IoVec as Region, MsgHdr};
use rsnetwork_udp::{AF_INET6, Received, SaFamily, SockAddrStorage};

#[cfg(target_os = "windows")]
use super::iocp::sys::{self, WsaBuf as Region};
use super::udp::{self, Arrivals, Batch};
use super::{
    Borrowed, Call, Completion, Driver, ECANCELED, Handle, Held, Kind, Medium, Operation, Payload, Reason, Refused,
    Resource, Resources, Route, Shot, Tag, borrow, number,
};
use crate::buffer::{Backing, Buffer as _, BufferMut as _, Lease, WriteLoan};
use crate::sync::Arc;
use crate::table::{OpId, Shape};
use crate::uring::Sqe;

/// `ENOBUFS` (`include/uapi/asm-generic/errno.h`, and `sys/errno.h` of macOS), on Windows
/// `WSAENOBUFS` (`winerror.h`): a selecting receive found no free buffer, and the data stays in
/// the socket.
#[cfg(target_os = "linux")]
const ENOBUFS: i32 = 105;
#[cfg(target_os = "macos")]
const ENOBUFS: i32 = 55;
#[cfg(target_os = "windows")]
pub(super) const ENOBUFS: i32 = 10055;
/// `EAGAIN`, on Windows `WSAEWOULDBLOCK`: a receive with no wait found no datagram.
#[cfg(target_os = "linux")]
pub(super) const EAGAIN: i32 = 11;
#[cfg(target_os = "macos")]
pub(super) const EAGAIN: i32 = 35;
#[cfg(target_os = "windows")]
pub(super) const EAGAIN: i32 = 10035;
/// `ENOMEM`, on Windows `ERROR_NOT_ENOUGH_MEMORY`: each resource slot of the driver is in use. It
/// reads as `OutOfMemory`.
#[cfg(unix)]
const ENOMEM: i32 = 12;
#[cfg(target_os = "windows")]
const ENOMEM: i32 = 8;
/// `EINVAL`, on Windows `WSAEINVAL`: an option value that the kernel cannot take.
#[cfg(unix)]
const EINVAL: i32 = 22;
#[cfg(target_os = "windows")]
const EINVAL: i32 = 10022;

/// The backlog of a listener: the most that the kernel takes, `net.core.somaxconn`.
pub(super) const BACKLOG: c_int = c_int::MAX;

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

    /// The resource of the listener, whose close a local type records at its drop.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) const fn resource(&self) -> Resource {
        self.resource
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
///   the count of the bytes that went, and each later send gives [`WriteFailed`] with
///   [`SendCount::Settled`]. A
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

    /// The resource of the stream, whose close a local type records at its drop.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) const fn resource(&self) -> Resource {
        self.read.resource
    }
}

/// The read half of a [`TcpStream`], and on Linux of a `UdpSocket` or a `PipeReader`: its receives
/// borrow it.
#[derive(Debug)]
pub struct ReadHalf {
    pub(super) resource: Resource,
}

/// The write half of a [`TcpStream`], and on Linux of a `UdpSocket` or a `PipeWriter`: its sends
/// borrow it.
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
    /// between them (`SO_REUSEPORT`). Windows has no such option: a value gives the error
    /// `WSAENOPROTOOPT`, and [`Driver::options`] gives `None`.
    pub reuse_port: Option<bool>,
}

impl TcpOptions {
    /// Sets each option that is not `None` on `socket`.
    pub(super) fn apply(self, socket: Borrowed<'_>) -> io::Result<()> {
        #[cfg(target_os = "windows")]
        if self.reuse_port.is_some() {
            return Err(io::Error::from_raw_os_error(sys::WSAENOPROTOOPT));
        }
        let flags = [
            (self.no_delay, sys::IPPROTO_TCP, sys::TCP_NODELAY),
            (self.keepalive, sys::SOL_SOCKET, sys::SO_KEEPALIVE),
            (self.only_v6, sys::IPPROTO_IPV6, sys::IPV6_V6ONLY),
            #[cfg(unix)]
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
    pub(super) fn read(socket: Borrowed<'_>) -> io::Result<Self> {
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
            #[cfg(unix)]
            reuse_port: flag(sys::SOL_SOCKET, sys::SO_REUSEPORT)?,
            #[cfg(target_os = "windows")]
            reuse_port: None,
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
///         // `failed.sent()` gives the count of the bytes of the dropped sends. Close the stream.
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
    /// The iovec array of the leases, or on Windows the `WSABUF` array, which the home writes
    /// before each submission. Its capacity is at least the number of leases.
    vectors: Vec<Region>,
    /// The message header that names `vectors`, which the home writes before each submission.
    #[cfg(unix)]
    header: MsgHdr,
}

/// A message header with no address, no region, and no control message.
#[cfg(unix)]
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
            #[cfg(unix)]
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
    #[cfg(unix)]
    fn message(&mut self) -> *const MsgHdr {
        self.vectors();
        self.header = MsgHdr {
            msg_iov: self.vectors.as_mut_ptr(),
            #[cfg(target_os = "linux")]
            msg_iovlen: self.vectors.len(),
            // The kernel refuses a count above `IOV_MAX`, also one above `int`.
            #[cfg(target_os = "macos")]
            msg_iovlen: c_int::try_from(self.vectors.len()).unwrap_or(c_int::MAX),
            ..EMPTY_HEADER
        };
        &raw const self.header
    }

    /// The iovec array of each region, and its length, for a submission. Its capacity holds the
    /// array, so it allocates nothing.
    pub(super) fn vectors(&mut self) -> (*const Region, u32) {
        self.vectors.clear();
        #[cfg(unix)]
        self.vectors.extend(self.leases.iter().map(|lease| IoVec {
            iov_base: lease.as_ptr().cast_mut().cast::<c_void>(),
            iov_len: lease.initialized(),
        }));
        // A `WSABUF` holds at most `u32::MAX` bytes, so a larger lease sends a prefix, and the
        // count of the send reports it.
        #[cfg(target_os = "windows")]
        self.vectors.extend(self.leases.iter().map(|lease| Region {
            len: u32::try_from(lease.initialized()).unwrap_or(u32::MAX),
            buf: lease.as_ptr().cast_mut().cast(),
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
            #[cfg(unix)]
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
    sent: SendCount,
}

impl WriteFailed {
    /// The count of the bytes that the dropped sends sent. The first send after the failure gives
    /// [`SendCount::Sent`], or [`SendCount::Unknown`] after an abort on Windows, and each later
    /// send gives [`SendCount::Settled`].
    #[must_use]
    pub const fn sent(self) -> SendCount {
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
            SendCount::Sent(sent) => write!(
                f,
                "the write direction failed after a dropped send, which sent {sent} bytes"
            ),
            SendCount::Unknown => f.write_str(
                "the write direction failed after a dropped send, and the kernel gave no count of its bytes",
            ),
            SendCount::Settled => f.write_str("the write direction failed after a dropped send"),
        }
    }
}

impl core::error::Error for WriteFailed {}

/// What a close did, after each operation of its socket ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    sent: SendCount,
}

impl Closed {
    /// The count of the bytes that the dropped sends sent, when a send that the close cancelled,
    /// or a failed send, owes it: [`SendCount::Sent`], or [`SendCount::Unknown`] when the close
    /// aborted a send on Windows. [`SendCount::Settled`] when no send owes it, also when a send
    /// already gave it with [`WriteFailed`].
    #[must_use]
    pub const fn sent(self) -> SendCount {
        self.sent
    }
}

/// The count of the bytes that the dropped sends of a stream sent, which [`WriteFailed::sent`] and
/// [`Closed::sent`] give one time, from the first byte of the first send whose result no future
/// took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendCount {
    /// No count is owed: each dropped send sent all its bytes, or an earlier result gave the count.
    Settled,
    /// The dropped sends sent this many bytes, and the peer can have read them.
    Sent(usize),
    /// The kernel gave no count, so you cannot know which bytes reached the peer: this occurs only
    /// on IOCP, after a cancel of a send that ran aborted the connection.
    Unknown,
}

/// One direction of a stream, or the accepts of a listener: the operation that runs or ran last,
/// and the operations that wait for it, in start order, linked through `Held::next`.
#[derive(Debug, Default)]
struct Direction {
    current: Option<OpId>,
    first: Option<OpId>,
    last: Option<OpId>,
}

/// No place of the [`Shelf`]: the end of a list.
const NO_PLACE: u32 = u32::MAX;

/// The filled buffers that the multishot receives of the streams of a driver keep for their next
/// receives ([`Shot::Multi`](super::Shot::Multi)), as a list for the read direction of each
/// stream. Each place holds one buffer, and the shelf has one place for each slot of the provided
/// pool and one for each resource slot: the buffers of the pool, and one buffer of a caller that a
/// receive copied into and dropped. So each kept buffer has a place, and a push allocates nothing.
#[derive(Debug)]
pub(super) struct Shelf {
    places: Box<[Place]>,
    /// The first free place, or `NO_PLACE`.
    free: u32,
}

/// One place of the [`Shelf`].
#[derive(Debug)]
struct Place {
    buffer: Option<Backing>,
    /// The next place of its list, or of the free places.
    next: u32,
}

impl Default for Shelf {
    fn default() -> Self {
        Self {
            places: Box::default(),
            free: NO_PLACE,
        }
    }
}

impl Shelf {
    /// A shelf of `places` free places. It allocates here, one time, or refuses.
    pub(super) fn new(places: u32) -> Result<Self, core::alloc::AllocError> {
        Ok(Self {
            places: crate::memory::items(places, |index| Place {
                buffer: None,
                next: if index + 1 < places { index + 1 } else { NO_PLACE },
            })?,
            free: if places > 0 { 0 } else { NO_PLACE },
        })
    }

    /// Puts `buffer` at the end of `list`, or at its start when `first`.
    fn put(&mut self, list: &mut Shelved, buffer: Backing, first: bool) {
        let place = self.free;
        // Each kept buffer is a slot of the pool, or the one buffer of a caller of its stream.
        let slot = self
            .places
            .get_mut(place as usize)
            .expect("the shelf has a place for each kept buffer");
        self.free = slot.next;
        slot.buffer = Some(buffer);
        if first || list.first == NO_PLACE {
            slot.next = list.first;
            if list.first == NO_PLACE {
                list.last = place;
            }
            list.first = place;
            list.start = 0;
        } else {
            slot.next = NO_PLACE;
            if let Some(last) = self.places.get_mut(list.last as usize) {
                last.next = place;
            }
            list.last = place;
        }
        list.len += 1;
    }

    /// The first buffer of `list`, with the bytes that a receive took from its start.
    fn first(&self, list: &Shelved) -> Option<(&Backing, usize)> {
        let buffer = self.places.get(list.first as usize)?.buffer.as_ref()?;
        Some((buffer, list.start as usize))
    }

    /// Takes the first buffer of `list`, with the bytes that a receive took from its start.
    fn take(&mut self, list: &mut Shelved) -> Option<(Backing, usize)> {
        let place = list.first;
        let slot = self.places.get_mut(place as usize)?;
        let buffer = slot.buffer.take()?;
        let start = list.start as usize;
        list.first = slot.next;
        if list.first == NO_PLACE {
            list.last = NO_PLACE;
        }
        list.start = 0;
        list.len -= 1;
        slot.next = self.free;
        self.free = place;
        Some((buffer, start))
    }

    /// Drops each buffer of `list`, whose slots go back to their pool.
    pub(super) fn clear(&mut self, list: &mut Shelved) {
        while self.take(list).is_some() {}
    }
}

/// The buffers that the read direction of a stream keeps on the [`Shelf`], oldest first.
#[derive(Debug, Clone, Copy)]
pub(super) struct Shelved {
    first: u32,
    last: u32,
    len: u32,
    /// The bytes of the first buffer that a receive into the caller's buffer copied. A shelved
    /// buffer holds the bytes of one result of the kernel, so they fit a `u32`.
    start: u32,
}

impl Default for Shelved {
    fn default() -> Self {
        Self {
            first: NO_PLACE,
            last: NO_PLACE,
            len: 0,
            start: 0,
        }
    }
}

impl Shelved {
    const fn is_empty(&self) -> bool {
        self.first == NO_PLACE
    }
}

/// A terminal result of the kernel, or none, in 4 bytes, so that the record of a stream keeps its
/// hot fields in two cache lines. No result of the kernel is `i32::MIN`: a count is at least 0, and
/// an error is a negated `errno`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Terminal(i32);

impl Terminal {
    /// No result.
    const NONE: Self = Self(i32::MIN);

    /// The result `result` of the kernel.
    fn of(result: i32) -> Self {
        assert_ne!(result, i32::MIN, "no result of the kernel is i32::MIN");
        Self(result)
    }

    const fn get(self) -> Option<i32> {
        if self.0 == i32::MIN { None } else { Some(self.0) }
    }

    const fn is_some(self) -> bool {
        self.0 != i32::MIN
    }

    const fn take(&mut self) -> Option<i32> {
        core::mem::replace(self, Self::NONE).get()
    }
}

impl Default for Terminal {
    fn default() -> Self {
        Self::NONE
    }
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
        fd: Handle,
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
    /// The direction failed, and the progress of a send is unknown: on Windows, a cancel of a send
    /// that runs aborts the connection, and the kernel gives no count. No report gives a count.
    #[cfg(target_os = "windows")]
    Lost,
}

/// The multishot receive of the read direction of a stream ([`Shot::Multi`](super::Shot::Multi)).
/// Its completions name the resource slot of the stream ([`Resource::record`]), so it holds no
/// operation slot.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Armed {
    /// No multishot receive runs.
    #[default]
    No,
    /// It runs.
    Runs,
    /// It runs, and the driver asked the kernel to cancel it.
    Ending,
}

/// The send of a local stream in its record, with no operation ([`Driver::send_record`]): from its
/// start to the take of its result, or to its terminal event when no future has interest. It runs
/// while it holds the lease and no result. Its waiter is [`Directions::sender`], in a later line.
#[cfg(all(feature = "local", target_os = "linux"))]
#[derive(Debug, Default)]
struct Sending {
    /// The lease that the kernel reads. The record holds a send while it holds the lease.
    lease: Option<Lease>,
    /// The bytes that the earlier sends of the kernel of a send of each byte sent.
    went: usize,
    /// The result of the terminal event, until the take.
    result: Terminal,
    /// A future has interest in the result.
    interest: bool,
    /// The driver asked the kernel to cancel the send.
    cancelled: bool,
    /// The send sends each byte of the lease, in as many sends of the kernel as it takes
    /// ([`Kind::SendAll`]).
    all: bool,
    /// A poll found no result and set [`Directions::sender`], which the terminal event wakes.
    waits: bool,
}

#[cfg(all(feature = "local", target_os = "linux"))]
impl Sending {
    /// The terminal event of the send did not come.
    const fn runs(&self) -> bool {
        self.lease.is_some() && !self.result.is_some()
    }
}

/// The directions of a stream or a listener, at the home.
///
/// The order of the fields is stated: a local receive and a local send of the record of a stream
/// read only the fields before `sender`, while `operated` is false, and the slot of the stream has
/// them in its first two cache lines. `sender` and `unreported` are read only behind a flag. The
/// state of the operations of the table comes after them.
#[derive(Debug, Default)]
#[repr(C)]
pub(super) struct Directions {
    armed: Armed,
    /// The current receive waits for a buffer of `armed`, or for its end, with no submission.
    awaiting: bool,
    /// `armed` filled no buffer yet.
    fresh: bool,
    /// The resource is in the pending list of the driver.
    pending: bool,
    /// A local receive waits for a buffer of `armed`, or for its end, with no operation, until its
    /// next poll ([`Driver::poll_record`]).
    #[cfg(all(feature = "local", target_os = "linux"))]
    reading: bool,
    sent: Sent,
    /// An operation of the table started on the resource, or its close started. Before that,
    /// `read`, `write`, `kept`, and `closing` hold nothing, so the record does not read them.
    operated: bool,
    /// The send of each byte of the write direction sent only part of its bytes, and its rest goes
    /// again at the next attend ([`Driver::resend`]): the operation that runs, or the send of the
    /// record.
    again: bool,
    /// `unreported` is above 0.
    counted: bool,
    /// The terminal result of the last multishot receive, after `shelved`: the end of the stream,
    /// or an error.
    ended: Terminal,
    /// The buffers that `armed` filled and no receive took, after `kept`.
    shelved: Shelved,
    /// The waiter of the local receive that waits, which a completion of `armed` wakes.
    #[cfg(all(feature = "local", target_os = "linux"))]
    reader: super::home::Waiter,
    #[cfg(all(feature = "local", target_os = "linux"))]
    sending: Sending,
    /// The waiter of the future of the send of the record, while `sending.waits`.
    #[cfg(all(feature = "local", target_os = "linux"))]
    sender: super::home::Waiter,
    /// The bytes that dropped sends sent, since the end of the last send whose result a future
    /// took, or since the last report.
    unreported: usize,
    read: Direction,
    write: Direction,
    kept: Option<Kept>,
    /// The close that waits for the operations of the resource.
    closing: Option<Closing>,
}

/// Who waits for the close of a resource.
#[derive(Debug, Clone, Copy)]
enum Closing {
    /// The future of the close operation, which gets the report.
    Future(OpId),
    /// No one: a drop of a local type recorded the close.
    #[cfg(all(feature = "local", target_os = "linux"))]
    Dropped,
}

impl Directions {
    /// The count that the write direction owes.
    const fn count(&self) -> SendCount {
        match self.sent {
            Sent::Ok | Sent::Reported => SendCount::Settled,
            Sent::Owe => SendCount::Sent(self.unreported),
            #[cfg(target_os = "windows")]
            Sent::Lost => SendCount::Unknown,
        }
    }

    /// The direction of an operation of `kind`.
    fn direction(&mut self, kind: Kind) -> Option<&mut Direction> {
        match kind {
            Kind::Recv | Kind::Selected | Kind::Accept | Kind::RecvFrom | Kind::RecvSelectedFrom | Kind::RecvBatch => {
                Some(&mut self.read)
            }
            Kind::Send | Kind::SendAll | Kind::Vectored | Kind::Shutdown | Kind::SendTo | Kind::SendBatch => {
                Some(&mut self.write)
            }
            Kind::Other | Kind::Close | Kind::Connect | Kind::Bind => None,
            Kind::File(_) => None,
            #[cfg(unix)]
            Kind::Spawn(_) | Kind::Exit(_) => None,
        }
    }

    /// Takes a completion of the multishot receive of the read direction, with `buffer` when it
    /// filled one, which goes after the shelved buffers. A completion with no `more` ends the
    /// receive with `result`. Gives whether the resource now needs the home, because a receive
    /// waits, the receive ended, or the direction keeps its most buffers, and it is not in the
    /// pending list yet. A `true` puts it there: the caller pushes it.
    pub(super) fn received(&mut self, buffer: Option<Backing>, result: i32, more: bool, shelf: &mut Shelf) -> bool {
        let filled = buffer.is_some();
        if let Some(buffer) = buffer {
            shelf.put(&mut self.shelved, buffer, false);
        }
        // The end of the stream or the error follows the buffers that the direction keeps. An
        // empty ring ends no stream: a receive that waits gets it only when the receive filled no
        // buffer, and else arms again, so a buffer that came back after the end is used. A
        // cancel, at the bound or at a close, gives nothing.
        if !more {
            self.armed = Armed::No;
            if !filled && result <= 0 && result != -ECANCELED && (result != -ENOBUFS || self.waits() && self.fresh) {
                self.ended = Terminal::of(result);
            }
        }
        self.fresh &= !filled;
        let needs = (self.awaiting || !more || self.shelved.len >= super::KEPT_BUFFERS) && !self.pending;
        self.pending |= needs;
        needs
    }

    /// Whether a receive waits for the multishot receive: an operation, or a local receive.
    const fn waits(&self) -> bool {
        #[cfg(all(feature = "local", target_os = "linux"))]
        if self.reading {
            return true;
        }
        self.awaiting
    }

    /// Takes the waiter of the local receive that waits, for a wake. The receive waits until its
    /// next poll.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(super) fn take_reader(&mut self) -> super::home::Waiter {
        core::mem::take(&mut self.reader)
    }

    /// The send of each byte of the write direction goes again with its rest. Gives whether the
    /// resource now needs the home, and it is not in the pending list yet. A `true` puts it there:
    /// the caller pushes it.
    pub(super) fn send_again(&mut self) -> bool {
        self.again = true;
        let needs = !self.pending;
        self.pending = true;
        needs
    }

    /// Whether the record of the stream holds a send whose terminal event did not come
    /// ([`Driver::send_record`]).
    const fn record_sends(&self) -> bool {
        cfg_select! {
            all(feature = "local", target_os = "linux") => self.sending.runs(),
            _ => false,
        }
    }

    /// Whether the record of the stream holds a send: it runs, or no future took its result.
    const fn record_holds(&self) -> bool {
        cfg_select! {
            all(feature = "local", target_os = "linux") => self.sending.lease.is_some(),
            _ => false,
        }
    }

    /// The completion of the send of the record, with `result`. At its terminal event, a future
    /// with interest takes the result later, and the caller wakes the waiter that this gives. Else
    /// the abandoned rule applies now, and the lease drops. Gives also whether the resource needs
    /// the home now, as [`Directions::needs_home`] does: the caller then pushes it.
    ///
    /// A send of each byte that sent a byte and has bytes left, with no cancel, goes again: the
    /// waiter is then `None`, the send runs on, and the resource needs the home.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(super) fn record_sent(&mut self, mut result: i32) -> (bool, Option<super::home::Waiter>) {
        if self.sending.all
            && let Ok(went) = usize::try_from(result)
        {
            let sending = &mut self.sending;
            sending.went = sending.went.saturating_add(went);
            result = 0;
            let len = sending.lease.as_ref().map_or(0, |lease| lease.initialized());
            if went > 0 && sending.went < len && !sending.cancelled {
                return (self.send_again(), None);
            }
        }
        let waiter = if self.sending.interest {
            self.sending.result = Terminal::of(result);
            self.take_sender()
        } else {
            self.abandon_record_send(result);
            super::home::Waiter::None
        };
        // With no operation and no close, nothing waits for the send.
        (self.operated && self.needs_home(), Some(waiter))
    }

    /// The abandoned rule for the send of the record, which ended with `result`: its lease drops,
    /// and a send that failed, sent only part of its bytes, or was cancelled makes the write
    /// direction fail ([`Directions::abandon`]).
    #[cfg(all(feature = "local", target_os = "linux"))]
    fn abandon_record_send(&mut self, result: i32) {
        let Sending {
            lease,
            cancelled,
            went,
            all,
            ..
        } = core::mem::take(&mut self.sending);
        if let Some(lease) = lease {
            let held = Held {
                cancelled,
                copied: went,
                ..Held::with_payload(Payload::Lease(lease))
            };
            self.abandon(if all { Kind::SendAll } else { Kind::Send }, result, held);
        }
    }

    /// The future of the send of the record ended its interest: it dropped, a later send of the
    /// direction started after a forget, or the stream closes. A send that ended takes the
    /// abandoned rule now, and one that runs takes it at its terminal event.
    #[cfg(all(feature = "local", target_os = "linux"))]
    fn forgo_send(&mut self) {
        match self.sending.result.get() {
            Some(result) => self.abandon_record_send(result),
            None => {
                self.sending.interest = false;
                drop(self.take_sender());
            }
        }
    }

    /// Takes the waiter of the send of the record, if a poll set it. Else it reads no line of the
    /// waiter.
    #[cfg(all(feature = "local", target_os = "linux"))]
    fn take_sender(&mut self) -> super::home::Waiter {
        if core::mem::take(&mut self.sending.waits) {
            core::mem::take(&mut self.sender)
        } else {
            super::home::Waiter::None
        }
    }

    /// The take of the result of the send of the record, with its lease, after its terminal event.
    /// Before it, the record keeps the waiter, `task` or `waker`. Gives `None` when the record
    /// holds no send.
    #[cfg(all(feature = "local", target_os = "linux"))]
    fn take_send(
        &mut self,
        task: Option<super::home::LocalTask>,
        waker: &core::task::Waker,
    ) -> Option<core::task::Poll<(io::Result<usize>, Lease)>> {
        use core::task::Poll;
        self.sending.lease.as_ref()?;
        let Some(result) = self.sending.result.get() else {
            self.sending.waits = true;
            self.sender.set(task, waker);
            return Some(Poll::Pending);
        };
        let Sending { lease, went, all, .. } = core::mem::take(&mut self.sending);
        // A send result that a future took reports each byte up to its end, as at
        // [`Directions::taken`].
        if self.sent == Sent::Ok {
            self.clear_unreported();
        }
        let sent = if all { all_count(result, went) } else { count(result) };
        Some(Poll::Ready((sent, lease?)))
    }

    /// Ends the records of the stream at the drop of the driver, and gives their waiters, which the
    /// caller wakes. The lease of a send that runs leaks, because the kernel can still read it.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(super) fn end_records(&mut self) -> [super::home::Waiter; 2] {
        if self.sending.runs() {
            let _leaked = core::mem::ManuallyDrop::new(self.sending.lease.take());
        }
        [self.take_reader(), self.take_sender()]
    }

    /// Gives each buffer that the direction keeps on `shelf` back to its pool.
    pub(super) fn unshelve(&mut self, shelf: &mut Shelf) {
        shelf.clear(&mut self.shelved);
    }

    /// Whether a receive of the read direction has a result here, or waits for the multishot
    /// receive: so it needs no submission, and the usual start does not apply.
    fn receives_here(&self) -> bool {
        self.armed != Armed::No || !self.shelved.is_empty() || self.ended.is_some()
    }

    /// The listener keeps a connection that no accept took.
    pub(super) fn keep_connection(&mut self, fd: Handle, peer: SockAddrStorage, peer_len: u32) {
        self.kept = Some(Kept::Connection { fd, peer, peer_len });
    }

    /// Takes the connection that the listener keeps, with the address of its peer.
    #[cfg(all(feature = "movable", target_os = "linux"))]
    pub(super) fn take_connection(&mut self) -> Option<(Handle, SockAddrStorage, u32)> {
        match self.kept.take() {
            Some(Kept::Connection { fd, peer, peer_len }) => Some((fd, peer, peer_len)),
            kept => {
                self.kept = kept;
                None
            }
        }
    }

    /// Whether the directions hold nothing for a later operation: no kept result, and no failure
    /// and no count of the write direction.
    #[cfg(all(feature = "movable", target_os = "linux"))]
    pub(super) fn is_clear(&self) -> bool {
        self.kept.is_none() && self.sent == Sent::Ok && self.unreported == 0
    }

    /// Takes from `earlier` the state of the direction of an operation of `kind`: what its receives
    /// or accepts keep, or the progress of its sends.
    #[cfg(all(feature = "movable", target_os = "linux"))]
    pub(super) fn inherit(&mut self, earlier: &mut Self, kind: Kind) {
        if kind.writes() {
            (self.sent, self.unreported, self.counted) = (earlier.sent, earlier.unreported, earlier.counted);
        } else {
            self.kept = earlier.kept.take();
        }
    }

    /// The report of a close of the resource.
    pub(super) const fn closed(&self) -> Closed {
        Closed { sent: self.count() }
    }

    /// Whether the resource now needs the home, because an operation or a close waits, and it is not
    /// in the pending list of the driver yet. A `true` puts it there: the caller pushes it, and the
    /// list holds each resource at most one time, so its capacity holds it.
    fn needs_home(&mut self) -> bool {
        // A receive that waited for a multishot receive that ended takes its result, or arms again.
        let waits = self.read.first.is_some()
            || self.write.first.is_some()
            || self.closing.is_some()
            || (self.awaiting && self.armed == Armed::No);
        let needs = waits && !self.pending;
        self.pending |= needs;
        needs
    }

    /// The directions have no operation that runs or waits, and no state that a later operation or
    /// the close takes. `runs` tells whether an operation still runs.
    #[cfg(unix)]
    fn idle(&self, runs: impl Fn(OpId) -> bool) -> bool {
        let free = |direction: &Direction| direction.current.is_none_or(|id| !runs(id)) && direction.first.is_none();
        free(&self.read)
            && free(&self.write)
            && matches!(self.sent, Sent::Ok | Sent::Reported)
            && self.closing.is_none()
            && !self.pending
            && self.armed == Armed::No
            && !self.record_holds()
            && self.shelved.is_empty()
            && !self.ended.is_some()
            && self.kept.is_none()
    }

    /// The consumer of the operation `id` of `kind` took its result: the direction forgets it, so
    /// the next operation does not look at it. A send result that a future took reports each byte
    /// up to its end.
    pub(super) fn taken(&mut self, kind: Kind, id: OpId) {
        if let Some(direction) = self.direction(kind)
            && direction.current == Some(id)
        {
            direction.current = None;
        }
        if matches!(kind, Kind::Send | Kind::SendAll | Kind::Vectored) && self.sent == Sent::Ok {
            self.clear_unreported();
        }
    }

    /// Adds `sent` to the bytes that dropped sends sent.
    fn count_unreported(&mut self, sent: usize) {
        self.unreported = self.unreported.saturating_add(sent);
        self.counted |= sent > 0;
    }

    /// Forgets the bytes that dropped sends sent. With no such byte, it reads no line of
    /// `unreported`.
    fn clear_unreported(&mut self) {
        if core::mem::take(&mut self.counted) {
            self.unreported = 0;
        }
    }

    /// The abandoned rule for the result of an operation of `kind` whose consumer ended its
    /// interest, with its resources `held`. Received bytes, the error of a receive, and an accepted
    /// connection stay for the next operation of the direction. A send that failed, sent only part
    /// of its bytes, or was cancelled makes the write direction fail.
    pub(super) fn abandon(&mut self, kind: Kind, result: i32, held: Held) {
        match kind {
            Kind::Recv | Kind::Selected => {
                let buffer = held.loan.map(WriteLoan::into_inner);
                if held.copied > 0
                    && let Some(Kept::Bytes { start, .. }) = &mut self.kept
                {
                    // The receive copied its bytes from the kept buffer, which still holds them.
                    *start -= held.copied;
                } else if let Ok(copied) = u32::try_from(held.copied)
                    && copied > 0
                    && kind == Kind::Recv
                    && self.shelved.start >= copied
                {
                    // The receive copied its bytes from the first shelved buffer, which still
                    // holds them: a later buffer starts with no copied byte.
                    self.shelved.start -= copied;
                } else if let (Ok(received), Some(buffer)) = (usize::try_from(result), buffer)
                    && received > 0
                {
                    let start = buffer.initialized() - received;
                    self.kept = Some(Kept::Bytes { buffer, start });
                } else if result < 0 && result != -ECANCELED && result != -ENOBUFS {
                    self.kept = Some(Kept::Error(result));
                }
            }
            Kind::Send | Kind::SendAll | Kind::Vectored | Kind::Shutdown => {
                if let Some(failed) = held.failed {
                    // The future did not take the failure: the next send gives it.
                    match failed.sent {
                        SendCount::Sent(sent) => {
                            self.count_unreported(sent);
                            self.sent = Sent::Owe;
                        }
                        #[cfg(target_os = "windows")]
                        SendCount::Unknown => self.sent = Sent::Lost,
                        _ => {}
                    }
                    return;
                }
                if kind == Kind::Shutdown {
                    return;
                }
                // A send of each byte that a cancel or a failure ended counts the bytes that went.
                let sent = if kind == Kind::SendAll {
                    held.copied
                } else {
                    usize::try_from(result).unwrap_or(0)
                };
                let len = match &held.payload {
                    Payload::Lease(lease) => lease.initialized(),
                    Payload::Regions(regions) => regions.total(),
                    _ => 0,
                };
                self.count_unreported(sent);
                #[cfg(target_os = "windows")]
                if result == -sys::WSAECONNABORTED {
                    self.sent = Sent::Lost;
                    return;
                }
                let short = sent < len || kind != Kind::SendAll && (result < 0 || held.cancelled);
                if short && self.sent == Sent::Ok {
                    self.sent = Sent::Owe;
                }
            }
            Kind::RecvFrom | Kind::RecvSelectedFrom => match (held.loan, held.payload) {
                (
                    Some(loan),
                    Payload::Receive {
                        received: Some(received),
                        ..
                    },
                ) if result >= 0 => {
                    self.kept = Some(Kept::Datagram {
                        buffer: loan.into_inner(),
                        received,
                    });
                }
                _ if result < 0 && result != -ECANCELED && result != -ENOBUFS => {
                    self.kept = Some(Kept::Error(result));
                }
                _ => {}
            },
            Kind::RecvBatch => match held.payload {
                Payload::Batch {
                    batch: Batch::Receive(arrivals),
                    ..
                } if !arrivals.is_empty() => {
                    self.kept = Some(Kept::Arrivals(arrivals));
                }
                _ if result < 0 && result != -ECANCELED && result != -ENOBUFS && result != -EAGAIN => {
                    self.kept = Some(Kept::Error(result));
                }
                _ => {}
            },
            // The connection that the accept took stays for the next accept.
            Kind::Accept => {
                if let Payload::Socket {
                    peer,
                    peer_len,
                    connection: Some(fd),
                } = held.payload
                {
                    self.keep_connection(fd, peer, peer_len);
                }
            }
            // A dropped datagram send fails no direction: its result is lost, as a datagram can be.
            Kind::Other | Kind::Close | Kind::Connect | Kind::Bind | Kind::SendTo | Kind::SendBatch => {}
            Kind::File(_) => {}
            #[cfg(unix)]
            Kind::Spawn(_) | Kind::Exit(_) => {}
        }
    }

    /// Gives an operation of `kind`, with its resources `held`, the result that the direction holds
    /// for it, with no submission: the bytes, the datagrams, or the error that an abandoned receive
    /// left, or the failure of the write direction. `interest` gives whether the consumer of the
    /// operation has interest. Adds the bytes that it copies to `copied`. Gives `None` when the
    /// operation needs a submission. An accept takes its kept connection at its caller.
    pub(super) fn resume(
        &mut self,
        held: &mut Held,
        kind: Kind,
        interest: impl Fn() -> bool,
        copied: &mut u64,
        shelf: &mut Shelf,
    ) -> Option<i32> {
        match (kind, self.kept.take()) {
            // The kept result of an abandoned receive comes first, then the shelved buffers of the
            // multishot receive, in order, then its end.
            (Kind::Selected, None) if !self.shelved.is_empty() => {
                let (buffer, start) = shelf.take(&mut self.shelved)?;
                Some(select(held, buffer, start, copied))
            }
            (Kind::Recv, None) if !self.shelved.is_empty() => {
                let (buffer, start) = shelf.first(&self.shelved)?;
                let bytes = buffer.bytes().get(start..).unwrap_or_default();
                let count = held.loan.as_mut().map_or(0, |loan| loan.fill_from(bytes));
                let rest = bytes.len() - count;
                held.copied = count;
                self.shelved.start += u32::try_from(count).expect("a shelved buffer holds one result of the kernel");
                if rest == 0 {
                    drop(shelf.take(&mut self.shelved));
                }
                *copied = copied.saturating_add(count as u64);
                // The bytes came from one kernel result, so their count fits an `i32`.
                Some(i32::try_from(count).unwrap_or(i32::MAX))
            }
            (Kind::Recv | Kind::Selected, None) if self.ended.is_some() => self.ended.take(),
            (Kind::Recv, Some(Kept::Bytes { buffer, mut start })) => {
                let count = held.loan.as_mut().map_or(0, |loan| {
                    loan.fill_from(buffer.bytes().get(start..).unwrap_or_default())
                });
                held.copied = count;
                start += count;
                if start < buffer.initialized() {
                    self.kept = Some(Kept::Bytes { buffer, start });
                }
                *copied = copied.saturating_add(count as u64);
                // The kept bytes came from one kernel result, so their count fits an `i32`.
                Some(i32::try_from(count).unwrap_or(i32::MAX))
            }
            (Kind::Selected, Some(Kept::Bytes { buffer, start })) => Some(select(held, buffer, start, copied)),
            (Kind::Recv | Kind::Selected, Some(Kept::Error(error))) => Some(error),
            (Kind::RecvFrom, Some(Kept::Datagram { buffer, received })) => {
                let (result, count) = deliver_copy(held, &buffer, received);
                *copied = copied.saturating_add(count as u64);
                Some(result)
            }
            (Kind::RecvFrom, Some(Kept::Arrivals(mut arrivals))) => {
                let first = arrivals.take_first();
                self.kept = udp::keep_arrivals(arrivals).map(Kept::Arrivals);
                first.map(|(buffer, received)| {
                    let (result, count) = deliver_copy(held, &buffer, received);
                    *copied = copied.saturating_add(count as u64);
                    result
                })
            }
            (Kind::RecvSelectedFrom, Some(Kept::Datagram { buffer, received })) => {
                Some(deliver(held, buffer, received))
            }
            (Kind::RecvSelectedFrom, Some(Kept::Arrivals(mut arrivals))) => {
                let first = arrivals.take_first();
                self.kept = udp::keep_arrivals(arrivals).map(Kept::Arrivals);
                first.map(|(buffer, received)| deliver(held, buffer, received))
            }
            (Kind::RecvBatch, Some(Kept::Datagram { buffer, received })) => {
                let mut taken = Some((buffer, received));
                if let Some(Batch::Receive(arrivals)) = held.payload.batch()
                    && let Some((buffer, received)) = taken.take()
                {
                    taken = arrivals.add(buffer, received);
                }
                self.kept = taken.map(|(buffer, received)| Kept::Datagram { buffer, received });
                Some(1)
            }
            (Kind::RecvBatch, Some(Kept::Arrivals(mut kept))) => {
                let mut count = 0;
                if let Some(Batch::Receive(arrivals)) = held.payload.batch() {
                    while arrivals.len() < arrivals.capacity() {
                        let Some((buffer, received)) = kept.take_first() else {
                            break;
                        };
                        if arrivals.add(buffer, received).is_none() {
                            count += 1;
                        }
                    }
                }
                self.kept = udp::keep_arrivals(kept).map(Kept::Arrivals);
                Some(count)
            }
            (Kind::RecvFrom | Kind::RecvSelectedFrom | Kind::RecvBatch, Some(Kept::Error(error))) => Some(error),
            (Kind::Send | Kind::SendAll | Kind::Vectored | Kind::Shutdown, kept) => {
                self.kept = kept;
                match self.sent {
                    Sent::Ok => None,
                    // A dropped send that waited never ran, so it gives no report.
                    Sent::Owe if !interest() => Some(-ECANCELED),
                    #[cfg(target_os = "windows")]
                    Sent::Lost if !interest() => Some(-ECANCELED),
                    _ => {
                        held.failed = Some(WriteFailed { sent: self.count() });
                        self.sent = Sent::Reported;
                        self.clear_unreported();
                        Some(0)
                    }
                }
            }
            (_, kept) => {
                self.kept = kept;
                None
            }
        }
    }

    /// Whether an operation of `kind` has its result here, with no submission.
    fn has_result(&self, kind: Kind) -> bool {
        match kind {
            Kind::Recv | Kind::Selected => {
                matches!(self.kept, Some(Kept::Bytes { .. } | Kept::Error(_)))
                    || !self.shelved.is_empty()
                    || self.ended.is_some()
                    || self.armed != Armed::No
            }
            Kind::Accept => matches!(self.kept, Some(Kept::Connection { .. })),
            Kind::Send | Kind::SendAll | Kind::Vectored | Kind::Shutdown => self.sent != Sent::Ok,
            Kind::RecvFrom | Kind::RecvSelectedFrom | Kind::RecvBatch => matches!(
                self.kept,
                Some(Kept::Datagram { .. } | Kept::Arrivals(_) | Kept::Error(_))
            ),
            Kind::Other | Kind::Close | Kind::Connect | Kind::Bind | Kind::SendTo | Kind::SendBatch => false,
            Kind::File(_) => false,
            #[cfg(unix)]
            Kind::Spawn(_) | Kind::Exit(_) => false,
        }
    }
}

/// The result of an operation as a count of bytes, or the error of the kernel.
pub(super) fn count(result: i32) -> io::Result<usize> {
    usize::try_from(result).map_err(|_| io::Error::from_raw_os_error(result.saturating_neg()))
}

/// The abandoned rule: the home applies it to the result of an operation of `tag` whose consumer
/// ended its interest. It closes the socket or the file that the operation made, and the
/// directions of the resource of `tag` take the rest ([`Directions::abandon`]).
pub(super) fn abandon(resources: &mut Resources, tag: Tag, result: i32, mut held: Held) {
    #[cfg(unix)]
    let closes = matches!(tag.kind, Kind::Connect | Kind::Bind | Kind::File(_));
    #[cfg(target_os = "windows")]
    let closes = matches!(tag.kind, Kind::Connect | Kind::Bind);
    if closes {
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
    // A stream that the home gave to a future that dropped before it took it goes back to a
    // descriptor.
    if tag.kind == Kind::Accept
        && let Payload::Socket { connection, .. } = &mut held.payload
        && connection.is_none()
    {
        *connection = held.accepted.take().and_then(|stream| resources.release(stream));
    }
    if let Some(owned) = resources.get(resource) {
        owned.directions.abandon(tag.kind, result, held);
    }
}

/// The consumer of the operation `id` of `tag` took its result ([`Directions::taken`]).
pub(super) fn taken(resources: &mut Resources, tag: Tag, id: OpId) {
    if let Some(owned) = tag.resource.and_then(|resource| resources.get(resource)) {
        owned.directions.taken(tag.kind, id);
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
    if owned.directions.needs_home() {
        pending.push(resource);
    }
}

/// The output of a receive: the count of received bytes, and the buffer.
pub(crate) fn received(result: i32, held: &mut Held) -> Option<(io::Result<usize>, Backing)> {
    Some((count(result), held.loan.take()?.into_inner()))
}

/// The output of a vectored send: the count of sent bytes, or the failure of the write direction,
/// and the regions.
pub(crate) fn sent_regions(result: i32, held: &mut Held) -> Option<(io::Result<usize>, Regions)> {
    let sent = held
        .failed
        .map_or_else(|| count(result), |failed| Err(io::Error::other(failed)));
    let Payload::Regions(regions) = core::mem::take(&mut held.payload) else {
        return None;
    };
    Some((sent, regions))
}

/// The output of a half-close: its result, or the failure of the write direction.
pub(crate) fn shut(result: i32, held: &mut Held) -> Option<io::Result<()>> {
    Some(match held.failed {
        Some(failed) => Err(io::Error::other(failed)),
        None => count(result).map(|_| ()),
    })
}

/// The output of a receive into a buffer that the kernel selected: the buffer with the received
/// bytes, or an empty buffer at the end of the stream, where no buffer was selected.
pub(crate) fn received_selected(result: i32, held: &mut Held) -> Option<io::Result<Backing>> {
    // An empty `Vec` allocates nothing.
    Some(count(result).map(|_| {
        held.loan
            .take()
            .map_or_else(|| Backing::from(Vec::new()), WriteLoan::into_inner)
    }))
}

/// The output of a send: the count of sent bytes, or the failure of the write direction, and the
/// lease.
pub(crate) fn sent(result: i32, held: &mut Held) -> Option<(io::Result<usize>, Lease)> {
    let sent = held
        .failed
        .map_or_else(|| count(result), |failed| Err(io::Error::other(failed)));
    Some((sent, held.payload.take_lease()?))
}

/// The output of a send of each byte ([`Kind::SendAll`]): the count of the bytes that went, or the
/// failure of the write direction, and the lease.
pub(crate) fn sent_all(result: i32, held: &mut Held) -> Option<(io::Result<usize>, Lease)> {
    let sent = held.failed.map_or_else(
        || all_count(result, held.copied),
        |failed| Err(io::Error::other(failed)),
    );
    Some((sent, held.payload.take_lease()?))
}

/// The output of a send of each byte that ended with `result` after `went` bytes: the count of the
/// bytes, which is each byte when `result` is 0, or the count that a cancel left. A failure after
/// some bytes gives [`WriteFailed`] with their count, and else the error of the kernel.
fn all_count(result: i32, went: usize) -> io::Result<usize> {
    if result >= 0 || went > 0 && result == -ECANCELED {
        Ok(went)
    } else if went > 0 {
        Err(io::Error::other(WriteFailed {
            sent: SendCount::Sent(went),
        }))
    } else {
        count(result)
    }
}

/// The completion of one send of the kernel of a send of each byte, with `event`: the bytes that
/// went join `held.copied`. Gives `true` when bytes remain, the send sent a byte, and no cancel
/// came: the send then goes again with the rest. Else `event` gets the result of the end: 0 when
/// the kernel sent, or its error.
pub(super) fn sent_part(held: &mut Held, event: &mut super::Event) -> bool {
    let Ok(went) = usize::try_from(event.result) else {
        return false;
    };
    held.copied = held.copied.saturating_add(went);
    event.result = 0;
    let len = match &held.payload {
        Payload::Lease(lease) => lease.initialized(),
        _ => 0,
    };
    went > 0 && held.copied < len && !held.cancelled
}

/// The output of a bind: the listener and its local address.
pub(crate) fn bound(result: i32, held: &mut Held) -> Option<io::Result<TcpListener>> {
    Some(count(result).and_then(|_| {
        let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
        let local = held
            .payload
            .peer()
            .and_then(|(peer, len)| peer.to_socket_addr(*len))
            .ok_or(io::ErrorKind::InvalidData)?;
        Ok(TcpListener { resource, local })
    }))
}

/// The output of a connect: the stream.
pub(crate) fn connected(result: i32, held: &mut Held) -> Option<io::Result<TcpStream>> {
    Some(count(result).and_then(|_| Ok(TcpStream::new(held.accepted.ok_or(io::ErrorKind::OutOfMemory)?))))
}

/// The output of an accept: the stream and the address of its peer.
pub(crate) fn accepted(result: i32, held: &mut Held) -> Option<io::Result<(TcpStream, SocketAddr)>> {
    Some(count(result).and_then(|_| {
        let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
        let peer = held
            .payload
            .peer()
            .and_then(|(peer, len)| peer.to_socket_addr(*len))
            .ok_or(io::ErrorKind::InvalidData)?;
        Ok((TcpStream::new(resource), peer))
    }))
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
        // A listener on Linux binds again while connections of an earlier one wait in
        // `TIME_WAIT`. On Windows, `SO_REUSEADDR` lets another socket take a bound port, so the
        // listener does not set it, as the standard library does.
        let setup = |socket: Borrowed<'_>| {
            #[cfg(target_os = "linux")]
            set_int(socket, sys::SOL_SOCKET, sys::SO_REUSEADDR, 1)?;
            options.apply(socket)
        };
        let completion = self.bind_socket(SockAddrStorage::new(address), true, setup)?;
        Ok(Operation::new(completion, bound))
    }

    /// Makes a socket of the family of the address `storage` of `len` bytes, sets it up with
    /// `setup`, binds it, and with `listen` listens on it: a TCP listener, or a UDP socket with no
    /// `listen`. The ring runs the bind, and the listen linked to it, where the kernel has
    /// those operations; elsewhere the driver makes the system calls, and the operation ends at
    /// once. Its resources then hold the socket and its local address.
    pub(super) fn bind_socket(
        &mut self,
        (storage, len): (SockAddrStorage, u32),
        listen: bool,
        setup: impl FnOnce(Borrowed<'_>) -> io::Result<()>,
    ) -> Result<Completion, Refused<()>> {
        let call = if listen { Call::Bind } else { Call::BindUdp };
        let ring = self.report.route(call) == Route::Ring;
        // On the ring, the bind and the listen are two linked entries, so the queue needs room for
        // both. A bind with system calls pushes no entry.
        let entries = match (ring, listen) {
            (false, _) => 0,
            (true, false) => 1,
            (true, true) => 2,
        };
        let tag = Tag::new(None, Kind::Bind);
        let held = |()| Held {
            payload: Payload::Socket {
                peer: storage,
                peer_len: len,
                connection: None,
            },
            ..Held::default()
        };
        let id = self
            .open(tag, entries, (), held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        let completion = self.completion(id, true);
        let kind = if listen { sys::SOCK_STREAM } else { sys::SOCK_DGRAM };
        match self.socket(id, storage.ss_family, kind, setup) {
            Err(error) => self.complete_now(id, errno(&error)),
            Ok((_, fd)) if ring => {
                // SAFETY: the bind names the address in the resources of `id`, which the home keeps
                // until its terminal event, and the socket, which `id` holds. The queue has room for
                // each entry, so the bind links only to the listen.
                #[expect(unsafe_code, reason = "the kernel reads the address in the resources of the bind")]
                unsafe {
                    // A bind always has its address.
                    let bind = |held: &mut Held| {
                        held.payload
                            .peer()
                            .map_or_else(Sqe::nop, |(peer, len)| Sqe::bind(fd, (&raw const *peer).cast(), *len))
                    };
                    if listen {
                        self.push(id, |held| bind(held).linked_quiet());
                        self.push(id, |_| Sqe::listen(fd, BACKLOG.cast_unsigned()));
                    } else {
                        self.push(id, bind);
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
                        self.hold(id, |held| {
                            if let Some((peer, len)) = held.payload.peer() {
                                (*peer, *len) = local;
                            }
                        });
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
        let completion = self.connect_socket(SockAddrStorage::new(address), |socket| options.apply(socket))?;
        Ok(Operation::new(completion, connected))
    }

    /// Makes a stream socket of the family of the address `storage` of `len` bytes, sets it up with
    /// `setup`, and connects it to that address on the ring. Its resources then hold the socket.
    fn connect_socket(
        &mut self,
        (storage, len): (SockAddrStorage, u32),
        setup: impl FnOnce(Borrowed<'_>) -> io::Result<()>,
    ) -> Result<Completion, Refused<()>> {
        let tag = Tag::new(None, Kind::Connect);
        let held = |()| Held {
            payload: Payload::Socket {
                peer: storage,
                peer_len: len,
                connection: None,
            },
            ..Held::default()
        };
        let id = self
            .open(tag, 1, (), held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        let completion = self.completion(id, true);
        match self.socket(id, storage.ss_family, sys::SOCK_STREAM, setup) {
            Err(error) => self.complete_now(id, errno(&error)),
            Ok((_, fd)) => {
                // SAFETY: the connect names the address in the resources of `id`, which the home
                // keeps until its terminal event, and the socket, which `id` holds. The queue has
                // room.
                #[expect(unsafe_code, reason = "the kernel reads the address in the resources of the connect")]
                unsafe {
                    self.push(id, |held| {
                        // A connect always has its address.
                        held.payload.peer().map_or_else(Sqe::nop, |(peer, len)| {
                            Sqe::connect(fd, (&raw const *peer).cast(), *len)
                        })
                    });
                }
            }
        }
        Ok(completion)
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
            payload: Payload::Regions(regions),
            ..Held::default()
        };
        let completion = self.begin(writer.as_mut().resource, Kind::Vectored, regions, held)?;
        Ok(Operation::new(completion, sent_regions))
    }

    /// Ends the write side of the stream after the sends before it (`SHUT_WR`): the peer reads
    /// the end of the stream after their bytes, and the receives of this side go on. A drop of the
    /// future does not cancel it. On a failed write direction it gives [`WriteFailed`] and sends
    /// nothing, so the peer never reads a clean end after a cut message.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, and [`Reason::Unsupported`] for a
    /// resource that is not a TCP or IPC stream: a `PipeWriter`, which you close instead, or a
    /// `UdpSocket`. A refusal changes nothing.
    pub fn shutdown_write<'a, W: AsMut<WriteHalf>>(
        &mut self,
        writer: &'a mut W,
    ) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        let resource = writer.as_mut().resource;
        // Only a stream socket has a half-close. The close of a pipe is the end of its stream, and
        // a datagram socket has no stream.
        if self
            .resources
            .get(resource)
            .is_some_and(|owned| owned.medium != Medium::Stream)
        {
            return Err(Refused {
                reason: Reason::Unsupported,
                value: (),
            });
        }
        let completion = self.begin(resource, Kind::Shutdown, (), |()| Held::default())?;
        Ok(Operation::new(completion, shut))
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

    /// Makes the socket of the connect or the bind `id`: a new nonblocking socket of `family` and
    /// `kind`, which `setup` sets up before the bind or the connect, as a resource that `id`
    /// holds. Gives the resource and its descriptor.
    fn socket(
        &mut self,
        id: OpId,
        family: SaFamily,
        kind: c_int,
        setup: impl FnOnce(Borrowed<'_>) -> io::Result<()>,
    ) -> io::Result<(Resource, i32)> {
        let socket = new_socket(family, kind)?;
        setup(borrow(&socket))?;
        let fd = number(borrow(&socket));
        let medium = if kind == sys::SOCK_DGRAM {
            Medium::Udp
        } else {
            Medium::Stream
        };
        let resource = self
            .adopt(socket, medium)
            .map_err(|_| io::Error::from_raw_os_error(ENOMEM))?;
        self.hold(id, |held| held.accepted = Some(resource));
        Ok((resource, fd))
    }

    /// Changes the resources of the running operation `id`, at the home.
    pub(super) fn hold(&self, id: OpId, change: impl FnOnce(&mut Held)) {
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
        let completion = self.begin_accept(listener.resource)?;
        Ok(Operation::new(completion, accepted))
    }

    /// Starts an accept on the listener `resource`. Its resources hold the peer address that the
    /// kernel writes, and then the stream.
    pub(super) fn begin_accept(&mut self, resource: Resource) -> Result<Completion, Refused<()>> {
        let held = |()| Held {
            payload: Payload::Socket {
                peer: SockAddrStorage::default(),
                peer_len: SockAddrStorage::LEN,
                connection: None,
            },
            ..Held::default()
        };
        self.begin(resource, Kind::Accept, (), held)
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
        Ok(Operation::new(completion, received))
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
        let resource = reader.as_mut().resource;
        self.own(resource, ())?;
        if self.selected.is_none() {
            return Err(Refused {
                reason: Reason::Buffers,
                value: (),
            });
        }
        let completion = self.begin(resource, Kind::Selected, (), |()| Held::default())?;
        Ok(Operation::new(completion, received_selected))
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
        let held = |lease| Held::with_payload(Payload::Lease(lease));
        let completion = self.begin(writer.as_mut().resource, Kind::Send, lease, held)?;
        Ok(Operation::new(completion, sent))
    }

    /// Sends each byte of `lease`, in as many sends of the kernel as it takes: a send that sent only
    /// part of its bytes goes again with the rest. The future gives the count of the bytes, which
    /// is the length of the lease, and the lease. After a cancel it gives the count of the bytes
    /// that went, or `ECANCELED`. A failure after some bytes gives a [`WriteFailed`] with their
    /// count, and a failure before them the error of the kernel. A drop of the future does not
    /// cancel the send: it runs on until each byte went or the send failed, and a failure then
    /// fails the write direction, so the next send gives the count (see [`TcpStream`]).
    ///
    /// # Errors
    ///
    /// A refusal, with the lease, when the driver has no room for the operation.
    pub fn send_all<'a, W: AsMut<WriteHalf>>(
        &mut self,
        writer: &'a mut W,
        lease: Lease,
    ) -> Result<Operation<'a, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let held = |lease| Held::with_payload(Payload::Lease(lease));
        let completion = self.begin(writer.as_mut().resource, Kind::SendAll, lease, held)?;
        Ok(Operation::new(completion, sent_all))
    }

    /// Takes `fd`, a connected TCP socket, as a stream of this driver: for example a connection
    /// that another driver accepted and gave with [`Driver::release_stream`]. The driver sets
    /// `O_NONBLOCK` of its open file description, as it does for each socket that it makes.
    ///
    /// # Errors
    ///
    /// A refusal, with `fd`, that gives [`Reason::NotTcp`] for a descriptor that is not a
    /// connected TCP socket, [`Reason::Resources`], or [`Reason::Register`] on epoll and kqueue.
    #[cfg(unix)]
    pub fn adopt_stream(&mut self, fd: std::os::fd::OwnedFd) -> Result<TcpStream, Refused<std::os::fd::OwnedFd>> {
        let stream = int(borrow(&fd), sys::SOL_SOCKET, sys::SO_TYPE).is_ok_and(|kind| kind == sys::SOCK_STREAM);
        let tcp = local_address(&fd).is_some_and(|(address, len)| address.to_socket_addr(len).is_some());
        if !(stream && tcp && peer_address(&fd).is_some() && super::pipe::set_nonblocking(borrow(&fd), true)) {
            return Err(Refused {
                reason: Reason::NotTcp,
                value: fd,
            });
        }
        let resource = self
            .adopt(fd, Medium::Stream)
            .map_err(|(reason, value)| Refused { reason, value })?;
        Ok(TcpStream::new(resource))
    }

    /// Gives the descriptor of `stream`, with no close, and frees its slot: for example to give a
    /// connection that this driver accepted to another driver, which takes it with
    /// [`Driver::adopt_stream`]. The stream must have no state: no operation runs or waits, and no
    /// bytes, error, report, or multishot receive stays from an earlier operation. A stream that
    /// ran no operation, such as an accepted one, has none.
    ///
    /// # Errors
    ///
    /// A refusal, with the stream, that gives [`Reason::Busy`] while the stream has a state, or
    /// [`Reason::Resource`] for a stream of another driver.
    #[cfg(unix)]
    pub fn release_stream(&mut self, stream: TcpStream) -> Result<std::os::fd::OwnedFd, Refused<TcpStream>> {
        let resource = stream.read.resource;
        let table = &self.table;
        let reason = match self.resources.get(resource) {
            Some(owned) if owned.directions.idle(|id| table.runs(id)) => None,
            Some(_) => Some(Reason::Busy),
            None => Some(Reason::Resource),
        };
        match reason.map_or_else(|| self.resources.release(resource).ok_or(Reason::Resource), Err) {
            Ok(fd) => Ok(fd),
            Err(reason) => Err(Refused { reason, value: stream }),
        }
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
    pub(super) fn begin_entries<P, H: FnOnce(P) -> Held>(
        &mut self,
        resource: Resource,
        kind: Kind,
        entries: u32,
        value: P,
        held: H,
    ) -> Result<Completion, Refused<P>> {
        if entries > self.queue.entries() {
            return Err(Refused {
                reason: Reason::Queue,
                value,
            });
        }
        // macOS has no segmentation offload, so a send with a segment size ends at its start.
        let (value, held) = if entries == 1 && !kind.batch() && !(cfg!(target_os = "macos") && kind == Kind::SendTo) {
            match self.start_usual(resource, kind, value, held) {
                Ok(completion) => return Ok(completion),
                Err(general) => general,
            }
        } else {
            (value, held)
        };
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
        owned.directions.operated = true;
        // A send of the record whose future was forgotten ends its interest, as `forgo` does.
        #[cfg(all(feature = "local", target_os = "linux"))]
        if kind.writes() && owned.directions.sending.interest {
            owned.directions.forgo_send();
        }
        let has_result = owned.directions.has_result(kind);
        // An operation of the write direction waits for the send of the record that runs.
        let record = kind.writes() && owned.directions.record_sends();
        let Some(direction) = owned.directions.direction(kind) else {
            return Err(Refused {
                reason: Reason::Resource,
                value,
            });
        };
        direction.current = direction.current.filter(|&id| table.runs(id));
        let now = direction.current.is_none() && direction.first.is_none() && !record;
        let tag = Tag::new(Some(resource), kind);
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
        Ok(self.completion(id, !kind.writes()))
    }

    /// The usual start of an operation of one entry: its direction runs no operation, keeps
    /// nothing, and has no failed send, and the table and the queue have room. The operation then
    /// submits now, with one look at its resource, and its record and its entry are written in one
    /// step. Gives `value` and `held` back for the general start otherwise, with no change.
    fn start_usual<P, H: FnOnce(P) -> Held>(
        &mut self,
        resource: Resource,
        kind: Kind,
        value: P,
        held: H,
    ) -> Result<Completion, (P, H)> {
        #[cfg(all(feature = "local", target_os = "linux"))]
        let consumer = self.consumer();
        let multi = self.report.receives() == Shot::Multi;
        let Self {
            resources,
            table,
            queue,
            shared,
            selected,
            ..
        } = self;
        #[cfg(target_os = "windows")]
        let ring = resources.registrar.ring;
        let Some(owned) = resources.get(resource) else {
            return Err((value, held));
        };
        let (Some(fd), pipe, place) = (owned.number(), owned.medium == Medium::Pipe, owned.place(resource)) else {
            return Err((value, held));
        };
        // The entry waits on the readiness of the resource, or names the facts of its packet.
        #[cfg(unix)]
        let token = super::poller::Token {
            index: resource.index,
            generation: resource.generation,
        };
        #[cfg(target_os = "windows")]
        let token = super::iocp::Token {
            skips: owned.skips,
            file: owned.file.is_some(),
            ring: ring && owned.medium == Medium::File,
        };
        let directions = &mut owned.directions;
        let clear = directions.kept.is_none()
            && directions.sent == Sent::Ok
            && !(kind.writes() && directions.record_holds())
            && !(matches!(kind, Kind::Recv | Kind::Selected)
                && (directions.receives_here() || multi && kind == Kind::Selected));
        // The state of the operations of the resource can hold this one from now on.
        directions.operated = true;
        let Some(direction) = directions
            .direction(kind)
            .filter(|direction| clear && direction.current.is_none() && direction.first.is_none() && queue.room() > 0)
        else {
            return Err((value, held));
        };
        #[cfg(all(feature = "local", target_os = "linux"))]
        let tag = Tag {
            consumer,
            ..Tag::new(Some(resource), kind)
        };
        #[cfg(not(all(feature = "local", target_os = "linux")))]
        let tag = Tag::new(Some(resource), kind);
        let Ok(id) = table.begin(tag, Shape::Single) else {
            return Err((value, held));
        };
        let Some(slot) = shared.entries.get(id.index() as usize) else {
            table.withdraw(id);
            return Err((value, held));
        };
        direction.current = Some(id);
        let group = selected.as_ref().map_or(0, |selected| selected.ring.group());
        let slot_len = selected.as_ref().map_or(0, |selected| selected.len as usize);
        slot.start(id, held(value), |held| {
            let sqe = submission(held, kind, fd, pipe, group, slot_len).with_user_data(id.user_data());
            let sqe = place.map_or(sqe, |place| sqe.fixed_file(fd, place));
            // SAFETY: the entry names the loan, the lease, the regions, the message, or the peer
            // address in the resources of `id`, which the home keeps in their slot until its
            // terminal event, and the descriptor of `resource`, which stays open until its close,
            // after each of its operations ends. The queue has room.
            #[expect(unsafe_code, reason = "the kernel uses the memory of the resources of the operation")]
            let pushed = unsafe { queue.push(&sqe, Some(token)) }.is_ok();
            assert!(pushed, "the queue has room for the entry");
        });
        let completion = Completion::new(Arc::clone(shared), id, !kind.writes());
        #[cfg(all(feature = "local", target_os = "linux"))]
        let completion = completion.with_consumer(consumer);
        Ok(completion)
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

    /// The consumer of `id`, an operation of this driver when `shared` is its own, took the result
    /// on the thread of the home: the slot frees, and the direction forgets the operation. Gives
    /// `false`, and changes nothing, for an operation of another driver.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(super) fn end_taken(&mut self, shared: &Arc<super::Shared>, id: OpId) -> bool {
        if !Arc::ptr_eq(shared, &self.shared) {
            return false;
        }
        // The consumer claimed the result, so the table holds it, and the slot frees here, as at
        // the take of the end of interest at a turn.
        if let Some(ended) = self.table.end_interest(id) {
            self.claimed(id, ended);
        }
        true
    }

    /// The future of `id`, an operation of this driver when `shared` is its own, dropped on the
    /// thread of the home: its interest ends now, and a running operation is cancelled when
    /// `cancel` is true, as the requests of the drop do ([`Request`](crate::table::Request)). Gives
    /// `false`, and changes nothing, for an operation of another driver.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(super) fn forgo_now(&mut self, shared: &Arc<super::Shared>, id: OpId, cancel: bool) -> bool {
        if !Arc::ptr_eq(shared, &self.shared) {
            return false;
        }
        self.consumers.clear(id);
        match self.table.end_interest(id) {
            Some(ended) => self.claimed(id, ended),
            None if cancel => self.cancel(id),
            None => {}
        }
        true
    }

    /// The poll of the home operation `id`, an operation of this driver when `shared` is its own,
    /// on the thread of the home: `take` makes the output from the result and the resources in
    /// their slot, and the slot frees; or the operation runs, and the driver keeps the waiter,
    /// `task` or `waker`. Gives `None`, and changes nothing, for an operation of another driver.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(super) fn poll_home<T>(
        &mut self,
        shared: &Arc<super::Shared>,
        id: OpId,
        task: Option<super::home::LocalTask>,
        waker: &core::task::Waker,
        take: impl FnOnce(i32, &mut Held) -> T,
    ) -> Option<core::task::Poll<Result<T, crate::completion::Gone>>> {
        use core::task::Poll;
        if !Arc::ptr_eq(shared, &self.shared) {
            return None;
        }
        let taken = self
            .shared
            .entries
            .get(id.index() as usize)
            .map(|slot| slot.take_home(id, take));
        Some(match taken {
            Some(None) => {
                self.consumers.wait(id, task, waker);
                Poll::Pending
            }
            Some(Some(Ok(output))) => {
                // The table holds the result, so the slot frees here, as at the take of a request.
                if let Some(ended) = self.table.end_interest(id) {
                    self.claimed(id, ended);
                }
                Poll::Ready(Ok(output))
            }
            Some(Some(Err(gone))) => Poll::Ready(Err(gone)),
            None => Poll::Ready(Err(crate::completion::Gone)),
        })
    }

    /// Whether a local receive of `reader` into a selected buffer takes the buffers of the multishot
    /// receive of its stream, with no operation ([`Driver::poll_record`]): the driver keeps one
    /// multishot receive for each stream, `reader` is a stream socket, and its read direction runs,
    /// keeps waiting, and keeps the result of no operation. Else the receive is an operation of
    /// [`Driver::recv_selected`].
    ///
    /// # Errors
    ///
    /// As for [`Driver::recv_selected`].
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn by_record(&mut self, reader: &ReadHalf) -> Result<bool, Refused<()>> {
        let resource = reader.resource;
        self.own(resource, ())?;
        if self.selected.is_none() {
            return Err(Refused {
                reason: Reason::Buffers,
                value: (),
            });
        }
        let (multi, table) = (self.report.receives() == Shot::Multi, &self.table);
        Ok(multi
            && self.resources.get(resource).is_some_and(|owned| {
                let directions = &owned.directions;
                owned.medium == Medium::Stream
                    && (!directions.operated
                        || directions.read.first.is_none()
                            && directions.read.current.is_none_or(|current| !table.runs(current))
                            && directions.kept.is_none())
            }))
    }

    /// The poll of a local receive of `resource` that takes the buffers of its multishot receive
    /// ([`Driver::by_record`]). It takes the first shelved buffer, the end of the stream or the
    /// error of the multishot receive, or `ECANCELED` when it is `cancelled`. Else the receive
    /// waits: the record of the stream keeps the waiter, `task` or `waker`, which the next
    /// completion wakes, and the multishot receive arms when it does not run. Gives `None` for a
    /// resource that this driver does not own.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn poll_record(
        &mut self,
        resource: Resource,
        cancelled: bool,
        task: Option<super::home::LocalTask>,
        waker: &core::task::Waker,
    ) -> Option<core::task::Poll<io::Result<Backing>>> {
        use core::task::Poll;
        let (owned, shelf) = self.resources.with_shelf(resource)?;
        let directions = &mut owned.directions;
        let output = if let Some((buffer, start)) = shelf.take(&mut directions.shelved) {
            let (buffer, moved) = from_start(buffer, start);
            self.copied = self.copied.saturating_add(moved as u64);
            Some(Ok(buffer))
        } else if let Some(result) = directions.ended.take() {
            // An empty `Vec` allocates nothing.
            Some(count(result).map(|_| Backing::from(Vec::new())))
        } else if cancelled {
            Some(Err(io::Error::from_raw_os_error(ECANCELED)))
        } else {
            None
        };
        if let Some(output) = output {
            directions.reading = false;
            directions.reader = super::home::Waiter::None;
            return Some(Poll::Ready(output));
        }
        directions.reading = true;
        directions.reader.set(task, waker);
        if directions.armed == Armed::No {
            let (fd, place) = (owned.number(), owned.place(resource));
            // With a full queue, the next poll arms it, after the turn submits.
            if !fd.is_some_and(|fd| self.arm(resource, fd, place)) {
                waker.wake_by_ref();
            }
        }
        Some(Poll::Pending)
    }

    /// The local receive of `resource` that waits for the buffers of its multishot receive ends its
    /// wait, because it dropped or a cancel ended it. Gives its waiter, which the caller wakes for
    /// a cancel. A direction that keeps its most buffers then ends its multishot receive.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn end_reading(&mut self, resource: Resource) -> super::home::Waiter {
        let Some(owned) = self.resources.get(resource) else {
            return super::home::Waiter::None;
        };
        let directions = &mut owned.directions;
        directions.reading = false;
        let (waiter, full) = (
            directions.take_reader(),
            directions.shelved.len >= super::KEPT_BUFFERS && !directions.awaiting,
        );
        // A failed cancel asks again at the next buffer, which keeps the direction full.
        if full {
            drop(self.cancel_armed(resource));
        }
        waiter
    }

    /// Starts the send of `lease` of a local stream, of `kind` ([`Kind::Send`] or
    /// [`Kind::SendAll`]), in the record of the stream, with no operation ([`Driver::poll_send`]):
    /// on io_uring, when `writer` is a stream socket whose write direction runs, keeps waiting, and
    /// owes no operation and no report, and whose record holds no send that runs. A send of the
    /// record whose future was forgotten ends its interest first. Gives `lease` back when the send
    /// needs an operation of [`Driver::send`] or [`Driver::send_all`].
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn send_record(&mut self, writer: &WriteHalf, lease: Lease, kind: Kind) -> Result<(), Lease> {
        let resource = writer.resource;
        if !matches!(self.queue, super::queue::Queue::Ring(_)) {
            return Err(lease);
        }
        let table = &self.table;
        let Some(owned) = self.resources.get(resource) else {
            return Err(lease);
        };
        let directions = &mut owned.directions;
        // Only a forgotten future can have interest here: the borrow of the half ended.
        if directions.sending.interest {
            directions.forgo_send();
        }
        let free = owned.medium == Medium::Stream
            && directions.sent == Sent::Ok
            && directions.sending.lease.is_none()
            && (!directions.operated
                || directions.closing.is_none()
                    && directions.write.first.is_none()
                    && directions.write.current.is_none_or(|current| !table.runs(current)));
        let (Some(fd), place) = (owned.number(), owned.place(resource)) else {
            return Err(lease);
        };
        if !free || !self.room(1) {
            return Err(lease);
        }
        let (bytes, len) = (lease.as_ptr(), u32::try_from(lease.initialized()).unwrap_or(u32::MAX));
        let sqe = Sqe::send(fd, bytes, len).with_user_data(resource.record(super::Half::Write));
        let sqe = place.map_or(sqe, |place| sqe.fixed_file(fd, place));
        // SAFETY: the entry names the bytes of the lease, which the record keeps until the terminal
        // event, also after the drop of the future, and the descriptor of `resource`, which stays
        // open until its close, after that event. The queue has room.
        #[expect(unsafe_code, reason = "the kernel reads the bytes of the lease")]
        let pushed = unsafe { self.queue.push(&sqe, None) }.is_ok();
        assert!(pushed, "the queue has room for the entry");
        if let Some(owned) = self.resources.get(resource) {
            owned.directions.sending = Sending {
                lease: Some(lease),
                interest: true,
                all: kind == Kind::SendAll,
                ..Sending::default()
            };
        }
        self.resources.in_flight += 1;
        Ok(())
    }

    /// The poll of the send of the record of `resource` ([`Driver::send_record`]): it takes the
    /// count of sent bytes and the lease after the terminal event, and else the record keeps the
    /// waiter, `task` or `waker`. Gives `None` when this driver owns no such send.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn poll_send(
        &mut self,
        resource: Resource,
        task: Option<super::home::LocalTask>,
        waker: &core::task::Waker,
    ) -> Option<core::task::Poll<(io::Result<usize>, Lease)>> {
        self.resources.get(resource)?.directions.take_send(task, waker)
    }

    /// The future of the send of the record of `resource` dropped: its interest ends, and the send
    /// runs on. The abandoned rule applies at its terminal event.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn end_send(&mut self, resource: Resource) {
        if let Some(owned) = self.resources.get(resource) {
            owned.directions.forgo_send();
        }
    }

    /// Asks the kernel to cancel the send of the record of `resource`, once. The future keeps its
    /// interest, and its result gives the exact progress.
    ///
    /// # Errors
    ///
    /// The error of the submission when the queue stays full: the send runs on.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn cancel_record_send(&mut self, resource: Resource) -> io::Result<()> {
        if self
            .resources
            .get(resource)
            .is_none_or(|owned| !owned.directions.sending.runs() || owned.directions.sending.cancelled)
        {
            return Ok(());
        }
        let cancel = Sqe::cancel(resource.record(super::Half::Write), super::RECORD_CANCEL);
        // SAFETY: a cancel names no memory.
        #[expect(unsafe_code, reason = "a cancel names no memory")]
        unsafe { super::push_now(&mut self.queue, &cancel) }?;
        if let Some(owned) = self.resources.get(resource) {
            owned.directions.sending.cancelled = true;
        }
        Ok(())
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
        let Some((owned, shelf)) = self.resources.with_shelf(resource) else {
            return true;
        };
        let Some(fd) = owned.number() else {
            return true;
        };
        let (pipe, place) = (owned.medium == Medium::Pipe, owned.place(resource));
        let Some(slot) = self.shared.entries.get(id.index() as usize) else {
            return true;
        };
        let directions = &mut owned.directions;
        let receive = matches!(kind, Kind::Recv | Kind::Selected);
        let here = receive && (!directions.shelved.is_empty() || directions.ended.is_some());
        // The kept state is last in the directions and is large, so the start reads only its
        // discriminant, and takes it only for an accept that gets its connection.
        let connection = kind == Kind::Accept && matches!(directions.kept, Some(Kept::Connection { .. }));
        let result = if connection {
            if let Some(Kept::Connection { fd, peer, peer_len }) = directions.kept.take() {
                match self.resources.adopt(fd, Medium::Stream) {
                    Ok(stream) => slot.update(id, |held| {
                        if let Some(held) = held {
                            held.accepted = Some(stream);
                            if let Some((kept, len)) = held.payload.peer() {
                                (*kept, *len) = (peer, peer_len);
                            }
                        }
                    }),
                    // With no free resource slot, or no registration, the listener keeps the
                    // connection, and this accept gives `OutOfMemory`.
                    Err((_, fd)) => {
                        if let Some(listener) = self.resources.get(resource) {
                            listener.directions.keep_connection(fd, peer, peer_len);
                        }
                    }
                }
            }
            Some(0)
        } else if directions.kept.is_none() && directions.sent == Sent::Ok && !here {
            // Nothing kept and no failed send: the operation needs its submission.
            None
        } else {
            let (table, copied) = (&self.table, &mut self.copied);
            let mut result = None;
            slot.update(id, |held| {
                if let Some(held) = held {
                    result = directions.resume(held, kind, || table.interest(id), copied, shelf);
                }
            });
            result
        };
        if let Some(result) = result {
            if let Some(owned) = self.resources.get(resource) {
                owned.directions.awaiting = false;
            }
            self.complete_now(id, result);
            return true;
        }
        // A receive waits for the multishot receive of its direction. A receive into the caller's
        // buffer ends it, so the next bytes go to that buffer with no copy once the shelved ones
        // are copied.
        if receive
            && let Some(owned) = self.resources.get(resource)
            && owned.directions.armed != Armed::No
        {
            owned.directions.awaiting = true;
            // After a failed cancel, the receive copies the next buffer.
            if kind == Kind::Recv {
                drop(self.cancel_armed(resource));
            }
            return true;
        }
        if kind == Kind::Selected && !pipe && self.report.receives() == Shot::Multi && self.arm(resource, fd, place) {
            if let Some(owned) = self.resources.get(resource) {
                owned.directions.awaiting = true;
            }
            return true;
        }
        // macOS has no segmentation offload: a send with a segment size ends with no call.
        #[cfg(target_os = "macos")]
        if kind == Kind::SendTo {
            let mut segmented = false;
            self.hold(id, |held| {
                segmented =
                    matches!(&held.payload, Payload::SendTo { datagram, .. } if datagram.segment_size.is_some());
            });
            if segmented {
                self.complete_now(id, udp::UNSEGMENTED);
                return true;
            }
        }
        if kind.batch() {
            return self.start_batch(id, fd, place);
        }
        if !self.room(1) {
            return false;
        }
        let group = self.selected.as_ref().map_or(0, |selected| selected.ring.group());
        let slot_len = self.selected.as_ref().map_or(0, |selected| selected.len as usize);
        // SAFETY: the entry names the loan, the lease, the regions, the message, or the peer
        // address in the resources of `id`, which the home keeps until its terminal event, and the
        // descriptor of `resource`, which stays open until its close, after each of its operations
        // ends. The queue has room.
        #[expect(unsafe_code, reason = "the kernel uses the memory of the resources of the operation")]
        unsafe {
            self.push(id, |held| {
                let sqe = submission(held, kind, fd, pipe, group, slot_len);
                place.map_or(sqe, |place| sqe.fixed_file(fd, place))
            });
        }
        true
    }

    /// Arms the multishot receive of the read direction of the stream `resource`. No operation
    /// holds it: each completion names the resource slot, and the direction keeps each buffer,
    /// until the terminal completion. Gives `false`, and changes nothing, when the driver has no
    /// provided buffers or the queue has no room.
    fn arm(&mut self, resource: Resource, fd: i32, place: Option<u32>) -> bool {
        let Some(group) = self.selected.as_ref().map(|selected| selected.ring.group()) else {
            return false;
        };
        if !self.room(1) {
            return false;
        }
        let sqe = Sqe::recv_multishot(fd, group).with_user_data(resource.record(super::Half::Read));
        let sqe = place.map_or(sqe, |place| sqe.fixed_file(fd, place));
        // SAFETY: the entry names no memory, and the descriptor of `resource`, which stays open
        // until its close, after the terminal completion of the receive. The queue has room.
        #[expect(unsafe_code, reason = "the kernel uses the descriptor of the resource")]
        let pushed = unsafe { self.queue.push(&sqe, None) }.is_ok();
        assert!(pushed, "the queue has room for the entry");
        if let Some(owned) = self.resources.get(resource) {
            owned.directions.armed = Armed::Runs;
            owned.directions.fresh = true;
        }
        self.resources.in_flight += 1;
        true
    }

    /// Asks the kernel to cancel the multishot receive of `resource`, once.
    ///
    /// # Errors
    ///
    /// The error of the submission when the queue stays full: the receive runs on.
    pub(super) fn cancel_armed(&mut self, resource: Resource) -> io::Result<()> {
        if self
            .resources
            .get(resource)
            .is_none_or(|owned| owned.directions.armed != Armed::Runs)
        {
            return Ok(());
        }
        let cancel = Sqe::cancel(resource.record(super::Half::Read), super::RECORD_CANCEL);
        // SAFETY: a cancel names no memory.
        #[expect(unsafe_code, reason = "a cancel names no memory")]
        unsafe { super::push_now(&mut self.queue, &cancel) }?;
        if let Some(owned) = self.resources.get(resource) {
            owned.directions.armed = Armed::Ending;
        }
        Ok(())
    }

    /// Asks again to cancel the multishot receive and the send of the record of each stream that
    /// closes, after a cancel found the queue full ([`Driver::start_close`]).
    pub(super) fn recancel(&mut self) {
        for index in 0..self.resources.slots.len() {
            let Some(owned) = self.resources.slots.get(index) else {
                continue;
            };
            if owned.directions.closing.is_some() {
                let resource = Resource {
                    driver: self.resources.driver,
                    index: u32::try_from(index).unwrap_or(u32::MAX),
                    generation: owned.generation,
                };
                #[cfg(all(feature = "local", target_os = "linux"))]
                let sends = self.cancel_record_send(resource).is_ok();
                #[cfg(not(all(feature = "local", target_os = "linux")))]
                let sends = true;
                if self.cancel_armed(resource).is_err() || !sends {
                    self.resources.recancel = true;
                    return;
                }
            }
        }
    }

    /// Ends the receive of `resource` that waits for the multishot receive of its stream, with no
    /// submission, as at a teardown.
    pub(super) fn end_awaiting(&mut self, resource: Resource) {
        let current = self
            .direction(resource, Kind::Recv)
            .and_then(|direction| direction.current);
        if let Some(id) = current
            && let Some(&tag) = self.table.resources(id)
            && self.unawait(tag, id)
        {
            self.complete_now(id, -ECANCELED);
        }
    }

    /// Ends `id` when it is the receive that waits for the multishot receive of its direction,
    /// with no submission: gives whether it waited.
    pub(super) fn unawait(&mut self, tag: Tag, id: OpId) -> bool {
        let Some(owned) = tag.resource.and_then(|resource| self.resources.get(resource)) else {
            return false;
        };
        let directions = &mut owned.directions;
        let waits = directions.awaiting && directions.read.current == Some(id);
        if waits {
            directions.awaiting = false;
        }
        waits
    }

    /// Submits the batch `id` on the socket `fd`: one entry for each datagram, each linked to the
    /// next, in one kernel entry. A batch receive waits only in its first entry. Gives `false`, and
    /// changes nothing, when the queue has no room for each entry.
    fn start_batch(&mut self, id: OpId, fd: i32, place: Option<u32>) -> bool {
        let mut entries = Ok(0);
        self.hold(id, |held| {
            entries = match held.payload.batch() {
                Some(Batch::Send(datagrams)) => datagrams.sendable(),
                Some(Batch::Receive(arrivals)) => Ok(arrivals.capacity()),
                None => Ok(0),
            };
        });
        let entries = match entries {
            Ok(0) => {
                self.complete_now(id, 0);
                return true;
            }
            Ok(entries) => entries,
            Err(result) => {
                self.complete_now(id, result);
                return true;
            }
        };
        if !self.room(u32::try_from(entries).unwrap_or(u32::MAX)) {
            return false;
        }
        let group = self.selected.as_ref().map_or(0, |selected| selected.ring.group());
        let slot_len = self.selected.as_ref().map_or(0, |selected| selected.len as usize);
        for index in 0..entries {
            let last = index + 1 == entries;
            let entry = move |held: &mut Held| {
                let sqe = match held.payload.batch() {
                    Some(Batch::Send(datagrams)) => datagrams
                        .message(index)
                        .map_or_else(Sqe::nop, |message| Sqe::send_message(fd, message)),
                    Some(Batch::Receive(arrivals)) => {
                        // The first receive waits for a datagram; the next ones take only the
                        // datagrams that wait.
                        // On Windows, the port takes the receives after the first of a chain with
                        // no wait.
                        #[cfg(unix)]
                        let flags = if index == 0 {
                            0
                        } else {
                            rsnetwork_udp::sys::MSG_DONTWAIT
                        };
                        #[cfg(target_os = "windows")]
                        let flags = 0;
                        arrivals.message(index, slot_len).map_or_else(Sqe::nop, |message| {
                            Sqe::recv_message_selected(fd, message, flags, group)
                        })
                    }
                    None => Sqe::nop(),
                };
                let sqe = place.map_or(sqe, |place| sqe.fixed_file(fd, place));
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

    /// Pushes again the rest of the send of each byte of the write direction of `resource` that
    /// sent only part of its bytes ([`Directions::send_again`]): the send of the record, or the
    /// operation that runs. An operation that a cancel reached meanwhile ends now, with the bytes
    /// that went. Gives `false`, and keeps the resource pending, when the queue has no room.
    fn resend(&mut self, resource: Resource) -> bool {
        let Some(owned) = self.resources.get(resource) else {
            return true;
        };
        if !core::mem::take(&mut owned.directions.again) {
            return true;
        }
        let (Some(fd), pipe, place) = (owned.number(), owned.medium == Medium::Pipe, owned.place(resource)) else {
            return true;
        };
        #[cfg(all(feature = "local", target_os = "linux"))]
        let record = owned.directions.record_sends();
        #[cfg(not(all(feature = "local", target_os = "linux")))]
        let record = false;
        // A cancel that came between two sends of the kernel ends the send now, with the bytes
        // that went.
        #[cfg(all(feature = "local", target_os = "linux"))]
        if record && owned.directions.sending.cancelled {
            let (needs, waiter) = owned.directions.record_sent(-ECANCELED);
            if let Some(waiter) = waiter {
                self.consumers.wake_waiter(waiter);
                self.resources.in_flight -= 1;
            }
            if needs {
                self.pending.push(resource);
            }
            return true;
        }
        let current = owned.directions.write.current;
        if !record && let Some(id) = current {
            let mut cancelled = false;
            self.hold(id, |held| cancelled = held.cancelled);
            if cancelled {
                self.complete_now(id, 0);
                return true;
            }
        }
        if !self.room(1) {
            if let Some(owned) = self.resources.get(resource) {
                owned.directions.again = true;
                if !owned.directions.pending {
                    owned.directions.pending = true;
                    self.pending.push(resource);
                }
            }
            return false;
        }
        #[cfg(all(feature = "local", target_os = "linux"))]
        if record {
            let Some(owned) = self.resources.get(resource) else {
                return true;
            };
            let sending = &owned.directions.sending;
            let rest = sending
                .lease
                .as_ref()
                .and_then(|lease| lease.bytes().get(sending.went..))
                .unwrap_or_default();
            let (bytes, len) = (rest.as_ptr(), u32::try_from(rest.len()).unwrap_or(u32::MAX));
            let sqe = Sqe::send(fd, bytes, len).with_user_data(resource.record(super::Half::Write));
            let sqe = place.map_or(sqe, |place| sqe.fixed_file(fd, place));
            // SAFETY: the entry names the rest of the bytes of the lease, which the record keeps
            // until the terminal event, and the descriptor of `resource`, which stays open until
            // its close, after that event. The queue has room.
            #[expect(unsafe_code, reason = "the kernel reads the bytes of the lease")]
            let pushed = unsafe { self.queue.push(&sqe, None) }.is_ok();
            assert!(pushed, "the queue has room for the entry");
            return true;
        }
        if let Some(id) = current {
            let group = self.selected.as_ref().map_or(0, |selected| selected.ring.group());
            let slot_len = self.selected.as_ref().map_or(0, |selected| selected.len as usize);
            // SAFETY: the entry names the rest of the lease in the resources of `id`, which the
            // home keeps until its terminal event, and the descriptor of `resource`, which stays
            // open until its close, after each of its operations ends. The queue has room.
            #[expect(unsafe_code, reason = "the kernel reads the bytes of the lease")]
            unsafe {
                self.push(id, |held| {
                    let sqe = submission(held, Kind::SendAll, fd, pipe, group, slot_len);
                    place.map_or(sqe, |place| sqe.fixed_file(fd, place))
                });
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
            self.attend_armed(resource);
            if !self.resend(resource) {
                return;
            }
            for kind in [Kind::Recv, Kind::Send] {
                let table = &self.table;
                let Some(owned) = self.resources.get(resource) else {
                    continue;
                };
                let record = kind == Kind::Send && owned.directions.record_sends();
                let runs = owned
                    .directions
                    .direction(kind)
                    .and_then(|direction| direction.current)
                    .is_some_and(|current| table.runs(current));
                if runs || record {
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

    /// The read direction of `resource` with a multishot receive: the receive that waits takes a
    /// shelved buffer or the end, or arms again, and a direction with no waiting receive that
    /// keeps [`KEPT_BUFFERS`](super::KEPT_BUFFERS) buffers ends its multishot receive.
    fn attend_armed(&mut self, resource: Resource) {
        let Some(owned) = self.resources.get(resource) else {
            return;
        };
        let directions = &owned.directions;
        let (awaiting, current, armed) = (directions.awaiting, directions.read.current, directions.armed);
        let (waits, full) = (directions.waits(), directions.shelved.len >= super::KEPT_BUFFERS);
        let waiting = current
            .filter(|&current| awaiting && self.table.runs(current))
            .and_then(|current| Some((current, self.table.resources(current)?.kind)));
        if awaiting && waiting.is_none() {
            owned.directions.awaiting = false;
        }
        if let Some((current, kind)) = waiting
            && !self.start(current, resource, kind)
        {
            if let Some(owned) = self.resources.get(resource) {
                owned.directions.awaiting = false;
            }
            self.wait_first(resource, kind, current);
        }
        // A failed cancel asks again at the next buffer, which keeps the direction full.
        if armed == Armed::Runs && !waits && full {
            drop(self.cancel_armed(resource));
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
        let tag = Tag::new(Some(resource), Kind::Close);
        let id = self
            .open(tag, 0, value, |_| Held::default())
            .map_err(|(reason, value)| Refused { reason, value })?;
        self.start_close(resource, Closing::Future(id));
        Ok(self.completion(id, false))
    }

    /// Starts the close of `resource` that a drop of a local type recorded, as
    /// [`Driver::close_resource`] does, with no operation and no report. A record of a resource
    /// that this driver no longer owns changes nothing.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(super) fn close_dropped_resource(&mut self, resource: Resource) {
        if self.resources.get(resource).is_some() {
            self.start_close(resource, Closing::Dropped);
        }
    }

    /// Marks `resource` closing, cancels each of its operations, and closes it after each one ends.
    fn start_close(&mut self, resource: Resource, closing: Closing) {
        if let Some(owned) = self.resources.get(resource) {
            owned.directions.closing = Some(closing);
            owned.directions.operated = true;
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
        // The close takes the result of the send of the record, and cancels it when it runs, a
        // dropped send too.
        #[cfg(all(feature = "local", target_os = "linux"))]
        if let Some(owned) = self.resources.get(resource) {
            owned.directions.forgo_send();
        }
        #[cfg(all(feature = "local", target_os = "linux"))]
        let sends = self.cancel_record_send(resource).is_ok();
        #[cfg(not(all(feature = "local", target_os = "linux")))]
        let sends = true;
        // A full queue asks again at the next turn.
        if self.cancel_armed(resource).is_err() || !sends {
            self.resources.recancel = true;
        }
        self.close_if_idle(resource);
        // A close can release a resource that the pending list holds, so the list is empty
        // before a new resource takes that slot.
        self.attend();
    }

    /// Ends the close of `resource` when no operation of it runs: releases what its directions
    /// keep, closes its descriptor, and gives the report to the future of the close.
    fn close_if_idle(&mut self, resource: Resource) {
        let table = &self.table;
        let Some(owned) = self.resources.get(resource) else {
            return;
        };
        let directions = &owned.directions;
        let Some(closing) = directions.closing else {
            return;
        };
        let runs = [directions.read.current, directions.write.current]
            .into_iter()
            .flatten()
            .any(|operation| table.runs(operation))
            || directions.armed != Armed::No
            || directions.record_sends()
            || directions.read.first.is_some()
            || directions.write.first.is_some()
            || running(owned);
        if runs {
            return;
        }
        let closed = directions.closed();
        // The release drops the kept bytes and connections, and the descriptor.
        drop(self.resources.release(resource));
        match closing {
            Closing::Future(id) => {
                if let Some(slot) = self.shared.entries.get(id.index() as usize) {
                    slot.update(id, |held| {
                        if let Some(held) = held {
                            held.closed = Some(closed);
                        }
                    });
                }
                self.complete_now(id, 0);
            }
            #[cfg(all(feature = "local", target_os = "linux"))]
            Closing::Dropped => {
                if let SendCount::Sent(sent) = closed.sent {
                    let sent = u64::try_from(sent).unwrap_or(u64::MAX);
                    self.dropped_sent = self.dropped_sent.saturating_add(sent);
                }
            }
        }
    }
}

/// The submission of an operation of `kind` on the descriptor `fd`, from its resources `held`:
/// `pipe` selects the calls of a pipe end, and a selecting receive takes a buffer of `slot_len`
/// bytes from the buffer group `group`. A batch submits in `start_batch`, and each other kind
/// always has its resources, so the last arm gives no entry that the kernel runs.
pub(super) fn submission(held: &mut Held, kind: Kind, fd: i32, pipe: bool, group: u16, slot_len: usize) -> Sqe {
    match (kind, &mut held.payload, held.loan.as_mut()) {
        (Kind::Recv, _, Some(loan)) => {
            let (buffer, len) = (loan.as_mut_ptr(), u32::try_from(loan.capacity()).unwrap_or(u32::MAX));
            if pipe {
                Sqe::read(fd, buffer, len)
            } else {
                Sqe::recv(fd, buffer, len)
            }
        }
        (Kind::Selected, _, _) if pipe => Sqe::read_selected(fd, group),
        (Kind::Selected, _, _) => Sqe::recv_selected(fd, group),
        (Kind::Send | Kind::SendAll, Payload::Lease(lease), _) => {
            // A send of each byte sends the bytes that its earlier sends did not.
            let rest = if kind == Kind::SendAll { held.copied } else { 0 };
            let bytes = lease.bytes().get(rest..).unwrap_or_default();
            let (bytes, len) = (bytes.as_ptr(), u32::try_from(bytes.len()).unwrap_or(u32::MAX));
            if pipe {
                Sqe::write(fd, bytes, len)
            } else {
                Sqe::send(fd, bytes, len)
            }
        }
        (Kind::Vectored, Payload::Regions(regions), _) if pipe => {
            let (vectors, count) = regions.vectors();
            Sqe::writev(fd, vectors, count)
        }
        #[cfg(unix)]
        (Kind::Vectored, Payload::Regions(regions), _) => Sqe::send_message(fd, regions.message()),
        // The port sends the `WSABUF` array of a vectored write with one `WSASend`.
        #[cfg(target_os = "windows")]
        (Kind::Vectored, Payload::Regions(regions), _) => {
            let (vectors, count) = regions.vectors();
            Sqe::writev(fd, vectors, count)
        }
        #[cfg(unix)]
        (Kind::Shutdown, _, _) => Sqe::shutdown(fd, sys::SHUT_WR),
        #[cfg(target_os = "windows")]
        (Kind::Shutdown, _, _) => Sqe::shutdown(fd, sys::SD_SEND),
        (Kind::RecvFrom, Payload::Receive { message, .. }, Some(loan)) => {
            let (region, len) = (loan.as_mut_ptr(), loan.capacity());
            Sqe::recv_message(fd, message.receive(region, len), 0)
        }
        (Kind::RecvSelectedFrom, Payload::Receive { message, .. }, _) => {
            Sqe::recv_message_selected(fd, message.receive(ptr::null_mut(), slot_len), 0, group)
        }
        (Kind::SendTo, Payload::SendTo { message, datagram }, _) => Sqe::send_message(fd, message.send(datagram)),
        (Kind::Accept, Payload::Socket { peer, peer_len, .. }, _) => {
            Sqe::accept(fd, (&raw mut *peer).cast(), &raw mut *peer_len)
        }
        _ => Sqe::nop(),
    }
}

/// `buffer` with its bytes from `start` moved to its start, and the count of the bytes that moved:
/// none when `start` is 0. A receive into the caller's buffer copied the bytes before `start`.
fn from_start(mut buffer: Backing, start: usize) -> (Backing, usize) {
    if start == 0 {
        return (buffer, 0);
    }
    let len = buffer.initialized() - start;
    buffer.bytes_mut().copy_within(start.., 0);
    buffer.truncate(len);
    (buffer, len)
}

/// Gives a selected receive, with its resources `held`, `buffer` with its bytes from `start`, and
/// adds the bytes that move to `copied`. Gives the count of the bytes of the buffer.
fn select(held: &mut Held, buffer: Backing, start: usize, copied: &mut u64) -> i32 {
    let (buffer, moved) = from_start(buffer, start);
    *copied = copied.saturating_add(moved as u64);
    held.copied = moved;
    let len = buffer.initialized();
    held.loan = Some(WriteLoan::new(buffer));
    // The bytes came from one kernel result, so their count fits an `i32`.
    i32::try_from(len).unwrap_or(i32::MAX)
}

/// Gives the kept datagram of `buffer` with `received` to the receive of `held`, copied into its
/// buffer: one copy, which [`Driver::copied`] counts. Gives the result of the receive and the
/// bytes that it copied.
fn deliver_copy(held: &mut Held, buffer: &Backing, received: Received) -> (i32, usize) {
    let mut copied = 0;
    if let (Some(loan), Payload::Receive { received: kept, .. }) = (held.loan.as_mut(), &mut held.payload) {
        let (facts, count) = udp::copy_kept(buffer, received, loan);
        *kept = Some(facts);
        copied = count;
    }
    // The kept bytes came from one kernel result, so their count fits an `i32`.
    (i32::try_from(copied).unwrap_or(i32::MAX), copied)
}

/// Gives the kept datagram of `buffer` with `received` to the selecting receive of `held`, with no
/// copy. Gives the result of the receive.
fn deliver(held: &mut Held, buffer: Backing, received: Received) -> i32 {
    let len = i32::try_from(received.len).unwrap_or(i32::MAX);
    if let Payload::Receive { received: facts, .. } = &mut held.payload {
        held.loan = Some(WriteLoan::new(buffer));
        *facts = Some(received);
    }
    len
}

/// Whether operations of the file of `owned` run, which its close waits for.
const fn running(owned: &super::Owned) -> bool {
    owned.running > 0
}

/// A new nonblocking TCP socket that listens at the address `storage` of `len` bytes, with
/// `options`, as [`Driver::bind`] makes it, and its local address. No driver owns the socket.
#[cfg(all(feature = "movable", target_os = "linux"))]
pub(super) fn listening(
    (storage, len): (SockAddrStorage, u32),
    options: TcpOptions,
) -> io::Result<(Handle, (SockAddrStorage, u32))> {
    let socket = new_socket(storage.ss_family, sys::SOCK_STREAM)?;
    set_int(borrow(&socket), sys::SOL_SOCKET, sys::SO_REUSEADDR, 1)?;
    options.apply(borrow(&socket))?;
    bind_now(borrow(&socket), &storage, len, true)?;
    let local = local_address(borrow(&socket)).ok_or_else(io::Error::last_os_error)?;
    Ok((socket, local))
}

/// The negative errno of `error`, as a kernel result gives it.
pub(super) fn errno(error: &io::Error) -> i32 {
    error.raw_os_error().unwrap_or(EINVAL).saturating_neg()
}

// The socket calls of Windows, which the port module declares.
#[cfg(target_os = "windows")]
pub(super) use super::iocp::{bind_now, int, local_address, new_socket, set_int};

/// The size of an `int`, as a `socklen_t`.
#[cfg(unix)]
const INT_LEN: u32 = 4;

const _: () = assert!(size_of::<c_int>() == 4);

/// A new nonblocking socket of `family` and `kind`, `SOCK_STREAM` or `SOCK_DGRAM`, which closes
/// at an exec.
#[cfg(unix)]
#[expect(unsafe_code, reason = "the C library gives a new descriptor")]
pub(super) fn new_socket(family: SaFamily, kind: c_int) -> io::Result<Handle> {
    #[cfg(target_os = "linux")]
    let kind = kind | sys::SOCK_NONBLOCK | sys::SOCK_CLOEXEC;
    // SAFETY: the call takes no pointer.
    let fd = unsafe { sys::socket(c_int::from(family), kind, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // macOS has no flags of `socket` for the two properties.
    #[cfg(target_os = "macos")]
    let fd = super::poller::prepare(fd).map_err(io::Error::from_raw_os_error)?;
    // SAFETY: the C library gave a new descriptor that nothing else owns.
    Ok(unsafe { Handle::from_raw_fd(fd) })
}

/// Sets the option `name` of `level` of `socket` to the `int` `value`.
#[cfg(unix)]
#[expect(unsafe_code, reason = "the call reads one `int` that lives during the call")]
fn set_int(socket: Borrowed<'_>, level: c_int, name: c_int, value: c_int) -> io::Result<()> {
    // SAFETY: `value` is an `int` that lives during the call, and `INT_LEN` is its size.
    let result = unsafe { sys::setsockopt(socket.as_raw_fd(), level, name, (&raw const value).cast(), INT_LEN) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// The `int` option `name` of `level` of `socket`.
#[cfg(unix)]
#[expect(unsafe_code, reason = "the kernel writes one `int` that lives during the call")]
fn int(socket: Borrowed<'_>, level: c_int, name: c_int) -> io::Result<c_int> {
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
#[cfg(unix)]
pub(super) fn local_address(socket: impl std::os::fd::AsFd) -> Option<(SockAddrStorage, u32)> {
    address(socket, sys::getsockname)
}

/// The address of the peer of `socket`, and its length, or `None` when the socket has no peer.
#[cfg(unix)]
fn peer_address(socket: impl std::os::fd::AsFd) -> Option<(SockAddrStorage, u32)> {
    address(socket, sys::getpeername)
}

/// The address of `socket` that `name` gives, `getsockname` or `getpeername`, and its length.
#[cfg(unix)]
#[expect(
    unsafe_code,
    reason = "the kernel writes the address into storage that lives during the call"
)]
fn address(
    socket: impl std::os::fd::AsFd,
    name: unsafe extern "C" fn(c_int, *mut c_void, *mut u32) -> c_int,
) -> Option<(SockAddrStorage, u32)> {
    let (mut address, mut len) = (SockAddrStorage::EMPTY, SockAddrStorage::LEN);
    // SAFETY: the kernel writes at most `len` bytes into `address`, which has room for `LEN` bytes
    // and lives during the call, and the length into `len`.
    let result = unsafe { name(socket.as_fd().as_raw_fd(), (&raw mut address).cast(), &raw mut len) };
    (result == 0).then_some((address, len))
}

/// Binds `socket` to the address of `len` bytes in `address`, and with `listen` listens on it,
/// with system calls.
#[cfg(unix)]
#[expect(unsafe_code, reason = "the call reads the address, which lives during the call")]
fn bind_now(socket: Borrowed<'_>, address: &SockAddrStorage, len: u32, listen: bool) -> io::Result<()> {
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
