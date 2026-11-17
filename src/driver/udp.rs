//! UDP on the driver: bind with options, whole datagrams with their metadata, segmented sends (GSO)
//! and coalesced receives (GRO), batches of datagrams, truncation, and close.
//!
//! Each datagram goes to the kernel as one message (`IORING_OP_SENDMSG` and `IORING_OP_RECVMSG`),
//! whose control messages `rsnetwork_udp::Control` writes and reads, so the driver and the
//! driver-free calls of `rsnetwork-udp` give the same metadata. A socket has the directions of a
//! stream ([`ReadHalf`] and [`WriteHalf`]) and the rules for a dropped or forgotten receive, in their
//! datagram form.

use alloc::vec::Vec;
#[cfg(target_os = "windows")]
use core::ffi::c_int;
use core::net::{IpAddr, SocketAddr};
use core::ptr;
use std::io;

// The message header of one datagram and its region: `struct msghdr` and `struct iovec` on Linux
// and macOS, `WSAMSG` and `WSABUF` on Windows.
#[cfg(unix)]
use rsnetwork_udp::sys::{IoVec as Region, MsgHdr as Header};
#[cfg(target_os = "windows")]
use rsnetwork_udp::sys::{WsaBuf as Region, WsaMsg as Header};
use rsnetwork_udp::{Control, Ecn, Received, SockAddrStorage, Transmit};

use super::tcp::{ReadHalf, TcpOptions, WriteHalf};
use super::{Borrowed, Completion, Driver, Held, Kind, Operation, Payload, Reason, Refused, Resource};
use crate::buffer::{Backing, Buffer as _, Lease, WriteLoan};

/// A UDP socket that a driver owns: its read half and its write half.
///
/// A receive borrows the read half exclusively, and a send borrows the write half exclusively, as
/// for a [`TcpStream`](super::TcpStream), so the rules for a dropped or forgotten receive keep each
/// datagram. A receive whose future drops leaves the datagram that it took, with its metadata and
/// its truncation result, to the next receive, which gives it before it submits. A send whose future
/// drops is not cancelled, and its result is lost, as a datagram can be lost: it fails no direction,
/// because datagram boundaries are not at risk.
///
/// A datagram with its ECN codepoint, from one socket to another, with `run` as in
/// [`Operation`](super::Operation):
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
/// use rsasync::datagram::Ecn;
/// use rsasync::driver::{Config, Datagram, Driver, UdpOptions};
///
/// let mut driver = Driver::new(Config::new())?;
/// // The receiver reports the ECN codepoint and the local address of each datagram.
/// let options = UdpOptions { metadata: true, ..UdpOptions::default() };
/// let bind = driver.bind_udp("127.0.0.1:0".parse()?, options)?;
/// let mut receiver = run(&mut driver, bind)???;
/// let bind = driver.bind_udp("127.0.0.1:0".parse()?, UdpOptions::default())?;
/// let mut sender = run(&mut driver, bind)???;
///
/// let datagram = Datagram {
///     ecn: Some(Ecn::Ect0),
///     ..Datagram::new(Lease::from(b"hello".to_vec()), receiver.local_addr())
/// };
/// let send = driver.send_to(&mut sender, datagram)?;
/// let (sent, _datagram) = run(&mut driver, send)??;
/// assert_eq!(sent?, 1);
///
/// let receive = driver.recv_from(&mut receiver, Backing::from(Vec::with_capacity(1500)))?;
/// let (received, buffer) = run(&mut driver, receive)??;
/// let received = received?;
/// assert_eq!(buffer.bytes(), b"hello");
/// assert_eq!((received.truncated, received.metadata.ecn), (false, Some(Ecn::Ect0)));
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct UdpSocket {
    read: ReadHalf,
    write: WriteHalf,
    local: SocketAddr,
}

impl UdpSocket {
    /// The local address of the socket.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    /// The read half and the write half, so that a receive and a send can run at one time.
    pub fn split(&mut self) -> (&mut ReadHalf, &mut WriteHalf) {
        (&mut self.read, &mut self.write)
    }
}

impl AsMut<ReadHalf> for UdpSocket {
    fn as_mut(&mut self) -> &mut ReadHalf {
        &mut self.read
    }
}

impl AsMut<WriteHalf> for UdpSocket {
    fn as_mut(&mut self) -> &mut WriteHalf {
        &mut self.write
    }
}

/// The options of a UDP socket, which [`Driver::bind_udp`] sets before the bind. `None` and
/// `false` keep the value of the system.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UdpOptions {
    /// An IPv6 socket takes no IPv4 traffic (`IPV6_V6ONLY`).
    pub only_v6: Option<bool>,
    /// The send buffer size in bytes (`SO_SNDBUF`), which Linux doubles.
    pub send_buffer: Option<u32>,
    /// The receive buffer size in bytes (`SO_RCVBUF`), which Linux doubles.
    pub receive_buffer: Option<u32>,
    /// Sockets of one user can bind the same address (`SO_REUSEPORT`).
    pub reuse_port: Option<bool>,
    /// Deliver the ECN codepoint and the local destination address of each received datagram
    /// (`datagram::configure`).
    pub metadata: bool,
    /// Refuse a datagram above the path MTU, which path MTU discovery needs
    /// (`datagram::set_dont_fragment`). Such a send gives `Refused::TooLarge`.
    pub dont_fragment: bool,
    /// Deliver coalesced datagrams of one peer in one receive, with their segment size (GRO, and
    /// URO on Windows). macOS has no coalesced receive, so there the bind gives `Unsupported`
    /// (`ENOTSUP`).
    pub gro: bool,
}

/// `ENOTSUP` (`sys/errno.h` of macOS): the system has no such operation.
#[cfg(target_os = "macos")]
const ENOTSUP: i32 = 45;

/// The result of a send that the system refuses before its call, because the datagram has a
/// segment size: macOS has no segmentation offload. `datagram::Refused::of_send` gives
/// `Refused::NoSegmentation` for it.
#[cfg(target_os = "macos")]
pub(super) const UNSEGMENTED: i32 = -ENOTSUP;

impl UdpOptions {
    /// Sets the options on the new socket `socket`.
    fn apply(self, socket: Borrowed<'_>) -> io::Result<()> {
        TcpOptions {
            only_v6: self.only_v6,
            send_buffer: self.send_buffer,
            receive_buffer: self.receive_buffer,
            reuse_port: self.reuse_port,
            ..TcpOptions::default()
        }
        .apply(socket)?;
        if self.metadata {
            rsnetwork_udp::configure(socket)?;
        }
        if self.dont_fragment {
            rsnetwork_udp::set_dont_fragment(socket)?;
        }
        #[cfg(target_os = "linux")]
        if self.gro {
            rsnetwork_udp::set_gro(socket, true)?;
        }
        #[cfg(target_os = "windows")]
        if self.gro {
            rsnetwork_udp::set_uro(socket, true)?;
        }
        #[cfg(target_os = "macos")]
        if self.gro {
            return Err(io::Error::from_raw_os_error(ENOTSUP));
        }
        Ok(())
    }
}

/// A datagram to send, which the send owns until it ends: its bytes and its metadata. With a
/// segment size, it is a batch of segments that the kernel or the device sends as one datagram
/// for each segment (GSO), all of them or none.
#[derive(Debug)]
pub struct Datagram {
    /// The bytes of the datagram, or of all of its segments.
    pub contents: Lease,
    /// The peer, or `None` for the peer of a connected socket.
    pub destination: Option<SocketAddr>,
    /// The size of each segment, the last one can be shorter, or `None` for one datagram.
    pub segment_size: Option<u16>,
    /// The ECN codepoint of each datagram.
    pub ecn: Option<Ecn>,
    /// The local source address, or `None` for the address that the route selects.
    pub source: Option<IpAddr>,
}

impl Datagram {
    /// A datagram of `contents` to `destination`, with no other metadata.
    #[must_use]
    pub fn new(contents: Lease, destination: SocketAddr) -> Self {
        Self {
            contents,
            destination: Some(destination),
            segment_size: None,
            ecn: None,
            source: None,
        }
    }

    /// The datagrams that the send of this one makes: one, or one for each segment.
    fn datagrams(&self) -> usize {
        self.transmit().datagrams()
    }

    fn transmit(&self) -> Transmit<'_> {
        Transmit {
            destination: self.destination,
            contents: self.contents.bytes(),
            segment_size: self.segment_size,
            ecn: self.ecn,
            source: self.source,
        }
    }
}

/// The message of one datagram for the kernel: its header, its region, its address, and its
/// control messages. Its pointers name its own fields, so it stays in one place from the build of
/// its header to the end of its operation: in the resources of the operation, or in the heap
/// array of a batch.
#[derive(Debug)]
pub(crate) struct Message {
    header: Header,
    region: Region,
    name: SockAddrStorage,
    control: Control,
}

#[expect(
    unsafe_code,
    reason = "the header names only the fields of this value and a region of its operation"
)]
// SAFETY: the pointers of `header` and `region` name the fields of this value and the region of a
// buffer or a lease that the same operation owns, and only the kernel reads or writes through them,
// during that operation. A move to another thread moves that memory with them.
unsafe impl Send for Message {}

impl Default for Message {
    fn default() -> Self {
        Self {
            header: EMPTY_HEADER,
            region: EMPTY_REGION,
            name: SockAddrStorage::EMPTY,
            control: Control::new(),
        }
    }
}

/// A region of no bytes.
#[cfg(unix)]
const EMPTY_REGION: Region = Region {
    iov_base: ptr::null_mut(),
    iov_len: 0,
};
#[cfg(target_os = "windows")]
const EMPTY_REGION: Region = Region {
    len: 0,
    buf: ptr::null_mut(),
};

/// A message header with no address, no region, and no control message.
#[cfg(target_os = "windows")]
const EMPTY_HEADER: Header = Header {
    name: ptr::null_mut(),
    namelen: 0,
    buffers: ptr::null_mut(),
    buffer_count: 0,
    control: EMPTY_REGION,
    flags: 0,
};

/// A message header with no address, no region, and no control message.
#[cfg(unix)]
const EMPTY_HEADER: Header = Header {
    msg_name: ptr::null_mut(),
    msg_namelen: 0,
    msg_iov: ptr::null_mut(),
    msg_iovlen: 0,
    msg_control: ptr::null_mut(),
    msg_controllen: 0,
    msg_flags: 0,
};

#[cfg(target_os = "windows")]
impl Message {
    /// The header of a send of `datagram`.
    pub(super) fn send(&mut self, datagram: &Datagram) -> *const Header {
        let (name, name_len) = datagram
            .destination
            .map_or((SockAddrStorage::EMPTY, 0), SockAddrStorage::new);
        self.name = name;
        let contents = datagram.contents.bytes();
        // A `WSABUF` holds at most `u32::MAX` bytes, more than a datagram can have.
        self.region = Region {
            len: u32::try_from(contents.len()).unwrap_or(u32::MAX),
            buf: contents.as_ptr().cast_mut().cast(),
        };
        let control_len = self.control.encode(&datagram.transmit());
        self.header = Header {
            name: if name_len == 0 {
                ptr::null_mut()
            } else {
                (&raw mut self.name).cast()
            },
            namelen: c_int::try_from(name_len).unwrap_or(0),
            buffers: &raw mut self.region,
            buffer_count: 1,
            control: Region {
                len: u32::try_from(control_len).unwrap_or(0),
                buf: if control_len == 0 {
                    ptr::null_mut()
                } else {
                    self.control.as_mut_ptr().cast()
                },
            },
            flags: 0,
        };
        &raw const self.header
    }

    /// The header of a receive of at most `len` bytes at `region`, or into a buffer that the port
    /// selects, of at most `len` bytes, for a null `region`.
    pub(super) fn receive(&mut self, region: *mut u8, len: usize) -> *mut Header {
        self.name = SockAddrStorage::EMPTY;
        self.control = Control::new();
        self.region = Region {
            len: u32::try_from(len).unwrap_or(u32::MAX),
            buf: region.cast(),
        };
        self.header = Header {
            name: (&raw mut self.name).cast(),
            namelen: c_int::try_from(SockAddrStorage::LEN).unwrap_or(0),
            buffers: &raw mut self.region,
            buffer_count: 1,
            control: Region {
                len: u32::try_from(Control::LEN).unwrap_or(0),
                buf: self.control.as_mut_ptr().cast(),
            },
            flags: 0,
        };
        &raw mut self.header
    }

    /// The datagram of `len` bytes that the stack received with this header.
    pub(super) fn received(&self, len: usize) -> io::Result<Received> {
        Received::from_message(
            &self.name,
            u32::try_from(self.header.namelen).unwrap_or(0),
            self.header.flags,
            &self.control,
            usize::try_from(self.header.control.len).unwrap_or(0),
            len,
        )
    }
}

#[cfg(unix)]
impl Message {
    /// The header of a send of `datagram`.
    pub(super) fn send(&mut self, datagram: &Datagram) -> *const Header {
        let (name, name_len) = datagram
            .destination
            .map_or((SockAddrStorage::EMPTY, 0), SockAddrStorage::new);
        self.name = name;
        let contents = datagram.contents.bytes();
        self.region = Region {
            iov_base: contents.as_ptr().cast_mut().cast(),
            iov_len: contents.len(),
        };
        let control_len = self.control.encode(&datagram.transmit());
        self.header = Header {
            msg_name: if name_len == 0 {
                ptr::null_mut()
            } else {
                (&raw mut self.name).cast()
            },
            msg_namelen: name_len,
            msg_iov: &raw mut self.region,
            msg_iovlen: 1,
            msg_control: if control_len == 0 {
                ptr::null_mut()
            } else {
                self.control.as_mut_ptr().cast()
            },
            #[cfg(target_os = "linux")]
            msg_controllen: control_len,
            #[cfg(target_os = "macos")]
            msg_controllen: u32::try_from(control_len).unwrap_or(u32::MAX),
            msg_flags: 0,
        };
        &raw const self.header
    }

    /// The header of a receive of at most `len` bytes at `region`, or into a buffer that the
    /// kernel selects, of at most `len` bytes, for a null `region`.
    pub(super) fn receive(&mut self, region: *mut u8, len: usize) -> *mut Header {
        self.name = SockAddrStorage::EMPTY;
        self.control = Control::new();
        self.region = Region {
            iov_base: region.cast(),
            iov_len: len,
        };
        self.header = Header {
            msg_name: (&raw mut self.name).cast(),
            msg_namelen: SockAddrStorage::LEN,
            msg_iov: &raw mut self.region,
            msg_iovlen: 1,
            msg_control: self.control.as_mut_ptr().cast(),
            #[cfg(target_os = "linux")]
            msg_controllen: Control::LEN,
            #[cfg(target_os = "macos")]
            msg_controllen: u32::try_from(Control::LEN).unwrap_or(u32::MAX),
            msg_flags: 0,
        };
        &raw mut self.header
    }

    /// The datagram of `len` bytes that the kernel received with this header.
    pub(super) fn received(&self, len: usize) -> io::Result<Received> {
        Received::from_message(
            &self.name,
            self.header.msg_namelen,
            self.header.msg_flags,
            &self.control,
            #[cfg(target_os = "linux")]
            self.header.msg_controllen,
            #[cfg(target_os = "macos")]
            usize::try_from(self.header.msg_controllen).unwrap_or(usize::MAX),
            len,
        )
    }
}

/// The datagrams of a batch send, in order, and the messages that name them for the kernel. Make
/// it one time and use it again, so that each batch allocates nothing. A batch holds at most as
/// many datagrams as the submission queue has entries ([`Config::submissions`](super::Config::submissions)).
#[derive(Debug, Default)]
pub struct Datagrams {
    datagrams: Vec<Datagram>,
    messages: Vec<Message>,
}

impl Datagrams {
    /// No datagram, with room for `datagrams` of them.
    #[must_use]
    pub fn with_capacity(datagrams: usize) -> Self {
        Self {
            datagrams: Vec::with_capacity(datagrams),
            messages: (0..datagrams).map(|_| Message::default()).collect(),
        }
    }

    /// Adds `datagram` after the others. It allocates only when the batch has no room.
    pub fn push(&mut self, datagram: Datagram) {
        self.datagrams.push(datagram);
        if self.messages.len() < self.datagrams.len() {
            self.messages.push(Message::default());
        }
    }

    /// The datagrams, in order.
    #[must_use]
    pub fn datagrams(&self) -> &[Datagram] {
        &self.datagrams
    }

    /// The number of datagrams.
    #[must_use]
    pub fn len(&self) -> usize {
        self.datagrams.len()
    }

    /// Whether there is no datagram.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.datagrams.is_empty()
    }

    /// Takes away the first `sent` datagrams, for example the count of a batch that the kernel
    /// sent only in part, so that the next batch sends the rest.
    pub fn advance(&mut self, sent: usize) {
        self.datagrams.drain(..sent.min(self.datagrams.len()));
    }

    /// Takes away each datagram.
    pub fn clear(&mut self) {
        self.datagrams.clear();
    }

    /// The number of datagrams from the first that one batch sends. On macOS, which has no
    /// segmentation offload, a batch ends before its first datagram with a segment size, and a
    /// batch that starts with one sends none: the error is then the result of the batch.
    pub(super) fn sendable(&self) -> Result<usize, i32> {
        #[cfg(target_os = "macos")]
        match self
            .datagrams
            .iter()
            .position(|datagram| datagram.segment_size.is_some())
        {
            Some(0) => return Err(UNSEGMENTED),
            Some(first) => return Ok(first),
            None => {}
        }
        Ok(self.datagrams.len())
    }

    /// The header of the message of datagram `index`, for a submission.
    pub(super) fn message(&mut self, index: usize) -> Option<*const Header> {
        let datagram = self.datagrams.get(index)?;
        Some(self.messages.get_mut(index)?.send(datagram))
    }
}

/// The datagrams that a batch receive took, each in a buffer that the kernel selected, with its
/// metadata, and the messages that the receive gives to the kernel. Make it one time with room for
/// the most datagrams of one batch, and use it again. Its capacity is at most the entries of the
/// submission queue ([`Config::submissions`](super::Config::submissions)).
#[derive(Debug, Default)]
pub struct Arrivals {
    datagrams: Vec<(Backing, Received)>,
    messages: Vec<Message>,
}

impl Arrivals {
    /// Room for a batch of at most `datagrams` datagrams.
    #[must_use]
    pub fn with_capacity(datagrams: usize) -> Self {
        Self {
            datagrams: Vec::with_capacity(datagrams),
            messages: (0..datagrams).map(|_| Message::default()).collect(),
        }
    }

    /// The most datagrams that one batch takes.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.messages.len()
    }

    /// The datagrams that the last batch took, in the order of their arrival.
    #[must_use]
    pub fn datagrams(&self) -> &[(Backing, Received)] {
        &self.datagrams
    }

    /// The number of datagrams.
    #[must_use]
    pub fn len(&self) -> usize {
        self.datagrams.len()
    }

    /// Whether there is no datagram.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.datagrams.is_empty()
    }

    /// Takes the datagrams, in order. The buffers go back to their pool when they drop.
    pub fn drain(&mut self) -> alloc::vec::Drain<'_, (Backing, Received)> {
        self.datagrams.drain(..)
    }

    /// Takes the first datagram.
    pub(super) fn take_first(&mut self) -> Option<(Backing, Received)> {
        (!self.datagrams.is_empty()).then(|| self.datagrams.remove(0))
    }

    /// Adds a datagram, at most up to the capacity. Gives it back when there is no room.
    pub(super) fn add(&mut self, buffer: Backing, received: Received) -> Option<(Backing, Received)> {
        if self.datagrams.len() < self.capacity() {
            self.datagrams.push((buffer, received));
            None
        } else {
            Some((buffer, received))
        }
    }

    /// The header of the message of entry `index`, for a receive into a buffer of at most `len`
    /// bytes that the kernel selects.
    pub(super) fn message(&mut self, index: usize, len: usize) -> Option<*mut Header> {
        Some(self.messages.get_mut(index)?.receive(ptr::null_mut(), len))
    }

    /// The datagram of `len` bytes that entry `index` received.
    pub(super) fn received(&self, index: usize, len: usize) -> Option<io::Result<Received>> {
        Some(self.messages.get(index)?.received(len))
    }
}

/// The batch of an operation: the datagrams of a batch send, or the arrivals of a batch receive.
#[derive(Debug)]
pub(crate) enum Batch {
    Send(Datagrams),
    Receive(Arrivals),
}

/// What a batch has, while its completions come.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Progress {
    /// The completions to come.
    pub(super) remaining: u32,
    /// The completions that came.
    pub(super) done: u32,
    /// The datagrams that the batch sent or took, from the first.
    pub(super) count: u32,
    /// The error of the first entry that failed, when no datagram went or came.
    pub(super) error: Option<i32>,
}

/// A completion of the batch whose resources are `held`: its entries complete in order, because
/// each one is linked to the next. Records the datagram of the completion, and gives whether more
/// completions come and the result of the batch so far: the count of the datagrams that went or
/// came, from the first, or the error of the first entry when none did.
#[expect(unsafe_code, reason = "the kernel initialized the bytes that the result counts")]
pub(super) fn batch_completion(held: &mut Held, result: i32, buffer: Option<Backing>) -> (bool, i32) {
    // A batch always has its datagrams.
    let Payload::Batch { batch, progress } = &mut held.payload else {
        return (false, result);
    };
    let index = progress.done;
    progress.done = progress.done.saturating_add(1);
    progress.remaining = progress.remaining.saturating_sub(1);
    // Only an entry whose entries before it each succeeded adds a datagram. An entry after a
    // failure, for example `EAGAIN` when no more datagrams wait, or `ECANCELED` after a failed
    // entry, ends the batch with no error.
    if progress.count == index {
        match (usize::try_from(result), batch) {
            (Ok(_), Batch::Send(_)) => progress.count = progress.count.saturating_add(1),
            (Ok(len), Batch::Receive(arrivals)) => {
                // A datagram of no bytes can come with no selected buffer.
                let buffer = buffer.unwrap_or_else(|| Backing::from(Vec::new()));
                if let Some(Ok(received)) = arrivals.received(index as usize, len) {
                    let mut loan = WriteLoan::new(buffer);
                    // SAFETY: a slot of the pool has no initialized bytes, so the loan starts at the
                    // start of the buffer, where the kernel wrote the bytes that the result counts,
                    // at most the slot capacity that the message names.
                    unsafe { loan.set_filled(len) };
                    if arrivals.add(loan.into_inner(), received).is_none() {
                        progress.count = progress.count.saturating_add(1);
                    }
                }
            }
            (Err(_), _) if index == 0 => progress.error = Some(result),
            _ => {}
        }
    }
    let result = match progress.error {
        Some(error) if progress.count == 0 => error,
        _ => i32::try_from(progress.count).unwrap_or(i32::MAX),
    };
    (progress.remaining > 0, result)
}

impl Driver {
    /// Binds a UDP socket to `address` with `options`. The future gives the socket and its local
    /// address. The bind runs on the ring where the kernel has the bind operation, as for
    /// [`Driver::bind`].
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system, or `OutOfMemory` when each resource slot of the driver is in use.
    pub fn bind_udp(
        &mut self,
        address: SocketAddr,
        options: UdpOptions,
    ) -> Result<Operation<'static, io::Result<UdpSocket>>, Refused<()>> {
        let completion = self.bind_socket(SockAddrStorage::new(address), false, |socket| options.apply(socket))?;
        Ok(Operation::new(completion, |result, mut held| {
            Some(super::tcp::count(result).and_then(|_| {
                let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
                let local = held
                    .payload
                    .peer()
                    .and_then(|(peer, len)| peer.to_socket_addr(*len))
                    .ok_or(io::ErrorKind::InvalidData)?;
                Ok(UdpSocket {
                    read: ReadHalf { resource },
                    write: WriteHalf { resource },
                    local,
                })
            }))
        }))
    }

    /// Sends `datagram`. The future gives the number of datagrams that went, all of them or none,
    /// and the datagram. The kernel reads its bytes in place. A refused send gives an error with
    /// `datagram::Refused`: `TooLarge` above the path MTU with the don't-fragment option,
    /// or `NoSegmentation` when the path cannot segment it; then send its segments with
    /// [`Driver::send_batch`].
    ///
    /// # Errors
    ///
    /// A refusal, with the datagram, when the driver has no room for the operation.
    pub fn send_to<'a, W: AsMut<WriteHalf>>(
        &mut self,
        writer: &'a mut W,
        datagram: Datagram,
    ) -> Result<Operation<'a, (io::Result<usize>, Datagram)>, Refused<Datagram>> {
        let held = |datagram| Held {
            payload: Payload::SendTo {
                message: Message::default(),
                datagram,
            },
            ..Held::default()
        };
        let completion = self.begin(writer.as_mut().resource, Kind::SendTo, datagram, held)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::SendTo { datagram, .. } = held.payload else {
                return None;
            };
            let sent = super::tcp::count(result)
                .map(|_| datagram.datagrams())
                .map_err(|error| rsnetwork_udp::Refused::of_send(error, datagram.segment_size.is_some()));
            Some((sent, datagram))
        }))
    }

    /// Sends the datagrams of `datagrams`, each with its own metadata, in one kernel entry: one
    /// linked submission for each, in order. A datagram that fails ends the batch, and the datagrams
    /// after it do not go. The future gives the number of datagrams that went, from the first, and
    /// the batch: [`Datagrams::advance`] then keeps the rest, which never joins another datagram.
    /// On macOS, a batch ends before its first datagram with a segment size, and a batch that
    /// starts with one gives `datagram::Refused::NoSegmentation`.
    ///
    /// # Errors
    ///
    /// A refusal, with the batch, when the driver has no room for the operation or its queue has
    /// no room for each datagram. A batch of more datagrams than the queue has entries always
    /// refuses with [`Reason::Queue`].
    pub fn send_batch<'a, W: AsMut<WriteHalf>>(
        &mut self,
        writer: &'a mut W,
        datagrams: Datagrams,
    ) -> Result<Operation<'a, (io::Result<usize>, Datagrams)>, Refused<Datagrams>> {
        let entries = u32::try_from(datagrams.sendable().unwrap_or(0)).unwrap_or(u32::MAX);
        let held = |datagrams| Held {
            payload: Payload::Batch {
                batch: Batch::Send(datagrams),
                progress: Progress {
                    remaining: entries,
                    ..Progress::default()
                },
            },
            ..Held::default()
        };
        let completion = self.begin_entries(writer.as_mut().resource, Kind::SendBatch, entries, datagrams, held)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::Batch {
                batch: Batch::Send(datagrams),
                ..
            } = held.payload
            else {
                return None;
            };
            // A batch that sent no datagram gives the error of its first one.
            let segmented = datagrams
                .datagrams()
                .first()
                .is_some_and(|datagram| datagram.segment_size.is_some());
            let sent = super::tcp::count(result).map_err(|error| rsnetwork_udp::Refused::of_send(error, segmented));
            Some((sent, datagrams))
        }))
    }

    /// Receives one datagram into the capacity of `buffer` after its initialized bytes. The future
    /// gives the datagram's facts, with `truncated` when it did not fit, and the buffer. With the
    /// `gro` option, one receive can give several coalesced datagrams with their segment size.
    ///
    /// # Errors
    ///
    /// A refusal, with the buffer, when the driver has no room for the operation.
    pub fn recv_from<'a, R: AsMut<ReadHalf>>(
        &mut self,
        reader: &'a mut R,
        buffer: Backing,
    ) -> Result<Operation<'a, (io::Result<Received>, Backing)>, Refused<Backing>> {
        let held = |buffer| Held {
            payload: Payload::Receive {
                message: Message::default(),
                received: None,
            },
            loan: Some(WriteLoan::new(buffer)),
            ..Held::default()
        };
        let completion = self.begin(reader.as_mut().resource, Kind::RecvFrom, buffer, held)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::Receive { received: facts, .. } = held.payload else {
                return None;
            };
            Some((received(result, facts), held.loan?.into_inner()))
        }))
    }

    /// Receives one datagram into a buffer that the kernel selects from the buffers of
    /// [`Driver::provide`] when it comes, so the wait holds no buffer. The future gives the
    /// datagram's facts and that buffer, which goes back to the kernel when it drops.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no provided buffers or no room for the operation.
    pub fn recv_selected_from<'a, R: AsMut<ReadHalf>>(
        &mut self,
        reader: &'a mut R,
    ) -> Result<Operation<'a, io::Result<(Received, Backing)>>, Refused<()>> {
        if self.selected.is_none() {
            return Err(Refused {
                reason: Reason::Buffers,
                value: (),
            });
        }
        let held = |()| Held {
            payload: Payload::Receive {
                message: Message::default(),
                received: None,
            },
            ..Held::default()
        };
        let completion = self.begin(reader.as_mut().resource, Kind::RecvSelectedFrom, (), held)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::Receive { received: facts, .. } = held.payload else {
                return None;
            };
            let buffer = held
                .loan
                .map_or_else(|| Backing::from(Vec::new()), WriteLoan::into_inner);
            Some(received(result, facts).map(|received| (received, buffer)))
        }))
    }

    /// Receives up to `arrivals.capacity()` datagrams in one kernel entry, each into a buffer that
    /// the kernel selects from the buffers of [`Driver::provide`], with its metadata: the first
    /// receive waits for a datagram, and the next ones take the datagrams that wait, with no wait.
    /// So the wait holds no buffer. The future gives the number of datagrams, and `arrivals` with
    /// them.
    ///
    /// # Errors
    ///
    /// A refusal, with `arrivals`, when the driver has no provided buffers, no room for the
    /// operation, or no room in its queue for each receive. An `arrivals` with a capacity above the
    /// entries of the queue always refuses with [`Reason::Queue`].
    pub fn recv_batch<'a, R: AsMut<ReadHalf>>(
        &mut self,
        reader: &'a mut R,
        mut arrivals: Arrivals,
    ) -> Result<Operation<'a, (io::Result<usize>, Arrivals)>, Refused<Arrivals>> {
        if self.selected.is_none() {
            return Err(Refused {
                reason: Reason::Buffers,
                value: arrivals,
            });
        }
        arrivals.datagrams.clear();
        let entries = u32::try_from(arrivals.capacity()).unwrap_or(u32::MAX);
        let held = |arrivals| Held {
            payload: Payload::Batch {
                batch: Batch::Receive(arrivals),
                progress: Progress {
                    remaining: entries,
                    ..Progress::default()
                },
            },
            ..Held::default()
        };
        let completion = self.begin_entries(reader.as_mut().resource, Kind::RecvBatch, entries, arrivals, held)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::Batch {
                batch: Batch::Receive(arrivals),
                ..
            } = held.payload
            else {
                return None;
            };
            Some((super::tcp::count(result), arrivals))
        }))
    }

    /// Closes `socket`, as [`Driver::close`] closes a stream: it cancels each operation, waits
    /// until each one ends, and releases the datagrams that no receive took.
    ///
    /// # Errors
    ///
    /// A refusal, with the socket, when the driver has no room for the operation.
    pub fn close_udp(&mut self, socket: UdpSocket) -> Result<Operation<'static, super::Closed>, Refused<UdpSocket>> {
        let resource: Resource = socket.read.resource;
        let completion: Completion = self.close_resource(resource, socket)?;
        Ok(Operation::new(completion, |_, held| held.closed))
    }
}

/// The result of a datagram receive: its facts, or the error of the kernel.
fn received(result: i32, received: Option<Received>) -> io::Result<Received> {
    super::tcp::count(result).and_then(|_| received.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData)))
}

/// A kept datagram of `buffer` with `received`, copied into `loan`: as many bytes as fit, and
/// `truncated` when it did not fit, as the kernel does. Gives the facts of the copy and the bytes
/// that it copied.
pub(super) fn copy_kept(buffer: &Backing, received: Received, loan: &mut WriteLoan<Backing>) -> (Received, usize) {
    let copied = loan.fill_from(buffer.bytes());
    let facts = Received {
        len: copied,
        truncated: received.truncated || copied < buffer.initialized(),
        ..received
    };
    (facts, copied)
}

/// Keeps the datagrams of a batch whose future dropped, for the next receives.
pub(super) fn keep_arrivals(arrivals: Arrivals) -> Option<Arrivals> {
    (!arrivals.is_empty()).then_some(arrivals)
}
