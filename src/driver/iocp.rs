//! The IOCP backend: a completion port that takes the submission entries of the driver, as a ring
//! does, and runs each one as one overlapped call. It posts the completion that a ring posts for
//! the same entry, so the operation table, the stream rules, close, and teardown run in the same
//! way on each backend.
//!
//! The driver associates each socket with the port at its adoption ([`Registrar`]). Each entry in
//! flight has a slot, and the `OVERLAPPED` of each slot is in an array that the port allocates one
//! time and never moves, so the kernel can write the status of a call at any time until its packet.
//!
//! # The terminal event
//!
//! Before each call, the port sets the NT status of the `OVERLAPPED` to `STATUS_PENDING`. The Win32
//! return separates three cases, and the NT status decides the third ([`terminal`]):
//!
//! - The call returned pending: the packet is the terminal event. The status can already hold the
//!   final status of a call that ended after it returned, and its packet still comes.
//! - The call succeeded at once: the packet is the terminal event. On a socket that skips the
//!   packets of calls that end at once, no packet comes, and the return is the terminal event. The
//!   port does not read the status here, because it can still show `STATUS_PENDING`.
//! - The call failed at once: the port reads the status. A warning, such as
//!   `STATUS_BUFFER_OVERFLOW`, which Win32 reports as a failure, means that a packet comes: it is
//!   the terminal event. On a socket that skips, the port takes the return as terminal, as the
//!   design states, but no run shows yet whether a packet still comes there. No TCP operation
//!   gives a warning status. An error status (`NT_ERROR`), or
//!   a status that is still `STATUS_PENDING` because the call did not reach the I/O manager, means
//!   that no packet comes, and the return is the terminal event.
//!
//! A cancel calls `CancelIoEx` with the `OVERLAPPED` of its target, and posts its own result at
//! once. The result is no terminal event: the target keeps its slot, and the driver keeps its
//! buffer, until the packet of the target, whatever status the packet gives.
//!
//! # Skip-on-success
//!
//! A socket skips the packet of a call that ends at once (`FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`)
//! only when its provider gives handles of the installable file system (`XP1_IFS_HANDLES`) and no
//! layered provider is above it (`SIO_BASE_HANDLE` gives the same socket). The driver makes each of
//! its sockets itself, with no inheritance (`WSA_FLAG_NO_HANDLE_INHERIT`), and no API of the driver
//! duplicates one, so no other handle of a skipping socket exists.
//!
//! # Operations
//!
//! A receive is one `WSARecv`, a send one `WSASend`, and a vectored send one `WSASend` of its
//! `WSABUF` array. A receive into a selected buffer is a receive of zero bytes, which waits for
//! data and holds no buffer; at its packet, one nonblocking `WSARecv` reads into the buffer at the
//! head of the provided ring. An accept is `AcceptEx` with a socket that the port makes, and a
//! connect is `ConnectEx` on a socket that the port binds to the wildcard address first. A
//! half-close is one `shutdown`, which ends at its return.

pub(crate) mod sys;

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cell::UnsafeCell;
use core::ffi::{c_char, c_int};
use core::ptr::{self, NonNull};
use core::sync::atomic::{AtomicBool, Ordering};
use std::io;
use std::os::windows::io::{
    AsRawHandle as _, AsRawSocket as _, AsSocket, BorrowedSocket, FromRawHandle as _, FromRawSocket as _,
    IntoRawSocket as _, OwnedHandle, OwnedSocket, RawSocket,
};
use std::sync::OnceLock;

use rsnetwork_udp::{AF_INET, AF_INET6, SaFamily, SockAddrStorage};
use sys::{Overlapped, OverlappedEntry};

use crate::uring::sys as ring;
use crate::uring::{Cqe, Group, Sqe};

/// The key of the association of each socket.
const SOCKET: usize = 0;

/// The key of the packet of a wake.
const WAKE: usize = 1;

/// The packets that one wait removes at most.
const EVENTS: usize = 128;

/// The room for one address in the output of `AcceptEx`: the largest address of the family,
/// `SOCKADDR_IN6`, and 16 bytes.
const ACCEPT_ADDRESS: u32 = 28 + 16;

const _: () = assert!(2 * ACCEPT_ADDRESS as usize <= size_of::<SockAddrStorage>());

/// The facts of the socket of an entry that the port needs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Token {
    /// The socket queues no packet for a call that ends at once.
    pub(crate) skips: bool,
}

/// The socket handle of the number `number` of an entry: the number extended with its sign.
pub(crate) const fn socket(number: i32) -> RawSocket {
    (number as i64).cast_unsigned()
}

/// The port handle and the state of its wake, which the port and the wake handles share.
#[derive(Debug)]
struct Shared {
    port: OwnedHandle,
    /// A wake packet waits in the port.
    posted: AtomicBool,
}

impl Shared {
    fn handle(&self) -> sys::Handle {
        self.port.as_raw_handle()
    }
}

/// The wake of a port, from any thread: a packet that ends a wait. At most one waits in the port.
#[derive(Debug)]
pub(crate) struct Wake(Arc<Shared>);

impl Wake {
    /// Posts the wake packet, unless one waits already.
    #[expect(unsafe_code, reason = "a posted packet names no memory")]
    pub(crate) fn post(&self) {
        if self.0.posted.swap(true, Ordering::AcqRel) {
            return;
        }
        // SAFETY: the port is open while `self` lives, and the packet names no overlapped
        // structure.
        if unsafe { sys::PostQueuedCompletionStatus(self.0.handle(), 0, WAKE, ptr::null_mut()) } == 0 {
            // No packet waits, so the next wake posts one.
            self.0.posted.store(false, Ordering::Release);
        }
    }
}

/// The associations of the sockets of a driver with its port.
#[derive(Debug)]
pub(crate) struct Registrar {
    port: Arc<Shared>,
    /// The test seam of a failed association: the next association fails with this error.
    #[cfg(test)]
    pub(crate) fail: core::cell::Cell<Option<i32>>,
    /// The test seam of the packets: no socket skips them.
    #[cfg(test)]
    pub(crate) packets: core::cell::Cell<bool>,
}

impl Registrar {
    /// Associates `socket` with the port for its life, and turns on skip-on-success where its
    /// provider allows it. Gives whether the socket skips.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for a socket handle with significant bits above the low 32, which no entry can
    /// hold, or the error of `CreateIoCompletionPort`. Nothing changed then.
    #[expect(unsafe_code, reason = "the calls take the open socket and the open port")]
    pub(crate) fn add(&self, socket: &OwnedSocket) -> io::Result<bool> {
        let raw = socket.as_raw_socket();
        if self::socket(super::number(socket.as_socket())) != raw {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        // The test seam gives the error of the association in place of the call.
        #[cfg(test)]
        if let Some(error) = self.fail.take() {
            return Err(io::Error::from_raw_os_error(error));
        }
        // SAFETY: the socket and the port are open handles during the call.
        if unsafe { sys::CreateIoCompletionPort(handle(raw), self.port.handle(), SOCKET, 0) }.is_null() {
            return Err(io::Error::last_os_error());
        }
        #[cfg(test)]
        if self.packets.get() {
            return Ok(false);
        }
        let flags = sys::FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | sys::FILE_SKIP_SET_EVENT_ON_HANDLE;
        // SAFETY: the socket is an open handle during the call.
        Ok(
            installed(socket.as_socket())
                && unsafe { sys::SetFileCompletionNotificationModes(handle(raw), flags) } != 0,
        )
    }
}

/// Whether the provider of `socket` gives handles that the I/O manager completes, with no layered
/// provider above it: the installed-provider check of skip-on-success.
#[expect(unsafe_code, reason = "the calls write into storage that lives during the call")]
fn installed(socket: BorrowedSocket<'_>) -> bool {
    let raw = socket.as_raw_socket();
    let mut info = sys::WsaProtocolInfoW {
        service_flags1: 0,
        rest: [0; 156],
    };
    let mut len = c_int::try_from(size_of::<sys::WsaProtocolInfoW>()).unwrap_or(0);
    // SAFETY: the option storage and its length live during the call, and the stack writes at most
    // `len` bytes.
    let read = unsafe {
        sys::getsockopt(
            raw,
            sys::SOL_SOCKET,
            sys::SO_PROTOCOL_INFOW,
            ptr::from_mut(&mut info).cast::<c_char>(),
            &raw mut len,
        )
    };
    if read == sys::SOCKET_ERROR || info.service_flags1 & sys::XP1_IFS_HANDLES == 0 {
        return false;
    }
    let (mut base, mut returned): (sys::Socket, u32) = (sys::INVALID_SOCKET, 0);
    // SAFETY: the output storage lives during the call, and the stack writes at most one socket.
    let asked = unsafe {
        sys::WSAIoctl(
            raw,
            sys::SIO_BASE_HANDLE,
            ptr::null(),
            0,
            ptr::from_mut(&mut base).cast(),
            u32_len(size_of::<sys::Socket>()),
            &raw mut returned,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    asked != sys::SOCKET_ERROR && base == raw
}

/// The submission queue is full.
#[derive(Debug)]
pub(crate) struct Full;

/// An entry that the port took and that has not completed.
#[derive(Debug)]
struct Slot {
    sqe: Sqe,
    skips: bool,
    /// The call of the entry runs, and its packet ends it.
    queued: bool,
    /// The socket that an accept makes, which the port owns until the completion of the accept.
    accepted: Option<OwnedSocket>,
}

/// How a call returned: it runs, it ended with this count of bytes, or it failed with this error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Returned {
    Pending,
    Done(u32),
    Failed(c_int),
}

/// The terminal event of a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Terminal {
    /// The packet of the call.
    Packet,
    /// The return of the call: no packet comes.
    Return,
}

/// The terminal event of a call that returned `returned`, on a socket that `skips` packets or not.
/// `status` is the NT status in its `OVERLAPPED` after a failed return. The Win32 return separates
/// three cases, and the NT status decides the third:
///
/// - The call runs: its packet comes. A call that runs can already have its final status, also an
///   error, and its packet still comes.
/// - The call succeeded at once: a packet comes, unless the socket skips it. The status is not read
///   here, because it can still show `STATUS_PENDING` at the return.
/// - The call failed at once: a warning status, such as `STATUS_BUFFER_OVERFLOW`, means that a
///   packet comes. On a socket that skips, the port takes the return as terminal, as the design
///   states; no run shows yet whether a packet still comes there. An error status, or a status that is still
///   `STATUS_PENDING` because the call did not reach the I/O manager, means that no packet comes.
const fn terminal(returned: Returned, status: u32, skips: bool) -> Terminal {
    match returned {
        Returned::Pending => Terminal::Packet,
        Returned::Failed(_) if status == sys::STATUS_PENDING || sys::nt_error(status) => Terminal::Return,
        Returned::Done(_) | Returned::Failed(_) if skips => Terminal::Return,
        Returned::Done(_) | Returned::Failed(_) => Terminal::Packet,
    }
}

/// A completion port of one driver.
pub(crate) struct Port {
    shared: Arc<Shared>,
    /// The entries that the driver pushed, with their sockets, which the next submit takes.
    queue: VecDeque<(Sqe, Option<Token>)>,
    entries: u32,
    /// The `OVERLAPPED` of each slot. The kernel writes it until the packet of its call, so the
    /// array never moves, and it leaks when the port drops before each packet came.
    overlapped: Box<[UnsafeCell<Overlapped>]>,
    slots: Vec<Option<Slot>>,
    free: Vec<u32>,
    completions: VecDeque<Cqe>,
    group: Option<Group>,
    events: Vec<OverlappedEntry>,
    /// The last wait filled the event array, so packets can remain.
    full: bool,
}

impl Port {
    /// A port with `entries` submission entries, rounded up to a power of two as a ring does, and
    /// room for `completions` entries in flight.
    ///
    /// # Errors
    ///
    /// The error of `WSAStartup` or of `CreateIoCompletionPort`, or `InvalidInput` for 0 entries.
    #[expect(unsafe_code, reason = "the call returns a new handle")]
    pub(crate) fn new(entries: u32, completions: u32) -> io::Result<Self> {
        let entries = entries
            .checked_next_power_of_two()
            .filter(|&entries| entries > 0)
            .ok_or(io::ErrorKind::InvalidInput)?;
        startup()?;
        // SAFETY: with no file handle, the call makes a new port and names no memory. One thread,
        // the thread of the driver, takes its packets.
        let port = unsafe {
            sys::CreateIoCompletionPort(
                ptr::without_provenance_mut(sys::INVALID_HANDLE_VALUE),
                ptr::null_mut(),
                0,
                1,
            )
        };
        if port.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the call gave a new handle that nothing else owns.
        let port = unsafe { OwnedHandle::from_raw_handle(port) };
        let completions = completions.max(entries);
        Ok(Self {
            shared: Arc::new(Shared {
                port,
                posted: AtomicBool::new(false),
            }),
            queue: VecDeque::with_capacity(entries as usize),
            entries,
            overlapped: (0..completions).map(|_| UnsafeCell::new(idle())).collect(),
            slots: (0..completions).map(|_| None).collect(),
            free: (0..completions).rev().collect(),
            completions: VecDeque::with_capacity(completions as usize),
            group: None,
            events: alloc::vec![
                OverlappedEntry {
                    key: 0,
                    overlapped: ptr::null_mut(),
                    internal: 0,
                    bytes: 0,
                };
                EVENTS
            ],
            full: false,
        })
    }

    /// The wake of this port.
    pub(crate) fn wake(&self) -> Wake {
        Wake(Arc::clone(&self.shared))
    }

    /// The associations of the sockets with this port.
    pub(crate) fn registrar(&self) -> Registrar {
        Registrar {
            port: Arc::clone(&self.shared),
            #[cfg(test)]
            fail: core::cell::Cell::new(None),
            #[cfg(test)]
            packets: core::cell::Cell::new(false),
        }
    }

    /// Copies `entry` of the socket of `token` to the submission queue. The next submit takes it.
    pub(crate) fn push(&mut self, entry: &Sqe, token: Option<Token>) -> Result<(), Full> {
        if self.queue.len() >= self.entries as usize {
            return Err(Full);
        }
        self.queue.push_back((*entry, token));
        Ok(())
    }

    /// The entries of the submission queue.
    pub(crate) const fn entries(&self) -> u32 {
        self.entries
    }

    /// The entries that the submission queue has room for.
    pub(crate) fn room(&self) -> u32 {
        self.entries
            .saturating_sub(u32::try_from(self.queue.len()).unwrap_or(u32::MAX))
    }

    /// The pushed entries that the port has not taken.
    pub(crate) fn unsubmitted(&self) -> u32 {
        u32::try_from(self.queue.len()).unwrap_or(u32::MAX)
    }

    /// Whether a completion is ready.
    pub(crate) fn ready(&self) -> bool {
        !self.completions.is_empty()
    }

    /// Whether packets can remain after the last wait, because it filled its array.
    pub(crate) const fn more(&self) -> bool {
        self.full
    }

    /// The ready completions, which stay for the driver.
    #[cfg(test)]
    pub(crate) fn peek(&self) -> Vec<Cqe> {
        self.completions.iter().copied().collect()
    }

    /// Takes the ready completions, in order.
    pub(crate) fn completions(&mut self) -> Completions<'_> {
        Completions(&mut self.completions)
    }

    /// Takes the buffer ring at `base` with `entries` entries, a power of two, as the buffers of
    /// the selecting receives. The memory stays until [`Port::release_group`].
    pub(crate) fn provide_group(&mut self, base: NonNull<u8>, entries: u16) {
        self.group = Some(Group::new(base, entries));
    }

    /// Ends the use of the buffer ring of [`Port::provide_group`].
    pub(crate) fn release_group(&mut self) {
        self.group = None;
    }

    /// Takes the entries of the submission queue and starts each one, takes the packets, and posts
    /// the completions. With `wait` above 0, it waits until a completion is ready or a wake came.
    /// Gives the number of entries that it took.
    ///
    /// # Errors
    ///
    /// An error of `GetQueuedCompletionStatusEx` other than the end of its time.
    pub(crate) fn submit(&mut self, wait: u32) -> io::Result<u32> {
        let taken = self.take();
        let mut woke = self.poll(0)?;
        while wait > 0 && !woke && self.completions.len() < wait as usize {
            woke = self.poll(sys::INFINITE)?;
        }
        Ok(taken)
    }

    /// Takes each entry of the submission queue that has room in flight, and starts it.
    fn take(&mut self) -> u32 {
        let mut taken = 0;
        // Each entry in flight posts at most one completion, so the completion queue keeps its
        // bound.
        while !self.queue.is_empty() && self.free.len() > self.completions.len() {
            let (Some((sqe, token)), Some(index)) = (self.queue.pop_front(), self.free.pop()) else {
                break;
            };
            self.slots[index as usize] = Some(Slot {
                sqe,
                skips: token.is_some_and(|token| token.skips),
                queued: false,
                accepted: None,
            });
            taken += 1;
            self.start(index);
        }
        taken
    }

    /// Starts the entry of `index`.
    #[expect(unsafe_code, reason = "a shutdown names no memory")]
    fn start(&mut self, index: u32) {
        let Some(sqe) = self.slots[index as usize].as_ref().map(|slot| slot.sqe) else {
            return;
        };
        match sqe.opcode {
            ring::IORING_OP_NOP => self.post(index, 0, 0),
            ring::IORING_OP_ASYNC_CANCEL => {
                let result = self.cancel(sqe.addr);
                self.post(index, result, 0);
            }
            ring::IORING_OP_SHUTDOWN => {
                // SAFETY: the call takes the open socket of the entry and no memory.
                let shut = unsafe { sys::shutdown(socket(sqe.fd), sqe.len.cast_signed()) };
                let result = if shut == sys::SOCKET_ERROR { last_error() } else { 0 };
                self.post(index, result, 0);
            }
            ring::IORING_OP_RECV
            | ring::IORING_OP_SEND
            | ring::IORING_OP_WRITEV
            | ring::IORING_OP_ACCEPT
            | ring::IORING_OP_CONNECT => self.call(index),
            // The driver submits no other entry on this backend.
            _ => self.post(index, -sys::WSAEINVAL, 0),
        }
    }

    /// Runs the overlapped call of the entry of `index`, and ends it at its return or waits for its
    /// packet, by [`terminal`].
    #[expect(
        unsafe_code,
        reason = "no call of the slot runs, so the kernel does not use its OVERLAPPED"
    )]
    fn call(&mut self, index: u32) {
        let overlapped = self.overlapped[index as usize].get();
        let Some(slot) = self.slots[index as usize].as_mut() else {
            return;
        };
        // SAFETY: no call of the slot runs, so nothing else reads or writes its `OVERLAPPED`.
        unsafe { overlapped.write(idle()) };
        let returned = match slot.sqe.opcode {
            ring::IORING_OP_RECV if slot.sqe.flags & ring::IOSQE_BUFFER_SELECT != 0 => {
                // SAFETY: a receive of no bytes names no memory but its `OVERLAPPED`.
                unsafe { receive(slot.sqe.fd, ptr::null_mut(), 0, overlapped) }
            }
            // SAFETY: the driver keeps the buffer of the entry until its completion.
            ring::IORING_OP_RECV => unsafe { receive(slot.sqe.fd, pointer(slot.sqe.addr), slot.sqe.len, overlapped) },
            ring::IORING_OP_SEND => {
                let region = sys::WsaBuf {
                    len: slot.sqe.len,
                    buf: pointer(slot.sqe.addr),
                };
                // SAFETY: the driver keeps the lease of the entry until its completion, and the
                // call copies the `WSABUF` before it returns.
                unsafe { send(slot.sqe.fd, &raw const region, 1, overlapped) }
            }
            // SAFETY: the driver keeps the regions and their `WSABUF` array until the completion.
            ring::IORING_OP_WRITEV => unsafe {
                send(
                    slot.sqe.fd,
                    pointer::<sys::WsaBuf>(slot.sqe.addr),
                    slot.sqe.len,
                    overlapped,
                )
            },
            // SAFETY: the driver keeps the address storage of the entry until its completion.
            ring::IORING_OP_ACCEPT => unsafe { accept(slot, overlapped) },
            // SAFETY: the driver keeps the address of the entry until its completion.
            ring::IORING_OP_CONNECT => unsafe { connect(&slot.sqe, overlapped) },
            _ => Returned::Failed(sys::WSAEINVAL),
        };
        let status = if let Returned::Failed(_) = returned {
            // SAFETY: the call failed at once, so the kernel wrote its status before the return, or
            // the call never reached the kernel.
            unsafe { status(overlapped) }
        } else {
            sys::STATUS_PENDING
        };
        if terminal(returned, status, slot.skips) == Terminal::Packet {
            slot.queued = true;
            return;
        }
        match returned {
            Returned::Done(bytes) => self.ended(index, Ok(bytes)),
            Returned::Failed(error) => self.ended(index, Err(error)),
            Returned::Pending => {}
        }
    }

    /// Waits up to `timeout` milliseconds for packets, and ends the calls of their slots. Gives
    /// whether a wake came.
    #[expect(unsafe_code, reason = "the kernel writes the event array")]
    fn poll(&mut self, timeout: u32) -> io::Result<bool> {
        let capacity = u32::try_from(self.events.len()).unwrap_or(u32::MAX);
        let mut removed = 0;
        // SAFETY: the array has `capacity` entries, and the kernel writes at most that many.
        let taken = unsafe {
            sys::GetQueuedCompletionStatusEx(
                self.shared.handle(),
                self.events.as_mut_ptr(),
                capacity,
                &raw mut removed,
                timeout,
                0,
            )
        };
        if taken == 0 {
            self.full = false;
            let error = io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(sys::WAIT_TIMEOUT) => Ok(false),
                _ => Err(error),
            };
        }
        self.full = removed == capacity;
        let mut woke = false;
        for event in 0..removed as usize {
            let OverlappedEntry {
                key, overlapped, bytes, ..
            } = self.events[event];
            if key == WAKE {
                // A wake that comes after this store posts a new packet.
                self.shared.posted.store(false, Ordering::Release);
                woke = true;
                continue;
            }
            let Some(index) = self.index_of(overlapped) else {
                continue;
            };
            let Some(slot) = self.slots[index as usize].as_mut().filter(|slot| slot.queued) else {
                continue;
            };
            slot.queued = false;
            let (fd, opcode) = (slot.sqe.fd, slot.sqe.opcode);
            // SAFETY: the packet of the call came, so the kernel wrote its status and writes no more.
            let status = unsafe { status(overlapped) };
            // A send that failed after it transferred bytes gives their count, so the driver reports
            // the exact progress: a dropped send fails its direction. A cancel of a send that runs
            // aborts the connection, and its packet gives no count, also when bytes went, so the
            // send gives the abort and no count.
            let sends = matches!(opcode, ring::IORING_OP_SEND | ring::IORING_OP_WRITEV);
            let result = if !sys::nt_error(status) || (sends && bytes > 0) {
                Ok(bytes)
            } else {
                match failure(fd, overlapped) {
                    super::ECANCELED if sends => Err(sys::WSAECONNABORTED),
                    error => Err(error),
                }
            };
            self.ended(index, result);
        }
        Ok(woke)
    }

    /// The slot whose `OVERLAPPED` is at `overlapped`.
    fn index_of(&self, overlapped: *mut Overlapped) -> Option<u32> {
        let offset = overlapped.addr().checked_sub(self.overlapped.as_ptr().addr())?;
        let index = offset / size_of::<Overlapped>();
        (offset % size_of::<Overlapped>() == 0 && index < self.overlapped.len())
            .then(|| u32::try_from(index).ok())
            .flatten()
    }

    /// The call of the entry of `index` ended with `result`, the transferred bytes or a Winsock
    /// error, at its terminal event. Posts its completion, or starts the next step of a selecting
    /// receive, an accept, or a connect.
    #[expect(unsafe_code, reason = "the calls take the sockets and the storage of the entry")]
    fn ended(&mut self, index: u32, result: Result<u32, c_int>) {
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => {
                self.post(index, error.saturating_neg(), 0);
                return;
            }
        };
        let Some((sqe, accepted)) = self.slots[index as usize]
            .as_mut()
            .map(|slot| (slot.sqe, slot.accepted.take()))
        else {
            return;
        };
        match sqe.opcode {
            ring::IORING_OP_RECV if sqe.flags & ring::IOSQE_BUFFER_SELECT != 0 => self.select(index),
            ring::IORING_OP_ACCEPT => {
                let Some(accepted) = accepted else {
                    self.post(index, -sys::WSAEINVAL, 0);
                    return;
                };
                // SAFETY: the address storage of the entry lives until its completion.
                let result = unsafe { accepted_peer(socket(sqe.fd), &accepted, &sqe) };
                match result {
                    Ok(()) => {
                        let number = super::number(accepted.as_socket());
                        // The driver takes the socket from the completion, as from an accept on
                        // Linux.
                        let _raw = accepted.into_raw_socket();
                        self.post(index, number, 0);
                    }
                    Err(error) => self.post(index, error.saturating_neg(), 0),
                }
            }
            ring::IORING_OP_CONNECT => {
                // SAFETY: the option names no memory.
                let updated = unsafe {
                    sys::setsockopt(
                        socket(sqe.fd),
                        sys::SOL_SOCKET,
                        sys::SO_UPDATE_CONNECT_CONTEXT,
                        ptr::null(),
                        0,
                    )
                };
                let result = if updated == sys::SOCKET_ERROR { last_error() } else { 0 };
                self.post(index, result, 0);
            }
            _ => self.post(index, i32::try_from(bytes).unwrap_or(i32::MAX), 0),
        }
    }

    /// The receive of no bytes of the selecting receive of `index` ended: data or the end of the
    /// stream waits. Reads into the buffer at the head of the provided ring with no wait. With no
    /// data, the receive of no bytes runs again.
    #[expect(
        unsafe_code,
        reason = "the receive writes into a provided buffer that the driver published"
    )]
    fn select(&mut self, index: u32) {
        let Some(fd) = self.slots[index as usize].as_ref().map(|slot| slot.sqe.fd) else {
            return;
        };
        let Some((address, len, bid)) = self.group.as_ref().and_then(|group| group.peek(0)) else {
            self.post(index, -super::tcp::ENOBUFS, 0);
            return;
        };
        // SAFETY: the buffer is free in the ring until the port takes it, and it has `len` bytes.
        match unsafe { receive(fd, pointer(address), len, ptr::null_mut()) } {
            Returned::Done(bytes) => {
                if let Some(group) = self.group.as_mut() {
                    group.advance();
                }
                let flags = ring::IORING_CQE_F_BUFFER | (u32::from(bid) << ring::IORING_CQE_BUFFER_SHIFT);
                self.post(index, i32::try_from(bytes).unwrap_or(i32::MAX), flags);
            }
            Returned::Failed(sys::WSAEWOULDBLOCK) => self.call(index),
            Returned::Failed(error) => self.post(index, error.saturating_neg(), 0),
            Returned::Pending => {}
        }
    }

    /// Posts the completion of the entry of `index`, and frees its slot.
    fn post(&mut self, index: u32, result: i32, flags: u32) {
        let Some(slot) = self.slots.get_mut(index as usize).and_then(Option::take) else {
            return;
        };
        self.free.push(index);
        self.completions.push_back(Cqe {
            user_data: slot.sqe.user_data,
            res: result,
            flags,
        });
    }

    /// Cancels the call of the entry of `user_data`. Gives the result of the cancel: 0, or the
    /// negative error of `CancelIoEx`, for example `ERROR_NOT_FOUND` for a call whose packet waits
    /// in the port. The target ends at its own packet.
    #[expect(
        unsafe_code,
        reason = "the call takes the socket and the OVERLAPPED of a running call"
    )]
    fn cancel(&mut self, user_data: u64) -> i32 {
        let target = self.slots.iter().position(|slot| {
            slot.as_ref()
                .is_some_and(|slot| slot.sqe.user_data == user_data && slot.queued)
        });
        let Some((index, fd)) = target.and_then(|index| Some((index, self.slots[index].as_ref()?.sqe.fd))) else {
            return -sys::ERROR_NOT_FOUND;
        };
        // SAFETY: the socket is open until the terminal event of its call, and the `OVERLAPPED`
        // stays in place until its packet.
        if unsafe { sys::CancelIoEx(handle(socket(fd)), self.overlapped[index].get()) } == 0 {
            return last_error();
        }
        0
    }
}

impl core::fmt::Debug for Port {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Port")
            .field("entries", &self.entries)
            .field("unsubmitted", &self.queue.len())
            .field("in_flight", &(self.slots.len() - self.free.len()))
            .field("completions", &self.completions.len())
            .finish_non_exhaustive()
    }
}

impl Drop for Port {
    /// The kernel can still write the `OVERLAPPED` of a call that runs, until its packet, also after
    /// its socket closes, so the array leaks then.
    fn drop(&mut self) {
        if self.slots.iter().flatten().any(|slot| slot.queued) {
            Box::leak(core::mem::take(&mut self.overlapped));
        }
    }
}

/// The ready completions of a port. Each one leaves the queue when the driver takes it.
pub(crate) struct Completions<'a>(&'a mut VecDeque<Cqe>);

impl Iterator for Completions<'_> {
    type Item = Cqe;

    fn next(&mut self) -> Option<Cqe> {
        self.0.pop_front()
    }
}

/// An `OVERLAPPED` before a call: the status `STATUS_PENDING`, which a call that fails before the
/// I/O manager leaves in place.
const fn idle() -> Overlapped {
    Overlapped {
        internal: sys::STATUS_PENDING as usize,
        internal_high: 0,
        offset: [0; 2],
        event: ptr::null_mut(),
    }
}

/// The NT status in the `OVERLAPPED` at `overlapped`.
///
/// # Safety
///
/// The kernel wrote the status of the call, or the call never reached it, and writes no more.
#[expect(
    unsafe_code,
    reason = "the caller gives an OVERLAPPED that the kernel no longer writes"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "an NTSTATUS is 32 bits, in the low bits of `Internal`"
)]
unsafe fn status(overlapped: *mut Overlapped) -> u32 {
    // SAFETY: the caller's contract, and the field is in an `OVERLAPPED` of the array.
    unsafe { (&raw const (*overlapped).internal).read_volatile() as u32 }
}

/// The Winsock error of the call of the `OVERLAPPED` at `overlapped` on the socket `fd`, whose
/// packet came with an error status.
#[expect(unsafe_code, reason = "the call reads the OVERLAPPED of a call that ended")]
fn failure(fd: i32, overlapped: *mut Overlapped) -> c_int {
    let (mut bytes, mut flags) = (0, 0);
    // SAFETY: the call ended, the socket is open until its terminal event, and the outputs live
    // during the call.
    let done = unsafe { sys::WSAGetOverlappedResult(socket(fd), overlapped, &raw mut bytes, 0, &raw mut flags) };
    if done == 0 {
        last_error().saturating_neg()
    } else {
        sys::WSAEINVAL
    }
}

/// One `WSARecv` of up to `len` bytes at `buffer` on the socket `fd`, overlapped with
/// `overlapped`, or with no wait for a null `overlapped`.
///
/// # Safety
///
/// `buffer` is valid for `len` bytes until the terminal event of the call, and `overlapped` stays
/// in place until then.
#[expect(unsafe_code, reason = "the caller keeps the buffer and the OVERLAPPED")]
unsafe fn receive(fd: i32, buffer: *mut c_char, len: u32, overlapped: *mut Overlapped) -> Returned {
    let mut region = sys::WsaBuf { len, buf: buffer };
    let (mut received, mut flags) = (0, 0);
    // SAFETY: the caller's contract, and the call copies the `WSABUF` before it returns.
    let result = unsafe {
        sys::WSARecv(
            socket(fd),
            &raw mut region,
            1,
            &raw mut received,
            &raw mut flags,
            overlapped,
            ptr::null_mut(),
        )
    };
    returned(result != sys::SOCKET_ERROR, received)
}

/// One overlapped `WSASend` of the `count` regions at `regions` on the socket `fd`.
///
/// # Safety
///
/// The bytes of each region stay valid until the terminal event of the call, and `overlapped`
/// stays in place until then.
#[expect(unsafe_code, reason = "the caller keeps the regions and the OVERLAPPED")]
unsafe fn send(fd: i32, regions: *const sys::WsaBuf, count: u32, overlapped: *mut Overlapped) -> Returned {
    let mut sent = 0;
    // SAFETY: the caller's contract, and the call copies the `WSABUF` array before it returns.
    let result = unsafe {
        sys::WSASend(
            socket(fd),
            regions,
            count,
            &raw mut sent,
            0,
            overlapped,
            ptr::null_mut(),
        )
    };
    returned(result != sys::SOCKET_ERROR, sent)
}

/// An `AcceptEx` on the listener of the entry of `slot`, with a new socket of its family, which
/// the slot keeps. The call writes its addresses into the address storage of the entry.
///
/// # Safety
///
/// The address storage of the entry stays valid until its terminal event, and `overlapped` stays in
/// place until then.
#[expect(unsafe_code, reason = "the caller keeps the address storage and the OVERLAPPED")]
unsafe fn accept(slot: &mut Slot, overlapped: *mut Overlapped) -> Returned {
    let listener = socket(slot.sqe.fd);
    let family = name(listener).map_or(AF_INET, |(address, _)| address.ss_family);
    let accepted = match new_socket(family, sys::SOCK_STREAM) {
        Ok(accepted) => accepted,
        Err(error) => return Returned::Failed(error.raw_os_error().unwrap_or(sys::WSAEINVAL)),
    };
    let Some(accept) = extension::<sys::AcceptEx>(listener, &ACCEPT_EX, sys::WSAID_ACCEPTEX) else {
        return Returned::Failed(sys::WSAEINVAL);
    };
    let raw = accepted.as_raw_socket();
    slot.accepted = Some(accepted);
    let mut received = 0;
    // SAFETY: the caller's contract. The storage holds both addresses (`ACCEPT_ADDRESS` each), and
    // the call receives no data.
    let result = unsafe {
        accept(
            listener,
            raw,
            pointer::<SockAddrStorage>(slot.sqe.addr).cast(),
            0,
            ACCEPT_ADDRESS,
            ACCEPT_ADDRESS,
            &raw mut received,
            overlapped,
        )
    };
    returned(result != 0, received)
}

/// After the accept on `listener` gave `accepted`: gives it the state of the listener and the
/// nonblocking mode, and writes its peer address and length where the entry names them.
///
/// # Safety
///
/// The address storage and the length of `sqe` are valid.
#[expect(unsafe_code, reason = "the calls write into the storage that the entry names")]
unsafe fn accepted_peer(listener: RawSocket, accepted: &OwnedSocket, sqe: &Sqe) -> Result<(), c_int> {
    let raw = accepted.as_raw_socket();
    // SAFETY: the option is the listener socket, which lives during the call.
    let updated = unsafe {
        sys::setsockopt(
            raw,
            sys::SOL_SOCKET,
            sys::SO_UPDATE_ACCEPT_CONTEXT,
            ptr::from_ref(&listener).cast(),
            c_int::try_from(size_of::<RawSocket>()).unwrap_or(0),
        )
    };
    if updated == sys::SOCKET_ERROR {
        return Err(last_error().saturating_neg());
    }
    nonblocking(raw).map_err(|error| error.raw_os_error().unwrap_or(sys::WSAEINVAL))?;
    if socket(super::number(accepted.as_socket())) != raw {
        return Err(WSAEMFILE);
    }
    let len = pointer::<u32>(sqe.off);
    // SAFETY: the caller's contract: the storage has room for the length that it names.
    let mut name_len = c_int::try_from(unsafe { len.read() }).unwrap_or(0);
    // SAFETY: the caller's contract: the storage has room for `name_len` bytes.
    if unsafe { sys::getpeername(raw, pointer(sqe.addr), &raw mut name_len) } == sys::SOCKET_ERROR {
        return Err(last_error().saturating_neg());
    }
    // SAFETY: the caller's contract.
    unsafe { len.write(u32::try_from(name_len).unwrap_or(0)) };
    Ok(())
}

/// `WSAEMFILE`: the handle of an accepted socket has significant bits above the low 32, so no
/// completion can hold it.
const WSAEMFILE: c_int = 10024;

/// A `ConnectEx` of the socket of `sqe` to its address, after a bind to the wildcard address of its
/// family.
///
/// # Safety
///
/// The address of the entry stays valid until its terminal event, and `overlapped` stays in place
/// until then.
#[expect(unsafe_code, reason = "the caller keeps the address and the OVERLAPPED")]
unsafe fn connect(sqe: &Sqe, overlapped: *mut Overlapped) -> Returned {
    let target = pointer::<SockAddrStorage>(sqe.addr);
    // SAFETY: the caller's contract.
    let family = unsafe { (*target).ss_family };
    let (wildcard, wildcard_len) = wildcard(family);
    let fd = socket(sqe.fd);
    // SAFETY: the wildcard address lives during the call.
    if unsafe { sys::bind(fd, ptr::from_ref(&wildcard).cast(), wildcard_len) } == sys::SOCKET_ERROR {
        return Returned::Failed(last_error().saturating_neg());
    }
    let Some(connect) = extension::<sys::ConnectEx>(fd, &CONNECT_EX, sys::WSAID_CONNECTEX) else {
        return Returned::Failed(sys::WSAEINVAL);
    };
    let mut sent = 0;
    // SAFETY: the caller's contract. The call sends no data.
    let result = unsafe {
        connect(
            fd,
            target.cast(),
            c_int::try_from(sqe.off).unwrap_or(0),
            ptr::null_mut(),
            0,
            &raw mut sent,
            overlapped,
        )
    };
    returned(result != 0, sent)
}

/// The wildcard address of `family`, with port 0, and its length.
fn wildcard(family: SaFamily) -> (SockAddrStorage, c_int) {
    let address = if family == AF_INET6 {
        core::net::SocketAddr::from((core::net::Ipv6Addr::UNSPECIFIED, 0))
    } else {
        core::net::SocketAddr::from((core::net::Ipv4Addr::UNSPECIFIED, 0))
    };
    let (storage, len) = SockAddrStorage::new(address);
    (storage, c_int::try_from(len).unwrap_or(0))
}

/// How a call returned: by its success, and then with `bytes`, or by the last Winsock error,
/// where `ERROR_IO_PENDING` means that it runs.
fn returned(succeeded: bool, bytes: u32) -> Returned {
    if succeeded {
        return Returned::Done(bytes);
    }
    match last_error().saturating_neg() {
        sys::ERROR_IO_PENDING => Returned::Pending,
        error => Returned::Failed(error),
    }
}

/// The pointer of an address in an entry.
fn pointer<T>(address: u64) -> *mut T {
    ptr::with_exposed_provenance_mut(usize::try_from(address).unwrap_or(0))
}

/// The handle of the socket `raw`.
fn handle(raw: RawSocket) -> sys::Handle {
    ptr::without_provenance_mut(usize::try_from(raw).unwrap_or(usize::MAX))
}

/// The negative Winsock error of the last call of this thread, as a completion gives it.
fn last_error() -> i32 {
    io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(sys::WSAEINVAL)
        .saturating_neg()
}

/// The size of a small structure, as a `DWORD`.
fn u32_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// The pointers of `AcceptEx` and `ConnectEx`, the same for each socket of the base provider.
static ACCEPT_EX: OnceLock<sys::AcceptEx> = OnceLock::new();
static CONNECT_EX: OnceLock<sys::ConnectEx> = OnceLock::new();

/// The extension function `id`, which `ws2_32.dll` gives only as a pointer, through `socket`. The
/// first call keeps it in `cell`.
#[expect(unsafe_code, reason = "the stack writes the function pointer into owned storage")]
fn extension<F: Copy>(socket: RawSocket, cell: &OnceLock<F>, id: sys::Guid) -> Option<F> {
    if let Some(&function) = cell.get() {
        return Some(function);
    }
    let mut function: Option<F> = None;
    let mut returned = 0;
    // SAFETY: the identifier and the storage of the pointer live during the call, and the stack
    // writes at most one function pointer, or null, which `Option<F>` holds for a function pointer
    // type `F`.
    let result = unsafe {
        sys::WSAIoctl(
            socket,
            sys::SIO_GET_EXTENSION_FUNCTION_POINTER,
            ptr::from_ref(&id).cast(),
            u32_len(size_of::<sys::Guid>()),
            ptr::from_mut(&mut function).cast(),
            u32_len(size_of::<Option<F>>()),
            &raw mut returned,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    let function = function.filter(|_| result != sys::SOCKET_ERROR)?;
    Some(*cell.get_or_init(|| function))
}

/// Starts Winsock for this process, one time. The driver may make the first socket of the
/// process, so it does not depend on another user of Winsock.
#[expect(unsafe_code, reason = "the call writes the facts of Winsock into owned storage")]
fn startup() -> io::Result<()> {
    static STARTED: OnceLock<c_int> = OnceLock::new();
    let error = *STARTED.get_or_init(|| {
        let mut data = sys::WsaData {
            version: 0,
            high_version: 0,
            max_sockets: 0,
            max_udp_dg: 0,
            vendor_info: ptr::null_mut(),
            description_and_status: [0; 257 + 129],
        };
        // SAFETY: the storage lives during the call. Winsock stays started for the life of the
        // process, as the standard library leaves it.
        unsafe { sys::WSAStartup(0x0202, &raw mut data) }
    });
    match error {
        0 => Ok(()),
        error => Err(io::Error::from_raw_os_error(error)),
    }
}

/// Sets the nonblocking mode of `socket`, which a call with no `OVERLAPPED` needs: a selecting
/// receive reads with no wait. Overlapped calls run as before.
#[expect(unsafe_code, reason = "the call reads one value that lives during the call")]
fn nonblocking(socket: RawSocket) -> io::Result<()> {
    let mut on: u32 = 1;
    // SAFETY: the value lives during the call.
    if unsafe { sys::ioctlsocket(socket, sys::FIONBIO, &raw mut on) } == sys::SOCKET_ERROR {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// A new nonblocking overlapped socket of `family` and `kind`, which no child process inherits.
///
/// # Errors
///
/// The error of `WSASocketW` or of `ioctlsocket`.
#[expect(unsafe_code, reason = "the call gives a new socket")]
pub(crate) fn new_socket(family: SaFamily, kind: c_int) -> io::Result<OwnedSocket> {
    startup()?;
    let flags = sys::WSA_FLAG_OVERLAPPED | sys::WSA_FLAG_NO_HANDLE_INHERIT;
    // SAFETY: the call names no memory.
    let raw = unsafe { sys::WSASocketW(c_int::from(family), kind, 0, ptr::null_mut(), 0, flags) };
    if raw == sys::INVALID_SOCKET {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the call gave a new socket that nothing else owns.
    let socket = unsafe { OwnedSocket::from_raw_socket(raw) };
    nonblocking(raw)?;
    Ok(socket)
}

/// Sets the option `name` of `level` of `socket` to the `int` `value`.
///
/// # Errors
///
/// The error of `setsockopt`.
#[expect(unsafe_code, reason = "the call reads one `int` that lives during the call")]
pub(crate) fn set_int(socket: BorrowedSocket<'_>, level: c_int, name: c_int, value: c_int) -> io::Result<()> {
    // SAFETY: `value` is an `int` that lives during the call.
    let result = unsafe {
        sys::setsockopt(
            socket.as_raw_socket(),
            level,
            name,
            ptr::from_ref(&value).cast(),
            c_int::try_from(size_of::<c_int>()).unwrap_or(0),
        )
    };
    if result == sys::SOCKET_ERROR {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The `int` option `name` of `level` of `socket`.
///
/// # Errors
///
/// The error of `getsockopt`.
#[expect(unsafe_code, reason = "the stack writes one `int` that lives during the call")]
pub(crate) fn int(socket: BorrowedSocket<'_>, level: c_int, name: c_int) -> io::Result<c_int> {
    let mut value: c_int = 0;
    let mut len = c_int::try_from(size_of::<c_int>()).unwrap_or(0);
    // SAFETY: the stack writes at most `len` bytes into `value`, which lives during the call.
    let result = unsafe {
        sys::getsockopt(
            socket.as_raw_socket(),
            level,
            name,
            ptr::from_mut(&mut value).cast(),
            &raw mut len,
        )
    };
    if result == sys::SOCKET_ERROR {
        return Err(io::Error::last_os_error());
    }
    Ok(value)
}

/// The local address of `socket`, and its length.
pub(crate) fn local_address(socket: impl AsSocket) -> Option<(SockAddrStorage, u32)> {
    name(socket.as_socket().as_raw_socket())
}

/// The local address of the open socket `socket`, and its length.
#[expect(
    unsafe_code,
    reason = "the stack writes the address into storage that lives during the call"
)]
fn name(socket: RawSocket) -> Option<(SockAddrStorage, u32)> {
    let mut address = SockAddrStorage::EMPTY;
    let mut len = c_int::try_from(SockAddrStorage::LEN).unwrap_or(0);
    // SAFETY: the stack writes at most `len` bytes into `address`, which lives during the call.
    let result = unsafe { sys::getsockname(socket, ptr::from_mut(&mut address).cast(), &raw mut len) };
    (result != sys::SOCKET_ERROR).then(|| (address, u32::try_from(len).unwrap_or(0)))
}

/// Binds `socket` to the address of `len` bytes in `address`, and with `listen` listens on it.
///
/// # Errors
///
/// The error of `bind` or of `listen`.
#[expect(unsafe_code, reason = "the call reads the address, which lives during the call")]
pub(crate) fn bind_now(
    socket: BorrowedSocket<'_>,
    address: &SockAddrStorage,
    len: u32,
    listen: bool,
) -> io::Result<()> {
    let raw = socket.as_raw_socket();
    // SAFETY: `address` holds an address of `len` bytes during the call.
    if unsafe { sys::bind(raw, ptr::from_ref(address).cast(), c_int::try_from(len).unwrap_or(0)) } == sys::SOCKET_ERROR
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the call names no memory.
    if listen && unsafe { sys::listen(raw, c_int::MAX) } == sys::SOCKET_ERROR {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
