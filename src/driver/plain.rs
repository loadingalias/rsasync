//! Operations on a socket that no driver owns: the operations of a movable resource
//! (`rsasync::movable`).
//!
//! A movable resource owns its descriptor and the state of its directions, and both move with its
//! task. Each operation is one submission with the plain descriptor on the ring of the driver of
//! the thread that starts it. That driver is the home of the operation until its terminal event,
//! and the consumer of the operation takes the result on any thread. The resources of the operation
//! hold the descriptor, so it stays open until that event.
//!
//! # The direction keeps its operation
//!
//! A [`Direction`] runs one operation at a time, and it is the consumer of that operation: the
//! future of the operation only borrows the direction. So a dropped or forgotten future leaves the
//! operation with its direction, and the home never applies the abandoned rule to it. The next
//! operation of the direction, or the close, takes its result.
//!
//! # The next operation waits at its own home
//!
//! An operation that starts while an earlier one of its direction still runs takes that earlier
//! operation with it, in a [`Chain`]. Its own home gives it a slot at once, so it never refuses
//! later, and it runs also when its future drops. That home is then the consumer of the earlier
//! operation: it takes the result when the home of the earlier operation publishes it, applies the
//! abandoned rule to it in the chain, and then starts the operation. So a direction never has two
//! operations in the kernel, and the order of its bytes never depends on two homes. The two homes
//! share only the completion entry of the earlier operation and its request entry.
//!
//! The chain also carries the state of the direction between operations: the bytes, the error, or
//! the connection that an abandoned operation left, and the progress that dropped sends owe. The
//! rules are those of a resource that a driver owns, in the same code (`tcp::Directions`). A
//! direction with no earlier operation and no state has no chain, so its operation allocates
//! nothing.
//!
//! An operation that a cancel or the teardown of its home ends before its start gives back the
//! earlier operation in its chain, and its consumer takes that one over.

use alloc::boxed::Box;
use core::mem;
use core::net::SocketAddr;
use core::task::{Context, Poll, ready};
use std::io;
use std::os::fd::{AsRawFd as _, OwnedFd, RawFd};

use rsnetwork_udp::{SockAddrStorage, sys};

use super::tcp::{self, Directions, Regions, TcpOptions};
use super::{Completion, Driver, ECANCELED, Held, Kind, Payload, Reason, Refused, Tag, borrow, out_of_memory};
use crate::buffer::{Backing, Lease, WriteLoan};
use crate::capacity::Permit;
use crate::completion::Gone;
use crate::table::OpId;
use crate::uring::Sqe;

/// A socket that no driver owns. Its resource and each of its operations hold it, so it closes
/// after the resource dropped and each operation ended. It is not the `Arc` of the `sync` shims:
/// no protocol that loom checks reads its count, and the size of [`Held`] then does not depend on
/// the build.
pub(crate) type PlainSocket = alloc::sync::Arc<Socket>;

/// The descriptor of a movable socket, with the charge of its memory to the budget of the owner
/// that made it, which ends when the last holder drops.
#[derive(Debug)]
pub(crate) struct Socket {
    fd: OwnedFd,
    _charge: Permit,
}

impl Socket {
    /// The number of the descriptor.
    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// Reserves `size` bytes of memory from the budget of the owner of this thread: [`Reason::Budget`]
/// when the budget refuses, and [`Reason::NoDriver`] when the thread has no owner, so no driver
/// either.
fn charge(size: usize) -> Result<Permit, Reason> {
    match crate::task::reserve(u64::try_from(size).unwrap_or(u64::MAX)) {
        Some(Ok(permit)) => Ok(permit),
        Some(Err(_)) => Err(Reason::Budget),
        None => Err(Reason::NoDriver),
    }
}

/// Makes the movable socket of `fd`, charged to the budget of the owner of this thread. A
/// refusal closes `fd`: `OutOfMemory` when the budget or the allocator refuses, and `NotFound`
/// when the thread has no owner.
fn charged(fd: OwnedFd) -> io::Result<PlainSocket> {
    let charge = charge(crate::sync::arc_size::<Socket>()).map_err(|reason| match reason {
        Reason::NoDriver => io::Error::from(io::ErrorKind::NotFound),
        _ => io::Error::from(io::ErrorKind::OutOfMemory),
    })?;
    PlainSocket::try_new(Socket { fd, _charge: charge }).map_err(out_of_memory)
}

/// Reads each TCP option of `socket`.
pub(crate) fn options(socket: &PlainSocket) -> io::Result<TcpOptions> {
    TcpOptions::read(borrow(&socket.fd))
}

/// Sets each TCP option of `options` that is not `None` on `socket`.
pub(crate) fn set_options(socket: &PlainSocket, options: TcpOptions) -> io::Result<()> {
    options.apply(borrow(&socket.fd))
}

/// A new chain, with the charge of its memory to the budget of the owner of this thread.
fn chain() -> Result<Box<Chain>, Reason> {
    let charge = charge(size_of::<Chain>())?;
    Box::try_new(Chain {
        _charge: Some(charge),
        ..Chain::default()
    })
    .map_err(|_| Reason::Allocation)
}

/// What an operation on a plain descriptor keeps beside the resources of its kind.
#[derive(Debug)]
pub(crate) struct Plain {
    /// The descriptor of the operation. It stays open until the terminal event.
    socket: PlainSocket,
    /// The state of the direction, and the earlier operations that the operation waits for.
    chain: Option<Box<Chain>>,
}

/// The operation of a direction whose result no consumer took.
#[derive(Debug)]
struct Tail {
    completion: Completion,
    kind: Kind,
}

impl Drop for Tail {
    /// No consumer takes the result any more, because the resource dropped or its close takes each
    /// operation. The home cancels the operation, a send too, as a close does. A close ends by
    /// itself.
    fn drop(&mut self) {
        if self.kind != Kind::Close {
            self.completion.cancel();
        }
    }
}

/// The state of the directions of a movable resource before an operation, and the earlier
/// operations that the operation waits for.
#[derive(Debug, Default)]
struct Chain {
    /// The earlier operation of the direction. A close has one for each direction of a stream.
    before: [Option<Tail>; 2],
    /// What the operations before left.
    flow: Directions,
    /// The charge of the memory of the chain, which ends with the chain.
    _charge: Option<Permit>,
}

impl Chain {
    /// Takes the result of each earlier operation that ended, and applies the abandoned rule to
    /// it. An earlier operation that ended before its start gives the operation that it waited
    /// for, which takes its place. With `cancel`, that operation gets a cancel, because a close
    /// cancels each operation. Registers the waker of `cx` at each operation that still runs. Gives
    /// whether no earlier operation remains.
    fn settle(&mut self, cx: &Context<'_>, cancel: bool) -> bool {
        for place in &mut self.before {
            while let Some(tail) = place {
                // The abandoned rule keeps the resources, so they leave the slot whole.
                let taken = tail
                    .completion
                    .take_with(cx, |result, held, _| (result, mem::take(held)));
                let Poll::Ready(taken) = taken else {
                    break;
                };
                let kind = tail.kind;
                *place = None;
                // The driver of an earlier operation that gives nothing ended, with its result.
                let Ok((result, mut held)) = taken else {
                    continue;
                };
                if let Some(mut earlier) = held.plain.as_mut().and_then(|plain| plain.chain.take()) {
                    self.flow.inherit(&mut earlier.flow, kind);
                    *place = earlier.before[0].take();
                    if cancel && let Some(tail) = place {
                        tail.completion.cancel();
                    }
                }
                self.flow.abandon(kind, result, held, &mut tcp::Shelf::default());
            }
        }
        self.before.iter().all(Option::is_none)
    }

    /// Gives the operation of `kind`, with the resources `held`, the result that its direction
    /// holds for it, when it needs no submission. Adds the bytes that it copies to `copied`.
    fn result(&mut self, held: &mut Held, kind: Kind, copied: &mut u64) -> Option<i32> {
        if kind != Kind::Accept {
            // The direction is the consumer of each operation, so each one has interest.
            return self
                .flow
                .resume(held, kind, || true, copied, &mut tcp::Shelf::default());
        }
        let Payload::Socket {
            peer,
            peer_len,
            connection,
        } = &mut held.payload
        else {
            return None;
        };
        let (fd, address, len) = self.flow.take_connection()?;
        (*peer, *peer_len, *connection) = (address, len, Some(fd));
        Some(0)
    }
}

/// One direction of a movable resource: the read direction or the write direction of a stream, or
/// the accepts of a listener. It is also the place of an operation that has no resource yet, such
/// as a connect.
#[derive(Debug, Default)]
pub(crate) struct Direction {
    /// The last operation of the direction, until a consumer takes its result.
    tail: Option<Tail>,
    /// The state of the direction, when the operations before left one.
    chain: Option<Box<Chain>>,
}

impl Direction {
    /// Starts a receive into `buffer` on `socket`, with `driver` as its home.
    pub(crate) fn recv(
        &mut self,
        driver: &mut Driver,
        socket: &PlainSocket,
        buffer: Backing,
    ) -> Result<(), Refused<Backing>> {
        self.begin(driver, socket, Kind::Recv, buffer, |buffer| Held {
            loan: Some(WriteLoan::new(buffer)),
            ..Held::default()
        })
    }

    /// Starts a send of `lease` on `socket`, with `driver` as its home.
    pub(crate) fn send(
        &mut self,
        driver: &mut Driver,
        socket: &PlainSocket,
        lease: Lease,
    ) -> Result<(), Refused<Lease>> {
        self.begin(driver, socket, Kind::Send, lease, |lease| Held {
            payload: Payload::Lease(lease),
            ..Held::default()
        })
    }

    /// Starts a send of the bytes of `regions` in order, in one operation, on `socket`, with
    /// `driver` as its home.
    pub(crate) fn send_vectored(
        &mut self,
        driver: &mut Driver,
        socket: &PlainSocket,
        regions: Regions,
    ) -> Result<(), Refused<Regions>> {
        self.begin(driver, socket, Kind::Vectored, regions, |regions| Held {
            payload: Payload::Regions(regions),
            ..Held::default()
        })
    }

    /// Starts the end of the write side of the stream `socket`, after the sends before it, with
    /// `driver` as its home.
    pub(crate) fn shutdown_write(&mut self, driver: &mut Driver, socket: &PlainSocket) -> Result<(), Refused<()>> {
        self.begin(driver, socket, Kind::Shutdown, (), |()| Held::default())
    }

    /// Starts a receive on `socket` into a buffer that the kernel selects from the provided
    /// buffers of `driver`, its home. Refuses with [`Reason::Buffers`] when that driver has none.
    pub(crate) fn recv_selected(&mut self, driver: &mut Driver, socket: &PlainSocket) -> Result<(), Refused<()>> {
        if driver.selected.is_none() {
            return Err(Refused {
                reason: Reason::Buffers,
                value: (),
            });
        }
        self.begin(driver, socket, Kind::Selected, (), |()| Held::default())
    }

    /// Starts an accept on the listening `socket`, with `driver` as its home.
    pub(crate) fn accept(&mut self, driver: &mut Driver, socket: &PlainSocket) -> Result<(), Refused<()>> {
        self.begin(driver, socket, Kind::Accept, (), |()| {
            socket_held(SockAddrStorage::default(), SockAddrStorage::LEN)
        })
    }

    /// Starts an operation of `kind` in this direction, with the resources that `held` makes from
    /// `value`. The operation takes the state of the direction, and the earlier operation whose
    /// result no future took. A refusal changes nothing.
    fn begin<P>(
        &mut self,
        driver: &mut Driver,
        socket: &PlainSocket,
        kind: Kind,
        value: P,
        held: impl FnOnce(P) -> Held,
    ) -> Result<(), Refused<P>> {
        // The memory comes before the slot, so the home makes the resources with no refusal.
        let fresh = if self.tail.is_some() && self.chain.is_none() {
            match chain() {
                Ok(chain) => Some(chain),
                Err(reason) => return Err(Refused { reason, value }),
            }
        } else {
            None
        };
        let completion = driver.begin_plain(kind, value, |value| {
            let chain = self.chain.take().or(fresh).map(|mut chain| {
                chain.before[0] = self.tail.take();
                // A receive or an accept with no future gets a cancel, as at a drop of its future.
                if let Some(earlier) = &chain.before[0]
                    && !earlier.kind.writes()
                {
                    earlier.completion.cancel();
                }
                chain
            });
            Held {
                plain: Some(Plain {
                    socket: PlainSocket::clone(socket),
                    chain,
                }),
                ..held(value)
            }
        })?;
        self.tail = Some(Tail { completion, kind });
        Ok(())
    }

    /// Starts the close of the resource of `socket`, with `driver` as its home: `read` and `write`
    /// are its directions, and a listener gives an empty second one. The close cancels each of
    /// their operations and ends after each one ended. Gives the place of the close. A refusal
    /// gives each part back, and changes nothing.
    pub(crate) fn close(
        driver: &mut Driver,
        socket: PlainSocket,
        read: Self,
        write: Self,
    ) -> Result<Self, Refused<(PlainSocket, Self, Self)>> {
        let tails = read.tail.is_some() || write.tail.is_some();
        let fresh = if tails && read.chain.is_none() && write.chain.is_none() {
            match chain() {
                Ok(chain) => Some(chain),
                Err(reason) => {
                    return Err(Refused {
                        reason,
                        value: (socket, read, write),
                    });
                }
            }
        } else {
            None
        };
        let completion = driver.begin_plain(Kind::Close, (socket, read, write), |(socket, mut read, mut write)| {
            let mut chain = match (read.chain.take(), write.chain.take()) {
                (Some(mut chain), Some(mut other)) => {
                    chain.flow.inherit(&mut other.flow, Kind::Send);
                    Some(chain)
                }
                (chain, other) => chain.or(other).or(fresh),
            };
            if let Some(chain) = &mut chain {
                chain.before = [read.tail.take(), write.tail.take()];
                for tail in chain.before.iter().flatten() {
                    tail.completion.cancel();
                }
            }
            Held {
                plain: Some(Plain { socket, chain }),
                ..Held::default()
            }
        })?;
        Ok(Self::of(completion, Kind::Close))
    }

    /// The place of the operation of `completion`, which has no resource.
    const fn of(completion: Completion, kind: Kind) -> Self {
        Self {
            tail: Some(Tail { completion, kind }),
            chain: None,
        }
    }

    /// Polls the operation of the direction for the future that borrows it: makes its output with
    /// `output` from its result and its resources, and keeps the state that it leaves. An operation
    /// that ended before its start gives back the earlier operation, which the direction keeps
    /// again. A direction with no operation gives [`Gone`].
    pub(crate) fn poll<T>(
        &mut self,
        cx: &mut Context<'_>,
        output: fn(i32, &mut Held) -> Option<T>,
    ) -> Poll<Result<Option<T>, Gone>> {
        let Some(tail) = &mut self.tail else {
            return Poll::Ready(Err(Gone));
        };
        let (kind, id) = (tail.kind, tail.completion.id().op);
        let taken = ready!(tail.completion.poll_with(cx, |result, held, _| {
            let chain = held.plain.as_mut().and_then(|plain| plain.chain.take());
            (chain, output(result, held))
        }));
        self.tail = None;
        Poll::Ready(taken.map(|(chain, output)| {
            if let Some(mut chain) = chain {
                self.tail = chain.before[0].take();
                chain.flow.taken(kind, id);
                if !chain.flow.is_clear() {
                    self.chain = Some(chain);
                }
            }
            output
        }))
    }

    /// Asks the home of the operation of the direction to cancel it. The direction keeps the
    /// operation, and a consumer later takes its result.
    pub(crate) fn cancel(&self) {
        if let Some(tail) = &self.tail {
            tail.completion.cancel();
        }
    }

    /// The future of the operation of the direction dropped: a receive, an accept, or a connect
    /// gets a cancel, and a send runs on.
    pub(crate) fn forgo(&self) {
        if let Some(tail) = &self.tail
            && !tail.kind.writes()
            && tail.kind != Kind::Close
        {
            tail.completion.cancel();
        }
    }
}

/// The resources of an accept, a connect, or a bind, with the address `peer` of `len` bytes.
fn socket_held(peer: SockAddrStorage, len: u32) -> Held {
    Held {
        payload: Payload::Socket {
            peer,
            peer_len: len,
            connection: None,
        },
        ..Held::default()
    }
}

/// The output of a plain accept, connect, or bind: the socket that the operation made, which no
/// driver owns, and the address that its resources hold. That is the peer of an accept, the target
/// of a connect, or the local address of a bind.
pub(crate) fn socket(result: i32, held: &mut Held) -> Option<io::Result<(PlainSocket, SocketAddr)>> {
    Some(tcp::count(result).and_then(|_| {
        let Payload::Socket {
            peer,
            peer_len,
            connection,
        } = &mut held.payload
        else {
            return Err(io::ErrorKind::InvalidData.into());
        };
        let socket = connection.take().ok_or(io::ErrorKind::InvalidData)?;
        let address = peer.to_socket_addr(*peer_len).ok_or(io::ErrorKind::InvalidData)?;
        Ok((charged(socket)?, address))
    }))
}

/// What the home does next with an operation on a plain descriptor.
enum Step {
    /// An earlier operation of its direction still runs.
    Wait,
    /// The operation ends with this result and no submission.
    End(i32),
    /// The operation needs its submission on this descriptor.
    Submit(i32),
}

/// The next step of the operation of `kind` with the resources `held`, after it took the result
/// of each earlier operation that ended.
fn step(held: &mut Held, kind: Kind, cx: &Context<'_>, copied: &mut u64) -> Step {
    let Some(plain) = &mut held.plain else {
        return Step::End(-ECANCELED);
    };
    let fd = plain.socket.as_raw_fd();
    let mut chain = plain.chain.take();
    if let Some(waiting) = &mut chain
        && !waiting.settle(cx, kind == Kind::Close)
    {
        plain.chain = chain;
        return Step::Wait;
    }
    if kind == Kind::Close {
        held.closed = Some(match &chain {
            Some(chain) => chain.flow.closed(),
            None => Directions::default().closed(),
        });
        // The descriptor closes here: each operation of the resource ended and dropped its hold.
        // The data and the connections that no operation took drop with the chain.
        held.plain = None;
        return Step::End(0);
    }
    let result = chain.as_mut().and_then(|chain| chain.result(held, kind, copied));
    if let Some(plain) = &mut held.plain {
        plain.chain = chain;
    }
    result.map_or(Step::Submit(fd), Step::End)
}

impl Driver {
    /// Gives `value` back when the backend is io_uring, which takes an entry with a descriptor
    /// that the driver does not own. The poller registers each descriptor that it waits for, so
    /// it refuses with [`Reason::Unsupported`].
    fn ring_only<P>(&mut self, value: P) -> Result<P, Refused<P>> {
        match self.queue.ring() {
            Some(_) => Ok(value),
            None => Err(Refused {
                reason: Reason::Unsupported,
                value,
            }),
        }
    }

    /// Records an operation of `kind` on a plain descriptor, with the resources that `held` makes
    /// from `value`, and starts it, or makes it wait for the earlier operations of its chain. This
    /// driver is its home. Refuses with `value`, before `held` runs, when the driver has no room.
    fn begin_plain<P>(&mut self, kind: Kind, value: P, held: impl FnOnce(P) -> Held) -> Result<Completion, Refused<P>> {
        let value = self.ring_only(value)?;
        let tag = Tag::new(None, kind);
        let id = self
            .open(tag, u32::from(kind != Kind::Close), value, held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        if !self.advance(id) {
            // At most one entry for each operation, so the capacity of the list holds it.
            self.following.push(id);
        }
        Ok(self.completion(id, false))
    }

    /// Moves the operation `id` forward: it takes the results of the earlier operations of its
    /// chain, and then it ends with the result that its direction holds, or the home submits it.
    /// Gives `false` while it waits: for an earlier operation, whose end wakes this driver, or for
    /// room in the queue, which the next turn gives.
    fn advance(&mut self, id: OpId) -> bool {
        let Some(&Tag { kind, .. }) = self.table.resources(id) else {
            return true;
        };
        let Some(slot) = self.shared.entries.get(id.index() as usize) else {
            return true;
        };
        let (cx, copied) = (Context::from_waker(&self.job_waker), &mut self.copied);
        let mut next = Step::Wait;
        slot.update(id, |held| {
            if let Some(held) = held {
                next = step(held, kind, &cx, copied);
            }
        });
        match next {
            Step::Wait => false,
            Step::End(result) => {
                self.complete_now(id, result);
                true
            }
            Step::Submit(fd) => {
                if !self.room(1) {
                    return false;
                }
                // A receive into a selected buffer takes it from the provided buffers of this home.
                let group = self.selected.as_ref().map_or(0, |selected| selected.ring.group());
                let slot_len = self.selected.as_ref().map_or(0, |selected| selected.len as usize);
                // SAFETY: the entry names the loan, the lease, the regions, or the peer address in
                // the resources of `id`, which the home keeps until its terminal event. Those
                // resources hold the descriptor too, so it stays open. The queue has room.
                #[expect(unsafe_code, reason = "the kernel uses the memory of the resources of the operation")]
                unsafe {
                    self.push(id, |held| tcp::submission(held, kind, fd, false, group, slot_len));
                }
                true
            }
        }
    }

    /// Moves each operation that waits forward. A turn calls it after its harvest, so an operation
    /// starts in the turn that ends the one before it at this home. The end of an earlier
    /// operation at another home wakes this driver, and its next turn comes here.
    pub(super) fn follow(&mut self) {
        let mut index = 0;
        while let Some(&id) = self.following.get(index) {
            if self.advance(id) {
                self.following.swap_remove(index);
            } else {
                index += 1;
            }
        }
    }

    /// Takes `id` out of the operations that wait, if it waits there: it never ran, so a cancel
    /// ends it at once. Gives whether it waited.
    pub(super) fn unfollow(&mut self, id: OpId) -> bool {
        let waited = self.following.iter().position(|&waiting| waiting == id);
        waited.map(|index| self.following.swap_remove(index)).is_some()
    }

    /// Ends each operation that waits, before a teardown or a drop: none of them ran, and each one
    /// gives the earlier operations of its chain to its consumer.
    pub(super) fn end_following(&mut self) {
        while let Some(id) = self.following.pop() {
            self.complete_now(id, -ECANCELED);
        }
    }

    /// Binds a new TCP socket that no driver owns to `address` with `options`, and listens on it,
    /// as [`Driver::bind`] does. The system calls end at once, so the operation is ready then.
    /// [`socket`] gives its output.
    pub(crate) fn bind_plain(&mut self, address: SocketAddr, options: TcpOptions) -> Result<Direction, Refused<()>> {
        self.ring_only(())?;
        let (storage, len) = SockAddrStorage::new(address);
        let tag = Tag::new(None, Kind::Bind);
        let id = self
            .open(tag, 0, (), |()| socket_held(storage, len))
            .map_err(|(reason, value)| Refused { reason, value })?;
        match tcp::listening((storage, len), options) {
            Ok((socket, (local, local_len))) => {
                self.hold(id, |held| {
                    held.payload = Payload::Socket {
                        peer: local,
                        peer_len: local_len,
                        connection: Some(socket),
                    };
                });
                self.complete_now(id, 0);
            }
            Err(error) => self.complete_now(id, tcp::errno(&error)),
        }
        Ok(Direction::of(self.completion(id, false), Kind::Bind))
    }

    /// Connects a new TCP socket that no driver owns, with `options`, to `address`, as
    /// [`Driver::connect`] does. The resources of the connect hold the socket, so an abandoned
    /// connect closes it. [`socket`] gives its output.
    pub(crate) fn connect_plain(&mut self, address: SocketAddr, options: TcpOptions) -> Result<Direction, Refused<()>> {
        self.ring_only(())?;
        let (storage, len) = SockAddrStorage::new(address);
        let tag = Tag::new(None, Kind::Connect);
        let id = self
            .open(tag, 1, (), |()| socket_held(storage, len))
            .map_err(|(reason, value)| Refused { reason, value })?;
        let made = tcp::new_socket(storage.ss_family, sys::SOCK_STREAM).and_then(|socket| {
            options.apply(borrow(&socket))?;
            Ok(socket)
        });
        match made {
            Err(error) => self.complete_now(id, tcp::errno(&error)),
            Ok(socket) => {
                let fd = socket.as_raw_fd();
                self.hold(id, |held| {
                    if let Payload::Socket { connection, .. } = &mut held.payload {
                        *connection = Some(socket);
                    }
                });
                // SAFETY: the connect names the address in the resources of `id`, which the home
                // keeps until its terminal event, and the socket, which those resources hold. The
                // queue has room.
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
        Ok(Direction::of(self.completion(id, false), Kind::Connect))
    }
}
