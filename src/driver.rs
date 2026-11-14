//! Driver-only use: the caller owns an io_uring driver, submits operations to it, and drives it
//! with bounded nonblocking turns. No scheduler runs.
//!
//! A [`Driver`] is the home of each operation that it accepts, from the submission to the
//! terminal event of the operation, which releases its buffer and its charge. Its future can move
//! to another thread, while the home stays on the thread that created the driver. The caller keeps
//! the home driven until the end:
//!
//! - [`Driver::turn`] does a bounded amount of work, and it never blocks. It takes the requests of
//!   other threads, such as the cancel of a dropped future, submits, enters the kernel when there
//!   is work for it, and gives each ready completion to its future, up to
//!   [`Config::completions_per_turn`]. [`Turn::more`] says that work remains.
//! - [`Driver::wait`] blocks in the kernel until work comes: a completion, or a request or wake
//!   from another thread. The kernel runs its deferred work during the wait.
//! - The wake descriptor, [`AsFd`] of the driver, becomes readable when work comes, so a host's
//!   reactor can wait for it.
//! - [`Driver::teardown`] cancels each running operation, drives until each terminal event, and
//!   closes each resource. Its [`Teardown`] reports the cancelled operations.
//!
//! TCP runs on the driver: [`Driver::bind`], [`Driver::accept`], [`Driver::recv`] into an owned
//! buffer, [`Driver::recv_selected`] into a buffer of [`Driver::provide`], [`Driver::send`] from a
//! lease, and [`Driver::close`]. The kernel reads and writes each buffer in place. Each direction of
//! a stream runs one operation at a time, and the rules for a dropped or forgotten future keep each
//! byte and each connection: see [`TcpStream`].
//!
//! Files run on the driver too: [`Driver::open_file`] with an explicit cache mode, positional and
//! vectored reads and writes, length, space, sync, rename, unlink, directories, and
//! [`Driver::close_file`]. Several operations of one file run at one time: see [`File`].
//!
//! The io_uring driver needs no helper thread: [`Driver::helper_threads`] gives 0. A kernel with no
//! ring operation for a file call runs that call on the blocking service of the caller:
//! [`Driver::blocking_work`] names those calls, and [`Driver::use_blocking`] gives the service.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use rsasync::driver::{Config, Driver};
//!
//! let mut driver = Driver::new(Config::new())?;
//! // Submit operations here, and keep their futures.
//! loop {
//!     let turn = driver.turn()?;
//!     if !turn.more() {
//!         driver.wait()?;
//!     }
//!     // Stop when your work is done.
//! }
//! # }
//! ```

mod completion;
mod file;
mod pipe;
mod probe;
mod signal;
mod sys;
mod tcp;
#[cfg(all(test, not(loom)))]
mod tests;
mod udp;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::mem::{self, ManuallyDrop};
use core::pin::Pin;
use core::task::{Context, Poll};
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsFd, AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd};

use rsnetwork_udp::SockAddrStorage;

use crate::buffer::{Backing, BufferMut as _, Lease, Pool, WriteLoan};
use crate::capacity::Permit;
use crate::sync::Arc;
use crate::table::{CONTROL, Ended as Done, Event, MAX_SLOTS, OpId, Outcome, Request, Requests, Shape, Table, Taken};
use crate::uring::{BufferRing, Cqe, Features, Ring, SetupError, Sqe};

pub use file::{
    Alignment, BlockingWork, Buffers, Cache, Directory, File, Incomplete, Length, OpenOptions, SyncMode, WriteMode,
};
pub use pipe::{PipeReader, PipeWriter};
pub use signal::{Escalation, Recv, Signal, SignalError, SignalStream};
pub use tcp::{Closed, ReadHalf, Regions, Socket, TcpListener, TcpOptions, TcpStream, WriteFailed, WriteHalf};
pub use udp::{Arrivals, Datagram, Datagrams, UdpOptions, UdpSocket};

use crate::completion::Entry;
pub(crate) use completion::Completion;

/// The limits of a driver. Each one bounds memory that the driver allocates when it starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    submissions: u32,
    operations: u32,
    resources: u32,
    completions_per_turn: u32,
}

impl Config {
    /// 64 submission entries, 256 operations, 256 resources, and 64 completions per turn.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            submissions: 64,
            operations: 256,
            resources: 256,
            completions_per_turn: 64,
        }
    }

    /// The entries of the submission queue. The kernel rounds the number up to a power of two.
    #[must_use]
    pub const fn submissions(self, submissions: u32) -> Self {
        Self { submissions, ..self }
    }

    /// The operations that can run at one time. A submission beyond them is refused.
    #[must_use]
    pub const fn operations(self, operations: u32) -> Self {
        Self { operations, ..self }
    }

    /// The resources, such as descriptors, that the driver can own at one time.
    #[must_use]
    pub const fn resources(self, resources: u32) -> Self {
        Self { resources, ..self }
    }

    /// The most completions that one turn gives to their futures.
    #[must_use]
    pub const fn completions_per_turn(self, completions: u32) -> Self {
        Self {
            completions_per_turn: completions,
            ..self
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// Why [`Driver::new`] made no driver.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The kernel or its policy refuses io_uring (`EPERM` or `ENOSYS`), for example under the
    /// default seccomp profile of a container or with `kernel.io_uring_disabled`.
    Refused(io::Error),
    /// A limit of [`Config`] is 0 or above what the kernel takes.
    Config,
    /// Another failure of the system, for example no memory.
    System(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(error) => write!(f, "the kernel refuses io_uring: {error}"),
            Self::Config => f.write_str("a limit of the driver configuration is 0 or too large"),
            Self::System(error) => write!(f, "the driver did not start: {error}"),
        }
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Refused(error) | Self::System(error) => Some(error),
            Self::Config => None,
        }
    }
}

/// What one [`Driver::turn`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Turn {
    completed: u32,
    more: bool,
}

impl Turn {
    /// The completions that the turn took from the ring, which the bound of the turn counts.
    /// They include the completions of the driver itself, such as the result of a cancel and the
    /// end of the poll of its interrupt descriptor, so the count can exceed the operations that
    /// the turn ended.
    #[must_use]
    pub const fn completed(self) -> u32 {
        self.completed
    }

    /// Work remains for another turn: completions beyond the bound of the turn, work of the
    /// kernel, or requests of other threads. Turn again before a wait.
    #[must_use]
    pub const fn more(self) -> bool {
        self.more
    }
}

/// What [`Driver::teardown`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Teardown {
    cancelled: u32,
}

impl Teardown {
    /// The operations that were still running, which teardown cancelled. Each one has ended.
    #[must_use]
    pub const fn cancelled(self) -> u32 {
        self.cancelled
    }
}

/// Wakes the driver from any thread: its wake descriptor becomes readable.
#[derive(Debug, Clone)]
pub struct WakeHandle(Arc<Shared>);

impl WakeHandle {
    /// Makes the wake descriptor of the driver readable, so that its caller turns it.
    pub fn wake(&self) {
        self.0.wake();
    }
}

/// The resources of a driver operation. The driver keeps them until the terminal event of the
/// operation.
#[derive(Debug, Default)]
pub(crate) struct Held {
    /// The loan of the buffer that a receive or a read fills.
    pub(crate) loan: Option<WriteLoan<Backing>>,
    pub(crate) lease: Option<Lease>,
    #[cfg_attr(
        any(not(test), loom),
        expect(dead_code, reason = "no operation takes a charge yet; the tests do")
    )]
    pub(crate) charge: Option<Permit>,
    /// The peer address that an accept writes, and its length.
    pub(crate) peer: SockAddrStorage,
    pub(crate) peer_len: u32,
    /// The socket that an accept, a connect, or a bind gave.
    pub(crate) accepted: Option<Resource>,
    /// The connection of an accept with no consumer, which the listener keeps.
    pub(crate) connection: Option<OwnedFd>,
    /// The home asked the kernel to cancel the operation.
    pub(crate) cancelled: bool,
    /// The bytes that a receive copied from the data that an abandoned receive left.
    pub(crate) copied: usize,
    /// A send on a failed direction gives this error, with no submission.
    pub(crate) failed: Option<WriteFailed>,
    /// The report of a close.
    pub(crate) closed: Option<Closed>,
    /// The regions of a vectored send, and the message header that names them.
    pub(crate) regions: Option<Regions>,
    /// The datagram of a send.
    pub(crate) datagram: Option<Datagram>,
    /// The message of a datagram send or receive, which the kernel reads or writes.
    pub(crate) message: udp::Message,
    /// The facts of a received datagram.
    pub(crate) received: Option<rsnetwork_udp::Received>,
    /// The datagrams of a batch, and its progress while its completions come.
    pub(crate) batch: Option<udp::Batch>,
    pub(crate) progress: udp::Progress,
    /// The next operation that waits in the same direction, in start order.
    pub(crate) next: Option<OpId>,
    /// The C strings of the paths of a file operation, which the kernel reads.
    pub(crate) path: Option<std::ffi::CString>,
    pub(crate) target: Option<std::ffi::CString>,
    /// The structure that a `statx` of a file writes.
    pub(crate) stat: sys::Statx,
    /// The buffers of a vectored read of a file.
    pub(crate) buffers: Option<Buffers>,
    /// The alignment of direct I/O of a file that an open gave.
    pub(crate) alignment: Option<Alignment>,
    /// The waiter of the job of an operation that runs on the blocking service.
    pub(crate) job: Option<crate::blocking::Waiter<i32>>,
    /// An error of the home that the future gives in place of the result of the kernel.
    pub(crate) error: Option<io::ErrorKind>,
}

/// What an operation is: the direction that it uses, and what the home does at its terminal
/// event.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// An operation with no direction, for example a read of a pipe in a test.
    #[default]
    Other,
    /// A receive into the loan of the caller's buffer.
    Recv,
    /// A receive into a buffer that the kernel selects, which becomes the loan.
    Selected,
    /// A send of the lease.
    Send,
    /// A send of the regions, in order.
    Vectored,
    /// A shutdown of the write side of a stream.
    Shutdown,
    /// A connect of a new socket, which `accepted` holds.
    Connect,
    /// A bind and a listen of a new socket, which `accepted` holds.
    Bind,
    /// An accept: the result is a new descriptor.
    Accept,
    /// A receive of one datagram into the loan of the caller's buffer.
    RecvFrom,
    /// A receive of one datagram into a buffer that the kernel selects.
    RecvSelectedFrom,
    /// A receive of several datagrams, one linked receive for each, into buffers that the kernel
    /// selects.
    RecvBatch,
    /// A send of one datagram, or of a batch of segments.
    SendTo,
    /// A send of several datagrams, one linked send for each.
    SendBatch,
    /// A close, which ends after each operation of its resource ends.
    Close,
    /// An operation of a file or a directory, which runs at the same time as the others of its
    /// file.
    File(file::Op),
}

impl Kind {
    /// Whether the operation uses the write direction of a stream. A drop of its future does not
    /// cancel it.
    pub(crate) const fn writes(self) -> bool {
        matches!(
            self,
            Self::Send | Self::Vectored | Self::Shutdown | Self::SendTo | Self::SendBatch
        )
    }

    /// Whether the operation is a batch of linked entries, one completion for each, in order.
    pub(crate) const fn batch(self) -> bool {
        matches!(self, Self::SendBatch | Self::RecvBatch)
    }
}

/// The facts of an operation that the operation table keeps for the home: its resource and its
/// kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tag {
    pub(crate) resource: Option<Resource>,
    pub(crate) kind: Kind,
}

/// Why the driver refused an operation. The caller keeps what it gave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reason {
    /// Each operation slot is in use.
    Operations,
    /// The submission queue is full, also after one submit.
    Queue,
    /// The descriptor is not one that this driver owns, for example after its close.
    Resource,
    /// The driver has no provided buffers ([`Driver::provide`]).
    Buffers,
    /// Each resource slot of the driver is in use.
    Resources,
    /// The descriptor is not a pipe or a FIFO.
    NotPipe,
    /// The pipe end does not have the direction of the adoption: a read end for a writer, or a
    /// write end for a reader.
    Direction,
    /// The operation does not apply to this resource, for example a half-close of a pipe.
    Unsupported,
    /// The buffer, the length, or the offset of a direct-I/O operation does not have the
    /// [`Alignment`] of its file.
    Alignment,
    /// The operation runs on the blocking service on this kernel ([`Driver::blocking_work`]),
    /// and the driver has no service ([`Driver::use_blocking`]), or the service refused the job.
    Blocking,
}

/// A refused operation: the reason, and the value that the caller gave, which it keeps.
#[derive(Debug)]
pub struct Refused<T> {
    reason: Reason,
    value: T,
}

impl<T> Refused<T> {
    /// Why the driver refused.
    #[must_use]
    pub fn reason(&self) -> Reason {
        self.reason
    }

    /// The value that the caller gave.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> fmt::Display for Refused<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the driver refused the operation: {:?}", self.reason)
    }
}

impl<T: fmt::Debug> core::error::Error for Refused<T> {}

/// The driver ended before the operation gave its result to its future: teardown released the
/// result, or the driver dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ended;

impl fmt::Display for Ended {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the driver ended before the operation gave its result")
    }
}

impl core::error::Error for Ended {}

/// The future of one operation. It can move to another thread while the driver stays driven, and
/// it wakes the waker of its latest poll. A drop ends the interest, not the operation: the driver
/// keeps the buffer until the kernel releases it.
///
/// The future borrows the direction of its stream, or its listener, for `'a`, so no other
/// operation can start in that direction while it exists.
///
/// Poll the future while you turn the driver, for example with this helper:
///
/// ```
/// use std::future::Future;
/// use std::pin::pin;
/// use std::task::{Context, Poll, Waker};
///
/// use rsasync::driver::Driver;
///
/// /// Polls `future` while the driver turns, until it gives its output.
/// fn run<F: Future>(driver: &mut Driver, future: F) -> std::io::Result<F::Output> {
///     let mut future = pin!(future);
///     let mut cx = Context::from_waker(Waker::noop());
///     loop {
///         if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
///             return Ok(output);
///         }
///         if !driver.turn()?.more() {
///             if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
///                 return Ok(output);
///             }
///             driver.wait()?;
///         }
///     }
/// }
/// ```
///
/// # Panics
///
/// A poll after the future gave `Ready` panics, as for the futures of the standard library.
pub struct Operation<'a, T> {
    completion: Completion,
    output: fn(i32, Held) -> Option<T>,
    direction: PhantomData<&'a mut ()>,
}

impl<T> Operation<'_, T> {
    fn new(completion: Completion, output: fn(i32, Held) -> Option<T>) -> Self {
        Self {
            completion,
            output,
            direction: PhantomData,
        }
    }

    /// Asks the driver to cancel the operation. The future keeps its interest: it later gives
    /// the result, with the original buffer and its exact progress. A receive gives the bytes
    /// that it received before the cancel took effect, with no copy, and a send leaves its
    /// direction usable.
    pub fn cancel(&self) {
        self.completion.cancel();
    }
}

impl<T> Future for Operation<'_, T> {
    type Output = Result<T, Ended>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let output = self.output;
        Pin::new(&mut self.completion)
            .poll(cx)
            .map(|taken| taken.ok().and_then(|(result, held)| output(result, held)).ok_or(Ended))
    }
}

impl<T> fmt::Debug for Operation<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Operation")
            .field("completion", &self.completion)
            .finish_non_exhaustive()
    }
}

/// A descriptor that the driver owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Resource {
    index: u32,
    generation: u32,
}

/// One descriptor slot of the driver.
#[derive(Debug, Default)]
struct Owned {
    generation: u32,
    fd: Option<OwnedFd>,
    /// The directions of a stream or a listener, and what they keep.
    directions: tcp::Directions,
    /// What the descriptor is, which selects the calls of its operations.
    medium: Medium,
    /// The operations of a file that run, which its close waits for.
    running: u32,
}

/// What a resource of the driver is.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Medium {
    /// A TCP stream or listener: its operations are receives and sends.
    #[default]
    Tcp,
    /// A pipe end: its operations are reads and writes, because a receive and a send need a
    /// socket. It has no half-close.
    Pipe,
    /// A UDP socket: its operations are datagram receives and sends. It has no half-close.
    Udp,
    /// A file or a directory: its operations are positional, and several run at one time.
    File,
}

/// The descriptors that the driver owns, in a slice that it allocates once.
#[derive(Debug)]
struct Resources(Box<[Owned]>);

impl Resources {
    /// Takes `fd`, a descriptor of `medium`. Refuses with `fd` when each slot is in use.
    fn adopt(&mut self, fd: OwnedFd, medium: Medium) -> Result<Resource, OwnedFd> {
        let Some((index, slot)) = (0..).zip(self.0.iter_mut()).find(|(_, slot)| slot.fd.is_none()) else {
            return Err(fd);
        };
        slot.fd = Some(fd);
        slot.medium = medium;
        Ok(Resource {
            index,
            generation: slot.generation,
        })
    }

    fn get(&mut self, resource: Resource) -> Option<&mut Owned> {
        self.0
            .get_mut(resource.index as usize)
            .filter(|slot| slot.generation == resource.generation && slot.fd.is_some())
    }

    /// The descriptor of `resource`, while the driver owns it.
    fn borrow(&self, resource: Resource) -> Option<BorrowedFd<'_>> {
        self.0
            .get(resource.index as usize)
            .filter(|slot| slot.generation == resource.generation)?
            .fd
            .as_ref()
            .map(AsFd::as_fd)
    }

    /// Ends `resource` and gives its descriptor. The identity is stale from now on.
    fn release(&mut self, resource: Resource) -> Option<OwnedFd> {
        let slot = self.get(resource)?;
        slot.generation = slot.generation.wrapping_add(1);
        slot.directions = tcp::Directions::default();
        slot.running = 0;
        slot.fd.take()
    }
}

/// The buffer group of the provided ring.
const GROUP: u16 = 0;

/// The user data of the poll of the interrupt descriptor that a wait keeps in the ring.
const WAKE: u64 = CONTROL;

/// The user data of the completion that a wake message posts at its home. It only ends a wait.
const ARRIVED: u64 = 1 << 62;

/// The user data of the completion of a wake message at its sender, with the index of its entry
/// in `Driver::messages` in the low bits.
const MESSAGE: u64 = (1 << 62) | (1 << 61);

/// The wake messages that a driver keeps at one time, until their completions. A power of two,
/// so the index of a message is the low bits of its user data.
pub(crate) const MESSAGES: usize = 64;

/// The bits of the index of a message in its user data.
const MESSAGE_INDEX: u64 = MESSAGES as u64 - 1;

/// The home of a local owner whose ring is a driver: the target of a wake from another thread.
pub(crate) struct Home {
    /// A duplicate of the ring descriptor. A message never names a closed descriptor, whose number
    /// another file could take.
    ring: OwnedFd,
    shared: Arc<Shared>,
}

impl Home {
    /// Ends a wait of the home from a thread with no ring: it writes the interrupt descriptor,
    /// which the wait polls in the ring.
    pub(crate) fn interrupt(&self) {
        // A full counter is a pending wake already, and nothing else can fail here.
        drop((&self.shared.interrupt).write(&1_u64.to_ne_bytes()));
    }

    /// Takes the writes of the interrupt descriptor since the last take, or since the last turn of
    /// the driver.
    #[cfg(test)]
    pub(crate) fn take_interrupts(&self) -> u64 {
        let mut count = [0; 8];
        (&self.shared.interrupt)
            .read(&mut count)
            .map_or(0, |_| u64::from_ne_bytes(count))
    }
}

/// The provided buffers: the slots of a pool, which the kernel selects for a receive. Each one is
/// in the kernel ring, or in a lease of a consumer, or back in the pool.
#[derive(Debug)]
struct Selected {
    pool: Pool,
    ring: BufferRing,
    /// The buffer of each buffer identifier that the kernel ring holds.
    buffers: Box<[Option<Backing>]>,
    /// The identifiers with no buffer, which a refill fills from the pool.
    free: Vec<u16>,
    /// The capacity of each slot.
    len: u32,
}

impl Selected {
    /// Takes each slot of `pool`, up to 32768, and gives each to a new provided ring of `ring`.
    fn new(ring: &Ring, mut pool: Pool) -> io::Result<Self> {
        let mut buffers = Vec::new();
        while buffers.len() < 1 << 15 {
            match pool.acquire() {
                Ok(backing) => buffers.push(Some(backing)),
                Err(_) => break,
            }
        }
        let count = u16::try_from(buffers.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let len = buffers
            .first()
            .and_then(|buffer| buffer.as_ref())
            .and_then(|buffer| u32::try_from(buffer.capacity()).ok())
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut selected = Self {
            ring: BufferRing::register(ring, count.next_power_of_two(), GROUP)?,
            pool,
            buffers: buffers.into_boxed_slice(),
            free: Vec::with_capacity(usize::from(count)),
            len,
        };
        for (bid, buffer) in (0..).zip(selected.buffers.iter_mut()) {
            if let Some(buffer) = buffer {
                selected.ring.add(bid, buffer.as_mut_ptr(), len);
            }
        }
        selected.ring.publish();
        Ok(selected)
    }

    /// Takes the buffer that the kernel selected as `bid`.
    fn take(&mut self, bid: u16) -> Option<Backing> {
        let buffer = self.buffers.get_mut(usize::from(bid))?.take()?;
        self.free.push(bid);
        Some(buffer)
    }

    /// Gives each slot that came back to the pool to the kernel ring again.
    fn refill(&mut self) {
        let mut added = false;
        while let Some(&bid) = self.free.last() {
            let Ok(mut buffer) = self.pool.acquire() else {
                break;
            };
            self.free.pop();
            self.ring.add(bid, buffer.as_mut_ptr(), self.len);
            if let Some(slot) = self.buffers.get_mut(usize::from(bid)) {
                *slot = Some(buffer);
            }
            added = true;
        }
        if added {
            self.ring.publish();
        }
    }
}

/// The state that the futures and the wake handles share with the home.
struct Shared {
    entries: Box<[Entry<Held>]>,
    requests: Requests,
    /// The wake descriptor: an `eventfd` that the kernel also signals at each completion and for
    /// its deferred work.
    wake: fs::File,
    /// An `eventfd` that only the other threads signal. A wait polls it in the ring. A signal of
    /// the kernel would also complete that poll, so the wait does not poll `wake`.
    interrupt: fs::File,
}

impl Shared {
    fn wake(&self) {
        for fd in [&self.wake, &self.interrupt] {
            // A full counter is a pending wake already, and nothing else can fail here.
            drop((&*fd).write(&1_u64.to_ne_bytes()));
        }
    }

    /// Clears the wake descriptors. Gives `true` when the wake descriptor had a wake.
    fn clear(&self) -> bool {
        let mut count = [0; 8];
        // A descriptor with no wake gives `WouldBlock`, which is the clear state.
        drop((&self.interrupt).read(&mut count));
        (&self.wake).read(&mut count).is_ok()
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("operations", &self.entries.len())
            .finish_non_exhaustive()
    }
}

/// An io_uring driver that its caller owns and drives. It stays on the thread that created it.
pub struct Driver {
    ring: Ring,
    table: Table<Tag>,
    shared: Arc<Shared>,
    resources: Resources,
    /// The requests that a take gives, applied after the take: at most two for each slot.
    taken: Vec<Taken<Tag>>,
    /// The resources whose directions need the home after an operation ended: an operation waits
    /// for that one, or a close waits for it. Each resource is in it at most one time.
    pending: Vec<Resource>,
    /// The bytes that receives copied from the data that an abandoned receive left.
    copied: u64,
    selected: Option<Selected>,
    /// The later operations of the kernel, from the probe of the ring.
    features: Features,
    completions_per_turn: u32,
    /// The ring polls the interrupt descriptor, so that a request or wake from another thread
    /// ends a wait.
    waking: bool,
    /// The kernel signals the wake descriptor for deferred work only one time in each
    /// registration (`probe`), so each turn that takes a wake registers it again.
    rearm_wake: bool,
    /// The signal streams whose home is this driver.
    signals: Vec<Arc<signal::Home>>,
    /// The blocking service of the operations that the kernel cannot run
    /// ([`Driver::use_blocking`]), and the waker that its jobs wake.
    blocking: Option<(crate::blocking::Submitter, core::task::Waker)>,
    /// The operations that run on the blocking service, which each turn polls.
    jobs: Vec<OpId>,
    /// The home of a local owner on this driver.
    home: Arc<Home>,
    /// The wake messages that this driver sent and that have not completed: the home of each.
    /// Each one keeps the descriptor of its target open until its completion.
    messages: [Option<Arc<Home>>; MESSAGES],
}

impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Driver")
            .field("ring", &self.ring)
            .field("resources", &self.resources.0.len())
            .field("selected", &self.selected)
            .field("completions_per_turn", &self.completions_per_turn)
            .field("rearm_wake", &self.rearm_wake)
            .finish_non_exhaustive()
    }
}

impl Driver {
    /// Starts a driver with the limits of `config`. It allocates here, one time. It opens four
    /// descriptors: the ring, a duplicate of the ring that is the target of wake messages, the
    /// wake descriptor, and the interrupt descriptor.
    ///
    /// # Errors
    ///
    /// [`Error::Refused`] when the kernel refuses io_uring, [`Error::Config`] for a limit that is
    /// 0 or too large, and [`Error::System`] for another failure.
    pub fn new(config: Config) -> Result<Self, Error> {
        if config.operations == 0
            || config.operations > MAX_SLOTS
            || config.resources == 0
            || config.completions_per_turn == 0
        {
            return Err(Error::Config);
        }
        let completions = config.operations.max(config.submissions).saturating_mul(2).min(1 << 16);
        let mut ring = Ring::new(config.submissions, completions).map_err(|error| match error {
            SetupError::Refused(error) => Error::Refused(error),
            SetupError::Failed(error) if error.kind() == io::ErrorKind::InvalidInput => Error::Config,
            SetupError::Failed(error) => Error::System(error),
        })?;
        // The probe needs a new ring, with no entry, completion, or buffer group.
        let features = ring.probe().map_err(Error::System)?;
        let (wake, interrupt) = (eventfd().map_err(Error::System)?, eventfd().map_err(Error::System)?);
        let wake = fs::File::from(wake);
        ring.register_eventfd(wake.as_raw_fd()).map_err(Error::System)?;
        let rearm_wake = probe::signal_stops_after_first(&mut ring, &wake).map_err(Error::System)?;
        let table = Table::new(config.operations);
        let shared = Arc::new(Shared {
            entries: (0..config.operations).map(|_| Entry::new()).collect(),
            requests: table.requests(),
            wake,
            interrupt: fs::File::from(interrupt),
        });
        let home = Arc::new(Home {
            ring: ring.duplicate().map_err(Error::System)?,
            shared: Arc::clone(&shared),
        });
        Ok(Self {
            ring,
            table,
            shared,
            resources: Resources((0..config.resources).map(|_| Owned::default()).collect()),
            taken: Vec::with_capacity(config.operations as usize * 2),
            pending: Vec::with_capacity(config.resources as usize),
            copied: 0,
            selected: None,
            features,
            completions_per_turn: config.completions_per_turn,
            waking: false,
            rearm_wake,
            signals: Vec::new(),
            blocking: None,
            jobs: Vec::with_capacity(config.operations as usize),
            home,
            messages: [const { None }; MESSAGES],
        })
    }

    /// The helper threads of the driver. The io_uring driver has none.
    #[must_use]
    pub const fn helper_threads(&self) -> u32 {
        0
    }

    /// The bytes that receives copied, in total, from the data that a dropped or forgotten receive
    /// left. Each other byte of a receive goes from the kernel to its buffer with no copy.
    #[must_use]
    pub const fn copied(&self) -> u64 {
        self.copied
    }

    /// A handle that wakes the driver from any thread.
    #[must_use]
    pub fn wake_handle(&self) -> WakeHandle {
        WakeHandle(Arc::clone(&self.shared))
    }

    /// The home of a local owner whose ring is this driver.
    pub(crate) fn home(&self) -> Arc<Home> {
        Arc::clone(&self.home)
    }

    /// Sends a wake message from this ring to `home`: the next submit sends it, and its completion
    /// at `home` ends a wait there. The driver keeps `home` until the completion of the message at
    /// this ring, and a failed message writes the interrupt descriptor of `home` instead. Gives
    /// `home` back when no entry or no room in the queue is free.
    #[expect(unsafe_code, reason = "a message names no memory")]
    pub(crate) fn message(&mut self, home: Arc<Home>) -> Result<(), Arc<Home>> {
        let Some(index) = self.messages.iter().position(Option::is_none) else {
            return Err(home);
        };
        let user_data = MESSAGE | index as u64;
        let entry = Sqe::message(home.ring.as_raw_fd(), ARRIVED, 0, user_data);
        // SAFETY: a message names no memory. The entry of `index` keeps the descriptor of the
        // target open until the completion of the message.
        if unsafe { self.ring.push(&entry) }.is_err() {
            return Err(home);
        }
        self.messages[index] = Some(home);
        Ok(())
    }

    /// Writes the interrupt descriptor of the home of each message whose completion this driver
    /// has not taken, before a time with no turn: such a message can still be in the queue, or it
    /// can fail with no turn that sees it. A message that arrives then wakes its home one more time.
    /// Each entry stays until the completion of its message, so a later message never takes the
    /// index of a message that the kernel can still complete.
    pub(crate) fn settle_messages(&self) {
        for home in self.messages.iter().flatten() {
            home.interrupt();
        }
    }

    /// A stream of `signal`, whose home is this driver: a turn wakes it when the signal arrived.
    /// The first stream of a signal installs its handler, and the last stream that closes restores
    /// the earlier disposition. The driver connects no signal to its own end.
    ///
    /// # Errors
    ///
    /// [`SignalError`] gives the reason, and nothing changes.
    pub fn signal(&mut self, signal: Signal, escalation: Escalation) -> Result<SignalStream, SignalError> {
        let (stream, home) = signal::register(&self.shared, signal, escalation)?;
        if self.signals.capacity() == 0 {
            self.signals.reserve_exact(signal::STREAMS);
        }
        self.signals.push(home);
        Ok(stream)
    }

    /// Gives the slots of `pool` to the kernel as the buffers of [`Driver::recv_selected`]: a
    /// receive then takes a buffer only when its data comes. A selected buffer comes back to the
    /// kernel when its last lease or view drops, on any thread, at the next turn.
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
    /// use core::alloc::Layout;
    /// use core::num::{NonZeroU32, NonZeroU64};
    ///
    /// use rsasync::buffer::Pool;
    /// use rsasync::capacity::Budget;
    ///
    /// // The pool charges its memory to a budget: 64 slots of 4096 bytes.
    /// let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    /// let slot = Layout::from_size_align(4096, 4096)?;
    /// driver.provide(Pool::new(&budget, NonZeroU32::new(64).unwrap(), slot)?)?;
    /// let receive = driver.recv_selected(&mut stream)?;
    /// let request = run(&mut driver, receive)???;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// `AlreadyExists` when the driver has its buffers, `InvalidInput` for a pool with no free slot
    /// or a slot above `u32::MAX` bytes, and an error of the registration.
    pub fn provide(&mut self, pool: Pool) -> io::Result<()> {
        if self.selected.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        self.selected = Some(Selected::new(&self.ring, pool)?);
        Ok(())
    }

    /// One bounded turn. It never blocks.
    ///
    /// # Errors
    ///
    /// A failure of the kernel entry other than an interrupt or a full completion queue, which
    /// leave work for the next turn.
    pub fn turn(&mut self) -> io::Result<Turn> {
        self.take_requests();
        self.poll_jobs();
        if let Some(selected) = &mut self.selected {
            selected.refill();
        }
        let mut more = false;
        if self.ring.unsubmitted() > 0 || self.ring.needs_enter() {
            match self.ring.submit(0) {
                Ok(_) => {}
                Err(error) if retryable(&error) => more = true,
                Err(error) => return Err(error),
            }
        }
        let completed = self.harvest(self.completions_per_turn);
        signal::wake(&mut self.signals);
        // Start the operations that waited for an operation that ended, and end the closes.
        self.attend();
        // Clear the wake descriptor, then look again: work that came after the harvest has woken
        // the descriptor already, so this turn reports it, or a later wake comes.
        if self.shared.clear() && self.rearm_wake {
            // On a kernel with the defect of `probe`, the next deferred work signals only a new
            // registration. Deferred work that comes before the new registration sets
            // `IORING_SQ_TASKRUN` before it tries the signal, so the look below sees it.
            rearm(&self.ring, &self.shared.wake)?;
        }
        more |= self.ring.ready()
            || self.ring.needs_enter()
            || self.ring.unsubmitted() > 0
            || !self.shared.requests.is_empty();
        Ok(Turn { completed, more })
    }

    /// Blocks in the kernel until a completion is ready, or a request or wake from another thread
    /// came. The kernel runs its deferred work during the wait, so the wait does not depend on a
    /// signal of the wake descriptor for that work.
    ///
    /// # Errors
    ///
    /// A failure of the kernel entry other than an interrupt or a full queue, which leave work for
    /// the next turn.
    #[expect(unsafe_code, reason = "a poll names no memory")]
    pub fn wait(&mut self) -> io::Result<()> {
        // A wait in the kernel ends only at a completion, so a request or a wake from another
        // thread, which only writes a descriptor, would not end it. That thread also writes the
        // interrupt descriptor, and the poll of it in the ring then completes. The poll does not
        // watch the wake descriptor, because the kernel signals that one at each completion.
        if !self.waking {
            let poll = Sqe::poll_readable(self.shared.interrupt.as_raw_fd()).with_user_data(WAKE);
            // SAFETY: a poll names no memory.
            match unsafe { push_now(&mut self.ring, &poll) } {
                Ok(()) => self.waking = true,
                // A full queue leaves work for the next turn.
                Err(error) if retryable(&error) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        loop {
            match self.ring.submit(1) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if !retryable(&error) => return Err(error),
                _ => return Ok(()),
            }
        }
    }

    /// Cancels each running operation, drives until each terminal event, and closes each
    /// resource. Each future that has not taken its result then gives [`Ended`]. A job of the
    /// blocking service ([`Driver::blocking_work`]) cannot be cancelled, so the teardown waits until
    /// it returns.
    ///
    /// # Errors
    ///
    /// A failure of the kernel entry other than an interrupt.
    pub fn teardown(mut self) -> io::Result<Teardown> {
        // An operation that waits for the one before it never ran, so it ends now.
        for index in 0..self.resources.0.len() {
            let Some(resource) = self.resources.0.get(index).map(|owned| Resource {
                index: u32::try_from(index).unwrap_or(u32::MAX),
                generation: owned.generation,
            }) else {
                continue;
            };
            for kind in [Kind::Recv, Kind::Send] {
                while let Some(id) = self.dequeue(resource, kind) {
                    self.complete_now(id, -ECANCELED);
                }
            }
        }
        let mut cancelled = 0_u32;
        for id in self.table.running() {
            // A close ends when the operations of its resource end.
            if self.table.resources(id).is_some_and(|tag| tag.kind == Kind::Close) {
                continue;
            }
            push_cancel(&mut self.ring, id)?;
            cancelled = cancelled.saturating_add(1);
        }
        while self.table.busy() {
            self.take_requests();
            self.poll_jobs();
            if !self.table.busy() {
                break;
            }
            // The wait also ends when a job of the blocking service ends, because the job wakes
            // the interrupt descriptor.
            self.wait()?;
            self.harvest(u32::MAX);
            self.attend();
        }
        self.take_requests();
        Ok(Teardown { cancelled })
    }

    /// Takes `fd`, a descriptor of `medium`, so that teardown closes it. Refuses with `fd` when
    /// each resource slot is in use.
    fn adopt(&mut self, fd: OwnedFd, medium: Medium) -> Result<Resource, OwnedFd> {
        self.resources.adopt(fd, medium)
    }

    /// The descriptor of `resource`, while the driver owns it.
    #[cfg(all(test, not(loom)))]
    pub(crate) fn fd(&mut self, resource: Resource) -> Option<std::os::fd::RawFd> {
        Some(self.resources.get(resource)?.fd.as_ref()?.as_raw_fd())
    }

    /// Submits an operation of no direction on `resource` with the resources that `held` makes
    /// from `value`, and gives its future. `entry` makes the submission from the resources in
    /// their final place. A drop of the future asks for a cancel when `cancel_on_drop` holds. A
    /// refusal gives back `value`.
    ///
    /// # Safety
    ///
    /// The memory that the entry names belongs to the resources, and each descriptor that it names
    /// is `resource`. The memory keeps its address while the resources move.
    #[cfg(all(test, not(loom)))]
    #[expect(unsafe_code, reason = "the kernel uses the memory that the entry names")]
    pub(crate) unsafe fn submit<P>(
        &mut self,
        value: P,
        resource: Option<Resource>,
        held: impl FnOnce(P) -> Held,
        entry: impl FnOnce(&mut Held) -> Sqe,
        cancel_on_drop: bool,
    ) -> Result<Completion, Refused<P>> {
        let refuse = |reason, value| Err(Refused { reason, value });
        if let Some(resource) = resource
            && self.resources.get(resource).is_none()
        {
            return refuse(Reason::Resource, value);
        }
        let tag = Tag {
            resource,
            kind: Kind::Other,
        };
        let id = self
            .open(tag, 1, value, held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        // SAFETY: the caller's contract, and the queue has room.
        unsafe { self.push(id, entry) };
        Ok(Completion::new(Arc::clone(&self.shared), id, cancel_on_drop))
    }

    /// Whether the submission queue has room for `entries` entries, after one submit if it does
    /// not.
    fn room(&mut self, entries: u32) -> bool {
        self.ring.room() >= entries || (self.ring.submit(0).is_ok() && self.ring.room() >= entries)
    }

    /// Records a new operation at the home, before its submission, with the resources that
    /// `held` makes from `value`, and with room in the queue for its `entries` submission entries.
    /// Refuses with `value` when each operation slot is in use or the queue has no room.
    pub(crate) fn open<P>(
        &mut self,
        tag: Tag,
        entries: u32,
        value: P,
        held: impl FnOnce(P) -> Held,
    ) -> Result<OpId, (Reason, P)> {
        self.open_with(tag, entries, value, |value| Ok(held(value)))
    }

    /// [`Driver::open`] with resources that `held` can refuse to make, for example when a job of
    /// the operation does not start. The refusal changes nothing.
    pub(crate) fn open_with<P>(
        &mut self,
        tag: Tag,
        entries: u32,
        value: P,
        held: impl FnOnce(P) -> Result<Held, (Reason, P)>,
    ) -> Result<OpId, (Reason, P)> {
        // The table counts the completions of a batch until its last one.
        let shape = if tag.kind.batch() {
            Shape::Multishot
        } else {
            Shape::Single
        };
        let id = match self.table.begin(tag, shape) {
            Ok(id) => id,
            Err(_) => {
                // Futures that ended give their slots back at the next take.
                self.take_requests();
                match self.table.begin(tag, shape) {
                    Ok(id) => id,
                    Err(_) => return Err((Reason::Operations, value)),
                }
            }
        };
        // The check of room comes after the take of requests, because their cancels and the
        // operations that they let start push entries.
        if !self.room(entries) {
            self.table.withdraw(id);
            return Err((Reason::Queue, value));
        }
        let Some(slot) = self.shared.entries.get(id.index() as usize) else {
            self.table.withdraw(id);
            return Err((Reason::Operations, value));
        };
        match held(value) {
            Ok(held) => {
                slot.start(id, held, |_| ());
                Ok(id)
            }
            Err(refused) => {
                self.table.withdraw(id);
                Err(refused)
            }
        }
    }

    /// Submits the operation `id`: `entry` makes its submission from its resources at the home.
    ///
    /// # Safety
    ///
    /// The memory that the entry names belongs to the resources of `id`, and each descriptor that
    /// it names stays open until the terminal event of `id`. The queue has room.
    #[expect(unsafe_code, reason = "the kernel uses the memory that the entry names")]
    pub(crate) unsafe fn push(&mut self, id: OpId, entry: impl FnOnce(&mut Held) -> Sqe) {
        let Some(slot) = self.shared.entries.get(id.index() as usize) else {
            return;
        };
        let mut sqe = None;
        slot.update(id, |held| sqe = held.as_mut().map(entry));
        if let Some(sqe) = sqe {
            // SAFETY: the entry of the slot keeps the resources until the terminal event of `id`,
            // and the caller keeps the descriptor open. The queue has room.
            let pushed = unsafe { self.ring.push(&sqe.with_user_data(id.user_data())) }.is_ok();
            debug_assert!(pushed, "the queue has room for the entry");
        }
    }

    /// Ends the operation `id` at the home with `result`, with no kernel event, as its terminal
    /// event.
    pub(crate) fn complete_now(&mut self, id: OpId, result: i32) {
        let event = Event {
            result,
            more: false,
            notice: false,
        };
        let outcome = self.table.complete(id.user_data(), event);
        self.ended(id, result, outcome);
    }

    /// Applies the outcome of a terminal event of `id`: wakes the consumer, or applies the
    /// abandoned rule.
    fn ended(&mut self, id: OpId, result: i32, outcome: Outcome<Tag>) {
        let Some(slot) = self.shared.entries.get(id.index() as usize) else {
            return;
        };
        let tag = match &outcome {
            Outcome::Ready => self.table.resources(id).copied(),
            Outcome::Abandoned(Done { resources, .. }) => *resources,
            _ => None,
        };
        if let Some(tag) = tag {
            file::ended(&mut self.resources, &mut self.pending, tag);
        }
        match outcome {
            Outcome::Ready => slot.publish(id, result),
            Outcome::Abandoned(Done {
                resources: Some(tag), ..
            }) => {
                if let Some(held) = slot.reclaim(id) {
                    tcp::abandon(&mut self.resources, tag, result, held);
                }
                tcp::after(&mut self.resources, &mut self.pending, tag);
            }
            _ => {}
        }
    }

    /// Applies the requests that other threads posted.
    fn take_requests(&mut self) {
        let mut taken = mem::take(&mut self.taken);
        self.table.take_requests(|request| taken.push(request));
        for request in taken.drain(..) {
            match request {
                Taken::Cancel(id) => self.cancel(id),
                Taken::Abandoned(id, ended) => self.claimed(id, ended),
            }
        }
        self.taken = taken;
        self.attend();
    }

    /// The consumer of `id` ended its interest after the operation ended. If it took the result,
    /// the home only frees the slot. If it did not, the home applies the abandoned rule.
    fn claimed(&mut self, id: OpId, ended: Done<Tag>) {
        let held = self
            .shared
            .entries
            .get(id.index() as usize)
            .and_then(|slot| slot.reclaim(id));
        if let Some(tag) = ended.resources {
            match held {
                Some(held) => tcp::abandon(&mut self.resources, tag, ended.result, held),
                None => tcp::taken(&mut self.resources, tag),
            }
            tcp::after(&mut self.resources, &mut self.pending, tag);
        }
    }

    /// Asks the kernel to cancel the running operation `id`, once. An operation that waits for
    /// the one before it never ran, so it ends now.
    fn cancel(&mut self, id: OpId) {
        let Some(&tag) = self.table.resources(id) else {
            return;
        };
        if tag.kind == Kind::Close {
            return;
        }
        if self.unqueue(tag, id) {
            self.complete_now(id, -ECANCELED);
            return;
        }
        let mut first = false;
        if let Some(slot) = self.shared.entries.get(id.index() as usize) {
            slot.update(id, |held| {
                if let Some(held) = held {
                    first = !held.cancelled;
                    held.cancelled = true;
                }
            });
        }
        if first && push_cancel(&mut self.ring, id).is_err() {
            // The queue stays full: the next turn takes the request again.
            if let Some(slot) = self.shared.entries.get(id.index() as usize) {
                slot.update(id, |held| {
                    if let Some(held) = held {
                        held.cancelled = false;
                    }
                });
            }
            self.shared.requests.post(id, Request::Cancel);
        }
    }

    /// Gives up to `limit` ready completions to their operations. Gives their number.
    fn harvest(&mut self, limit: u32) -> u32 {
        let mut completed = 0;
        let (shared, table, pending) = (&self.shared, &mut self.table, &mut self.pending);
        let (resources, selected, waking) = (&mut self.resources, &mut self.selected, &mut self.waking);
        let messages = &mut self.messages;
        for cqe in self.ring.completions().take(limit as usize) {
            completed += 1;
            let mut event = cqe.event();
            // A completion of a batch is one of its entries: its last one ends the batch, and the
            // result of the batch is its count.
            if let Some(id) = OpId::from_user_data(cqe.user_data)
                && table.resources(id).is_some_and(|tag| tag.kind.batch())
                && table.runs(id)
                && let Some(slot) = shared.entries.get(id.index() as usize)
            {
                let buffer = cqe
                    .buffer()
                    .zip(selected.as_mut())
                    .and_then(|(bid, selected)| selected.take(bid));
                slot.update(id, |held| {
                    if let Some(held) = held {
                        (event.more, event.result) = udp::batch_completion(held, cqe.res, buffer);
                    }
                });
            }
            let outcome = table.complete(cqe.user_data, event);
            let (id, abandoned, tag) = match (outcome, OpId::from_user_data(cqe.user_data)) {
                // The poll of the interrupt descriptor ended: the next wait polls it again.
                (Outcome::Control(WAKE), _) => {
                    *waking = false;
                    continue;
                }
                // A wake message of this driver completed. If it failed, it did not reach its
                // home, and the interrupt descriptor wakes the home instead.
                (Outcome::Control(data), _) if data & !MESSAGE_INDEX == MESSAGE => {
                    let index = usize::try_from(data & MESSAGE_INDEX).ok();
                    if let Some(home) = index.and_then(|index| messages.get_mut(index)?.take())
                        && cqe.res < 0
                    {
                        home.interrupt();
                    }
                    continue;
                }
                (Outcome::Ready, Some(id)) => match table.resources(id) {
                    Some(&tag) => (id, false, tag),
                    None => continue,
                },
                (
                    Outcome::Abandoned(Done {
                        resources: Some(tag), ..
                    }),
                    Some(id),
                ) => (id, true, tag),
                // No driver operation is multishot or a zero-copy send yet. The other outcomes,
                // such as a wake message that `ARRIVED`, change nothing.
                _ => continue,
            };
            let Some(slot) = shared.entries.get(id.index() as usize) else {
                continue;
            };
            slot.update(id, |held| {
                if let Some(held) = held {
                    finish(held, tag, &cqe, abandoned, resources, selected.as_mut());
                }
            });
            file::ended(resources, pending, tag);
            if abandoned {
                if let Some(held) = slot.reclaim(id) {
                    tcp::abandon(resources, tag, event.result, held);
                }
                tcp::after(resources, pending, tag);
            } else {
                slot.publish(id, event.result);
            }
        }
        completed
    }
}

/// `ECANCELED` (`include/uapi/asm-generic/errno.h`).
pub(crate) const ECANCELED: i32 = 125;

/// The home finishes the operation of `held` at its terminal event `cqe`, before its consumer
/// takes the result, or before the home applies the abandoned rule.
#[expect(unsafe_code, reason = "the kernel initialized the bytes that the result counts")]
fn finish(
    held: &mut Held,
    tag: Tag,
    cqe: &Cqe,
    abandoned: bool,
    resources: &mut Resources,
    selected: Option<&mut Selected>,
) {
    let count = usize::try_from(cqe.res);
    match tag.kind {
        Kind::File(op) => file::finish(held, op, cqe.res, abandoned, resources),
        Kind::Other
        | Kind::Send
        | Kind::Vectored
        | Kind::Shutdown
        | Kind::Close
        | Kind::SendTo
        | Kind::SendBatch
        | Kind::RecvBatch => {}
        Kind::RecvFrom => {
            if let (Ok(count), Some(loan)) = (count, held.loan.as_mut()) {
                // SAFETY: the receive named the capacity of the loan in its message, and the kernel
                // wrote `count` bytes at its start, at most that capacity.
                unsafe { loan.set_filled(count) };
                held.received = held.message.received(count).ok();
            }
        }
        Kind::RecvSelectedFrom => {
            if let (Ok(count), Some(buffer)) = (
                count,
                cqe.buffer()
                    .zip(selected)
                    .and_then(|(bid, selected)| selected.take(bid)),
            ) {
                let mut loan = WriteLoan::new(buffer);
                // SAFETY: a slot of the pool has no initialized bytes, so the loan starts at the
                // start of the buffer. The kernel wrote the bytes that the result counts there, at
                // most the slot capacity that the message names.
                unsafe { loan.set_filled(count) };
                held.loan = Some(loan);
                held.received = held.message.received(count).ok();
            }
        }
        // A failed connect or bind closes its socket. The abandoned rule closes the socket of an
        // operation with no consumer.
        Kind::Connect | Kind::Bind if abandoned => {}
        Kind::Connect | Kind::Bind => match held.accepted {
            Some(socket) if cqe.res < 0 => {
                drop(resources.release(socket));
                held.accepted = None;
            }
            // A listener gives its local address, for example the port of a bind to port 0.
            Some(socket) if tag.kind == Kind::Bind => {
                if let Some((address, len)) = resources
                    .get(socket)
                    .and_then(|owned| tcp::local_address(owned.fd.as_ref()?))
                {
                    (held.peer, held.peer_len) = (address, len);
                }
            }
            _ => {}
        },
        Kind::Recv => {
            if let (Ok(count), Some(loan)) = (count, held.loan.as_mut()) {
                // SAFETY: the receive named the capacity of the loan, and the kernel wrote `count`
                // bytes at its start, at most that capacity.
                unsafe { loan.set_filled(count) };
            }
        }
        Kind::Selected => {
            if let Some(buffer) = cqe
                .buffer()
                .zip(selected)
                .and_then(|(bid, selected)| selected.take(bid))
            {
                let mut loan = WriteLoan::new(buffer);
                // SAFETY: a slot of the pool has no initialized bytes, so the loan starts at the
                // start of the buffer. The kernel wrote the bytes that the result counts there, at
                // most the slot capacity that the ring names.
                unsafe { loan.set_filled(count.unwrap_or(0)) };
                held.loan = Some(loan);
            }
        }
        Kind::Accept => {
            if cqe.res >= 0 {
                // SAFETY: the kernel gave a new descriptor that nothing else owns.
                let fd = unsafe { OwnedFd::from_raw_fd(cqe.res) };
                if abandoned {
                    held.connection = Some(fd);
                } else {
                    match resources.adopt(fd, Medium::Tcp) {
                        Ok(stream) => held.accepted = Some(stream),
                        // With no free resource slot, the listener keeps the connection for the
                        // next accept, and this one gives `OutOfMemory`.
                        Err(fd) => {
                            if let Some(listener) = tag.resource.and_then(|listener| resources.get(listener)) {
                                listener.directions.keep_connection(fd, held.peer, held.peer_len);
                            }
                        }
                    }
                }
            }
        }
    }
}

impl AsFd for Driver {
    /// The wake descriptor. It becomes readable when a completion is ready, when the kernel has
    /// deferred work for the driver, and when another thread posts a request or a wake. Turn the
    /// driver when it is readable.
    ///
    /// A Linux kernel with the io_uring change `cd305ee3633a` and not `04fe9aeb4f3c` signals the
    /// descriptor for deferred work only one time in each registration, for example Ubuntu 26.04
    /// with `7.0.0-1016-aws`. The first driver of a process finds such a kernel, and each driver
    /// then registers the descriptor again in each turn that takes a wake, so the descriptor still
    /// becomes readable for each completion and each deferred work. A kernel with no defect makes
    /// no extra system call.
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.shared.wake.as_fd()
    }
}

impl Drop for Driver {
    /// Ends each future: it gives [`Ended`]. The resources of a running operation leak, because the
    /// kernel can still use them, and so do the provided buffers then; [`Driver::teardown`] ends
    /// the operations first.
    fn drop(&mut self) {
        self.settle_messages();
        let busy = self.table.busy();
        for entry in &self.shared.entries {
            entry.close();
        }
        signal::end(&self.signals);
        if let Some(selected) = self.selected.take() {
            if busy {
                let _leaked = ManuallyDrop::new(selected);
            } else {
                // A failure leaves the ring registered, and then its memory leaks.
                drop(selected.ring.unregister(&self.ring));
            }
        }
    }
}

/// Pushes a cancel of `id` by its own user data, and submits once when the queue is full.
#[expect(unsafe_code, reason = "a cancel names no memory")]
fn push_cancel(ring: &mut Ring, id: OpId) -> io::Result<()> {
    // SAFETY: a cancel names no memory.
    unsafe { push_now(ring, &Sqe::cancel(id.user_data(), id.cancel_user_data())) }
}

/// Pushes `entry`, and submits once when the queue is full.
///
/// # Safety
///
/// `entry` names no memory, as a cancel or a poll does.
#[expect(unsafe_code, reason = "the caller gives an entry that names no memory")]
unsafe fn push_now(ring: &mut Ring, entry: &Sqe) -> io::Result<()> {
    // SAFETY: the entry names no memory.
    if unsafe { ring.push(entry) }.is_ok() {
        return Ok(());
    }
    ring.submit(0)?;
    // SAFETY: as above.
    unsafe { ring.push(entry) }.map_err(|_| io::ErrorKind::WouldBlock.into())
}

/// Ends the registration of the wake descriptor and makes a new one, which starts with no
/// deferred signal (`probe`).
fn rearm(ring: &Ring, wake: &fs::File) -> io::Result<()> {
    ring.unregister_eventfd()?;
    ring.register_eventfd(wake.as_raw_fd())
}

/// A failure of the kernel entry that leaves the work for the next turn: an interrupt, or a full
/// completion queue (`EAGAIN`, `EBUSY`).
fn retryable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::ResourceBusy
    )
}

/// A new nonblocking `eventfd`.
#[expect(unsafe_code, reason = "the C library returns a new descriptor")]
fn eventfd() -> io::Result<OwnedFd> {
    // SAFETY: the call takes no pointer.
    let fd = unsafe { sys::eventfd(0, sys::EFD_CLOEXEC | sys::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the C library returned a new descriptor that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}
