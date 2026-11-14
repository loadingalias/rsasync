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
//! lease, and [`Driver::close`]. The kernel reads and writes each buffer in place.
//!
//! The io_uring driver needs no helper thread: [`Driver::helper_threads`] gives 0.
//!
//! ```no_run
//! use rsasync::driver::{Config, Driver};
//!
//! let mut driver = Driver::new(Config::new())?;
//! // ... submit operations, and keep their futures ...
//! for _ in 0..1000 {
//!     if !driver.turn()?.more() {
//!         driver.wait()?;
//!     }
//! }
//! let report = driver.teardown()?;
//! assert_eq!(report.cancelled(), 0);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod completion;
mod probe;
mod sys;
mod tcp;
#[cfg(all(test, not(loom)))]
mod tests;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;
use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsFd, AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd, RawFd};

use rsnetwork_udp::SockAddrStorage;

use crate::buffer::{Backing, BufferMut as _, Lease, Pool, WriteLoan};
use crate::capacity::Permit;
use crate::sync::Arc;
use crate::table::{CONTROL, MAX_SLOTS, OpId, Outcome, Request, Requests, Shape, Table, Taken};
use crate::uring::{BufferRing, Cqe, Ring, SetupError, Sqe};

pub use tcp::{TcpListener, TcpStream};

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
    /// The descriptor that the operation uses. It stays open while the operation runs.
    pub(crate) resource: Option<Resource>,
    /// What the home does with the result at the terminal event.
    pub(crate) finish: Finish,
    /// The peer address that an accept writes, and its length.
    pub(crate) peer: SockAddrStorage,
    pub(crate) peer_len: u32,
    /// The stream that an accept gave.
    pub(crate) accepted: Option<Resource>,
}

/// What the home does with the result of an operation at its terminal event.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Finish {
    #[default]
    Nothing,
    /// The result counts the bytes that the kernel wrote into `loan`.
    Received,
    /// The kernel selected a buffer of the provided ring, which becomes `loan`.
    Selected,
    /// The result is a new descriptor, which becomes `accepted`.
    Accepted,
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
    /// An operation still runs on the descriptor.
    Busy,
    /// The driver has no provided buffers ([`Driver::provide`]).
    Buffers,
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
/// # Panics
///
/// A poll after the future gave `Ready` panics, as for the futures of the standard library.
pub struct Operation<T> {
    completion: Completion,
    output: fn(i32, Held) -> Option<T>,
}

impl<T> Operation<T> {
    fn new(completion: Completion, output: fn(i32, Held) -> Option<T>) -> Self {
        Self { completion, output }
    }
}

impl<T> Future for Operation<T> {
    type Output = Result<T, Ended>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let output = self.output;
        Pin::new(&mut self.completion)
            .poll(cx)
            .map(|taken| taken.ok().and_then(|(result, held)| output(result, held)).ok_or(Ended))
    }
}

impl<T> fmt::Debug for Operation<T> {
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
    /// The operations that run on the descriptor.
    running: u32,
}

/// The descriptors that the driver owns, in a slice that it allocates once.
#[derive(Debug)]
struct Resources(Box<[Owned]>);

impl Resources {
    /// Takes `fd`. Refuses with `fd` when each slot is in use.
    fn adopt(&mut self, fd: OwnedFd) -> Result<Resource, OwnedFd> {
        let Some((index, slot)) = (0..).zip(self.0.iter_mut()).find(|(_, slot)| slot.fd.is_none()) else {
            return Err(fd);
        };
        slot.fd = Some(fd);
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

    /// Closes `resource` when no operation runs on it.
    fn close(&mut self, resource: Resource) -> Result<(), Reason> {
        let slot = self.get(resource).ok_or(Reason::Resource)?;
        if slot.running > 0 {
            return Err(Reason::Busy);
        }
        slot.fd = None;
        slot.generation = slot.generation.wrapping_add(1);
        Ok(())
    }
}

/// The buffer group of the provided ring.
const GROUP: u16 = 0;

/// The user data of the poll of the interrupt descriptor that a wait keeps in the ring.
const WAKE: u64 = CONTROL;

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
    wake: File,
    /// An `eventfd` that only the other threads signal. A wait polls it in the ring. A signal of
    /// the kernel would also complete that poll, so the wait does not poll `wake`.
    interrupt: File,
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
    table: Table<()>,
    shared: Arc<Shared>,
    resources: Resources,
    selected: Option<Selected>,
    completions_per_turn: u32,
    /// The ring polls the interrupt descriptor, so that a request or wake from another thread
    /// ends a wait.
    waking: bool,
    /// The kernel signals the wake descriptor for deferred work only one time in each
    /// registration (`probe`), so each turn that takes a wake registers it again.
    rearm_wake: bool,
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
    /// Starts a driver with the limits of `config`. It allocates here, one time.
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
        let (wake, interrupt) = (eventfd().map_err(Error::System)?, eventfd().map_err(Error::System)?);
        let wake = File::from(wake);
        ring.register_eventfd(wake.as_raw_fd()).map_err(Error::System)?;
        let rearm_wake = probe::signal_stops_after_first(&mut ring, &wake).map_err(Error::System)?;
        let table = Table::new(config.operations);
        let shared = Arc::new(Shared {
            entries: (0..config.operations).map(|_| Entry::new()).collect(),
            requests: table.requests(),
            wake,
            interrupt: File::from(interrupt),
        });
        Ok(Self {
            ring,
            table,
            shared,
            resources: Resources((0..config.resources).map(|_| Owned::default()).collect()),
            selected: None,
            completions_per_turn: config.completions_per_turn,
            waking: false,
            rearm_wake,
        })
    }

    /// The helper threads of the driver. The io_uring driver has none.
    #[must_use]
    pub const fn helper_threads(&self) -> u32 {
        0
    }

    /// A handle that wakes the driver from any thread.
    #[must_use]
    pub fn wake_handle(&self) -> WakeHandle {
        WakeHandle(Arc::clone(&self.shared))
    }

    /// Gives the slots of `pool` to the kernel as the buffers of [`Driver::recv_selected`]: a
    /// receive then takes a buffer only when its data comes. A selected buffer comes back to the
    /// kernel when its last lease or view drops, on any thread, at the next turn.
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
    /// resource. Each future that has not taken its result then gives [`Ended`].
    ///
    /// # Errors
    ///
    /// A failure of the kernel entry other than an interrupt.
    pub fn teardown(mut self) -> io::Result<Teardown> {
        let mut cancelled = 0_u32;
        for id in self.table.running() {
            push_cancel(&mut self.ring, id)?;
            cancelled = cancelled.saturating_add(1);
        }
        while self.table.busy() {
            self.take_requests();
            match self.ring.submit(1) {
                Ok(_) => {}
                Err(error) if retryable(&error) => {}
                Err(error) => return Err(error),
            }
            self.harvest(u32::MAX);
        }
        self.take_requests();
        Ok(Teardown { cancelled })
    }

    /// Takes `fd`, so that teardown closes it. Refuses with `fd` when each resource slot is in use.
    pub(crate) fn adopt(&mut self, fd: OwnedFd) -> Result<Resource, OwnedFd> {
        self.resources.adopt(fd)
    }

    /// The descriptor of `resource`, while the driver owns it.
    pub(crate) fn fd(&mut self, resource: Resource) -> Option<RawFd> {
        Some(self.resources.get(resource)?.fd.as_ref()?.as_raw_fd())
    }

    /// Submits an operation on `resource` with the resources that `held` makes from `value`, and
    /// gives its future. `entry` makes the submission from the resources in their final place. A
    /// drop of the future asks for a cancel when `cancel_on_drop` holds. A refusal gives back
    /// `value`.
    ///
    /// # Safety
    ///
    /// The memory that the entry names belongs to the resources, and each descriptor that it names
    /// is `resource`. The memory keeps its address while the resources move.
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
        if !self.ring.has_room() && (self.ring.submit(0).is_err() || !self.ring.has_room()) {
            return refuse(Reason::Queue, value);
        }
        let id = match self.table.begin((), Shape::Single) {
            Ok(id) => id,
            Err(_) => {
                // Futures that ended give their slots back at the next take.
                self.take_requests();
                match self.table.begin((), Shape::Single) {
                    Ok(id) => id,
                    Err(_) => return refuse(Reason::Operations, value),
                }
            }
        };
        let Some(slot) = self.shared.entries.get(id.index() as usize) else {
            self.table.withdraw(id);
            return refuse(Reason::Operations, value);
        };
        let mut held = held(value);
        held.resource = resource;
        let entry = slot.start(id, held, entry).with_user_data(id.user_data());
        if let Some(owned) = resource.and_then(|resource| self.resources.get(resource)) {
            owned.running += 1;
        }
        // SAFETY: the entry of the slot keeps the resources until the terminal event of `id`, and
        // the driver keeps the descriptor open while the operation runs. The queue has room.
        let pushed = unsafe { self.ring.push(&entry) }.is_ok();
        debug_assert!(pushed, "the queue has room for the entry");
        Ok(Completion::new(Arc::clone(&self.shared), id, cancel_on_drop))
    }

    /// Applies the requests that other threads posted.
    fn take_requests(&mut self) {
        let (ring, shared) = (&mut self.ring, &self.shared);
        self.table.take_requests(|taken| match taken {
            Taken::Cancel(id) => {
                if push_cancel(ring, id).is_err() {
                    // The queue stays full: the next turn takes the request again.
                    shared.requests.post(id, Request::Cancel);
                }
            }
            Taken::Abandoned(id, _) => {
                if let Some(slot) = shared.entries.get(id.index() as usize) {
                    slot.reclaim(id);
                }
            }
        });
    }

    /// Gives up to `limit` ready completions to their operations. Gives their number.
    fn harvest(&mut self, limit: u32) -> u32 {
        let mut completed = 0;
        let (shared, table) = (&self.shared, &mut self.table);
        let (resources, selected, waking) = (&mut self.resources, &mut self.selected, &mut self.waking);
        for cqe in self.ring.completions().take(limit as usize) {
            completed += 1;
            let target =
                OpId::from_user_data(cqe.user_data).and_then(|id| Some((id, shared.entries.get(id.index() as usize)?)));
            let (abandoned, (id, slot)) = match (table.complete(cqe.user_data, cqe.event()), target) {
                (Outcome::Ready, Some(target)) => (false, target),
                (Outcome::Abandoned(_), Some(target)) => (true, target),
                // The poll of the interrupt descriptor ended: the next wait polls it again.
                (Outcome::Control(WAKE), _) => {
                    *waking = false;
                    continue;
                }
                // No driver operation is multishot or a zero-copy send yet; the other outcomes
                // change nothing.
                _ => continue,
            };
            slot.update(id, |held| {
                if let Some(held) = held {
                    finish(held, &cqe, abandoned, resources, selected.as_mut());
                }
            });
            if abandoned {
                slot.reclaim(id);
            } else {
                slot.publish(id, cqe.res);
            }
        }
        completed
    }
}

/// The home finishes the operation of `held` at its terminal event `cqe`, before its consumer
/// takes the result, or before the home drops the resources of an abandoned operation.
#[expect(unsafe_code, reason = "the kernel initialized the bytes that the result counts")]
fn finish(held: &mut Held, cqe: &Cqe, abandoned: bool, resources: &mut Resources, selected: Option<&mut Selected>) {
    if let Some(owned) = held.resource.and_then(|resource| resources.get(resource)) {
        owned.running = owned.running.saturating_sub(1);
    }
    let count = usize::try_from(cqe.res);
    match held.finish {
        Finish::Nothing => {}
        Finish::Received => {
            if let (Ok(count), Some(loan)) = (count, held.loan.as_mut()) {
                // SAFETY: the receive named the capacity of the loan, and the kernel wrote `count`
                // bytes at its start, at most that capacity.
                unsafe { loan.set_filled(count) };
            }
        }
        Finish::Selected => {
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
                if !abandoned {
                    held.loan = Some(loan);
                }
            }
        }
        Finish::Accepted => {
            if cqe.res >= 0 {
                // SAFETY: the kernel gave a new descriptor that nothing else owns.
                let fd = unsafe { OwnedFd::from_raw_fd(cqe.res) };
                if !abandoned {
                    held.accepted = resources.adopt(fd).ok();
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
        let busy = self.table.busy();
        for entry in &self.shared.entries {
            entry.close();
        }
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
fn rearm(ring: &Ring, wake: &File) -> io::Result<()> {
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
