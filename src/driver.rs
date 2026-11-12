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
//! - The wake descriptor, [`AsFd`] of the driver, becomes readable when work comes: a completion,
//!   deferred work of the kernel, or a request from another thread. A host's reactor can wait for
//!   it, or [`Driver::wait`] blocks until it.
//! - [`Driver::teardown`] cancels each running operation, drives until each terminal event, and
//!   closes each resource. Its [`Teardown`] reports the cancelled operations.
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

#![cfg_attr(
    any(not(test), loom),
    expect(
        dead_code,
        reason = "the tests submit operations now; the TCP operations are the first other users"
    )
)]

mod completion;
mod sys;
#[cfg(all(test, not(loom)))]
mod tests;

use alloc::boxed::Box;
use core::fmt;
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsFd, AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd, RawFd};

use crate::buffer::Backing;
use crate::capacity::Permit;
use crate::sync::Arc;
use crate::table::{MAX_SLOTS, OpId, Outcome, Request, Requests, Shape, Table, Taken};
use crate::uring::{Ring, SetupError, Sqe};

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
    /// The completions that the turn gave to their operations.
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
    pub(crate) buffer: Option<Backing>,
    pub(crate) charge: Option<Permit>,
}

/// A refused submission: its reason, and the resources, which the caller keeps.
#[derive(Debug)]
pub(crate) struct Refused {
    pub(crate) reason: Reason,
    pub(crate) held: Held,
}

/// Why a submission was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reason {
    /// Each operation slot is in use.
    Operations,
    /// The submission queue is full, also after one submit.
    Queue,
}

/// A descriptor that the driver owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Resource {
    index: u32,
    generation: u32,
}

/// The state that the futures and the wake handles share with the home.
struct Shared {
    entries: Box<[Entry<Held>]>,
    requests: Requests,
    /// The wake descriptor: an `eventfd` that the kernel also signals at each completion.
    wake: File,
}

impl Shared {
    fn wake(&self) {
        // A full counter is a pending wake already, and nothing else can fail here.
        drop((&self.wake).write(&1_u64.to_ne_bytes()));
    }

    /// Clears the wake descriptor.
    fn clear(&self) {
        let mut count = [0; 8];
        // A descriptor with no wake gives `WouldBlock`, which is the clear state.
        drop((&self.wake).read(&mut count));
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
    resources: Box<[(u32, Option<OwnedFd>)]>,
    completions_per_turn: u32,
}

impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Driver")
            .field("ring", &self.ring)
            .field("resources", &self.resources.len())
            .field("completions_per_turn", &self.completions_per_turn)
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
        let ring = Ring::new(config.submissions, completions).map_err(|error| match error {
            SetupError::Refused(error) => Error::Refused(error),
            SetupError::Failed(error) if error.kind() == io::ErrorKind::InvalidInput => Error::Config,
            SetupError::Failed(error) => Error::System(error),
        })?;
        let wake = eventfd().map_err(Error::System)?;
        ring.register_eventfd(wake.as_raw_fd()).map_err(Error::System)?;
        let table = Table::new(config.operations);
        let shared = Arc::new(Shared {
            entries: (0..config.operations).map(|_| Entry::new()).collect(),
            requests: table.requests(),
            wake: File::from(wake),
        });
        Ok(Self {
            ring,
            table,
            shared,
            resources: (0..config.resources).map(|_| (0, None)).collect(),
            completions_per_turn: config.completions_per_turn,
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

    /// One bounded turn. It never blocks.
    ///
    /// # Errors
    ///
    /// A failure of the kernel entry other than an interrupt or a full completion queue, which
    /// leave work for the next turn.
    pub fn turn(&mut self) -> io::Result<Turn> {
        self.take_requests();
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
        self.shared.clear();
        more |= self.ring.ready()
            || self.ring.needs_enter()
            || self.ring.unsubmitted() > 0
            || !self.shared.requests.is_empty();
        Ok(Turn { completed, more })
    }

    /// Blocks until the wake descriptor is readable: a completion, work of the kernel, or a
    /// request or wake from another thread came.
    ///
    /// # Errors
    ///
    /// A failure of `poll` other than an interrupt.
    #[expect(unsafe_code, reason = "poll reads and writes one pollfd")]
    pub fn wait(&self) -> io::Result<()> {
        let mut fd = sys::PollFd {
            fd: self.shared.wake.as_raw_fd(),
            events: sys::POLLIN,
            revents: 0,
        };
        loop {
            // SAFETY: `fd` is one valid `pollfd` during the call.
            if unsafe { sys::poll(&raw mut fd, 1, -1) } >= 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    /// Cancels each running operation, drives until each terminal event, and closes each
    /// resource. Each future that has not taken its result then gives its end of the driver.
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
        let Some((index, (generation, slot))) = (0..)
            .zip(self.resources.iter_mut())
            .find(|(_, (_, slot))| slot.is_none())
        else {
            return Err(fd);
        };
        *slot = Some(fd);
        Ok(Resource {
            index,
            generation: *generation,
        })
    }

    /// The descriptor of `resource`, while the driver owns it.
    pub(crate) fn fd(&self, resource: Resource) -> Option<RawFd> {
        match self.resources.get(resource.index as usize)? {
            (generation, Some(fd)) if *generation == resource.generation => Some(fd.as_raw_fd()),
            _ => None,
        }
    }

    /// Submits `entry` for an operation with the resources `held`, and gives its future. A drop of
    /// the future asks for a cancel when `cancel_on_drop` holds.
    ///
    /// # Safety
    ///
    /// The memory that `entry` names belongs to `held`, and each descriptor that it names is a
    /// resource of this driver. The memory keeps its address while `held` moves.
    #[expect(unsafe_code, reason = "the kernel uses the memory that the entry names")]
    pub(crate) unsafe fn submit(
        &mut self,
        held: Held,
        entry: Sqe,
        cancel_on_drop: bool,
    ) -> Result<Completion, Refused> {
        let id = match self.table.begin((), Shape::Single) {
            Ok(id) => id,
            Err(_) => {
                // Futures that ended give their slots back at the next take.
                self.take_requests();
                match self.table.begin((), Shape::Single) {
                    Ok(id) => id,
                    Err(_) => {
                        return Err(Refused {
                            reason: Reason::Operations,
                            held,
                        });
                    }
                }
            }
        };
        let Some(slot) = self.shared.entries.get(id.index() as usize) else {
            return Err(Refused {
                reason: Reason::Operations,
                held,
            });
        };
        slot.start(id, held);
        let entry = entry.with_user_data(id.user_data());
        // SAFETY: the entry of the slot keeps `held` until the terminal event of `id`, and the
        // driver owns each descriptor until teardown, which ends each operation first.
        let pushed = unsafe { self.ring.push(&entry) }.is_ok()
            || (self.ring.submit(0).is_ok()
                // SAFETY: as above.
                && unsafe { self.ring.push(&entry) }.is_ok());
        if !pushed {
            self.table.withdraw(id);
            return Err(Refused {
                reason: Reason::Queue,
                held: slot.take_back().unwrap_or_default(),
            });
        }
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
        let shared = &self.shared;
        let table = &mut self.table;
        for cqe in self.ring.completions().take(limit as usize) {
            completed += 1;
            let entry =
                OpId::from_user_data(cqe.user_data).and_then(|id| Some((id, shared.entries.get(id.index() as usize)?)));
            match (table.complete(cqe.user_data, cqe.event()), entry) {
                (Outcome::Ready, Some((id, slot))) => slot.publish(id, cqe.res),
                (Outcome::Abandoned(_), Some((id, slot))) => slot.reclaim(id),
                // No driver operation is multishot or a zero-copy send yet; the other outcomes
                // change nothing.
                _ => {}
            }
        }
        completed
    }
}

impl AsFd for Driver {
    /// The wake descriptor.
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.shared.wake.as_fd()
    }
}

impl Drop for Driver {
    /// Ends each future: it gives the end of the driver. The resources of a running operation
    /// leak, because the kernel can still use them; [`Driver::teardown`] ends the operations
    /// first.
    fn drop(&mut self) {
        for entry in &self.shared.entries {
            entry.close();
        }
    }
}

/// Pushes a cancel of `id` by its own user data, and submits once when the queue is full.
#[expect(unsafe_code, reason = "a cancel names no memory")]
fn push_cancel(ring: &mut Ring, id: OpId) -> io::Result<()> {
    let cancel = Sqe::cancel(id.user_data(), id.cancel_user_data());
    // SAFETY: a cancel names no memory.
    if unsafe { ring.push(&cancel) }.is_ok() {
        return Ok(());
    }
    ring.submit(0)?;
    // SAFETY: as above.
    unsafe { ring.push(&cancel) }.map_err(|_| io::ErrorKind::WouldBlock.into())
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
