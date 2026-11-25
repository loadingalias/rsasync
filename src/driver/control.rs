//! Controlled delivery: the public seam of the completions of a driver ([`Delivery::Controlled`]).
//! The seam chooses only when and with what result each completion of the backend reaches the
//! driver. The table, the harvest, and the rules of each resource run as with native delivery.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use core::alloc::AllocError;
use core::fmt;
use std::io;

use super::{DriverId, Resources};
use crate::memory::items;
use crate::table::OpId;
use crate::uring::Cqe;

/// The result that a cancel of an operation that already runs gives: `EALREADY` on Linux
/// (`include/uapi/asm-generic/errno.h`) and macOS (`sys/errno.h`), and `WSAEALREADY` (`winerror.h`)
/// on Windows.
#[cfg(target_os = "linux")]
const EALREADY: i32 = 114;
#[cfg(target_os = "macos")]
const EALREADY: i32 = 37;
#[cfg(target_os = "windows")]
const EALREADY: i32 = 10037;

/// How a driver gives the completions of its backend to their operations
/// ([`Config::delivery`](super::Config::delivery)).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Each completion in the turn that takes it from the backend.
    #[default]
    Native,
    /// Each completion in the turn after the turn that took it from the backend, so each drop,
    /// cancel, and close of that next turn comes first. The [`Control`] of the driver can hold, fail,
    /// or repeat a completion. On IOCP, no socket skips its packet, so each completion comes
    /// through the port.
    Controlled,
}

/// The identity of an operation of one driver ([`Operation::id`](super::Operation::id)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationId {
    pub(super) driver: DriverId,
    pub(super) op: OpId,
}

/// Why a call of [`Control`] changed nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ControlError {
    /// The operation is of another driver.
    OtherDriver,
    /// The control has no completion of the operation to give again: no completion of the
    /// operation reached its future yet, or a later operation of its slot got a completion since.
    NoCompletion,
    /// The control already keeps its most completions: the completion room of the driver.
    Full,
    /// The error has no code of the system, so no completion can hold it.
    NoCode,
    /// The backend registers no resource: io_uring.
    NoRegistration,
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::OtherDriver => "the operation is of another driver",
            Self::NoCompletion => "the control has no completion of the operation to give again",
            Self::Full => "the control keeps its most completions",
            Self::NoCode => "the error has no code of the system",
            Self::NoRegistration => "the backend registers no resource",
        })
    }
}

impl core::error::Error for ControlError {}

/// The controls of one operation slot.
#[derive(Debug, Default, Clone, Copy)]
struct Slot {
    /// The generation of the operation that `hold`, `fail`, and `running` control.
    generation: u32,
    /// The driver keeps each completion of the operation.
    hold: bool,
    /// The error code that the next completion of the operation gives.
    fail: Option<i32>,
    /// The next cancel result of the operation says that the operation runs.
    running: bool,
    /// The last completion of an operation of the slot that the driver gave.
    last: Option<Cqe>,
}

/// The state of controlled delivery.
#[derive(Debug)]
pub(super) struct Controlled {
    /// The completions that the driver took from the backend and did not give, in the order of the
    /// takes. The next turn gives each one that no hold keeps.
    taken: VecDeque<Cqe>,
    /// The most completions in `taken`: the completion room of the driver.
    capacity: usize,
    slots: Box<[Slot]>,
}

impl Controlled {
    /// The state for `operations` operation slots and at most `completions` kept completions.
    pub(super) fn new(operations: u32, completions: u32) -> Result<Self, AllocError> {
        let capacity = completions as usize;
        let mut taken = VecDeque::new();
        taken.try_reserve_exact(capacity).map_err(|_| AllocError)?;
        Ok(Self {
            taken,
            capacity,
            slots: items(operations, |_| Slot::default())?,
        })
    }

    /// The controls of the operation `op`. Controls of an earlier operation of the slot end.
    fn slot(&mut self, op: OpId) -> Option<&mut Slot> {
        let slot = self.slots.get_mut(op.index() as usize)?;
        if slot.generation != op.generation() {
            *slot = Slot {
                generation: op.generation(),
                last: slot.last,
                ..Slot::default()
            };
        }
        Some(slot)
    }

    /// Whether a hold keeps `cqe`.
    fn held(&self, cqe: &Cqe) -> bool {
        OpId::from_user_data(cqe.user_data).is_some_and(|op| {
            self.slots
                .get(op.index() as usize)
                .is_some_and(|slot| slot.hold && slot.generation == op.generation())
        })
    }

    /// Whether the next turn gives a completion.
    pub(super) fn due(&self) -> bool {
        self.taken.iter().any(|cqe| !self.held(cqe))
    }

    /// Keeps the completions that `posted` gives, while room remains, for the next turn. A
    /// completion beyond the room stays in the backend for a later take.
    pub(super) fn take(&mut self, posted: impl Iterator<Item = Cqe>) {
        let room = self.capacity.saturating_sub(self.taken.len());
        // Within the capacity that `new` reserved, so no push allocates.
        self.taken.extend(posted.take(room));
    }

    /// Gives the first kept completion that no hold keeps, with its failure or its cancel result,
    /// and records it as the last completion of its slot.
    pub(super) fn give(&mut self) -> Option<Cqe> {
        let index = self.taken.iter().position(|cqe| !self.held(cqe))?;
        let mut cqe = self.taken.remove(index)?;
        // A stale completion leaves the controls of the operation that now holds its slot.
        if let Some(op) = OpId::from_user_data(cqe.user_data)
            && let Some(slot) = self.slots.get_mut(op.index() as usize)
        {
            if slot.generation == op.generation()
                && let Some(code) = slot.fail.take()
            {
                cqe.res = code.saturating_neg();
            }
            slot.last = Some(cqe);
        } else if let Some(op) = OpId::from_cancel_user_data(cqe.user_data)
            && let Some(slot) = self.slots.get_mut(op.index() as usize)
            && slot.generation == op.generation()
            && core::mem::take(&mut slot.running)
        {
            cqe.res = -EALREADY;
        }
        Some(cqe)
    }

    /// Ends each hold, so a teardown gets each terminal event.
    pub(super) fn deliver_all(&mut self) {
        for slot in &mut self.slots {
            slot.hold = false;
        }
    }
}

/// The control of a driver under [`Delivery::Controlled`] ([`Driver::control`](super::Driver::control)).
/// Each call names an operation by its [`OperationId`], and refuses one of another driver.
///
/// A receive whose future drops after its bytes came leaves them to the next receive. The hold
/// makes the drop come before the completion on each run:
///
/// ```
/// # #[cfg(unix)]
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // The same text as `docs/content/controlled-delivery.md`. Edit both.
/// use std::io::Write as _;
/// use std::os::fd::OwnedFd;
/// use std::pin::pin;
/// use std::task::{Context, Poll, Waker};
///
/// use rsasync::buffer::{Backing, Buffer as _};
/// use rsasync::driver::{Config, Delivery, Driver};
///
/// let mut driver = Driver::new(Config::new().delivery(Delivery::Controlled))?;
/// let (read, mut write) = std::io::pipe()?;
/// let mut reader = driver.adopt_reader(OwnedFd::from(read))?;
/// let receive = driver.recv(&mut reader, Backing::from(Vec::with_capacity(64)))?;
/// let id = receive.id();
/// driver.control().ok_or("native delivery")?.hold(id)?;
/// write.write_all(b"hello")?;
///
/// // A turn takes the completion into the control, so look after each turn and before each wait.
/// loop {
///     let more = driver.turn()?.more();
///     if driver.control().ok_or("native delivery")?.posted(id)?.next().is_some() {
///         break;
///     }
///     if !more {
///         driver.wait()?;
///     }
/// }
/// // The receive completed in the kernel, and its completion waits in the control. Its drop comes
/// // first, so the driver applies the rule of an abandoned receive: the stream keeps the bytes.
/// drop(receive);
/// driver.control().ok_or("native delivery")?.deliver(id)?;
///
/// let mut next = pin!(driver.recv(&mut reader, Backing::from(Vec::with_capacity(64)))?);
/// let (count, buffer) = loop {
///     let more = driver.turn()?.more();
///     if let Poll::Ready(output) = next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
///         break output?;
///     }
///     if !more {
///         driver.wait()?;
///     }
/// };
/// assert_eq!((count?, buffer.bytes()), (5, &b"hello"[..]));
/// // The next receive gave the kept bytes with one counted copy.
/// assert_eq!(driver.copied(), 5);
/// driver.teardown()?;
/// # Ok(())
/// # }
/// # #[cfg(not(unix))]
/// # fn main() {}
/// ```
pub struct Control<'a> {
    pub(super) state: &'a mut Controlled,
    pub(super) resources: &'a mut Resources,
}

impl Control<'_> {
    /// The controls of the operation `id` of this driver.
    fn slot(&mut self, id: OperationId) -> Result<&mut Slot, ControlError> {
        if id.driver != self.resources.driver {
            return Err(ControlError::OtherDriver);
        }
        self.state.slot(id.op).ok_or(ControlError::OtherDriver)
    }

    /// Keeps each completion of `id` that the driver takes from the backend, and each one that it
    /// keeps already, until [`Control::deliver`]. The operation runs on, and its future stays
    /// pending. A teardown ends each hold.
    ///
    /// # Errors
    ///
    /// [`ControlError::OtherDriver`].
    pub fn hold(&mut self, id: OperationId) -> Result<(), ControlError> {
        self.slot(id)?.hold = true;
        Ok(())
    }

    /// Ends the hold of `id`: the next turn gives its kept completions, in the order that the
    /// backend posted them.
    ///
    /// # Errors
    ///
    /// [`ControlError::OtherDriver`].
    pub fn deliver(&mut self, id: OperationId) -> Result<(), ControlError> {
        self.slot(id)?.hold = false;
        Ok(())
    }

    /// The next completion of `id` that the driver gives has `error` in place of its result, as a
    /// failure of the system would.
    ///
    /// # Errors
    ///
    /// [`ControlError::OtherDriver`], or [`ControlError::NoCode`] for an error with no
    /// [`io::Error::raw_os_error`].
    pub fn fail(&mut self, id: OperationId, error: &io::Error) -> Result<(), ControlError> {
        let code = error
            .raw_os_error()
            .filter(|&code| code > 0)
            .ok_or(ControlError::NoCode)?;
        self.slot(id)?.fail = Some(code);
        Ok(())
    }

    /// The next cancel of `id` finds the operation running: its result says so (`EALREADY`), and
    /// the operation ends only at its own completion, with its own result.
    ///
    /// # Errors
    ///
    /// [`ControlError::OtherDriver`].
    pub fn cancel_finds_running(&mut self, id: OperationId) -> Result<(), ControlError> {
        self.slot(id)?.running = true;
        Ok(())
    }

    /// Gives the last completion of `id` again at the next turn: a late completion with the
    /// identity of `id`. After a later operation took the slot, the driver finds the identity
    /// stale, and the completion changes nothing.
    ///
    /// # Errors
    ///
    /// [`ControlError::OtherDriver`], [`ControlError::NoCompletion`], or [`ControlError::Full`].
    pub fn repeat(&mut self, id: OperationId) -> Result<(), ControlError> {
        if id.driver != self.resources.driver {
            return Err(ControlError::OtherDriver);
        }
        // A read only: the controls of a later operation of the slot stay.
        let last = self
            .state
            .slots
            .get(id.op.index() as usize)
            .and_then(|slot| slot.last)
            .filter(|cqe| cqe.user_data == id.op.user_data())
            .ok_or(ControlError::NoCompletion)?;
        if self.state.taken.len() >= self.state.capacity {
            return Err(ControlError::Full);
        }
        self.state.taken.push_back(last);
        Ok(())
    }

    /// The registration of the next socket or pipe that the driver takes fails, as a refusal of
    /// the poller or the port would: its adoption refuses with
    /// [`Reason::Register`](super::Reason::Register), and an accept keeps its connection for the
    /// next accept. On io_uring with a file table ([`Files::Registered`](super::Files::Registered)),
    /// the next resource gets no place in the table, as a refusal of the kernel would: it keeps
    /// its plain descriptor, and its operations run on it.
    ///
    /// # Errors
    ///
    /// [`ControlError::NoRegistration`] on io_uring with no file table.
    pub fn fail_next_registration(&mut self) -> Result<(), ControlError> {
        if !self.resources.registers() {
            return Err(ControlError::NoRegistration);
        }
        self.resources.fail_registration = true;
        Ok(())
    }

    /// The results of the completions of `id` that the driver took from the backend and did not
    /// give yet, in order: a count, or the error of the system.
    ///
    /// # Errors
    ///
    /// [`ControlError::OtherDriver`].
    pub fn posted(&self, id: OperationId) -> Result<impl Iterator<Item = io::Result<u32>> + '_, ControlError> {
        if id.driver != self.resources.driver {
            return Err(ControlError::OtherDriver);
        }
        let data = id.op.user_data();
        Ok(self
            .state
            .taken
            .iter()
            .filter(move |cqe| cqe.user_data == data)
            .map(|cqe| u32::try_from(cqe.res).map_err(|_| io::Error::from_raw_os_error(cqe.res.saturating_neg()))))
    }
}

impl fmt::Debug for Control<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Control")
            .field("kept", &self.state.taken.len())
            .finish_non_exhaustive()
    }
}
