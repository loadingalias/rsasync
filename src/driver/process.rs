//! Child processes on the driver: a spawn of a [`Command`] on the blocking service, or the adoption
//! of a [`std::process::Child`]; the exit, which the driver reaps once and reports once; signals
//! through the process descriptor; a process tree; and the end policy.
//!
//! The driver holds a process descriptor (pidfd) for each child. The exit is an operation with a
//! home: on Linux 6.7 and later, `IORING_OP_WAITID` takes the status in the ring; before 6.7, the
//! ring polls the descriptor, and when it becomes readable, the home takes the status with one
//! nonblocking `waitid`. Each wait names the descriptor of the child, so the driver never takes
//! the status of a child that it does not own. It installs no `SIGCHLD` handler and starts no
//! thread for a wait.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::ffi::c_int;
use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::os::fd::{AsRawFd as _, OwnedFd};
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::process::{Command, ExitStatus};

use super::tcp::{ReadHalf, WriteHalf};
use super::{
    Call, Completion, Driver, ECANCELED, Ended, Held, Kind, Medium, Operation, Payload, PipeReader, PipeWriter, Reason,
    Refused, Route, Signal, Tag, sys,
};
use crate::blocking::{Failed, Job};
use crate::sync::Arc;
use crate::table::OpId;
use crate::uring::Sqe;

/// `SIGKILL` (`include/uapi/asm-generic/signal.h`).
const SIGKILL: c_int = 9;

/// `ESRCH` (`include/uapi/asm-generic/errno-base.h`): no process has the identity.
const ESRCH: i32 = 3;

/// `EIO` (`include/uapi/asm-generic/errno-base.h`), for a spawn whose job gave no result.
const EIO: i32 = 5;

/// The resource slots that a spawn reserves while its job runs: one for each standard stream that
/// the command can pipe.
const STREAMS: u32 = 3;

/// What a close, a drop, and a teardown do to a child that still runs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// The child runs on. The driver still reaps it when it ends, while the driver exists.
    /// [`Teardown::kept`](super::Teardown::kept) counts each such child that still runs.
    #[default]
    Keep,
    /// The driver kills the child, or each process of its tree, and then reaps it.
    Kill,
}

/// The options of [`Driver::spawn`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ChildOptions {
    tree: bool,
    end: End,
}

impl ChildOptions {
    /// No process tree, and [`End::Keep`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tree: false,
            end: End::Keep,
        }
    }

    /// The child leads a new process group, its tree, with the group ID equal to its process ID.
    /// [`Driver::signal_tree`] and [`Driver::kill_tree`] then signal each process of the group,
    /// and [`End::Kill`] kills each one. A descendant that leaves the group is outside the tree.
    #[must_use]
    pub const fn tree(self, tree: bool) -> Self {
        Self { tree, ..self }
    }

    /// What a close, a drop, and a teardown do to the child while it runs.
    #[must_use]
    pub const fn end(self, end: End) -> Self {
        Self { end, ..self }
    }
}

/// Why the driver sent no signal to a child.
#[derive(Debug)]
#[non_exhaustive]
pub enum SignalChildError {
    /// The driver reaped the child: no process has its identity for the driver now.
    Reaped,
    /// A tree signal of a child that leads no process tree ([`ChildOptions::tree`]).
    NoTree,
    /// The system refused the signal, for example `EINVAL` for a number that is not a signal.
    System(io::Error),
}

impl fmt::Display for SignalChildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reaped => f.write_str("the driver reaped the child"),
            Self::NoTree => f.write_str("the child leads no process tree"),
            Self::System(error) => write!(f, "the system refused the signal: {error}"),
        }
    }
}

impl core::error::Error for SignalChildError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::System(error) => Some(error),
            Self::Reaped | Self::NoTree => None,
        }
    }
}

/// The identity of a child slot of a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChildId {
    index: u32,
    generation: u32,
}

/// A child process that a driver owns, from [`Driver::spawn`] or [`Driver::adopt_child`].
///
/// The driver reaps the child exactly once, when it ends, also after a drop of this handle, and
/// [`Child::wait`] gives the status. Each piped standard stream is a pipe end of the driver, a
/// resource of its own, so a caller writes the input while it receives both outputs.
///
/// A close ([`Driver::close_child`]) or a drop applies the [`End`] of the child. A drop of a pipe
/// end keeps its descriptor open until teardown, as for each resource of the driver: close the
/// pipe ends that you take.
///
/// A spawn of `echo`, a receive of its output, and the wait for its exit, with `run` as in
/// [`Operation`]:
///
/// ```
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
/// use std::process::{Command, Stdio};
///
/// use rsasync::blocking::{self, Service};
/// use rsasync::buffer::{Backing, Buffer as _};
/// use rsasync::capacity::Budget;
/// use rsasync::driver::{ChildOptions, Config, Driver};
///
/// let budget = Budget::new(core::num::NonZeroU64::new(4 << 20).unwrap());
/// let service = Service::new(&budget, blocking::Config::new().threads(1).stack(256 << 10))?;
/// let mut driver = Driver::new(Config::new())?;
/// driver.use_blocking(service.submitter());
///
/// let mut command = Command::new("echo");
/// command.arg("hello").stdout(Stdio::piped());
/// let spawn = driver.spawn(command, ChildOptions::new())?;
/// let mut child = run(&mut driver, spawn)???;
///
/// let mut stdout = child.stdout.take().unwrap();
/// let receive = driver.recv(&mut stdout, Backing::from(Vec::with_capacity(64)))?;
/// let (received, buffer) = run(&mut driver, receive)??;
/// assert_eq!((received?, buffer.bytes()), (6, &b"hello\n"[..]));
/// let status = run(&mut driver, child.wait())??;
/// assert!(status?.success());
///
/// let close = driver.close_reader(stdout)?;
/// run(&mut driver, close)??;
/// driver.teardown()?;
/// service.teardown();
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Child {
    /// The write end of the standard input, when the command piped it.
    pub stdin: Option<PipeWriter>,
    /// The read end of the standard output, when the command piped it.
    pub stdout: Option<PipeReader>,
    /// The read end of the standard error, when the command piped it.
    pub stderr: Option<PipeReader>,
    id: ChildId,
    pid: u32,
    /// The exit operation of the child. Its drop asks for a cancel with [`End::Kill`], which kills
    /// the child.
    exit: Completion,
    /// What the exit operation gave: the status or the `errno` of its error, or [`Ended`]. The
    /// operation gives it once, so each later wait takes it from here.
    status: Option<Result<Result<ExitStatus, i32>, Ended>>,
}

impl Child {
    /// The process ID of the child. After the reap, the system can give the number to another
    /// process, so signal the child through the driver, which uses its process descriptor.
    #[must_use]
    pub const fn id(&self) -> u32 {
        self.pid
    }

    /// Gives a future of the exit status of the child. A drop of the future ends nothing: the
    /// driver still reaps the child, and the next wait gives the status. A wait after the status
    /// gives it again at once.
    pub fn wait(&mut self) -> Wait<'_> {
        Wait(self)
    }
}

/// The future of [`Child::wait`]. It gives an error when another waiter of the process took the
/// status first (`ECHILD`), for example a `SIGCHLD` disposition of `SIG_IGN`, and [`Ended`] when
/// the driver ended first.
#[derive(Debug)]
pub struct Wait<'a>(&'a mut Child);

impl Future for Wait<'_> {
    type Output = Result<io::Result<ExitStatus>, Ended>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let child = &mut *self.0;
        let status = match child.status {
            Some(status) => status,
            None => {
                let Poll::Ready(taken) = Pin::new(&mut child.exit).poll(cx) else {
                    return Poll::Pending;
                };
                let status = match taken {
                    Ok((
                        0,
                        Held {
                            payload: Payload::Exit(info),
                            ..
                        },
                    )) => Ok(Ok(status(&info))),
                    // Only teardown cancels an exit operation, and the driver then ends first.
                    Ok((result, _)) if result == -ECANCELED => Err(Ended),
                    Ok((result, _)) => Ok(Err(result.min(-1).saturating_neg())),
                    Err(_) => Err(Ended),
                };
                *child.status.insert(status)
            }
        };
        Poll::Ready(status.map(|status| status.map_err(io::Error::from_raw_os_error)))
    }
}

/// The exit status that `info`, the result of a `waitid`, gives.
fn status(info: &sys::SigInfo) -> ExitStatus {
    // The form of `wait(2)`: an exit code in bits 8 to 15, or a signal in bits 0 to 6, and bit 7
    // for a core dump.
    let raw = match info.code {
        sys::CLD_EXITED => (info.status & 0xff) << 8,
        sys::CLD_DUMPED => (info.status & 0x7f) | 0x80,
        _ => info.status & 0x7f,
    };
    ExitStatus::from_raw(raw)
}

/// What a spawn job gives the home: the process descriptor and the process ID of the child, and
/// the parent ends of its piped standard streams.
#[derive(Debug)]
pub(crate) struct Spawned {
    pidfd: OwnedFd,
    pid: u32,
    stdin: Option<OwnedFd>,
    stdout: Option<OwnedFd>,
    stderr: Option<OwnedFd>,
}

/// The job of a spawn on the blocking service. A refusal gives it back, with its command.
struct SpawnJob {
    command: Command,
    tree: bool,
}

impl Job for SpawnJob {
    type Output = io::Result<Spawned>;

    fn run(mut self) -> io::Result<Spawned> {
        if self.tree {
            self.command.process_group(0);
        }
        let mut child = self.command.spawn()?;
        match pidfd_open(child.id()) {
            Ok(pidfd) => Ok(Spawned {
                pidfd,
                pid: child.id(),
                stdin: child.stdin.take().map(OwnedFd::from),
                stdout: child.stdout.take().map(OwnedFd::from),
                stderr: child.stderr.take().map(OwnedFd::from),
            }),
            // No child runs without a descriptor: this thread kills it and reaps it. The process
            // ID still names the child, because nothing has reaped it.
            Err(error) => {
                drop(child.kill());
                drop(child.wait());
                Err(error)
            }
        }
    }
}

/// One child slot: a child from its spawn or adoption to its reap.
#[derive(Debug, Default)]
struct Slot {
    generation: u32,
    /// The exit operation. A slot with none is free.
    exit: Option<OpId>,
    /// The process descriptor, from the end of the spawn job to the reap.
    pidfd: Option<OwnedFd>,
    pid: c_int,
    tree: bool,
    end: End,
}

/// The child slots of a driver, in a slice that it allocates once.
#[derive(Debug)]
pub(super) struct Children {
    slots: Box<[Slot]>,
    /// The kernel gives process descriptors. The first spawn or adoption finds it.
    descriptors: Option<bool>,
    /// The exit operations whose poll ended before the status of their child was ready, which the
    /// home polls again.
    again: Vec<OpId>,
    /// The children with [`End::Keep`] that teardown left unreaped.
    pub(super) kept: u32,
    /// Teardown runs: a child with [`End::Keep`] whose status is not ready ends its exit
    /// operation, because the cancel of teardown can miss a poll that the home pushes again.
    pub(super) ending: bool,
}

impl Children {
    pub(super) fn new(children: u32) -> Self {
        Self {
            slots: (0..children).map(|_| Slot::default()).collect(),
            descriptors: None,
            again: Vec::with_capacity(children as usize),
            kept: 0,
            ending: false,
        }
    }

    /// The index of a free slot.
    fn free(&self) -> Option<u32> {
        (0..)
            .zip(self.slots.iter())
            .find_map(|(index, slot)| slot.exit.is_none().then_some(index))
    }

    fn slot(&mut self, index: u32) -> Option<&mut Slot> {
        self.slots.get_mut(index as usize)
    }

    /// The slot of `id`, while its child is not reaped.
    fn get(&self, id: ChildId) -> Option<&Slot> {
        self.slots
            .get(id.index as usize)
            .filter(|slot| slot.generation == id.generation && slot.pidfd.is_some())
    }

    /// Ends the slot `index`: it closes the descriptor, and the identity is stale from now on.
    fn release(&mut self, index: u32) {
        if let Some(slot) = self.slot(index) {
            slot.generation = slot.generation.wrapping_add(1);
            slot.exit = None;
            slot.pidfd = None;
        }
    }

    /// Whether the kernel gives process descriptors. It opens one for this process the first time.
    fn descriptors(&mut self) -> bool {
        *self
            .descriptors
            .get_or_insert_with(|| pidfd_open(unsafe_call::getpid().cast_unsigned()).is_ok())
    }

    /// Sends `signal` to the child of `id`, or to its process group when `tree` holds.
    fn signal(&self, id: ChildId, signal: c_int, tree: bool) -> Result<(), SignalChildError> {
        let slot = self.get(id).ok_or(SignalChildError::Reaped)?;
        let pidfd = slot.pidfd.as_ref().ok_or(SignalChildError::Reaped)?;
        if !tree {
            return unsafe_call::pidfd_send_signal(pidfd, signal).map_err(refusal);
        }
        if !slot.tree {
            return Err(SignalChildError::NoTree);
        }
        // The group ID names this group only while the driver holds its leader unreaped. The
        // kernel reaps for `IORING_OP_WAITID` only in an entry of this thread to the ring, so no
        // reap comes between this look and the signal.
        if !unsafe_call::held(pidfd) {
            return Err(SignalChildError::Reaped);
        }
        unsafe_call::kill_group(slot.pid, signal).map_err(refusal)
    }

    /// Whether the child of the slot `index` has [`End::Kill`].
    pub(super) fn kills(&self, index: u32) -> bool {
        self.slots.get(index as usize).is_some_and(|slot| slot.end == End::Kill)
    }

    /// Applies [`End::Kill`] to the child of the slot `index`: a kill of the child, or of each
    /// process of its tree. A child with [`End::Keep`], or one that ended already, changes nothing.
    pub(super) fn kill(&self, index: u32) {
        let Some(slot) = self.slots.get(index as usize) else {
            return;
        };
        if slot.end == End::Kill {
            let id = ChildId {
                index,
                generation: slot.generation,
            };
            drop(self.signal(id, SIGKILL, slot.tree));
        }
    }

    /// Applies the completion `res` of the exit operation of the slot `index`, whose status the
    /// kernel wrote in `info` when `ring` holds (`IORING_OP_WAITID`), or whose poll ended when it
    /// does not. Gives the result of the operation `id`, or `None` when the status is not ready and
    /// the home polls again. A result ends the slot.
    pub(super) fn exited(
        &mut self,
        id: OpId,
        index: u32,
        res: i32,
        ring: bool,
        info: &mut sys::SigInfo,
    ) -> Option<i32> {
        // An exit operation has its entry only while its slot has the descriptor.
        let Some(pidfd) = self.slots.get(index as usize).and_then(|slot| slot.pidfd.as_ref()) else {
            return Some(res);
        };
        let result = match res {
            // Only teardown cancels an exit operation, for a child with `End::Keep`. The home
            // reaps a child that ended just then, and it counts a child that runs.
            res if res == -ECANCELED => match unsafe_call::reap(pidfd, info) {
                Ok(true) => 0,
                _ => {
                    self.kept = self.kept.saturating_add(1);
                    -ECANCELED
                }
            },
            res if res < 0 || ring => res,
            // The descriptor is readable: the child ended. A child that another process traces
            // shows its status to that process first, so the poll repeats until it is ready.
            _ => match unsafe_call::reap(pidfd, info) {
                Ok(true) => 0,
                Ok(false) if self.ending && !self.kills(index) => {
                    self.kept = self.kept.saturating_add(1);
                    -ECANCELED
                }
                Ok(false) => {
                    // At most one entry for each child, so the capacity of the list holds it.
                    self.again.push(id);
                    return None;
                }
                Err(errno) => -errno,
            },
        };
        self.release(index);
        Some(result)
    }
}

/// The error of a signal that the system refused. A child that ended and that the kernel reaped
/// has no process for its descriptor.
fn refusal(errno: i32) -> SignalChildError {
    match errno {
        ESRCH => SignalChildError::Reaped,
        errno => SignalChildError::System(io::Error::from_raw_os_error(errno)),
    }
}

/// Opens the process descriptor of the process `pid`, which closes on `exec`.
fn pidfd_open(pid: u32) -> io::Result<OwnedFd> {
    unsafe_call::pidfd_open(pid)
}

/// The system calls of children. Each one names no memory, or only the structure that it gets.
#[expect(unsafe_code, reason = "each call names no memory or only the structure that it gets")]
mod unsafe_call {
    use core::ffi::c_int;
    use std::io;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};

    use super::super::sys;

    /// The `errno` of the last failed call.
    fn errno() -> i32 {
        io::Error::last_os_error().raw_os_error().unwrap_or(super::EIO)
    }

    pub(super) fn getpid() -> c_int {
        // SAFETY: `getpid` names no memory.
        unsafe { sys::getpid() }
    }

    pub(super) fn pidfd_open(pid: u32) -> io::Result<OwnedFd> {
        // SAFETY: `pidfd_open` names no memory.
        let fd = unsafe { sys::syscall(sys::SYS_PIDFD_OPEN, pid, 0_u32) };
        match c_int::try_from(fd) {
            // SAFETY: the kernel gave a new descriptor that nothing else owns.
            Ok(fd) if fd >= 0 => Ok(unsafe { OwnedFd::from_raw_fd(fd) }),
            _ => Err(io::Error::last_os_error()),
        }
    }

    pub(super) fn pidfd_send_signal(pidfd: &OwnedFd, signal: c_int) -> Result<(), i32> {
        // SAFETY: a null `info` makes the kernel fill the information as `kill` does.
        let result = unsafe {
            sys::syscall(
                sys::SYS_PIDFD_SEND_SIGNAL,
                pidfd.as_raw_fd(),
                signal,
                core::ptr::null::<u8>(),
                0_u32,
            )
        };
        if result == 0 { Ok(()) } else { Err(errno()) }
    }

    pub(super) fn kill_group(group: c_int, signal: c_int) -> Result<(), i32> {
        // SAFETY: `kill` names no memory.
        if unsafe { sys::kill(group.saturating_neg(), signal) } == 0 {
            Ok(())
        } else {
            Err(errno())
        }
    }

    /// Whether the driver still holds the child of `pidfd` unreaped. The look reaps nothing.
    pub(super) fn held(pidfd: &OwnedFd) -> bool {
        let mut info = sys::SigInfo::default();
        let options = sys::WEXITED | sys::WNOHANG | sys::WNOWAIT;
        // SAFETY: the call writes only `info`.
        unsafe { sys::waitid(sys::P_PIDFD, pidfd.as_raw_fd().cast_unsigned(), &raw mut info, options) == 0 }
    }

    /// Reaps the child of `pidfd` into `info` when it ended. Gives whether it ended.
    pub(super) fn reap(pidfd: &OwnedFd, info: &mut sys::SigInfo) -> Result<bool, i32> {
        *info = sys::SigInfo::default();
        let options = sys::WEXITED | sys::WNOHANG;
        // SAFETY: the call writes only `info`.
        if unsafe { sys::waitid(sys::P_PIDFD, pidfd.as_raw_fd().cast_unsigned(), info, options) } == 0 {
            Ok(info.pid != 0)
        } else {
            Err(errno())
        }
    }
}

/// The entry of the exit operation of the child `pidfd`: an `IORING_OP_WAITID` that writes the
/// status into the payload of `held` when `ring` holds, or a poll of the descriptor.
fn exit_entry(ring: bool, pidfd: std::os::fd::RawFd, held: &mut Held) -> Sqe {
    match &mut held.payload {
        Payload::Exit(info) if ring => {
            let options = sys::WEXITED.cast_unsigned();
            Sqe::waitid(sys::P_PIDFD.cast_unsigned(), pidfd, options, (&raw mut *info).cast())
        }
        _ => Sqe::poll_readable(pidfd),
    }
}

/// The output of a spawn: the child, or the error of the spawn.
fn spawned_child(result: i32, held: Held) -> Option<io::Result<Child>> {
    Some(match held.payload {
        Payload::Spawn { child: Some(child), .. } if result >= 0 => Ok(child),
        _ => Err(io::Error::from_raw_os_error(
            result.checked_neg().filter(|errno| *errno > 0).unwrap_or(EIO),
        )),
    })
}

impl Driver {
    /// Spawns `command` as a child of the driver, with `options`. The spawn runs as a job of the
    /// blocking service of [`Driver::use_blocking`], because process creation blocks. The future
    /// gives the [`Child`], with a pipe end of the driver for each standard stream that the command
    /// pipes ([`std::process::Stdio::piped`]), or the error of the spawn. A drop of the future ends
    /// only the interest: the child starts, and its [`End`] applies as at a drop of the child.
    ///
    /// While its job runs, a spawn holds an operation slot, a job slot of the service, and three
    /// resource slots for the pipe ends. Each child holds a child slot ([`Config::children`]) until
    /// the driver reaps it, and an operation slot until its status is taken or it drops.
    ///
    /// [`Config::children`]: super::Config::children
    ///
    /// # Errors
    ///
    /// A refusal, with `command`, that gives [`Reason::Children`] when each child slot is in use,
    /// [`Reason::Blocking`] when the driver has no service or the service refused the job,
    /// [`Reason::Resources`] for fewer than three free resource slots, [`Reason::Unsupported`] on
    /// a kernel with no process descriptor, or [`Reason::Operations`] or [`Reason::Queue`] when
    /// the driver has no room for the operations.
    #[expect(
        clippy::result_large_err,
        reason = "a refusal gives the caller its `Command` back, which the design requires, and a refusal ends the call"
    )]
    pub fn spawn(
        &mut self,
        command: Command,
        options: ChildOptions,
    ) -> Result<Operation<'static, io::Result<Child>>, Refused<Command>> {
        #[expect(clippy::result_large_err, reason = "the refusal gives the command back")]
        let refuse = |reason, value| Err(Refused { reason, value });
        let Some(index) = self.children.free() else {
            return refuse(Reason::Children, command);
        };
        if !self.children.descriptors() {
            return refuse(Reason::Unsupported, command);
        }
        let Some(submitter) = self.blocking.as_ref().map(|(submitter, _)| submitter.clone()) else {
            return refuse(Reason::Blocking, command);
        };
        if !self.resources.reserve(STREAMS) {
            return refuse(Reason::Resources, command);
        }
        // The exit operation starts with the spawn, so the end of the job always has its slot.
        let exit = match self.open_exit(index, 0) {
            Ok(exit) => exit,
            Err(reason) => {
                self.resources.unreserve(STREAMS);
                return refuse(reason, command);
            }
        };
        let tag = Tag {
            resource: None,
            kind: Kind::Spawn(index),
        };
        let job = SpawnJob {
            command,
            tree: options.tree,
        };
        #[expect(clippy::result_large_err, reason = "the refusal of the service gives the job back")]
        let held = move |job| match submitter.submit(job) {
            Ok(waiter) => Ok(Held {
                payload: Payload::Spawn { waiter, child: None },
                ..Held::default()
            }),
            Err(refused) => Err((Reason::Blocking, refused.job)),
        };
        match self.open_with(tag, 0, job, held) {
            Ok(id) => {
                if let Some(slot) = self.children.slot(index) {
                    (slot.exit, slot.tree, slot.end) = (Some(exit), options.tree, options.end);
                }
                // At most one entry for each operation, so the capacity of the list holds it.
                self.jobs.push(id);
                let completion = Completion::new(Arc::clone(&self.shared), id, false);
                Ok(Operation::new(completion, spawned_child))
            }
            Err((reason, job)) => {
                self.end_exit(exit);
                self.resources.unreserve(STREAMS);
                refuse(reason, job.command)
            }
        }
    }

    /// Takes `child`, which the caller started, for example suspended or with its own launcher,
    /// with the end policy `end`. The driver takes its piped standard streams as pipe ends, and
    /// clears `O_NONBLOCK` of each one. A child that the caller adopts leads no process tree for
    /// the driver.
    ///
    /// # Errors
    ///
    /// A refusal, with `child`, that gives [`Reason::Children`] when each child slot is in use,
    /// [`Reason::Exited`] when the child ended and its status is in `child` (call its `wait`),
    /// [`Reason::Resources`] when the driver has no resource slot for a pipe end,
    /// [`Reason::Unsupported`] when the kernel gives no process descriptor, or
    /// [`Reason::Operations`] or [`Reason::Queue`] when the driver has no room for the operation.
    pub fn adopt_child(
        &mut self,
        mut child: std::process::Child,
        end: End,
    ) -> Result<Child, Refused<std::process::Child>> {
        let refuse = |reason, value| Err(Refused { reason, value });
        let Some(index) = self.children.free() else {
            return refuse(Reason::Children, child);
        };
        if !self.children.descriptors() {
            return refuse(Reason::Unsupported, child);
        }
        let streams = [child.stdin.is_some(), child.stdout.is_some(), child.stderr.is_some()]
            .into_iter()
            .map(u32::from)
            .sum();
        if !self.resources.reserve(streams) {
            return refuse(Reason::Resources, child);
        }
        let exit = match self.open_exit(index, 1) {
            Ok(exit) => exit,
            Err(reason) => {
                self.resources.unreserve(streams);
                return refuse(reason, child);
            }
        };
        // The standard library keeps the status that its own wait took. Such a child is reaped,
        // and its process ID can name another process now, so the driver opens no descriptor for
        // it. A child that is not reaped keeps its process ID, so the descriptor names it.
        let pidfd = match child.try_wait() {
            Ok(None) => pidfd_open(child.id()).map_err(|_| Reason::Unsupported),
            Ok(Some(_)) | Err(_) => Err(Reason::Exited),
        };
        let pidfd = match pidfd {
            Ok(pidfd) => pidfd,
            Err(reason) => {
                self.end_exit(exit);
                self.resources.unreserve(streams);
                return refuse(reason, child);
            }
        };
        if let Some(slot) = self.children.slot(index) {
            (slot.exit, slot.tree, slot.end) = (Some(exit), false, end);
        }
        let spawned = Spawned {
            pidfd,
            pid: child.id(),
            stdin: child.stdin.take().map(OwnedFd::from),
            stdout: child.stdout.take().map(OwnedFd::from),
            stderr: child.stderr.take().map(OwnedFd::from),
        };
        Ok(self.register(index, exit, spawned, streams))
    }

    /// Sends `signal` to `child` through its process descriptor, so a reused process ID is never
    /// the target. A signal ends no wait: the wait ends when the child ends.
    ///
    /// # Errors
    ///
    /// [`SignalChildError::Reaped`] after the reap, and [`SignalChildError::System`] when the
    /// system refuses the signal.
    pub fn signal_child(&mut self, child: &Child, signal: Signal) -> Result<(), SignalChildError> {
        self.children.signal(child.id, signal.as_raw(), false)
    }

    /// Kills `child` (`SIGKILL`) through its process descriptor. The wait then gives the status.
    ///
    /// # Errors
    ///
    /// As for [`Driver::signal_child`].
    pub fn kill_child(&mut self, child: &Child) -> Result<(), SignalChildError> {
        self.children.signal(child.id, SIGKILL, false)
    }

    /// Sends `signal` to each process of the tree of `child` ([`ChildOptions::tree`]): its process
    /// group. The driver signals the group only while it holds the child unreaped, so the group
    /// ID cannot name another group.
    ///
    /// # Errors
    ///
    /// [`SignalChildError::NoTree`] for a child with no tree, [`SignalChildError::Reaped`] after
    /// the reap of the child, and [`SignalChildError::System`] when the system refuses the signal.
    pub fn signal_tree(&mut self, child: &Child, signal: Signal) -> Result<(), SignalChildError> {
        self.children.signal(child.id, signal.as_raw(), true)
    }

    /// Kills (`SIGKILL`) each process of the tree of `child`, as [`Driver::signal_tree`] does.
    ///
    /// # Errors
    ///
    /// As for [`Driver::signal_tree`].
    pub fn kill_tree(&mut self, child: &Child) -> Result<(), SignalChildError> {
        self.children.signal(child.id, SIGKILL, true)
    }

    /// Closes `child` now by its [`End`], as its drop does at the next turn: with [`End::Kill`],
    /// the driver kills the child, or each process of its tree, and then reaps it; with
    /// [`End::Keep`], the child runs on, and the driver reaps it when it ends.
    pub fn close_child(&mut self, child: Child) {
        drop(child);
        self.take_requests();
    }

    /// Begins the exit operation of the child slot `index`, with room for `entries` entries.
    fn open_exit(&mut self, index: u32, entries: u32) -> Result<OpId, Reason> {
        let tag = Tag {
            resource: None,
            kind: Kind::Exit(index),
        };
        let held = |()| Held {
            payload: Payload::Exit(sys::SigInfo::default()),
            ..Held::default()
        };
        self.open(tag, entries, (), held).map_err(|(reason, ())| reason)
    }

    /// Ends the exit operation `exit`, which never had its entry, with no consumer.
    fn end_exit(&mut self, exit: OpId) {
        // The operation runs, so the end of the interest gives no result.
        self.table.end_interest(exit);
        self.complete_now(exit, -ECANCELED);
    }

    /// Registers the child of `spawned` in the slot `index`: its descriptor, its pipe ends, which
    /// take `reserved` reserved resource slots, and the entry of its exit operation `exit`. The
    /// queue has room for the entry.
    #[expect(unsafe_code, reason = "the entry of the exit operation names its own payload")]
    fn register(&mut self, index: u32, exit: OpId, spawned: Spawned, reserved: u32) -> Child {
        let raw = spawned.pidfd.as_raw_fd();
        let (generation, end) = match self.children.slot(index) {
            Some(slot) => {
                (slot.pidfd, slot.pid) = (Some(spawned.pidfd), spawned.pid.cast_signed());
                (slot.generation, slot.end)
            }
            None => (0, End::Keep),
        };
        let mut used = 0;
        let mut pipe = |fd: Option<OwnedFd>| {
            // A pipe end that a caller made nonblocking ends each read and write at once.
            let fd = fd.filter(super::pipe::blocks)?;
            let resource = self.resources.adopt_reserved(fd, Medium::Pipe)?;
            used += 1;
            Some(resource)
        };
        let stdin = pipe(spawned.stdin).map(|resource| PipeWriter {
            write: WriteHalf { resource },
        });
        let stdout = pipe(spawned.stdout).map(|resource| PipeReader {
            read: ReadHalf { resource },
        });
        let stderr = pipe(spawned.stderr).map(|resource| PipeReader {
            read: ReadHalf { resource },
        });
        self.resources.unreserve(reserved.saturating_sub(used));
        let ring = self.report.route(Call::WaitChild) == Route::Ring;
        // SAFETY: the entry names the status structure in the payload of `exit`, which stays in
        // its slot until the terminal event, and the descriptor, which the child slot keeps open
        // until that event. The caller made room in the queue.
        unsafe { self.push(exit, |held| exit_entry(ring, raw, held)) };
        Child {
            stdin,
            stdout,
            stderr,
            id: ChildId { index, generation },
            pid: spawned.pid,
            exit: Completion::new(Arc::clone(&self.shared), exit, end == End::Kill),
            status: None,
        }
    }

    /// Applies the end of the job of the spawn `id` for the child slot `index`.
    pub(super) fn spawn_ended(&mut self, id: OpId, index: u32, ended: Result<io::Result<Spawned>, Failed>) {
        let Some(exit) = self.children.slot(index).and_then(|slot| slot.exit) else {
            return;
        };
        let spawned = match ended {
            Ok(Ok(spawned)) => spawned,
            failed => {
                let errno = match failed {
                    Ok(Err(error)) => error.raw_os_error().unwrap_or(EIO),
                    _ => EIO,
                };
                self.children.release(index);
                self.end_exit(exit);
                self.resources.unreserve(STREAMS);
                self.complete_now(id, -errno);
                return;
            }
        };
        let child = self.register(index, exit, spawned, STREAMS);
        if let Some(slot) = self.shared.entries.get(id.index() as usize) {
            slot.update(id, |held| {
                if let Some(Held {
                    payload: Payload::Spawn { child: place, .. },
                    ..
                }) = held
                {
                    *place = Some(child);
                }
            });
        }
        self.complete_now(id, 0);
    }

    /// Polls the job of the spawn `id`.
    pub(super) fn poll_spawn(&mut self, id: OpId, cx: &Context<'_>) -> Option<Result<io::Result<Spawned>, Failed>> {
        let mut ended = None;
        if let Some(slot) = self.shared.entries.get(id.index() as usize) {
            slot.update(id, |held| {
                if let Some(Held {
                    payload: Payload::Spawn { waiter, .. },
                    ..
                }) = held
                    && let Poll::Ready(result) = waiter.poll(cx)
                {
                    ended = Some(result);
                }
            });
        }
        ended
    }

    /// Polls again each exit operation whose poll ended before the status of its child was ready.
    /// One that finds no room in the queue stays for the next turn.
    #[expect(unsafe_code, reason = "a poll names no memory")]
    pub(super) fn poll_again(&mut self) {
        while let Some(&id) = self.children.again.last() {
            let fd = match self.table.resources(id) {
                Some(&Tag {
                    kind: Kind::Exit(index),
                    ..
                }) => self
                    .children
                    .slots
                    .get(index as usize)
                    .and_then(|slot| slot.pidfd.as_ref()),
                _ => None,
            };
            if let Some(fd) = fd {
                let entry = Sqe::poll_readable(fd.as_raw_fd()).with_user_data(id.user_data());
                // SAFETY: a poll names no memory.
                if unsafe { super::push_now(&mut self.ring, &entry) }.is_err() {
                    return;
                }
            }
            self.children.again.pop();
        }
    }
}

// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
pub(super) mod tests;
