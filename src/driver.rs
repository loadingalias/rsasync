//! Driver-only use: the caller owns a driver, submits operations to it, and drives it with bounded
//! nonblocking turns. No scheduler runs. On Linux, the driver runs on io_uring, or on epoll where the
//! kernel refuses io_uring. On macOS, it runs TCP, UDP, IPC (inter-process communication) streams, pipes, child processes, and
//! signal streams on kqueue, and files on the blocking service. On Windows, it runs TCP, UDP, IPC
//! streams, and files on an I/O completion port, with the reads, writes, and syncs of files on an
//! I/O ring where the system has one, and the other file calls on the blocking service
//! ([`Backend`]). Each backend has
//! the same operations and the same ownership rules.
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
//! - On Linux and macOS, the wake descriptor, `AsFd` of the driver, becomes readable when work
//!   comes, so a host's reactor can wait for it.
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
//! [`Driver::report`] states what the driver found and applied when it started: the backend, the
//! features of the kernel, and the [`Route`] of each [`Call`] that depends on them. The driver
//! starts no helper thread. A kernel with no ring operation for a file call runs that call on the
//! blocking service of the caller, which [`Driver::use_blocking`] gives. On epoll and kqueue each
//! file call runs there, and on IOCP each file call with no overlapped form. [`Config::require`]
//! makes a missing feature refuse the driver, and [`Config::backend`] a missing backend.
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

mod capability;
mod completion;
mod control;
mod file;
#[cfg(all(feature = "local", target_os = "linux"))]
mod home;
#[cfg(all(feature = "local", target_os = "linux"))]
pub(crate) use home::LocalTask;
#[cfg(target_os = "windows")]
mod iocp;
mod ipc;
#[cfg(unix)]
mod pipe;
#[cfg(all(feature = "local", target_os = "linux"))]
mod plain;
#[cfg(unix)]
mod poller;
#[cfg(target_os = "linux")]
mod probe;
#[cfg(unix)]
mod process;
mod queue;
#[cfg(unix)]
mod signal;
// Two attributes each, so that Clippy sees the test modules (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
mod suite;
#[cfg(target_os = "linux")]
mod sys;
mod tcp;
#[cfg(test)]
#[cfg(not(loom))]
pub(crate) mod tests;
mod udp;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::alloc::AllocError;
#[cfg(target_os = "linux")]
use core::cell::Cell;
use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::mem::{self, ManuallyDrop};
use core::num::NonZeroU64;
use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::{Context, Poll};
#[cfg(unix)]
use std::fs;
use std::io;
#[cfg(unix)]
use std::io::{Read as _, Write as _};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd as _;
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd, FromRawFd as _, OwnedFd};
// The descriptor of a resource, as the platform names it: a file descriptor on Linux and macOS,
// and a socket handle on Windows, where the driver owns only sockets.
#[cfg(unix)]
use std::os::fd::{BorrowedFd as Borrowed, OwnedFd as Handle};
#[cfg(target_os = "windows")]
use std::os::windows::io::{BorrowedSocket as Borrowed, OwnedSocket as Handle};

use rsnetwork_udp::SockAddrStorage;

use crate::buffer::{Backing, BufferMut as _, Lease, Pool, WriteLoan};
use crate::memory::{items, room};
use crate::sync::{Arc, try_arc};
#[cfg(target_os = "linux")]
use crate::table::CONTROL;
use crate::table::{Ended as Done, Event, MAX_SLOTS, OpId, Outcome, Request, Requests, Shape, Table, Taken};
use crate::uring::{BufferRing, Cqe, Sqe};
#[cfg(target_os = "linux")]
use crate::uring::{Ring, SetupError};
use queue::Queue;

pub use capability::{Backend, Call, Feature, Features, Files, Report, Route, Shot};
pub use control::{Control, ControlError, Delivery, OperationId};
#[cfg(target_os = "windows")]
pub use file::NvmePath;
pub use file::{Alignment, Buffers, Cache, Directory, File, Incomplete, Length, OpenOptions, SyncMode, WriteMode};
#[cfg(target_os = "windows")]
pub use ipc::Sid;
pub use ipc::{IpcListener, IpcStream, Peer, Stale};
#[cfg(unix)]
pub use pipe::{PipeReader, PipeWriter};
#[cfg(unix)]
pub use process::{Child, ChildOptions, End, SignalChildError, Wait};
#[cfg(unix)]
pub use signal::{Escalation, Recv, Signal, SignalError, SignalStream};
pub use tcp::{
    Closed, ReadHalf, Regions, SendCount, Socket, TcpListener, TcpOptions, TcpStream, WriteFailed, WriteHalf,
};
pub use udp::{Arrivals, Datagram, Datagrams, UdpOptions, UdpSocket};
// The outputs of the operations that make a resource, which the local I/O types wrap.
#[cfg(all(feature = "local", target_os = "linux"))]
pub(crate) use tcp::{accepted, bound as tcp_bound, connected};
#[cfg(all(feature = "local", target_os = "linux"))]
pub(crate) use udp::bound as udp_bound;
#[cfg(all(feature = "local", target_os = "linux"))]
pub(crate) use {
    file::{directory_opened, file_opened},
    ipc::{ipc_bound, ipc_connected},
    process::spawned_child,
};

use crate::completion::Entry;
pub(crate) use completion::Completion;
#[cfg(all(feature = "local", target_os = "linux"))]
pub(crate) use home::{Cleanup, Home, MESSAGES};
// The operations of the movable I/O types, and their outputs.
#[cfg(all(feature = "local", target_os = "linux"))]
pub(crate) use plain::{Direction, PlainSocket, socket as plain_socket};
#[cfg(all(feature = "local", target_os = "linux"))]
pub(crate) use tcp::{received, sent};

/// The limits of a driver, each of which bounds memory that the driver allocates when it starts,
/// and the features that it requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    submissions: u32,
    operations: u32,
    resources: u32,
    #[cfg(unix)]
    children: u32,
    completions_per_turn: u32,
    required: Features,
    backend: Option<Backend>,
    delivery: Delivery,
    files: Files,
    receives: Shot,
}

impl Config {
    /// 64 submission entries, 256 operations, 256 resources, 64 children, 64 completions per turn,
    /// no required feature, the permissive selection of the backend, native delivery, plain
    /// descriptors, and one submission for each receive.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            submissions: 64,
            operations: 256,
            resources: 256,
            #[cfg(unix)]
            children: 64,
            completions_per_turn: 64,
            required: Features::NONE,
            backend: None,
            delivery: Delivery::Native,
            files: Files::Plain,
            receives: Shot::Single,
        }
    }

    /// Gives the completions of the backend by `delivery`. With [`Delivery::Controlled`],
    /// [`Driver::control`] gives the [`Control`] that holds, fails, or repeats them, so a test of
    /// the caller makes each order of completions and requests on demand.
    ///
    /// ```
    /// # #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    /// # fn main() -> Result<(), rsasync::driver::Error> {
    /// // The same text as `docs/content/controlled-delivery.md`. Edit both.
    /// use rsasync::driver::{Config, Delivery, Driver};
    ///
    /// let mut driver = Driver::new(Config::new().delivery(Delivery::Controlled))?;
    /// assert!(driver.control().is_some());
    /// # Ok(())
    /// # }
    /// # #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn delivery(self, delivery: Delivery) -> Self {
        Self { delivery, ..self }
    }

    /// Requires `backend`: with [`Backend::IoUring`], a kernel that refuses io_uring makes
    /// [`Driver::new`] refuse with [`Error::Refused`], before a driver exists. With no
    /// requirement, the driver runs io_uring, and it runs epoll when the creation of the ring
    /// fails with `EPERM` or `ENOSYS`. [`Report::backend`] and [`Report::fallback`] state it.
    ///
    /// On Windows, [`Backend::IoRing`] runs the reads, writes, and syncs of files on an I/O ring,
    /// and a system whose probe refuses the ring makes [`Driver::new`] refuse with
    /// [`Error::Refused`]. [`Backend::Iocp`] runs each file operation as on a system with no ring.
    /// With no requirement, the driver runs IoRing where the probe passes, and IOCP else.
    #[must_use]
    pub const fn backend(self, backend: Backend) -> Self {
        Self {
            backend: Some(backend),
            ..self
        }
    }

    /// Requires `feature`: on a kernel with no such feature, [`Driver::new`] refuses with
    /// [`Error::Unsupported`], before a driver exists. With no requirement, the driver uses the
    /// route of each call that the kernel has, and [`Driver::report`] states it.
    #[must_use]
    pub const fn require(self, feature: Feature) -> Self {
        Self {
            required: self.required.with(feature),
            ..self
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

    /// The descriptors that the operations of the resources name: [`Files::Plain`], the default,
    /// or [`Files::Registered`], a file table of the ring. [`Report::files`] gives the table in use.
    #[must_use]
    pub const fn files(self, files: Files) -> Self {
        Self { files, ..self }
    }

    /// How the receives of a stream into kernel-selected buffers run: [`Shot::Single`], the
    /// default, or [`Shot::Multi`], one multishot receive for each stream on io_uring.
    /// [`Report::receives`] gives the form in use.
    #[must_use]
    pub const fn receives(self, receives: Shot) -> Self {
        Self { receives, ..self }
    }

    /// The resources, such as descriptors, that the driver can own at one time.
    #[must_use]
    pub const fn resources(self, resources: u32) -> Self {
        Self { resources, ..self }
    }

    /// The child processes that the driver can own at one time ([`Driver::spawn`]). Each one also
    /// holds an operation slot until its status is taken or it drops.
    #[cfg(unix)]
    #[must_use]
    pub const fn children(self, children: u32) -> Self {
        Self { children, ..self }
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
    /// default seccomp profile of a container or with `kernel.io_uring_disabled`, and
    /// [`Config::backend`] requires [`Backend::IoUring`]. On Windows, the IoRing probe refuses the
    /// ring and [`Config::backend`] requires [`Backend::IoRing`]: the error is the `HRESULT` of
    /// [`Report::fallback`]. [`Config::backend`] also refuses with this error, of the kind
    /// `Unsupported`, a backend that the system does not have.
    Refused(io::Error),
    /// A limit of [`Config`] is 0 or above what the kernel takes.
    Config,
    /// Another failure of the system, for example no memory.
    System(io::Error),
    /// The kernel has no feature of these, which [`Config::require`] required.
    Unsupported(Features),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(error) => write!(f, "the system refuses the required backend: {error}"),
            Self::Config => f.write_str("a limit of the driver configuration is 0 or too large"),
            Self::System(error) => write!(f, "the driver did not start: {error}"),
            Self::Unsupported(missing) => write!(f, "the kernel has no required feature of {missing:?}"),
        }
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Refused(error) | Self::System(error) => Some(error),
            Self::Config | Self::Unsupported(_) => None,
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
    /// end of the poll of its wake descriptor, so the count can exceed the operations that
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
    #[cfg(unix)]
    kept: u32,
}

impl Teardown {
    /// The operations that were still running, which teardown cancelled. Each one has ended.
    #[must_use]
    pub const fn cancelled(self) -> u32 {
        self.cancelled
    }

    /// The children with [`End::Keep`] that teardown left unreaped, because they still ran.
    /// Nothing reaps them now: each one stays a zombie after its end until this process ends.
    #[cfg(unix)]
    #[must_use]
    pub const fn kept(self) -> u32 {
        self.kept
    }
}

/// A vector of `count` items from `make`, or `AllocError` when the allocator refuses it.
pub(crate) fn filled<T>(count: usize, make: impl FnMut() -> T) -> Result<Vec<T>, AllocError> {
    let mut items = room(count)?;
    items.resize_with(count, make);
    Ok(items)
}

/// The I/O error of a refusal of the allocator.
pub(crate) fn out_of_memory(_: AllocError) -> io::Error {
    io::ErrorKind::OutOfMemory.into()
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
/// operation, in the slot of the operation, so each address that the kernel uses stays the same.
/// The fields hold what the home writes for the kinds of more than one medium, and the payload
/// holds the resources of the kind.
///
/// The order of the fields is stated: the fields of each kind come first, and the large payload
/// last, so a receive or a send touches the first cache lines of its slot.
#[derive(Debug)]
#[repr(C)]
pub(crate) struct Held {
    /// The loan of the buffer that a receive or a read fills.
    pub(crate) loan: Option<WriteLoan<Backing>>,
    /// The next operation that waits in the same direction, in start order.
    pub(crate) next: Option<OpId>,
    /// The home asked the kernel to cancel the operation.
    pub(crate) cancelled: bool,
    /// An error of the home that the future gives in place of the result of the kernel.
    pub(crate) error: Option<io::ErrorKind>,
    /// The bytes that a receive copied from the data that an abandoned receive left.
    pub(crate) copied: usize,
    /// A send on a failed direction gives this error, with no submission.
    pub(crate) failed: Option<WriteFailed>,
    /// The socket that an accept, a connect, or a bind gave, or the file that an open gave.
    pub(crate) accepted: Option<Resource>,
    /// The report of a close.
    pub(crate) closed: Option<Closed>,
    /// The descriptor and the chain of an operation of a movable resource, which no driver owns.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) plain: Option<plain::Plain>,
    /// The resources of the kind of the operation. Its start sets the variant, and nothing
    /// changes the variant before the terminal event.
    pub(crate) payload: Payload,
}

impl Held {
    /// The operation starts with its payload and no result or waiter.
    const fn with_payload(payload: Payload) -> Self {
        Self {
            payload,
            loan: None,
            accepted: None,
            next: None,
            failed: None,
            closed: None,
            copied: 0,
            cancelled: false,
            error: None,
            #[cfg(all(feature = "local", target_os = "linux"))]
            plain: None,
        }
    }
}

impl Default for Held {
    fn default() -> Self {
        Self::with_payload(Payload::Empty)
    }
}

// Each of the `Config::operations` slots holds one `Held`, so a kind with large inline resources
// goes into its own variant of `Payload`, and a larger bound needs a reviewed change. The bound is
// the size on a 64-bit target, where the largest variant is `Payload::SendTo`.
const _: () = assert!(
    size_of::<Held>() <= 576,
    "the resources of an operation grew: put the new resources into the payload of their kind"
);

/// The resources of one kind of operation.
#[derive(Debug, Default)]
pub(crate) enum Payload {
    /// No resources: for example a shutdown, a close, a receive, or a sync of a file.
    #[default]
    Empty,
    /// The lease of a send or a write.
    Lease(Lease),
    /// The regions of a vectored send or write, and the message header that names them.
    Regions(Regions),
    /// The address of a connect or a bind, the local address that a bind gives, or the peer
    /// address that an accept writes, with its length.
    Socket {
        peer: SockAddrStorage,
        peer_len: u32,
        /// The connection of an accept with no consumer, which the listener keeps.
        connection: Option<Handle>,
    },
    /// The message of a datagram receive, which the kernel writes, and the facts of the datagram.
    Receive {
        message: udp::Message,
        received: Option<rsnetwork_udp::Received>,
    },
    /// The datagram of a send, and its message, which the kernel reads.
    SendTo { message: udp::Message, datagram: Datagram },
    /// The datagrams of a batch, and its progress while its completions come.
    Batch { batch: udp::Batch, progress: udp::Progress },
    /// The paths of a rename or an unlink, which the kernel reads: C strings, and UTF-16 strings on
    /// Windows.
    Paths {
        path: file::Name,
        target: Option<file::Name>,
    },
    /// The open of a file or a directory: its path, which the kernel reads, and what the open
    /// gives, such as the alignment of direct I/O.
    Open(file::Opening),
    /// The structure that a `statx` of a file writes on Linux, an `fstat` on macOS, and
    /// `GetFileInformationByHandleEx` on Windows.
    Stat(file::Stat),
    /// The buffers of a vectored read of a file.
    Buffers(Buffers),
    /// The waiter of the job of an operation that runs on the blocking service.
    Job(crate::blocking::Waiter<i32>),
    /// The waiter of the job of a spawn, and the child that the home makes at the end of the job.
    #[cfg(unix)]
    Spawn {
        waiter: crate::blocking::Waiter<io::Result<process::Spawned>>,
        child: Option<Child>,
    },
    /// The status of a child, which a `waitid` of the exit operation writes.
    #[cfg(unix)]
    Exit(process::SigInfo),
}

impl Payload {
    /// Takes only the lease. An empty static slice needs no allocation or release.
    fn take_lease(&mut self) -> Option<Lease> {
        match self {
            Self::Lease(lease) => Some(mem::replace(lease, Lease::from(&[][..]))),
            _ => None,
        }
    }

    /// The address of a connect or a bind, the local address that a bind gives, or the peer
    /// address that an accept writes, with its length.
    pub(crate) fn peer(&mut self) -> Option<(&mut SockAddrStorage, &mut u32)> {
        match self {
            Self::Socket { peer, peer_len, .. } => Some((peer, peer_len)),
            _ => None,
        }
    }

    /// The datagrams of a batch.
    pub(crate) fn batch(&mut self) -> Option<&mut udp::Batch> {
        match self {
            Self::Batch { batch, .. } => Some(batch),
            _ => None,
        }
    }
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
    /// The multishot receive of the read direction of a stream ([`Shot::Multi`]): one completion
    /// for each provided buffer that it fills, which its direction keeps for the receives. No
    /// future consumes it.
    Multishot,
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
    /// A spawn of a child in the child slot of the index, as a job of the blocking service.
    #[cfg(unix)]
    Spawn(u32),
    /// The exit of the child in the child slot of the index: it ends when the driver reaps the
    /// child.
    #[cfg(unix)]
    Exit(u32),
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

/// The facts of an operation that the operation table keeps for the home: its resource, its
/// kind, and who consumes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tag {
    pub(crate) resource: Option<Resource>,
    pub(crate) kind: Kind,
    pub(crate) consumer: Consumer,
}

impl Tag {
    /// The tag of an operation of `kind` on `resource`, whose consumer can be on any thread.
    pub(crate) const fn new(resource: Option<Resource>, kind: Kind) -> Self {
        Self {
            resource,
            kind,
            consumer: Consumer::Any,
        }
    }
}

/// Where the consumer of an operation polls it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Consumer {
    /// On any thread: the completion entry and its waiter give the result and the wake.
    Any,
    /// On the thread of the home, through its driver: the operation of a local type, whose future
    /// is not `Send`. The home takes the result in place and wakes the waiter that the driver
    /// keeps.
    #[cfg(all(feature = "local", target_os = "linux"))]
    Home,
}

/// Why the driver refused an operation. The caller keeps what it gave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reason {
    /// Each operation slot is in use.
    Operations,
    /// The submission queue is full, also after one submit.
    Queue,
    /// The descriptor or the child is not one that this driver owns: another driver made it, or it
    /// closed.
    Resource,
    /// A call of `rsasync::local` or `rsasync::movable` has no driver on this thread that it can
    /// use: the thread runs no owner of a `Runtime`, its owner is a `task::LocalOwner`, which has no
    /// driver, or the driver of `Runtime::block_on` did not start, and `block_on` then gives its
    /// error. A turn of the driver also uses it, so a call in a destructor that the turn runs gets
    /// this refusal.
    #[cfg(all(feature = "local", target_os = "linux"))]
    NoDriver,
    /// The driver has no provided buffers ([`Driver::provide`]).
    Buffers,
    /// Each resource slot of the driver is in use.
    Resources,
    /// The descriptor is not a pipe or a FIFO.
    #[cfg(unix)]
    NotPipe,
    /// The pipe end does not have the direction of the adoption: a read end for a writer, or a
    /// write end for a reader.
    #[cfg(unix)]
    Direction,
    /// The operation does not apply to this resource, for example a half-close of a pipe. A call
    /// of `rsasync::movable` also gives it when the driver of the thread runs on epoll: only
    /// io_uring runs the operations of a movable resource.
    Unsupported,
    /// The buffer, the length, or the offset of a direct-I/O operation does not have the
    /// [`Alignment`] of its file.
    Alignment,
    /// The operation runs on the blocking service on this kernel or backend
    /// ([`Route::BlockingService`]), and the driver has no service ([`Driver::use_blocking`]), or
    /// the service refused the job.
    Blocking,
    /// The allocator did not give the memory of the operation, such as the job of an operation on
    /// the blocking service. A later operation can succeed.
    Allocation,
    /// Each child slot of the driver is in use ([`Config::children`]).
    #[cfg(unix)]
    Children,
    /// The child ended, and the [`std::process::Child`] that the refusal gives back has its
    /// status.
    #[cfg(unix)]
    Exited,
    /// The backend did not take the descriptor: on epoll, `epoll_ctl` failed, for example at the
    /// limit of `fs.epoll.max_user_watches`. On kqueue, `kevent` refused a registration, for
    /// example with no kernel memory. On IOCP, the association of the socket with the port failed
    /// (`CreateIoCompletionPort`), or the socket handle has significant bits above the low 32.
    Register,
}

/// A refused operation: the reason, and the value that the caller gave, which it keeps.
pub struct Refused<T> {
    reason: Reason,
    value: T,
}

impl<T> Refused<T> {
    /// A refusal with `reason` that gives `value` back.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) const fn new(reason: Reason, value: T) -> Self {
        Self { reason, value }
    }

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

// Not derived: the value need not be `Debug`, and the message does not show it, so a refused
// send shows none of the bytes of its lease.
impl<T> fmt::Debug for Refused<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Refused")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Display for Refused<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the driver refused the operation: {:?}", self.reason)
    }
}

impl<T> core::error::Error for Refused<T> {}

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
/// operation can start in that direction while it exists. A poll makes the output on the thread
/// of the poll, so the future is `Send` only when its output is.
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
    output: Output<T>,
    /// The direction that the future borrows, and the output that its poll makes: a local resource
    /// is not `Send`, so the future that makes one stays on the thread of its home.
    marker: PhantomData<(&'a mut (), T)>,
}

/// How an operation makes its output from its result and its resources.
enum Output<T> {
    /// The output of a call of the driver.
    Driver(fn(i32, &mut Held) -> Option<T>),
    /// The output of a call of a local type, which also gets the operation: each resource that it
    /// makes keeps the home of its close ([`Completion::cleanup`]).
    #[cfg(all(feature = "local", target_os = "linux"))]
    Local(fn(i32, &mut Held, &Completion) -> Option<T>),
}

// The future never pins its output, which exists only at the end of a poll.
impl<T> Unpin for Operation<'_, T> {}

impl<'a, T> Operation<'a, T> {
    fn new(completion: Completion, output: fn(i32, &mut Held) -> Option<T>) -> Self {
        Self {
            completion,
            output: Output::Driver(output),
            marker: PhantomData,
        }
    }

    /// The same operation, with `output` in place of its output: the local I/O types give their own
    /// resources.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn with_output<U>(self, output: fn(i32, &mut Held, &Completion) -> Option<U>) -> Operation<'a, U> {
        Operation {
            completion: self.completion,
            output: Output::Local(output),
            marker: PhantomData,
        }
    }

    /// The identity of the operation, which the [`Control`] of its driver takes.
    #[must_use]
    pub fn id(&self) -> OperationId {
        self.completion.id()
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.completion
            .poll_with(cx, |result, held, _operation| match this.output {
                Output::Driver(output) => output(result, held),
                #[cfg(all(feature = "local", target_os = "linux"))]
                Output::Local(output) => output(result, held, _operation),
            })
            .map(|output| output.ok().flatten().ok_or(Ended))
    }
}

impl<T> fmt::Debug for Operation<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Operation")
            .field("completion", &self.completion)
            .finish_non_exhaustive()
    }
}

/// The identity of a driver. No two drivers of the process have one identity, so a resource or a
/// child that one driver made never names a slot of another driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DriverId(NonZeroU64);

impl DriverId {
    /// A new identity. At the end of the count, `Driver::new` refuses with the kind
    /// `QuotaExceeded`, so no identity repeats. At one new driver each nanosecond, the count needs
    /// 584 years to reach its end.
    fn next() -> Result<Self, Error> {
        // Not 0 is the niche that keeps `Option<Resource>` at 16 bytes.
        static NEXT: AtomicU64 = AtomicU64::new(1);
        NEXT.try_update(Ordering::Relaxed, Ordering::Relaxed, |next| next.checked_add(1))
            .ok()
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or_else(|| Error::System(io::ErrorKind::QuotaExceeded.into()))
    }
}

/// A descriptor that the driver owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Resource {
    driver: DriverId,
    index: u32,
    generation: u32,
}

/// One descriptor slot of the driver.
///
/// The order of the fields is stated: each look checks the generation and the descriptor, and a
/// receive or a send uses the medium and its direction, so these come first.
#[derive(Debug, Default)]
#[repr(C)]
struct Owned {
    generation: u32,
    fd: Option<Handle>,
    /// The next free slot after this free slot, or `NO_SLOT`.
    next_free: u32,
    /// The descriptor is in the place of this slot in the file table of the ring.
    #[cfg(target_os = "linux")]
    registered: bool,
    /// What the descriptor is, which selects the calls of its operations.
    medium: Medium,
    /// The operations of a file that run, which its close waits for.
    running: u32,
    /// The directions of a stream or a listener, and what they keep.
    directions: tcp::Directions,
    /// The last operation of the file that runs alone on it (`file::Op::Alone`), on macOS and
    /// Windows, while the table says that it runs.
    #[cfg(any(target_os = "macos", target_os = "windows"))]
    exclusive: Option<OpId>,
    /// The port queues no packet for an operation of the socket that ends at its call
    /// (skip-on-success).
    #[cfg(target_os = "windows")]
    skips: bool,
    /// The handles of the file, the directory, or the named pipe of the slot, on Windows, where
    /// `fd` holds only sockets.
    #[cfg(target_os = "windows")]
    file: Option<file::Handles>,
    /// What a pipe listener keeps to make its next instance, on Windows.
    #[cfg(target_os = "windows")]
    listening: Option<Box<ipc::Listening>>,
    /// What the driver knows of the user of the client of an accepted pipe stream, on Windows.
    #[cfg(target_os = "windows")]
    client: Option<ipc::Client>,
    /// The lock file of an IPC listener, whose lock lives while the socket lives.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    lock: Option<OwnedFd>,
}

/// What a resource of the driver is.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Medium {
    /// A stream socket or listener, TCP or Unix-domain: its operations are receives and sends.
    #[default]
    Stream,
    /// A pipe end: its operations are reads and writes, because a receive and a send need a
    /// socket. It has no half-close. On Windows, a named pipe of an IPC stream.
    Pipe,
    /// A UDP socket: its operations are datagram receives and sends. It has no half-close.
    Udp,
    /// A file or a directory: its operations are positional, and several run at one time.
    File,
}

impl Owned {
    /// The place of the descriptor in the file table of the ring, when it is there: the index of
    /// the slot of `resource`.
    #[cfg_attr(
        not(target_os = "linux"),
        expect(clippy::unused_self, reason = "only io_uring has a file table")
    )]
    const fn place(&self, resource: Resource) -> Option<u32> {
        #[cfg(target_os = "linux")]
        if self.registered {
            return Some(resource.index);
        }
        let _ = resource;
        None
    }

    /// Whether the slot holds a resource.
    #[cfg(unix)]
    const fn used(&self) -> bool {
        self.fd.is_some()
    }

    /// Whether the slot holds a resource: a socket, or a file, a directory, or a named pipe.
    #[cfg(target_os = "windows")]
    const fn used(&self) -> bool {
        self.fd.is_some() || self.file.is_some()
    }

    /// The number of the descriptor of the slot in an entry.
    #[cfg(unix)]
    fn number(&self) -> Option<i32> {
        self.fd.as_ref().map(|fd| number(borrow(fd)))
    }

    /// The number of the socket or the handle of the slot in an entry.
    #[cfg(target_os = "windows")]
    fn number(&self) -> Option<i32> {
        self.fd.as_ref().map(|fd| number(borrow(fd))).or_else(|| {
            let handles = self.file.as_ref()?;
            file::windows::number(std::os::windows::io::AsRawHandle::as_raw_handle(&handles.file))
        })
    }
}

/// The descriptors that the driver owns, in a slice that it allocates once.
#[derive(Debug)]
struct Resources {
    /// The driver of the resources.
    driver: DriverId,
    slots: Box<[Owned]>,
    /// The first free slot, or `NO_SLOT`: a release puts its slot first, so a new resource gets
    /// the slot that freed last, with no search.
    first_free: u32,
    /// The number of free slots.
    free: u32,
    /// The operations of files that run, the sum of `Owned::running`. A wait of the ring bounds
    /// each entry of the kernel while one runs (`FILE_WAIT`).
    file_operations: u32,
    /// The descriptor of the ring whose file table has a place for each slot ([`Files::Registered`]).
    #[cfg(target_os = "linux")]
    files: Option<std::os::fd::RawFd>,
    /// The free slots that spawns keep for the pipe ends of their children.
    reserved: u32,
    /// The registrations in the poller, on the epoll and kqueue backends, or the associations with
    /// the port.
    registrar: Registrar,
    /// The next registration fails ([`Control::fail_next_registration`]).
    fail_registration: bool,
    /// The buffers that the multishot receives of the streams keep for their next receives.
    shelf: tcp::Shelf,
}

/// No resource slot: the end of the free list.
const NO_SLOT: u32 = u32::MAX;

/// The registrations of the backend: the poller on epoll and kqueue, none on io_uring.
#[cfg(unix)]
type Registrar = Option<poller::Registrar>;
/// The associations with the port.
#[cfg(target_os = "windows")]
type Registrar = iocp::Registrar;

impl Resources {
    /// The `count` resource slots of `driver`, each free. It allocates here, one time.
    fn new(driver: DriverId, count: u32, registrar: Registrar) -> Result<Self, AllocError> {
        Ok(Self {
            driver,
            slots: items(count, |index| Owned {
                next_free: if index + 1 < count { index + 1 } else { NO_SLOT },
                ..Owned::default()
            })?,
            first_free: if count > 0 { 0 } else { NO_SLOT },
            free: count,
            file_operations: 0,
            #[cfg(target_os = "linux")]
            files: None,
            reserved: 0,
            registrar,
            fail_registration: false,
            shelf: tcp::Shelf::default(),
        })
    }

    /// Whether the backend registers each socket and pipe: the poller and the port do, and a ring
    /// with a file table registers each resource.
    #[cfg(unix)]
    const fn registers(&self) -> bool {
        #[cfg(target_os = "linux")]
        if self.files.is_some() {
            return true;
        }
        self.registrar.is_some()
    }

    /// Whether the backend registers each socket and pipe: the port does.
    #[cfg(target_os = "windows")]
    #[expect(clippy::unused_self, reason = "the same call as on Unix, where the backend decides")]
    const fn registers(&self) -> bool {
        true
    }

    /// Takes `fd`, a descriptor of `medium`. Refuses with `fd` and [`Reason::Resources`] when each
    /// slot that no spawn keeps is in use, or with [`Reason::Register`] when the poller does not
    /// take it.
    fn adopt(&mut self, fd: Handle, medium: Medium) -> Result<Resource, (Reason, Handle)> {
        if self.reserved > 0 && self.free() <= self.reserved {
            return Err((Reason::Resources, fd));
        }
        self.take(fd, medium)
    }

    /// Takes `fd` into a slot that [`Resources::reserve`] kept.
    #[cfg(unix)]
    fn adopt_reserved(&mut self, fd: OwnedFd, medium: Medium) -> Option<Resource> {
        self.reserved = self.reserved.checked_sub(1)?;
        self.take(fd, medium).ok()
    }

    /// Keeps `count` free slots for [`Resources::adopt_reserved`]. Gives whether that many are
    /// free.
    #[cfg(unix)]
    fn reserve(&mut self, count: u32) -> bool {
        let free = self.free().saturating_sub(self.reserved);
        if free < count {
            return false;
        }
        self.reserved += count;
        true
    }

    /// Ends the keep of `count` slots.
    #[cfg(unix)]
    fn unreserve(&mut self, count: u32) {
        self.reserved = self.reserved.saturating_sub(count);
    }

    /// The free slots.
    fn free(&self) -> u32 {
        self.free
    }

    /// Takes `fd` into the first free slot, and registers it in the poller on the epoll and kqueue
    /// backends, or associates it with the port on IOCP. A file has no readiness, so it has no
    /// registration.
    fn take(&mut self, fd: Handle, medium: Medium) -> Result<Resource, (Reason, Handle)> {
        let index = self.first_free;
        let Some(slot) = self.slots.get_mut(index as usize) else {
            return Err((Reason::Resources, fd));
        };
        #[cfg(unix)]
        let registers = medium != Medium::File;
        #[cfg(unix)]
        if let Some(registrar) = &self.registrar
            && registers
        {
            let token = poller::Token {
                index,
                generation: slot.generation,
            };
            // A pipe end gets `O_NONBLOCK` first, because a read or a write that waits would stop
            // the driver. A failure changes nothing but that flag.
            if medium == Medium::Pipe && !pipe::set_nonblocking(fd.as_fd(), true) {
                return Err((Reason::Register, fd));
            }
            if mem::take(&mut self.fail_registration) || registrar.add(fd.as_fd(), token).is_err() {
                return Err((Reason::Register, fd));
            }
        }
        #[cfg(target_os = "windows")]
        {
            let added = if mem::take(&mut self.fail_registration) {
                Err(())
            } else {
                self.registrar.add(&fd).map_err(drop)
            };
            match added {
                Ok(skips) => slot.skips = skips,
                Err(()) => return Err((Reason::Register, fd)),
            }
        }
        // A failed update leaves the resource on its plain descriptor.
        #[cfg(target_os = "linux")]
        if let Some(ring) = self.files {
            slot.registered = !mem::take(&mut self.fail_registration)
                && crate::uring::update_file(ring, index, fd.as_raw_fd()).is_ok();
        }
        slot.fd = Some(fd);
        slot.medium = medium;
        self.first_free = slot.next_free;
        self.free -= 1;
        Ok(Resource {
            driver: self.driver,
            index,
            generation: slot.generation,
        })
    }

    /// Takes the handles of a file, a directory, or a named pipe of `medium` into the first free
    /// slot, and with `associate` associates them with the port. Refuses with them when each slot
    /// that no spawn keeps is in use or the port does not take them.
    #[cfg(target_os = "windows")]
    fn adopt_handles(
        &mut self,
        file: file::Handles,
        medium: Medium,
        associate: bool,
    ) -> Result<Resource, (Reason, file::Handles)> {
        if self.reserved > 0 && self.free() <= self.reserved {
            return Err((Reason::Resources, file));
        }
        let index = self.first_free;
        let Some(slot) = self.slots.get_mut(index as usize) else {
            return Err((Reason::Resources, file));
        };
        // The ring refuses a file on a port, so a driver with a ring keeps its files off the port.
        let associated = !associate
            || medium == Medium::File && self.registrar.ring
            || self.registrar.add_file(&file.file).is_ok()
                && file
                    .through
                    .as_ref()
                    .is_none_or(|through| self.registrar.add_file(through).is_ok());
        if !associated {
            return Err((Reason::Register, file));
        }
        slot.file = Some(file);
        slot.medium = medium;
        slot.skips = false;
        self.first_free = slot.next_free;
        self.free -= 1;
        Ok(Resource {
            driver: self.driver,
            index,
            generation: slot.generation,
        })
    }

    fn get(&mut self, resource: Resource) -> Option<&mut Owned> {
        let driver = self.driver;
        self.slots
            .get_mut(resource.index as usize)
            .filter(|slot| resource.driver == driver && slot.generation == resource.generation && slot.used())
    }

    /// Puts `buffer`, which the multishot receive of `resource` filled, after the buffers that its
    /// read direction keeps. Gives whether the resource needs the home now, because a receive
    /// waits or the direction keeps its most buffers. A `true` puts it in the pending list: the
    /// caller pushes it.
    fn shelve(&mut self, resource: Resource, buffer: Backing) -> bool {
        self.with_shelf(resource)
            .is_some_and(|(owned, shelf)| owned.directions.shelve(buffer, shelf))
    }

    /// The slot of `resource`, as [`Resources::get`] gives it, and the shelf of the buffers that
    /// its directions keep.
    fn with_shelf(&mut self, resource: Resource) -> Option<(&mut Owned, &mut tcp::Shelf)> {
        let driver = self.driver;
        let owned = self
            .slots
            .get_mut(resource.index as usize)
            .filter(|slot| resource.driver == driver && slot.generation == resource.generation && slot.used())?;
        Some((owned, &mut self.shelf))
    }

    /// The slot of `resource`, when this driver made it and the slot has its generation.
    fn slot(&self, resource: Resource) -> Option<&Owned> {
        self.slots
            .get(resource.index as usize)
            .filter(|slot| resource.driver == self.driver && slot.generation == resource.generation)
    }

    /// The descriptor of `resource`, while the driver owns it.
    fn borrow(&self, resource: Resource) -> Option<Borrowed<'_>> {
        self.slot(resource)?.fd.as_ref().map(borrow)
    }

    /// Ends `resource` and its registration, and gives its descriptor. The identity is stale from
    /// now on.
    fn release(&mut self, resource: Resource) -> Option<Handle> {
        #[cfg(unix)]
        let registrar = self.registrar.as_ref();
        let slot = self
            .slots
            .get_mut(resource.index as usize)
            .filter(|slot| resource.driver == self.driver && slot.generation == resource.generation && slot.used())?;
        slot.generation = slot.generation.wrapping_add(1);
        // The buffers that the stream keeps go back to their pool.
        slot.directions.unshelve(&mut self.shelf);
        slot.directions = tcp::Directions::default();
        slot.running = 0;
        slot.next_free = self.first_free;
        self.first_free = resource.index;
        self.free += 1;
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        {
            slot.exclusive = None;
        }
        // A file or a pipe closes here. It stays associated with its port until then, as a socket
        // does.
        #[cfg(target_os = "windows")]
        {
            drop(slot.file.take());
            slot.listening = None;
            slot.client = None;
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            slot.lock = None;
        }
        // The table holds its own reference of the file, so the place empties before the close, and
        // the close then closes the file. A failed update keeps the file open until the ring ends.
        #[cfg(target_os = "linux")]
        if mem::take(&mut slot.registered)
            && let Some(ring) = self.files
        {
            drop(crate::uring::update_file(ring, resource.index, -1));
        }
        let fd = slot.fd.take()?;
        // A socket stays associated with its port until its close, and no system call ends the
        // association.
        #[cfg(unix)]
        if let Some(registrar) = registrar {
            registrar.delete(fd.as_fd());
        }
        Some(fd)
    }
}

/// The buffer group of the provided ring.
const GROUP: u16 = 0;

/// The filled buffers that the read direction of a stream keeps before it ends its multishot
/// receive ([`Shot::Multi`]), when no receive waits. The buffers that the kernel filled before the
/// end still come, so a direction can keep more for a time. A low count shares the provided buffers
/// among the streams, and the kernel keeps the later bytes in the socket.
pub const KEPT_BUFFERS: u32 = 4;

/// The user data of the poll of the wake descriptor that a wait keeps in the ring.
#[cfg(target_os = "linux")]
const WAKE: u64 = CONTROL;

/// The longest entry of the kernel in a wait while a file operation runs on the ring. The kernel
/// selects the idle state of the CPU from the time to its next timer, so the CPU then enters no
/// state whose target residency is longer, such as C6 of an Intel Xeon server: a residency of
/// 600 µs and an exit of 190 µs. The device wakes the CPU at once when the operation ends, and
/// most file operations end before this time. A longer one costs one more entry for each 250 µs.
#[cfg(target_os = "linux")]
const FILE_WAIT: core::time::Duration = core::time::Duration::from_micros(250);

/// The user data of the poll of the timer of the children whose poll found no status, on the
/// ring or on epoll.
#[cfg(target_os = "linux")]
const CHILD_TIMER: u64 = CONTROL | 1;

/// The provided buffers: the slots of a pool, which the kernel selects for a receive. Each one is
/// in the kernel ring, or in a lease of a consumer, or back in the pool.
#[derive(Debug)]
struct Selected {
    pool: Pool,
    ring: BufferRing,
    /// The buffer of each buffer identifier that the kernel ring holds.
    buffers: Vec<Option<Backing>>,
    /// The identifiers with no buffer, which a refill fills from the pool.
    free: Vec<u16>,
    /// The capacity of each slot.
    len: u32,
}

impl Selected {
    /// Takes each slot of `pool`, up to 32768, and gives each to a new provided ring of `ring`.
    /// It counts the other slots, which a refill gives to the ring. The shelf of the multishot
    /// receives has a place for each slot of the pool, and one for a buffer of a caller of each of
    /// the `streams` resource slots.
    fn new(queue: &mut Queue, mut pool: Pool, streams: u32) -> io::Result<(Self, tcp::Shelf)> {
        let mut buffers = Vec::new();
        while buffers.len() < 1 << 15 {
            let Ok(backing) = pool.acquire() else { break };
            buffers.try_reserve(1).map_err(|_| io::ErrorKind::OutOfMemory)?;
            buffers.push(Some(backing));
        }
        let mut slots = u32::try_from(buffers.len()).unwrap_or(u32::MAX);
        let mut more = Vec::new();
        while let Ok(backing) = pool.acquire() {
            more.try_reserve(1).map_err(|_| io::ErrorKind::OutOfMemory)?;
            more.push(backing);
            slots = slots.saturating_add(1);
        }
        // The other slots go back to the pool, for the refills.
        drop(more);
        let count = u16::try_from(buffers.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let len = buffers
            .first()
            .and_then(|buffer| buffer.as_ref())
            .and_then(|buffer| u32::try_from(buffer.capacity()).ok())
            .ok_or(io::ErrorKind::InvalidInput)?;
        // The memory comes before the buffer ring, so a refusal of the allocator provides nothing.
        let free = room(usize::from(count)).map_err(out_of_memory)?;
        let shelf = tcp::Shelf::new(slots.saturating_add(streams)).map_err(out_of_memory)?;
        let mut selected = Self {
            ring: queue.provide(count.next_power_of_two(), GROUP)?,
            pool,
            buffers,
            free,
            len,
        };
        for (bid, buffer) in (0..).zip(selected.buffers.iter_mut()) {
            if let Some(buffer) = buffer {
                selected.ring.add(bid, buffer.as_mut_ptr(), len);
            }
        }
        selected.ring.publish();
        Ok((selected, shelf))
    }

    /// Takes the buffer that the kernel selected as `bid` and filled with the `result` bytes that
    /// the result of its receive counts. `None` for a failed receive.
    #[expect(unsafe_code, reason = "the kernel initialized the bytes that the result counts")]
    fn filled(&mut self, bid: u16, result: i32) -> Option<Backing> {
        let count = usize::try_from(result).ok()?;
        let mut loan = WriteLoan::new(self.take(bid)?);
        // SAFETY: a slot of the pool has no initialized bytes, so the loan starts at the start of
        // the buffer. The kernel wrote the bytes that the result counts there, at most the slot
        // capacity that the ring names.
        unsafe { loan.set_filled(count) };
        Some(loan.into_inner())
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
    /// The driver of the operations: their identity names it ([`Operation::id`]).
    driver: DriverId,
    entries: Box<[Entry<Held>]>,
    requests: Requests,
    /// The closes that the drops of local resources record ([`home::Cleanup`]).
    #[cfg(all(feature = "local", target_os = "linux"))]
    closes: home::Closes,
    /// The wake descriptor: the one descriptor that a wake of another thread writes, and a signal
    /// handler. On Linux, an `eventfd`. The poller watches it. A ring polls it during a wait, and
    /// registers it at its first use by a host, so that the kernel also signals it at each
    /// completion and for its deferred work (`Driver::use_wake`). So a wake needs no flag and no
    /// order with that first use: it writes the descriptor in each case. On macOS, the write end
    /// of the wake pipe, whose read end the kqueue watches: a pipe, because a signal handler wakes
    /// the home too, and `write` is async-signal-safe, which `kevent` is not.
    #[cfg(unix)]
    wake: fs::File,
    /// The read end of the wake pipe, which each turn clears. It lives as long as the write end,
    /// so a wake or a signal handler never writes a pipe with no reader, which raises `SIGPIPE`.
    #[cfg(target_os = "macos")]
    wake_read: fs::File,
    /// The wake of the port: a packet that another thread posts, which ends a wait.
    #[cfg(target_os = "windows")]
    wake: iocp::Wake,
}

impl Shared {
    #[cfg(unix)]
    fn wake(&self) {
        // A full counter or a full pipe is a pending wake already. A pipe takes the 8 bytes whole
        // or not at all, as it takes the writes of a signal handler.
        drop((&self.wake).write(&1_u64.to_ne_bytes()));
    }

    #[cfg(target_os = "windows")]
    fn wake(&self) {
        self.wake.post();
    }

    /// The descriptor that a wake writes, which a signal handler writes too.
    #[cfg(unix)]
    fn wake_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.wake)
    }

    /// Clears the wake descriptor. Gives `true` when it had a wake. A wake that comes after the
    /// read completes the next poll of the ring at once, and wakes the poller or a host again.
    #[cfg(target_os = "linux")]
    fn clear(&self) -> bool {
        let mut count = [0; 8];
        // A descriptor with no wake gives `WouldBlock`, which is the clear state.
        (&self.wake).read(&mut count).is_ok()
    }

    /// Clears the wake pipe. One read takes at most 64 wakes of 8 bytes, and bytes that remain
    /// keep the level-triggered registration readable, so the next wait ends at once.
    #[cfg(target_os = "macos")]
    fn clear(&self) {
        let mut bytes = [0; 512];
        // An empty pipe gives `WouldBlock`, which is the clear state.
        drop((&self.wake_read).read(&mut bytes));
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("operations", &self.entries.len())
            .finish_non_exhaustive()
    }
}

/// A driver that its caller owns and drives, on io_uring, epoll, kqueue, or IOCP ([`Backend`]). It
/// stays on the thread that created it.
pub struct Driver {
    /// The queues of the backend: the ring, or the poller.
    queue: Queue,
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
    /// What the driver found and applied when it started.
    report: Report,
    completions_per_turn: u32,
    /// The ring polls the wake descriptor, so that a request or wake from another thread ends a
    /// wait.
    #[cfg(target_os = "linux")]
    waking: bool,
    /// Something reads the wake descriptor outside a wait: the poller, from the start, or the
    /// reactor of a host, from the first `AsFd` of a ring. Each turn then clears the descriptor.
    /// A ring that only its caller waits in never sets it: the kernel then signals no descriptor
    /// at a completion, and no turn reads one. Only the thread of the driver uses the flag.
    #[cfg(target_os = "linux")]
    watched: Cell<bool>,
    /// The kernel signals the wake descriptor for deferred work only one time in each
    /// registration, or a signal of the probe came late (`probe`). Each turn that takes a wake thus
    /// registers it again. The first use of the descriptor sets it.
    #[cfg(target_os = "linux")]
    rearm_wake: Cell<bool>,
    /// The failure of the registration of the wake descriptor at its first use, which the next
    /// turn gives.
    #[cfg(target_os = "linux")]
    wake_failed: Cell<Option<io::Error>>,
    /// The signal streams whose home is this driver.
    #[cfg(unix)]
    signals: signal::Homes,
    /// The child processes that this driver owns.
    #[cfg(unix)]
    children: process::Children,
    /// The blocking service of the operations that the kernel cannot run
    /// ([`Driver::use_blocking`]), and the waker that its jobs wake.
    blocking: Option<crate::blocking::Submitter>,
    /// The waker of the jobs of [`Driver::use_blocking`], which wakes this driver.
    job_waker: core::task::Waker,
    /// The operations that run on the blocking service, which each turn polls.
    jobs: Vec<OpId>,
    /// The file operations that wait for an operation that runs alone on their file, or that such
    /// an operation waits for, in start order: at most one entry for each operation.
    #[cfg(any(target_os = "macos", target_os = "windows"))]
    waiting: Vec<file::Waiting>,
    /// The home of a local owner on this driver, and the wake messages that it sent.
    #[cfg(all(feature = "local", target_os = "linux"))]
    wakes: home::Wakes,
    /// The operations of movable resources that wait here before their submission: for an earlier
    /// operation of their direction, or for room in the queue. At most one entry for each
    /// operation.
    #[cfg(all(feature = "local", target_os = "linux"))]
    following: Vec<OpId>,
    /// The consumers of the home operations: the operations of local types.
    #[cfg(all(feature = "local", target_os = "linux"))]
    consumers: home::Consumers,
    /// The state of [`Delivery::Controlled`].
    control: Option<control::Controlled>,
    /// The bytes that dropped sends had sent when the closes of their dropped resources ended
    /// them, for the shutdown report of a runtime.
    #[cfg(all(feature = "local", target_os = "linux"))]
    dropped_sent: u64,
}

impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Driver");
        debug
            .field("queue", &self.queue)
            .field("resources", &self.resources.slots.len())
            .field("selected", &self.selected)
            .field("completions_per_turn", &self.completions_per_turn);
        #[cfg(target_os = "linux")]
        debug.field("rearm_wake", &self.rearm_wake.get());
        debug.finish_non_exhaustive()
    }
}

impl Driver {
    /// Starts a driver with the limits of `config`. It allocates here, one time. On io_uring, it
    /// opens three descriptors: the ring, the wake descriptor, and the timer of the children whose
    /// status a poll did not find. With the `local` feature, it also
    /// opens a duplicate of the ring, the target of the wake messages of local tasks. On epoll, the
    /// epoll descriptor takes the place of the ring. On macOS, it opens one kqueue and the two ends
    /// of its wake pipe, and on Windows one completion port.
    ///
    /// # Errors
    ///
    /// [`Error::Refused`] when the kernel refuses io_uring and [`Config::backend`] requires it, or
    /// when [`Config::backend`] requires a backend that the target does not have, [`Error::Config`]
    /// for a limit that is 0 or too large, [`Error::Unsupported`] for a required feature that the
    /// kernel or the backend does not have, and [`Error::System`] for another failure, of the kind
    /// `OutOfMemory` when the allocator does not give the memory of the driver. After 2^64 - 2
    /// drivers of the process, it gives the kind `QuotaExceeded`, so no two drivers have one
    /// identity.
    pub fn new(config: Config) -> Result<Self, Error> {
        if config.operations == 0
            || config.operations > MAX_SLOTS
            || config.resources == 0
            || config.completions_per_turn == 0
        {
            return Err(Error::Config);
        }
        let completions = config.operations.max(config.submissions).saturating_mul(2).min(1 << 16);
        let mut driver = Self::start_backend(config, completions)?;
        if config.delivery == Delivery::Controlled {
            driver.control = Some(
                control::Controlled::new(config.operations, completions)
                    .map_err(|error| Error::System(out_of_memory(error)))?,
            );
        }
        Ok(driver)
    }

    /// Starts the port of a valid `config` with room for `completions` operations in flight, and
    /// the ring of its files where the IoRing probe passes, or where the caller requires it.
    #[cfg(target_os = "windows")]
    fn start_backend(config: Config, completions: u32) -> Result<Self, Error> {
        let refused = |error| Error::System(out_of_memory(error));
        // IOCP and IoRing are the backends of Windows, and a feature is a feature of io_uring.
        if config
            .backend
            .is_some_and(|backend| !matches!(backend, Backend::Iocp | Backend::IoRing))
        {
            return Err(Error::Refused(io::ErrorKind::Unsupported.into()));
        }
        if !config.required.is_empty() {
            return Err(Error::Unsupported(config.required));
        }
        let mut port =
            iocp::Port::new(config.submissions, completions, config.operations).map_err(|error| {
                match error.kind() {
                    io::ErrorKind::InvalidInput => Error::Config,
                    _ => Error::System(error),
                }
            })?;
        let mut selection = capability::Selection {
            requested: config.backend,
            backend: Backend::IoRing,
            fallback: None,
        };
        if config.backend == Some(Backend::Iocp) {
            selection.backend = Backend::Iocp;
        } else if let Err(error) = port.start_ring() {
            // The permissive selection runs the files on the port where the probe refuses the ring.
            if config.backend.is_some() {
                return Err(Error::Refused(io::Error::from_raw_os_error(error)));
            }
            selection.backend = Backend::Iocp;
            selection.fallback = Some(error);
        }
        let (wake, registrar) = (port.wake(), port.registrar(config.delivery));
        let table = Table::new(config.operations).map_err(refused)?;
        let driver = DriverId::next()?;
        let shared = try_arc(Shared {
            driver,
            entries: items(config.operations, |_| Entry::new()).map_err(refused)?,
            requests: table.requests(),
            wake,
        })
        .map_err(refused)?;
        let job_waker = file::job_waker(&shared).map_err(refused)?;
        Ok(Self {
            queue: Queue::Port(port),
            table,
            shared,
            resources: Resources::new(driver, config.resources, registrar).map_err(refused)?,
            taken: room(config.operations as usize * 2).map_err(refused)?,
            pending: room(config.resources as usize).map_err(refused)?,
            copied: 0,
            selected: None,
            report: Report::new(selection, config.required, Features::NONE, false),
            completions_per_turn: config.completions_per_turn,
            blocking: None,
            job_waker,
            jobs: room(config.operations as usize).map_err(refused)?,
            waiting: room(config.operations as usize).map_err(refused)?,
            control: None,
        })
    }

    /// Starts the kqueue poller of a valid `config` with room for `completions` operations in
    /// flight.
    #[cfg(target_os = "macos")]
    fn start_backend(config: Config, completions: u32) -> Result<Self, Error> {
        let refused = |error| Error::System(out_of_memory(error));
        // kqueue is the one backend of macOS, and a feature is a feature of io_uring.
        if config.backend.is_some_and(|backend| backend != Backend::Kqueue) {
            return Err(Error::Refused(io::ErrorKind::Unsupported.into()));
        }
        if !config.required.is_empty() {
            return Err(Error::Unsupported(config.required));
        }
        let selection = capability::Selection {
            requested: config.backend,
            backend: Backend::Kqueue,
            fallback: None,
        };
        let (wake_read, wake) = wake_pipe().map_err(Error::System)?;
        let poller = poller::Poller::new(
            config.submissions,
            completions,
            config.resources,
            config.operations,
            wake_read.as_fd(),
        )
        .map_err(|error| match error.kind() {
            io::ErrorKind::InvalidInput => Error::Config,
            _ => Error::System(error),
        })?;
        let registrar = poller.registrar();
        let table = Table::new(config.operations).map_err(refused)?;
        let driver = DriverId::next()?;
        let shared = try_arc(Shared {
            driver,
            entries: items(config.operations, |_| Entry::new()).map_err(refused)?,
            requests: table.requests(),
            wake,
            wake_read,
        })
        .map_err(refused)?;
        let job_waker = file::job_waker(&shared).map_err(refused)?;
        Ok(Self {
            queue: Queue::Poller(poller),
            table,
            shared,
            resources: Resources::new(driver, config.resources, Some(registrar)).map_err(refused)?,
            taken: room(config.operations as usize * 2).map_err(refused)?,
            pending: room(config.resources as usize).map_err(refused)?,
            copied: 0,
            selected: None,
            report: Report::new(selection, config.required, Features::NONE, false),
            completions_per_turn: config.completions_per_turn,
            signals: room(signal::STREAMS).map_err(refused)?,
            children: process::Children::new(config.children, driver).map_err(Error::System)?,
            blocking: None,
            job_waker,
            jobs: room(config.operations as usize).map_err(refused)?,
            waiting: room(config.operations as usize).map_err(refused)?,
            control: None,
        })
    }

    /// Starts the backend of a valid `config` with room for `completions` completions: io_uring,
    /// or epoll where the kernel refuses io_uring.
    #[cfg(target_os = "linux")]
    fn start_backend(config: Config, completions: u32) -> Result<Self, Error> {
        let refused = |error| Error::System(out_of_memory(error));
        // io_uring and epoll are the backends of Linux.
        if config
            .backend
            .is_some_and(|backend| !matches!(backend, Backend::IoUring | Backend::Epoll))
        {
            return Err(Error::Refused(io::ErrorKind::Unsupported.into()));
        }
        let wake = fs::File::from(eventfd().map_err(Error::System)?);
        let mut selection = capability::Selection {
            requested: config.backend,
            backend: Backend::IoUring,
            fallback: None,
        };
        let ring = match config.backend {
            Some(Backend::Epoll) => None,
            requested => match Ring::new(config.submissions, completions) {
                Ok(ring) => Some(ring),
                // The permissive selection runs epoll where the kernel refuses io_uring.
                Err(SetupError::Refused(error)) if requested.is_none() => {
                    selection.fallback = error.raw_os_error();
                    None
                }
                Err(SetupError::Refused(error)) => return Err(Error::Refused(error)),
                Err(SetupError::Failed(error)) if error.kind() == io::ErrorKind::InvalidInput => {
                    return Err(Error::Config);
                }
                Err(SetupError::Failed(error)) => return Err(Error::System(error)),
            },
        };
        let (mut queue, probed) = match ring {
            Some(mut ring) => {
                // The probe needs a new ring, with no entry, completion, or buffer group.
                let probed = Features::probed(ring.probe().map_err(Error::System)?);
                // A strict requirement refuses before the driver exists.
                let missing = config.required.without(probed);
                if !missing.is_empty() {
                    return Err(Error::Unsupported(missing));
                }
                // Nothing reads the wake descriptor yet, so it gets no registration, and the
                // kernel does not signal it (`Driver::use_wake`).
                (Queue::Ring(ring), probed)
            }
            None => {
                selection.backend = Backend::Epoll;
                // A feature is a feature of io_uring, so epoll has none.
                if !config.required.is_empty() {
                    return Err(Error::Unsupported(config.required));
                }
                let poller = poller::Poller::new(
                    config.submissions,
                    completions,
                    config.resources,
                    config.operations,
                    wake.as_fd(),
                )
                .map_err(|error| match error.kind() {
                    io::ErrorKind::InvalidInput => Error::Config,
                    _ => Error::System(error),
                })?;
                (Queue::Poller(poller), Features::NONE)
            }
        };
        let registrar = match &queue {
            Queue::Poller(poller) => Some(poller.registrar()),
            Queue::Ring(_) => None,
        };
        // The poller waits on the wake descriptor from its start.
        let watched = Cell::new(matches!(queue, Queue::Poller(_)));
        let table = Table::new(config.operations).map_err(refused)?;
        let driver = DriverId::next()?;
        let mut resources = Resources::new(driver, config.resources, registrar).map_err(refused)?;
        // A ring that cannot register its table, or whose kernel keeps the file of an emptied place
        // open, names the plain descriptors.
        let mut files = Files::Plain;
        if config.files == Files::Registered
            && let Queue::Ring(ring) = &mut queue
            && ring.register_files(config.resources).is_ok()
        {
            resources.files = Some(ring.fd());
            files = Files::Registered;
        }
        // Only a ring has a multishot receive.
        let receives = match &queue {
            Queue::Ring(_) => config.receives,
            Queue::Poller(_) => Shot::Single,
        };
        let shared = try_arc(Shared {
            driver,
            entries: items(config.operations, |_| Entry::new()).map_err(refused)?,
            requests: table.requests(),
            #[cfg(all(feature = "local", target_os = "linux"))]
            closes: home::Closes::new(config.resources).map_err(refused)?,
            wake,
        })
        .map_err(refused)?;
        let job_waker = file::job_waker(&shared).map_err(refused)?;
        #[cfg(all(feature = "local", target_os = "linux"))]
        let wakes = home::Wakes::new(&queue, &shared).map_err(Error::System)?;
        Ok(Self {
            queue,
            table,
            shared,
            resources,
            children: process::Children::new(config.children, driver).map_err(Error::System)?,
            taken: room(config.operations as usize * 2).map_err(refused)?,
            pending: room(config.resources as usize).map_err(refused)?,
            copied: 0,
            selected: None,
            report: Report::new(selection, config.required, probed, false)
                .with_files(files)
                .with_receives(receives),
            completions_per_turn: config.completions_per_turn,
            waking: false,
            watched,
            rearm_wake: Cell::new(false),
            wake_failed: Cell::new(None),
            signals: room(signal::STREAMS).map_err(refused)?,
            blocking: None,
            job_waker,
            jobs: room(config.operations as usize).map_err(refused)?,
            #[cfg(all(feature = "local", target_os = "linux"))]
            wakes,
            #[cfg(all(feature = "local", target_os = "linux"))]
            following: room(config.operations as usize).map_err(refused)?,
            #[cfg(feature = "local")]
            consumers: home::Consumers::new(config.operations).map_err(refused)?,
            control: None,
            #[cfg(all(feature = "local", target_os = "linux"))]
            dropped_sent: 0,
        })
    }

    /// What the driver found and applied when it started. [`Report::rearms_wake`] gives what the
    /// first use of the wake descriptor found, and `false` before it.
    #[must_use]
    pub fn report(&self) -> Report {
        #[cfg(target_os = "linux")]
        return self.report.with_rearms_wake(self.rearm_wake.get());
        #[cfg(not(target_os = "linux"))]
        self.report
    }

    /// The bytes that receives copied, in total, from the data that a dropped or forgotten receive
    /// left. Each other byte of a receive goes from the kernel to its buffer with no copy.
    #[must_use]
    pub const fn copied(&self) -> u64 {
        self.copied
    }

    /// The bytes that dropped sends had sent when the closes of their dropped resources ended
    /// them, in total.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) const fn dropped_sent(&self) -> u64 {
        self.dropped_sent
    }

    /// Whether an operation of the driver runs, so that a completion can still come.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn busy(&self) -> bool {
        self.table.busy()
    }

    /// The [`Control`] of a driver under [`Delivery::Controlled`], or `None` under native
    /// delivery.
    pub fn control(&mut self) -> Option<Control<'_>> {
        let state = self.control.as_mut()?;
        Some(Control {
            state,
            resources: &mut self.resources,
        })
    }

    /// A handle that wakes the driver from any thread.
    #[must_use]
    pub fn wake_handle(&self) -> WakeHandle {
        WakeHandle(Arc::clone(&self.shared))
    }

    /// A stream of `signal`, whose home is this driver: a turn wakes it when the signal arrived.
    /// The first stream of a signal installs its handler, and the last stream that closes restores
    /// the earlier disposition. The driver connects no signal to its own end.
    ///
    /// # Errors
    ///
    /// [`SignalError`] gives the reason, and nothing changes.
    #[cfg(unix)]
    pub fn signal(&mut self, signal: Signal, escalation: Escalation) -> Result<SignalStream, SignalError> {
        signal::register(&self.shared, &mut self.signals, signal, escalation)
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
    /// or a slot above `u32::MAX` bytes, `OutOfMemory` when the allocator refuses, and an error of
    /// the registration.
    pub fn provide(&mut self, pool: Pool) -> io::Result<()> {
        if self.selected.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        let streams = u32::try_from(self.resources.slots.len()).unwrap_or(u32::MAX);
        let (selected, shelf) = Selected::new(&mut self.queue, pool, streams)?;
        self.resources.shelf = shelf;
        self.selected = Some(selected);
        Ok(())
    }

    /// One bounded turn. It never blocks.
    ///
    /// # Errors
    ///
    /// A failure of the kernel entry other than an interrupt or a full completion queue, which
    /// leave work for the next turn.
    pub fn turn(&mut self) -> io::Result<Turn> {
        #[cfg(target_os = "linux")]
        if let Some(error) = self.wake_failed.take() {
            return Err(error);
        }
        self.take_requests();
        // A packet of the port wakes the next wait for a job that ends later, and no turn clears it.
        #[cfg(target_os = "windows")]
        self.poll_jobs();
        if let Some(selected) = &mut self.selected {
            selected.refill();
        }
        let mut more = false;
        if self.queue.unsubmitted() > 0 || self.queue.needs_enter() {
            match self.queue.submit(0) {
                Ok(_) => {}
                Err(error) if retryable(&error) => more = true,
                Err(error) => return Err(error),
            }
        }
        let completed = self.harvest(self.completions_per_turn);
        // Start the operations that waited for an operation that ended, and end the closes.
        self.attend();
        #[cfg(all(feature = "local", target_os = "linux"))]
        self.follow();
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        self.start_waiting();
        // Clear the wake descriptor, then look again: work that came after the harvest has woken
        // the descriptor already, so this turn reports it, or a later wake comes.
        #[cfg(target_os = "linux")]
        if self.watched.get()
            && self.shared.clear()
            && self.rearm_wake.get()
            && let Some(ring) = self.queue.ring()
        {
            // On a kernel with the defect of `probe`, the next deferred work signals only a new
            // registration. Deferred work that comes before the new registration sets
            // `IORING_SQ_TASKRUN` before it tries the signal, so the look below sees it.
            rearm(ring, &self.shared.wake)?;
        }
        #[cfg(target_os = "macos")]
        self.shared.clear();
        // After the clear: a signal that came before it counted its delivery first, so this look
        // sees it, and a later one wakes the descriptor again. This is an R220 correction: a look
        // before the clear lost the wake of a signal between the two.
        #[cfg(unix)]
        signal::wake(&mut self.signals);
        // The jobs of the driver are polled after the clear too, for the same reason: a job that
        // ended before it is taken now. The end of a job can end the wait of a close.
        #[cfg(unix)]
        {
            self.poll_jobs();
            self.attend();
        }
        more |= self.queue.ready()
            || self.queue.more()
            || self.queue.unsubmitted() > 0
            || !self.shared.requests.is_empty()
            || self.control.as_ref().is_some_and(control::Controlled::due);
        // A drop that this turn ran, for example of a buffer, can record a close. The owner turns
        // again before it waits, so the close needs no wake.
        #[cfg(all(feature = "local", target_os = "linux"))]
        {
            more |= self.shared.closes.posted();
        }
        Ok(Turn { completed, more })
    }

    /// Blocks in the kernel until a completion is ready, or a request or wake from another thread
    /// came. The kernel runs its deferred work during the wait, so the wait does not depend on a
    /// signal of the wake descriptor for that work. A handled signal also ends the wait.
    ///
    /// # Errors
    ///
    /// A failure of the kernel entry other than an interrupt or a full queue, which leave work for
    /// the next turn.
    pub fn wait(&mut self) -> io::Result<()> {
        self.wait_in(None)
    }

    /// Blocks as [`wait`](Self::wait) does, but at most until `deadline`, an instant of `clock`. A
    /// loop that owns the driver waits with it for its I/O and for its next timer in one call: the
    /// kernel ends the wait, and the driver starts no timer thread.
    ///
    /// A deadline that passed gives no block: the call takes the pushed entries and returns. The
    /// wait can also end before the deadline with no work, for example at a handled signal. Call
    /// it again with the same deadline then: the time does not start again, because the driver
    /// reads the time that is left at each entry of the kernel.
    ///
    /// On epoll and IOCP, the kernel counts the wait in milliseconds, so the driver rounds the
    /// time up. The kernel measures the wait on a clock that pauses while the system is suspended.
    /// So with a clock of [`Suspend::Advances`](crate::time::Suspend::Advances), a suspend during
    /// the wait can end the wait later than the deadline.
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use core::time::Duration;
    ///
    /// use rsasync::clock::NativeClock;
    /// use rsasync::driver::{Config, Driver};
    /// use rsasync::time::{Deadline, Suspend};
    ///
    /// let clock = NativeClock::new(Suspend::Pauses)?;
    /// let mut driver = Driver::new(Config::new())?;
    /// loop {
    ///     // Poll the futures of your operations here, and run each timer that is due.
    ///     let next_timer = Deadline::at(clock.now().checked_add(Duration::from_millis(250))?);
    ///     if !driver.turn()?.more() {
    ///         driver.wait_until(next_timer, clock)?;
    ///     }
    /// }
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The kind `InvalidInput`, with no wait, when `deadline` is not an instant of the domain of
    /// `clock`. A failure of the kernel entry, as for [`wait`](Self::wait).
    pub fn wait_until(&mut self, deadline: crate::time::Deadline, clock: crate::clock::NativeClock) -> io::Result<()> {
        let until = crate::clock::Until::checked(deadline.instant(), clock).ok_or(io::ErrorKind::InvalidInput)?;
        self.wait_to(until)
    }

    /// The wait of [`wait_until`](Self::wait_until), until a deadline that has its clock.
    pub(crate) fn wait_to(&mut self, until: crate::clock::Until) -> io::Result<()> {
        self.wait_in(Some(until))
    }

    /// Clears a wake descriptor that something watches outside a wait, after a harvest with no
    /// turn: the poller watches it, and so does the ring after its first use by a host. A wake
    /// that stays there ends each later wait at once.
    #[cfg(unix)]
    fn clear_watched(&self) {
        #[cfg(target_os = "linux")]
        if self.watched.get() {
            self.shared.clear();
        }
        #[cfg(target_os = "macos")]
        self.shared.clear();
    }

    /// The wait of [`wait`](Self::wait), with no limit or until `until`. On IOCP, a request or a
    /// wake of another thread posts a packet to the port, which ends the wait.
    fn wait_in(&mut self, until: Option<crate::clock::Until>) -> io::Result<()> {
        // A completion that the control keeps for the next turn is ready.
        if self.control.as_ref().is_some_and(control::Controlled::due) {
            return Ok(());
        }
        // A wait in the ring ends only at a completion, so a request or a wake from another
        // thread, which only writes the wake descriptor, would not end it. The ring polls that
        // descriptor, and the poll then completes. While no host uses the descriptor, the kernel
        // does not signal it, so no completion ends the poll. After the first use by a host, each
        // completion signals it: such a wait can end one time more for a completion. The poller
        // watches the descriptor itself.
        #[cfg(target_os = "linux")]
        #[expect(unsafe_code, reason = "a poll names no memory")]
        if self.queue.as_ring().is_some() && !self.waking {
            let poll = Sqe::poll_readable(self.shared.wake.as_raw_fd()).with_user_data(WAKE);
            // SAFETY: a poll names no memory.
            match unsafe { push_now(&mut self.queue, &poll) } {
                Ok(()) => self.waking = true,
                // A full queue leaves work for the next turn.
                Err(error) if retryable(&error) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        let entered = self.enter_wait(until);
        // A handled signal ends the wait with `Interrupted`, also under `SA_RESTART`. The wait
        // then returns: a second entry with the same time left would wait the whole time again,
        // so signals that come faster than that time would keep it past `until`.
        match entered {
            Err(error) if !retryable(&error) => Err(error),
            _ => Ok(()),
        }
    }

    /// Enters the kernel for a wait until a completion is ready, or until `until`. While a file
    /// operation runs on the ring, each entry ends after `FILE_WAIT` at most, and the wait enters
    /// again: so the CPU sleeps in no idle state that wakes later than the device ends.
    fn enter_wait(&mut self, until: Option<crate::clock::Until>) -> io::Result<u32> {
        #[cfg(target_os = "linux")]
        if self.resources.file_operations > 0
            && let Some(ring) = self.queue.ring()
        {
            loop {
                let left = until.map_or(FILE_WAIT, |until| until.remaining().min(FILE_WAIT));
                let taken = ring.submit_timeout(1, left)?;
                // A shorter entry ended at the deadline.
                if ring.ready() || ring.needs_enter() || left < FILE_WAIT {
                    return Ok(taken);
                }
            }
        }
        match until {
            None => self.queue.submit(1),
            Some(until) => self.queue.submit_until(1, until),
        }
    }

    /// Cancels each running operation, drives until each terminal event, and closes each
    /// resource. Each future that has not taken its result then gives [`Ended`]. A job of the
    /// blocking service ([`Route::BlockingService`]) cannot be cancelled, so the teardown waits until
    /// it returns.
    ///
    /// On Linux and macOS, teardown applies the `End` of each child: it kills a child with `End::Kill`, or
    /// each process of its tree, and waits until the driver reaps it. A child with `End::Keep` runs
    /// on, and `Teardown::kept` counts it.
    ///
    /// # Errors
    ///
    /// A failure of the kernel entry other than an interrupt.
    pub fn teardown(mut self) -> io::Result<Teardown> {
        // A hold would keep a terminal event that the teardown waits for.
        if let Some(control) = &mut self.control {
            control.deliver_all();
        }
        // An operation that waits for an earlier one of its direction never ran, so it ends now.
        #[cfg(all(feature = "local", target_os = "linux"))]
        self.end_following();
        // A spawn cannot be cancelled, and its child starts at the end of its job, so teardown
        // applies the end of each child after each spawn ended.
        #[cfg(unix)]
        while self.spawns() {
            self.take_requests();
            self.poll_jobs();
            if self.spawns() {
                self.wait()?;
                self.harvest(u32::MAX);
                self.clear_watched();
                self.attend();
            }
        }
        // An operation that waits for the one before it never ran, so it ends now.
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        for waiting in mem::take(&mut self.waiting) {
            self.complete_now(waiting.id(), -ECANCELED);
        }
        for index in 0..self.resources.slots.len() {
            let Some(resource) = self.resources.slots.get(index).map(|owned| Resource {
                driver: self.resources.driver,
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
            // A receive that waits for the multishot receive of its stream has no submission.
            self.end_awaiting(resource);
        }
        let mut cancelled = 0_u32;
        #[cfg(unix)]
        {
            self.children.ending = true;
        }
        for id in self.table.running() {
            match self.table.resources(id).map(|tag| tag.kind) {
                // A close ends when the operations of its resource end.
                Some(Kind::Close) => continue,
                // The exit of a child with `End::Kill` ends after its kill. A cancel ends the exit
                // operation of a child with `End::Keep`.
                #[cfg(unix)]
                Some(Kind::Exit(child)) if self.children.kills(child) => {
                    self.children.kill(child);
                    continue;
                }
                _ => {}
            }
            push_cancel(&mut self.queue, id)?;
            // The multishot receive of a stream is no operation of the caller.
            if self.table.resources(id).is_none_or(|tag| tag.kind != Kind::Multishot) {
                cancelled = cancelled.saturating_add(1);
            }
        }
        while self.table.busy() {
            self.take_requests();
            self.poll_jobs();
            if !self.table.busy() {
                break;
            }
            // The wait also ends when a job of the blocking service ends, because the job wakes
            // the driver: the wake descriptor on Linux, the wake pipe on macOS, and a packet of
            // the port on Windows.
            self.wait()?;
            self.harvest(u32::MAX);
            #[cfg(unix)]
            self.clear_watched();
            self.attend();
        }
        self.take_requests();
        Ok(Teardown {
            cancelled,
            #[cfg(unix)]
            kept: self.children.kept,
        })
    }

    /// Whether the job of a spawn runs.
    #[cfg(unix)]
    fn spawns(&self) -> bool {
        self.jobs.iter().any(|&id| {
            matches!(
                self.table.resources(id),
                Some(Tag {
                    kind: Kind::Spawn(_),
                    ..
                })
            )
        })
    }

    /// Takes `fd`, a descriptor of `medium`, so that teardown closes it. Refuses with `fd` and the
    /// reason when each resource slot is in use or the poller does not take it.
    fn adopt(&mut self, fd: Handle, medium: Medium) -> Result<Resource, (Reason, Handle)> {
        self.resources.adopt(fd, medium)
    }

    /// Whether the submission queue has room for `entries` entries, after one submit if it does
    /// not.
    fn room(&mut self, entries: u32) -> bool {
        self.queue.room() >= entries || (self.queue.submit(0).is_ok() && self.queue.room() >= entries)
    }

    /// The future of the new operation `id`, which a drop cancels when `cancel_on_drop` is true.
    /// During a call of a local type, its consumer is at the home ([`Consumer::Home`]), and its tag
    /// says so.
    pub(crate) fn completion(&mut self, id: OpId, cancel_on_drop: bool) -> Completion {
        let completion = Completion::new(Arc::clone(&self.shared), id, cancel_on_drop);
        #[cfg(all(feature = "local", target_os = "linux"))]
        if self.consumers.calls
            && let Some(tag) = self.table.resources_mut(id)
        {
            tag.consumer = Consumer::Home;
            return completion.with_consumer(Consumer::Home);
        }
        completion
    }

    /// The consumer of an operation that starts now.
    #[cfg(all(feature = "local", target_os = "linux"))]
    const fn consumer(&self) -> Consumer {
        if self.consumers.calls {
            Consumer::Home
        } else {
            Consumer::Any
        }
    }

    /// Gives `value` back when this driver owns `resource`, and else refuses it with
    /// [`Reason::Resource`]. A call that can refuse for another reason checks this first, so a
    /// resource of another driver always gives this reason.
    fn own<P>(&mut self, resource: Resource, value: P) -> Result<P, Refused<P>> {
        match self.resources.get(resource) {
            Some(_) => Ok(value),
            None => Err(Refused {
                reason: Reason::Resource,
                value,
            }),
        }
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
        let shape = if tag.kind.batch() || tag.kind == Kind::Multishot {
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
        // The socket that the operation made and holds, as a connect holds its new socket.
        #[cfg(unix)]
        let mut made = None;
        slot.update(id, |held| {
            if let Some(held) = held {
                #[cfg(unix)]
                {
                    made = held.accepted;
                }
                sqe = Some(entry(held));
            }
        });
        if let Some(sqe) = sqe {
            let resource = self.table.resources(id).and_then(|tag| tag.resource);
            // An entry waits on the readiness of the resource of its tag, or else of the socket
            // that its operation made. That socket has the registration of a resource, and a
            // one-shot registration of its descriptor would replace it, so that no later receive
            // or send of the socket could wait.
            #[cfg(unix)]
            let token = resource.or(made).map(|resource| poller::Token {
                index: resource.index,
                generation: resource.generation,
            });
            #[cfg(target_os = "windows")]
            let ring = self.resources.registrar.ring;
            #[cfg(target_os = "windows")]
            let token = resource
                .and_then(|resource| self.resources.get(resource))
                .map(|owned| iocp::Token {
                    skips: owned.skips,
                    file: owned.file.is_some(),
                    ring: ring && owned.medium == Medium::File,
                });
            // SAFETY: the entry of the slot keeps the resources in place until the terminal event
            // of `id`, and nothing changes the variant of their payload before it. The caller keeps
            // the descriptor open. The queue has room.
            let pushed = unsafe { self.queue.push(&sqe.with_user_data(id.user_data()), token) }.is_ok();
            assert!(pushed, "the queue has room for the entry");
        }
    }

    /// Ends the operation `id` at the home with `result`, with no kernel event, as its terminal
    /// event.
    pub(crate) fn complete_now(&mut self, id: OpId, result: i32) {
        let event = Event { result, more: false };
        let outcome = self.table.complete(id.user_data(), event);
        self.ended(id, result, outcome);
    }

    /// Applies the outcome of a terminal event of `id`: wakes the consumer, or applies the
    /// abandoned rule.
    fn ended(&mut self, id: OpId, result: i32, outcome: Outcome<Tag>) {
        let Some(slot) = self.shared.entries.get(id.index() as usize) else {
            return;
        };
        if let Some(tag) = match &outcome {
            Outcome::Ready => self.table.resources(id).copied(),
            Outcome::Abandoned(Done { resources, .. }) => Some(*resources),
            _ => None,
        } {
            file::ended(&mut self.resources, &mut self.pending, tag);
        }
        match outcome {
            #[cfg(all(feature = "local", target_os = "linux"))]
            Outcome::Ready
                if self
                    .table
                    .resources(id)
                    .is_some_and(|tag| tag.consumer == Consumer::Home) =>
            {
                slot.publish_home(id, result);
                self.consumers.wake(id);
            }
            Outcome::Ready => slot.publish(id, result),
            Outcome::Abandoned(Done { resources: tag, .. }) => {
                if let Some(held) = slot.reclaim(id) {
                    tcp::abandon(&mut self.resources, tag, result, held);
                }
                tcp::after(&mut self.resources, &mut self.pending, tag);
            }
            _ => {}
        }
    }

    /// Applies the requests that other threads posted, and the closes that drops of local resources
    /// recorded.
    fn take_requests(&mut self) {
        // A post that a wake announces happens before the turn that the wake starts, so this load
        // sees it. Another post waits for the next take.
        if !self.shared.requests.is_empty() {
            let mut taken = mem::take(&mut self.taken);
            self.table.take_requests(|request| taken.push(request));
            for request in taken.drain(..) {
                match request {
                    Taken::Cancel(id) => self.cancel(id),
                    Taken::Abandoned(id, ended) => self.claimed(id, ended),
                }
            }
            self.taken = taken;
        }
        #[cfg(all(feature = "local", target_os = "linux"))]
        self.take_closes();
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
        let tag = ended.resources;
        match held {
            Some(held) => tcp::abandon(&mut self.resources, tag, ended.result, held),
            None => tcp::taken(&mut self.resources, tag, id),
        }
        tcp::after(&mut self.resources, &mut self.pending, tag);
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
        // The cancel of the exit of a child is a kill by its end policy, and no cancel of the
        // kernel: the operation ends when the child ends.
        #[cfg(unix)]
        if let Kind::Exit(child) = tag.kind {
            self.children.kill(child);
            return;
        }
        if self.unqueue(tag, id) {
            self.complete_now(id, -ECANCELED);
            return;
        }
        #[cfg(all(feature = "local", target_os = "linux"))]
        if self.unfollow(id) {
            self.complete_now(id, -ECANCELED);
            return;
        }
        if self.unawait(tag, id) {
            self.complete_now(id, -ECANCELED);
            return;
        }
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        if self.unwait(id) {
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
        if first && push_cancel(&mut self.queue, id).is_err() {
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

    /// Gives up to `limit` ready completions to their operations. Gives their number. Under
    /// [`Delivery::Controlled`], they are the completions of earlier turns that no hold keeps, and
    /// the completions that the backend posted wait for the next turn.
    fn harvest(&mut self, limit: u32) -> u32 {
        let mut completed = 0;
        let (mut native, mut kept) = (None, None);
        match &mut self.control {
            None => native = Some(self.queue.completions().take(limit as usize)),
            Some(control) => kept = Some(core::iter::from_fn(|| control.give()).take(limit as usize)),
        }
        let (shared, table, pending) = (&self.shared, &mut self.table, &mut self.pending);
        let (resources, selected) = (&mut self.resources, &mut self.selected);
        #[cfg(target_os = "linux")]
        let (waking, watched) = (&mut self.waking, self.watched.get());
        #[cfg(all(feature = "local", target_os = "linux"))]
        let (wakes, consumers) = (&mut self.wakes, &mut self.consumers);
        #[cfg(unix)]
        let (children, ring_wait) = (&mut self.children, self.report.route(Call::WaitChild) == Route::Ring);
        for cqe in native.iter_mut().flatten().chain(kept.iter_mut().flatten()) {
            completed += 1;
            let mut event = cqe.event();
            // One look at the slot gives the tag of the operation, for each step below.
            let op = OpId::from_user_data(cqe.user_data);
            let tagged = op.and_then(|id| table.resources(id).copied());
            // The completion of an exit operation ends it when the home has the status of its child.
            #[cfg(unix)]
            if let Some(id) = op
                && let Some(Tag {
                    kind: Kind::Exit(child),
                    ..
                }) = tagged
                && table.runs(id)
                && let Some(slot) = shared.entries.get(id.index() as usize)
            {
                let mut ended = Some(cqe.res);
                slot.update(id, |held| {
                    if let Some(Held {
                        payload: Payload::Exit(info),
                        ..
                    }) = held
                    {
                        ended = children.exited(id, child, cqe.res, ring_wait, info);
                    }
                });
                match ended {
                    Some(result) => event.result = result,
                    None => continue,
                }
            }
            // A completion of a batch is one of its entries: its last one ends the batch, and the
            // result of the batch is its count.
            if let Some(id) = op
                && tagged.is_some_and(|tag| tag.kind.batch())
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
            // A buffer of a multishot receive goes to the shelf of its stream. A receive that waits
            // takes it after the harvest, and a full shelf ends the multishot receive there.
            if let Some(id) = op
                && let Some(Tag {
                    kind: Kind::Multishot,
                    resource: Some(resource),
                    ..
                }) = tagged
                && event.more
                && table.runs(id)
                && let Some(buffer) = cqe
                    .buffer()
                    .zip(selected.as_mut())
                    .and_then(|(bid, selected)| selected.filled(bid, cqe.res))
                && resources.shelve(resource, buffer)
            {
                pending.push(resource);
            }
            let outcome = table.complete(cqe.user_data, event);
            let (id, abandoned, tag) = match (outcome, op) {
                // The poll of the wake descriptor ended: the next wait polls it again. When
                // something watches the descriptor, the turn clears it after the harvest, and
                // registers it again on a kernel with the defect of `probe`.
                #[cfg(target_os = "linux")]
                (Outcome::Control(WAKE), _) => {
                    *waking = false;
                    if !watched {
                        shared.clear();
                    }
                    continue;
                }
                // The timer of the children ended: the poll of each child whose time came follows.
                #[cfg(target_os = "linux")]
                (Outcome::Control(CHILD_TIMER), _) => {
                    children.timed();
                    continue;
                }
                // A wake message of this driver completed. If it failed, it did not reach its
                // home, and the wake descriptor wakes the home instead.
                #[cfg(all(feature = "local", target_os = "linux"))]
                (Outcome::Control(data), _) if home::Wakes::is_message(data) => {
                    wakes.complete(data, cqe.res);
                    continue;
                }
                (Outcome::Ready, Some(id)) => match tagged {
                    Some(tag) => (id, false, tag),
                    None => continue,
                },
                (Outcome::Abandoned(Done { resources: tag, .. }), Some(id)) => (id, true, tag),
                // No driver operation is multishot yet. The other outcomes, such as a wake message
                // that `ARRIVED`, change nothing.
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
                #[cfg(all(feature = "local", target_os = "linux"))]
                if tag.consumer == Consumer::Home {
                    slot.publish_home(id, event.result);
                    consumers.wake(id);
                    continue;
                }
                slot.publish(id, event.result);
            }
        }
        if let Some(control) = &mut self.control {
            control.take(self.queue.completions());
        }
        #[cfg(unix)]
        self.poll_again();
        completed
    }
}

/// The error of an operation that a cancel ended: `ECANCELED` (`include/uapi/asm-generic/errno.h`)
/// on Linux, `ECANCELED` (`sys/errno.h`) on macOS, and `ERROR_OPERATION_ABORTED` (`winerror.h`),
/// which is also `WSA_OPERATION_ABORTED`, on Windows.
#[cfg(target_os = "linux")]
pub(crate) const ECANCELED: i32 = 125;
#[cfg(target_os = "macos")]
pub(crate) const ECANCELED: i32 = 89;
#[cfg(target_os = "windows")]
pub(crate) const ECANCELED: i32 = 995;

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
        #[cfg(unix)]
        Kind::Spawn(_) | Kind::Exit(_) => {}
        Kind::RecvFrom => {
            if let (Ok(count), Some(loan)) = (count, held.loan.as_mut()) {
                // SAFETY: the receive named the capacity of the loan in its message, and the kernel
                // wrote `count` bytes at its start, at most that capacity.
                unsafe { loan.set_filled(count) };
                if let Payload::Receive { message, received } = &mut held.payload {
                    *received = message.received(count).ok();
                }
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
                if let Payload::Receive { message, received } = &mut held.payload {
                    *received = message.received(count).ok();
                }
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
                    && let Some((peer, peer_len)) = held.payload.peer()
                {
                    (*peer, *peer_len) = (address, len);
                }
            }
            _ => {}
        },
        Kind::Recv => {
            if let (Ok(count), Some(loan)) = (count, held.loan.as_mut()) {
                // SAFETY: the receive named the capacity of the loan, and the kernel wrote `count`
                // bytes at its start, at most that capacity.
                unsafe { loan.set_filled(count) };
                // The first data of a client of a pipe gives its user.
                #[cfg(target_os = "windows")]
                if count > 0 {
                    ipc::first_data(resources, tag.resource);
                }
            }
        }
        Kind::Selected | Kind::Multishot => {
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
        // The accept of a pipe listener takes its instance, and the listener makes the next one.
        #[cfg(target_os = "windows")]
        Kind::Accept
            if tag
                .resource
                .and_then(|listener| resources.get(listener))
                .is_some_and(|owned| owned.medium == Medium::Pipe) =>
        {
            ipc::accepted(held, tag, cqe.res, abandoned, resources);
        }
        Kind::Accept => {
            if cqe.res >= 0 {
                // SAFETY: the kernel gave a new descriptor that nothing else owns.
                let fd = unsafe { owned(cqe.res) };
                let Payload::Socket {
                    peer,
                    peer_len,
                    connection,
                } = &mut held.payload
                else {
                    // An accept always has its address, and the drop closes the descriptor.
                    return;
                };
                // The connection of a listener that no driver owns stays a plain descriptor.
                if abandoned || tag.resource.is_none() {
                    *connection = Some(fd);
                } else {
                    match resources.adopt(fd, Medium::Stream) {
                        Ok(stream) => held.accepted = Some(stream),
                        // With no free resource slot, or no registration, the listener keeps the
                        // connection for the next accept, and this one gives `OutOfMemory`.
                        Err((_, fd)) => {
                            if let Some(listener) = tag.resource.and_then(|listener| resources.get(listener)) {
                                listener.directions.keep_connection(fd, *peer, *peer_len);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(unix)]
impl AsFd for Driver {
    /// The wake descriptor. It becomes readable when a completion is ready, when the kernel has
    /// deferred work for the driver, and when another thread posts a request or a wake. Turn the
    /// driver when it is readable. On epoll and kqueue, it is the epoll descriptor or the kqueue,
    /// which is readable when an event of a resource or a wake waits.
    ///
    /// On io_uring, the first call registers the descriptor with the ring, and it is readable
    /// then, so that the first turn takes the work that came before. A driver whose caller never
    /// asks for the descriptor, and waits only in [`Driver::wait`] or [`Driver::wait_until`], has
    /// no registration: the kernel signals no descriptor at a completion, and no turn reads one.
    /// When the registration fails, the next [`Driver::turn`] gives its error.
    ///
    /// A Linux kernel with the io_uring change `cd305ee3633a` and not `04fe9aeb4f3c` signals the
    /// descriptor for deferred work only one time in each registration, for example Ubuntu 26.04
    /// with `7.0.0-1016-aws`. The first use of a wake descriptor in a process finds such a kernel
    /// with a probe, and each driver then registers the descriptor again in each turn that takes a
    /// wake, so the descriptor still becomes readable for each completion and each deferred work.
    /// A kernel with no defect makes no extra system call.
    fn as_fd(&self) -> BorrowedFd<'_> {
        #[cfg(target_os = "linux")]
        {
            self.use_wake();
            self.queue.readable(self.shared.wake.as_fd())
        }
        #[cfg(target_os = "macos")]
        return self.queue.readable();
    }
}

#[cfg(target_os = "linux")]
impl Driver {
    /// The first use of the wake descriptor of a ring: the reactor of a host reads it from now on.
    /// Registers it with the ring, so that the kernel signals it at each completion, and finds
    /// whether the kernel has the defect of `probe`. Each turn clears it from now on. A wake of
    /// another thread writes the descriptor before and after this step, so the two need no order.
    fn use_wake(&self) {
        if self.watched.get() {
            return;
        }
        let Some(ring) = self.queue.as_ring() else {
            return;
        };
        match ring.register_eventfd(self.shared.wake.as_raw_fd()) {
            Ok(()) => self.rearm_wake.set(probe::defect()),
            Err(error) => self.wake_failed.set(Some(error)),
        }
        self.watched.set(true);
        // A completion that came before the registration signalled nothing. The descriptor is
        // readable now, so the host turns the driver one time and takes that work.
        drop((&self.shared.wake).write(&1_u64.to_ne_bytes()));
    }
}

impl Drop for Driver {
    /// Ends each future: it gives [`Ended`]. The resources of a running operation leak, because the
    /// kernel can still use them, and so do the provided buffers then; [`Driver::teardown`] ends
    /// the operations first.
    fn drop(&mut self) {
        // The file table holds its own reference of each file in it. The kernel ends the table
        // with the ring, after this drop, so the table ends here, and the close of each descriptor
        // then closes its file at once, as a plain descriptor does.
        #[cfg(target_os = "linux")]
        if self.resources.files.take().is_some()
            && let Some(ring) = self.queue.ring()
        {
            drop(ring.unregister_files());
        }
        #[cfg(all(feature = "local", target_os = "linux"))]
        {
            self.settle_messages();
            // The kernel uses nothing of an operation that waits for its start, so it leaks nothing.
            self.end_following();
        }
        let busy = self.table.busy();
        for entry in &self.shared.entries {
            entry.close();
        }
        #[cfg(all(feature = "local", target_os = "linux"))]
        self.consumers.end();
        #[cfg(unix)]
        signal::end(&self.signals);
        if let Some(selected) = self.selected.take() {
            if busy {
                let _leaked = ManuallyDrop::new(selected);
            } else {
                // A failure leaves the ring registered, and then its memory leaks.
                drop(self.queue.release(selected.ring));
            }
        }
    }
}

/// The wake pipe of a kqueue driver: its read end and its write end. Both ends close at an exec,
/// and neither blocks, so a wake or a signal handler never waits for a full pipe.
#[cfg(target_os = "macos")]
fn wake_pipe() -> io::Result<(fs::File, fs::File)> {
    let (read, write) = io::pipe()?;
    let (read, write) = (OwnedFd::from(read), OwnedFd::from(write));
    if !(pipe::set_nonblocking(read.as_fd(), true) && pipe::set_nonblocking(write.as_fd(), true)) {
        return Err(io::Error::last_os_error());
    }
    Ok((fs::File::from(read), fs::File::from(write)))
}

/// Pushes a cancel of `id` by its own user data, and submits once when the queue is full.
#[expect(unsafe_code, reason = "a cancel names no memory")]
fn push_cancel(ring: &mut Queue, id: OpId) -> io::Result<()> {
    // SAFETY: a cancel names no memory.
    unsafe { push_now(ring, &Sqe::cancel(id.user_data(), id.cancel_user_data())) }
}

/// Pushes `entry`, and submits once when the queue is full.
///
/// # Safety
///
/// `entry` names no memory, as a cancel or a poll does.
#[expect(unsafe_code, reason = "the caller gives an entry that names no memory")]
unsafe fn push_now(ring: &mut Queue, entry: &Sqe) -> io::Result<()> {
    // SAFETY: the entry names no memory.
    if unsafe { ring.push(entry, None) }.is_ok() {
        return Ok(());
    }
    ring.submit(0)?;
    // SAFETY: as above.
    unsafe { ring.push(entry, None) }.map_err(|_| io::ErrorKind::WouldBlock.into())
}

/// Ends the registration of the wake descriptor and makes a new one, which starts with no
/// deferred signal (`probe`).
#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
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

/// The borrowed descriptor of `handle`.
#[cfg(unix)]
fn borrow(handle: &Handle) -> Borrowed<'_> {
    handle.as_fd()
}

/// The borrowed socket of `handle`.
#[cfg(target_os = "windows")]
fn borrow(handle: &Handle) -> Borrowed<'_> {
    std::os::windows::io::AsSocket::as_socket(handle)
}

/// The number of `handle` in a submission entry: its descriptor.
#[cfg(unix)]
fn number(handle: Borrowed<'_>) -> i32 {
    std::os::fd::AsRawFd::as_raw_fd(&handle)
}

/// The number of `handle` in a submission entry: the low 32 bits of the socket handle, its
/// significant bits, because 64-bit Windows uses 32-bit handles for interoperability. The port
/// extends the number again with its sign ([`iocp::socket`]), and the adoption of each socket
/// checks that the two agree.
#[cfg(target_os = "windows")]
#[expect(
    clippy::cast_possible_truncation,
    reason = "the adoption of each socket checks that its handle has no significant bit above the low 32"
)]
fn number(handle: Borrowed<'_>) -> i32 {
    std::os::windows::io::AsRawSocket::as_raw_socket(&handle) as i32
}

/// The descriptor that the completion of an accept gives as `number`.
///
/// # Safety
///
/// `number` is a new descriptor that nothing else owns.
#[cfg(unix)]
#[expect(unsafe_code, reason = "the caller gives a new descriptor")]
unsafe fn owned(number: i32) -> Handle {
    // SAFETY: the caller's contract.
    unsafe { OwnedFd::from_raw_fd(number) }
}

/// The socket that the completion of an accept gives as `number`.
///
/// # Safety
///
/// `number` is the number of a new socket that nothing else owns.
#[cfg(target_os = "windows")]
#[expect(unsafe_code, reason = "the caller gives a new socket")]
unsafe fn owned(number: i32) -> Handle {
    // SAFETY: the caller's contract.
    unsafe { std::os::windows::io::FromRawSocket::from_raw_socket(iocp::socket(number)) }
}
