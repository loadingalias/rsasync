//! Driver-only use: the caller owns a driver, submits operations to it, and drives it with bounded
//! nonblocking turns. No scheduler runs. On Linux, the driver runs on io_uring, or on epoll where the
//! kernel refuses io_uring. On macOS, it runs TCP and UDP on kqueue, and files on the blocking
//! service. On Windows, it runs TCP and UDP on an I/O completion port ([`Backend`]). Each backend has
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
//! On Linux and macOS, files run on the driver too: `Driver::open_file` with an explicit cache
//! mode, positional and vectored reads and writes, length, space, sync, rename, unlink,
//! directories, and `Driver::close_file`. Several operations of one file run at one time: see
//! `File`.
//!
//! [`Driver::report`] states what the driver found and applied when it started: the backend, the
//! features of the kernel, and the [`Route`] of each [`Call`] that depends on them. The driver
//! starts no helper thread. A kernel with no ring operation for a file call runs that call on the
//! blocking service of the caller, which `Driver::use_blocking` gives, and on epoll and kqueue each
//! file call runs there. [`Config::require`] makes a missing feature refuse the driver, and
//! [`Config::backend`] a missing backend.
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
#[cfg(unix)]
mod file;
#[cfg(all(feature = "local", target_os = "linux"))]
mod home;
#[cfg(target_os = "windows")]
mod iocp;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod local;
#[cfg(target_os = "linux")]
mod pipe;
#[cfg(unix)]
mod poller;
#[cfg(target_os = "linux")]
mod probe;
#[cfg(target_os = "linux")]
mod process;
mod queue;
#[cfg(target_os = "linux")]
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
use core::time::Duration;
#[cfg(target_os = "linux")]
use std::fs;
use std::io;
#[cfg(target_os = "linux")]
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
use crate::capacity::Permit;
use crate::sync::Arc;
#[cfg(target_os = "linux")]
use crate::table::CONTROL;
use crate::table::{Ended as Done, Event, MAX_SLOTS, OpId, Outcome, Request, Requests, Shape, Table, Taken};
use crate::uring::{BufferRing, Cqe, Sqe};
#[cfg(target_os = "linux")]
use crate::uring::{Ring, SetupError};
use queue::Queue;

pub use capability::{Backend, Call, Feature, Features, Report, Route};
#[cfg(unix)]
pub use file::{Alignment, Buffers, Cache, Directory, File, Incomplete, Length, OpenOptions, SyncMode, WriteMode};
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub use local::{LocalListener, LocalStream, Peer, Stale};
#[cfg(target_os = "linux")]
pub use pipe::{PipeReader, PipeWriter};
#[cfg(target_os = "linux")]
pub use process::{Child, ChildOptions, End, SignalChildError, Wait};
#[cfg(target_os = "linux")]
pub use signal::{Escalation, Recv, Signal, SignalError, SignalStream};
pub use tcp::{
    Closed, ReadHalf, Regions, SendCount, Socket, TcpListener, TcpOptions, TcpStream, WriteFailed, WriteHalf,
};
pub use udp::{Arrivals, Datagram, Datagrams, UdpOptions, UdpSocket};

use crate::completion::Entry;
pub(crate) use completion::Completion;
#[cfg(all(feature = "local", target_os = "linux"))]
pub(crate) use home::{Home, MESSAGES};

/// The limits of a driver, each of which bounds memory that the driver allocates when it starts,
/// and the features that it requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    submissions: u32,
    operations: u32,
    resources: u32,
    #[cfg(target_os = "linux")]
    children: u32,
    completions_per_turn: u32,
    required: Features,
    backend: Option<Backend>,
}

impl Config {
    /// 64 submission entries, 256 operations, 256 resources, 64 children, 64 completions per turn,
    /// no required feature, and the permissive selection of the backend.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            submissions: 64,
            operations: 256,
            resources: 256,
            #[cfg(target_os = "linux")]
            children: 64,
            completions_per_turn: 64,
            required: Features::NONE,
            backend: None,
        }
    }

    /// Requires `backend`: with [`Backend::IoUring`], a kernel that refuses io_uring makes
    /// [`Driver::new`] refuse with [`Error::Refused`], before a driver exists. With no
    /// requirement, the driver runs io_uring, and it runs epoll when the creation of the ring
    /// fails with `EPERM` or `ENOSYS`. [`Report::backend`] and [`Report::fallback`] state it.
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

    /// The resources, such as descriptors, that the driver can own at one time.
    #[must_use]
    pub const fn resources(self, resources: u32) -> Self {
        Self { resources, ..self }
    }

    /// The child processes that the driver can own at one time ([`Driver::spawn`]). Each one also
    /// holds an operation slot until its status is taken or it drops.
    #[cfg(target_os = "linux")]
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
    /// [`Config::backend`] requires [`Backend::IoUring`].
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
            Self::Refused(error) => write!(f, "the kernel refuses io_uring: {error}"),
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
    #[cfg(target_os = "linux")]
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
    #[cfg(target_os = "linux")]
    #[must_use]
    pub const fn kept(self) -> u32 {
        self.kept
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
/// operation, in the slot of the operation, so each address that the kernel uses stays the same.
/// The fields hold what the home writes for the kinds of more than one medium, and the payload
/// holds the resources of the kind.
#[derive(Debug, Default)]
pub(crate) struct Held {
    /// The resources of the kind of the operation. Its start sets the variant, and nothing
    /// changes the variant before the terminal event.
    pub(crate) payload: Payload,
    /// The loan of the buffer that a receive or a read fills.
    pub(crate) loan: Option<WriteLoan<Backing>>,
    #[cfg_attr(
        any(not(test), loom, not(target_os = "linux")),
        expect(dead_code, reason = "no operation takes a charge yet; the tests of Linux do")
    )]
    pub(crate) charge: Option<Permit>,
    /// The socket that an accept, a connect, or a bind gave, or the file that an open gave.
    pub(crate) accepted: Option<Resource>,
    /// The next operation that waits in the same direction, in start order.
    pub(crate) next: Option<OpId>,
    /// A send on a failed direction gives this error, with no submission.
    pub(crate) failed: Option<WriteFailed>,
    /// The report of a close.
    pub(crate) closed: Option<Closed>,
    /// The bytes that a receive copied from the data that an abandoned receive left.
    pub(crate) copied: usize,
    /// The home asked the kernel to cancel the operation.
    pub(crate) cancelled: bool,
    /// An error of the home that the future gives in place of the result of the kernel.
    #[cfg(unix)]
    pub(crate) error: Option<io::ErrorKind>,
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
    /// The C strings of the paths of a rename or an unlink, which the kernel reads.
    #[cfg(unix)]
    Paths {
        path: std::ffi::CString,
        target: Option<std::ffi::CString>,
    },
    /// The C string of the path of an open, which the kernel reads, and the alignment of direct
    /// I/O of the file that the open gave.
    #[cfg(unix)]
    Open {
        path: std::ffi::CString,
        alignment: Option<Alignment>,
    },
    /// The structure that a `statx` of a file writes on Linux, and an `fstat` on macOS.
    #[cfg(unix)]
    Stat(file::Stat),
    /// The buffers of a vectored read of a file.
    #[cfg(unix)]
    Buffers(Buffers),
    /// The waiter of the job of an operation that runs on the blocking service.
    #[cfg(unix)]
    Job(crate::blocking::Waiter<i32>),
    /// The waiter of the job of a spawn, and the child that the home makes at the end of the job.
    #[cfg(target_os = "linux")]
    Spawn {
        waiter: crate::blocking::Waiter<io::Result<process::Spawned>>,
        child: Option<Child>,
    },
    /// The status of a child, which a `waitid` of the exit operation writes.
    #[cfg(target_os = "linux")]
    Exit(sys::SigInfo),
}

impl Payload {
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
    #[cfg(unix)]
    File(file::Op),
    /// A spawn of a child in the child slot of the index, as a job of the blocking service.
    #[cfg(target_os = "linux")]
    Spawn(u32),
    /// The exit of the child in the child slot of the index: it ends when the driver reaps the
    /// child.
    #[cfg(target_os = "linux")]
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
    #[cfg(target_os = "linux")]
    NotPipe,
    /// The pipe end does not have the direction of the adoption: a read end for a writer, or a
    /// write end for a reader.
    #[cfg(target_os = "linux")]
    Direction,
    /// The operation does not apply to this resource, for example a half-close of a pipe.
    Unsupported,
    /// The buffer, the length, or the offset of a direct-I/O operation does not have the
    /// [`Alignment`] of its file.
    #[cfg(unix)]
    Alignment,
    /// The operation runs on the blocking service on this kernel or backend
    /// ([`Route::BlockingService`]), and the driver has no service ([`Driver::use_blocking`]), or
    /// the service refused the job.
    #[cfg(unix)]
    Blocking,
    /// Each child slot of the driver is in use ([`Config::children`]).
    #[cfg(target_os = "linux")]
    Children,
    /// The child ended, and the [`std::process::Child`] that the refusal gives back has its
    /// status.
    #[cfg(target_os = "linux")]
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
    fd: Option<Handle>,
    /// The directions of a stream or a listener, and what they keep.
    directions: tcp::Directions,
    /// What the descriptor is, which selects the calls of its operations.
    medium: Medium,
    /// The operations of a file that run, which its close waits for.
    #[cfg(unix)]
    running: u32,
    /// The last allocation of the file that extends its length on macOS, which runs alone on the
    /// file while the table says that it runs.
    #[cfg(target_os = "macos")]
    exclusive: Option<OpId>,
    /// The port queues no packet for an operation of the socket that ends at its call
    /// (skip-on-success).
    #[cfg(target_os = "windows")]
    skips: bool,
    /// The lock file of a local listener, whose lock lives while the socket lives.
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
    /// socket. It has no half-close.
    #[cfg(target_os = "linux")]
    Pipe,
    /// A UDP socket: its operations are datagram receives and sends. It has no half-close.
    Udp,
    /// A file or a directory: its operations are positional, and several run at one time.
    #[cfg(unix)]
    File,
}

/// The descriptors that the driver owns, in a slice that it allocates once.
#[derive(Debug)]
struct Resources {
    slots: Box<[Owned]>,
    /// The free slots that spawns keep for the pipe ends of their children.
    reserved: u32,
    /// The registrations in the poller, on the epoll and kqueue backends.
    #[cfg(unix)]
    registrar: Option<poller::Registrar>,
    /// The associations with the port.
    #[cfg(target_os = "windows")]
    registrar: iocp::Registrar,
}

impl Resources {
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
    #[cfg(target_os = "linux")]
    fn adopt_reserved(&mut self, fd: OwnedFd, medium: Medium) -> Option<Resource> {
        self.reserved = self.reserved.checked_sub(1)?;
        self.take(fd, medium).ok()
    }

    /// Keeps `count` free slots for [`Resources::adopt_reserved`]. Gives whether that many are
    /// free.
    #[cfg(target_os = "linux")]
    fn reserve(&mut self, count: u32) -> bool {
        let free = self.free().saturating_sub(self.reserved);
        if free < count {
            return false;
        }
        self.reserved += count;
        true
    }

    /// Ends the keep of `count` slots.
    #[cfg(target_os = "linux")]
    fn unreserve(&mut self, count: u32) {
        self.reserved = self.reserved.saturating_sub(count);
    }

    /// The free slots.
    fn free(&self) -> u32 {
        let free = self.slots.iter().filter(|slot| slot.fd.is_none()).count();
        u32::try_from(free).unwrap_or(u32::MAX)
    }

    /// Takes `fd` into the first free slot, and registers it in the poller on the epoll and kqueue
    /// backends, or associates it with the port on IOCP. A file has no readiness, so it has no
    /// registration.
    fn take(&mut self, fd: Handle, medium: Medium) -> Result<Resource, (Reason, Handle)> {
        let Some((index, slot)) = (0..).zip(self.slots.iter_mut()).find(|(_, slot)| slot.fd.is_none()) else {
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
            #[cfg(target_os = "linux")]
            if medium == Medium::Pipe && !pipe::set_nonblocking(fd.as_fd(), true) {
                return Err((Reason::Register, fd));
            }
            if registrar.add(fd.as_fd(), token).is_err() {
                return Err((Reason::Register, fd));
            }
        }
        #[cfg(target_os = "windows")]
        match self.registrar.add(&fd) {
            Ok(skips) => slot.skips = skips,
            Err(_) => return Err((Reason::Register, fd)),
        }
        slot.fd = Some(fd);
        slot.medium = medium;
        Ok(Resource {
            index,
            generation: slot.generation,
        })
    }

    fn get(&mut self, resource: Resource) -> Option<&mut Owned> {
        self.slots
            .get_mut(resource.index as usize)
            .filter(|slot| slot.generation == resource.generation && slot.fd.is_some())
    }

    /// The descriptor of `resource`, while the driver owns it.
    fn borrow(&self, resource: Resource) -> Option<Borrowed<'_>> {
        self.slots
            .get(resource.index as usize)
            .filter(|slot| slot.generation == resource.generation)?
            .fd
            .as_ref()
            .map(borrow)
    }

    /// Ends `resource` and its registration, and gives its descriptor. The identity is stale from
    /// now on.
    fn release(&mut self, resource: Resource) -> Option<Handle> {
        #[cfg(unix)]
        let registrar = self.registrar.as_ref();
        let slot = self
            .slots
            .get_mut(resource.index as usize)
            .filter(|slot| slot.generation == resource.generation && slot.fd.is_some())?;
        slot.generation = slot.generation.wrapping_add(1);
        slot.directions = tcp::Directions::default();
        #[cfg(unix)]
        {
            slot.running = 0;
        }
        #[cfg(target_os = "macos")]
        {
            slot.exclusive = None;
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            slot.lock = None;
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

/// The user data of the poll of the interrupt descriptor that a wait keeps in the ring.
#[cfg(target_os = "linux")]
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
    fn new(queue: &mut Queue, mut pool: Pool) -> io::Result<Self> {
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
            ring: queue.provide(count.next_power_of_two(), GROUP)?,
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
    #[cfg(target_os = "linux")]
    wake: fs::File,
    /// An `eventfd` that only the other threads signal. A wait polls it in the ring. A signal of
    /// the kernel would also complete that poll, so the wait does not poll `wake`.
    #[cfg(target_os = "linux")]
    interrupt: fs::File,
    /// The wake of the port: a packet that another thread posts, which ends a wait.
    #[cfg(target_os = "windows")]
    wake: iocp::Wake,
    /// The wake of the kqueue: a user event that another thread triggers, which ends a wait and
    /// makes the kqueue readable.
    #[cfg(target_os = "macos")]
    wake: poller::Wake,
}

impl Shared {
    #[cfg(target_os = "linux")]
    fn wake(&self) {
        for fd in [&self.wake, &self.interrupt] {
            // A full counter is a pending wake already, and nothing else can fail here.
            drop((&*fd).write(&1_u64.to_ne_bytes()));
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn wake(&self) {
        self.wake.post();
    }

    /// Clears the wake descriptors. Gives `true` when the wake descriptor had a wake.
    #[cfg(target_os = "linux")]
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

/// A driver that its caller owns and drives, on io_uring or on epoll ([`Backend`]). It stays on
/// the thread that created it.
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
    /// The ring polls the interrupt descriptor, so that a request or wake from another thread
    /// ends a wait.
    #[cfg(target_os = "linux")]
    waking: bool,
    /// The kernel signals the wake descriptor for deferred work only one time in each
    /// registration, or a signal of the probe came late (`probe`). Each turn that takes a wake thus
    /// registers it again.
    #[cfg(target_os = "linux")]
    rearm_wake: bool,
    /// The signal streams whose home is this driver.
    #[cfg(target_os = "linux")]
    signals: Vec<Arc<signal::Home>>,
    /// The child processes that this driver owns.
    #[cfg(target_os = "linux")]
    children: process::Children,
    /// The blocking service of the operations that the kernel cannot run
    /// ([`Driver::use_blocking`]), and the waker that its jobs wake.
    #[cfg(unix)]
    blocking: Option<(crate::blocking::Submitter, core::task::Waker)>,
    /// The operations that run on the blocking service, which each turn polls.
    #[cfg(unix)]
    jobs: Vec<OpId>,
    /// The file operations that wait for an allocation that extends their file, or that such an
    /// allocation waits for, in start order: at most one entry for each operation.
    #[cfg(target_os = "macos")]
    waiting: Vec<file::Waiting>,
    /// The home of a local owner on this driver, and the wake messages that it sent.
    #[cfg(all(feature = "local", target_os = "linux"))]
    wakes: home::Wakes,
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
        debug.field("rearm_wake", &self.rearm_wake);
        debug.finish_non_exhaustive()
    }
}

impl Driver {
    /// Starts a driver with the limits of `config`. It allocates here, one time. On io_uring, it
    /// opens three descriptors: the ring, the wake descriptor, and the interrupt descriptor. With
    /// the `local` feature, it also opens a duplicate of the ring, the target of the wake messages
    /// of local tasks. On epoll, the epoll descriptor takes the place of the ring. On macOS, it
    /// opens one kqueue, and on Windows one completion port.
    ///
    /// # Errors
    ///
    /// [`Error::Refused`] when the kernel refuses io_uring and [`Config::backend`] requires it, or
    /// when [`Config::backend`] requires a backend that the target does not have, [`Error::Config`]
    /// for a limit that is 0 or too large, [`Error::Unsupported`] for a required feature that the
    /// kernel or the backend does not have, and [`Error::System`] for another failure.
    pub fn new(config: Config) -> Result<Self, Error> {
        if config.operations == 0
            || config.operations > MAX_SLOTS
            || config.resources == 0
            || config.completions_per_turn == 0
        {
            return Err(Error::Config);
        }
        let completions = config.operations.max(config.submissions).saturating_mul(2).min(1 << 16);
        Self::start_backend(config, completions)
    }

    /// Starts the port of a valid `config` with room for `completions` operations in flight.
    #[cfg(target_os = "windows")]
    fn start_backend(config: Config, completions: u32) -> Result<Self, Error> {
        // IOCP is the one backend of Windows, and a feature is a feature of io_uring.
        if config.backend.is_some_and(|backend| backend != Backend::Iocp) {
            return Err(Error::Refused(io::ErrorKind::Unsupported.into()));
        }
        if !config.required.is_empty() {
            return Err(Error::Unsupported(config.required));
        }
        let selection = capability::Selection {
            requested: config.backend,
            backend: Backend::Iocp,
            fallback: None,
        };
        let port = iocp::Port::new(config.submissions, completions).map_err(|error| match error.kind() {
            io::ErrorKind::InvalidInput => Error::Config,
            _ => Error::System(error),
        })?;
        let (wake, registrar) = (port.wake(), port.registrar());
        let table = Table::new(config.operations);
        let shared = Arc::new(Shared {
            entries: (0..config.operations).map(|_| Entry::new()).collect(),
            requests: table.requests(),
            wake,
        });
        Ok(Self {
            queue: Queue::Port(port),
            table,
            shared,
            resources: Resources {
                slots: (0..config.resources).map(|_| Owned::default()).collect(),
                reserved: 0,
                registrar,
            },
            taken: Vec::with_capacity(config.operations as usize * 2),
            pending: Vec::with_capacity(config.resources as usize),
            copied: 0,
            selected: None,
            report: Report::new(selection, config.required, Features::NONE, false),
            completions_per_turn: config.completions_per_turn,
        })
    }

    /// Starts the kqueue poller of a valid `config` with room for `completions` operations in
    /// flight.
    #[cfg(target_os = "macos")]
    fn start_backend(config: Config, completions: u32) -> Result<Self, Error> {
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
        let poller = poller::Poller::new(config.submissions, completions, config.resources, config.operations)
            .map_err(|error| match error.kind() {
                io::ErrorKind::InvalidInput => Error::Config,
                _ => Error::System(error),
            })?;
        let (wake, registrar) = (poller.wake(), poller.registrar());
        let table = Table::new(config.operations);
        let shared = Arc::new(Shared {
            entries: (0..config.operations).map(|_| Entry::new()).collect(),
            requests: table.requests(),
            wake,
        });
        Ok(Self {
            queue: Queue::Poller(poller),
            table,
            shared,
            resources: Resources {
                slots: (0..config.resources).map(|_| Owned::default()).collect(),
                reserved: 0,
                registrar: Some(registrar),
            },
            taken: Vec::with_capacity(config.operations as usize * 2),
            pending: Vec::with_capacity(config.resources as usize),
            copied: 0,
            selected: None,
            report: Report::new(selection, config.required, Features::NONE, false),
            completions_per_turn: config.completions_per_turn,
            blocking: None,
            jobs: Vec::with_capacity(config.operations as usize),
            waiting: Vec::with_capacity(config.operations as usize),
        })
    }

    /// Starts the backend of a valid `config` with room for `completions` completions: io_uring,
    /// or epoll where the kernel refuses io_uring.
    #[cfg(target_os = "linux")]
    fn start_backend(config: Config, completions: u32) -> Result<Self, Error> {
        let (wake, interrupt) = (eventfd().map_err(Error::System)?, eventfd().map_err(Error::System)?);
        let wake = fs::File::from(wake);
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
        let (queue, probed, rearm_wake) = match ring {
            Some(mut ring) => {
                // The probe needs a new ring, with no entry, completion, or buffer group.
                let probed = Features::probed(ring.probe().map_err(Error::System)?);
                // A strict requirement refuses before the driver exists.
                let missing = config.required.without(probed);
                if !missing.is_empty() {
                    return Err(Error::Unsupported(missing));
                }
                ring.register_eventfd(wake.as_raw_fd()).map_err(Error::System)?;
                let rearm_wake = probe::signal_stops_after_first(&mut ring, &wake).map_err(Error::System)?;
                (Queue::Ring(ring), probed, rearm_wake)
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
                (Queue::Poller(poller), Features::NONE, false)
            }
        };
        let registrar = match &queue {
            Queue::Poller(poller) => Some(poller.registrar()),
            Queue::Ring(_) => None,
        };
        let table = Table::new(config.operations);
        let shared = Arc::new(Shared {
            entries: (0..config.operations).map(|_| Entry::new()).collect(),
            requests: table.requests(),
            wake,
            interrupt: fs::File::from(interrupt),
        });
        #[cfg(all(feature = "local", target_os = "linux"))]
        let wakes = home::Wakes::new(&queue, &shared).map_err(Error::System)?;
        Ok(Self {
            queue,
            table,
            shared,
            resources: Resources {
                slots: (0..config.resources).map(|_| Owned::default()).collect(),
                reserved: 0,
                registrar,
            },
            children: process::Children::new(config.children),
            taken: Vec::with_capacity(config.operations as usize * 2),
            pending: Vec::with_capacity(config.resources as usize),
            copied: 0,
            selected: None,
            report: Report::new(selection, config.required, probed, rearm_wake),
            completions_per_turn: config.completions_per_turn,
            waking: false,
            rearm_wake,
            signals: Vec::new(),
            blocking: None,
            jobs: Vec::with_capacity(config.operations as usize),
            #[cfg(all(feature = "local", target_os = "linux"))]
            wakes,
        })
    }

    /// What the driver found and applied when it started.
    #[must_use]
    pub const fn report(&self) -> Report {
        self.report
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

    /// A stream of `signal`, whose home is this driver: a turn wakes it when the signal arrived.
    /// The first stream of a signal installs its handler, and the last stream that closes restores
    /// the earlier disposition. The driver connects no signal to its own end.
    ///
    /// # Errors
    ///
    /// [`SignalError`] gives the reason, and nothing changes.
    #[cfg(target_os = "linux")]
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
        self.selected = Some(Selected::new(&mut self.queue, pool)?);
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
        #[cfg(unix)]
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
        #[cfg(target_os = "linux")]
        signal::wake(&mut self.signals);
        // Start the operations that waited for an operation that ended, and end the closes.
        self.attend();
        #[cfg(target_os = "macos")]
        self.start_waiting();
        // Clear the wake descriptor, then look again: work that came after the harvest has woken
        // the descriptor already, so this turn reports it, or a later wake comes.
        #[cfg(target_os = "linux")]
        if self.shared.clear()
            && self.rearm_wake
            && let Some(ring) = self.queue.ring()
        {
            // On a kernel with the defect of `probe`, the next deferred work signals only a new
            // registration. Deferred work that comes before the new registration sets
            // `IORING_SQ_TASKRUN` before it tries the signal, so the look below sees it.
            rearm(ring, &self.shared.wake)?;
        }
        more |=
            self.queue.ready() || self.queue.more() || self.queue.unsubmitted() > 0 || !self.shared.requests.is_empty();
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

    /// Blocks as [`wait`](Self::wait) does, but at most for `timeout`.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn wait_for(&mut self, timeout: Duration) -> io::Result<()> {
        self.wait_in(Some(timeout))
    }

    /// The wait of [`wait`](Self::wait), with no limit or at most for `timeout`. On IOCP, a
    /// request or a wake of another thread posts a packet to the port, which ends the wait.
    fn wait_in(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        // A wait in the kernel ends only at a completion, so a request or a wake from another
        // thread, which only writes a descriptor, would not end it. That thread also writes the
        // interrupt descriptor, and the poll of it in the ring then completes. The poll does not
        // watch the wake descriptor, because the kernel signals that one at each completion.
        #[cfg(target_os = "linux")]
        #[expect(unsafe_code, reason = "a poll names no memory")]
        if !self.waking {
            let poll = Sqe::poll_readable(self.shared.interrupt.as_raw_fd()).with_user_data(WAKE);
            // SAFETY: a poll names no memory.
            match unsafe { push_now(&mut self.queue, &poll) } {
                Ok(()) => self.waking = true,
                // A full queue leaves work for the next turn.
                Err(error) if retryable(&error) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        let entered = match timeout {
            #[cfg(all(feature = "local", target_os = "linux"))]
            Some(timeout) => self.queue.submit_timeout(1, timeout),
            _ => self.queue.submit(1),
        };
        // A handled signal ends the wait with `Interrupted`, also under `SA_RESTART`. The wait
        // then returns: a second entry with the same `timeout` would wait the whole time again,
        // so signals that come faster than `timeout` would keep it past its end.
        match entered {
            Err(error) if !retryable(&error) => Err(error),
            _ => Ok(()),
        }
    }

    /// Cancels each running operation, drives until each terminal event, and closes each
    /// resource. Each future that has not taken its result then gives [`Ended`]. A job of the
    /// blocking service ([`Route::BlockingService`]) cannot be cancelled, so the teardown waits until
    /// it returns.
    ///
    /// On Linux, teardown applies the `End` of each child: it kills a child with `End::Kill`, or
    /// each process of its tree, and waits until the driver reaps it. A child with `End::Keep` runs
    /// on, and `Teardown::kept` counts it.
    ///
    /// # Errors
    ///
    /// A failure of the kernel entry other than an interrupt.
    pub fn teardown(mut self) -> io::Result<Teardown> {
        // A spawn cannot be cancelled, and its child starts at the end of its job, so teardown
        // applies the end of each child after each spawn ended.
        #[cfg(target_os = "linux")]
        while self.spawns() {
            self.take_requests();
            self.poll_jobs();
            if self.spawns() {
                self.wait()?;
                self.harvest(u32::MAX);
                self.attend();
            }
        }
        // An operation that waits for the one before it never ran, so it ends now.
        #[cfg(target_os = "macos")]
        for waiting in mem::take(&mut self.waiting) {
            self.complete_now(waiting.id(), -ECANCELED);
        }
        for index in 0..self.resources.slots.len() {
            let Some(resource) = self.resources.slots.get(index).map(|owned| Resource {
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
        #[cfg(target_os = "linux")]
        {
            self.children.ending = true;
        }
        for id in self.table.running() {
            match self.table.resources(id).map(|tag| tag.kind) {
                // A close ends when the operations of its resource end.
                Some(Kind::Close) => continue,
                // The exit of a child with `End::Kill` ends after its kill. A cancel ends the exit
                // operation of a child with `End::Keep`.
                #[cfg(target_os = "linux")]
                Some(Kind::Exit(child)) if self.children.kills(child) => {
                    self.children.kill(child);
                    continue;
                }
                _ => {}
            }
            push_cancel(&mut self.queue, id)?;
            cancelled = cancelled.saturating_add(1);
        }
        while self.table.busy() {
            self.take_requests();
            #[cfg(unix)]
            self.poll_jobs();
            if !self.table.busy() {
                break;
            }
            // The wait also ends when a job of the blocking service ends, because the job wakes
            // the driver: the interrupt descriptor on Linux, and the user event on macOS.
            self.wait()?;
            self.harvest(u32::MAX);
            self.attend();
        }
        self.take_requests();
        Ok(Teardown {
            cancelled,
            #[cfg(target_os = "linux")]
            kept: self.children.kept,
        })
    }

    /// Whether the job of a spawn runs.
    #[cfg(target_os = "linux")]
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

    /// The descriptor of `resource`, while the driver owns it.
    #[cfg(all(test, not(loom), unix))]
    pub(crate) fn fd(&mut self, resource: Resource) -> Option<std::os::fd::RawFd> {
        Some(std::os::fd::AsRawFd::as_raw_fd(
            self.resources.get(resource)?.fd.as_ref()?,
        ))
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
    #[cfg(all(test, not(loom), target_os = "linux"))]
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
        self.queue.room() >= entries || (self.queue.submit(0).is_ok() && self.queue.room() >= entries)
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
            let resource = self.table.resources(id).and_then(|tag| tag.resource);
            #[cfg(unix)]
            let token = resource.map(|resource| poller::Token {
                index: resource.index,
                generation: resource.generation,
            });
            #[cfg(target_os = "windows")]
            let token = resource
                .and_then(|resource| self.resources.get(resource))
                .map(|owned| iocp::Token { skips: owned.skips });
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
        #[cfg(unix)]
        if let Some(tag) = match &outcome {
            Outcome::Ready => self.table.resources(id).copied(),
            Outcome::Abandoned(Done { resources, .. }) => *resources,
            _ => None,
        } {
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
        // The cancel of the exit of a child is a kill by its end policy, and no cancel of the
        // kernel: the operation ends when the child ends.
        #[cfg(target_os = "linux")]
        if let Kind::Exit(child) = tag.kind {
            self.children.kill(child);
            return;
        }
        if self.unqueue(tag, id) {
            self.complete_now(id, -ECANCELED);
            return;
        }
        #[cfg(target_os = "macos")]
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

    /// Gives up to `limit` ready completions to their operations. Gives their number.
    fn harvest(&mut self, limit: u32) -> u32 {
        let mut completed = 0;
        let (shared, table, pending) = (&self.shared, &mut self.table, &mut self.pending);
        let (resources, selected) = (&mut self.resources, &mut self.selected);
        #[cfg(target_os = "linux")]
        let waking = &mut self.waking;
        #[cfg(all(feature = "local", target_os = "linux"))]
        let wakes = &mut self.wakes;
        #[cfg(target_os = "linux")]
        let (children, ring_wait) = (&mut self.children, self.report.route(Call::WaitChild) == Route::Ring);
        for cqe in self.queue.completions().take(limit as usize) {
            completed += 1;
            let mut event = cqe.event();
            // The completion of an exit operation ends it when the home has the status of its child.
            #[cfg(target_os = "linux")]
            if let Some(id) = OpId::from_user_data(cqe.user_data)
                && let Some(&Tag {
                    kind: Kind::Exit(child),
                    ..
                }) = table.resources(id)
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
                #[cfg(target_os = "linux")]
                (Outcome::Control(WAKE), _) => {
                    *waking = false;
                    continue;
                }
                // A wake message of this driver completed. If it failed, it did not reach its
                // home, and the interrupt descriptor wakes the home instead.
                #[cfg(all(feature = "local", target_os = "linux"))]
                (Outcome::Control(data), _) if home::Wakes::is_message(data) => {
                    wakes.complete(data, cqe.res);
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
            #[cfg(unix)]
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
        #[cfg(target_os = "linux")]
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
        #[cfg(unix)]
        Kind::File(op) => file::finish(held, op, cqe.res, abandoned, resources),
        Kind::Other
        | Kind::Send
        | Kind::Vectored
        | Kind::Shutdown
        | Kind::Close
        | Kind::SendTo
        | Kind::SendBatch
        | Kind::RecvBatch => {}
        #[cfg(target_os = "linux")]
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
                if abandoned {
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
    /// A Linux kernel with the io_uring change `cd305ee3633a` and not `04fe9aeb4f3c` signals the
    /// descriptor for deferred work only one time in each registration, for example Ubuntu 26.04
    /// with `7.0.0-1016-aws`. The first driver of a process finds such a kernel, and each driver
    /// then registers the descriptor again in each turn that takes a wake, so the descriptor still
    /// becomes readable for each completion and each deferred work. A kernel with no defect makes
    /// no extra system call.
    fn as_fd(&self) -> BorrowedFd<'_> {
        #[cfg(target_os = "linux")]
        return self.queue.readable(self.shared.wake.as_fd());
        #[cfg(target_os = "macos")]
        return self.queue.readable();
    }
}

impl Drop for Driver {
    /// Ends each future: it gives [`Ended`]. The resources of a running operation leak, because the
    /// kernel can still use them, and so do the provided buffers then; [`Driver::teardown`] ends
    /// the operations first.
    fn drop(&mut self) {
        #[cfg(all(feature = "local", target_os = "linux"))]
        self.settle_messages();
        let busy = self.table.busy();
        for entry in &self.shared.entries {
            entry.close();
        }
        #[cfg(target_os = "linux")]
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
