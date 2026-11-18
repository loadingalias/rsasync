//! The capability report of a driver: the backend that its caller required and the backend that
//! runs, the features that its caller required, the features that the probes of its kernel found,
//! the route of each call that depends on them, and what the driver applied.
//! [`Driver::new`](super::Driver::new) builds it one time.

use core::fmt;

#[cfg(target_os = "linux")]
use crate::uring;

/// The kernel interface that runs the operations of a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Backend {
    /// io_uring: the kernel runs each operation and posts its completion.
    IoUring,
    /// epoll: the driver runs each socket and pipe operation as one nonblocking call on its own
    /// thread when epoll reports its descriptor ready. Each file operation runs on the blocking
    /// service ([`Route::BlockingService`]), because epoll cannot wait on regular files.
    Epoll,
    /// IOCP, on Windows: each socket operation, and each read, write, and hole punch of a file, is
    /// one overlapped call or a few in order, and its completion packet or the return of the call
    /// ends it, by the NT status of the call. Each other file operation has no overlapped form, so
    /// it runs on the blocking service ([`Route::BlockingService`]).
    Iocp,
    /// kqueue, on macOS: the driver runs each socket operation as one nonblocking call on its own
    /// thread when kqueue reports its descriptor ready, as on epoll. UDP has no segmentation
    /// offload and no coalesced receive: a send with a segment size gives
    /// `datagram::Refused::NoSegmentation` with no call, and the `gro` option of a UDP socket gives
    /// `Unsupported`. A caller learns it as on each system, from the send of a `datagram::Probe`.
    /// Each file operation runs on the blocking service ([`Route::BlockingService`]), as on epoll.
    Kqueue,
}

/// A feature of the kernel that the driver probes on its ring when it starts. A probe runs the
/// feature, or reads it from the kernel: the version of the kernel proves nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Feature {
    /// `IORING_OP_WAITID`: a wait for a child (Linux 6.7).
    ChildWait,
    /// `IORING_OP_FTRUNCATE`: a change of the length of a file (Linux 6.9).
    Truncate,
    /// `IORING_FEAT_RECVSEND_BUNDLE`: send and receive bundles (Linux 6.10).
    Bundles,
    /// `IORING_OP_BIND`: a bind on the ring (Linux 6.11).
    Bind,
    /// `IORING_OP_LISTEN`: a listen on the ring (Linux 6.11).
    Listen,
    /// `IOU_PBUF_RING_INC`: incremental buffer rings (Linux 6.12).
    IncrementalBufferRings,
    /// `IORING_REGISTER_SEND_MSG_RING`: a cross-ring message from a thread with no ring (Linux
    /// 6.13).
    RinglessMessages,
    /// `IORING_OP_RECV_ZC`: zero-copy receive (Linux 6.15).
    ZeroCopyReceive,
}

/// Each feature, in the order of the bits of [`Features`].
const ALL: [Feature; 8] = [
    Feature::ChildWait,
    Feature::Truncate,
    Feature::Bundles,
    Feature::Bind,
    Feature::Listen,
    Feature::IncrementalBufferRings,
    Feature::RinglessMessages,
    Feature::ZeroCopyReceive,
];

impl Feature {
    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

/// A set of [`Feature`] values.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct Features(u8);

impl Features {
    /// The set with no feature.
    pub const NONE: Self = Self(0);

    /// This set and `feature`.
    #[must_use]
    pub const fn with(self, feature: Feature) -> Self {
        Self(self.0 | feature.bit())
    }

    /// Whether the set holds `feature`.
    #[must_use]
    pub const fn contains(self, feature: Feature) -> bool {
        self.0 & feature.bit() != 0
    }

    /// Whether the set holds no feature.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The features of this set that `other` does not hold.
    #[must_use]
    pub const fn without(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    /// Each feature of the set.
    pub fn iter(self) -> impl Iterator<Item = Feature> {
        ALL.into_iter().filter(move |feature| self.contains(*feature))
    }
}

impl fmt::Debug for Features {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[cfg(target_os = "linux")]
impl Features {
    /// The features that `probed` found.
    pub(super) fn probed(probed: uring::Features) -> Self {
        let found = [
            probed.child_wait,
            probed.truncate,
            probed.bundles,
            probed.bind,
            probed.listen,
            probed.incremental_buffer_rings,
            probed.ringless_messages,
            probed.zero_copy_receive,
        ];
        ALL.into_iter()
            .zip(found)
            .filter(|&(_, found)| found)
            .fold(Self::NONE, |set, (feature, _)| set.with(feature))
    }
}

/// A call of the driver whose path depends on the features of the kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Call {
    /// [`Driver::bind`](super::Driver::bind): a bind and a listen of a TCP listener.
    Bind,
    /// `Driver::bind_udp`, on Linux: a bind of a UDP socket.
    BindUdp,
    /// [`Driver::set_file_len`](super::Driver::set_file_len): a change of the length of a file.
    SetFileLen,
    /// The exit of a `Child`, on Linux: the reap of the child and its status.
    WaitChild,
    /// The operations on the bytes of a file: the positional and vectored reads and writes, and the
    /// hole punch.
    FileData,
    /// [`Driver::sync_directory`](super::Driver::sync_directory): the sync that makes the creates,
    /// renames, and unlinks of a directory durable.
    SyncDirectory,
    /// Each other operation of a file or a directory: open, length inspection, preallocation,
    /// sync, rename, and unlink. On Windows, also the report of the device of a file.
    File,
}

/// How a call runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Route {
    /// An entry of the ring.
    Ring,
    /// System calls on the thread of the driver, which end at once.
    SystemCall,
    /// A job of the blocking service that [`Driver::use_blocking`](super::Driver::use_blocking)
    /// gives. With no service, the call refuses with
    /// [`Reason::Blocking`](super::Reason::Blocking).
    BlockingService,
    /// A poll of a descriptor on the ring or the poller, then one nonblocking system call on the
    /// thread of the driver when the descriptor is readable.
    Poll,
    /// Overlapped calls on the completion port, on Windows: the packet of each call, or its return,
    /// ends it.
    Port,
    /// No qualified form of the call: it gives `Unsupported` at once, with no system call. On
    /// Windows, no source states that a flush of a directory makes its namespace changes durable,
    /// so a directory sync has this route (selected on 2026-11-18).
    Unsupported,
}

/// Each call, in the order of the report.
const CALLS: [Call; 7] = [
    Call::Bind,
    Call::BindUdp,
    Call::SetFileLen,
    Call::WaitChild,
    Call::FileData,
    Call::SyncDirectory,
    Call::File,
];

/// What a driver found and applied when it started. The report does not change after
/// [`Driver::new`](super::Driver::new). Its `Debug` form also gives the route of each call and the
/// helper threads.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Report {
    requested: Option<Backend>,
    backend: Backend,
    fallback: Option<i32>,
    required: Features,
    probed: Features,
    rearms_wake: bool,
}

impl Report {
    pub(super) const fn new(selection: Selection, required: Features, probed: Features, rearms_wake: bool) -> Self {
        Self {
            requested: selection.requested,
            backend: selection.backend,
            fallback: selection.fallback,
            required,
            probed,
            rearms_wake,
        }
    }

    /// The backend that the caller required with [`Config::backend`](super::Config::backend), or
    /// `None` for the permissive selection: io_uring, and epoll when the kernel refuses io_uring.
    #[must_use]
    pub const fn requested(&self) -> Option<Backend> {
        self.requested
    }

    /// The backend that runs the operations of the driver.
    #[must_use]
    pub const fn backend(&self) -> Backend {
        self.backend
    }

    /// The error number with which the kernel refused to create a ring (`EPERM` or `ENOSYS`), when
    /// the permissive selection runs epoll for that reason. `EPERM` comes, for example, from the
    /// default seccomp profile of a container or from `kernel.io_uring_disabled`.
    #[must_use]
    pub const fn fallback(&self) -> Option<i32> {
        self.fallback
    }

    /// The features that the caller required with [`Config::require`](super::Config::require).
    /// The driver has each of them: a kernel with no such feature refuses the driver.
    #[must_use]
    pub const fn required(&self) -> Features {
        self.required
    }

    /// The features that the probes of the kernel found.
    #[must_use]
    pub const fn probed(&self) -> Features {
        self.probed
    }

    /// The route of `call` on this kernel and backend. A probe runs only on io_uring, so on epoll
    /// no feature is probed.
    #[must_use]
    pub const fn route(&self, call: Call) -> Route {
        let ring = match call {
            Call::Bind => self.probed.contains(Feature::Bind) && self.probed.contains(Feature::Listen),
            Call::BindUdp => self.probed.contains(Feature::Bind),
            Call::SetFileLen => self.probed.contains(Feature::Truncate),
            Call::WaitChild => self.probed.contains(Feature::ChildWait),
            Call::FileData | Call::SyncDirectory | Call::File => matches!(self.backend, Backend::IoUring),
        };
        let iocp = matches!(self.backend, Backend::Iocp);
        match (ring, call) {
            (true, _) => Route::Ring,
            (false, Call::FileData) if iocp => Route::Port,
            (false, Call::SyncDirectory) if iocp => Route::Unsupported,
            (false, Call::Bind | Call::BindUdp) => Route::SystemCall,
            (false, Call::SetFileLen | Call::FileData | Call::SyncDirectory | Call::File) => Route::BlockingService,
            (false, Call::WaitChild) => Route::Poll,
        }
    }

    /// The threads that the driver starts: none, on each backend. A call on
    /// [`Route::BlockingService`] runs on a thread of the service that
    /// [`Driver::use_blocking`](super::Driver::use_blocking) gives, which the caller owns.
    #[must_use]
    pub const fn helper_threads(&self) -> u32 {
        0
    }

    /// Whether the driver registers its wake descriptor again in each turn that takes a wake. It
    /// does so on a kernel whose registered event descriptor signals deferred work one time. It
    /// also does so when a deferred signal came late during the probe of the kernel (see the
    /// `AsFd` form of the driver on Linux).
    #[must_use]
    pub const fn rearms_wake(&self) -> bool {
        self.rearms_wake
    }
}

impl fmt::Debug for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        /// The route of each call, as a map.
        struct Routes<'a>(&'a Report);

        impl fmt::Debug for Routes<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_map()
                    .entries(CALLS.into_iter().map(|call| (call, self.0.route(call))))
                    .finish()
            }
        }

        f.debug_struct("Report")
            .field("requested", &self.requested)
            .field("backend", &self.backend)
            .field("fallback", &self.fallback)
            .field("required", &self.required)
            .field("probed", &self.probed)
            .field("routes", &Routes(self))
            .field("helper_threads", &self.helper_threads())
            .field("rearms_wake", &self.rearms_wake)
            .finish()
    }
}

/// The backend selection of [`Driver::new`](super::Driver::new): what the caller required, what
/// runs, and the refusal of io_uring that made the permissive selection run epoll.
#[derive(Debug, Clone, Copy)]
pub(super) struct Selection {
    pub(super) requested: Option<Backend>,
    pub(super) backend: Backend,
    pub(super) fallback: Option<i32>,
}
