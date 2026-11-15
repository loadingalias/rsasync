//! The capability report of a driver: the features that its caller required, the features that the
//! probes of its kernel found, the route of each call that depends on them, and what the driver
//! applied. [`Driver::new`](super::Driver::new) builds it one time.

use core::fmt;

use crate::uring;

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
    /// [`Driver::bind`](super::Driver::bind) and [`Driver::bind_local`](super::Driver::bind_local):
    /// a bind and a listen of a TCP or a local listener.
    Bind,
    /// [`Driver::bind_udp`](super::Driver::bind_udp): a bind of a UDP socket.
    BindUdp,
    /// [`Driver::set_file_len`](super::Driver::set_file_len): a change of the length of a file.
    SetFileLen,
    /// The exit of a [`Child`](super::Child): the reap of the child and its status.
    WaitChild,
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
    /// A poll of a descriptor on the ring, then one nonblocking system call on the thread of the
    /// driver when the descriptor is readable.
    Poll,
}

/// Each call, in the order of the report.
const CALLS: [Call; 4] = [Call::Bind, Call::BindUdp, Call::SetFileLen, Call::WaitChild];

/// What a driver found and applied when it started, for its io_uring backend. The report does not
/// change after [`Driver::new`](super::Driver::new). Its `Debug` form also gives the route of each
/// call and the helper threads.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Report {
    required: Features,
    probed: Features,
    rearms_wake: bool,
}

impl Report {
    pub(super) const fn new(required: Features, probed: Features, rearms_wake: bool) -> Self {
        Self {
            required,
            probed,
            rearms_wake,
        }
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

    /// The route of `call` on this kernel.
    #[must_use]
    pub const fn route(&self, call: Call) -> Route {
        let ring = match call {
            Call::Bind => self.probed.contains(Feature::Bind) && self.probed.contains(Feature::Listen),
            Call::BindUdp => self.probed.contains(Feature::Bind),
            Call::SetFileLen => self.probed.contains(Feature::Truncate),
            Call::WaitChild => self.probed.contains(Feature::ChildWait),
        };
        match (ring, call) {
            (true, _) => Route::Ring,
            (false, Call::Bind | Call::BindUdp) => Route::SystemCall,
            (false, Call::SetFileLen) => Route::BlockingService,
            (false, Call::WaitChild) => Route::Poll,
        }
    }

    /// The threads that the driver starts: none. A call on [`Route::BlockingService`] runs on
    /// a thread of that service.
    #[must_use]
    pub const fn helper_threads(&self) -> u32 {
        0
    }

    /// Whether the driver registers its wake descriptor again in each turn that takes a wake. It
    /// does so on a kernel whose registered event descriptor signals deferred work one time. It
    /// also does so when a deferred signal came late during the probe of the kernel (see the
    /// [`AsFd`](std::os::fd::AsFd) form of the driver).
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
            .field("required", &self.required)
            .field("probed", &self.probed)
            .field("routes", &Routes(self))
            .field("helper_threads", &self.helper_threads())
            .field("rearms_wake", &self.rearms_wake)
            .finish()
    }
}
