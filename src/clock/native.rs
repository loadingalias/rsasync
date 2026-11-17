//! The native monotonic clocks of the operating system.

use core::time::Duration;

use super::{NATIVE, Unsupported};
use crate::time::{ClockDomain, Instant, Suspend};

/// The native monotonic clock of one clock domain. Each read is one read of the clock of the
/// operating system, with no system call where the system maps its clock into the process.
///
/// | Target | `Suspend::Advances` | `Suspend::Pauses` | Resolution |
/// | --- | --- | --- | --- |
/// | Linux, x86_64 and aarch64 | `CLOCK_BOOTTIME` | `CLOCK_MONOTONIC` | `clock_getres` |
/// | macOS | `CLOCK_MONOTONIC_RAW` | `CLOCK_UPTIME_RAW` | `clock_getres` |
/// | Windows, x86_64 and aarch64 | `QueryInterruptTimePrecise` | `QueryUnbiasedInterruptTimePrecise` | 100 ns, the unit of the count |
///
/// Each other target has no native clock, and [`NativeClock::new`] gives [`Unsupported`].
///
/// ```
/// use rsasync::clock::NativeClock;
/// use rsasync::time::Suspend;
///
/// let clock = NativeClock::new(Suspend::Pauses)?;
/// let start = clock.now();
/// let end = clock.now();
/// assert!(end.compare(start)?.is_ge());
/// assert!(!clock.resolution().is_zero());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// An instant is the nanoseconds since the origin of the clock, for example the start of the
/// system. A count above `u64::MAX` nanoseconds, about 584 years, stays at the last instant of the
/// domain.
#[derive(Debug, Clone, Copy)]
pub struct NativeClock {
    domain: ClockDomain,
    resolution: Duration,
    source: sys::Source,
}

impl NativeClock {
    /// The native clock of the domain with `suspend`, on the identifier [`NATIVE`].
    ///
    /// # Errors
    ///
    /// [`Unsupported`] when the target has no native clock for that domain.
    pub fn new(suspend: Suspend) -> Result<Self, Unsupported> {
        let (source, resolution) = sys::Source::new(suspend).ok_or(Unsupported)?;
        Ok(Self {
            domain: ClockDomain::new(NATIVE, suspend),
            resolution,
            source,
        })
    }

    /// The domain of the clock.
    #[must_use]
    pub fn domain(self) -> ClockDomain {
        self.domain
    }

    /// The resolution that the operating system reports for the clock.
    #[must_use]
    pub fn resolution(self) -> Duration {
        self.resolution
    }

    /// The current instant of the clock. It is never before an instant that a read before it gave,
    /// on any thread.
    #[must_use]
    pub fn now(self) -> Instant {
        Instant::new(self.domain, self.source.now())
    }
}

cfg_select! {
    any(
        all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")),
        target_os = "macos"
    ) => {
        /// `clock_gettime` of the C library, on Linux and macOS.
        mod sys {
            use core::ffi::{c_int, c_long};
            use core::time::Duration;

            use crate::time::Suspend;

            /// The clock identifiers: `include/uapi/linux/time.h` of Linux 6.1, and `_time.h` of the
            /// macOS 27.2 SDK, where `clockid_t` is an `enum` of `int` values.
            #[cfg(target_os = "linux")]
            const CLOCKS: [c_int; 2] = [CLOCK_BOOTTIME, CLOCK_MONOTONIC];
            #[cfg(target_os = "linux")]
            const CLOCK_MONOTONIC: c_int = 1;
            #[cfg(target_os = "linux")]
            const CLOCK_BOOTTIME: c_int = 7;
            #[cfg(target_os = "macos")]
            const CLOCKS: [c_int; 2] = [CLOCK_MONOTONIC_RAW, CLOCK_UPTIME_RAW];
            #[cfg(target_os = "macos")]
            const CLOCK_MONOTONIC_RAW: c_int = 4;
            #[cfg(target_os = "macos")]
            const CLOCK_UPTIME_RAW: c_int = 8;

            /// `struct timespec`, with a `time_t` of `long` on these 64-bit targets: `time.h` of glibc
            /// and musl, and `sys/_types/_timespec.h` of the macOS SDK.
            #[repr(C)]
            struct Timespec {
                tv_sec: c_long,
                tv_nsec: c_long,
            }

            const _: () = {
                assert!(size_of::<Timespec>() == 16);
                assert!(core::mem::offset_of!(Timespec, tv_nsec) == 8);
            };

            #[expect(unsafe_code, reason = "the C library of Linux and of macOS gives these functions")]
            unsafe extern "C" {
                /// `int clock_getres(clockid_t clock_id, struct timespec *res)`.
                fn clock_getres(clock_id: c_int, res: *mut Timespec) -> c_int;
                /// `int clock_gettime(clockid_t clock_id, struct timespec *tp)`.
                fn clock_gettime(clock_id: c_int, tp: *mut Timespec) -> c_int;
            }

            /// The clock of one domain.
            #[derive(Debug, Clone, Copy)]
            pub(super) struct Source(c_int);

            impl Source {
                /// The clock of the domain with `suspend`, and its resolution. `None` when the system
                /// does not know the clock, for example a kernel without it.
                #[expect(unsafe_code, reason = "the call writes only the `timespec` that it gets")]
                pub(super) fn new(suspend: Suspend) -> Option<(Self, Duration)> {
                    let clock = match suspend {
                        Suspend::Advances => CLOCKS[0],
                        Suspend::Pauses => CLOCKS[1],
                    };
                    let mut res = Timespec { tv_sec: 0, tv_nsec: 0 };
                    // SAFETY: `res` is a valid `timespec` for the call to write.
                    if unsafe { clock_getres(clock, &raw mut res) } != 0 {
                        return None;
                    }
                    Some((Self(clock), Duration::from_nanos(nanos(&res)?)))
                }

                /// The nanoseconds since the origin of the clock.
                #[expect(unsafe_code, reason = "the call writes only the `timespec` that it gets")]
                #[expect(
                    clippy::panic,
                    reason = "clock_gettime fails only for a clock that the system does not know, and `Source::new` checked the clock with clock_getres; the caller has no recovery path"
                )]
                pub(super) fn now(self) -> u64 {
                    let mut tp = Timespec { tv_sec: 0, tv_nsec: 0 };
                    // SAFETY: `tp` is a valid `timespec` for the call to write.
                    let result = unsafe { clock_gettime(self.0, &raw mut tp) };
                    match (result, nanos(&tp)) {
                        (0, Some(nanos)) => nanos,
                        _ => panic!("clock_gettime failed or gave a negative time for clock {}", self.0),
                    }
                }
            }

            /// The nanoseconds of `time`, up to `u64::MAX`, or `None` for a negative time.
            fn nanos(time: &Timespec) -> Option<u64> {
                let secs = u64::try_from(time.tv_sec).ok()?;
                let nanos = u64::try_from(time.tv_nsec).ok()?;
                Some(secs.saturating_mul(1_000_000_000).saturating_add(nanos))
            }
        }
    }
    all(windows, any(target_arch = "x86_64", target_arch = "aarch64")) => {
        /// The interrupt-time counts of Windows, from `realtimeapiset.h` of the Windows SDK, in the API set
        /// `api-ms-win-core-realtime-l1-1-1` of Windows 10 and later. Each count is in units of 100 ns
        /// since the start of the system.
        mod sys {
            use core::time::Duration;

            use crate::time::Suspend;

            #[expect(unsafe_code, reason = "Windows 10 and later give these functions")]
            #[link(
                name = "api-ms-win-core-realtime-l1-1-1.dll",
                kind = "raw-dylib",
                modifiers = "+verbatim"
            )]
            unsafe extern "system" {
                /// `VOID QueryInterruptTimePrecise(PULONGLONG lpInterruptTimePrecise)`: the count also
                /// counts the time of sleep and hibernation.
                #[link_name = "QueryInterruptTimePrecise"]
                fn query_interrupt_time_precise(count: *mut u64);
                /// `VOID QueryUnbiasedInterruptTimePrecise(PULONGLONG lpUnbiasedInterruptTimePrecise)`:
                /// the count does not count the time of sleep and hibernation.
                #[link_name = "QueryUnbiasedInterruptTimePrecise"]
                fn query_unbiased_interrupt_time_precise(count: *mut u64);
            }

            /// The count of one domain.
            #[derive(Debug, Clone, Copy)]
            pub(super) struct Source(Suspend);

            impl Source {
                /// The count of the domain with `suspend`, and its unit, which is its resolution.
                pub(super) fn new(suspend: Suspend) -> Option<(Self, Duration)> {
                    Some((Self(suspend), Duration::from_nanos(100)))
                }

                /// The nanoseconds since the start of the system.
                #[expect(unsafe_code, reason = "the call writes only the count that it gets")]
                pub(super) fn now(self) -> u64 {
                    let mut count = 0;
                    // SAFETY: `count` is a valid `u64` for the call to write.
                    unsafe {
                        match self.0 {
                            Suspend::Advances => query_interrupt_time_precise(&raw mut count),
                            Suspend::Pauses => query_unbiased_interrupt_time_precise(&raw mut count),
                        }
                    }
                    count.saturating_mul(100)
                }
            }
        }
    }
    _ => {
        /// No native clock on this target.
        mod sys {
            use core::time::Duration;

            use crate::time::Suspend;

            /// No value of this type exists.
            #[derive(Debug, Clone, Copy)]
            pub(super) enum Source {}

            impl Source {
                pub(super) fn new(_suspend: Suspend) -> Option<(Self, Duration)> {
                    None
                }

                pub(super) fn now(self) -> u64 {
                    match self {}
                }
            }
        }
    }
}
