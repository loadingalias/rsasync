//! The physical-time source: the civil time of the system, with an uncertainty bound.

#[cfg(test)]
mod tests;

use core::time::Duration;

use super::Unsupported;
use crate::time::{Reading, ReadingSource};

/// The civil time of the system, as a [`Reading`]: an estimate, an uncertainty bound or "unknown",
/// and the source of the bound.
///
/// | Target | Estimate | Bound |
/// | --- | --- | --- |
/// | Linux, x86_64 and aarch64 | `adjtimex` | The maximum error of the kernel, while the clock is synchronized. Else unknown |
/// | macOS, and Windows on x86_64 and aarch64 | [`std::time::SystemTime`] | Unknown |
///
/// Each other target has no physical-time source, and [`PhysicalClock::new`] gives
/// [`Unsupported`]. A bound that the caller supplies with [`PhysicalClock::with_bound`] replaces
/// the bound of the system. The source never invents a bound: an estimate before the Unix epoch
/// shows a wrong clock, so it gives an estimate of 0 with an unknown bound.
///
/// A correction or a step of the civil clock changes the readings, but never an instant of a
/// [`NativeClock`](super::NativeClock), so it never moves a deadline.
///
/// ```
/// use core::time::Duration;
///
/// use rsasync::clock::PhysicalClock;
/// use rsasync::time::ReadingSource;
///
/// let reading = PhysicalClock::new()?.read();
/// assert_eq!(reading.source(), ReadingSource::SystemClock);
///
/// // A bound that the caller knows, for example from its own time service.
/// let reading = PhysicalClock::new()?.with_bound(Duration::from_millis(20)).read();
/// assert_eq!(reading.bound(), Some(Duration::from_millis(20)));
/// assert_eq!(reading.source(), ReadingSource::Caller);
/// # Ok::<(), rsasync::clock::Unsupported>(())
/// ```
#[derive(Debug, Clone, Copy)]
pub struct PhysicalClock {
    bound: Option<Duration>,
    source: sys::Source,
}

impl PhysicalClock {
    /// The physical-time source of the system, with the bound of the system.
    ///
    /// # Errors
    ///
    /// [`Unsupported`] when the target has no physical-time source.
    pub fn new() -> Result<Self, Unsupported> {
        Ok(Self {
            bound: None,
            source: sys::Source::new().ok_or(Unsupported)?,
        })
    }

    /// The same source, with a bound that the caller supplies for each reading.
    #[must_use]
    pub fn with_bound(self, bound: Duration) -> Self {
        Self {
            bound: Some(bound),
            ..self
        }
    }

    /// Reads the civil time.
    #[must_use]
    pub fn read(self) -> Reading {
        let Sample { estimate, bound } = self.source.read();
        match (estimate, self.bound) {
            (Some(estimate), Some(caller)) => Reading::new(estimate, Some(caller), ReadingSource::Caller),
            (Some(estimate), None) => Reading::new(estimate, bound, ReadingSource::SystemClock),
            (None, _) => Reading::new(Duration::ZERO, None, ReadingSource::SystemClock),
        }
    }
}

/// What the system gives: the time since the Unix epoch, or `None` for a time before it, and the
/// bound of the system.
struct Sample {
    estimate: Option<Duration>,
    bound: Option<Duration>,
}

cfg_select! {
    any(
        all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")),
        target_os = "macos",
        all(windows, any(target_arch = "x86_64", target_arch = "aarch64"))
    ) => {
        /// The civil clock of the system: the kernel clock on Linux, and the civil time of std on macOS
        /// and Windows.
        mod sys {
            use super::Sample;

            /// The civil clock of the system.
            #[derive(Debug, Clone, Copy)]
            pub(super) struct Source;

            impl Source {
                pub(super) fn new() -> Option<Self> {
                    Some(Self)
                }

                pub(super) fn read(self) -> Sample {
                    #[cfg(target_os = "linux")]
                    if let Some(sample) = kernel::read() {
                        return sample;
                    }
                    Sample {
                        estimate: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok(),
                        bound: None,
                    }
                }
            }

            /// `adjtimex` of the C library, on Linux: the civil time and the maximum error of the
            /// kernel.
            #[cfg(target_os = "linux")]
            pub(super) mod kernel {
                use core::ffi::{c_int, c_long, c_uint};
                use core::time::Duration;

                use super::Sample;

                /// The constants of `include/uapi/linux/timex.h` of Linux 6.1: the clock is not
                /// synchronized, the time has nanoseconds, and the state of an unsynchronized clock.
                const STA_UNSYNC: c_int = 0x0040;
                const STA_NANO: c_int = 0x2000;
                const TIME_ERROR: c_int = 5;

                /// `struct timeval`.
                #[derive(Default)]
                #[repr(C)]
                pub(in super::super) struct Timeval {
                    pub(in super::super) tv_sec: c_long,
                    /// Microseconds, or nanoseconds when the status has `STA_NANO`.
                    pub(in super::super) tv_usec: c_long,
                }

                /// `struct timex` of `sys/timex.h` of glibc and musl, on these 64-bit targets. It agrees
                /// with `struct __kernel_timex` of the Linux UAPI.
                #[derive(Default)]
                #[repr(C)]
                pub(in super::super) struct Timex {
                    pub(in super::super) modes: c_uint,
                    pub(in super::super) offset: c_long,
                    pub(in super::super) freq: c_long,
                    /// The maximum error, in microseconds.
                    pub(in super::super) maxerror: c_long,
                    pub(in super::super) esterror: c_long,
                    pub(in super::super) status: c_int,
                    pub(in super::super) constant: c_long,
                    pub(in super::super) precision: c_long,
                    pub(in super::super) tolerance: c_long,
                    pub(in super::super) time: Timeval,
                    pub(in super::super) tick: c_long,
                    pub(in super::super) ppsfreq: c_long,
                    pub(in super::super) jitter: c_long,
                    pub(in super::super) shift: c_int,
                    pub(in super::super) stabil: c_long,
                    pub(in super::super) jitcnt: c_long,
                    pub(in super::super) calcnt: c_long,
                    pub(in super::super) errcnt: c_long,
                    pub(in super::super) stbcnt: c_long,
                    pub(in super::super) tai: c_int,
                    pub(in super::super) padding: [c_int; 11],
                }

                const _: () = {
                    assert!(size_of::<Timex>() == 208);
                    assert!(core::mem::offset_of!(Timex, maxerror) == 24);
                    assert!(core::mem::offset_of!(Timex, status) == 40);
                    assert!(core::mem::offset_of!(Timex, time) == 72);
                    assert!(core::mem::offset_of!(Timex, tai) == 160);
                };

                #[expect(unsafe_code, reason = "the C library of each Linux target gives this function")]
                unsafe extern "C" {
                    /// `int adjtimex(struct timex *buf)`. With `modes` 0 it only reads, and each user can
                    /// call it.
                    fn adjtimex(buf: *mut Timex) -> c_int;
                }

                /// One call gives the time and its maximum error together. `None` when the call fails,
                /// for example in a sandbox that refuses it.
                #[expect(unsafe_code, reason = "the call writes only the `timex` that it gets")]
                pub(super) fn read() -> Option<Sample> {
                    let mut timex = Timex::default();
                    // SAFETY: `timex` is a valid `struct timex`, and `modes` 0 makes the call only read.
                    let state = unsafe { adjtimex(&raw mut timex) };
                    (state != -1).then(|| decode(state, &timex))
                }

                /// The sample of the state and the `timex` that `adjtimex` gave. The kernel keeps a
                /// maximum error of 16 s while the clock is not synchronized, so the bound is unknown
                /// then.
                pub(in super::super) fn decode(state: c_int, timex: &Timex) -> Sample {
                    let nanos = timex.status & STA_NANO != 0;
                    let fraction = u64::try_from(timex.time.tv_usec).ok().map(|fraction| {
                        if nanos {
                            Duration::from_nanos(fraction)
                        } else {
                            Duration::from_micros(fraction)
                        }
                    });
                    let estimate = u64::try_from(timex.time.tv_sec)
                        .ok()
                        .zip(fraction)
                        .and_then(|(secs, fraction)| Duration::from_secs(secs).checked_add(fraction));
                    // A time in microseconds is up to 1 µs before the clock, so the bound adds 1 µs.
                    let truncation = if nanos {
                        Duration::ZERO
                    } else {
                        Duration::from_micros(1)
                    };
                    let synchronized = state != TIME_ERROR && timex.status & STA_UNSYNC == 0;
                    let bound = u64::try_from(timex.maxerror)
                        .ok()
                        .filter(|_| synchronized)
                        .and_then(|micros| Duration::from_micros(micros).checked_add(truncation));
                    Sample { estimate, bound }
                }
            }
        }
    }
    _ => {
        /// No physical-time source on this target.
        mod sys {
            use super::Sample;

            /// No value of this type exists.
            #[derive(Debug, Clone, Copy)]
            pub(super) enum Source {}

            impl Source {
                pub(super) fn new() -> Option<Self> {
                    None
                }

                pub(super) fn read(self) -> Sample {
                    match self {}
                }
            }
        }
    }
}
