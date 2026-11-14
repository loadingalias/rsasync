// The tests need a physical-time source.
#![cfg(any(
    all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")),
    target_os = "macos",
    all(windows, any(target_arch = "x86_64", target_arch = "aarch64"))
))]

use core::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use super::PhysicalClock;
use crate::time::ReadingSource;

/// The civil time of std, which the tests compare with the readings.
fn civil_time() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

#[test]
fn a_caller_bound_is_the_bound_of_each_reading() {
    let bound = Duration::from_millis(20);
    let reading = PhysicalClock::new().unwrap().with_bound(bound).read();
    assert_eq!(
        (reading.bound(), reading.source()),
        (Some(bound), ReadingSource::Caller)
    );
    assert!(reading.estimate().abs_diff(civil_time()) < Duration::from_secs(1));
}

/// The test prints the reading, so a run with `--no-capture` records it.
#[test]
fn a_reading_of_the_system_is_the_civil_time_with_the_bound_of_the_system() {
    let reading = PhysicalClock::new().unwrap().read();
    std::println!("{reading:?}");
    assert_eq!(reading.source(), ReadingSource::SystemClock);
    assert!(reading.estimate().abs_diff(civil_time()) < Duration::from_secs(1));
    if cfg!(target_os = "linux") {
        // The kernel sets an unsynchronized state at a maximum error of 16 s.
        assert!(reading.bound().is_none_or(|bound| bound <= Duration::from_secs(16)));
    } else {
        assert_eq!(reading.bound(), None);
    }
}

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod kernel {
    use core::ffi::c_int;
    use core::future::Future;
    use core::num::{NonZeroU32, NonZeroU64};
    use core::pin::Pin;
    use core::task::{Context, Poll, Waker};
    use core::time::Duration;

    use super::super::sys::kernel::{Timeval, Timex, decode};
    use super::{PhysicalClock, civil_time};
    use crate::capacity::Budget;
    use crate::clock::NativeClock;
    use crate::time::{Deadline, Suspend};
    use crate::timer::Timers;

    const TIME_OK: c_int = 0;
    const TIME_ERROR: c_int = 5;
    const STA_PLL: c_int = 0x0001;
    const STA_UNSYNC: c_int = 0x0040;
    const STA_NANO: c_int = 0x2000;

    fn timex(status: c_int, maxerror: i64, tv_usec: i64) -> Timex {
        Timex {
            status,
            maxerror,
            time: Timeval {
                tv_sec: 1_800_000_000,
                tv_usec,
            },
            ..Timex::default()
        }
    }

    #[test]
    fn the_kernel_bound_is_its_maximum_error_only_while_the_clock_is_synchronized() {
        let secs = Duration::from_secs(1_800_000_000);
        // Microseconds: the bound adds the 1 µs of the truncated time.
        let micros = decode(TIME_OK, &timex(STA_PLL, 1_234, 567));
        assert_eq!(micros.estimate, Some(secs + Duration::from_micros(567)));
        assert_eq!(micros.bound, Some(Duration::from_micros(1_235)));
        let nanos = decode(TIME_OK, &timex(STA_PLL | STA_NANO, 1_234, 567_891));
        assert_eq!(nanos.estimate, Some(secs + Duration::from_nanos(567_891)));
        assert_eq!(nanos.bound, Some(Duration::from_micros(1_234)));
        // An unsynchronized clock keeps a maximum error of 16 s. It is not a bound.
        let unsynchronized = decode(TIME_ERROR, &timex(STA_UNSYNC, 16_000_000, 567));
        assert_eq!(unsynchronized.estimate, Some(secs + Duration::from_micros(567)));
        assert_eq!(unsynchronized.bound, None);
        assert_eq!(decode(TIME_OK, &timex(STA_UNSYNC, 1_234, 567)).bound, None);
    }

    /// `struct timespec` and `clock_settime` of `time.h` of glibc and musl.
    #[repr(C)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: i64,
    }

    #[expect(unsafe_code, reason = "the C library of each Linux target gives this function")]
    unsafe extern "C" {
        /// `int clock_settime(clockid_t clockid, const struct timespec *tp)`.
        fn clock_settime(clockid: c_int, tp: *const Timespec) -> c_int;
    }

    /// Sets the civil clock, `CLOCK_REALTIME`, to `time` since the Unix epoch.
    #[expect(unsafe_code, reason = "the call reads only the `timespec` that it gets")]
    fn set_civil_time(time: Duration) {
        let time = Timespec {
            tv_sec: i64::try_from(time.as_secs()).unwrap(),
            tv_nsec: i64::from(time.subsec_nanos()),
        };
        // SAFETY: `time` is a valid `timespec` for the call to read.
        assert_eq!(unsafe { clock_settime(0, &raw const time) }, 0, "clock_settime");
    }

    /// Steps the civil clock back when it drops, also after a failed assertion.
    struct StepBack(Duration);

    impl Drop for StepBack {
        fn drop(&mut self) {
            set_civil_time(civil_time().checked_sub(self.0).unwrap());
        }
    }

    #[test]
    #[ignore = "needs CAP_SYS_TIME: it steps the civil clock of the host"]
    fn a_step_of_the_civil_clock_moves_no_monotonic_timer() {
        let step = Duration::from_secs(60);
        let clock = NativeClock::new(Suspend::Pauses).unwrap();
        let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
        let one = NonZeroU32::new(1).unwrap();
        let mut timers = Timers::new(one, Duration::from_millis(1), clock.now(), &budget).unwrap();
        let deadline = clock.now().checked_add(Duration::from_secs(1)).unwrap();
        let mut timer = timers.insert(Deadline::at(deadline)).unwrap();

        set_civil_time(civil_time() + step);
        let _step_back = StepBack(step);
        // A set of the civil clock clears the synchronized state of the kernel.
        assert_eq!(PhysicalClock::new().unwrap().read().bound(), None);

        let fired = loop {
            let now = clock.now();
            timers.turn(now, 1).unwrap();
            if Pin::new(&mut timer).poll(&mut Context::from_waker(Waker::noop())) == Poll::Ready(Ok(())) {
                break now;
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        let late = fired
            .duration_since(deadline)
            .expect("the timer fired before its deadline");
        assert!(
            late <= Duration::from_millis(100),
            "the timer fired {late:?} after its deadline"
        );
    }
}
