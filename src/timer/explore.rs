//! The named risk: a fire of the home that races a poll and a drop of the timer on another thread
//! loses the wake, or the home gives the place of the timer to a new timer while the old consumer
//! still uses the waiter cell. Loom reports the second as a data race on the cell.

use core::future::Future;
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use super::Timers;
use crate::capacity::Budget;
use crate::time::{ClockDomain, Deadline, Instant, Suspend};
use crate::wait::tests::counter;

const DOMAIN: ClockDomain = ClockDomain::new(1, Suspend::Pauses);

fn deadline(nanos: u64) -> Deadline {
    Deadline::at(Instant::new(DOMAIN, nanos))
}

/// Checks `f` in each thread order within a preemption bound of 3.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

/// A storage of one timer, with ticks of 1 ns, at the instant 0.
fn storage() -> Timers {
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let one = NonZeroU32::new(1).unwrap();
    Timers::new(one, Duration::from_nanos(1), Instant::new(DOMAIN, 0), &budget).unwrap()
}

#[test]
fn a_fire_that_races_a_poll_wakes_the_consumer() {
    model(|| {
        let mut timers = storage();
        let mut timer = timers.insert(deadline(1)).unwrap();
        let consumer = loom::thread::spawn(move || {
            let (woken, waker) = counter();
            let polled = Pin::new(&mut timer).poll(&mut Context::from_waker(&waker));
            (polled, woken, timer)
        });
        timers.turn(Instant::new(DOMAIN, 1), 1).unwrap();
        let (polled, woken, _timer) = consumer.join().unwrap();
        assert!(
            polled.is_ready() || woken.get() == 1,
            "the fire did not wake the consumer"
        );
    });
}

#[test]
fn a_place_goes_to_a_new_timer_only_after_the_drop_of_its_old_timer() {
    model(|| {
        let mut timers = storage();
        let mut timer = timers.insert(deadline(1)).unwrap();
        let consumer = loom::thread::spawn(move || {
            // The poll registers a waker in the cell, before or after the fire.
            let _ready = Pin::new(&mut timer).poll(&mut Context::from_waker(Waker::noop()));
            drop(timer);
        });
        timers.turn(Instant::new(DOMAIN, 1), 1).unwrap();
        // The insert takes the place when it sees the drop. The new timer waits.
        if let Ok(mut again) = timers.insert(deadline(5)) {
            let polled = Pin::new(&mut again).poll(&mut Context::from_waker(Waker::noop()));
            assert_eq!(polled, Poll::Pending);
        }
        consumer.join().unwrap();
    });
}
