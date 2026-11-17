use alloc::sync::Arc;
use alloc::vec::Vec;
use core::assert_matches;
use core::future::Future;
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use super::{Closed, Invalid, Outcome, Refused, Timer, Timers};
use crate::capacity::Budget;
use crate::time::{ClockDomain, Deadline, Instant, Suspend, TimeError};
use crate::wait::tests::{Count, counter};

const DOMAIN: ClockDomain = ClockDomain::new(1, Suspend::Pauses);
const MS: u64 = 1_000_000;
/// A limit that no turn of these tests reaches.
const ALL: u32 = u32::MAX;
/// The ticks after the tick of the last turn that the storage takes, by its documentation.
const RANGE_TICKS: u64 = (1 << 36) - 1;

fn at(nanos: u64) -> Instant {
    Instant::new(DOMAIN, nanos)
}

fn deadline(nanos: u64) -> Deadline {
    Deadline::at(at(nanos))
}

fn budget() -> Budget {
    Budget::new(NonZeroU64::new(1 << 30).unwrap())
}

fn storage(capacity: u32, resolution: u64, now: u64) -> Timers {
    Timers::new(
        NonZeroU32::new(capacity).unwrap(),
        Duration::from_nanos(resolution),
        at(now),
        &budget(),
    )
    .unwrap()
}

fn poll(timer: &mut Timer, waker: &Waker) -> Poll<Result<(), Closed>> {
    Pin::new(timer).poll(&mut Context::from_waker(waker))
}

/// A timer that waits, with the waker of its last poll.
struct Waiting {
    deadline: u64,
    timer: Timer,
    woken: Arc<Count>,
    waker: Waker,
}

/// Inserts timers on each level of the wheel, at deadlines on and off the ticks, and polls each
/// one time. A xorshift generator with a fixed seed gives the deadlines.
fn insert_on_each_level(timers: &mut Timers, start: u64, resolution: u64) -> Vec<Waiting> {
    let first_tick = start.div_ceil(resolution) * resolution;
    let mut deadlines = Vec::from([start + 1, first_tick, first_tick + 1, timers.latest().as_nanos()]);
    let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
    for index in 0..240 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let ticks = (1_u64 << (6 * (index % 6 + 1))).min(RANGE_TICKS);
        deadlines.push(start + 1 + seed % (ticks * resolution));
    }
    deadlines
        .into_iter()
        .map(|nanos| {
            let mut timer = timers.insert(deadline(nanos)).unwrap();
            let (woken, waker) = counter();
            assert_eq!(poll(&mut timer, &waker), Poll::Pending);
            Waiting {
                deadline: nanos,
                timer,
                woken,
                waker,
            }
        })
        .collect()
}

/// Turns at `now`, and checks that no timer fired before its deadline, that each timer whose
/// deadline is at least `coarsening` before `now` fired, and that each fired timer woke its
/// consumer. Removes the fired timers.
fn turn_and_check(timers: &mut Timers, waiting: &mut Vec<Waiting>, now: u64, coarsening: u64) {
    assert!(!timers.turn(at(now), ALL).unwrap());
    waiting.retain_mut(|timer| {
        let fired = poll(&mut timer.timer, &timer.waker).is_ready();
        assert!(
            !fired || now >= timer.deadline,
            "a timer of deadline {} fired at {now}",
            timer.deadline
        );
        assert!(
            fired || now < timer.deadline + coarsening,
            "a timer of deadline {} did not fire at {now}",
            timer.deadline
        );
        assert!(
            !fired || timer.woken.get() > 0,
            "a fired timer did not wake its consumer"
        );
        !fired
    });
}

#[test]
fn no_timer_fires_before_its_deadline_and_each_fires_within_the_coarsening() {
    // A resolution that is not a power of two, and a start that is not on a tick.
    let resolution = MS + 3;
    let coarsening = resolution - 1;
    let start = 7 * resolution + 12_345;
    let mut timers = storage(256, resolution, start);
    assert_eq!(timers.max_coarsening(), Duration::from_nanos(coarsening));
    let mut waiting = insert_on_each_level(&mut timers, start, resolution);
    let mut turns: Vec<u64> = waiting
        .iter()
        .flat_map(|timer| [timer.deadline - 1, timer.deadline + coarsening])
        .collect();
    turns.sort_unstable();
    turns.dedup();
    for now in turns {
        turn_and_check(&mut timers, &mut waiting, now, coarsening);
    }
    assert!(waiting.is_empty());
}

#[test]
fn a_home_that_turns_at_each_next_turn_fires_each_timer_within_the_coarsening() {
    let resolution = MS;
    let coarsening = resolution - 1;
    let start = 3 * resolution + 1;
    let mut timers = storage(256, resolution, start);
    let mut waiting = insert_on_each_level(&mut timers, start, resolution);
    let mut turns = 0;
    while let Some(next) = timers.next_turn() {
        turn_and_check(&mut timers, &mut waiting, next.as_nanos(), coarsening);
        turns += 1;
        // Each timer fires at one turn and moves down at most five times.
        assert!(turns <= 6 * 244, "the turns do not end");
    }
    assert!(waiting.is_empty());
}

#[test]
fn due_timers_beyond_the_limit_fire_in_deadline_order_in_the_next_turns() {
    const LIMIT: u32 = 7;
    let mut timers = storage(64, MS, 0);
    // 40 deadlines on three levels of the wheel, one in each tick, inserted out of order.
    let mut waiting: Vec<(u64, Timer)> = (1..=40_u64)
        .map(|index| index * 17 % 40 + 1)
        .map(|index| {
            let nanos = index.pow(3) * MS - index % 3 * 1_000;
            (nanos, timers.insert(deadline(nanos)).unwrap())
        })
        .collect();
    // The turns reach the deadlines of the first 30, and of the two late inserts.
    let now = 30_u64.pow(3) * MS;
    let mut turns = 0;
    let mut fired = 0;
    loop {
        let more = timers.turn(at(now), LIMIT).unwrap();
        turns += 1;
        let (ready, rest): (Vec<_>, Vec<_>) = waiting
            .into_iter()
            .map(|(nanos, mut timer)| (nanos, poll(&mut timer, Waker::noop()).is_ready(), timer))
            .partition(|&(_, ready, _)| ready);
        waiting = rest.into_iter().map(|(nanos, _, timer)| (nanos, timer)).collect();
        let latest_fired = ready.iter().map(|&(nanos, ..)| nanos).max();
        let earliest_waiting = waiting.iter().map(|&(nanos, _)| nanos).min();
        fired += ready.len();
        assert!(ready.len() <= LIMIT as usize, "a turn fired {} timers", ready.len());
        assert!(latest_fired <= Some(now), "a timer fired before its deadline");
        assert!(
            latest_fired < earliest_waiting,
            "a timer fired before a timer of an earlier deadline"
        );
        if turns == 1 {
            // Two late inserts while due timers remain: one before the first tick that the storage
            // has not fired, and one between the ticks of two due timers.
            for nanos in [100 * MS - 1_000, 4_000 * MS - 2_000] {
                waiting.push((nanos, timers.insert(deadline(nanos)).unwrap()));
            }
        }
        assert_eq!(more, fired < 32);
        if !more {
            break;
        }
    }
    assert_eq!((turns, fired, waiting.len()), (5, 32, 10));
}

#[test]
fn a_past_deadline_fires_at_the_next_turn() {
    let now = 10 * MS + MS / 2;
    let mut timers = storage(4, MS, 0);
    assert!(!timers.turn(at(now), ALL).unwrap());
    // Long past, in the current tick before the last turn, and at the last turn.
    let mut past: Vec<Timer> = [0, now - MS / 4, now]
        .into_iter()
        .map(|nanos| timers.insert(deadline(nanos)).unwrap())
        .collect();
    assert_eq!(timers.next_turn(), Some(at(10 * MS)));
    // The next turn is in the same tick.
    assert!(!timers.turn(at(now + 1), ALL).unwrap());
    for timer in &mut past {
        assert_eq!(poll(timer, Waker::noop()), Poll::Ready(Ok(())));
    }
}

#[test]
fn a_full_storage_refuses_and_a_dropped_timer_frees_its_place() {
    let mut timers = storage(2, MS, 0);
    let waiting = timers.insert(deadline(5 * MS)).unwrap();
    let mut expiring = timers.insert(deadline(MS)).unwrap();
    assert_eq!(timers.insert(deadline(5 * MS)).unwrap_err(), Refused::Full);
    // A dropped timer that waits frees its place, with no turn.
    drop(waiting);
    let _replacement = timers.insert(deadline(5 * MS)).unwrap();
    assert_eq!(timers.insert(deadline(5 * MS)).unwrap_err(), Refused::Full);
    // An expired timer keeps its place until it drops.
    assert!(!timers.turn(at(MS), ALL).unwrap());
    assert_eq!(poll(&mut expiring, Waker::noop()), Poll::Ready(Ok(())));
    assert_eq!(timers.insert(deadline(5 * MS)).unwrap_err(), Refused::Full);
    drop(expiring);
    timers.insert(deadline(5 * MS)).unwrap();
}

#[test]
fn a_deadline_after_the_range_refuses_and_the_last_one_fires_on_time() {
    let start = 5_000 * MS + 300_000;
    let mut timers = storage(2, MS, start);
    let latest = (start / MS + RANGE_TICKS) * MS;
    assert_eq!(timers.latest(), at(latest));
    assert_eq!(timers.insert(deadline(latest + 1)).unwrap_err(), Refused::Range);
    // The last deadline is in the current slot of the top level, one turn of the ring ahead.
    let mut last = timers.insert(deadline(latest)).unwrap();
    assert!(!timers.turn(at(latest - 1), ALL).unwrap());
    assert_eq!(poll(&mut last, Waker::noop()), Poll::Pending);
    assert!(!timers.turn(at(latest), ALL).unwrap());
    assert_eq!(poll(&mut last, Waker::noop()), Poll::Ready(Ok(())));
}

#[test]
fn a_deadline_after_the_last_tick_of_the_domain_refuses_and_never_wraps() {
    let mut timers = storage(2, MS, u64::MAX - 10 * MS);
    let last_tick = u64::MAX / MS * MS;
    assert_eq!(timers.latest(), at(last_tick));
    assert_eq!(timers.insert(deadline(last_tick + 1)).unwrap_err(), Refused::Range);
    let mut last = timers.insert(deadline(last_tick)).unwrap();
    assert!(!timers.turn(at(last_tick - 1), ALL).unwrap());
    assert_eq!(poll(&mut last, Waker::noop()), Poll::Pending);
    assert!(!timers.turn(at(u64::MAX), ALL).unwrap());
    assert_eq!(poll(&mut last, Waker::noop()), Poll::Ready(Ok(())));
}

#[test]
fn close_ends_each_waiting_timer_and_cancel_tells_what_came_first() {
    let mut timers = storage(4, MS, 0);
    let mut expired = timers.insert(deadline(MS)).unwrap();
    let cancelled = timers.insert(deadline(5 * MS)).unwrap();
    let mut closed = timers.insert(deadline(5 * MS)).unwrap();
    let (woken, waker) = counter();
    assert_eq!(poll(&mut closed, &waker), Poll::Pending);
    assert!(!timers.turn(at(MS), ALL).unwrap());
    assert_eq!(poll(&mut expired, Waker::noop()), Poll::Ready(Ok(())));
    assert_eq!(cancelled.cancel(), Outcome::Cancelled);
    timers.close();
    assert_eq!(woken.get(), 1);
    assert_eq!(poll(&mut closed, &waker), Poll::Ready(Err(Closed)));
    assert_eq!(timers.insert(deadline(5 * MS)).unwrap_err(), Refused::Closed);
    assert_eq!(expired.cancel(), Outcome::Expired);
    assert_eq!(closed.cancel(), Outcome::Closed);
}

#[test]
fn a_dropped_storage_ends_each_waiting_timer_with_closed() {
    let mut timers = storage(1, MS, 0);
    let mut timer = timers.insert(deadline(5 * MS)).unwrap();
    let (woken, waker) = counter();
    assert_eq!(poll(&mut timer, &waker), Poll::Pending);
    drop(timers);
    assert_eq!(woken.get(), 1);
    assert_eq!(poll(&mut timer, &waker), Poll::Ready(Err(Closed)));
}

#[test]
fn an_instant_of_another_domain_changes_nothing() {
    let other = ClockDomain::new(2, Suspend::Advances);
    let mut timers = storage(1, MS, 0);
    assert_eq!(
        timers.insert(Deadline::at(Instant::new(other, MS))).unwrap_err(),
        Refused::Domain
    );
    let mut timer = timers.insert(deadline(MS)).unwrap();
    assert_eq!(
        timers.turn(Instant::new(other, 5 * MS), ALL),
        Err(TimeError::DomainMismatch)
    );
    assert_eq!(poll(&mut timer, Waker::noop()), Poll::Pending);
}

#[test]
fn the_storage_charges_its_memory_until_its_last_timer_drops() {
    let capacity = NonZeroU32::new(8).unwrap();
    let bound = 1 << 20;
    let budget = Budget::new(NonZeroU64::new(bound).unwrap());
    let memory = Timers::memory(capacity);
    let mut timers = Timers::new(capacity, Duration::from_nanos(MS), at(0), &budget).unwrap();
    assert_eq!(budget.available(), bound - memory);
    let timer = timers.insert(deadline(MS)).unwrap();
    drop(timers);
    assert_eq!(budget.available(), bound - memory);
    drop(timer);
    assert_eq!(budget.available(), bound);
    let small = Budget::new(NonZeroU64::new(memory - 1).unwrap());
    assert_matches!(
        Timers::new(capacity, Duration::from_nanos(MS), at(0), &small),
        Err(Invalid::Exhausted(_))
    );
}
