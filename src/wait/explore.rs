//! The named risk: a publication that races a registration, a cancel, or a replacement of the
//! waker is lost. A lost wake shows as a waiter that saw no condition at its recheck and whose
//! latest waker no wake reached.

use core::task::{Context, Poll};

use super::Waiter;
use super::tests::counter;
use crate::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Checks `f` in each thread order within a preemption bound of 3.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

/// A waiter and its condition. The condition uses `Relaxed`: the state word of the waiter
/// orders it.
struct Shared {
    waiter: Waiter,
    ready: AtomicBool,
}

fn shared() -> Arc<Shared> {
    Arc::new(Shared {
        waiter: Waiter::new(),
        ready: AtomicBool::new(false),
    })
}

/// Publishes on another thread: changes the condition, then wakes.
fn publish(shared: &Arc<Shared>) -> loom::thread::JoinHandle<()> {
    let shared = Arc::clone(shared);
    loom::thread::spawn(move || {
        shared.ready.store(true, Ordering::Relaxed);
        shared.waiter.wake();
    })
}

#[test]
fn a_publication_that_races_a_registration_is_not_lost() {
    model(|| {
        let shared = shared();
        let (count, waker) = counter();
        let publisher = publish(&shared);
        let polled = shared.waiter.poll_wait(&Context::from_waker(&waker), || {
            shared.ready.load(Ordering::Relaxed).then_some(())
        });
        publisher.join().unwrap();
        assert!(polled.is_ready() || count.get() == 1);
    });
}

#[test]
fn two_publications_that_race_a_registration_are_both_seen() {
    model(|| {
        let shared = shared();
        let second = Arc::new(AtomicBool::new(false));
        let (count, waker) = counter();
        let context = Context::from_waker(&waker);
        let ready = || (shared.ready.load(Ordering::Relaxed) && second.load(Ordering::Relaxed)).then_some(());
        let first_publisher = publish(&shared);
        let second_publisher = {
            let (shared, second) = (Arc::clone(&shared), Arc::clone(&second));
            loom::thread::spawn(move || {
                second.store(true, Ordering::Relaxed);
                shared.waiter.wake();
            })
        };
        // The executor polls the task again after each wake that it sees.
        let mut seen = 0;
        let mut polled = shared.waiter.poll_wait(&context, ready);
        while polled.is_pending() && count.get() > seen {
            seen = count.get();
            polled = shared.waiter.poll_wait(&context, ready);
        }
        first_publisher.join().unwrap();
        second_publisher.join().unwrap();
        assert!(polled.is_ready() || count.get() > seen);
    });
}

#[test]
fn a_publication_that_races_a_replacement_reaches_the_latest_waker() {
    model(|| {
        let shared = shared();
        let (_, first) = counter();
        let (latest, second) = counter();
        let ready = || shared.ready.load(Ordering::Relaxed).then_some(());
        let publisher = publish(&shared);
        let polled = match shared.waiter.poll_wait(&Context::from_waker(&first), ready) {
            Poll::Ready(()) => Poll::Ready(()),
            Poll::Pending => shared.waiter.poll_wait(&Context::from_waker(&second), ready),
        };
        publisher.join().unwrap();
        assert!(polled.is_ready() || latest.get() == 1);
    });
}

#[test]
fn a_publication_that_races_a_cancel_reaches_another_waiter() {
    model(|| {
        // The publication wakes one of two waiters. Waiter 0 gives up concurrently.
        let waiters = Arc::new([Waiter::new(), Waiter::new()]);
        let (_, waker_0) = counter();
        let (one, waker_1) = counter();
        waiters[0].register(&waker_0);
        waiters[1].register(&waker_1);
        let publisher = {
            let waiters = Arc::clone(&waiters);
            loom::thread::spawn(move || {
                if !waiters[0].wake() {
                    waiters[1].wake();
                }
            })
        };
        // A waiter that a wake reached, and that gives up, passes the wake on.
        if !waiters[0].cancel() {
            waiters[1].wake();
        }
        publisher.join().unwrap();
        assert!(one.get() >= 1);
    });
}
