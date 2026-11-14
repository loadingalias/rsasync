use alloc::sync::Arc;
use alloc::task::Wake;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::{Context, Waker};

use super::Waiter;

/// A waker that counts its wakes. It drives the code under test, so it uses the native atomics.
pub(crate) struct Count(AtomicUsize);

impl Count {
    pub(crate) fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn counter() -> (Arc<Count>, Waker) {
    let count = Arc::new(Count(AtomicUsize::new(0)));
    (Arc::clone(&count), Waker::from(count))
}

#[test]
fn a_second_poll_wakes_only_its_latest_waker() {
    let waiter = Waiter::new();
    let (a, waker_a) = counter();
    let (b, waker_b) = counter();
    waiter.register(&waker_a);
    waiter.register(&waker_b);
    assert!(waiter.wake());
    assert_eq!((a.get(), b.get()), (0, 1));
}

#[test]
fn cancelling_one_waiter_leaves_the_other_registered() {
    let waiters = [Waiter::new(), Waiter::new()];
    let (a, waker_a) = counter();
    let (b, waker_b) = counter();
    waiters[0].register(&waker_a);
    waiters[1].register(&waker_b);
    assert!(waiters[0].cancel());
    // The publication wakes each registered waiter.
    let woken = waiters.iter().filter(|waiter| waiter.wake()).count();
    assert_eq!((woken, a.get(), b.get()), (1, 0, 1));
}

#[test]
fn a_pending_wait_is_woken_by_the_next_publication() {
    let waiter = Waiter::new();
    let ready = AtomicBool::new(false);
    let (count, waker) = counter();
    let context = Context::from_waker(&waker);
    let check = || ready.load(Ordering::Relaxed).then_some(());
    assert!(waiter.poll_wait(&context, check).is_pending());
    ready.store(true, Ordering::Relaxed);
    assert!(waiter.wake());
    assert_eq!(count.get(), 1);
    assert!(waiter.poll_wait(&context, check).is_ready());
}

/// A waker that, on its first wake, registers itself again in the waiter that woke it.
struct Reenter {
    waiter: Waiter,
    wakes: AtomicUsize,
    again: AtomicBool,
}

impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
        if !self.again.swap(true, Ordering::Relaxed) {
            self.waiter.register(&Waker::from(Arc::clone(self)));
        }
    }
}

#[test]
fn a_waker_that_enters_the_waiter_again_does_not_deadlock() {
    let reenter = Arc::new(Reenter {
        waiter: Waiter::new(),
        wakes: AtomicUsize::new(0),
        again: AtomicBool::new(false),
    });
    reenter.waiter.register(&Waker::from(Arc::clone(&reenter)));
    assert!(reenter.waiter.wake());
    // The registration from inside the wake is in the cell, so the next wake reaches it.
    assert!(reenter.waiter.wake());
    assert_eq!(reenter.wakes.load(Ordering::Relaxed), 2);
}

#[cfg(feature = "std")]
#[test]
fn a_blocking_wait_returns_after_a_publication_from_another_thread() {
    let shared = Arc::new((Waiter::new(), AtomicBool::new(false)));
    let other = Arc::clone(&shared);
    let waits = std::thread::spawn(move || other.0.wait(|| other.1.load(Ordering::Acquire).then_some(())));
    shared.1.store(true, Ordering::Release);
    shared.0.wake();
    waits.join().unwrap();
}
