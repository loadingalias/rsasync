//! The wait handshake: one waiter cell for each waiting endpoint or future of a channel, a permit
//! wait, or a one-shot.
//!
//! A wait registers interest, rechecks its condition, and only then parks. A publisher first
//! changes the condition and then wakes the waiter. Thus a publication can come before the
//! registration, between the registration and the recheck, or after the recheck, and in each case
//! the waiter sees it: the recheck sees the condition, or the wake comes.
//!
//! [`Waiter`] holds the waker of one waiter. A new registration replaces the earlier waker, so
//! a wake goes to the waker of the latest poll. Each waiter has its own cell, so the cancel of one
//! interest cannot clear the registration of another. A wake takes the waker out of the cell and
//! wakes it after the cell is free again, so no lock and no borrow is held while a waker runs, and
//! a waker that enters the primitive again does not deadlock. The handshake spins at most one
//! spin hint in each registration: a blocking wait rechecks one time after its registration and
//! then parks its thread. A registration that meets a publisher in its take wakes its own waiter,
//! which then repeats the handshake; the take holds no waker call, so it ends after a few
//! instructions.
//!
//! # Orderings
//!
//! One atomic word, `state`, orders each access to the cell. Its bits are `REGISTERING`, which
//! the owner of the waiter sets while it changes the cell, and `WAKING`, which a publisher sets
//! while it takes the waker. Each change of the word is a read-modify-write, so the changes have
//! one total order, and each change reads the change before it:
//!
//! - The owner takes the cell with an `Acquire` change from 0 to `REGISTERING`, and gives it back
//!   with a `Release` change to 0. A publisher takes the cell with an `AcqRel` change that sets
//!   `WAKING`, and gives it back with a `Release` change that clears it. Each change reads the
//!   change before it, so each taker reads the release of the last giver, and each access to the
//!   cell happens after the access before it.
//! - A publication that ends before the registration starts: the registration reads the release
//!   of the wake, so the change of the condition, which comes before the wake, happens before the
//!   recheck. The recheck sees it.
//! - A publication whose wake comes while the owner holds the cell: the owner's change back to 0
//!   fails on `WAKING`, so the owner wakes its own new waker. The owner then clears the word with
//!   an `AcqRel` swap, which reads the change of each publisher that came while it held the cell.
//!   A plain store would read none of them, and a later recheck could then miss a condition whose
//!   wake the owner absorbed.
//! - A publication whose wake starts after the registration: the wake reads the release of the
//!   registration, so it sees the new waker and wakes it.
//!
//! The condition itself needs no ordering of its own for this argument: the changes of `state`
//! carry it. A publisher must change the condition before it calls [`Waiter::wake`], and a waiter
//! must recheck the condition after [`Waiter::register`].
//!
//! The module has no documentation for users: it is private, and each channel, permit wait, and
//! one-shot documents its own waits.

use core::task::{Context, Poll, Waker};

#[cfg(feature = "std")]
use crate::sync::thread;
use crate::sync::{
    UnsafeCell,
    atomic::{AtomicUsize, Ordering},
    hint,
};

/// The owner changes the cell.
const REGISTERING: usize = 0b01;
/// A publisher takes the waker.
const WAKING: usize = 0b10;

/// The waker of one waiter, or of no waiter.
enum Wake {
    Task(Waker),
    #[cfg(feature = "std")]
    Thread(thread::Thread),
}

impl Wake {
    fn wake(self) {
        match self {
            Self::Task(waker) => waker.wake(),
            #[cfg(feature = "std")]
            Self::Thread(thread) => thread.unpark(),
        }
    }
}

/// The cell of one waiter. Only its owner registers and cancels; any thread can wake it.
pub(crate) struct Waiter {
    state: AtomicUsize,
    wake: UnsafeCell<Option<Wake>>,
}

impl Waiter {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            wake: UnsafeCell::new(None),
        }
    }

    /// Gives `f` the content of the cell.
    ///
    /// # Safety
    ///
    /// The caller holds the cell: it set `REGISTERING` or `WAKING` with the change that took it.
    #[expect(
        unsafe_code,
        reason = "the bit that the caller set gives it the only access to the cell"
    )]
    unsafe fn with_cell<R>(&self, f: impl FnOnce(&mut Option<Wake>) -> R) -> R {
        // SAFETY: the caller holds the cell, so no other access to it runs.
        self.wake.with_mut(|cell| f(unsafe { &mut *cell }))
    }

    /// Registers `waker` as the waker of this waiter, in place of an earlier one.
    pub(crate) fn register(&self, waker: &Waker) {
        self.put(|cell| match cell {
            Some(Wake::Task(old)) if old.will_wake(waker) => {}
            _ => *cell = Some(Wake::Task(waker.clone())),
        });
    }

    /// Registers the current thread as this waiter, in place of an earlier waker.
    #[cfg(feature = "std")]
    pub(crate) fn register_thread(&self) {
        self.put(|cell| *cell = Some(Wake::Thread(thread::current())));
    }

    /// Changes the cell with `f` while the owner holds it. If a publisher came meanwhile, wakes
    /// the new waker.
    #[expect(unsafe_code, reason = "each change of the cell holds the bit that takes it")]
    fn put(&self, f: impl FnOnce(&mut Option<Wake>)) {
        // Ordering: `Acquire` reads the release of the last holder of the cell (module docs).
        match self
            .state
            .compare_exchange(0, REGISTERING, Ordering::Acquire, Ordering::Acquire)
        {
            Ok(_) => {
                // SAFETY: the change to `REGISTERING` took the cell.
                unsafe { self.with_cell(f) };
                // Ordering: `Release` publishes the new waker to the next publisher.
                if self
                    .state
                    .compare_exchange(REGISTERING, 0, Ordering::Release, Ordering::Acquire)
                    .is_err()
                {
                    // A publisher set `WAKING` and left the cell to the owner. Wake the new
                    // waker, after the cell is free.
                    // SAFETY: the owner still holds the cell: the publisher did not take it.
                    let woken = unsafe { self.with_cell(Option::take) };
                    // Ordering: `Release` gives the cell back, as above. `Acquire` reads the
                    // change of each publisher that came meanwhile, so the owner sees their
                    // conditions.
                    self.state.swap(0, Ordering::AcqRel);
                    if let Some(wake) = woken {
                        wake.wake();
                    }
                }
            }
            // A publisher holds the cell and wakes the earlier waker, which can be of another
            // task. Wake this waiter too, and give the publisher time to end its take: the next
            // poll or park of the waiter repeats the handshake. The cell keeps the earlier waker
            // until the publisher takes it.
            Err(_) => {
                let mut fresh = None;
                f(&mut fresh);
                if let Some(wake) = fresh {
                    wake.wake();
                }
                hint::spin_loop();
            }
        }
    }

    /// Wakes the registered waker, and removes it. Gives `true` when it woke a waker. When it
    /// gives `false`, no waker was registered, or the owner was changing the cell and wakes its
    /// own new waker; a caller that wakes one waiter of several then wakes another.
    #[expect(unsafe_code, reason = "the change that sets `WAKING` takes the cell")]
    pub(crate) fn wake(&self) -> bool {
        // Ordering: `AcqRel` reads the release of the registration, and orders the change of the
        // condition before it for the next registration (module docs).
        if self.state.fetch_or(WAKING, Ordering::AcqRel) != 0 {
            return false;
        }
        // SAFETY: the change from 0 to `WAKING` took the cell.
        let woken = unsafe { self.with_cell(Option::take) };
        // Ordering: `Release` gives the cell back.
        self.state.fetch_and(!WAKING, Ordering::Release);
        match woken {
            Some(wake) => {
                wake.wake();
                true
            }
            None => false,
        }
    }

    /// Removes the interest of this waiter: no later wake reaches its waker. Gives `true` when
    /// it removed a registered waker, and `false` when a wake took the waker first. A waiter that
    /// gives up without using the publication that woke it then passes the wake to another
    /// waiter.
    #[expect(unsafe_code, reason = "the change to `REGISTERING` takes the cell")]
    pub(crate) fn cancel(&self) -> bool {
        // Ordering: as in `put`.
        if self
            .state
            .compare_exchange(0, REGISTERING, Ordering::Acquire, Ordering::Acquire)
            .is_err()
        {
            // A publisher holds the cell and takes the waker.
            return false;
        }
        // SAFETY: the change to `REGISTERING` took the cell.
        let removed = unsafe { self.with_cell(Option::take) };
        // A publisher that came meanwhile set `WAKING` and left: its wake is for an interest that
        // is now gone. Ordering: as in `put`.
        self.state.swap(0, Ordering::AcqRel);
        removed.is_some()
    }

    /// The handshake of an asynchronous wait: `ready` gives the result when the condition holds.
    /// It registers the waker of `cx`, rechecks, and gives `Pending` only after the recheck.
    pub(crate) fn poll_wait<T>(&self, cx: &Context<'_>, mut ready: impl FnMut() -> Option<T>) -> Poll<T> {
        if let Some(value) = ready() {
            return Poll::Ready(value);
        }
        self.register(cx.waker());
        match ready() {
            Some(value) => {
                self.cancel();
                Poll::Ready(value)
            }
            None => Poll::Pending,
        }
    }

    /// The handshake of a blocking wait: registers the current thread, rechecks, and parks until
    /// `ready` gives the result. A wake that this wait does not use can make a later park of the
    /// thread return at once, as the standard park permits.
    #[cfg(feature = "std")]
    pub(crate) fn wait<T>(&self, mut ready: impl FnMut() -> Option<T>) -> T {
        loop {
            if let Some(value) = ready() {
                return value;
            }
            self.register_thread();
            if let Some(value) = ready() {
                self.cancel();
                return value;
            }
            // A wake can come before the park: the park then returns at once.
            thread::park();
        }
    }
}

#[expect(unsafe_code, reason = "the state word orders each access to the cell")]
// SAFETY: the cell holds a `Waker` or a `Thread`, which are `Send`, and each access to it holds
// the bit that took it.
unsafe impl Send for Waiter {}

#[expect(unsafe_code, reason = "the state word orders each access to the cell")]
// SAFETY: as for `Send`: a shared `Waiter` gives access to the cell only through the state word.
unsafe impl Sync for Waiter {}

#[cfg(test)]
mod tests;

#[cfg(all(test, loom))]
mod explore;
