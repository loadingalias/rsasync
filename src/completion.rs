//! The completion entry of one operation slot: the home of the operation writes its resources and
//! its result, and the consumer of the operation, on any thread, takes them.
//!
//! One word, `state`, gives each of the two its turn:
//!
//! | Phase | Owner of the resources | Next phase |
//! | --- | --- | --- |
//! | `RUNNING` | The home, which keeps them while the kernel can use them | `READY` at the terminal event; or the home takes them back at the terminal event when the consumer ended its interest |
//! | `READY` | The consumer, which takes them with a change to `TAKEN`; or the home, which takes them back with a change to `EMPTY` after the consumer ended its interest | `TAKEN`, `EMPTY`, or `CLOSED` |
//! | `TAKEN` | The consumer | The next operation of the slot |
//! | `CLOSED` | Nobody: the driver ended | None |
//!
//! The upper 32 bits of `state` hold the generation of the operation, so a consumer of another
//! generation takes nothing. The operation table keeps the slot of an operation until its
//! consumer ended its interest, so the next operation starts only after the consumer is done.
//!
//! Orderings: the home writes the resources and the result, then sets `READY` with a release. The
//! consumer's change from `READY` to `TAKEN` acquires that release, so it reads both. The home's
//! change from `READY` and the consumer's change to `TAKEN` are both read-modify-write changes
//! from the one value `READY`, so only one of the two succeeds, and only that one takes the
//! resources. The waiter of the entry gives the wake: the home sets `READY` before it wakes, and
//! the consumer rechecks after it registers ([`crate::wait`]).

#[cfg(all(test, loom))]
mod explore;

use core::mem::ManuallyDrop;
use core::task::{Context, Poll};

use crate::sync::UnsafeCell;
use crate::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use crate::table::OpId;
use crate::wait::Waiter;

const EMPTY: u64 = 0;
const RUNNING: u64 = 1;
const READY: u64 = 2;
const TAKEN: u64 = 3;
const CLOSED: u64 = 4;
const PHASE: u64 = 0xFF;

/// The driver ended before the operation gave its result to its consumer, so the consumer gets
/// none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Gone;

/// The completion entry of one slot, with the resources `T` of its operation.
pub(crate) struct Entry<T> {
    state: AtomicU64,
    result: AtomicI32,
    held: UnsafeCell<Option<T>>,
    waiter: Waiter,
}

fn word(id: OpId, phase: u64) -> u64 {
    u64::from(id.generation()) << 32 | phase
}

impl<T> Entry<T> {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU64::new(EMPTY),
            result: AtomicI32::new(0),
            held: UnsafeCell::new(None),
            waiter: Waiter::new(),
        }
    }

    /// The home starts the operation `id` of this slot with its resources, and gives `f` the
    /// resources in their place, for example to name their memory in a submission.
    #[expect(unsafe_code, reason = "the home owns the cell of a slot that has no consumer")]
    pub(crate) fn start<R>(&self, id: OpId, held: T, f: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: the table gave the slot to a new operation: the consumer of its last operation
        // took its resources or ended, and no consumer of `id` exists yet.
        let output = self.held.with_mut(|cell| f(unsafe { &mut *cell }.insert(held)));
        // Release: a consumer that sees `READY` later also sees these resources.
        self.state.store(word(id, RUNNING), Ordering::Release);
        output
    }

    /// The home changes the resources of `id` while the operation runs: it alone owns them then.
    #[expect(unsafe_code, reason = "the home owns the cell while the operation runs")]
    pub(crate) fn update(&self, id: OpId, f: impl FnOnce(&mut Option<T>)) {
        debug_assert_eq!(self.state.load(Ordering::Relaxed), word(id, RUNNING));
        // SAFETY: the phase is `RUNNING`, so only the home uses the cell, and only on its thread.
        self.held.with_mut(|cell| f(unsafe { &mut *cell }));
    }

    /// The home publishes the result of the terminal event of `id`, then wakes the consumer.
    pub(crate) fn publish(&self, id: OpId, result: i32) {
        self.result.store(result, Ordering::Relaxed);
        // Release: see the module docs.
        self.state.store(word(id, READY), Ordering::Release);
        self.waiter.wake();
    }

    /// After the terminal event of `id` and the end of interest of its consumer, the home takes
    /// back the resources and drops them. A consumer that took them first keeps them.
    pub(crate) fn reclaim(&self, id: OpId) {
        let state = self.state.load(Ordering::Relaxed);
        let ready = word(id, READY);
        let home = state == word(id, RUNNING)
            || (state == ready
                && self
                    .state
                    .compare_exchange(ready, word(id, EMPTY), Ordering::Acquire, Ordering::Relaxed)
                    .is_ok());
        if home {
            drop(self.take());
        }
    }

    /// The driver ends: no consumer takes anything from now on, and each waiting consumer wakes
    /// and gets [`Gone`]. The resources of a running operation leak, because the kernel can still
    /// use them.
    pub(crate) fn close(&self) {
        let state = self.state.swap(CLOSED, Ordering::Acquire);
        match state & PHASE {
            READY => drop(self.take()),
            RUNNING => {
                let _leaked = ManuallyDrop::new(self.take());
            }
            _ => {}
        }
        self.waiter.wake();
    }

    /// The consumer of `id` takes the result and the resources when they are ready, and otherwise
    /// registers the waker of `cx`.
    pub(crate) fn poll_take(&self, cx: &Context<'_>, id: OpId) -> Poll<Result<(i32, T), Gone>> {
        self.waiter.poll_wait(cx, || self.try_take(id))
    }

    /// The consumer of `id` ends its wait: no later wake reaches its waker.
    pub(crate) fn cancel_wait(&self) {
        self.waiter.cancel();
    }

    fn try_take(&self, id: OpId) -> Option<Result<(i32, T), Gone>> {
        let state = self.state.load(Ordering::Acquire);
        let ready = word(id, READY);
        if state == ready
            && self
                .state
                .compare_exchange(ready, word(id, TAKEN), Ordering::Acquire, Ordering::Acquire)
                .is_ok()
        {
            let result = self.result.load(Ordering::Relaxed);
            return Some(self.take().map(|held| (result, held)).ok_or(Gone));
        }
        (state != word(id, RUNNING)).then_some(Err(Gone))
    }

    /// Takes the resources out of the cell.
    #[expect(unsafe_code, reason = "the caller owns the cell by the phase that it changed")]
    fn take(&self) -> Option<T> {
        // SAFETY: each caller owns the cell: the home while `RUNNING`, and the one whose change
        // from `READY` succeeded.
        self.held.with_mut(|cell| unsafe { (*cell).take() })
    }
}

impl<T> core::fmt::Debug for Entry<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Entry")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

#[expect(unsafe_code, reason = "the phase word gives the cell to one owner at a time")]
// SAFETY: the cell holds a `T`, which is `Send`, and each access to it holds the phase that the
// module docs give: the home while `RUNNING`, and the one whose change from `READY` succeeded. The
// other fields are atomics and a waiter, which are `Sync`.
unsafe impl<T: Send> Sync for Entry<T> {}
