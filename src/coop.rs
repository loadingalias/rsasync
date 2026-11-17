//! The poll budget of a task: the number of runtime leaves that become ready in one poll of the
//! task.
//!
//! A runtime leaf is a future of this crate that waits for the runtime: an I/O completion, a
//! channel send or receive, a permit acquisition, or a timer. Each leaf polls through [`poll`]. A
//! local owner gives each poll of a task, and of its root future, a budget with [`with_budget`].
//! Each leaf that becomes ready spends one unit. When no unit is left, the next leaf wakes its task
//! and gives `Pending` once, with no poll of its state, so the owner polls its other ready tasks
//! first. Thus a task that loops on leaves that are always ready yields its thread.
//!
//! A leaf that gives `Pending` spends nothing. Outside a poll of an owner, and in a build with no
//! `local` feature, a leaf has no budget. Code that uses no leaf is never stopped: the owner cannot
//! preempt a poll.

use core::task::{Context, Poll};

#[cfg(feature = "local")]
use core::cell::Cell;
#[cfg(feature = "local")]
use core::mem::DropGuard;

#[cfg(feature = "local")]
use crate::sync::thread_local;

#[cfg(all(feature = "local", not(all(test, loom))))]
thread_local! {
    /// The units that the poll of the task on this thread has left, or `None` outside a poll of
    /// an owner.
    static LEFT: Cell<Option<u32>> = const { Cell::new(None) };
}

// The loom form of the macro takes no `const` block.
#[cfg(all(feature = "local", test, loom))]
thread_local! {
    static LEFT: Cell<Option<u32>> = Cell::new(None);
}

/// Polls a runtime leaf with `poll`, if its task has a unit of its budget left. A leaf that becomes
/// ready spends one unit. With no unit left, it wakes the task and gives `Pending`.
#[inline]
pub(crate) fn poll<T>(cx: &mut Context<'_>, poll: impl FnOnce(&mut Context<'_>) -> Poll<T>) -> Poll<T> {
    #[cfg(feature = "local")]
    if LEFT.with(Cell::get) == Some(0) {
        cx.waker().wake_by_ref();
        return Poll::Pending;
    }
    let polled = poll(cx);
    #[cfg(feature = "local")]
    if polled.is_ready() {
        LEFT.with(|left| left.update(|left| left.map(|units| units.saturating_sub(1))));
    }
    polled
}

/// Calls `f` with a budget of `units` for the leaves that it polls, and then restores the budget
/// before it, also when `f` panics.
#[cfg(feature = "local")]
pub(crate) fn with_budget<R>(units: u32, f: impl FnOnce() -> R) -> R {
    let before = LEFT.with(|left| left.replace(Some(units)));
    let _restore = DropGuard::new(before, |before| LEFT.with(|left| left.set(before)));
    f()
}
