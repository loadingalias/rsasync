//! The waiting spawns of one local owner: an intrusive list of nodes in pinned futures, first in,
//! first out. Only the thread of the owner uses the list, so it needs no atomic and no lock, and
//! it allocates nothing: each node lives in its future.
//!
//! A future links its node before it returns `Pending`, and unlinks it before its drop. Thus each
//! node in the list is alive, and its address does not change while it is linked, because the
//! future is pinned.

use core::cell::Cell;
use core::marker::PhantomPinned;
use core::ptr;
use core::task::Waker;

/// The node of one waiting spawn.
pub(super) struct Node {
    waker: Cell<Option<Waker>>,
    previous: Cell<*const Node>,
    next: Cell<*const Node>,
    linked: Cell<bool>,
    /// A freed slot woke this node, and its future did not spawn yet.
    woken: Cell<bool>,
    _pinned: PhantomPinned,
}

impl Node {
    pub(super) fn new() -> Self {
        Self {
            waker: Cell::new(None),
            previous: Cell::new(ptr::null()),
            next: Cell::new(ptr::null()),
            linked: Cell::new(false),
            woken: Cell::new(false),
            _pinned: PhantomPinned,
        }
    }
}

pub(super) struct List {
    first: Cell<*const Node>,
    last: Cell<*const Node>,
}

impl List {
    pub(super) fn new() -> Self {
        Self {
            first: Cell::new(ptr::null()),
            last: Cell::new(ptr::null()),
        }
    }

    /// Makes `node` wait with `waker`: links it at the end of the list, or replaces its waker if it
    /// is linked.
    ///
    /// # Safety
    ///
    /// The node does not move, and it is unlinked with [`List::remove`] before it drops.
    #[expect(unsafe_code, reason = "each linked node is alive and pinned (module docs)")]
    pub(super) unsafe fn wait(&self, node: &Node, waker: &Waker) {
        node.waker.set(Some(waker.clone()));
        node.woken.set(false);
        if node.linked.replace(true) {
            return;
        }
        let last = self.last.replace(node);
        node.previous.set(last);
        node.next.set(ptr::null());
        if last.is_null() {
            self.first.set(node);
        } else {
            // SAFETY: a linked node is alive (module docs).
            unsafe { (*last).next.set(node) };
        }
    }

    /// Unlinks `node` if it is linked. Gives `true` if a freed slot woke it, and its future did not
    /// spawn yet: the caller that gives up then passes the wake on.
    #[expect(unsafe_code, reason = "each linked node is alive and pinned (module docs)")]
    pub(super) fn remove(&self, node: &Node) -> bool {
        if node.linked.replace(false) {
            let (previous, next) = (node.previous.get(), node.next.get());
            if previous.is_null() {
                self.first.set(next);
            } else {
                // SAFETY: a linked node is alive (module docs).
                unsafe { (*previous).next.set(next) };
            }
            if next.is_null() {
                self.last.set(previous);
            } else {
                // SAFETY: a linked node is alive (module docs).
                unsafe { (*next).previous.set(previous) };
            }
            node.waker.set(None);
        }
        node.woken.replace(false)
    }

    /// Wakes the first waiting spawn, after a slot became free.
    #[expect(unsafe_code, reason = "each linked node is alive and pinned (module docs)")]
    pub(super) fn wake_first(&self) {
        let first = self.first.get();
        if first.is_null() {
            return;
        }
        // SAFETY: a linked node is alive (module docs).
        let node = unsafe { &*first };
        let waker = node.waker.take();
        self.remove(node);
        node.woken.set(true);
        // The waker runs after the list is consistent, so a waker that enters the owner again
        // finds no broken link.
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
