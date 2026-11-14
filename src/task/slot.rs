//! The part of a local owner that each thread can change: the word of each task slot, the
//! remote-ready list, and the flag of the root future.
//!
//! # The slot word
//!
//! The word of a slot holds the generation of its task above `GENERATION_SHIFT`, and four bits:
//! `SCHEDULED` (the task has one entry in a ready queue), `RUNNING` (the owner polls it), `NOTIFIED`
//! (a wake came during the poll), and `CANCEL` (a cancel was requested). Each change of the word is
//! a read-modify-write, so the changes have one total order:
//!
//! - A wake or a cancel from any thread is one compare-exchange, which also checks the generation.
//!   A waker of an ended task, whose slot holds a later generation, changes nothing. A wake of an
//!   idle task sets `SCHEDULED`, and the waker that set it pushes the one queue entry. A wake during
//!   a poll sets `NOTIFIED`, and the end of the poll schedules the task again.
//! - The start of a poll changes `SCHEDULED` to `RUNNING` in one change, with `Acquire`, so the
//!   owner sees the state that the last poll left. It reads `CANCEL` in the same change: a
//!   cancelled task is dropped there, before its poll.
//! - The end of a poll clears `RUNNING`, with `Release`, and reads `NOTIFIED` and `CANCEL` in the
//!   same change. If either is set, it sets `SCHEDULED` again. Thus a wake or a cancel that comes
//!   during a poll is never lost, and the future never drops during its poll.
//! - The end of a task changes the word to the next generation with no bit.
//!
//! A load and a separate store would let a compare-exchange of another thread come between them.
//! A stale waker could then change the word of the next task, so each change is one
//! read-modify-write, as the design model requires (`models/task-lifecycle`).
//!
//! # The remote-ready list
//!
//! The list is a stack of slots, linked by one `next` index for each slot. The bit `SCHEDULED`
//! gives each slot at most one entry, so the list needs no allocation and has a fixed bound. A push
//! is a compare-exchange of the head with `Release`, and the pusher unparks the owner when the list
//! was empty: one unpark for each batch. The owner takes the whole list with one swap with
//! `Acquire`, before it parks. A push after that swap finds the list empty, so it unparks the
//! owner, and the token of the park makes a park after the push return.

use alloc::boxed::Box;

use crate::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    thread,
};

/// In a slot word: the task has one entry in a ready queue.
const SCHEDULED: u64 = 1;
/// In a slot word: the owner polls the task.
const RUNNING: u64 = 1 << 1;
/// In a slot word: a wake came during the poll.
const NOTIFIED: u64 = 1 << 2;
/// In a slot word: a cancel of the task was requested.
const CANCEL: u64 = 1 << 3;
/// The generation is above these bits.
const GENERATION_SHIFT: u32 = 8;

/// The link of a slot that ends the remote-ready list.
const END: u32 = u32::MAX;

pub(super) struct Shared {
    words: Box<[AtomicU64]>,
    /// The next slot in the remote-ready list, for each slot in it.
    next: Box<[AtomicU32]>,
    /// The first slot in the remote-ready list, or `END`.
    remote: AtomicU32,
    /// The root future of `block_on` was woken.
    root: AtomicBool,
    /// The thread of the owner, which a push to an empty list unparks.
    thread: thread::Thread,
}

impl Shared {
    pub(super) fn new(words: Box<[AtomicU64]>, next: Box<[AtomicU32]>) -> Self {
        Self {
            words,
            next,
            remote: AtomicU32::new(END),
            root: AtomicBool::new(false),
            thread: thread::current(),
        }
    }

    /// The generation of the task in `slot`, or of its next task if the slot is free. Only the
    /// owner calls it.
    pub(super) fn generation(&self, slot: u32) -> u64 {
        // Ordering: `Relaxed`: only the owner changes the generation.
        self.words[slot as usize].load(Ordering::Relaxed) >> GENERATION_SHIFT
    }

    /// Starts a new task in the free `slot`. The owner pushes its first entry to the local queue.
    pub(super) fn start(&self, slot: u32) {
        // Ordering: `Relaxed`: the owner gives the waker of the new task, with its generation, to
        // other threads only after this change, through its own synchronization. The change is a
        // read-modify-write, so it keeps the order of the word with each stale wake.
        self.words[slot as usize].fetch_or(SCHEDULED, Ordering::Relaxed);
    }

    /// Wakes the task of `generation` in `slot`, and requests its cancel if `cancel` is true. A
    /// task of another generation does not change.
    pub(super) fn wake(&self, slot: u32, generation: u64, cancel: bool) {
        let word = &self.words[slot as usize];
        let flag = if cancel { CANCEL } else { 0 };
        // Ordering: `Relaxed` for the first load: the compare-exchange below reads the latest word.
        let mut current = word.load(Ordering::Relaxed);
        loop {
            if current >> GENERATION_SHIFT != generation {
                return;
            }
            let next = if current & RUNNING != 0 {
                current | NOTIFIED | flag
            } else {
                current | SCHEDULED | flag
            };
            if next == current {
                return;
            }
            // Ordering: `AcqRel`: the change orders the event of the wake before the next poll,
            // which starts with `Acquire`, and it reads the end of the last poll.
            match word.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        if current & (RUNNING | SCHEDULED) == 0 {
            self.push(slot);
        }
    }

    /// Pushes `slot` to the remote-ready list, and unparks the owner if the list was empty.
    fn push(&self, slot: u32) {
        // Ordering: `Relaxed`: the compare-exchange below reads the latest head.
        let mut head = self.remote.load(Ordering::Relaxed);
        loop {
            // Ordering: `Relaxed`: the release of the change of the head publishes the link.
            self.next[slot as usize].store(head, Ordering::Relaxed);
            // Ordering: `Release` publishes the link and the change of the slot word to the owner,
            // which takes the list with `Acquire`.
            match self
                .remote
                .compare_exchange_weak(head, slot, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => head = actual,
            }
        }
        if head == END {
            self.thread.unpark();
        }
    }

    /// Takes each slot of the remote-ready list, and gives it to `each`. Only the owner calls it.
    pub(super) fn harvest(&self, mut each: impl FnMut(u32)) {
        // Ordering: `Acquire` reads the release of each push.
        let mut slot = self.remote.swap(END, Ordering::Acquire);
        while slot != END {
            // Ordering: `Relaxed`: the swap above read the release of the push of this link. The
            // slot stays scheduled, so no push changes the link before its poll.
            let next = self.next[slot as usize].load(Ordering::Relaxed);
            each(slot);
            slot = next;
        }
    }

    /// Starts a poll of the scheduled task in `slot`. Gives `true` if a cancel was requested: the
    /// owner then drops the future, and does not poll it.
    pub(super) fn poll_start(&self, slot: u32) -> bool {
        // Ordering: `Acquire` reads the release of the end of the last poll and of each wake. The
        // task is scheduled and not running, so the change clears one bit and sets the other.
        let old = self.words[slot as usize].fetch_xor(SCHEDULED | RUNNING, Ordering::Acquire);
        old & CANCEL != 0
    }

    /// Ends a poll of the task in `slot` that gave `Pending`. Gives `true` if a wake or a cancel
    /// came during the poll: the task is then scheduled again, and the owner pushes its entry to
    /// the local queue.
    pub(super) fn poll_end(&self, slot: u32) -> bool {
        let word = &self.words[slot as usize];
        // Ordering: `Relaxed` for the first load, as in `wake`.
        let mut current = word.load(Ordering::Relaxed);
        loop {
            let again = current & (NOTIFIED | CANCEL) != 0;
            let mut next = current & !(RUNNING | NOTIFIED);
            if again {
                next |= SCHEDULED;
            }
            // Ordering: `Release` orders the poll before the next wake, which reads this change.
            match word.compare_exchange_weak(current, next, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => return again,
                Err(actual) => current = actual,
            }
        }
    }

    /// Ends the task in `slot`: the slot is free, with the next generation. Each waker of the
    /// ended task then changes nothing.
    pub(super) fn end(&self, slot: u32) {
        let word = &self.words[slot as usize];
        // Ordering: `Relaxed` for the first load, as in `wake`.
        let mut current = word.load(Ordering::Relaxed);
        loop {
            let next = ((current >> GENERATION_SHIFT) + 1) << GENERATION_SHIFT;
            // Ordering: `Release` orders the end of the task before a later read of the word. The
            // change is a read-modify-write: a wake between a load and a store would be lost in
            // the order of the word, and it could then change the next task.
            match word.compare_exchange_weak(current, next, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Wakes the root future of `block_on`.
    pub(super) fn wake_root(&self) {
        // Ordering: `Release` orders the event of the wake before the poll of the root, which takes
        // the flag with `Acquire`.
        self.root.store(true, Ordering::Release);
        self.thread.unpark();
    }

    /// Takes the flag of the root future. Gives `true` if it was woken.
    pub(super) fn take_root(&self) -> bool {
        // Ordering: `Acquire` reads the release of the wake.
        self.root.swap(false, Ordering::Acquire)
    }

    /// Whether the root future was woken, and no poll took the flag yet.
    pub(super) fn root_woken(&self) -> bool {
        // Ordering: `Relaxed`: the owner takes the flag with `Acquire` before it polls the root.
        self.root.load(Ordering::Relaxed)
    }
}

/// The identity of one task, for its waker and its cancel handle.
pub(super) struct TaskRef {
    pub(super) shared: Arc<Shared>,
    pub(super) slot: u32,
    pub(super) generation: u64,
}

impl alloc::task::Wake for TaskRef {
    fn wake(self: alloc::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &alloc::sync::Arc<Self>) {
        self.shared.wake(self.slot, self.generation, false);
    }
}

/// The waker of the root future of `block_on`.
pub(super) struct RootRef(pub(super) Arc<Shared>);

impl alloc::task::Wake for RootRef {
    fn wake(self: alloc::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &alloc::sync::Arc<Self>) {
        self.0.wake_root();
    }
}
