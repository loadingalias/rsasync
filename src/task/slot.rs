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
//! is a compare-exchange of the head with `Release`, and the pusher signals the home when the list
//! was empty: one signal for each batch. The owner takes the whole list with one swap with
//! `Acquire`. A push after that swap finds the list empty, so it signals the home. A signal stays
//! until the home takes it, so a park or a wait after the push returns.
//!
//! # The signal of the home
//!
//! The signal is the native one of the home and of the sender:
//!
//! - The home itself sends no signal. Its owner looks at the list and the root flag before it
//!   parks or waits.
//! - A home with no ring parks its thread, and the signal unparks it.
//! - A home with a ring waits in its ring. A thread whose owner has a ring puts the home in the
//!   outbox of that owner, which sends a message from its own ring after the poll (`ring`). Each
//!   other thread writes the interrupt descriptor of the home, which the wait polls in its ring.
//!
//! The owner of `block_on` gets its ring at its first operation that needs one, while a wake from
//! another thread can run. A wake that reads no ring unparks the thread, which then does not park
//! again. So the owner sets the ring, and then takes the list and the root flag with a
//! read-modify-write before it waits in the ring (`Shared::set_ring`). A push or a root wake before
//! that read-modify-write is in the order of the list or of the flag before it, so the owner sees
//! it. A push or a root wake after it reads its `Release` with `Acquire`, so it sees the ring. The
//! cell of the ring (`late`) publishes it with `Release`, so a wake that finds it with no such
//! order also sees it whole. When the owner ends, it detaches the ring: a kept waker then holds no
//! descriptor of it, and a later wake does nothing.
//!
//! # The free slots
//!
//! `vacant` counts the free slots that no spawn reserved. A spawn on the owner, and a factory spawn
//! from another thread, each take one before they take a slot, and the end of a task gives it back.
//! So the free list of the owner always has a slot for each reservation.

use alloc::boxed::Box;
use core::cell::Cell;

use crate::capacity::Permit;
use core::ptr;

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
use super::late::{Late, Read};
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
use crate::driver;
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

/// The native signal of the home of an owner: it ends the park or the wait of the home.
pub(super) struct Home {
    /// The thread of the owner, which parks while the owner has no ring.
    thread: thread::Thread,
    /// The ring of the driver of the owner, which the owner sets one time and detaches when it
    /// ends, so a kept waker holds no descriptor of it.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    ring: Late<Arc<driver::Home>>,
}

impl Home {
    /// The home of an owner on `thread`, which waits in `ring` if it has one.
    pub(super) fn new(
        thread: thread::Thread,
        #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))] ring: Option<
            Arc<driver::Home>,
        >,
    ) -> Self {
        Self {
            thread,
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            ring: Late::new(ring),
        }
    }
}

pub(super) struct Shared {
    words: Box<[AtomicU64]>,
    /// The next slot in the remote-ready list, for each slot in it.
    next: Box<[AtomicU32]>,
    /// The first slot in the remote-ready list, or `END`.
    remote: AtomicU32,
    /// The root future of `block_on` was woken.
    root: AtomicBool,
    /// The free slots that no spawn reserved.
    vacant: AtomicU32,
    /// The signal of the home, which a push to an empty list sends.
    home: Home,
    /// The charge of this part of the owner, which a waker can keep after the owner drops.
    _charge: Permit,
}

impl Shared {
    pub(super) fn new(words: Box<[AtomicU64]>, next: Box<[AtomicU32]>, home: Home, charge: Permit) -> Self {
        let slots = u32::try_from(words.len()).expect("an owner has at most 2^16 slots");
        Self {
            words,
            next,
            remote: AtomicU32::new(END),
            root: AtomicBool::new(false),
            vacant: AtomicU32::new(slots),
            home,
            _charge: charge,
        }
    }

    /// Reserves a free slot for a spawn, from any thread. Gives `false` if each free slot is
    /// reserved. The owner then takes a slot from its free list for the reservation.
    pub(super) fn reserve_slot(&self) -> bool {
        // Ordering: `Relaxed`: the count publishes no data. The free list of the owner has a slot
        // for each reservation, because the owner frees a slot before it gives its unit back.
        let mut vacant = self.vacant.load(Ordering::Relaxed);
        loop {
            let Some(next) = vacant.checked_sub(1) else {
                return false;
            };
            match self
                .vacant
                .compare_exchange_weak(vacant, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(actual) => vacant = actual,
            }
        }
    }

    /// Gives back the reservation of a slot: the owner freed the slot, or a spawn that reserved
    /// it did not take it.
    pub(super) fn release_slot(&self) {
        // Ordering: `Relaxed`, as in `reserve_slot`.
        self.vacant.fetch_add(1, Ordering::Relaxed);
    }

    /// Gives the home its ring, the first time the owner needs one. The next turn of the owner then
    /// takes the list and the root flag before it waits in the ring.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    #[cfg_attr(
        not(all(test, not(loom))),
        expect(dead_code, reason = "the local I/O types of R307 build on it")
    )]
    pub(super) fn set_ring(&self, home: Arc<driver::Home>) {
        self.home.ring.set(home);
    }

    /// Detaches the ring of the home when its owner ends: each later wake does nothing, and the
    /// descriptors of the ring close when the last message to it completes.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) fn detach_ring(&self) {
        self.home.ring.detach();
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

    /// Pushes `slot` to the remote-ready list, and signals the home if the list was empty.
    fn push(&self, slot: u32) {
        // Ordering: `Relaxed`: the compare-exchange below reads the latest head.
        let mut head = self.remote.load(Ordering::Relaxed);
        loop {
            // Ordering: `Relaxed`: the release of the change of the head publishes the link.
            self.next[slot as usize].store(head, Ordering::Relaxed);
            // Ordering: `AcqRel`. `Release` publishes the link and the change of the slot word to
            // the owner, which takes the list with `Acquire`. `Acquire` reads the release of that
            // take, so a push after it sees a ring that the owner set before it.
            match self
                .remote
                .compare_exchange_weak(head, slot, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => head = actual,
            }
        }
        if head == END {
            self.signal();
        }
    }

    /// Ends the park or the wait of the home, after a push to an empty list or a wake of the root.
    fn signal(&self) {
        let current = super::HOME.with(Cell::get);
        if ptr::eq(current, self) {
            return;
        }
        #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
        match self.home.ring.with(|home| {
            if current.is_null() || !super::ring::post(home) {
                home.interrupt();
            }
        }) {
            Read::Empty => self.home.thread.unpark(),
            // A detached home has no owner that waits.
            Read::Value(()) | Read::Detached => {}
        }
        #[cfg(not(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64"))))]
        self.home.thread.unpark();
    }

    /// Whether the remote-ready list has a slot. Only the owner calls it, before it parks: a push
    /// that it does not see found the list empty, and that push signals the home.
    pub(super) fn has_remote(&self) -> bool {
        // Ordering: `Relaxed`: the harvest takes the list with `Acquire`.
        self.remote.load(Ordering::Relaxed) != END
    }

    /// Takes each slot of the remote-ready list, and gives it to `each`. Only the owner calls it.
    pub(super) fn harvest(&self, mut each: impl FnMut(u32)) {
        // Ordering: `AcqRel`. `Acquire` reads the release of each push. `Release` orders a ring
        // that the owner set before this take before each later push, which reads it.
        let mut slot = self.remote.swap(END, Ordering::AcqRel);
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

    /// Wakes the root future of `block_on`. Only the wake that sets the flag signals the home.
    pub(super) fn wake_root(&self) {
        // Ordering: `AcqRel`. `Release` orders the event of the wake before the poll of the root,
        // which takes the flag with `Acquire`. `Acquire` reads the release of that take, as a push
        // reads the take of the list.
        if !self.root.swap(true, Ordering::AcqRel) {
            self.signal();
        }
    }

    /// Takes the flag of the root future. Gives `true` if it was woken.
    pub(super) fn take_root(&self) -> bool {
        // Ordering: `AcqRel`: `Acquire` reads the release of the wake, and `Release` orders a ring
        // that the owner set before this take before each later wake.
        self.root.swap(false, Ordering::AcqRel)
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
    /// The charge of this identity, which a waker or a cancel handle can keep after the task.
    pub(super) _charge: Permit,
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
