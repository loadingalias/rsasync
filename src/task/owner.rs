//! The part of a local owner that only its thread uses: the table of tasks, the free slots, the
//! local ready queue, and the loop that polls the ready tasks.
//!
//! Each event of a task has one code point, for the instrumentation hook: the spawn is in
//! `Local::spawn`, the start and the end of a poll are in `Local::poll_slot`, and the completion,
//! the cancel, and the contained panic end in `Local::finish`. The choice of the next task is
//! `Local::next_ready`.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::alloc::AllocError;
use core::any::Any;
use core::cell::{Cell, RefCell};
use core::future::Future;
use core::mem::MaybeUninit;
use core::panic::AssertUnwindSafe;
use core::pin::{Pin, pin};
use core::task::{Context, Poll, Waker};
use std::panic::catch_unwind;

use super::slot::{Home, RootRef, Shared, TaskRef};
use super::waiting::List;
use super::{CancelHandle, JoinHandle, Outcome, Panic, Reason, Refused, SpawnRefused, Target};
use crate::capacity::{Budget, Permit};
use crate::channel::oneshot::{self, Staged};
use crate::clock::{NativeClock, Until};
use crate::coop;
use crate::memory::{items, room};
use crate::sync::{
    Arc, arc_size,
    atomic::{AtomicU32, AtomicU64},
    thread, try_arc,
};
use crate::time::TimeError;
use crate::timer::Timers;

/// The largest number of task slots of one owner.
pub(crate) const MAX_SLOTS: u32 = 1 << 16;
/// The most timers that one turn of an owner fires. The others fire in the turns that follow.
const TIMERS_PER_TURN: u32 = 64;

/// Why a task ends with no output.
pub(super) enum End {
    Cancelled,
    Panicked(Box<dyn Any + Send>),
}

/// A spawned future and the sender of its outcome, behind one pointer.
///
/// A task that ends gives its outcome staged ([`Staged`]): its owner frees the memory, the charge,
/// and the slot of the task, and then drops the outcome, which shows it to the join handle. So a
/// spawn after a join finds the slot and the charge of the joined task free.
pub(super) trait Task {
    /// Polls the future. When it gives its output, drops the future, and gives `Ready` with the
    /// outcome, or with `None` when the task had ended.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Staged>>;

    /// Ends the task with no output: drops the future, and gives the outcome.
    fn end(self: Pin<&mut Self>, end: End) -> Option<Staged>;
}

/// The fields drop in their order: a cell that drops with its sender, at a failure of the owner,
/// gives its charge back before its channel closes.
pub(super) struct TaskCell<F: Future> {
    /// The future, pinned in the cell. It drops in place, at the end of the task.
    pub(super) future: Option<F>,
    pub(super) _charge: Permit,
    pub(super) sender: Option<oneshot::Sender<Outcome<F::Output>>>,
}

impl<F: Future> TaskCell<F> {
    /// The pinned future, for a pinned cell.
    #[expect(unsafe_code, reason = "the cell never moves its future: the future drops in place")]
    fn future(self: Pin<&mut Self>) -> Pin<&mut Option<F>> {
        // SAFETY: the future is pinned with the cell. The cell moves no field out of it, and it
        // ends the future with `Pin::set`, which drops it in place.
        unsafe { self.map_unchecked_mut(|cell| &mut cell.future) }
    }

    /// Stages `outcome` in the channel of the join handle. A join handle that dropped leaves the
    /// outcome to the drop of the [`Staged`].
    #[expect(unsafe_code, reason = "the sender is not pinned: the cell moves it out")]
    fn stage(self: Pin<&mut Self>, outcome: Outcome<F::Output>) -> Option<Staged>
    where
        F::Output: 'static,
    {
        // SAFETY: the sender is not structurally pinned, and this moves only the sender.
        let sender = unsafe { &mut self.get_unchecked_mut().sender };
        match sender.take().map(|sender| sender.stage(outcome)) {
            Some(Ok(staged)) => Some(staged),
            Some(Err(unjoined)) => {
                discard(unjoined);
                None
            }
            None => None,
        }
    }
}

impl<F: Future> Task for TaskCell<F>
where
    F::Output: 'static,
{
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Staged>> {
        let mut future = self.as_mut().future();
        let Some(running) = future.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };
        let Poll::Ready(output) = running.poll(cx) else {
            return Poll::Pending;
        };
        // A panic of the drop of the future makes the task panicked, and its output drops.
        Poll::Ready(match catch_unwind(AssertUnwindSafe(|| future.set(None))) {
            Ok(()) => self.stage(Outcome::Completed(output)),
            Err(payload) => {
                discard(output);
                self.stage(Outcome::Panicked(Panic(payload)))
            }
        })
    }

    fn end(mut self: Pin<&mut Self>, end: End) -> Option<Staged> {
        // A panic of the drop of the future makes a cancelled task panicked. A task that panicked
        // already keeps the payload of its poll.
        let dropped = catch_unwind(AssertUnwindSafe(|| self.as_mut().future().set(None)));
        let outcome = match (end, dropped) {
            (End::Cancelled, Ok(())) => Outcome::Cancelled,
            (End::Cancelled, Err(payload)) => Outcome::Panicked(Panic(payload)),
            (End::Panicked(payload), dropped) => {
                if let Err(second) = dropped {
                    discard(second);
                }
                Outcome::Panicked(Panic(payload))
            }
        };
        self.stage(outcome)
    }
}

/// Drops `value`, which holds code of a task, such as an output or a panic payload. A panic of
/// that drop is contained, and its payload drops in the same way. The payload of a panic of that
/// second drop leaks: one allocation, so that the owner continues.
pub(super) fn discard<T>(value: T) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value)))
        && let Err(second) = catch_unwind(AssertUnwindSafe(|| drop(payload)))
    {
        Box::leak(second);
    }
}

/// A live task in its slot.
struct Entry {
    task: Pin<Box<dyn Task>>,
    waker: Waker,
}

/// How the home of an owner does its other work, and how it waits for a wake.
pub(super) trait Park {
    type Error;

    /// Does the other work of the home with no wait. Gives `true` if work remains for the next
    /// turn. With [`Next::Wait`], a home that waits in its driver leaves the pushed work to the
    /// wait, which submits it.
    fn drive(&mut self, local: &Local, next: Next) -> Result<bool, Self::Error>;

    /// Gives the failure of the home that a poll or a destructor stored, such as a driver that did
    /// not start at its first use. A home that cannot fail stores none.
    fn check(&mut self, _local: &Local) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Waits until a signal of the home comes, or until the instant of `until` at the latest.
    fn park(&mut self, local: &Local, until: Option<Until>) -> Result<(), Self::Error>;
}

/// What the owner does after a drive of its home.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Next {
    /// It polls a ready task, fires a timer, or polls the root, so the drive submits now.
    Work,
    /// It has nothing ready, so it parks when the drive finds no work either.
    Wait,
}

/// A home with no ring: it has no other work, and it parks its thread.
pub(super) struct Thread;

impl Park for Thread {
    type Error = !;

    fn drive(&mut self, _local: &Local, _next: Next) -> Result<bool, Self::Error> {
        Ok(false)
    }

    fn park(&mut self, _local: &Local, until: Option<Until>) -> Result<(), Self::Error> {
        match until {
            // The verification form has no timed park, and its models make no timer.
            #[cfg(not(all(test, loom)))]
            Some(until) => thread::park_timeout(until.remaining()),
            _ => thread::park(),
        }
        Ok(())
    }
}

/// The charge of the identity of a task: the reference of its waker, in its `Arc`.
const IDENTITY: usize = arc_size::<TaskRef>();

/// The charge of a task of `F`: its cell, its outcome, and its identity. A size too large for a
/// budget saturates, and the budget refuses it.
pub(super) fn task_size<F: Future>() -> u64 {
    (size_of::<TaskCell<F>>() as u64)
        .saturating_add(oneshot::size::<Outcome<F::Output>>() as u64)
        .saturating_add(IDENTITY as u64)
}

/// The memory of the cell and the identity of a task of `F`. A spawn allocates it before the
/// future moves, so that a refusal of the allocator can give the future back. Neither holds a
/// value until `Local::place` writes it.
pub(super) struct TaskMemory<F: Future> {
    cell: Box<MaybeUninit<TaskCell<F>>>,
    identity: alloc::sync::Arc<MaybeUninit<TaskRef>>,
}

impl<F: Future> TaskMemory<F> {
    pub(super) fn try_new() -> Result<Self, AllocError> {
        Ok(Self {
            cell: Box::try_new_uninit()?,
            identity: alloc::sync::Arc::try_new_uninit()?,
        })
    }
}

#[expect(unsafe_code, reason = "the memory of a task holds no value until its home writes it")]
#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "the cell holds no value of `F` while it can move between threads"
)]
// SAFETY: the cell holds no value of `F` until `Local::place` writes it, on the thread of the
// owner, and the identity is `Send`. A move or a drop on a different thread moves or frees only
// the memory.
unsafe impl<F: Future> Send for TaskMemory<F> {}

/// Writes `value` into `memory`, a new `Arc` with one count.
#[expect(unsafe_code, reason = "the write initializes the value of the new `Arc`")]
pub(super) fn init<T>(mut memory: alloc::sync::Arc<MaybeUninit<T>>, value: T) -> alloc::sync::Arc<T> {
    alloc::sync::Arc::get_mut(&mut memory)
        .expect("the memory of a task is a new `Arc`, with one count")
        .write(value);
    // SAFETY: the write above initialized the value.
    unsafe { memory.assume_init() }
}

/// The settings of the driver that an owner makes at its first operation that needs a home
/// (`RingOwner::lazy`): its limits, the blocking service, and the provided buffers, which the budget
/// charges then.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub(super) struct Lazy {
    pub(super) config: crate::driver::Config,
    pub(super) blocking: Option<crate::blocking::Submitter>,
    pub(super) buffers: Option<(crate::runtime::ReceiveBuffers, crate::capacity::Budget)>,
}

pub(super) struct Local {
    shared: Arc<Shared>,
    pub(super) budget: Budget,
    entries: Box<[Cell<Option<Entry>>]>,
    free: RefCell<Vec<u32>>,
    /// The local ready queue. It holds at most one entry for each slot.
    queue: RefCell<VecDeque<u32>>,
    /// The homes with a ring that a wake on this thread signals with a message from the ring of
    /// this owner (`ring`). It has no room if this home can have no ring.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) outbox: RefCell<Vec<Arc<crate::driver::Home>>>,
    /// The driver of this owner, the home of the operations of its tasks (`ring`).
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) driver: RefCell<Option<crate::driver::Driver>>,
    /// The limits of the driver that this owner makes at the first operation that needs a home,
    /// if it has no driver.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) lazy: Cell<Option<Lazy>>,
    /// The live tasks that the end of the last `block_on` cancelled.
    pub(super) cancelled: Cell<u32>,
    /// The owner has a driver, so the messages of its outbox go out at its next turn or wait.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) sends: Cell<bool>,
    /// The home got its ring after the last turn, so the next park returns at once: the turn
    /// after it takes the list and the root flag before a wait in the ring (`slot`).
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) switched: Cell<bool>,
    /// The error of a driver that did not start, which fails the owner at its next turn (`ring`).
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) failure: Cell<Option<crate::driver::Error>>,
    /// The generator of this owner, and the index of its next child (`task::generator`).
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) generator: RefCell<Option<(rscrypto::ChaCha20Rng, u64)>>,
    live: Cell<u32>,
    /// The end of `block_on` refuses new spawns.
    closed: Cell<bool>,
    /// The waker of the root future of each `block_on`, which `new` makes once.
    root: Waker,
    /// The poll budget of each poll of a task or of the root (`crate::coop`).
    pub(super) poll_budget: Cell<u32>,
    /// The timer storage of the owner and the clock of its domain, from `use_timers`.
    pub(super) timers: RefCell<Option<(Timers, NativeClock)>>,
    pub(super) waiting: List,
    /// The task that the owner polls, with the data and the vtable of its own waker.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    polling: Cell<Option<Polling>>,
    _charge: Permit,
}

/// The task that an owner polls: a home operation that the task's own waker polls wakes the task
/// directly ([`crate::driver::LocalTask`]).
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
#[derive(Clone, Copy)]
struct Polling {
    task: crate::driver::LocalTask,
    data: *const (),
    vtable: *const core::task::RawWakerVTable,
}

impl Local {
    /// Makes the tables of an owner with `slots` task slots, and an outbox with room for `outbox`
    /// homes: zero for a home that never has a ring.
    pub(super) fn new(budget: &Budget, slots: u32, home: Home, outbox: usize) -> Result<Self, Refused> {
        if slots == 0 {
            return Err(Refused::Zero);
        }
        if slots > MAX_SLOTS {
            return Err(Refused::TooLarge);
        }
        // Each slot has a word, a link, an entry, a place in the free list, and a place in the
        // queue. Each place in the outbox is one pointer.
        let slot_size =
            size_of::<AtomicU64>() + size_of::<AtomicU32>() + size_of::<Cell<Option<Entry>>>() + 2 * size_of::<u32>();
        let size = slot_size
            .checked_mul(slots as usize)
            .and_then(|size| size.checked_add(arc_size::<Shared>() + arc_size::<Self>() + arc_size::<RootRef>()))
            .and_then(|size| size.checked_add(outbox * size_of::<usize>()))
            .and_then(|size| u64::try_from(size).ok())
            .ok_or(Refused::TooLarge)?;
        let mut charge = budget.reserve(size).map_err(Refused::Budget)?;
        // The shared part, which a waker can keep after the owner drops, holds its own charge.
        let shared = (size_of::<AtomicU64>() + size_of::<AtomicU32>()) * slots as usize + arc_size::<Shared>();
        let shared_charge = charge
            .split(shared as u64)
            .expect("the charge of the owner holds its shared part");
        // A root waker, too, can outlive the owner.
        let root_charge = charge
            .split(arc_size::<RootRef>() as u64)
            .expect("the charge of the owner holds its root waker");
        let refused = |_| Refused::Allocation;
        let words = items(slots, |_| AtomicU64::new(0)).map_err(refused)?;
        let next = items(slots, |_| AtomicU32::new(0)).map_err(refused)?;
        let entries = items(slots, |_| Cell::new(None)).map_err(refused)?;
        let mut free = room(slots as usize).map_err(refused)?;
        free.extend((0..slots).rev());
        let queue = VecDeque::from(room(slots as usize).map_err(refused)?);
        #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
        let outbox = RefCell::new(room(outbox).map_err(refused)?);
        let shared = try_arc(Shared::new(words, next, home, shared_charge)).map_err(refused)?;
        let root = alloc::sync::Arc::try_new(RootRef {
            shared: Arc::clone(&shared),
            _charge: root_charge,
        })
        .map_err(refused)?;
        Ok(Self {
            shared,
            budget: budget.clone(),
            entries,
            free: RefCell::new(free),
            queue: RefCell::new(queue),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            outbox,
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            driver: RefCell::new(None),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            lazy: Cell::new(None),
            cancelled: Cell::new(0),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            sends: Cell::new(false),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            switched: Cell::new(false),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            failure: Cell::new(None),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            generator: RefCell::new(None),
            live: Cell::new(0),
            closed: Cell::new(false),
            root: Waker::from(root),
            poll_budget: Cell::new(super::DEFAULT_POLL_BUDGET.get()),
            timers: RefCell::new(None),
            waiting: List::new(),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            polling: Cell::new(None),
            _charge: charge,
        })
    }

    /// Spawns `future` in a free slot. A refusal gives the future back. Only a refusal of the
    /// allocator comes after an allocation, and it frees it.
    pub(super) fn spawn<F>(&self, future: F) -> Result<JoinHandle<F::Output>, SpawnRefused<F>>
    where
        F: Future + 'static,
    {
        if self.closed.get() {
            return Err(SpawnRefused {
                future,
                reason: Reason::Closed,
            });
        }
        let outcome = oneshot::size::<Outcome<F::Output>>() as u64;
        let reserved = self.budget.reserve(task_size::<F>()).and_then(|mut charge| {
            let outcome = charge.split(outcome)?;
            Ok((charge, outcome))
        });
        let (charge, outcome) = match reserved {
            Ok(charges) => charges,
            Err(exhausted) => {
                return Err(SpawnRefused {
                    future,
                    reason: Reason::Budget(exhausted),
                });
            }
        };
        if !self.shared.reserve_slot() {
            return Err(SpawnRefused {
                future,
                reason: Reason::NoSlot,
            });
        }
        // The memory comes after the slot, so a refusal for a slot or a charge allocates nothing.
        // This thread is the home, so it can give the slot back to a waiting spawn.
        let Ok(((sender, receiver), memory)) =
            oneshot::with_charge(outcome).and_then(|ends| Ok((ends, TaskMemory::try_new()?)))
        else {
            self.release_slot();
            return Err(SpawnRefused {
                future,
                reason: Reason::Allocation,
            });
        };
        let identity = self.place(future, sender, charge, memory);
        Ok(JoinHandle {
            receiver,
            cancel: CancelHandle(Target::Local(identity)),
        })
    }

    /// Puts `future` in a free slot, after a spawn reserved the slot (`Shared::reserve_slot`),
    /// `charge`, and `memory`, and schedules it. Gives the identity of the task, which holds its own
    /// part of the charge: a waker or a cancel handle can keep it after the task ends.
    pub(super) fn place<F>(
        &self,
        future: F,
        sender: oneshot::Sender<Outcome<F::Output>>,
        mut charge: Permit,
        memory: TaskMemory<F>,
    ) -> alloc::sync::Arc<TaskRef>
    where
        F: Future + 'static,
    {
        let identity_charge = charge
            .split(IDENTITY as u64)
            .expect("the charge of a task holds its identity");
        let slot = self
            .free
            .borrow_mut()
            .pop()
            .expect("the free list has a slot for each reservation");
        let TaskMemory { cell, identity } = memory;
        let identity = init(
            identity,
            TaskRef {
                shared: Arc::clone(&self.shared),
                slot,
                generation: self.shared.generation(slot),
                _charge: identity_charge,
            },
        );
        let waker = Waker::from(alloc::sync::Arc::clone(&identity));
        let task: Pin<Box<dyn Task>> = Box::into_pin(Box::write(
            cell,
            TaskCell {
                future: Some(future),
                sender: Some(sender),
                _charge: charge,
            },
        ));
        self.shared.start(slot);
        self.entries[slot as usize].set(Some(Entry { task, waker }));
        self.live.update(|live| live + 1);
        self.queue.borrow_mut().push_back(slot);
        identity
    }

    /// Whether the `block_on` of this owner ended, or failed: it refuses each new task.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) fn closed(&self) -> bool {
        self.closed.get()
    }

    /// Gives back the reservation of a slot that a spawn did not take, and wakes the first waiting
    /// spawn.
    pub(super) fn release_slot(&self) {
        self.shared.release_slot();
        self.waiting.wake_first();
    }

    /// The task that this owner polls, when `waker` is that task's own waker.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) fn polling_task(&self, waker: &Waker) -> Option<crate::driver::LocalTask> {
        let polling = self.polling.get()?;
        (core::ptr::eq(waker.data(), polling.data) && core::ptr::eq(waker.vtable(), polling.vtable))
            .then_some(polling.task)
    }

    /// Wakes `task` of this owner on its thread: a task that was idle goes to the local ready
    /// queue, with no remote-ready list. A task of another generation does not change.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) fn wake_local(&self, task: crate::driver::LocalTask) {
        // The next turn polls the task: its entry loads while the wake changes its word.
        if let Some(entry) = self.entries.get(task.slot as usize) {
            crate::prefetch::lines(entry, size_of::<Cell<Option<Entry>>>());
        }
        if self.shared.wake_idle(task.slot, task.generation) {
            // The queue has room for each slot, and the word gives each slot one entry.
            self.queue.borrow_mut().push_back(task.slot);
        }
    }

    /// The part of this owner that each thread can change.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// Whether a task waits in the local ready queue.
    fn has_ready(&self) -> bool {
        !self.queue.borrow().is_empty()
    }

    /// The scheduling choice: the next ready task, first in, first out.
    fn next_ready(&self) -> Option<u32> {
        self.queue.borrow_mut().pop_front()
    }

    /// Polls the scheduled task in `slot`, or drops it at this poll boundary if a cancel was
    /// requested.
    fn poll_slot(&self, slot: u32) {
        let Some(mut entry) = self.entries[slot as usize].take() else {
            return;
        };
        // The future of a task spans a few cache lines, which its poll reads one after another:
        // they load together.
        crate::prefetch::lines(&raw const *entry.task, size_of_val(&*entry.task));
        if self.shared.poll_start(slot) {
            let outcome = entry.task.as_mut().end(End::Cancelled);
            return self.finish(slot, entry, outcome);
        }
        let Entry { task, waker } = &mut entry;
        #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
        self.polling.set(Some(Polling {
            task: crate::driver::LocalTask {
                slot,
                generation: self.shared.generation(slot),
            },
            data: waker.data(),
            vtable: waker.vtable(),
        }));
        let polled = catch_unwind(AssertUnwindSafe(|| {
            coop::with_budget(self.poll_budget.get(), || {
                task.as_mut().poll(&mut Context::from_waker(waker))
            })
        }));
        #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
        self.polling.set(None);
        match polled {
            Ok(Poll::Pending) => {
                self.entries[slot as usize].set(Some(entry));
                if self.shared.poll_end(slot) {
                    self.queue.borrow_mut().push_back(slot);
                }
            }
            Ok(Poll::Ready(outcome)) => self.finish(slot, entry, outcome),
            Err(payload) => {
                let outcome = entry.task.as_mut().end(End::Panicked(payload));
                self.finish(slot, entry, outcome);
            }
        }
    }

    /// Ends the task in `slot`: frees its storage, its charge, and its slot, wakes a waiting spawn,
    /// and then shows its `outcome` to its join handle.
    fn finish(&self, slot: u32, entry: Entry, outcome: Option<Staged>) {
        self.shared.end(slot);
        drop(entry);
        // The slot is free before its reservation is, so each reservation finds a free slot.
        self.free.borrow_mut().push(slot);
        self.shared.release_slot();
        self.live.update(|live| live - 1);
        self.waiting.wake_first();
        // A panic of the drop of an output that no join takes is contained.
        discard(outcome);
    }

    /// One turn: takes the remote-ready list, and polls each task that was ready when the turn
    /// started. Gives `false` if no task was ready. A task that a poll wakes waits for the next
    /// turn.
    fn turn(&self) -> bool {
        self.shared.harvest(|slot| self.queue.borrow_mut().push_back(slot));
        let ready = self.queue.borrow().len();
        for _ in 0..ready {
            let Some(slot) = self.next_ready() else {
                break;
            };
            self.poll_slot(slot);
        }
        ready > 0
    }

    /// The end of a loop iteration: drives `home` when work is `owed` or ready, and else parks it
    /// and drives it after the wait, so the next turn polls the tasks of its completions. Work is
    /// ready when a timer is `due`, a task is in the local queue, or `woken` (the remote-ready list
    /// or the root flag of the loop). With nothing ready, the drive enters no kernel, and the wait
    /// submits the pushed work in its one entry. Gives whether a drive is owed for the next
    /// iteration.
    fn settle<P: Park>(&self, home: &mut P, due: bool, owed: bool, woken: impl Fn() -> bool) -> Result<bool, P::Error> {
        let ready = due || self.has_ready() || woken();
        let next = if ready { Next::Work } else { Next::Wait };
        let more = if owed || ready { home.drive(self, next)? } else { false };
        // The drive clears the wake descriptor. A push or a root wake that came before the clear is
        // in the list or the flag, so this look after the drive sees it, and a later one signals
        // the descriptor again.
        if ready || more || woken() {
            return Ok(more);
        }
        home.park(self, self.until())?;
        home.drive(self, Next::Wait)
    }

    /// Gives the owner `timers`, driven on `clock`, in place of its storage before.
    pub(super) fn use_timers(&self, timers: Timers, clock: NativeClock) -> Result<(), TimeError> {
        if timers.domain() != clock.domain() {
            return Err(TimeError::DomainMismatch);
        }
        let before = self.timers.replace(Some((timers, clock)));
        // The drop of the storage before closes it: each of its timers gives `Closed`.
        drop(before);
        Ok(())
    }

    /// Fires the due timers, at most `TIMERS_PER_TURN`. Gives `true` if more timers are due.
    fn fire_timers(&self) -> bool {
        // The turn wakes the tasks of the timers, and the storage stays borrowed in its place
        // meanwhile: a waker that makes a timer gets a refusal (`timer()`). No other borrow is
        // active here: each one ends in its call, and `use_timers` cannot run during `block_on`.
        let mut timers = self.timers.borrow_mut();
        let Some((timers, clock)) = timers.as_mut() else {
            return false;
        };
        // A storage with no waiting timer needs no read of the clock: `timer()` brings it to the
        // clock before its next insert.
        if timers.next_turn().is_none() {
            return false;
        }
        // No mismatch: `use_timers` checked that the clock is on the domain of the storage.
        timers.turn(clock.now(), TIMERS_PER_TURN).unwrap_or(false)
    }

    /// The next turn of the timers that has work, or `None` with no waiting timer. It is at most
    /// the deadline of the next timer and the coarsening of the storage after it.
    #[cfg_attr(
        not(any(
            all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")),
            target_os = "macos",
            all(windows, any(target_arch = "x86_64", target_arch = "aarch64"))
        )),
        expect(
            clippy::uninhabited_references,
            reason = "with no native clock, `NativeClock` has no value, so no owner holds timers and the read never runs"
        )
    )]
    fn until(&self) -> Option<Until> {
        let timers = self.timers.borrow();
        let (timers, clock) = timers.as_ref()?;
        Some(Until::new(timers.next_turn()?, *clock))
    }

    /// Requests the cancel of each live task.
    fn cancel_all(&self) {
        for (slot, entry) in (0..).zip(self.entries.iter()) {
            let live = entry.take();
            if live.is_some() {
                self.shared.wake(slot, self.shared.generation(slot), true);
            }
            entry.set(live);
        }
    }

    /// Drops each live task with no outcome, after the owner failed. A join of such a task gives
    /// `OwnerFailed`.
    fn fail_all(&self) {
        // A destructor that spawns gets its future back.
        self.closed.set(true);
        self.queue.borrow_mut().clear();
        self.shared.harvest(|_| {});
        for (slot, entry) in (0..).zip(self.entries.iter()) {
            if let Some(entry) = entry.take() {
                self.shared.end(slot);
                // The slot is free before the drop closes the channel of the join handle.
                self.free.borrow_mut().push(slot);
                self.shared.release_slot();
                self.live.update(|live| live - 1);
                // A panic of the drop of one future does not stop the drop of the others.
                discard(entry);
            }
        }
        self.closed.set(false);
    }

    /// Polls `root` and the tasks of this owner on this thread until `root` gives its output.
    /// Then it cancels each task that is still live, and returns after they ended. `home` does the
    /// other work of the home in each turn, and it waits when no work is ready. A failure of `home`
    /// ends the loop.
    pub(super) fn block_on<F: Future, P: Park>(&self, root: F, home: &mut P) -> Result<F::Output, P::Error> {
        let mut root = pin!(root);
        let mut cx = Context::from_waker(&self.root);
        let mut poll_root = true;
        // A poll or a drive since the last drive can leave work for the home, such as a close that
        // a drop recorded, so the home turns before the owner parks.
        let mut owed = true;
        let output = loop {
            if poll_root || self.shared.take_root() {
                poll_root = false;
                owed = true;
                let polled = coop::with_budget(self.poll_budget.get(), || root.as_mut().poll(&mut cx));
                // A failure of the home in the poll ends the loop before a task polls or the
                // cancel of the end, so each live task ends with `OwnerFailed`.
                home.check(self)?;
                if let Poll::Ready(output) = polled {
                    break output;
                }
            }
            owed |= self.turn();
            let due = self.fire_timers();
            owed = self.settle(home, due, owed, || self.shared.has_remote() || self.shared.root_woken())?;
        };
        self.closed.set(true);
        self.cancelled.set(self.live.get());
        self.cancel_all();
        owed = true;
        while self.live.get() > 0 {
            owed |= self.turn();
            let due = self.fire_timers();
            // The turn can end the last task, and then nothing would end the park.
            owed = self.settle(home, due, owed, || self.shared.has_remote() || self.live.get() == 0)?;
        }
        self.closed.set(false);
        // The destructor of a cancelled task can fail the home, also after the last turn.
        home.check(self)?;
        Ok(output)
    }
}

/// The owner of this thread while `block_on` runs. Its drop restores the owner before, and, when
/// `block_on` does not end normally, drops each live task with no outcome.
pub(super) struct Enter {
    local: Rc<Local>,
    previous: Option<Rc<Local>>,
    previous_home: *const Shared,
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    previous_owner: *const Local,
    pub(super) ended: bool,
}

impl Enter {
    pub(super) fn new(local: &Rc<Local>) -> Self {
        let previous = super::CURRENT.with(|current| current.replace(Some(Rc::clone(local))));
        let previous_home = super::HOME.with(|home| home.replace(Arc::as_ptr(&local.shared)));
        #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
        let previous_owner = super::OWNER.with(|owner| owner.replace(Rc::as_ptr(local)));
        Self {
            local: Rc::clone(local),
            previous,
            previous_home,
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            previous_owner,
            ended: false,
        }
    }
}

impl Drop for Enter {
    fn drop(&mut self) {
        if !self.ended {
            self.local.fail_all();
        }
        super::HOME.with(|home| home.set(self.previous_home));
        #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
        super::OWNER.with(|owner| owner.set(self.previous_owner));
        let previous = self.previous.take();
        super::CURRENT.with(|current| current.replace(previous));
        // No later turn of this owner sends the messages of a wake that came after its last turn,
        // such as a wake from the drop of the root.
        #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
        for home in self.local.outbox.borrow_mut().drain(..) {
            home.interrupt();
        }
    }
}
