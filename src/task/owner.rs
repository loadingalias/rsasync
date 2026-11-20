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
use core::any::Any;
use core::cell::{Cell, RefCell};
use core::future::Future;
use core::panic::AssertUnwindSafe;
use core::pin::{Pin, pin};
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::panic::catch_unwind;

use super::slot::{Home, RootRef, Shared, TaskRef};
use super::waiting::List;
use super::{CancelHandle, JoinHandle, Outcome, Panic, Reason, Refused, SpawnRefused, Target};
use crate::capacity::{Budget, Permit};
use crate::channel::oneshot;
use crate::clock::NativeClock;
use crate::coop;
use crate::sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64},
    thread,
};
use crate::time::{Instant, TimeError};
use crate::timer::Timers;

/// The largest number of task slots of one owner.
pub(crate) const MAX_SLOTS: u32 = 1 << 16;
/// The most timers that one turn of an owner fires. The others fire in the turns that follow.
const TIMERS_PER_TURN: u32 = 64;

/// Why a task ends with no output.
enum End {
    Cancelled,
    Panicked(Box<dyn Any + Send>),
}

/// A spawned future and the sender of its outcome, behind one pointer.
trait Task {
    /// Polls the future. When it gives its output, drops the future, sends the output, and gives
    /// `Ready`.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()>;

    /// Ends the task with no output: drops the future, and then sends the outcome.
    fn end(self: Pin<&mut Self>, end: End);
}

struct TaskCell<F: Future> {
    /// The future, pinned in the cell. It drops in place, at the end of the task.
    future: Option<F>,
    sender: Option<oneshot::Sender<Outcome<F::Output>>>,
    _charge: Permit,
}

impl<F: Future> TaskCell<F> {
    /// The pinned future, for a pinned cell.
    #[expect(unsafe_code, reason = "the cell never moves its future: the future drops in place")]
    fn future(self: Pin<&mut Self>) -> Pin<&mut Option<F>> {
        // SAFETY: the future is pinned with the cell. The cell moves no field out of it, and it
        // ends the future with `Pin::set`, which drops it in place.
        unsafe { self.map_unchecked_mut(|cell| &mut cell.future) }
    }

    #[expect(unsafe_code, reason = "the sender is not pinned: the cell moves it out")]
    fn send(self: Pin<&mut Self>, outcome: Outcome<F::Output>) {
        // SAFETY: the sender is not structurally pinned, and this moves only the sender.
        let sender = unsafe { &mut self.get_unchecked_mut().sender };
        if let Some(sender) = sender.take()
            && let Err(unjoined) = sender.send(outcome)
        {
            // A join handle that dropped leaves the outcome to this send.
            discard(unjoined);
        }
    }
}

impl<F: Future> Task for TaskCell<F> {
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut future = self.as_mut().future();
        let Some(running) = future.as_mut().as_pin_mut() else {
            return Poll::Ready(());
        };
        let Poll::Ready(output) = running.poll(cx) else {
            return Poll::Pending;
        };
        // A panic of the drop of the future makes the task panicked, and its output drops.
        match catch_unwind(AssertUnwindSafe(|| future.set(None))) {
            Ok(()) => self.send(Outcome::Completed(output)),
            Err(payload) => {
                discard(output);
                self.send(Outcome::Panicked(Panic(payload)));
            }
        }
        Poll::Ready(())
    }

    fn end(mut self: Pin<&mut Self>, end: End) {
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
        self.send(outcome);
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
    /// turn.
    fn drive(&mut self, local: &Local) -> Result<bool, Self::Error>;

    /// Waits until a signal of the home comes, or until the instant of `until` at the latest.
    fn park(&mut self, local: &Local, until: Option<Until>) -> Result<(), Self::Error>;
}

/// The instant at which a park ends at the latest: the next turn of the timers that has work, on
/// the clock of the timers.
#[derive(Clone, Copy, Debug)]
pub(super) struct Until {
    pub(super) instant: Instant,
    clock: NativeClock,
}

impl Until {
    /// The time from now until the instant, or zero when it passed.
    pub(super) fn remaining(self) -> Duration {
        self.instant.duration_since(self.clock.now()).unwrap_or(Duration::ZERO)
    }
}

/// A home with no ring: it has no other work, and it parks its thread.
pub(super) struct Thread;

impl Park for Thread {
    type Error = !;

    fn drive(&mut self, _local: &Local) -> Result<bool, Self::Error> {
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

/// Makes `count` items with `make`, or refuses if the allocator does not give the memory.
fn items<X>(count: u32, make: impl Fn(u32) -> X) -> Result<Box<[X]>, Refused> {
    let mut items = Vec::new();
    items
        .try_reserve_exact(count as usize)
        .map_err(|_| Refused::Allocation)?;
    items.extend((0..count).map(make));
    Ok(items.into_boxed_slice())
}

/// The charge of a task of `F`: its cell, its outcome, and the reference of its waker, which holds
/// two counts. A size too large for a budget saturates, and the budget refuses it.
pub(super) fn task_size<F: Future>() -> u64 {
    (size_of::<TaskCell<F>>() as u64)
        .saturating_add(oneshot::size::<Outcome<F::Output>>() as u64)
        .saturating_add((size_of::<TaskRef>() + 2 * size_of::<usize>()) as u64)
}

pub(super) struct Local {
    shared: Arc<Shared>,
    budget: Budget,
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
    pub(super) lazy: Cell<Option<crate::driver::Config>>,
    /// The owner has a driver, so the messages of its outbox go out at its next turn or wait.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) sends: Cell<bool>,
    /// The home got its ring after the last turn, so the next park returns at once: the turn
    /// after it takes the list and the root flag before a wait in the ring (`slot`).
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) switched: Cell<bool>,
    /// The generator of this owner, from which its tasks take random values.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) generator: RefCell<Option<rscrypto::ChaCha20Rng>>,
    live: Cell<u32>,
    /// The end of `block_on` refuses new spawns.
    closed: Cell<bool>,
    /// The poll budget of each poll of a task or of the root (`crate::coop`).
    pub(super) poll_budget: Cell<u32>,
    /// The timer storage of the owner and the clock of its domain, from `use_timers`.
    pub(super) timers: RefCell<Option<(Timers, NativeClock)>>,
    pub(super) waiting: List,
    _charge: Permit,
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
            .and_then(|size| size.checked_add(size_of::<Shared>() + size_of::<Self>()))
            .and_then(|size| size.checked_add(outbox * size_of::<usize>()))
            .and_then(|size| u64::try_from(size).ok())
            .ok_or(Refused::TooLarge)?;
        let charge = budget.reserve(size).map_err(Refused::Budget)?;
        let words = items(slots, |_| AtomicU64::new(0))?;
        let next = items(slots, |_| AtomicU32::new(0))?;
        let entries = items(slots, |_| Cell::new(None))?;
        let mut free = Vec::new();
        free.try_reserve_exact(slots as usize)
            .map_err(|_| Refused::Allocation)?;
        free.extend((0..slots).rev());
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(slots as usize)
            .map_err(|_| Refused::Allocation)?;
        #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
        let outbox = {
            let mut homes = Vec::new();
            homes.try_reserve_exact(outbox).map_err(|_| Refused::Allocation)?;
            RefCell::new(homes)
        };
        Ok(Self {
            shared: Arc::new(Shared::new(words, next, home)),
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
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            sends: Cell::new(false),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            switched: Cell::new(false),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            generator: RefCell::new(None),
            live: Cell::new(0),
            closed: Cell::new(false),
            poll_budget: Cell::new(super::DEFAULT_POLL_BUDGET.get()),
            timers: RefCell::new(None),
            waiting: List::new(),
            _charge: charge,
        })
    }

    /// Spawns `future` in a free slot. A refusal gives the future back, and allocates nothing.
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
        let (sender, receiver) = oneshot::with_charge(outcome);
        let identity = self.place(future, sender, charge);
        Ok(JoinHandle {
            receiver,
            cancel: CancelHandle(Target::Local(identity)),
        })
    }

    /// Puts `future` in a free slot, after a spawn reserved the slot (`Shared::reserve_slot`) and
    /// `charge`, and schedules it. Gives the identity of the task.
    pub(super) fn place<F>(
        &self,
        future: F,
        sender: oneshot::Sender<Outcome<F::Output>>,
        charge: Permit,
    ) -> alloc::sync::Arc<TaskRef>
    where
        F: Future + 'static,
    {
        let slot = self
            .free
            .borrow_mut()
            .pop()
            .expect("the free list has a slot for each reservation");
        let identity = alloc::sync::Arc::new(TaskRef {
            shared: Arc::clone(&self.shared),
            slot,
            generation: self.shared.generation(slot),
        });
        let waker = Waker::from(alloc::sync::Arc::clone(&identity));
        let task: Pin<Box<dyn Task>> = Box::pin(TaskCell {
            future: Some(future),
            sender: Some(sender),
            _charge: charge,
        });
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

    /// Gives back the reservation of a slot that a factory spawn did not take, and wakes the first
    /// waiting spawn.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) fn release_slot(&self) {
        self.shared.release_slot();
        self.waiting.wake_first();
    }

    /// The part of this owner that each thread can change.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) fn shared(&self) -> &Arc<Shared> {
        &self.shared
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
        if self.shared.poll_start(slot) {
            entry.task.as_mut().end(End::Cancelled);
            return self.finish(slot, entry);
        }
        let Entry { task, waker } = &mut entry;
        let polled = catch_unwind(AssertUnwindSafe(|| {
            coop::with_budget(self.poll_budget.get(), || {
                task.as_mut().poll(&mut Context::from_waker(waker))
            })
        }));
        match polled {
            Ok(Poll::Pending) => {
                self.entries[slot as usize].set(Some(entry));
                if self.shared.poll_end(slot) {
                    self.queue.borrow_mut().push_back(slot);
                }
            }
            Ok(Poll::Ready(())) => self.finish(slot, entry),
            Err(payload) => {
                entry.task.as_mut().end(End::Panicked(payload));
                self.finish(slot, entry);
            }
        }
    }

    /// Ends the task in `slot`, after it sent its outcome: frees its storage, its charge, and its
    /// slot, and wakes a waiting spawn.
    fn finish(&self, slot: u32, entry: Entry) {
        self.shared.end(slot);
        drop(entry);
        // The slot is free before its reservation is, so each reservation finds a free slot.
        self.free.borrow_mut().push(slot);
        self.shared.release_slot();
        self.live.update(|live| live - 1);
        self.waiting.wake_first();
    }

    /// One turn: takes the remote-ready list, and polls each task that was ready when the turn
    /// started. Gives `false` if no task was ready.
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
        // The turn wakes the tasks of the timers. The storage is out of its cell meanwhile, so a
        // waker that makes a timer gets a refusal, not a panic of the cell.
        let Some((mut timers, clock)) = self.timers.take() else {
            return false;
        };
        // No mismatch: `use_timers` checked that the clock is on the domain of the storage.
        let more = timers.turn(clock.now(), TIMERS_PER_TURN).unwrap_or(false);
        let replaced = self.timers.replace(Some((timers, clock)));
        drop(replaced);
        more
    }

    /// The next turn of the timers that has work, or `None` with no waiting timer. It is at most
    /// the deadline of the next timer and the coarsening of the storage after it.
    fn until(&self) -> Option<Until> {
        let timers = self.timers.borrow();
        let (timers, clock) = timers.as_ref()?;
        Some(Until {
            instant: timers.next_turn()?,
            clock: *clock,
        })
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
                // A panic of the drop of one future does not stop the drop of the others.
                discard(entry);
                self.free.borrow_mut().push(slot);
                self.shared.release_slot();
                self.live.update(|live| live - 1);
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
        let waker = Waker::from(alloc::sync::Arc::new(RootRef(Arc::clone(&self.shared))));
        let mut cx = Context::from_waker(&waker);
        let mut poll_root = true;
        let output = loop {
            if poll_root || self.shared.take_root() {
                poll_root = false;
                if let Poll::Ready(output) = coop::with_budget(self.poll_budget.get(), || root.as_mut().poll(&mut cx)) {
                    break output;
                }
            }
            let ran = self.turn();
            let due = self.fire_timers();
            let more = home.drive(self)?;
            if !ran && !due && !more && !self.shared.has_remote() && !self.shared.root_woken() {
                home.park(self, self.until())?;
            }
        };
        self.closed.set(true);
        self.cancel_all();
        while self.live.get() > 0 {
            let ran = self.turn();
            let due = self.fire_timers();
            let more = home.drive(self)?;
            if !ran && !due && !more && !self.shared.has_remote() {
                home.park(self, self.until())?;
            }
        }
        self.closed.set(false);
        Ok(output)
    }
}

/// The owner of this thread while `block_on` runs. Its drop restores the owner before, and, when
/// `block_on` does not end normally, drops each live task with no outcome.
pub(super) struct Enter {
    local: Rc<Local>,
    previous: Option<Rc<Local>>,
    previous_home: *const Shared,
    pub(super) ended: bool,
}

impl Enter {
    pub(super) fn new(local: &Rc<Local>) -> Self {
        let previous = super::CURRENT.with(|current| current.replace(Some(Rc::clone(local))));
        let previous_home = super::HOME.with(|home| home.replace(Arc::as_ptr(&local.shared)));
        Self {
            local: Rc::clone(local),
            previous,
            previous_home,
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
