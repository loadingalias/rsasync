//! The movable pool: tasks that each worker of the pool can poll, and that an idle worker steals.
//!
//! # Slots and the slot word
//!
//! The pool has one table of task slots for all its workers. The word of a slot follows the
//! protocol of the local owner (`slot`): one read-modify-write for each change, the generation
//! check of each wake, and at most one queue entry for each slot. The worker that takes the entry
//! starts the poll, so only one worker polls a task, and a cancel drops the future at a poll
//! boundary. The location of a slot is the worker that polls its task, or polled it last; the poll
//! start records it.
//!
//! # Queues
//!
//! Each worker, a lane, has a run queue (`deque`) that only it pushes and pops, and a remote-ready
//! list, which each thread can push: a stack of slots, linked by one `next` for each slot, as the
//! list of the local owner. A wake on a worker of the pool pushes to the queue of that worker. A
//! wake on another thread pushes to the list of the location of the task, and the push that finds
//! the list empty wakes the root future of that worker: the native signal of its home ends its
//! park. So each push reaches a worker that runs or that its signal wakes, and no wake is lost. A
//! full queue gives its overflow to the list of its own worker, so no queue grows past its slots.
//!
//! # Steals
//!
//! A worker with no task searches. It steals from the workers of its cache domain first, and then
//! from the others, each time from a start of its generator. It takes the older half of a victim's
//! run queue or up to half a queue of its oldest remote-ready tasks. It polls the first task and
//! puts the others in its own queue in order. The victim keeps the rest available to other thieves.
//! The task-lifecycle model checks this steal (`Steal`). At most half of
//! the workers search at one time.
//!
//! # Parks and balance
//!
//! A worker that finds no task gives `Pending` from its root future. Its home then runs its local
//! tasks and its driver, and it parks when they have no work. At that park, and not before, the
//! worker counts itself parked, and then it looks one last time at each queue (`Pool::park`). So
//! a worker that runs a local task is not parked. A worker with more than one task in its queue,
//! and each push to a list, wakes one parked worker when no worker searches: that worker waits in
//! its home, so it takes the work at once. That wake only balances the work: the no-loss rule
//! above does not need it.
//!
//! # Admission and close
//!
//! One word holds the closed bit and the count of the reserved slots. A spawn reserves a slot with
//! one read-modify-write that refuses when the bit is set or each slot is reserved. It marks its
//! slot live and then reads the word again with a read-modify-write: the close sets the bit with a
//! read-modify-write and then cancels each live slot. So the close cancels a task that started
//! before that second read, and a spawn that reads the bit cancels its own task. The end of a task
//! gives its slot back. Each worker ends when the word holds the bit and no reserved slot.

use alloc::boxed::Box;
use alloc::task::Wake;
use core::alloc::AllocError;
use core::cell::Cell;
use core::future::Future;
use core::panic::AssertUnwindSafe;
use core::pin::Pin;
use core::ptr;
use core::task::{Context, Poll, Waker};
use std::panic::catch_unwind;

use super::deque::{self, Deque};
use super::late::Late;
use super::owner::{End, Task, TaskCell, discard, init};
use super::slot::{self, Shared as Home};
use super::{CancelHandle, JoinHandle, Outcome, Reason, SpawnRefused, Target};
use crate::capacity::{Budget, Permit};
use crate::channel::oneshot::{self, Staged};
use crate::memory::{items, room};
use crate::sync::atomic::{self, AtomicBool, AtomicU32, AtomicU64, Ordering};
use crate::sync::{Arc, arc_size, thread_local, try_arc};

/// The link that ends a list of slots.
const END: u32 = u32::MAX;
/// A worker holds the links of a lane's oldest remote-ready tasks. Slot indices and `END` fit in `u32`.
const TAKING: u64 = u64::MAX;
/// In the admission word: the pool closed, and it admits no task.
const CLOSED: u64 = 1 << 63;
/// The tasks that one turn of a worker polls at most, before its driver turns: one full run queue.
const TURN_TASKS: u32 = deque::CAPACITY;

thread_local! {
    /// The pool and the lane of this thread, if it is a worker of a pool. A wake reads only this
    /// value, which needs no destructor, so a wake from a new thread never allocates for it.
    #[cfg_attr(
        not(all(test, loom)),
        expect(
            clippy::missing_const_for_thread_local,
            reason = "the loom form of the macro takes no `const` block, and both forms take this one"
        )
    )]
    static LANE: Cell<(*const Shared, u32)> = Cell::new((ptr::null(), 0));

    /// The worker of this thread runs a turn of its pool, so a push to its queue needs no wake of
    /// its root future.
    #[cfg_attr(
        not(all(test, loom)),
        expect(
            clippy::missing_const_for_thread_local,
            reason = "the loom form of the macro takes no `const` block, and both forms take this one"
        )
    )]
    static TURNING: Cell<bool> = Cell::new(false);

    /// The worker of this thread woke a parked worker. The wake waits in the outbox of the owner of
    /// this thread until the root future returns, so the turn that runs, or the next one, ends
    /// after its task.
    #[cfg_attr(
        not(all(test, loom)),
        expect(
            clippy::missing_const_for_thread_local,
            reason = "the loom form of the macro takes no `const` block, and both forms take this one"
        )
    )]
    static WOKE: Cell<bool> = Cell::new(false);
}

/// A live task in its slot: the task and its waker.
struct Movable {
    task: Pin<Box<dyn Task + Send>>,
    waker: Waker,
}

mod cells {
    use alloc::boxed::Box;
    use core::alloc::AllocError;

    use super::Movable;
    use crate::memory::items;
    use crate::sync::UnsafeCell;

    /// The task of each slot. The thread that holds a slot is its one accessor: the spawn, from its
    /// pop of the free slot to its start, and the poller, from its poll start to its poll end or to
    /// the end of the task. The slot word orders each change of holder: the start and the poll end
    /// publish with `Release`, and the poll start reads with `Acquire`.
    pub(super) struct Cells(Box<[UnsafeCell<Option<Movable>>]>);

    #[expect(unsafe_code, reason = "each cell has one accessor at a time, the holder of its slot")]
    // SAFETY: a cell is accessed only through `take` and `put`, whose callers hold its slot, so no
    // two threads access one cell at one time. Its value is `Send`.
    unsafe impl Sync for Cells {}

    impl Cells {
        pub(super) fn new(slots: u32) -> Result<Self, AllocError> {
            Ok(Self(items(slots, |_| UnsafeCell::new(None))?))
        }

        /// Takes the task of `slot`.
        ///
        /// # Safety
        ///
        /// The caller holds `slot`.
        #[expect(unsafe_code, reason = "the caller is the one accessor of the cell")]
        pub(super) unsafe fn take(&self, slot: u32) -> Option<Movable> {
            // SAFETY: the caller holds the slot, so no other access runs.
            self.0[slot as usize].with_mut(|cell| unsafe { (*cell).take() })
        }

        /// Puts `movable` in the cell of `slot`.
        ///
        /// # Safety
        ///
        /// The caller holds `slot`.
        #[expect(unsafe_code, reason = "the caller is the one accessor of the cell")]
        pub(super) unsafe fn put(&self, slot: u32, movable: Movable) {
            // SAFETY: as in `take`.
            self.0[slot as usize].with_mut(|cell| unsafe { *cell = Some(movable) });
        }
    }
}

/// One worker of the pool.
struct Lane {
    queue: Deque,
    /// The newest push since the last reversal, or `END`.
    remote: AtomicU32,
    /// The oldest slot of the reversed list, `END`, or `TAKING`.
    oldest: AtomicU64,
    /// The shared part of the owner of the worker: a wake of its root future ends its park. The
    /// worker sets it before it runs, and detaches it at its end.
    owner: Late<Home>,
    /// The worker counts itself parked: its home waits ([`Pool::park`]).
    parked: AtomicBool,
    /// The cache domain of the worker: its first place in `Shared::order`, and its length.
    group: (u32, u32),
}

/// The part of the pool that a waker keeps: what a wake changes.
pub(super) struct Shared {
    words: Box<[AtomicU64]>,
    /// The next slot of each slot in a remote-ready list or in the free list.
    next: Box<[AtomicU32]>,
    /// The worker that polls the task of each slot, or polled it last.
    location: Box<[AtomicU32]>,
    lanes: Box<[Lane]>,
    /// The number of lanes.
    count: u32,
    /// The workers, in the order of their cache domains.
    order: Box<[u32]>,
    /// The workers that count themselves parked, and those that search: hints for balance.
    parked: AtomicU32,
    searching: AtomicU32,
    /// The next lane for a spawn or a wake of a parked worker from another thread.
    spread: AtomicU32,
    /// The charge of this part, which a waker can keep after the pool.
    _charge: Permit,
}

/// The identity of one movable task, for its waker and its cancel handle.
pub(super) struct MovableRef {
    shared: Arc<Shared>,
    slot: u32,
    generation: u64,
    /// The charge of this identity, which a waker or a cancel handle can keep after the task.
    _charge: Permit,
}

impl MovableRef {
    /// Requests the cancel of the task.
    pub(super) fn cancel(&self) {
        self.shared.wake(self.slot, self.generation, true);
    }
}

impl Wake for MovableRef {
    fn wake(self: alloc::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &alloc::sync::Arc<Self>) {
        self.shared.wake(self.slot, self.generation, false);
    }
}

/// The charge of the identity of a movable task.
const IDENTITY: usize = arc_size::<MovableRef>();

/// The charge of a movable task of `F`: its cell, its outcome, and its identity.
fn task_size<F: Future>() -> u64 {
    (size_of::<TaskCell<F>>() as u64)
        .saturating_add(oneshot::size::<Outcome<F::Output>>() as u64)
        .saturating_add(IDENTITY as u64)
}

impl Shared {
    fn lane_of_thread(&self) -> Option<u32> {
        let (pool, lane) = LANE.with(Cell::get);
        ptr::eq(pool, self).then_some(lane)
    }

    /// Wakes the task of `generation` in `slot`, and requests its cancel if `cancel` is true.
    fn wake(&self, slot: u32, generation: u64, cancel: bool) {
        if slot::wake(&self.words[slot as usize], generation, cancel) {
            self.schedule(slot);
        }
    }

    /// Pushes the one entry of the scheduled `slot`: to the queue of the worker of this thread, or
    /// else to the list of the location of the task.
    fn schedule(&self, slot: u32) {
        match self.lane_of_thread() {
            Some(lane) => self.push_local(lane, slot),
            None => {
                // Ordering: `Relaxed`: each worker is a valid place; the location only keeps the
                // task near its last worker.
                let lane = self.location[slot as usize].load(Ordering::Relaxed);
                self.push_list(lane, slot, true);
                self.balance();
            }
        }
    }

    /// Pushes `slot` to the queue of `lane`, the worker of this thread. A wake outside a turn, such
    /// as from its driver or from a local task, also wakes its root future. A second task in the
    /// queue wakes a parked worker: the completions of one turn of the driver can make each task of
    /// the worker ready at one time.
    fn push_local(&self, lane: u32, slot: u32) {
        self.queue(lane, slot);
        let mine = &self.lanes[lane as usize];
        if !TURNING.with(Cell::get) {
            let _ = mine.owner.with(|home| home.wake_root());
        }
        if mine.queue.len() > 1 {
            self.balance();
        }
    }

    /// Pushes `slot` to the queue of `lane`, the worker of this thread, or to its list when the
    /// queue is full. The worker runs, so it needs no signal.
    fn queue(&self, lane: u32, slot: u32) {
        if let Err(slot) = self.lanes[lane as usize].queue.push(slot) {
            self.push_list(lane, slot, false);
        }
    }

    /// Pushes `slot` to the remote-ready list of `lane`. With `signal`, the push that finds the list
    /// empty wakes the root future of the worker.
    fn push_list(&self, lane: u32, slot: u32, signal: bool) {
        let target = &self.lanes[lane as usize];
        // Ordering: `Relaxed`: the compare-exchange below reads the latest head.
        let mut head = target.remote.load(Ordering::Relaxed);
        loop {
            // Ordering: `Relaxed`: the release of the change of the head publishes the link.
            self.next[slot as usize].store(head, Ordering::Relaxed);
            // Ordering: `AcqRel`: `Release` publishes the link and the slot word to the taker,
            // which swaps the head with `Acquire`. `Acquire` reads the release of that take.
            match target
                .remote
                .compare_exchange_weak(head, slot, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => head = actual,
            }
        }
        if signal && head == END {
            let _ = target.owner.with(|home| home.wake_root());
        }
    }

    /// Takes the oldest slot. A thief can also take a bounded prefix into its own queue.
    fn take_list(&self, lane: u32, into: Option<&Deque>) -> Option<u32> {
        let lane = &self.lanes[lane as usize];
        // Ordering: these loads only skip an empty or claimed list. `TAKING` is not a slot: another
        // taker holds the links.
        let observed = lane.oldest.load(Ordering::Relaxed);
        let mut first = u32::try_from(observed).ok()?;
        if first == END && lane.remote.load(Ordering::Relaxed) == END {
            return None;
        }
        // The exchange claims the links from the value that this taker read. A taker that loses
        // writes nothing and skips this lane, so the release of the holder is the last write of the
        // word. A blind swap of `TAKING` is a write that loom can order after that release: it has
        // no rule for the atomicity of a read-modify-write, and the list then stays claimed.
        // Ordering: `Acquire` reads the release of the last taker.
        lane.oldest
            .compare_exchange(observed, TAKING, Ordering::AcqRel, Ordering::Relaxed)
            .ok()?;
        if first == END {
            // Acquire reads the published links. Producers can push the next group during this reversal.
            let mut head = lane.remote.swap(END, Ordering::AcqRel);
            while head != END {
                let next = self.next[head as usize].load(Ordering::Relaxed);
                self.next[head as usize].store(first, Ordering::Relaxed);
                first = head;
                head = next;
            }
        }
        let first = (first != END).then_some(first);
        let mut rest = first.map_or(END, |slot| self.next[slot as usize].load(Ordering::Relaxed));
        if let Some(queue) = into {
            for _ in 1..deque::CAPACITY / 2 {
                if rest == END {
                    break;
                }
                // A thief of this queue can run the slot as soon as the push publishes it.
                let next = self.next[rest as usize].load(Ordering::Relaxed);
                if queue.push(rest).is_err() {
                    break;
                }
                rest = next;
            }
        }
        // Release publishes the suffix before this worker polls a task. Each link is reversed only once.
        lane.oldest.store(u64::from(rest), Ordering::Release);
        first
    }

    /// Wakes one parked worker when no worker searches, after a push: work spreads.
    fn balance(&self) {
        // Ordering: this `SeqCst` fence after the push, and the one of a worker that parks before
        // its last look, order the two: the push sees the parked worker, or that worker sees the
        // push. Without them, a worker could park while another holds work that it pushed.
        atomic::fence(Ordering::SeqCst);
        // Ordering: `Relaxed`: the fence orders these loads after the push.
        if self.searching.load(Ordering::Relaxed) == 0 && self.parked.load(Ordering::Relaxed) > 0 {
            self.unpark_one();
        }
    }

    fn unpark_one(&self) {
        let count = self.count;
        // Ordering: `Relaxed`: the counter only spreads the wakes.
        let start = self.spread.fetch_add(1, Ordering::Relaxed);
        for index in 0..count {
            let lane = &self.lanes[(start.wrapping_add(index) % count) as usize];
            // Ordering: `Relaxed`: the wake of the root future orders the work before the next
            // turn of the worker.
            if lane
                .parked
                .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.parked.fetch_sub(1, Ordering::Relaxed);
                let _ = lane.owner.with(|home| home.wake_root());
                // Also outside a turn, for a wake from the driver: the next turn then ends after
                // its first task.
                if self.lane_of_thread().is_some() {
                    WOKE.with(|woke| woke.set(true));
                }
                return;
            }
        }
    }

    /// Wakes the root future of each worker: each one looks at the queues and the admission again.
    fn wake_all(&self) {
        for lane in &self.lanes {
            let _ = lane.owner.with(|home| home.wake_root());
        }
    }

    /// Whether a queue or a list of a worker holds a task: the last look of a worker that parks.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    fn has_work(&self) -> bool {
        // Ordering: `Relaxed`: the fence before this look orders it, as in `balance`.
        self.lanes.iter().any(|lane| {
            lane.queue.len() > 0
                || lane.remote.load(Ordering::Relaxed) != END
                || lane.oldest.load(Ordering::Relaxed) != u64::from(END)
        })
    }
}

/// The side of the pool that its workers and its spawns use.
pub(crate) struct Pool {
    shared: Arc<Shared>,
    cells: cells::Cells,
    /// The free list: a slot in the low half, or `END`, and in the high half a tag that each change
    /// of the head adds one to, so a head that came back is not the head that a pop read.
    free: AtomicU64,
    /// The closed bit and the count of the reserved slots.
    admission: AtomicU64,
    /// The tasks that a worker dropped because the close cancelled them.
    cancelled: AtomicU32,
    slots: u32,
    budget: Budget,
    poll_budget: u32,
    _charge: Permit,
}

/// What a turn of a worker found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Turn {
    /// The turn polled its limit of tasks: the worker turns its driver and then runs again.
    Busy,
    /// The worker found no task. Its home can park, after [`Pool::park`].
    Idle,
    /// The pool closed and no task remains: the worker ends.
    Ended,
}

/// Why the pool did not start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PoolRefused {
    /// The budget has fewer free units than the tables of the pool.
    Budget(crate::capacity::Exhausted),
    /// The allocator did not give the memory of the tables.
    Allocation,
}

impl Pool {
    /// A pool with `slots` task slots and one lane for each entry of `domains`, the cache domain of
    /// each worker. It charges its tables to `budget`, and each task at its spawn.
    pub(crate) fn new(
        budget: &Budget,
        slots: u32,
        domains: &[Option<u32>],
        poll_budget: u32,
    ) -> Result<Self, PoolRefused> {
        let lanes = u32::try_from(domains.len()).map_err(|_| PoolRefused::Allocation)?;
        assert!(lanes > 0 && slots > 0, "a pool has a worker and a slot");
        let per_slot = size_of::<AtomicU64>() + 2 * size_of::<AtomicU32>();
        let shared_size = (per_slot as u64)
            .saturating_mul(u64::from(slots))
            .saturating_add(
                ((size_of::<Lane>() + deque::SIZE + size_of::<u32>()) as u64).saturating_mul(u64::from(lanes)),
            )
            .saturating_add(arc_size::<Shared>() as u64);
        let cells = (size_of::<crate::sync::UnsafeCell<Option<Movable>>>() as u64).saturating_mul(u64::from(slots));
        let mut charge = budget
            .reserve(
                shared_size
                    .saturating_add(cells)
                    .saturating_add(size_of::<Self>() as u64),
            )
            .map_err(PoolRefused::Budget)?;
        let shared_charge = charge
            .split(shared_size)
            .expect("the charge of the pool holds its shared part");
        let refused = |_: AllocError| PoolRefused::Allocation;
        // The workers in the order of their domains, and the group of each one.
        let mut order = items(lanes, |lane| lane).map_err(refused)?;
        order.sort_by_key(|&lane| domains[lane as usize]);
        let group = |lane: u32| {
            let domain = domains[lane as usize];
            let (mut first, mut length) = (None, 0);
            for (place, &other) in (0..).zip(order.iter()) {
                if domains[other as usize] == domain {
                    first.get_or_insert(place);
                    length += 1;
                }
            }
            (first.unwrap_or(0), length)
        };
        let mut built = room(lanes as usize).map_err(refused)?;
        for lane in 0..lanes {
            built.push(Lane {
                queue: Deque::new().map_err(refused)?,
                remote: AtomicU32::new(END),
                oldest: AtomicU64::new(u64::from(END)),
                owner: Late::new(None),
                parked: AtomicBool::new(false),
                group: group(lane),
            });
        }
        let next = items(slots, |slot| {
            AtomicU32::new(if slot + 1 == slots { END } else { slot + 1 })
        })
        .map_err(refused)?;
        let shared = try_arc(Shared {
            words: items(slots, |_| AtomicU64::new(0)).map_err(refused)?,
            next,
            location: items(slots, |_| AtomicU32::new(0)).map_err(refused)?,
            lanes: built.into_boxed_slice(),
            count: lanes,
            order,
            parked: AtomicU32::new(0),
            searching: AtomicU32::new(0),
            spread: AtomicU32::new(0),
            _charge: shared_charge,
        })
        .map_err(refused)?;
        Ok(Self {
            shared,
            cells: cells::Cells::new(slots).map_err(refused)?,
            free: AtomicU64::new(0),
            admission: AtomicU64::new(0),
            cancelled: AtomicU32::new(0),
            slots,
            budget: budget.clone(),
            poll_budget,
            _charge: charge,
        })
    }

    /// The workers of the pool.
    pub(crate) fn lanes(&self) -> u32 {
        self.shared.count
    }

    /// Gives worker `lane` the shared part of its owner, whose root future runs its turns. Call it
    /// on the thread of the worker, before its first turn.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(super) fn attach(&self, lane: u32, owner: Arc<Home>) {
        self.shared.lanes[lane as usize].owner.set(owner);
    }

    /// Detaches worker `lane` from its owner at its end: a later wake signals nothing.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(crate) fn detach(&self, lane: u32) {
        self.shared.lanes[lane as usize].owner.detach();
    }

    /// Spawns `future` in the pool, from any thread: on the worker of this thread if it is one,
    /// else on the next worker in turn.
    ///
    /// # Errors
    ///
    /// [`SpawnRefused`] with the future and the [`Reason`]: `Budget`, `Closed`, `NoSlot`, or
    /// `Allocation`. A refusal keeps no charge and no reservation.
    pub(crate) fn spawn<F>(&self, future: F) -> Result<JoinHandle<F::Output>, SpawnRefused<F>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let outcome = oneshot::size::<Outcome<F::Output>>() as u64;
        let reserved = self.budget.reserve(task_size::<F>()).and_then(|mut charge| {
            let outcome = charge.split(outcome)?;
            Ok((charge, outcome))
        });
        let (mut charge, outcome) = match reserved {
            Ok(charges) => charges,
            Err(exhausted) => {
                return Err(SpawnRefused {
                    future,
                    reason: Reason::Budget(exhausted),
                });
            }
        };
        if let Err(reason) = self.admit() {
            return Err(SpawnRefused { future, reason });
        }
        let Ok(((sender, receiver), cell, identity)) = oneshot::with_charge(outcome).and_then(|ends| {
            Ok((
                ends,
                Box::<TaskCell<F>>::try_new_uninit()?,
                alloc::sync::Arc::<MovableRef>::try_new_uninit()?,
            ))
        }) else {
            self.release();
            return Err(SpawnRefused {
                future,
                reason: Reason::Allocation,
            });
        };
        let slot = self.pop_free();
        let identity_charge = charge
            .split(IDENTITY as u64)
            .expect("the charge of a task holds its identity");
        let word = &self.shared.words[slot as usize];
        let identity = init(
            identity,
            MovableRef {
                shared: Arc::clone(&self.shared),
                slot,
                generation: slot::generation(word),
                _charge: identity_charge,
            },
        );
        let waker = Waker::from(alloc::sync::Arc::clone(&identity));
        let task: Pin<Box<dyn Task + Send>> = Box::into_pin(Box::write(
            cell,
            TaskCell {
                future: Some(future),
                sender: Some(sender),
                _charge: charge,
            },
        ));
        #[expect(unsafe_code, reason = "the spawn holds the free slot that it popped")]
        // SAFETY: the slot was free, and this spawn popped it, so it is the one accessor of the
        // cell until its start publishes the task.
        unsafe {
            self.cells.put(slot, Movable { task, waker });
        }
        let lane = self.shared.lane_of_thread();
        let target = lane.unwrap_or_else(|| {
            // Ordering: `Relaxed`: the counter only spreads the spawns.
            self.shared.spread.fetch_add(1, Ordering::Relaxed) % self.lanes()
        });
        // Ordering: `Relaxed`: the start below publishes it with the task.
        self.shared.location[slot as usize].store(target, Ordering::Relaxed);
        slot::start_live(word);
        // Ordering: `AcqRel`: a close whose read-modify-write comes after this one reads the
        // start above, and cancels the task. Else this read finds the closed bit.
        if self.admission.fetch_or(0, Ordering::AcqRel) & CLOSED != 0 {
            let _requested = slot::cancel_live(word);
        }
        match lane {
            Some(lane) => self.shared.push_local(lane, slot),
            None => {
                self.shared.push_list(target, slot, true);
                self.shared.balance();
            }
        }
        Ok(JoinHandle {
            receiver,
            cancel: CancelHandle(Target::Movable(identity)),
        })
    }

    /// Reserves a slot for a spawn.
    fn admit(&self) -> Result<(), Reason> {
        // Ordering: `Relaxed`: the compare-exchange below reads the latest word.
        let mut current = self.admission.load(Ordering::Relaxed);
        loop {
            if current & CLOSED != 0 {
                return Err(Reason::Closed);
            }
            if current >= u64::from(self.slots) {
                return Err(Reason::NoSlot);
            }
            // Ordering: `AcqRel`: the admissions and the close have one order in this word.
            match self
                .admission
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    /// Gives back a reserved slot. The last one after the close wakes each worker, which ends.
    fn release(&self) {
        // Ordering: `AcqRel`, as in `admit`.
        if self.admission.fetch_sub(1, Ordering::AcqRel) == CLOSED | 1 {
            self.shared.wake_all();
        }
    }

    /// Pops a free slot. Only a spawn that reserved a slot calls it, so the list holds one: each end
    /// of a task pushes its slot before it gives its reservation back.
    fn pop_free(&self) -> u32 {
        // Ordering: `Acquire` reads the push of the head and of its link.
        let mut head = self.free.load(Ordering::Acquire);
        loop {
            #[expect(clippy::cast_possible_truncation, reason = "the low half of the head is the slot")]
            let slot = head as u32;
            assert!(slot != END, "the free list holds a slot for each reservation");
            // Ordering: `Relaxed`: the `Acquire` of the head read the push of this link. A link
            // that another pop changed meanwhile fails the exchange, because the tag changed.
            let next = self.shared.next[slot as usize].load(Ordering::Relaxed);
            let changed = (((head >> 32) + 1) << 32) | u64::from(next);
            // Ordering: `Acquire` on success reads the push of the next head.
            match self
                .free
                .compare_exchange_weak(head, changed, Ordering::Acquire, Ordering::Acquire)
            {
                Ok(_) => return slot,
                Err(actual) => head = actual,
            }
        }
    }

    /// Pushes `slot` to the free list.
    fn push_free(&self, slot: u32) {
        // Ordering: `Relaxed`: the exchange below reads the latest head.
        let mut head = self.free.load(Ordering::Relaxed);
        loop {
            #[expect(clippy::cast_possible_truncation, reason = "the low half of the head is the slot")]
            let next = head as u32;
            // Ordering: `Relaxed`: the release of the exchange publishes it.
            self.shared.next[slot as usize].store(next, Ordering::Relaxed);
            let changed = (((head >> 32) + 1) << 32) | u64::from(slot);
            // Ordering: `Release` publishes the link and the end of the task to the next pop.
            match self
                .free
                .compare_exchange_weak(head, changed, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }

    /// Closes the pool: each spawn refuses from now, and each live task is cancelled. The workers
    /// end when the last task ends.
    pub(crate) fn close(&self) {
        // Ordering: `AcqRel`: see the admission in the module documentation.
        self.admission.fetch_or(CLOSED, Ordering::AcqRel);
        for slot in 0..self.slots {
            if slot::cancel_live(&self.shared.words[slot as usize]) == Some(true) {
                self.shared.schedule(slot);
            }
        }
        // Each worker looks again, also one that parked while the tasks of a worker that failed
        // wait for a thief.
        self.shared.wake_all();
    }

    /// A worker of the pool ended with an error: each other worker wakes and looks at the queues
    /// again, so the tasks of that worker go to a thief.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(crate) fn worker_failed(&self) {
        self.shared.wake_all();
    }

    /// The tasks that a worker dropped because the close cancelled them. A task in its last poll at
    /// the close completes, and it does not count. The count is final when each worker ended.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(crate) fn cancelled(&self) -> u32 {
        // Ordering: `Relaxed`: the join of each worker orders its counts before this read.
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Whether the pool closed and no task remains.
    fn ended(&self) -> bool {
        // Ordering: `Acquire` reads the release of the end of the last task.
        self.admission.load(Ordering::Acquire) == CLOSED
    }

    /// Worker `lane` counts itself parked, at the park of its home, and then looks one last time at
    /// each queue. Gives `false`, with the worker not parked, when a queue holds a task or the
    /// pool ended: the worker then turns again and does not wait.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(crate) fn park(&self, lane: u32) -> bool {
        let shared = &*self.shared;
        // Ordering: `Relaxed`: the counts are hints, and the fence orders them before the look.
        shared.lanes[lane as usize].parked.store(true, Ordering::Relaxed);
        shared.parked.fetch_add(1, Ordering::Relaxed);
        // Ordering: see `balance`.
        atomic::fence(Ordering::SeqCst);
        let parked = !shared.has_work() && !self.ended();
        if !parked {
            self.unpark(lane);
        }
        parked
    }

    /// The park of worker `lane` ended, or did not start. A balance wake that selected the worker
    /// took its mark already.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(crate) fn unpark(&self, lane: u32) {
        let shared = &*self.shared;
        // Ordering: `Relaxed`: the counts are hints.
        if shared.lanes[lane as usize].parked.swap(false, Ordering::Relaxed) {
            shared.parked.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// One turn of worker `lane` on this thread: it polls at most `TURN_TASKS` tasks of its queue,
    /// of its list, or of a steal. `start` is a random value for the order of the steal.
    pub(crate) fn turn(&self, lane: u32, start: u32) -> Turn {
        LANE.with(|cell| cell.set((Arc::as_ptr(&self.shared), lane)));
        TURNING.with(|cell| cell.set(true));
        let turn = self.run_turn(lane, start);
        TURNING.with(|cell| cell.set(false));
        turn
    }

    fn run_turn(&self, lane: u32, start: u32) -> Turn {
        for polled in 0..TURN_TASKS {
            let Some(slot) = self.next(lane, start, polled) else {
                return if self.ended() { Turn::Ended } else { Turn::Idle };
            };
            self.run(lane, slot);
            // A parked worker that this turn woke steals only after the flush of the wake.
            if WOKE.with(|woke| woke.replace(false)) {
                return Turn::Busy;
            }
        }
        Turn::Busy
    }

    /// The scheduling choice of worker `lane`: the task that it polls next, after the `polled`
    /// tasks of this turn. The first choice of a turn is the oldest entry of its own list, so a
    /// queue that never empties does not starve the list: a spawn or a wake from another thread, or
    /// a cancel. Then come its queue, its list, and a steal.
    fn next(&self, lane: u32, start: u32, polled: u32) -> Option<u32> {
        let shared = &*self.shared;
        let listed = if polled == 0 {
            shared.take_list(lane, None)
        } else {
            None
        };
        listed
            .or_else(|| shared.lanes[lane as usize].queue.pop())
            .or_else(|| shared.take_list(lane, None))
            .or_else(|| self.search(lane, start))
    }

    /// Steals a task for `lane`, if fewer than half of the workers search.
    fn search(&self, lane: u32, start: u32) -> Option<u32> {
        let shared = &*self.shared;
        let lanes = self.lanes();
        // Ordering: `Relaxed`: the count is a hint.
        if lanes < 2 || 2 * shared.searching.load(Ordering::Relaxed) >= lanes {
            return None;
        }
        shared.searching.fetch_add(1, Ordering::Relaxed);
        let found = self.steal(lane, start);
        // The last worker that searches and found work wakes another one, so work spreads.
        if shared.searching.fetch_sub(1, Ordering::Relaxed) == 1 && found.is_some() {
            shared.balance();
        }
        found
    }

    /// Steals from the workers of the cache domain of `lane` first, and then from the others, each
    /// time from `start`.
    fn steal(&self, lane: u32, start: u32) -> Option<u32> {
        let shared = &*self.shared;
        let total = shared.count;
        let (first, length) = shared.lanes[lane as usize].group;
        let near = (0..length).map(|index| first + start.wrapping_add(index) % length);
        let rest = total - length;
        let far = (0..rest).map(|index| (first + length + start.wrapping_add(index) % rest) % total);
        near.chain(far)
            .map(|place| shared.order[place as usize])
            .filter(|&victim| victim != lane)
            .find_map(|victim| self.steal_from(lane, victim))
    }

    /// Takes the older half of the queue of `victim`, or a batch from its remote-ready list.
    fn steal_from(&self, lane: u32, victim: u32) -> Option<u32> {
        let shared = &*self.shared;
        if let Some(slot) = shared.lanes[victim as usize]
            .queue
            .steal_into(&shared.lanes[lane as usize].queue)
        {
            return Some(slot);
        }
        shared.take_list(victim, Some(&shared.lanes[lane as usize].queue))
    }

    /// Polls the task in `slot`, or drops it at this poll boundary if a cancel was requested.
    fn run(&self, lane: u32, slot: u32) {
        let shared = &*self.shared;
        let word = &shared.words[slot as usize];
        let cancel = slot::poll_start(word);
        // Ordering: `Relaxed`: a wake reads it as a hint, and this thread reads its own store.
        shared.location[slot as usize].store(lane, Ordering::Relaxed);
        #[expect(unsafe_code, reason = "the poll start made this worker the holder of the slot")]
        // SAFETY: this worker took the one queue entry of the slot, and its poll start made it the
        // poller, so it is the one accessor of the cell until the poll ends.
        let mut movable = unsafe { self.cells.take(slot) }.expect("a scheduled slot holds its task");
        if cancel {
            if slot::closing(word) {
                // Ordering: `Relaxed`: a count, which the join of the worker publishes.
                self.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            let outcome = movable.task.as_mut().end(End::Cancelled);
            return self.finish(slot, movable, outcome);
        }
        let Movable { task, waker } = &mut movable;
        let polled = catch_unwind(AssertUnwindSafe(|| {
            crate::coop::with_budget(self.poll_budget, || task.as_mut().poll(&mut Context::from_waker(waker)))
        }));
        match polled {
            Ok(Poll::Pending) => {
                #[expect(unsafe_code, reason = "the poll end has not come, so this worker holds the slot")]
                // SAFETY: as above. The put comes before the poll end, which publishes it.
                unsafe {
                    self.cells.put(slot, movable);
                }
                if slot::poll_end(word) {
                    shared.queue(lane, slot);
                }
            }
            Ok(Poll::Ready(outcome)) => self.finish(slot, movable, outcome),
            Err(payload) => {
                let outcome = movable.task.as_mut().end(End::Panicked(payload));
                self.finish(slot, movable, outcome);
            }
        }
    }

    /// Ends the task in `slot`: frees its storage, its charge, and its slot, and then shows its
    /// `outcome` to its join handle.
    fn finish(&self, slot: u32, movable: Movable, outcome: Option<Staged>) {
        slot::end(&self.shared.words[slot as usize]);
        discard(movable);
        // The slot is free before its reservation is, so each reservation finds a free slot.
        self.push_free(slot);
        self.release();
        // A panic of the drop of an output that no join takes is contained.
        discard(outcome);
    }
}

/// The root future of worker `lane` of `pool`: it runs the turns of the worker until the pool
/// closed and no task remains. `start` gives the random start of each steal.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub(crate) fn serve<'a>(
    pool: &'a Pool,
    lane: u32,
    mut start: impl FnMut() -> u32 + 'a,
) -> impl Future<Output = ()> + 'a {
    core::future::poll_fn(move |cx| match pool.turn(lane, start()) {
        Turn::Busy => {
            // The driver and the local tasks of the worker turn, and then this future runs again.
            cx.waker().wake_by_ref();
            Poll::Pending
        }
        Turn::Idle => Poll::Pending,
        Turn::Ended => Poll::Ready(()),
    })
}

impl From<PoolRefused> for super::Refused {
    fn from(refused: PoolRefused) -> Self {
        match refused {
            PoolRefused::Budget(exhausted) => Self::Budget(exhausted),
            PoolRefused::Allocation => Self::Allocation,
        }
    }
}

impl core::fmt::Debug for Pool {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Pool")
            .field("lanes", &self.lanes())
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, loom))]
mod explore;
// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(all(not(loom), target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod tests;
