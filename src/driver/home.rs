//! The home of a local owner whose ring is a driver, the wake messages that a driver sends to
//! such homes, and the closes that the drops of local resources record at their home. Only the
//! local owner of `rsasync::task` and the types of `rsasync::local` (the `local` feature) use them.

use alloc::boxed::Box;
use core::alloc::AllocError;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::io::{self};
use std::os::fd::{AsRawFd as _, OwnedFd};

use super::queue::Queue;
use super::{Completion, Driver, Resource, Shared};
use crate::driver::out_of_memory;
use crate::memory::{items, room};
use crate::sync::{Arc, try_arc};
use crate::table::OpId;
use crate::uring::Sqe;

/// The mark of a word of [`Closes`] that holds a close, above the generation of its resource.
const POSTED: u64 = 1 << 32;

/// A mark of a word of [`Closes`] other than a close, above the generation of its resource: what a
/// future of the record of the stream did in a turn of the driver ([`Cleanup::post_mark`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub(crate) enum Mark {
    /// The local receive ended its wait.
    Reader = 1 << 33,
    /// The future of the send dropped.
    SendDropped = 1 << 34,
    /// The future of the send asked for a cancel.
    SendCancelled = 1 << 35,
}

/// The closes that the drops of local resources record when they cannot reach the driver, and the
/// marks of the futures of their records: one word for each resource slot, 0 or the generation of
/// its resource with [`POSTED`], marks, or both. The driver makes the words at its start, so a
/// record needs no free slot.
pub(super) struct Closes {
    posted: AtomicBool,
    words: Box<[AtomicU64]>,
}

impl Closes {
    /// No record, with a word for each of `resources` slots.
    pub(super) fn new(resources: u32) -> Result<Self, AllocError> {
        Ok(Self {
            posted: AtomicBool::new(false),
            words: items(resources, |_| AtomicU64::new(0))?,
        })
    }

    /// Whether a record waits for the driver.
    pub(super) fn posted(&self) -> bool {
        self.posted.load(Ordering::Relaxed)
    }
}

/// A task of the local owner of a driver: its slot and its generation. The owner wakes it after a
/// turn of the driver ([`Driver::take_woken`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocalTask {
    pub(crate) slot: u32,
    pub(crate) generation: u64,
}

/// The waiter of a home operation ([`Consumer::Home`](super::Consumer::Home)), or of a local receive
/// of a stream that waits for its multishot receive with no operation.
#[derive(Debug, Default)]
pub(crate) enum Waiter {
    /// No consumer waits.
    #[default]
    None,
    /// The task that the local owner polled, with its own waker.
    Task(LocalTask),
    /// Another waker, such as the waker of a child future of a task.
    Waker(core::task::Waker),
}

impl Waiter {
    /// Records `task` when the waker of the poll is the task's own waker, and else a clone of
    /// `waker`, unless the waiter wakes it already.
    pub(super) fn set(&mut self, task: Option<LocalTask>, waker: &core::task::Waker) {
        match (task, &*self) {
            (Some(task), _) => *self = Self::Task(task),
            (None, Self::Waker(old)) if old.will_wake(waker) => {}
            (None, _) => *self = Self::Waker(waker.clone()),
        }
    }

    /// Wakes the waiter now, outside a turn of its driver: a task goes to the ready queue of the
    /// owner of this thread.
    pub(crate) fn wake(self) {
        match self {
            Self::Task(task) => crate::task::wake_local(task),
            Self::Waker(waker) => waker.wake(),
            Self::None => {}
        }
    }
}

/// The consumers of the home operations of a driver: a waiter for each operation slot, and the
/// tasks that the completions of a turn woke, which the owner takes after the turn. The driver
/// makes both at its start, so a wake needs no allocation: an operation, and the local receive of a
/// stream, each wake at most one task.
pub(super) struct Consumers {
    /// A local type calls the driver: each operation that starts has its consumer at the home.
    pub(super) calls: bool,
    waiters: Box<[Waiter]>,
    woken: alloc::vec::Vec<LocalTask>,
}

impl Consumers {
    /// No waiter, for `operations` operation slots and `resources` resource slots.
    pub(super) fn new(operations: u32, resources: u32) -> Result<Self, AllocError> {
        Ok(Self {
            calls: false,
            waiters: items(operations, |_| Waiter::None)?,
            woken: room(operations as usize + resources as usize)?,
        })
    }

    /// Records the waiter of the home operation `id` ([`Waiter::set`]).
    pub(super) fn wait(&mut self, id: OpId, task: Option<LocalTask>, waker: &core::task::Waker) {
        if let Some(waiter) = self.waiters.get_mut(id.index() as usize) {
            waiter.set(task, waker);
        }
    }

    /// Wakes the waiter of the home operation `id`, which ended.
    pub(super) fn wake(&mut self, id: OpId) {
        if let Some(waiter) = self.waiters.get_mut(id.index() as usize).map(core::mem::take) {
            self.wake_waiter(waiter);
        }
    }

    /// Wakes `waiter`: a task goes to the list that the owner takes, and another waker wakes now.
    pub(super) fn wake_waiter(&mut self, waiter: Waiter) {
        match waiter {
            // The list has room for each operation slot and each resource slot, and each one has
            // one waiter.
            Waiter::Task(task) if self.woken.len() < self.woken.capacity() => self.woken.push(task),
            Waiter::Waker(waker) => waker.wake(),
            _ => {}
        }
    }

    /// Forgets the waiter of the operation `id`, whose consumer ended its interest.
    pub(super) fn clear(&mut self, id: OpId) {
        if let Some(waiter) = self.waiters.get_mut(id.index() as usize) {
            *waiter = Waiter::None;
        }
    }

    /// Gives each task that a completion woke since the last take, and empties the list.
    pub(super) fn take(&mut self) -> alloc::vec::Drain<'_, LocalTask> {
        self.woken.drain(..)
    }

    /// Wakes each waker that waits, at the end of the driver: its consumer then gets `Gone`.
    pub(super) fn end(&mut self) {
        for waiter in &mut self.waiters {
            if let Waiter::Waker(waker) = core::mem::take(waiter) {
                waker.wake();
            }
        }
    }
}

/// The home of a resource that a local type owns: the drop of the type closes the resource there.
#[derive(Debug)]
pub(crate) struct Cleanup {
    shared: Arc<Shared>,
    resource: Resource,
}

impl Cleanup {
    /// Records the close at the home. The drop runs on the thread of the home, whose owner turns
    /// its driver before it waits, and that turn starts the close. A home that ended closed the
    /// resource at its teardown, and nothing reads the record.
    pub(crate) fn post(self) {
        let closes = &self.shared.closes;
        if let Some(word) = closes.words.get(self.resource.index as usize) {
            word.store(POSTED | u64::from(self.resource.generation), Ordering::Relaxed);
            closes.posted.store(true, Ordering::Release);
        }
    }

    /// The resource of the home.
    pub(crate) const fn resource(&self) -> Resource {
        self.resource
    }

    /// Records `mark` at the home, when a future of the record of the stream cannot reach the
    /// driver, in a turn of the driver: the next turn applies it ([`Driver::take_closes`]).
    pub(crate) fn post_mark(&self, mark: Mark) {
        let closes = &self.shared.closes;
        if let Some(word) = closes.words.get(self.resource.index as usize) {
            // A close of the resource that waits in the word keeps its mark. The next turn takes
            // each word before a new resource can take the slot, so the word holds no other
            // generation.
            word.fetch_or(mark as u64 | u64::from(self.resource.generation), Ordering::Relaxed);
            closes.posted.store(true, Ordering::Release);
        }
    }
}

impl Driver {
    /// The home of `resource`, which this driver adopted, for the local type that owns it.
    pub(crate) fn cleanup(&self, resource: Resource) -> Cleanup {
        Cleanup {
            shared: Arc::clone(&self.shared),
            resource,
        }
    }
}

impl Completion {
    /// The home of `resource`, which this operation made, for the local type that owns it.
    pub(crate) fn cleanup(&self, resource: Resource) -> Cleanup {
        Cleanup {
            shared: Arc::clone(&self.shared),
            resource,
        }
    }
}

/// The user data of the completion that a wake message posts at its home. It only ends a wait.
const ARRIVED: u64 = 1 << 62;

/// The user data of the completion of a wake message at its sender, with the index of its entry
/// in `Wakes::sent` in the low bits.
const MESSAGE: u64 = (1 << 62) | (1 << 61);

/// The wake messages that a driver keeps at one time, until their completions. A power of two,
/// so the index of a message is the low bits of its user data.
pub(crate) const MESSAGES: usize = 64;

/// The bits of the index of a message in its user data.
const MESSAGE_INDEX: u64 = MESSAGES as u64 - 1;

/// The home of a local owner whose ring is a driver: the target of a wake from another thread.
pub(crate) struct Home {
    /// A duplicate of the ring descriptor, or of the epoll descriptor, to which a message fails. A
    /// message never names a closed descriptor, whose number another file could take.
    ring: OwnedFd,
    shared: Arc<Shared>,
}

impl Home {
    /// Ends a wait of the home from a thread with no ring: it writes the wake descriptor, which
    /// the wait polls in the ring.
    pub(crate) fn interrupt(&self) {
        self.shared.wake();
    }
}

/// The home of a local owner on a driver, and the wake messages that the driver sent and that
/// have not completed.
pub(super) struct Wakes {
    home: Arc<Home>,
    /// The home of each message. Each one keeps the descriptor of its target open until the
    /// completion of the message.
    sent: [Option<Arc<Home>>; MESSAGES],
}

impl Wakes {
    /// The home on `queue`, with a duplicate of its descriptor.
    pub(super) fn new(queue: &Queue, shared: &Arc<Shared>) -> io::Result<Self> {
        let home = try_arc(Home {
            ring: queue.duplicate()?,
            shared: Arc::clone(shared),
        })
        .map_err(out_of_memory)?;
        Ok(Self {
            home,
            sent: [const { None }; MESSAGES],
        })
    }

    /// Whether `data`, the user data of a control completion, is the completion of a wake message.
    pub(super) const fn is_message(data: u64) -> bool {
        data & !MESSAGE_INDEX == MESSAGE
    }

    /// Ends the wake message of `data`, whose completion has `result`. If it failed, it did not
    /// reach its home, and the wake descriptor wakes the home instead.
    pub(super) fn complete(&mut self, data: u64, result: i32) {
        let index = usize::try_from(data & MESSAGE_INDEX).ok();
        if let Some(home) = index.and_then(|index| self.sent.get_mut(index)?.take())
            && result < 0
        {
            home.interrupt();
        }
    }
}

impl Driver {
    /// The home of a local owner whose ring is this driver.
    pub(crate) fn home(&self) -> Arc<Home> {
        Arc::clone(&self.wakes.home)
    }

    /// Starts the close of the resource of `cleanup` with no operation, when this driver is its
    /// home. Gives `cleanup` back when it is not.
    pub(crate) fn close_dropped(&mut self, cleanup: Cleanup) -> Result<(), Cleanup> {
        if !Arc::ptr_eq(&cleanup.shared, &self.shared) {
            return Err(cleanup);
        }
        self.close_dropped_resource(cleanup.resource);
        Ok(())
    }

    /// Starts each close that a drop recorded ([`Cleanup::post`]), and applies each mark of a future
    /// of a record ([`Cleanup::post_mark`]).
    pub(super) fn take_closes(&mut self) {
        // A load first: a turn with no record changes no shared line.
        if !self.shared.closes.posted.load(Ordering::Relaxed)
            || !self.shared.closes.posted.swap(false, Ordering::Acquire)
        {
            return;
        }
        for index in 0..self.resources.slots.len() {
            let word = self
                .shared
                .closes
                .words
                .get(index)
                .map_or(0, |word| word.swap(0, Ordering::Relaxed));
            let resource = Resource {
                driver: self.resources.driver,
                index: u32::try_from(index).unwrap_or(u32::MAX),
                generation: u32::try_from(word & u64::from(u32::MAX)).unwrap_or_default(),
            };
            // A close ends the wait of a receive and the interest of a send too.
            if word & POSTED != 0 {
                self.close_dropped_resource(resource);
                continue;
            }
            if word & Mark::Reader as u64 != 0 {
                let waiter = self.end_reading(resource);
                self.consumers.wake_waiter(waiter);
            }
            // A full queue asks again at the next turn.
            if word & Mark::SendCancelled as u64 != 0
                && self.cancel_record_send(resource).is_err()
                && let Some(word) = self.shared.closes.words.get(index)
            {
                word.fetch_or(
                    Mark::SendCancelled as u64 | u64::from(resource.generation),
                    Ordering::Relaxed,
                );
                self.shared.closes.posted.store(true, Ordering::Release);
            }
            if word & Mark::SendDropped as u64 != 0 {
                self.end_send(resource);
            }
        }
    }

    /// Calls `f` with this driver in the mode of a call of a local type: each operation that
    /// starts has its consumer at the home ([`Consumer::Home`](super::Consumer::Home)).
    pub(crate) fn home_call<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        let before = core::mem::replace(&mut self.consumers.calls, true);
        let output = f(self);
        self.consumers.calls = before;
        output
    }

    /// Gives each local task that the completions of home operations woke since the last call to
    /// `wake`. Gives whether it gave one.
    pub(crate) fn take_woken(&mut self, mut wake: impl FnMut(LocalTask)) -> bool {
        let mut woke = false;
        for task in self.consumers.take() {
            woke = true;
            wake(task);
        }
        woke
    }

    /// Sends a wake message from this ring to `home`: the next submit sends it, and its completion
    /// at `home` ends a wait there. The driver keeps `home` until the completion of the message at
    /// this ring, and a failed message writes the wake descriptor of `home` instead. Gives
    /// `home` back when no entry or no room in the queue is free.
    #[expect(unsafe_code, reason = "a message names no memory")]
    pub(crate) fn message(&mut self, home: Arc<Home>) -> Result<(), Arc<Home>> {
        let Some(index) = self.wakes.sent.iter().position(Option::is_none) else {
            return Err(home);
        };
        let user_data = MESSAGE | index as u64;
        let entry = Sqe::message(home.ring.as_raw_fd(), ARRIVED, 0, user_data);
        // SAFETY: a message names no memory. The entry of `index` keeps the descriptor of the
        // target open until the completion of the message.
        if unsafe { self.queue.push(&entry, None) }.is_err() {
            return Err(home);
        }
        self.wakes.sent[index] = Some(home);
        Ok(())
    }

    /// Writes the wake descriptor of the home of each message whose completion this driver
    /// has not taken, before a time with no turn: such a message can still be in the queue, or it
    /// can fail with no turn that sees it. A message that arrives then wakes its home one more time.
    /// Each entry stays until the completion of its message, so a later message never takes the
    /// index of a message that the kernel can still complete.
    pub(crate) fn settle_messages(&self) {
        for home in self.wakes.sent.iter().flatten() {
            home.interrupt();
        }
    }
}
