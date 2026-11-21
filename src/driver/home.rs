//! The home of a local owner whose ring is a driver, the wake messages that a driver sends to
//! such homes, and the closes that the drops of local resources record at their home. Only the
//! local owner of `rsasync::task` and the types of `rsasync::local` (the `local` feature) use them.

use alloc::boxed::Box;
use core::alloc::AllocError;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::io::{self, Write as _};
use std::os::fd::{AsRawFd as _, OwnedFd};

use super::queue::Queue;
use super::{Completion, Driver, Resource, Shared};
use crate::driver::out_of_memory;
use crate::memory::items;
use crate::sync::{Arc, try_arc};
use crate::uring::Sqe;

/// The mark of a word of [`Closes`] that holds a record, above the generation of its resource.
const POSTED: u64 = 1 << 32;

/// The closes that the drops of local resources record when they cannot reach the driver: one word
/// for each resource slot, 0 or the generation of its resource with [`POSTED`]. The driver makes
/// the words at its start, so a record needs no free slot.
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
    /// Ends a wait of the home from a thread with no ring: it writes the interrupt descriptor,
    /// which the wait polls in the ring.
    pub(crate) fn interrupt(&self) {
        // A full counter is a pending wake already, and nothing else can fail here.
        drop((&self.shared.interrupt).write(&1_u64.to_ne_bytes()));
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
    /// reach its home, and the interrupt descriptor wakes the home instead.
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

    /// Starts each close that a drop recorded ([`Cleanup::post`]).
    pub(super) fn take_closes(&mut self) {
        if !self.shared.closes.posted.swap(false, Ordering::Acquire) {
            return;
        }
        for index in 0..self.resources.slots.len() {
            let word = self
                .shared
                .closes
                .words
                .get(index)
                .map_or(0, |word| word.swap(0, Ordering::Relaxed));
            // A word with no record gives `POSTED`, which does not fit a generation.
            if let Ok(generation) = u32::try_from(word ^ POSTED) {
                self.close_dropped_resource(Resource {
                    driver: self.resources.driver,
                    index: u32::try_from(index).unwrap_or(u32::MAX),
                    generation,
                });
            }
        }
    }

    /// Sends a wake message from this ring to `home`: the next submit sends it, and its completion
    /// at `home` ends a wait there. The driver keeps `home` until the completion of the message at
    /// this ring, and a failed message writes the interrupt descriptor of `home` instead. Gives
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

    /// Writes the interrupt descriptor of the home of each message whose completion this driver
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
