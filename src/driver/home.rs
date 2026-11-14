//! The home of a local owner whose ring is a driver, and the wake messages that a driver sends to
//! such homes. Only the local owner of `rsasync::task` (the `local` feature) uses them.

use std::io::{self, Write as _};
use std::os::fd::{AsRawFd as _, OwnedFd};

use super::{Driver, Shared};
use crate::sync::Arc;
use crate::uring::{Ring, Sqe};

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
    /// A duplicate of the ring descriptor. A message never names a closed descriptor, whose number
    /// another file could take.
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

    /// Takes the writes of the interrupt descriptor since the last take, or since the last turn of
    /// the driver.
    #[cfg(test)]
    pub(crate) fn take_interrupts(&self) -> u64 {
        use std::io::Read as _;

        let mut count = [0; 8];
        (&self.shared.interrupt)
            .read(&mut count)
            .map_or(0, |_| u64::from_ne_bytes(count))
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
    /// The home on `ring`, with a duplicate of its descriptor.
    pub(super) fn new(ring: &Ring, shared: &Arc<Shared>) -> io::Result<Self> {
        let home = Arc::new(Home {
            ring: ring.duplicate()?,
            shared: Arc::clone(shared),
        });
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
        if unsafe { self.ring.push(&entry) }.is_err() {
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
