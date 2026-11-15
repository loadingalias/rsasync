//! The submission and completion queues of a driver: the io_uring ring, or the epoll poller,
//! which takes the same entries and posts the same completions.

use std::io;
use std::os::fd::BorrowedFd;
#[cfg(feature = "local")]
use std::os::fd::OwnedFd;

use super::epoll::{self, Poller, Token};
use crate::uring::{BufferRing, Cqe, Ring, Sqe};

/// The queues of the backend of a driver.
#[derive(Debug)]
pub(crate) enum Queue {
    Ring(Ring),
    Poller(Poller),
}

/// The ready completions of a backend, in order.
pub(crate) enum Completions<'a> {
    Ring(crate::uring::Completions<'a>),
    Poller(epoll::Completions<'a>),
}

impl Iterator for Completions<'_> {
    type Item = Cqe;

    fn next(&mut self) -> Option<Cqe> {
        match self {
            Self::Ring(completions) => completions.next(),
            Self::Poller(completions) => completions.next(),
        }
    }
}

impl Queue {
    /// Copies `entry` of the resource `token` to the submission queue.
    ///
    /// # Safety
    ///
    /// The contract of [`Ring::push`].
    #[expect(unsafe_code, reason = "the backend uses the memory that the entry names")]
    pub(crate) unsafe fn push(&mut self, entry: &Sqe, token: Option<Token>) -> Result<(), ()> {
        match self {
            // SAFETY: the caller's contract.
            Self::Ring(ring) => unsafe { ring.push(entry) }.map_err(drop),
            Self::Poller(poller) => poller.push(entry, token).map_err(drop),
        }
    }

    /// Takes the pushed entries, and returns when `wait` completions are ready, or for the poller
    /// also when a wake came. Gives the number of entries that the backend took.
    pub(crate) fn submit(&mut self, wait: u32) -> io::Result<u32> {
        match self {
            Self::Ring(ring) => ring.submit(wait),
            Self::Poller(poller) => poller.submit(wait),
        }
    }

    /// Submits as [`Queue::submit`] does, and waits at most for `timeout`. The end of the time is
    /// no error.
    #[cfg(feature = "local")]
    pub(crate) fn submit_timeout(&mut self, wait: u32, timeout: core::time::Duration) -> io::Result<u32> {
        match self {
            Self::Ring(ring) => ring.submit_timeout(wait, timeout),
            Self::Poller(poller) => poller.submit_timeout(wait, timeout),
        }
    }

    /// Whether a submit has work with no entry to submit. The poller always looks for events.
    pub(crate) fn needs_enter(&self) -> bool {
        match self {
            Self::Ring(ring) => ring.needs_enter(),
            Self::Poller(_) => true,
        }
    }

    /// Whether work remains for the next turn with no entry to submit: deferred work of the
    /// kernel, or events that the last wait of the poller did not take.
    pub(crate) fn more(&self) -> bool {
        match self {
            Self::Ring(ring) => ring.needs_enter(),
            Self::Poller(poller) => poller.more(),
        }
    }

    pub(crate) fn entries(&self) -> u32 {
        match self {
            Self::Ring(ring) => ring.entries(),
            Self::Poller(poller) => poller.entries(),
        }
    }

    pub(crate) fn room(&self) -> u32 {
        match self {
            Self::Ring(ring) => ring.room(),
            Self::Poller(poller) => poller.room(),
        }
    }

    pub(crate) fn unsubmitted(&self) -> u32 {
        match self {
            Self::Ring(ring) => ring.unsubmitted(),
            Self::Poller(poller) => poller.unsubmitted(),
        }
    }

    pub(crate) fn ready(&self) -> bool {
        match self {
            Self::Ring(ring) => ring.ready(),
            Self::Poller(poller) => poller.ready(),
        }
    }

    pub(crate) fn completions(&mut self) -> Completions<'_> {
        match self {
            Self::Ring(ring) => Completions::Ring(ring.completions()),
            Self::Poller(poller) => Completions::Poller(poller.completions()),
        }
    }

    /// The descriptor that a host reactor waits for: the wake descriptor of a ring, which the
    /// kernel signals, or the epoll descriptor.
    pub(crate) fn readable<'a>(&'a self, wake: BorrowedFd<'a>) -> BorrowedFd<'a> {
        match self {
            Self::Ring(_) => wake,
            Self::Poller(poller) => poller.fd(),
        }
    }

    /// A new descriptor of the backend, the target of the wake messages of local tasks. A message
    /// to the descriptor of a poller fails, and the home is then woken by its interrupt descriptor.
    #[cfg(feature = "local")]
    pub(crate) fn duplicate(&self) -> io::Result<OwnedFd> {
        match self {
            Self::Ring(ring) => ring.duplicate(),
            Self::Poller(poller) => poller.fd().try_clone_to_owned(),
        }
    }

    /// The descriptor number of the backend.
    #[cfg(test)]
    pub(crate) fn fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd as _;
        match self {
            Self::Ring(ring) => ring.fd(),
            Self::Poller(poller) => poller.fd().as_raw_fd(),
        }
    }

    /// Registers a buffer ring of `entries` entries as the buffer group `group`.
    pub(crate) fn provide(&mut self, entries: u16, group: u16) -> io::Result<BufferRing> {
        match self {
            Self::Ring(ring) => BufferRing::register(ring, entries, group),
            Self::Poller(poller) => {
                let buffers = BufferRing::unregistered(entries, group)?;
                poller.provide_group(buffers.base(), entries);
                Ok(buffers)
            }
        }
    }

    /// Ends the buffer ring of [`Queue::provide`] and frees its memory. A failure leaves the ring
    /// registered, and its memory leaks.
    pub(crate) fn release(&mut self, buffers: BufferRing) -> io::Result<()> {
        match self {
            Self::Ring(ring) => buffers.unregister(ring),
            Self::Poller(poller) => {
                poller.release_group();
                buffers.free();
                Ok(())
            }
        }
    }

    /// The ready completions, which stay for the driver.
    #[cfg(test)]
    pub(crate) fn peek(&self) -> alloc::vec::Vec<Cqe> {
        match self {
            Self::Ring(ring) => ring.peek(),
            Self::Poller(poller) => poller.peek(),
        }
    }

    /// The ring, for the parts of the driver and the tests that only io_uring has.
    pub(crate) fn ring(&mut self) -> Option<&mut Ring> {
        match self {
            Self::Ring(ring) => Some(ring),
            Self::Poller(_) => None,
        }
    }

    /// The poller.
    pub(crate) fn poller(&mut self) -> Option<&mut Poller> {
        match self {
            Self::Ring(_) => None,
            Self::Poller(poller) => Some(poller),
        }
    }
}
