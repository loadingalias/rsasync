//! The submission and completion queues of a driver: the io_uring ring, or the epoll poller, on
//! Linux, the kqueue poller on macOS, and the completion port on Windows. The pollers and the port
//! take the entries of a ring and post the completions that a ring posts for them.

use std::io;
#[cfg(unix)]
use std::os::fd::BorrowedFd;
#[cfg(all(feature = "local", target_os = "linux"))]
use std::os::fd::OwnedFd;

#[cfg(target_os = "windows")]
use super::iocp::{self, Port, Token};
#[cfg(unix)]
use super::poller::{self, Poller, Token};
#[cfg(target_os = "linux")]
use crate::uring::Ring;
use crate::uring::{BufferRing, Cqe, Sqe};

/// The queues of the backend of a driver.
#[derive(Debug)]
pub(crate) enum Queue {
    #[cfg(target_os = "linux")]
    Ring(Ring),
    #[cfg(unix)]
    Poller(Poller),
    #[cfg(target_os = "windows")]
    Port(Port),
}

/// The ready completions of a backend, in order.
pub(crate) enum Completions<'a> {
    #[cfg(target_os = "linux")]
    Ring(crate::uring::Completions<'a>),
    #[cfg(unix)]
    Poller(poller::Completions<'a>),
    #[cfg(target_os = "windows")]
    Port(iocp::Completions<'a>),
}

impl Iterator for Completions<'_> {
    type Item = Cqe;

    fn next(&mut self) -> Option<Cqe> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(completions) => completions.next(),
            #[cfg(unix)]
            Self::Poller(completions) => completions.next(),
            #[cfg(target_os = "windows")]
            Self::Port(completions) => completions.next(),
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
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => unsafe { ring.push(entry) }.map_err(drop),
            #[cfg(unix)]
            Self::Poller(poller) => poller.push(entry, token).map_err(drop),
            #[cfg(target_os = "windows")]
            Self::Port(port) => port.push(entry, token).map_err(drop),
        }
    }

    /// Takes the pushed entries, and returns when `wait` completions are ready, or for the poller
    /// also when a wake came. Gives the number of entries that the backend took.
    pub(crate) fn submit(&mut self, wait: u32) -> io::Result<u32> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => ring.submit(wait),
            #[cfg(unix)]
            Self::Poller(poller) => poller.submit(wait),
            #[cfg(target_os = "windows")]
            Self::Port(port) => port.submit(wait),
        }
    }

    /// Submits as [`Queue::submit`] does, and waits at most until `until`. The end of the time is
    /// no error.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn submit_until(&mut self, wait: u32, until: crate::clock::Until) -> io::Result<u32> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => ring.submit_timeout(wait, until.remaining()),
            #[cfg(unix)]
            Self::Poller(poller) => poller.submit_until(wait, until),
        }
    }

    /// Whether a submit has work with no entry to submit. The poller and the port always look for
    /// events.
    pub(crate) fn needs_enter(&self) -> bool {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => ring.needs_enter(),
            #[cfg(unix)]
            Self::Poller(_) => true,
            #[cfg(target_os = "windows")]
            Self::Port(_) => true,
        }
    }

    /// Whether work remains for the next turn with no entry to submit: deferred work of the
    /// kernel, or events that the last wait of the poller or the port did not take.
    pub(crate) fn more(&self) -> bool {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => ring.needs_enter(),
            #[cfg(unix)]
            Self::Poller(poller) => poller.more(),
            #[cfg(target_os = "windows")]
            Self::Port(port) => port.more(),
        }
    }

    pub(crate) fn entries(&self) -> u32 {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => ring.entries(),
            #[cfg(unix)]
            Self::Poller(poller) => poller.entries(),
            #[cfg(target_os = "windows")]
            Self::Port(port) => port.entries(),
        }
    }

    pub(crate) fn room(&self) -> u32 {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => ring.room(),
            #[cfg(unix)]
            Self::Poller(poller) => poller.room(),
            #[cfg(target_os = "windows")]
            Self::Port(port) => port.room(),
        }
    }

    pub(crate) fn unsubmitted(&self) -> u32 {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => ring.unsubmitted(),
            #[cfg(unix)]
            Self::Poller(poller) => poller.unsubmitted(),
            #[cfg(target_os = "windows")]
            Self::Port(port) => port.unsubmitted(),
        }
    }

    pub(crate) fn ready(&self) -> bool {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => ring.ready(),
            #[cfg(unix)]
            Self::Poller(poller) => poller.ready(),
            #[cfg(target_os = "windows")]
            Self::Port(port) => port.ready(),
        }
    }

    pub(crate) fn completions(&mut self) -> Completions<'_> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => Completions::Ring(ring.completions()),
            #[cfg(unix)]
            Self::Poller(poller) => Completions::Poller(poller.completions()),
            #[cfg(target_os = "windows")]
            Self::Port(port) => Completions::Port(port.completions()),
        }
    }

    /// The descriptor that a host reactor waits for: the wake descriptor of a ring, which the
    /// kernel signals, or the epoll descriptor.
    #[cfg(target_os = "linux")]
    pub(crate) fn readable<'a>(&'a self, wake: BorrowedFd<'a>) -> BorrowedFd<'a> {
        match self {
            Self::Ring(_) => wake,
            Self::Poller(poller) => poller.fd(),
        }
    }

    /// The descriptor that a host reactor waits for: the kqueue.
    #[cfg(target_os = "macos")]
    pub(crate) fn readable(&self) -> BorrowedFd<'_> {
        let Self::Poller(poller) = self;
        poller.fd()
    }

    /// A new descriptor of the backend, the target of the wake messages of local tasks. A message
    /// to the descriptor of a poller fails, and the home is then woken by its interrupt descriptor.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(crate) fn duplicate(&self) -> io::Result<OwnedFd> {
        match self {
            Self::Ring(ring) => ring.duplicate(),
            Self::Poller(poller) => poller.fd().try_clone_to_owned(),
        }
    }

    /// Registers a buffer ring of `entries` entries as the buffer group `group`.
    pub(crate) fn provide(&mut self, entries: u16, group: u16) -> io::Result<BufferRing> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => BufferRing::register(ring, entries, group),
            #[cfg(unix)]
            Self::Poller(poller) => {
                let buffers = BufferRing::unregistered(entries, group)?;
                poller.provide_group(buffers.base(), entries);
                Ok(buffers)
            }
            #[cfg(target_os = "windows")]
            Self::Port(port) => {
                let buffers = BufferRing::unregistered(entries, group)?;
                port.provide_group(buffers.base(), entries);
                Ok(buffers)
            }
        }
    }

    /// Ends the buffer ring of [`Queue::provide`] and frees its memory. A failure leaves the ring
    /// registered, and its memory leaks.
    pub(crate) fn release(&mut self, buffers: BufferRing) -> io::Result<()> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(ring) => buffers.unregister(ring),
            #[cfg(unix)]
            Self::Poller(poller) => {
                poller.release_group();
                buffers.free();
                Ok(())
            }
            #[cfg(target_os = "windows")]
            Self::Port(port) => {
                port.release_group();
                buffers.free();
                Ok(())
            }
        }
    }

    /// The ring, for the parts of the driver and the tests that only io_uring has.
    #[cfg(target_os = "linux")]
    pub(crate) fn ring(&mut self) -> Option<&mut Ring> {
        match self {
            Self::Ring(ring) => Some(ring),
            Self::Poller(_) => None,
        }
    }

    /// The port.
    #[cfg(target_os = "windows")]
    pub(crate) fn port(&mut self) -> &mut Port {
        let Self::Port(port) = self;
        port
    }

    /// The poller.
    #[cfg(unix)]
    pub(crate) fn poller(&mut self) -> Option<&mut Poller> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ring(_) => None,
            Self::Poller(poller) => Some(poller),
        }
    }
}
