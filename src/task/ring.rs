//! A home whose owner waits in the ring of its driver, and the wake messages of its thread.
//!
//! The owner waits in `io_uring_enter` with `IORING_ENTER_GETEVENTS` (`Driver::wait`), never on
//! the registered event descriptor (`driver::probe`). Each signal of the home ends that wait:
//!
//! - A thread whose owner has a ring sends a message from that ring. A waker cannot use the driver
//!   of its thread, because a turn of the driver calls wakers. So the waker puts the home in the
//!   outbox of its owner, and the owner sends the messages before its next turn or wait, with its
//!   other entries.
//! - Each other thread, such as a thread of the blocking service, writes the interrupt descriptor
//!   of the home. The wait polls it in the ring.
//!
//! The outbox has a fixed room, and so does the table of messages of the driver. A home that gets
//! no room gets a write of its interrupt descriptor, so no wake waits for room.

use alloc::rc::Rc;
use core::future::Future;
use std::io;

use super::Refused;
use super::owner::{Enter, Local, Park};
use super::slot::Home;
use crate::capacity::Budget;
use crate::driver::{self, Driver};
use crate::sync::Arc;

/// A local owner whose home waits in the ring of its driver.
#[cfg_attr(not(test), expect(dead_code, reason = "R304 places workers with it"))]
pub(crate) struct RingOwner {
    local: Rc<Local>,
    driver: Driver,
}

#[cfg_attr(not(test), expect(dead_code, reason = "R304 places workers with it"))]
impl RingOwner {
    /// Makes an owner with `slots` task slots, on this thread, whose home is `driver`.
    pub(crate) fn new(budget: &Budget, slots: u32, driver: Driver) -> Result<Self, Refused> {
        let local = Local::new(budget, slots, Home::Ring(driver.home()))?;
        Ok(Self {
            local: Rc::new(local),
            driver,
        })
    }

    /// Makes this thread the owner, and polls `root`, the tasks of the owner, and the driver until
    /// `root` gives its output, as `LocalOwner::block_on` does.
    ///
    /// # Errors
    ///
    /// A failure of the driver ends the loop. Each live task then ends with `OwnerFailed`.
    pub(crate) fn block_on<F: Future>(&mut self, root: F) -> io::Result<F::Output> {
        let mut enter = Enter::new(&self.local);
        let output = self.local.block_on(root, &mut Drive(&mut self.driver));
        self.driver.settle_messages();
        enter.ended = output.is_ok();
        output
    }
}

/// The work and the wait of a home with a ring.
struct Drive<'a>(&'a mut Driver);

impl Park for Drive<'_> {
    type Error = io::Error;

    fn drive(&mut self, local: &Local) -> io::Result<bool> {
        flush(local, self.0);
        Ok(self.0.turn()?.more())
    }

    fn park(&mut self, local: &Local) -> io::Result<()> {
        // The turn of `drive` can wake a task of another home after its own flush.
        flush(local, self.0);
        self.0.wait()
    }
}

/// Sends a message from `driver` to each home in the outbox of `local`. The next submit of the
/// driver sends them.
fn flush(local: &Local, driver: &mut Driver) {
    for home in local.outbox.borrow_mut().drain(..) {
        if let Err(home) = driver.message(home) {
            home.interrupt();
        }
    }
}

/// Puts `home` in the outbox of the owner of this thread. Gives `false` if the owner has no ring
/// or its outbox has no room: the caller then writes the interrupt descriptor of `home`.
pub(super) fn post(home: &Arc<driver::Home>) -> bool {
    super::CURRENT.with(|current| {
        let Ok(current) = current.try_borrow() else {
            return false;
        };
        let Some(local) = current.as_ref() else {
            return false;
        };
        let Ok(mut outbox) = local.outbox.try_borrow_mut() else {
            return false;
        };
        if outbox.len() == outbox.capacity() {
            return false;
        }
        outbox.push(Arc::clone(home));
        true
    })
}

#[cfg(test)]
mod tests;
