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
//! no room gets a write of its interrupt descriptor, so no wake waits for room. On epoll, a message
//! fails with `EBADFD`, and the driver writes the interrupt descriptor of its home instead.
//!
//! The driver is in the owner, so a task reaches it with `with_driver`. A worker has its driver
//! from its start. The owner of `block_on` makes its driver at the first operation that needs a
//! home, and its home gets its ring then (`slot`).

use alloc::rc::Rc;
use core::future::Future;
use std::io;

use super::Refused;
use super::owner::{Enter, Local, Park, Until};
use super::slot::Home;
use crate::capacity::Budget;
use crate::clock::NativeClock;
use crate::driver::{self, Config, Driver};
use crate::sync::{Arc, thread};
use crate::time::TimeError;
use crate::timer::Timers;

/// A local owner whose home waits in the ring of its driver.
pub(crate) struct RingOwner {
    local: Rc<Local>,
}

impl RingOwner {
    /// Makes an owner with `slots` task slots, on this thread, whose home is `driver`.
    pub(crate) fn new(budget: &Budget, slots: u32, driver: Driver) -> Result<Self, Refused> {
        let local = Local::new(
            budget,
            slots,
            Home::new(thread::current(), Some(driver.home())),
            driver::MESSAGES,
        )?;
        local.driver.replace(Some(driver));
        local.sends.set(true);
        Ok(Self { local: Rc::new(local) })
    }

    /// Makes an owner with `slots` task slots, on this thread, that makes its driver with `config`
    /// at the first operation that needs a home (`with_driver`).
    pub(crate) fn lazy(budget: &Budget, slots: u32, config: Config) -> Result<Self, Refused> {
        let local = Local::new(budget, slots, Home::new(thread::current(), None), driver::MESSAGES)?;
        local.lazy.set(Some(config));
        Ok(Self { local: Rc::new(local) })
    }

    /// Gives the owner a timer storage, which `clock` drives, as `LocalOwner::use_timers` does.
    pub(crate) fn use_timers(&mut self, timers: Timers, clock: NativeClock) -> Result<(), TimeError> {
        self.local.use_timers(timers, clock)
    }

    /// Gives the owner the generator from which it derives the generator of each caller of
    /// `task::generator`.
    pub(crate) fn use_generator(&mut self, generator: rscrypto::ChaCha20Rng) {
        self.local.generator.replace(Some((generator, 0)));
    }

    /// The side of this owner that another thread uses to spawn on it.
    pub(crate) fn placer(&self, budget: &Budget) -> super::factory::Placer {
        super::factory::Placer::new(self.local.shared(), budget)
    }

    /// Makes this thread the owner, and polls `root`, the tasks of the owner, and the driver until
    /// `root` gives its output, as `LocalOwner::block_on` does. Then it tears the driver down: it
    /// cancels each operation that the driver homes, drives it until each terminal event, and
    /// closes each resource that remains. The owner ends with the call.
    ///
    /// # Errors
    ///
    /// A failure of the driver. If it ends the loop, each live task ends with `OwnerFailed`. If the
    /// teardown fails, the output of `root` drops.
    pub(crate) fn block_on<F: Future>(self, root: F) -> io::Result<F::Output> {
        let mut enter = Enter::new(&self.local);
        let output = self.local.block_on(root, &mut Drive);
        enter.ended = output.is_ok();
        let output = output?;
        // The slot stays borrowed through the teardown, and the owner makes no other driver: a
        // waker that the teardown calls and that asks for the driver gets `InUse`.
        self.local.lazy.set(None);
        let mut slot = self.local.driver.borrow_mut();
        if let Some(driver) = slot.take() {
            // The report counts the operations that the teardown cancelled, which their futures saw.
            let _cancelled = driver.teardown()?;
        }
        Ok(output)
    }
}

impl Drop for RingOwner {
    /// Detaches the ring of the home: a waker that the caller keeps holds no descriptor of it.
    fn drop(&mut self) {
        self.local.shared().detach_ring();
    }
}

/// The work and the wait of a home that can have a ring: the driver of the owner, while it has
/// one, and else its thread.
struct Drive;

impl Park for Drive {
    type Error = io::Error;

    fn drive(&mut self, local: &Local) -> io::Result<bool> {
        let mut driver = local.driver.borrow_mut();
        let Some(driver) = driver.as_mut() else {
            return Ok(false);
        };
        flush(local, driver);
        Ok(driver.turn()?.more())
    }

    fn park(&mut self, local: &Local, until: Option<Until>) -> io::Result<()> {
        // The home got its ring in this turn. A wake that did not see the ring unparked the thread,
        // so the next turn takes the list and the root flag before a wait in the ring.
        if local.switched.take() {
            return Ok(());
        }
        let mut driver = local.driver.borrow_mut();
        let Some(driver) = driver.as_mut() else {
            let Ok(()) = super::owner::Thread.park(local, until);
            return Ok(());
        };
        // The turn of `drive` can wake a task of another home after its own flush.
        flush(local, driver);
        match until {
            Some(until) => driver.wait_for(until.remaining()),
            None => driver.wait(),
        }
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

/// Puts `home` in the outbox of the owner of this thread. Gives `false` if the owner has no driver
/// or its outbox has no room: the caller then writes the interrupt descriptor of `home`.
pub(super) fn post(home: &Arc<driver::Home>) -> bool {
    super::CURRENT.with(|current| {
        let Ok(current) = current.try_borrow() else {
            return false;
        };
        let Some(local) = current.as_ref() else {
            return false;
        };
        if !local.sends.get() {
            return false;
        }
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

impl Local {
    /// Calls `f` with the driver of this owner. The owner of `block_on` makes its driver here the
    /// first time, and its home gets its ring.
    #[cfg_attr(
        not(all(test, not(loom))),
        expect(dead_code, reason = "the local I/O types of R307 build on it")
    )]
    pub(super) fn with_driver<R>(&self, f: impl FnOnce(&mut Driver) -> R) -> Result<R, super::DriverRefused> {
        let mut slot = self.driver.try_borrow_mut().map_err(|_| super::DriverRefused::InUse)?;
        if slot.is_none() {
            let config = self.lazy.get().ok_or(super::DriverRefused::NoDriver)?;
            let driver = Driver::new(config).map_err(super::DriverRefused::Start)?;
            self.shared().set_ring(driver.home());
            self.sends.set(true);
            self.switched.set(true);
            *slot = Some(driver);
        }
        let driver = slot.as_mut().expect("the owner has its driver");
        Ok(f(driver))
    }
}

#[cfg(test)]
mod tests;
