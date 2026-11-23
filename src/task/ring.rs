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
use super::owner::{Enter, Local, Park};
use super::slot::Home;
use crate::capacity::Budget;
use crate::clock::{NativeClock, Until};
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
        Ok(Self {
            local: Rc::try_new(local).map_err(|_| Refused::Allocation)?,
        })
    }

    /// Makes an owner with `slots` task slots, on this thread, that makes its driver with `config`
    /// at the first operation that needs a home (`with_driver`). The driver uses the blocking
    /// service of `blocking`.
    pub(crate) fn lazy(
        budget: &Budget,
        slots: u32,
        config: Config,
        blocking: Option<crate::blocking::Submitter>,
    ) -> Result<Self, Refused> {
        let local = Local::new(budget, slots, Home::new(thread::current(), None), driver::MESSAGES)?;
        local.lazy.set(Some((config, blocking)));
        Ok(Self {
            local: Rc::try_new(local).map_err(|_| Refused::Allocation)?,
        })
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

    /// Makes this owner worker `lane` of `pool`: a push to the list of the worker wakes the root
    /// future of this owner.
    pub(crate) fn attach(&self, pool: &super::Pool, lane: u32) {
        pool.attach(lane, Arc::clone(self.local.shared()));
    }

    /// The side of this owner that another thread uses to spawn on it.
    pub(crate) fn placer(&self, budget: &Budget) -> super::factory::Placer {
        super::factory::Placer::new(self.local.shared(), budget)
    }

    /// Runs `root`, the loop of a worker, as [`RingOwner::block_on`] does. `root` gives the drain
    /// of the end and the count of [`Driver::dropped_sent`] at its end. After the tasks ended, the
    /// driver runs until no operation runs or until the end of the drain. Then the teardown cancels
    /// each operation that still runs, and it waits for each terminal event, with no deadline.
    ///
    /// # Errors
    ///
    /// As for [`RingOwner::block_on`].
    pub(crate) fn serve<F: Future<Output = (Drain, u64)>>(self, root: F) -> Result<Ended, Failure> {
        let mut enter = Enter::new(&self.local);
        let output = self.local.block_on(root, &mut Drive);
        enter.ended = output.is_ok();
        let (drain, sent) = output?;
        let cancelled = self.local.cancelled.get();
        self.local.lazy.set(None);
        let mut slot = self.local.driver.borrow_mut();
        let Some(mut driver) = slot.take() else {
            return Ok(Ended {
                cancelled,
                ..Ended::default()
            });
        };
        let mut expired = false;
        while driver.busy() {
            let until = match drain {
                Drain::Until(until) if !until.remaining().is_zero() => Some(until),
                Drain::Now | Drain::Until(_) => {
                    expired = true;
                    break;
                }
                Drain::Unbounded => None,
            };
            if !driver.turn().map_err(Failure::Driver)?.more() && driver.busy() {
                match until {
                    Some(until) => driver.wait_until(until),
                    None => driver.wait(),
                }
                .map_err(Failure::Driver)?;
            }
        }
        let sent = driver.dropped_sent().saturating_sub(sent);
        let teardown = driver.teardown().map_err(Failure::Driver)?;
        Ok(Ended {
            cancelled,
            debt: teardown.cancelled(),
            sent,
            kept: teardown.kept(),
            expired,
        })
    }

    /// Makes this thread the owner, and polls `root`, the tasks of the owner, and the driver until
    /// `root` gives its output, as `LocalOwner::block_on` does. Then it tears the driver down: it
    /// cancels each operation that the driver homes, drives it until each terminal event, and
    /// closes each resource that remains. The owner ends with the call.
    ///
    /// # Errors
    ///
    /// A failure of the driver, or of its start. If it ends the loop, each live task ends with
    /// `OwnerFailed`. If the teardown fails, the output of `root` drops.
    pub(crate) fn block_on<F: Future>(self, root: F) -> Result<F::Output, Failure> {
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
            let _cancelled = driver.teardown().map_err(Failure::Driver)?;
        }
        Ok(output)
    }
}

/// How long the operations of a worker can end by themselves after its tasks ended, before its
/// teardown cancels them ([`RingOwner::serve`]).
#[derive(Debug, Clone, Copy)]
pub(crate) enum Drain {
    /// No time: the teardown comes at once.
    Now,
    /// Until the deadline.
    Until(Until),
    /// Until each operation ended.
    Unbounded,
}

/// What the end of a worker did ([`RingOwner::serve`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ended {
    /// The live tasks that the end cancelled.
    pub(crate) cancelled: u32,
    /// The operations that still ran at the deadline, which the teardown cancelled.
    pub(crate) debt: u32,
    /// The bytes that dropped sends had sent when the closes of their resources ended them.
    pub(crate) sent: u64,
    /// The children with `End::Keep` that still ran at the teardown.
    pub(crate) kept: u32,
    /// The deadline came while an operation still ran.
    pub(crate) expired: bool,
}

/// Why the owner of a home that can have a ring failed.
#[derive(Debug)]
pub(crate) enum Failure {
    /// The driver did not start at its first use.
    Start(driver::Error),
    /// The driver failed.
    Driver(io::Error),
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
    type Error = Failure;

    fn drive(&mut self, local: &Local) -> Result<bool, Failure> {
        self.check(local)?;
        let mut driver = local.driver.borrow_mut();
        let Some(driver) = driver.as_mut() else {
            return Ok(false);
        };
        flush(local, driver);
        Ok(driver.turn().map_err(Failure::Driver)?.more())
    }

    fn check(&mut self, local: &Local) -> Result<(), Failure> {
        local.failure.take().map_or(Ok(()), |error| Err(Failure::Start(error)))
    }

    fn park(&mut self, local: &Local, until: Option<Until>) -> Result<(), Failure> {
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
            Some(until) => driver.wait_until(until),
            None => driver.wait(),
        }
        .map_err(Failure::Driver)
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
    /// Calls `f` with the driver of this owner and `value`, or gives `value` back. The owner of
    /// `block_on` makes its driver here the first time, and its home gets its ring. A driver that
    /// does not start fails the owner: the next turn gives its error (`Drive`).
    pub(super) fn with_driver<V, R>(&self, value: V, f: impl FnOnce(&mut Driver, V) -> R) -> Result<R, V> {
        let Ok(mut slot) = self.driver.try_borrow_mut() else {
            return Err(value);
        };
        if slot.is_none() {
            let Some((config, blocking)) = self.lazy.take() else {
                return Err(value);
            };
            match Driver::new(config.owned()) {
                Ok(mut driver) => {
                    if let Some(submitter) = blocking {
                        driver.use_blocking(submitter);
                    }
                    self.shared().set_ring(driver.home());
                    self.sends.set(true);
                    self.switched.set(true);
                    *slot = Some(driver);
                }
                Err(error) => {
                    self.failure.set(Some(error));
                    return Err(value);
                }
            }
        }
        match slot.as_mut() {
            Some(driver) => Ok(f(driver, value)),
            None => Err(value),
        }
    }
}

// Two attributes, so that Clippy sees the test module. The loom build runs only the exploration
// tests, and the driver tests that these use are not in it.
#[cfg(test)]
#[cfg(not(loom))]
mod tests;
