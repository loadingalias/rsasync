//! The spawn of a factory on an owner from another thread.
//!
//! The caller reserves the charge of the task and a free slot of the owner (`Shared::reserve_slot`),
//! makes the channel of the outcome, and gives a [`Job`] to the inbox of the owner. On the thread of
//! the owner, the job calls the factory and puts its future in the reserved slot. A refusal before
//! the job exists keeps no reservation and gives the factory back. A job that drops before it runs,
//! such as a job in the inbox of an owner that stops, gives its reservation back, and its task ends
//! with [`Outcome::Cancelled`]. Only an owner that closed drops such a job, so no waiting spawn of
//! the owner needs that slot.
//!
//! A cancel can come before the job runs. The cancel stores its request and then reads the slot of
//! the task. The job stores the slot and then reads the request. Both use `SeqCst`, so at least one
//! of them sees the other, and the owner gets the cancel.

use alloc::boxed::Box;
use core::any::Any;
use core::future::Future;
use core::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use super::owner::{discard, task_size};
use super::slot::Shared;
use super::{CancelHandle, JoinHandle, Outcome, Panic, Reason, SpawnRefused, Target};
use crate::capacity::{Budget, Permit};
use crate::channel::oneshot;
use crate::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};

/// The slot of a task that its job did not place yet.
const UNPLACED: u32 = u32::MAX;

/// The side of an owner that another thread uses to spawn on it.
#[derive(Clone)]
pub(crate) struct Placer {
    shared: Arc<Shared>,
    budget: Budget,
}

impl Placer {
    pub(super) fn new(shared: &Arc<Shared>, budget: &Budget) -> Self {
        Self {
            shared: Arc::clone(shared),
            budget: budget.clone(),
        }
    }

    /// Reserves a free slot of the owner and the charge of the task of `factory`, and gives the
    /// job that spawns it and the handle of the task. The charge also holds the job.
    ///
    /// # Errors
    ///
    /// [`Reason::NoSlot`] when each free slot is reserved, and [`Reason::Budget`] when the budget has
    /// too few free units. The refusal keeps no reservation.
    pub(crate) fn prepare<Fa, F>(&self, factory: Fa) -> Result<(Job, JoinHandle<F::Output>), SpawnRefused<Fa>>
    where
        Fa: FnOnce() -> F + Send + 'static,
        F: Future + 'static,
        F::Output: Send + 'static,
    {
        let outcome = oneshot::size::<Outcome<F::Output>>() as u64;
        // The job, the cancel cell, and its two counts.
        let job = size_of::<Spawn<Fa, F::Output>>() + size_of::<Remote>() + 2 * size_of::<usize>();
        let reserved = self
            .budget
            .reserve(task_size::<F>().saturating_add(job as u64))
            .and_then(|mut charge| {
                let outcome = charge.split(outcome)?;
                Ok((charge, outcome))
            });
        let (charge, outcome) = match reserved {
            Ok(charges) => charges,
            Err(exhausted) => {
                return Err(SpawnRefused {
                    future: factory,
                    reason: Reason::Budget(exhausted),
                });
            }
        };
        // The slot comes after the charge: no refusal after this point gives a reservation back,
        // so no spawn of the owner waits for a slot that is free.
        if !self.shared.reserve_slot() {
            return Err(SpawnRefused {
                future: factory,
                reason: Reason::NoSlot,
            });
        }
        let (sender, receiver) = oneshot::with_charge(outcome);
        let remote = alloc::sync::Arc::new(Remote {
            shared: Arc::clone(&self.shared),
            requested: AtomicBool::new(false),
            slot: AtomicU32::new(UNPLACED),
            generation: AtomicU64::new(0),
        });
        let job = Job(Box::new(Spawn {
            factory: Some(factory),
            sender: Some(sender),
            charge: Some(charge),
            remote: alloc::sync::Arc::clone(&remote),
            reserved: true,
        }));
        let handle = JoinHandle {
            receiver,
            cancel: CancelHandle(Target::Remote(remote)),
        };
        Ok((job, handle))
    }
}

/// A factory spawn on its way to its owner.
pub(crate) struct Job(Box<dyn Run>);

impl Job {
    /// Calls the factory and spawns its future, on the thread of the owner of the job. With no
    /// owner, or on an owner that closed, the job drops, and its task ends with `Cancelled`. A
    /// panic of the factory ends the task with `Panicked`.
    pub(crate) fn run(self) {
        self.0.run();
    }

    /// Gives back the factory of a job that did not reach its owner, and the reservation of the
    /// job. `Fa` and `T` are the types of [`Placer::prepare`] that made the job.
    pub(crate) fn into_factory<Fa: Send + 'static, T: Send + 'static>(self) -> Fa {
        let mut spawn = self
            .0
            .into_any()
            .downcast::<Spawn<Fa, T>>()
            .expect("the job holds a factory of these types");
        // The caller gets no handle, so the task needs no outcome.
        spawn.sender = None;
        spawn.factory.take().expect("a job that did not run holds its factory")
    }
}

impl core::fmt::Debug for Job {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Job").finish_non_exhaustive()
    }
}

trait Run: Send {
    fn run(self: Box<Self>);

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

struct Spawn<Fa, T> {
    factory: Option<Fa>,
    sender: Option<oneshot::Sender<Outcome<T>>>,
    charge: Option<Permit>,
    remote: alloc::sync::Arc<Remote>,
    /// The job holds the reservation of a slot, which its drop gives back.
    reserved: bool,
}

impl<Fa, F, T> Run for Spawn<Fa, T>
where
    Fa: FnOnce() -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    fn run(mut self: Box<Self>) {
        let Some(local) = super::current() else {
            return;
        };
        assert!(
            Arc::ptr_eq(local.shared(), &self.remote.shared),
            "a job runs on the owner that it reserved a slot of"
        );
        if local.closed() {
            return;
        }
        let (Some(factory), Some(sender), Some(charge)) = (self.factory.take(), self.sender.take(), self.charge.take())
        else {
            return;
        };
        match catch_unwind(AssertUnwindSafe(factory)) {
            Ok(future) => {
                self.reserved = false;
                let task = local.place(future, sender, charge);
                self.remote.placed(task.slot, task.generation);
            }
            Err(payload) => {
                self.reserved = false;
                local.release_slot();
                if let Err(unjoined) = sender.send(Outcome::Panicked(Panic(payload))) {
                    discard(unjoined);
                }
            }
        }
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}

impl<Fa, T> Drop for Spawn<Fa, T> {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take()
            && let Err(unjoined) = sender.send(Outcome::Cancelled)
        {
            discard(unjoined);
        }
        if self.reserved {
            self.remote.shared.release_slot();
        }
    }
}

/// The target of the cancel handle of a task that a factory spawn makes.
pub(super) struct Remote {
    shared: Arc<Shared>,
    requested: AtomicBool,
    /// The slot of the task, or `UNPLACED`.
    slot: AtomicU32,
    /// The generation of the task in `slot`.
    generation: AtomicU64,
}

impl Remote {
    /// Requests the cancel of the task, before or after its job placed it.
    pub(super) fn cancel(&self) {
        // Ordering: `SeqCst`: this store and the load of `slot`, and the store of `slot` and the
        // load of `requested` in `placed`, have one total order, so one of the loads sees the store.
        self.requested.store(true, Ordering::SeqCst);
        let slot = self.slot.load(Ordering::SeqCst);
        if slot != UNPLACED {
            // Ordering: `Relaxed`: the load of `slot` above read the release of `placed`.
            let generation = self.generation.load(Ordering::Relaxed);
            self.shared.wake(slot, generation, true);
        }
    }

    /// Records the slot of the task, and requests its cancel if a cancel came before.
    fn placed(&self, slot: u32, generation: u64) {
        // Ordering: `Relaxed`: the store of `slot` below publishes it.
        self.generation.store(generation, Ordering::Relaxed);
        // Ordering: `SeqCst`, as in `cancel`.
        self.slot.store(slot, Ordering::SeqCst);
        if self.requested.load(Ordering::SeqCst) {
            self.shared.wake(slot, generation, true);
        }
    }
}
