//! The thread of one local worker: it applies its placement, makes its driver, its tables, and its
//! timers on itself, reports that it is ready, and then runs the jobs of its inbox until the stop.

use core::num::NonZeroU32;
use core::time::Duration;
use std::io;

use rscrypto::ChaCha20Rng;

use super::placement::{Fallback, Placed, Selection};
use super::{TimersError, WorkerError, WorkerReport, sys};
use crate::capacity::Budget;
use crate::channel::{mpsc, oneshot};
use crate::clock::NativeClock;
use crate::driver::{Config, Driver};
use crate::task::{Job, Placer, RingOwner};
use crate::time::Suspend;
use crate::timer::Timers;

/// A message in the inbox of a worker.
pub(super) enum Message {
    /// A factory spawn on the worker.
    Job(Job),
    /// The end of the worker: the inbox closes, and each job in it ends with `Cancelled`.
    Stop,
}

impl Message {
    /// The job of the message, if it has one.
    pub(super) fn into_job(self) -> Option<Job> {
        match self {
            Self::Job(job) => Some(job),
            Self::Stop => None,
        }
    }
}

/// What a worker needs to start.
pub(super) struct Start {
    pub(super) index: u32,
    pub(super) plan: Placed,
    pub(super) selection: Selection,
    pub(super) budget: Budget,
    pub(super) slots: u32,
    pub(super) driver: Config,
    pub(super) timers: (NonZeroU32, Duration),
    pub(super) generator: ChaCha20Rng,
}

/// The ready report of a worker, or why it did not start.
pub(super) type Ready = Result<(WorkerReport, Placer), WorkerError>;

/// The timer storage of an owner, and the native clock that pauses while the system is
/// suspended, which drives it.
pub(super) fn timers(
    budget: &Budget,
    (capacity, resolution): (NonZeroU32, Duration),
) -> Result<(Timers, NativeClock), TimersError> {
    let clock = NativeClock::new(Suspend::Pauses).map_err(TimersError::Clock)?;
    let timers = Timers::new(capacity, resolution, clock.now(), budget).map_err(TimersError::Storage)?;
    Ok((timers, clock))
}

/// The body of the thread of a worker. It gives the cleanup outcome: the result of the teardown of
/// its driver, or `Ok` if it did not start.
pub(super) fn run(start: Start, inbox: mpsc::Receiver<Message>, ready: oneshot::Sender<Ready>) -> io::Result<()> {
    let budget = start.budget.clone();
    match prepare(start) {
        Ok((owner, report)) => {
            let placer = owner.placer(&budget);
            // The start waits for the report, so only a start that ended can refuse it.
            if ready.send(Ok((report, placer))).is_err() {
                return Ok(());
            }
            owner.block_on(serve(inbox))
        }
        Err(error) => {
            let _unreported = ready.send(Err(error));
            Ok(())
        }
    }
}

/// Applies the placement of the worker, and makes its driver, its tables, and its timers on this
/// thread, so that their memory is local to its node.
fn prepare(start: Start) -> Result<(RingOwner, WorkerReport), WorkerError> {
    let Start {
        index: _,
        mut plan,
        selection,
        budget,
        slots,
        driver,
        timers: limits,
        generator,
    } = start;
    if let Some(target) = &plan.target {
        match sys::bind(target) {
            Ok(()) => plan.applied = sys::affinity().ok(),
            Err(error) if selection == Selection::Strict => return Err(WorkerError::Placement(error)),
            Err(error) => plan.fallback = Some(Fallback::Refused(error.raw_os_error().unwrap_or(0))),
        }
    }
    // On Linux, the driver runs io_uring, or epoll, and the home of an owner runs on both.
    let driver = Driver::new(driver).map_err(WorkerError::Driver)?;
    let report = driver.report();
    let mut owner = RingOwner::new(&budget, slots, driver).map_err(WorkerError::Owner)?;
    let (timers, clock) = timers(&budget, limits).map_err(WorkerError::Timers)?;
    owner
        .use_timers(timers, clock)
        .expect("the timers are on the domain of their own clock");
    owner.use_generator(generator);
    let report = WorkerReport {
        placement: plan,
        driver: report,
    };
    Ok((owner, report))
}

/// The root of a worker: it runs each job of the inbox until the stop. Then it closes the inbox,
/// and each job that waits there drops, so its task ends with `Cancelled`.
async fn serve(mut inbox: mpsc::Receiver<Message>) {
    while let Ok(Message::Job(job)) = inbox.recv().await {
        job.run();
    }
    inbox.close();
    while let Ok(message) = inbox.try_recv() {
        drop(message);
    }
}
