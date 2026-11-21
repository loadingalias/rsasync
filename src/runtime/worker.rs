//! The thread of one local worker: it applies its placement, makes its driver, its tables, and its
//! timers on itself, reports that it is ready, and then runs the jobs of its inbox until the stop.

use core::num::NonZeroU32;
use core::time::Duration;
use std::io;
use std::time::Instant;

use rscrypto::ChaCha20Rng;

use super::placement::{Fallback, Placed, Selection};
use super::{TimersError, WorkerError, WorkerReport, sys};
use crate::blocking::Submitter;
use crate::capacity::Budget;
use crate::channel::{mpsc, oneshot};
use crate::clock::NativeClock;
use crate::driver::{Config, Driver};
use crate::task::{Ended, Failure, Job, Placer, RingOwner, with_driver};
use crate::time::Suspend;
use crate::timer::Timers;

/// A message in the inbox of a worker.
pub(super) enum Message {
    /// A factory spawn on the worker.
    Job(Job),
    /// The end of the worker: the inbox closes, and each job in it ends with `Cancelled`. The
    /// operations of the worker can end by themselves until the deadline ([`RingOwner::serve`]).
    Stop(Option<Instant>),
}

impl Message {
    /// The job of the message, if it has one.
    pub(super) fn into_job(self) -> Option<Job> {
        match self {
            Self::Job(job) => Some(job),
            Self::Stop(_) => None,
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
    /// The blocking service of the runtime, which the driver uses.
    pub(super) blocking: Option<Submitter>,
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

/// The body of the thread of a worker. It gives the cleanup outcome: what its end did, or the
/// failure of the teardown of its driver, or `Ok` with nothing if it did not start.
pub(super) fn run(start: Start, inbox: mpsc::Receiver<Message>, ready: oneshot::Sender<Ready>) -> io::Result<Ended> {
    let budget = start.budget.clone();
    match prepare(start) {
        Ok((owner, report)) => {
            let placer = owner.placer(&budget);
            // The start waits for the report, so only a start that ended can refuse it.
            if ready.send(Ok((report, placer))).is_err() {
                return Ok(Ended::default());
            }
            owner.serve(serve(inbox)).map_err(|failure| match failure {
                Failure::Driver(error) => error,
                // A worker has its driver from its start, so no first use starts one.
                Failure::Start(error) => io::Error::other(error),
            })
        }
        Err(error) => {
            let _unreported = ready.send(Err(error));
            Ok(Ended::default())
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
        blocking,
    } = start;
    if let Some(target) = &plan.target {
        match sys::bind(target) {
            Ok(()) => plan.applied = sys::affinity().ok(),
            Err(error) if selection == Selection::Strict => return Err(WorkerError::Placement(error)),
            Err(error) => plan.fallback = Some(Fallback::Refused(error.raw_os_error().unwrap_or(0))),
        }
    }
    // On Linux, the driver runs io_uring, or epoll, and the home of an owner runs on both.
    let mut driver = Driver::new(driver).map_err(WorkerError::Driver)?;
    if let Some(submitter) = blocking {
        driver.use_blocking(submitter);
    }
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
/// Gives the deadline of the stop, and the count of `Driver::dropped_sent` at the stop, so the end
/// counts only what the stop ended.
async fn serve(mut inbox: mpsc::Receiver<Message>) -> (Option<Instant>, u64) {
    let deadline = loop {
        match inbox.recv().await {
            Ok(Message::Job(job)) => job.run(),
            Ok(Message::Stop(deadline)) => break deadline,
            // No stop reaches a closed inbox, so the teardown comes at once.
            Err(_) => break Some(Instant::now()),
        }
    };
    inbox.close();
    while let Ok(message) = inbox.try_recv() {
        drop(message);
    }
    let sent = with_driver((), |driver, ()| driver.dropped_sent()).unwrap_or(0);
    (deadline, sent)
}
