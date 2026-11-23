//! The thread of one local worker: it applies its placement, makes its driver, its tables, and its
//! timers on itself, reports that it is ready, and then runs the jobs of its inbox until the stop.

use core::num::NonZeroU32;
use core::time::Duration;
use std::io;

use rscrypto::ChaCha20Rng;

use super::placement::{Fallback, Placed, Selection};
use super::{TimersError, WorkerError, WorkerReport, sys};
use crate::blocking::Submitter;
use crate::capacity::Budget;
use crate::channel::{mpsc, oneshot};
use crate::clock::NativeClock;
use crate::driver::{Config, Driver};
use crate::sync::Arc;
use crate::task::{self, Drain, Ended, Failure, Job, Placer, Pool, RingOwner, with_driver};
use crate::time::Suspend;
use crate::timer::Timers;

/// A message in the inbox of a worker.
pub(super) enum Message {
    /// A factory spawn on the worker.
    Job(Job),
    /// The end of the worker: the inbox closes, and each job in it ends with `Cancelled`. The
    /// operations of the worker can end by themselves until the end of the drain
    /// ([`RingOwner::serve`]).
    Stop(Drain),
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

/// What a worker runs.
pub(super) enum Role {
    /// Its own tasks, and the factory spawns of its inbox.
    Local,
    /// The turns of `lane` of the movable pool.
    Movable(u32),
}

/// What a worker needs to start.
pub(super) struct Start {
    pub(super) role: Role,
    /// The movable pool of the runtime, if it has one: the pool of `task::spawn_movable` on this
    /// thread.
    pub(super) pool: Option<Arc<Pool>>,
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
    let role = match start.role {
        Role::Local => None,
        Role::Movable(lane) => Some(lane),
    };
    let pool = start.pool.clone();
    match prepare(start) {
        Ok((owner, report, steal)) => {
            if let (Some(lane), Some(pool)) = (role, &pool) {
                owner.attach(pool, lane);
            }
            let _none = task::use_pool(pool.clone());
            let placer = owner.placer(&budget);
            // The start waits for the report, so only a start that ended can refuse it.
            if ready.send(Ok((report, placer))).is_err() {
                return Ok(Ended::default());
            }
            let served = match (role, pool) {
                (Some(lane), Some(pool)) => {
                    let served = owner.serve(serve_movable(Arc::clone(&pool), lane, inbox, steal));
                    // The tasks of this worker wait in its queue and its list for a thief.
                    if served.is_err() {
                        pool.worker_failed();
                    }
                    served
                }
                _ => owner.serve(serve(inbox)),
            };
            served.map_err(|failure| match failure {
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
/// thread, so that their memory is local to its node. Gives the owner, the report, and the
/// generator of the steals of a movable worker.
fn prepare(start: Start) -> Result<(RingOwner, WorkerReport, ChaCha20Rng), WorkerError> {
    let Start {
        role: _,
        pool: _,
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
    let steal = generator.child(b"steal\0\0\0", 0);
    owner.use_generator(generator);
    let report = WorkerReport {
        placement: plan,
        driver: report,
    };
    Ok((owner, report, steal))
}

/// The root of a movable worker: it runs the turns of `lane` of `pool` until the pool closed and
/// its last task ended. Each steal starts at a value of `steal`. Then it takes the stop, which the
/// close sends, and gives its drain, and the count of `Driver::dropped_sent` at that time.
async fn serve_movable(
    pool: Arc<Pool>,
    lane: u32,
    mut inbox: mpsc::Receiver<Message>,
    mut steal: ChaCha20Rng,
) -> (Drain, u64) {
    task::serve_pool(&pool, lane, || steal.next_u32()).await;
    pool.detach(lane);
    // A movable worker gets no factory spawn, only its stop.
    let drain = loop {
        match inbox.recv().await {
            Ok(Message::Stop(drain)) => break drain,
            Ok(Message::Job(job)) => drop(job),
            Err(_) => break Drain::Now,
        }
    };
    inbox.close();
    let sent = with_driver((), |driver, ()| driver.dropped_sent()).unwrap_or(0);
    (drain, sent)
}

/// The root of a worker: it runs each job of the inbox until the stop. Then it closes the inbox,
/// and each job that waits there drops, so its task ends with `Cancelled`.
/// Gives the drain of the stop, and the count of `Driver::dropped_sent` at the stop, so the end
/// counts only what the stop ended.
async fn serve(mut inbox: mpsc::Receiver<Message>) -> (Drain, u64) {
    let drain = loop {
        match inbox.recv().await {
            Ok(Message::Job(job)) => job.run(),
            Ok(Message::Stop(drain)) => break drain,
            // No stop reaches a closed inbox, so the teardown comes at once.
            Err(_) => break Drain::Now,
        }
    };
    inbox.close();
    while let Ok(message) = inbox.try_recv() {
        drop(message);
    }
    let sent = with_driver((), |driver, ()| driver.dropped_sent()).unwrap_or(0);
    (drain, sent)
}
