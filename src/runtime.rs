//! The runtime: local workers, each a thread that owns its tasks and its driver, and
//! [`Runtime::block_on`], which makes the calling thread a local owner for the call.
//!
//! [`Builder::build`] validates the plan, places the workers, and starts them one at a time. Each
//! worker binds its thread, then makes its driver, its tables, and its timers on that thread, so
//! their memory is local to its node, and then reports that it is ready. The runtime is ready only
//! after each worker is. A failure at worker k stops and joins each worker before it, and the
//! [`StartFailure`] gives the cause and the cleanup outcome of each worker that started. No thread
//! of a failed start remains, and a failed start leaves nothing to reuse.
//!
//! ```no_run
//! use std::rc::Rc;
//!
//! use rsasync::runtime::{Placement, Runtime, Selection, WorkerId};
//! use rsasync::task::{Outcome, spawn_local};
//!
//! let runtime = Runtime::builder()
//!     .local_workers(4)
//!     .placement(Placement::Dedicated, Selection::Permissive)
//!     .build()?;
//! let mut handle = runtime.handle()?;
//! let sum = runtime.block_on(async move {
//!     // The factory moves to worker 2, which makes the future there, so the future can hold an
//!     // `Rc` and spawn local tasks on that worker.
//!     let task = handle.spawn_on(WorkerId::new(2), || async {
//!         let shared = Rc::new(20);
//!         let local = spawn_local({
//!             let shared = Rc::clone(&shared);
//!             async move { *shared + 1 }
//!         });
//!         match local.ok()?.join().await {
//!             Outcome::Completed(value) => Some(value + *shared),
//!             _ => None,
//!         }
//!     })?;
//!     match task.join().await {
//!         Outcome::Completed(sum) => Ok::<_, Box<dyn std::error::Error>>(sum),
//!         _ => Err("the task did not complete".into()),
//!     }
//! })??;
//! assert_eq!(sum, Some(41));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Placement
//!
//! [`Placement`] selects where each worker runs, and [`Startup::workers`] reports what each worker
//! asked for, what the plan resolved, and what its thread applied. The default, `Domain`, binds each
//! worker to the CPUs that share one last-level cache and leaves the core to the kernel. On a host
//! with one such domain, it binds nothing. `Dedicated` pins each worker to one CPU, for a host or
//! a CPU set that the process owns. [`Startup::host`] gives the facts of the host, and
//! [`Host::shows_dedication`] says when they show such a set.
//!
//! # Random values
//!
//! The runtime has one generator, rscrypto's [`ChaCha20Rng`], from [`Builder::seed`],
//! [`Builder::generator`], or the operating system. Worker `i` takes the child `worker\0\0` with
//! index `i`, and call `n` of `block_on` takes the child `block_on` with index `n`. So one seed
//! gives each owner the same values again, and no two owners share a stream.
//! [`generator()`](crate::task::generator) gives a task a child of the generator of its owner,
//! which the task owns, so no task can read or replace the generator of its owner.

mod entropy;
mod group;
mod placement;
mod sys;
mod topology;
mod worker;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::cell::Cell;
use core::fmt;
use core::future::Future;
use core::num::{NonZeroU32, NonZeroU64};
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::io;
use std::thread::JoinHandle as Thread;
use std::time::Instant;

pub use group::{Group, GroupError};
pub use placement::{CpuSet, Fallback, MAX_CPUS, Placed, Placement, PlanError, Request, Selection};
pub use rscrypto::ChaCha20Rng;
pub use topology::{Host, Quota};

use crate::blocking::{self, Service, Submitter};
use crate::capacity::Budget;
use crate::channel::{self, TrySendError, mpsc};
use crate::clock;
use crate::driver::{self, Config};
use crate::memory::room;
use crate::sync::{Arc, try_arc};
use crate::task::{self, Ended, JoinHandle, Placer, RingOwner, SpawnRefused};
use crate::timer;
use worker::Message;

/// The task slots of each owner that [`Builder::task_slots`] did not change.
pub const DEFAULT_TASK_SLOTS: u32 = 1024;
/// The bytes of the budget that [`Builder::budget`] did not change: 256 MiB.
pub const DEFAULT_BUDGET: u64 = 256 << 20;
/// The timers of each owner that [`Builder::timers`] did not change.
pub const DEFAULT_TIMERS: u32 = 1024;
/// The resolution of the timers that [`Builder::timers`] did not change: 1 ms.
pub const DEFAULT_TIMER_RESOLUTION: Duration = Duration::from_millis(1);
/// The handles that [`Builder::handles`] did not change.
pub const DEFAULT_HANDLES: u32 = 64;
/// The grace period that [`Builder::grace_period`] did not change: 5 s, inside the 10 s stop timeout
/// of `docker stop` and the 30 s default grace of Kubernetes.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(5);
/// The most local workers of one runtime.
pub const MAX_WORKERS: u32 = placement::MAX_CPUS;

/// The settings of a runtime, from [`Runtime::builder`].
pub struct Builder {
    local_workers: u32,
    task_slots: u32,
    budget: Option<Budget>,
    driver: Config,
    timers: (NonZeroU32, Duration),
    placement: Placement,
    selection: Selection,
    generator: Option<ChaCha20Rng>,
    handles: u32,
    blocking: Option<blocking::Config>,
    grace: Duration,
}

impl Builder {
    /// No local worker, [`DEFAULT_TASK_SLOTS`], a budget of [`DEFAULT_BUDGET`] bytes, the driver of
    /// [`Config::new`], [`DEFAULT_TIMERS`] at [`DEFAULT_TIMER_RESOLUTION`], `Domain` placement that
    /// is permissive, a seed from the operating system, [`DEFAULT_HANDLES`], a blocking service of
    /// [`blocking::Config::new`], and [`DEFAULT_GRACE`].
    fn new() -> Self {
        Self {
            local_workers: 0,
            task_slots: DEFAULT_TASK_SLOTS,
            budget: None,
            driver: Config::new(),
            timers: (
                NonZeroU32::new(DEFAULT_TIMERS).expect("the default is not zero"),
                DEFAULT_TIMER_RESOLUTION,
            ),
            placement: Placement::Domain,
            selection: Selection::Permissive,
            generator: None,
            handles: DEFAULT_HANDLES,
            blocking: Some(blocking::Config::new()),
            grace: DEFAULT_GRACE,
        }
    }

    /// The local workers: threads that each own their tasks and their driver. At most
    /// [`MAX_WORKERS`].
    #[must_use]
    pub fn local_workers(self, workers: u32) -> Self {
        Self {
            local_workers: workers,
            ..self
        }
    }

    /// The task slots of each owner: each worker, and each call of [`Runtime::block_on`]. At least
    /// 1, and at most 2^16.
    #[must_use]
    pub fn task_slots(self, slots: u32) -> Self {
        Self {
            task_slots: slots,
            ..self
        }
    }

    /// The budget that each owner charges its tables, its timers, and each task to.
    #[must_use]
    pub fn budget(self, budget: Budget) -> Self {
        Self {
            budget: Some(budget),
            ..self
        }
    }

    /// The limits and the requirements of the driver of each owner.
    #[must_use]
    pub fn driver(self, config: Config) -> Self {
        Self { driver: config, ..self }
    }

    /// The timers of each owner, at `resolution`, on the native clock that pauses while the system
    /// is suspended.
    #[must_use]
    pub fn timers(self, capacity: NonZeroU32, resolution: Duration) -> Self {
        Self {
            timers: (capacity, resolution),
            ..self
        }
    }

    /// Where the workers run. [`Selection::Strict`] refuses a plan that falls back, before a
    /// worker starts. [`Selection::Permissive`] runs each such worker unpinned, and the report
    /// gives the [`Fallback`].
    #[must_use]
    pub fn placement(self, placement: Placement, selection: Selection) -> Self {
        Self {
            placement,
            selection,
            ..self
        }
    }

    /// Seeds the generator of the runtime with `seed`, so a second runtime with the same seed gives
    /// each owner the same values. Use it to replay a run. Keep a production seed secret.
    #[must_use]
    pub fn seed(self, seed: [u8; ChaCha20Rng::SEED_SIZE]) -> Self {
        self.generator(ChaCha20Rng::from_seed(seed))
    }

    /// Uses `generator` as the generator of the runtime, from which each owner derives its own.
    #[must_use]
    pub fn generator(self, generator: ChaCha20Rng) -> Self {
        Self {
            generator: Some(generator),
            ..self
        }
    }

    /// The handles ([`Runtime::handle`]) that can exist at one time.
    #[must_use]
    pub fn handles(self, handles: u32) -> Self {
        Self { handles, ..self }
    }

    /// The blocking service of the runtime, which each driver of the runtime uses: the spawn of a
    /// child runs on it, and each file call that the backend has no operation for
    /// ([`Route::BlockingService`](driver::Route::BlockingService)), such as each file call on
    /// epoll. The budget charges it, and [`Handle::blocking`] gives its submitter for your own
    /// blocking jobs. `None` starts no service: those calls then refuse with
    /// [`Reason::Blocking`](driver::Reason::Blocking).
    #[must_use]
    pub fn blocking(self, config: Option<blocking::Config>) -> Self {
        Self {
            blocking: config,
            ..self
        }
    }

    /// The grace period of [`Runtime::shutdown`]: the time from the request until the end of the
    /// wait for the operations to end by themselves.
    #[must_use]
    pub fn grace_period(self, grace: Duration) -> Self {
        Self { grace, ..self }
    }

    /// Validates the plan, places the workers, and starts them one at a time. The runtime is
    /// ready after each worker made its driver and its tables and applied its placement.
    ///
    /// The plan reads the topology of the host from `/sys` and the CPU set of the process, and the
    /// seed of the generator from the operating system, unless [`Builder::seed`] gives one.
    ///
    /// # Errors
    ///
    /// [`StartFailure`] with the [`Cause`] and the cleanup outcome of each worker that started.
    pub fn build(self) -> Result<Runtime, StartFailure> {
        let fail = |cause| StartFailure {
            cause,
            cleanup: Vec::new(),
        };
        if self.task_slots == 0 || self.task_slots > task::MAX_SLOTS {
            return Err(fail(Cause::Plan(PlanError::TaskSlots)));
        }
        if self.local_workers > MAX_WORKERS {
            return Err(fail(Cause::Plan(PlanError::TooManyWorkers)));
        }
        let budget = match self.budget {
            Some(budget) => budget,
            None => Budget::try_new(NonZeroU64::new(DEFAULT_BUDGET).expect("the default is not zero"))
                .map_err(|_| fail(Cause::Allocation))?,
        };
        let host = topology::discover().map_err(|_| fail(Cause::Allocation))?;
        let plans = placement::resolve(&self.placement, self.selection, self.local_workers, &host).map_err(fail)?;
        let generator = match self.generator {
            Some(generator) => generator,
            None => entropy::generator().map_err(|error| fail(Cause::Seed(error)))?,
        };
        let mut started = Started::with_capacity(plans.len()).ok_or_else(|| fail(Cause::Allocation))?;
        let service = self
            .blocking
            .map(|config| Service::new(&budget, config))
            .transpose()
            .map_err(|error| fail(Cause::Blocking(error)))?;
        let submitter = service.as_ref().map(Service::submitter);
        let closed = try_arc(AtomicBool::new(false)).map_err(|_| fail(Cause::Allocation))?;
        for (index, plan) in (0..).zip(plans) {
            let start = worker::Start {
                index,
                plan,
                selection: self.selection,
                budget: budget.clone(),
                slots: self.task_slots,
                driver: self.driver,
                timers: self.timers,
                generator: generator.child(b"worker\0\0", u64::from(index)),
                blocking: submitter.clone(),
            };
            if let Err((cause, failed)) = started.start(start, self.handles) {
                let mut cleanup = started.stop();
                cleanup.extend(failed);
                // No thread of the start remains, also no thread of the service.
                if let Some(service) = service {
                    let _teardown = service.teardown();
                }
                return Err(StartFailure { cause, cleanup });
            }
        }
        let Started {
            workers,
            routes,
            reports,
            ..
        } = started;
        Ok(Runtime {
            workers: workers.into_boxed_slice(),
            handle: Handle {
                routes: routes.into_boxed_slice(),
                blocking: submitter,
                closed,
            },
            blocking: service,
            grace: self.grace,
            budget,
            slots: self.task_slots,
            driver: self.driver,
            timers: self.timers,
            generator,
            calls: Cell::new(0),
            report: Startup {
                host,
                workers: reports.into_boxed_slice(),
            },
        })
    }
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The generator is secret.
        f.debug_struct("Builder")
            .field("local_workers", &self.local_workers)
            .field("task_slots", &self.task_slots)
            .field("budget", &self.budget)
            .field("driver", &self.driver)
            .field("timers", &self.timers)
            .field("placement", &self.placement)
            .field("selection", &self.selection)
            .field("handles", &self.handles)
            .field("blocking", &self.blocking)
            .field("grace", &self.grace)
            .finish_non_exhaustive()
    }
}

/// The workers that started, during [`Builder::build`], and the room of the cleanup outcomes of a
/// failed start. Each table has room for each worker, so no push of the start allocates.
struct Started {
    workers: Vec<Thread<io::Result<Ended>>>,
    routes: Vec<Route>,
    reports: Vec<WorkerReport>,
    cleanup: Vec<Cleanup>,
}

impl Started {
    /// The tables of `workers` workers, or `None` if the allocator did not give them.
    fn with_capacity(workers: usize) -> Option<Self> {
        fn table<T>(workers: usize) -> Option<Vec<T>> {
            let mut table = Vec::new();
            table.try_reserve_exact(workers).ok()?;
            Some(table)
        }
        Some(Self {
            workers: table(workers)?,
            routes: table(workers)?,
            reports: table(workers)?,
            cleanup: table(workers)?,
        })
    }

    /// Starts the worker of `start`, and waits until it is ready. A failure gives the cause and,
    /// if its thread started, its cleanup outcome.
    fn start(&mut self, start: worker::Start, handles: u32) -> Result<(), (Cause, Option<Cleanup>)> {
        let index = start.index;
        let failed = |error| (Cause::Worker { index, error }, None);
        let budget = start.budget.clone();
        // A place for each slot, and one for the stop.
        let (sender, inbox) = mpsc::channel(&budget, start.slots.saturating_add(1), handles.saturating_add(1))
            .map_err(|refused| failed(WorkerError::Inbox(refused)))?;
        let (ready, mut ready_wait) =
            channel::oneshot::channel(&budget).map_err(|refused| failed(WorkerError::Inbox(refused)))?;
        let thread = std::thread::Builder::new()
            .name(alloc::format!("rsasync-local-{index}"))
            .spawn(move || worker::run(start, inbox, ready))
            .map_err(|error| failed(WorkerError::Thread(error)))?;
        match ready_wait.recv_blocking() {
            Ok(Ok((report, placer))) => {
                self.workers.push(thread);
                self.routes.push(Route { sender, placer });
                self.reports.push(report);
                Ok(())
            }
            Ok(Err(error)) => Err((Cause::Worker { index, error }, Some(join(index, thread)))),
            // The thread ended with no report: it panicked.
            Err(channel::Closed) => Err((
                Cause::Worker {
                    index,
                    error: WorkerError::Ended,
                },
                Some(join(index, thread)),
            )),
        }
    }

    /// Stops each worker that started, and joins it. Gives the outcome of each, with room for the
    /// outcome of the worker that failed.
    fn stop(&mut self) -> Vec<Cleanup> {
        stop(&mut self.routes, Some(Instant::now()));
        let mut cleanup = core::mem::take(&mut self.cleanup);
        for (index, thread) in (0..).zip(self.workers.drain(..)) {
            cleanup.push(join(index, thread));
        }
        cleanup
    }
}

/// Sends the stop with `deadline` to each worker: the operations of the worker can end by
/// themselves until it, and with no deadline until each one ended. Its inbox has a place for it.
fn stop(routes: &mut [Route], deadline: Option<Instant>) {
    for route in routes {
        // A worker whose driver failed ended, and its inbox is closed.
        let sent = route.sender.try_send(Message::Stop(deadline));
        assert!(
            !matches!(sent, Err(TrySendError::Full(_))),
            "the inbox of a worker has a place for the stop"
        );
    }
}

/// Joins the thread of worker `index`, and gives its cleanup outcome.
fn join(index: u32, thread: Thread<io::Result<Ended>>) -> Cleanup {
    let (outcome, _ended) = finish(thread);
    Cleanup {
        worker: WorkerId(index),
        outcome,
    }
}

/// Joins `thread`, and gives its outcome and what its end did.
fn finish(thread: Thread<io::Result<Ended>>) -> (Outcome, Ended) {
    match thread.join() {
        Ok(Ok(ended)) => (Outcome::Joined, ended),
        Ok(Err(error)) => (Outcome::Failed(error), Ended::default()),
        Err(_payload) => (Outcome::Panicked, Ended::default()),
    }
}

/// A started runtime: its local workers, and [`Runtime::block_on`].
///
/// [`Runtime::shutdown`] stops it with a report. A drop is a backstop with no grace period: it
/// closes the inbox of each worker, which ends each factory spawn that waits there with
/// [`Outcome::Cancelled`](crate::task::Outcome::Cancelled), cancels each task of the worker, tears
/// its driver down, and joins its thread. A drop waits for each task to end, so a task that never
/// returns from its poll stops the drop.
pub struct Runtime {
    workers: Box<[Thread<io::Result<Ended>>]>,
    handle: Handle,
    budget: Budget,
    slots: u32,
    driver: Config,
    timers: (NonZeroU32, Duration),
    generator: ChaCha20Rng,
    /// The calls of `block_on` so far: the index of the generator of the next call.
    calls: Cell<u64>,
    report: Startup,
    /// The blocking service of each driver of the runtime ([`Builder::blocking`]).
    blocking: Option<Service>,
    /// The grace period of [`Runtime::shutdown`].
    grace: Duration,
}

impl Runtime {
    /// The builder of a runtime, with the defaults of each setting.
    #[must_use]
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// What the start found and applied: the host, and the placement and the driver of each worker.
    #[must_use]
    pub const fn report(&self) -> &Startup {
        &self.report
    }

    /// Gives a handle that spawns on the workers. The handle can move to another thread, also to a
    /// task of a worker.
    ///
    /// # Errors
    ///
    /// [`HandleRefused`] when [`Builder::handles`] handles exist, or when the allocator refuses
    /// the memory of the handle.
    pub fn handle(&self) -> Result<Handle, HandleRefused> {
        self.handle.try_clone()
    }

    /// Makes the calling thread a local owner for the call, and polls `root` there until it gives
    /// its output. `root` and the tasks that it spawns with
    /// [`spawn_local`](crate::task::spawn_local) can hold `!Send` state. The first operation that
    /// needs a home makes the driver of the owner, so a root that only waits starts none. The types
    /// of [`rsasync::local`](crate::local) run their operations on that driver.
    ///
    /// After `root` gives its output, `block_on` cancels each task that is still on the owner and
    /// waits until each one ended. It then cancels each operation that the driver homes, drives the
    /// driver until each one reaches its terminal event, closes each resource that remains, and
    /// returns. Work that outlives the root belongs on a worker. A waker of the call that the caller
    /// keeps holds no descriptor of the driver after the return, and a wake through it does nothing.
    ///
    /// # Errors
    ///
    /// [`BlockOnError`] when the owner gets no tables or timers, which gives `root` back, unpolled
    /// ([`BlockOnError::into_root`]), or when its driver fails. A driver failure during the loop
    /// ends each live task with [`OwnerFailed`](crate::task::Outcome::OwnerFailed). A failure of
    /// the final teardown drops the output of `root`.
    pub fn block_on<F: Future>(&self, root: F) -> Result<F::Output, BlockOnError<F>> {
        let call = self.calls.get();
        self.calls.set(call.wrapping_add(1));
        let blocking = self.blocking.as_ref().map(Service::submitter);
        let mut owner = match RingOwner::lazy(&self.budget, self.slots, self.driver, blocking) {
            Ok(owner) => owner,
            Err(refused) => return Err(BlockOnError::Owner(refused, root)),
        };
        let (timers, clock) = match worker::timers(&self.budget, self.timers) {
            Ok(timers) => timers,
            Err(error) => return Err(BlockOnError::Timers(error, root)),
        };
        if let Err(error) = owner.use_timers(timers, clock) {
            return Err(BlockOnError::Time(error, root));
        }
        owner.use_generator(self.generator.child(b"block_on", call));
        owner.block_on(root).map_err(|failure| match failure {
            task::Failure::Start(error) => BlockOnError::Start(error),
            task::Failure::Driver(error) => BlockOnError::Driver(error),
        })
    }
}

impl Runtime {
    /// Stops the runtime in one bounded, reported lifecycle, and gives its [`Shutdown`] report.
    ///
    /// 1. Each spawn refuses from now on, with `Reason::Closed`, and gives its future back.
    /// 2. Each worker closes its inbox, and each factory that waits there ends with
    ///    `Outcome::Cancelled`.
    /// 3. Each task is cancelled at once: its future drops at its next poll, and the drop closes
    ///    its resources, which cancels their operations. A task that never returns from its poll
    ///    stops the shutdown.
    /// 4. Until the end of the grace period ([`Builder::grace_period`]), each operation can end by
    ///    itself. Then each worker closes each resource that remains, cancels each operation that
    ///    still runs, and kills each child with `End::Kill`.
    /// 5. Each worker waits until each operation reaches its terminal event, with no deadline: no
    ///    deadline frees a buffer that the kernel can still use. So the shutdown can outlast its
    ///    grace period, and [`Shutdown::expired`] says when the grace period ended first.
    /// 6. It joins each worker, and then tears the blocking service down.
    ///
    /// A graceful drain of work in flight is the work of the application: end your tasks, for
    /// example with a message, before you call `shutdown`.
    #[must_use]
    pub fn shutdown(mut self) -> Shutdown {
        // One deadline for each worker, which no step restarts. A grace period too long for the
        // clock has no deadline.
        let deadline = Instant::now().checked_add(self.grace);
        self.handle.closed.store(true, Ordering::Release);
        stop(&mut self.handle.routes, deadline);
        let workers = core::mem::take(&mut self.workers)
            .into_iter()
            .zip(0..)
            .map(|(thread, index)| {
                let (outcome, ended) = finish(thread);
                WorkerShutdown {
                    worker: WorkerId(index),
                    outcome,
                    ended,
                }
            })
            .collect();
        let blocking = self.blocking.take().map(Service::teardown);
        Shutdown { workers, blocking }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.handle.closed.store(true, Ordering::Release);
        // A backstop: the teardown of each worker comes at once.
        stop(&mut self.handle.routes, Some(Instant::now()));
        for (index, thread) in (0..).zip(core::mem::take(&mut self.workers)) {
            // The drop is a backstop: the shutdown of R305 reports these outcomes.
            let _outcome = join(index, thread);
        }
        // After the workers, whose drivers submit to it.
        if let Some(service) = self.blocking.take() {
            let _teardown = service.teardown();
        }
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("workers", &self.workers.len())
            .field("report", &self.report)
            .finish_non_exhaustive()
    }
}

/// The index of a local worker, from 0 to the number of workers less 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerId(u32);

impl WorkerId {
    /// The worker with `index`.
    #[must_use]
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    /// The index of the worker.
    #[must_use]
    pub const fn index(self) -> u32 {
        self.0
    }
}

/// Spawns on the workers of a runtime, from any thread. Each handle holds a sender of the inbox of
/// each worker: [`Builder::handles`] bounds them.
pub struct Handle {
    routes: Box<[Route]>,
    blocking: Option<Submitter>,
    /// The runtime stops: each spawn refuses.
    closed: Arc<AtomicBool>,
}

/// The way of a handle to one worker: a sender of its inbox, and the side of its owner that
/// reserves a slot and a charge.
struct Route {
    sender: mpsc::Sender<Message>,
    placer: Placer,
}

impl Handle {
    /// The local workers of the runtime.
    #[must_use]
    pub fn workers(&self) -> u32 {
        u32::try_from(self.routes.len()).expect("a runtime has at most MAX_WORKERS workers")
    }

    /// Spawns the future that `factory` makes on `worker`. The factory moves to the worker, which
    /// calls it there, so the future can hold `!Send` state, such as an `Rc`. The output moves back
    /// to the caller, so it is `Send`.
    ///
    /// The spawn reserves a free slot of the worker and the charge of the task on this thread,
    /// before it sends the factory, so the worker spawns it with no wait. A panic of the factory
    /// ends the task with [`Outcome::Panicked`](crate::task::Outcome::Panicked). A factory that the
    /// worker did not call when it stopped ends with
    /// [`Outcome::Cancelled`](crate::task::Outcome::Cancelled).
    ///
    /// # Errors
    ///
    /// [`SpawnRefused`] with the factory and the [`Reason`](crate::task::Reason): `NoWorker`,
    /// `NoSlot` (each free slot of the worker is reserved), `Budget`, or `Closed` (the worker
    /// stopped). A refusal keeps no reservation.
    pub fn spawn_on<Fa, F>(&mut self, worker: WorkerId, factory: Fa) -> Result<JoinHandle<F::Output>, SpawnRefused<Fa>>
    where
        Fa: FnOnce() -> F + Send + 'static,
        F: Future + 'static,
        F::Output: Send + 'static,
    {
        // Acquire: a spawn after the start of a shutdown sees it and refuses.
        if self.closed.load(Ordering::Acquire) {
            return Err(SpawnRefused::new(factory, task::Reason::Closed));
        }
        let index = worker.0 as usize;
        let Some(Route { sender, placer }) = self.routes.get_mut(index) else {
            return Err(SpawnRefused::new(factory, task::Reason::NoWorker));
        };
        let (job, handle) = placer.prepare(factory)?;
        let (closed, message) = match sender.try_send(Message::Job(job)) {
            Ok(()) => return Ok(handle),
            Err(TrySendError::Closed(message)) => (true, message),
            Err(TrySendError::Full(message)) => (false, message),
        };
        assert!(closed, "the inbox has a place for each reserved slot");
        let job = message.into_job().expect("the handle sent a job");
        Err(SpawnRefused::new(job.into_factory::<Fa, F>(), task::Reason::Closed))
    }

    /// Gives another handle of the same runtime.
    ///
    /// # Errors
    ///
    /// [`HandleRefused`] when [`Builder::handles`] handles exist, or when the allocator refuses
    /// the memory of the handle.
    pub fn try_clone(&self) -> Result<Self, HandleRefused> {
        let mut routes = room(self.routes.len()).map_err(|_| HandleRefused::Allocation)?;
        for route in &self.routes {
            routes.push(Route {
                sender: route.sender.try_clone().map_err(|_| HandleRefused::TooMany)?,
                placer: route.placer.clone(),
            });
        }
        Ok(Self {
            routes: routes.into_boxed_slice(),
            blocking: self.blocking.clone(),
            closed: Arc::clone(&self.closed),
        })
    }

    /// The submitter of the blocking service of the runtime ([`Builder::blocking`]), for blocking
    /// jobs of your own, or `None` when the runtime has no service.
    #[must_use]
    pub const fn blocking(&self) -> Option<&Submitter> {
        self.blocking.as_ref()
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle").field("workers", &self.routes.len()).finish()
    }
}

/// Why the runtime gave no handle. Nothing changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandleRefused {
    /// [`Builder::handles`] handles exist, so no other one can.
    TooMany,
    /// The allocator did not give the memory of the handle.
    Allocation,
}

impl fmt::Display for HandleRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TooMany => "the runtime has its most handles",
            Self::Allocation => "the allocator did not give the memory of the handle",
        })
    }
}

impl core::error::Error for HandleRefused {}

/// What a start found and applied.
#[derive(Debug)]
pub struct Startup {
    host: Host,
    workers: Box<[WorkerReport]>,
}

impl Startup {
    /// The facts of the host that the plan used.
    #[must_use]
    pub const fn host(&self) -> &Host {
        &self.host
    }

    /// The report of each worker, by index.
    #[must_use]
    pub const fn workers(&self) -> &[WorkerReport] {
        &self.workers
    }
}

/// What one worker found and applied when it started.
#[derive(Debug)]
pub struct WorkerReport {
    placement: Placed,
    driver: driver::Report,
}

impl WorkerReport {
    /// The requested, resolved, and applied placement of the worker.
    #[must_use]
    pub const fn placement(&self) -> &Placed {
        &self.placement
    }

    /// What the driver of the worker found and applied.
    #[must_use]
    pub const fn driver(&self) -> &driver::Report {
        &self.driver
    }
}

/// A start that failed: its cause, and the cleanup outcome of each worker that started, by index.
#[derive(Debug)]
pub struct StartFailure {
    cause: Cause,
    cleanup: Vec<Cleanup>,
}

impl StartFailure {
    /// The failure that ended the start.
    #[must_use]
    pub const fn cause(&self) -> &Cause {
        &self.cause
    }

    /// The cleanup outcome of each worker whose thread started, the failed one too.
    #[must_use]
    pub const fn cleanup(&self) -> &[Cleanup] {
        self.cleanup.as_slice()
    }
}

impl fmt::Display for StartFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the runtime did not start: {}", self.cause)?;
        let failed = self
            .cleanup
            .iter()
            .filter(|cleanup| cleanup.outcome != Outcome::Joined)
            .count();
        if failed > 0 {
            write!(f, "; the cleanup of {failed} workers failed")?;
        }
        Ok(())
    }
}

impl core::error::Error for StartFailure {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// Why a start failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Cause {
    /// The plan is invalid, or a strict placement fell back. No worker started.
    Plan(PlanError),
    /// The operating system gave no seed. No worker started.
    Seed(io::Error),
    /// The allocator did not give the tables of the start. No worker started.
    Allocation,
    /// A worker did not start.
    Worker {
        /// The worker.
        index: u32,
        /// Its failure.
        error: WorkerError,
    },
    /// The blocking service did not start ([`Builder::blocking`]). No worker started.
    Blocking(blocking::Error),
}

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plan(error) => write!(f, "the plan is invalid: {error}"),
            Self::Seed(error) => write!(f, "the system gave no seed: {error}"),
            Self::Allocation => f.write_str("the allocator did not give the tables of the start"),
            Self::Worker { index, error } => write!(f, "worker {index} did not start: {error}"),
            Self::Blocking(error) => write!(f, "the blocking service did not start: {error}"),
        }
    }
}

impl core::error::Error for Cause {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Plan(error) => Some(error),
            Self::Seed(error) => Some(error),
            Self::Allocation => None,
            Self::Worker { error, .. } => Some(error),
            Self::Blocking(error) => Some(error),
        }
    }
}

/// What [`Runtime::shutdown`] did.
#[derive(Debug)]
pub struct Shutdown {
    workers: Box<[WorkerShutdown]>,
    blocking: Option<blocking::Teardown>,
}

impl Shutdown {
    /// What each worker did, in the order of their indexes.
    #[must_use]
    pub fn workers(&self) -> &[WorkerShutdown] {
        &self.workers
    }

    /// Whether the grace period ended while an operation of a worker still ran, so that the worker
    /// closed its resources and cancelled its operations at that time.
    #[must_use]
    pub fn expired(&self) -> bool {
        self.workers.iter().any(WorkerShutdown::expired)
    }

    /// What the teardown of the blocking service did, or `None` for a runtime with no service.
    #[must_use]
    pub const fn blocking(&self) -> Option<blocking::Teardown> {
        self.blocking
    }
}

/// What the shutdown did on one worker ([`Shutdown::workers`]).
#[derive(Debug)]
pub struct WorkerShutdown {
    worker: WorkerId,
    outcome: Outcome,
    ended: Ended,
}

impl WorkerShutdown {
    /// The worker.
    #[must_use]
    pub const fn worker(&self) -> WorkerId {
        self.worker
    }

    /// How its thread ended. A failure or a panic gives 0 for each count below.
    #[must_use]
    pub const fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    /// The tasks that the shutdown cancelled.
    #[must_use]
    pub const fn cancelled(&self) -> u32 {
        self.ended.cancelled
    }

    /// The operations that still ran at the end of the grace period. The worker cancelled each
    /// one then, and each one reached its terminal event before the shutdown returned.
    #[must_use]
    pub const fn debt(&self) -> u32 {
        self.ended.debt
    }

    /// The bytes that dropped sends had sent when the shutdown closed their resources.
    #[must_use]
    pub const fn sent(&self) -> u64 {
        self.ended.sent
    }

    /// The children with `End::Keep` that still ran. They run on, and nothing reaps them now.
    #[must_use]
    pub const fn kept(&self) -> u32 {
        self.ended.kept
    }

    /// Whether the grace period ended while an operation of this worker still ran.
    #[must_use]
    pub const fn expired(&self) -> bool {
        self.ended.expired
    }
}

/// Why a worker did not start.
#[derive(Debug)]
#[non_exhaustive]
pub enum WorkerError {
    /// The system did not start its thread.
    Thread(io::Error),
    /// The system refused its strict placement.
    Placement(io::Error),
    /// Its driver did not start.
    Driver(driver::Error),
    /// Its task tables did not fit.
    Owner(task::Refused),
    /// Its inbox, or the channel of its ready report, did not fit.
    Inbox(channel::Refused),
    /// Its timers did not start.
    Timers(TimersError),
    /// Its thread ended with no report.
    Ended,
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Thread(error) => write!(f, "the system did not start its thread: {error}"),
            Self::Placement(error) => write!(f, "the system refused its placement: {error}"),
            Self::Driver(error) => write!(f, "its driver did not start: {error}"),
            Self::Owner(refused) => write!(f, "its task tables did not fit: {refused}"),
            Self::Inbox(refused) => write!(f, "its inbox did not fit: {refused}"),
            Self::Timers(error) => write!(f, "its timers did not start: {error}"),
            Self::Ended => f.write_str("its thread ended with no report"),
        }
    }
}

impl core::error::Error for WorkerError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Thread(error) | Self::Placement(error) => Some(error),
            Self::Driver(error) => Some(error),
            Self::Owner(refused) => Some(refused),
            Self::Inbox(refused) => Some(refused),
            Self::Timers(error) => Some(error),
            Self::Ended => None,
        }
    }
}

/// Why the timers of an owner did not start.
#[derive(Debug)]
#[non_exhaustive]
pub enum TimersError {
    /// The system has no native clock that pauses while it is suspended.
    Clock(clock::Unsupported),
    /// The storage of the timers did not start.
    Storage(timer::Invalid),
}

impl fmt::Display for TimersError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clock(error) => write!(f, "no native clock: {error}"),
            Self::Storage(error) => write!(f, "the timer storage did not start: {error}"),
        }
    }
}

impl core::error::Error for TimersError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Clock(error) => Some(error),
            Self::Storage(error) => Some(error),
        }
    }
}

/// The cleanup outcome of one worker of a failed start.
#[derive(Debug)]
pub struct Cleanup {
    worker: WorkerId,
    outcome: Outcome,
}

impl Cleanup {
    /// The worker.
    #[must_use]
    pub const fn worker(&self) -> WorkerId {
        self.worker
    }

    /// How its cleanup ended.
    #[must_use]
    pub const fn outcome(&self) -> &Outcome {
        &self.outcome
    }
}

/// How the cleanup of one worker ended.
#[derive(Debug)]
#[non_exhaustive]
pub enum Outcome {
    /// The worker tore its driver down, and its thread ended.
    Joined,
    /// The teardown of its driver failed: the kernel can still own its buffers, which then leak.
    Failed(io::Error),
    /// Its thread panicked.
    Panicked,
}

impl PartialEq for Outcome {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Joined, Self::Joined) | (Self::Failed(_), Self::Failed(_)) | (Self::Panicked, Self::Panicked)
        )
    }
}

/// Why [`Runtime::block_on`] failed. A refusal before the first poll holds the root, which
/// [`BlockOnError::into_root`] gives back. The messages show no part of the root.
#[non_exhaustive]
pub enum BlockOnError<F> {
    /// The task tables of the owner did not fit.
    Owner(task::Refused, F),
    /// The timers of the owner did not start.
    Timers(TimersError, F),
    /// The timers of the owner are on another clock domain than its clock.
    Time(crate::time::TimeError, F),
    /// The driver of the owner did not start at the first operation that needed it, for example at
    /// the limit of open files.
    Start(driver::Error),
    /// The driver of the owner failed.
    Driver(io::Error),
}

impl<F> BlockOnError<F> {
    /// The root of a refusal before its first poll, or `None` after a failure of the driver or of
    /// its start.
    pub fn into_root(self) -> Option<F> {
        match self {
            Self::Owner(_, root) | Self::Timers(_, root) | Self::Time(_, root) => Some(root),
            Self::Start(_) | Self::Driver(_) => None,
        }
    }
}

// Not derived: the root need not be `Debug`, and the message does not show it.
impl<F> fmt::Debug for BlockOnError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner(refused, _) => f.debug_tuple("Owner").field(refused).finish_non_exhaustive(),
            Self::Timers(error, _) => f.debug_tuple("Timers").field(error).finish_non_exhaustive(),
            Self::Time(error, _) => f.debug_tuple("Time").field(error).finish_non_exhaustive(),
            Self::Start(error) => f.debug_tuple("Start").field(error).finish(),
            Self::Driver(error) => f.debug_tuple("Driver").field(error).finish(),
        }
    }
}

impl<F> fmt::Display for BlockOnError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner(refused, _) => write!(f, "the task tables of the owner did not fit: {refused}"),
            Self::Timers(error, _) => write!(f, "the timers of the owner did not start: {error}"),
            Self::Time(error, _) => write!(f, "the timers of the owner did not start: {error}"),
            Self::Start(error) => write!(f, "the driver of the owner did not start: {error}"),
            Self::Driver(error) => write!(f, "the driver of the owner failed: {error}"),
        }
    }
}

impl<F> core::error::Error for BlockOnError<F> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Owner(refused, _) => Some(refused),
            Self::Timers(error, _) => Some(error),
            Self::Time(error, _) => Some(error),
            Self::Start(error) => Some(error),
            Self::Driver(error) => Some(error),
        }
    }
}

// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
mod tests;
