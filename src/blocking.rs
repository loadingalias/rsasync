//! The bounded blocking service: a fixed number of threads that run blocking host work, such as
//! name resolution or a host trust check, for the application and for other libraries.
//!
//! [`Service::new`] starts the threads. [`Service::submitter`] gives a [`Submitter`], which each
//! library can clone and keep. [`Submitter::submit`] never waits: it admits the [`Job`], or it
//! refuses and gives the job back in [`Refused`] with its [`Reason`]. An admitted job gives its
//! result through its [`Waiter`], with a future or a blocking wait, so a caller-owned loop, a
//! scheduler, or a plain thread can wait for it.
//!
//! ```
//! use core::num::NonZeroU64;
//! use std::net::ToSocketAddrs as _;
//!
//! use rsasync::blocking::{Config, Service};
//! use rsasync::capacity::Budget;
//!
//! let budget = Budget::new(NonZeroU64::new(4 << 20).unwrap());
//! // 2 threads of 256 KiB of stack each, and at most 8 jobs at one time.
//! let service = Service::new(&budget, Config::new().threads(2).jobs(8).stack(256 << 10))?;
//! let submitter = service.submitter();
//!
//! // A name lookup blocks in the system resolver, so it runs on the service.
//! let mut lookup = submitter.submit(|| "localhost:443".to_socket_addrs().map(Iterator::count))?;
//! let addresses = lookup.wait_blocking()??;
//! assert!(addresses > 0);
//!
//! // The teardown joins each thread.
//! let report = service.teardown();
//! assert_eq!((report.running(), report.dropped()), (0, 0));
//! # Ok::<(), Box<dyn core::error::Error>>(())
//! ```
//!
//! # Bounds and charges
//!
//! - The service has [`Config::threads`] threads for its whole life.
//! - At most [`Config::jobs`] jobs are admitted at one time. An admitted job holds its job slot
//!   while it waits in the queue and while it runs, until it returns. The slot is free before the
//!   waiter can see the result, so a waiter that has its result can submit again at once.
//! - [`Service::new`] charges the memory of the service and of the queue to its budget, and
//!   [`Config::stack`] for each thread. The platform can round a stack up, so keep headroom for it.
//!   Each thread holds the charge of its stack until it ends.
//! - Each submission charges the memory of its job and of its result to the same budget before it
//!   allocates. The job charge ends when the job returns. The result charge ends when the waiter
//!   and the job are both done with it.
//!
//! The service cannot measure what a job allocates or the stack that it uses. Count that memory
//! in the budget of the caller, or keep headroom for it.
//!
//! # An abandoned job
//!
//! No job is cancelled, because no arbitrary closure can be made to stop. A drop of the waiter
//! ends only the interest in the result: the job runs until it returns, and it holds its job slot
//! until then. Its result then drops on the thread of the service, still under the charge of the
//! job. A panic in that drop does not end the thread.
//!
//! # Teardown
//!
//! [`Service::teardown`] stops the admission of jobs, and then joins each thread. A thread drops
//! each job of the queue that it takes after the stop, and the waiter of that job gives
//! [`Failed::Ended`]. A submission that races the stop can admit its job and then drop it in the
//! same way. A job that runs when the teardown starts runs until it returns, and the teardown
//! waits for it. [`Teardown`] reports both counts.
//!
//! Only the teardown joins the threads. A drop of the service also stops it and drops the jobs of
//! the queue, but it does not wait: each thread ends after its running job returns.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;
use core::mem::MaybeUninit;
use core::num::NonZeroU64;
use core::panic::AssertUnwindSafe;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::panic;

use crate::capacity::{Budget, Exhausted, Permit};
use crate::channel::{self, Closed, TooManyReceivers, mpmc, oneshot};
use crate::sync::thread::{self, JoinHandle};
use crate::sync::{
    Arc, arc_size,
    atomic::{AtomicBool, Ordering},
    try_arc,
};

/// Blocking work that the service runs on one of its threads.
///
/// Each closure `FnOnce() -> R` is a job. A library can also make its own job type, so that a
/// refusal gives back the input of the job, for example the host of a lookup or a command that it
/// must keep:
///
/// ```
/// use std::io;
/// use std::net::{SocketAddr, ToSocketAddrs as _};
///
/// use rsasync::blocking::{Job, Refused, Submitter};
///
/// /// A name lookup of a networking library. A refusal gives its host back.
/// struct Resolve {
///     host: String,
///     port: u16,
/// }
///
/// impl Job for Resolve {
///     type Output = io::Result<Vec<SocketAddr>>;
///
///     fn run(self) -> Self::Output {
///         Ok((self.host.as_str(), self.port).to_socket_addrs()?.collect())
///     }
/// }
///
/// /// Resolves `host` on the service of `submitter`, and waits on this thread for the addresses.
/// fn resolve(
///     submitter: &Submitter,
///     host: String,
///     port: u16,
/// ) -> Result<Vec<SocketAddr>, Box<dyn core::error::Error>> {
///     match submitter.submit(Resolve { host, port }) {
///         Ok(mut lookup) => Ok(lookup.wait_blocking()??),
///         // The refusal gives the lookup back, so the error can name its host.
///         Err(Refused { reason, job }) => Err(format!("{}: {reason}", job.host).into()),
///     }
/// }
/// # use rsasync::blocking::{Config, Service};
/// # let budget = rsasync::capacity::Budget::new(core::num::NonZeroU64::new(4 << 20).unwrap());
/// # let service = Service::new(&budget, Config::new().threads(1).stack(256 << 10))?;
/// # assert!(!resolve(&service.submitter(), "localhost".into(), 443)?.is_empty());
/// # service.teardown();
/// # Ok::<(), Box<dyn core::error::Error>>(())
/// ```
pub trait Job: Send + 'static {
    /// The result of the job.
    type Output: Send + 'static;

    /// Runs the job. It can block.
    fn run(self) -> Self::Output;
}

impl<F, R> Job for F
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    type Output = R;

    fn run(self) -> R {
        self()
    }
}

/// The limits of a service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    threads: u32,
    jobs: u32,
    stack: usize,
}

impl Config {
    /// 4 threads, 64 jobs, and 2 MiB of stack for each thread.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            threads: 4,
            jobs: 64,
            stack: 2 << 20,
        }
    }

    /// The threads, which run the jobs. The service starts them all, and keeps them until it
    /// ends. At least 1, and at most 65 535.
    #[must_use]
    pub const fn threads(self, threads: u32) -> Self {
        Self { threads, ..self }
    }

    /// The jobs that the service admits at one time, in the queue and on the threads together.
    /// At least 1, and at most 2^27.
    #[must_use]
    pub const fn jobs(self, jobs: u32) -> Self {
        Self { jobs, ..self }
    }

    /// The bytes of stack of each thread. Not 0.
    #[must_use]
    pub const fn stack(self, bytes: usize) -> Self {
        Self { stack: bytes, ..self }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// Why [`Service::new`] made no service. A refusal keeps no charge, and no thread of it remains.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A limit of [`Config`] is 0 or too large.
    Config,
    /// The budget has fewer free units than the service, its queue, and its stacks.
    Budget(Exhausted),
    /// The allocator did not give the memory of the queue.
    Allocation,
    /// The system did not start a thread.
    Thread(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config => f.write_str("a limit of the blocking service is 0 or too large"),
            Self::Budget(refused) => write!(f, "the budget cannot charge the blocking service: {refused}"),
            Self::Allocation => f.write_str("the allocator did not give the memory of the blocking service"),
            Self::Thread(error) => write!(f, "a thread of the blocking service did not start: {error}"),
        }
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Thread(error) => Some(error),
            Self::Budget(refused) => Some(refused),
            Self::Config | Self::Allocation => None,
        }
    }
}

/// Why a submission is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// Each job slot is occupied. A later submission can succeed after a job returns.
    Full,
    /// The budget has fewer free units than the job and its result.
    Budget(Exhausted),
    /// The service stopped. No later submission can succeed.
    Ended,
    /// The allocator did not give the memory of the job. A later submission can succeed.
    Allocation,
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("each job slot of the blocking service is occupied"),
            Self::Budget(refused) => write!(f, "the budget cannot charge the job: {refused}"),
            Self::Ended => f.write_str("the blocking service stopped"),
            Self::Allocation => f.write_str("the allocator did not give the memory of the job"),
        }
    }
}

impl core::error::Error for Reason {}

/// A refused submission. The job comes back, and the refusal keeps no charge.
pub struct Refused<J> {
    /// Why the service refused the job.
    pub reason: Reason,
    /// The job, which did not run.
    pub job: J,
}

// Not derived: the job need not be `Debug`, and the message does not show it.
impl<J> fmt::Debug for Refused<J> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Refused")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl<J> fmt::Display for Refused<J> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}

impl<J> core::error::Error for Refused<J> {}

/// Why an admitted job gives no result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failed {
    /// The service stopped before the job ran, so the job dropped. A wait after the waiter took
    /// the result gives this result too.
    Ended,
    /// The job panicked. The panic hook reported the panic, and the thread of the service dropped
    /// its payload and runs the next job. With `panic = "abort"`, a panic ends the process, so no
    /// job gives this result.
    Panicked,
}

impl fmt::Display for Failed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ended => f.write_str("the blocking service stopped before the job ran"),
            Self::Panicked => f.write_str("the job panicked"),
        }
    }
}

impl core::error::Error for Failed {}

/// What [`Service::teardown`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Teardown {
    running: u32,
    dropped: u32,
}

impl Teardown {
    /// The jobs that ran when the teardown started. The teardown waited until each one returned.
    #[must_use]
    pub const fn running(self) -> u32 {
        self.running
    }

    /// The jobs of the queue that did not run. Each one dropped, and its waiter gives
    /// [`Failed::Ended`].
    #[must_use]
    pub const fn dropped(self) -> u32 {
        self.dropped
    }
}

/// An admitted job, with its reply and its charges.
struct Task<J: Job> {
    job: J,
    reply: oneshot::Sender<Result<J::Output, Failed>>,
    /// The job slot.
    slot: Permit,
    /// The charge of this task.
    _memory: Permit,
}

/// A task of any job type, for the queue.
trait Run: Send {
    /// Runs the job, and gives its result to the waiter, or drops it when the waiter is gone.
    /// Gives whether the job returned after the stop of the service in `stopped`.
    fn run(self: Box<Self>, stopped: &AtomicBool) -> bool;
}

impl<J: Job> Run for Task<J> {
    fn run(self: Box<Self>, stopped: &AtomicBool) -> bool {
        let Self {
            job,
            reply,
            slot,
            _memory,
        } = *self;
        // The payload of a panic drops here: the panic hook reported it.
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| job.run())).map_err(|_| Failed::Panicked);
        // The check comes before the send: a waiter that took the result before the stop makes
        // the teardown count no running job. Ordering: `Acquire` reads the release of the stop.
        let late = stopped.load(Ordering::Acquire);
        // The job slot ends when the job returned, before the waiter can see the result, so a
        // waiter that has its result finds its slot free.
        drop(slot);
        // A waiter that is gone gives the result back, and it drops here, still under the memory
        // charge of the task and on this thread.
        drop(reply.send(outcome));
        late
    }
}

/// The state that the service, its submitters, and its threads share.
struct Shared {
    queue: mpmc::Sender<Box<dyn Run>>,
    /// One unit for each job slot.
    slots: Budget,
    /// The budget of [`Service::new`].
    memory: Budget,
    /// The service stopped: it admits no job, and its threads drop the jobs of the queue.
    stopped: AtomicBool,
    _charge: Permit,
}

/// A bounded service of blocking threads. Only [`Service::teardown`] joins its threads.
pub struct Service {
    shared: Arc<Shared>,
    /// The receiver of the service itself, which closes the queue at the stop.
    queue: mpmc::Receiver<Box<dyn Run>>,
    threads: Vec<JoinHandle<Teardown>>,
    config: Config,
}

impl Service {
    /// Starts a service with the limits of `config`. It charges the service, its queue, and the
    /// stack of each thread to `budget` before it allocates, and each submission charges its job
    /// and its result to `budget` too.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when a limit is 0 or too large, [`Error::Budget`] when the budget has too
    /// few free units, [`Error::Allocation`] when the allocator did not give the memory of the service, and
    /// [`Error::Thread`] when the system did not start a thread. The threads that started then
    /// end, and the service joins them before it returns.
    pub fn new(budget: &Budget, config: Config) -> Result<Self, Error> {
        let Config { threads, jobs, stack } = config;
        let Some(slots) = NonZeroU64::new(u64::from(jobs)).filter(|_| threads > 0 && stack > 0) else {
            return Err(Error::Config);
        };
        let stack_units = u64::try_from(stack).map_err(|_| Error::Config)?;
        let fixed = size_of::<JoinHandle<Teardown>>()
            .checked_mul(threads as usize)
            .and_then(|handles| handles.checked_add(arc_size::<Shared>()))
            .and_then(|size| u64::try_from(size).ok())
            .ok_or(Error::Config)?;
        let charge = budget.reserve(fixed).map_err(Error::Budget)?;
        // A receiver place for each thread, and one for the service.
        let (sender, receiver) =
            mpmc::channel(budget, jobs, 1, threads.saturating_add(1)).map_err(|refused| match refused {
                channel::Refused::Zero | channel::Refused::TooLarge => Error::Config,
                channel::Refused::Budget(exhausted) => Error::Budget(exhausted),
                channel::Refused::Allocation => Error::Allocation,
            })?;
        let shared = try_arc(Shared {
            queue: sender,
            slots: Budget::new(slots),
            memory: budget.clone(),
            stopped: AtomicBool::new(false),
            _charge: charge,
        })
        .map_err(|_| Error::Allocation)?;
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(threads as usize)
            .map_err(|_| Error::Allocation)?;
        let mut service = Self {
            shared,
            queue: receiver,
            threads: handles,
            config,
        };
        for _ in 0..threads {
            if let Err(error) = service.start(budget, stack_units) {
                service.teardown();
                return Err(error);
            }
        }
        Ok(service)
    }

    /// Starts one more thread, which holds the charge of its stack until it ends.
    fn start(&mut self, budget: &Budget, stack_units: u64) -> Result<(), Error> {
        let stack = budget.reserve(stack_units).map_err(Error::Budget)?;
        // The channel has a receiver place for each thread, so the clone succeeds.
        let queue = self.queue.try_clone().map_err(|TooManyReceivers| Error::Config)?;
        let shared = Arc::clone(&self.shared);
        let thread = thread::Builder::new()
            .name("rsasync-blocking".into())
            .stack_size(self.config.stack)
            .spawn(move || serve(&shared, queue, stack))
            .map_err(Error::Thread)?;
        self.threads.push(thread);
        Ok(())
    }

    /// Gives a submitter of this service. Each clone submits to the same service.
    #[must_use]
    pub fn submitter(&self) -> Submitter {
        Submitter(Arc::clone(&self.shared))
    }

    /// The threads of the service.
    #[must_use]
    pub const fn threads(&self) -> u32 {
        self.config.threads
    }

    /// The jobs that the service admits at one time.
    #[must_use]
    pub const fn jobs(&self) -> u32 {
        self.config.jobs
    }

    /// The bytes of stack of each thread, as the service asked the platform for them.
    #[must_use]
    pub const fn stack(&self) -> usize {
        self.config.stack
    }

    /// Stops the service, and joins each thread. Each later submission gives [`Reason::Ended`].
    /// A job of the queue drops with no run. A running job runs until it returns, and the
    /// teardown waits for it, so a job that never returns stops the teardown.
    pub fn teardown(mut self) -> Teardown {
        self.stop();
        let mut report = Teardown { running: 0, dropped: 0 };
        for thread in self.threads.drain(..) {
            // A thread catches each panic of a job, so its join gives its counts.
            if let Ok(counts) = thread.join() {
                report.running += counts.running;
                report.dropped += counts.dropped;
            }
        }
        report
    }

    /// Stops the admission of jobs, and closes the queue, which wakes each waiting thread.
    fn stop(&mut self) {
        // Ordering: `Release` orders the stop before each load that sees it, so a submission or a
        // thread that sees the stop also sees each change before it.
        self.shared.stopped.store(true, Ordering::Release);
        self.queue.close();
    }
}

impl Drop for Service {
    /// Stops the service, and joins no thread: each thread ends after its running job returns.
    fn drop(&mut self) {
        self.stop();
    }
}

impl fmt::Debug for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Service")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The work of one thread: runs each job of the queue until the stop, and then drops the jobs
/// that are left. It holds the charge of its stack until it ends.
///
/// A panic in the drop of a job or of a result is caught here, so a thread ends only at the stop,
/// and the service keeps its number of threads.
fn serve(shared: &Shared, mut queue: mpmc::Receiver<Box<dyn Run>>, _stack: Permit) -> Teardown {
    let mut counts = Teardown { running: 0, dropped: 0 };
    while let Ok(task) = queue.recv_blocking() {
        // Ordering: `Acquire` reads the release of the stop.
        if shared.stopped.load(Ordering::Acquire) {
            drop(panic::catch_unwind(AssertUnwindSafe(|| drop(task))));
            counts.dropped += 1;
            continue;
        }
        let late = panic::catch_unwind(AssertUnwindSafe(|| task.run(&shared.stopped)))
            // A drop of the result panicked after the check, so check again. Ordering: `Acquire`,
            // as above.
            .unwrap_or_else(|_| shared.stopped.load(Ordering::Acquire));
        if late {
            counts.running += 1;
        }
    }
    counts
}

/// Submits jobs to a [`Service`]. Each clone submits to the same service, from any thread.
#[derive(Clone)]
pub struct Submitter(Arc<Shared>);

impl Submitter {
    /// Admits `job`, and gives the waiter of its result. It never waits.
    ///
    /// # Errors
    ///
    /// [`Refused`] with the job and its [`Reason`]: [`Reason::Full`] when each job slot is
    /// occupied, [`Reason::Budget`] when the budget cannot charge the job and its result,
    /// [`Reason::Allocation`] when the allocator does not give their memory, and
    /// [`Reason::Ended`] when the service stopped.
    pub fn submit<J: Job>(&self, job: J) -> Result<Waiter<J::Output>, Refused<J>> {
        match self.reserve() {
            Ok(reservation) => Ok(reservation.send(job)),
            Err(reason) => Err(Refused { reason, job }),
        }
    }

    /// Admits one job of type `J` that comes later: it takes the job slot, the charge, the
    /// one-shot, and the memory of the job now, so the [`Reservation::send`] of the job cannot fail. A drop of the
    /// reservation gives them back. It never waits.
    pub(crate) fn reserve<J: Job>(&self) -> Result<Reservation<'_, J>, Reason> {
        let shared = &*self.0;
        // Ordering: `Acquire` reads the release of the stop.
        if shared.stopped.load(Ordering::Acquire) {
            return Err(Reason::Ended);
        }
        let Ok(slot) = shared.slots.reserve(1) else {
            return Err(Reason::Full);
        };
        let size = u64::try_from(size_of::<Task<J>>()).unwrap_or(u64::MAX);
        let memory = shared.memory.reserve(size).map_err(Reason::Budget)?;
        let (reply, waiter) = match oneshot::channel(&shared.memory) {
            Ok(pair) => pair,
            Err(channel::Refused::Budget(exhausted)) => return Err(Reason::Budget(exhausted)),
            Err(channel::Refused::Allocation) => return Err(Reason::Allocation),
            // A one-shot refuses its budget, or a size above `u64::MAX`, which no budget has.
            Err(_) => {
                return Err(Reason::Budget(Exhausted {
                    requested: u64::MAX,
                    available: shared.memory.available(),
                }));
            }
        };
        let task = Box::try_new_uninit().map_err(|_| Reason::Allocation)?;
        Ok(Reservation {
            shared,
            reply,
            waiter,
            slot,
            memory,
            task,
        })
    }
}

/// The admission of one job of type `J` that [`Submitter::reserve`] gave: its slot, its charge,
/// its one-shot, and its memory.
pub(crate) struct Reservation<'a, J: Job> {
    shared: &'a Shared,
    reply: oneshot::Sender<Result<J::Output, Failed>>,
    waiter: oneshot::Receiver<Result<J::Output, Failed>>,
    slot: Permit,
    memory: Permit,
    task: Box<MaybeUninit<Task<J>>>,
}

impl<J: Job> Reservation<'_, J> {
    /// Sends `job` to the service, and gives the waiter of its result.
    pub(crate) fn send(self, job: J) -> Waiter<J::Output> {
        let task = Box::write(
            self.task,
            Task {
                job,
                reply: self.reply,
                slot: self.slot,
                _memory: self.memory,
            },
        );
        // The queue has a slot for each job slot, so only the stop refuses the task. The stop came
        // after the reservation: the task drops, and its waiter gives `Failed::Ended`.
        drop(self.shared.queue.try_send_shared(task));
        Waiter { reply: self.waiter }
    }
}

impl<J: Job> fmt::Debug for Reservation<'_, J> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reservation").finish_non_exhaustive()
    }
}

impl fmt::Debug for Submitter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Submitter").finish_non_exhaustive()
    }
}

/// The waiter of an admitted job. Its drop ends only the interest in the result: the job runs
/// until it returns, and its result then drops.
pub struct Waiter<R> {
    reply: oneshot::Receiver<Result<R, Failed>>,
}

impl<R> Waiter<R> {
    /// Gives a future that waits for the result of the job. A drop of the future keeps the result
    /// for a later wait.
    pub fn wait(&mut self) -> Wait<'_, R> {
        Wait(self.reply.recv())
    }

    /// Waits on this thread for the result of the job.
    ///
    /// # Errors
    ///
    /// [`Failed`] when the job gives no result.
    pub fn wait_blocking(&mut self) -> Result<R, Failed> {
        result(self.reply.recv_blocking())
    }

    /// One poll of the wait, for the driver, which keeps the waiter of a job and no future. The
    /// waker of `cx` stays registered after a `Pending`, so the end of the job wakes it.
    #[cfg(any(
        all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")),
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "windows", target_arch = "x86_64")
    ))]
    pub(crate) fn poll(&mut self, cx: &Context<'_>) -> Poll<Result<R, Failed>> {
        self.reply.poll_recv(cx).map(result)
    }
}

impl<R> fmt::Debug for Waiter<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Waiter").finish_non_exhaustive()
    }
}

/// A wait for the result of a job, from [`Waiter::wait`].
#[must_use = "a future does nothing unless you poll it"]
pub struct Wait<'a, R>(oneshot::RecvFuture<'a, Result<R, Failed>>);

impl<R> Future for Wait<'_, R> {
    type Output = Result<R, Failed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx).map(result)
    }
}

impl<R> fmt::Debug for Wait<'_, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wait").finish_non_exhaustive()
    }
}

/// The result of a wait: a one-shot that closed with no value lost its job at the stop.
fn result<R>(received: Result<Result<R, Failed>, Closed>) -> Result<R, Failed> {
    received.unwrap_or(Err(Failed::Ended))
}

#[cfg(test)]
mod tests;
