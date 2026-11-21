//! Tasks on a local owner: a thread that polls its tasks, and drops them, only on itself.
//!
//! A [`LocalOwner`] has a fixed number of task slots. [`LocalOwner::block_on`] makes the calling
//! thread the owner while it runs a root future. In that time, [`spawn_local`] spawns a task on the
//! owner. The future can be `!Send`, such as a future that holds an `Rc`: the owner polls it and
//! drops it on its own thread only.
//!
//! A spawn claims a free slot and charges the memory of the task to the budget of the owner before
//! it allocates. With no free slot, or with too few free units, it allocates nothing, and the
//! refusal gives the future back with a typed [`Reason`]. [`spawn_local_waiting`] waits for a free
//! slot instead.
//!
//! ```
//! use core::num::NonZeroU64;
//!
//! use rsasync::capacity::Budget;
//! use rsasync::task::{LocalOwner, Outcome, Reason, spawn_local};
//!
//! let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
//! let mut owner = LocalOwner::new(&budget, 1)?;
//!
//! let sum = owner.block_on(async {
//!     let task = spawn_local(async { 2 + 2 }).expect("the owner has a free slot");
//!     // The one slot is in use: the refusal gives the future back, with its reason.
//!     let refused = spawn_local(async { 3 }).unwrap_err();
//!     assert_eq!(refused.reason(), Reason::NoSlot);
//!     let _future = refused.into_future();
//!     match task.join().await {
//!         Outcome::Completed(sum) => sum,
//!         Outcome::Cancelled | Outcome::Panicked(_) | Outcome::OwnerFailed(_) => 0,
//!     }
//! });
//! assert_eq!(sum, 4);
//! # Ok::<(), rsasync::task::Refused>(())
//! ```
//!
//! # Outcomes
//!
//! | Outcome | When |
//! | --- | --- |
//! | [`Outcome::Completed`] | The future gave its output |
//! | [`Outcome::Cancelled`] | A cancel was requested, or `block_on` ended: the owner dropped the future at a poll boundary |
//! | [`Outcome::Panicked`] | A poll of the future, or the drop of the future, panicked: the owner dropped the future |
//! | [`Outcome::OwnerFailed`] | The owner dropped the future with no outcome, because `block_on` ended with a panic |
//!
//! A drop of a [`JoinHandle`] does not cancel its task. The outcome of a task stays in the channel
//! of its join handle, charged to the budget, until the join takes it or the handle drops.
//!
//! A cancel never drops a future during its poll: the owner drops it at the start of the next poll,
//! or after the poll that is running ends. A [`CancelHandle`] works from each thread, and so does
//! the waker of a task.
//!
//! # Poll budget
//!
//! Each poll of a task, and of the root future, has a budget of runtime leaves: the futures of this
//! crate that wait for the runtime, such as a channel send or receive, an I/O operation of the
//! driver, a permit acquisition, and a timer. Each leaf that becomes ready in the poll spends one
//! unit. When none is left, the next leaf wakes its task and gives `Pending` once. The owner then
//! polls the other ready tasks before it polls that task again. Thus a task that loops on leaves
//! that are always ready cannot keep the thread from the other tasks.
//!
//! The budget is [`DEFAULT_POLL_BUDGET`], and [`LocalOwner::set_poll_budget`] changes it. It is not
//! a guarantee of fairness: the owner cannot stop a poll, so code that uses no leaf runs until it
//! gives `Pending` or ends.
//!
//! # Timers
//!
//! [`LocalOwner::use_timers`] gives the owner a [`Timers`] storage and the native clock of its
//! domain. In a task or in the root future, [`timer()`] then makes a [`Timer`] in that storage. Each
//! turn of the owner fires at most 64 due timers. A timer never fires before its deadline.
//!
//! The owner parks at most until the next turn of the storage that has work: the start of the tick
//! of the next timer, or an earlier turn. That instant is at most the deadline of the timer and
//! [`Timers::max_coarsening`] after it, which is the resolution of the storage less 1 ns. The
//! wake of the thread by the operating system comes on top of that, and the runtime does not bound
//! it: on a host with a heavy load, a timer can fire later.
//!
//! ```
//! use core::num::{NonZeroU32, NonZeroU64};
//! use core::time::Duration;
//!
//! use rsasync::capacity::Budget;
//! use rsasync::clock::NativeClock;
//! use rsasync::task::{LocalOwner, timer};
//! use rsasync::time::{Deadline, Suspend};
//! use rsasync::timer::Timers;
//!
//! let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
//! let clock = NativeClock::new(Suspend::Pauses)?;
//! let resolution = Duration::from_millis(1);
//! let timers = Timers::new(NonZeroU32::new(64).unwrap(), resolution, clock.now(), &budget)?;
//! let mut owner = LocalOwner::new(&budget, 8)?;
//! owner.use_timers(timers, clock)?;
//!
//! let deadline = clock.now().checked_add(Duration::from_millis(3))?;
//! let fired = owner.block_on(async { timer(Deadline::at(deadline)).expect("the owner has timers").await });
//! assert_eq!(fired, Ok(()));
//! // The timer fired at or after its deadline.
//! assert!(deadline.compare(clock.now())?.is_le());
//! # Ok::<(), Box<dyn core::error::Error>>(())
//! ```
//!
//! # Panics
//!
//! The owner contains each panic of the state of a task: a poll of its future, and the drop of its
//! future or of an output that no join takes. That task ends with [`Outcome::Panicked`], and the
//! other tasks continue. A panic in the drop of one task does not stop the drop of the others, and
//! each charge returns to the budget.
//!
//! The owner drops each panic payload that no join takes. If that drop panics, the owner drops the
//! new payload in the same way. If that drop panics too, its payload leaks: one allocation.
//!
//! A panic of the root future, or of a waker that the owner calls, ends `block_on` with that panic.
//! Each live task then ends with [`Outcome::OwnerFailed`]. With `panic = "abort"`, each panic ends
//! the process, and no task ends with [`Outcome::Panicked`].
//!
//! A destructor of a task can use the owner again: it can spawn, wake, and cancel. At the end of
//! `block_on`, and after a panic of the root, a spawn gives [`Reason::Closed`] and its future back.

use core::any::Any;
use core::cell::{Cell, RefCell};
use core::fmt;
use core::future::Future;
use core::num::NonZeroU32;
use core::pin::Pin;
use core::ptr;
use core::task::{Context, Poll};

use alloc::boxed::Box;
use alloc::rc::Rc;

use crate::capacity::{Budget, Exhausted};
use crate::channel::{Closed, oneshot};
use crate::clock::NativeClock;
use crate::sync::thread_local;
use crate::time::{Deadline, TimeError};
use crate::timer::{self, Timer, Timers};

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod factory;
#[cfg(any(
    all(test, loom),
    all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64"))
))]
mod late;
mod owner;
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod ring;
mod slot;
mod waiting;

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub(crate) use factory::{Job, Placer};
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub(crate) use owner::MAX_SLOTS;
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub(crate) use ring::{Failure, RingOwner};

use owner::{Enter, Local};
use slot::TaskRef;
use waiting::Node;

thread_local! {
    /// The owner of this thread while its `block_on` runs.
    #[cfg_attr(
        not(all(test, loom)),
        expect(
            clippy::missing_const_for_thread_local,
            reason = "the loom form of the macro takes no `const` block, and both forms take this one"
        )
    )]
    static CURRENT: RefCell<Option<Rc<Local>>> = RefCell::new(None);

    /// The shared state of the owner in `CURRENT`, or null. A wake reads only this value, which
    /// needs no destructor, so a wake from a new thread never allocates for `CURRENT`.
    #[cfg_attr(
        not(all(test, loom)),
        expect(
            clippy::missing_const_for_thread_local,
            reason = "the loom form of the macro takes no `const` block, and both forms take this one"
        )
    )]
    static HOME: Cell<*const slot::Shared> = Cell::new(ptr::null());
}

/// A refusal to make an owner. A refusal keeps no charge and no memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The number of slots is zero. An owner has at least one slot.
    Zero,
    /// The number of slots is larger than 2^16.
    TooLarge,
    /// The budget has fewer free units than the tables of the owner.
    Budget(Exhausted),
    /// The allocator did not give the memory.
    Allocation,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => f.write_str("an owner cannot have zero task slots"),
            Self::TooLarge => f.write_str("the owner has too many task slots"),
            Self::Budget(refused) => write!(f, "the budget cannot charge the owner: {refused}"),
            Self::Allocation => f.write_str("the allocator did not give the memory of the owner"),
        }
    }
}

impl core::error::Error for Refused {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Budget(exhausted) => Some(exhausted),
            Self::Zero | Self::TooLarge | Self::Allocation => None,
        }
    }
}

/// The poll budget of an owner that [`LocalOwner::set_poll_budget`] did not change: the runtime
/// leaves that one poll of a task can complete before the next leaf yields. See
/// [Poll budget](self#poll-budget).
///
/// The value is measured (`benches/poll_budget.rs`): it is the smallest budget whose loop on
/// always-ready channel operations costs at most 2% more than no limit, on x86_64 and on aarch64.
pub const DEFAULT_POLL_BUDGET: NonZeroU32 = NonZeroU32::new(128).expect("the budget is not zero");

/// An owner of local tasks, with a fixed number of task slots. It stays on the thread that made it.
pub struct LocalOwner {
    local: Rc<Local>,
}

impl LocalOwner {
    /// Makes an owner with `slots` task slots, on this thread. It charges its tables to `budget`
    /// before it allocates, and it charges each task to `budget` at its spawn.
    ///
    /// The number of slots is at least 1 and at most 2^16.
    pub fn new(budget: &Budget, slots: u32) -> Result<Self, Refused> {
        let home = slot::Home::new(
            crate::sync::thread::current(),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            None,
        );
        let local = Local::new(budget, slots, home, 0)?;
        Ok(Self {
            local: Rc::try_new(local).map_err(|_| Refused::Allocation)?,
        })
    }

    /// Sets the poll budget of each later poll of a task or of the root future: the runtime leaves
    /// that one poll can complete. When a poll has spent it, the next leaf wakes its task and gives
    /// `Pending` once, so the owner polls its other ready tasks. See
    /// [Poll budget](self#poll-budget).
    pub fn set_poll_budget(&mut self, budget: NonZeroU32) {
        self.local.poll_budget.set(budget.get());
    }

    /// Gives the owner a timer storage, which `clock` drives: each turn of the owner fires its due
    /// timers, and the owner parks at most until the next timer is due. [`timer()`] makes a timer of
    /// a task in it. A storage before this one closes, and each of its timers gives
    /// [`Closed`](crate::timer::Closed). See [Timers](self#timers).
    ///
    /// # Errors
    ///
    /// [`TimeError::DomainMismatch`] when `clock` is not on the domain of `timers`. The owner keeps
    /// the storage that it had.
    pub fn use_timers(&mut self, timers: Timers, clock: NativeClock) -> Result<(), TimeError> {
        self.local.use_timers(timers, clock)
    }

    /// Makes this thread the owner, and polls `root` and the tasks of the owner until `root` gives
    /// its output. Then it cancels each task that is still live, and it returns after they end.
    ///
    /// The thread parks when no task is ready. If `root` panics, each live task ends with
    /// [`Outcome::OwnerFailed`], and the panic continues.
    pub fn block_on<F: Future>(&mut self, root: F) -> F::Output {
        let mut enter = Enter::new(&self.local);
        let Ok(output) = self.local.block_on(root, &mut owner::Thread);
        enter.ended = true;
        output
    }
}

impl fmt::Debug for LocalOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalOwner").finish_non_exhaustive()
    }
}

/// Spawns `future` on the owner of this thread, which polls it and drops it on this thread.
///
/// # Errors
///
/// [`SpawnRefused`] with the future and the [`Reason`]: no free slot, too few free units in the
/// budget, the end of `block_on`, or no owner on this thread. A refusal allocates nothing.
pub fn spawn_local<F>(future: F) -> Result<JoinHandle<F::Output>, SpawnRefused<F>>
where
    F: Future + 'static,
{
    match current() {
        Some(local) => local.spawn(future),
        None => Err(SpawnRefused {
            future,
            reason: Reason::NoOwner,
        }),
    }
}

/// Gives a future that waits for a free slot of the owner of this thread, and then spawns `future`.
/// The waiting spawns get the free slots first in, first out.
///
/// The future gives [`SpawnRefused`] for each other reason: too few free units in the budget, the
/// end of `block_on`, or no owner on this thread.
pub fn spawn_local_waiting<F>(future: F) -> WaitSpawn<F>
where
    F: Future + 'static,
{
    WaitSpawn {
        local: current(),
        future: Some(future),
        node: Node::new(),
    }
}

/// The owner of this thread. A destructor of another thread-local value can run at the exit of the
/// thread, after the destructor of `CURRENT`, and the thread then has no owner.
fn current() -> Option<Rc<Local>> {
    CURRENT.try_with(|current| current.borrow().clone()).ok().flatten()
}

/// Makes a timer that expires at `deadline` in the timer storage of the owner of this thread
/// ([`LocalOwner::use_timers`]). The owner fires it in the first turn at or after its tick: never
/// before `deadline`. A drop of the timer cancels it and frees its place, and the cancel wakes no
/// task.
///
/// # Errors
///
/// [`TimerRefused`] gives the reason, and nothing changes.
pub fn timer(deadline: Deadline) -> Result<Timer, TimerRefused> {
    let local = current().ok_or(TimerRefused::NoOwner)?;
    let mut timers = local.timers.borrow_mut();
    let (timers, _) = timers.as_mut().ok_or(TimerRefused::NoTimers)?;
    timers.insert(deadline).map_err(TimerRefused::Storage)
}

/// Why [`timer()`] made no timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimerRefused {
    /// No `block_on` runs on this thread.
    NoOwner,
    /// The owner has no timer storage: give it one with [`LocalOwner::use_timers`].
    NoTimers,
    /// The storage refused the timer: it is full, the deadline is after its range, or the deadline
    /// is on another clock domain.
    Storage(timer::Refused),
}

impl fmt::Display for TimerRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoOwner => f.write_str("no owner runs on this thread"),
            Self::NoTimers => f.write_str("the owner of this thread has no timer storage"),
            Self::Storage(refused) => write!(f, "the timer storage refused the timer: {refused}"),
        }
    }
}

impl core::error::Error for TimerRefused {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Storage(refused) => Some(refused),
            Self::NoOwner | Self::NoTimers => None,
        }
    }
}

/// Calls `f` with the driver of the owner of this thread and `value`. The driver is the home of each
/// operation that `f` submits. A worker of a runtime has its driver from its start. The owner of
/// `Runtime::block_on` makes its driver here, the first time, so a root that only waits starts
/// none. A driver that does not start fails that owner, so `block_on` gives its error. The futures
/// of the operations do not borrow the driver, so a task awaits them after `f` returns.
///
/// The local I/O types build on it. No public path gives `&mut Driver`: safe code could replace
/// the driver, and the owner would then wait in a ring that no wake signals.
///
/// # Errors
///
/// `value`, when `f` does not run: no owner runs on this thread, its owner has no driver (a
/// [`LocalOwner`]), its driver did not start, or the driver is in use on this thread, by a turn
/// that calls a waker or a destructor.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub(crate) fn with_driver<V, R>(value: V, f: impl FnOnce(&mut crate::driver::Driver, V) -> R) -> Result<R, V> {
    match current() {
        Some(local) => local.with_driver(value, f),
        None => Err(value),
    }
}

/// Gives a new generator that the caller owns: a child of the generator of the owner of this
/// thread, with `label` and the next index of the owner. Each owner of a
/// [`Runtime`](crate::runtime::Runtime) has its own generator, which it derives from the generator
/// of the runtime. So no two calls share a stream, no task reads, predicts, or replaces the
/// generator of its owner, and one seed gives each call the same generator again when the calls of
/// the owner come in the same order. Derive more children from it with
/// [`ChaCha20Rng::child`](crate::runtime::ChaCha20Rng::child).
///
/// # Errors
///
/// [`GeneratorRefused`] gives the reason.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn generator(label: &[u8; 8]) -> Result<rscrypto::ChaCha20Rng, GeneratorRefused> {
    let local = current().ok_or(GeneratorRefused::NoOwner)?;
    let mut generator = local.generator.borrow_mut();
    let (owner, next) = generator.as_mut().ok_or(GeneratorRefused::NoGenerator)?;
    let child = owner.child(label, *next);
    *next = next.checked_add(1).expect("an owner makes fewer than 2^64 generators");
    Ok(child)
}

/// Why [`generator()`] gave no generator.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum GeneratorRefused {
    /// No owner runs on this thread.
    NoOwner,
    /// The owner of this thread has no generator: a [`LocalOwner`] has none.
    NoGenerator,
}

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
impl fmt::Display for GeneratorRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoOwner => "no owner runs on this thread",
            Self::NoGenerator => "the owner of this thread has no generator",
        })
    }
}

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
impl core::error::Error for GeneratorRefused {}

/// Why a spawn is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// Each task slot of the owner is in use.
    NoSlot,
    /// The budget has fewer free units than the memory of the task.
    Budget(Exhausted),
    /// The allocator did not give the memory of the task.
    Allocation,
    /// The `block_on` of the owner is at its end, and it cancels its tasks, or it failed, and it
    /// drops them.
    Closed,
    /// This thread has no owner: no `block_on` runs on it.
    NoOwner,
    /// The runtime has no worker with this index.
    NoWorker,
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSlot => f.write_str("each task slot of the owner is in use"),
            Self::Budget(refused) => write!(f, "the budget cannot charge the task: {refused}"),
            Self::Allocation => f.write_str("the allocator did not give the memory of the task"),
            Self::Closed => f.write_str("the owner is at the end of its block_on, or it failed"),
            Self::NoOwner => f.write_str("this thread has no local owner"),
            Self::NoWorker => f.write_str("the runtime has no worker with this index"),
        }
    }
}

/// A refused spawn: the future that was not spawned, and the reason.
pub struct SpawnRefused<F> {
    future: F,
    reason: Reason,
}

impl<F> SpawnRefused<F> {
    /// A refusal of the spawn of `future`.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub(crate) const fn new(future: F, reason: Reason) -> Self {
        Self { future, reason }
    }

    /// Why the spawn was refused.
    pub fn reason(&self) -> Reason {
        self.reason
    }

    /// Gives back the future that was not spawned.
    pub fn into_future(self) -> F {
        self.future
    }
}

// Not derived: the future need not be `Debug`, and the message does not show it.
impl<F> fmt::Debug for SpawnRefused<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnRefused")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl<F> fmt::Display for SpawnRefused<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the spawn was refused: {}", self.reason)
    }
}

impl<F> core::error::Error for SpawnRefused<F> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match &self.reason {
            Reason::Budget(exhausted) => Some(exhausted),
            Reason::NoSlot | Reason::Allocation | Reason::Closed | Reason::NoOwner | Reason::NoWorker => None,
        }
    }
}

/// A waiting spawn, from [`spawn_local_waiting`]. It holds the future until the spawn. A drop of
/// this future drops it.
#[must_use = "a future does nothing unless you poll it"]
pub struct WaitSpawn<F> {
    local: Option<Rc<Local>>,
    future: Option<F>,
    /// The node in the list of waiting spawns of the owner. It is pinned with this future.
    node: Node,
}

impl<F: Future + 'static> Future for WaitSpawn<F> {
    type Output = Result<JoinHandle<F::Output>, SpawnRefused<F>>;

    #[expect(unsafe_code, reason = "the node is pinned with the future, and its drop unlinks it")]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: this future does not move its node. The future to spawn is not pinned: it moves
        // out of this future by value.
        let this = unsafe { self.get_unchecked_mut() };
        let Some(future) = this.future.take() else {
            return Poll::Pending;
        };
        let Some(local) = &this.local else {
            return Poll::Ready(Err(SpawnRefused {
                future,
                reason: Reason::NoOwner,
            }));
        };
        let woken = local.waiting.remove(&this.node);
        match local.spawn(future) {
            Err(refused) if refused.reason == Reason::NoSlot => {
                this.future = Some(refused.future);
                // SAFETY: the node is pinned with this future, and its drop unlinks it.
                unsafe { local.waiting.wait(&this.node, cx.waker()) };
                Poll::Pending
            }
            result => {
                // A freed slot woke this spawn, and another refusal left the slot free: the next
                // waiting spawn gets it.
                if woken && result.is_err() {
                    local.waiting.wake_first();
                }
                Poll::Ready(result)
            }
        }
    }
}

impl<F> Drop for WaitSpawn<F> {
    fn drop(&mut self) {
        // A freed slot woke this spawn, and it does not use the slot: the next one gets it.
        if let Some(local) = &self.local
            && local.waiting.remove(&self.node)
        {
            local.waiting.wake_first();
        }
    }
}

impl<F> fmt::Debug for WaitSpawn<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitSpawn").finish_non_exhaustive()
    }
}

/// How a task ended.
#[derive(Debug)]
pub enum Outcome<T> {
    /// The future gave its output.
    Completed(T),
    /// A cancel was requested, or `block_on` ended: the owner dropped the future at a poll
    /// boundary.
    Cancelled,
    /// A poll of the future panicked, and the owner dropped the future.
    Panicked(Panic),
    /// The owner dropped the future with no outcome, because its `block_on` ended with a panic.
    OwnerFailed(OwnerFailure),
}

/// The payload of a panic that a task contained.
pub struct Panic(Box<dyn Any + Send>);

impl Panic {
    /// Gives the payload of the panic, as `std::panic::catch_unwind` gives it.
    pub fn into_payload(self) -> Box<dyn Any + Send> {
        self.0
    }
}

impl fmt::Debug for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Panic(..)")
    }
}

/// The owner of a task failed before the task had an outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerFailure;

impl fmt::Display for OwnerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the owner of the task failed")
    }
}

impl core::error::Error for OwnerFailure {}

/// The handle that observes the outcome of a task. Its drop does not cancel the task. It can move
/// to another thread if the output can.
pub struct JoinHandle<T> {
    receiver: oneshot::Receiver<Outcome<T>>,
    cancel: CancelHandle,
}

impl<T> JoinHandle<T> {
    /// Gives a future that waits for the outcome of the task.
    pub fn join(self) -> Join<T> {
        Join {
            receiver: self.receiver,
            waits: false,
            done: false,
        }
    }

    /// Waits on this thread for the outcome of the task. Do not call it on the thread of the owner,
    /// which then cannot poll the task.
    pub fn join_blocking(mut self) -> Outcome<T> {
        outcome(self.receiver.recv_blocking())
    }

    /// Gives a handle that requests the cancel of the task.
    pub fn cancel_handle(&self) -> CancelHandle {
        self.cancel.clone()
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle").finish_non_exhaustive()
    }
}

/// The outcome that a join received, or `OwnerFailed` if the task ended with none.
fn outcome<T>(received: Result<Outcome<T>, Closed>) -> Outcome<T> {
    received.unwrap_or(Outcome::OwnerFailed(OwnerFailure))
}

/// A waiting join, from [`JoinHandle::join`]. A poll after its outcome gives `Pending`.
#[must_use = "a future does nothing unless you poll it"]
pub struct Join<T> {
    receiver: oneshot::Receiver<Outcome<T>>,
    waits: bool,
    done: bool,
}

impl<T> Future for Join<T> {
    type Output = Outcome<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.done {
            return Poll::Pending;
        }
        let polled = this.receiver.poll_recv(cx);
        this.waits = polled.is_pending();
        this.done = polled.is_ready();
        polled.map(outcome)
    }
}

impl<T> Drop for Join<T> {
    fn drop(&mut self) {
        if self.waits {
            self.receiver.unwait();
        }
    }
}

impl<T> fmt::Debug for Join<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Join").finish_non_exhaustive()
    }
}

/// The handle that requests the cancel of a task, from each thread. The owner drops the future at
/// a poll boundary, and the outcome is [`Outcome::Cancelled`]. A cancel after the task ended does
/// nothing.
#[derive(Clone)]
pub struct CancelHandle(Target);

/// The task that a cancel handle names.
#[derive(Clone)]
enum Target {
    /// A task that a spawn on its owner made.
    Local(alloc::sync::Arc<TaskRef>),
    /// A task that a factory spawn from another thread makes on its owner.
    #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
    Remote(alloc::sync::Arc<factory::Remote>),
}

impl CancelHandle {
    /// Requests the cancel of the task.
    pub fn cancel(&self) {
        match &self.0 {
            Target::Local(task) => task.shared.wake(task.slot, task.generation, true),
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            Target::Remote(remote) => remote.cancel(),
        }
    }
}

impl fmt::Debug for CancelHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancelHandle").finish_non_exhaustive()
    }
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(all(test, loom))]
mod explore;
