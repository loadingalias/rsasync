//! Signal streams: a stream reports that a signal that its caller registered arrived.
//!
//! The first stream of a signal installs a handler for it, and the last stream that closes
//! restores the earlier disposition. The handler does only async-signal-safe work: it counts the
//! delivery, writes the wake descriptor of the home of each stream of the signal, and, at a stated
//! delivery, restores the default action and raises the signal again. The home driver compares
//! the count at each turn and wakes each stream whose signal arrived.
//!
//! # The handler and the process-global state
//!
//! A handler can run on any thread, between any two instructions, so it uses only these:
//!
//! - lock-free atomic loads, stores, and read-modify-writes on statics;
//! - `write`, `sigaction`, and `raise`, which POSIX lists as async-signal-safe;
//! - `__errno_location` on Linux and `__error` on macOS, to keep `errno` of the code that it
//!   interrupts.
//!
//! It takes no lock, allocates nothing, and runs no destructor. The statics are hardware atomics
//! in each form of [`crate::sync`]: a static needs a `const` atomic, and the verification form has
//! none, and it cannot deliver signals either. The bookkeeping of the registrations, which only
//! threads change, has one lock, which the handler never takes.
//!
//! A slot names the wake descriptor of one stream's home, which a wake from another thread also
//! writes: the wake `eventfd` on Linux, and the write end of the wake pipe of the kqueue on macOS.
//! The handler writes 8 bytes to it, which a pipe takes whole or not at all. A close stores 0 in the signal of the
//! slot, and then waits until no handler is in the slot. A handler announces itself in the slot
//! and then reads the signal again. Each of the four accesses is `SeqCst`, so the close sees the
//! handler, or the handler sees the close. Thus no handler writes a descriptor after its stream
//! closed, and the stream keeps the descriptor open until then.

use core::ffi::c_int;
use core::fmt;
use core::future::Future;
use core::num::NonZeroU32;
use core::pin::Pin;
use core::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use core::task::{Context, Poll};
use std::sync::{Mutex, PoisonError};

use super::{Ended, Shared};
use crate::sync::{Arc, atomic::AtomicBool, hint, try_arc};
use crate::wait::Waiter;

/// The signals 1 to 64 of Linux, or 1 to 31 of macOS, and an unused 0.
#[cfg(target_os = "linux")]
const SIGNALS: usize = 65;
#[cfg(target_os = "macos")]
const SIGNALS: usize = 32;
/// The streams of the process, of all signals and drivers.
pub(super) const STREAMS: usize = 64;

/// The delivery count of each signal. A stream reports a change of the count.
static DELIVERIES: [AtomicU64; SIGNALS] = [const { AtomicU64::new(0) }; SIGNALS];
/// For each signal, the count from which the handler restores the default action and raises the
/// signal again, or `u64::MAX`.
static ESCALATE: [AtomicU64; SIGNALS] = [const { AtomicU64::new(u64::MAX) }; SIGNALS];
static SLOTS: [Slot; STREAMS] = [const { Slot::new() }; STREAMS];
static REGISTRY: Mutex<Registry> = Mutex::new(Registry::new());

const _: () = assert!(
    cfg!(target_has_atomic = "64"),
    "the handler needs lock-free 64-bit atomics"
);

/// The wake descriptor of the home of one stream.
struct Slot {
    /// The signal of the stream, or 0 for a free slot.
    signal: AtomicI32,
    /// The descriptor that a wake of another thread writes at the home (`Shared::wake_fd`).
    wake: AtomicI32,
    /// The handlers that are in the slot now.
    active: AtomicU32,
}

impl Slot {
    const fn new() -> Self {
        Self {
            signal: AtomicI32::new(0),
            wake: AtomicI32::new(-1),
            active: AtomicU32::new(0),
        }
    }
}

/// The bookkeeping that registrations change, under [`REGISTRY`].
struct Registry {
    /// The streams of each signal.
    streams: [u32; SIGNALS],
    /// The disposition that the first stream of each signal replaced.
    earlier: [Option<sys::SigAction>; SIGNALS],
    /// The escalation count of the stream of each slot, or `u64::MAX`.
    escalate: [u64; STREAMS],
}

impl Registry {
    const fn new() -> Self {
        Self {
            streams: [0; SIGNALS],
            earlier: [const { None }; SIGNALS],
            escalate: [u64::MAX; STREAMS],
        }
    }

    /// Sets the escalation count of `signal` to the lowest count of its streams.
    fn escalate(&self, signal: c_int) {
        let lowest = SLOTS
            .iter()
            .zip(self.escalate)
            .filter(|(slot, _)| slot.signal.load(Ordering::SeqCst) == signal)
            .map(|(_, count)| count)
            .min()
            .unwrap_or(u64::MAX);
        ESCALATE[index(signal)].store(lowest, Ordering::SeqCst);
    }
}

/// A signal of the system, by its number. Some numbers differ between Linux and macOS, so each
/// constant gives the number of the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Signal(c_int);

impl Signal {
    /// `SIGHUP`: the terminal hung up, or a daemon reloads its configuration.
    pub const HANGUP: Self = Self(1);
    /// `SIGINT`: an interrupt from the terminal, for example Ctrl-C.
    pub const INTERRUPT: Self = Self(2);
    /// `SIGQUIT`: a quit from the terminal.
    pub const QUIT: Self = Self(3);
    /// `SIGUSR1`: a signal that the application defines.
    pub const USER1: Self = Self(sys::SIGUSR1);
    /// `SIGUSR2`: a signal that the application defines.
    pub const USER2: Self = Self(sys::SIGUSR2);
    /// `SIGPIPE`: a write to a pipe or socket whose reader closed.
    pub const PIPE: Self = Self(13);
    /// `SIGALRM`: a timer of `alarm` ended.
    pub const ALARM: Self = Self(14);
    /// `SIGTERM`: a request to end, for example from a service manager.
    pub const TERMINATE: Self = Self(15);
    /// `SIGCHLD`: a child stopped or ended.
    pub const CHILD: Self = Self(sys::SIGCHLD);
    /// `SIGWINCH`: the size of the terminal window changed.
    pub const WINDOW_CHANGE: Self = Self(28);

    /// The signal with the number `number`.
    #[must_use]
    pub const fn from_raw(number: c_int) -> Self {
        Self(number)
    }

    /// The number of the signal.
    #[must_use]
    pub const fn as_raw(self) -> c_int {
        self.0
    }
}

/// When the handler stops reporting a signal and ends the process instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Escalation {
    /// Each delivery goes to the streams.
    Never,
    /// From this delivery after the registration on, counted from 1, the handler restores the
    /// default action and raises the signal again. Thus a second interrupt ends a process that is
    /// stuck. A signal whose default action does not end the process refuses it.
    At(NonZeroU32),
}

/// Why [`Driver::signal`](super::Driver::signal) made no stream. A refusal changes nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SignalError {
    /// The signal reports a fault of the thread that caused it: `SIGSEGV`, `SIGBUS`, `SIGFPE`,
    /// `SIGILL`, `SIGTRAP`, or `SIGSYS`, and `SIGEMT` on macOS.
    Fault,
    /// No handler can take the signal: `SIGKILL` or `SIGSTOP`.
    Uncatchable,
    /// The number is not a signal, or the C library keeps the signal for itself.
    Invalid,
    /// Each of the 64 streams of the process is in use.
    Full,
    /// The registration states an [`Escalation::At`], and the default action of the signal does
    /// not end the process: `SIGCHLD`, `SIGCONT`, `SIGURG`, `SIGWINCH`, the stop signals
    /// `SIGTSTP`, `SIGTTIN`, and `SIGTTOU`, and `SIGIO` and `SIGINFO` on macOS.
    Escalation,
    /// The allocator did not give the memory of the stream.
    Allocation,
    /// A call of `rsasync::local` has no driver on this thread, as for
    /// [`Reason::NoDriver`](super::Reason::NoDriver).
    #[cfg(all(feature = "local", target_os = "linux"))]
    NoDriver,
}

impl fmt::Display for SignalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fault => "a fault signal cannot have a stream",
            Self::Uncatchable => "no handler can take this signal",
            Self::Invalid => "the number is not a signal that a handler can take",
            Self::Full => "each signal stream of the process is in use",
            Self::Escalation => "the default action of this signal does not end the process",
            Self::Allocation => "the allocator did not give the memory of the stream",
            #[cfg(all(feature = "local", target_os = "linux"))]
            Self::NoDriver => "no driver runs on this thread for the call",
        })
    }
}

impl core::error::Error for SignalError {}

/// The part of a stream that its home driver also holds.
pub(super) struct Home {
    signal: c_int,
    /// The count at the last wake of the home.
    woken: AtomicU64,
    waiter: Waiter,
    /// The driver dropped.
    ended: AtomicBool,
}

/// The homes of the streams of one driver, each at the slot of its stream. A new stream takes the
/// place of the closed stream that had its slot, so the table never grows.
pub(super) type Homes = alloc::boxed::Box<[Option<Arc<Home>>]>;

/// Wakes each stream of `homes` whose signal arrived since the last turn, and forgets each stream
/// that closed.
pub(super) fn wake(homes: &mut [Option<Arc<Home>>]) {
    for place in homes.iter_mut() {
        drop(place.take_if(|home| Arc::strong_count(home) == 1));
        if let Some(home) = place {
            let count = DELIVERIES[index(home.signal)].load(Ordering::SeqCst);
            if home.woken.swap(count, Ordering::Relaxed) != count {
                home.waiter.wake();
            }
        }
    }
}

/// Ends each stream of `homes`: it gives [`Ended`].
pub(super) fn end(homes: &[Option<Arc<Home>>]) {
    for home in homes.iter().flatten() {
        home.ended.store(true, Ordering::Release);
        home.waiter.wake();
    }
}

/// Registers a stream of `signal` at the home of `shared`, whose driver keeps the home in `homes`,
/// and gives the stream.
pub(super) fn register(
    shared: &Arc<Shared>,
    homes: &mut [Option<Arc<Home>>],
    signal: Signal,
    escalation: Escalation,
) -> Result<SignalStream, SignalError> {
    let number = signal.0;
    if sys::FAULTS.contains(&number) {
        return Err(SignalError::Fault);
    }
    if sys::UNCATCHABLE.contains(&number) {
        return Err(SignalError::Uncatchable);
    }
    if sys::NOT_ENDING.contains(&number) && escalation != Escalation::Never {
        return Err(SignalError::Escalation);
    }
    if !(1..SIGNALS).contains(&usize::try_from(number).unwrap_or(0)) {
        return Err(SignalError::Invalid);
    }
    // The home comes first, so a refusal of the allocator changes no registration.
    let home = try_arc(Home {
        signal: number,
        woken: AtomicU64::new(0),
        waiter: Waiter::new(),
        ended: AtomicBool::new(false),
    })
    .map_err(|_| SignalError::Allocation)?;
    let mut registry = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
    let slot = SLOTS
        .iter()
        .position(|slot| slot.signal.load(Ordering::SeqCst) == 0)
        .ok_or(SignalError::Full)?;
    if registry.streams[index(number)] == 0 {
        registry.earlier[index(number)] = Some(sys::install(number).ok_or(SignalError::Invalid)?);
    }
    registry.streams[index(number)] += 1;
    let count = DELIVERIES[index(number)].load(Ordering::SeqCst);
    registry.escalate[slot] = match escalation {
        Escalation::Never => u64::MAX,
        Escalation::At(at) => count.saturating_add(u64::from(at.get())),
    };
    let entry = &SLOTS[slot];
    entry.wake.store(shared.wake_fd(), Ordering::Relaxed);
    // Ordering: the handler reads the descriptor after it reads the signal.
    entry.signal.store(number, Ordering::SeqCst);
    registry.escalate(number);
    // Ordering: `Relaxed`: only this thread has the home until the return.
    home.woken.store(count, Ordering::Relaxed);
    let stream = SignalStream {
        home: Arc::clone(&home),
        slot,
        reported: count,
        _shared: Arc::clone(shared),
    };
    // The stream that had this slot closed, so its home leaves the table.
    homes[slot] = Some(home);
    Ok(stream)
}

/// A stream of one signal. It reports that the signal arrived since its last report, not how many
/// times. Its home is the driver that made it, which wakes it at a turn.
///
/// The stream can move to another thread. A drop closes it: when the last stream of the signal
/// closes, the disposition that the first stream replaced returns.
///
/// This loop drives a driver until an interrupt arrives:
///
/// ```no_run
/// use core::future::Future;
/// use core::pin::pin;
/// use core::task::{Context, Poll, Waker};
///
/// use rsasync::driver::{Config, Driver, Escalation, Signal};
///
/// let mut driver = Driver::new(Config::new())?;
/// let mut interrupt = driver.signal(Signal::INTERRUPT, Escalation::Never)?;
/// let mut cx = Context::from_waker(Waker::noop());
/// loop {
///     let more = driver.turn()?.more();
///     if let Poll::Ready(result) = pin!(interrupt.recv()).poll(&mut cx) {
///         result?;
///         break;
///     }
///     if !more {
///         driver.wait()?;
///     }
/// }
/// // Start the shutdown of your program here. For the driver, `teardown` ends each operation.
/// drop(interrupt);
/// let report = driver.teardown()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct SignalStream {
    home: Arc<Home>,
    slot: usize,
    /// The count at the last report.
    reported: u64,
    /// Keeps the wake descriptor open while the slot names it.
    _shared: Arc<Shared>,
}

impl SignalStream {
    /// The signal of the stream.
    #[must_use]
    pub fn signal(&self) -> Signal {
        Signal(self.home.signal)
    }

    /// Gives `Ok(())` when the signal arrived since the last report, and [`Ended`] when the home
    /// driver dropped. Else it registers the waker of `cx`.
    pub fn poll_recv(&mut self, cx: &Context<'_>) -> Poll<Result<(), Ended>> {
        let home = &self.home;
        let reported = &mut self.reported;
        home.waiter.poll_wait(cx, || {
            let count = DELIVERIES[index(home.signal)].load(Ordering::SeqCst);
            if count != *reported {
                *reported = count;
                Some(Ok(()))
            } else if home.ended.load(Ordering::Acquire) {
                Some(Err(Ended))
            } else {
                None
            }
        })
    }

    /// Waits until the signal arrives, as [`SignalStream::poll_recv`] does.
    pub fn recv(&mut self) -> Recv<'_> {
        Recv(self)
    }
}

impl Drop for SignalStream {
    fn drop(&mut self) {
        let mut registry = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
        let number = self.home.signal;
        let slot = &SLOTS[self.slot];
        // Ordering: see the module documentation.
        slot.signal.store(0, Ordering::SeqCst);
        while slot.active.load(Ordering::SeqCst) != 0 {
            hint::spin_loop();
        }
        registry.escalate[self.slot] = u64::MAX;
        registry.escalate(number);
        let streams = &mut registry.streams[index(number)];
        *streams -= 1;
        if *streams == 0
            && let Some(earlier) = registry.earlier[index(number)].take()
        {
            sys::restore(number, &earlier);
        }
    }
}

impl fmt::Debug for SignalStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignalStream")
            .field("signal", &self.home.signal)
            .finish_non_exhaustive()
    }
}

/// The future of [`SignalStream::recv`].
#[derive(Debug)]
pub struct Recv<'a>(&'a mut SignalStream);

impl Future for Recv<'_> {
    type Output = Result<(), Ended>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        crate::coop::poll(cx, |cx| self.0.poll_recv(cx))
    }
}

/// The index of `signal`, which is 1 to 64 on Linux and 1 to 31 on macOS.
fn index(signal: c_int) -> usize {
    usize::try_from(signal).unwrap_or(0) % SIGNALS
}

/// The handler of each registered signal. It does only async-signal-safe work: see the module
/// documentation.
extern "C" fn handle(signal: c_int) {
    let errno = sys::errno();
    let count = DELIVERIES[index(signal)].fetch_add(1, Ordering::SeqCst).wrapping_add(1);
    for slot in &SLOTS {
        if slot.signal.load(Ordering::SeqCst) != signal {
            continue;
        }
        // Ordering: see the module documentation.
        slot.active.fetch_add(1, Ordering::SeqCst);
        if slot.signal.load(Ordering::SeqCst) == signal {
            sys::wake(slot.wake.load(Ordering::Relaxed));
        }
        slot.active.fetch_sub(1, Ordering::SeqCst);
    }
    if count >= ESCALATE[index(signal)].load(Ordering::SeqCst) {
        sys::escalate(signal);
    }
    sys::set_errno(errno);
}

/// The declarations of `signal.h`, `unistd.h`, and `errno.h`: of glibc and musl on x86_64 and
/// aarch64 Linux, which have the same signal numbers and `struct sigaction`, and of the macOS 27.2
/// SDK (`MacOSX27.2.sdk`) on aarch64, with `sys/signal.h`.
mod sys {
    use core::ffi::{c_int, c_void};

    /// `SIG_DFL`: the default action.
    const SIG_DFL: usize = 0;

    /// `SA_RESTART`: a call that the handler interrupts restarts.
    #[cfg(target_os = "linux")]
    const SA_RESTART: c_int = 0x1000_0000;
    #[cfg(target_os = "macos")]
    const SA_RESTART: c_int = 0x2;

    /// The signals whose numbers differ between the systems.
    #[cfg(target_os = "linux")]
    pub(super) const SIGUSR1: c_int = 10;
    #[cfg(target_os = "linux")]
    pub(super) const SIGUSR2: c_int = 12;
    #[cfg(target_os = "linux")]
    pub(super) const SIGCHLD: c_int = 17;
    #[cfg(target_os = "macos")]
    pub(super) const SIGUSR1: c_int = 30;
    #[cfg(target_os = "macos")]
    pub(super) const SIGUSR2: c_int = 31;
    #[cfg(target_os = "macos")]
    pub(super) const SIGCHLD: c_int = 20;

    /// The fault signals: `SIGILL`, `SIGTRAP`, `SIGBUS`, `SIGFPE`, `SIGSEGV`, and `SIGSYS`, and
    /// `SIGEMT` on macOS.
    #[cfg(target_os = "linux")]
    pub(super) const FAULTS: [c_int; 6] = [4, 5, 7, 8, 11, 31];
    #[cfg(target_os = "macos")]
    pub(super) const FAULTS: [c_int; 7] = [4, 5, 7, 8, 10, 11, 12];
    /// `SIGKILL` and `SIGSTOP`.
    #[cfg(target_os = "linux")]
    pub(super) const UNCATCHABLE: [c_int; 2] = [9, 19];
    #[cfg(target_os = "macos")]
    pub(super) const UNCATCHABLE: [c_int; 2] = [9, 17];
    /// The signals whose default action ignores them or stops the process: `SIGCHLD`, `SIGCONT`,
    /// `SIGURG`, `SIGWINCH`, `SIGTSTP`, `SIGTTIN`, and `SIGTTOU`, and `SIGIO` and `SIGINFO` on macOS.
    #[cfg(target_os = "linux")]
    pub(super) const NOT_ENDING: [c_int; 7] = [17, 18, 20, 21, 22, 23, 28];
    #[cfg(target_os = "macos")]
    pub(super) const NOT_ENDING: [c_int; 9] = [16, 18, 19, 20, 21, 22, 23, 28, 29];

    /// `struct sigaction`: the handler, a `sigset_t` of 128 bytes, the flags, and the restorer,
    /// which the C library sets.
    #[cfg(target_os = "linux")]
    #[derive(Clone, Copy)]
    #[repr(C)]
    pub(super) struct SigAction {
        pub(super) handler: usize,
        mask: [u64; 16],
        flags: c_int,
        restorer: usize,
    }

    #[cfg(target_os = "linux")]
    const _: () = {
        assert!(size_of::<SigAction>() == 152);
        assert!(core::mem::offset_of!(SigAction, flags) == 136);
        assert!(core::mem::offset_of!(SigAction, restorer) == 144);
    };

    #[cfg(target_os = "linux")]
    impl SigAction {
        pub(super) const fn new(handler: usize, flags: c_int) -> Self {
            Self {
                handler,
                mask: [0; 16],
                flags,
                restorer: 0,
            }
        }
    }

    /// `struct sigaction`: the handler, a `sigset_t` of 32 bits, and the flags.
    #[cfg(target_os = "macos")]
    #[derive(Clone, Copy)]
    #[repr(C)]
    pub(super) struct SigAction {
        pub(super) handler: usize,
        mask: u32,
        flags: c_int,
    }

    #[cfg(target_os = "macos")]
    const _: () = {
        assert!(size_of::<SigAction>() == 16);
        assert!(core::mem::offset_of!(SigAction, mask) == 8);
        assert!(core::mem::offset_of!(SigAction, flags) == 12);
    };

    #[cfg(target_os = "macos")]
    impl SigAction {
        pub(super) const fn new(handler: usize, flags: c_int) -> Self {
            Self {
                handler,
                mask: 0,
                flags,
            }
        }
    }

    #[expect(unsafe_code, reason = "the C library of each target gives these functions")]
    unsafe extern "C" {
        /// `int sigaction(int sig, const struct sigaction *act, struct sigaction *oact)`.
        fn sigaction(sig: c_int, act: *const SigAction, oact: *mut SigAction) -> c_int;
        /// `int raise(int sig)`.
        fn raise(sig: c_int) -> c_int;
        /// `ssize_t write(int fd, const void *buf, size_t count)`.
        fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
        /// `int *__errno_location(void)`: the `errno` of this thread.
        #[cfg(target_os = "linux")]
        #[link_name = "__errno_location"]
        fn errno_location() -> *mut c_int;
        /// `int *__error(void)`: the `errno` of this thread.
        #[cfg(target_os = "macos")]
        #[link_name = "__error"]
        fn errno_location() -> *mut c_int;
    }

    /// Installs the handler for `signal`, and gives the disposition that it replaced. `None`
    /// when the C library refuses the signal.
    #[expect(unsafe_code, reason = "the call reads and writes only the two structures")]
    pub(super) fn install(signal: c_int) -> Option<SigAction> {
        let handler = (super::handle as extern "C" fn(c_int) as *const ()).expose_provenance();
        let ours = SigAction::new(handler, SA_RESTART);
        let mut earlier = SigAction::new(SIG_DFL, 0);
        // SAFETY: both pointers name valid structures for the call.
        (unsafe { sigaction(signal, &raw const ours, &raw mut earlier) } == 0).then_some(earlier)
    }

    /// Restores `earlier` for `signal`.
    #[expect(unsafe_code, reason = "the call reads only the structure")]
    pub(super) fn restore(signal: c_int, earlier: &SigAction) {
        // SAFETY: `earlier` is a valid structure for the call, and a null `oact` is allowed.
        // `install` took this signal, so the call cannot fail.
        unsafe { sigaction(signal, earlier, core::ptr::null_mut()) };
    }

    /// Restores the default action of `signal`, and raises it again. The signal is blocked while
    /// the handler runs, so it ends the process when the handler returns.
    #[expect(unsafe_code, reason = "both calls are async-signal-safe and name no other memory")]
    pub(super) fn escalate(signal: c_int) {
        let default = SigAction::new(SIG_DFL, 0);
        // SAFETY: `default` is a valid structure for the call.
        unsafe {
            sigaction(signal, &raw const default, core::ptr::null_mut());
            raise(signal);
        }
    }

    /// Adds 1 to the `eventfd` counter of `fd`, or writes 8 bytes to the pipe of `fd`. A full
    /// counter or pipe is a wake already.
    #[expect(unsafe_code, reason = "the call reads only the 8 bytes of the value")]
    pub(super) fn wake(fd: c_int) {
        let one = 1_u64;
        // SAFETY: the buffer is the 8 bytes of `one`. The slot keeps `fd` open: see the module
        // documentation.
        unsafe { write(fd, (&raw const one).cast::<c_void>(), 8) };
    }

    /// The `errno` of this thread.
    #[expect(unsafe_code, reason = "the location is the `errno` of this thread")]
    pub(super) fn errno() -> c_int {
        // SAFETY: the C library gives a valid location for each thread.
        unsafe { *errno_location() }
    }

    /// Sets the `errno` of this thread to `value`.
    #[expect(unsafe_code, reason = "the location is the `errno` of this thread")]
    pub(super) fn set_errno(value: c_int) {
        // SAFETY: the C library gives a valid location for each thread.
        unsafe { *errno_location() = value };
    }
}

#[cfg(test)]
pub(super) mod tests;
