//! Each test runs in a process of its own (nextest), so the signal state of one test does not
//! reach another.
// The loom form cannot deliver a signal.
#![cfg(not(loom))]

use alloc::vec::Vec;
use core::ffi::{c_int, c_short, c_void};
use core::num::NonZeroU32;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::os::fd::{AsFd as _, AsRawFd as _};
use std::os::unix::process::ExitStatusExt as _;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use super::sys::{self, SigAction};
use super::{REGISTRY, Signal, SignalError, SignalStream};
use crate::driver::suite::{self, Backend};
use crate::driver::{Config, Driver, Escalation};
use crate::wait::tests::counter;

const SIGINT: c_int = 2;
const SIGTERM: c_int = 15;
#[cfg(target_os = "linux")]
const SIGUSR1: c_int = 10;
#[cfg(target_os = "linux")]
const SIGUSR2: c_int = 12;
#[cfg(target_os = "macos")]
const SIGUSR1: c_int = 30;
#[cfg(target_os = "macos")]
const SIGUSR2: c_int = 31;
const SIG_IGN: usize = 1;

/// `struct pollfd`, with one layout on Linux and macOS, and `POLLIN`.
#[repr(C)]
struct PollFd {
    fd: c_int,
    events: c_short,
    revents: c_short,
}

const POLLIN: c_short = 0x1;

/// `nfds_t`: an `unsigned long` on Linux, and an `unsigned int` on macOS.
#[cfg(target_os = "linux")]
type Nfds = core::ffi::c_ulong;
#[cfg(target_os = "macos")]
type Nfds = core::ffi::c_uint;

#[expect(unsafe_code, reason = "the C library of each target gives these functions")]
unsafe extern "C" {
    fn raise(sig: c_int) -> c_int;
    fn sigaction(sig: c_int, act: *const SigAction, oact: *mut SigAction) -> c_int;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn poll(fds: *mut PollFd, nfds: Nfds, timeout: c_int) -> c_int;
}

#[expect(unsafe_code, reason = "raise names no memory")]
fn raise_signal(signal: c_int) {
    // SAFETY: `raise` names no memory.
    assert_eq!(unsafe { raise(signal) }, 0);
}

/// The handler of the disposition of `signal`.
#[expect(unsafe_code, reason = "the call writes only the structure that it gets")]
fn handler_of(signal: c_int) -> usize {
    let mut action = SigAction::new(0, 0);
    // SAFETY: a null `act` only reads the disposition into `action`.
    assert_eq!(unsafe { sigaction(signal, core::ptr::null(), &raw mut action) }, 0);
    action.handler
}

/// Sets the disposition of `signal` to `SIG_IGN` in this test process.
#[expect(unsafe_code, reason = "the call reads only the structure that it gets")]
pub(in crate::driver) fn ignore(signal: c_int) {
    let action = SigAction::new(SIG_IGN, 0);
    // SAFETY: `action` is a valid structure for the call.
    let result = unsafe { sigaction(signal, &raw const action, core::ptr::null_mut()) };
    assert_eq!(result, 0);
}

/// Whether the wake descriptor of `driver` is readable now.
#[expect(unsafe_code, reason = "the call reads and writes only the one `pollfd`")]
fn woken(driver: &Driver) -> bool {
    let mut fd = PollFd {
        fd: driver.as_fd().as_raw_fd(),
        events: POLLIN,
        revents: 0,
    };
    // SAFETY: `fd` is one valid `pollfd`, and the timeout 0 does not wait.
    let ready = unsafe { poll(&raw mut fd, 1, 0) };
    assert!(ready >= 0);
    fd.revents & POLLIN != 0
}

fn stream(driver: &mut Driver, signal: Signal) -> SignalStream {
    driver.signal(signal, Escalation::Never).unwrap()
}

pub(in crate::driver) fn several_raises_before_one_poll_give_one_report(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let mut stream = stream(&mut driver, Signal::TERMINATE);
    let (woken, waker) = counter();
    assert_eq!(stream.poll_recv(&Context::from_waker(&waker)), Poll::Pending);
    for _ in 0..3 {
        raise_signal(SIGTERM);
    }
    driver.turn().unwrap();
    assert_eq!(woken.get(), 1);
    let cx = Context::from_waker(Waker::noop());
    assert_eq!(stream.poll_recv(&cx), Poll::Ready(Ok(())));
    assert_eq!(stream.poll_recv(&cx), Poll::Pending);
}

pub(in crate::driver) fn two_streams_of_one_signal_on_two_drivers_both_report(backend: Backend) {
    let mut drivers = [
        suite::driver(Config::new(), backend),
        suite::driver(Config::new(), backend),
    ];
    let mut streams = drivers.each_mut().map(|driver| stream(driver, Signal::TERMINATE));
    let cx = Context::from_waker(Waker::noop());
    for (driver, stream) in drivers.iter_mut().zip(&mut streams) {
        driver.turn().unwrap();
        assert!(!woken(driver));
        assert_eq!(stream.poll_recv(&cx), Poll::Pending);
    }
    raise_signal(SIGTERM);
    for (driver, stream) in drivers.iter_mut().zip(&mut streams) {
        // The handler wakes the home of each stream.
        assert!(woken(driver));
        driver.turn().unwrap();
        assert_eq!(stream.poll_recv(&cx), Poll::Ready(Ok(())));
    }
}

pub(in crate::driver) fn a_close_restores_the_earlier_disposition_and_other_signals_keep_theirs() {
    ignore(SIGUSR1);
    let other = handler_of(SIGUSR2);
    let mut driver = Driver::new(Config::new()).unwrap();
    let first = stream(&mut driver, Signal::USER1);
    let second = stream(&mut driver, Signal::USER1);
    assert_ne!(handler_of(SIGUSR1), SIG_IGN);
    assert_eq!(handler_of(SIGUSR2), other);
    drop(first);
    assert_ne!(handler_of(SIGUSR1), SIG_IGN);
    drop(second);
    assert_eq!(handler_of(SIGUSR1), SIG_IGN);
    assert_eq!(handler_of(SIGUSR2), other);
}

pub(in crate::driver) fn a_fault_an_uncatchable_or_a_reserved_signal_and_a_full_registry_refuse() {
    let mut driver = Driver::new(Config::new()).unwrap();
    let refusal = |driver: &mut Driver, number| driver.signal(Signal::from_raw(number), Escalation::Never).unwrap_err();
    // SIGILL, SIGTRAP, SIGBUS, SIGFPE, SIGSEGV, and SIGSYS, and SIGEMT on macOS.
    #[cfg(target_os = "linux")]
    let faults = [4, 5, 7, 8, 11, 31].as_slice();
    #[cfg(target_os = "macos")]
    let faults = [4, 5, 7, 8, 10, 11, 12].as_slice();
    for &fault in faults {
        assert_eq!(refusal(&mut driver, fault), SignalError::Fault);
    }
    // SIGKILL and SIGSTOP.
    #[cfg(target_os = "linux")]
    let uncatchable = [9, 19];
    #[cfg(target_os = "macos")]
    let uncatchable = [9, 17];
    for uncatchable in uncatchable {
        assert_eq!(refusal(&mut driver, uncatchable), SignalError::Uncatchable);
    }
    // The default action of these signals ignores them or stops the process: SIGCHLD, SIGCONT,
    // SIGTSTP, SIGTTIN, SIGTTOU, SIGURG, and SIGWINCH, and SIGIO and SIGINFO on macOS.
    let first = Escalation::At(NonZeroU32::MIN);
    #[cfg(target_os = "linux")]
    let not_ending = [17, 18, 20, 21, 22, 23, 28].as_slice();
    #[cfg(target_os = "macos")]
    let not_ending = [16, 18, 19, 20, 21, 22, 23, 28, 29].as_slice();
    for &number in not_ending {
        let refusal = driver.signal(Signal::from_raw(number), first).unwrap_err();
        assert_eq!(refusal, SignalError::Escalation);
    }
    // 0 and 65 are not signals, and the C library of Linux keeps 32 for itself. macOS has the
    // signals 1 to 31.
    for invalid in [0, 32, 65] {
        assert_eq!(refusal(&mut driver, invalid), SignalError::Invalid);
    }
    let _streams: Vec<SignalStream> = (0..super::STREAMS)
        .map(|_| stream(&mut driver, Signal::USER2))
        .collect();
    assert_eq!(refusal(&mut driver, SIGUSR2), SignalError::Full);
}

/// The child process registers `SIGINT` with escalation at the second delivery, never polls, and
/// raises `SIGINT` two times. It exits with 0 only when the second delivery does not end it.
pub(in crate::driver) fn escalation_at_the_second_delivery_ends_a_child_that_never_polls() {
    // The test of this case in the index of `driver::suite`, which the child runs.
    const NAME: &str =
        "driver::suite::operation::once::escalation_at_the_second_delivery_ends_a_child_that_never_polls";
    const CHILD: &str = "RSASYNC_SIGNAL_ESCALATION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let mut driver = Driver::new(Config::new()).unwrap();
        let second = NonZeroU32::new(2).unwrap();
        let _stream = driver.signal(Signal::INTERRUPT, Escalation::At(second)).unwrap();
        raise_signal(SIGINT);
        raise_signal(SIGINT);
        std::process::exit(0);
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", NAME])
        .env(CHILD, "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let start = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(start.elapsed() < Duration::from_secs(10), "the child did not end");
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(status.signal(), Some(SIGINT), "{status}");
}

/// A handler that takes the registry lock waits while another thread holds it.
#[test]
fn the_handler_takes_no_lock() {
    let mut driver = Driver::new(Config::new()).unwrap();
    let _stream = stream(&mut driver, Signal::TERMINATE);
    let lock = REGISTRY.lock().unwrap();
    let (sent, received) = mpsc::channel();
    // `raise` runs the handler on the thread that raises.
    let raising = thread::spawn(move || {
        raise_signal(SIGTERM);
        sent.send(()).unwrap();
    });
    let handled = received.recv_timeout(Duration::from_secs(5));
    drop(lock);
    raising.join().unwrap();
    assert_eq!(handled, Ok(()), "the handler waited for the registry lock");
}

/// A signal stream and a wake handle that outlive their driver raise no `SIGPIPE` at their writes,
/// which would end a process that does not ignore the signal, such as a C host. The wake pipe of
/// macOS keeps its read end open while a stream or a handle lives.
#[cfg(target_os = "macos")]
#[test]
#[expect(unsafe_code, reason = "the call changes only the disposition of `SIGPIPE`")]
fn a_stream_and_a_wake_handle_that_outlive_their_driver_raise_no_sigpipe() {
    const SIGPIPE: c_int = 13;
    let action = SigAction::new(0, 0);
    // SAFETY: `action` is a valid structure for the call, and the test process alone has it.
    let result = unsafe { sigaction(SIGPIPE, &raw const action, core::ptr::null_mut()) };
    assert_eq!(result, 0);
    let mut driver = Driver::new(Config::new()).unwrap();
    let wake = driver.wake_handle();
    let stream = stream(&mut driver, Signal::TERMINATE);
    drop(driver);
    raise_signal(SIGTERM);
    wake.wake();
    drop(stream);
}

/// The handler keeps the `errno` of the code that it interrupts, also when its own write fails.
#[test]
fn the_handler_keeps_errno() {
    let mut driver = Driver::new(Config::new()).unwrap();
    let _stream = stream(&mut driver, Signal::TERMINATE);
    fill_wake(&driver);
    sys::set_errno(1234);
    raise_signal(SIGTERM);
    assert_eq!(sys::errno(), 1234);
}

/// Fills the wake descriptor of `driver`, so that the next write of the handler fails with
/// `EAGAIN`: the `eventfd` counter on Linux, and the wake pipe on macOS.
#[expect(unsafe_code, reason = "each write reads only the 8 bytes of its value")]
fn fill_wake(driver: &Driver) {
    let fd = driver.shared.wake.as_raw_fd();
    #[cfg(target_os = "linux")]
    let full = u64::MAX - 1;
    #[cfg(target_os = "macos")]
    let full = 1_u64;
    // A pipe takes at most 64 KiB, 8192 writes of 8 bytes.
    for _ in 0..=8192 {
        // SAFETY: the buffer is the 8 bytes of `full`.
        if unsafe { write(fd, (&raw const full).cast(), 8) } < 0 {
            return;
        }
    }
    panic!("the wake descriptor took each write");
}

/// A refusal of the allocator gives `Allocation`, keeps no memory, and changes no registration, so
/// the next stream of the signal starts. A stream whose allocation aborts the process fails.
#[test]
fn a_stream_that_the_allocator_refuses_changes_no_registration() {
    use crate::table::tests::allocations::each_refusal;

    let mut driver = Driver::new(Config::new()).unwrap();
    // On macOS, the standard library allocates the lock of the registry at its first use, so a
    // first stream takes it before the allocator refuses.
    drop(stream(&mut driver, Signal::TERMINATE));
    let refusals = each_refusal(|| driver.signal(Signal::TERMINATE, Escalation::Never).map(drop));
    assert!(!refusals.is_empty());
    assert!(refusals.iter().all(|refusal| *refusal == (SignalError::Allocation, 0)));
    driver.teardown().unwrap();
}
