//! Each test runs in a process of its own (nextest), so the signal state of one test does not
//! reach another.
// The loom form cannot deliver a signal.
#![cfg(not(loom))]

use alloc::vec::Vec;
use core::ffi::{c_int, c_void};
use core::num::NonZeroU32;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::os::fd::{AsFd as _, AsRawFd as _};
use std::os::unix::process::ExitStatusExt as _;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use super::sys::SigAction;
use super::{REGISTRY, Signal, SignalError, SignalStream};
use crate::driver::sys::{POLLIN, PollFd, poll};
use crate::driver::{Config, Driver, Escalation};
use crate::wait::tests::counter;

const SIGINT: c_int = 2;
const SIGTERM: c_int = 15;
const SIGUSR1: c_int = 10;
const SIGUSR2: c_int = 12;
const SIG_IGN: usize = 1;

#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    fn raise(sig: c_int) -> c_int;
    fn sigaction(sig: c_int, act: *const SigAction, oact: *mut SigAction) -> c_int;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn __errno_location() -> *mut c_int;
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

#[expect(unsafe_code, reason = "the call reads only the structure that it gets")]
fn ignore(signal: c_int) {
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

#[test]
fn several_raises_before_one_poll_give_one_report() {
    let mut driver = Driver::new(Config::new()).unwrap();
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

#[test]
fn two_streams_of_one_signal_on_two_drivers_both_report() {
    let mut drivers = [Driver::new(Config::new()).unwrap(), Driver::new(Config::new()).unwrap()];
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

#[test]
fn a_close_restores_the_earlier_disposition_and_other_signals_keep_theirs() {
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

#[test]
fn a_fault_an_uncatchable_or_a_reserved_signal_and_a_full_registry_refuse() {
    let mut driver = Driver::new(Config::new()).unwrap();
    let refusal = |driver: &mut Driver, number| driver.signal(Signal::from_raw(number), Escalation::Never).unwrap_err();
    for fault in [4, 5, 7, 8, 11, 31] {
        assert_eq!(refusal(&mut driver, fault), SignalError::Fault);
    }
    for uncatchable in [9, 19] {
        assert_eq!(refusal(&mut driver, uncatchable), SignalError::Uncatchable);
    }
    // The default action of these signals does not end the process.
    let first = Escalation::At(NonZeroU32::MIN);
    for ignored in [17, 18, 23, 28] {
        let refusal = driver.signal(Signal::from_raw(ignored), first).unwrap_err();
        assert_eq!(refusal, SignalError::Escalation);
    }
    // 0 and 65 are not signals, and the C library keeps 32 for itself.
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
#[test]
fn escalation_at_the_second_delivery_ends_a_child_that_never_polls() {
    const NAME: &str = "driver::signal::tests::escalation_at_the_second_delivery_ends_a_child_that_never_polls";
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

/// The handler keeps the `errno` of the code that it interrupts, also when its own write fails.
#[test]
#[expect(
    unsafe_code,
    reason = "the calls name only the counter value and the `errno` of this thread"
)]
fn the_handler_keeps_errno() {
    let mut driver = Driver::new(Config::new()).unwrap();
    let _stream = stream(&mut driver, Signal::TERMINATE);
    // A full `eventfd` counter makes the next write of the handler fail with `EAGAIN`.
    let full = u64::MAX - 1;
    // SAFETY: the buffer is the 8 bytes of `full`.
    let written = unsafe { write(driver.as_fd().as_raw_fd(), (&raw const full).cast(), 8) };
    assert_eq!(written, 8);
    // SAFETY: the C library gives a valid location for each thread.
    unsafe { *__errno_location() = 1234 };
    raise_signal(SIGTERM);
    // SAFETY: as above.
    assert_eq!(unsafe { *__errno_location() }, 1234);
}
