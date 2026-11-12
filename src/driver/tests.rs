use alloc::task::Wake;
use core::ffi::c_int;
use core::future::Future;
use core::mem::{self, ManuallyDrop};
use core::num::NonZeroU64;
use core::pin::{Pin, pin};
use core::ptr::NonNull;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
use std::fs::File;
use std::io::{ErrorKind, Write as _};
use std::os::fd::{FromRawFd as _, OwnedFd};
use std::sync::{Arc, Mutex};
use std::thread::{self, Thread};
use std::vec::Vec;

use super::{Completion, Config, Driver, Held, Reason, Refused};
use crate::buffer::{Backing, BufferMut, Release};
use crate::capacity::Budget;
use crate::completion::Gone;
use crate::uring::Sqe;

#[expect(unsafe_code, reason = "the C library of each Linux target gives this function")]
unsafe extern "C" {
    /// `int pipe2(int pipefd[2], int flags)`.
    fn pipe2(fds: *mut c_int, flags: c_int) -> c_int;
}

/// `O_NONBLOCK` and `O_CLOEXEC` (`include/uapi/asm-generic/fcntl.h`).
const O_NONBLOCK: c_int = 0o4_000;
const O_CLOEXEC: c_int = 0o2_000_000;

/// A nonblocking pipe: its read end and its write end. Linux 6.1 then waits for its readiness and
/// reads it in the task work of the ring, as Linux 7.0 does for each pipe. A read of a blocking
/// pipe on 6.1 goes to an io-wq worker instead, because 6.1 pipes lack `FMODE_NOWAIT`.
#[expect(unsafe_code, reason = "the test makes its pipe with the C library")]
fn pipe() -> (OwnedFd, File) {
    let mut fds = [0; 2];
    // SAFETY: `fds` has room for the two descriptors.
    assert_eq!(unsafe { pipe2(fds.as_mut_ptr(), O_NONBLOCK | O_CLOEXEC) }, 0);
    // SAFETY: `pipe2` gave two new descriptors that nothing else owns.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), File::from_raw_fd(fds[1])) }
}

/// The order of the events of a test, from any thread.
type Log = Arc<Mutex<Vec<&'static str>>>;

fn note(log: &Log, what: &'static str) {
    log.lock().expect("no test thread panics with the log").push(what);
}

fn taken(log: &Log) -> Vec<&'static str> {
    mem::take(&mut *log.lock().expect("no test thread panics with the log"))
}

/// A backing of `capacity` bytes whose release, through its release hook, records "released".
#[expect(
    unsafe_code,
    reason = "the test maps its own block into a backing, as a library does"
)]
fn recorded(log: &Log, capacity: usize) -> Backing {
    let mut block = ManuallyDrop::new(Vec::<u8>::with_capacity(capacity));
    let ptr = NonNull::new(block.as_mut_ptr()).expect("a vector gives a pointer that is not null");
    let data = Arc::into_raw(Arc::clone(log)).cast::<()>();
    // SAFETY: the block of the vector is valid for `capacity` bytes and only the backing uses it.
    // `release_recorded` frees it, on any thread.
    unsafe {
        Backing::from_raw_parts(
            ptr,
            block.capacity(),
            0,
            block.capacity(),
            Release::new(data, release_recorded),
        )
    }
}

/// The release hook of `recorded`: records the release, then frees the block.
///
/// # Safety
///
/// `data` is from `Arc::into_raw` of a `Log`, and `ptr` and `capacity` are the block of a
/// vector that nothing dropped. Nothing uses them after this call.
#[expect(
    unsafe_code,
    reason = "the hook takes back the log and the block that `recorded` gave"
)]
unsafe fn release_recorded(data: *const (), ptr: NonNull<u8>, capacity: usize) {
    // SAFETY: `recorded` made `data` with `Arc::into_raw`, and this call is its only use.
    let log = unsafe { Arc::from_raw(data.cast::<Mutex<Vec<&'static str>>>()) };
    note(&log, "released");
    // SAFETY: the caller gives the block of a vector that nothing dropped.
    drop(unsafe { Vec::from_raw_parts(ptr.as_ptr(), 0, capacity) });
}

fn budget() -> Budget {
    Budget::new(NonZeroU64::MIN.saturating_add(99))
}

fn driver(config: Config) -> Driver {
    Driver::new(config).expect("the test host allows io_uring")
}

/// Submits a read of the pipe `fd`, which the driver then owns, into the buffer of `held`.
#[expect(unsafe_code, reason = "the read names the buffer of its resources")]
fn read(driver: &mut Driver, fd: OwnedFd, mut held: Held) -> Completion {
    let resource = driver.adopt(fd).expect("the test driver has a free resource slot");
    let fd = driver.fd(resource).expect("the driver owns the pipe");
    let buffer = held.buffer.as_mut().expect("a read has a buffer");
    let len = u32::try_from(buffer.capacity()).expect("a test buffer is small");
    let entry = Sqe::read(fd, buffer.as_mut_ptr(), len);
    // SAFETY: the buffer of `held` keeps its address while `held` moves, and the pipe is a resource
    // of the driver.
    unsafe { driver.submit(held, entry, true) }.expect("the test driver has room")
}

/// Turns the driver until a turn gives a completion to its operation.
fn drive_until_completed(driver: &mut Driver) {
    loop {
        let turn = driver.turn().expect("a turn of the test driver works");
        if turn.completed() > 0 {
            return;
        }
        if !turn.more() {
            driver.wait().expect("the wait of the test driver works");
        }
    }
}

/// Counts its wakes.
#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Polls `future` to its end on this thread.
fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

fn held(log: &Log, budget: &Budget) -> Held {
    Held {
        buffer: Some(recorded(log, 16)),
        charge: Some(budget.reserve(10).expect("the test budget has room")),
    }
}

/// A future polled with waker A, then with waker B, wakes only B at the completion.
#[test]
fn a_completion_wakes_only_the_waker_of_its_latest_poll() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, mut writer) = pipe();
    let mut future = read(&mut driver, reader, held(&log, &budget));
    assert!(!driver.turn().unwrap().more());
    let (a, b) = (Arc::new(Count::default()), Arc::new(Count::default()));
    for waker in [&a, &b] {
        let waker = Waker::from(Arc::clone(waker));
        assert!(
            Pin::new(&mut future)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }

    writer.write_all(b"x").unwrap();
    drive_until_completed(&mut driver);
    assert_eq!((a.0.load(Ordering::SeqCst), b.0.load(Ordering::SeqCst)), (0, 1));
    let waker = Waker::from(b);
    let Poll::Ready(Ok((1, _))) = Pin::new(&mut future).poll(&mut Context::from_waker(&waker)) else {
        panic!("the read gives its byte");
    };
}

/// A forgotten future ends no operation: the buffer and the charge stay until the terminal
/// completion, and teardown releases them after it.
#[test]
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
fn a_forgotten_future_releases_its_buffer_and_charge_only_after_the_terminal_completion() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, mut writer) = pipe();
    mem::forget(read(&mut driver, reader, held(&log, &budget)));
    driver.turn().unwrap();
    assert!(taken(&log).is_empty());
    assert_eq!(budget.available(), 90);

    writer.write_all(b"x").unwrap();
    drive_until_completed(&mut driver);
    note(&log, "completed");
    assert_eq!(driver.teardown().unwrap().cancelled(), 0);
    assert_eq!(taken(&log), ["completed", "released"]);
    assert_eq!(budget.available(), 100);
}

/// A future on a second thread completes while the first thread drives the home.
#[test]
fn a_future_on_a_second_thread_completes_while_the_first_drives() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, mut writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    let consumer = thread::spawn(move || block_on(future).map(|(result, held)| (result, held.charge.is_some())));
    driver.turn().unwrap();
    writer.write_all(b"data").unwrap();
    drive_until_completed(&mut driver);
    assert_eq!(consumer.join().unwrap(), Ok((4, true)));
    assert_eq!(taken(&log), ["released"]);
    assert_eq!(budget.available(), 100);
}

/// A turn gives at most its bound of completions, and it reports the work that remains.
#[test]
#[expect(unsafe_code, reason = "a no-op names no memory")]
fn a_turn_stops_at_its_bound_and_reports_the_remaining_work() {
    let mut driver = driver(Config::new().completions_per_turn(2));
    let futures: Vec<Completion> = (0..3)
        // SAFETY: a no-op names no memory.
        .map(|_| unsafe { driver.submit(Held::default(), Sqe::nop(), false) }.unwrap())
        .collect();
    let turns = [driver.turn().unwrap(), driver.turn().unwrap()];
    assert_eq!(
        turns.map(|turn| (turn.completed(), turn.more())),
        [(2, true), (1, false)]
    );
    for future in futures {
        assert!(matches!(block_on(future), Ok((0, _))));
    }
}

/// With the task-run flag set and no entry to submit, a turn enters the kernel, which runs the
/// deferred work, and the pending read completes.
#[test]
fn a_turn_enters_the_kernel_for_deferred_work_with_nothing_to_submit() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, mut writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();
    writer.write_all(b"x").unwrap();
    driver.wait().unwrap();
    assert!(driver.ring.task_run_pending());
    assert_eq!(driver.ring.unsubmitted(), 0);
    assert_eq!(driver.turn().unwrap().completed(), 1);
    assert!(matches!(block_on(future), Ok((1, _))));
}

/// Teardown cancels a read that gets no data, returns after the completion of the read, closes
/// the pipe, and reports the cancelled read. Its future then gets `Gone`.
#[test]
fn teardown_cancels_a_read_with_no_data_closes_its_pipe_and_reports_it() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, mut writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();

    assert_eq!(driver.teardown().unwrap().cancelled(), 1);
    assert_eq!(taken(&log), ["released"]);
    assert_eq!(budget.available(), 100);
    assert_eq!(writer.write(b"x").unwrap_err().kind(), ErrorKind::BrokenPipe);
    assert!(matches!(block_on(future), Err(Gone)));
}

/// A driver that drops with a running read ends the future with `Gone`, and it keeps the buffer
/// that the kernel can still use.
#[test]
fn a_dropped_driver_ends_its_futures_and_keeps_the_buffers_of_running_operations() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, _writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();
    drop(driver);
    assert!(matches!(block_on(future), Err(Gone)));
    assert!(taken(&log).is_empty());
    assert_eq!(budget.available(), 90);
}

/// A driver with each operation slot in use refuses the next submission with a typed reason, and
/// it gives back the buffer and the charge.
#[test]
#[expect(unsafe_code, reason = "a no-op names no memory")]
fn a_full_driver_refuses_a_submission_and_returns_its_resources() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new().operations(1));
    // SAFETY: a no-op names no memory.
    let running = unsafe { driver.submit(Held::default(), Sqe::nop(), false) }.unwrap();
    // SAFETY: as above.
    let Err(Refused {
        reason: Reason::Operations,
        held,
    }) = (unsafe { driver.submit(held(&log, &budget), Sqe::nop(), false) })
    else {
        panic!("a full driver refuses");
    };
    assert!(held.buffer.is_some());
    assert_eq!(budget.available(), 90);
    drop(held);
    assert_eq!(taken(&log), ["released"]);
    assert_eq!(budget.available(), 100);
    drop(running);
}
