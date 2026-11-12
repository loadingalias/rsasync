use alloc::task::Wake;
use core::alloc::Layout;
use core::ffi::c_int;
use core::future::Future;
use core::mem::{self, ManuallyDrop};
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::{Pin, pin};
use core::ptr::NonNull;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
use std::fs::File;
use std::io::{ErrorKind, Read as _, Write as _};
use std::os::fd::{FromRawFd as _, OwnedFd};
use std::sync::{Arc, Mutex};
use std::thread::{self, Thread};
use std::vec::Vec;

use super::{Completion, Config, Driver, Held, Reason, Refused};
use crate::buffer::{Backing, Buffer as _, BufferMut, Lease, Pool, Release, WriteLoan};
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
    unsafe { driver.submit(held, Some(resource), |held| held, |_| entry, true) }.expect("the test driver has room")
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
        ..Held::default()
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
        .map(|_| unsafe { driver.submit((), None, |()| Held::default(), |_| Sqe::nop(), false) }.unwrap())
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
/// deferred work, and the pending read completes. The write wakes the poll of the read, which
/// queues the deferred work and sets the flag before the write returns.
#[test]
fn a_turn_enters_the_kernel_for_deferred_work_with_nothing_to_submit() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, mut writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();
    writer.write_all(b"x").unwrap();
    assert!(driver.ring.task_run_pending());
    assert_eq!(driver.ring.unsubmitted(), 0);
    assert_eq!(driver.turn().unwrap().completed(), 1);
    assert!(matches!(block_on(future), Ok((1, _))));
}

/// A wake from another thread ends a wait of the home that has no completion to wait for, also
/// when the wake comes before the wait.
#[test]
fn a_wake_from_another_thread_ends_a_wait() {
    let mut driver = driver(Config::new());
    let wake = driver.wake_handle();
    let waker = thread::spawn(move || wake.wake());
    driver.wait().unwrap();
    waker.join().unwrap();
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
    let running = unsafe { driver.submit((), None, |()| Held::default(), |_| Sqe::nop(), false) }.unwrap();
    // SAFETY: as above.
    let Err(Refused {
        reason: Reason::Operations,
        value: held,
    }) = (unsafe { driver.submit(held(&log, &budget), None, |held| held, |_| Sqe::nop(), false) })
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

/// Turns the driver and polls `future` on the home thread, until it gives its output. It polls
/// once after each turn, so it never polls a future that gave its output.
fn drive<F: Future + Unpin>(driver: &mut Driver, mut future: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        let more = driver.turn().expect("a turn of the test driver works").more();
        if let Poll::Ready(output) = Pin::new(&mut future).poll(&mut cx) {
            return output;
        }
        if !more {
            driver.wait().expect("the wait of the test driver works");
        }
    }
}

/// Enters the kernel until it posts a completion, and gives the `len` bytes at `address` before
/// the driver takes that completion. A copy that the driver makes when it takes the completion is
/// not in them, so they are the bytes that the kernel itself wrote at `address`.
#[expect(unsafe_code, reason = "the test reads initialized memory that nothing writes now")]
fn written_in_place(driver: &mut Driver, address: *const u8, len: usize) -> Vec<u8> {
    while !driver.ring.ready() {
        driver.ring.submit(1).expect("the test ring enters the kernel");
    }
    // SAFETY: `address` is in a buffer that the driver holds for the completed operation, and the
    // test initialized its first `len` bytes before the submission. The kernel ended its writes
    // before it posted the completion, and the driver writes nothing before it takes it.
    unsafe { core::slice::from_raw_parts(address, len) }.to_vec()
}

/// A connection receives a request into an owned buffer after its earlier bytes, and sends a
/// response from a lease. Each buffer comes back with its exact extent and at its own address. The
/// request is at the address of the buffer when the kernel completes, before the driver takes the
/// completion: the kernel wrote it in place, with no staging buffer.
#[test]
fn a_request_into_an_owned_buffer_and_a_response_from_a_lease_keep_their_addresses() {
    let mut driver = driver(Config::new());
    let listener = driver.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream.write_all(b"GET / HTTP/1.1\r\n\r\n").unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        (stream.local_addr().unwrap(), response)
    });
    let accept = driver.accept(&listener).unwrap();
    let (stream, peer) = drive(&mut driver, accept).unwrap().unwrap();

    // Zero capacity, so that the check of the bytes in place reads initialized memory.
    let mut request = Vec::from([0; 64]);
    request.clear();
    request.extend_from_slice(b"seen:");
    let request = Backing::from(request);
    let request_address = request.as_ptr();
    let receive = driver.recv(&stream, request).unwrap();
    assert_eq!(
        written_in_place(&mut driver, request_address, 23),
        b"seen:GET / HTTP/1.1\r\n\r\n"
    );
    let (received, request) = drive(&mut driver, receive).unwrap();
    assert_eq!(received.unwrap(), 18);
    assert_eq!(request.as_ptr(), request_address);
    assert_eq!(request.bytes(), b"seen:GET / HTTP/1.1\r\n\r\n");

    let response = Lease::from(b"HTTP/1.1 200 OK\r\n\r\n".to_vec());
    let response_address = response.as_ptr();
    let send = driver.send(&stream, response).unwrap();
    let (sent, response) = drive(&mut driver, send).unwrap();
    assert_eq!(sent.unwrap(), 19);
    assert_eq!(response.as_ptr(), response_address);
    driver.close(stream).unwrap();
    let (client_address, bytes) = client.join().unwrap();
    assert_eq!(peer, client_address);
    assert_eq!(bytes, b"HTTP/1.1 200 OK\r\n\r\n");
}

/// A request in a buffer that the kernel selected is read in place, at the address of the one
/// slot of the pool, where the kernel wrote it before the driver took the completion. When the last
/// view of that buffer drops on another thread, the buffer comes back to the kernel ring through
/// the home, so the next selecting receive gets it again.
#[test]
fn a_selected_buffer_is_read_in_place_and_returns_to_its_ring_when_its_last_view_drops_elsewhere() {
    let budget = budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut pool = Pool::new(&budget, NonZeroU32::MIN, slot).unwrap();
    // Zero the slot, so that the check of the bytes in place reads initialized memory.
    let mut zeroed = WriteLoan::new(pool.acquire().unwrap());
    zeroed.fill_from(&[0; 64]);
    let slot = zeroed.into_inner().as_ptr();
    let mut driver = driver(Config::new());
    driver.provide(pool).unwrap();
    let listener = driver.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let (next, wait) = std::sync::mpsc::channel::<()>();
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream.write_all(b"first").unwrap();
        wait.recv().unwrap();
        stream.write_all(b"second").unwrap();
    });
    let accept = driver.accept(&listener).unwrap();
    let (stream, _) = drive(&mut driver, accept).unwrap().unwrap();

    let receive = driver.recv_selected(&stream).unwrap();
    assert_eq!(written_in_place(&mut driver, slot, 5), b"first");
    let first = drive(&mut driver, receive).unwrap().unwrap();
    assert_eq!((first.bytes(), first.as_ptr()), (&b"first"[..], slot));
    let view = first.view(1..).unwrap();
    drop(first);
    thread::spawn(move || assert_eq!(view.bytes(), b"irst")).join().unwrap();

    next.send(()).unwrap();
    let receive = driver.recv_selected(&stream).unwrap();
    let second = drive(&mut driver, receive).unwrap().unwrap();
    assert_eq!((second.bytes(), second.as_ptr()), (&b"second"[..], slot));
    client.join().unwrap();
}

/// `ENOBUFS` (`include/uapi/asm-generic/errno.h`).
const ENOBUFS: i32 = 105;

/// A selecting receive that finds no free buffer ends with `ENOBUFS`, which its future gives: the
/// driver does not retry it. The data stays in the socket, and a receive after the buffer comes
/// back gets it.
#[test]
fn a_selecting_receive_with_no_free_buffer_gives_enobufs() {
    let budget = budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut driver = driver(Config::new());
    driver
        .provide(Pool::new(&budget, NonZeroU32::MIN, slot).unwrap())
        .unwrap();
    let listener = driver.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let (next, wait) = std::sync::mpsc::channel::<()>();
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream.write_all(b"first").unwrap();
        wait.recv().unwrap();
        stream.write_all(b"second").unwrap();
        wait.recv().unwrap();
    });
    let accept = driver.accept(&listener).unwrap();
    let (stream, _) = drive(&mut driver, accept).unwrap().unwrap();
    let receive = driver.recv_selected(&stream).unwrap();
    let held = drive(&mut driver, receive).unwrap().unwrap();

    next.send(()).unwrap();
    let receive = driver.recv_selected(&stream).unwrap();
    let refused = drive(&mut driver, receive).unwrap().unwrap_err();
    assert_eq!(refused.raw_os_error(), Some(ENOBUFS));

    drop(held);
    let receive = driver.recv_selected(&stream).unwrap();
    assert_eq!(drive(&mut driver, receive).unwrap().unwrap().bytes(), b"second");
    next.send(()).unwrap();
    client.join().unwrap();
}
