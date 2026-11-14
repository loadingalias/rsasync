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
use std::os::fd::{AsFd as _, AsRawFd as _, FromRawFd as _, OwnedFd};
use std::sync::{Arc, Mutex};
use std::thread::{self, Thread};
use std::time::Instant;
use std::vec::Vec;

use super::{Completion, Config, Driver, ECANCELED, Held, Reason, Refused, TcpStream, WriteFailed, sys};
use crate::buffer::{Backing, Buffer as _, BufferMut as _, Lease, Pool, Release, WriteLoan};
use crate::capacity::Budget;
use crate::completion::Gone;
use crate::uring::{Cqe, Sqe};

#[expect(unsafe_code, reason = "the C library of each Linux target gives this function")]
unsafe extern "C" {
    /// `int pipe2(int pipefd[2], int flags)`.
    fn pipe2(fds: *mut c_int, flags: c_int) -> c_int;
}

/// Blocks until the wake descriptor of `driver` is readable, as the reactor of a host waits for
/// it.
#[expect(unsafe_code, reason = "poll reads and writes one pollfd")]
fn wait_readable(driver: &Driver) {
    let mut fd = sys::PollFd {
        fd: driver.as_fd().as_raw_fd(),
        events: sys::POLLIN,
        revents: 0,
    };
    // SAFETY: `fd` is one valid `pollfd` during the call.
    assert_eq!(unsafe { sys::poll(&raw mut fd, 1, -1) }, 1);
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

/// Submits a read of the pipe `fd`, which the driver then owns, into the loan of `held`.
#[expect(unsafe_code, reason = "the read names the buffer of its resources")]
fn read(driver: &mut Driver, fd: OwnedFd, mut held: Held) -> Completion {
    let resource = driver.adopt(fd).expect("the test driver has a free resource slot");
    let fd = driver.fd(resource).expect("the driver owns the pipe");
    let loan = held.loan.as_mut().expect("a read has a loan");
    let len = u32::try_from(loan.capacity()).expect("a test buffer is small");
    let entry = Sqe::read(fd, loan.as_mut_ptr(), len);
    // SAFETY: the loan of `held` keeps its address while `held` moves, and the pipe is a resource
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
        loan: Some(WriteLoan::new(recorded(log, 16))),
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
/// deferred work, and the pending read completes. The test waits for the wake descriptor, as a
/// host reactor does, because `Driver::wait` runs the deferred work itself.
#[test]
fn a_turn_enters_the_kernel_for_deferred_work_with_nothing_to_submit() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, mut writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();
    writer.write_all(b"x").unwrap();
    wait_readable(&driver);
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

/// A completion during a wait adds no other completion. The poll that a wait keeps in the ring
/// watches a descriptor that only the other threads signal, not the wake descriptor, which the
/// kernel signals at each completion.
#[test]
fn a_completion_during_a_wait_adds_no_other_completion() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, mut writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();
    writer.write_all(b"x").unwrap();
    driver.wait().unwrap();
    let mut completed = 0;
    loop {
        let turn = driver.turn().unwrap();
        completed += turn.completed();
        if !turn.more() {
            break;
        }
    }
    assert_eq!(completed, 1);
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
    let running = unsafe { driver.submit((), None, |()| Held::default(), |_| Sqe::nop(), false) }.unwrap();
    // SAFETY: as above.
    let Err(Refused {
        reason: Reason::Operations,
        value: held,
    }) = (unsafe { driver.submit(held(&log, &budget), None, |held| held, |_| Sqe::nop(), false) })
    else {
        panic!("a full driver refuses");
    };
    assert!(held.loan.is_some());
    assert_eq!(budget.available(), 90);
    drop(held);
    assert_eq!(taken(&log), ["released"]);
    assert_eq!(budget.available(), 100);
    drop(running);
}

/// A future polled after it gave `Ready` panics, as the futures of the standard library do. It
/// does not report that the driver ended, which it did not.
#[test]
#[should_panic(expected = "polled after it gave `Ready`")]
#[expect(unsafe_code, reason = "a no-op names no memory")]
fn a_poll_after_ready_panics() {
    let mut driver = driver(Config::new());
    // SAFETY: a no-op names no memory.
    let mut nop = unsafe { driver.submit((), None, |()| Held::default(), |_| Sqe::nop(), false) }.unwrap();
    drive(&mut driver, &mut nop).unwrap();
    drop(Pin::new(&mut nop).poll(&mut Context::from_waker(Waker::noop())));
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
    let mut listener = driver.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream.write_all(b"GET / HTTP/1.1\r\n\r\n").unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        (stream.local_addr().unwrap(), response)
    });
    let accept = driver.accept(&mut listener).unwrap();
    let (mut stream, peer) = drive(&mut driver, accept).unwrap().unwrap();

    // Zero capacity, so that the check of the bytes in place reads initialized memory.
    let mut request = Vec::from([0; 64]);
    request.clear();
    request.extend_from_slice(b"seen:");
    let request = Backing::from(request);
    let request_address = request.as_ptr();
    let receive = driver.recv(&mut stream, request).unwrap();
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
    let send = driver.send(&mut stream, response).unwrap();
    let (sent, response) = drive(&mut driver, send).unwrap();
    assert_eq!(sent.unwrap(), 19);
    assert_eq!(response.as_ptr(), response_address);
    let close = driver.close(stream).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), None);
    let (client_address, bytes) = client.join().unwrap();
    assert_eq!(peer, client_address);
    assert_eq!(bytes, b"HTTP/1.1 200 OK\r\n\r\n");
}

/// A record in a buffer that the kernel selected is decrypted in place, at the address of the one
/// slot of the pool, where the kernel wrote it before the driver took the completion. The receive
/// gives the slot as a writable backing, so the decryption needs no copy, and the decrypted bytes
/// become a lease at the same address. When the last view of that buffer drops on another thread,
/// the buffer comes back to the kernel ring through the home, so the next selecting receive gets it
/// again.
#[test]
fn a_selected_buffer_is_decrypted_in_place_and_returns_to_its_ring_when_its_last_view_drops_elsewhere() {
    let budget = budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut pool = Pool::new(&budget, NonZeroU32::MIN, slot).unwrap();
    // Zero the slot, so that the check of the bytes in place reads initialized memory.
    let mut zeroed = WriteLoan::new(pool.acquire().unwrap());
    zeroed.fill_from(&[0; 64]);
    let slot = zeroed.into_inner().as_ptr();
    let mut driver = driver(Config::new());
    driver.provide(pool).unwrap();
    let mut listener = driver.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let (next, wait) = std::sync::mpsc::channel::<()>();
    // The "cipher" of the record: each byte XOR this key.
    const KEY: u8 = 0x5a;
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream.write_all(&b"first".map(|byte| byte ^ KEY)).unwrap();
        wait.recv().unwrap();
        stream.write_all(b"second").unwrap();
    });
    let accept = driver.accept(&mut listener).unwrap();
    let (mut stream, _) = drive(&mut driver, accept).unwrap().unwrap();

    let receive = driver.recv_selected(&mut stream).unwrap();
    assert_eq!(written_in_place(&mut driver, slot, 5), b"first".map(|byte| byte ^ KEY));
    let mut first = drive(&mut driver, receive).unwrap().unwrap();
    for byte in first.bytes_mut() {
        *byte ^= KEY;
    }
    let first = Lease::from(first);
    assert_eq!((first.bytes(), first.as_ptr()), (&b"first"[..], slot));
    let view = first.view(1..).unwrap();
    drop(first);
    thread::spawn(move || assert_eq!(view.bytes(), b"irst")).join().unwrap();

    next.send(()).unwrap();
    let receive = driver.recv_selected(&mut stream).unwrap();
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
    let mut listener = driver.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let (next, wait) = std::sync::mpsc::channel::<()>();
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream.write_all(b"first").unwrap();
        wait.recv().unwrap();
        stream.write_all(b"second").unwrap();
        wait.recv().unwrap();
    });
    let accept = driver.accept(&mut listener).unwrap();
    let (mut stream, _) = drive(&mut driver, accept).unwrap().unwrap();
    let receive = driver.recv_selected(&mut stream).unwrap();
    let held = drive(&mut driver, receive).unwrap().unwrap();

    next.send(()).unwrap();
    let receive = driver.recv_selected(&mut stream).unwrap();
    let refused = drive(&mut driver, receive).unwrap().unwrap_err();
    assert_eq!(refused.raw_os_error(), Some(ENOBUFS));

    drop(held);
    let receive = driver.recv_selected(&mut stream).unwrap();
    assert_eq!(drive(&mut driver, receive).unwrap().unwrap().bytes(), b"second");
    next.send(()).unwrap();
    client.join().unwrap();
}

/// Turns the driver each time that its wake descriptor is readable, as the reactor of a host does,
/// and polls `future` after each turn, until it gives its output. It never calls `Driver::wait`.
fn drive_from_reactor<F: Future + Unpin>(driver: &mut Driver, mut future: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        let more = driver.turn().expect("a turn of the test driver works").more();
        if let Poll::Ready(output) = Pin::new(&mut future).poll(&mut cx) {
            return output;
        }
        if !more {
            wait_readable(driver);
        }
    }
}

/// A reactor that waits only for the wake descriptor sees each of two selecting receives that wait
/// for their data. On a kernel with the eventfd defect of `probe`, the second receive completes
/// only because the driver registers the descriptor again.
#[test]
fn a_host_reactor_wakes_for_two_selecting_receives_that_wait_for_data() {
    let budget = budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut driver = driver(Config::new());
    driver
        .provide(Pool::new(&budget, NonZeroU32::MIN, slot).unwrap())
        .unwrap();
    let mut listener = driver.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let (next, wait) = std::sync::mpsc::channel::<&'static [u8]>();
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        for bytes in wait {
            stream.write_all(bytes).unwrap();
        }
    });
    let accept = driver.accept(&mut listener).unwrap();
    let (mut stream, _) = drive_from_reactor(&mut driver, accept).unwrap().unwrap();
    for bytes in [&b"first"[..], b"second"] {
        let receive = driver.recv_selected(&mut stream).unwrap();
        // The turn submits the receive, which finds no data and waits for it.
        driver.turn().unwrap();
        next.send(bytes).unwrap();
        let received = drive_from_reactor(&mut driver, receive).unwrap().unwrap();
        assert_eq!(received.bytes(), bytes);
    }
    drop(next);
    client.join().unwrap();
}

/// Only the first driver of a process probes the kernel: a second driver gets the same result and
/// makes no probe. On a kernel with the defect, the probe waits for a deferred signal that does not
/// come, so the start of the second driver is then far shorter than the start of the first.
#[test]
fn only_the_first_driver_of_a_process_probes_the_kernel() {
    let start = Instant::now();
    let first = driver(Config::new());
    let probed = start.elapsed();
    let start = Instant::now();
    let second = driver(Config::new());
    let read = start.elapsed();
    assert_eq!(second.rearm_wake, first.rearm_wake);
    if first.rearm_wake {
        assert!(
            read * 10 < probed,
            "the second start took {read:?}, and the first {probed:?}"
        );
    }
}

/// The probe finds the eventfd defect on the kernels of the evidence hosts that have it: the
/// kernel `7.0.0-1016-aws` of Ubuntu 26.04 has `cd305ee3633a` and not `04fe9aeb4f3c`, and Linux
/// 6.1 of Debian 12 has neither. On another kernel, only the start of the driver is checked.
#[test]
fn the_probe_finds_the_eventfd_defect_on_the_kernels_that_have_it() {
    let release = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap();
    let driver = driver(Config::new());
    let expected = match release.trim() {
        "7.0.0-1016-aws" => Some(true),
        release if release.starts_with("6.1.") => Some(false),
        _ => None,
    };
    if let Some(expected) = expected {
        assert_eq!(driver.rearm_wake, expected, "the kernel {release}");
    }
}

/// The bytes of a numbered stream from `start`: each byte is its position modulo 251, so a lost
/// or a repeated run of bytes changes the bytes after it.
fn numbered(start: usize, len: usize) -> Vec<u8> {
    (start..start + len)
        .map(|position| u8::try_from(position % 251).expect("a remainder of 251 fits a byte"))
        .collect()
}

/// A stream of the driver that a listener at `address` accepted, and its peer. The listener is
/// closed.
fn connected(driver: &mut Driver, address: &str) -> (TcpStream, std::net::TcpStream) {
    let address = address.parse().expect("the test gives a socket address");
    let mut listener = driver.bind(address).expect("the test host binds loopback");
    let peer = std::net::TcpStream::connect(listener.local_addr()).expect("the peer connects");
    let accept = driver.accept(&mut listener).expect("the test driver has room");
    let (stream, _) = drive(driver, accept)
        .expect("the driver runs")
        .expect("the listener accepts");
    let close = driver.close_listener(listener).expect("the test driver has room");
    drive(driver, close).expect("the driver runs");
    (stream, peer)
}

/// Submits and enters the kernel until it posted at least `count` completions, and gives them.
/// They stay in the ring for the driver.
fn posted(driver: &mut Driver, count: usize) -> Vec<Cqe> {
    loop {
        let ready = driver.ring.peek();
        if ready.len() >= count {
            return ready;
        }
        driver.ring.submit(1).expect("the test ring enters the kernel");
    }
}

/// A send of 64 MiB, more than the socket buffers hold while the peer does not read, so the
/// kernel sends only part of it. Gives the count that it sent, from the numbered stream.
fn fill(driver: &mut Driver, stream: &mut TcpStream) -> usize {
    let send = driver
        .send(stream, Lease::from(numbered(0, 64 << 20)))
        .expect("the test driver has room");
    let sent = drive(driver, send).expect("the driver runs").0.expect("the send works");
    assert!(sent < 64 << 20, "the socket buffers held the whole send");
    sent
}

/// A receive whose future drops after its bytes came leaves them with the stream. The next
/// receives give them before they submit, as many as fit each buffer, with one counted copy, and
/// the stream keeps the rest. Each byte comes once and in order with the bytes after it.
#[test]
fn a_dropped_receive_leaves_its_bytes_to_the_next_receives_in_order() {
    let mut driver = driver(Config::new());
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    peer.write_all(&numbered(0, 1000)).unwrap();
    let receive = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    let kept = usize::try_from(posted(&mut driver, 1)[0].res).unwrap();
    drop(receive);
    peer.write_all(&numbered(1000, 500)).unwrap();
    peer.shutdown(std::net::Shutdown::Write).unwrap();

    // Receive until the end of the stream, so that a lost run of bytes shows as a short stream.
    let mut received = Vec::new();
    loop {
        let receive = driver
            .recv(&mut stream, Backing::from(Vec::with_capacity(300)))
            .unwrap();
        let (count, buffer) = drive(&mut driver, receive).unwrap();
        if count.unwrap() == 0 {
            break;
        }
        received.extend_from_slice(buffer.bytes());
    }
    assert_eq!(received, numbered(0, 1500));
    assert_eq!(driver.copied(), u64::try_from(kept).unwrap());
}

/// An explicit cancel of a receive whose bytes came gives the original buffer with the bytes,
/// with no copy.
#[test]
fn an_explicit_cancel_of_a_receive_gives_its_buffer_and_bytes_with_no_copy() {
    let mut driver = driver(Config::new());
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    peer.write_all(b"hello").unwrap();
    let buffer = Backing::from(Vec::with_capacity(64));
    let address = buffer.as_ptr();
    let receive = driver.recv(&mut stream, buffer).unwrap();
    posted(&mut driver, 1);
    receive.cancel();
    let (count, buffer) = drive(&mut driver, receive).unwrap();
    assert_eq!(
        (count.unwrap(), buffer.as_ptr(), buffer.bytes()),
        (5, address, &b"hello"[..])
    );
    assert_eq!(driver.copied(), 0);
}

/// A receive whose future is forgotten after its bytes came leaves them to the next receive on
/// its half, which gives them before it submits.
#[test]
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
fn a_forgotten_receive_leaves_its_bytes_to_the_next_receive_which_gives_them_before_it_submits() {
    let mut driver = driver(Config::new());
    let (mut stream, mut peer) = connected(&mut driver, "[::1]:0");
    let (reader, _) = stream.split();
    let receive = driver.recv(reader, Backing::from(Vec::with_capacity(64))).unwrap();
    peer.write_all(b"kept").unwrap();
    drive_until_completed(&mut driver);
    mem::forget(receive);

    let next = driver.recv(reader, Backing::from(Vec::with_capacity(64))).unwrap();
    assert_eq!(driver.ring.unsubmitted(), 0, "the receive submitted");
    let (count, buffer) = drive(&mut driver, next).unwrap();
    assert_eq!((count.unwrap(), buffer.bytes()), (4, &b"kept"[..]));
    assert_eq!(driver.copied(), 4);
}

/// A send whose future drops sends only part of its bytes, so the write direction fails. The next
/// send gives the count of the bytes that went, each later send gives no count, and the close
/// reports none: the count is reported once. The peer reads exactly those bytes.
#[test]
fn a_dropped_partial_send_fails_the_write_direction_and_reports_its_count_once() {
    let mut driver = driver(Config::new());
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    drop(driver.send(&mut stream, Lease::from(numbered(0, 64 << 20))).unwrap());
    let sent = usize::try_from(posted(&mut driver, 1)[0].res).unwrap();
    assert!(0 < sent && sent < 64 << 20, "the send sent all or nothing: {sent}");
    driver.turn().unwrap();

    let next = driver.send(&mut stream, Lease::from(b"next".to_vec())).unwrap();
    let failed = drive(&mut driver, next).unwrap().0.unwrap_err();
    assert_eq!(WriteFailed::of(&failed).map(WriteFailed::sent), Some(Some(sent)));
    let later = driver.send(&mut stream, Lease::from(b"later".to_vec())).unwrap();
    let failed = drive(&mut driver, later).unwrap().0.unwrap_err();
    assert_eq!(WriteFailed::of(&failed).map(WriteFailed::sent), Some(None));
    let close = driver.close(stream).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), None);

    let mut received = Vec::new();
    peer.read_to_end(&mut received).unwrap();
    assert!(received == numbered(0, sent), "the peer read other bytes");
}

/// A send whose future drops and that sends all its bytes reports nothing, and the next send
/// succeeds after it.
#[test]
fn a_dropped_send_that_sends_all_its_bytes_reports_nothing_and_the_next_send_succeeds() {
    let mut driver = driver(Config::new());
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    drop(driver.send(&mut stream, Lease::from(numbered(0, 1000))).unwrap());
    let next = driver.send(&mut stream, Lease::from(numbered(1000, 1000))).unwrap();
    assert_eq!(drive(&mut driver, next).unwrap().0.unwrap(), 1000);
    let close = driver.close(stream).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), None);

    let mut received = Vec::new();
    peer.read_to_end(&mut received).unwrap();
    assert_eq!(received, numbered(0, 2000));
}

/// Two sends whose futures drop, one in the kernel and one that waits for it, still run, and a
/// third send waits behind them and completes after both. Each submits after the end of the one
/// before it: the peer reads one numbered stream in order.
#[test]
fn two_dropped_sends_and_a_third_send_run_in_start_order() {
    let mut driver = driver(Config::new());
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let filled = fill(&mut driver, &mut stream);
    let first = driver.send(&mut stream, Lease::from(numbered(filled, 4096))).unwrap();
    driver.turn().unwrap();
    drop(first);
    drop(
        driver
            .send(&mut stream, Lease::from(numbered(filled + 4096, 4096)))
            .unwrap(),
    );
    let third = driver
        .send(&mut stream, Lease::from(numbered(filled + 8192, 4096)))
        .unwrap();
    assert_eq!(
        driver.ring.unsubmitted(),
        0,
        "a send submitted before the send before it ended"
    );

    let reader = thread::spawn(move || {
        let mut received = Vec::new();
        peer.read_to_end(&mut received).unwrap();
        received
    });
    assert_eq!(drive(&mut driver, third).unwrap().0.unwrap(), 4096);
    let close = driver.close(stream).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), None);
    assert!(
        reader.join().unwrap() == numbered(0, filled + 3 * 4096),
        "the peer read other bytes"
    );
}

/// An explicit cancel of a send that waits for room gives its lease and its progress, and the
/// next send in the direction succeeds.
#[test]
fn an_explicit_cancel_of_a_send_gives_its_progress_and_keeps_the_direction_usable() {
    let mut driver = driver(Config::new());
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let filled = fill(&mut driver, &mut stream);
    let send = driver.send(&mut stream, Lease::from(numbered(filled, 4096))).unwrap();
    driver.turn().unwrap();
    send.cancel();
    let (result, lease) = drive(&mut driver, send).unwrap();
    assert_eq!(result.unwrap_err().raw_os_error(), Some(ECANCELED));
    assert_eq!(lease.initialized(), 4096);

    let reader = thread::spawn(move || {
        let mut received = Vec::new();
        peer.read_to_end(&mut received).unwrap();
        received
    });
    let next = driver.send(&mut stream, lease).unwrap();
    assert_eq!(drive(&mut driver, next).unwrap().0.unwrap(), 4096);
    let close = driver.close(stream).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), None);
    assert!(
        reader.join().unwrap() == numbered(0, filled + 4096),
        "the peer read other bytes"
    );
}

/// An accept whose future drops after the kernel accepted a connection leaves it with the
/// listener, and the next accept gives it, with its peer address, before it submits. On IPv4 and
/// on IPv6.
#[test]
fn a_dropped_accept_leaves_its_connection_to_the_next_accept() {
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let mut driver = driver(Config::new());
        let mut listener = driver.bind(address.parse().unwrap()).unwrap();
        let address = listener.local_addr();
        let accept = driver.accept(&mut listener).unwrap();
        let mut client = std::net::TcpStream::connect(address).unwrap();
        posted(&mut driver, 1);
        drop(accept);
        driver.turn().unwrap();

        let next = driver.accept(&mut listener).unwrap();
        assert_eq!(driver.ring.unsubmitted(), 0, "the accept submitted");
        let (mut stream, peer) = drive(&mut driver, next).unwrap().unwrap();
        assert_eq!(peer, client.local_addr().unwrap());
        client.write_all(b"up").unwrap();
        let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(8))).unwrap();
        assert_eq!(drive(&mut driver, receive).unwrap().1.bytes(), b"up");
    }
}

/// A close releases the bytes that no receive took, through the release hook of their buffer,
/// and a close of a listener closes the connection that no accept took: its client reads the end
/// of the stream.
#[test]
fn close_releases_the_bytes_and_the_connection_that_no_operation_took() {
    let log = Log::default();
    let mut driver = driver(Config::new());
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    peer.write_all(b"unread").unwrap();
    let receive = driver.recv(&mut stream, recorded(&log, 64)).unwrap();
    posted(&mut driver, 1);
    drop(receive);
    driver.turn().unwrap();
    assert!(taken(&log).is_empty(), "the stream did not keep the bytes");
    let close = driver.close(stream).unwrap();
    drive(&mut driver, close).unwrap();
    assert_eq!(taken(&log), ["released"]);

    let mut listener = driver.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let accept = driver.accept(&mut listener).unwrap();
    let mut client = std::net::TcpStream::connect(address).unwrap();
    posted(&mut driver, 1);
    drop(accept);
    driver.turn().unwrap();
    let close = driver.close_listener(listener).unwrap();
    drive(&mut driver, close).unwrap();
    let mut byte = [0];
    assert_eq!(client.read(&mut byte).unwrap(), 0);
}

/// A close of a stream whose peer neither reads nor sends, with a dropped receive and a dropped
/// send in the kernel, sends one cancel for each of them, by its own user data. Each cancel finds
/// its operation in flight, each operation gets its own result, and the close returns and reports
/// the count of the send.
#[test]
fn close_cancels_each_dropped_operation_by_its_own_request_and_reports_the_send() {
    let mut driver = driver(Config::new());
    let (mut stream, _peer) = connected(&mut driver, "127.0.0.1:0");
    let filled = fill(&mut driver, &mut stream);
    let (reader, writer) = stream.split();
    let receive = driver.recv(reader, Backing::from(Vec::with_capacity(64))).unwrap();
    let send = driver.send(writer, Lease::from(numbered(filled, 4096))).unwrap();
    driver.turn().unwrap();
    drop((receive, send));

    let close = driver.close(stream).unwrap();
    assert_eq!(
        driver.ring.unsubmitted(),
        2,
        "the close did not push one cancel for each operation"
    );
    let posted = posted(&mut driver, 4);
    let cancel = 1 << 63;
    let (results, targets): (Vec<&Cqe>, Vec<&Cqe>) = posted.iter().partition(|cqe| cqe.user_data & cancel != 0);
    assert_eq!(
        results.iter().map(|cqe| cqe.res).collect::<Vec<_>>(),
        [0, 0],
        "a cancel missed its target"
    );
    for target in &targets {
        assert_eq!(target.res, -ECANCELED);
        assert_eq!(
            results
                .iter()
                .filter(|result| result.user_data == target.user_data | cancel)
                .count(),
            1
        );
    }
    assert_eq!(drive(&mut driver, close).unwrap().sent(), Some(0));
}

/// Idle connections whose receives wait for a buffer that the kernel selects hold no receive
/// buffer: eight receives wait while the pool has one slot, and its charge does not change. The
/// data of the last connection then arrives in that slot.
#[test]
fn idle_connections_with_a_waiting_selecting_receive_hold_no_receive_buffer() {
    let budget = budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut driver = driver(Config::new());
    driver
        .provide(Pool::new(&budget, NonZeroU32::MIN, slot).unwrap())
        .unwrap();
    let charged = budget.available();
    let (mut streams, mut peers): (Vec<_>, Vec<_>) = (0..8).map(|_| connected(&mut driver, "127.0.0.1:0")).unzip();
    let mut waiting = Vec::new();
    for stream in &mut streams {
        waiting.push(driver.recv_selected(stream).unwrap());
    }
    driver.turn().unwrap();
    assert_eq!(budget.available(), charged);

    peers.last_mut().unwrap().write_all(b"last").unwrap();
    let last = waiting.pop().unwrap();
    assert_eq!(drive(&mut driver, last).unwrap().unwrap().bytes(), b"last");
    drop(waiting);
    driver.teardown().unwrap();
}
