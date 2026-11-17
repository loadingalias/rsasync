#[cfg(target_os = "linux")]
use alloc::task::Wake;
use core::alloc::Layout;
#[cfg(unix)]
use core::assert_matches;
#[cfg(target_os = "linux")]
use core::ffi::c_int;
use core::future::Future;
use core::mem::{self, ManuallyDrop};
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::Pin;
#[cfg(target_os = "linux")]
use core::pin::pin;
use core::ptr::NonNull;
#[cfg(target_os = "linux")]
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
#[cfg(target_os = "linux")]
use std::fs::File;
use std::io::{ErrorKind, Read as _, Write as _};
#[cfg(unix)]
use std::os::fd::{AsFd as _, AsRawFd as _};
#[cfg(target_os = "linux")]
use std::os::fd::{FromRawFd as _, OwnedFd};
use std::sync::{Arc, Mutex};
use std::thread;
#[cfg(target_os = "linux")]
use std::thread::Thread;
#[cfg(unix)]
use std::time::Instant;
use std::vec::Vec;

use core::net::IpAddr;

use rsnetwork_udp::{Ecn, Received};

use super::suite::{self, Backend};
use super::{
    Arrivals, Config, Datagram, Datagrams, Driver, ECANCELED, Reason, Refused, SendCount, TcpListener, TcpOptions,
    TcpStream, UdpOptions, UdpSocket, WriteFailed,
};
#[cfg(target_os = "linux")]
use super::{Call, Completion, Ended, Error, Feature, Features, Held, Medium, Route, eventfd, probe, sys};
use crate::buffer::{Backing, Buffer as _, BufferMut as _, Lease, Pool, Release, WriteLoan};
use crate::capacity::Budget;
#[cfg(target_os = "linux")]
use crate::completion::Gone;
use crate::uring::Cqe;
#[cfg(target_os = "linux")]
use crate::uring::{Ring, Sqe};

#[expect(unsafe_code, reason = "the C library of each Linux target gives this function")]
#[cfg(target_os = "linux")]
unsafe extern "C" {
    /// `int pipe2(int pipefd[2], int flags)`.
    fn pipe2(fds: *mut c_int, flags: c_int) -> c_int;
}

/// `struct pollfd` and `poll` of macOS (`sys/poll.h` of the macOS 27.2 SDK), with `nfds_t` an
/// `unsigned int`: the reactor of a host in `wait_readable`.
#[cfg(target_os = "macos")]
mod sys {
    use core::ffi::{c_int, c_short, c_uint};

    #[repr(C)]
    pub(super) struct PollFd {
        pub(super) fd: c_int,
        pub(super) events: c_short,
        pub(super) revents: c_short,
    }

    pub(super) const POLLIN: c_short = 0x1;

    #[expect(unsafe_code, reason = "libSystem of macOS gives this function")]
    unsafe extern "C" {
        pub(super) fn poll(fds: *mut PollFd, nfds: c_uint, timeout: c_int) -> c_int;
    }
}

/// Blocks until the wake descriptor of `driver` is readable, as the reactor of a host waits for
/// it.
#[expect(unsafe_code, reason = "poll reads and writes one pollfd")]
#[cfg(unix)]
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
#[cfg(target_os = "linux")]
const O_NONBLOCK: c_int = 0o4_000;
#[cfg(target_os = "linux")]
const O_CLOEXEC: c_int = 0o2_000_000;

/// A nonblocking pipe: its read end and its write end. Linux 6.1 then waits for its readiness and
/// reads it in the task work of the ring, as Linux 7.0 does for each pipe. A read of a blocking
/// pipe on 6.1 goes to an io-wq worker instead, because 6.1 pipes lack `FMODE_NOWAIT`.
#[expect(unsafe_code, reason = "the test makes its pipe with the C library")]
#[cfg(target_os = "linux")]
fn pipe() -> (OwnedFd, File) {
    let mut fds = [0; 2];
    // SAFETY: `fds` has room for the two descriptors.
    assert_eq!(unsafe { pipe2(fds.as_mut_ptr(), O_NONBLOCK | O_CLOEXEC) }, 0);
    // SAFETY: `pipe2` gave two new descriptors that nothing else owns.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), File::from_raw_fd(fds[1])) }
}

/// The order of the events of a test, from any thread.
pub(super) type Log = Arc<Mutex<Vec<&'static str>>>;

pub(super) fn note(log: &Log, what: &'static str) {
    log.lock().expect("no test thread panics with the log").push(what);
}

pub(super) fn taken(log: &Log) -> Vec<&'static str> {
    mem::take(&mut *log.lock().expect("no test thread panics with the log"))
}

/// A backing of `capacity` bytes whose release, through its release hook, records "released".
#[expect(
    unsafe_code,
    reason = "the test maps its own block into a backing, as a library does"
)]
pub(super) fn recorded(log: &Log, capacity: usize) -> Backing {
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

pub(super) fn budget() -> Budget {
    Budget::new(NonZeroU64::MIN.saturating_add(99))
}

pub(super) fn driver(config: Config) -> Driver {
    Driver::new(config).expect("the test host allows io_uring")
}

/// Submits a read of the pipe `fd`, which the driver then owns, into the loan of `held`.
#[expect(unsafe_code, reason = "the read names the buffer of its resources")]
#[cfg(target_os = "linux")]
fn read(driver: &mut Driver, fd: OwnedFd, mut held: Held) -> Completion {
    let resource = driver
        .adopt(fd, Medium::Pipe)
        .expect("the test driver has a free resource slot");
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
#[cfg(target_os = "linux")]
struct Count(AtomicUsize);

#[cfg(target_os = "linux")]
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Polls `future` to its end on this thread.
#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
fn held(log: &Log, budget: &Budget) -> Held {
    Held {
        loan: Some(WriteLoan::new(recorded(log, 16))),
        charge: Some(budget.reserve(10).expect("the test budget has room")),
        ..Held::default()
    }
}

/// A future polled with waker A, then with waker B, wakes only B at the completion.
#[test]
#[cfg(target_os = "linux")]
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
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
#[cfg(target_os = "linux")]
pub(in crate::driver) fn a_forgotten_future_releases_its_buffer_and_charge_only_after_the_terminal_completion(
    backend: Backend,
) {
    let (log, budget) = (Log::default(), budget());
    let mut driver = suite::driver(Config::new(), backend);
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
#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
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
        assert_matches!(block_on(future), Ok((0, _)));
    }
}

/// With the task-run flag set and no entry to submit, a turn enters the kernel, which runs the
/// deferred work, and the pending read completes. The test waits for the wake descriptor, as a
/// host reactor does, because `Driver::wait` runs the deferred work itself.
#[test]
#[cfg(target_os = "linux")]
fn a_turn_enters_the_kernel_for_deferred_work_with_nothing_to_submit() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new());
    let (reader, mut writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();
    writer.write_all(b"x").unwrap();
    wait_readable(&driver);
    assert!(driver.queue.ring().unwrap().task_run_pending());
    assert_eq!(driver.queue.unsubmitted(), 0);
    assert_eq!(driver.turn().unwrap().completed(), 1);
    assert_matches!(block_on(future), Ok((1, _)));
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
#[cfg(target_os = "linux")]
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
    assert_matches!(block_on(future), Ok((1, _)));
}

/// Teardown cancels a read that gets no data, returns after the completion of the read, closes
/// the pipe, and reports the cancelled read. Its future then gets `Gone`.
#[cfg(target_os = "linux")]
pub(in crate::driver) fn teardown_cancels_a_read_with_no_data_closes_its_pipe_and_reports_it(backend: Backend) {
    let (log, budget) = (Log::default(), budget());
    let mut driver = suite::driver(Config::new(), backend);
    let (reader, mut writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();

    assert_eq!(driver.teardown().unwrap().cancelled(), 1);
    assert_eq!(taken(&log), ["released"]);
    assert_eq!(budget.available(), 100);
    assert_eq!(writer.write(b"x").unwrap_err().kind(), ErrorKind::BrokenPipe);
    assert_matches!(block_on(future), Err(Gone));
}

/// A driver that drops with a running read ends the future with `Gone`, and it keeps the buffer
/// that the kernel can still use.
#[cfg(target_os = "linux")]
pub(in crate::driver) fn a_dropped_driver_ends_its_futures_and_keeps_the_buffers_of_running_operations(
    backend: Backend,
) {
    let (log, budget) = (Log::default(), budget());
    let mut driver = suite::driver(Config::new(), backend);
    let (reader, _writer) = pipe();
    let future = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();
    drop(driver);
    assert_matches!(block_on(future), Err(Gone));
    assert!(taken(&log).is_empty());
    assert_eq!(budget.available(), 90);
}

/// A driver with each operation slot in use refuses the next submission with a typed reason, and
/// it gives back the buffer and the charge.
#[expect(unsafe_code, reason = "a no-op names no memory")]
#[cfg(target_os = "linux")]
pub(in crate::driver) fn a_full_driver_refuses_a_submission_and_returns_its_resources() {
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

/// The message of a refused send names the reason and none of the bytes that the refusal gives
/// back, so a log of the error shows no data of the caller.
#[test]
fn the_debug_of_a_refused_send_shows_its_reason_and_none_of_its_bytes() {
    let refused = Refused {
        reason: Reason::Queue,
        value: Lease::from(b"secret".to_vec()),
    };
    assert_eq!(alloc::format!("{refused:?}"), "Refused { reason: Queue, .. }");
}

/// A submission that finds the table full takes the requests of ended futures first, and the
/// cancel of one of them takes the last free entry of the queue. The submission then still gets an
/// entry, so it goes to the kernel and completes.
#[test]
#[expect(unsafe_code, reason = "a no-op names no memory")]
#[cfg(target_os = "linux")]
fn a_submission_completes_after_the_requests_that_it_takes_fill_the_queue() {
    let (log, budget) = (Log::default(), budget());
    let mut driver = driver(Config::new().operations(9).submissions(8));
    // SAFETY: a no-op names no memory.
    let nop =
        |driver: &mut Driver| unsafe { driver.submit((), None, |()| Held::default(), |_| Sqe::nop(), false) }.unwrap();
    let mut done = nop(&mut driver);
    let (reader, _writer) = pipe();
    let waiting = read(&mut driver, reader, held(&log, &budget));
    driver.turn().unwrap();
    let ready = Pin::new(&mut done).poll(&mut Context::from_waker(Waker::noop()));
    assert_matches!(ready, Poll::Ready(Ok((0, _))), "the first no-op did not complete");
    // Seven entries wait in the queue of eight, and nine operations fill the table.
    let queued: Vec<Completion> = (0..7).map(|_| nop(&mut driver)).collect();
    // The ended no-op gives its slot back, and the cancel of the read takes the last entry.
    drop(done);
    drop(waiting);
    let last = nop(&mut driver);
    for future in queued.into_iter().chain([last]) {
        assert_matches!(drive(&mut driver, future), Ok((0, _)));
    }
}

/// A future polled after it gave `Ready` panics, as the futures of the standard library do. It
/// does not report that the driver ended, which it did not.
#[test]
#[should_panic(expected = "polled after it gave `Ready`")]
#[expect(unsafe_code, reason = "a no-op names no memory")]
#[cfg(target_os = "linux")]
fn a_poll_after_ready_panics() {
    let mut driver = driver(Config::new());
    // SAFETY: a no-op names no memory.
    let mut nop = unsafe { driver.submit((), None, |()| Held::default(), |_| Sqe::nop(), false) }.unwrap();
    drive(&mut driver, &mut nop).unwrap();
    drop(Pin::new(&mut nop).poll(&mut Context::from_waker(Waker::noop())));
}

/// Turns the driver and polls `future` on the home thread, until it gives its output. It polls
/// once after each turn, so it never polls a future that gave its output.
pub(super) fn drive<F: Future + Unpin>(driver: &mut Driver, mut future: F) -> F::Output {
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
    while !driver.queue.ready() {
        driver.queue.submit(1).expect("the test ring enters the kernel");
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
pub(in crate::driver) fn a_request_into_an_owned_buffer_and_a_response_from_a_lease_keep_their_addresses(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = listen(&mut driver, "127.0.0.1:0");
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
    assert_eq!(drive(&mut driver, close).unwrap().sent(), SendCount::Settled);
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
pub(in crate::driver) fn a_selected_buffer_is_decrypted_in_place_and_returns_to_its_ring_when_its_last_view_drops_elsewhere(
    backend: Backend,
) {
    let budget = budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut pool = Pool::new(&budget, NonZeroU32::MIN, slot).unwrap();
    // Zero the slot, so that the check of the bytes in place reads initialized memory.
    let mut zeroed = WriteLoan::new(pool.acquire().unwrap());
    zeroed.fill_from(&[0; 64]);
    let slot = zeroed.into_inner().as_ptr();
    let mut driver = suite::driver(Config::new(), backend);
    driver.provide(pool).unwrap();
    let mut listener = listen(&mut driver, "127.0.0.1:0");
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

/// `ENOBUFS` (`include/uapi/asm-generic/errno.h`, and `sys/errno.h` of macOS).
#[cfg(target_os = "linux")]
const ENOBUFS: i32 = 105;
#[cfg(target_os = "macos")]
const ENOBUFS: i32 = 55;
#[cfg(target_os = "windows")]
const ENOBUFS: i32 = 10055;

/// A selecting receive that finds no free buffer ends with `ENOBUFS`, which its future gives: the
/// driver does not retry it. The data stays in the socket, and a receive after the buffer comes
/// back gets it.
pub(in crate::driver) fn a_selecting_receive_with_no_free_buffer_gives_enobufs(backend: Backend) {
    let budget = budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut driver = suite::driver(Config::new(), backend);
    driver
        .provide(Pool::new(&budget, NonZeroU32::MIN, slot).unwrap())
        .unwrap();
    let mut listener = listen(&mut driver, "127.0.0.1:0");
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
#[cfg(unix)]
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
/// only because the driver registers the descriptor again. On macOS, the descriptor is the kqueue.
#[test]
#[cfg(unix)]
fn a_host_reactor_wakes_for_two_selecting_receives_that_wait_for_data() {
    let budget = budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut driver = driver(Config::new());
    driver
        .provide(Pool::new(&budget, NonZeroU32::MIN, slot).unwrap())
        .unwrap();
    let mut listener = listen(&mut driver, "127.0.0.1:0");
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
#[cfg(target_os = "linux")]
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
/// 6.1 of Debian 12 has neither. On a kernel with the defect, it also finds the defect when the
/// first deferred signal comes after the first wait, as after a slow grace period. On another
/// kernel, only the start of the driver is checked.
#[test]
#[cfg(target_os = "linux")]
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
    if expected == Some(true) {
        // A first wait of zero ends before the grace period of the first signal.
        let mut ring = Ring::new(4, 8).unwrap();
        let wake = File::from(eventfd().unwrap());
        ring.register_eventfd(wake.as_raw_fd()).unwrap();
        let defect = probe::probe(&mut ring, &wake, core::time::Duration::ZERO).unwrap();
        assert!(defect, "a late first signal hid the defect of the kernel {release}");
    }
}

/// The bytes of a numbered stream from `start`: each byte is its position modulo 251, so a lost
/// or a repeated run of bytes changes the bytes after it.
pub(super) fn numbered(start: usize, len: usize) -> Vec<u8> {
    (start..start + len)
        .map(|position| u8::try_from(position % 251).expect("a remainder of 251 fits a byte"))
        .collect()
}

/// A listener of the driver at `address`, with the default options.
fn listen(driver: &mut Driver, address: &str) -> TcpListener {
    let address = address.parse().expect("the test gives a socket address");
    let bind = driver
        .bind(address, TcpOptions::default())
        .expect("the test driver has room");
    drive(driver, bind)
        .expect("the driver runs")
        .expect("the test host binds loopback")
}

/// A stream of the driver that a listener at `address` accepted, and its peer. The listener is
/// closed.
fn connected(driver: &mut Driver, address: &str) -> (TcpStream, std::net::TcpStream) {
    let mut listener = listen(driver, address);
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
pub(super) fn posted(driver: &mut Driver, count: usize) -> Vec<Cqe> {
    loop {
        let ready = driver.queue.peek();
        if ready.len() >= count {
            return ready;
        }
        driver.queue.submit(1).expect("the test ring enters the kernel");
    }
}

/// On Windows, the socket of `stream` gets no send buffer (`SO_SNDBUF` 0), so a send larger than
/// the window of a peer that does not read waits in the kernel. With a send buffer, the stack takes
/// each byte of a send of 64 MiB at once.
#[cfg(target_os = "windows")]
fn unbuffered(driver: &mut Driver, stream: &TcpStream) {
    let options = TcpOptions {
        send_buffer: Some(0),
        ..TcpOptions::default()
    };
    driver
        .set_options(stream, options)
        .expect("the socket takes the option");
}

/// On Windows, a send goes in part only when its connection fails, so the case resets the
/// connection of the send that waits (`unbuffered`): after turns that submit it and a pause in
/// which the window of the peer fills, a close of `peer`, which has unread bytes, resets the
/// connection. The send then ends with the count of the bytes that went, and no peer remains to
/// read them.
#[cfg(target_os = "windows")]
fn reset(driver: &mut Driver, peer: std::net::TcpStream) {
    for _ in 0..10 {
        driver.turn().expect("a turn of the test driver works");
    }
    thread::sleep(core::time::Duration::from_millis(100));
    assert!(driver.queue.peek().is_empty(), "the send ended before the reset");
    drop(peer);
}

/// A send of 64 MiB, more than the socket buffers hold while the peer does not read, so the
/// kernel sends only part of it. Gives the count that it sent, from the numbered stream.
#[cfg(target_os = "linux")]
fn fill(driver: &mut Driver, stream: &mut TcpStream) -> usize {
    let send = driver
        .send(stream, Lease::from(numbered(0, 64 << 20)))
        .expect("the test driver has room");
    let sent = drive(driver, send).expect("the driver runs").0.expect("the send works");
    assert!(sent < 64 << 20, "the socket buffers held the whole send");
    sent
}

/// Sends a numbered stream from 0 until the socket buffers hold no more bytes while the peer does
/// not read, and gives the count that went. On macOS, the loopback delivers the segments of a send
/// after its return, so the window of the peer fills after the send, and the send buffer has room
/// again. Each next send of 4 MiB takes that room, until one send finds no room for 200 ms. Its
/// cancel then ends it with no byte sent. The socket buffers of macOS hold at most 8 MiB
/// (`kern.ipc.maxsockbuf`) on each side, so `SENDS_MAX` sends of 4 MiB are more than they hold.
#[cfg(target_os = "macos")]
fn fill(driver: &mut Driver, stream: &mut TcpStream) -> usize {
    const SENDS_MAX: usize = 64;
    let mut filled = 0;
    'fill: for _ in 0..SENDS_MAX {
        let mut send = driver
            .send(stream, Lease::from(numbered(filled, 4 << 20)))
            .expect("the test driver has room");
        let start = Instant::now();
        while start.elapsed() < core::time::Duration::from_millis(200) {
            driver.turn().expect("a turn of the test driver works");
            if let Poll::Ready(output) = Pin::new(&mut send).poll(&mut Context::from_waker(Waker::noop())) {
                filled += output.expect("the driver runs").0.expect("the send works");
                continue 'fill;
            }
            thread::sleep(core::time::Duration::from_millis(1));
        }
        send.cancel();
        match drive(driver, send).expect("the driver runs").0 {
            // Room came at the end of the wait, before the cancel.
            Ok(sent) => filled += sent,
            Err(error) => {
                assert_eq!(error.raw_os_error(), Some(ECANCELED), "the send failed");
                return filled;
            }
        }
    }
    panic!("the socket took {SENDS_MAX} sends of 4 MiB while its peer did not read");
}

/// A send of 64 MiB that waits in the kernel, because the peer does not read and the socket has no
/// send buffer (`unbuffered`), and whose future drops. A dropped send is not cancelled: it holds the
/// write direction until the peer reads each of its bytes, so each later send waits behind it.
/// Gives its count. A send on Windows goes whole or fails, so this form takes the place of a send
/// that goes only in part.
#[cfg(target_os = "windows")]
fn fill(driver: &mut Driver, stream: &mut TcpStream) -> usize {
    unbuffered(driver, stream);
    let send = driver
        .send(stream, Lease::from(numbered(0, 64 << 20)))
        .expect("the test driver has room");
    for _ in 0..10 {
        driver.turn().expect("a turn of the test driver works");
    }
    assert!(driver.queue.peek().is_empty(), "the send did not wait");
    drop(send);
    64 << 20
}

/// A receive whose future drops after its bytes came leaves them with the stream. The next
/// receives give them before they submit, as many as fit each buffer, with one counted copy, and
/// the stream keeps the rest. Each byte comes once and in order with the bytes after it.
pub(in crate::driver) fn a_dropped_receive_leaves_its_bytes_to_the_next_receives_in_order(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
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

/// `ECONNRESET` (`include/uapi/asm-generic/errno.h`, and `sys/errno.h` of macOS).
#[cfg(target_os = "linux")]
const ECONNRESET: i32 = 104;
#[cfg(target_os = "macos")]
const ECONNRESET: i32 = 54;
#[cfg(target_os = "windows")]
const ECONNRESET: i32 = 10054;

/// A receive whose future drops after it failed leaves its error with the stream, and the next
/// receive gives it. The peer resets the connection: it closes with bytes that it did not read.
pub(in crate::driver) fn a_dropped_receive_that_failed_leaves_its_error_to_the_next_receive(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, peer) = connected(&mut driver, "127.0.0.1:0");
    let send = driver.send(&mut stream, Lease::from(b"unread".to_vec())).unwrap();
    assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 6);
    assert_eq!(peer.peek(&mut [0; 6]).unwrap(), 6, "the bytes wait in the peer");
    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(64))).unwrap();
    drop(peer);
    assert_eq!(posted(&mut driver, 1)[0].res, -ECONNRESET);
    drop(receive);

    let next = driver.recv(&mut stream, Backing::from(Vec::with_capacity(64))).unwrap();
    let error = drive(&mut driver, next).unwrap().0.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ConnectionReset);
}

/// An explicit cancel of a receive whose bytes came gives the original buffer with the bytes,
/// with no copy.
pub(in crate::driver) fn an_explicit_cancel_of_a_receive_gives_its_buffer_and_bytes_with_no_copy(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
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
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
pub(in crate::driver) fn a_forgotten_receive_leaves_its_bytes_to_the_next_receive_which_gives_them_before_it_submits(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, mut peer) = connected(&mut driver, "[::1]:0");
    let (reader, _) = stream.split();
    let receive = driver.recv(reader, Backing::from(Vec::with_capacity(64))).unwrap();
    peer.write_all(b"kept").unwrap();
    drive_until_completed(&mut driver);
    mem::forget(receive);

    let next = driver.recv(reader, Backing::from(Vec::with_capacity(64))).unwrap();
    assert_eq!(driver.queue.unsubmitted(), 0, "the receive submitted");
    let (count, buffer) = drive(&mut driver, next).unwrap();
    assert_eq!((count.unwrap(), buffer.bytes()), (4, &b"kept"[..]));
    assert_eq!(driver.copied(), 4);
}

/// A send whose future drops sends only part of its bytes, so the write direction fails. The next
/// send gives the count of the bytes that went, each later send gives no count, and the close
/// reports none: the count is reported once. The peer reads exactly those bytes.
pub(in crate::driver) fn a_dropped_partial_send_fails_the_write_direction_and_reports_its_count_once(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, peer) = connected(&mut driver, "127.0.0.1:0");
    #[cfg(target_os = "windows")]
    unbuffered(&mut driver, &stream);
    drop(driver.send(&mut stream, Lease::from(numbered(0, 64 << 20))).unwrap());
    #[cfg(target_os = "windows")]
    reset(&mut driver, peer);
    let sent = usize::try_from(posted(&mut driver, 1)[0].res).unwrap();
    assert!(0 < sent && sent < 64 << 20, "the send sent all or nothing: {sent}");
    driver.turn().unwrap();

    let next = driver.send(&mut stream, Lease::from(b"next".to_vec())).unwrap();
    let failed = drive(&mut driver, next).unwrap().0.unwrap_err();
    assert_eq!(
        WriteFailed::of(&failed).map(WriteFailed::sent),
        Some(SendCount::Sent(sent))
    );
    let later = driver.send(&mut stream, Lease::from(b"later".to_vec())).unwrap();
    let failed = drive(&mut driver, later).unwrap().0.unwrap_err();
    assert_eq!(
        WriteFailed::of(&failed).map(WriteFailed::sent),
        Some(SendCount::Settled)
    );
    let close = driver.close(stream).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), SendCount::Settled);

    #[cfg(unix)]
    {
        let mut peer = peer;
        let mut received = Vec::new();
        peer.read_to_end(&mut received).unwrap();
        assert!(received == numbered(0, sent), "the peer read other bytes");
    }
}

/// A send whose future drops and that sends all its bytes reports nothing, and the next send
/// succeeds after it.
pub(in crate::driver) fn a_dropped_send_that_sends_all_its_bytes_reports_nothing_and_the_next_send_succeeds(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    drop(driver.send(&mut stream, Lease::from(numbered(0, 1000))).unwrap());
    let next = driver.send(&mut stream, Lease::from(numbered(1000, 1000))).unwrap();
    assert_eq!(drive(&mut driver, next).unwrap().0.unwrap(), 1000);
    let close = driver.close(stream).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), SendCount::Settled);

    let mut received = Vec::new();
    peer.read_to_end(&mut received).unwrap();
    assert_eq!(received, numbered(0, 2000));
}

/// Two sends whose futures drop, one in the kernel and one that waits for it, still run, and a
/// third send waits behind them and completes after both. Each submits after the end of the one
/// before it: the peer reads one numbered stream in order.
pub(in crate::driver) fn two_dropped_sends_and_a_third_send_run_in_start_order(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
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
        driver.queue.unsubmitted(),
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
    assert_eq!(drive(&mut driver, close).unwrap().sent(), SendCount::Settled);
    assert!(
        reader.join().unwrap() == numbered(0, filled + 3 * 4096),
        "the peer read other bytes"
    );
}

/// A lease of 4096 numbered bytes from `start`, whose release records "released" in `log`.
#[cfg(target_os = "linux")]
fn recorded_lease(log: &Log, start: usize) -> Lease {
    let mut loan = WriteLoan::new(recorded(log, 4096));
    assert_eq!(loan.fill_from(&numbered(start, 4096)), 4096);
    Lease::from(loan.into_inner())
}

/// A dropped send that waits in the driver behind a send in the kernel never runs when the close
/// comes first: the close ends it with no submission, and its lease returns once, through its
/// release hook, before the close returns. The peer reads none of its bytes.
#[cfg(target_os = "linux")]
pub(in crate::driver) fn close_ends_a_waiting_send_unsent_and_releases_its_lease_once(backend: Backend) {
    let log = Log::default();
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let filled = fill(&mut driver, &mut stream);
    drop(driver.send(&mut stream, Lease::from(numbered(filled, 4096))).unwrap());
    driver.turn().unwrap();
    drop(driver.send(&mut stream, recorded_lease(&log, filled + 4096)).unwrap());
    assert_eq!(driver.queue.unsubmitted(), 0, "the waiting send submitted");

    let close = driver.close(stream).unwrap();
    assert_eq!(
        driver.queue.unsubmitted(),
        1,
        "the close did not push exactly the cancel of the send in the kernel"
    );
    assert_eq!(drive(&mut driver, close).unwrap().sent(), SendCount::Sent(0));
    assert_eq!(taken(&log), ["released"]);
    let mut received = Vec::new();
    peer.read_to_end(&mut received).unwrap();
    assert!(
        received == numbered(0, filled),
        "the peer read bytes of a cancelled send"
    );
}

/// A send that waits in the driver behind a send in the kernel never runs when teardown comes
/// first: teardown ends it with no submission and cancels only the send in the kernel. The lease
/// of the waiting send returns once, through its release hook, and its future gives `Ended`.
#[cfg(target_os = "linux")]
pub(in crate::driver) fn teardown_ends_a_waiting_send_unsent_and_releases_its_lease_once(backend: Backend) {
    let log = Log::default();
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, _peer) = connected(&mut driver, "127.0.0.1:0");
    let filled = fill(&mut driver, &mut stream);
    drop(driver.send(&mut stream, Lease::from(numbered(filled, 4096))).unwrap());
    driver.turn().unwrap();
    let waiting = driver.send(&mut stream, recorded_lease(&log, filled + 4096)).unwrap();
    assert_eq!(driver.queue.unsubmitted(), 0, "the waiting send submitted");

    assert_eq!(driver.teardown().unwrap().cancelled(), 1);
    assert_eq!(taken(&log), ["released"]);
    assert_matches!(block_on(waiting), Err(Ended));
}

/// An explicit cancel of a send that waits for room gives its lease and its progress, and the
/// next send in the direction succeeds.
pub(in crate::driver) fn an_explicit_cancel_of_a_send_gives_its_progress_and_keeps_the_direction_usable(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
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
    assert_eq!(drive(&mut driver, close).unwrap().sent(), SendCount::Settled);
    assert!(
        reader.join().unwrap() == numbered(0, filled + 4096),
        "the peer read other bytes"
    );
}

/// An accept whose future drops after the kernel accepted a connection leaves it with the
/// listener, and the next accept gives it, with its peer address, before it submits. On IPv4 and
/// on IPv6.
pub(in crate::driver) fn a_dropped_accept_leaves_its_connection_to_the_next_accept(backend: Backend) {
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let mut driver = suite::driver(Config::new(), backend);
        let mut listener = listen(&mut driver, address);
        let address = listener.local_addr();
        let accept = driver.accept(&mut listener).unwrap();
        let mut client = std::net::TcpStream::connect(address).unwrap();
        posted(&mut driver, 1);
        drop(accept);
        driver.turn().unwrap();

        let next = driver.accept(&mut listener).unwrap();
        assert_eq!(driver.queue.unsubmitted(), 0, "the accept submitted");
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
pub(in crate::driver) fn close_releases_the_bytes_and_the_connection_that_no_operation_took(backend: Backend) {
    let log = Log::default();
    let mut driver = suite::driver(Config::new(), backend);
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

    let mut listener = listen(&mut driver, "127.0.0.1:0");
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
pub(in crate::driver) fn close_cancels_each_dropped_operation_by_its_own_request_and_reports_the_send(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, _peer) = connected(&mut driver, "127.0.0.1:0");
    let filled = fill(&mut driver, &mut stream);
    let (reader, writer) = stream.split();
    let receive = driver.recv(reader, Backing::from(Vec::with_capacity(64))).unwrap();
    let send = driver.send(writer, Lease::from(numbered(filled, 4096))).unwrap();
    driver.turn().unwrap();
    drop((receive, send));

    let close = driver.close(stream).unwrap();
    assert_eq!(
        driver.queue.unsubmitted(),
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
    // On Windows, the dropped send in the kernel is the send of `fill`, and its cancel aborts the
    // connection: it gives the abort and no count, and the close reports the unknown count.
    #[cfg(unix)]
    let (ends, sent) = ([-ECANCELED, -ECANCELED], SendCount::Sent(0));
    #[cfg(target_os = "windows")]
    let (ends, sent) = ([-WSAECONNABORTED, -ECANCELED], SendCount::Unknown);
    let mut results_of_targets: Vec<i32> = targets.iter().map(|target| target.res).collect();
    results_of_targets.sort_unstable();
    assert_eq!(results_of_targets, ends);
    for target in &targets {
        assert_eq!(
            results
                .iter()
                .filter(|result| result.user_data == target.user_data | cancel)
                .count(),
            1
        );
    }
    assert_eq!(drive(&mut driver, close).unwrap().sent(), sent);
}

/// `WSAECONNABORTED` (`winerror.h`): the stack aborted the connection.
#[cfg(target_os = "windows")]
const WSAECONNABORTED: i32 = 10053;

/// On Windows, a cancel of a send that runs in the kernel aborts the connection, and the kernel
/// gives no count, also when bytes went. So the send gives the abort with its whole lease and no
/// count, and the read direction fails too. When the future of the aborted send dropped, the next
/// send gives `WriteFailed` with the unknown count.
#[cfg(target_os = "windows")]
pub(in crate::driver) fn a_cancel_of_a_send_that_runs_aborts_the_connection_and_gives_no_count(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, _peer) = connected(&mut driver, "127.0.0.1:0");
    unbuffered(&mut driver, &stream);
    let send = driver.send(&mut stream, Lease::from(numbered(0, 64 << 20))).unwrap();
    for _ in 0..10 {
        driver.turn().unwrap();
    }
    assert!(driver.queue.peek().is_empty(), "the send did not wait");
    send.cancel();
    let (result, lease) = drive(&mut driver, send).unwrap();
    assert_eq!(result.unwrap_err().raw_os_error(), Some(WSAECONNABORTED));
    assert_eq!(lease.initialized(), 64 << 20);

    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(8))).unwrap();
    let (received, _) = drive(&mut driver, receive).unwrap();
    assert!(received.is_err(), "the read direction did not fail: {received:?}");

    let (mut stream, _peer) = connected(&mut driver, "127.0.0.1:0");
    unbuffered(&mut driver, &stream);
    let send = driver.send(&mut stream, Lease::from(numbered(0, 64 << 20))).unwrap();
    for _ in 0..10 {
        driver.turn().unwrap();
    }
    assert!(driver.queue.peek().is_empty(), "the second send did not wait");
    send.cancel();
    drop(send);
    let next = driver.send(&mut stream, Lease::from(b"next".to_vec())).unwrap();
    let failed = drive(&mut driver, next).unwrap().0.unwrap_err();
    assert_eq!(
        WriteFailed::of(&failed).map(WriteFailed::sent),
        Some(SendCount::Unknown)
    );
}

/// On Windows, a buffer stays owned after `CancelIoEx` until the packet of its operation: the
/// close of a stream with a forgotten receive cancels the receive, the cancel gives its own result
/// at once, and the receive gives its result, `ERROR_OPERATION_ABORTED`, only from its packet.
/// The buffer returns once, after that packet.
#[cfg(target_os = "windows")]
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
pub(in crate::driver) fn a_cancelled_receive_keeps_its_buffer_until_its_packet(backend: Backend) {
    let log = Log::default();
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, _peer) = connected(&mut driver, "127.0.0.1:0");
    mem::forget(driver.recv(&mut stream, recorded(&log, 16)).unwrap());
    driver.turn().unwrap();
    let close = driver.close(stream).unwrap();
    let posted = posted(&mut driver, 2);
    assert!(
        taken(&log).is_empty(),
        "the buffer returned before the driver took the packet"
    );
    assert_eq!(
        posted.iter().map(|cqe| cqe.res).collect::<Vec<_>>(),
        [0, -ECANCELED],
        "the cancel did not end before the packet of its receive"
    );
    drive(&mut driver, close).unwrap();
    assert_eq!(taken(&log), ["released"]);
}

/// Idle connections whose receives wait for a buffer that the kernel selects hold no receive
/// buffer: eight receives wait while the pool has one slot, and its charge does not change. The
/// data of the last connection then arrives in that slot.
pub(in crate::driver) fn idle_connections_with_a_waiting_selecting_receive_hold_no_receive_buffer(backend: Backend) {
    let budget = budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut driver = suite::driver(Config::new(), backend);
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

/// Turns the driver until no operation runs and no event can come, for at most 1000 turns.
#[cfg(unix)]
fn settle(driver: &mut Driver) {
    for _ in 0..1000 {
        if !driver.table.busy() {
            break;
        }
        let more = driver.turn().expect("a turn of the test driver works").more();
        // A wait with no operation in flight would wait for an event that cannot come.
        if !more && driver.table.busy() {
            driver.wait().expect("the wait of the test driver works");
        }
    }
    assert!(!driver.table.busy(), "an operation still runs after 1000 turns");
}

/// The descriptors that this process has open: Linux lists them in `/proc/self/fd`, and macOS in
/// `/dev/fd`.
#[cfg(unix)]
fn descriptors() -> usize {
    let directory = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    std::fs::read_dir(directory)
        .expect("the system lists the descriptors of a process")
        .count()
}

/// A connect to a port with no listener gives the typed refusal of the system, on IPv4 and IPv6.
pub(in crate::driver) fn a_connect_to_a_port_with_no_listener_is_refused(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    for address in ["127.0.0.1:0", "[::1]:0"] {
        // A port that was free and has no listener now.
        let free = std::net::TcpListener::bind(address).unwrap().local_addr().unwrap();
        let connect = driver.connect(free, TcpOptions::default()).unwrap();
        let refused = drive(&mut driver, connect).unwrap().unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::ConnectionRefused);
    }
}

/// A connect whose future drops after the kernel connected leaves no descriptor open after its
/// terminal event: the driver closes the connection, and the peer reads the end of the stream.
#[cfg(unix)]
pub(in crate::driver) fn a_dropped_connect_that_completes_closes_its_connection(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let open = descriptors();
    let connect = driver
        .connect(listener.local_addr().unwrap(), TcpOptions::default())
        .unwrap();
    assert_eq!(posted(&mut driver, 1)[0].res, 0, "the kernel did not connect");
    drop(connect);
    settle(&mut driver);
    assert_eq!(descriptors(), open, "a descriptor stayed open");
    listener.set_nonblocking(true).unwrap();
    let start = Instant::now();
    let mut peer = loop {
        match listener.accept() {
            Ok((peer, _)) => break peer,
            Err(error) if error.kind() == ErrorKind::WouldBlock && start.elapsed().as_secs() < 2 => {}
            Err(error) => panic!("the listener has no connection: {error}"),
        }
    };
    peer.set_nonblocking(false).unwrap();
    peer.set_read_timeout(Some(core::time::Duration::from_secs(2))).unwrap();
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).unwrap(), 0, "the peer read a byte");
}

/// The regions of a numbered stream from `start`, with these lengths.
fn regions(start: usize, lens: &[usize]) -> super::Regions {
    let mut regions = super::Regions::with_capacity(lens.len());
    let mut at = start;
    for &len in lens {
        regions.push(Lease::from(numbered(at, len)));
        at += len;
    }
    regions
}

/// A vectored send that the socket buffers stop gives the exact count across its regions: it
/// ends in the third region, and the peer reads exactly those bytes. After `Regions::advance`, a
/// second vectored send sends the rest, and the peer reads one numbered stream.
pub(in crate::driver) fn a_vectored_send_gives_its_exact_progress_across_regions(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, peer) = connected(&mut driver, "[::1]:0");
    let lens = [1000, 2000, 64 << 20];
    #[cfg(target_os = "windows")]
    unbuffered(&mut driver, &stream);
    let send = driver.send_vectored(&mut stream, regions(0, &lens)).unwrap();
    #[cfg(target_os = "windows")]
    reset(&mut driver, peer);
    let (sent, mut rest) = drive(&mut driver, send).unwrap();
    let sent = sent.unwrap();
    assert!(
        3000 < sent && sent < 3000 + (64 << 20),
        "the send ended outside the third region: {sent}"
    );
    rest.advance(sent);
    assert_eq!(rest.total(), 3000 + (64 << 20) - sent);
    // On Windows, the reset connection takes no more bytes.
    #[cfg(unix)]
    send_the_rest(&mut driver, stream, peer, rest);
}

/// Sends `rest` of a numbered stream of the regions `[1000, 2000, 64 MiB]` on `stream` until each
/// byte went, closes it, and checks that `peer` read the whole numbered stream.
#[cfg(unix)]
fn send_the_rest(driver: &mut Driver, mut stream: TcpStream, mut peer: std::net::TcpStream, mut rest: super::Regions) {
    let reader = thread::spawn(move || {
        let mut received = Vec::new();
        peer.read_to_end(&mut received).unwrap();
        received
    });
    while !rest.is_empty() {
        let send = driver.send_vectored(&mut stream, rest).unwrap();
        let (more, back) = drive(driver, send).unwrap();
        rest = back;
        rest.advance(more.unwrap());
    }
    let close = driver.close(stream).unwrap();
    assert_eq!(drive(driver, close).unwrap().sent(), SendCount::Settled);
    assert!(
        reader.join().unwrap() == numbered(0, 3000 + (64 << 20)),
        "the peer read other bytes"
    );
}

/// A vectored send whose future drops sends only part of its regions, so the write direction
/// fails, and the next send gives the count of the bytes that went: the bytes that the peer
/// reads.
pub(in crate::driver) fn a_dropped_partial_vectored_send_fails_the_write_direction_with_its_count(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, peer) = connected(&mut driver, "127.0.0.1:0");
    #[cfg(target_os = "windows")]
    unbuffered(&mut driver, &stream);
    drop(
        driver
            .send_vectored(&mut stream, regions(0, &[1000, 64 << 20]))
            .unwrap(),
    );
    #[cfg(target_os = "windows")]
    reset(&mut driver, peer);
    let sent = usize::try_from(posted(&mut driver, 1)[0].res).unwrap();
    driver.turn().unwrap();
    let next = driver.send(&mut stream, Lease::from(b"next".to_vec())).unwrap();
    let failed = drive(&mut driver, next).unwrap().0.unwrap_err();
    assert_eq!(
        WriteFailed::of(&failed).map(WriteFailed::sent),
        Some(SendCount::Sent(sent))
    );
    let close = driver.close(stream).unwrap();
    drive(&mut driver, close).unwrap();
    #[cfg(unix)]
    {
        let mut peer = peer;
        let mut received = Vec::new();
        peer.read_to_end(&mut received).unwrap();
        assert!(received == numbered(0, sent), "the peer read other bytes");
    }
}

/// Each option reads back the value that the caller set, on a listener that set them before its
/// bind and on a stream that sets them after its accept. Linux doubles the buffer sizes.
pub(in crate::driver) fn each_socket_option_reads_back_the_value_that_was_set(backend: Backend) {
    // Linux doubles each buffer size for its own bookkeeping, and macOS and Windows keep it.
    // Windows has no `SO_REUSEPORT`, so the option reads `None` there.
    #[cfg(target_os = "linux")]
    let (scale, reuse_port) = (2, |off| Some(off));
    #[cfg(target_os = "macos")]
    let (scale, reuse_port) = (1, |off| Some(off));
    #[cfg(target_os = "windows")]
    let (scale, reuse_port) = (1, |_| None);
    let mut driver = suite::driver(Config::new(), backend);
    for (on, off) in [(true, false), (false, true)] {
        let options = TcpOptions {
            no_delay: Some(on),
            keepalive: Some(off),
            only_v6: Some(on),
            send_buffer: Some(64 << 10),
            receive_buffer: Some(32 << 10),
            reuse_port: reuse_port(off),
        };
        // A bind to one IPv6 address makes Linux set `only_v6` itself, so the test binds `::`.
        let bind = driver.bind("[::]:0".parse().unwrap(), options).unwrap();
        let mut listener = drive(&mut driver, bind).unwrap().unwrap();
        let doubled = TcpOptions {
            send_buffer: Some(scale * (64 << 10)),
            receive_buffer: Some(scale * (32 << 10)),
            ..options
        };
        assert_eq!(driver.options(&listener).unwrap(), doubled);

        let port = listener.local_addr().port();
        let _client = std::net::TcpStream::connect(("::1", port)).unwrap();
        let accept = driver.accept(&mut listener).unwrap();
        let (stream, _) = drive(&mut driver, accept).unwrap().unwrap();
        let options = TcpOptions {
            no_delay: Some(off),
            keepalive: Some(on),
            send_buffer: Some(16 << 10),
            receive_buffer: Some(8 << 10),
            ..TcpOptions::default()
        };
        driver.set_options(&stream, options).unwrap();
        let read = driver.options(&stream).unwrap();
        assert_eq!(
            (read.no_delay, read.keepalive, read.send_buffer, read.receive_buffer),
            (Some(off), Some(on), Some(scale * (16 << 10)), Some(scale * (8 << 10)))
        );
    }
    // Windows refuses the option that it does not have, and binds nothing.
    #[cfg(target_os = "windows")]
    {
        let options = TcpOptions {
            reuse_port: Some(true),
            ..TcpOptions::default()
        };
        let bind = driver.bind("127.0.0.1:0".parse().unwrap(), options).unwrap();
        let refused = drive(&mut driver, bind).unwrap().unwrap_err();
        assert_eq!(refused.raw_os_error(), Some(10042), "not `WSAENOPROTOOPT`");
    }
    let bind = driver
        .bind("127.0.0.1:0".parse().unwrap(), TcpOptions::default())
        .unwrap();
    let listener = drive(&mut driver, bind).unwrap().unwrap();
    assert_eq!(
        driver.options(&listener).unwrap().only_v6,
        None,
        "an IPv4 socket has no IPv6 option"
    );
}

/// A half-close ends the peer's receive after the bytes that went before it, and the receives of
/// this side still complete.
pub(in crate::driver) fn a_half_close_ends_the_peer_receive_at_the_sent_count_and_local_receives_go_on(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let send = driver.send(&mut stream, Lease::from(numbered(0, 1000))).unwrap();
    assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 1000);
    let shutdown = driver.shutdown_write(&mut stream).unwrap();
    drive(&mut driver, shutdown).unwrap().unwrap();
    let mut received = Vec::new();
    peer.read_to_end(&mut received).unwrap();
    assert_eq!(received, numbered(0, 1000));

    peer.write_all(b"reply").unwrap();
    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(8))).unwrap();
    assert_eq!(drive(&mut driver, receive).unwrap().1.bytes(), b"reply");
}

/// On a failed write direction, a half-close gives `WriteFailed` and sends nothing: the peer reads
/// the bytes that went, and then no end of the stream, until the close.
pub(in crate::driver) fn a_half_close_on_a_failed_write_direction_sends_no_end_of_stream(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, peer) = connected(&mut driver, "127.0.0.1:0");
    #[cfg(target_os = "windows")]
    unbuffered(&mut driver, &stream);
    drop(driver.send(&mut stream, Lease::from(numbered(0, 64 << 20))).unwrap());
    #[cfg(target_os = "windows")]
    reset(&mut driver, peer);
    let sent = usize::try_from(posted(&mut driver, 1)[0].res).unwrap();
    driver.turn().unwrap();
    let shutdown = driver.shutdown_write(&mut stream).unwrap();
    let failed = drive(&mut driver, shutdown).unwrap().unwrap_err();
    assert_eq!(
        WriteFailed::of(&failed).map(WriteFailed::sent),
        Some(SendCount::Sent(sent))
    );
    // On Windows, the reset peer cannot show that no end of the stream came.
    #[cfg(unix)]
    no_end_of_stream_after(peer, sent);
}

/// `peer` reads the `sent` bytes of a numbered stream, and then no end of the stream.
#[cfg(unix)]
fn no_end_of_stream_after(mut peer: std::net::TcpStream, sent: usize) {
    let mut received = std::vec![0; sent];
    peer.read_exact(&mut received).unwrap();
    assert!(received == numbered(0, sent), "the peer read other bytes");
    peer.set_read_timeout(Some(core::time::Duration::from_millis(200)))
        .unwrap();
    let mut byte = [0];
    let wait = peer.read(&mut byte).unwrap_err();
    assert_matches!(
        wait.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut,
        "the peer read the end of the stream"
    );
}

/// The bind and the listen run as linked entries of the ring where the report gives
/// `Route::Ring`, and as system calls with a future that is ready at once elsewhere. On IPv4 and IPv6, the
/// listener then accepts.
#[test]
#[cfg(target_os = "linux")]
fn bind_and_listen_run_on_the_ring_exactly_where_the_report_gives_the_ring() {
    let mut driver = driver(Config::new());
    let ring = driver.report().route(Call::Bind) == Route::Ring;
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let mut bind = driver.bind(address.parse().unwrap(), TcpOptions::default()).unwrap();
        let unsubmitted = driver.queue.unsubmitted();
        let mut listener = match Pin::new(&mut bind).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(listener) => {
                assert!(!ring && unsubmitted == 0, "the bind did not use the ring that has it");
                listener.unwrap().unwrap()
            }
            Poll::Pending => {
                assert!(ring && unsubmitted == 2, "the bind used the ring that lacks it");
                // The bind posts no completion when it succeeds, and the listen posts 0.
                let posted: Vec<i32> = posted(&mut driver, 1).iter().map(|cqe| cqe.res).collect();
                assert_eq!(posted, [0], "the ring did not bind and listen");
                drive(&mut driver, bind).unwrap().unwrap()
            }
        };
        let client = std::net::TcpStream::connect(listener.local_addr()).unwrap();
        let accept = driver.accept(&mut listener).unwrap();
        let (_, peer) = drive(&mut driver, accept).unwrap().unwrap();
        assert_eq!(peer, client.local_addr().unwrap());
    }
}

/// A UDP socket of the driver at `address`, with `options`.
fn udp(driver: &mut Driver, address: &str, options: UdpOptions) -> UdpSocket {
    let address = address.parse().expect("the test gives a socket address");
    let bind = driver.bind_udp(address, options).expect("the test driver has room");
    drive(driver, bind)
        .expect("the driver runs")
        .expect("the test host binds loopback")
}

/// The options of a receiver that reports the metadata of each datagram.
const METADATA: UdpOptions = UdpOptions {
    only_v6: None,
    send_buffer: None,
    receive_buffer: None,
    reuse_port: None,
    metadata: true,
    dont_fragment: false,
    gro: false,
};

/// Receives one datagram on `socket` into a buffer of `capacity` bytes.
fn receive(driver: &mut Driver, socket: &mut UdpSocket, capacity: usize) -> (Received, Vec<u8>) {
    let receive = driver
        .recv_from(socket, Backing::from(Vec::with_capacity(capacity)))
        .expect("the test driver has room");
    let (received, buffer) = drive(driver, receive).expect("the driver runs");
    (received.expect("the receive works"), buffer.bytes().to_vec())
}

/// One send with a segment size sends one datagram for each segment, all of them, and its report
/// counts them. A receiver with no GRO gets each segment as a datagram; a receiver with GRO gets
/// them in one receive, with their segment size. On IPv4 and IPv6.
#[cfg(not(target_os = "macos"))]
pub(in crate::driver) fn a_segmented_send_arrives_as_its_datagrams_and_a_coalesced_receive_reports_its_segment_size(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let mut sender = udp(&mut driver, address, UdpOptions::default());
        let mut plain = udp(&mut driver, address, UdpOptions::default());
        let gro = UdpOptions {
            gro: true,
            ..UdpOptions::default()
        };
        let mut coalescing = udp(&mut driver, address, gro);
        for receiver in [plain.local_addr(), coalescing.local_addr()] {
            let datagram = Datagram {
                segment_size: Some(1000),
                ..Datagram::new(Lease::from(numbered(0, 4000)), receiver)
            };
            let send = driver.send_to(&mut sender, datagram).unwrap();
            assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 4);
        }
        for segment in 0..4 {
            let (received, bytes) = receive(&mut driver, &mut plain, 2000);
            assert_eq!((received.len, received.metadata.segment_size), (1000, None));
            assert!(
                bytes == numbered(segment * 1000, 1000),
                "the segments came out of order"
            );
        }
        let (received, bytes) = receive(&mut driver, &mut coalescing, 8000);
        assert_eq!((received.len, received.metadata.segment_size), (4000, Some(1000)));
        assert!(bytes == numbered(0, 4000), "the coalesced bytes differ");
    }
}

/// macOS has no segmentation offload. A send with a segment size gives
/// `Refused::NoSegmentation` and sends nothing, and the bind of a socket with the GRO option gives
/// `Unsupported`. A batch sends its datagrams before its first datagram with a segment size, and
/// its report counts them: that datagram and each one after it stay unsent, and no part of them
/// arrives. The next batch, which starts with that datagram, gives `NoSegmentation`. On IPv4 and
/// IPv6.
#[cfg(target_os = "macos")]
pub(in crate::driver) fn a_segment_size_gives_no_segmentation_and_a_batch_sends_only_the_datagrams_before_it(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let no_segmentation = |error: std::io::Error| rsnetwork_udp::Refused::of(&error);
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let gro = UdpOptions {
            gro: true,
            ..UdpOptions::default()
        };
        let bind = driver.bind_udp(address.parse().unwrap(), gro).unwrap();
        assert_eq!(
            drive(&mut driver, bind).unwrap().unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        let mut sender = udp(&mut driver, address, UdpOptions::default());
        let mut receiver = udp(&mut driver, address, UdpOptions::default());
        let segmented = |start, to| Datagram {
            segment_size: Some(1000),
            ..Datagram::new(Lease::from(numbered(start, 4000)), to)
        };
        let send = driver
            .send_to(&mut sender, segmented(0, receiver.local_addr()))
            .unwrap();
        let (sent, _) = drive(&mut driver, send).unwrap();
        assert_eq!(
            sent.map_err(no_segmentation).unwrap_err(),
            Some(rsnetwork_udp::Refused::NoSegmentation)
        );

        let mut batch = Datagrams::with_capacity(3);
        batch.push(Datagram::new(Lease::from(numbered(0, 100)), receiver.local_addr()));
        batch.push(segmented(100, receiver.local_addr()));
        batch.push(Datagram::new(Lease::from(numbered(4100, 300)), receiver.local_addr()));
        let send = driver.send_batch(&mut sender, batch).unwrap();
        let (sent, mut rest) = drive(&mut driver, send).unwrap();
        assert_eq!(sent.unwrap(), 1);
        rest.advance(1);
        let send = driver.send_batch(&mut sender, rest).unwrap();
        let (sent, rest) = drive(&mut driver, send).unwrap();
        assert_eq!(
            sent.map_err(no_segmentation).unwrap_err(),
            Some(rsnetwork_udp::Refused::NoSegmentation)
        );
        assert_eq!(rest.len(), 2);

        let (received, bytes) = receive(&mut driver, &mut receiver, 8000);
        assert!(
            received.len == 100 && bytes == numbered(0, 100),
            "the first datagram differs"
        );
        let receive = driver
            .recv_from(&mut receiver, Backing::from(Vec::with_capacity(8000)))
            .unwrap();
        driver.turn().unwrap();
        receive.cancel();
        let (more, _) = drive(&mut driver, receive).unwrap();
        assert_eq!(
            more.unwrap_err().raw_os_error(),
            Some(ECANCELED),
            "an unsent datagram arrived"
        );
    }
}

/// A batch that sends no datagram gives the error of its first datagram, read with the segment
/// size of that datagram. On Linux, `EIO` on a send with a segment size means that the device
/// cannot segment it, so the batch gives `Refused::NoSegmentation`. The injection gives the `EIO`
/// of the first datagram, because the loopback device segments each datagram.
#[test]
#[cfg(target_os = "linux")]
fn a_batch_that_fails_at_its_first_segmented_datagram_gives_no_segmentation() {
    let mut driver = suite::driver(Config::new(), Backend::Injected);
    let mut sender = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let receiver = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut batch = Datagrams::with_capacity(2);
    batch.push(Datagram {
        segment_size: Some(1000),
        ..Datagram::new(Lease::from(numbered(0, 4000)), receiver.local_addr())
    });
    batch.push(Datagram::new(Lease::from(numbered(4000, 100)), receiver.local_addr()));
    let send = driver.send_batch(&mut sender, batch).unwrap();
    let id = driver.table.running().next().expect("the batch runs");
    driver
        .queue
        .ring()
        .unwrap()
        .replace(id.user_data(), -rsnetwork_udp::sys::EIO);
    let (sent, rest) = drive(&mut driver, send).unwrap();
    let refused = sent.unwrap_err();
    assert_eq!(
        rsnetwork_udp::Refused::of(&refused),
        Some(rsnetwork_udp::Refused::NoSegmentation),
        "{refused}"
    );
    assert_eq!(rest.len(), 2);
}

/// The receiver reports the local destination address and the ECN codepoint that the sender set,
/// and the source address that the sender chose. On IPv4, with a source that is not the address
/// of the route, and on IPv6.
pub(in crate::driver) fn send_metadata_sets_the_source_and_ecn_that_the_receive_metadata_reports(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    // macOS has one IPv4 loopback address unless an administrator adds more, and so does
    // Windows, so the source there is that address.
    #[cfg(target_os = "linux")]
    let ipv4 = "127.0.0.2";
    #[cfg(not(target_os = "linux"))]
    let ipv4 = "127.0.0.1";
    for (any, receiver, source) in [("0.0.0.0:0", "127.0.0.1:0", ipv4), ("[::]:0", "[::1]:0", "::1")] {
        let mut sender = udp(&mut driver, any, UdpOptions::default());
        let mut receiver = udp(&mut driver, receiver, METADATA);
        let source: IpAddr = source.parse().unwrap();
        let datagram = Datagram {
            ecn: Some(Ecn::Ect1),
            source: Some(source),
            ..Datagram::new(Lease::from(b"metadata".to_vec()), receiver.local_addr())
        };
        let send = driver.send_to(&mut sender, datagram).unwrap();
        assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 1);
        let (received, bytes) = receive(&mut driver, &mut receiver, 64);
        assert_eq!(bytes, b"metadata");
        assert_eq!(received.source.ip(), source);
        assert_eq!(received.metadata.ecn, Some(Ecn::Ect1));
        assert_eq!(received.metadata.destination, Some(receiver.local_addr().ip()));
    }
}

/// A half-close of a UDP socket refuses before any submission, and the socket goes on: a datagram
/// goes and arrives after it.
pub(in crate::driver) fn a_half_close_of_a_udp_socket_refuses_and_changes_nothing(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let mut sender = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut receiver = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let refused = driver.shutdown_write(&mut sender).unwrap_err();
    assert_eq!(refused.reason(), Reason::Unsupported);
    let send = driver
        .send_to(
            &mut sender,
            Datagram::new(Lease::from(numbered(0, 100)), receiver.local_addr()),
        )
        .unwrap();
    assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 1);
    let (received, bytes) = receive(&mut driver, &mut receiver, 200);
    assert_eq!((received.len, received.source), (100, sender.local_addr()));
    assert!(bytes == numbered(0, 100), "the receive gave other bytes");
}

/// A datagram larger than its buffer gives the truncation outcome, with the bytes that fit.
pub(in crate::driver) fn an_oversized_datagram_gives_the_truncation_outcome(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let mut sender = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut receiver = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let send = driver
        .send_to(
            &mut sender,
            Datagram::new(Lease::from(numbered(0, 2000)), receiver.local_addr()),
        )
        .unwrap();
    drive(&mut driver, send).unwrap().0.unwrap();
    let (received, bytes) = receive(&mut driver, &mut receiver, 1000);
    assert!(received.truncated, "the receive did not report the truncation");
    assert!(bytes == numbered(0, 1000), "the receive gave other bytes");
}

/// A receive whose future drops after its datagram came leaves the datagram, its metadata, and its
/// truncation result to the next receive, which gives them before it submits, with one counted
/// copy. A close then releases a datagram that no receive took.
pub(in crate::driver) fn a_dropped_receive_leaves_its_datagram_metadata_and_truncation_to_the_next_receive(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let mut sender = udp(&mut driver, "[::1]:0", UdpOptions::default());
    let mut receiver = udp(&mut driver, "[::1]:0", METADATA);
    let to = receiver.local_addr();
    // The datagram waits in the socket, so the receive takes it in its first kernel entry.
    let datagram = Datagram {
        ecn: Some(Ecn::Ect0),
        ..Datagram::new(Lease::from(numbered(0, 150)), to)
    };
    let send = driver.send_to(&mut sender, datagram).unwrap();
    drive(&mut driver, send).unwrap().0.unwrap();
    let receive = driver
        .recv_from(&mut receiver, Backing::from(Vec::with_capacity(100)))
        .unwrap();
    assert_eq!(posted(&mut driver, 1)[0].res, 100);
    drop(receive);
    driver.turn().unwrap();

    let next = driver
        .recv_from(&mut receiver, Backing::from(Vec::with_capacity(200)))
        .unwrap();
    assert_eq!(driver.queue.unsubmitted(), 0, "the receive submitted");
    let (received, buffer) = drive(&mut driver, next).unwrap();
    let received = received.unwrap();
    assert!(buffer.bytes() == numbered(0, 100), "the kept bytes differ");
    assert_eq!(
        (received.len, received.truncated, received.source, received.metadata.ecn),
        (100, true, sender.local_addr(), Some(Ecn::Ect0))
    );
    assert_eq!(driver.copied(), 100);

    // Another datagram stays with the socket, and the close releases it.
    let send = driver
        .send_to(&mut sender, Datagram::new(Lease::from(b"kept".to_vec()), to))
        .unwrap();
    drive(&mut driver, send).unwrap().0.unwrap();
    let receive = driver
        .recv_from(&mut receiver, Backing::from(Vec::with_capacity(100)))
        .unwrap();
    assert_eq!(posted(&mut driver, 1)[0].res, 4);
    drop(receive);
    driver.turn().unwrap();
    let close = driver.close_udp(receiver).unwrap();
    drive(&mut driver, close).unwrap();
}

/// A batch of datagrams of different sizes goes in one kernel entry, one linked send for each, and
/// arrives as those datagrams, each with its own size. Its report counts them.
#[test]
#[cfg(target_os = "linux")]
fn a_batch_of_datagrams_arrives_as_each_datagram_after_one_kernel_entry() {
    let mut driver = driver(Config::new());
    let mut sender = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut receiver = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let sizes = [100, 2000, 300];
    let mut batch = Datagrams::with_capacity(sizes.len());
    let mut at = 0;
    for size in sizes {
        batch.push(Datagram::new(Lease::from(numbered(at, size)), receiver.local_addr()));
        at += size;
    }
    let mut send = driver.send_batch(&mut sender, batch).unwrap();
    assert_eq!(
        driver.queue.unsubmitted(),
        3,
        "the batch did not push one entry for each datagram"
    );
    driver.turn().unwrap();
    let Poll::Ready(sent) = Pin::new(&mut send).poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("the batch needs more than one kernel entry");
    };
    assert_eq!(sent.unwrap().0.unwrap(), 3);
    let mut at = 0;
    for size in sizes {
        let (received, bytes) = receive(&mut driver, &mut receiver, 4000);
        assert_eq!(received.len, size);
        assert!(bytes == numbered(at, size), "a datagram arrived with other bytes");
        at += size;
    }
}

/// A datagram of a batch that the kernel refuses ends the batch: the report counts the datagrams
/// before it, and the datagrams after it do not go, so no rest becomes a new datagram. The
/// receiver gets exactly the counted datagrams.
pub(in crate::driver) fn a_refused_datagram_ends_its_batch_and_the_report_counts_the_datagrams_before_it(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let mut sender = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut receiver = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut batch = Datagrams::with_capacity(3);
    // A UDP datagram over IPv4 holds at most 65507 bytes, so the second one is refused.
    for (start, size) in [(0, 100), (100, 70_000), (70_100, 300)] {
        batch.push(Datagram::new(Lease::from(numbered(start, size)), receiver.local_addr()));
    }
    let send = driver.send_batch(&mut sender, batch).unwrap();
    let (sent, mut rest) = drive(&mut driver, send).unwrap();
    assert_eq!(sent.unwrap(), 1);
    rest.advance(1);
    assert_eq!(rest.len(), 2);

    let (received, bytes) = receive(&mut driver, &mut receiver, 4000);
    assert!(
        received.len == 100 && bytes == numbered(0, 100),
        "the first datagram differs"
    );
    let receive = driver
        .recv_from(&mut receiver, Backing::from(Vec::with_capacity(4000)))
        .unwrap();
    driver.turn().unwrap();
    receive.cancel();
    let (more, _) = drive(&mut driver, receive).unwrap();
    assert_eq!(
        more.unwrap_err().raw_os_error(),
        Some(ECANCELED),
        "another datagram arrived"
    );
}

/// A batch with more datagrams than the submission queue has entries can never start, so it
/// refuses with `Reason::Queue` and gives its datagrams back. The socket then sends a datagram.
pub(in crate::driver) fn a_batch_larger_than_the_submission_queue_refuses_and_gives_its_datagrams_back() {
    let mut driver = driver(Config::new().submissions(8));
    let mut sender = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut receiver = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut batch = Datagrams::with_capacity(9);
    for start in 0..9 {
        batch.push(Datagram::new(
            Lease::from(numbered(start * 10, 10)),
            receiver.local_addr(),
        ));
    }
    let refused = driver.send_batch(&mut sender, batch).unwrap_err();
    assert_eq!(refused.reason(), Reason::Queue);
    assert_eq!(refused.into_inner().len(), 9);
    let send = driver
        .send_to(
            &mut sender,
            Datagram::new(Lease::from(numbered(0, 100)), receiver.local_addr()),
        )
        .unwrap();
    assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 1);
    let (received, bytes) = receive(&mut driver, &mut receiver, 200);
    assert!(
        received.len == 100 && bytes == numbered(0, 100),
        "the datagram after the refusal differs"
    );
}

/// A batch receive takes the datagrams that wait in one kernel entry, each into a buffer that the
/// kernel selects, with its own metadata. Its report counts them.
pub(in crate::driver) fn a_batch_receive_returns_several_datagrams_each_with_its_metadata(backend: Backend) {
    let budget = Budget::new(NonZeroU64::new(4096).unwrap());
    let mut driver = suite::driver(Config::new(), backend);
    let slot = Layout::from_size_align(64, 64).unwrap();
    driver
        .provide(Pool::new(&budget, NonZeroU32::new(1).unwrap().saturating_add(3), slot).unwrap())
        .unwrap();
    let mut sender = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut receiver = udp(&mut driver, "127.0.0.1:0", METADATA);
    // Windows refuses to send the CE codepoint (`WSAEINVAL`).
    #[cfg(unix)]
    let codepoints = [Ecn::Ect0, Ecn::Ect1, Ecn::Ce];
    #[cfg(target_os = "windows")]
    let codepoints = [Ecn::Ect0, Ecn::Ect1, Ecn::Ect0];
    for (index, ecn) in codepoints.into_iter().enumerate() {
        let datagram = Datagram {
            ecn: Some(ecn),
            ..Datagram::new(Lease::from(numbered(index, 10)), receiver.local_addr())
        };
        let send = driver.send_to(&mut sender, datagram).unwrap();
        drive(&mut driver, send).unwrap().0.unwrap();
    }
    #[cfg(target_os = "macos")]
    wait_datagrams(&mut driver, &mut receiver, 3);
    let receive = driver.recv_batch(&mut receiver, Arrivals::with_capacity(4)).unwrap();
    let (count, mut arrivals) = drive(&mut driver, receive).unwrap();
    assert_eq!(count.unwrap(), 3);
    for (index, (buffer, received)) in arrivals.drain().enumerate() {
        assert!(
            buffer.bytes() == numbered(index, 10),
            "a datagram arrived with other bytes"
        );
        assert_eq!(received.metadata.ecn, Some(codepoints[index]));
        assert_eq!(received.source, sender.local_addr());
    }
}

/// Waits until `count` datagrams wait in `socket` (`SO_NUMRCVPKT` of `sys/socket.h`), for at most
/// 5 seconds. The loopback of macOS can deliver a datagram after its send returns, so a case whose
/// receive takes only the datagrams that wait first waits for them here.
#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "the kernel writes one `int` that lives during the call")]
fn wait_datagrams(driver: &mut Driver, socket: &mut UdpSocket, count: core::ffi::c_int) {
    const SO_NUMRCVPKT: core::ffi::c_int = 0x1112;
    let read: &mut super::ReadHalf = socket.as_mut();
    let fd = driver.fd(read.resource).expect("the driver owns the socket");
    let start = Instant::now();
    loop {
        let (mut waiting, mut len): (core::ffi::c_int, u32) = (0, 4);
        // SAFETY: `waiting` is an `int` that lives during the call, and `len` is its size.
        let result = unsafe {
            rsnetwork_udp::sys::getsockopt(
                fd,
                rsnetwork_udp::sys::SOL_SOCKET,
                SO_NUMRCVPKT,
                (&raw mut waiting).cast(),
                &raw mut len,
            )
        };
        assert_eq!(result, 0, "the socket gives its datagram count");
        if waiting >= count {
            return;
        }
        assert!(start.elapsed().as_secs() < 5, "the datagrams did not arrive");
        thread::sleep(core::time::Duration::from_millis(1));
    }
}

/// A receive into a buffer that the kernel selects waits for its datagram, which then arrives in
/// the one slot of the pool, with its length and its source. When that buffer drops, the slot goes
/// back to the kernel ring, so the next datagram arrives in it again.
pub(in crate::driver) fn a_selected_datagram_arrives_in_the_slot_of_the_pool_which_returns_at_its_drop(
    backend: Backend,
) {
    let budget = budget();
    let mut pool = Pool::new(&budget, NonZeroU32::MIN, Layout::from_size_align(64, 64).unwrap()).unwrap();
    let slot = pool.acquire().unwrap().as_ptr();
    let mut driver = suite::driver(Config::new(), backend);
    driver.provide(pool).unwrap();
    let mut sender = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut receiver = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let to = receiver.local_addr();
    for bytes in [&b"first"[..], b"second"] {
        let receive = driver.recv_selected_from(&mut receiver).unwrap();
        // The turn submits the receive, which finds no datagram and waits for it.
        driver.turn().unwrap();
        let send = driver
            .send_to(&mut sender, Datagram::new(Lease::from(bytes.to_vec()), to))
            .unwrap();
        drive(&mut driver, send).unwrap().0.unwrap();
        let (received, buffer) = drive(&mut driver, receive).unwrap().unwrap();
        assert_eq!((received.len, received.source), (bytes.len(), sender.local_addr()));
        assert_eq!((buffer.bytes(), buffer.as_ptr()), (bytes, slot));
    }
}

/// A strict requirement refuses before a driver exists: a kernel with no such feature gives
/// `Error::Unsupported` with that feature. A kernel with it gives a driver whose report states the
/// requirement. The test prints the report of the kernel.
#[test]
#[cfg(target_os = "linux")]
fn a_strict_requirement_refuses_before_the_driver_is_ready() {
    let report = driver(Config::new()).report();
    std::println!("capability report: {report:#?}");
    let truncate = Features::NONE.with(Feature::Truncate);
    match Driver::new(Config::new().require(Feature::Truncate)) {
        Ok(strict) => {
            assert!(report.probed().contains(Feature::Truncate));
            assert_eq!(strict.report().required(), truncate);
        }
        Err(Error::Unsupported(missing)) => {
            assert!(!report.probed().contains(Feature::Truncate));
            assert_eq!(missing, truncate);
        }
        Err(error) => panic!("the driver did not start: {error}"),
    }
}

/// A new driver registers no file and no buffer, and it starts no submission poller: the view of
/// the kernel of its ring (`/proc/self/fdinfo`) shows none.
#[test]
#[cfg(target_os = "linux")]
fn a_new_driver_registers_no_file_or_buffer_and_starts_no_poller() {
    let driver = driver(Config::new());
    let info = std::fs::read_to_string(alloc::format!("/proc/self/fdinfo/{}", driver.queue.fd())).unwrap();
    let field = |name: &str| info.lines().find_map(|line| line.strip_prefix(name)).map(str::trim);
    assert_eq!(
        (field("UserFiles:"), field("UserBufs:"), field("SqThread:")),
        (Some("0"), Some("0"), Some("-1")),
        "{info}"
    );
}
