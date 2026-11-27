use alloc::borrow::ToOwned as _;
use alloc::format;
use alloc::string::String;
#[cfg(unix)]
use alloc::task::Wake;
use core::alloc::Layout;
#[cfg(unix)]
use core::assert_matches;
use core::future::Future;
use core::mem::{self, ManuallyDrop};
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::Pin;
#[cfg(unix)]
use core::pin::pin;
use core::ptr::NonNull;
#[cfg(target_os = "linux")]
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
#[cfg(unix)]
use std::fs::File;
use std::io::{self, ErrorKind, Read as _, Write as _};
#[cfg(unix)]
use std::os::fd::OwnedFd;
#[cfg(unix)]
use std::os::fd::{AsFd as _, AsRawFd as _};
use std::sync::{Arc, Mutex};
use std::thread;
#[cfg(unix)]
use std::thread::Thread;
#[cfg(unix)]
use std::time::Instant;
use std::vec::Vec;

use core::net::IpAddr;

use rsnetwork_udp::{Ecn, Received};

use super::suite::{self, Backend};
use super::{
    Arrivals, Config, Datagram, Datagrams, Delivery, Driver, ECANCELED, Operation, OperationId, Reason, Refused,
    SendCount, TcpListener, TcpOptions, TcpStream, UdpOptions, UdpSocket, WriteFailed,
};
#[cfg(target_os = "linux")]
use super::{Call, Error, Feature, Features, Files, KEPT_BUFFERS, Route, Shot, eventfd, probe, sys};
#[cfg(unix)]
use super::{Ended, PipeReader};
use crate::buffer::{Backing, Buffer as _, BufferMut as _, Lease, Pool, Release, WriteLoan};
use crate::capacity::Budget;
#[cfg(target_os = "linux")]
use crate::uring::Ring;

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
#[cfg(unix)]
fn wait_readable(driver: &Driver) {
    assert!(readable_within(driver, -1));
}

/// Whether the wake descriptor of `driver` becomes readable in at most `millis` milliseconds, or
/// with no limit for -1.
#[expect(unsafe_code, reason = "poll reads and writes one pollfd")]
#[cfg(unix)]
fn readable_within(driver: &Driver, millis: core::ffi::c_int) -> bool {
    let mut fd = sys::PollFd {
        fd: driver.as_fd().as_raw_fd(),
        events: sys::POLLIN,
        revents: 0,
    };
    // SAFETY: `fd` is one valid `pollfd` during the call.
    let ready = unsafe { sys::poll(&raw mut fd, 1, millis) };
    assert!(ready >= 0, "poll failed");
    ready == 1
}

/// A nonblocking pipe that closes at an exec: its read end and its write end, from
/// `std::io::pipe` and then `fcntl`, on each system. Linux 6.1 then waits for its readiness and
/// reads it in the task work of the ring, as Linux 7.0 does for each pipe. A read of a blocking
/// pipe on 6.1 goes to an io-wq worker instead, because 6.1 pipes lack `FMODE_NOWAIT`.
#[cfg(unix)]
pub(crate) fn pipe() -> (OwnedFd, File) {
    let (read, write) = std::io::pipe().expect("the test host makes a pipe");
    let (read, write) = (OwnedFd::from(read), OwnedFd::from(write));
    let nonblocking = super::pipe::set_nonblocking(read.as_fd(), true);
    assert!(
        nonblocking && super::pipe::set_nonblocking(write.as_fd(), true),
        "fcntl"
    );
    (read, File::from(write))
}

/// A descriptor of the wake descriptor of `driver`, which shares its count, so a read takes the
/// writes of the other threads since the last read. The kernel signals the descriptor only after
/// a host used it, which these tests do not do.
#[cfg(all(feature = "local", target_os = "linux"))]
pub(crate) fn interrupts(driver: &Driver) -> File {
    driver
        .shared
        .wake
        .try_clone()
        .expect("the test process has a free descriptor")
}

/// The order of the events of a test, from any thread.
pub(crate) type Log = Arc<Mutex<Vec<&'static str>>>;

pub(super) fn note(log: &Log, what: &'static str) {
    log.lock().expect("no test thread panics with the log").push(what);
}

pub(crate) fn taken(log: &Log) -> Vec<&'static str> {
    mem::take(&mut *log.lock().expect("no test thread panics with the log"))
}

/// A backing of `capacity` bytes whose release, through its release hook, records "released".
#[expect(
    unsafe_code,
    reason = "the test maps its own block into a backing, as a library does"
)]
pub(crate) fn recorded(log: &Log, capacity: usize) -> Backing {
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

/// A budget for a test pool of a few small slots, with the entries and the shared state that the
/// pool also charges.
pub(super) fn pool_budget() -> Budget {
    Budget::new(NonZeroU64::new(4096).unwrap())
}

pub(super) fn driver(config: Config) -> Driver {
    Driver::new(config).expect("the test host allows io_uring")
}

/// The read end `fd` of a pipe, which the driver then owns.
#[cfg(unix)]
fn reader(driver: &mut Driver, fd: OwnedFd) -> PipeReader {
    driver
        .adopt_reader(fd)
        .expect("the test driver has a free resource slot")
}

/// A pipe whose read end the driver owns, and whose write end holds one byte for it.
#[cfg(unix)]
fn ready_reader(driver: &mut Driver) -> (PipeReader, File) {
    let (read, mut write) = pipe();
    write.write_all(b"x").expect("the pipe has room");
    (reader(driver, read), write)
}

/// Holds each completion of `operation` in the control of `driver`, which runs under
/// `Delivery::Controlled`, and gives its identity.
pub(super) fn hold<T>(driver: &mut Driver, operation: &Operation<'_, T>) -> OperationId {
    let id = operation.id();
    driver
        .control()
        .expect("the test driver is controlled")
        .hold(id)
        .expect("the operation is of the driver");
    id
}

/// The longest time that a test loop waits for the driver. It is below the limit of 30 s of
/// nextest, so a hang fails with the operation that did not end, not with no message.
const WAIT_MAX: core::time::Duration = core::time::Duration::from_secs(10);

/// The bound of the waits of one test loop: `WAIT_MAX` after its creation.
pub(super) struct Bound {
    clock: crate::clock::NativeClock,
    deadline: crate::time::Deadline,
}

impl Bound {
    pub(super) fn new() -> Self {
        let clock = deadline_clock();
        Self {
            clock,
            deadline: deadline_after(clock, WAIT_MAX),
        }
    }

    /// Waits as [`Driver::wait`] does, but at most until the bound. At the bound, the test fails
    /// with `what`, the operation that the loop waits for, and the state of the queue: a ready
    /// completion that the wait did not see, or none.
    pub(super) fn wait(&self, driver: &mut Driver, what: impl FnOnce() -> String) {
        driver
            .wait_until(self.deadline, self.clock)
            .expect("the wait of the test driver works");
        let reached = self
            .deadline
            .is_reached(self.clock.now())
            .expect("the deadline is an instant of its clock");
        assert!(
            !reached,
            "the driver did not end {} in {WAIT_MAX:?}: {} entries not submitted, a completion ready: {}",
            what(),
            driver.queue.unsubmitted(),
            driver.queue.ready()
        );
    }
}

/// Turns `driver` until its control keeps a completion of `id`, and gives the result of the first
/// one. The hold stays.
pub(super) fn held(driver: &mut Driver, id: OperationId) -> io::Result<u32> {
    let bound = Bound::new();
    loop {
        // The turn takes the completion into the control, so the look comes before each wait.
        let more = driver.turn().expect("a turn of the test driver works").more();
        if let Some(result) = driver
            .control()
            .expect("the test driver is controlled")
            .posted(id)
            .expect("the operation is of the driver")
            .next()
        {
            return result;
        }
        if !more {
            bound.wait(driver, || format!("the held completion of {id:?}"));
        }
    }
}

/// As [`held`], and then ends the hold: the next turn gives the completion, after the requests of
/// that turn.
pub(super) fn posted(driver: &mut Driver, id: OperationId) -> io::Result<u32> {
    let result = held(driver, id);
    driver
        .control()
        .expect("the test driver is controlled")
        .deliver(id)
        .expect("the operation is of the driver");
    result
}

/// Turns the driver until a turn gives a completion to its operation.
fn drive_until_completed(driver: &mut Driver) {
    let bound = Bound::new();
    loop {
        let turn = driver.turn().expect("a turn of the test driver works");
        if turn.completed() > 0 {
            return;
        }
        if !turn.more() {
            bound.wait(driver, || "a completion of its operation".to_owned());
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
#[cfg(unix)]
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

/// A future polled with waker A, then with waker B, wakes only B at the completion.
#[test]
#[cfg(target_os = "linux")]
fn a_completion_wakes_only_the_waker_of_its_latest_poll() {
    let mut driver = driver(Config::new());
    let (read, mut writer) = pipe();
    let mut reader = reader(&mut driver, read);
    let mut future = driver.recv(&mut reader, Backing::from(Vec::with_capacity(16))).unwrap();
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
    let Poll::Ready(Ok((Ok(1), _))) = Pin::new(&mut future).poll(&mut Context::from_waker(&waker)) else {
        panic!("the read gives its byte");
    };
}

/// A forgotten future ends no operation: the buffer stays until the terminal completion, and
/// teardown releases it after it.
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
#[cfg(unix)]
pub(in crate::driver) fn a_forgotten_future_releases_its_buffer_only_after_the_terminal_completion(backend: Backend) {
    let log = Log::default();
    let mut driver = suite::driver(Config::new(), backend);
    let (read, mut writer) = pipe();
    let mut reader = reader(&mut driver, read);
    mem::forget(driver.recv(&mut reader, recorded(&log, 16)).unwrap());
    driver.turn().unwrap();
    assert!(taken(&log).is_empty());

    writer.write_all(b"x").unwrap();
    drive_until_completed(&mut driver);
    note(&log, "completed");
    assert_eq!(driver.teardown().unwrap().cancelled(), 0);
    assert_eq!(taken(&log), ["completed", "released"]);
}

/// A future on a second thread completes while the first thread drives the home.
#[test]
#[cfg(target_os = "linux")]
fn a_future_on_a_second_thread_completes_while_the_first_drives() {
    let log = Log::default();
    let mut driver = driver(Config::new());
    let (read, mut writer) = pipe();
    let mut reader = reader(&mut driver, read);
    let future = driver.recv(&mut reader, recorded(&log, 16)).unwrap();
    thread::scope(|scope| {
        let consumer =
            scope.spawn(move || block_on(future).map(|(count, buffer)| (count.unwrap(), buffer.bytes().to_vec())));
        driver.turn().unwrap();
        writer.write_all(b"data").unwrap();
        drive_until_completed(&mut driver);
        assert_eq!(consumer.join().unwrap(), Ok((4, b"data".to_vec())));
    });
    assert_eq!(taken(&log), ["released"]);
}

/// A turn gives at most its bound of completions, and it reports the work that remains. Each send
/// of one byte finds room in its socket buffer, so the kernel completes it when it takes it.
#[test]
#[cfg(target_os = "linux")]
fn a_turn_stops_at_its_bound_and_reports_the_remaining_work() {
    let mut driver = driver(Config::new().completions_per_turn(2));
    let mut streams: Vec<_> = (0..3).map(|_| connected(&mut driver, "127.0.0.1:0")).collect();
    let futures: Vec<_> = streams
        .iter_mut()
        .map(|(stream, _)| driver.send(stream, Lease::from(b"x".to_vec())).unwrap())
        .collect();
    let turns = [driver.turn().unwrap(), driver.turn().unwrap()];
    assert_eq!(
        turns.map(|turn| (turn.completed(), turn.more())),
        [(2, true), (1, false)]
    );
    for future in futures {
        assert_matches!(block_on(future), Ok((Ok(1), _)));
    }
}

/// With the task-run flag set and no entry to submit, a turn enters the kernel, which runs the
/// deferred work, and the pending receive completes: the data of a socket that a receive waits for
/// reaches the receive in that work. The test waits for the wake descriptor, as a host reactor
/// does, because `Driver::wait` runs the deferred work itself.
#[test]
#[cfg(target_os = "linux")]
fn a_turn_enters_the_kernel_for_deferred_work_with_nothing_to_submit() {
    let mut driver = driver(Config::new());
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let future = driver.recv(&mut stream, Backing::from(Vec::with_capacity(16))).unwrap();
    // The first use of the wake descriptor makes it readable one time, and the turn clears it. So
    // the wait below ends only at the signal of the kernel.
    let _first_use = driver.as_fd();
    driver.turn().unwrap();
    peer.write_all(b"x").unwrap();
    wait_readable(&driver);
    assert!(driver.queue.needs_enter(), "the kernel holds no deferred work");
    assert_eq!(driver.queue.unsubmitted(), 0);
    assert_eq!(driver.turn().unwrap().completed(), 1);
    assert_matches!(block_on(future), Ok((Ok(1), _)));
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
/// watches the wake descriptor, which only the other threads signal while no host uses it: the
/// ring registers it with the kernel only at its first use by a host.
#[test]
#[cfg(target_os = "linux")]
fn a_completion_during_a_wait_adds_no_other_completion() {
    let mut driver = driver(Config::new());
    let (read, mut writer) = pipe();
    let mut reader = reader(&mut driver, read);
    let future = driver.recv(&mut reader, Backing::from(Vec::with_capacity(16))).unwrap();
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
    assert_matches!(block_on(future), Ok((Ok(1), _)));
}

/// Teardown cancels a read that gets no data, returns after the completion of the read, closes
/// the pipe, and reports the cancelled read. Its future then gets `Ended`.
#[cfg(unix)]
pub(in crate::driver) fn teardown_cancels_a_read_with_no_data_closes_its_pipe_and_reports_it(backend: Backend) {
    let log = Log::default();
    let mut driver = suite::driver(Config::new(), backend);
    let (read, mut writer) = pipe();
    let mut reader = reader(&mut driver, read);
    let future = driver.recv(&mut reader, recorded(&log, 16)).unwrap();
    driver.turn().unwrap();

    assert_eq!(driver.teardown().unwrap().cancelled(), 1);
    assert_eq!(taken(&log), ["released"]);
    assert_eq!(writer.write(b"x").unwrap_err().kind(), ErrorKind::BrokenPipe);
    assert_matches!(block_on(future), Err(Ended));
}

/// A driver that drops with a running read ends the future with `Ended`, and it keeps the buffer
/// that the kernel can still use.
#[cfg(unix)]
pub(in crate::driver) fn a_dropped_driver_ends_its_futures_and_keeps_the_buffers_of_running_operations(
    backend: Backend,
) {
    let log = Log::default();
    let mut driver = suite::driver(Config::new(), backend);
    let (read, _writer) = pipe();
    let mut reader = reader(&mut driver, read);
    let future = driver.recv(&mut reader, recorded(&log, 16)).unwrap();
    driver.turn().unwrap();
    drop(driver);
    assert_matches!(block_on(future), Err(Ended));
    assert!(taken(&log).is_empty());
}

/// A driver with each operation slot in use refuses the next submission with a typed reason, and
/// it gives back the buffer.
#[cfg(unix)]
pub(in crate::driver) fn a_full_driver_refuses_a_submission_and_returns_its_resources() {
    let log = Log::default();
    let mut driver = driver(Config::new().operations(1));
    let (first, _first) = pipe();
    let (second, _second) = pipe();
    let (mut first, mut second) = (reader(&mut driver, first), reader(&mut driver, second));
    let running = driver.recv(&mut first, Backing::from(Vec::with_capacity(16))).unwrap();
    let Err(refused) = driver.recv(&mut second, recorded(&log, 16)) else {
        panic!("a full driver refuses");
    };
    assert_eq!(refused.reason(), Reason::Operations);
    let buffer = refused.into_inner();
    assert_eq!(buffer.capacity(), 16);
    assert!(taken(&log).is_empty());
    drop(buffer);
    assert_eq!(taken(&log), ["released"]);
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
/// entry, so it goes to the kernel and completes. Each send of one byte finds room in its socket
/// buffer, so the kernel completes it when it takes it, and the receive waits for data that does
/// not come.
#[test]
#[cfg(target_os = "linux")]
fn a_submission_completes_after_the_requests_that_it_takes_fill_the_queue() {
    let mut driver = driver(Config::new().operations(9).submissions(8));
    let byte = || Lease::from(b"x".to_vec());
    let mut streams: Vec<_> = (0..10).map(|_| connected(&mut driver, "127.0.0.1:0")).collect();
    let ((idle, _), rest) = streams.split_first_mut().unwrap();
    let ((first, _), rest) = rest.split_first_mut().unwrap();
    let mut done = driver.send(first, byte()).unwrap();
    let waiting = driver.recv(idle, Backing::from(Vec::with_capacity(1))).unwrap();
    driver.turn().unwrap();
    let ready = Pin::new(&mut done).poll(&mut Context::from_waker(Waker::noop()));
    assert_matches!(ready, Poll::Ready(Ok((Ok(1), _))), "the first send did not complete");
    // Seven entries wait in the queue of eight, and nine operations fill the table.
    let (queued, last) = rest.split_at_mut(7);
    let queued: Vec<_> = queued
        .iter_mut()
        .map(|(stream, _)| driver.send(stream, byte()).unwrap())
        .collect();
    // The ended send gives its slot back, and the cancel of the waiting receive takes the last
    // entry.
    drop(done);
    drop(waiting);
    let last = driver.send(&mut last[0].0, byte()).unwrap();
    for future in queued.into_iter().chain([last]) {
        assert_matches!(drive(&mut driver, future), Ok((Ok(1), _)));
    }
}

/// A future polled after it gave `Ready` panics, as the futures of the standard library do. It
/// does not report that the driver ended, which it did not.
#[test]
#[should_panic(expected = "polled after it gave `Ready`")]
#[cfg(unix)]
fn a_poll_after_ready_panics() {
    let mut driver = driver(Config::new());
    let (mut reader, _writer) = ready_reader(&mut driver);
    let mut read = driver.recv(&mut reader, Backing::from(Vec::with_capacity(1))).unwrap();
    assert_matches!(drive(&mut driver, &mut read), Ok((Ok(1), _)));
    drop(Pin::new(&mut read).poll(&mut Context::from_waker(Waker::noop())));
}

/// Turns the driver and polls `future` on the home thread, until it gives its output. It polls
/// once after each turn, so it never polls a future that gave its output.
pub(super) fn drive<F: Future + Unpin>(driver: &mut Driver, mut future: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    let bound = Bound::new();
    loop {
        let more = driver.turn().expect("a turn of the test driver works").more();
        if let Poll::Ready(output) = Pin::new(&mut future).poll(&mut cx) {
            return output;
        }
        if !more {
            bound.wait(driver, || format!("the future {}", core::any::type_name::<F>()));
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
    let budget = pool_budget();
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
    let budget = pool_budget();
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
    let budget = pool_budget();
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

/// Only the first use of a wake descriptor in a process probes the kernel: a second driver gets
/// the same result and makes no probe. On a kernel with the defect, the probe waits for a deferred
/// signal that does not come, so the first use by the second driver is then far shorter than the
/// first use by the first. Before that use, a driver has no registration, and its report says so.
#[test]
#[cfg(target_os = "linux")]
fn only_the_first_use_of_a_wake_descriptor_in_a_process_probes_the_kernel() {
    let (first, second) = (driver(Config::new()), driver(Config::new()));
    assert!(!first.report().rearms_wake());
    let start = Instant::now();
    let _first_use = first.as_fd();
    let probed = start.elapsed();
    let start = Instant::now();
    let _first_use = second.as_fd();
    let read = start.elapsed();
    assert_eq!(second.report().rearms_wake(), first.report().rearms_wake());
    if first.report().rearms_wake() {
        assert!(
            read * 10 < probed,
            "the second first use took {read:?}, and the first {probed:?}"
        );
    }
}

/// The probe finds the eventfd defect on the kernels of the evidence hosts that have it: the
/// kernel `7.0.0-1016-aws` of Ubuntu 26.04 has `cd305ee3633a` and not `04fe9aeb4f3c`, and Linux
/// 6.1 of Debian 12 has neither. On a kernel with the defect, it also finds the defect when the
/// first deferred signal comes after the first wait, as after a slow grace period. On another
/// kernel, only the first use of the wake descriptor is checked.
#[test]
#[cfg(target_os = "linux")]
fn the_probe_finds_the_eventfd_defect_on_the_kernels_that_have_it() {
    let release = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap();
    let driver = driver(Config::new());
    let _first_use = driver.as_fd();
    let expected = match release.trim() {
        "7.0.0-1016-aws" => Some(true),
        release if release.starts_with("6.1.") => Some(false),
        _ => None,
    };
    if let Some(expected) = expected {
        assert_eq!(driver.report().rearms_wake(), expected, "the kernel {release}");
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

/// R530: a ring registers its wake descriptor at its first use, so the kernel signalled nothing
/// for the work that came before. The descriptor is readable at that first use, and the turn of
/// the reactor then takes that work. Here, the bytes of the receive come before the first use.
#[test]
#[cfg(target_os = "linux")]
fn the_wake_descriptor_is_readable_at_its_first_use_for_the_work_that_came_before_it() {
    let mut driver = suite::driver(Config::new(), Backend::Kernel);
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(8))).unwrap();
    // The turn submits the receive, which waits for its bytes.
    driver.turn().unwrap();
    peer.write_all(b"early").unwrap();
    wait_readable(&driver);
    let (count, bytes) = drive_from_reactor(&mut driver, receive).unwrap();
    assert_eq!((count.unwrap(), bytes.bytes()), (5, &b"early"[..]));
}

/// R531: each wake of another thread writes the wake descriptor itself, so it reaches the reactor
/// of a host at once, with no flag, also after a turn took a wake before it. A second descriptor
/// that only the ring polls does not do this: the kernel defers the signal of the wake descriptor
/// for that poll, and on Linux 7.0.14 it gave the first wake 11 ms late and the second one never.
#[test]
#[cfg(target_os = "linux")]
fn each_wake_of_another_thread_makes_the_wake_descriptor_of_a_ring_readable() {
    let mut driver = suite::driver(Config::new(), Backend::Kernel);
    let wake = driver.wake_handle();
    // The first use makes the descriptor readable one time, and the turns of the host take that
    // signal.
    let _host = driver.as_fd();
    while driver.turn().unwrap().more() {}
    for _ in 0..3 {
        assert!(
            !readable_within(&driver, 0),
            "the turns left the wake descriptor readable"
        );
        let wake = wake.clone();
        thread::spawn(move || wake.wake()).join().unwrap();
        assert!(
            readable_within(&driver, 5_000),
            "the wake did not reach the wake descriptor"
        );
        // The host turns while the driver reports more work, and then it waits again.
        while driver.turn().unwrap().more() {}
    }
}

/// R531: a host that uses the wake descriptor can also wait in the driver. The kernel then
/// signals the descriptor that the wait polls, so a wait can end one time more for a completion.
/// The waits do not spin: after a completion, a loop waits a few times in 100 ms with no work. A
/// wake descriptor that no turn clears ends thousands of waits in that time.
#[test]
#[cfg(target_os = "linux")]
fn the_waits_of_a_ring_whose_host_uses_the_wake_descriptor_do_not_spin() {
    let mut driver = suite::driver(Config::new(), Backend::Kernel);
    let _host = driver.as_fd();
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(8))).unwrap();
    peer.write_all(b"x").unwrap();
    // The receive completes in a wait of the driver, and the kernel signals the wake descriptor.
    assert_matches!(drive(&mut driver, receive), Ok((Ok(1), _)));
    let clock = deadline_clock();
    let deadline = deadline_after(clock, core::time::Duration::from_millis(100));
    let mut waits = 0;
    while !deadline.is_reached(clock.now()).unwrap() {
        while driver.turn().unwrap().more() {}
        driver.wait_until(deadline, clock).unwrap();
        waits += 1;
    }
    assert!(waits <= 8, "the wait returned {waits} times in 100 ms");
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

/// The bytes of a stream cross its multishot receive in order and with no gap. The receives
/// alternate between selected buffers and the caller's buffer, which ends the multishot receive
/// and copies the kept bytes, also a part of one buffer. With no receive, the direction keeps its
/// most buffers, the receive ends, the pool runs out, and each later receive arms it again.
#[test]
#[cfg(target_os = "linux")]
fn the_bytes_of_a_stream_cross_its_multishot_receive_in_order_through_its_bound_and_an_empty_pool() {
    let budget = pool_budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut driver = driver(Config::new().receives(Shot::Multi));
    driver
        .provide(Pool::new(&budget, NonZeroU32::new(KEPT_BUFFERS * 2).unwrap(), slot).unwrap())
        .unwrap();
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let sent = numbered(0, 64 * 48);
    let first = driver.recv_selected(&mut stream).unwrap();
    driver.turn().unwrap();
    peer.write_all(&sent).unwrap();
    let mut received = drive(&mut driver, first).unwrap().unwrap().bytes().to_vec();
    // No receive waits: the multishot receive fills buffers until its end.
    for _ in 0..64 {
        driver.turn().unwrap();
    }
    for selected in [true, false].into_iter().cycle() {
        if received.len() == sent.len() {
            break;
        }
        let bytes = if selected {
            let receive = driver.recv_selected(&mut stream).unwrap();
            drive(&mut driver, receive).unwrap().unwrap().bytes().to_vec()
        } else {
            let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(48))).unwrap();
            let (count, buffer) = drive(&mut driver, receive).unwrap();
            assert_eq!(count.unwrap(), buffer.bytes().len());
            buffer.bytes().to_vec()
        };
        assert!(
            (1..=64).contains(&bytes.len()),
            "a receive gives the bytes of one buffer"
        );
        received.extend_from_slice(&bytes);
    }
    assert_eq!(received, sent);
}

/// A receive that drops while it waits loses no byte: the multishot receive of its direction
/// fills a buffer with the bytes that come after the drop, and the next receive gets that buffer.
#[test]
#[cfg(target_os = "linux")]
fn the_next_receive_gets_the_bytes_that_the_multishot_receive_took_after_a_dropped_receive() {
    let budget = pool_budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut pool = Pool::new(&budget, NonZeroU32::MIN, slot).unwrap();
    let mut zeroed = WriteLoan::new(pool.acquire().unwrap());
    zeroed.fill_from(&[0; 64]);
    let slot = zeroed.into_inner().as_ptr();
    let mut driver = driver(Config::new().receives(Shot::Multi));
    driver.provide(pool).unwrap();
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let receive = driver.recv_selected(&mut stream).unwrap();
    driver.turn().unwrap();
    drop(receive);
    driver.turn().unwrap();
    peer.write_all(b"kept").unwrap();
    assert_eq!(written_in_place(&mut driver, slot, 4), b"kept");
    let receive = driver.recv_selected(&mut stream).unwrap();
    let kept = drive(&mut driver, receive).unwrap().unwrap();
    assert_eq!((kept.bytes(), kept.as_ptr()), (&b"kept"[..], slot));
}

/// The kernel never writes a buffer that a caller holds: the multishot receive fills only the
/// buffers of the ring, and a held buffer goes back to the ring only after its drop.
#[test]
#[cfg(target_os = "linux")]
fn the_multishot_receive_never_writes_a_buffer_that_a_caller_holds() {
    let budget = pool_budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut driver = driver(Config::new().receives(Shot::Multi));
    driver
        .provide(Pool::new(&budget, NonZeroU32::new(2).unwrap(), slot).unwrap())
        .unwrap();
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    peer.write_all(&[1; 64]).unwrap();
    let receive = driver.recv_selected(&mut stream).unwrap();
    let mut held = drive(&mut driver, receive).unwrap().unwrap();
    held.bytes_mut().fill(0xa5);
    for round in 2..10 {
        peer.write_all(&[round; 64]).unwrap();
        let receive = driver.recv_selected(&mut stream).unwrap();
        let buffer = drive(&mut driver, receive).unwrap().unwrap();
        assert_eq!(buffer.bytes(), [round; 64]);
    }
    assert_eq!(held.bytes(), [0xa5; 64]);
}

/// Whether the kernel of the host frees the file of an emptied place of a file table at once, also
/// while other operations of the table run: Linux 6.13 and later, where each file of the table has
/// its own count of references. The version is the expectation of the probe of the ring, which
/// tests the release itself.
#[cfg(target_os = "linux")]
pub(in crate::driver) fn kernel_frees_a_removed_file_at_once() -> bool {
    let release = std::fs::read_to_string("/proc/sys/kernel/osrelease").expect("Linux gives its release");
    let mut numbers = release.split(['.', '-']).map(|number| number.trim().parse::<u32>());
    let version = (numbers.next(), numbers.next());
    let (Some(Ok(major)), Some(Ok(minor))) = version else {
        panic!("the release {release:?} starts with its version")
    };
    (major, minor) >= (6, 13)
}

/// The files that a ring with `Files::Registered` names on the kernel of the host.
#[cfg(target_os = "linux")]
pub(in crate::driver) fn registered_files() -> Files {
    if kernel_frees_a_removed_file_at_once() {
        Files::Registered
    } else {
        Files::Plain
    }
}

/// A stream that gets no place in the file table of the ring, as at a refusal of the kernel,
/// keeps its plain descriptor: its send and its receive run on it, and its close closes it. A
/// kernel that keeps the file of an emptied place open gives the ring no table.
#[cfg(target_os = "linux")]
pub(in crate::driver) fn a_stream_with_no_place_in_the_file_table_runs_on_its_plain_descriptor() {
    let mut driver = driver(Config::new().files(Files::Registered).delivery(Delivery::Controlled));
    assert_eq!(driver.report().files(), registered_files());
    if driver.report().files() == Files::Plain {
        return;
    }
    let mut listener = listen(&mut driver, "127.0.0.1:0");
    let mut peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
    driver.control().unwrap().fail_next_registration().unwrap();
    let accept = driver.accept(&mut listener).unwrap();
    let (mut stream, _) = drive(&mut driver, accept).unwrap().unwrap();

    let send = driver.send(&mut stream, Lease::from(b"ping".to_vec())).unwrap();
    assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 4);
    let mut received = [0; 4];
    peer.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"ping");
    peer.write_all(b"pong").unwrap();
    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(4))).unwrap();
    let (count, buffer) = drive(&mut driver, receive).unwrap();
    assert_eq!((count.unwrap(), buffer.bytes()), (4, &b"pong"[..]));

    let close = driver.close(stream).unwrap();
    drive(&mut driver, close).unwrap();
    assert_eq!(peer.read(&mut received).unwrap(), 0, "the close ends the stream");
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
/// read them. A send that ended before the reset gives each byte, which its case refuses.
#[cfg(target_os = "windows")]
fn reset(driver: &mut Driver, peer: std::net::TcpStream) {
    for _ in 0..10 {
        driver.turn().expect("a turn of the test driver works");
    }
    thread::sleep(core::time::Duration::from_millis(100));
    drop(peer);
}

/// Whether `future` is still pending after turns of `driver`, which give each completion that the
/// backend posted to its future.
#[cfg(target_os = "windows")]
fn waits<F: Future + Unpin>(driver: &mut Driver, future: &mut F) -> bool {
    for _ in 0..10 {
        driver.turn().expect("a turn of the test driver works");
    }
    Pin::new(future)
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending()
}

/// Sends a numbered stream from 0 until the socket buffers hold no more bytes while the peer does
/// not read, and gives the count that went. The loopback can deliver the segments of a send after
/// its return: on macOS each time, and on Linux under load. The window of the peer then fills after
/// the send, and the send buffer has room again. Each next send of 4 MiB takes that room, until one
/// send finds no room for 200 ms. Its cancel then ends it with no byte sent. A send that waits
/// wakes only when the socket has much room again, so less room can be there after its wait: one
/// more send takes it with its first attempt, and the fill ends when that send also finds no room
/// for 200 ms. The socket buffers hold at most 8 MiB on each side (`kern.ipc.maxsockbuf` of macOS,
/// and the `tcp_wmem` and `tcp_rmem` maxima of Linux), so `SENDS_MAX` sends of 4 MiB are more than
/// they hold.
#[cfg(unix)]
fn fill(driver: &mut Driver, stream: &mut TcpStream) -> usize {
    const SENDS_MAX: usize = 64;
    let mut filled = 0;
    // The send before this one waited for 200 ms and sent no byte.
    let mut full = false;
    'fill: for _ in 0..SENDS_MAX {
        let mut send = driver
            .send(stream, Lease::from(numbered(filled, 4 << 20)))
            .expect("the test driver has room");
        let start = Instant::now();
        while start.elapsed() < core::time::Duration::from_millis(200) {
            driver.turn().expect("a turn of the test driver works");
            if let Poll::Ready(output) = Pin::new(&mut send).poll(&mut Context::from_waker(Waker::noop())) {
                filled += output.expect("the driver runs").0.expect("the send works");
                full = false;
                continue 'fill;
            }
            thread::sleep(core::time::Duration::from_millis(1));
        }
        send.cancel();
        match drive(driver, send).expect("the driver runs").0 {
            // Room came at the end of the wait, before the cancel.
            Ok(sent) => {
                filled += sent;
                full = false;
            }
            Err(error) => {
                assert_eq!(error.raw_os_error(), Some(ECANCELED), "the send failed");
                if full {
                    return filled;
                }
                full = true;
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
    let mut send = driver
        .send(stream, Lease::from(numbered(0, 64 << 20)))
        .expect("the test driver has room");
    assert!(waits(driver, &mut send), "the send did not wait");
    drop(send);
    64 << 20
}

/// A receive whose future drops after its bytes came leaves them with the stream. The next
/// receives give them before they submit, as many as fit each buffer, with one counted copy, and
/// the stream keeps the rest. Each byte comes once and in order with the bytes after it.
pub(in crate::driver) fn a_dropped_receive_leaves_its_bytes_to_the_next_receives_in_order(backend: Backend) {
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    peer.write_all(&numbered(0, 1000)).unwrap();
    let receive = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    let id = hold(&mut driver, &receive);
    let kept = usize::try_from(posted(&mut driver, id).unwrap()).unwrap();
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

/// A stream that one driver accepted and released runs on a second driver that adopts it, as one
/// acceptor gives each connection to another worker: the bytes cross it in order both ways. The
/// release refuses while a dropped receive left its bytes with the stream, so they stay with the
/// connection. An adopt refuses each descriptor that is not a connected TCP socket, and gives it
/// back.
#[cfg(unix)]
pub(in crate::driver) fn a_stream_that_one_driver_releases_runs_on_the_driver_that_adopts_it(backend: Backend) {
    let mut acceptor = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let mut worker = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (stream, mut peer) = connected(&mut acceptor, "127.0.0.1:0");
    let fd = acceptor
        .release_stream(stream)
        .expect("an accepted stream has no state");
    let mut stream = worker
        .adopt_stream(fd)
        .expect("the descriptor is a connected TCP socket");

    peer.write_all(&numbered(0, 1000)).unwrap();
    let mut received = Vec::new();
    while received.len() < 1000 {
        let receive = worker
            .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
            .unwrap();
        let (count, buffer) = drive(&mut worker, receive).unwrap();
        assert!(count.unwrap() > 0, "the stream ended after {} bytes", received.len());
        received.extend_from_slice(buffer.bytes());
    }
    assert_eq!(received, numbered(0, 1000));
    let send = worker.send(&mut stream, Lease::from(numbered(1000, 1000))).unwrap();
    assert_eq!(drive(&mut worker, send).unwrap().0.unwrap(), 1000);
    let mut echoed = [0; 1000];
    peer.read_exact(&mut echoed).unwrap();
    assert_eq!(echoed[..], numbered(1000, 1000));

    // A receive whose future drops after its bytes came leaves them with the stream.
    peer.write_all(&numbered(2000, 10)).unwrap();
    let receive = worker
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    let id = hold(&mut worker, &receive);
    assert_eq!(posted(&mut worker, id).unwrap(), 10);
    drop(receive);
    let refused = worker.release_stream(stream).expect_err("the stream keeps the bytes");
    assert_eq!(refused.reason(), Reason::Busy);
    let mut stream = refused.into_inner();
    let receive = worker
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    let (count, buffer) = drive(&mut worker, receive).unwrap();
    assert_eq!((count.unwrap(), buffer.bytes()), (10, &numbered(2000, 10)[..]));

    // Each descriptor fails one check: the type, the peer, and the address family.
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    udp.connect(udp.local_addr().unwrap()).unwrap();
    let udp = OwnedFd::from(udp);
    let listener = OwnedFd::from(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    let (local, _other) = std::os::unix::net::UnixStream::pair().unwrap();
    for fd in [udp, listener, OwnedFd::from(local)] {
        let raw = fd.as_raw_fd();
        let refused = worker
            .adopt_stream(fd)
            .expect_err("the descriptor is not a connected TCP socket");
        assert_eq!(
            (refused.reason(), refused.into_inner().as_raw_fd()),
            (Reason::NotTcp, raw)
        );
    }
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
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (mut stream, peer) = connected(&mut driver, "127.0.0.1:0");
    let send = driver.send(&mut stream, Lease::from(b"unread".to_vec())).unwrap();
    assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 6);
    assert_eq!(peer.peek(&mut [0; 6]).unwrap(), 6, "the bytes wait in the peer");
    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(64))).unwrap();
    let id = hold(&mut driver, &receive);
    drop(peer);
    let failed = posted(&mut driver, id).unwrap_err();
    assert_eq!(failed.raw_os_error(), Some(ECONNRESET));
    drop(receive);

    let next = driver.recv(&mut stream, Backing::from(Vec::with_capacity(64))).unwrap();
    let error = drive(&mut driver, next).unwrap().0.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ConnectionReset);
}

/// An explicit cancel of a receive whose bytes came gives the original buffer with the bytes,
/// with no copy.
pub(in crate::driver) fn an_explicit_cancel_of_a_receive_gives_its_buffer_and_bytes_with_no_copy(backend: Backend) {
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    peer.write_all(b"hello").unwrap();
    let buffer = Backing::from(Vec::with_capacity(64));
    let address = buffer.as_ptr();
    let receive = driver.recv(&mut stream, buffer).unwrap();
    let id = hold(&mut driver, &receive);
    posted(&mut driver, id).unwrap();
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
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (mut stream, peer) = connected(&mut driver, "127.0.0.1:0");
    #[cfg(target_os = "windows")]
    unbuffered(&mut driver, &stream);
    let send = driver.send(&mut stream, Lease::from(numbered(0, 64 << 20))).unwrap();
    let id = hold(&mut driver, &send);
    drop(send);
    #[cfg(target_os = "windows")]
    reset(&mut driver, peer);
    let sent = usize::try_from(posted(&mut driver, id).unwrap()).unwrap();
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
        let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
        let mut listener = listen(&mut driver, address);
        let address = listener.local_addr();
        let accept = driver.accept(&mut listener).unwrap();
        let id = hold(&mut driver, &accept);
        let mut client = std::net::TcpStream::connect(address).unwrap();
        posted(&mut driver, id).unwrap();
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
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    peer.write_all(b"unread").unwrap();
    let receive = driver.recv(&mut stream, recorded(&log, 64)).unwrap();
    let id = hold(&mut driver, &receive);
    posted(&mut driver, id).unwrap();
    drop(receive);
    driver.turn().unwrap();
    assert!(taken(&log).is_empty(), "the stream did not keep the bytes");
    let close = driver.close(stream).unwrap();
    drive(&mut driver, close).unwrap();
    assert_eq!(taken(&log), ["released"]);

    let mut listener = listen(&mut driver, "127.0.0.1:0");
    let address = listener.local_addr();
    let accept = driver.accept(&mut listener).unwrap();
    let id = hold(&mut driver, &accept);
    let mut client = std::net::TcpStream::connect(address).unwrap();
    posted(&mut driver, id).unwrap();
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
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (mut stream, _peer) = connected(&mut driver, "127.0.0.1:0");
    let filled = fill(&mut driver, &mut stream);
    let (reader, writer) = stream.split();
    let receive = driver.recv(reader, Backing::from(Vec::with_capacity(64))).unwrap();
    let send = driver.send(writer, Lease::from(numbered(filled, 4096))).unwrap();
    // On Windows, the dropped send in the kernel is the send of `fill`, and this send waits in the
    // driver behind it. The cancel of the send of `fill` aborts the connection: it gives the abort
    // and no count, and the close reports the unknown count.
    #[cfg(unix)]
    let (ids, sent) = (
        [hold(&mut driver, &receive), hold(&mut driver, &send)],
        SendCount::Sent(0),
    );
    #[cfg(target_os = "windows")]
    let (ids, sent) = ([hold(&mut driver, &receive)], SendCount::Unknown);
    driver.turn().unwrap();
    drop((receive, send));

    let close = driver.close(stream).unwrap();
    assert_eq!(
        driver.queue.unsubmitted(),
        2,
        "the close did not push one cancel for each operation"
    );
    // Each operation in the kernel ends only by its own cancel, so a cancel that missed its target,
    // or that named another operation, leaves an operation that does not end with `ECANCELED`.
    for id in ids {
        let end = posted(&mut driver, id).unwrap_err();
        assert_eq!(end.raw_os_error(), Some(ECANCELED), "a cancel missed its target");
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
    let mut send = driver.send(&mut stream, Lease::from(numbered(0, 64 << 20))).unwrap();
    assert!(waits(&mut driver, &mut send), "the send did not wait");
    send.cancel();
    let (result, lease) = drive(&mut driver, send).unwrap();
    assert_eq!(result.unwrap_err().raw_os_error(), Some(WSAECONNABORTED));
    assert_eq!(lease.initialized(), 64 << 20);

    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(8))).unwrap();
    let (received, _) = drive(&mut driver, receive).unwrap();
    assert!(received.is_err(), "the read direction did not fail: {received:?}");

    let (mut stream, _peer) = connected(&mut driver, "127.0.0.1:0");
    unbuffered(&mut driver, &stream);
    let mut send = driver.send(&mut stream, Lease::from(numbered(0, 64 << 20))).unwrap();
    assert!(waits(&mut driver, &mut send), "the second send did not wait");
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
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (mut stream, _peer) = connected(&mut driver, "127.0.0.1:0");
    let receive = driver.recv(&mut stream, recorded(&log, 16)).unwrap();
    let id = hold(&mut driver, &receive);
    mem::forget(receive);
    driver.turn().unwrap();
    let close = driver.close(stream).unwrap();
    let ended = held(&mut driver, id).unwrap_err();
    assert_eq!(ended.raw_os_error(), Some(ECANCELED));
    assert!(
        taken(&log).is_empty(),
        "the buffer returned before the driver took the packet"
    );
    driver.control().unwrap().deliver(id).unwrap();
    drive(&mut driver, close).unwrap();
    assert_eq!(taken(&log), ["released"]);
}

/// Idle connections whose receives wait for a buffer that the kernel selects hold no receive
/// buffer: eight receives wait while the pool has one slot, and its charge does not change. The
/// data of the last connection then arrives in that slot.
pub(in crate::driver) fn idle_connections_with_a_waiting_selecting_receive_hold_no_receive_buffer(backend: Backend) {
    let budget = pool_budget();
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
    let bound = Bound::new();
    for _ in 0..1000 {
        if !driver.table.busy() {
            break;
        }
        let more = driver.turn().expect("a turn of the test driver works").more();
        // A wait with no operation in flight would wait for an event that cannot come.
        if !more && driver.table.busy() {
            bound.wait(driver, || "each running operation".to_owned());
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

/// A stream that the driver connected waits for bytes that come after its receive, and for room to
/// send while its peer does not read, and each wait ends. On epoll and kqueue, the wait of the
/// connect does not replace the registration of the socket, which each later wait needs.
pub(in crate::driver) fn a_connected_stream_waits_for_late_bytes_and_for_room_to_send(backend: Backend) {
    const TOTAL: usize = 16 << 20;
    let mut driver = suite::driver(Config::new(), backend);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let connect = driver
        .connect(listener.local_addr().unwrap(), TcpOptions::default())
        .unwrap();
    let mut stream = drive(&mut driver, connect).unwrap().unwrap();
    let (mut peer, _) = listener.accept().unwrap();

    // The receive starts before the peer sends, so it waits.
    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(64))).unwrap();
    for _ in 0..3 {
        driver.turn().unwrap();
    }
    peer.write_all(b"late").unwrap();
    let (count, bytes) = drive(&mut driver, receive).unwrap();
    assert_eq!((count.unwrap(), bytes.bytes()), (4, &b"late"[..]));

    // The peer reads only after the sends fill the buffers of the socket, so a send waits.
    let reader = thread::spawn(move || {
        thread::sleep(core::time::Duration::from_millis(200));
        let mut received = Vec::new();
        peer.read_to_end(&mut received).unwrap();
        received
    });
    let lease = Lease::from(numbered(0, TOTAL));
    let mut sent = 0;
    while sent < TOTAL {
        let send = driver.send(&mut stream, lease.view(sent..).unwrap()).unwrap();
        sent += drive(&mut driver, send).unwrap().0.unwrap();
    }
    let close = driver.close(stream).unwrap();
    drive(&mut driver, close).unwrap();
    assert!(
        reader.join().unwrap() == numbered(0, TOTAL),
        "the peer did not read the sent bytes"
    );
}

/// A connect whose future drops after the kernel connected leaves no descriptor open after its
/// terminal event: the driver closes the connection, and the peer reads the end of the stream.
#[cfg(unix)]
pub(in crate::driver) fn a_dropped_connect_that_completes_closes_its_connection(backend: Backend) {
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let open = descriptors();
    let connect = driver
        .connect(listener.local_addr().unwrap(), TcpOptions::default())
        .unwrap();
    let id = hold(&mut driver, &connect);
    assert_eq!(posted(&mut driver, id).unwrap(), 0, "the kernel did not connect");
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
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (mut stream, peer) = connected(&mut driver, "127.0.0.1:0");
    #[cfg(target_os = "windows")]
    unbuffered(&mut driver, &stream);
    let send = driver
        .send_vectored(&mut stream, regions(0, &[1000, 64 << 20]))
        .unwrap();
    let id = hold(&mut driver, &send);
    drop(send);
    #[cfg(target_os = "windows")]
    reset(&mut driver, peer);
    let sent = usize::try_from(posted(&mut driver, id).unwrap()).unwrap();
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
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let (mut stream, peer) = connected(&mut driver, "127.0.0.1:0");
    #[cfg(target_os = "windows")]
    unbuffered(&mut driver, &stream);
    let send = driver.send(&mut stream, Lease::from(numbered(0, 64 << 20))).unwrap();
    let id = hold(&mut driver, &send);
    drop(send);
    #[cfg(target_os = "windows")]
    reset(&mut driver, peer);
    let sent = usize::try_from(posted(&mut driver, id).unwrap()).unwrap();
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
    let mut driver = driver(Config::new().delivery(Delivery::Controlled));
    let ring = driver.report().route(Call::Bind) == Route::Ring;
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let mut bind = driver.bind(address.parse().unwrap(), TcpOptions::default()).unwrap();
        let id = hold(&mut driver, &bind);
        let unsubmitted = driver.queue.unsubmitted();
        let mut listener = match Pin::new(&mut bind).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(listener) => {
                assert!(!ring && unsubmitted == 0, "the bind did not use the ring that has it");
                listener.unwrap().unwrap()
            }
            Poll::Pending => {
                assert!(ring && unsubmitted == 2, "the bind used the ring that lacks it");
                // The bind posts no completion when it succeeds, and the listen posts 0.
                assert_eq!(posted(&mut driver, id).unwrap(), 0, "the ring did not bind and listen");
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

/// A segmented send reports the number of datagrams. Each datagram keeps its bytes on IPv4 and IPv6.
/// A receive that combines datagrams reports their segment size.
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
        let contents = numbered(0, 4000);
        let mut expected = contents.chunks(1000);
        for _ in 0..expected.len() {
            let (received, bytes) = receive(&mut driver, &mut coalescing, 8000);
            #[cfg(target_os = "linux")]
            assert_eq!(received.len, contents.len(), "Linux loopback combines this send");
            assert_eq!(received.len, bytes.len());
            assert!(!bytes.is_empty() && !received.truncated);
            assert_eq!(received.metadata.segment_size, (received.len > 1000).then_some(1000));
            for segment in bytes.chunks(1000) {
                assert_eq!(Some(segment), expected.next());
            }
            if expected.len() == 0 {
                break;
            }
        }
        assert_eq!(expected.len(), 0);
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
/// cannot segment it, so the batch gives `Refused::NoSegmentation`. The control gives the `EIO` of
/// the first datagram, because the loopback device segments each datagram.
#[test]
#[cfg(target_os = "linux")]
fn a_batch_that_fails_at_its_first_segmented_datagram_gives_no_segmentation() {
    let mut driver = suite::driver(Config::new(), Backend::Controlled);
    let mut sender = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let receiver = udp(&mut driver, "127.0.0.1:0", UdpOptions::default());
    let mut batch = Datagrams::with_capacity(2);
    batch.push(Datagram {
        segment_size: Some(1000),
        ..Datagram::new(Lease::from(numbered(0, 4000)), receiver.local_addr())
    });
    batch.push(Datagram::new(Lease::from(numbered(4000, 100)), receiver.local_addr()));
    let send = driver.send_batch(&mut sender, batch).unwrap();
    driver
        .control()
        .unwrap()
        .fail(send.id(), &io::Error::from_raw_os_error(rsnetwork_udp::sys::EIO))
        .unwrap();
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
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
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
    let id = hold(&mut driver, &receive);
    assert_eq!(posted(&mut driver, id).unwrap(), 100);
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
    let id = hold(&mut driver, &receive);
    assert_eq!(posted(&mut driver, id).unwrap(), 4);
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

/// Each driver makes its first listener in slot 0 with generation 0. On the other driver, an accept
/// of that listener refuses with `Reason::Resource`, and an option read gives an error, so no call
/// reaches the listener in the same slot there. Each listener then accepts on its own driver. The
/// other driver has no pool, and each selected receive of a stream or a socket of the first driver
/// still refuses with `Reason::Resource`, before `Reason::Buffers`.
pub(in crate::driver) fn a_resource_of_another_driver_refuses_and_each_driver_keeps_its_own() {
    let mut first = driver(Config::new());
    let mut second = driver(Config::new());
    let mut theirs = listen(&mut first, "127.0.0.1:0");
    let mut ours = listen(&mut second, "127.0.0.1:0");

    assert_eq!(second.accept(&mut theirs).unwrap_err().reason(), Reason::Resource);
    second.options(&theirs).unwrap_err();

    let _peer = std::net::TcpStream::connect(theirs.local_addr()).unwrap();
    let accept = first.accept(&mut theirs).unwrap();
    let (mut stream, _) = drive(&mut first, accept).unwrap().unwrap();
    let mut socket = udp(&mut first, "127.0.0.1:0", UdpOptions::default());
    assert_eq!(
        second.recv_selected(&mut stream).unwrap_err().reason(),
        Reason::Resource
    );
    assert_eq!(
        second.recv_selected_from(&mut socket).unwrap_err().reason(),
        Reason::Resource
    );
    let refused = second.recv_batch(&mut socket, Arrivals::with_capacity(1)).unwrap_err();
    assert_eq!(refused.reason(), Reason::Resource);

    for (driver, listener) in [(&mut first, &mut theirs), (&mut second, &mut ours)] {
        let _peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
        let accept = driver.accept(listener).unwrap();
        drive(driver, accept).unwrap().unwrap();
    }
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
    let (count, arrivals) = drive(&mut driver, receive).unwrap();
    assert_eq!(count.unwrap(), 3);
    // A refusal gives the datagrams of the batch back: here a queue with fewer entries than the
    // batch refuses it.
    let mut small = suite::driver(Config::new().submissions(2), backend);
    small
        .provide(Pool::new(&budget, NonZeroU32::MIN, slot).unwrap())
        .unwrap();
    let mut other = udp(&mut small, "127.0.0.1:0", UdpOptions::default());
    let Err(refused) = small.recv_batch(&mut other, arrivals) else {
        panic!("a batch above the entries of the queue refuses");
    };
    assert_eq!(refused.reason(), Reason::Queue);
    let mut arrivals = refused.into_inner();
    assert_eq!(arrivals.len(), 3, "the refusal gives each datagram of the batch back");
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
    let owned = driver.resources.get(read.resource).expect("the driver owns the socket");
    let fd = owned.fd.as_ref().expect("the slot holds the socket").as_raw_fd();
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
    let budget = pool_budget();
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

/// On Windows, the report gives the route of each file call on each backend, as the IoRing row of
/// the design states: IoRing runs the reads, the writes, and the syncs of a file on its ring, and a
/// hole punch and the sync of a directory on the blocking service, and IOCP runs the reads, the
/// writes, and the hole punches on the port. Where the probe refuses the ring, a strict IoRing driver refuses before it exists, and
/// the permissive driver runs IOCP and gives the `HRESULT` of the refusal. The test prints the
/// report.
#[test]
#[cfg(target_os = "windows")]
fn each_windows_backend_reports_the_route_of_each_file_call() {
    use super::{Backend, Call, Error, Report, Route};
    use Route::{BlockingService as Service, Port, Ring};

    let routes = |report: Report| {
        [
            Call::FileData,
            Call::PunchHole,
            Call::SyncFile,
            Call::SyncDirectory,
            Call::File,
            Call::SetFileLen,
        ]
        .map(|call| report.route(call))
    };
    let permissive = driver(Config::new()).report();
    std::println!("capability report: {permissive:#?}");
    let iocp = driver(Config::new().backend(Backend::Iocp)).report();
    assert_eq!(
        (iocp.requested(), iocp.backend(), iocp.fallback()),
        (Some(Backend::Iocp), Backend::Iocp, None)
    );
    assert_eq!(routes(iocp), [Port, Port, Service, Service, Service, Service]);
    match Driver::new(Config::new().backend(Backend::IoRing)) {
        Ok(strict) => {
            let report = strict.report();
            assert_eq!(
                (report.requested(), report.backend(), report.fallback()),
                (Some(Backend::IoRing), Backend::IoRing, None)
            );
            assert_eq!(routes(report), [Ring, Service, Ring, Service, Service, Service]);
            assert_eq!((permissive.backend(), permissive.fallback()), (Backend::IoRing, None));
        }
        Err(Error::Refused(error)) => {
            assert_eq!(
                (permissive.backend(), permissive.fallback()),
                (Backend::Iocp, error.raw_os_error())
            );
            assert_eq!(routes(permissive), routes(iocp));
        }
        Err(error) => panic!("the driver did not start: {error}"),
    }
}

/// A new driver registers no file and no buffer, and it starts no submission poller: the view of
/// the kernel of its ring (`/proc/self/fdinfo`) shows none.
#[test]
#[cfg(target_os = "linux")]
fn a_new_driver_registers_no_file_or_buffer_and_starts_no_poller() {
    let mut driver = driver(Config::new());
    let ring = driver.queue.ring().expect("the test driver runs io_uring").fd();
    let info = std::fs::read_to_string(alloc::format!("/proc/self/fdinfo/{ring}")).unwrap();
    let field = |name: &str| info.lines().find_map(|line| line.strip_prefix(name)).map(str::trim);
    assert_eq!(
        (field("UserFiles:"), field("UserBufs:"), field("SqThread:")),
        (Some("0"), Some("0"), Some("-1")),
        "{info}"
    );
}

/// Each refusal of the allocator in the start of a driver and in `provide` gives `OutOfMemory`,
/// and keeps no memory. A driver whose allocation aborts the process fails.
#[test]
fn a_driver_that_the_allocator_refuses_keeps_no_memory() {
    use crate::table::tests::allocations::{each_refusal, held};

    let refusals = each_refusal(|| Driver::new(Config::new()).map(drop));
    assert!(!refusals.is_empty());
    for (error, held) in refusals {
        let super::Error::System(error) = error else {
            panic!("a refusal of the allocator gives `Error::System`, not {error:?}");
        };
        assert_eq!((error.kind(), held), (ErrorKind::OutOfMemory, 0));
    }

    let mut driver = Driver::new(Config::new()).unwrap();
    let budget = pool_budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    // The bytes and the units of one pool.
    let (bytes, units) = (held(), budget.available());
    let probe = Pool::new(&budget, NonZeroU32::MIN, slot).unwrap();
    let (bytes, units) = (held() - bytes, units - budget.available());
    drop(probe);
    // A pool for each run, made before the allocator refuses.
    let mut pools: Vec<_> = (0..16)
        .map(|_| Pool::new(&budget, NonZeroU32::MIN, slot).unwrap())
        .collect();
    let refusals = each_refusal(|| {
        let available = budget.available();
        driver
            .provide(pools.pop().unwrap())
            .map_err(|error| (error, budget.available() - available))
    });
    assert!(!refusals.is_empty());
    // A refused `provide` drops the pool that it took: it frees each byte and each unit of the
    // pool, and keeps nothing else.
    for ((error, refunded), held) in refusals {
        assert_eq!(error.kind(), ErrorKind::OutOfMemory);
        assert_eq!((held, refunded), (-bytes, units));
    }
    driver.teardown().unwrap();
}

/// The clock of the deadlines of a case. It pauses while the system is suspended, as the wait of
/// the kernel does.
fn deadline_clock() -> crate::clock::NativeClock {
    crate::clock::NativeClock::new(crate::time::Suspend::Pauses).expect("the test host has a monotonic clock")
}

/// The deadline `time` after now on `clock`.
fn deadline_after(clock: crate::clock::NativeClock, time: core::time::Duration) -> crate::time::Deadline {
    crate::time::Deadline::at(clock.now().checked_add(time).expect("the deadline is in the domain"))
}

/// A deadline that no case reaches: a wait that ends there fails its test at the time limit.
const NEVER: core::time::Duration = core::time::Duration::from_secs(600);

/// R530: a loop that owns the driver waits in it until its next timer. With no work, no wake, and
/// no signal, the wait blocks until the deadline: a wait that does not block returns thousands of
/// times in that time.
pub(in crate::driver) fn a_timed_wait_with_no_work_blocks_until_its_deadline(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let clock = deadline_clock();
    let deadline = deadline_after(clock, core::time::Duration::from_millis(50));
    let mut waits = 0;
    while !deadline.is_reached(clock.now()).unwrap() {
        driver.turn().unwrap();
        driver.wait_until(deadline, clock).unwrap();
        waits += 1;
    }
    assert!(waits <= 8, "the wait returned {waits} times in 50 ms");
}

/// R530: a deadline that passed gives no block, also with no work that could end a wait. A
/// deadline of another clock refuses, and does not wait.
pub(in crate::driver) fn a_timed_wait_whose_deadline_passed_does_not_block(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let clock = deadline_clock();
    let passed = crate::time::Deadline::at(clock.now());
    driver.turn().unwrap();
    driver.wait_until(passed, clock).unwrap();

    let other = crate::time::ClockDomain::new(clock.domain().id() ^ 1, crate::time::Suspend::Pauses);
    let foreign = crate::time::Deadline::at(crate::time::Instant::new(other, u64::MAX));
    let refused = driver.wait_until(foreign, clock).unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::InvalidInput);
}

/// R530: a completion ends a timed wait before its deadline. The receive waits in the kernel
/// when the peer sends.
pub(in crate::driver) fn a_completion_ends_a_timed_wait_before_its_deadline(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut stream, mut peer) = connected(&mut driver, "127.0.0.1:0");
    let clock = deadline_clock();
    let never = deadline_after(clock, NEVER);
    let mut receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(8))).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    driver.turn().unwrap();
    let sender = thread::spawn(move || {
        peer.write_all(b"now").unwrap();
        peer
    });
    let (count, bytes) = loop {
        let more = driver.turn().unwrap().more();
        if let Poll::Ready(output) = Pin::new(&mut receive).poll(&mut cx) {
            break output.unwrap();
        }
        if !more {
            driver.wait_until(never, clock).unwrap();
        }
    };
    assert_eq!((count.unwrap(), bytes.bytes()), (3, &b"now"[..]));
    assert!(!never.is_reached(clock.now()).unwrap());
    drop(sender.join().unwrap());
}

/// R530: a wake from another thread ends a timed wait before its deadline, also when the wake
/// comes before the wait.
pub(in crate::driver) fn a_wake_ends_a_timed_wait_before_its_deadline(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let clock = deadline_clock();
    let never = deadline_after(clock, NEVER);
    let handle = driver.wake_handle();
    driver.turn().unwrap();
    // No turn takes the wake before the wait.
    let waker = thread::spawn(move || handle.wake());
    driver.wait_until(never, clock).unwrap();
    assert!(!never.is_reached(clock.now()).unwrap());
    waker.join().unwrap();
}

/// R530: handled signals that come faster than the time of a wait do not keep the loop past its
/// deadline. Each signal ends the wait in the kernel, and the next wait takes only the time that
/// is left. A wait that takes its whole time again at each signal ends only after the last signal.
#[cfg(unix)]
pub(in crate::driver) fn signals_do_not_delay_the_deadline_of_a_timed_wait(backend: Backend) {
    use core::ffi::c_int;
    use core::sync::atomic::AtomicU32;
    use core::time::Duration;
    use std::os::unix::thread::{JoinHandleExt as _, RawPthread};
    use std::sync::mpsc;

    /// `SIGUSR1` (`asm-generic/signal.h` of Linux, `sys/signal.h` of macOS).
    #[cfg(target_os = "linux")]
    const SIGUSR1: c_int = 10;
    #[cfg(target_os = "macos")]
    const SIGUSR1: c_int = 30;
    /// The time from the first wait to the deadline: many intervals of the signals.
    const TIMEOUT: Duration = Duration::from_millis(100);
    const LATE: Duration = Duration::from_secs(1);
    static HANDLED: AtomicU32 = AtomicU32::new(0);
    extern "C" fn handle(_signal: c_int) {
        HANDLED.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }
    #[expect(unsafe_code, reason = "the C library of each Unix target gives these functions")]
    unsafe extern "C" {
        /// `sighandler_t signal(int sig, sighandler_t handler)`. `SIG_ERR` is `-1`.
        fn signal(sig: c_int, handler: extern "C" fn(c_int)) -> isize;
        /// `int pthread_kill(pthread_t thread, int sig)`.
        fn pthread_kill(thread: RawPthread, sig: c_int) -> c_int;
    }

    // SAFETY: the handler only adds to an atomic counter, which is async-signal-safe.
    #[expect(unsafe_code, reason = "the handler is async-signal-safe")]
    let installed = unsafe { signal(SIGUSR1, handle) };
    assert_ne!(installed, -1);
    let (ready, started) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let mut driver = suite::driver(Config::new(), backend);
        let clock = deadline_clock();
        let deadline = deadline_after(clock, TIMEOUT);
        ready.send(()).unwrap();
        while !deadline.is_reached(clock.now()).unwrap() {
            driver.turn().unwrap();
            driver.wait_until(deadline, clock).unwrap();
        }
        clock.now().duration_since(deadline.instant()).unwrap()
    });
    started.recv().unwrap();
    let start = Instant::now();
    while !waiter.is_finished() && start.elapsed() < TIMEOUT + 2 * LATE {
        // SAFETY: the thread is not joined, so its handle is valid.
        #[expect(unsafe_code, reason = "the handle names a thread that is not joined")]
        unsafe {
            pthread_kill(waiter.as_pthread_t(), SIGUSR1);
        }
        thread::sleep(Duration::from_millis(1));
    }
    let late = waiter.join().unwrap();
    assert_ne!(
        HANDLED.load(core::sync::atomic::Ordering::Relaxed),
        0,
        "the signals reach the waiting thread"
    );
    assert!(late < LATE, "{late:?}");
}
