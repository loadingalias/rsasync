use alloc::vec::Vec;
use core::alloc::Layout;
use core::future::Future;
use core::num::NonZeroU32;
use core::pin::Pin;
use core::task::{Context, Waker};
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, OwnedFd};

use super::super::suite::{self, Backend};
use super::super::tests::{drive, driver, numbered, pool_budget, posted};
use super::super::{Config, Driver, Reason, SendCount};
use super::{PipeReader, PipeWriter, sys};
use crate::buffer::{Backing, Buffer as _, Lease, Pool};

/// A pipe whose two ends the driver owns.
fn adopted(driver: &mut Driver) -> (PipeReader, PipeWriter) {
    let (read, write) = io::pipe().expect("the test host makes a pipe");
    let reader = driver
        .adopt_reader(OwnedFd::from(read))
        .expect("the test driver has room");
    let writer = driver
        .adopt_writer(OwnedFd::from(write))
        .expect("the test driver has room");
    (reader, writer)
}

/// Receives on `reader` into buffers of `capacity` bytes until the end of the stream.
fn receive_all(driver: &mut Driver, reader: &mut PipeReader, capacity: usize) -> Vec<u8> {
    let mut received = Vec::new();
    loop {
        let receive = driver
            .recv(reader, Backing::from(Vec::with_capacity(capacity)))
            .expect("the test driver has room");
        let (count, buffer) = drive(driver, receive).expect("the driver runs");
        if count.expect("the receive works") == 0 {
            return received;
        }
        received.extend_from_slice(buffer.bytes());
    }
}

/// A dropped send that sends all its bytes and a dropped receive whose bytes came lose no byte:
/// the next send goes after them, and the next receives give them first. The close of the writer
/// ends the reader at the sent count.
pub(in crate::driver) fn a_numbered_stream_crosses_a_pipe_with_no_gap_and_ends_at_the_close_of_the_writer(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut reader, mut writer) = adopted(&mut driver);
    drop(driver.send(&mut writer, Lease::from(numbered(0, 1000))).unwrap());
    let next = driver.send(&mut writer, Lease::from(numbered(1000, 1000))).unwrap();
    assert_eq!(drive(&mut driver, next).unwrap().0.unwrap(), 1000);

    let receive = driver
        .recv(&mut reader, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    let kept = usize::try_from(posted(&mut driver, 1)[0].res).unwrap();
    drop(receive);
    let close = driver.close_writer(writer).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), SendCount::Settled);

    assert_eq!(receive_all(&mut driver, &mut reader, 300), numbered(0, 2000));
    assert_eq!(driver.copied(), u64::try_from(kept).unwrap());
}

/// A send after the read end closed gives `EPIPE`, and the process lives: a Rust program
/// ignores `SIGPIPE`.
pub(in crate::driver) fn a_send_after_the_reader_closed_gives_broken_pipe(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (reader, mut writer) = adopted(&mut driver);
    let close = driver.close_reader(reader).unwrap();
    drive(&mut driver, close).unwrap();
    let send = driver.send(&mut writer, Lease::from(b"lost".to_vec())).unwrap();
    let error = drive(&mut driver, send).unwrap().0.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

/// The bytes in the pipe of the read end `fd`.
#[expect(unsafe_code, reason = "the call writes only the one `int`")]
fn queued(fd: &impl std::os::fd::AsRawFd) -> usize {
    /// `FIONREAD` (`include/uapi/asm-generic/ioctls.h` of Linux, and `sys/filio.h` of macOS).
    #[cfg(target_os = "linux")]
    const FIONREAD: core::ffi::c_ulong = 0x541B;
    #[cfg(target_os = "macos")]
    const FIONREAD: core::ffi::c_ulong = 0x4004_667F;
    #[expect(unsafe_code, reason = "the C library of each target gives this function")]
    unsafe extern "C" {
        fn ioctl(fd: core::ffi::c_int, request: core::ffi::c_ulong, ...) -> core::ffi::c_int;
    }
    let mut count: core::ffi::c_int = 0;
    // SAFETY: `FIONREAD` writes one `int`, at `count`.
    let result = unsafe { ioctl(fd.as_raw_fd(), FIONREAD, &raw mut count) };
    assert_eq!(result, 0, "FIONREAD");
    usize::try_from(count).expect("a count is not negative")
}

/// The close of each end returns while the other end neither reads nor writes. A dropped send of
/// more than the pipe holds writes what fits and waits for room, or ends with that count, by the
/// kernel. The close of the writer then reports that count, and the reader reads exactly those
/// bytes.
pub(in crate::driver) fn a_close_returns_while_the_other_end_is_idle_and_reports_the_cancelled_send(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut read, write) = io::pipe().unwrap();
    let (full, size) = full_pipe(&read);
    let mut writer = driver.adopt_writer(OwnedFd::from(write)).unwrap();
    drop(driver.send(&mut writer, Lease::from(numbered(0, size))).unwrap());
    let start = std::time::Instant::now();
    while queued(&read) < full {
        assert!(
            start.elapsed() < core::time::Duration::from_secs(5),
            "the send did not fill the pipe"
        );
        driver.turn().unwrap();
    }
    let filled = queued(&read);
    let close = driver.close_writer(writer).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), SendCount::Sent(filled));
    let mut received = Vec::new();
    read.read_to_end(&mut received).unwrap();
    assert!(received == numbered(0, filled), "the reader read other bytes");

    let (read, write) = io::pipe().unwrap();
    let mut reader = driver.adopt_reader(OwnedFd::from(read)).unwrap();
    drop(driver.recv(&mut reader, Backing::from(Vec::with_capacity(64))).unwrap());
    driver.turn().unwrap();
    let close = driver.close_reader(reader).unwrap();
    assert_eq!(drive(&mut driver, close).unwrap().sent(), SendCount::Settled);
    drop(write);
}

/// The bytes in the pipe of `read` that show that a send filled it, and a send larger than the
/// pipe. Linux states the capacity of a pipe, and an io_uring send can fill it in steps. macOS
/// states no capacity, but the poller makes one nonblocking write on the thread of the driver, so
/// the first byte in the pipe shows that the write ended. The send of 1 MiB is larger than a pipe
/// of macOS: one took 64 KiB on macOS 27.2.1 (measured).
#[cfg(target_os = "linux")]
#[expect(unsafe_code, reason = "fcntl names no memory with this command")]
fn full_pipe(read: &io::PipeReader) -> (usize, usize) {
    // SAFETY: the command names no memory.
    let capacity = usize::try_from(unsafe { sys::fcntl(read.as_raw_fd(), sys::F_GETPIPE_SZ) }).unwrap();
    (capacity, capacity + 4096)
}

#[cfg(target_os = "macos")]
fn full_pipe(_read: &io::PipeReader) -> (usize, usize) {
    (1, 1 << 20)
}

/// Idle read ends whose receives wait for a buffer that the kernel selects hold no buffer: eight
/// receives wait while the pool has one slot. The data of the last pipe then arrives in that slot.
pub(in crate::driver) fn idle_read_ends_with_a_waiting_selecting_receive_hold_no_buffer(backend: Backend) {
    let budget = pool_budget();
    let slot = Layout::from_size_align(64, 64).unwrap();
    let mut driver = suite::driver(Config::new(), backend);
    driver
        .provide(Pool::new(&budget, NonZeroU32::MIN, slot).unwrap())
        .unwrap();
    let mut readers = Vec::new();
    let mut writers = Vec::new();
    for _ in 0..8 {
        let (read, write) = io::pipe().unwrap();
        readers.push(driver.adopt_reader(OwnedFd::from(read)).unwrap());
        writers.push(write);
    }
    let mut waiting = Vec::new();
    for reader in &mut readers {
        waiting.push(driver.recv_selected(reader).unwrap());
    }
    driver.turn().unwrap();

    writers.last_mut().unwrap().write_all(b"last").unwrap();
    let last = waiting.pop().unwrap();
    assert_eq!(drive(&mut driver, last).unwrap().unwrap().bytes(), b"last");
    drop(waiting);
    driver.teardown().unwrap();
}

/// A half-close of a pipe writer refuses before any submission, and the writer goes on: its close
/// is the end of its stream.
pub(in crate::driver) fn a_half_close_of_a_pipe_writer_refuses_and_changes_nothing(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (mut reader, mut writer) = adopted(&mut driver);
    let refused = driver.shutdown_write(&mut writer).unwrap_err();
    assert_eq!(refused.reason(), Reason::Unsupported);
    let send = driver.send(&mut writer, Lease::from(numbered(0, 100))).unwrap();
    assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 100);
    let close = driver.close_writer(writer).unwrap();
    drive(&mut driver, close).unwrap();
    assert_eq!(receive_all(&mut driver, &mut reader, 64), numbered(0, 100));
}

/// A socket, an end of the wrong direction, and an end with no free resource slot refuse with
/// their reasons, and give the descriptor back.
pub(in crate::driver) fn an_adoption_refuses_a_socket_a_wrong_direction_and_a_full_driver() {
    let mut driver = driver(Config::new().resources(1));
    let socket = OwnedFd::from(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
    let raw = socket.as_raw_fd();
    let refused = driver.adopt_reader(socket).unwrap_err();
    assert_eq!(
        (refused.reason(), refused.into_inner().as_raw_fd()),
        (Reason::NotPipe, raw)
    );

    let (read, write) = io::pipe().unwrap();
    let refused = driver.adopt_reader(OwnedFd::from(write)).unwrap_err();
    assert_eq!(refused.reason(), Reason::Direction);
    let refused = driver.adopt_writer(OwnedFd::from(read)).unwrap_err();
    assert_eq!(refused.reason(), Reason::Direction);

    let (read, write) = io::pipe().unwrap();
    let _reader = driver.adopt_reader(OwnedFd::from(read)).unwrap();
    let refused = driver.adopt_writer(OwnedFd::from(write)).unwrap_err();
    assert_eq!(refused.reason(), Reason::Resources);
}

/// A read end that was nonblocking waits for data after its adoption, and does not end at once.
#[expect(unsafe_code, reason = "fcntl names no memory with these commands")]
pub(in crate::driver) fn an_adopted_nonblocking_read_end_waits_for_data(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let (read, mut write) = io::pipe().unwrap();
    let read = OwnedFd::from(read);
    // SAFETY: the commands name no memory, and the argument is an `int`.
    let set = unsafe {
        let flags = sys::fcntl(read.as_raw_fd(), sys::F_GETFL);
        sys::fcntl(read.as_raw_fd(), sys::F_SETFL, flags | sys::O_NONBLOCK)
    };
    assert_eq!(set, 0);
    let mut reader = driver.adopt_reader(read).unwrap();
    let mut receive = driver.recv(&mut reader, Backing::from(Vec::with_capacity(64))).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..3 {
        driver.turn().unwrap();
        assert!(
            Pin::new(&mut receive).poll(&mut cx).is_pending(),
            "the receive did not wait"
        );
    }
    write.write_all(b"data").unwrap();
    let (count, buffer) = drive(&mut driver, receive).unwrap();
    assert_eq!((count.unwrap(), buffer.bytes()), (4, &b"data"[..]));
}
