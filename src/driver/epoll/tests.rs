use alloc::vec::Vec;
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, OwnedFd};

use super::super::tests::{drive, driver};
use super::super::{Backend, Call, Config, Driver, Error, OpenOptions, Reason, Route, TcpOptions};
use crate::buffer::{Backing, Buffer as _, Lease};

/// `ENOSPC` (`include/uapi/asm-generic/errno-base.h`): `epoll_ctl` at the limit of
/// `fs.epoll.max_user_watches`.
const ENOSPC: i32 = 28;

/// `EPERM` (`include/uapi/asm-generic/errno-base.h`).
const EPERM: i32 = 1;

/// A refused registration gives the descriptor back, open, with [`Reason::Register`], and the
/// driver takes the same descriptor later. The test seam of the registrar gives the error of
/// `epoll_ctl`, because no unprivileged path makes `epoll_ctl` fail on a pipe or a socket: each
/// adoption refuses a regular file, which epoll does not take, before the registration, and the
/// limit `fs.epoll.max_user_watches` is a setting of root for each user of the host.
#[test]
fn a_failed_registration_returns_the_descriptor_open_with_its_reason() {
    let mut driver = driver(Config::new().backend(Backend::Epoll));
    let (read, mut write) = io::pipe().unwrap();
    let read = OwnedFd::from(read);
    let raw = read.as_raw_fd();
    let registrar = driver.resources.registrar.as_ref().unwrap();
    registrar.fail.set(Some(ENOSPC));

    let refused = driver.adopt_reader(read).unwrap_err();
    assert_eq!(refused.reason(), Reason::Register);
    let read = refused.into_inner();
    assert_eq!(read.as_raw_fd(), raw);
    write.write_all(b"kept").unwrap();
    let mut kept = [0; 4];
    io::PipeReader::from(read.try_clone().unwrap())
        .read_exact(&mut kept)
        .unwrap();
    assert_eq!(&kept, b"kept");

    let mut reader = driver.adopt_reader(read).unwrap();
    let receive = driver.recv(&mut reader, Backing::from(Vec::with_capacity(8))).unwrap();
    write.write_all(b"next").unwrap();
    let (count, buffer) = drive(&mut driver, receive).unwrap();
    assert_eq!((count.unwrap(), buffer.bytes()), (4, &b"next"[..]));
}

/// A strict epoll driver reports epoll, and each file call on the blocking service. With no
/// service, a file operation refuses before it starts, because no file call blocks the driver.
#[test]
fn an_epoll_driver_reports_its_backend_and_refuses_a_file_operation_with_no_service() {
    let mut driver = driver(Config::new().backend(Backend::Epoll));
    let report = driver.report();
    assert_eq!(
        (report.requested(), report.backend(), report.fallback()),
        (Some(Backend::Epoll), Backend::Epoll, None)
    );
    assert_eq!(report.route(Call::File), Route::BlockingService);
    assert_eq!(report.route(Call::SetFileLen), Route::BlockingService);
    let path = std::env::current_exe().unwrap();
    let refused = driver.open_file(&path, OpenOptions::new()).unwrap_err();
    assert_eq!(refused.reason(), Reason::Blocking);
}

/// With `kernel.io_uring_disabled` set to 2 (Linux 6.6), the permissive selection runs epoll and
/// reports the `EPERM` of the ring, and a stream works on it. A strict requirement of io_uring
/// refuses before a driver exists.
#[test]
#[ignore = "needs kernel.io_uring_disabled=2, which only root can set"]
fn a_disabled_io_uring_runs_epoll_in_the_permissive_selection_and_refuses_the_strict_one() {
    let setting = std::fs::read_to_string("/proc/sys/kernel/io_uring_disabled").unwrap();
    assert_eq!(setting.trim(), "2", "run this test with kernel.io_uring_disabled=2");

    let strict = Driver::new(Config::new().backend(Backend::IoUring));
    assert!(matches!(strict, Err(Error::Refused(error)) if error.raw_os_error() == Some(EPERM)));

    let mut driver = driver(Config::new());
    let report = driver.report();
    assert_eq!(
        (report.requested(), report.backend(), report.fallback()),
        (None, Backend::Epoll, Some(EPERM))
    );
    let bind = driver
        .bind("127.0.0.1:0".parse().unwrap(), TcpOptions::default())
        .unwrap();
    let mut listener = drive(&mut driver, bind).unwrap().unwrap();
    let mut peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
    let accept = driver.accept(&mut listener).unwrap();
    let (mut stream, _) = drive(&mut driver, accept).unwrap().unwrap();
    let send = driver.send(&mut stream, Lease::from(b"ping".to_vec())).unwrap();
    assert_eq!(drive(&mut driver, send).unwrap().0.unwrap(), 4);
    let mut ping = [0; 4];
    peer.read_exact(&mut ping).unwrap();
    assert_eq!(&ping, b"ping");
}
