use alloc::vec::Vec;
use core::mem;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::super::tests::{drive, driver, numbered, posted};
use super::super::{Config, Driver, Regions};
use super::{LocalListener, LocalStream, Stale};
use crate::buffer::{Backing, Buffer as _, Lease};

/// The user and the group `nobody` of Debian and Ubuntu, the second OS user of the tests that run
/// as root.
const NOBODY: u32 = 65_534;

/// A directory for socket files in the temporary directory, which each user can enter. Its path
/// is short, because a socket path has at most 107 bytes.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(alloc::format!("rsasync-local-{name}-{}", std::process::id()));
        drop(fs::remove_dir_all(&path));
        fs::create_dir_all(&path).expect("the test makes its directory");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("the test sets the mode");
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}

/// Binds a local listener at `path` with `stale`, and gives the result of the bind.
fn bind(driver: &mut Driver, path: &Path, stale: Stale) -> io::Result<LocalListener> {
    let bind = driver.bind_local(path, stale).expect("the test driver has room");
    drive(driver, bind).expect("the driver runs")
}

/// Receives on `stream` into buffers of `capacity` bytes until the end of the stream.
fn receive_all(driver: &mut Driver, stream: &mut LocalStream, capacity: usize) -> Vec<u8> {
    let mut received = Vec::new();
    loop {
        let receive = driver
            .recv(stream, Backing::from(Vec::with_capacity(capacity)))
            .expect("the test driver has room");
        let (count, buffer) = drive(driver, receive).expect("the driver runs");
        if count.expect("the receive works") == 0 {
            return received;
        }
        received.extend_from_slice(buffer.bytes());
    }
}

/// A numbered byte stream crosses a local stream in each direction with no gap and no repeat,
/// through the abandoned operations of the stream rules: a dropped accept leaves its connection to
/// the next accept, a dropped send that sends all its bytes reports nothing, and a dropped or a
/// forgotten receive leaves its bytes to the next receive. A half-close ends the receive of the
/// peer at the sent count.
#[test]
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
fn a_numbered_stream_crosses_a_local_stream_with_no_gap_through_the_abandoned_operations() {
    let scratch = Scratch::new("stream");
    let path = scratch.join("socket");
    let mut driver = driver(Config::new());
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let accept = driver.accept_local(&mut listener).unwrap();
    let mut peer = UnixStream::connect(&path).unwrap();
    posted(&mut driver, 1);
    drop(accept);
    driver.turn().unwrap();
    let next = driver.accept_local(&mut listener).unwrap();
    assert_eq!(driver.ring.unsubmitted(), 0, "the accept submitted");
    let mut stream = drive(&mut driver, next).unwrap().unwrap();

    drop(driver.send(&mut stream, Lease::from(numbered(0, 1000))).unwrap());
    let mut regions = Regions::with_capacity(2);
    regions.push(Lease::from(numbered(1000, 300)));
    regions.push(Lease::from(numbered(1300, 700)));
    let vectored = driver.send_vectored(&mut stream, regions).unwrap();
    assert_eq!(drive(&mut driver, vectored).unwrap().0.unwrap(), 1000);
    let shutdown = driver.shutdown_write(&mut stream).unwrap();
    drive(&mut driver, shutdown).unwrap().unwrap();
    let mut received = Vec::new();
    peer.read_to_end(&mut received).unwrap();
    assert_eq!(received, numbered(0, 2000));

    // A write to a local stream is in the receive queue of the peer when it returns, so the first
    // receive takes 600 bytes, and 400 bytes wait for the forgotten receive.
    peer.write_all(&numbered(0, 1000)).unwrap();
    let dropped = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(600)))
        .unwrap();
    posted(&mut driver, 1);
    drop(dropped);
    let next = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    assert_eq!(drive(&mut driver, next).unwrap().1.bytes(), numbered(0, 600));
    let forgotten = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    posted(&mut driver, 1);
    driver.turn().unwrap();
    mem::forget(forgotten);
    peer.write_all(&numbered(1000, 500)).unwrap();
    peer.shutdown(std::net::Shutdown::Write).unwrap();
    let mut received = numbered(0, 600);
    received.extend(receive_all(&mut driver, &mut stream, 300));
    assert_eq!(received, numbered(0, 1500));
}

/// The socket file of a local listener has mode 0600 when the bind ends, before any connect, also
/// with the usual umask of 022, which leaves the group and others the read and execute bits.
#[test]
fn the_socket_file_of_a_local_listener_has_mode_0600_when_its_bind_ends() {
    let scratch = Scratch::new("mode");
    let path = scratch.join("socket");
    let mut driver = driver(Config::new());
    let _listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let metadata = fs::symlink_metadata(&path).unwrap();
    assert_eq!(metadata.mode() & 0o7777, 0o600);
}

/// A bind on the path of a live listener gives `AddrInUse`, also with `Stale::Replace`, and the
/// live listener keeps its socket file. After the close of the listener, the socket file stays: a
/// bind with `Stale::Refuse` gives `AddrInUse`, and a bind with `Stale::Replace` replaces it. A
/// bind with `Stale::Replace` on a file that is not a socket gives `AddrInUse` and keeps the file.
#[test]
fn a_bind_refuses_a_live_listener_path_and_replaces_only_a_stale_socket_file() {
    let scratch = Scratch::new("stale");
    let path = scratch.join("socket");
    let mut driver = driver(Config::new());
    let listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    for stale in [Stale::Refuse, Stale::Replace] {
        let error = bind(&mut driver, &path, stale).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse, "{stale:?}");
    }
    let live = UnixStream::connect(&path).expect("the live listener keeps its socket file");
    let close = driver.close_local_listener(listener).unwrap();
    drive(&mut driver, close).unwrap();
    drop(live);

    assert!(fs::symlink_metadata(&path).unwrap().file_type().is_socket());
    let error = bind(&mut driver, &path, Stale::Refuse).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    let mut listener = bind(&mut driver, &path, Stale::Replace).unwrap();
    let mut peer = UnixStream::connect(&path).unwrap();
    let accept = driver.accept_local(&mut listener).unwrap();
    let mut stream = drive(&mut driver, accept).unwrap().unwrap();
    peer.write_all(b"new").unwrap();
    let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(8))).unwrap();
    assert_eq!(drive(&mut driver, receive).unwrap().1.bytes(), b"new");

    let file = scratch.join("file");
    fs::write(&file, b"data").unwrap();
    let error = bind(&mut driver, &file, Stale::Replace).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    assert_eq!(fs::read(&file).unwrap(), b"data");
}

/// A path that the kernel would change gives `InvalidInput` and makes no socket: an empty path,
/// which would bind an abstract name with no file and no permission, a path with a NUL byte, and a
/// path of 108 bytes, which has no room for the NUL byte. A path of 107 bytes binds.
#[test]
fn a_path_that_does_not_fit_a_socket_address_gives_invalid_input() {
    let scratch = Scratch::new("path");
    let mut driver = driver(Config::new());
    let base = scratch.join("").into_os_string().into_vec();
    let long = |len: usize| {
        let mut path = base.clone();
        path.resize(len, b'p');
        PathBuf::from(OsStr::from_bytes(&path))
    };
    for path in [
        PathBuf::new(),
        PathBuf::from(OsStr::from_bytes(b"/tmp/a\0b")),
        long(108),
    ] {
        let error = bind(&mut driver, &path, Stale::Refuse).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{path:?}");
        let connect = driver.connect_local(&path).unwrap();
        let error = drive(&mut driver, connect).unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{path:?}");
    }
    let path = long(107);
    let _listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    UnixStream::connect(&path).unwrap();
}

/// Starts a process as the user and the group `nobody` that connects a socket to `path`, and gives
/// its process ID. The connect runs in the new process before it runs `/bin/true`, so a failure of
/// the connect is the error of the start.
#[expect(unsafe_code, reason = "the hook of the new process makes only system calls")]
fn connect_as_nobody(path: &Path) -> io::Result<u32> {
    let (address, len) = super::unix_address(path).ok_or(io::ErrorKind::InvalidInput)?;
    let mut command = Command::new("/bin/true");
    command.uid(NOBODY).gid(NOBODY);
    // SAFETY: the hook allocates nothing and takes no lock: it makes a socket and connects it with
    // system calls, in the new process. The address is a copy that the hook owns.
    unsafe {
        command.pre_exec(move || {
            let socket = super::super::tcp::new_socket(super::sys::AF_UNIX, rsnetwork_udp::sys::SOCK_STREAM)?;
            let fd = std::os::fd::AsRawFd::as_raw_fd(&socket);
            if super::sys::connect(fd, core::ptr::from_ref(&address).cast(), len) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    let process = child.id();
    if !child.wait()?.success() {
        return Err(io::Error::other("`/bin/true` failed"));
    }
    Ok(process)
}

/// A connect of a second OS user to the socket file of a listener gives `PermissionDenied`: the
/// mode 0600 refuses it, in a directory that each user can enter.
#[test]
#[ignore = "starts a process as a second OS user, which only root can do"]
fn a_connect_of_a_second_os_user_is_refused() {
    let scratch = Scratch::new("second-user");
    let path = scratch.join("socket");
    let mut driver = driver(Config::new());
    let _listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let error = connect_as_nobody(&path).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}

/// An accepted stream reports the user, the group, and the process ID of the process that
/// connected, not those of the listener, which runs as root here.
#[test]
#[ignore = "starts a process as a second OS user, which only root can do"]
fn an_accepted_stream_reports_the_user_group_and_process_of_its_peer() {
    let scratch = Scratch::new("peer");
    let path = scratch.join("socket");
    let mut driver = driver(Config::new());
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    // Only the test opens the socket file to the second user.
    fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
    let process = connect_as_nobody(&path).unwrap();
    let accept = driver.accept_local(&mut listener).unwrap();
    let stream = drive(&mut driver, accept).unwrap().unwrap();
    let peer = driver.local_peer(&stream).unwrap();
    assert_eq!((peer.user(), peer.group(), peer.process()), (NOBODY, NOBODY, process));
}

/// A bind with `Stale::Replace` on a stale socket file of another user gives `PermissionDenied`
/// and keeps the file.
#[test]
#[ignore = "changes the owner of a file to a second OS user, which only root can do"]
fn a_bind_does_not_replace_a_stale_socket_file_of_another_user() {
    let scratch = Scratch::new("foreign");
    let path = scratch.join("socket");
    drop(UnixListener::bind(&path).unwrap());
    std::os::unix::fs::chown(&path, Some(NOBODY), Some(NOBODY)).unwrap();
    let mut driver = driver(Config::new());
    let error = bind(&mut driver, &path, Stale::Replace).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(fs::symlink_metadata(&path).unwrap().uid(), NOBODY);
}
