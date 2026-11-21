use alloc::vec::Vec;
use core::ffi::c_int;
use core::{mem, ptr};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};

use super::super::suite::{self, Backend};
use super::super::tests::{drive, hold, numbered, posted};
use super::super::{Config, Delivery, Driver, Regions};
use super::{LocalListener, LocalStream, Stale};
use crate::buffer::{Backing, Buffer as _, Lease};

/// The user and the group `nobody`, the second OS user of the tests that run as root: 65534 on
/// Debian and Ubuntu, and -2 on macOS.
#[cfg(target_os = "linux")]
const NOBODY: u32 = 65_534;
#[cfg(target_os = "macos")]
const NOBODY: u32 = 4_294_967_294;

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
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
pub(in crate::driver) fn a_numbered_stream_crosses_a_local_stream_with_no_gap_through_the_abandoned_operations(
    backend: Backend,
) {
    let scratch = Scratch::new("stream");
    let path = scratch.join("socket");
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let accept = driver.accept_local(&mut listener).unwrap();
    let id = hold(&mut driver, &accept);
    let mut peer = UnixStream::connect(&path).unwrap();
    posted(&mut driver, id).unwrap();
    drop(accept);
    driver.turn().unwrap();
    let next = driver.accept_local(&mut listener).unwrap();
    assert_eq!(driver.queue.unsubmitted(), 0, "the accept submitted");
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
    let id = hold(&mut driver, &dropped);
    posted(&mut driver, id).unwrap();
    drop(dropped);
    let next = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    assert_eq!(drive(&mut driver, next).unwrap().1.bytes(), numbered(0, 600));
    let forgotten = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    let id = hold(&mut driver, &forgotten);
    posted(&mut driver, id).unwrap();
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
pub(in crate::driver) fn the_socket_file_of_a_local_listener_has_mode_0600_when_its_bind_ends(backend: Backend) {
    let scratch = Scratch::new("mode");
    let path = scratch.join("socket");
    let mut driver = suite::driver(Config::new(), backend);
    let _listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let metadata = fs::symlink_metadata(&path).unwrap();
    assert_eq!(metadata.mode() & 0o7777, 0o600);
}

/// A bind on the path of a live listener gives `AddrInUse`, also with `Stale::Replace`, and the
/// live listener keeps its socket file. After the close of the listener, the socket file stays: a
/// bind with `Stale::Refuse` gives `AddrInUse`, and a bind with `Stale::Replace` replaces it. A
/// bind with `Stale::Replace` on a file that is not a socket gives `AddrInUse` and keeps the file.
pub(in crate::driver) fn a_bind_refuses_a_live_listener_path_and_replaces_only_a_stale_socket_file(backend: Backend) {
    let scratch = Scratch::new("stale");
    let path = scratch.join("socket");
    let mut driver = suite::driver(Config::new(), backend);
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

/// A bind of the path of a live listener gives `AddrInUse` while the listener holds the lock of
/// the path, also with `Stale::Replace` after its socket file is gone. The lock file has mode
/// 0600, and the close of the listener keeps it, as it keeps the socket file. A bind then takes
/// the lock.
pub(in crate::driver) fn a_bind_refuses_while_a_listener_holds_the_lock_of_its_path(backend: Backend) {
    let scratch = Scratch::new("lock");
    let path = scratch.join("socket");
    let lock = scratch.join("socket.lock");
    let mut driver = suite::driver(Config::new(), backend);
    let listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    assert_eq!(fs::symlink_metadata(&lock).unwrap().mode() & 0o7777, 0o600);
    fs::remove_file(&path).unwrap();
    for stale in [Stale::Refuse, Stale::Replace] {
        let error = bind(&mut driver, &path, stale).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse, "{stale:?}");
    }
    let close = driver.close_local_listener(listener).unwrap();
    drive(&mut driver, close).unwrap();
    assert!(fs::symlink_metadata(&lock).unwrap().file_type().is_file());
    let _listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
}

/// A bind with `Stale::Replace` on the socket file of a live listener of another program, which
/// holds no lock of the path, gives `AddrInUse` and keeps the file.
pub(in crate::driver) fn a_bind_does_not_replace_the_socket_file_of_a_live_listener_with_no_lock(backend: Backend) {
    let scratch = Scratch::new("other");
    let path = scratch.join("socket");
    let other = UnixListener::bind(&path).unwrap();
    let mut driver = suite::driver(Config::new(), backend);
    let error = bind(&mut driver, &path, Stale::Replace).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    let _peer = UnixStream::connect(&path).unwrap();
    other.accept().unwrap();
}

/// A listener of the test at `path` whose queue holds one connection that waits for an accept: a
/// socket, a bind, and a listen with the backlog 1.
#[expect(unsafe_code, reason = "the calls read the address, which lives during them")]
fn small_listener(path: &Path) -> OwnedFd {
    let (address, len) = super::unix_address(path).unwrap();
    // SAFETY: the call takes no pointer.
    let fd =
        unsafe { rsnetwork_udp::sys::socket(c_int::from(super::sys::AF_UNIX), rsnetwork_udp::sys::SOCK_STREAM, 0) };
    assert!(fd >= 0);
    // SAFETY: the C library gave a new descriptor that nothing else owns.
    let socket = unsafe { OwnedFd::from_raw_fd(fd) };
    // SAFETY: `address` holds an address of `len` bytes during the call.
    let bound = unsafe { rsnetwork_udp::sys::bind(fd, ptr::from_ref(&address).cast(), len) };
    assert_eq!(bound, 0);
    // SAFETY: the call takes no pointer.
    assert_eq!(unsafe { rsnetwork_udp::sys::listen(fd, 1) }, 0);
    socket
}

/// Takes the lock of the listener at `path` in its lock file, the path with `.lock` after it, as a
/// live listener of the driver holds it.
#[expect(unsafe_code, reason = "the call takes no pointer")]
fn hold_lock(path: &Path) -> fs::File {
    let mut lock = std::ffi::OsString::from(path.as_os_str());
    lock.push(".lock");
    let file = fs::File::create(lock).unwrap();
    // SAFETY: the call takes no pointer.
    let locked = unsafe { super::sys::flock(file.as_raw_fd(), super::sys::LOCK_EX | super::sys::LOCK_NB) };
    assert_eq!(locked, 0);
    file
}

/// Connects to the listener at `path` until a connect fails, and gives the streams and the error.
fn fill(driver: &mut Driver, path: &Path) -> (Vec<LocalStream>, io::Error) {
    /// More connects than a queue of one connection takes: two on Linux, and one on macOS.
    const CONNECTS_MAX: usize = 8;
    let mut streams = Vec::new();
    loop {
        assert!(
            streams.len() < CONNECTS_MAX,
            "the queue took {CONNECTS_MAX} connections"
        );
        let connect = driver.connect_local(path).unwrap();
        match drive(driver, connect).unwrap() {
            Ok(stream) => streams.push(stream),
            Err(error) => return (streams, error),
        }
    }
}

/// A connect to a live listener whose queue of connections that wait for an accept is full gives
/// `ResourceBusy`. After the listener closes and its lock ends, a connect gives
/// `ConnectionRefused`. macOS refuses both connects with one error, and the lock of the listener
/// tells them apart.
pub(in crate::driver) fn a_connect_to_a_full_listener_is_busy_and_to_a_closed_one_is_refused(backend: Backend) {
    let scratch = Scratch::new("busy");
    let path = scratch.join("socket");
    let mut driver = suite::driver(Config::new(), backend);
    let listener = small_listener(&path);
    let lock = hold_lock(&path);
    let (streams, busy) = fill(&mut driver, &path);
    assert_eq!(busy.kind(), io::ErrorKind::ResourceBusy);
    assert!(
        !streams.is_empty(),
        "the listener took a connection before its queue was full"
    );
    drop((listener, lock));
    let connect = driver.connect_local(&path).unwrap();
    let error = drive(&mut driver, connect).unwrap().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
}

/// A bind with `Stale::Replace` on the path of a live listener that holds the lock gives
/// `AddrInUse` when the queue of the listener is full, and the socket file stays. On macOS, a full
/// queue refuses a connect, as a socket file with no listener does, so only the lock keeps the
/// file.
pub(in crate::driver) fn a_bind_does_not_replace_the_socket_file_of_a_busy_listener_that_holds_the_lock(
    backend: Backend,
) {
    let scratch = Scratch::new("busy-replace");
    let path = scratch.join("socket");
    let mut driver = suite::driver(Config::new(), backend);
    let _listener = small_listener(&path);
    let _lock = hold_lock(&path);
    let _streams = fill(&mut driver, &path);
    let error = bind(&mut driver, &path, Stale::Replace).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    assert!(fs::symlink_metadata(&path).unwrap().file_type().is_socket());
}

/// A path that the kernel would change gives `InvalidInput` and makes no socket: an empty path,
/// which would bind an abstract name with no file and no permission on Linux, a path with a NUL
/// byte, and a path that fills the path array of `struct sockaddr_un`, which then has no room for
/// the NUL byte. A path one byte shorter binds.
pub(in crate::driver) fn a_path_that_does_not_fit_a_socket_address_gives_invalid_input(backend: Backend) {
    /// The size of `sun_path`: `include/uapi/linux/un.h` of Linux, and `sys/un.h` of macOS.
    #[cfg(target_os = "linux")]
    const SUN_PATH: usize = 108;
    #[cfg(target_os = "macos")]
    const SUN_PATH: usize = 104;
    let scratch = Scratch::new("path");
    let mut driver = suite::driver(Config::new(), backend);
    let base = scratch.join("").into_os_string().into_vec();
    let long = |len: usize| {
        let mut path = base.clone();
        path.resize(len, b'p');
        PathBuf::from(OsStr::from_bytes(&path))
    };
    for path in [
        PathBuf::new(),
        PathBuf::from(OsStr::from_bytes(b"/tmp/a\0b")),
        long(SUN_PATH),
    ] {
        let error = bind(&mut driver, &path, Stale::Refuse).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{path:?}");
        let connect = driver.connect_local(&path).unwrap();
        let error = drive(&mut driver, connect).unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{path:?}");
    }
    let path = long(SUN_PATH - 1);
    let _listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    UnixStream::connect(&path).unwrap();
}

/// An accepted stream reports the identity of the process that connected, here this process: the
/// user and the group that own the socket file, which this process made, and its process ID.
pub(in crate::driver) fn an_accepted_stream_reports_the_user_group_and_process_of_this_process(backend: Backend) {
    let scratch = Scratch::new("own");
    let path = scratch.join("socket");
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let _peer = UnixStream::connect(&path).unwrap();
    let accept = driver.accept_local(&mut listener).unwrap();
    let stream = drive(&mut driver, accept).unwrap().unwrap();
    let peer = driver.local_peer(&stream).unwrap();
    let owner = fs::symlink_metadata(&path).unwrap();
    assert_eq!(
        (peer.user(), peer.group(), peer.process()),
        (owner.uid(), owner.gid(), Some(std::process::id()))
    );
}

/// A process of the user and the group `nobody` that holds a socket connected to a listener. Its
/// drop ends the process.
#[derive(Debug)]
struct Connected(Child);

impl Drop for Connected {
    fn drop(&mut self) {
        drop(self.0.kill());
        drop(self.0.wait());
    }
}

/// An accepted stream whose peer closed its socket before the accept still reports the user and
/// the group of the peer. Linux also reports its process, and macOS reports none, because macOS
/// has no process for a peer that closed its socket.
pub(in crate::driver) fn an_accepted_stream_of_a_closed_peer_reports_its_user_and_group(backend: Backend) {
    let scratch = Scratch::new("closed");
    let path = scratch.join("socket");
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    drop(UnixStream::connect(&path).unwrap());
    let accept = driver.accept_local(&mut listener).unwrap();
    let stream = drive(&mut driver, accept).unwrap().unwrap();
    let peer = driver.local_peer(&stream).unwrap();
    let owner = fs::symlink_metadata(&path).unwrap();
    #[cfg(target_os = "linux")]
    let process = Some(std::process::id());
    #[cfg(target_os = "macos")]
    let process = None;
    assert_eq!(
        (peer.user(), peer.group(), peer.process()),
        (owner.uid(), owner.gid(), process)
    );
}

/// Starts a process as the user and the group `nobody` that connects a socket to `path`, and then
/// waits in `/bin/sleep` with the connection open: macOS reports the process of a live peer only.
/// The connect runs in the new process before the exec, so a failure of the connect is the error
/// of the start.
#[expect(unsafe_code, reason = "the hook of the new process makes only system calls")]
fn connect_as_nobody(path: &Path) -> io::Result<Connected> {
    let (address, len) = super::unix_address(path).ok_or(io::ErrorKind::InvalidInput)?;
    let mut command = Command::new("/bin/sleep");
    command.arg("60").uid(NOBODY).gid(NOBODY);
    // SAFETY: the hook allocates nothing and takes no lock: it makes a socket and connects it with
    // system calls, in the new process. The address is a copy that the hook owns. The socket has
    // no close-on-exec flag, so `sleep` keeps the connection.
    unsafe {
        command.pre_exec(move || {
            let fd = rsnetwork_udp::sys::socket(c_int::from(super::sys::AF_UNIX), rsnetwork_udp::sys::SOCK_STREAM, 0);
            if fd < 0 || super::sys::connect(fd, core::ptr::from_ref(&address).cast(), len) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command.spawn().map(Connected)
}

/// A connect of a second OS user to the socket file of a listener gives `PermissionDenied`: the
/// mode 0600 refuses it, in a directory that each user can enter.
pub(in crate::driver) fn a_connect_of_a_second_os_user_is_refused(backend: Backend) {
    let scratch = Scratch::new("second-user");
    let path = scratch.join("socket");
    let mut driver = suite::driver(Config::new(), backend);
    let _listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let error = connect_as_nobody(&path).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}

/// An accepted stream reports the user, the group, and the process ID of the process that
/// connected, not those of the listener, which runs as root here.
pub(in crate::driver) fn an_accepted_stream_reports_the_user_group_and_process_of_its_peer(backend: Backend) {
    let scratch = Scratch::new("peer");
    let path = scratch.join("socket");
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    // Only the test opens the socket file to the second user.
    fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
    let connected = connect_as_nobody(&path).unwrap();
    let accept = driver.accept_local(&mut listener).unwrap();
    let stream = drive(&mut driver, accept).unwrap().unwrap();
    let peer = driver.local_peer(&stream).unwrap();
    assert_eq!(
        (peer.user(), peer.group(), peer.process()),
        (NOBODY, NOBODY, Some(connected.0.id()))
    );
}

/// A bind with `Stale::Replace` on a stale socket file of another user gives `PermissionDenied`
/// and keeps the file.
pub(in crate::driver) fn a_bind_does_not_replace_a_stale_socket_file_of_another_user(backend: Backend) {
    let scratch = Scratch::new("foreign");
    let path = scratch.join("socket");
    drop(UnixListener::bind(&path).unwrap());
    std::os::unix::fs::chown(&path, Some(NOBODY), Some(NOBODY)).unwrap();
    let mut driver = suite::driver(Config::new(), backend);
    let error = bind(&mut driver, &path, Stale::Replace).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(fs::symlink_metadata(&path).unwrap().uid(), NOBODY);
}
