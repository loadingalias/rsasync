use alloc::string::String;
use alloc::vec::Vec;
use core::{mem, ptr};
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use super::super::super::iocp::sys;
use super::super::super::suite::{self, Backend};
use super::super::super::tests::{drive, numbered, posted};
use super::super::super::{Config, Driver, Reason, Regions};
use super::super::{LocalListener, LocalStream, Stale};
use crate::buffer::{Backing, Buffer as _, Lease};

/// The pipe name of the case `case`, which no other test process uses.
fn pipe(case: &str) -> PathBuf {
    PathBuf::from(alloc::format!(r"\\.\pipe\rsasync-{case}-{}", std::process::id()))
}

/// Binds a local listener at `path` with `stale`, and gives the result of the bind.
fn bind(driver: &mut Driver, path: &Path, stale: Stale) -> io::Result<LocalListener> {
    let bind = driver.bind_local(path, stale).expect("the test driver has room");
    drive(driver, bind).expect("the driver runs")
}

/// Connects a local stream to the listener at `path`, and gives the result of the connect.
fn connect(driver: &mut Driver, path: &Path) -> io::Result<LocalStream> {
    let connect = driver.connect_local(path).expect("the test driver has room");
    drive(driver, connect).expect("the driver runs")
}

/// Opens the pipe at `path` as a client of the test, with the synchronous handle of `std::fs`.
fn client(path: &Path) -> io::Result<fs::File> {
    fs::OpenOptions::new().read(true).write(true).open(path)
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

/// A numbered byte stream crosses a named pipe in each direction with no gap and no repeat,
/// through the abandoned operations of the stream rules: a dropped accept leaves its client to the
/// next accept, a dropped send that sends all its bytes reports nothing, and a dropped or a
/// forgotten receive leaves its bytes to the next receive. A pipe has no half-close, and the close
/// of one end is the end of the stream for the other end, after each byte that went before it.
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
pub(in crate::driver) fn a_numbered_stream_crosses_a_named_pipe_with_no_gap_through_the_abandoned_operations(
    backend: Backend,
) {
    let path = pipe("stream");
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let accept = driver.accept_local(&mut listener).unwrap();
    driver.turn().unwrap();
    let mut peer = client(&path).unwrap();
    posted(&mut driver, 1);
    drop(accept);
    driver.turn().unwrap();
    let next = driver.accept_local(&mut listener).unwrap();
    let mut stream = drive(&mut driver, next).unwrap().unwrap();
    assert_eq!(
        driver.shutdown_write(&mut stream).unwrap_err().reason(),
        Reason::Unsupported
    );

    drop(driver.send(&mut stream, Lease::from(numbered(0, 1000))).unwrap());
    let mut regions = Regions::with_capacity(2);
    regions.push(Lease::from(numbered(1000, 300)));
    regions.push(Lease::from(numbered(1300, 700)));
    let vectored = driver.send_vectored(&mut stream, regions).unwrap();
    assert_eq!(drive(&mut driver, vectored).unwrap().0.unwrap(), 1000);
    let close = driver.close_local(stream).unwrap();
    drive(&mut driver, close).unwrap();
    let mut received = Vec::new();
    peer.read_to_end(&mut received).unwrap();
    assert_eq!(received, numbered(0, 2000));
    drop(peer);

    // A write of the client is in the buffer of the pipe when it returns, so the first receive
    // takes 600 bytes, and 400 bytes wait for the forgotten receive.
    let accept = driver.accept_local(&mut listener).unwrap();
    driver.turn().unwrap();
    let mut peer = client(&path).unwrap();
    let mut stream = drive(&mut driver, accept).unwrap().unwrap();
    peer.write_all(&numbered(0, 1000)).unwrap();
    let dropped = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(600)))
        .unwrap();
    posted(&mut driver, 1);
    drop(dropped);
    let next = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    let (count, buffer) = drive(&mut driver, next).unwrap();
    assert_eq!((count.unwrap(), buffer.bytes()), (600, &numbered(0, 600)[..]));
    let forgotten = driver
        .recv(&mut stream, Backing::from(Vec::with_capacity(4096)))
        .unwrap();
    posted(&mut driver, 1);
    driver.turn().unwrap();
    mem::forget(forgotten);
    peer.write_all(&numbered(1000, 500)).unwrap();
    drop(peer);
    let mut received = numbered(0, 600);
    received.extend(receive_all(&mut driver, &mut stream, 300));
    assert_eq!(received, numbered(0, 1500));
}

/// The SID of the user of this process, as `whoami /user` gives it.
fn this_user() -> String {
    let output = Command::new("whoami")
        .args(["/user", "/fo", "csv", "/nh"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    // One line: "<domain>\<user>","<SID>".
    let line = String::from_utf8_lossy(&output.stdout);
    let sid = line.trim().rsplit(',').next().unwrap().trim_matches('"');
    assert!(sid.starts_with("S-1-"), "whoami gave {line}");
    sid.into()
}

/// Receives on `stream` into a buffer of 8 bytes, and gives the bytes.
fn receive(driver: &mut Driver, stream: &mut LocalStream) -> Vec<u8> {
    let receive = driver.recv(stream, Backing::from(Vec::with_capacity(8))).unwrap();
    let (count, buffer) = drive(driver, receive).unwrap();
    count.unwrap();
    buffer.bytes().to_vec()
}

/// The user of an accepted stream is the user of its client, which the driver reads one time and
/// keeps. A client of dynamic tracking, here of `std::fs`, gives `ERROR_CANNOT_IMPERSONATE` until
/// the first receive takes its data, then its user, as `whoami` gives it, and the user stays after
/// a later write. A client in another process, here PowerShell, gives its process ID. A client of
/// `connect_local` gives its user at the accept, and its stream gives the identity of its server,
/// this process: its process ID and the owner of the pipe, the user of this process.
pub(in crate::driver) fn an_accepted_stream_reports_the_process_and_user_of_its_client(backend: Backend) {
    let path = pipe("peer");
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let user = this_user();

    let accept = driver.accept_local(&mut listener).unwrap();
    driver.turn().unwrap();
    let mut peer = client(&path).unwrap();
    let mut stream = drive(&mut driver, accept).unwrap().unwrap();
    let early = driver.local_peer(&stream).unwrap_err();
    assert_eq!(early.raw_os_error(), Some(ERROR_CANNOT_IMPERSONATE), "{early}");
    peer.write_all(&[1]).unwrap();
    assert_eq!(receive(&mut driver, &mut stream), [1]);
    let first = driver.local_peer(&stream).unwrap();
    assert_eq!(first.process(), Some(std::process::id()));
    assert_eq!(alloc::format!("{}", first.user()), user);
    peer.write_all(&[2]).unwrap();
    assert_eq!(receive(&mut driver, &mut stream), [2]);
    assert_eq!(driver.local_peer(&stream).unwrap(), first);

    let accept = driver.accept_local(&mut listener).unwrap();
    let name = path.to_str().unwrap().trim_start_matches(r"\\.\pipe\");
    // The client connects, writes one byte, and waits until its input ends.
    let script = alloc::format!(
        "$c = New-Object System.IO.Pipes.NamedPipeClientStream('.', '{name}', 'InOut'); $c.Connect(30000); \
         $c.WriteByte(7); $c.Flush(); [Console]::In.ReadToEnd() | Out-Null"
    );
    let mut child = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stream = drive(&mut driver, accept).unwrap().unwrap();
    assert_eq!(receive(&mut driver, &mut stream), [7]);
    let peer = driver.local_peer(&stream).unwrap();
    assert_eq!(peer.process(), Some(child.id()));
    assert_eq!(alloc::format!("{}", peer.user()), user);
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());

    let accept = driver.accept_local(&mut listener).unwrap();
    let connected = connect(&mut driver, &path).unwrap();
    let accepted = drive(&mut driver, accept).unwrap().unwrap();
    let peer = driver.local_peer(&accepted).unwrap();
    assert_eq!(peer.process(), Some(std::process::id()));
    assert_eq!(alloc::format!("{}", peer.user()), user);
    let server = driver.local_peer(&connected).unwrap();
    assert_eq!(server.process(), Some(std::process::id()));
    assert_eq!(alloc::format!("{}", server.user()), user);
}

/// A burst of `BACKLOG` clients that connect before an accept each connect, and the accepts then
/// take them, each at once, in the order of their connects: each client writes its number, and
/// the accepted streams receive the numbers in order. A client after them gets `ResourceBusy`, the
/// Windows form of a full backlog, and after the accepts a client connects again.
pub(in crate::driver) fn a_burst_of_backlog_clients_connects_and_each_one_is_accepted(backend: Backend) {
    let path = pipe("backlog");
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let clients: Vec<fs::File> = (0..super::BACKLOG)
        .map(|number| {
            let mut client = client(&path).unwrap();
            client.write_all(&[u8::try_from(number).unwrap()]).unwrap();
            client
        })
        .collect();
    assert_eq!(
        connect(&mut driver, &path).unwrap_err().kind(),
        io::ErrorKind::ResourceBusy
    );
    for number in 0..clients.len() {
        let accept = driver.accept_local(&mut listener).unwrap();
        let mut stream = drive(&mut driver, accept).unwrap().unwrap();
        let receive = driver.recv(&mut stream, Backing::from(Vec::with_capacity(8))).unwrap();
        let (count, buffer) = drive(&mut driver, receive).unwrap();
        assert_eq!(
            (count.unwrap(), buffer.bytes()),
            (1, &[u8::try_from(number).unwrap()][..])
        );
    }
    let accept = driver.accept_local(&mut listener).unwrap();
    let _late = connect(&mut driver, &path).unwrap();
    drive(&mut driver, accept).unwrap().unwrap();
}

/// `ERROR_CANNOT_IMPERSONATE` (`winerror.h`): the server has read no data of a client whose
/// security context the system captures at its writes.
const ERROR_CANNOT_IMPERSONATE: i32 = 1368;

/// An instance of the pipe of `path` that the test makes first, as a squatter does: with no
/// first-instance flag and the default security descriptor, which lets this user make more
/// instances.
#[expect(unsafe_code, reason = "the call reads the name and gives a new handle")]
fn squat(path: &Path, flags: u32) -> OwnedHandle {
    let name = super::name(path).unwrap();
    // SAFETY: the name is NUL-terminated, and the call names no other memory.
    let raw = unsafe {
        sys::CreateNamedPipeW(
            name.as_ptr(),
            sys::PIPE_ACCESS_DUPLEX | flags,
            sys::PIPE_BYTES,
            sys::PIPE_UNLIMITED_INSTANCES,
            4096,
            4096,
            0,
            ptr::null_mut(),
        )
    };
    assert_ne!(raw.addr(), sys::INVALID_HANDLE_VALUE, "{}", io::Error::last_os_error());
    // SAFETY: the call gave a new handle that nothing else owns.
    unsafe { OwnedHandle::from_raw_handle(raw) }
}

/// A bind of a pipe name with an instance gives `AddrInUse`, with `Stale::Refuse` and with
/// `Stale::Replace`: an instance of a listener of the driver, and an instance that a squatter made
/// first, which would else take the clients of the listener. After the close of the squatter, the
/// name binds.
pub(in crate::driver) fn a_bind_refuses_a_pipe_name_with_an_instance_also_one_that_a_squatter_made_first(
    backend: Backend,
) {
    let mut driver = suite::driver(Config::new(), backend);
    let path = pipe("second");
    let listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    for stale in [Stale::Refuse, Stale::Replace] {
        let error = bind(&mut driver, &path, stale).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse, "{error}");
    }
    let close = driver.close_local_listener(listener).unwrap();
    drive(&mut driver, close).unwrap();

    let path = pipe("squatted");
    let squatter = squat(&path, 0);
    for stale in [Stale::Refuse, Stale::Replace] {
        let error = bind(&mut driver, &path, stale).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse, "{error}");
    }
    drop(squatter);
    let listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let close = driver.close_local_listener(listener).unwrap();
    drive(&mut driver, close).unwrap();
}

/// A client on another computer cannot connect: an open of the pipe through the network path of
/// this computer (`\\localhost\pipe\…`) fails. The same open of a pipe of the test with no
/// rejection of remote clients works, so the network path works on the host.
pub(in crate::driver) fn a_remote_client_of_a_pipe_is_rejected(backend: Backend) {
    let remote = |path: &Path| PathBuf::from(path.to_str().unwrap().replace(r"\\.\", r"\\localhost\"));
    let control = pipe("remote-control");
    let _instance = squat(&control, 0);
    client(&remote(&control)).unwrap();

    let path = pipe("remote");
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let accept = driver.accept_local(&mut listener).unwrap();
    driver.turn().unwrap();
    assert!(client(&remote(&path)).is_err(), "a remote client opened the pipe");
    client(&path).unwrap();
    drive(&mut driver, accept).unwrap().unwrap();
}

/// A close of a listener whose accept waits for a client cancels the accept, which ends with an
/// error, and closes the instance, so the name binds again.
pub(in crate::driver) fn a_waiting_accept_ends_at_the_close_of_its_listener_and_the_name_binds_again(backend: Backend) {
    let path = pipe("cancel");
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let accept = driver.accept_local(&mut listener).unwrap();
    driver.turn().unwrap();
    // The accept borrows the listener, so the close comes after the drop of the future, which asks
    // for the cancel, as a close does.
    drop(accept);
    let close = driver.close_local_listener(listener).unwrap();
    drive(&mut driver, close).unwrap();
    let listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let close = driver.close_local_listener(listener).unwrap();
    drive(&mut driver, close).unwrap();
}

/// A path that is not a pipe name gives `InvalidInput` and makes no pipe: a file path, the prefix
/// with no name, a name of more than 256 units, and a name with a NUL unit. A name of 256 units
/// binds.
pub(in crate::driver) fn a_path_that_is_not_a_pipe_name_gives_invalid_input(backend: Backend) {
    let mut driver = suite::driver(Config::new(), backend);
    let long = alloc::format!(r"\\.\pipe\{}", "n".repeat(256 - 9));
    for path in [
        r"C:\rsasync.sock",
        r"\\.\pipe\",
        &alloc::format!("{long}n"),
        "\\\\.\\pipe\\a\0b",
    ] {
        let error = bind(&mut driver, Path::new(path), Stale::Refuse).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{path:?}: {error}");
        let error = connect(&mut driver, Path::new(path)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{path:?}: {error}");
    }
    let listener = bind(&mut driver, Path::new(&long), Stale::Refuse).unwrap();
    let close = driver.close_local_listener(listener).unwrap();
    drive(&mut driver, close).unwrap();
}

#[expect(unsafe_code, reason = "advapi32.dll gives these functions")]
#[link(name = "advapi32")]
unsafe extern "system" {
    /// `BOOL LogonUserW(LPCWSTR lpszUsername, LPCWSTR lpszDomain, LPCWSTR lpszPassword, DWORD
    /// dwLogonType, DWORD dwLogonProvider, PHANDLE phToken)` (`winbase.h`).
    fn LogonUserW(
        user: *const u16,
        domain: *const u16,
        password: *const u16,
        kind: u32,
        provider: u32,
        token: *mut sys::Handle,
    ) -> i32;
    /// `BOOL ImpersonateLoggedOnUser(HANDLE hToken)` (`securitybaseapi.h`).
    fn ImpersonateLoggedOnUser(token: sys::Handle) -> i32;
}

/// `LOGON32_LOGON_NETWORK`: a logon whose token can open local objects.
const LOGON32_LOGON_NETWORK: u32 = 3;

/// A NUL-terminated UTF-16 string of `text`.
fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain([0]).collect()
}

/// A connect of a client of a second OS user gives `PermissionDenied`: the security descriptor of
/// the pipe grants access only to the user of the listener. The thread of the test connects while
/// it impersonates the second user, whom `RSASYNC_SECOND_USER` and `RSASYNC_SECOND_PASSWORD` name.
/// As itself, it connects.
#[expect(unsafe_code, reason = "the calls read strings and give a token that the test closes")]
pub(in crate::driver) fn a_connect_of_a_second_os_user_is_denied(backend: Backend) {
    let user = std::env::var("RSASYNC_SECOND_USER").expect("the run names a second OS user");
    let password = std::env::var("RSASYNC_SECOND_PASSWORD").expect("the run gives its password");
    let path = pipe("second-user");
    let mut driver = suite::driver(Config::new(), backend);
    let mut listener = bind(&mut driver, &path, Stale::Refuse).unwrap();
    let mut token = ptr::null_mut();
    let (user, domain, password) = (wide(&user), wide("."), wide(&password));
    // SAFETY: the strings are NUL-terminated, and the call writes one handle.
    let logged_on = unsafe {
        LogonUserW(
            user.as_ptr(),
            domain.as_ptr(),
            password.as_ptr(),
            LOGON32_LOGON_NETWORK,
            0,
            &raw mut token,
        )
    };
    assert_ne!(logged_on, 0, "{}", io::Error::last_os_error());
    // SAFETY: the call gave a new handle that nothing else owns.
    let token = unsafe { OwnedHandle::from_raw_handle(token) };
    // SAFETY: the token is open. The thread acts as the second user until `RevertToSelf`.
    assert_ne!(unsafe { ImpersonateLoggedOnUser(token.as_raw_handle()) }, 0);
    let denied = connect(&mut driver, &path);
    // SAFETY: the call takes no argument.
    assert_ne!(unsafe { sys::RevertToSelf() }, 0);
    assert_eq!(denied.unwrap_err().kind(), io::ErrorKind::PermissionDenied);

    let accept = driver.accept_local(&mut listener).unwrap();
    let _stream = connect(&mut driver, &path).unwrap();
    drive(&mut driver, accept).unwrap().unwrap();
}

/// The SID of the local user `user`, as PowerShell translates the name.
fn sid_of(user: &str) -> String {
    let script = alloc::format!(
        "(New-Object System.Security.Principal.NTAccount('{user}')).Translate([System.Security.Principal.SecurityIdentifier]).Value"
    );
    let output = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8_lossy(&output.stdout).trim().into()
}

/// A client of a pipe that a second OS user made reads the user of that server: the owner of the
/// pipe is the second user, as PowerShell translates its name, and the process is the process that
/// made the pipe, here the process of the test. The thread of the test makes the pipe while it
/// impersonates the user that `RSASYNC_SECOND_USER` and `RSASYNC_SECOND_PASSWORD` name, with the
/// default security descriptor, which lets an administrator connect.
#[expect(unsafe_code, reason = "the calls read strings and give a token that the test closes")]
pub(in crate::driver) fn a_client_reads_the_process_and_user_of_a_server_of_a_second_os_user(backend: Backend) {
    let user = std::env::var("RSASYNC_SECOND_USER").expect("the run names a second OS user");
    let password = std::env::var("RSASYNC_SECOND_PASSWORD").expect("the run gives its password");
    let path = pipe("second-server");
    let mut token = ptr::null_mut();
    let (user_name, domain, password) = (wide(&user), wide("."), wide(&password));
    // SAFETY: the strings are NUL-terminated, and the call writes one handle.
    let logged_on = unsafe {
        LogonUserW(
            user_name.as_ptr(),
            domain.as_ptr(),
            password.as_ptr(),
            LOGON32_LOGON_NETWORK,
            0,
            &raw mut token,
        )
    };
    assert_ne!(logged_on, 0, "{}", io::Error::last_os_error());
    // SAFETY: the call gave a new handle that nothing else owns.
    let token = unsafe { OwnedHandle::from_raw_handle(token) };
    // SAFETY: the token is open. The thread acts as the second user until `RevertToSelf`.
    assert_ne!(unsafe { ImpersonateLoggedOnUser(token.as_raw_handle()) }, 0);
    let server = squat(&path, sys::FILE_FLAG_OVERLAPPED);
    // SAFETY: the call takes no argument.
    assert_ne!(unsafe { sys::RevertToSelf() }, 0);

    let mut driver = suite::driver(Config::new(), backend);
    let stream = connect(&mut driver, &path).unwrap();
    let peer = driver.local_peer(&stream).unwrap();
    assert_eq!(peer.process(), Some(std::process::id()));
    assert_eq!(alloc::format!("{}", peer.user()), sid_of(&user));
    drop(server);
}
