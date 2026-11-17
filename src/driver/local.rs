//! Local streams on the driver: Unix-domain stream sockets at a path, under the stream rules of
//! TCP.
//!
//! A local stream has the directions of a TCP stream ([`ReadHalf`] and [`WriteHalf`]), so the
//! receive, send, vectored send, and half-close operations of [`TcpStream`](super::TcpStream)
//! serve it, with the same rules for a dropped, forgotten, or cancelled operation. A local
//! listener has the rules of a [`TcpListener`](super::TcpListener).

#[cfg(target_os = "macos")]
use core::ffi::c_int;
use core::ptr;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use rsnetwork_udp::SockAddrStorage;
use rsnetwork_udp::sys::SOCK_STREAM;
use sys::{EADDRINUSE, EAGAIN, EBUSY, ECONNREFUSED, EINVAL, ENOENT, ENOMEM, EPERM};

use super::completion::Completion;
use super::tcp::{self, Closed, ReadHalf, WriteHalf};
use super::{Driver, Held, Kind, Medium, Operation, Refused, Resource, Tag};
use crate::sync::Arc;
use crate::table::OpId;

mod sys;

/// The mode of the socket file and of the lock file of a listener: read and write for its owner
/// only.
const OWNER_ONLY: u16 = 0o600;

/// A connected Unix-domain stream socket that a driver owns: its read half and its write half.
///
/// A receive borrows the read half exclusively, and a send borrows the write half exclusively, as
/// for a [`TcpStream`](super::TcpStream). [`Driver::recv`], [`Driver::recv_selected`],
/// [`Driver::send`], [`Driver::send_vectored`], and [`Driver::shutdown_write`] take it, with the
/// rules of a TCP stream for a dropped, forgotten, or cancelled operation.
///
/// A request and its reply over a local stream, with `run` as in
/// [`Operation`](super::Operation):
///
/// ```no_run
/// # fn run<F: std::future::Future>(driver: &mut rsasync::driver::Driver, future: F) -> std::io::Result<F::Output> {
/// #     use std::future::Future as _;
/// #     let mut future = std::pin::pin!(future);
/// #     let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
/// #     loop {
/// #         if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #             return Ok(output);
/// #         }
/// #         if !driver.turn()?.more() {
/// #             if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #                 return Ok(output);
/// #             }
/// #             driver.wait()?;
/// #         }
/// #     }
/// # }
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use rsasync::buffer::{Backing, Buffer as _, Lease};
/// use rsasync::driver::{Config, Driver, Stale};
///
/// let mut driver = Driver::new(Config::new())?;
/// let path = std::env::temp_dir().join("service.sock");
/// // The socket file has mode 0600. A file that a closed listener left is replaced.
/// let bind = driver.bind_local(&path, Stale::Replace)?;
/// let mut listener = run(&mut driver, bind)???;
///
/// let connect = driver.connect_local(&path)?;
/// let mut client = run(&mut driver, connect)???;
/// let accept = driver.accept_local(&mut listener)?;
/// let mut server = run(&mut driver, accept)???;
/// // The kernel recorded the user and the process of the client at its connect.
/// assert_eq!(driver.local_peer(&server)?.process(), Some(std::process::id()));
///
/// let send = driver.send(&mut client, Lease::from(b"ping".to_vec()))?;
/// let (sent, _lease) = run(&mut driver, send)??;
/// assert_eq!(sent?, 4);
/// let receive = driver.recv(&mut server, Backing::from(Vec::with_capacity(64)))?;
/// let (received, buffer) = run(&mut driver, receive)??;
/// assert_eq!((received?, buffer.bytes()), (4, &b"ping"[..]));
///
/// for stream in [client, server] {
///     let close = driver.close_local(stream)?;
///     run(&mut driver, close)??;
/// }
/// let close = driver.close_local_listener(listener)?;
/// run(&mut driver, close)??;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct LocalStream {
    read: ReadHalf,
    write: WriteHalf,
}

impl LocalStream {
    fn new(resource: Resource) -> Self {
        Self {
            read: ReadHalf { resource },
            write: WriteHalf { resource },
        }
    }

    /// The read half and the write half, so that a receive and a send can run at one time.
    pub fn split(&mut self) -> (&mut ReadHalf, &mut WriteHalf) {
        (&mut self.read, &mut self.write)
    }
}

impl AsMut<ReadHalf> for LocalStream {
    fn as_mut(&mut self) -> &mut ReadHalf {
        &mut self.read
    }
}

impl AsMut<WriteHalf> for LocalStream {
    fn as_mut(&mut self) -> &mut WriteHalf {
        &mut self.write
    }
}

/// A listening Unix-domain stream socket that a driver owns, at a path.
///
/// An accept borrows the listener exclusively, so one accept runs at a time. When an accept future
/// drops, the driver asks the kernel to cancel the accept. A connection that the accept took before
/// the cancel took effect stays with the listener, and the next accept gives it before it submits.
/// A close of the listener closes it, and the socket file stays at its path.
#[derive(Debug)]
pub struct LocalListener {
    resource: Resource,
}

/// What a bind of a local listener does when its path exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stale {
    /// The bind gives `AddrInUse`.
    Refuse,
    /// The bind removes a stale socket file and binds: a socket file that the user of the process
    /// owns, whose lock no listener holds, and to which a connect is refused. A live listener, or a
    /// file that is not a socket, gives `AddrInUse`, and a socket file of another user gives
    /// `PermissionDenied`. The driver removes no other file.
    ///
    /// On macOS, a listener of another program, which holds no lock, refuses a connect also when
    /// its queue of connections that wait for an accept is full. Such a busy listener looks stale.
    Replace,
}

/// The OS identity of the peer of a local stream: `SO_PEERCRED` on Linux, and `getpeereid` and
/// `LOCAL_PEERPID` on macOS. Check the user and the group to authorize a peer: the kernel recorded
/// them at the connect on each system. On macOS, the process is advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    user: u32,
    group: u32,
    process: Option<u32>,
}

impl Peer {
    /// The effective user ID of the peer.
    #[must_use]
    pub const fn user(self) -> u32 {
        self.user
    }

    /// The effective group ID of the peer.
    #[must_use]
    pub const fn group(self) -> u32 {
        self.group
    }

    /// The process ID of the peer, where the system gives it: `None` on Linux for a peer in a
    /// process namespace that this process cannot see, and on macOS for a peer that closed its
    /// socket.
    #[must_use]
    pub const fn process(self) -> Option<u32> {
        self.process
    }
}

impl Driver {
    /// Binds a Unix-domain stream socket to the socket file at `path`, and listens on it. Only the
    /// owner of the socket file can connect: the file has mode 0600 before any connect can
    /// succeed, and the umask of the process can only remove more permissions. Keep the directory
    /// of `path` closed to writes of other users, because a user who can write it can replace
    /// the socket file. `stale` selects what the bind does when the path exists. The future gives
    /// the listener, and it is ready at once: the driver makes the system calls.
    ///
    /// The listener holds a lock on the lock file at `path` with `.lock` after it, which the bind
    /// makes with mode 0600, until its socket closes. A bind of the path refuses while a listener
    /// holds the lock, and the lock ends when the process of the listener ends. A close keeps the
    /// lock file, as it keeps the socket file.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives `InvalidInput`
    /// for a path that is empty, holds a NUL byte, or has more than 107 bytes on Linux or 103 on
    /// macOS, `AddrInUse` for a path that exists or whose lock a listener holds, as [`Stale`]
    /// tells, `PermissionDenied` for a lock file of another user, another error of the system, or
    /// `OutOfMemory` when each resource slot of the driver is in use.
    pub fn bind_local(
        &mut self,
        path: &Path,
        stale: Stale,
    ) -> Result<Operation<'static, io::Result<LocalListener>>, Refused<()>> {
        let id = self.begin_now(Kind::Bind)?;
        let listening = unix_address(path)
            .ok_or(EINVAL)
            .and_then(|address| listen(path, address, stale));
        self.end_now(id, listening);
        Ok(Operation::new(
            Completion::new(Arc::clone(&self.shared), id, false),
            |result, held| {
                Some(tcp::count(result).and_then(|_| {
                    let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
                    Ok(LocalListener { resource })
                }))
            },
        ))
    }

    /// Connects a new Unix-domain stream socket to the listener at `path`. The future gives the
    /// stream, and it is ready at once: a Unix-domain connect never waits in the kernel, so the
    /// driver makes the system call. A drop of the future closes the stream that it did not give,
    /// so its peer reads the end of the stream.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives `InvalidInput`
    /// for a path as for [`Driver::bind_local`], `NotFound` for a path with no file,
    /// `ConnectionRefused` for a socket file with no listener, `ResourceBusy` for a live listener
    /// whose queue of connections that wait for an accept is full, so that a later connect can
    /// succeed, `PermissionDenied` for a socket file that the user of the process cannot write,
    /// for example one of another user, another error of the system, or `OutOfMemory` when each
    /// resource slot of the driver is in use.
    pub fn connect_local(&mut self, path: &Path) -> Result<Operation<'static, io::Result<LocalStream>>, Refused<()>> {
        let id = self.begin_now(Kind::Connect)?;
        let connected = unix_address(path).ok_or(EINVAL).and_then(|address| {
            connect(address).map_err(|errno| match errno {
                // A live listener with a full queue: Linux gives `EAGAIN`, and macOS gives
                // `ECONNREFUSED`, as for a socket file with no listener. On macOS, the lock of
                // the listener tells the two apart.
                EAGAIN => EBUSY,
                #[cfg(target_os = "macos")]
                ECONNREFUSED if locked(path) => EBUSY,
                errno => errno,
            })
        });
        self.end_now(id, connected.map(|socket| (socket, None)));
        Ok(Operation::new(
            Completion::new(Arc::clone(&self.shared), id, false),
            |result, held| {
                Some(tcp::count(result).and_then(|_| {
                    let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
                    Ok(LocalStream::new(resource))
                }))
            },
        ))
    }

    /// Accepts a connection on `listener`. The future gives the stream; [`Driver::local_peer`]
    /// gives the identity of its peer. A drop of the future asks for a cancel, and a connection
    /// that the accept took stays with the listener for the next accept.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system, or `OutOfMemory` when each resource slot of the driver is in use.
    pub fn accept_local<'a>(
        &mut self,
        listener: &'a mut LocalListener,
    ) -> Result<Operation<'a, io::Result<LocalStream>>, Refused<()>> {
        let completion = self.begin_accept(listener.resource)?;
        Ok(Operation::new(completion, |result, held| {
            Some(tcp::count(result).and_then(|_| {
                let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
                Ok(LocalStream::new(resource))
            }))
        }))
    }

    /// The OS identity of the peer of `stream`. For an accepted stream, it is the process that
    /// connected, and for a connected stream, the process that listened. The kernel recorded the
    /// user and the group at the connect, so they do not change when the peer changes its user
    /// later. On Linux, the kernel also recorded the process at the connect.
    ///
    /// On macOS, the process is the last process that used the socket of the peer. It changes when
    /// the peer gives the socket to another process, for example a child after a fork, and it can
    /// name a process that ended. Use the user and the group to authorize a peer.
    ///
    /// On macOS, the system has no process for a peer that closed its socket, also before the
    /// accept, so [`Peer::process`] gives `None`. The user and the group stay.
    ///
    /// # Errors
    ///
    /// An error of the system.
    pub fn local_peer(&self, stream: &LocalStream) -> io::Result<Peer> {
        let fd = self
            .resources
            .borrow(stream.read.resource)
            .ok_or(io::ErrorKind::NotFound)?;
        peer(fd)
    }

    /// Closes `stream`, as [`Driver::close`] closes a TCP stream: it cancels each operation, waits
    /// until each one ends, releases the bytes that no receive took, and closes the socket. The
    /// future gives the progress that dropped sends owe.
    ///
    /// # Errors
    ///
    /// A refusal, with the stream, when the driver has no room for the operation.
    pub fn close_local(&mut self, stream: LocalStream) -> Result<Operation<'static, Closed>, Refused<LocalStream>> {
        let resource = stream.read.resource;
        let completion = self.close_resource(resource, stream)?;
        Ok(Operation::new(completion, |_, held| held.closed))
    }

    /// Closes `listener`. The close cancels the accept that runs, waits until it ends, closes each
    /// connection that no accept took, and closes the socket. The socket file stays at its path, so
    /// a next bind of the path needs [`Stale::Replace`].
    ///
    /// # Errors
    ///
    /// A refusal, with the listener, when the driver has no room for the operation.
    pub fn close_local_listener(
        &mut self,
        listener: LocalListener,
    ) -> Result<Operation<'static, Closed>, Refused<LocalListener>> {
        let resource = listener.resource;
        let completion = self.close_resource(resource, listener)?;
        Ok(Operation::new(completion, |_, held| held.closed))
    }

    /// Records an operation of `kind` that ends at once, with no submission.
    fn begin_now(&mut self, kind: Kind) -> Result<OpId, Refused<()>> {
        let tag = Tag { resource: None, kind };
        self.open(tag, 0, (), |()| Held::default())
            .map_err(|(reason, value)| Refused { reason, value })
    }

    /// Ends the operation `id` with the socket of `made` and the lock of a listener, which the
    /// driver then owns, or with the error number of `made`, or with `ENOMEM` when each resource
    /// slot is in use. The socket closes when the operation has no consumer.
    fn end_now(&mut self, id: OpId, made: Result<(OwnedFd, Option<OwnedFd>), i32>) {
        let result = made.and_then(|(socket, lock)| {
            let resource = self.adopt(socket, Medium::Stream).map_err(|_| ENOMEM)?;
            if let Some(owned) = self.resources.get(resource) {
                owned.lock = lock;
            }
            self.hold(id, |held| held.accepted = Some(resource));
            Ok(())
        });
        self.complete_now(id, result.err().map_or(0, i32::saturating_neg));
    }
}

/// The OS identity of the peer of the connected socket `fd`, from `SO_PEERCRED`.
#[cfg(target_os = "linux")]
#[expect(
    unsafe_code,
    reason = "the kernel writes one `struct ucred` that lives during the call"
)]
fn peer(fd: BorrowedFd<'_>) -> io::Result<Peer> {
    let (mut credentials, mut len) = (sys::Ucred::default(), sys::UCRED_LEN);
    // SAFETY: the kernel writes at most `len` bytes into `credentials`, which is a `struct ucred`
    // that lives during the call, and the length into `len`.
    let result = unsafe {
        rsnetwork_udp::sys::getsockopt(
            fd.as_raw_fd(),
            rsnetwork_udp::sys::SOL_SOCKET,
            sys::SO_PEERCRED,
            (&raw mut credentials).cast(),
            &raw mut len,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    // The kernel gives the process 0 for a peer in a process namespace that this one cannot see.
    Ok(Peer {
        user: credentials.uid,
        group: credentials.gid,
        process: (credentials.pid > 0).then(|| credentials.pid.cast_unsigned()),
    })
}

/// The OS identity of the peer of the connected socket `fd`: its user and group from
/// `getpeereid`, and its process from `LOCAL_PEERPID`.
#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "the kernel writes integers that live during the calls")]
fn peer(fd: BorrowedFd<'_>) -> io::Result<Peer> {
    let (mut user, mut group) = (0, 0);
    // SAFETY: the kernel writes one `uid_t` and one `gid_t`, which live during the call.
    if unsafe { sys::getpeereid(fd.as_raw_fd(), &raw mut user, &raw mut group) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let (mut process, mut len): (c_int, u32) = (0, 4);
    // SAFETY: the kernel writes at most `len` bytes into `process`, which is a `pid_t` that lives
    // during the call, and the length into `len`.
    let result = unsafe {
        rsnetwork_udp::sys::getsockopt(
            fd.as_raw_fd(),
            sys::SOL_LOCAL,
            sys::LOCAL_PEERPID,
            (&raw mut process).cast(),
            &raw mut len,
        )
    };
    // The system has no process for a peer that closed its socket.
    let process = match result {
        0 => Some(process.cast_unsigned()),
        _ if last_errno() == sys::ENOTCONN => None,
        _ => return Err(io::Error::last_os_error()),
    };
    Ok(Peer { user, group, process })
}

/// The `struct sockaddr_un` of `path` in socket address storage, and its length with the NUL
/// byte. `None` for a path that is empty, holds a NUL byte, or has no room in the path array of
/// the address with its NUL byte: more than 107 bytes on Linux, or 103 on macOS. On Linux, an
/// empty path asks the kernel for an abstract name, which has no file and no permission.
#[expect(unsafe_code, reason = "the storage has room and alignment for the address")]
fn unix_address(path: &Path) -> Option<(SockAddrStorage, u32)> {
    let bytes = path.as_os_str().as_bytes();
    if bytes.is_empty() || bytes.contains(&0) {
        return None;
    }
    let mut address = sys::SockAddrUn {
        #[cfg(target_os = "macos")]
        sun_len: 0,
        sun_family: sys::AF_UNIX,
        sun_path: [0; _],
    };
    // The last byte of the path array stays for the NUL byte.
    let room = address.sun_path.len();
    address
        .sun_path
        .get_mut(..bytes.len())
        .filter(|path| path.len() < room)?
        .copy_from_slice(bytes);
    let len = core::mem::offset_of!(sys::SockAddrUn, sun_path) + bytes.len() + 1;
    #[cfg(target_os = "macos")]
    {
        address.sun_len = u8::try_from(len).ok()?;
    }
    let mut storage = SockAddrStorage::EMPTY;
    // SAFETY: the storage is 128 bytes with the alignment of a pointer, and the address is at most
    // 110 bytes with the alignment of its family. Each field of the storage is an integer, so each
    // byte of the address is a valid value of it.
    unsafe { ptr::write((&raw mut storage).cast::<sys::SockAddrUn>(), address) };
    Some((storage, u32::try_from(len).ok()?))
}

/// The error number of `error`.
fn errno(error: &io::Error) -> i32 {
    error.raw_os_error().unwrap_or(EINVAL)
}

/// The error number of the last call that failed on this thread.
fn last_errno() -> i32 {
    errno(&io::Error::last_os_error())
}

/// Takes the lock of `path`, then makes a socket that listens at `path`, whose address is
/// `address`. `stale` selects what happens when the path exists. Gives the socket and the lock.
#[expect(
    unsafe_code,
    reason = "the calls read the address and the path, which live during them"
)]
fn listen(
    path: &Path,
    (storage, len): (SockAddrStorage, u32),
    stale: Stale,
) -> Result<(OwnedFd, Option<OwnedFd>), i32> {
    let lock = lock(path)?;
    // With the lock, no listener of the driver lives at the path. The probe of a stale socket file
    // finds a live listener of another program.
    if stale == Stale::Replace {
        remove_stale(path, (storage, len))?;
    }
    let socket = tcp::new_socket(sys::AF_UNIX, SOCK_STREAM).map_err(|error| errno(&error))?;
    // Linux makes the socket file with the mode of the socket, less the umask, so the file never
    // has more permissions.
    // SAFETY: the call takes no pointer.
    #[cfg(target_os = "linux")]
    if unsafe { sys::fchmod(socket.as_raw_fd(), OWNER_ONLY.into()) } < 0 {
        return Err(last_errno());
    }
    // SAFETY: `storage` holds an address of `len` bytes during the call.
    if unsafe { rsnetwork_udp::sys::bind(socket.as_raw_fd(), ptr::from_ref(&storage).cast(), len) } < 0 {
        return Err(last_errno());
    }
    // macOS makes the socket file with mode 0777 less the umask, and refuses a mode of a socket.
    // A connect gets `ECONNREFUSED` until the listen, so the mode is 0600 before a connect can
    // succeed.
    #[cfg(target_os = "macos")]
    {
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| EINVAL)?;
        // SAFETY: `path` is a string with a NUL byte that lives during the call.
        if unsafe { sys::fchmodat(sys::AT_FDCWD, path.as_ptr(), OWNER_ONLY, sys::AT_SYMLINK_NOFOLLOW) } < 0 {
            return Err(last_errno());
        }
    }
    // SAFETY: the call takes no pointer.
    if unsafe { rsnetwork_udp::sys::listen(socket.as_raw_fd(), tcp::BACKLOG) } < 0 {
        return Err(last_errno());
    }
    Ok((socket, Some(lock)))
}

/// Connects a new socket to the listener whose address is `address`. A Unix-domain connect does
/// not wait in the kernel: it connects, or it fails at once.
#[expect(unsafe_code, reason = "the call reads the address, which lives during it")]
fn connect((storage, len): (SockAddrStorage, u32)) -> Result<OwnedFd, i32> {
    let socket = tcp::new_socket(sys::AF_UNIX, SOCK_STREAM).map_err(|error| errno(&error))?;
    // SAFETY: `storage` holds an address of `len` bytes during the call.
    if unsafe { sys::connect(socket.as_raw_fd(), ptr::from_ref(&storage).cast(), len) } < 0 {
        return Err(last_errno());
    }
    Ok(socket)
}

/// The lock file of the listener at `path`: the path with `.lock` after it.
fn lock_path(path: &Path) -> PathBuf {
    let mut lock = OsString::from(path.as_os_str());
    lock.push(".lock");
    PathBuf::from(lock)
}

/// Takes the lock of the listener at `path`, in its lock file, which has mode 0600 from the
/// moment it exists. Gives `EADDRINUSE` while another listener holds the lock. A lock file that is
/// a symbolic link gives `ELOOP`.
#[expect(unsafe_code, reason = "the call takes no pointer")]
fn lock(path: &Path) -> Result<OwnedFd, i32> {
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .mode(OWNER_ONLY.into())
        .custom_flags(sys::O_NOFOLLOW)
        .open(lock_path(path))
        .map_err(|error| errno(&error))?;
    // SAFETY: the call takes no pointer.
    if unsafe { sys::flock(file.as_raw_fd(), sys::LOCK_EX | sys::LOCK_NB) } < 0 {
        return Err(match last_errno() {
            EAGAIN => EADDRINUSE,
            errno => errno,
        });
    }
    Ok(file.into())
}

/// Whether a listener holds the lock of `path`. `F_GETLK` reads the lock and takes none, so a
/// bind of the path at the same time does not fail. macOS keeps the locks of `flock` and of
/// `fcntl` in one list, so `F_GETLK` sees the lock of a listener.
#[cfg(target_os = "macos")]
#[expect(
    unsafe_code,
    reason = "the call reads and writes one `struct flock` that lives during it"
)]
fn locked(path: &Path) -> bool {
    let Ok(file) = fs::OpenOptions::new()
        .read(true)
        .custom_flags(sys::O_NOFOLLOW)
        .open(lock_path(path))
    else {
        return false;
    };
    let mut lock = sys::Flock {
        l_start: 0,
        l_len: 0,
        l_pid: 0,
        l_type: sys::F_WRLCK,
        l_whence: 0,
    };
    // SAFETY: `F_GETLK` reads and writes one `struct flock`, which lives during the call.
    unsafe { sys::fcntl(file.as_raw_fd(), sys::F_GETLK, &raw mut lock) == 0 && lock.l_type != sys::F_UNLCK }
}

/// Removes the socket file at `path` when it is stale: a socket file that the user of the process
/// owns, to which a connect is refused. The caller holds the lock of the path, so the connect
/// finds only a listener of another program. A path with no file, or a file that is not a socket,
/// changes nothing, so the bind then binds or gives `EADDRINUSE`. Gives the error number of a live
/// listener, of a socket file of another user, or of a failed call.
#[expect(unsafe_code, reason = "the calls read only the address, which lives during them")]
fn remove_stale(path: &Path, (storage, len): (SockAddrStorage, u32)) -> Result<(), i32> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(errno(&error)),
    };
    if !metadata.file_type().is_socket() {
        return Ok(());
    }
    // SAFETY: the call takes no pointer.
    if metadata.uid() != unsafe { sys::geteuid() } {
        return Err(EPERM);
    }
    let probe = tcp::new_socket(sys::AF_UNIX, SOCK_STREAM).map_err(|error| errno(&error))?;
    // SAFETY: `storage` holds an address of `len` bytes during the call. The socket is nonblocking,
    // so the call does not wait for a listener with a full backlog.
    let connected = unsafe { sys::connect(probe.as_raw_fd(), ptr::from_ref(&storage).cast(), len) } == 0;
    match io::Error::last_os_error().raw_os_error() {
        _ if connected => Err(EADDRINUSE),
        Some(ECONNREFUSED) => match fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(errno(&error)),
            _ => Ok(()),
        },
        Some(ENOENT) => Ok(()),
        Some(EAGAIN) => Err(EADDRINUSE),
        error => Err(error.unwrap_or(EINVAL)),
    }
}

// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
pub(super) mod tests;
