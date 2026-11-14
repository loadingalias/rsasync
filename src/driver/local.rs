//! Local streams on the driver: Unix-domain stream sockets at a path, under the stream rules of
//! TCP.
//!
//! A local stream has the directions of a TCP stream ([`ReadHalf`] and [`WriteHalf`]), so the
//! receive, send, vectored send, and half-close operations of [`TcpStream`](super::TcpStream)
//! serve it, with the same rules for a dropped, forgotten, or cancelled operation. A local
//! listener has the rules of a [`TcpListener`](super::TcpListener).

use core::ptr;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
use std::path::Path;

use rsnetwork_udp::SockAddrStorage;

use super::tcp::{self, Closed, ReadHalf, WriteHalf};
use super::{Driver, Operation, Refused, Resource, sys};

/// `EPERM` (`include/uapi/asm-generic/errno-base.h`).
const EPERM: i32 = 1;
/// `ENOENT`.
const ENOENT: i32 = 2;
/// `EAGAIN`.
const EAGAIN: i32 = 11;
/// `EINVAL`.
const EINVAL: i32 = 22;
/// `EADDRINUSE` (`include/uapi/asm-generic/errno.h`).
const EADDRINUSE: i32 = 98;
/// `ECONNREFUSED`.
const ECONNREFUSED: i32 = 111;

/// The mode of the socket file of a listener: read and write for its owner only.
const OWNER_ONLY: u32 = 0o600;

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
/// assert_eq!(driver.local_peer(&server)?.process(), std::process::id());
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
    /// owns, to which a connect is refused. A live listener, or a file that is not a socket, gives
    /// `AddrInUse`, and a socket file of another user gives `PermissionDenied`. The driver removes
    /// no other file.
    Replace,
}

/// The OS identity of the peer of a local stream, which the kernel recorded when the stream
/// connected (`SO_PEERCRED`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    user: u32,
    group: u32,
    process: u32,
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

    /// The process ID of the peer.
    #[must_use]
    pub const fn process(self) -> u32 {
        self.process
    }
}

impl Driver {
    /// Binds a Unix-domain stream socket to the socket file at `path`, and listens on it. The
    /// socket file has mode 0600 from the moment it exists, so only its owner can connect. The
    /// umask of the process can only remove more permissions. `stale` selects what the bind does
    /// when the path exists. The future gives the listener.
    ///
    /// Where the kernel has `IORING_OP_BIND` and `IORING_OP_LISTEN` (Linux 6.11), the ring runs
    /// the bind and the listen. On an earlier kernel, the driver makes the system calls, and the
    /// future is ready at once. The check of a stale path is a set of system calls before the
    /// bind.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives `InvalidInput`
    /// for a path that is empty, holds a NUL byte, or has more than 107 bytes, `AddrInUse` for a
    /// path that exists, as [`Stale`] tells, another error of the system, or `OutOfMemory` when
    /// each resource slot of the driver is in use.
    pub fn bind_local(
        &mut self,
        path: &Path,
        stale: Stale,
    ) -> Result<Operation<'static, io::Result<LocalListener>>, Refused<()>> {
        let address = unix_address(path).ok_or(EINVAL).and_then(|address| {
            if stale == Stale::Replace {
                remove_stale(path, address)?;
            }
            Ok(address)
        });
        let completion = match address {
            Ok(address) => self.bind_socket(address, true, owner_only)?,
            Err(errno) => self.fail_now(errno)?,
        };
        Ok(Operation::new(completion, |result, held| {
            Some(tcp::count(result).and_then(|_| {
                let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
                Ok(LocalListener { resource })
            }))
        }))
    }

    /// Connects a new Unix-domain stream socket to the listener at `path`. The future gives the
    /// stream. A drop of the future asks for a cancel, and the driver closes a connection that
    /// completes after the drop, so its peer reads the end of the stream.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives `InvalidInput`
    /// for a path as for [`Driver::bind_local`], `NotFound` for a path with no file,
    /// `ConnectionRefused` for a socket file with no listener, `PermissionDenied` for a socket file
    /// that the user of the process cannot write, for example one of another user, another error
    /// of the system, or `OutOfMemory` when each resource slot of the driver is in use.
    pub fn connect_local(&mut self, path: &Path) -> Result<Operation<'static, io::Result<LocalStream>>, Refused<()>> {
        let completion = match unix_address(path) {
            Some(address) => self.connect_socket(address, |_| Ok(()))?,
            None => self.fail_now(EINVAL)?,
        };
        Ok(Operation::new(completion, |result, held| {
            Some(tcp::count(result).and_then(|_| {
                let resource = held.accepted.ok_or(io::ErrorKind::OutOfMemory)?;
                Ok(LocalStream::new(resource))
            }))
        }))
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
    /// connected, and for a connected stream, the process that listened. The kernel recorded it at
    /// the connect, so it does not change when the peer changes its user later.
    ///
    /// # Errors
    ///
    /// An error of the system.
    #[expect(
        unsafe_code,
        reason = "the kernel writes one `struct ucred` that lives during the call"
    )]
    pub fn local_peer(&self, stream: &LocalStream) -> io::Result<Peer> {
        let fd = self
            .resources
            .borrow(stream.read.resource)
            .ok_or(io::ErrorKind::NotFound)?;
        let (mut credentials, mut len) = (sys::Ucred::default(), sys::UCRED_LEN);
        // SAFETY: the kernel writes at most `len` bytes into `credentials`, which is a
        // `struct ucred` that lives during the call, and the length into `len`.
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
        Ok(Peer {
            user: credentials.uid,
            group: credentials.gid,
            process: credentials.pid.cast_unsigned(),
        })
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
}

/// The `struct sockaddr_un` of `path` in socket address storage, and its length with the NUL
/// byte. `None` for a path that is empty, holds a NUL byte, or has more than 107 bytes: an empty
/// path asks the kernel for an abstract name, which has no file and no permission.
#[expect(unsafe_code, reason = "the storage has room and alignment for the address")]
fn unix_address(path: &Path) -> Option<(SockAddrStorage, u32)> {
    let bytes = path.as_os_str().as_bytes();
    if bytes.is_empty() || bytes.contains(&0) {
        return None;
    }
    let mut address = sys::SockAddrUn {
        sun_family: sys::AF_UNIX,
        sun_path: [0; 108],
    };
    // The last byte of the path array stays for the NUL byte.
    address
        .sun_path
        .get_mut(..bytes.len())
        .filter(|path| path.len() < 108)?
        .copy_from_slice(bytes);
    let mut storage = SockAddrStorage::EMPTY;
    // SAFETY: the storage is 128 bytes with the alignment of a pointer, and the address is 110 bytes
    // with the alignment of a `u16`. Each field of the storage is an integer, so each byte of the
    // address is a valid value of it.
    unsafe { ptr::write((&raw mut storage).cast::<sys::SockAddrUn>(), address) };
    let len = core::mem::offset_of!(sys::SockAddrUn, sun_path) + bytes.len() + 1;
    Some((storage, u32::try_from(len).ok()?))
}

/// Gives the new socket of a listener mode 0600 before its bind. Linux makes the socket file with
/// the mode of the socket, less the umask, so the file never has more permissions.
#[expect(unsafe_code, reason = "the call takes no pointer")]
fn owner_only(socket: BorrowedFd<'_>) -> io::Result<()> {
    // SAFETY: the call takes no pointer.
    if unsafe { sys::fchmod(socket.as_raw_fd(), OWNER_ONLY) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Removes the socket file at `path` when it is stale: a socket file that the user of the process
/// owns, to which a connect is refused. A path with no file, or a file that is not a socket,
/// changes nothing, so the bind then binds or gives `EADDRINUSE`. Gives the error number of a live
/// listener, of a socket file of another user, or of a failed call.
#[expect(unsafe_code, reason = "the calls read only the address, which lives during them")]
fn remove_stale(path: &Path, (storage, len): (SockAddrStorage, u32)) -> Result<(), i32> {
    let errno = |error: &io::Error| error.raw_os_error().unwrap_or(EINVAL);
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
    let probe = tcp::new_socket(sys::AF_UNIX, rsnetwork_udp::sys::SOCK_STREAM).map_err(|error| errno(&error))?;
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
