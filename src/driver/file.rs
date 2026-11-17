//! Files on the driver: open with an explicit cache mode, positional and vectored reads and
//! writes, an exact read, length, space, sync, rename, unlink, directories, and close.
//!
//! Each operation of a file is positional, so several operations of one file run at one time, and
//! the close of the file waits until each of them ends. Each operation keeps its buffer until its
//! terminal event, also after a drop of its future. A dropped write is not cancelled: it can still
//! change the file.

use alloc::vec::Vec;
use core::ffi::{c_int, c_void};
use core::fmt;
use core::task::{Context, Poll};
use std::ffi::CString;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;

use rsnetwork_udp::sys::IoVec;

use super::tcp::{Regions, count};
// The open flags and the structures of the file calls: the declarations of the driver on Linux,
// and those of the poller on macOS, where only the file jobs of the poller make file calls.
#[cfg(target_os = "macos")]
use super::poller::sys;
#[cfg(target_os = "linux")]
use super::sys;
use super::{
    Call, Completion, Driver, ECANCELED, Held, Kind, Medium, Operation, Payload, Reason, Refused, Resource, Resources,
    Route, Tag, poller,
};
use crate::blocking::{Job, Submitter};
use crate::buffer::{Backing, Buffer as _, Lease, WriteLoan};
use crate::sync::Arc;
use crate::uring::Sqe;
use crate::uring::sys::{FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, RWF_DSYNC};

/// The empty path of a call on the descriptor itself.
#[cfg(target_os = "linux")]
const EMPTY: &core::ffi::CStr = c"";

/// `EIO` (`include/uapi/asm-generic/errno-base.h` of Linux, and `sys/errno.h` of macOS), for a job
/// of the blocking service that gives no error number.
const EIO: i32 = 5;
/// `EINVAL`, for options or a path that no call can take.
const EINVAL: i32 = 22;

/// The alignment of direct I/O when the file system does not report it: one page.
#[cfg(target_os = "linux")]
const PAGE: Alignment = Alignment {
    memory: 4096,
    offset: 4096,
};

/// The structure that the length inspection of a file fills: `struct statx` on Linux, and
/// `struct stat` on macOS.
#[cfg(target_os = "linux")]
pub(crate) type Stat = sys::Statx;
#[cfg(target_os = "macos")]
pub(crate) type Stat = sys::Stat;

/// The file operation of [`Kind::File`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    /// An open of the path of [`Payload::Open`]: the result is a new descriptor, which becomes a
    /// resource. A direct-I/O open also reads the alignment of the file.
    Open { direct: bool },
    /// A read into the loan.
    Read,
    /// A vectored read into the buffers.
    ReadVectored,
    /// An operation that gives only its result: a write, a sync, a change of length or space, a
    /// `statx`, a rename, or an unlink.
    Plain,
    /// A preallocation that extends the length, on macOS ([`Length::Extend`]): it runs alone on its
    /// file, because its length change is a second call.
    #[cfg(target_os = "macos")]
    Extend,
}

/// A file operation that waits on macOS until it can run, because an allocation that extends the
/// length runs alone on its file ([`Driver::allocate`]): its operation, its file, and its job.
#[cfg(target_os = "macos")]
pub(super) struct Waiting {
    id: super::OpId,
    resource: Resource,
    op: Op,
    job: FileJob,
}

#[cfg(target_os = "macos")]
impl Waiting {
    /// The operation that waits.
    pub(super) const fn id(&self) -> super::OpId {
        self.id
    }
}

/// A file that the driver owns.
///
/// Its operations borrow it shared, so several of them run at one time, each at its own offset.
/// [`Driver::close_file`] takes it, and waits until each operation of it ends, also each operation
/// whose future dropped.
///
/// A file that is made, written, made durable, read, closed, and removed, with `run` as in
/// [`Operation`]:
///
/// ```
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
/// use core::num::NonZeroU64;
///
/// use rsasync::blocking::{self, Service};
/// use rsasync::buffer::{Backing, Buffer as _, Lease};
/// use rsasync::capacity::Budget;
/// use rsasync::driver::{Config, Driver, OpenOptions, SyncMode, WriteMode};
///
/// let mut driver = Driver::new(Config::new())?;
/// // On epoll and kqueue, each file call is a job of a blocking service.
/// let budget = Budget::new(NonZeroU64::new(16 << 20).unwrap());
/// let service = Service::new(&budget, blocking::Config::new())?;
/// driver.use_blocking(service.submitter());
/// let path = std::env::temp_dir().join(format!("rsasync-file-{}", std::process::id()));
///
/// let options = OpenOptions::new().read(true).write(true).create(true);
/// let open = driver.open_file(&path, options)?;
/// let file = run(&mut driver, open)???;
/// let write = driver.write_at(&file, Lease::from(b"hello".to_vec()), 0, WriteMode::Plain)?;
/// let (written, _lease) = run(&mut driver, write)??;
/// assert_eq!(written?, 5);
/// // The written bytes become durable.
/// let sync = driver.sync(&file, SyncMode::Data)?;
/// run(&mut driver, sync)???;
///
/// // A read gives the exact count, which is fewer at the end of the file.
/// let read = driver.read_at(&file, Backing::from(Vec::with_capacity(64)), 0)?;
/// let (read, buffer) = run(&mut driver, read)??;
/// assert_eq!((read?, buffer.bytes()), (5, &b"hello"[..]));
///
/// let close = driver.close_file(file)?;
/// run(&mut driver, close)??;
/// let unlink = driver.unlink(&path)?;
/// run(&mut driver, unlink)???;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct File {
    resource: Resource,
    alignment: Option<Alignment>,
}

impl File {
    /// The alignment of direct I/O, for a file that [`Cache::Direct`] opened. `None` for a file
    /// that uses the page cache.
    #[must_use]
    pub const fn alignment(&self) -> Option<Alignment> {
        self.alignment
    }

    /// Whether a transfer of `len` bytes at `address` and the file offset `offset` has the
    /// alignment of the file.
    fn allows(&self, address: usize, len: usize, offset: u64) -> bool {
        self.alignment.is_none_or(|alignment| {
            address.is_multiple_of(alignment.memory as usize)
                && len.is_multiple_of(alignment.offset as usize)
                && offset.is_multiple_of(u64::from(alignment.offset))
        })
    }
}

/// A directory that the driver owns, for the sync that makes a create, a rename, or an unlink in
/// it durable.
#[derive(Debug)]
pub struct Directory {
    resource: Resource,
}

/// The alignment that direct I/O needs, from `statx` (`STATX_DIOALIGN`, Linux 6.1).
///
/// A file system that does not report it gets 4096 bytes for both values. On macOS, both values
/// are one page: 16 KiB on Apple silicon.
///
/// A direct-I/O read into a buffer of a pool whose slots have the alignment, with `run` as in
/// [`Operation`]:
///
/// ```
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
/// use core::alloc::Layout;
/// use core::num::{NonZeroU32, NonZeroU64};
///
/// use rsasync::buffer::{Buffer as _, Pool};
/// use rsasync::capacity::Budget;
/// use rsasync::driver::{Cache, Config, Driver, OpenOptions};
///
/// let mut driver = Driver::new(Config::new())?;
/// # let budget = Budget::new(NonZeroU64::new(16 << 20).unwrap());
/// # let service = rsasync::blocking::Service::new(&budget, rsasync::blocking::Config::new())?;
/// # driver.use_blocking(service.submitter());
/// let path = std::env::temp_dir().join(format!("rsasync-direct-{}", std::process::id()));
/// std::fs::write(&path, vec![7; 16384])?;
///
/// let open = driver.open_file(&path, OpenOptions::new().cache(Cache::Direct))?;
/// let file = run(&mut driver, open)???;
/// let alignment = file.alignment().expect("a direct-I/O file has its alignment");
/// // Each slot starts at the memory alignment, and its size is a multiple of the offset
/// // alignment.
/// let align = alignment.memory().max(alignment.offset()) as usize;
/// let slot = Layout::from_size_align(align.max(4096), align)?;
/// let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
/// let mut pool = Pool::new(&budget, NonZeroU32::MIN, slot)?;
/// let read = driver.read_at(&file, pool.acquire()?, 0)?;
/// let (read, buffer) = run(&mut driver, read)??;
/// assert_eq!(read?, slot.size());
/// assert!(buffer.bytes().iter().all(|&byte| byte == 7));
/// # let close = driver.close_file(file)?;
/// # run(&mut driver, close)??;
/// # std::fs::remove_file(&path)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Alignment {
    memory: u32,
    offset: u32,
}

impl Alignment {
    /// The bytes to which the address of each buffer is aligned.
    #[must_use]
    pub const fn memory(self) -> u32 {
        self.memory
    }

    /// The bytes to which each file offset and each length is aligned.
    #[must_use]
    pub const fn offset(self) -> u32 {
        self.offset
    }
}

/// How the reads and writes of a file use the page cache.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Cache {
    /// Through the page cache.
    #[default]
    Buffered,
    /// Between the device and the buffers of the caller, with no copy in the page cache
    /// (`O_DIRECT`, and `F_NOCACHE` after the open on macOS). Each buffer, length, and offset has
    /// the [`Alignment`] of the file.
    Direct,
}

/// How [`Driver::open_file`] opens a file. The defaults: read only, through the page cache, and mode
/// `0o666` (less the umask) for a file that the open makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    create: bool,
    create_new: bool,
    truncate: bool,
    mode: u32,
    cache: Cache,
}

impl OpenOptions {
    /// Read only, through the page cache, and mode `0o666`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            read: true,
            write: false,
            create: false,
            create_new: false,
            truncate: false,
            mode: 0o666,
            cache: Cache::Buffered,
        }
    }

    /// Opens for reads.
    #[must_use]
    pub const fn read(self, read: bool) -> Self {
        Self { read, ..self }
    }

    /// Opens for writes.
    #[must_use]
    pub const fn write(self, write: bool) -> Self {
        Self { write, ..self }
    }

    /// Makes the file if it does not exist. It needs writes.
    #[must_use]
    pub const fn create(self, create: bool) -> Self {
        Self { create, ..self }
    }

    /// Makes the file, and fails if it exists. It needs writes.
    #[must_use]
    pub const fn create_new(self, create_new: bool) -> Self {
        Self { create_new, ..self }
    }

    /// Sets the length of an existing file to 0. It needs writes.
    #[must_use]
    pub const fn truncate(self, truncate: bool) -> Self {
        Self { truncate, ..self }
    }

    /// The mode of a file that the open makes, less the umask.
    #[must_use]
    pub const fn mode(self, mode: u32) -> Self {
        Self { mode, ..self }
    }

    /// How the reads and writes use the page cache.
    #[must_use]
    pub const fn cache(self, cache: Cache) -> Self {
        Self { cache, ..self }
    }

    /// The flags of `openat`, or `None` for options that no open can take, as for
    /// `std::fs::OpenOptions`.
    fn flags(self) -> Option<c_int> {
        let access = match (self.read, self.write) {
            (true, false) => sys::O_RDONLY,
            (false, true) => sys::O_WRONLY,
            (true, true) => sys::O_RDWR,
            (false, false) => return None,
        };
        if !self.write && (self.create || self.create_new || self.truncate) {
            return None;
        }
        let mut flags = access | sys::O_CLOEXEC;
        if self.create_new {
            flags |= sys::O_CREAT | sys::O_EXCL;
        } else if self.create {
            flags |= sys::O_CREAT;
        }
        if self.truncate {
            flags |= sys::O_TRUNC;
        }
        // macOS has no such flag: the open turns the cache of the file off after the call.
        #[cfg(target_os = "linux")]
        if self.cache == Cache::Direct {
            flags |= sys::O_DIRECT;
        }
        Some(flags)
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// What [`Driver::sync`] makes durable. On macOS, each mode is `F_FULLFSYNC`, which also writes the
/// cache of the device to its media: a plain `fsync` of macOS leaves the bytes in that cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// The contents, and the metadata that a later read needs, such as the length (`fdatasync`).
    Data,
    /// The contents and all metadata (`fsync`).
    All,
}

/// How a write is durable.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// The write ends when the kernel has the bytes. A later [`Driver::sync`] makes them durable.
    #[default]
    Plain,
    /// The write ends when its bytes are durable, as with a `fdatasync` after it (`RWF_DSYNC`). On
    /// macOS, the job of the write makes an `F_FULLFSYNC` after it.
    DataSync,
}

impl WriteMode {
    const fn flags(self) -> u32 {
        match self {
            Self::Plain => 0,
            Self::DataSync => RWF_DSYNC,
        }
    }
}

/// Whether [`Driver::allocate`] changes the length of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Length {
    /// The length stays the same, also when the range ends after it (`FALLOC_FL_KEEP_SIZE`).
    Keep,
    /// The length becomes the end of the range when the range ends after it.
    Extend,
}

/// An exact read that ended before the end of its buffer: at the end of the file, or on a file
/// system that gives short reads. The buffer holds the bytes that the read put in. Continue at
/// the offset of the read plus [`read`](Self::read).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Incomplete {
    read: usize,
}

impl Incomplete {
    /// The bytes that the read put in the buffer.
    #[must_use]
    pub const fn read(self) -> usize {
        self.read
    }
}

impl fmt::Display for Incomplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the read ended after {} bytes, before the end of its buffer",
            self.read
        )
    }
}

impl core::error::Error for Incomplete {}

/// The buffers of a vectored read, in order: the read fills each one after its initialized bytes,
/// up to its capacity, before the next one.
#[derive(Debug)]
pub struct Buffers {
    loans: Vec<WriteLoan<Backing>>,
    /// The iovec array of the read, whose capacity the pushes reserve.
    vectors: Vec<IoVec>,
}

#[expect(unsafe_code, reason = "the array holds only addresses of memory that the loans own")]
#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "the raw pointers name only the loans of this value"
)]
// SAFETY: `vectors` holds only addresses of the capacity of `loans`, which this value owns, and
// only the kernel writes through them, during a read whose resources the home keeps. A move to
// another thread moves that memory with them, and a backing is `Send`.
unsafe impl Send for Buffers {}

impl Buffers {
    /// Makes an empty set with room for `buffers` buffers.
    #[must_use]
    pub fn with_capacity(buffers: usize) -> Self {
        Self {
            loans: Vec::with_capacity(buffers),
            vectors: Vec::with_capacity(buffers),
        }
    }

    /// Adds `buffer` after the others.
    pub fn push(&mut self, buffer: Backing) {
        self.loans.push(WriteLoan::new(buffer));
        self.vectors
            .reserve(self.loans.len().saturating_sub(self.vectors.len()));
    }

    /// The buffers, in order, each with the bytes that reads put in.
    #[must_use]
    pub fn into_buffers(self) -> Vec<Backing> {
        self.loans.into_iter().map(WriteLoan::into_inner).collect()
    }

    /// The iovec array of the capacity of each buffer, and its length. Its capacity holds the
    /// array, so it allocates nothing.
    fn vectors(&mut self) -> (*const IoVec, u32) {
        self.vectors.clear();
        self.vectors.extend(self.loans.iter_mut().map(|loan| IoVec {
            iov_base: loan.as_mut_ptr().cast::<c_void>(),
            iov_len: loan.capacity(),
        }));
        (
            self.vectors.as_ptr(),
            u32::try_from(self.vectors.len()).unwrap_or(u32::MAX),
        )
    }

    /// Records that a read filled the first `count` bytes of the capacity, in order.
    #[expect(unsafe_code, reason = "the kernel initialized the bytes that the result counts")]
    fn fill(&mut self, count: usize) {
        let mut rest = count;
        for loan in &mut self.loans {
            let filled = rest.min(loan.capacity());
            // SAFETY: the read named the capacity of each loan in order, and the kernel wrote
            // `count` bytes from the start, so it initialized the first `filled` bytes of this one.
            unsafe { loan.set_filled(filled) };
            rest -= filled;
        }
    }

    /// Whether each region of the read has the alignment of `file` at `offset`.
    fn aligned(&mut self, file: &File, offset: u64) -> bool {
        file.alignment.is_none()
            || self
                .loans
                .iter_mut()
                .all(|loan| file.allows(loan.as_mut_ptr().addr(), loan.capacity(), offset))
    }
}

/// The home finishes the file operation `op` of `held` at its terminal event, whose result is
/// `result`: an open makes its descriptor a resource, and a read records the bytes that it put in.
#[expect(
    unsafe_code,
    reason = "the kernel gave the descriptor, and initialized the bytes that the result counts"
)]
pub(super) fn finish(held: &mut Held, op: Op, result: i32, abandoned: bool, resources: &mut Resources) {
    let Ok(count) = usize::try_from(result) else {
        return;
    };
    match op {
        Op::Open { direct } => {
            // SAFETY: the kernel gave a new descriptor, which only this operation names.
            let fd = unsafe { OwnedFd::from_raw_fd(result) };
            if abandoned {
                // No caller takes the file, so the drop closes it.
                return;
            }
            let alignment = match direct.then(|| alignment(&fd)).transpose() {
                Ok(alignment) => alignment,
                Err(kind) => {
                    held.error = Some(kind);
                    return;
                }
            };
            match resources.adopt(fd, Medium::File) {
                Ok(resource) => {
                    held.accepted = Some(resource);
                    if let Payload::Open { alignment: field, .. } = &mut held.payload {
                        *field = alignment;
                    }
                }
                Err((_, _closed)) => held.error = Some(io::ErrorKind::OutOfMemory),
            }
        }
        Op::Read => {
            if let Some(loan) = held.loan.as_mut() {
                // SAFETY: the read named the capacity of the loan, and the kernel wrote `count`
                // bytes at its start, at most that capacity.
                unsafe { loan.set_filled(count) };
            }
        }
        Op::ReadVectored => {
            if let Payload::Buffers(buffers) = &mut held.payload {
                buffers.fill(count);
            }
        }
        Op::Plain => {}
        #[cfg(target_os = "macos")]
        Op::Extend => {}
    }
}

/// An operation of `tag` reached its terminal event: a file counts one running operation less,
/// and its close goes on when no other runs.
pub(super) fn ended(resources: &mut Resources, pending: &mut Vec<Resource>, tag: Tag) {
    if !matches!(tag.kind, Kind::File(_)) {
        return;
    }
    let Some(owned) = tag.resource.and_then(|resource| resources.get(resource)) else {
        return;
    };
    // Each start of an operation of the file counted it, and this is its one terminal event.
    owned.running -= 1;
    super::tcp::after(resources, pending, tag);
}

/// The alignment of direct I/O of the open file `fd`. It keeps the attributes that the kernel
/// has, so it never waits for a remote server.
#[cfg(target_os = "linux")]
#[expect(unsafe_code, reason = "the call writes only the structure")]
fn alignment(fd: &OwnedFd) -> Result<Alignment, io::ErrorKind> {
    let mut stat = sys::Statx::default();
    // SAFETY: the path is an empty C string, the flags name the descriptor itself, and the call
    // writes only `stat`, which is a `struct statx`.
    let result = unsafe {
        sys::syscall(
            sys::SYS_STATX,
            fd.as_raw_fd(),
            EMPTY.as_ptr(),
            sys::AT_EMPTY_PATH | sys::AT_STATX_DONT_SYNC,
            sys::STATX_DIOALIGN,
            &raw mut stat,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error().kind());
    }
    if stat.mask & sys::STATX_DIOALIGN == 0 {
        return Ok(PAGE);
    }
    // A file system with no direct I/O for the file reports 0.
    if stat.dio_mem_align == 0 || stat.dio_offset_align == 0 {
        return Err(io::ErrorKind::Unsupported);
    }
    Ok(Alignment {
        memory: stat.dio_mem_align,
        offset: stat.dio_offset_align,
    })
}

/// Turns the data cache of the open file `fd` off (`F_NOCACHE`), and gives the alignment of its
/// direct I/O: one page, as `sysconf` gives it. xnu moves a transfer whose address, offset, and
/// length are multiples of a page between the device and the buffer, with no copy in the cache.
#[cfg(target_os = "macos")]
#[expect(
    unsafe_code,
    reason = "the command takes one `int` argument, and `sysconf` takes no pointer"
)]
fn alignment(fd: &OwnedFd) -> Result<Alignment, io::ErrorKind> {
    // SAFETY: the command takes one `int` argument.
    if unsafe { sys::fcntl(fd.as_raw_fd(), sys::F_NOCACHE, 1_i32) } < 0 {
        return Err(io::Error::last_os_error().kind());
    }
    // SAFETY: the call takes no pointer.
    let page = u32::try_from(unsafe { sys::sysconf(sys::_SC_PAGESIZE) }).map_err(|_| io::ErrorKind::Unsupported)?;
    Ok(Alignment {
        memory: page,
        offset: page,
    })
}

/// The length of a file in the structure of its inspection.
#[cfg(target_os = "linux")]
fn length(stat: &Stat) -> io::Result<u64> {
    if stat.mask & sys::STATX_SIZE == 0 {
        return Err(io::ErrorKind::Unsupported.into());
    }
    Ok(stat.size)
}

/// The length of a file in the structure of its inspection.
#[cfg(target_os = "macos")]
fn length(stat: &Stat) -> io::Result<u64> {
    u64::try_from(stat.st_size).map_err(|_| io::ErrorKind::InvalidData.into())
}

/// The C string of `path`, or `None` when it holds a NUL byte.
fn c_path(path: &Path) -> Option<CString> {
    CString::new(path.as_os_str().as_bytes()).ok()
}

/// The result of an operation that gives no value, or the error of the home.
fn done(result: i32, held: &Held) -> io::Result<()> {
    if let Some(kind) = held.error {
        return Err(kind.into());
    }
    count(result).map(|_| ())
}

/// The resource and the alignment of a file that an open gave.
fn opened(result: i32, held: &Held) -> io::Result<(Resource, Option<Alignment>)> {
    count(result)?;
    if let Some(kind) = held.error {
        return Err(kind.into());
    }
    let alignment = match held.payload {
        Payload::Open { alignment, .. } => alignment,
        _ => None,
    };
    held.accepted
        .map(|resource| (resource, alignment))
        .ok_or_else(|| io::ErrorKind::OutOfMemory.into())
}

/// The length of the submission of a transfer of `len` bytes. Linux moves at most about 2 GiB in
/// one call, and the rest of the bytes stay for the next one.
fn submitted(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// Polls the job of an operation, for [`Driver::poll_jobs`].
fn poll_job(held: Option<&mut Held>, cx: &Context<'_>) -> Option<i32> {
    let Payload::Job(job) = &mut held?.payload else {
        return None;
    };
    match job.poll(cx) {
        Poll::Ready(result) => Some(result.unwrap_or(-ECANCELED)),
        Poll::Pending => None,
    }
}

/// Whether the file operation `op` can start on the file of `owned` on macOS, with `waiting` of its
/// operations that wait: no allocation that extends the file runs, and such an allocation starts
/// only when no other operation of the file runs.
#[cfg(target_os = "macos")]
fn free(owned: &super::Owned, table: &crate::table::Table<Tag>, op: Op, waiting: usize) -> bool {
    let exclusive = owned.exclusive.is_some_and(|id| table.runs(id));
    let others = (owned.running as usize).saturating_sub(waiting);
    !exclusive && (op != Op::Extend || others == 0)
}

/// The job of a file entry on the epoll and kqueue backends: its entry, and the duplicate of its
/// descriptor, which stays open until the job returns.
struct FileJob {
    sqe: Sqe,
    _fd: Option<OwnedFd>,
}

impl Job for FileJob {
    type Output = i32;

    fn run(self) -> i32 {
        poller::run_file(&self.sqe)
    }
}

/// The waker of the jobs of the blocking service: it wakes the driver, also in a wait.
struct JobWaker(super::WakeHandle);

impl alloc::task::Wake for JobWaker {
    fn wake(self: alloc::sync::Arc<Self>) {
        self.0.wake();
    }

    fn wake_by_ref(self: &alloc::sync::Arc<Self>) {
        self.0.wake();
    }
}

impl Driver {
    /// Gives the driver the blocking service of `submitter`, for the operations that the kernel
    /// has no ring operation for ([`Route::BlockingService`]). The driver keeps the submitter, and
    /// each such operation is one job of the service.
    pub fn use_blocking(&mut self, submitter: Submitter) {
        let waker = core::task::Waker::from(alloc::sync::Arc::new(JobWaker(self.wake_handle())));
        self.blocking = Some((submitter, waker));
    }

    /// Opens the file at `path` with `options` (`IORING_OP_OPENAT`). The descriptor closes on
    /// `exec`. A [`Cache::Direct`] open also reads the [`Alignment`] of the file. An open that
    /// creates or truncates changes the file system, so a drop of its future does not cancel it:
    /// it runs to its end. A drop of the future of another open asks for a cancel. The driver
    /// closes a file that an open gives after a drop of its future.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system, `InvalidInput` for options that no open can take or a path with a NUL byte,
    /// `Unsupported` for direct I/O on a file system without it, or `OutOfMemory` when each
    /// resource slot of the driver is in use.
    pub fn open_file(
        &mut self,
        path: &Path,
        options: OpenOptions,
    ) -> Result<Operation<'static, io::Result<File>>, Refused<()>> {
        let op = Op::Open {
            direct: options.cache == Cache::Direct,
        };
        // An open that changes the file system runs to its end.
        let changes = options.create || options.create_new || options.truncate;
        let completion = self.begin_open(path, options.flags(), options.mode, op, !changes)?;
        Ok(Operation::new(completion, |result, held| {
            Some(opened(result, &held).map(|(resource, alignment)| File { resource, alignment }))
        }))
    }

    /// Opens the directory at `path` (`O_DIRECTORY`), for [`Driver::sync_directory`]. The open
    /// changes nothing, so a drop of its future asks for a cancel.
    ///
    /// # Errors
    ///
    /// As for [`Driver::open_file`]: the future gives `NotADirectory` for a path that is not a
    /// directory.
    pub fn open_directory(&mut self, path: &Path) -> Result<Operation<'static, io::Result<Directory>>, Refused<()>> {
        let flags = sys::O_RDONLY | sys::O_DIRECTORY | sys::O_CLOEXEC;
        let completion = self.begin_open(path, Some(flags), 0, Op::Open { direct: false }, true)?;
        Ok(Operation::new(completion, |result, held| {
            Some(opened(result, &held).map(|(resource, _)| Directory { resource }))
        }))
    }

    /// Reads into the capacity of `buffer` after its initialized bytes, from the file offset
    /// `offset` (`IORING_OP_READ`). The future gives the bytes that the read put in, which are
    /// fewer at the end of the file, and 0 at or after it, and the buffer with them. A drop of the
    /// future asks for a cancel, and the driver keeps the buffer until the read ends.
    ///
    /// # Errors
    ///
    /// A refusal, with the buffer, when the driver has no room for the operation, and
    /// [`Reason::Alignment`] for a direct-I/O file when the buffer, its capacity, or the offset
    /// does not have the alignment of the file.
    pub fn read_at<'a>(
        &mut self,
        file: &'a File,
        buffer: Backing,
        offset: u64,
    ) -> Result<Operation<'a, (io::Result<usize>, Backing)>, Refused<Backing>> {
        let completion = self.begin_read(file, buffer, offset)?;
        Ok(Operation::new(completion, |result, held| {
            Some((count(result), held.loan?.into_inner()))
        }))
    }

    /// Reads into the whole capacity of `buffer` after its initialized bytes, from the file offset
    /// `offset`, as [`Driver::read_at`] does, in one read. The future gives `Ok` when the read
    /// filled the capacity. A read that put in fewer bytes gives an error of the kind
    /// `UnexpectedEof` whose inner error is [`Incomplete`], with the count, and the buffer holds
    /// those bytes. A drop of the future asks for a cancel, as for [`Driver::read_at`].
    ///
    /// # Errors
    ///
    /// As for [`Driver::read_at`].
    pub fn read_exact_at<'a>(
        &mut self,
        file: &'a File,
        buffer: Backing,
        offset: u64,
    ) -> Result<Operation<'a, (io::Result<()>, Backing)>, Refused<Backing>> {
        let completion = self.begin_read(file, buffer, offset)?;
        Ok(Operation::new(completion, |result, held| {
            let loan = held.loan?;
            let wanted = submitted(loan.capacity()) as usize;
            let read = match count(result) {
                Ok(read) if read == wanted => Ok(()),
                Ok(read) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, Incomplete { read })),
                Err(error) => Err(error),
            };
            Some((read, loan.into_inner()))
        }))
    }

    /// Reads into the capacity of each buffer of `buffers`, in order, from the file offset
    /// `offset` (`IORING_OP_READV`). The future gives the bytes that the read put in, and the
    /// buffers with them. A drop of the future asks for a cancel, as for [`Driver::read_at`].
    ///
    /// # Errors
    ///
    /// As for [`Driver::read_at`], for each buffer.
    pub fn read_vectored_at<'a>(
        &mut self,
        file: &'a File,
        mut buffers: Buffers,
        offset: u64,
    ) -> Result<Operation<'a, (io::Result<usize>, Buffers)>, Refused<Buffers>> {
        if !buffers.aligned(file, offset) {
            return Err(Refused {
                reason: Reason::Alignment,
                value: buffers,
            });
        }
        let held = |buffers| Held {
            payload: Payload::Buffers(buffers),
            ..Held::default()
        };
        let entry = move |fd, held: &mut Held| match &mut held.payload {
            Payload::Buffers(buffers) => {
                let (vectors, count) = buffers.vectors();
                Sqe::readv(fd, vectors, count).at(offset)
            }
            _ => Sqe::nop(),
        };
        let completion = self.begin_file(Some(file.resource), Op::ReadVectored, buffers, held, entry, true)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::Buffers(buffers) = held.payload else {
                return None;
            };
            Some((count(result), buffers))
        }))
    }

    /// Writes the bytes of `lease` at the file offset `offset`, with `mode` (`IORING_OP_WRITE`).
    /// The future gives the bytes that the write wrote, and the lease. A drop of the future does
    /// not cancel the write: it can still change the file, and the driver keeps the lease until it
    /// ends.
    ///
    /// # Errors
    ///
    /// A refusal, with the lease, when the driver has no room for the operation, and
    /// [`Reason::Alignment`] for a direct-I/O file when the bytes or the offset do not have the
    /// alignment of the file.
    pub fn write_at<'a>(
        &mut self,
        file: &'a File,
        lease: Lease,
        offset: u64,
        mode: WriteMode,
    ) -> Result<Operation<'a, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let len = submitted(lease.initialized());
        if !file.allows(lease.as_ptr().addr(), len as usize, offset) {
            return Err(Refused {
                reason: Reason::Alignment,
                value: lease,
            });
        }
        let held = |lease| Held {
            payload: Payload::Lease(lease),
            ..Held::default()
        };
        let entry = move |fd, held: &mut Held| match &held.payload {
            Payload::Lease(lease) => Sqe::write(fd, lease.as_ptr(), len)
                .at(offset)
                .with_rw_flags(mode.flags()),
            _ => Sqe::nop(),
        };
        let completion = self.begin_file(Some(file.resource), Op::Plain, lease, held, entry, false)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::Lease(lease) = held.payload else {
                return None;
            };
            Some((count(result), lease))
        }))
    }

    /// Writes the bytes of each region of `regions`, in order, at the file offset `offset`, with
    /// `mode` (`IORING_OP_WRITEV`), as [`Driver::write_at`] writes one lease. A drop of the future
    /// does not cancel the write.
    ///
    /// # Errors
    ///
    /// As for [`Driver::write_at`], for each region.
    pub fn write_vectored_at<'a>(
        &mut self,
        file: &'a File,
        regions: Regions,
        offset: u64,
        mode: WriteMode,
    ) -> Result<Operation<'a, (io::Result<usize>, Regions)>, Refused<Regions>> {
        let aligned = regions
            .leases()
            .iter()
            .all(|lease| file.allows(lease.as_ptr().addr(), lease.initialized(), offset));
        if !aligned {
            return Err(Refused {
                reason: Reason::Alignment,
                value: regions,
            });
        }
        let held = |regions| Held {
            payload: Payload::Regions(regions),
            ..Held::default()
        };
        let entry = move |fd, held: &mut Held| match &mut held.payload {
            Payload::Regions(regions) => {
                let (vectors, count) = regions.vectors();
                Sqe::writev(fd, vectors, count).at(offset).with_rw_flags(mode.flags())
            }
            _ => Sqe::nop(),
        };
        let completion = self.begin_file(Some(file.resource), Op::Plain, regions, held, entry, false)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::Regions(regions) = held.payload else {
                return None;
            };
            Some((count(result), regions))
        }))
    }

    /// The length of `file` in bytes (`IORING_OP_STATX`, and `fstat` on macOS). A drop of the
    /// future asks for a cancel.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system.
    pub fn file_len<'a>(&mut self, file: &'a File) -> Result<Operation<'a, io::Result<u64>>, Refused<()>> {
        let held = |()| Held {
            payload: Payload::Stat(Stat::default()),
            ..Held::default()
        };
        #[cfg(target_os = "linux")]
        let entry = |fd, held: &mut Held| match &mut held.payload {
            Payload::Stat(stat) => Sqe::statx(
                fd,
                EMPTY.as_ptr().cast(),
                sys::AT_EMPTY_PATH.cast_unsigned(),
                sys::STATX_SIZE,
                (&raw mut *stat).cast(),
            ),
            _ => Sqe::nop(),
        };
        // The job makes an `fstat` of the descriptor of the entry into its structure.
        #[cfg(target_os = "macos")]
        let entry = |fd, held: &mut Held| match &mut held.payload {
            Payload::Stat(stat) => Sqe::statx(fd, core::ptr::null(), 0, 0, (&raw mut *stat).cast()),
            _ => Sqe::nop(),
        };
        let completion = self.begin_file(Some(file.resource), Op::Plain, (), held, entry, true)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::Stat(stat) = held.payload else {
                return None;
            };
            Some(count(result).and_then(|_| length(&stat)))
        }))
    }

    /// Sets the length of `file` to `len` bytes: a longer file reads as zeros after its old end.
    /// It runs on the ring with `IORING_OP_FTRUNCATE` (Linux 6.9), and else as a job of the
    /// blocking service ([`Route::BlockingService`]). A drop of the future does not cancel it.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, and [`Reason::Blocking`] when the
    /// change runs on the blocking service and the driver has none, or the service refused the
    /// job. The future gives an error of the system.
    pub fn set_file_len<'a>(&mut self, file: &'a File, len: u64) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        // The ring has the call from Linux 6.9, and on epoll and kqueue it is a file job.
        let entry = self.report.route(Call::SetFileLen) == Route::Ring
            || self.report.route(Call::File) == Route::BlockingService;
        let completion = if entry {
            let entry = move |fd, _: &mut Held| Sqe::ftruncate(fd, len);
            self.begin_file(Some(file.resource), Op::Plain, (), |()| Held::default(), entry, false)?
        } else {
            self.begin_job(file.resource, len)?
        };
        Ok(Operation::new(completion, |result, held| Some(done(result, &held))))
    }

    /// Reserves blocks for `len` bytes from `offset` of `file` (`IORING_OP_FALLOCATE`), so that a
    /// later write there does not fail for space. With [`Length::Extend`], the length becomes the
    /// end of the range when the range ends after it. A drop of the future does not cancel it.
    ///
    /// On macOS, `F_PREALLOCATE` reserves blocks only after the physical end of the file: the job
    /// asks for the blocks from the allocated bytes to the end of the range, all of them or none.
    /// A hole before the physical end stays a hole, so a later write into it can still fail for
    /// space. Then `ftruncate` makes the length for [`Length::Extend`]. The two calls are not one
    /// change, so such an allocation runs alone on its file: it starts when each other operation
    /// of the file ended, and each later one waits until it ends. Another process that writes the
    /// file at the same time is outside that order, as for each `ftruncate`.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system, for example `Unsupported` on a file system without preallocation, `StorageFull`
    /// when the space is not free, and `InvalidInput` for 0 bytes.
    pub fn allocate<'a>(
        &mut self,
        file: &'a File,
        offset: u64,
        len: u64,
        length: Length,
    ) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        let mode = match length {
            Length::Keep => FALLOC_FL_KEEP_SIZE,
            Length::Extend => 0,
        };
        #[cfg(target_os = "macos")]
        let op = match length {
            Length::Keep => Op::Plain,
            Length::Extend => Op::Extend,
        };
        #[cfg(target_os = "linux")]
        let op = Op::Plain;
        self.fallocate(file, op, mode, offset, len)
    }

    /// Frees the blocks of `len` bytes from `offset` of `file`, which then read as zeros, and
    /// keeps the length (`IORING_OP_FALLOCATE` with `FALLOC_FL_PUNCH_HOLE` and
    /// `FALLOC_FL_KEEP_SIZE`). A block that the range holds only in part keeps its blocks, and the
    /// bytes of the range in it become zeros. A drop of the future does not cancel it.
    ///
    /// On macOS, `F_PUNCHHOLE` frees the whole blocks of the file system in the range, and then
    /// the job writes zeros into the bytes of the range in its first and last block, before the
    /// end of the file.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system: `Unsupported` (`EOPNOTSUPP`, and `ENOTTY` on macOS) on a file system without hole
    /// punching, before the operation changes a byte, and `InvalidInput` for 0 bytes.
    pub fn punch_hole<'a>(
        &mut self,
        file: &'a File,
        offset: u64,
        len: u64,
    ) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        self.fallocate(file, Op::Plain, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, len)
    }

    /// Makes the writes of `file` that ended before it durable, as `mode` says
    /// (`IORING_OP_FSYNC`, and `F_FULLFSYNC` on macOS). A drop of the future does not cancel it.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system: on macOS, `Unsupported` on a file system with no `F_FULLFSYNC`. The driver never
    /// makes a weaker sync in its place.
    pub fn sync<'a>(&mut self, file: &'a File, mode: SyncMode) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        self.fsync(file.resource, mode == SyncMode::Data)
    }

    /// Makes the creates, renames, and unlinks in `directory` that ended before it durable
    /// (`IORING_OP_FSYNC` on the directory, and `F_FULLFSYNC` on macOS). A drop of the future does
    /// not cancel it.
    ///
    /// # Errors
    ///
    /// As for [`Driver::sync`].
    pub fn sync_directory<'a>(
        &mut self,
        directory: &'a Directory,
    ) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        self.fsync(directory.resource, false)
    }

    /// Renames `from` to `to`, and replaces a file at `to` (`IORING_OP_RENAMEAT`). A sync of each
    /// parent directory makes the change durable. A drop of the future does not cancel it: the
    /// rename runs to its end, so its effect never depends on a race with the drop.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system, or `InvalidInput` for a path with a NUL byte.
    pub fn rename(&mut self, from: &Path, to: &Path) -> Result<Operation<'static, io::Result<()>>, Refused<()>> {
        let completion = match (c_path(from), c_path(to)) {
            (Some(from), Some(to)) => {
                let held = |(path, target)| Held {
                    payload: Payload::Paths {
                        path,
                        target: Some(target),
                    },
                    ..Held::default()
                };
                let entry = |_, held: &mut Held| match &held.payload {
                    Payload::Paths {
                        path,
                        target: Some(target),
                    } => Sqe::renameat(
                        sys::AT_FDCWD,
                        path.as_ptr().cast(),
                        sys::AT_FDCWD,
                        target.as_ptr().cast(),
                        0,
                    ),
                    _ => Sqe::nop(),
                };
                self.begin_file(None, Op::Plain, (from, to), held, entry, false)
                    .map_err(|refused| Refused {
                        reason: refused.reason,
                        value: (),
                    })?
            }
            _ => self.fail_now(EINVAL)?,
        };
        Ok(Operation::new(completion, |result, held| Some(done(result, &held))))
    }

    /// Removes the file at `path` (`IORING_OP_UNLINKAT`). A sync of the parent directory makes the
    /// change durable. A drop of the future does not cancel it, as for [`Driver::rename`].
    ///
    /// # Errors
    ///
    /// As for [`Driver::rename`].
    pub fn unlink(&mut self, path: &Path) -> Result<Operation<'static, io::Result<()>>, Refused<()>> {
        let completion = match c_path(path) {
            Some(path) => {
                let held = |path| Held {
                    payload: Payload::Paths { path, target: None },
                    ..Held::default()
                };
                let entry = |_, held: &mut Held| match &held.payload {
                    Payload::Paths { path, .. } => Sqe::unlinkat(sys::AT_FDCWD, path.as_ptr().cast(), 0),
                    _ => Sqe::nop(),
                };
                self.begin_file(None, Op::Plain, path, held, entry, false)
                    .map_err(|refused| Refused {
                        reason: refused.reason,
                        value: (),
                    })?
            }
            None => self.fail_now(EINVAL)?,
        };
        Ok(Operation::new(completion, |result, held| Some(done(result, &held))))
    }

    /// Closes `file`. The close waits until each operation of the file ends, also each operation
    /// whose future dropped, and then closes the descriptor. It makes nothing durable: use
    /// [`Driver::sync`] for that.
    ///
    /// # Errors
    ///
    /// A refusal, with the file, when the driver has no room for the operation.
    pub fn close_file(&mut self, file: File) -> Result<Operation<'static, ()>, Refused<File>> {
        let completion = self.close_resource(file.resource, file)?;
        Ok(Operation::new(completion, |_, _| Some(())))
    }

    /// Closes `directory`, as [`Driver::close_file`] closes a file.
    ///
    /// # Errors
    ///
    /// A refusal, with the directory, when the driver has no room for the operation.
    pub fn close_directory(&mut self, directory: Directory) -> Result<Operation<'static, ()>, Refused<Directory>> {
        let completion = self.close_resource(directory.resource, directory)?;
        Ok(Operation::new(completion, |_, _| Some(())))
    }

    /// Polls the job of each operation that runs on the blocking service, and gives the result
    /// of each job that ended to its operation as its terminal event.
    pub(super) fn poll_jobs(&mut self) {
        if self.jobs.is_empty() {
            return;
        }
        let Some(waker) = self.blocking.as_ref().map(|(_, waker)| waker.clone()) else {
            return;
        };
        let cx = Context::from_waker(&waker);
        let mut index = 0;
        while let Some(&id) = self.jobs.get(index) {
            #[cfg(target_os = "linux")]
            if let Some(&Tag {
                kind: Kind::Spawn(child),
                ..
            }) = self.table.resources(id)
            {
                // The end of a spawn pushes the entry of the exit operation of its child, so its
                // result waits for room in the queue.
                match self.room(1).then(|| self.poll_spawn(id, &cx)).flatten() {
                    Some(spawned) => {
                        self.jobs.swap_remove(index);
                        self.spawn_ended(id, child, spawned);
                    }
                    None => index += 1,
                }
                continue;
            }
            let mut ended = None;
            if let Some(slot) = self.shared.entries.get(id.index() as usize) {
                slot.update(id, |held| ended = poll_job(held.as_mut(), &cx));
            }
            match ended {
                Some(result) => {
                    self.jobs.swap_remove(index);
                    self.complete_now(id, result);
                }
                None => index += 1,
            }
        }
    }

    /// Starts an open of `path` with `flags` and `mode`, or an operation that fails at once with
    /// `EINVAL` when `flags` is `None` or the path holds a NUL byte. A drop of the future asks for
    /// a cancel when `cancel_on_drop` holds.
    fn begin_open(
        &mut self,
        path: &Path,
        flags: Option<c_int>,
        mode: u32,
        op: Op,
        cancel_on_drop: bool,
    ) -> Result<Completion, Refused<()>> {
        let (Some(flags), Some(path)) = (flags, c_path(path)) else {
            return self.fail_now(EINVAL);
        };
        let held = |path| Held {
            payload: Payload::Open { path, alignment: None },
            ..Held::default()
        };
        let entry = move |_, held: &mut Held| match &held.payload {
            Payload::Open { path, .. } => Sqe::openat(sys::AT_FDCWD, path.as_ptr().cast(), flags.cast_unsigned(), mode),
            _ => Sqe::nop(),
        };
        self.begin_file(None, op, path, held, entry, cancel_on_drop)
            .map_err(|refused| Refused {
                reason: refused.reason,
                value: (),
            })
    }

    /// Starts a read of `file` into `buffer` at `offset`.
    fn begin_read(&mut self, file: &File, buffer: Backing, offset: u64) -> Result<Completion, Refused<Backing>> {
        let mut loan = WriteLoan::new(buffer);
        let len = submitted(loan.capacity());
        if !file.allows(loan.as_mut_ptr().addr(), len as usize, offset) {
            return Err(Refused {
                reason: Reason::Alignment,
                value: loan.into_inner(),
            });
        }
        let held = |loan| Held {
            loan: Some(loan),
            ..Held::default()
        };
        let entry = move |fd, held: &mut Held| match held.loan.as_mut() {
            Some(loan) => Sqe::read(fd, loan.as_mut_ptr(), len).at(offset),
            None => Sqe::nop(),
        };
        self.begin_file(Some(file.resource), Op::Read, loan, held, entry, true)
            .map_err(|refused| Refused {
                reason: refused.reason,
                value: refused.value.into_inner(),
            })
    }

    /// Starts a `fallocate` of `file` with `mode`, as the file operation `op`.
    fn fallocate<'a>(
        &mut self,
        file: &'a File,
        op: Op,
        mode: u32,
        offset: u64,
        len: u64,
    ) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        let entry = move |fd, _: &mut Held| Sqe::fallocate(fd, mode, offset, len);
        let completion = self.begin_file(Some(file.resource), op, (), |()| Held::default(), entry, false)?;
        Ok(Operation::new(completion, |result, held| Some(done(result, &held))))
    }

    /// Starts a sync of `resource`: `fdatasync` when `data` holds, else `fsync`.
    fn fsync<'a>(&mut self, resource: Resource, data: bool) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        let entry = move |fd, _: &mut Held| Sqe::fsync(fd, data);
        let completion = self.begin_file(Some(resource), Op::Plain, (), |()| Held::default(), entry, false)?;
        Ok(Operation::new(completion, |result, held| Some(done(result, &held))))
    }

    /// Starts an operation that ends at once with the error `errno`, with no submission.
    pub(super) fn fail_now(&mut self, errno: i32) -> Result<Completion, Refused<()>> {
        let tag = Tag {
            resource: None,
            kind: Kind::File(Op::Plain),
        };
        let id = self
            .open(tag, 0, (), |()| Held::default())
            .map_err(|(reason, value)| Refused { reason, value })?;
        self.complete_now(id, -errno);
        Ok(Completion::new(Arc::clone(&self.shared), id, false))
    }

    /// Starts the file operation `op` on `resource`, or on no resource for a path operation, with
    /// the resources that `held` makes from `value`. `entry` makes its submission from the
    /// descriptor and the resources in their final place. A drop of the future asks for a cancel
    /// when `cancel_on_drop` holds. A refusal gives back `value`.
    fn begin_file<P>(
        &mut self,
        resource: Option<Resource>,
        op: Op,
        value: P,
        held: impl FnOnce(P) -> Held,
        entry: impl FnOnce(RawFd, &mut Held) -> Sqe,
        cancel_on_drop: bool,
    ) -> Result<Completion, Refused<P>> {
        let fd = match resource {
            None => sys::AT_FDCWD,
            Some(resource) => match self.resources.get(resource) {
                Some(owned) if owned.medium == Medium::File => match owned.fd.as_ref() {
                    Some(fd) => fd.as_raw_fd(),
                    None => {
                        return Err(Refused {
                            reason: Reason::Resource,
                            value,
                        });
                    }
                },
                _ => {
                    return Err(Refused {
                        reason: Reason::Resource,
                        value,
                    });
                }
            },
        };
        if self.report.route(Call::File) == Route::BlockingService {
            return self.begin_file_job(resource, op, value, held, entry, cancel_on_drop);
        }
        let tag = Tag {
            resource,
            kind: Kind::File(op),
        };
        let id = self
            .open(tag, 1, value, held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        self.count_start(resource);
        // SAFETY: the entry names only the loan, the lease, the regions, the buffers, the paths,
        // or the structure in the resources of `id`, which the home keeps until its terminal
        // event, and the descriptor of `resource`, which stays open until its close, after each
        // of its operations ends. `open` found room in the queue.
        #[expect(unsafe_code, reason = "the kernel uses the memory of the resources of the operation")]
        unsafe {
            self.push(id, |held| entry(fd, held));
        }
        Ok(Completion::new(Arc::clone(&self.shared), id, cancel_on_drop))
    }

    /// Starts the file operation `op` of [`Driver::begin_file`] as a job of the blocking service,
    /// which makes the call of its entry with one positional call ([`poller::run_file`]), because
    /// epoll and kqueue cannot wait on a file. The job owns a duplicate of the descriptor, so it never names a
    /// descriptor number that the driver closed and the system gave to another file. The job
    /// posts the completion of the entry to the poller when it returns.
    fn begin_file_job<P>(
        &mut self,
        resource: Option<Resource>,
        op: Op,
        value: P,
        held: impl FnOnce(P) -> Held,
        entry: impl FnOnce(RawFd, &mut Held) -> Sqe,
        cancel_on_drop: bool,
    ) -> Result<Completion, Refused<P>> {
        let Some((submitter, waker)) = self
            .blocking
            .as_ref()
            .map(|(submitter, waker)| (submitter.clone(), waker.clone()))
        else {
            return Err(Refused {
                reason: Reason::Blocking,
                value,
            });
        };
        // The reservation comes first, so a full service refuses before the operation exists.
        let Ok(reservation) = submitter.reserve::<FileJob>() else {
            return Err(Refused {
                reason: Reason::Blocking,
                value,
            });
        };
        #[cfg(target_os = "macos")]
        let waits = resource.is_some_and(|resource| self.waits(resource, op));
        let tag = Tag {
            resource,
            kind: Kind::File(op),
        };
        let id = self
            .open(tag, 0, value, held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        self.count_start(resource);
        let duplicate = match resource.and_then(|resource| self.resources.get(resource)?.fd.as_ref()) {
            Some(fd) => fd.try_clone().map(Some),
            None => Ok(None),
        };
        let fd = match duplicate {
            Ok(fd) => fd,
            Err(error) => {
                self.complete_now(id, -error.raw_os_error().unwrap_or(EIO));
                return Ok(Completion::new(Arc::clone(&self.shared), id, cancel_on_drop));
            }
        };
        let mut sqe = None;
        if let Some(slot) = self.shared.entries.get(id.index() as usize) {
            let raw = fd.as_ref().map_or(sys::AT_FDCWD, AsRawFd::as_raw_fd);
            slot.update(id, |held| sqe = held.as_mut().map(|held| entry(raw, held)));
        }
        #[cfg(target_os = "macos")]
        if let (true, Some(sqe), Some(resource)) = (waits, sqe, resource) {
            // At most one entry for each operation, so the capacity of the list holds it.
            self.waiting.push(Waiting {
                id,
                resource,
                op,
                job: FileJob { sqe, _fd: fd },
            });
            return Ok(Completion::new(Arc::clone(&self.shared), id, cancel_on_drop));
        }
        #[cfg(target_os = "macos")]
        self.started(resource, op, id);
        match (sqe, self.queue.poller()) {
            (Some(sqe), Some(poller)) => {
                poller.add_job(id.user_data(), reservation.send(FileJob { sqe, _fd: fd }), &waker)
            }
            _ => self.complete_now(id, -EINVAL),
        }
        Ok(Completion::new(Arc::clone(&self.shared), id, cancel_on_drop))
    }

    /// Whether a new file operation `op` of `resource` waits on macOS: an earlier operation of the
    /// file waits, an allocation that extends the file runs, or `op` is such an allocation and
    /// another operation of the file runs.
    #[cfg(target_os = "macos")]
    fn waits(&mut self, resource: Resource, op: Op) -> bool {
        let waiting = self
            .waiting
            .iter()
            .filter(|waiting| waiting.resource == resource)
            .count();
        let (table, resources) = (&self.table, &mut self.resources);
        waiting > 0
            || resources
                .get(resource)
                .is_some_and(|owned| !free(owned, table, op, waiting))
    }

    /// Records that the file operation `id` of `resource` started on macOS: an allocation that
    /// extends the file then runs alone on it.
    #[cfg(target_os = "macos")]
    fn started(&mut self, resource: Option<Resource>, op: Op, id: super::OpId) {
        if op == Op::Extend
            && let Some(owned) = resource.and_then(|resource| self.resources.get(resource))
        {
            owned.exclusive = Some(id);
        }
    }

    /// Starts each waiting file operation that can run now, in the order of their starts on each
    /// file, on macOS. A full service starts none: a job that ends wakes the driver, and a later
    /// turn tries again. A stopped service ends each one, as it ends a job.
    #[cfg(target_os = "macos")]
    pub(super) fn start_waiting(&mut self) {
        let Some((submitter, waker)) = self
            .blocking
            .as_ref()
            .map(|(submitter, waker)| (submitter.clone(), waker.clone()))
        else {
            return;
        };
        let mut index = 0;
        // Each step starts, ends, or skips one entry, and the list holds at most one entry for each
        // operation, so the loop ends after at most `Config::operations` steps.
        while let Some(waiting) = self.waiting.get(index) {
            let (resource, op) = (waiting.resource, waiting.op);
            // An earlier operation of the same file waits, and it starts first.
            let first = !self.waiting[..index].iter().any(|earlier| earlier.resource == resource);
            let count = self.waiting.iter().filter(|other| other.resource == resource).count();
            let (table, resources) = (&self.table, &mut self.resources);
            if !first
                || resources
                    .get(resource)
                    .is_some_and(|owned| !free(owned, table, op, count))
            {
                index += 1;
                continue;
            }
            let reservation = match submitter.reserve::<FileJob>() {
                Ok(reservation) => reservation,
                Err(crate::blocking::Reason::Ended) => {
                    let waiting = self.waiting.remove(index);
                    self.complete_now(waiting.id, -ECANCELED);
                    continue;
                }
                Err(_) => return,
            };
            let waiting = self.waiting.remove(index);
            self.started(Some(resource), op, waiting.id);
            if let Some(poller) = self.queue.poller() {
                poller.add_job(waiting.id.user_data(), reservation.send(waiting.job), &waker);
            }
        }
    }

    /// Ends the waiting file operation `id` on macOS, which never ran. Gives whether it waited.
    #[cfg(target_os = "macos")]
    pub(super) fn unwait(&mut self, id: super::OpId) -> bool {
        let Some(index) = self.waiting.iter().position(|waiting| waiting.id == id) else {
            return false;
        };
        drop(self.waiting.remove(index));
        self.complete_now(id, -ECANCELED);
        true
    }

    /// Starts a change of the length of the file `resource` to `len` as a job of the blocking
    /// service. The job owns a duplicate of the descriptor, so it never names a descriptor number
    /// that the driver closed and the system gave to another file.
    fn begin_job(&mut self, resource: Resource, len: u64) -> Result<Completion, Refused<()>> {
        let refuse = |reason| Err(Refused { reason, value: () });
        let Some(submitter) = self.blocking.as_ref().map(|(submitter, _)| submitter.clone()) else {
            return refuse(Reason::Blocking);
        };
        let duplicate = match self.resources.get(resource) {
            Some(owned) if owned.medium == Medium::File => match owned.fd.as_ref() {
                Some(fd) => fd.try_clone(),
                None => return refuse(Reason::Resource),
            },
            _ => return refuse(Reason::Resource),
        };
        let fd = match duplicate {
            Ok(fd) => fd,
            Err(error) => return self.fail_now(error.raw_os_error().unwrap_or(EIO)),
        };
        let tag = Tag {
            resource: Some(resource),
            kind: Kind::File(Op::Plain),
        };
        let job = move || match fs::File::from(fd).set_len(len) {
            Ok(()) => 0,
            Err(error) => -error.raw_os_error().unwrap_or(EIO),
        };
        let held = move |()| match submitter.submit(job) {
            Ok(waiter) => Ok(Held {
                payload: Payload::Job(waiter),
                ..Held::default()
            }),
            // The job drops, and its descriptor closes.
            Err(_refused) => Err((Reason::Blocking, ())),
        };
        let id = self
            .open_with(tag, 0, (), held)
            .map_err(|(reason, value)| Refused { reason, value })?;
        self.count_start(Some(resource));
        // At most one entry for each operation, so the capacity of the list holds it.
        self.jobs.push(id);
        Ok(Completion::new(Arc::clone(&self.shared), id, false))
    }

    /// Counts a started operation of the file `resource`, which its terminal event uncounts.
    fn count_start(&mut self, resource: Option<Resource>) {
        if let Some(owned) = resource.and_then(|resource| self.resources.get(resource)) {
            owned.running += 1;
        }
    }
}

// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
pub(super) mod tests;
