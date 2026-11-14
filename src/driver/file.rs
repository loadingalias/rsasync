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
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;

use rsnetwork_udp::sys::IoVec;

use super::tcp::{Regions, count};
use super::{
    Call, Completion, Driver, ECANCELED, Held, Kind, Medium, Operation, Payload, Reason, Refused, Resource, Resources,
    Route, Tag, sys,
};
use crate::blocking::Submitter;
use crate::buffer::{Backing, Buffer as _, Lease, WriteLoan};
use crate::sync::Arc;
use crate::uring::Sqe;

/// The empty path of a call on the descriptor itself.
const EMPTY: &core::ffi::CStr = c"";

/// `EIO` (`include/uapi/asm-generic/errno-base.h`), for a job of the blocking service that gives
/// no error number.
const EIO: i32 = 5;
/// `EINVAL`, for options or a path that no call can take.
const EINVAL: i32 = 22;

/// The alignment of direct I/O when the file system does not report it: one page.
const PAGE: Alignment = Alignment {
    memory: 4096,
    offset: 4096,
};

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
/// use rsasync::buffer::{Backing, Buffer as _, Lease};
/// use rsasync::driver::{Config, Driver, OpenOptions, SyncMode, WriteMode};
///
/// let mut driver = Driver::new(Config::new())?;
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
/// A file system that does not report it gets 4096 bytes for both values.
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
    /// (`O_DIRECT`). Each buffer, length, and offset has the [`Alignment`] of the file.
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

/// What [`Driver::sync`] makes durable.
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
    /// The write ends when its bytes are durable, as with a `fdatasync` after it (`RWF_DSYNC`).
    DataSync,
}

impl WriteMode {
    const fn flags(self) -> u32 {
        match self {
            Self::Plain => 0,
            Self::DataSync => sys::RWF_DSYNC.cast_unsigned(),
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
                Err(_closed) => held.error = Some(io::ErrorKind::OutOfMemory),
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

    /// The length of `file` in bytes (`IORING_OP_STATX`). A drop of the future asks for a cancel.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system.
    pub fn file_len<'a>(&mut self, file: &'a File) -> Result<Operation<'a, io::Result<u64>>, Refused<()>> {
        let held = |()| Held {
            payload: Payload::Stat(sys::Statx::default()),
            ..Held::default()
        };
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
        let completion = self.begin_file(Some(file.resource), Op::Plain, (), held, entry, true)?;
        Ok(Operation::new(completion, |result, held| {
            let Payload::Stat(stat) = held.payload else {
                return None;
            };
            Some(count(result).and_then(|_| {
                if stat.mask & sys::STATX_SIZE == 0 {
                    return Err(io::ErrorKind::Unsupported.into());
                }
                Ok(stat.size)
            }))
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
        let completion = if self.report.route(Call::SetFileLen) == Route::Ring {
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
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system, for example `Unsupported` on a file system without preallocation.
    pub fn allocate<'a>(
        &mut self,
        file: &'a File,
        offset: u64,
        len: u64,
        length: Length,
    ) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        let mode = match length {
            Length::Keep => sys::FALLOC_FL_KEEP_SIZE,
            Length::Extend => 0,
        };
        self.fallocate(file, mode, offset, len)
    }

    /// Frees the blocks of `len` bytes from `offset` of `file`, which then read as zeros, and
    /// keeps the length (`IORING_OP_FALLOCATE` with `FALLOC_FL_PUNCH_HOLE` and
    /// `FALLOC_FL_KEEP_SIZE`). A drop of the future does not cancel it.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system: `Unsupported` (`EOPNOTSUPP`) on a file system without hole punching.
    pub fn punch_hole<'a>(
        &mut self,
        file: &'a File,
        offset: u64,
        len: u64,
    ) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        self.fallocate(file, sys::FALLOC_FL_PUNCH_HOLE | sys::FALLOC_FL_KEEP_SIZE, offset, len)
    }

    /// Makes the writes of `file` that ended before it durable, as `mode` says
    /// (`IORING_OP_FSYNC`). A drop of the future does not cancel it.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation. The future gives an error of the
    /// system.
    pub fn sync<'a>(&mut self, file: &'a File, mode: SyncMode) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        self.fsync(file.resource, mode == SyncMode::Data)
    }

    /// Makes the creates, renames, and unlinks in `directory` that ended before it durable
    /// (`IORING_OP_FSYNC` on the directory). A drop of the future does not cancel it.
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

    /// Starts a `fallocate` of `file` with `mode`.
    fn fallocate<'a>(
        &mut self,
        file: &'a File,
        mode: c_int,
        offset: u64,
        len: u64,
    ) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
        let entry = move |fd, _: &mut Held| Sqe::fallocate(fd, mode.cast_unsigned(), offset, len);
        let completion = self.begin_file(Some(file.resource), Op::Plain, (), |()| Held::default(), entry, false)?;
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

#[cfg(all(test, not(loom)))]
mod tests;
