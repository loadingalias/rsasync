//! The io_uring ring of one driver: creation, the shared mappings, submission, harvest, and the
//! probes of the later kernel features.
//!
//! Each ring has one issuer and runs deferred task work (`IORING_SETUP_SINGLE_ISSUER` and
//! `IORING_SETUP_DEFER_TASKRUN`, Linux 6.1). Thus only the thread that creates a ring can submit to
//! it or wait on it, and [`Ring`] is not `Send`. The kernel runs the deferred work only when that
//! thread enters with `IORING_ENTER_GETEVENTS`. `IORING_SETUP_TASKRUN_FLAG` makes the kernel show
//! such work in the ring flags: see [`Ring::task_run_pending`].
//!
//! The kernel reads and writes the head, tail, and flag words of the mapping while the ring exists.
//! These words are hardware atomics in every build, because the other party is the kernel.

mod buffers;
pub(crate) mod sys;
// Two attributes each, so that Clippy sees the test modules (`allow-unwrap-in-tests`).
#[cfg(all(test, not(loom)))]
#[cfg(target_os = "linux")]
mod table_tests;
#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests;

#[cfg(target_os = "linux")]
use core::ffi::{c_int, c_long, c_void};
#[cfg(target_os = "linux")]
use core::mem::size_of;
#[cfg(target_os = "linux")]
use core::ptr::{self, NonNull};
#[cfg(target_os = "linux")]
use core::sync::atomic::{AtomicU32, Ordering};
#[cfg(all(feature = "local", target_os = "linux"))]
use core::time::Duration;
use std::io;
#[cfg(target_os = "macos")]
use std::os::fd::RawFd;
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
/// The descriptor number of an entry. On Windows it holds the low 32 bits of a socket handle, which
/// are its significant bits: 64-bit Windows uses 32-bit handles for interoperability.
#[cfg(target_os = "windows")]
type RawFd = i32;

/// One region of a vectored entry, and the message header of a message entry: `struct iovec` and
/// `struct msghdr` on Linux and macOS, `WSABUF` and `WSAMSG` on Windows.
#[cfg(unix)]
use rsnetwork_udp::sys::{IoVec as Region, MsgHdr as Message};
#[cfg(target_os = "windows")]
use rsnetwork_udp::sys::{WsaBuf as Region, WsaMsg as Message};

pub(crate) use buffers::{BufferRing, Group};
pub(crate) use sys::{Cqe, Sqe};

use crate::table::Event;

/// The setup flags of each ring.
#[cfg(target_os = "linux")]
const FLAGS: u32 = sys::IORING_SETUP_SINGLE_ISSUER
    | sys::IORING_SETUP_DEFER_TASKRUN
    | sys::IORING_SETUP_TASKRUN_FLAG
    | sys::IORING_SETUP_CQSIZE;

/// The buffer group that the probe of incremental buffer rings registers and removes again.
#[cfg(target_os = "linux")]
const PROBE_GROUP: u16 = u16::MAX;

/// The user data of the message that the probe of ringless messages sends to its own ring.
#[cfg(target_os = "linux")]
const PROBE_MESSAGE: u64 = u64::MAX;

/// Why a ring was not created.
#[derive(Debug)]
#[cfg(target_os = "linux")]
pub(crate) enum SetupError {
    /// `EPERM` or `ENOSYS`: the kernel, a seccomp profile, or `kernel.io_uring_disabled` refuses
    /// io_uring. The runtime then selects epoll.
    Refused(io::Error),
    /// Another failure, for example a number of entries that the kernel refuses, or no memory.
    Failed(io::Error),
}

/// Each entry of the submission queue waits for the kernel.
#[derive(Debug)]
#[cfg(target_os = "linux")]
pub(crate) struct Full;

/// The later kernel features, each probed on the ring, not read from the kernel version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(target_os = "linux")]
pub(crate) struct Features {
    /// `IORING_OP_WAITID`: wait for a child (Linux 6.7).
    pub(crate) child_wait: bool,
    /// `IORING_OP_FTRUNCATE`: change the length of a file (Linux 6.9).
    pub(crate) truncate: bool,
    /// `IORING_FEAT_RECVSEND_BUNDLE`: send and receive bundles (Linux 6.10).
    pub(crate) bundles: bool,
    /// `IORING_OP_BIND` (Linux 6.11).
    pub(crate) bind: bool,
    /// `IORING_OP_LISTEN` (Linux 6.11).
    pub(crate) listen: bool,
    /// `IOU_PBUF_RING_INC`: incremental buffer rings (Linux 6.12).
    pub(crate) incremental_buffer_rings: bool,
    /// `IORING_REGISTER_SEND_MSG_RING`: a cross-ring message from a thread with no ring (Linux
    /// 6.13).
    pub(crate) ringless_messages: bool,
    /// `IORING_OP_RECV_ZC`: zero-copy receive (Linux 6.15).
    pub(crate) zero_copy_receive: bool,
}

impl Sqe {
    /// A no-op (`IORING_OP_NOP`): it completes with 0 and names no memory.
    pub(crate) fn nop() -> Self {
        Self {
            opcode: sys::IORING_OP_NOP,
            ..Self::default()
        }
    }

    /// A read of up to `len` bytes from `fd` into `buffer` (`IORING_OP_READ`), at the position of
    /// the file.
    pub(crate) fn read(fd: RawFd, buffer: *mut u8, len: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_READ,
            fd,
            off: u64::MAX,
            addr: buffer.addr() as u64,
            len,
            ..Self::default()
        }
    }

    /// A read from the pipe `fd` into a buffer that the kernel selects from the provided buffer
    /// ring of `group` when data comes (`IORING_OP_READ` with `IOSQE_BUFFER_SELECT`).
    pub(crate) fn read_selected(fd: RawFd, group: u16) -> Self {
        Self {
            opcode: sys::IORING_OP_READ,
            flags: sys::IOSQE_BUFFER_SELECT,
            fd,
            off: u64::MAX,
            buf_index: group,
            ..Self::default()
        }
    }

    /// A write of `len` bytes from `buffer` to the pipe `fd` (`IORING_OP_WRITE`). A pipe whose
    /// reader closed gives `EPIPE`, and also `SIGPIPE` where the process does not ignore it.
    pub(crate) fn write(fd: RawFd, buffer: *const u8, len: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_WRITE,
            fd,
            off: u64::MAX,
            addr: buffer.addr() as u64,
            len,
            ..Self::default()
        }
    }

    /// A write of the `count` regions of the iovec array `vectors` to the pipe `fd`, in order
    /// (`IORING_OP_WRITEV`).
    pub(crate) fn writev(fd: RawFd, vectors: *const Region, count: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_WRITEV,
            fd,
            off: u64::MAX,
            addr: vectors.addr() as u64,
            len: count,
            ..Self::default()
        }
    }

    /// A read of the `count` regions of the iovec array `vectors` from `fd`, in order
    /// (`IORING_OP_READV`), at the position of the file.
    pub(crate) fn readv(fd: RawFd, vectors: *const Region, count: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_READV,
            fd,
            off: u64::MAX,
            addr: vectors.addr() as u64,
            len: count,
            ..Self::default()
        }
    }

    /// The read or the write at the file offset `offset`, not at the position of the file.
    pub(crate) fn at(self, offset: u64) -> Self {
        Self { off: offset, ..self }
    }

    /// The read or the write with the `RWF_*` flags `flags`, for example `RWF_DSYNC`.
    pub(crate) fn with_rw_flags(self, flags: u32) -> Self {
        Self {
            rw_flags: flags,
            ..self
        }
    }

    /// A sync of the file `fd` (`IORING_OP_FSYNC`): `fdatasync` when `data` holds, else `fsync`.
    pub(crate) fn fsync(fd: RawFd, data: bool) -> Self {
        Self {
            opcode: sys::IORING_OP_FSYNC,
            fd,
            rw_flags: if data { sys::IORING_FSYNC_DATASYNC } else { 0 },
            ..Self::default()
        }
    }

    /// A `fallocate` of `len` bytes from `offset` of the file `fd` with `mode`
    /// (`IORING_OP_FALLOCATE`). The entry holds the length in `addr` and the mode in `len`.
    pub(crate) fn fallocate(fd: RawFd, mode: u32, offset: u64, len: u64) -> Self {
        Self {
            opcode: sys::IORING_OP_FALLOCATE,
            fd,
            off: offset,
            addr: len,
            len: mode,
            ..Self::default()
        }
    }

    /// An `openat` of the C string `path` relative to `directory` with `flags` and `mode`
    /// (`IORING_OP_OPENAT`). The kernel adds `O_LARGEFILE`.
    pub(crate) fn openat(directory: RawFd, path: *const u8, flags: u32, mode: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_OPENAT,
            fd: directory,
            addr: path.addr() as u64,
            len: mode,
            rw_flags: flags,
            ..Self::default()
        }
    }

    /// A `statx` of the C string `path` relative to `directory`, with `flags` and the fields of
    /// `mask`, into the structure at `buffer` (`IORING_OP_STATX`). The entry holds the structure
    /// in `off`.
    pub(crate) fn statx(directory: RawFd, path: *const u8, flags: u32, mask: u32, buffer: *mut u8) -> Self {
        Self {
            opcode: sys::IORING_OP_STATX,
            fd: directory,
            addr: path.addr() as u64,
            len: mask,
            off: buffer.addr() as u64,
            rw_flags: flags,
            ..Self::default()
        }
    }

    /// A `renameat2` of the C string `from` relative to `from_directory` to the C string `to`
    /// relative to `to_directory`, with `flags` (`IORING_OP_RENAMEAT`). The entry holds the second
    /// directory in `len` and the second path in `off`.
    pub(crate) fn renameat(
        from_directory: RawFd,
        from: *const u8,
        to_directory: RawFd,
        to: *const u8,
        flags: u32,
    ) -> Self {
        Self {
            opcode: sys::IORING_OP_RENAMEAT,
            fd: from_directory,
            addr: from.addr() as u64,
            len: to_directory.cast_unsigned(),
            off: to.addr() as u64,
            rw_flags: flags,
            ..Self::default()
        }
    }

    /// An `unlinkat` of the C string `path` relative to `directory`, with `flags`, for example
    /// `AT_REMOVEDIR` (`IORING_OP_UNLINKAT`).
    pub(crate) fn unlinkat(directory: RawFd, path: *const u8, flags: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_UNLINKAT,
            fd: directory,
            addr: path.addr() as u64,
            rw_flags: flags,
            ..Self::default()
        }
    }

    /// An `ftruncate` of the file `fd` to `len` bytes (`IORING_OP_FTRUNCATE`, Linux 6.9). The entry
    /// holds the length in `off`.
    pub(crate) fn ftruncate(fd: RawFd, len: u64) -> Self {
        Self {
            opcode: sys::IORING_OP_FTRUNCATE,
            fd,
            off: len,
            ..Self::default()
        }
    }

    /// The entry with the user data `user_data`, which its completion gives back.
    pub(crate) fn with_user_data(self, user_data: u64) -> Self {
        Self { user_data, ..self }
    }

    /// An accept on the listener `fd` (`IORING_OP_ACCEPT`). The kernel writes the peer address
    /// into `address` and its length into `len`, and the new descriptor is nonblocking and closes
    /// at an exec.
    pub(crate) fn accept(fd: RawFd, address: *mut u8, len: *mut u32) -> Self {
        Self {
            opcode: sys::IORING_OP_ACCEPT,
            fd,
            addr: address.addr() as u64,
            off: len.addr() as u64,
            #[cfg(target_os = "linux")]
            rw_flags: sys::SOCK_NONBLOCK | sys::SOCK_CLOEXEC,
            ..Self::default()
        }
    }

    /// A receive of up to `len` bytes from the socket `fd` into `buffer` (`IORING_OP_RECV`).
    pub(crate) fn recv(fd: RawFd, buffer: *mut u8, len: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_RECV,
            fd,
            addr: buffer.addr() as u64,
            len,
            ..Self::default()
        }
    }

    /// A receive from the socket `fd` into a buffer that the kernel selects from the provided
    /// buffer ring of `group` when data comes (`IOSQE_BUFFER_SELECT`).
    pub(crate) fn recv_selected(fd: RawFd, group: u16) -> Self {
        Self {
            opcode: sys::IORING_OP_RECV,
            flags: sys::IOSQE_BUFFER_SELECT,
            fd,
            buf_index: group,
            ..Self::default()
        }
    }

    /// A send of `len` bytes from `buffer` on the socket `fd` (`IORING_OP_SEND`). A closed peer
    /// gives `EPIPE` and no `SIGPIPE`.
    pub(crate) fn send(fd: RawFd, buffer: *const u8, len: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_SEND,
            fd,
            addr: buffer.addr() as u64,
            len,
            rw_flags: sys::MSG_NOSIGNAL,
            ..Self::default()
        }
    }

    /// A send of the message `message` on the socket `fd` (`IORING_OP_SENDMSG`): the regions that
    /// its iovec array names, in order. A closed peer gives `EPIPE` and no `SIGPIPE`.
    pub(crate) fn send_message(fd: RawFd, message: *const Message) -> Self {
        Self {
            opcode: sys::IORING_OP_SENDMSG,
            fd,
            addr: message.addr() as u64,
            len: 1,
            rw_flags: sys::MSG_NOSIGNAL,
            ..Self::default()
        }
    }

    /// A receive of one message on the socket `fd` into the regions, the address storage, and the
    /// control storage that `message` names (`IORING_OP_RECVMSG`), with the call flags `flags`, for
    /// example `MSG_DONTWAIT`. The kernel writes the lengths and the flags back into `message`.
    pub(crate) fn recv_message(fd: RawFd, message: *mut Message, flags: i32) -> Self {
        Self {
            opcode: sys::IORING_OP_RECVMSG,
            fd,
            addr: message.addr() as u64,
            len: 1,
            rw_flags: flags.cast_unsigned(),
            ..Self::default()
        }
    }

    /// A receive of one message into a buffer that the kernel selects from the provided buffer
    /// ring of `group` (`IOSQE_BUFFER_SELECT`). `message` names one region with no address, whose
    /// length is the most that the kernel writes.
    pub(crate) fn recv_message_selected(fd: RawFd, message: *mut Message, flags: i32, group: u16) -> Self {
        Self {
            flags: sys::IOSQE_BUFFER_SELECT,
            buf_index: group,
            ..Self::recv_message(fd, message, flags)
        }
    }

    /// The entry, linked to the next one: the next one starts only after this one succeeds, and
    /// it fails with `ECANCELED` when this one fails.
    pub(crate) fn linked(self) -> Self {
        Self {
            flags: self.flags | sys::IOSQE_IO_LINK,
            ..self
        }
    }

    /// A connect of the socket `fd` to the address of `len` bytes at `address`
    /// (`IORING_OP_CONNECT`).
    pub(crate) fn connect(fd: RawFd, address: *const u8, len: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_CONNECT,
            fd,
            addr: address.addr() as u64,
            off: u64::from(len),
            ..Self::default()
        }
    }

    /// A shutdown of the socket `fd` (`IORING_OP_SHUTDOWN`), with `how`, for example `SHUT_WR`.
    /// The entry names no memory.
    pub(crate) fn shutdown(fd: RawFd, how: i32) -> Self {
        Self {
            opcode: sys::IORING_OP_SHUTDOWN,
            fd,
            len: how.cast_unsigned(),
            ..Self::default()
        }
    }

    /// A bind of the socket `fd` to the address of `len` bytes at `address` (`IORING_OP_BIND`,
    /// Linux 6.11).
    pub(crate) fn bind(fd: RawFd, address: *const u8, len: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_BIND,
            fd,
            addr: address.addr() as u64,
            off: u64::from(len),
            ..Self::default()
        }
    }

    /// A listen on the bound socket `fd` with `backlog` (`IORING_OP_LISTEN`, Linux 6.11). The
    /// kernel takes at most `net.core.somaxconn`. The entry names no memory.
    pub(crate) fn listen(fd: RawFd, backlog: u32) -> Self {
        Self {
            opcode: sys::IORING_OP_LISTEN,
            fd,
            len: backlog,
            ..Self::default()
        }
    }

    /// The entry, linked to the next one, which starts only after this one succeeds. This one posts
    /// a completion only when it fails, and the next one then fails with `ECANCELED`.
    pub(crate) fn linked_quiet(self) -> Self {
        Self {
            flags: self.linked().flags | sys::IOSQE_CQE_SKIP_SUCCESS,
            ..self
        }
    }

    /// A one-shot poll of `fd` for readability (`IORING_OP_POLL_ADD`). It completes at once when
    /// `fd` is readable already. The entry names no memory.
    pub(crate) fn poll_readable(fd: RawFd) -> Self {
        Self {
            opcode: sys::IORING_OP_POLL_ADD,
            fd,
            rw_flags: sys::POLLIN,
            ..Self::default()
        }
    }

    /// A `waitid` of the type `idtype` for the identity `id`, with `options`, which writes the
    /// `siginfo_t` at `info` (`IORING_OP_WAITID`, Linux 6.7). The entry holds the type in `len`,
    /// the options in `file_index`, and the structure in `addr2` (`io_uring/waitid.c`).
    pub(crate) fn waitid(idtype: u32, id: RawFd, options: u32, info: *mut u8) -> Self {
        Self {
            opcode: sys::IORING_OP_WAITID,
            fd: id,
            len: idtype,
            splice_fd_in: options.cast_signed(),
            off: info.addr() as u64,
            ..Self::default()
        }
    }

    /// A cancel of the request whose user data is `target` (`IORING_OP_ASYNC_CANCEL`). It cancels
    /// one request. Its own completion has the user data `user_data`.
    pub(crate) fn cancel(target: u64, user_data: u64) -> Self {
        Self {
            opcode: sys::IORING_OP_ASYNC_CANCEL,
            fd: -1,
            addr: target,
            user_data,
            ..Self::default()
        }
    }

    /// A cross-ring message (`IORING_OP_MSG_RING` with `IORING_MSG_DATA`). The ring of `target`
    /// gets a completion with the user data `data` and the result `result`. The entry names no
    /// memory.
    pub(crate) fn message(target: RawFd, data: u64, result: i32, user_data: u64) -> Self {
        Self {
            opcode: sys::IORING_OP_MSG_RING,
            fd: target,
            off: data,
            addr: sys::IORING_MSG_DATA,
            len: result.cast_unsigned(),
            user_data,
            ..Self::default()
        }
    }
}

/// A shared mapping of a ring descriptor. It unmaps on drop. On macOS and Windows, it is only the
/// memory of a buffer ring that no kernel ring reads, and it frees on drop.
#[derive(Debug)]
struct Mapping {
    base: core::ptr::NonNull<u8>,
    #[cfg(target_os = "linux")]
    len: usize,
    /// The layout of the allocation, which its release needs.
    #[cfg(not(target_os = "linux"))]
    layout: core::alloc::Layout,
}

#[cfg(not(target_os = "linux"))]
impl Mapping {
    /// The alignment of the memory: a page, as a mapping has on Linux.
    const PAGE: usize = 4096;

    /// New zero memory of `len` bytes that starts at a page boundary.
    fn anonymous(len: usize) -> io::Result<Self> {
        use core::alloc::Allocator as _;

        let layout =
            core::alloc::Layout::from_size_align(len.max(1), Self::PAGE).map_err(|_| io::ErrorKind::InvalidInput)?;
        let base = alloc::alloc::Global
            .allocate_zeroed(layout)
            .map_err(|_| io::ErrorKind::OutOfMemory)?;
        Ok(Self {
            base: base.cast(),
            layout,
        })
    }
}

#[cfg(not(target_os = "linux"))]
impl Drop for Mapping {
    #[expect(unsafe_code, reason = "the memory ends with its owner")]
    fn drop(&mut self) {
        use core::alloc::Allocator as _;

        // SAFETY: `anonymous` allocated `base` from `Global` with `layout`, and no reference into
        // it outlives its owner.
        unsafe { alloc::alloc::Global.deallocate(self.base, self.layout) };
    }
}

#[cfg(target_os = "linux")]
impl Mapping {
    /// A shared mapping of the ring descriptor `fd`.
    fn new(fd: &OwnedFd, len: usize, offset: i64) -> io::Result<Self> {
        Self::map(len, sys::MAP_SHARED | sys::MAP_POPULATE, fd.as_raw_fd(), offset)
    }

    /// New zero memory of `len` bytes that starts at a page boundary.
    fn anonymous(len: usize) -> io::Result<Self> {
        Self::map(len, sys::MAP_PRIVATE | sys::MAP_ANONYMOUS, -1, 0)
    }

    #[expect(unsafe_code, reason = "a new mapping changes no memory that Rust code uses")]
    fn map(len: usize, flags: c_int, fd: RawFd, offset: i64) -> io::Result<Self> {
        // SAFETY: with no requested address, the kernel puts the mapping in unused address space,
        // so the call changes no memory that Rust code uses.
        let base = unsafe {
            sys::mmap(
                ptr::null_mut(),
                len,
                sys::PROT_READ | sys::PROT_WRITE,
                flags,
                fd,
                offset,
            )
        };
        // `MAP_FAILED` is the address `-1`.
        if base.addr() == usize::MAX {
            return Err(io::Error::last_os_error());
        }
        NonNull::new(base.cast())
            .map(|base| Self { base, len })
            .ok_or_else(|| io::ErrorKind::AddrNotAvailable.into())
    }
}

#[cfg(target_os = "linux")]
impl Drop for Mapping {
    #[expect(unsafe_code, reason = "the mapping ends with its owner")]
    fn drop(&mut self) {
        // SAFETY: `base` and `len` are a mapping that `new` made, and no reference into it outlives
        // its owner. The kernel keeps the ring until the last mapping and the descriptor close.
        unsafe { sys::munmap(self.base.as_ptr().cast(), self.len) };
    }
}

/// An io_uring ring with one issuer and deferred task running.
#[derive(Debug)]
#[cfg(target_os = "linux")]
pub(crate) struct Ring {
    /// The submission and completion rings, in one mapping (`IORING_FEAT_SINGLE_MMAP`).
    rings: Mapping,
    /// The submission entries.
    sqes: Mapping,
    fd: OwnedFd,
    /// `IORING_FEAT_*` of the kernel.
    features: u32,
    sq_mask: u32,
    sq_entries: u32,
    /// Byte offsets in `rings`.
    sq_head: usize,
    sq_tail: usize,
    sq_flags: usize,
    /// The tail after the last pushed entry. The kernel sees it at the next submit.
    sq_pushed: u32,
    cq_mask: u32,
    /// Byte offsets in `rings`.
    cq_head: usize,
    cq_tail: usize,
    cqes: usize,
    /// The completion-injection seam of the tests: with it, each take gives the completions
    /// that the test lets through, not the ones that the kernel posted since the last take.
    #[cfg(test)]
    injection: core::cell::RefCell<Option<Injection>>,
}

#[cfg(target_os = "linux")]
impl Ring {
    /// Creates a ring with `submissions` submission entries and `completions` completion entries.
    /// The kernel rounds each up to a power of two.
    pub(crate) fn new(submissions: u32, completions: u32) -> Result<Self, SetupError> {
        let mut params = sys::Params {
            cq_entries: completions,
            flags: FLAGS,
            ..sys::Params::default()
        };
        let fd = setup(submissions, &mut params).map_err(|error| match error.raw_os_error() {
            Some(sys::EPERM | sys::ENOSYS) => SetupError::Refused(error),
            _ => SetupError::Failed(error),
        })?;
        // A kernel that accepts `IORING_SETUP_DEFER_TASKRUN` (6.1) maps both rings at once (5.4)
        // and drops no completion (5.5).
        debug_assert_eq!(
            params.features & (sys::IORING_FEAT_SINGLE_MMAP | sys::IORING_FEAT_NODROP),
            sys::IORING_FEAT_SINGLE_MMAP | sys::IORING_FEAT_NODROP,
            "the kernel maps both rings at once and drops no completion"
        );
        let (sq, cq) = (&params.sq_off, &params.cq_off);
        let word = size_of::<u32>();
        // The mapping covers each word and array that the ring uses.
        let len = [
            span(sq.head, 1, word),
            span(sq.tail, 1, word),
            span(sq.flags, 1, word),
            span(sq.array, params.sq_entries, word),
            span(cq.head, 1, word),
            span(cq.tail, 1, word),
            span(cq.cqes, params.cq_entries, size_of::<Cqe>()),
        ]
        .into_iter()
        .fold(0, usize::max);
        let rings = Mapping::new(&fd, len, sys::IORING_OFF_SQ_RING).map_err(SetupError::Failed)?;
        let sqes = Mapping::new(&fd, span(0, params.sq_entries, size_of::<Sqe>()), sys::IORING_OFF_SQES)
            .map_err(SetupError::Failed)?;
        let ring = Self {
            rings,
            sqes,
            fd,
            features: params.features,
            sq_mask: params.sq_entries.wrapping_sub(1),
            sq_entries: params.sq_entries,
            sq_head: offset(sq.head),
            sq_tail: offset(sq.tail),
            sq_flags: offset(sq.flags),
            sq_pushed: 0,
            cq_mask: params.cq_entries.wrapping_sub(1),
            cq_head: offset(cq.head),
            cq_tail: offset(cq.tail),
            cqes: offset(cq.cqes),
            #[cfg(test)]
            injection: core::cell::RefCell::new(None),
        };
        // Queue position `i` names entry `i` for the life of the ring, so a push only moves the
        // tail. The kernel reads the array only at a submit.
        for index in 0..params.sq_entries {
            ring.word(span(sq.array, index, word)).store(index, Ordering::Relaxed);
        }
        Ok(ring)
    }

    /// The descriptor of the ring: the target of a cross-ring message.
    pub(crate) fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// A new descriptor of the ring. It keeps the ring open after this ring closes, so a message
    /// to it never names a descriptor number that another file took.
    #[cfg(feature = "local")]
    pub(crate) fn duplicate(&self) -> io::Result<OwnedFd> {
        self.fd.try_clone()
    }

    /// Copies `entry` to the next free entry of the submission queue. The kernel reads it at the
    /// next [`submit`](Self::submit).
    ///
    /// # Safety
    ///
    /// The memory and the descriptors that `entry` names stay valid until the kernel posts the
    /// last completion of the entry. Until then, nothing else uses memory that the kernel writes
    /// for it. If the ring drops first, the kernel can still use that memory after the drop.
    #[expect(unsafe_code, reason = "the kernel uses the memory that the entry names")]
    pub(crate) unsafe fn push(&mut self, entry: &Sqe) -> Result<(), Full> {
        let head = self.word(self.sq_head).load(Ordering::Acquire);
        if self.sq_pushed.wrapping_sub(head) == self.sq_entries {
            return Err(Full);
        }
        let index = (self.sq_pushed & self.sq_mask) as usize;
        // SAFETY: `index` is less than the number of entries in `sqes`. The entry is free: the
        // kernel copied each entry before `head` before it released `head`, and it reads no entry
        // after the published tail.
        unsafe { self.sqes.base.cast::<Sqe>().add(index).write(*entry) };
        self.sq_pushed = self.sq_pushed.wrapping_add(1);
        Ok(())
    }

    /// Publishes the pushed entries, then enters the kernel with `IORING_ENTER_GETEVENTS`. The
    /// kernel takes the entries, runs the deferred task work of the ring, and returns when `wait`
    /// completions are ready. Gives the number of entries that the kernel took.
    pub(crate) fn submit(&mut self, wait: u32) -> io::Result<u32> {
        self.word(self.sq_tail).store(self.sq_pushed, Ordering::Release);
        // A completion that the injection keeps for the next take is ready, so no wait is needed.
        #[cfg(test)]
        let wait = if self.injected_ready() { 0 } else { wait };
        enter(self.fd(), self.unsubmitted(), wait, sys::IORING_ENTER_GETEVENTS)
    }

    /// Submits as [`submit`](Self::submit) does, and waits for `wait` completions at most for
    /// `timeout` (`IORING_ENTER_EXT_ARG`). The end of the time is no error.
    #[cfg(feature = "local")]
    pub(crate) fn submit_timeout(&mut self, wait: u32, timeout: Duration) -> io::Result<u32> {
        self.word(self.sq_tail).store(self.sq_pushed, Ordering::Release);
        #[cfg(test)]
        let wait = if self.injected_ready() { 0 } else { wait };
        let ts = sys::KernelTimespec {
            tv_sec: i64::try_from(timeout.as_secs()).unwrap_or(i64::MAX),
            tv_nsec: i64::from(timeout.subsec_nanos()),
        };
        match enter_timeout(self.fd(), self.unsubmitted(), wait, &ts) {
            Err(error) if error.raw_os_error() == Some(sys::ETIME) => Ok(0),
            entered => entered,
        }
    }

    /// Whether the kernel holds deferred task work for the ring (`IORING_SQ_TASKRUN`). Only a
    /// [`submit`](Self::submit) runs it, also when no entry waits.
    pub(crate) fn task_run_pending(&self) -> bool {
        self.word(self.sq_flags).load(Ordering::Relaxed) & sys::IORING_SQ_TASKRUN != 0
    }

    /// Whether a [`submit`](Self::submit) has work with no entry to submit: deferred task work,
    /// or completions that the kernel holds because the completion queue was full
    /// (`IORING_SQ_CQ_OVERFLOW`).
    pub(crate) fn needs_enter(&self) -> bool {
        self.word(self.sq_flags).load(Ordering::Relaxed) & (sys::IORING_SQ_TASKRUN | sys::IORING_SQ_CQ_OVERFLOW) != 0
    }

    /// The entries of the submission queue.
    pub(crate) const fn entries(&self) -> u32 {
        self.sq_entries
    }

    /// The entries that the submission queue has room for.
    pub(crate) fn room(&self) -> u32 {
        let used = self
            .sq_pushed
            .wrapping_sub(self.word(self.sq_head).load(Ordering::Acquire));
        self.sq_entries.saturating_sub(used)
    }

    /// The pushed entries that the kernel has not taken.
    pub(crate) fn unsubmitted(&self) -> u32 {
        self.sq_pushed
            .wrapping_sub(self.word(self.sq_head).load(Ordering::Acquire))
    }

    /// Whether a completion is ready.
    pub(crate) fn ready(&self) -> bool {
        #[cfg(test)]
        if self.injected_ready() {
            return true;
        }
        self.word(self.cq_tail).load(Ordering::Acquire) != self.word(self.cq_head).load(Ordering::Relaxed)
    }

    /// Makes the kernel signal the event descriptor `fd` when it posts a completion or queues
    /// deferred task work (`IORING_REGISTER_EVENTFD`).
    #[expect(unsafe_code, reason = "the kernel reads the descriptor number")]
    pub(crate) fn register_eventfd(&self, fd: RawFd) -> io::Result<()> {
        let mut fd = fd;
        // SAFETY: `fd` is one `int` that is valid for reads during the call.
        unsafe { register(self.fd(), sys::IORING_REGISTER_EVENTFD, (&raw mut fd).cast(), 1) }.map(drop)
    }

    /// Ends the registration of `register_eventfd` (`IORING_UNREGISTER_EVENTFD`). A new
    /// registration then starts with a new state in the kernel.
    #[expect(unsafe_code, reason = "the call reads no argument")]
    pub(crate) fn unregister_eventfd(&self) -> io::Result<()> {
        // SAFETY: the operation reads no argument, and `nr_args` is 0.
        unsafe { register(self.fd(), sys::IORING_UNREGISTER_EVENTFD, core::ptr::null_mut(), 0) }.map(drop)
    }

    /// The completions that are ready, in order. Each entry goes back to the kernel as the
    /// iterator gives it, so a leaked iterator gives no entry two times.
    pub(crate) fn completions(&mut self) -> Completions<'_> {
        // Only this thread writes the head.
        let head = self.word(self.cq_head).load(Ordering::Relaxed);
        let tail = self.word(self.cq_tail).load(Ordering::Acquire);
        #[cfg(test)]
        if self.injection.get_mut().is_some() {
            let posted = self.peek();
            let given = self
                .injection
                .get_mut()
                .as_mut()
                .map(|injection| injection.take(posted))
                .unwrap_or_default();
            // The release keeps the reads of the entries before the kernel can reuse them.
            self.word(self.cq_head).store(tail, Ordering::Release);
            return Completions {
                ring: self,
                head: tail,
                tail,
                given,
            };
        }
        Completions {
            ring: self,
            head,
            tail,
            #[cfg(test)]
            given: alloc::collections::VecDeque::new(),
        }
    }

    /// Probes each later feature. Call it once, on a new ring with no entry, completion, or buffer
    /// group: a probe registers a buffer group and posts a completion, and it removes both before
    /// it returns.
    pub(crate) fn probe(&mut self) -> io::Result<Features> {
        let opcodes = self.opcodes()?;
        Ok(Features {
            child_wait: opcodes.supports(sys::IORING_OP_WAITID),
            truncate: opcodes.supports(sys::IORING_OP_FTRUNCATE),
            bundles: self.features & sys::IORING_FEAT_RECVSEND_BUNDLE != 0,
            bind: opcodes.supports(sys::IORING_OP_BIND),
            listen: opcodes.supports(sys::IORING_OP_LISTEN),
            incremental_buffer_rings: self.probe_incremental_buffer_ring(),
            ringless_messages: self.probe_ringless_message()?,
            zero_copy_receive: opcodes.supports(sys::IORING_OP_RECV_ZC),
        })
    }

    /// The opcode table of the kernel (`IORING_REGISTER_PROBE`).
    #[expect(unsafe_code, reason = "the kernel writes the opcode table into the probe")]
    fn opcodes(&self) -> io::Result<Opcodes> {
        let mut probe = sys::Probe::ZERO;
        // SAFETY: `probe` is a zero `io_uring_probe` with room for `PROBE_OPS` entries, and it is
        // valid for writes during the call.
        unsafe {
            register(
                self.fd(),
                sys::IORING_REGISTER_PROBE,
                (&raw mut probe).cast(),
                sys::PROBE_OPS,
            )
        }?;
        Ok(Opcodes(probe))
    }

    /// Registers a buffer ring with `IOU_PBUF_RING_INC`, then removes it. An earlier kernel refuses
    /// the flag with `EINVAL`.
    #[expect(unsafe_code, reason = "the kernel reads the buffer ring registration")]
    fn probe_incremental_buffer_ring(&self) -> bool {
        let mut ring = sys::BufReg {
            ring_entries: 1,
            bgid: PROBE_GROUP,
            flags: sys::IOU_PBUF_RING_MMAP | sys::IOU_PBUF_RING_INC,
            ..sys::BufReg::default()
        };
        // SAFETY: `ring` is a valid `io_uring_buf_reg` during the call. With `IOU_PBUF_RING_MMAP`,
        // the kernel allocates the ring memory, so the registration names no memory of ours.
        if unsafe { register(self.fd(), sys::IORING_REGISTER_PBUF_RING, (&raw mut ring).cast(), 1) }.is_err() {
            return false;
        }
        let mut group = sys::BufReg {
            bgid: PROBE_GROUP,
            ..sys::BufReg::default()
        };
        // SAFETY: as above. The kernel frees the ring memory that it allocated.
        unsafe { register(self.fd(), sys::IORING_UNREGISTER_PBUF_RING, (&raw mut group).cast(), 1) }.is_ok()
    }

    /// Sends a message to this ring with no source ring, then takes its completion. An earlier
    /// kernel refuses the descriptor `-1` with `EBADF`.
    #[expect(unsafe_code, reason = "the kernel reads the message entry")]
    fn probe_ringless_message(&mut self) -> io::Result<bool> {
        let mut message = Sqe::message(self.fd(), PROBE_MESSAGE, 0, 0);
        // SAFETY: `message` is a whole `io_uring_sqe` that names no memory, and the kernel copies it
        // during the call.
        if unsafe { register(-1, sys::IORING_REGISTER_SEND_MSG_RING, (&raw mut message).cast(), 1) }.is_err() {
            return Ok(false);
        }
        // The kernel owes this ring one completion. Take it, so that the probe leaves no entry.
        loop {
            match self.submit(1) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => {
                    result?;
                    break;
                }
            }
        }
        Ok(self.completions().filter(|cqe| cqe.user_data == PROBE_MESSAGE).count() == 1)
    }

    /// The ready completions, which stay in the ring for the next take: a test reads them before
    /// the driver takes them.
    #[cfg(test)]
    pub(crate) fn peek(&self) -> alloc::vec::Vec<Cqe> {
        // Only this thread writes the head.
        let head = self.word(self.cq_head).load(Ordering::Relaxed);
        let tail = self.word(self.cq_tail).load(Ordering::Acquire);
        (0..tail.wrapping_sub(head))
            .map(|index| self.cqe((head.wrapping_add(index) & self.cq_mask) as usize))
            .collect()
    }

    /// The shared word at byte `offset` of the ring mapping.
    #[expect(unsafe_code, reason = "the kernel shares the words of the ring mapping")]
    fn word(&self, offset: usize) -> &AtomicU32 {
        // SAFETY: `new` made the mapping cover each offset that it gives, and the kernel puts each
        // word of `struct io_rings` and each array entry at a multiple of 4 from the page-aligned
        // base. The mapping lives as long as `self`, and the kernel accesses the words only with
        // atomic operations.
        unsafe { AtomicU32::from_ptr(self.rings.base.as_ptr().add(offset).cast()) }
    }

    /// The completion entry at `index`, which the kernel has published.
    #[expect(unsafe_code, reason = "the kernel writes the completion entries")]
    fn cqe(&self, index: usize) -> Cqe {
        // SAFETY: `index` is less than the number of completion entries, which the mapping covers
        // from the 8-aligned offset `cqes`. The kernel wrote the entry before it released the tail
        // that `completions` acquired, and it does not write the entry again before the head
        // passes it.
        unsafe { self.rings.base.add(self.cqes).cast::<Cqe>().add(index).read() }
    }
}

/// The ready completions of a ring. See [`Ring::completions`].
#[derive(Debug)]
#[cfg(target_os = "linux")]
pub(crate) struct Completions<'ring> {
    ring: &'ring Ring,
    head: u32,
    tail: u32,
    /// The completions that the injection gives in this take.
    #[cfg(test)]
    given: alloc::collections::VecDeque<Cqe>,
}

#[cfg(target_os = "linux")]
impl Iterator for Completions<'_> {
    type Item = Cqe;

    fn next(&mut self) -> Option<Cqe> {
        #[cfg(test)]
        if let Some(cqe) = self.given.pop_front() {
            return Some(cqe);
        }
        if self.head == self.tail {
            return None;
        }
        let cqe = self.ring.cqe((self.head & self.ring.cq_mask) as usize);
        self.head = self.head.wrapping_add(1);
        // The release keeps the read of the entry before the kernel can reuse it.
        self.ring.word(self.ring.cq_head).store(self.head, Ordering::Release);
        Some(cqe)
    }
}

/// A completion that the take did not give stays for the next take, first.
#[cfg(all(test, target_os = "linux"))]
impl Drop for Completions<'_> {
    fn drop(&mut self) {
        if let Some(injection) = self.ring.injection.borrow_mut().as_mut() {
            while let Some(cqe) = self.given.pop_back() {
                injection.due.push_front(cqe);
            }
        }
    }
}

/// The completion-injection seam of the driver tests, at the boundary of the backend. With it, a
/// completion that the kernel posts reaches the driver at the take after the one that found it, so
/// the driver applies each request of that turn first. A test can also hold the completions of one
/// operation until it releases them, replace a result, and add a completion that the kernel never
/// posted. It changes no transition of the table or the driver: it only chooses their inputs. It
/// exists only in the tests, and no public injection exists.
#[cfg(all(test, target_os = "linux"))]
#[derive(Debug, Default)]
pub(crate) struct Injection {
    /// The completions that the next take gives, in order.
    due: alloc::collections::VecDeque<Cqe>,
    /// The completions of held user data, which wait for their release.
    held: alloc::vec::Vec<Cqe>,
    /// The user data whose completions the injection holds.
    holds: alloc::vec::Vec<u64>,
    /// The results that replace the result of the next completion of their user data.
    results: alloc::vec::Vec<(u64, i32)>,
}

#[cfg(all(test, target_os = "linux"))]
impl Injection {
    /// Gives the completions due now, and keeps the `posted` ones for the next take.
    fn take(&mut self, posted: alloc::vec::Vec<Cqe>) -> alloc::collections::VecDeque<Cqe> {
        let given = core::mem::take(&mut self.due);
        for mut cqe in posted {
            if let Some(index) = self.results.iter().position(|&(data, _)| data == cqe.user_data) {
                cqe.res = self.results.swap_remove(index).1;
            }
            if self.holds.contains(&cqe.user_data) {
                self.held.push(cqe);
            } else {
                self.due.push_back(cqe);
            }
        }
        given
    }
}

#[cfg(all(test, target_os = "linux"))]
impl Ring {
    /// Starts the injection: from now on, each take gives the completions that the injection lets
    /// through.
    pub(crate) fn inject(&mut self) {
        *self.injection.get_mut() = Some(Injection::default());
    }

    /// Whether the injection keeps a completion for the next take.
    fn injected_ready(&self) -> bool {
        self.injection
            .borrow()
            .as_ref()
            .is_some_and(|injection| !injection.due.is_empty())
    }

    /// The injection, which [`Ring::inject`] started.
    fn injection(&mut self) -> &mut Injection {
        self.injection
            .get_mut()
            .as_mut()
            .expect("the test started the injection")
    }

    /// Holds each later completion of `user_data` until [`Ring::release`].
    pub(crate) fn hold(&mut self, user_data: u64) {
        self.injection().holds.push(user_data);
    }

    /// Ends the hold of `user_data`: its held completions are due at the next take, in order.
    pub(crate) fn release(&mut self, user_data: u64) {
        let injection = self.injection();
        injection.holds.retain(|&data| data != user_data);
        let (released, held) = injection
            .held
            .drain(..)
            .partition::<alloc::vec::Vec<_>, _>(|cqe| cqe.user_data == user_data);
        injection.held = held;
        injection.due.extend(released);
    }

    /// Replaces the result of the next completion of `user_data` with `res`.
    pub(crate) fn replace(&mut self, user_data: u64, res: i32) {
        self.injection().results.push((user_data, res));
    }

    /// Makes `cqe`, which the kernel did not post, due at the next take.
    pub(crate) fn add(&mut self, cqe: Cqe) {
        self.injection().due.push_back(cqe);
    }
}

impl Cqe {
    /// The buffer that the kernel selected from a provided buffer ring for this completion
    /// (`IORING_CQE_F_BUFFER`).
    pub(crate) fn buffer(&self) -> Option<u16> {
        (self.flags & sys::IORING_CQE_F_BUFFER != 0).then_some((self.flags >> sys::IORING_CQE_BUFFER_SHIFT) as u16)
    }

    /// The completion in the terms of the operation table.
    pub(crate) fn event(&self) -> Event {
        Event {
            result: self.res,
            more: self.flags & sys::IORING_CQE_F_MORE != 0,
            notice: self.flags & sys::IORING_CQE_F_NOTIF != 0,
        }
    }
}

/// The opcode table that `IORING_REGISTER_PROBE` wrote.
#[derive(Debug)]
#[cfg(target_os = "linux")]
struct Opcodes(sys::Probe);

#[cfg(target_os = "linux")]
impl Opcodes {
    fn supports(&self, opcode: u8) -> bool {
        opcode < self.0.ops_len && self.0.ops[usize::from(opcode)].flags & sys::IO_URING_OP_SUPPORTED != 0
    }
}

/// The byte offset of a kernel offset.
#[cfg(target_os = "linux")]
fn offset(kernel: u32) -> usize {
    kernel as usize
}

/// The end of `count` items of `size` bytes from the kernel offset `start`. The kernel gives at
/// most 32768 submission and 65536 completion entries, so the result cannot overflow.
#[cfg(target_os = "linux")]
fn span(start: u32, count: u32, size: usize) -> usize {
    offset(start) + count as usize * size
}

/// A system call result: a negative result is the error in `errno`.
#[cfg(target_os = "linux")]
fn result(value: c_long) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| io::Error::last_os_error())
}

/// `io_uring_setup(entries, params)`.
#[expect(unsafe_code, reason = "the kernel returns a new descriptor")]
#[cfg(target_os = "linux")]
fn setup(entries: u32, params: &mut sys::Params) -> io::Result<OwnedFd> {
    // SAFETY: `params` is a valid `io_uring_params` for reads and writes during the call.
    let fd = result(unsafe { sys::syscall(sys::SYS_IO_URING_SETUP, c_long::from(entries), ptr::from_mut(params)) })?;
    // SAFETY: the kernel returned a new descriptor that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd.cast_signed()) })
}

/// `io_uring_enter(fd, to_submit, min_complete, flags, NULL, 0)`.
#[expect(unsafe_code, reason = "the kernel reads the published submission entries")]
#[cfg(target_os = "linux")]
fn enter(fd: RawFd, to_submit: u32, min_complete: u32, flags: u32) -> io::Result<u32> {
    // SAFETY: with no argument pointer, the call uses only the ring of `fd`. Each published entry
    // obeys the contract of `Ring::push`.
    result(unsafe {
        sys::syscall(
            sys::SYS_IO_URING_ENTER,
            c_long::from(fd),
            c_long::from(to_submit),
            c_long::from(min_complete),
            c_long::from(flags),
            ptr::null::<c_void>(),
            0_usize,
        )
    })
}

/// `io_uring_enter(fd, to_submit, min_complete, IORING_ENTER_GETEVENTS | IORING_ENTER_EXT_ARG,
/// &arg, sizeof(arg))`, with no signal mask and the relative timeout `ts`.
#[cfg(all(feature = "local", target_os = "linux"))]
#[expect(
    unsafe_code,
    reason = "the kernel reads the published entries and the argument of the wait"
)]
fn enter_timeout(fd: RawFd, to_submit: u32, min_complete: u32, ts: &sys::KernelTimespec) -> io::Result<u32> {
    let arg = sys::GeteventsArg {
        ts: ptr::from_ref(ts).addr() as u64,
        ..sys::GeteventsArg::default()
    };
    // SAFETY: `arg` and the timespec that it names are valid for reads during the call, and the
    // call reads no other memory but the ring of `fd`. Each published entry obeys the contract of
    // `Ring::push`.
    result(unsafe {
        sys::syscall(
            sys::SYS_IO_URING_ENTER,
            c_long::from(fd),
            c_long::from(to_submit),
            c_long::from(min_complete),
            c_long::from(sys::IORING_ENTER_GETEVENTS | sys::IORING_ENTER_EXT_ARG),
            ptr::from_ref(&arg),
            size_of::<sys::GeteventsArg>(),
        )
    })
}

/// `io_uring_register(fd, opcode, arg, nr_args)`.
///
/// # Safety
///
/// `arg` points to `nr_args` values of the type that `opcode` reads or writes, valid for that
/// access during the call.
#[expect(unsafe_code, reason = "the kernel reads or writes the argument of the operation")]
#[cfg(target_os = "linux")]
unsafe fn register(fd: RawFd, opcode: u32, arg: *mut c_void, nr_args: u32) -> io::Result<u32> {
    // SAFETY: the caller makes `arg` valid for `opcode` and `nr_args`.
    result(unsafe {
        sys::syscall(
            sys::SYS_IO_URING_REGISTER,
            c_long::from(fd),
            c_long::from(opcode),
            arg,
            c_long::from(nr_args),
        )
    })
}
