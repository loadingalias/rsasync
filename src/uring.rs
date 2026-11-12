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

mod sys;
#[cfg(all(test, not(loom)))]
mod table_tests;
#[cfg(test)]
mod tests;

use core::ffi::{c_long, c_void};
use core::mem::size_of;
use core::ptr::{self, NonNull};
use core::sync::atomic::{AtomicU32, Ordering};
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};

pub(crate) use sys::{Cqe, Sqe};

use crate::table::Event;

/// The setup flags of each ring.
const FLAGS: u32 = sys::IORING_SETUP_SINGLE_ISSUER
    | sys::IORING_SETUP_DEFER_TASKRUN
    | sys::IORING_SETUP_TASKRUN_FLAG
    | sys::IORING_SETUP_CQSIZE;

/// The buffer group that the probe of incremental buffer rings registers and removes again.
const PROBE_GROUP: u16 = u16::MAX;

/// The user data of the message that the probe of ringless messages sends to its own ring.
const PROBE_MESSAGE: u64 = u64::MAX;

/// Why a ring was not created.
#[derive(Debug)]
pub(crate) enum SetupError {
    /// `EPERM` or `ENOSYS`: the kernel, a seccomp profile, or `kernel.io_uring_disabled` refuses
    /// io_uring. The runtime then selects epoll.
    Refused(io::Error),
    /// Another failure, for example a number of entries that the kernel refuses, or no memory.
    Failed(io::Error),
}

/// Each entry of the submission queue waits for the kernel.
#[derive(Debug)]
pub(crate) struct Full;

/// The later kernel features, each probed on the ring, not read from the kernel version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// A shared mapping of a ring descriptor. It unmaps on drop.
#[derive(Debug)]
struct Mapping {
    base: NonNull<u8>,
    len: usize,
}

impl Mapping {
    #[expect(
        unsafe_code,
        reason = "a new mapping of the ring changes no memory that Rust code uses"
    )]
    fn new(fd: &OwnedFd, len: usize, offset: i64) -> io::Result<Self> {
        // SAFETY: with no requested address, the kernel puts the mapping in unused address space,
        // so the call changes no memory that Rust code uses.
        let base = unsafe {
            sys::mmap(
                ptr::null_mut(),
                len,
                sys::PROT_READ | sys::PROT_WRITE,
                sys::MAP_SHARED | sys::MAP_POPULATE,
                fd.as_raw_fd(),
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
}

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
        let pending = self
            .sq_pushed
            .wrapping_sub(self.word(self.sq_head).load(Ordering::Acquire));
        enter(self.fd(), pending, wait, sys::IORING_ENTER_GETEVENTS)
    }

    /// Whether the kernel holds deferred task work for the ring (`IORING_SQ_TASKRUN`). Only a
    /// [`submit`](Self::submit) runs it, also when no entry waits.
    pub(crate) fn task_run_pending(&self) -> bool {
        self.word(self.sq_flags).load(Ordering::Relaxed) & sys::IORING_SQ_TASKRUN != 0
    }

    /// The completions that are ready, in order. Each entry goes back to the kernel as the
    /// iterator gives it, so a leaked iterator gives no entry two times.
    pub(crate) fn completions(&mut self) -> Completions<'_> {
        // Only this thread writes the head.
        let head = self.word(self.cq_head).load(Ordering::Relaxed);
        let tail = self.word(self.cq_tail).load(Ordering::Acquire);
        Completions { ring: self, head, tail }
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
pub(crate) struct Completions<'ring> {
    ring: &'ring Ring,
    head: u32,
    tail: u32,
}

impl Iterator for Completions<'_> {
    type Item = Cqe;

    fn next(&mut self) -> Option<Cqe> {
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

impl Cqe {
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
struct Opcodes(sys::Probe);

impl Opcodes {
    fn supports(&self, opcode: u8) -> bool {
        opcode < self.0.ops_len && self.0.ops[usize::from(opcode)].flags & sys::IO_URING_OP_SUPPORTED != 0
    }
}

/// The byte offset of a kernel offset.
fn offset(kernel: u32) -> usize {
    kernel as usize
}

/// The end of `count` items of `size` bytes from the kernel offset `start`. The kernel gives at
/// most 32768 submission and 65536 completion entries, so the result cannot overflow.
fn span(start: u32, count: u32, size: usize) -> usize {
    offset(start) + count as usize * size
}

/// A system call result: a negative result is the error in `errno`.
fn result(value: c_long) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| io::Error::last_os_error())
}

/// `io_uring_setup(entries, params)`.
#[expect(unsafe_code, reason = "the kernel returns a new descriptor")]
fn setup(entries: u32, params: &mut sys::Params) -> io::Result<OwnedFd> {
    // SAFETY: `params` is a valid `io_uring_params` for reads and writes during the call.
    let fd = result(unsafe { sys::syscall(sys::SYS_IO_URING_SETUP, c_long::from(entries), ptr::from_mut(params)) })?;
    // SAFETY: the kernel returned a new descriptor that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd.cast_signed()) })
}

/// `io_uring_enter(fd, to_submit, min_complete, flags, NULL, 0)`.
#[expect(unsafe_code, reason = "the kernel reads the published submission entries")]
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

/// `io_uring_register(fd, opcode, arg, nr_args)`.
///
/// # Safety
///
/// `arg` points to `nr_args` values of the type that `opcode` reads or writes, valid for that
/// access during the call.
#[expect(unsafe_code, reason = "the kernel reads or writes the argument of the operation")]
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
