//! The io_uring declarations in use: three system calls, two mapping calls, and the kernel
//! structures and constants of the ring.
//!
//! Each item agrees with `include/uapi/linux/io_uring.h` of Linux 6.1 and of Linux 7.0, the kernel
//! of Ubuntu 26.04. The system call numbers come from `include/uapi/asm-generic/unistd.h`
//! (aarch64) and `arch/x86/entry/syscalls/syscall_64.tbl` (x86_64) of Linux 6.1, and they are the
//! same on both. The mapping and error constants come from `include/uapi/asm-generic/mman.h`,
//! `mman-common.h`, `errno-base.h`, and `errno.h`, which x86_64 and aarch64 both use. The C
//! library, glibc or musl, gives the calls.

use core::ffi::{c_int, c_long, c_void};
use core::mem::{offset_of, size_of};

#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    /// `long syscall(long number, ...)`. It returns -1 and sets `errno` on failure.
    pub(super) fn syscall(number: c_long, ...) -> c_long;
    /// `void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)`, with a
    /// 64-bit `off_t` on both targets.
    pub(super) fn mmap(
        addr: *mut c_void,
        length: usize,
        prot: c_int,
        flags: c_int,
        fd: c_int,
        offset: i64,
    ) -> *mut c_void;
    /// `int munmap(void *addr, size_t length)`.
    pub(super) fn munmap(addr: *mut c_void, length: usize) -> c_int;
}

pub(super) const SYS_IO_URING_SETUP: c_long = 425;
pub(super) const SYS_IO_URING_ENTER: c_long = 426;
pub(super) const SYS_IO_URING_REGISTER: c_long = 427;

pub(super) const EPERM: i32 = 1;
pub(super) const ENOSYS: i32 = 38;

pub(super) const PROT_READ: c_int = 0x1;
pub(super) const PROT_WRITE: c_int = 0x2;
pub(super) const MAP_SHARED: c_int = 0x01;
pub(super) const MAP_PRIVATE: c_int = 0x02;
pub(super) const MAP_ANONYMOUS: c_int = 0x20;
pub(super) const MAP_POPULATE: c_int = 0x00_8000;

pub(super) const IORING_SETUP_CQSIZE: u32 = 1 << 3;
pub(super) const IORING_SETUP_TASKRUN_FLAG: u32 = 1 << 9;
pub(super) const IORING_SETUP_SINGLE_ISSUER: u32 = 1 << 12;
pub(super) const IORING_SETUP_DEFER_TASKRUN: u32 = 1 << 13;

pub(super) const IORING_FEAT_SINGLE_MMAP: u32 = 1 << 0;
pub(super) const IORING_FEAT_NODROP: u32 = 1 << 1;
/// Send and receive bundles, Linux 6.10.
pub(super) const IORING_FEAT_RECVSEND_BUNDLE: u32 = 1 << 14;

pub(super) const IORING_SQ_CQ_OVERFLOW: u32 = 1 << 1;
pub(super) const IORING_SQ_TASKRUN: u32 = 1 << 2;
pub(super) const IORING_ENTER_GETEVENTS: u32 = 1 << 0;

pub(super) const IORING_OFF_SQ_RING: i64 = 0;
pub(super) const IORING_OFF_SQES: i64 = 0x1000_0000;

pub(super) const IORING_REGISTER_EVENTFD: u32 = 4;
pub(super) const IORING_UNREGISTER_EVENTFD: u32 = 5;
pub(super) const IORING_REGISTER_PROBE: u32 = 8;
pub(super) const IORING_REGISTER_PBUF_RING: u32 = 22;
pub(super) const IORING_UNREGISTER_PBUF_RING: u32 = 23;
/// A cross-ring message from a thread with no ring, Linux 6.13.
pub(super) const IORING_REGISTER_SEND_MSG_RING: u32 = 31;

/// The kernel allocates the buffer ring, Linux 6.4.
pub(super) const IOU_PBUF_RING_MMAP: u16 = 1;
/// Incremental buffer consumption, Linux 6.12.
pub(super) const IOU_PBUF_RING_INC: u16 = 2;

pub(super) const IO_URING_OP_SUPPORTED: u16 = 1 << 0;

pub(super) const IORING_OP_NOP: u8 = 0;
pub(super) const IORING_OP_POLL_ADD: u8 = 6;
pub(super) const IORING_OP_ACCEPT: u8 = 13;
pub(super) const IORING_OP_ASYNC_CANCEL: u8 = 14;
pub(super) const IORING_OP_READV: u8 = 1;
pub(super) const IORING_OP_WRITEV: u8 = 2;
pub(super) const IORING_OP_FSYNC: u8 = 3;
pub(super) const IORING_OP_FALLOCATE: u8 = 17;
pub(super) const IORING_OP_OPENAT: u8 = 18;
pub(super) const IORING_OP_STATX: u8 = 21;
pub(super) const IORING_OP_RENAMEAT: u8 = 35;
pub(super) const IORING_OP_UNLINKAT: u8 = 36;
pub(super) const IORING_OP_READ: u8 = 22;
pub(super) const IORING_OP_WRITE: u8 = 23;
pub(super) const IORING_OP_SENDMSG: u8 = 9;
pub(super) const IORING_OP_RECVMSG: u8 = 10;
pub(super) const IORING_OP_CONNECT: u8 = 16;
pub(super) const IORING_OP_SHUTDOWN: u8 = 34;
pub(super) const IORING_OP_SEND: u8 = 26;
pub(super) const IORING_OP_RECV: u8 = 27;
pub(super) const IORING_OP_MSG_RING: u8 = 40;
/// Linux 6.7.
pub(super) const IORING_OP_WAITID: u8 = 50;
/// Linux 6.9.
pub(super) const IORING_OP_FTRUNCATE: u8 = 55;
/// Linux 6.11.
pub(super) const IORING_OP_BIND: u8 = 56;
/// Linux 6.11.
pub(super) const IORING_OP_LISTEN: u8 = 57;
/// Linux 6.15.
pub(super) const IORING_OP_RECV_ZC: u8 = 58;

/// `IOSQE_IO_LINK`: the next entry starts after this one succeeds, and fails with `ECANCELED`
/// when this one fails.
pub(super) const IOSQE_IO_LINK: u8 = 1 << 2;
pub(super) const IOSQE_BUFFER_SELECT: u8 = 1 << 5;
/// `IOSQE_CQE_SKIP_SUCCESS` (Linux 5.17): the entry posts a completion only when it fails.
pub(super) const IOSQE_CQE_SKIP_SUCCESS: u8 = 1 << 6;

pub(super) const IORING_CQE_F_BUFFER: u32 = 1 << 0;
pub(super) const IORING_CQE_BUFFER_SHIFT: u32 = 16;
pub(super) const IORING_CQE_F_MORE: u32 = 1 << 1;
pub(super) const IORING_CQE_F_NOTIF: u32 = 1 << 3;

/// `POLLIN` (`include/uapi/asm-generic/poll.h`): the descriptor is readable.
pub(super) const POLLIN: u32 = 0x1;
/// `MSG_NOSIGNAL` (`include/linux/socket.h`): a send to a closed peer gives `EPIPE` and raises no
/// `SIGPIPE`.
pub(super) const MSG_NOSIGNAL: u32 = 0x4000;
/// The flags of a new descriptor, from their declarations in `rsnetwork_udp::sys`, as the `u32`
/// flags of an entry.
pub(super) const SOCK_NONBLOCK: u32 = rsnetwork_udp::sys::SOCK_NONBLOCK.cast_unsigned();
pub(super) const SOCK_CLOEXEC: u32 = rsnetwork_udp::sys::SOCK_CLOEXEC.cast_unsigned();

/// `IORING_FSYNC_DATASYNC`: the sync of `IORING_OP_FSYNC` is `fdatasync`, not `fsync`.
pub(super) const IORING_FSYNC_DATASYNC: u32 = 1 << 0;

/// The command of `IORING_OP_MSG_RING` that posts `len` and `off` as a completion of the target.
pub(super) const IORING_MSG_DATA: u64 = 0;

/// `struct io_sqring_offsets`. Linux 6.5 renamed `resv2` to `user_addr`.
#[repr(C)]
#[derive(Debug, Default)]
pub(super) struct SqOffsets {
    pub(super) head: u32,
    pub(super) tail: u32,
    pub(super) ring_mask: u32,
    pub(super) ring_entries: u32,
    pub(super) flags: u32,
    pub(super) dropped: u32,
    pub(super) array: u32,
    pub(super) resv1: u32,
    pub(super) user_addr: u64,
}

/// `struct io_cqring_offsets`. Linux 6.5 renamed `resv2` to `user_addr`.
#[repr(C)]
#[derive(Debug, Default)]
pub(super) struct CqOffsets {
    pub(super) head: u32,
    pub(super) tail: u32,
    pub(super) ring_mask: u32,
    pub(super) ring_entries: u32,
    pub(super) overflow: u32,
    pub(super) cqes: u32,
    pub(super) flags: u32,
    pub(super) resv1: u32,
    pub(super) user_addr: u64,
}

/// `struct io_uring_params`.
#[repr(C)]
#[derive(Debug, Default)]
pub(super) struct Params {
    pub(super) sq_entries: u32,
    pub(super) cq_entries: u32,
    pub(super) flags: u32,
    pub(super) sq_thread_cpu: u32,
    pub(super) sq_thread_idle: u32,
    pub(super) features: u32,
    pub(super) wq_fd: u32,
    pub(super) resv: [u32; 3],
    pub(super) sq_off: SqOffsets,
    pub(super) cq_off: CqOffsets,
}

/// `struct io_uring_sqe`, 64 bytes. Each field has the name of the first member of its union.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Sqe {
    pub(super) opcode: u8,
    pub(super) flags: u8,
    pub(super) ioprio: u16,
    pub(super) fd: i32,
    pub(super) off: u64,
    pub(super) addr: u64,
    pub(super) len: u32,
    pub(super) rw_flags: u32,
    pub(super) user_data: u64,
    pub(super) buf_index: u16,
    pub(super) personality: u16,
    pub(super) splice_fd_in: i32,
    pub(super) addr3: u64,
    pub(super) pad2: u64,
}

/// `struct io_uring_cqe`, 16 bytes: a ring without `IORING_SETUP_CQE32`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cqe {
    pub(crate) user_data: u64,
    pub(crate) res: i32,
    pub(crate) flags: u32,
}

/// `struct io_uring_probe_op`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) struct ProbeOp {
    pub(super) op: u8,
    pub(super) resv: u8,
    pub(super) flags: u16,
    pub(super) resv2: u32,
}

/// The number of entries in [`Probe`]: one for each value of the `u8` opcode.
pub(super) const PROBE_OPS: u32 = 256;

/// `struct io_uring_probe` with room for every opcode.
#[repr(C)]
#[derive(Debug)]
pub(super) struct Probe {
    pub(super) last_op: u8,
    pub(super) ops_len: u8,
    pub(super) resv: u16,
    pub(super) resv2: [u32; 3],
    pub(super) ops: [ProbeOp; PROBE_OPS as usize],
}

impl Probe {
    /// The kernel refuses a probe whose memory is not zero.
    pub(super) const ZERO: Self = Self {
        last_op: 0,
        ops_len: 0,
        resv: 0,
        resv2: [0; 3],
        ops: [ProbeOp {
            op: 0,
            resv: 0,
            flags: 0,
            resv2: 0,
        }; PROBE_OPS as usize],
    };
}

/// `struct io_uring_buf`: one buffer of a provided buffer ring, 16 bytes. Its last field, `resv`,
/// overlays the tail of the ring in the first entry, so the driver never writes it.
pub(super) const BUF_SIZE: usize = 16;
pub(super) const BUF_LEN: usize = 8;
pub(super) const BUF_BID: usize = 12;
/// The byte offset of the tail in `struct io_uring_buf_ring`.
pub(super) const BUF_RING_TAIL: usize = 14;

/// `struct io_uring_buf_reg`. Linux 6.4 renamed `pad` to `flags`.
#[repr(C)]
#[derive(Debug, Default)]
pub(super) struct BufReg {
    pub(super) ring_addr: u64,
    pub(super) ring_entries: u32,
    pub(super) bgid: u16,
    pub(super) flags: u16,
    pub(super) resv: [u64; 3],
}

const _: () = {
    assert!(size_of::<SqOffsets>() == 40);
    assert!(offset_of!(SqOffsets, flags) == 16);
    assert!(offset_of!(SqOffsets, array) == 24);
    assert!(offset_of!(SqOffsets, user_addr) == 32);
    assert!(size_of::<CqOffsets>() == 40);
    assert!(offset_of!(CqOffsets, cqes) == 20);
    assert!(offset_of!(CqOffsets, user_addr) == 32);
    assert!(size_of::<Params>() == 120);
    assert!(offset_of!(Params, features) == 20);
    assert!(offset_of!(Params, sq_off) == 40);
    assert!(offset_of!(Params, cq_off) == 80);
    assert!(size_of::<Sqe>() == 64);
    assert!(offset_of!(Sqe, fd) == 4);
    assert!(offset_of!(Sqe, off) == 8);
    assert!(offset_of!(Sqe, addr) == 16);
    assert!(offset_of!(Sqe, len) == 24);
    assert!(offset_of!(Sqe, rw_flags) == 28);
    assert!(offset_of!(Sqe, user_data) == 32);
    assert!(offset_of!(Sqe, buf_index) == 40);
    assert!(offset_of!(Sqe, personality) == 42);
    assert!(offset_of!(Sqe, splice_fd_in) == 44);
    assert!(offset_of!(Sqe, addr3) == 48);
    assert!(size_of::<Cqe>() == 16);
    assert!(offset_of!(Cqe, res) == 8);
    assert!(offset_of!(Cqe, flags) == 12);
    assert!(size_of::<ProbeOp>() == 8);
    assert!(offset_of!(ProbeOp, flags) == 2);
    assert!(offset_of!(Probe, ops) == 16);
    assert!(size_of::<BufReg>() == 40);
    assert!(offset_of!(BufReg, bgid) == 12);
    assert!(offset_of!(BufReg, flags) == 14);
};
