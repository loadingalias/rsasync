//! The declarations of the wake descriptor, `eventfd`, of `poll`, which the probe of the
//! descriptor uses, of `fcntl`, which the adoption of a pipe end uses, and of the file calls, from
//! the C library, glibc or musl.
//!
//! Each item agrees with `sys/eventfd.h`, `poll.h`, and `fcntl.h` of glibc and musl, and with
//! `include/uapi/asm-generic/fcntl.h` and `include/uapi/asm-generic/poll.h` of Linux 6.1, which
//! x86_64 and aarch64 both use. The file items agree with `include/uapi/linux/fcntl.h`,
//! `include/uapi/linux/fs.h`, `include/uapi/linux/falloc.h`, and `include/uapi/linux/stat.h` of
//! Linux 6.1 and 7.0. Two open flags have other values on aarch64, from
//! `arch/arm64/include/uapi/asm/fcntl.h`, and the number of `statx` comes from
//! `arch/x86/entry/syscalls/syscall_64.tbl` (x86_64) and `include/uapi/asm-generic/unistd.h`
//! (aarch64).

use core::ffi::{c_int, c_long, c_short, c_uint, c_ulong};

#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    /// `int eventfd(unsigned int initval, int flags)`.
    pub(super) fn eventfd(initval: c_uint, flags: c_int) -> c_int;
    /// `int poll(struct pollfd *fds, nfds_t nfds, int timeout)`, with `nfds_t` an
    /// `unsigned long`.
    pub(super) fn poll(fds: *mut PollFd, nfds: c_ulong, timeout: c_int) -> c_int;
}

/// `struct pollfd`.
#[repr(C)]
pub(super) struct PollFd {
    pub(super) fd: c_int,
    pub(super) events: c_short,
    pub(super) revents: c_short,
}

pub(super) const POLLIN: c_short = 0x1;

const _: () = {
    assert!(size_of::<PollFd>() == 8);
    assert!(core::mem::offset_of!(PollFd, events) == 4);
    assert!(core::mem::offset_of!(PollFd, revents) == 6);
};

/// `EFD_CLOEXEC` is `O_CLOEXEC`, and `EFD_NONBLOCK` is `O_NONBLOCK`.
pub(super) const EFD_CLOEXEC: c_int = 0o2_000_000;
pub(super) const EFD_NONBLOCK: c_int = 0o4_000;

#[expect(unsafe_code, reason = "the C library of each Linux target gives this function")]
unsafe extern "C" {
    /// `int fcntl(int fd, int cmd, ...)`.
    pub(super) fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
}

/// The commands of `fcntl` (`include/uapi/asm-generic/fcntl.h` and `include/uapi/linux/fcntl.h`):
/// the file status flags, and the capacity of a pipe, which only a pipe or a FIFO has.
pub(super) const F_GETFL: c_int = 3;
pub(super) const F_SETFL: c_int = 4;
pub(super) const F_GETPIPE_SZ: c_int = 1032;
/// The access mode of the file status flags, and its values.
pub(super) const O_ACCMODE: c_int = 0o3;
pub(super) const O_RDONLY: c_int = 0o0;
pub(super) const O_WRONLY: c_int = 0o1;
pub(super) const O_RDWR: c_int = 0o2;
pub(super) const O_NONBLOCK: c_int = 0o4_000;

/// The open flags of a file (`include/uapi/asm-generic/fcntl.h`).
pub(super) const O_CREAT: c_int = 0o100;
pub(super) const O_EXCL: c_int = 0o200;
pub(super) const O_TRUNC: c_int = 0o1_000;
pub(super) const O_CLOEXEC: c_int = 0o2_000_000;
#[cfg(target_arch = "x86_64")]
pub(super) const O_DIRECT: c_int = 0o40_000;
#[cfg(target_arch = "aarch64")]
pub(super) const O_DIRECT: c_int = 0o200_000;
#[cfg(target_arch = "x86_64")]
pub(super) const O_DIRECTORY: c_int = 0o200_000;
#[cfg(target_arch = "aarch64")]
pub(super) const O_DIRECTORY: c_int = 0o40_000;

/// The current directory as the directory of a path (`include/uapi/linux/fcntl.h`).
pub(super) const AT_FDCWD: c_int = -100;
/// A `statx` of the descriptor itself, with an empty path.
pub(super) const AT_EMPTY_PATH: c_int = 0x1000;
/// A `statx` that keeps the attributes that the kernel has, with no call to a remote server.
pub(super) const AT_STATX_DONT_SYNC: c_int = 0x4000;

/// The write flag of a data-sync write (`include/uapi/linux/fs.h`).
pub(super) const RWF_DSYNC: c_int = 0x2;

/// The modes of `fallocate` (`include/uapi/linux/falloc.h`).
pub(super) const FALLOC_FL_KEEP_SIZE: c_int = 0x1;
pub(super) const FALLOC_FL_PUNCH_HOLE: c_int = 0x2;

/// The fields of `statx` (`include/uapi/linux/stat.h`): the length, and the alignment of direct
/// I/O (Linux 6.1).
pub(super) const STATX_SIZE: c_uint = 0x200;
pub(super) const STATX_DIOALIGN: c_uint = 0x2000;

/// `struct statx`, 256 bytes: the fields that the driver reads, and the others as spare words.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Statx {
    pub(super) mask: u32,
    _before_size: [u32; 9],
    pub(super) size: u64,
    _before_alignment: [u64; 13],
    pub(super) dio_mem_align: u32,
    pub(super) dio_offset_align: u32,
    _spare: [u64; 12],
}

const _: () = {
    assert!(size_of::<Statx>() == 256);
    assert!(core::mem::offset_of!(Statx, size) == 40);
    assert!(core::mem::offset_of!(Statx, dio_mem_align) == 152);
    assert!(core::mem::offset_of!(Statx, dio_offset_align) == 156);
};

#[cfg(target_arch = "x86_64")]
pub(super) const SYS_STATX: c_long = 332;
#[cfg(target_arch = "aarch64")]
pub(super) const SYS_STATX: c_long = 291;

#[expect(unsafe_code, reason = "the C library of each Linux target gives this function")]
unsafe extern "C" {
    /// `long syscall(long number, ...)`, for `statx`, which older C libraries do not have. It
    /// returns -1 and sets `errno` on failure.
    pub(super) fn syscall(number: c_long, ...) -> c_long;
}
