//! The declarations of the calls that the poller and the file jobs of the blocking service make for
//! the entries of the driver, from the C library: glibc or musl on Linux, and libSystem on macOS.
//!
//! On Linux, the calls agree with `unistd.h`, `sys/uio.h`, `sys/socket.h`, `fcntl.h`, and `stdio.h`
//! of glibc and musl. The constants agree with `include/uapi/asm-generic/errno-base.h`, `errno.h`,
//! and `socket.h` of Linux 6.1, which x86_64 and aarch64 both use. The numbers of `preadv2`,
//! `pwritev2`, and `renameat2` come from `arch/x86/entry/syscalls/syscall_64.tbl` (x86_64) and
//! `include/uapi/asm-generic/unistd.h` (aarch64).
//!
//! On macOS, the calls, the constants, and the structures agree with `unistd.h`, `sys/uio.h`,
//! `sys/socket.h`, `sys/fcntl.h`, `sys/stdio.h`, `sys/stat.h`, `sys/mount.h`, and `sys/errno.h` of
//! the macOS 27.2 SDK (`MacOSX27.2.sdk`) on aarch64. The file module of the driver also takes its open
//! flags and its `struct stat` from here on macOS, because there only the file jobs make file
//! calls.

#[cfg(target_os = "macos")]
use core::ffi::c_long;
use core::ffi::{c_char, c_int, c_void};
#[cfg(target_os = "linux")]
use core::ffi::{c_long, c_uint};
#[cfg(target_os = "macos")]
use core::mem::{offset_of, size_of};

use rsnetwork_udp::sys::IoVec;

#[expect(unsafe_code, reason = "the C library of each target gives these functions")]
unsafe extern "C" {
    /// `ssize_t read(int fd, void *buf, size_t count)`.
    pub(super) fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    /// `ssize_t write(int fd, const void *buf, size_t count)`.
    pub(super) fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    /// `ssize_t readv(int fd, const struct iovec *iov, int iovcnt)`.
    pub(super) fn readv(fd: c_int, iov: *const IoVec, iovcnt: c_int) -> isize;
    /// `ssize_t writev(int fd, const struct iovec *iov, int iovcnt)`.
    pub(super) fn writev(fd: c_int, iov: *const IoVec, iovcnt: c_int) -> isize;
    /// `ssize_t recv(int sockfd, void *buf, size_t len, int flags)`.
    pub(super) fn recv(sockfd: c_int, buf: *mut c_void, len: usize, flags: c_int) -> isize;
    /// `ssize_t send(int sockfd, const void *buf, size_t len, int flags)`.
    pub(super) fn send(sockfd: c_int, buf: *const c_void, len: usize, flags: c_int) -> isize;
    /// `int shutdown(int sockfd, int how)`.
    pub(super) fn shutdown(sockfd: c_int, how: c_int) -> c_int;
}

#[cfg(target_os = "linux")]
#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    /// `int accept4(int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags)`.
    pub(super) fn accept4(sockfd: c_int, addr: *mut c_void, addrlen: *mut c_uint, flags: c_int) -> c_int;
    /// `int fsync(int fd)`.
    pub(super) fn fsync(fd: c_int) -> c_int;
    /// `int fdatasync(int fd)`.
    pub(super) fn fdatasync(fd: c_int) -> c_int;
    /// `int fallocate(int fd, int mode, off_t offset, off_t len)`, with a 64-bit `off_t` on both
    /// targets.
    pub(super) fn fallocate(fd: c_int, mode: c_int, offset: i64, len: i64) -> c_int;
    /// `int ftruncate(int fd, off_t length)`.
    pub(super) fn ftruncate(fd: c_int, length: i64) -> c_int;
    /// `int openat(int dirfd, const char *pathname, int flags, ...)`, with the mode as its one
    /// variadic argument.
    pub(super) fn openat(dirfd: c_int, pathname: *const c_char, flags: c_int, ...) -> c_int;
    /// `int unlinkat(int dirfd, const char *pathname, int flags)`.
    pub(super) fn unlinkat(dirfd: c_int, pathname: *const c_char, flags: c_int) -> c_int;
}

#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "libSystem of macOS gives these functions")]
unsafe extern "C" {
    /// `int accept(int, struct sockaddr *restrict, socklen_t *restrict)`.
    pub(super) fn accept(sockfd: c_int, addr: *mut c_void, addrlen: *mut u32) -> c_int;
    /// `int connect(int, const struct sockaddr *, socklen_t)`.
    pub(super) fn connect(sockfd: c_int, addr: *const c_void, addrlen: u32) -> c_int;
    /// `int fcntl(int, int, ...)`.
    pub(in crate::driver) fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
}

/// The commands of `fcntl` that set the descriptor flags and the file status flags, the flag of a
/// close at an exec, and the status flag of a descriptor that does not block.
#[cfg(target_os = "macos")]
pub(super) const F_SETFD: c_int = 2;
#[cfg(target_os = "macos")]
pub(super) const F_SETFL: c_int = 4;
#[cfg(target_os = "macos")]
pub(super) const FD_CLOEXEC: c_int = 1;
#[cfg(target_os = "macos")]
pub(super) const O_NONBLOCK: c_int = 0x4;

#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "libSystem of macOS gives these functions")]
unsafe extern "C" {
    /// `ssize_t pread(int, void *, size_t, off_t)`.
    pub(super) fn pread(fd: c_int, buf: *mut c_void, nbyte: usize, offset: i64) -> isize;
    /// `ssize_t pwrite(int, const void *, size_t, off_t)`.
    pub(super) fn pwrite(fd: c_int, buf: *const c_void, nbyte: usize, offset: i64) -> isize;
    /// `ssize_t preadv(int, const struct iovec *, int, off_t)`, macOS 11.
    pub(super) fn preadv(fd: c_int, iov: *const IoVec, iovcnt: c_int, offset: i64) -> isize;
    /// `ssize_t pwritev(int, const struct iovec *, int, off_t)`, macOS 11.
    pub(super) fn pwritev(fd: c_int, iov: *const IoVec, iovcnt: c_int, offset: i64) -> isize;
    /// `int openat(int, const char *, int, ...)`, with the mode as its one variadic argument.
    pub(super) fn openat(fd: c_int, path: *const c_char, oflag: c_int, ...) -> c_int;
    /// `int renameat(int, const char *, int, const char *)`.
    pub(super) fn renameat(fromfd: c_int, from: *const c_char, tofd: c_int, to: *const c_char) -> c_int;
    /// `int unlinkat(int, const char *, int)`.
    pub(super) fn unlinkat(fd: c_int, path: *const c_char, flag: c_int) -> c_int;
    /// `int ftruncate(int, off_t)`.
    pub(super) fn ftruncate(fd: c_int, length: i64) -> c_int;
    /// `int fstat(int, struct stat *)`, the form with 64-bit inode numbers, which is the one form
    /// on aarch64.
    pub(super) fn fstat(fd: c_int, buf: *mut Stat) -> c_int;
    /// `int fstatfs(int, struct statfs *)` (`sys/mount.h`), the form with 64-bit inode numbers.
    pub(super) fn fstatfs(fd: c_int, buf: *mut Statfs) -> c_int;
    /// `long sysconf(int)`.
    pub(in crate::driver) fn sysconf(name: c_int) -> c_long;
}

/// The open flags of a file, and the current directory as the directory of a path.
#[cfg(target_os = "macos")]
pub(in crate::driver) const O_RDONLY: c_int = 0x0;
#[cfg(target_os = "macos")]
pub(in crate::driver) const O_WRONLY: c_int = 0x1;
#[cfg(target_os = "macos")]
pub(in crate::driver) const O_RDWR: c_int = 0x2;
#[cfg(target_os = "macos")]
pub(in crate::driver) const O_CREAT: c_int = 0x200;
#[cfg(target_os = "macos")]
pub(in crate::driver) const O_TRUNC: c_int = 0x400;
#[cfg(target_os = "macos")]
pub(in crate::driver) const O_EXCL: c_int = 0x800;
#[cfg(target_os = "macos")]
pub(in crate::driver) const O_DIRECTORY: c_int = 0x10_0000;
#[cfg(target_os = "macos")]
pub(in crate::driver) const O_CLOEXEC: c_int = 0x100_0000;
#[cfg(target_os = "macos")]
pub(in crate::driver) const AT_FDCWD: c_int = -2;

/// The commands of `fcntl` for the space and the durability of a file: allocate space after the
/// physical end, write the cache of the device to its media too, turn the data cache off, and free
/// a range.
#[cfg(target_os = "macos")]
pub(super) const F_PREALLOCATE: c_int = 42;
#[cfg(target_os = "macos")]
pub(in crate::driver) const F_NOCACHE: c_int = 48;
#[cfg(target_os = "macos")]
pub(super) const F_FULLFSYNC: c_int = 51;
#[cfg(target_os = "macos")]
pub(super) const F_PUNCHHOLE: c_int = 99;
/// The flag of `F_PREALLOCATE` that allocates all the space or none, and its mode that allocates
/// after the physical end of the file.
#[cfg(target_os = "macos")]
pub(super) const F_ALLOCATEALL: u32 = 0x4;
#[cfg(target_os = "macos")]
pub(super) const F_PEOFPOSMODE: c_int = 3;
/// The name of the size of a page in `sysconf` (`unistd.h`).
#[cfg(target_os = "macos")]
pub(in crate::driver) const _SC_PAGESIZE: c_int = 29;
/// The unit of `st_blocks` (`S_BLKSIZE`).
#[cfg(target_os = "macos")]
pub(super) const S_BLKSIZE: i64 = 512;

/// `fstore_t`: the request of `F_PREALLOCATE`, and the bytes that it allocated.
#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Debug, Default)]
pub(super) struct Fstore {
    pub(super) fst_flags: u32,
    pub(super) fst_posmode: c_int,
    pub(super) fst_offset: i64,
    pub(super) fst_length: i64,
    pub(super) fst_bytesalloc: i64,
}

/// `fpunchhole_t`: the range of `F_PUNCHHOLE`.
#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Debug, Default)]
pub(super) struct Punchhole {
    pub(super) fp_flags: u32,
    pub(super) reserved: u32,
    pub(super) fp_offset: i64,
    pub(super) fp_length: i64,
}

/// `struct stat`, 144 bytes: the fields that the driver reads, and the others as spare words.
#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Stat {
    _before_size: [u64; 12],
    pub(in crate::driver) st_size: i64,
    pub(super) st_blocks: i64,
    _after_blocks: [u64; 4],
}

/// `struct statfs`, 2168 bytes: the block size of the file system, and the other fields as spare
/// words.
#[cfg(target_os = "macos")]
#[repr(C)]
pub(super) struct Statfs {
    pub(super) f_bsize: u32,
    _after_bsize: [u32; 1],
    _rest: [u64; 270],
}

#[cfg(target_os = "macos")]
impl Statfs {
    pub(super) const fn new() -> Self {
        Self {
            f_bsize: 0,
            _after_bsize: [0],
            _rest: [0; 270],
        }
    }
}

#[cfg(target_os = "macos")]
const _: () = {
    assert!(size_of::<Statfs>() == 2168);
    assert!(offset_of!(Statfs, f_bsize) == 0);
    assert!(size_of::<Fstore>() == 32);
    assert!(offset_of!(Fstore, fst_posmode) == 4);
    assert!(offset_of!(Fstore, fst_offset) == 8);
    assert!(offset_of!(Fstore, fst_length) == 16);
    assert!(offset_of!(Fstore, fst_bytesalloc) == 24);
    assert!(size_of::<Punchhole>() == 24);
    assert!(offset_of!(Punchhole, fp_offset) == 8);
    assert!(offset_of!(Punchhole, fp_length) == 16);
    assert!(size_of::<Stat>() == 144);
    assert!(offset_of!(Stat, st_size) == 96);
    assert!(offset_of!(Stat, st_blocks) == 104);
};

/// The system call numbers of `preadv2`, `pwritev2`, and `renameat2`. The C library of each
/// target does not always give these functions, so the poller enters the kernel directly.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) const SYS_PREADV2: c_long = 327;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) const SYS_PWRITEV2: c_long = 328;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) const SYS_RENAMEAT2: c_long = 316;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
pub(super) const SYS_PREADV2: c_long = 286;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
pub(super) const SYS_PWRITEV2: c_long = 287;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
pub(super) const SYS_RENAMEAT2: c_long = 276;

/// The option of the pending error of a socket (`include/uapi/asm-generic/socket.h`, and
/// `sys/socket.h` of macOS).
#[cfg(target_os = "linux")]
pub(super) const SO_ERROR: c_int = 4;
#[cfg(target_os = "macos")]
pub(super) const SO_ERROR: c_int = 0x1007;

pub(super) const ENOENT: i32 = 2;
pub(super) const EINVAL: i32 = 22;
#[cfg(target_os = "linux")]
pub(super) const EAGAIN: i32 = 11;
#[cfg(target_os = "macos")]
pub(super) const EAGAIN: i32 = 35;
#[cfg(target_os = "linux")]
pub(super) const EBADFD: i32 = 77;
#[cfg(target_os = "linux")]
pub(super) const ENOBUFS: i32 = 105;
#[cfg(target_os = "macos")]
pub(super) const ENOBUFS: i32 = 55;
#[cfg(target_os = "linux")]
pub(super) const EINPROGRESS: i32 = 115;
#[cfg(target_os = "macos")]
pub(super) const EINPROGRESS: i32 = 36;
/// A file system with no such command of `fcntl` gives `ENOTTY`. The job gives `ENOTSUP` for it,
/// which `std::io` reads as `Unsupported`. A range above the largest offset gives `EFBIG`.
#[cfg(target_os = "macos")]
pub(super) const ENOTTY: i32 = 25;
#[cfg(target_os = "macos")]
pub(super) const ENOTSUP: i32 = 45;
#[cfg(target_os = "macos")]
pub(super) const EFBIG: i32 = 27;
/// A write of zeros that wrote no byte gives `EIO`.
#[cfg(target_os = "macos")]
pub(super) const EIO: i32 = 5;
