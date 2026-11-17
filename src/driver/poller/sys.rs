//! The declarations of the calls that the poller and the file jobs of the blocking service make for
//! the entries of the driver, from the C library: glibc or musl on Linux, and libSystem on macOS.
//!
//! On Linux, the calls agree with `unistd.h`, `sys/uio.h`, `sys/socket.h`, `fcntl.h`, and `stdio.h`
//! of glibc and musl. The constants agree with `include/uapi/asm-generic/errno-base.h`, `errno.h`,
//! and `socket.h` of Linux 6.1, which x86_64 and aarch64 both use. The numbers of `preadv2`,
//! `pwritev2`, and `renameat2` come from `arch/x86/entry/syscalls/syscall_64.tbl` (x86_64) and
//! `include/uapi/asm-generic/unistd.h` (aarch64).
//!
//! On macOS, the calls and the constants agree with `unistd.h`, `sys/uio.h`, `sys/socket.h`,
//! `sys/fcntl.h`, and `sys/errno.h` of the macOS 27.2 SDK (`MacOSX27.2.sdk`) on aarch64.

#[cfg(target_os = "linux")]
use core::ffi::{c_char, c_long, c_uint};
use core::ffi::{c_int, c_void};

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
    pub(super) fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
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
