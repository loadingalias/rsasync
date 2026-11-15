//! The declarations of epoll, and of the calls that the poller and the file jobs of the blocking
//! service make for the entries of the driver, from the C library, glibc or musl.
//!
//! The epoll items agree with `sys/epoll.h` of glibc and musl and with
//! `include/uapi/linux/eventpoll.h` of Linux 6.1 and 7.0. The structure `epoll_event` is packed on
//! x86_64 (`EPOLL_PACKED`) and has its natural layout on aarch64. The calls agree with `unistd.h`,
//! `sys/uio.h`, `sys/socket.h`, `fcntl.h`, and `stdio.h` of glibc and musl. The constants agree
//! with `include/uapi/asm-generic/errno-base.h`, `errno.h`, `socket.h`, and
//! `include/linux/socket.h` of Linux 6.1, which x86_64 and aarch64 both use. The numbers of
//! `preadv2`, `pwritev2`, and `renameat2` come from `arch/x86/entry/syscalls/syscall_64.tbl`
//! (x86_64) and `include/uapi/asm-generic/unistd.h` (aarch64).

use core::ffi::{c_char, c_int, c_long, c_uint, c_void};
use core::mem::{offset_of, size_of};

use rsnetwork_udp::sys::IoVec;

#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    /// `int epoll_create1(int flags)`.
    pub(super) fn epoll_create1(flags: c_int) -> c_int;
    /// `int epoll_ctl(int epfd, int op, int fd, struct epoll_event *event)`.
    pub(super) fn epoll_ctl(epfd: c_int, op: c_int, fd: c_int, event: *mut EpollEvent) -> c_int;
    /// `int epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout)`.
    pub(super) fn epoll_wait(epfd: c_int, events: *mut EpollEvent, maxevents: c_int, timeout: c_int) -> c_int;
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
    /// `int accept4(int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags)`.
    pub(super) fn accept4(sockfd: c_int, addr: *mut c_void, addrlen: *mut c_uint, flags: c_int) -> c_int;
    /// `int shutdown(int sockfd, int how)`.
    pub(super) fn shutdown(sockfd: c_int, how: c_int) -> c_int;
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

/// `struct epoll_event`: the events of a descriptor, and the data of its registration.
#[cfg_attr(target_arch = "x86_64", repr(C, packed))]
#[cfg_attr(not(target_arch = "x86_64"), repr(C))]
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct EpollEvent {
    pub(super) events: u32,
    pub(super) data: u64,
}

#[cfg(target_arch = "x86_64")]
const _: () = {
    assert!(size_of::<EpollEvent>() == 12);
    assert!(offset_of!(EpollEvent, data) == 4);
};
#[cfg(target_arch = "aarch64")]
const _: () = {
    assert!(size_of::<EpollEvent>() == 16);
    assert!(offset_of!(EpollEvent, data) == 8);
};

/// `EPOLL_CLOEXEC` is `O_CLOEXEC`.
pub(super) const EPOLL_CLOEXEC: c_int = 0o2_000_000;
pub(super) const EPOLL_CTL_ADD: c_int = 1;
pub(super) const EPOLL_CTL_DEL: c_int = 2;
pub(super) const EPOLL_CTL_MOD: c_int = 3;
pub(super) const EPOLLIN: u32 = 0x001;
pub(super) const EPOLLPRI: u32 = 0x002;
pub(super) const EPOLLOUT: u32 = 0x004;
pub(super) const EPOLLERR: u32 = 0x008;
pub(super) const EPOLLHUP: u32 = 0x010;
pub(super) const EPOLLRDHUP: u32 = 0x2000;
pub(super) const EPOLLONESHOT: u32 = 1 << 30;
pub(super) const EPOLLET: u32 = 1 << 31;

/// The system call numbers of `preadv2`, `pwritev2`, and `renameat2`. The C library of each
/// target does not always give these functions, so the poller enters the kernel directly.
#[cfg(target_arch = "x86_64")]
pub(super) const SYS_PREADV2: c_long = 327;
#[cfg(target_arch = "x86_64")]
pub(super) const SYS_PWRITEV2: c_long = 328;
#[cfg(target_arch = "x86_64")]
pub(super) const SYS_RENAMEAT2: c_long = 316;
#[cfg(target_arch = "aarch64")]
pub(super) const SYS_PREADV2: c_long = 286;
#[cfg(target_arch = "aarch64")]
pub(super) const SYS_PWRITEV2: c_long = 287;
#[cfg(target_arch = "aarch64")]
pub(super) const SYS_RENAMEAT2: c_long = 276;

/// The option of the pending error of a socket (`include/uapi/asm-generic/socket.h`).
pub(super) const SO_ERROR: c_int = 4;
/// `MSG_DONTWAIT` (`include/linux/socket.h`).
pub(super) const MSG_DONTWAIT: c_int = 0x40;

pub(super) const ENOENT: i32 = 2;
pub(super) const EAGAIN: i32 = 11;
pub(super) const EINVAL: i32 = 22;
pub(super) const EBADFD: i32 = 77;
pub(super) const ENOBUFS: i32 = 105;
pub(super) const EINPROGRESS: i32 = 115;
