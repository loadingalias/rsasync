//! The declarations of the wake descriptor, `eventfd`, of `poll`, which the probe of the
//! descriptor uses, and of `fcntl`, which the adoption of a pipe end uses, from the C library,
//! glibc or musl.
//!
//! Each item agrees with `sys/eventfd.h`, `poll.h`, and `fcntl.h` of glibc and musl, and with
//! `include/uapi/asm-generic/fcntl.h` and `include/uapi/asm-generic/poll.h` of Linux 6.1, which
//! x86_64 and aarch64 both use.

use core::ffi::{c_int, c_short, c_uint, c_ulong};

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
