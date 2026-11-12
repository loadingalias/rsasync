//! The declarations of the wake descriptor, `eventfd`, and of `poll`, which the probe of the
//! descriptor uses, from the C library, glibc or musl.
//!
//! Each item agrees with `sys/eventfd.h` and `poll.h` of glibc and musl, and with
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
