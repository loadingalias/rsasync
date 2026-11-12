//! The declaration of the wake descriptor: `eventfd` of the C library, glibc or musl.
//!
//! Each item agrees with `sys/eventfd.h` of glibc and musl, and with
//! `include/uapi/asm-generic/fcntl.h` of Linux 6.1, which x86_64 and aarch64 both use.

use core::ffi::{c_int, c_uint};

#[expect(unsafe_code, reason = "the C library of each Linux target gives this function")]
unsafe extern "C" {
    /// `int eventfd(unsigned int initval, int flags)`.
    pub(super) fn eventfd(initval: c_uint, flags: c_int) -> c_int;
}

/// `EFD_CLOEXEC` is `O_CLOEXEC`, and `EFD_NONBLOCK` is `O_NONBLOCK`.
pub(super) const EFD_CLOEXEC: c_int = 0o2_000_000;
pub(super) const EFD_NONBLOCK: c_int = 0o4_000;
