//! The declarations of pipe ends, from the C library: glibc or musl on Linux, and libSystem on
//! macOS.
//!
//! On Linux, the items are those of the driver (`driver/sys.rs`). On macOS, each item agrees with
//! `sys/fcntl.h` and `sys/stat.h` of the macOS 27.2 SDK (`MacOSX27.2.sdk`) on aarch64.

use core::ffi::c_int;

#[cfg(target_os = "macos")]
pub(super) use super::super::poller::sys::{F_SETFL, O_NONBLOCK, O_RDONLY, O_RDWR, O_WRONLY, fcntl};
#[cfg(target_os = "linux")]
pub(super) use super::super::sys::{
    F_GETFL, F_GETPIPE_SZ, F_SETFL, O_ACCMODE, O_NONBLOCK, O_RDONLY, O_RDWR, O_WRONLY, fcntl,
};

/// The command of `fcntl` that reads the file status flags, and their access mode.
#[cfg(target_os = "macos")]
pub(super) const F_GETFL: c_int = 3;
#[cfg(target_os = "macos")]
pub(super) const O_ACCMODE: c_int = 0x3;

/// The type bits of the mode of a file, and the type of a pipe or a FIFO.
#[cfg(target_os = "macos")]
const S_IFMT: u16 = 0o170_000;
#[cfg(target_os = "macos")]
const S_IFIFO: u16 = 0o010_000;

/// Whether `fd` is a pipe or a FIFO: only those have a capacity of a pipe on Linux.
#[cfg(target_os = "linux")]
#[expect(unsafe_code, reason = "fcntl names no memory with this command")]
pub(super) fn is_pipe(fd: c_int) -> bool {
    // SAFETY: the command names no memory.
    unsafe { fcntl(fd, F_GETPIPE_SZ) >= 0 }
}

/// Whether `fd` is a pipe or a FIFO, by the type of its file.
#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "the call writes only the structure that it gets")]
pub(super) fn is_pipe(fd: c_int) -> bool {
    let mut stat = super::super::poller::sys::Stat::default();
    // SAFETY: the call writes only `stat`, which is a `struct stat`.
    let result = unsafe { super::super::poller::sys::fstat(fd, &raw mut stat) };
    result == 0 && stat.st_mode & S_IFMT == S_IFIFO
}
