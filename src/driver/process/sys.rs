//! The declarations of child processes, from the C library: glibc or musl on Linux, and libSystem
//! on macOS.
//!
//! On Linux, each item agrees with `sys/wait.h`, `signal.h`, `unistd.h`, and `sys/timerfd.h` of
//! glibc and musl, and with `include/uapi/linux/wait.h`, `include/uapi/asm-generic/siginfo.h`,
//! `include/uapi/asm-generic/unistd.h`, `include/uapi/linux/time.h`, and
//! `include/uapi/asm-generic/fcntl.h` of Linux 6.1 and 7.0, which x86_64 and aarch64 both use. The
//! number of each system call also agrees with `arch/x86/entry/syscalls/syscall_64.tbl`.
//!
//! On macOS, each item agrees with `sys/wait.h`, `sys/signal.h`, `signal.h`, and `unistd.h` of
//! the macOS 27.2 SDK (`MacOSX27.2.sdk`) on aarch64.

#[cfg(target_os = "linux")]
use core::ffi::c_long;
use core::ffi::{c_int, c_uint};

#[cfg(target_os = "linux")]
pub(super) use super::super::sys::syscall;

/// `pidfd_open` and `pidfd_send_signal`: a new system call has one number on each architecture.
/// Older C libraries do not have their functions.
#[cfg(target_os = "linux")]
pub(super) const SYS_PIDFD_SEND_SIGNAL: c_long = 424;
#[cfg(target_os = "linux")]
pub(super) const SYS_PIDFD_OPEN: c_long = 434;

/// The kind of the identity of a `waitid` of the driver: a process descriptor on Linux, and a
/// process ID on macOS, which has no process descriptor.
#[cfg(target_os = "linux")]
pub(super) const P_PIDFD: c_int = 3;
#[cfg(target_os = "macos")]
pub(super) const P_PID: c_int = 1;
/// The options of a `waitid`.
pub(super) const WNOHANG: c_int = 0x1;
pub(super) const WEXITED: c_int = 0x4;
#[cfg(target_os = "linux")]
pub(super) const WNOWAIT: c_int = 0x0100_0000;
#[cfg(target_os = "macos")]
pub(super) const WNOWAIT: c_int = 0x20;

/// The codes of a `SIGCHLD` for an exit and for a core dump, which both systems share. The third
/// code of an end, `CLD_KILLED`, is 2.
pub(super) const CLD_EXITED: c_int = 1;
pub(super) const CLD_DUMPED: c_int = 3;

/// `siginfo_t` of 128 bytes, which x86_64 and aarch64 both use: the fields of a `SIGCHLD`, which a
/// `waitid` writes, and the others as spare words.
#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SigInfo {
    signo: c_int,
    errno: c_int,
    pub(super) code: c_int,
    pad: c_int,
    pub(super) pid: c_int,
    uid: c_uint,
    pub(super) status: c_int,
    spare: [c_int; 25],
}

#[cfg(target_os = "linux")]
const _: () = {
    assert!(size_of::<SigInfo>() == 128);
    assert!(core::mem::offset_of!(SigInfo, code) == 8);
    assert!(core::mem::offset_of!(SigInfo, pid) == 16);
    assert!(core::mem::offset_of!(SigInfo, status) == 24);
};

/// `siginfo_t` of 104 bytes: the fields of a `SIGCHLD`, which a `waitid` writes, and the address,
/// value, band, and reserved words as spare words.
#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SigInfo {
    signo: c_int,
    errno: c_int,
    pub(super) code: c_int,
    pub(super) pid: c_int,
    uid: c_uint,
    pub(super) status: c_int,
    spare: [u64; 10],
}

#[cfg(target_os = "macos")]
const _: () = {
    assert!(size_of::<SigInfo>() == 104);
    assert!(core::mem::offset_of!(SigInfo, code) == 8);
    assert!(core::mem::offset_of!(SigInfo, pid) == 12);
    assert!(core::mem::offset_of!(SigInfo, status) == 20);
};

#[expect(unsafe_code, reason = "the C library of each target gives these functions")]
unsafe extern "C" {
    /// `int waitid(idtype_t idtype, id_t id, siginfo_t *infop, int options)`, with `idtype_t` an
    /// `int` and `id_t` an `unsigned int`. Both C libraries of Linux give `P_PIDFD` to the kernel
    /// as it is.
    pub(super) fn waitid(idtype: c_int, id: c_uint, infop: *mut SigInfo, options: c_int) -> c_int;
    /// `int kill(pid_t pid, int sig)`: a negative `pid` names a process group.
    pub(super) fn kill(pid: c_int, sig: c_int) -> c_int;
}

#[cfg(target_os = "linux")]
#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    /// `pid_t getpid(void)`.
    pub(super) fn getpid() -> c_int;
    /// `int timerfd_create(int clockid, int flags)`.
    pub(super) fn timerfd_create(clockid: c_int, flags: c_int) -> c_int;
    /// `int timerfd_settime(int fd, int flags, const struct itimerspec *new_value, struct
    /// itimerspec *old_value)`.
    pub(super) fn timerfd_settime(fd: c_int, flags: c_int, new: *const ITimerSpec, old: *mut ITimerSpec) -> c_int;
}

/// The monotonic clock, and the flags of a timer that does not block and closes at an exec.
#[cfg(target_os = "linux")]
pub(super) const CLOCK_MONOTONIC: c_int = 1;
#[cfg(target_os = "linux")]
pub(super) const TFD_NONBLOCK: c_int = 0o4_000;
#[cfg(target_os = "linux")]
pub(super) const TFD_CLOEXEC: c_int = 0o2_000_000;

/// `struct timespec`, with a 64-bit `time_t` and `long` on both targets.
#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Timespec {
    pub(super) tv_sec: i64,
    pub(super) tv_nsec: i64,
}

/// `struct itimerspec`: the period of a timer, 0 for one end, and the time to its first end.
#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ITimerSpec {
    pub(super) it_interval: Timespec,
    pub(super) it_value: Timespec,
}

#[cfg(target_os = "linux")]
const _: () = {
    assert!(size_of::<ITimerSpec>() == 32);
    assert!(core::mem::offset_of!(ITimerSpec, it_value) == 16);
};
