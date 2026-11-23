//! The declarations of IPC streams, from the C library: glibc or musl on Linux, and libSystem on
//! macOS.
//!
//! On Linux, each item agrees with `sys/socket.h`, `sys/un.h`, `sys/stat.h`, `sys/file.h`, and
//! `unistd.h` of glibc and musl, and with `include/uapi/linux/un.h`, `include/linux/socket.h`,
//! `include/uapi/asm-generic/socket.h`, `fcntl.h`, `errno-base.h`, and `errno.h` of Linux 6.1 and
//! 7.0, which x86_64 and aarch64 both use. `O_NOFOLLOW` has another value on aarch64, from
//! `arch/arm64/include/uapi/asm/fcntl.h`.
//!
//! On macOS, each item agrees with `sys/socket.h`, `sys/un.h`, `sys/stat.h`, `sys/fcntl.h`,
//! `unistd.h`, and `sys/errno.h` of the macOS 27.2 SDK (`MacOSX27.2.sdk`) on aarch64.

#[cfg(target_os = "macos")]
use core::ffi::c_char;
use core::ffi::c_int;
#[cfg(target_os = "linux")]
use core::ffi::c_uint;

use rsnetwork_udp::SaFamily;

#[cfg(target_os = "macos")]
pub(super) use super::super::poller::sys::{connect, fcntl};
#[cfg(target_os = "linux")]
pub(super) use super::super::sys::connect;

/// The family of Unix-domain sockets.
pub(super) const AF_UNIX: SaFamily = 1;

/// `EPERM`.
pub(super) const EPERM: i32 = 1;
/// `ENOENT`.
pub(super) const ENOENT: i32 = 2;
/// `ENOMEM`.
pub(super) const ENOMEM: i32 = 12;
/// `EBUSY`, which `std::io` reads as `ResourceBusy`.
pub(super) const EBUSY: i32 = 16;
/// `EINVAL`.
pub(super) const EINVAL: i32 = 22;
/// `EAGAIN`.
#[cfg(target_os = "linux")]
pub(super) const EAGAIN: i32 = 11;
#[cfg(target_os = "macos")]
pub(super) const EAGAIN: i32 = 35;
/// `EADDRINUSE`.
#[cfg(target_os = "linux")]
pub(super) const EADDRINUSE: i32 = 98;
#[cfg(target_os = "macos")]
pub(super) const EADDRINUSE: i32 = 48;
/// `ECONNREFUSED`.
#[cfg(target_os = "linux")]
pub(super) const ECONNREFUSED: i32 = 111;
#[cfg(target_os = "macos")]
pub(super) const ECONNREFUSED: i32 = 61;
/// `ENOTCONN`.
#[cfg(target_os = "macos")]
pub(super) const ENOTCONN: i32 = 57;
/// `ENAMETOOLONG`, which `std::io` reads as `InvalidFilename`.
#[cfg(target_os = "linux")]
pub(super) const ENAMETOOLONG: i32 = 36;
#[cfg(target_os = "macos")]
pub(super) const ENAMETOOLONG: i32 = 63;

/// `struct sockaddr_un`: the family, and a path of at most 107 bytes and its NUL byte.
#[cfg(target_os = "linux")]
#[repr(C)]
pub(super) struct SockAddrUn {
    pub(super) sun_family: SaFamily,
    pub(super) sun_path: [u8; 108],
}

/// `struct sockaddr_un`: the length of the address, the family, and a path of at most 103 bytes
/// and its NUL byte.
#[cfg(target_os = "macos")]
#[repr(C)]
pub(super) struct SockAddrUn {
    pub(super) sun_len: u8,
    pub(super) sun_family: SaFamily,
    pub(super) sun_path: [u8; 104],
}

#[cfg(target_os = "linux")]
const _: () = assert!(size_of::<SockAddrUn>() == 110);
#[cfg(target_os = "macos")]
const _: () = assert!(size_of::<SockAddrUn>() == 106);

/// The open flag that refuses a symbolic link as the last part of the path.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) const O_NOFOLLOW: c_int = 0o400_000;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
pub(super) const O_NOFOLLOW: c_int = 0o100_000;
#[cfg(target_os = "macos")]
pub(super) const O_NOFOLLOW: c_int = 0x100;

/// The operations of `flock`: an exclusive lock, and no wait for the lock.
pub(super) const LOCK_EX: c_int = 2;
pub(super) const LOCK_NB: c_int = 4;

#[expect(unsafe_code, reason = "the C library of each target gives this function")]
unsafe extern "C" {
    /// `int flock(int fd, int operation)`.
    pub(super) fn flock(fd: c_int, operation: c_int) -> c_int;
}

/// The credentials of the peer of a Unix-domain socket.
#[cfg(target_os = "linux")]
pub(super) const SO_PEERCRED: c_int = 17;

/// `struct ucred`: the process, user, and group of a peer.
#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Ucred {
    pub(super) pid: c_int,
    pub(super) uid: c_uint,
    pub(super) gid: c_uint,
}

/// The size of `struct ucred`, as a `socklen_t`.
#[cfg(target_os = "linux")]
pub(super) const UCRED_LEN: c_uint = 12;

#[cfg(target_os = "linux")]
const _: () = assert!(size_of::<Ucred>() == UCRED_LEN as usize);

/// The level of the options of a Unix-domain socket.
#[cfg(target_os = "macos")]
pub(super) const SOL_LOCAL: c_int = 0;
/// The process ID of the peer of a Unix-domain socket, a `pid_t`.
#[cfg(target_os = "macos")]
pub(super) const LOCAL_PEERPID: c_int = 2;
/// The directory of the process, for `fchmodat`.
#[cfg(target_os = "macos")]
pub(super) const AT_FDCWD: c_int = -2;
/// A change of the link itself, not of its target.
#[cfg(target_os = "macos")]
pub(super) const AT_SYMLINK_NOFOLLOW: c_int = 0x20;
/// The command of `fcntl` that reads the first lock that would block a lock, and the lock types.
#[cfg(target_os = "macos")]
pub(super) const F_GETLK: c_int = 7;
#[cfg(target_os = "macos")]
pub(super) const F_UNLCK: i16 = 2;
#[cfg(target_os = "macos")]
pub(super) const F_WRLCK: i16 = 3;

/// `struct flock`: the range of a lock, its owner, its type, and the origin of its range.
#[cfg(target_os = "macos")]
#[repr(C)]
pub(super) struct Flock {
    pub(super) l_start: i64,
    pub(super) l_len: i64,
    pub(super) l_pid: c_int,
    pub(super) l_type: i16,
    pub(super) l_whence: i16,
}

#[cfg(target_os = "macos")]
const _: () = assert!(size_of::<Flock>() == 24);

#[cfg(target_os = "linux")]
#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    /// `int fchmod(int fd, mode_t mode)`, with `mode_t` an `unsigned int`.
    pub(super) fn fchmod(fd: c_int, mode: c_uint) -> c_int;
    /// `uid_t geteuid(void)`, with `uid_t` an `unsigned int`.
    pub(super) fn geteuid() -> c_uint;
}

#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "libSystem of macOS gives these functions")]
unsafe extern "C" {
    /// `int fchmodat(int, const char *, mode_t, int)`, with `mode_t` a `__uint16_t`.
    pub(super) fn fchmodat(fd: c_int, path: *const c_char, mode: u16, flag: c_int) -> c_int;
    /// `uid_t geteuid(void)`, with `uid_t` a `__uint32_t`.
    pub(super) fn geteuid() -> u32;
    /// `int getpeereid(int, uid_t *, gid_t *)`: the effective user and group of the peer, which
    /// the kernel recorded at the connect.
    pub(super) fn getpeereid(fd: c_int, uid: *mut u32, gid: *mut u32) -> c_int;
}
