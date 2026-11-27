//! The declarations of `sched_getaffinity`, `sched_setaffinity`, `getrandom`, and the directory
//! calls, from the C library, glibc or musl, and their safe forms.
//!
//! Each item agrees with `sched.h`, `sys/random.h`, `dirent.h`, and `errno.h` of glibc (2.25 or
//! later) and musl (1.1.20 or later) on x86_64 and aarch64. Both wrappers of the affinity calls
//! return 0, and the one of `sched_getaffinity` clears the part of the mask that the kernel did not
//! write. A `pid` of 0 names the calling thread.

use core::ffi::{CStr, c_char, c_int, c_uint, c_void};
use core::ptr::NonNull;
use std::io;

use super::placement::CpuSet;

#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    /// `int sched_getaffinity(pid_t pid, size_t cpusetsize, cpu_set_t *mask)`.
    fn sched_getaffinity(pid: c_int, cpusetsize: usize, mask: *mut u64) -> c_int;
    /// `int sched_setaffinity(pid_t pid, size_t cpusetsize, const cpu_set_t *mask)`.
    fn sched_setaffinity(pid: c_int, cpusetsize: usize, mask: *const u64) -> c_int;
    /// `ssize_t getrandom(void *buf, size_t buflen, unsigned int flags)`.
    fn getrandom(buf: *mut c_void, buflen: usize, flags: c_uint) -> isize;
    /// `DIR *opendir(const char *name)`.
    fn opendir(name: *const c_char) -> *mut c_void;
    /// `struct dirent *readdir(DIR *dirp)`.
    fn readdir(dir: *mut c_void) -> *mut c_void;
    /// `int closedir(DIR *dirp)`.
    fn closedir(dir: *mut c_void) -> c_int;
    /// `int *__errno_location(void)`: the `errno` of the calling thread.
    fn __errno_location() -> *mut c_int;
}

/// The offset of `d_name` in `struct dirent` of glibc and musl on x86_64 and aarch64: after
/// `d_ino` (8 bytes), `d_off` (8), `d_reclen` (2), and `d_type` (1).
const NAME_OFFSET: usize = 19;

/// The most interrupted calls of `getrandom` in one fill.
const INTERRUPTS: u32 = 64;

/// The CPU set of the calling thread.
#[expect(unsafe_code, reason = "the kernel writes at most the mask that the call names")]
pub(super) fn affinity() -> io::Result<CpuSet> {
    let mut set = CpuSet::new();
    let words = set.words_mut();
    // SAFETY: the mask is `size_of_val(words)` writable bytes.
    if unsafe { sched_getaffinity(0, size_of_val(words), words.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(set)
}

/// Binds the calling thread to `set`.
#[expect(unsafe_code, reason = "the kernel reads at most the mask that the call names")]
pub(super) fn bind(set: &CpuSet) -> io::Result<()> {
    let words = set.words();
    // SAFETY: the mask is `size_of_val(words)` readable bytes.
    if unsafe { sched_setaffinity(0, size_of_val(words), words.as_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Fills `buffer` with random bytes of the kernel. The call waits until the kernel initialized
/// its generator, at boot.
#[expect(unsafe_code, reason = "the kernel writes at most the bytes that the call names")]
pub(super) fn random(buffer: &mut [u8]) -> io::Result<()> {
    let mut filled = 0;
    let mut interrupts = 0;
    while let Some(rest) = buffer.get_mut(filled..).filter(|rest| !rest.is_empty()) {
        // SAFETY: `rest` is `rest.len()` writable bytes.
        let count = unsafe { getrandom(rest.as_mut_ptr().cast(), rest.len(), 0) };
        match usize::try_from(count) {
            // The kernel fills at least one byte of a nonempty buffer, so no call gives 0.
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(count) => filled += count,
            Err(_) => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted || interrupts == INTERRUPTS {
                    return Err(error);
                }
                interrupts += 1;
            }
        }
    }
    Ok(())
}

/// A directory that is open for [`Directory::next`]. A drop closes it.
pub(super) struct Directory(NonNull<c_void>);

impl Directory {
    /// Opens the directory at `path`. The C library allocates its buffer, and a refusal gives
    /// `ENOMEM`.
    #[expect(unsafe_code, reason = "`opendir` reads the path up to its 0 byte")]
    pub(super) fn open(path: &CStr) -> io::Result<Self> {
        // SAFETY: `path` ends with a 0 byte.
        NonNull::new(unsafe { opendir(path.as_ptr()) })
            .map(Self)
            .ok_or_else(io::Error::last_os_error)
    }

    /// The name of the next entry, `None` after the last entry, or the error of the read. The name
    /// stays valid until the next call, which the borrow keeps.
    #[expect(unsafe_code, reason = "`readdir` gives an entry of the open directory, or null")]
    pub(super) fn next(&mut self) -> io::Result<Option<&CStr>> {
        // SAFETY: the `errno` of this thread is a valid `int`. A null result with `errno` 0 is the end.
        unsafe { __errno_location().write(0) };
        // SAFETY: the directory is open until the drop.
        let Some(entry) = NonNull::new(unsafe { readdir(self.0.as_ptr()) }) else {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(0) {
                Ok(None)
            } else {
                Err(error)
            };
        };
        // SAFETY: an entry is valid until the next `readdir` or `closedir`, and its name ends with
        // a 0 byte in the entry. The record can be shorter than `struct dirent`, so only the name is
        // read, through its pointer.
        Ok(Some(unsafe {
            CStr::from_ptr(entry.as_ptr().byte_add(NAME_OFFSET).cast())
        }))
    }
}

impl Drop for Directory {
    #[expect(unsafe_code, reason = "`closedir` closes the directory that `opendir` gave")]
    fn drop(&mut self) {
        // SAFETY: the directory is open, and nothing uses it after the drop. An error of the close
        // leaves nothing to release.
        let _closed = unsafe { closedir(self.0.as_ptr()) };
    }
}
