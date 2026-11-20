//! The declarations of `sched_getaffinity`, `sched_setaffinity`, and `getrandom`, from the C
//! library, glibc or musl, and their safe forms.
//!
//! Each item agrees with `sched.h` and `sys/random.h` of glibc (2.25 or later) and musl (1.1.20 or
//! later). Both wrappers of the affinity calls return 0, and the one of `sched_getaffinity` clears
//! the part of the mask that the kernel did not write. A `pid` of 0 names the calling thread.

use core::ffi::{c_int, c_uint, c_void};
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
}

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
