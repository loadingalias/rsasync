//! A hint to the CPU to load the cache lines of memory that the code reads soon. The hint changes
//! no value and never faults. So the misses of a few cold lines, which the code would else take
//! one after another, overlap: a core keeps many misses in flight.

/// The most cache lines that one call asks for.
const LINES: usize = 8;

/// The size of a cache line on each target of the runtime.
const LINE: usize = 64;

/// Asks the CPU to load the cache lines of the `bytes` bytes at `ptr`, at most `LINES` of them.
pub(crate) fn lines<T: ?Sized>(ptr: *const T, bytes: usize) {
    let start = ptr.cast::<u8>();
    for line in 0..bytes.div_ceil(LINE).min(LINES) {
        one(start.wrapping_add(line * LINE));
    }
}

/// Asks the CPU to load the cache line of `ptr` for a read.
#[cfg(target_arch = "aarch64")]
#[expect(unsafe_code, reason = "a prefetch is a hint that reads no memory and never faults")]
fn one(ptr: *const u8) {
    // SAFETY: `PRFM` changes no register, no flag, and no memory, and no address makes it fault.
    unsafe { core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) ptr, options(nostack, readonly, preserves_flags)) };
}

/// Asks the CPU to load the cache line of `ptr` for a read.
#[cfg(target_arch = "x86_64")]
#[expect(unsafe_code, reason = "a prefetch is a hint that reads no memory and never faults")]
fn one(ptr: *const u8) {
    // SAFETY: `PREFETCHT0` changes no register, no flag, and no memory, and no address makes it
    // fault.
    unsafe { core::arch::asm!("prefetcht0 [{0}]", in(reg) ptr, options(nostack, readonly, preserves_flags)) };
}

/// A target with no prefetch here loads each line at its first read.
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
const fn one(_ptr: *const u8) {}
