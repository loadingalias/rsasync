//! The fallible forms of the allocations of the state that the runtime keeps. A refusal of the
//! allocator gives `AllocError`, and each caller turns it into its typed refusal, so no allocation
//! of that state aborts the process.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::alloc::AllocError;

/// An empty vector with room for exactly `count` items.
pub(crate) fn room<T>(count: usize) -> Result<Vec<T>, AllocError> {
    let mut items = Vec::new();
    items.try_reserve_exact(count).map_err(|_| AllocError)?;
    Ok(items)
}

/// The items that `make` gives for each index of `0..count`.
pub(crate) fn items<T>(count: u32, make: impl FnMut(u32) -> T) -> Result<Box<[T]>, AllocError> {
    let mut items = room(count as usize)?;
    items.extend((0..count).map(make));
    Ok(items.into_boxed_slice())
}

/// Adds `item` to the end of `items`, which grows if it has no room.
#[cfg_attr(
    not(all(
        feature = "local",
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    )),
    expect(
        dead_code,
        reason = "the start of the runtime uses it, and the runtime is on Linux only"
    )
)]
pub(crate) fn push<T>(items: &mut Vec<T>, item: T) -> Result<(), AllocError> {
    items.try_reserve(1).map_err(|_| AllocError)?;
    items.push(item);
    Ok(())
}
