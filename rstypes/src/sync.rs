//! The memory-model substitution point. Each atomic and reference count of this crate comes from
//! this module, so one algorithm runs in each form. The exceptions are the two
//! `Arc`s of a trait object, which the loom `Arc` cannot hold: the owner of `Lease::from_owner`,
//! and the release listener of a `Budget`.
//!
//! - Native, with or without `std`: the `core` atomics and `alloc::sync::Arc`. On a target with
//!   one thread, such as `wasm32v1-none`, the atomic operations compile to plain memory operations.
//! - Verification: with `--cfg loom`, the unit tests use the loom model of each item. Loom runs
//!   each test in many thread orders, and gives some loads an older value that the memory model
//!   permits. `cargo xtask explore` runs these tests.

#[cfg(all(test, loom))]
pub(crate) use loom::sync::{Arc, atomic};
#[cfg(not(all(test, loom)))]
pub(crate) use {alloc::sync::Arc, core::sync::atomic};

/// `Arc::try_new` in each form: a refusal of the allocator gives `AllocError`. The loom `Arc` has no
/// fallible form, so its allocation never fails.
#[cfg(not(all(test, loom)))]
pub(crate) fn try_arc<T>(value: T) -> Result<Arc<T>, core::alloc::AllocError> {
    Arc::try_new(value)
}

/// See the native form.
#[cfg(all(test, loom))]
pub(crate) fn try_arc<T>(value: T) -> Result<Arc<T>, core::alloc::AllocError> {
    Ok(Arc::new(value))
}

/// The layout of the allocation of a native `Arc<T>`: its two counts, and then `T` at its
/// alignment. Evaluate it in a `const` block: a `T` too large for an allocation then fails the
/// build.
#[expect(
    clippy::panic,
    reason = "each caller evaluates it in a `const` block, so a panic is a build error, never a run-time one"
)]
pub(crate) const fn arc_layout<T>() -> core::alloc::Layout {
    match core::alloc::Layout::new::<[usize; 2]>().extend(core::alloc::Layout::new::<T>()) {
        Ok((inner, _)) => inner.pad_to_align(),
        Err(_) => panic!("the type is too large for the allocation of an `Arc`"),
    }
}

/// The size of the allocation of a native `Arc<T>`: its two counts, and then `T` at its
/// alignment. A charge of a shared state counts it. A layout too large for an allocation gives
/// `usize::MAX`, so the charge is too large for each budget.
pub(crate) const fn arc_size<T>() -> usize {
    match core::alloc::Layout::new::<[usize; 2]>().extend(core::alloc::Layout::new::<T>()) {
        Ok((inner, _)) => inner.pad_to_align().size(),
        Err(_) => usize::MAX,
    }
}
