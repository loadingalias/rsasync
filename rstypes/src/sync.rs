//! The memory-model substitution point. Each atomic, reference count, and shared cell of this
//! crate comes from this module, so one algorithm runs in each form. The exceptions are the two
//! `Arc`s of a trait object, which the loom `Arc` cannot hold: the owner of `Lease::from_owner`,
//! and the release listener of a `Budget`.
//!
//! - Native, with or without `std`: the `core` atomics, `alloc::sync::Arc`, and a
//!   `core::cell::UnsafeCell`. On a target with one thread, such as `wasm32v1-none`, the atomic
//!   operations compile to plain memory operations.
//! - Verification: with `--cfg loom`, the unit tests use the loom model of each item. Loom runs
//!   each test in many thread orders, gives some loads an older value that the memory model
//!   permits, and finds data races on the cells. `cargo xtask explore` runs these tests.

#![expect(
    unused_imports,
    reason = "the module comes before its users; when each item has a user, this expectation fails, and then you remove it"
)]

#[cfg(all(test, loom))]
pub(crate) use loom::{
    cell::UnsafeCell,
    sync::{Arc, atomic},
};
#[cfg(not(all(test, loom)))]
pub(crate) use {alloc::sync::Arc, core::sync::atomic, native::UnsafeCell};

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

#[cfg(not(all(test, loom)))]
mod native {
    #![expect(
        dead_code,
        reason = "the module comes before its users; when the cell has a user, this expectation fails, and then you remove it"
    )]

    /// `core::cell::UnsafeCell` with the access API of the loom form. Each access is a closure,
    /// so the loom form knows when the access starts and ends.
    pub(crate) struct UnsafeCell<T: ?Sized>(core::cell::UnsafeCell<T>);

    impl<T: ?Sized> UnsafeCell<T> {
        pub(crate) fn new(value: T) -> Self
        where
            T: Sized,
        {
            Self(core::cell::UnsafeCell::new(value))
        }

        /// Calls `f` with a pointer to the value, for shared access while `f` runs.
        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        /// Calls `f` with a pointer to the value, for exclusive access while `f` runs.
        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}
