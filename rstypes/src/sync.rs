//! The memory-model substitution point. Each atomic, reference count, and shared cell of this
//! crate comes from this module, so one algorithm runs in each form. The one exception is the
//! `Arc` of a trait object, which the loom `Arc` cannot hold: the owner of `Lease::from_owner`.
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
