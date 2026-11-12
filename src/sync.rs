//! The memory-model substitution point. Each atomic, reference count, shared cell, spin hint, and
//! thread park of this crate comes from this module, so one algorithm runs in each form. The one exception is
//! the words of an io_uring ring that the kernel also reads and writes: they are hardware atomics
//! in each form.
//!
//! - Native: the `core` atomics, `alloc::sync::Arc`, a `core::cell::UnsafeCell`, `core::hint`,
//!   and, with the `std` feature, `std::thread` for parking.
//! - Without `std`: the same items with no thread. Thus a wait in this build is asynchronous. On a
//!   target with one thread, such as `wasm32v1-none`, the atomic operations compile to plain
//!   memory operations.
//! - Verification: with `--cfg loom`, the unit tests use the loom model of each item. Loom runs
//!   each test in many thread orders, gives some loads an older value that the memory model
//!   permits, and finds data races on the cells. Its spin hint lets another thread run.
//!   `cargo xtask explore` runs these tests.

#[cfg(all(test, loom, feature = "std"))]
pub(crate) use loom::thread;
#[cfg(all(test, loom))]
pub(crate) use loom::{
    cell::UnsafeCell,
    hint,
    sync::{Arc, atomic},
};
#[cfg(all(not(all(test, loom)), feature = "std"))]
pub(crate) use std::thread;
#[cfg(not(all(test, loom)))]
pub(crate) use {alloc::sync::Arc, core::hint, core::sync::atomic, native::UnsafeCell};

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
