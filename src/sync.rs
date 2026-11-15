//! The memory-model substitution point. Each atomic, reference count, shared cell, lock, spin
//! hint, thread park, and thread-local value of this crate comes from this module, so one
//! algorithm runs in each form. Two exceptions are hardware atomics in each form: the words of an
//! io_uring ring, which the kernel also reads and writes, and the statics of the signal handler
//! (`driver::signal`), which a signal changes between any two instructions. The verification form cannot deliver a signal, and its
//! atomics cannot be `const` in a static. The registrations of that handler share one
//! `std::sync::Mutex`, which the handler never takes.
//!
//! - Native: the `core` atomics, `alloc::sync::Arc`, a `core::cell::UnsafeCell`, `core::hint`,
//!   and, with the `std` feature, `std::thread` for parking and `std::sync::Mutex` for the lock.
//! - Without `std`: the same items with no thread, and a spin lock. Thus a wait in this build is
//!   asynchronous. The spin lock is for targets on which nothing preempts a holder of the lock,
//!   such as single-threaded WebAssembly. On a target with one thread, such as `wasm32v1-none`, the
//!   atomic operations compile to plain memory operations, and the lock never spins.
//! - Verification: with `--cfg loom`, the unit tests use the loom model of each item. Loom runs
//!   each test in many thread orders, gives some loads an older value that the memory model
//!   permits, and finds data races on the cells. Its spin hint lets another thread run.
//!   `cargo xtask explore` runs these tests.

#[cfg(all(test, loom, feature = "std"))]
pub(crate) use loom::thread;
#[cfg(all(test, loom, feature = "local"))]
pub(crate) use loom::thread_local;
#[cfg(all(test, loom))]
pub(crate) use loom::{
    cell::UnsafeCell,
    hint,
    sync::{Arc, atomic},
};
#[cfg(all(not(all(test, loom)), not(feature = "std")))]
pub(crate) use native::Mutex;
#[cfg(all(not(all(test, loom)), feature = "std"))]
pub(crate) use std::thread;
#[cfg(all(not(all(test, loom)), feature = "local"))]
pub(crate) use std::thread_local;
#[cfg(not(all(test, loom)))]
pub(crate) use {alloc::sync::Arc, core::hint, core::sync::atomic, native::UnsafeCell};

#[cfg(not(all(test, loom)))]
mod native {
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

    /// The lock without `std`: a spin lock, for targets on which nothing preempts a holder of the
    /// lock, such as single-threaded WebAssembly. Its users hold it for a few instructions and run
    /// no foreign code under it, so a spin waits for those instructions only. A preempted holder
    /// makes the others spin until it runs again. A target with one thread never spins.
    #[cfg(not(feature = "std"))]
    pub(crate) struct Mutex<T> {
        locked: core::sync::atomic::AtomicBool,
        value: core::cell::UnsafeCell<T>,
    }

    #[cfg(not(feature = "std"))]
    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Self {
                locked: core::sync::atomic::AtomicBool::new(false),
                value: core::cell::UnsafeCell::new(value),
            }
        }

        pub(crate) fn lock(&self) -> Guard<'_, T> {
            use core::sync::atomic::Ordering;
            // Ordering: `Acquire` reads the `Release` of the last unlock, so each change of the
            // last holder happens before this holder's access.
            while self
                .locked
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                core::hint::spin_loop();
            }
            Guard(self)
        }
    }

    #[cfg(not(feature = "std"))]
    #[expect(unsafe_code, reason = "the lock word gives the value to one holder at a time")]
    // SAFETY: the lock moves the value between threads, so `T` is `Send`, and only its holder gets
    // access to it.
    unsafe impl<T: Send> Sync for Mutex<T> {}

    /// The holder of a [`Mutex`]. Its drop unlocks.
    #[cfg(not(feature = "std"))]
    pub(crate) struct Guard<'a, T>(&'a Mutex<T>);

    #[cfg(not(feature = "std"))]
    impl<T> core::ops::Deref for Guard<'_, T> {
        type Target = T;

        #[expect(unsafe_code, reason = "the guard holds the lock")]
        fn deref(&self) -> &T {
            // SAFETY: the guard holds the lock, so no other access to the value runs.
            unsafe { &*self.0.value.get() }
        }
    }

    #[cfg(not(feature = "std"))]
    impl<T> core::ops::DerefMut for Guard<'_, T> {
        #[expect(unsafe_code, reason = "the guard holds the lock")]
        fn deref_mut(&mut self) -> &mut T {
            // SAFETY: the guard holds the lock, and the `&mut` guard gives the only access.
            unsafe { &mut *self.0.value.get() }
        }
    }

    #[cfg(not(feature = "std"))]
    impl<T> Drop for Guard<'_, T> {
        fn drop(&mut self) {
            // Ordering: `Release` publishes the changes of this holder to the next one.
            self.0.locked.store(false, core::sync::atomic::Ordering::Release);
        }
    }
}

/// A lock for short changes of shared state. Its users hold it for a few instructions, and run no
/// waker, no drop of a permit, and no other foreign code under it. A poisoned lock is used as is:
/// the users change their state in steps that cannot panic.
#[cfg(any(all(test, loom), feature = "std"))]
pub(crate) struct Mutex<T>(Lock<T>);

#[cfg(all(test, loom))]
use loom::sync::Mutex as Lock;
#[cfg(all(not(all(test, loom)), feature = "std"))]
use std::sync::Mutex as Lock;

#[cfg(any(all(test, loom), feature = "std"))]
impl<T> Mutex<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(Lock::new(value))
    }

    pub(crate) fn lock(&self) -> impl core::ops::DerefMut<Target = T> + '_ {
        match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
