//! Dual async runtime. This crate is in development.
//!
//! The [`buffer`], [`capacity`], and [`time`] modules hold the types that the runtime shares with
//! networking and applications. Use them through this crate:
//!
//! ```
//! use rsasync::{buffer, capacity, time};
//! ```
//!
//! Without the `std` feature, which is on by default, the crate needs only `core` and `alloc`.

#![no_std]

extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

mod sync;

// The io_uring declarations are checked against the kernel headers of these two architectures
// only. Another architecture has different constants, so it gets no ring.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    feature = "std"
))]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the tests use the ring now; the driver is its first other user")
)]
mod uring;

// By name, not `*`: a module of this crate with one of these names is then a
// compile error, not a module that hides the shared one.
#[doc(inline)]
pub use rstypes::{buffer, capacity, time};
