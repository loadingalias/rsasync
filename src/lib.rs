//! Dual async runtime. This crate is in development.
//!
//! The [`buffer`], [`capacity`], and [`time`] modules hold the types that the runtime shares with
//! networking and applications. Use them through this crate:
//!
//! ```
//! use rsasync::{buffer, capacity, time};
//! ```
//!
//! The [`channel`] module holds the bounded channels, [`acquire`] holds the waiting reservation of
//! capacity, [`clock`] holds the native and the injected clocks, and [`timer`] holds bounded
//! timers. None of them needs a scheduler or driver. With the `local` feature, which turns on
//! `std`, the [`task`] module runs local tasks on a local owner.
//!
//! With the `std` feature, the `blocking` module holds the bounded blocking service, which runs
//! blocking host work on a fixed number of threads.
//!
//! The features `std` and `local` are on by default. Without `std`, the crate needs only `core`
//! and `alloc`. With `std` and not `local`, the driver builds with no scheduler.

#![no_std]

extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

pub mod acquire;
// The blocking service needs the threads of `std`.
#[cfg(feature = "std")]
pub mod blocking;
pub mod channel;
// The completion entries of the driver, which the exploration tests also check on each host.
#[cfg(any(
    all(test, loom),
    all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64"),
        feature = "std"
    )
))]
#[cfg_attr(
    not(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64"),
        feature = "std"
    )),
    expect(dead_code, reason = "with no driver, only the exploration tests use the entries")
)]
mod completion;
// The poll budget of the runtime leaves.
mod coop;
mod sync;
#[cfg_attr(
    not(all(
        test,
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64"),
        feature = "std"
    )),
    expect(
        dead_code,
        reason = "the driver uses only single operations so far, and only the driver uses some items"
    )
)]
mod table;
mod wait;

pub mod clock;
#[cfg(feature = "local")]
pub mod task;
pub mod timer;

/// Driver-only use of the driver, on io_uring or epoll, on Linux.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    feature = "std"
))]
pub mod driver;

// The io_uring declarations are checked against the kernel headers of these two architectures
// only. Another architecture has different constants, so it gets no ring.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    feature = "std"
))]
#[cfg_attr(
    not(all(test, not(loom))),
    expect(
        dead_code,
        reason = "the driver uses only some items. The tests of the ring and the driver use the others, and the loom build leaves those tests out"
    )
)]
mod uring;

// By name, not `*`: a module of this crate with one of these names is then a
// compile error, not a module that hides the shared one.
#[doc(inline)]
pub use rstypes::{buffer, capacity, time};

/// The datagram types of the UDP operations of the driver, and datagram I/O on any socket, with
/// no driver: the `rsnetwork-udp` crate, so that its users add no dependency.
#[cfg(feature = "std")]
pub use rsnetwork_udp as datagram;
