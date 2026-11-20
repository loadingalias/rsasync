//! The types that the rsasync runtime shares with networking and applications: owned buffers,
//! capacity, and time. This crate has no scheduler and no driver, and it starts nothing.
//!
//! An application does not name this crate. It uses these types through `rsasync`, or through
//! the library whose API uses them. A library that uses them in its public API re-exports them.
//!
//! Without the `std` feature, which is on by default, the crate needs only `core` and `alloc`.

#![no_std]
// `Arc::try_new`: each allocation on a refusal path refuses, and does not abort
// (docs/content/unstable-features.md). The loom form of `sync::try_arc` does not use it.
#![cfg_attr(not(all(test, loom)), feature(allocator_ext))]

extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

// rsasync re-exports these modules by name. Write each doc example in them
// through `rsasync::`, the path that users write, after the hidden line
// `# extern crate rstypes as rsasync;`, so it builds without the runtime.
pub mod buffer;
pub mod capacity;
mod sync;
pub mod time;
