//! Dual async runtime. This crate is in development.
//!
//! The [`buffer`], [`capacity`], and [`time`] modules hold the types that the runtime shares with
//! networking and applications. Use them through this crate:
//!
//! ```
//! use rsasync::{buffer, capacity, time};
//! ```

// By name, not `*`: a module of this crate with one of these names is then a
// compile error, not a module that hides the shared one.
#[doc(inline)]
pub use rstypes::{buffer, capacity, time};
