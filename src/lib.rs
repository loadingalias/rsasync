//! Dual async runtime. This crate is in development.
//!
//! The [`buffer`], [`capacity`], and [`time`] modules hold the types that the runtime shares with
//! networking and applications. Use them through this crate:
//!
//! ```
//! use rsasync::{buffer, capacity, time};
//! ```

#[doc(inline)]
pub use rstypes::{buffer, capacity, time};
