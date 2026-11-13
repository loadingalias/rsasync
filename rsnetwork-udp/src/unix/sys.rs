//! The declarations of the datagram calls, their message headers, and their control messages, for the target
//! system. An item with the same name has the same meaning on each system, with the value and the layout of that
//! system.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use macos::*;
