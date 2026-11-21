//! The standard-futures example of rsasync. A task on one local worker combines its I/O with the
//! futures and combinators of the standard library only: `core::future::join!`, `poll_fn`, and a
//! small select that this example writes with `poll_fn`. Its joins borrow data on the stack of the
//! task. It shows partial progress, the reuse of a buffer after a success and of a lease after a
//! failure, and a receive that a select abandons. Then a shutdown reports no live debt.
//!
//! Run it on Linux with `cargo run -p standard_futures`. It prints one line for each outcome.

// `core::future::join!` polls several futures in one task, on the targets of the runtime.
#![cfg_attr(
    all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")),
    feature(future_join)
)]

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod journey;

/// Runs the example, and prints its outcomes on standard output.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
fn main() -> Result<(), Box<dyn core::error::Error>> {
    journey::run(&mut std::io::stdout().lock())
}

/// The local workers need Linux on x86_64 or aarch64.
#[cfg(not(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64"))))]
fn main() -> Result<(), Box<dyn core::error::Error>> {
    Err("the standard-futures example needs Linux on x86_64 or aarch64".into())
}

/// The outcomes that the example prints.
#[cfg(test)]
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
// The same text as the expected output in `docs/content/runtime.md`. Edit both.
const EXPECTED: &str = r#"join: a send and a receive ran together, and the receive gave "hello", as the stack data of the task says
partial: a receive into 4 bytes gave "hell"
partial: the same buffer then received the rest: "o"
select: the timer came first, and the receive with no data dropped
select: the next receive on the same stream gave "after"
failure: a send after the half-close gave BrokenPipe and its lease back
failure: the same lease then sent "again" on a new stream
shutdown: 0 tasks cancelled, 0 operations of debt, grace period expired: false
"#;

/// The example prints the documented outcomes.
#[cfg(test)]
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
#[test]
fn the_example_prints_its_documented_outcomes() {
    let mut output = Vec::new();
    journey::run(&mut output).unwrap();
    assert_eq!(String::from_utf8(output).unwrap(), EXPECTED);
}
