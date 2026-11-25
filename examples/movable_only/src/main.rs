//! The movable-only example of rsasync. A runtime with two movable workers and no local worker
//! runs `Send` tasks in its work-stealing pool, and they do TCP I/O with the types of
//! `rsasync::movable`, which move with their task to each worker that polls it. Many echo tasks
//! and tasks that use the CPU make task and I/O pressure. The example shows partial progress, the
//! reuse of a buffer after a success and of a lease after a failure, a dropped receive, and the
//! close of each resource. Then a shutdown cancels the task that still waits, and it reports no
//! live debt.
//!
//! Run it on Linux with `cargo run -p movable_only`. It prints one line for each outcome.

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod journey;

/// Runs the example, and prints its outcomes on standard output.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
fn main() -> Result<(), Box<dyn core::error::Error>> {
    journey::run(&mut std::io::stdout().lock())
}

/// The movable workers need Linux on x86_64 or aarch64.
#[cfg(not(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64"))))]
fn main() -> Result<(), Box<dyn core::error::Error>> {
    Err("the movable-only example needs Linux on x86_64 or aarch64".into())
}

/// The outcomes that the example prints.
#[cfg(test)]
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
// The same text as the expected output in `docs/content/mixed-mode.md`, Run the examples. Edit both.
const EXPECTED: &str = r#"pressure: 64 echo tasks each got their 4 KiB back in order
pressure: 8 tasks that use the CPU each summed 1000000 numbers
tcp: the receive gave 8 of the 11 sent bytes: "hello wo"
tcp: the same buffer then received the rest: "rld"
tcp: after a dropped receive, the next receive gave "late"
tcp: a send to a peer that reset failed, and the same lease then went on a new stream: "again"
tcp: closed each stream and the listener
shutdown: 1 movable task cancelled, 0 operations of debt, grace period expired: false
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
