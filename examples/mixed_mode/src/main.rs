//! The mixed-mode example of rsasync. One runtime has one local worker, two movable workers, and a
//! compute service under one host plan. A local task keeps its state in an `Rc`, and it hands
//! typed requests to a movable task through bounded channels; the movable task sums on the compute
//! service and replies. A movable stream sends to a local stream. The example shows partial
//! progress, the reuse of a value after a failed handoff, a wait that drops, a cancel of a movable
//! task from the local task, and a shutdown that joins both modes with no live debt.
//!
//! Run it on Linux with `cargo run -p mixed_mode`. It prints one line for each outcome.

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod journey;

/// Runs the example, and prints its outcomes on standard output.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
fn main() -> Result<(), Box<dyn core::error::Error>> {
    journey::run(&mut std::io::stdout().lock())
}

/// The workers need Linux on x86_64 or aarch64.
#[cfg(not(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64"))))]
fn main() -> Result<(), Box<dyn core::error::Error>> {
    Err("the mixed-mode example needs Linux on x86_64 or aarch64".into())
}

/// The outcomes that the example prints.
#[cfg(test)]
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
// The same text as the expected output in `docs/content/mixed-mode.md`, Run the examples. Edit both.
const EXPECTED: &str = r#"plan: worker 0 is local, workers 1 and 2 are movable, and 1 compute thread runs beside them
handoff: the local task sent 3 requests, and the movable task sent back the sums [45, 4950, 499500]
handoff: a full channel gave the request back, and the same request then went through: 10
tcp: the local stream received 8 of the 11 bytes that the movable stream sent: "hello wo"
tcp: the same buffer then received the rest: "rld"
state: one Rc kept the 4 notes of the local task on worker 0
cancel: the local task cancelled the movable task that waited: Cancelled
handoff: the movable task found that the local task dropped its wait, and the reply came back
shutdown: 1 local and 1 movable task cancelled, 0 operations of debt, 0 compute jobs ran at the end, grace period expired: false
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
