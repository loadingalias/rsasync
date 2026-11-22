//! The process-supervision example of rsasync. A task on one local worker spawns two children with
//! piped standard streams. It writes the input of each child while both of its outputs drain, in
//! one join. Then an interrupt signal arrives, and the task stops each child: `SIGTERM`, and a kill
//! at a deadline when the child ignores it. Then a shutdown reports no live debt and no running
//! child.
//!
//! Run it on Linux with `cargo run -p process_supervision`. It prints one line for each outcome. It
//! sends the interrupt to itself with `kill -s INT`, as Ctrl-C at a terminal does.

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
    Err("the process-supervision example needs Linux on x86_64 or aarch64".into())
}

/// The outcomes that the example prints.
#[cfg(test)]
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
// The same text as the expected output in `docs/content/runtime.md`. Edit both.
const EXPECTED: &str = r"interrupt: the stream of SIGINT is ready, and a second SIGINT ends the process
io: the child that obeys SIGTERM took 262144 bytes while its stdout and stderr drained
io: its stdout gave 262144 bytes and its stderr gave 262144 bytes, each the same as the input: true
io: the child that ignores SIGTERM took 262144 bytes while its stdout and stderr drained
io: its stdout gave 262144 bytes and its stderr gave 262144 bytes, each the same as the input: true
interrupt: kill -s INT sent SIGINT to this process, and the stream reported it
stop: the child that obeys SIGTERM ended by signal 15 before the deadline of 1s
stop: the child that ignores SIGTERM ran on to the deadline of 1s, and the kill ended it by signal 9
shutdown: 0 tasks cancelled, 0 operations of debt, 0 children run on, grace period expired: false
";

/// The example prints the documented outcomes.
#[cfg(test)]
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
#[test]
fn the_example_prints_its_documented_outcomes() {
    let mut output = Vec::new();
    journey::run(&mut output).unwrap();
    assert_eq!(String::from_utf8(output).unwrap(), EXPECTED);
}
