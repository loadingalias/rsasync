//! The local-only example of rsasync. One task on one local worker keeps its state in an `Rc`
//! across each wait, and it does file, TCP, UDP, and IPC stream I/O with the types of
//! `rsasync::local`, with no driver argument. It shows partial progress, the reuse of a buffer after
//! a success and after a failure, a dropped receive, and the close of each resource. Then a shutdown
//! reports no live debt.
//!
//! Run it on Linux with `cargo run -p local_only`. It prints one line for each outcome.

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
    Err("the local-only example needs Linux on x86_64 or aarch64".into())
}

/// The outcomes that the example prints.
#[cfg(test)]
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
// The same text as the expected output in `docs/content/runtime.md`. Edit both.
const EXPECTED: &str = r#"file: wrote 11 bytes and made them durable
file: an exact read of 16 bytes stopped at the end of the file after 11 bytes: "hello world"
file: the same buffer then read 5 bytes: "world"
file: closed and removed the file
tcp: the receive gave 8 of the 11 sent bytes: "hello wo"
tcp: the same buffer then received the rest: "rld"
tcp: after a dropped receive, the next receive gave "late"
tcp: closed both streams and the listener
udp: a receive into 4 bytes gave "ping", truncated: true
udp: the same buffer then received "pong", truncated: false
udp: closed both sockets
ipc: the server received "hello" from a peer of this user: true
ipc: closed both streams and the listener
state: one Rc kept the 13 notes of the task on worker 0
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
