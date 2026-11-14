//! The driver-only example of rsasync. The program owns an io_uring driver and drives it from its
//! own loop, with no scheduler in the build. It does file, TCP, and UDP I/O on owned buffers. It
//! shows partial progress, a cancel, dropped futures, and the reuse of each buffer after a success
//! and after a failure. Then it closes each resource and ends the driver with no live operation.
//!
//! Run it on Linux with `cargo run -p driver_only`. It prints one line for each outcome.

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod journey;

/// Runs the example, and prints its outcomes on standard output.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
fn main() -> Result<(), Box<dyn core::error::Error>> {
    journey::run(&mut std::io::stdout().lock())
}

/// The driver needs Linux on x86_64 or aarch64.
#[cfg(not(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64"))))]
fn main() -> Result<(), Box<dyn core::error::Error>> {
    Err("the driver-only example needs Linux on x86_64 or aarch64".into())
}

/// The outcomes that the example prints on a kernel that has each feature of the capability
/// report.
#[cfg(test)]
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
// The same text as the expected output in `docs/content/driver-only.md`. Edit both.
const EXPECTED: &str = r#"report: 0 helper threads; set_file_len route: Ring
file: wrote 11 bytes and made them durable
file: an exact read of 16 bytes stopped at the end of the file after 11 bytes: "hello world"
file: set the length to 5, and the length is 5
file: the same buffer then read 5 bytes: "hello"
file: closed and removed the file
tcp: the receive woke its current waiter 1 time and its earlier waiter 0 times
tcp: the receive gave 8 of the 11 sent bytes: "hello wo"
tcp: the same buffer then received the rest: "rld"
tcp: the cancelled receive gave ECANCELED and its buffer back
tcp: the same buffer then received "again"
tcp: after a dropped receive, the next receive gave "late"
tcp: closed the listener after a dropped accept
tcp: closed both streams; Closed::sent() gives [None, None]
udp: a receive into 4 bytes gave "ping", truncated: true
udp: the same buffer then received "pong", truncated: false
udp: after a dropped receive, the next receive gave "late"
udp: closed both sockets
driver: teardown cancelled 0 operations and kept 0 children
blocking service: teardown found 0 running jobs and dropped 0 jobs
"#;

/// The example prints the documented outcomes on this kernel. The route of `set_file_len` is
/// the one line that depends on the kernel: before Linux 6.9, it is `BlockingService`.
#[cfg(test)]
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
#[test]
fn the_example_prints_its_documented_outcomes() {
    let mut output = Vec::new();
    journey::run(&mut output).unwrap();
    let output = String::from_utf8(output).unwrap();
    let output = output.replacen("set_file_len route: BlockingService", "set_file_len route: Ring", 1);
    assert_eq!(output, EXPECTED);
}
