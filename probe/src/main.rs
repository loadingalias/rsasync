//! The early probe of rsasync: TCP request/response on local workers against a baseline server,
//! and file operations on a driver that its own loop drives: reads at a queue depth, and durable
//! writes at queue depth 1.
//!
//! `probe server` runs an echo server on local workers, one reuse-port listener on each worker.
//! `probe client` offers an open-loop load to a server and prints one JSON line: the offered and
//! the achieved rate, and the latency and the send lag at the 50th, 99th, and 99.9th percentiles.
//! The same client measures each server, so a ratio of two servers comes from one method.
//!
//! ```sh
//! probe server --workers 4 --port 7000 --completions 64
//! probe client --target 10.0.0.2:7000 --connections 1000 --rate 200000 --workers 15 \
//!     --warmup 10 --window 60 --seed 1
//! probe file --path /mnt/probe/data --op read --block 4096 --size 4294967296 --count 40000 \
//!     --seed 1 --drive turn --depth 32
//! ```
//!
//! `probe file` prints one JSON line: the latency at the 50th, 99th, and 99.9th percentiles, the
//! turns, waits, and voluntary context switches of all of the operations, and the CPU time of the
//! process. `probe/run-file.sh` compares it with fio at queue depth 1 on one host, and
//! `probe/run-roofline.sh` compares its reads with fio and `t/io_uring` at each queue depth.

#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod client;
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod file;
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod histogram;
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
mod server;

/// The bytes of each request, and of its response.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
const SIZE: usize = 1024;

/// Runs the subcommand of the arguments.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
fn main() -> Result<(), String> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match arguments.split_first() {
        Some((command, rest)) if command == "server" => server::run(&server::Plan::parse(rest)?),
        Some((command, rest)) if command == "client" => client::run(client::Plan::parse(rest)?),
        Some((command, rest)) if command == "file" => file::run(&file::Plan::parse(rest)?),
        _ => Err(
            "usage: probe server --workers N --port P --completions N | probe client --target ADDRESS \
                  --connections N --rate PER_SECOND --workers N --warmup SECONDS --window SECONDS \
                  --seed N | probe file --path FILE --op read|write --block BYTES --size BYTES \
                  --count N --seed N --drive turn|spin [--depth N]"
                .to_owned(),
        ),
    }
}

/// The local workers need Linux on x86_64 or aarch64.
#[cfg(not(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64"))))]
fn main() -> Result<(), String> {
    Err("the probe needs Linux on x86_64 or aarch64".to_owned())
}

/// The value after `--name` in `arguments`, parsed.
#[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
fn value<T: core::str::FromStr>(arguments: &[String], name: &str) -> Result<T, String> {
    let flag = format!("--{name}");
    let text = arguments
        .iter()
        .position(|argument| *argument == flag)
        .and_then(|index| arguments.get(index.checked_add(1)?))
        .ok_or_else(|| format!("{flag} needs a value"))?;
    text.parse()
        .map_err(|_| format!("{flag}: {text:?} is not a valid value"))
}
