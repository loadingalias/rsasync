# Platform support

rsasync is in development, and its alpha releases are on crates.io. This page gives the Linux C
libraries that rsasync supports, the WebAssembly targets of its portable subset, and the evidence
that its tests give today.

## Linux C libraries

Linux support includes two C libraries, glibc and musl, on x86_64 and aarch64:

| C library | Rust targets |
| --- | --- |
| glibc | `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` |
| musl | `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl` |

The pinned toolchain in `rust-toolchain.toml` includes the two musl targets. The datagram calls of
`rsnetwork-udp` support the same four targets: see [Datagram I/O on any socket](datagram-io.md).

## Evidence today

On each push to `main`, CI builds, links, and runs the tests on x86_64 Linux two times, with
these commands:

| C library | Command |
| --- | --- |
| glibc | `cargo xtask test` |
| musl | `cargo xtask test --target x86_64-unknown-linux-musl` |

Each command runs the tests of the whole workspace. Only the glibc run includes the doctests.

Today, the tests check the repository tools, the shared types, and the io_uring ring of the Linux
driver: ring creation, a message from one ring to another, the probes of kernel features, the
operation table with the completions of the kernel, the driver-only surface (turns, waits, wakes,
and teardown), and TCP on the driver. The TCP tests bind, listen, and accept; receive into an
owned buffer and into a buffer that the kernel selects; and send from a lease. The receive into an
owned buffer and the send from a lease keep the addresses of their buffers and allocate nothing
for each operation. Only the driver and `rsnetwork-udp` make system calls: the io_uring ring and
its waits in the kernel, its two event descriptors, the bind and listen of a TCP socket through
`std`, and the datagram calls of `rsnetwork-udp`. Thus a pass shows that the build links and runs
on musl, and that these calls work through the system calls of musl. It does not show that a
runtime with a scheduler works on musl: the library has no scheduler yet. From the Linux driver stage on, each stage exit
also runs the tests on x86_64 and aarch64 Linux hosts with both C libraries. This page then names
each operation that has that evidence.

## WebAssembly

The portable subset is the part of rsasync that runs on WebAssembly. Today it is the `rstypes`
crate, which holds the shared buffer and time types. The subset builds without `std`, and it uses
only `core` and `alloc`.

| Target | What runs | Evidence today |
| --- | --- | --- |
| `wasm32-unknown-unknown` | The build of `rstypes` without `std` | The build compiles. No test runs on this target |
| `wasm32-wasip1` | The tests of `rstypes`, with `rstypes` built without `std`, in Wasmtime | The tests pass in Wasmtime 50.0.1 on an aarch64 macOS host |

Both targets are 32-bit and have one thread. The tests on `wasm32-wasip1` use the same `rstypes`
code and features as the `wasm32-unknown-unknown` build. The pinned toolchain in
`rust-toolchain.toml` includes the two targets.

### Limits

- On `wasm32-unknown-unknown`, `std` compiles, but its clocks, threads, and blocking waits fail
  when they run. The subset does not use `std`, so it cannot call them.
- A test that needs a second thread does not run on these targets.
- The doctests do not run on WebAssembly.
- A test that panics stops its process. Nextest runs each test in a separate process, so it
  reports that test as failed, with the panic message.
- CI does not run these commands. A WebAssembly result does not depend on the host.

### Build and test the subset

You need Wasmtime on your `PATH`; `.cargo/config.toml` makes it the test runner for
`wasm32-wasip1`. Run these commands from the repository root:

```sh
cargo build --locked -p rstypes --target wasm32-unknown-unknown --no-default-features
cargo xtask test -p rstypes --target wasm32-wasip1 --no-default-features
```

`--no-default-features` builds `rstypes` without its `std` feature. The test harness uses the
`std` of `wasm32-wasip1`, but the crate under test does not. The test command passes when Nextest
reports that each test passed, for example `8 tests run: 8 passed, 0 skipped`.
