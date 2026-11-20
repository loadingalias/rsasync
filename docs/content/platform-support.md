# Platform support

rsasync is in development, and its alpha releases are on crates.io. This page gives the Linux C
libraries that rsasync supports, the hosts of the io_uring driver, the macOS and Windows parts of
the driver and of `rsnetwork-udp`, the WebAssembly targets of its portable subset, and the evidence
that its tests give today.

## Linux C libraries

Linux support includes two C libraries, glibc and musl, on x86_64 and aarch64:

| C library | Rust targets |
| --- | --- |
| glibc | `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` |
| musl | `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl` |

The pinned toolchain in `rust-toolchain.toml` includes the two musl targets. The datagram calls of
`rsnetwork-udp` support the same four targets: see [Datagram I/O on any socket](datagram-io.md).

## The io_uring driver on Linux

The driver-only surface, `rsasync::driver` on io_uring, is supported on these four hosts, each
with glibc and musl:

| Distribution | Architecture | Kernel |
| --- | --- | --- |
| Debian 12 | x86_64 | `6.1.0-54-cloud-amd64` |
| Debian 12 | aarch64 | `6.1.0-54-cloud-arm64` |
| Ubuntu 26.04 | x86_64 | `7.0.0-1016-aws` |
| Ubuntu 26.04 | aarch64 | `7.0.0-1016-aws` |

On 2026-11-14, at revision `10f7987`, one run on each host passed these commands. Each host was an
AWS EC2 instance:

```sh
cargo xtask test
cargo xtask test --target <arch>-unknown-linux-musl   # x86_64 or aarch64
cargo run -p driver_only
```

Each test command ran 276 tests of the whole workspace, and the glibc command also ran the doctests.
Six tests need root or `CAP_SYS_TIME`, so they did not run. The last command runs the example of
[Drive the I/O driver yourself](driver-only.md#run-the-example), and it printed the documented
output on each host. The tests cover file, TCP, UDP, local stream, and pipe operations on owned
buffers, child processes, signal streams, cancellation, close, teardown, and the capability report.
The negative-path cases also run under injected completions. Debian 12 and Ubuntu 26.04 take
different routes for some calls: see the [capability report](capability-report.md).

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
and teardown), and TCP and UDP on the driver. The TCP tests bind, listen, and accept; receive into
an owned buffer and into a buffer that the kernel selects; send from a lease and from several
regions; connect; half-close; set and read the socket options; and close. Bind and listen run on the
ring where the kernel has them (Ubuntu 26.04), and as system calls elsewhere (Debian 12). They also
check that a dropped, forgotten, or cancelled receive, send, or accept loses no byte and no
connection, on IPv4 and IPv6. The UDP tests send and receive datagrams with their metadata,
segmented sends and coalesced receives, batches in one kernel entry, and truncation. They also
check that a dropped receive keeps its datagram. The [file](files.md) tests, on ext4, read and write
at an offset, also vectored and with direct I/O, read exactly, inspect and change the length,
preallocate, punch holes, sync, rename, unlink, and sync a directory. The length change runs on the
ring on Ubuntu 26.04, and on the blocking service on Debian 12. They also check that a dropped write
changes the file and keeps its lease until its completion. The [local stream](local-streams.md)
tests bind at a path with a socket file of mode 0600, refuse the path of a live listener, replace
only a stale socket file, refuse a path that does not fit a socket address, and carry a numbered
stream with no gap through a dropped accept, send, and receive. Run as root on Debian 12 on x86_64,
they also check that a second OS user cannot connect, that an accepted stream reports the user,
group, and process of its peer, and that a bind does not replace the socket file of another user.
The [child process](processes.md) tests spawn and adopt children, report each exit once, signal and
kill through the process descriptor, kill a process tree, and apply each end policy. They also check
that the driver installs no `SIGCHLD` handler and never reaps a child that the test started itself.

The receive into an owned buffer and the send from a lease keep the addresses of their buffers and
allocate nothing for each operation. The tests also check the
[blocking service](blocking-service.md): its bound of jobs, its refusals, its teardown, and the
charges of its threads. Only the driver, the blocking service, the local tasks, and `rsnetwork-udp`
make system calls: the io_uring ring and its waits in the kernel, its two event descriptors, the
socket, option, and bind calls of a socket, the listen call of a TCP socket, the `statx` call of a
direct-I/O open, the threads that the blocking service starts through `std` and their length changes
of a file and spawns of a child, the process descriptor, `waitid`, and `kill` calls of a child, the
park and unpark of the thread of a local owner through `std`, and the datagram calls of
`rsnetwork-udp`. Thus a pass shows that the build links and runs on musl, and that these calls work
through the system calls of musl.

Each stage exit also runs the tests on x86_64 and aarch64 Linux hosts with both C libraries, as
[The io_uring driver on Linux](#the-io_uring-driver-on-linux) shows for the driver.

## The runtime on Linux

The [runtime](runtime.md), `rsasync::runtime`, is on Linux only. On 2026-11-20, `cargo xtask test`
passed with glibc on two AWS EC2 hosts:

| Distribution | Architecture | Kernel | Instance |
| --- | --- | --- | --- |
| Debian 12 | x86_64 | `6.1.0-56-cloud-amd64` | c8i.4xlarge, 8 cores with SMT |
| Ubuntu 26.04 | aarch64 | `7.0.0-1018-aws` | c8g.4xlarge, 16 cores |

Each run passed 463 tests and the doctests. The runtime tests check a start that fails at one
worker of four at the descriptor limit, the factory spawn and its refusals, a cancel of a factory
spawn before and after its worker places it, `Dedicated` placement on one CPU of each physical core,
an `Exact` CPU outside the CPU set, the release of the operations of `block_on` before it returns,
a root that starts no driver, a wake after the first operation, and the generator of each owner.
They run on io_uring, and the factory spawn, the release, and the wake also run on epoll. The final
form of two of these tests, the failed start and the release, ran only on the x86_64 host. No host
with two NUMA nodes or two cache domains ran them, so the spread of `Domain` and `Dedicated` over
nodes and domains has only the test of the plan on the facts of such a host.

## macOS

The driver runs TCP, UDP, local streams, [pipes](pipes.md), [child processes](processes.md),
[signal streams](signals.md), and files on macOS on aarch64 (`aarch64-apple-darwin`), on
[kqueue](kqueue.md), in driver-only use. The files run on your blocking service, with the macOS
calls of [Files](kqueue.md#files). The datagram calls of `rsnetwork-udp` also support macOS on aarch64:
see [Datagram I/O on any socket](datagram-io.md). CI does not run on macOS.

On 2026-11-17, these runs passed on an aarch64 Mac (MacBookPro18,3, Apple M1 Pro) with macOS 27.2.1
(build 26B131), Darwin 27.2.0, xnu-13432.41.4:

- At revision `b5f0d71`, `cargo xtask test -j 2` ran the 197 tests of the whole workspace, and
  `cargo test --locked --doc --workspace` ran the 25, 1, and 9 doctests. The tests include the TCP,
  UDP, and ownership cases of the driver suites on kqueue, and the tests of `rsnetwork-udp` through
  Tokio sockets.
- In the same tree, a C build of the macOS 27.2 SDK headers showed that the 57 checked values and
  layouts and the 18 checked signatures of the new macOS declarations of the driver and
  `rsnetwork-udp` match them.
- At revision `9a90403`, `cargo xtask test -j 2` ran the 208 tests of the whole workspace, and
  `cargo test --locked --doc --workspace` ran the 27, 1, and 9 doctests. The tests include the file
  cases of the driver suites on kqueue, on APFS: reads, writes, direct I/O, length, preallocation,
  hole punching, names, and directory sync.
- In the same tree, a C build of the macOS 27.2 SDK headers showed that the 47 checked values and
  layouts and the 12 checked signatures of the macOS file declarations of the driver match them.
  Revision `f8ea665` is `9a90403` on a later `main`, whose change is only in `models/` and the
  design-models page.
- A probe on exFAT and FAT32 disk images (`hdiutil`, with no root) showed that `F_PUNCHHOLE` gives
  `ENOTTY` there, which the driver gives as `Unsupported`, and that `F_PREALLOCATE` and
  `F_FULLFSYNC` work there.

On the same Mac, the tests of `rsnetwork-udp` pass with each of these commands:

```sh
cargo xtask test -p rsnetwork-udp
cargo xtask test -p rsnetwork-udp --features macos-private-batch
```

## Windows

The datagram calls of `rsnetwork-udp` also support Windows on x86_64 (`x86_64-pc-windows-msvc`): see
[Datagram I/O on any socket](datagram-io.md#windows). The driver runs TCP, UDP, files, and local
streams on Windows x86_64, on an [I/O completion port](iocp.md), in driver-only use. Where the
system has an I/O ring of version 3, the reads, writes, and syncs of files run on the ring
([IoRing files](iocp.md#ioring-files)). The files run on NTFS, with the other file calls on your
blocking service ([Files](iocp.md#files)). Pipes, processes, and signals of the driver have no
Windows form yet. CI does not run on Windows.

On 2026-11-15, these runs passed on an AWS EC2 c8i.2xlarge instance with
Windows Server 2025, OS build 10.0.26100.33822. The tests use Tokio sockets. Revision `285cc44` has
the same `rsnetwork-udp` files and the same datagram page as the two trees that ran:

- Run `20261115T062154Z-19708`: `cargo xtask test` ran the 153 tests of the whole workspace and
  the doctests. In the same run, an MSVC build of the Windows SDK 10.0.28000.0 headers showed that
  the 40 checked `ws2_32` declarations of `rsnetwork-udp` match them.
- Run `20261115T063750Z-50670`: `cargo xtask test -p rsnetwork-udp` ran the 13 tests of the
  crate.

On the same instance type and OS build, run `20261115T092057Z-52931` of revision `ac235f2` ran
`cargo xtask test`: 211 tests of the workspace and the doctests passed. They include the 23 TCP
cases of the driver suites on the port, each one with skip-on-success and with no skip. In the same
run, an MSVC build of the Windows SDK headers showed that the 53 checked values and layouts and the
15 signatures of the IOCP declarations of the driver match them.

UDP on the port then passed in run `20261115T100000Z-87983` of the same instance type and OS
build: `cargo xtask test` ran 228 tests of the workspace and the doctests. They include the 31 TCP
and UDP cases of the driver suites, each one with skip-on-success and with no skip, and the header
check matched 57 values and layouts. Revision `067075e` differs from the tree of that run only in
the classification of a warning at once on a skipping socket, which no host run can make, and in
its docs.

Files on the port then passed in run `20261118T060435Z-36193` of revision `7855dd4`, on the same
instance type and OS build: `cargo xtask test` ran 252 tests of the workspace and the doctests.
They include the 12 file cases of the driver suites on NTFS, each one with skip-on-success and with
no skip, and the header check matched 41 values and layouts and 17 signatures of the file and
device declarations. On that host, the disk is an EBS volume behind the AWS NVMe adapter
(`AWSNVMe`) and the disk class driver (`disk`), so `Driver::nvme_path` gives `NvmePath::Other`, as
the driver service of the disk says. No run shows `NvmePath::Native`, which needs a host with the
native NVMe path on. A directory sync gives `Unsupported` on Windows.

IoRing files then passed in run `20261119T130017Z-65169` of revision `ba8c9c5`, on the same
instance type and OS build: `cargo xtask test` ran 285 tests of the workspace and the doctests.
They include the 14 file cases of the driver suites on IoRing, and the same cases on the port with
`Backend::Iocp`. The permissive selection gave `Backend::IoRing`. In the same run, the header check
matched 22 values, 22 layouts, and 15 signatures of the IoRing and loader declarations.

Windows 11 is not run, and Windows on aarch64 has no run evidence yet.

## WebAssembly

The portable subset is the part of rsasync that runs on WebAssembly. It builds without `std`, and
it uses only `core` and `alloc`. It needs no scheduler and no driver. It has these items:

| Item | Path | Not in the subset |
| --- | --- | --- |
| The shared buffer, capacity, and time types | `rsasync::buffer`, `rsasync::capacity`, `rsasync::time` (the `rstypes` crate) | Nothing |
| The channels (see [Channels](channels.md)) | `rsasync::channel::spsc`, `mpsc`, `mpmc`, `oneshot` | The blocking forms, `send_blocking` and `recv_blocking`, which need `std` |
| The waiting reservations (see [Wait for free units](capacity.md#wait-for-free-units)) | `rsasync::acquire` | Nothing |
| The timer storage (see [Timers](timers.md)) | `rsasync::timer` | Nothing |
| The injected clock (see [Inject the time in a test](clocks.md#inject-the-time-in-a-test)) | `rsasync::clock::InjectedClock` | `NativeClock` and `PhysicalClock`, which need `std` |

The channels and the waiting reservations are not in a published release yet.

| Target | What runs | Evidence today |
| --- | --- | --- |
| `wasm32-unknown-unknown` | The library builds of `rstypes` and `rsasync` without `std` | The builds compile. No test runs on this target |
| `wasm32-wasip1` | The unit tests of `rstypes` and `rsasync`, with both crates built without `std`, in Wasmtime | The tests pass in Wasmtime 50.0.1 on an aarch64 macOS host |

Both targets are 32-bit and have one thread. The tests on `wasm32-wasip1` use the same code and
features as the `wasm32-unknown-unknown` build. The pinned toolchain in `rust-toolchain.toml`
includes the two targets.

### Limits

- On `wasm32-unknown-unknown`, `std` compiles, but its clocks, threads, and blocking waits fail
  when they run. The subset does not use `std`, so it cannot call them.
- A test that needs a second thread does not run on these targets. The native tests and the
  exploration tests (see [Explore concurrent code](concurrency-exploration.md)) check the
  concurrent behavior of the subset.
- Without `std`, the lock of rsasync is a spin lock. When something preempts a holder of the lock,
  the other users spin until the holder runs again. On a target with one thread, the lock never
  spins.
- The doctests do not run on WebAssembly.
- A test that panics stops its process. Nextest runs each test in a separate process, so it
  reports that test as failed, with the panic message.
- CI does not run these commands. A WebAssembly result does not depend on the host.

### Build and test the subset

You need Wasmtime on your `PATH`; `.cargo/config.toml` makes it the test runner for
`wasm32-wasip1`. Run these commands from the repository root:

```sh
cargo build --locked -p rstypes -p rsasync --lib --target wasm32-unknown-unknown --no-default-features
cargo xtask test -p rstypes -p rsasync --target wasm32-wasip1 --no-default-features
```

`--no-default-features` builds both crates without their `std` feature, and rsasync also without
its `local` feature, the scheduler. The test harness uses the `std` of `wasm32-wasip1`, but the
crates under test do not. The test command passes when Nextest reports that each test passed, for
example `97 tests run: 97 passed, 0 skipped`.
