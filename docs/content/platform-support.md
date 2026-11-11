# Platform support

rsasync is in development and has no release. This page gives the Linux C libraries that rsasync
supports, and the evidence that its tests give today.

## Linux C libraries

Linux support includes two C libraries, glibc and musl, on x86_64 and aarch64:

| C library | Rust targets |
| --- | --- |
| glibc | `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` |
| musl | `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl` |

The pinned toolchain in `rust-toolchain.toml` includes the two musl targets.

## Evidence today

On each push to `main`, CI builds, links, and runs the tests on x86_64 Linux two times, with
these commands:

| C library | Command |
| --- | --- |
| glibc | `cargo xtask test` |
| musl | `cargo xtask test --target x86_64-unknown-linux-musl` |

Each command runs the tests of the whole workspace. Only the glibc run includes the doctests.

Today, all of the tests are tests of the repository tools, and the library makes no system call of
its own. Thus a pass shows that the build links and runs on musl. It does not show that a runtime
operation works on musl. From the Linux driver stage on, each stage exit also runs the tests on
x86_64 and aarch64 Linux hosts with both C libraries. This page then names each operation that has
that evidence.
