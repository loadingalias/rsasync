# rsasync

Thread-per-core and work-stealing in one Rust runtime: one build, one lifecycle, one resource plan.

Today you choose Monoio or Glommio for pinned cores, Tokio for everything else, and run two
runtimes side by side. rsasync is built to run both modes under one shutdown and one memory
budget.

rsasync is built to give you:

- **Both modes in one process.** Keep hot state on its core with `Rc` and no locks. Move other work
  between workers. A typed handoff connects the two.
- **Sound completion I/O.** io_uring and IOCP take owned buffers. A dropped or forgotten future never
  frees memory that the kernel still uses.
- **Idle connections hold no buffer.** The kernel selects a receive buffer when data arrives.
- **Nothing is unbounded.** Every queue, task slot, timer, and buffer has a limit. A full spawn
  returns your future with a reason, not an out-of-memory crash.
- **Shutdown tells the truth.** It cancels, drains, joins every worker, and reports what the kernel
  still owns. A slow peer cannot hold it.
- **One ownership law on every backend.** io_uring, epoll, kqueue, and IOCP. The capability report
  says which backend ran. The driver also runs with no scheduler, inside your own event loop.
- **Pure Rust.** No C, no binding crates, no hidden Tokio.

**The bar.** A release that claims speed measures against Tokio, Monoio, Glommio, and Compio. A
headline cell must win by at least 10% in throughput per core or 25% in tail latency, and no other
cell may lose. The mixed-mode cell runs both modes in one build against Tokio alone and against
Monoio or Compio composed with Tokio. Every loss is published, and no "fastest" claim ships without
this evidence.

**In this alpha.** These parts have run evidence:

- The shared types: `rsasync::buffer`, `rsasync::capacity`, and `rsasync::time`. Their tests run on
  macOS and Linux, and on WebAssembly (`wasm32-wasip1`) without `std`, the portable subset.
- The driver-only io_uring surface, `rsasync::driver`, on Linux 6.1 (Debian 12) and Linux 7.0
  (Ubuntu 26.04), on x86_64 and aarch64, with glibc and musl: bounded turns, a wake descriptor for
  the event loop of a host, terminal teardown, and TCP accept, receive (into an owned buffer or into
  a buffer that the kernel selects), and send.

- The local workers of `rsasync::runtime` on Linux 6.1 (Debian 12, x86_64) and Linux 7.0 (Ubuntu
  26.04, aarch64), on io_uring and epoll: a start that reports each worker, `block_on` that releases
  each operation before it returns, the factory spawn on a worker, placement, and seeded generators.

`rsasync::task` runs local tasks on one thread, behind the `local` feature, which is on by default.
The work-stealing pool, the local workers on macOS and Windows, and the other platforms come later.
Each alpha can change the API.

Part of one family on [rscrypto](https://github.com/loadingalias/rscrypto): rsasync (runtime),
rsnetwork (networking), and rsauthnz (auth). Apache-2.0. Develop with `cargo xtask fix`,
`cargo xtask test`, and `cargo xtask check`.
