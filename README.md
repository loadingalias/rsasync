# rsasync

Thread-per-core and work-stealing in one Rust runtime: one build, one lifecycle, one resource plan.

Today you choose Monoio or Glommio for pinned cores, Tokio for everything else, and run two
runtimes side by side. rsasync runs both modes under one shutdown and one memory budget.

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

**The bar.** Each release measures against Tokio, Monoio, Glommio, and Compio. A headline cell must
win by at least 10% in throughput per core or 25% in tail latency, and no other cell may lose. The
mixed-mode cell runs both modes in one build against Tokio alone and against Monoio or Compio
composed with Tokio. Every loss is published, and no "fastest" claim ships without this evidence.

Status: in development. Alpha releases are on crates.io; each one can change the API.

Part of one family on [rscrypto](https://github.com/loadingalias/rscrypto): rsasync (runtime),
rsnetwork (networking), and rsauthnz (auth). Apache-2.0. Develop with `cargo xtask fix`,
`cargo xtask test`, and `cargo xtask check`.
