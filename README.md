# rsasync

**DO NOT USE IN PRODUCTION.**

rsasync is an async runtime for Rust. Its goal is one runtime for thread-per-core tasks and
work-stealing tasks, with one shutdown and one memory budget. Its I/O uses owned buffers on
io_uring, epoll, kqueue, and IOCP. Each queue, task slot, timer, and buffer has a limit. It is pure
Rust, with no C.

rsasync is in development. Each release can change the API.

rsasync is one part of a family of Rust libraries:
[rscrypto](https://github.com/loadingalias/rscrypto) (cryptography),
rsasync (runtime),
[rsnetwork](https://github.com/loadingalias/rsnetwork) (networking),
[rsauthnz](https://github.com/loadingalias/rsauthnz) (auth), and
[rscodec](https://github.com/loadingalias/rscodec) (codecs).

License: Apache-2.0.
