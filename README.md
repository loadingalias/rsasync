# rsasync

A dual async runtime for Rust: thread-per-core local tasks and work-stealing movable tasks under one
lifecycle, with completion I/O on owned buffers (io_uring, epoll, kqueue, IOCP), bounded channels,
capacity permits, and time.

Status: in development. There is no public API yet.

Develop with `cargo xtask fix`, `cargo xtask test`, and `cargo xtask check`.

License: Apache-2.0.
