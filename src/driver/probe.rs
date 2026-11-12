//! The probe of the wake descriptor: does the registered `eventfd` signal each deferred work of
//! the kernel, or only the first one?
//!
//! The source argument, from `io_uring/tw.c`, `io_uring/poll.c`, and `io_uring/eventfd.c` of
//! Linux:
//!
//! - A ring with `IORING_SETUP_DEFER_TASKRUN` queues the completion of a poll wakeup as task work
//!   (`io_req_local_work_add`). The add that finds the work list empty sets `IORING_SQ_TASKRUN`
//!   first, and then signals the registered `eventfd` (`io_eventfd_signal`).
//! - `cd305ee3633a` ("io_uring: defer eventfd signaling when queued from a wakeup") sends that
//!   signal from a waitqueue wakeup through `call_rcu_hurry`: `__io_eventfd_signal` sets
//!   `IO_EVENTFD_OP_SIGNAL_BIT` and queues `io_eventfd_do_signal`, unless the bit is set already.
//! - Before `04fe9aeb4f3c` ("io_uring/eventfd: reset deferred signal state"),
//!   `io_eventfd_do_signal` does not clear the bit. After the first deferred signal of a
//!   registration, the kernel thus skips each later one. Ubuntu 26.04 with `7.0.0-1016-aws` has
//!   the first change and not the second.
//! - `io_eventfd_unregister` drops the state of the registration, and `io_eventfd_register`
//!   makes a new one with the bit clear. A new registration thus signals its first deferred work.
//!
//! The probe makes two deferred signals on one registration: each one is a poll of a pipe, which
//! a write to the pipe wakes. The first signal comes on each kernel that signals deferred work: at
//! once without `cd305ee3633a`, and after an RCU grace period with it. The second one comes in the
//! same way on a kernel with `04fe9aeb4f3c`, and never on a kernel with the defect. The probe waits
//! for it for 16 times the delay of the first signal, at least 10 ms and at most 1 s. A slower
//! grace period can only make a kernel with no defect do the registrations, not the reverse.

use core::time::Duration;
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::os::fd::AsRawFd as _;
use std::time::Instant;

use super::{push_now, sys};
use crate::uring::{Ring, Sqe};

/// The time for the first deferred signal, which each kernel that signals deferred work gives.
const FIRST: Duration = Duration::from_secs(1);
/// The bounds of the time for the second deferred signal.
const LEAST: Duration = Duration::from_millis(10);
const MOST: Duration = Duration::from_secs(1);

/// Gives `true` when the kernel signals the registered `wake` for deferred work only one time in
/// the life of a registration. Call it on a new ring with no entry and no completion: it takes each
/// completion that it makes.
#[expect(unsafe_code, reason = "a poll names no memory")]
pub(super) fn signal_stops_after_first(ring: &mut Ring, wake: &File) -> io::Result<bool> {
    let (mut reader, mut writer) = io::pipe()?;
    let mut limit = FIRST;
    for round in 0..2 {
        let poll = Sqe::poll_readable(reader.as_raw_fd());
        // SAFETY: a poll names no memory.
        unsafe { push_now(ring, &poll) }?;
        // The poll waits in the kernel before the write wakes it.
        ring.submit(0)?;
        let start = Instant::now();
        writer.write_all(&[0])?;
        let signaled = readable(wake, limit)?;
        let delay = start.elapsed();
        // Run the deferred work, which posts the completion of the poll, and take it.
        while !ring.ready() {
            match ring.submit(1) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => drop(result?),
            }
        }
        ring.completions().for_each(drop);
        reader.read_exact(&mut [0])?;
        // The completion signals the descriptor too. A clear descriptor has no count.
        drop((&*wake).read(&mut [0; 8]));
        if !signaled {
            // With no signal for the first deferred work, the kernel does not have this defect.
            return Ok(round == 1);
        }
        limit = delay.saturating_mul(16).clamp(LEAST, MOST);
    }
    Ok(false)
}

/// Waits until `fd` is readable, for at most `limit`.
#[expect(unsafe_code, reason = "poll reads and writes one pollfd")]
fn readable(fd: &File, limit: Duration) -> io::Result<bool> {
    let mut pollfd = sys::PollFd {
        fd: fd.as_raw_fd(),
        events: sys::POLLIN,
        revents: 0,
    };
    // The limit is at most one second, so its milliseconds fit an `int`.
    let timeout = i32::try_from(limit.as_millis()).unwrap_or(i32::MAX);
    loop {
        // SAFETY: `pollfd` is one valid `pollfd` during the call.
        match unsafe { sys::poll(&raw mut pollfd, 1, timeout) } {
            ready if ready >= 0 => return Ok(ready > 0),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}
