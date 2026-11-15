//! The probe of the wake descriptor: does the registered `eventfd` signal each deferred work of
//! the kernel, or only the first one?
//!
//! The source argument, from `io_uring/tw.c`, `io_uring/poll.c`, and `io_uring/eventfd.c` of
//! Linux:
//!
//! - A ring with `IORING_SETUP_DEFER_TASKRUN` queues the completion of a poll wakeup as task work
//!   (`io_req_local_work_add`). The add that finds the work list empty sets `IORING_SQ_TASKRUN`
//!   first, and then signals the registered `eventfd` (`io_eventfd_signal`). Each kernel with the
//!   flag does this, because `21a091b970cd` added the signal in Linux 6.1, with the flag.
//! - Without `cd305ee3633a`, the signal comes in the `write` that wakes the poll, as on 6.1.
//! - `cd305ee3633a` ("io_uring: defer eventfd signaling when queued from a wakeup") sends that
//!   signal from a waitqueue wakeup through `call_rcu_hurry`: `__io_eventfd_signal` sets
//!   `IO_EVENTFD_OP_SIGNAL_BIT` and queues `io_eventfd_do_signal`, unless the bit is set already.
//!   The signal thus comes after an RCU grace period, which has no fixed bound.
//! - Before `04fe9aeb4f3c` ("io_uring/eventfd: reset deferred signal state"),
//!   `io_eventfd_do_signal` does not clear the bit. After the first deferred signal of a
//!   registration, the kernel thus skips each later one. Ubuntu 26.04 with `7.0.0-1016-aws` has
//!   the first change and not the second.
//! - `io_eventfd_unregister` drops the state of the registration, and `io_eventfd_register`
//!   makes a new one with the bit clear. A new registration thus signals its first deferred work.
//!
//! The probe makes two deferred signals on one registration: each one is a poll of a pipe, which
//! a write to the pipe wakes. The probe waits 1 s for the first signal. It waits for the second
//! one for 16 times the delay of the first, at least 10 ms and at most 1 s. A kernel with the
//! defect never gives the second one. The probe finds no defect only when both signals come in
//! time. A late signal in either round, as after a slow grace period, thus can only make a kernel
//! with no defect do the registrations, not the reverse.
//!
//! A kernel with no defect thus makes no extra system call in a turn only while its grace periods
//! stay in these bounds. A slower kernel does registrations that it does not need, but it misses
//! no wake. A signal that comes after the probe wakes the descriptor one time more, which a turn
//! clears.
//!
//! The defect is a fact of the kernel, not of a ring, so only the first driver of a process
//! probes. Each later driver reads that result.

use core::time::Duration;
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::os::fd::AsRawFd as _;
// A physical measurement of the kernel at startup. A deterministic run does not replace it.
use std::time::Instant;

use super::{push_now, sys};
use crate::uring::{Ring, Sqe};

/// The time for the first deferred signal. Each kernel gives it, but with `cd305ee3633a` only after
/// a grace period, which has no fixed bound.
const FIRST: Duration = Duration::from_secs(1);
/// The bounds of the time for the second deferred signal.
const LEAST: Duration = Duration::from_millis(10);
const MOST: Duration = Duration::from_secs(1);

/// The result of the first probe of the process. `crate::sync` has no cell that is written one
/// time, and loom explores no part of the probe, so this is the cell of `std`.
static DEFECT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Gives `true` when the driver must register `wake` again in each turn: the kernel signals the
/// registered `wake` for deferred work only one time in the life of a registration, or a signal
/// of the probe came late. The first call of the process probes `ring`, and then registers
/// `wake` again, because the probe used the registration. Call it on a new ring with no entry and
/// no completion: a probe takes each completion that it makes.
pub(super) fn signal_stops_after_first(ring: &mut Ring, wake: &File) -> io::Result<bool> {
    if let Some(defect) = DEFECT.get() {
        return Ok(*defect);
    }
    let defect = probe(ring, wake, FIRST)?;
    super::rearm(ring, wake)?;
    // Two first drivers on two threads can both probe; each gets one result.
    Ok(*DEFECT.get_or_init(|| defect))
}

/// Makes two deferred signals on the registration of `wake`, and gives `false` only when both
/// come in time. It waits `first` for the first one.
#[expect(unsafe_code, reason = "a poll names no memory")]
pub(super) fn probe(ring: &mut Ring, wake: &File, first: Duration) -> io::Result<bool> {
    let (mut reader, mut writer) = io::pipe()?;
    let mut limit = first;
    for _ in 0..2 {
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
            // A late or missing signal gives the defect. With no defect, it only adds registrations.
            return Ok(true);
        }
        limit = delay.saturating_mul(16).clamp(LEAST, MOST);
    }
    Ok(false)
}

/// Waits until `fd` is readable, for at most `limit` in total.
#[expect(unsafe_code, reason = "poll reads and writes one pollfd")]
fn readable(fd: &File, limit: Duration) -> io::Result<bool> {
    let start = Instant::now();
    let mut pollfd = sys::PollFd {
        fd: fd.as_raw_fd(),
        events: sys::POLLIN,
        revents: 0,
    };
    loop {
        // After an interrupt, the wait continues only for the rest of the limit. A full limit for
        // each wait never ends while a periodic signal, for example of a profiler, comes sooner.
        let rest = limit.saturating_sub(start.elapsed());
        // The limit is at most one second, so its milliseconds fit an `int`.
        let timeout = i32::try_from(rest.as_millis()).unwrap_or(i32::MAX);
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
