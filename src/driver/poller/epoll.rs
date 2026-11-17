//! The selector of the poller on Linux: one epoll descriptor. Each resource has one
//! edge-triggered registration for reads and writes, a descriptor that is no resource has a
//! one-shot registration, and the wake descriptor has a level-triggered one.
//!
//! The declarations agree with `sys/epoll.h` of glibc and musl and with
//! `include/uapi/linux/eventpoll.h` of Linux 6.1 and 7.0. The structure `epoll_event` is packed on
//! x86_64 (`EPOLL_PACKED`) and has its natural layout on aarch64. The event bits of epoll are the
//! `poll` bits that the poller reads.

use alloc::rc::Rc;
use core::ffi::c_int;
use core::mem::{offset_of, size_of};
use core::time::Duration;
use std::io;
use std::os::fd::{AsFd as _, AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd, RawFd};

use super::{POLLIN, POLLOUT, POLLRDHUP, WAKE};

/// The events of a resource: each edge of its reads and its writes, and the end of the read side
/// of its peer.
const RESOURCE_EVENTS: u32 = POLLIN | POLLOUT | POLLRDHUP | EPOLLET;

/// The epoll descriptor of a poller. A clone refers to the same descriptor.
#[derive(Debug, Clone)]
pub(super) struct Selector(Rc<OwnedFd>);

impl Selector {
    /// A new epoll descriptor, with `wake` registered, so that a wake from another thread ends a
    /// wait.
    ///
    /// # Errors
    ///
    /// The error of `epoll_create1` or of the registration of `wake`.
    #[expect(unsafe_code, reason = "the C library returns a new descriptor")]
    pub(super) fn new(wake: BorrowedFd<'_>) -> io::Result<Self> {
        // SAFETY: the call takes no pointer.
        let fd = unsafe { epoll_create1(EPOLL_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the C library returned a new descriptor that nothing else owns.
        let selector = Self(Rc::new(unsafe { OwnedFd::from_raw_fd(fd) }));
        selector.control(EPOLL_CTL_ADD, wake.as_raw_fd(), POLLIN, WAKE)?;
        Ok(selector)
    }

    /// The epoll descriptor: it is readable when an event waits.
    pub(super) fn fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }

    /// Registers the resource `fd` with `data`, edge-triggered for reads and writes.
    pub(super) fn add(&self, fd: RawFd, data: u64) -> io::Result<()> {
        self.control(EPOLL_CTL_ADD, fd, RESOURCE_EVENTS, data)
    }

    /// Ends the registration of `fd`. The poller then reports no event of it, also when another
    /// descriptor still refers to its file. A descriptor with no registration, such as a file,
    /// gives `ENOENT`, and nothing changes.
    pub(super) fn delete(&self, fd: RawFd) {
        drop(self.control(EPOLL_CTL_DEL, fd, 0, 0));
    }

    /// Waits one time for the `poll` bits `events` of `fd`, with `data`.
    pub(super) fn arm(&self, fd: RawFd, events: u32, data: u64) -> io::Result<()> {
        let events = events | EPOLLONESHOT;
        // A descriptor keeps its one-shot registration after its event, disabled, so the next
        // wait changes it.
        self.control(EPOLL_CTL_MOD, fd, events, data)
            .or_else(|_| self.control(EPOLL_CTL_ADD, fd, events, data))
    }

    /// Ends the one-shot registration of `fd`.
    pub(super) fn disarm(&self, fd: RawFd, _events: u32) {
        self.delete(fd);
    }

    /// Waits at most for `timeout`, or with no limit, for events, and writes them into `events`.
    /// Gives their number.
    ///
    /// # Errors
    ///
    /// An error of `epoll_wait`, such as `Interrupted`.
    #[expect(unsafe_code, reason = "the kernel writes the event array")]
    pub(super) fn wait(&self, events: &mut [Event], timeout: Option<Duration>) -> io::Result<usize> {
        // Rounded up, so the wait does not end before its time.
        let timeout = timeout.map_or(-1, |timeout| {
            c_int::try_from(timeout.as_nanos().div_ceil(1_000_000)).unwrap_or(c_int::MAX)
        });
        let capacity = c_int::try_from(events.len()).unwrap_or(c_int::MAX);
        // SAFETY: the array has `capacity` events, and the kernel writes at most that many.
        let count = unsafe { epoll_wait(self.0.as_raw_fd(), events.as_mut_ptr(), capacity, timeout) };
        usize::try_from(count).map_err(|_| io::Error::last_os_error())
    }

    /// One `epoll_ctl`.
    #[expect(unsafe_code, reason = "the call reads one event that lives during the call")]
    fn control(&self, op: c_int, fd: RawFd, events: u32, data: u64) -> io::Result<()> {
        let mut event = Event { events, data };
        // SAFETY: `event` is valid for reads during the call.
        if unsafe { epoll_ctl(self.0.as_raw_fd(), op, fd, &raw mut event) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// The data of the registration of `event`, and its events as `poll` bits.
pub(super) const fn decode(event: Event) -> (u64, u32) {
    (event.data, event.events)
}

#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    /// `int epoll_create1(int flags)`.
    fn epoll_create1(flags: c_int) -> c_int;
    /// `int epoll_ctl(int epfd, int op, int fd, struct epoll_event *event)`.
    fn epoll_ctl(epfd: c_int, op: c_int, fd: c_int, event: *mut Event) -> c_int;
    /// `int epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout)`.
    fn epoll_wait(epfd: c_int, events: *mut Event, maxevents: c_int, timeout: c_int) -> c_int;
}

/// `struct epoll_event`: the events of a descriptor, and the data of its registration.
#[cfg_attr(target_arch = "x86_64", repr(C, packed))]
#[cfg_attr(not(target_arch = "x86_64"), repr(C))]
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Event {
    events: u32,
    data: u64,
}

#[cfg(target_arch = "x86_64")]
const _: () = {
    assert!(size_of::<Event>() == 12);
    assert!(offset_of!(Event, data) == 4);
};
#[cfg(target_arch = "aarch64")]
const _: () = {
    assert!(size_of::<Event>() == 16);
    assert!(offset_of!(Event, data) == 8);
};

/// `EPOLL_CLOEXEC` is `O_CLOEXEC`.
const EPOLL_CLOEXEC: c_int = 0o2_000_000;
const EPOLL_CTL_ADD: c_int = 1;
const EPOLL_CTL_DEL: c_int = 2;
const EPOLL_CTL_MOD: c_int = 3;
const EPOLLONESHOT: u32 = 1 << 30;
const EPOLLET: u32 = 1 << 31;
