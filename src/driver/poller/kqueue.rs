//! The selector of the poller on macOS: one kqueue. Each resource has two edge-triggered
//! registrations (`EV_CLEAR`), one of `EVFILT_READ` and one of `EVFILT_WRITE`. A descriptor that is
//! no resource has a one-shot registration (`EV_ONESHOT`) of the filter of its direction. The read
//! end of the wake pipe has a level-triggered registration of `EVFILT_READ`: another thread, or a
//! signal handler, writes the pipe, and `write` is async-signal-safe, which `kevent` is not.
//!
//! A poll of the exit of a process ([`EXITED`]) is a one-shot registration of `EVFILT_PROC` with
//! `NOTE_EXIT`, whose identifier is the process ID. A process that ended, also one that is a zombie
//! now, refuses the registration with `ESRCH`: measured on macOS 27.2.1 (`xnu-13432.41.4`).
//!
//! A poll of a time ([`ELAPSED`]) is a one-shot timer (`EVFILT_TIMER` with `NOTE_USECONDS`), whose
//! identifier is the descriptor of its entry.
//!
//! The selector reports each event with the `poll` bit of its filter: `POLLIN` for `EVFILT_READ`,
//! `POLLOUT` for `EVFILT_WRITE`, [`EXITED`] for `EVFILT_PROC`, and [`ELAPSED`] for `EVFILT_TIMER`. The declarations agree with `sys/event.h` and
//! `sys/_types/_timespec.h` of the macOS 27.2 SDK (`MacOSX27.2.sdk`) on aarch64.

use alloc::rc::Rc;
use core::ffi::{c_int, c_long, c_void};
use core::mem::{offset_of, size_of};
use core::ptr;
use core::time::Duration;
use std::io;
use std::os::fd::{AsFd as _, AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd, RawFd};

use super::sys::{F_SETFD, FD_CLOEXEC, fcntl};
use super::{ELAPSED, EXITED, POLLIN, POLLOUT, WAKE};
use crate::driver::out_of_memory;

/// The kqueue of a poller. A clone refers to the same kqueue.
#[derive(Debug, Clone)]
pub(super) struct Selector(Rc<OwnedFd>);

impl Selector {
    /// A new kqueue that closes at an exec, with the read end `wake` of the wake pipe registered,
    /// so that a wake from another thread or from a signal handler ends a wait.
    ///
    /// # Errors
    ///
    /// The error of `kqueue`, of `fcntl`, or of the registration of `wake`.
    #[expect(unsafe_code, reason = "the C library returns a new descriptor")]
    pub(super) fn new(wake: BorrowedFd<'_>) -> io::Result<Self> {
        // SAFETY: the call takes no pointer.
        let fd = unsafe { kqueue() };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the C library returned a new descriptor that nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: the command takes one `int` argument.
        if unsafe { fcntl(fd.as_raw_fd(), F_SETFD, FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let selector = Self(Rc::try_new(fd).map_err(out_of_memory)?);
        change(&selector.0, registration(wake.as_raw_fd(), EVFILT_READ, EV_ADD, WAKE))?;
        Ok(selector)
    }

    /// The kqueue: it is readable when an event waits.
    pub(super) fn fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }

    /// Registers the resource `fd` with `data`, edge-triggered for reads and writes.
    ///
    /// # Errors
    ///
    /// The error of a registration. Nothing changed then.
    pub(super) fn add(&self, fd: RawFd, data: u64) -> io::Result<()> {
        let flags = EV_ADD | EV_CLEAR;
        let changes = [EVFILT_READ, EVFILT_WRITE].map(|filter| registration(fd, filter, flags, data));
        let added = changes_all(&self.0, &changes);
        if added.is_err() {
            self.delete(fd);
        }
        added
    }

    /// Ends the registrations of `fd`. The kqueue ends them at the close of `fd` too, but another
    /// descriptor can keep the file open while the poller gives `fd` to its caller. A descriptor
    /// with no registration, such as a file, gives `ENOENT`, and nothing changes.
    pub(super) fn delete(&self, fd: RawFd) {
        let changes = [EVFILT_READ, EVFILT_WRITE].map(|filter| registration(fd, filter, EV_DELETE, 0));
        drop(changes_all(&self.0, &changes));
    }

    /// Waits one time for the `poll` bits `events` of `fd`, with `data`: for the exit of the
    /// process `fd` with [`EXITED`], else for `POLLIN` when it has it, else for `POLLOUT`. A
    /// one-shot registration ends at its event.
    pub(super) fn arm(&self, fd: RawFd, events: u32, data: u64) -> io::Result<()> {
        change(&self.0, registration(fd, filter(events), EV_ADD | EV_ONESHOT, data))
    }

    /// Waits one time for `micros` microseconds, in a timer whose identifier is `ident`, with
    /// `data`.
    pub(super) fn delay(&self, ident: RawFd, micros: u64, data: u64) -> io::Result<()> {
        let timer = Kevent {
            data: isize::try_from(micros).unwrap_or(isize::MAX),
            ..registration(ident, EVFILT_TIMER, EV_ADD | EV_ONESHOT, data)
        };
        change(&self.0, timer)
    }

    /// Ends the one-shot registration of `fd` for `events`.
    pub(super) fn disarm(&self, fd: RawFd, events: u32) {
        drop(change(&self.0, registration(fd, filter(events), EV_DELETE, 0)));
    }

    /// Waits at most for `timeout`, or with no limit, for events, and writes them into `events`.
    /// Gives their number.
    ///
    /// # Errors
    ///
    /// An error of `kevent`, such as `Interrupted`.
    #[expect(unsafe_code, reason = "the kernel writes the event array")]
    pub(super) fn wait(&self, events: &mut [Event], timeout: Option<Duration>) -> io::Result<usize> {
        let timeout = timeout.map(|timeout| Timespec {
            // A wait of more than 136 years has no end in practice.
            tv_sec: c_long::try_from(timeout.as_secs())
                .unwrap_or(c_long::MAX)
                .min(c_long::from(u32::MAX)),
            tv_nsec: c_long::from(timeout.subsec_nanos()),
        });
        let timeout = timeout.as_ref().map_or(ptr::null(), ptr::from_ref);
        let capacity = c_int::try_from(events.len()).unwrap_or(c_int::MAX);
        // SAFETY: the array has `capacity` events, and the kernel writes at most that many. The
        // call reads no change, and `timeout` is null or valid during the call.
        let count = unsafe {
            kevent(
                self.0.as_raw_fd(),
                ptr::null(),
                0,
                events.as_mut_ptr(),
                capacity,
                timeout,
            )
        };
        usize::try_from(count).map_err(|_| io::Error::last_os_error())
    }
}

/// The data of the registration of `event`, and its events as `poll` bits.
pub(super) fn decode(event: Event) -> (u64, u32) {
    let events = match event.filter {
        EVFILT_READ => POLLIN,
        EVFILT_WRITE => POLLOUT,
        EVFILT_PROC => EXITED,
        EVFILT_TIMER => ELAPSED,
        _ => 0,
    };
    (event.udata.addr() as u64, events)
}

/// The filter of the `poll` bits `events`.
const fn filter(events: u32) -> i16 {
    if events & EXITED != 0 {
        EVFILT_PROC
    } else if events & ELAPSED != 0 {
        EVFILT_TIMER
    } else if events & POLLIN != 0 {
        EVFILT_READ
    } else {
        EVFILT_WRITE
    }
}

/// A change of the registration of `filter` of `fd`, which is a process ID for `EVFILT_PROC` and
/// the identifier of a timer for `EVFILT_TIMER`.
fn registration(fd: RawFd, filter: i16, flags: u16, data: u64) -> Kevent {
    Kevent {
        ident: usize::try_from(fd).unwrap_or(usize::MAX),
        filter,
        flags,
        fflags: match filter {
            EVFILT_PROC => NOTE_EXIT,
            EVFILT_TIMER => NOTE_USECONDS,
            _ => 0,
        },
        data: 0,
        udata: udata(data),
    }
}

/// The data of a registration as its `udata`, which the kernel gives back and never reads: a
/// pointer with no provenance. A pointer of aarch64 holds each `u64`.
fn udata(data: u64) -> *mut c_void {
    ptr::without_provenance_mut(usize::try_from(data).unwrap_or(usize::MAX))
}

/// Applies the one change `change` of `kqueue`.
fn change(kqueue: &OwnedFd, change: Kevent) -> io::Result<()> {
    changes_all(kqueue, &[change])
}

/// Applies each change of `changes`, at most two, to `kqueue`. Each change gets `EV_RECEIPT`: the
/// kernel applies each one and writes its result as a receipt in place of an event. So the call
/// takes no event, which is for the wait of the poller, and it does not wait.
///
/// # Errors
///
/// The error of the call, or of the first change that failed.
#[expect(unsafe_code, reason = "the kernel reads the changes and writes their receipts")]
fn changes_all(kqueue: &OwnedFd, given: &[Kevent]) -> io::Result<()> {
    let (mut changes, mut receipts) = ([Kevent::default(); 2], [Kevent::default(); 2]);
    let count = given.len().min(changes.len());
    for (change, given) in changes.iter_mut().zip(given) {
        *change = Kevent {
            flags: given.flags | EV_RECEIPT,
            ..*given
        };
    }
    let len = c_int::try_from(count).unwrap_or(0);
    let now = Timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `changes` has `len` changes, and `receipts` has room for `len` receipts, which the
    // kernel writes. `now` is valid during the call, and the call does not wait.
    let result = unsafe {
        kevent(
            kqueue.as_raw_fd(),
            changes.as_ptr(),
            len,
            receipts.as_mut_ptr(),
            len,
            &raw const now,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    let reported = usize::try_from(result).unwrap_or(0).min(count);
    match receipts[..reported]
        .iter()
        .find(|receipt| receipt.flags & EV_ERROR != 0 && receipt.data != 0)
    {
        Some(receipt) => Err(io::Error::from_raw_os_error(
            c_int::try_from(receipt.data).unwrap_or(c_int::MAX),
        )),
        None => Ok(()),
    }
}

#[expect(unsafe_code, reason = "libSystem of macOS gives these functions")]
unsafe extern "C" {
    /// `int kqueue(void)`.
    fn kqueue() -> c_int;
    /// `int kevent(int kq, const struct kevent *changelist, int nchanges, struct kevent
    /// *eventlist, int nevents, const struct timespec *timeout)`.
    fn kevent(
        kq: c_int,
        changelist: *const Kevent,
        nchanges: c_int,
        eventlist: *mut Kevent,
        nevents: c_int,
        timeout: *const Timespec,
    ) -> c_int;
}

/// `struct kevent`: a change of a registration, or an event of one.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) struct Kevent {
    /// The identifier of the event: a descriptor, or the identifier of a user event.
    ident: usize,
    filter: i16,
    flags: u16,
    fflags: u32,
    /// The data of the filter, or the error of a change in a receipt.
    data: isize,
    /// The data of the registration, which the kernel does not read.
    udata: *mut c_void,
}

impl Default for Kevent {
    fn default() -> Self {
        Self {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: ptr::null_mut(),
        }
    }
}

/// An event of the kqueue.
pub(super) type Event = Kevent;

/// `struct timespec`, with a `long` for each field on aarch64.
#[repr(C)]
struct Timespec {
    tv_sec: c_long,
    tv_nsec: c_long,
}

const _: () = {
    assert!(size_of::<Kevent>() == 32);
    assert!(offset_of!(Kevent, filter) == 8);
    assert!(offset_of!(Kevent, flags) == 10);
    assert!(offset_of!(Kevent, fflags) == 12);
    assert!(offset_of!(Kevent, data) == 16);
    assert!(offset_of!(Kevent, udata) == 24);
    assert!(size_of::<Timespec>() == 16);
};

const EVFILT_READ: i16 = -1;
const EVFILT_WRITE: i16 = -2;
const EVFILT_PROC: i16 = -5;
const EVFILT_TIMER: i16 = -7;
const EV_ADD: u16 = 0x0001;
const EV_DELETE: u16 = 0x0002;
const EV_ONESHOT: u16 = 0x0010;
const EV_CLEAR: u16 = 0x0020;
const EV_RECEIPT: u16 = 0x0040;
const EV_ERROR: u16 = 0x4000;
/// The `fflags` of `EVFILT_PROC` for the exit of the process, and of `EVFILT_TIMER` for a time in
/// microseconds.
const NOTE_EXIT: u32 = 0x8000_0000;
const NOTE_USECONDS: u32 = 0x2;
