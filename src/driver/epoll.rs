//! The epoll backend: a poller that takes the submission entries of the driver, as a ring does,
//! and runs each one as one nonblocking call when its descriptor is ready. It posts the
//! completion that a ring posts for the same entry, so the operation table, the stream rules,
//! close, and teardown run in the same way on both backends.
//!
//! The driver registers each resource one time, at its adoption, edge-triggered for reads and
//! writes, with the index and the generation of its slot as the data ([`Registrar`]). An entry
//! runs at once when its direction is ready. When its call gives `EAGAIN`, the direction is not
//! ready, and the entry waits in the list of that direction until the next edge. A poll of a
//! descriptor that is not a resource, such as a process descriptor, has a one-shot registration.
//! A linked chain of datagram sends or receives, the batch of the driver, is one `sendmmsg` or
//! `recvmmsg`, through `rsnetwork-udp`.
//!
//! The terminal event of each readiness operation is the return of its nonblocking call. After
//! that return, the kernel keeps no reference to the buffer. Files have no readiness, so their
//! entries run as jobs of the blocking service ([`run_file`]), and each job posts its completion
//! here when it returns.

mod sys;

use alloc::collections::VecDeque;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::ffi::c_int;
use core::ptr::NonNull;
use core::task::{Context, Waker};
use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd, RawFd};
use std::time::Instant;

use rsnetwork_udp::sys::{IoVec, MmsgHdr, MsgHdr};

use crate::blocking::Waiter;
use crate::uring::sys as ring;
use crate::uring::{Cqe, Group, Sqe};

/// The data of the registration of the wake descriptor.
const WAKE: u64 = u64::MAX;

/// The bit of the data of a one-shot registration, with the index of its slot in the low bits.
const ONE_SHOT: u64 = 1 << 63;

/// The events of a resource: each edge of its reads and its writes.
const RESOURCE_EVENTS: u32 = sys::EPOLLIN | sys::EPOLLOUT | sys::EPOLLRDHUP | sys::EPOLLET;

/// The events that make the read direction ready.
const READABLE: u32 = sys::EPOLLIN | sys::EPOLLPRI | sys::EPOLLRDHUP | sys::EPOLLHUP | sys::EPOLLERR;

/// The events that make the write direction ready.
const WRITABLE: u32 = sys::EPOLLOUT | sys::EPOLLHUP | sys::EPOLLERR;

/// The events that one wait takes at most.
const EVENTS: usize = 128;

/// The read direction and the write direction of a resource.
const READ: usize = 0;
const WRITE: usize = 1;

/// The identity of a resource slot of the driver: its index and its generation, which is the data
/// of its registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Token {
    pub(crate) index: u32,
    pub(crate) generation: u32,
}

impl Token {
    /// The data of the registration. The high bit is clear, so it never is a one-shot or the wake.
    const fn data(self) -> u64 {
        (((self.generation & 0x7FFF_FFFF) as u64) << 32) | self.index as u64
    }

    fn of(data: u64) -> Self {
        Self {
            index: u32::try_from(data & 0xFFFF_FFFF).unwrap_or(u32::MAX),
            generation: u32::try_from(data >> 32).unwrap_or(u32::MAX),
        }
    }
}

/// The registrations of the resources of a driver in its poller.
#[derive(Debug)]
pub(crate) struct Registrar {
    epoll: Rc<OwnedFd>,
    /// The test seam of a failed registration: the next registration fails with this error.
    #[cfg(test)]
    pub(crate) fail: core::cell::Cell<Option<i32>>,
}

impl Registrar {
    /// Registers the resource `fd` of `token`, edge-triggered for reads and writes. A pipe end gets
    /// `O_NONBLOCK` first, because a read or a write that waits would stop the driver.
    ///
    /// # Errors
    ///
    /// The error of `fcntl` or of `epoll_ctl`. Nothing changed then, except `O_NONBLOCK`.
    pub(crate) fn add(&self, fd: BorrowedFd<'_>, token: Token, pipe: bool) -> io::Result<()> {
        if pipe && !super::pipe::set_nonblocking(fd, true) {
            return Err(io::Error::last_os_error());
        }
        // The test seam gives the error of `epoll_ctl` in place of the call.
        #[cfg(test)]
        if let Some(errno) = self.fail.take() {
            return Err(io::Error::from_raw_os_error(errno));
        }
        control(
            &self.epoll,
            sys::EPOLL_CTL_ADD,
            fd.as_raw_fd(),
            RESOURCE_EVENTS,
            token.data(),
        )
    }

    /// Ends the registration of `fd`, before its close. The poller then reports no event of it,
    /// also when another descriptor still refers to its file.
    pub(crate) fn delete(&self, fd: BorrowedFd<'_>) {
        // A descriptor with no registration, such as a file, gives `ENOENT`, and nothing changes.
        drop(control(&self.epoll, sys::EPOLL_CTL_DEL, fd.as_raw_fd(), 0, 0));
    }
}

/// One `epoll_ctl`.
#[expect(unsafe_code, reason = "the call reads one event that lives during the call")]
fn control(epoll: &OwnedFd, op: c_int, fd: RawFd, events: u32, data: u64) -> io::Result<()> {
    let mut event = sys::EpollEvent { events, data };
    // SAFETY: `event` is valid for reads during the call.
    if unsafe { sys::epoll_ctl(epoll.as_raw_fd(), op, fd, &raw mut event) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The submission queue is full.
#[derive(Debug)]
pub(crate) struct Full;

/// An entry that the poller took and that has not completed.
#[derive(Debug, Clone, Copy)]
struct Slot {
    sqe: Sqe,
    /// The resource of the entry.
    token: Option<Token>,
    /// The next entry of its linked chain.
    link: Option<u32>,
    /// The next entry that waits in the same direction.
    next: Option<u32>,
    /// The kernel took the connect of the entry, and its result comes with the next write edge.
    connecting: bool,
}

/// The readiness of the directions of one resource slot.
#[derive(Debug, Default, Clone, Copy)]
struct Readiness {
    generation: u32,
    /// The direction gave no `EAGAIN` since its last edge.
    ready: [bool; 2],
    /// The first and the last entry that wait in each direction.
    head: [Option<u32>; 2],
    tail: [Option<u32>; 2],
}

/// What one call of an entry did.
enum Step {
    /// The entry ended with this result and these completion flags.
    Done(i32, u32),
    /// The direction is not ready. The entry of the index waits for the next edge: the same
    /// entry, or the rest of its chain.
    Wait(u32),
    /// Each entry of the chain posted its completion.
    Posted,
}

/// A poller of one driver.
pub(crate) struct Poller {
    epoll: Rc<OwnedFd>,
    /// The entries that the driver pushed, with their resources, which the next submit takes.
    queue: VecDeque<(Sqe, Option<Token>)>,
    entries: u32,
    slots: Vec<Option<Slot>>,
    free: Vec<u32>,
    completions: VecDeque<Cqe>,
    readiness: Vec<Readiness>,
    group: Option<Group>,
    /// The headers of one batch, for `sendmmsg` and `recvmmsg`.
    batch: Vec<MmsgHdr>,
    /// The entries of that batch.
    chain: Vec<u32>,
    /// The jobs of the blocking service that run file entries, and the waker of the driver.
    jobs: Vec<(u64, Waiter<i32>)>,
    waker: Option<Waker>,
    events: Vec<sys::EpollEvent>,
    /// The last wait filled the event array, so events can remain.
    full: bool,
}

impl Poller {
    /// A poller with `entries` submission entries, rounded up to a power of two as a ring does, room
    /// for `completions` entries in flight, `resources` resource slots, and `jobs` file jobs. The
    /// wake descriptor `wake` stays registered, so a wake from another thread ends a wait.
    ///
    /// # Errors
    ///
    /// The error of `epoll_create1` or of the registration of `wake`, or `InvalidInput` for 0
    /// entries.
    #[expect(unsafe_code, reason = "the C library returns a new descriptor")]
    pub(crate) fn new(
        entries: u32,
        completions: u32,
        resources: u32,
        jobs: u32,
        wake: BorrowedFd<'_>,
    ) -> io::Result<Self> {
        let entries = entries
            .checked_next_power_of_two()
            .filter(|&entries| entries > 0)
            .ok_or(io::ErrorKind::InvalidInput)?;
        // SAFETY: the call takes no pointer.
        let fd = unsafe { sys::epoll_create1(sys::EPOLL_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the C library returned a new descriptor that nothing else owns.
        let epoll = Rc::new(unsafe { OwnedFd::from_raw_fd(fd) });
        control(&epoll, sys::EPOLL_CTL_ADD, wake.as_raw_fd(), sys::EPOLLIN, WAKE)?;
        let completions = completions.max(entries);
        Ok(Self {
            epoll,
            queue: VecDeque::with_capacity(entries as usize),
            entries,
            slots: alloc::vec![None; completions as usize],
            free: (0..completions).rev().collect(),
            completions: VecDeque::with_capacity((completions + jobs) as usize),
            // No generation of a slot is `u32::MAX`, so the first use of each slot starts it ready.
            readiness: alloc::vec![
                Readiness {
                    generation: u32::MAX,
                    ..Readiness::default()
                };
                resources as usize
            ],
            group: None,
            batch: Vec::with_capacity(entries as usize),
            chain: Vec::with_capacity(entries as usize),
            jobs: Vec::with_capacity(jobs as usize),
            waker: None,
            events: alloc::vec![sys::EpollEvent::default(); EVENTS],
            full: false,
        })
    }

    /// The registrations of the resources in this poller.
    pub(crate) fn registrar(&self) -> Registrar {
        Registrar {
            epoll: Rc::clone(&self.epoll),
            #[cfg(test)]
            fail: core::cell::Cell::new(None),
        }
    }

    /// The epoll descriptor: it is readable when an event waits.
    pub(crate) fn fd(&self) -> BorrowedFd<'_> {
        use std::os::fd::AsFd as _;
        self.epoll.as_fd()
    }

    /// Copies `entry` of the resource `token` to the submission queue. The next submit takes it.
    pub(crate) fn push(&mut self, entry: &Sqe, token: Option<Token>) -> Result<(), Full> {
        if self.queue.len() >= self.entries as usize {
            return Err(Full);
        }
        self.queue.push_back((*entry, token));
        Ok(())
    }

    /// The entries of the submission queue.
    pub(crate) const fn entries(&self) -> u32 {
        self.entries
    }

    /// The entries that the submission queue has room for.
    pub(crate) fn room(&self) -> u32 {
        self.entries
            .saturating_sub(u32::try_from(self.queue.len()).unwrap_or(u32::MAX))
    }

    /// The pushed entries that the poller has not taken.
    pub(crate) fn unsubmitted(&self) -> u32 {
        u32::try_from(self.queue.len()).unwrap_or(u32::MAX)
    }

    /// Whether a completion is ready.
    pub(crate) fn ready(&self) -> bool {
        !self.completions.is_empty()
    }

    /// Whether events can remain after the last wait, because it filled its array.
    pub(crate) const fn more(&self) -> bool {
        self.full
    }

    /// The ready completions, which stay for the driver.
    #[cfg(test)]
    pub(crate) fn peek(&self) -> Vec<Cqe> {
        self.completions.iter().copied().collect()
    }

    /// Takes the ready completions, in order.
    pub(crate) fn completions(&mut self) -> Completions<'_> {
        Completions(&mut self.completions)
    }

    /// Takes the buffer ring at `base` with `entries` entries, a power of two, as the buffers of
    /// `group`. The memory stays mapped until [`Poller::release_group`].
    pub(crate) fn provide_group(&mut self, base: NonNull<u8>, entries: u16) {
        self.group = Some(Group::new(base, entries));
    }

    /// Ends the use of the buffer ring of [`Poller::provide_group`].
    pub(crate) fn release_group(&mut self) {
        self.group = None;
    }

    /// Takes the waiter of a job that runs the entry of `user_data` on the blocking service. The
    /// job posts its completion when it returns. `waker` wakes the driver at that return.
    pub(crate) fn add_job(&mut self, user_data: u64, waiter: Waiter<i32>, waker: &Waker) {
        if self.waker.is_none() {
            self.waker = Some(waker.clone());
        }
        // The list has room for each operation, and each one has at most one job.
        self.jobs.push((user_data, waiter));
    }

    /// Takes the entries of the submission queue and runs each one that is ready, takes the
    /// events, and posts the completions. With `wait` above 0, it waits until a completion is
    /// ready or a wake came. Gives the number of entries that it took.
    ///
    /// # Errors
    ///
    /// An error of `epoll_wait`, such as `Interrupted`.
    pub(crate) fn submit(&mut self, wait: u32) -> io::Result<u32> {
        self.enter(wait, None)
    }

    /// Submits as [`submit`](Self::submit) does, and waits at most for `timeout`. The end of the
    /// time is no error.
    #[cfg(feature = "local")]
    pub(crate) fn submit_timeout(&mut self, wait: u32, timeout: core::time::Duration) -> io::Result<u32> {
        self.enter(wait, Some(Instant::now() + timeout))
    }

    /// The submit of [`submit`](Self::submit), with no limit or until `deadline`.
    fn enter(&mut self, wait: u32, deadline: Option<Instant>) -> io::Result<u32> {
        let taken = self.take();
        self.poll_jobs();
        let mut woke = self.poll(0)?;
        while wait > 0 && !woke && self.completions.len() < wait as usize {
            let timeout = match deadline {
                None => -1,
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    // Rounded up, so the wait does not end before the deadline.
                    c_int::try_from(left.as_nanos().div_ceil(1_000_000)).unwrap_or(c_int::MAX)
                }
            };
            woke = self.poll(timeout)?;
            self.poll_jobs();
        }
        Ok(taken)
    }

    /// Takes each chain of the submission queue that has room in flight, and starts it.
    fn take(&mut self) -> u32 {
        let mut taken = 0;
        loop {
            let chain = self
                .queue
                .iter()
                .position(|(sqe, _)| sqe.flags & ring::IOSQE_IO_LINK == 0)
                .map_or(0, |last| last + 1);
            // Each entry in flight posts at most one completion, so the completion queue keeps its
            // bound. A chain whose last entry is not pushed yet waits.
            if chain == 0 || self.free.len() < chain + self.completions.len() {
                return taken;
            }
            let mut head = None;
            let mut previous: Option<u32> = None;
            for _ in 0..chain {
                let (Some((sqe, token)), Some(index)) = (self.queue.pop_front(), self.free.pop()) else {
                    return taken;
                };
                self.slots[index as usize] = Some(Slot {
                    sqe,
                    token,
                    link: None,
                    next: None,
                    connecting: false,
                });
                match previous {
                    Some(previous) => {
                        if let Some(slot) = self.slots[previous as usize].as_mut() {
                            slot.link = Some(index);
                        }
                    }
                    None => head = Some(index),
                }
                previous = Some(index);
            }
            taken += u32::try_from(chain).unwrap_or(u32::MAX);
            if let Some(head) = head {
                self.start(head);
            }
        }
    }

    /// Starts the entry of `index`: it runs now, or it waits for its direction.
    fn start(&mut self, index: u32) {
        let Some(slot) = self.slots[index as usize] else {
            return;
        };
        match slot.sqe.opcode {
            ring::IORING_OP_ASYNC_CANCEL => {
                let result = self.cancel(slot.sqe.addr);
                self.post(index, result, 0);
            }
            ring::IORING_OP_POLL_ADD => self.arm(index, slot.sqe.rw_flags),
            opcode => match (direction(opcode), slot.token) {
                (Some(direction), Some(token)) => {
                    let readiness = self.readiness_of(token);
                    let Some(readiness) = readiness else {
                        self.post(index, -sys::EINVAL, 0);
                        return;
                    };
                    let (ready, head) = {
                        let readiness = &self.readiness[readiness];
                        (readiness.ready[direction], readiness.head[direction])
                    };
                    if ready && head.is_none() {
                        match self.step(index) {
                            Step::Done(result, flags) => self.post(index, result, flags),
                            Step::Posted => {}
                            Step::Wait(waiting) => {
                                self.readiness[readiness].ready[direction] = false;
                                self.enqueue(readiness, direction, waiting);
                            }
                        }
                    } else {
                        self.enqueue(readiness, direction, index);
                    }
                }
                // An entry of a descriptor that is no resource waits in a one-shot registration.
                (Some(direction), None) => match self.step(index) {
                    Step::Done(result, flags) => self.post(index, result, flags),
                    Step::Posted => {}
                    Step::Wait(waiting) => {
                        let events = if direction == READ { sys::EPOLLIN } else { sys::EPOLLOUT };
                        self.arm(waiting, events);
                    }
                },
                (None, _) => match self.step(index) {
                    Step::Done(result, flags) => self.post(index, result, flags),
                    Step::Posted => {}
                    Step::Wait(waiting) => self.post(waiting, -sys::EINVAL, 0),
                },
            },
        }
    }

    /// The index of the readiness of `token`. A new generation of its slot starts with each
    /// direction ready.
    fn readiness_of(&mut self, token: Token) -> Option<usize> {
        let index = token.index as usize;
        let readiness = self.readiness.get_mut(index)?;
        let generation = token.generation & 0x7FFF_FFFF;
        if readiness.generation != generation {
            *readiness = Readiness {
                generation,
                ready: [true, true],
                ..Readiness::default()
            };
        }
        Some(index)
    }

    /// Adds the entry of `index` after the others that wait in `direction` of the resource.
    fn enqueue(&mut self, readiness: usize, direction: usize, index: u32) {
        let state = &mut self.readiness[readiness];
        match state.tail[direction] {
            Some(tail) => {
                if let Some(slot) = self.slots[tail as usize].as_mut() {
                    slot.next = Some(index);
                }
            }
            None => state.head[direction] = Some(index),
        }
        state.tail[direction] = Some(index);
    }

    /// Waits for `events` of the descriptor of the entry of `index`, which is no resource, in a
    /// one-shot registration.
    fn arm(&mut self, index: u32, events: u32) {
        let Some(slot) = self.slots[index as usize] else {
            return;
        };
        let (fd, data) = (slot.sqe.fd, ONE_SHOT | u64::from(index));
        let events = events | sys::EPOLLONESHOT;
        // A descriptor keeps its one-shot registration after its event, disabled, so the next
        // wait changes it.
        let armed = control(&self.epoll, sys::EPOLL_CTL_MOD, fd, events, data)
            .or_else(|_| control(&self.epoll, sys::EPOLL_CTL_ADD, fd, events, data));
        match armed {
            Ok(()) => {}
            Err(error) => self.post(index, -error.raw_os_error().unwrap_or(sys::EINVAL), 0),
        }
    }

    /// Posts the completion of the entry of `index`, ends the entries of its chain after a failure,
    /// and frees their slots.
    fn post(&mut self, index: u32, result: i32, flags: u32) {
        let Some(slot) = self.slots.get_mut(index as usize).and_then(Option::take) else {
            return;
        };
        self.free.push(index);
        if result < 0 || slot.sqe.flags & ring::IOSQE_CQE_SKIP_SUCCESS == 0 {
            self.completions.push_back(Cqe {
                user_data: slot.sqe.user_data,
                res: result,
                flags,
            });
        }
        if let Some(link) = slot.link {
            if result < 0 {
                self.post(link, -ECANCELED, 0);
            } else {
                self.start(link);
            }
        }
    }

    /// Cancels the entry of `user_data` that waits, and each entry of its chain. Gives the result
    /// of the cancel: 0, or `-ENOENT` when no such entry waits. An entry that the poller runs
    /// ends at the return of its call, so no entry is ever running at a cancel.
    fn cancel(&mut self, user_data: u64) -> i32 {
        let Some(index) = (0..).zip(&self.slots).find_map(|(index, slot)| {
            slot.filter(|slot| slot.sqe.user_data == user_data && self.waits(index))
                .map(|_| index)
        }) else {
            return -sys::ENOENT;
        };
        self.unwait(index);
        self.post(index, -ECANCELED, 0);
        0
    }

    /// Whether the entry of `index` waits for an edge.
    fn waits(&self, index: u32) -> bool {
        let Some(slot) = self.slots[index as usize] else {
            return false;
        };
        match (slot.token, direction(slot.sqe.opcode)) {
            (Some(token), Some(direction)) if slot.sqe.opcode != ring::IORING_OP_POLL_ADD => {
                let mut next = self
                    .readiness
                    .get(token.index as usize)
                    .and_then(|readiness| readiness.head[direction]);
                while let Some(waiting) = next {
                    if waiting == index {
                        return true;
                    }
                    next = self.slots[waiting as usize].and_then(|slot| slot.next);
                }
                false
            }
            // An entry with a one-shot registration waits until its event.
            _ => true,
        }
    }

    /// Takes the waiting entry of `index` out of its list or its one-shot registration.
    fn unwait(&mut self, index: u32) {
        let Some(slot) = self.slots[index as usize] else {
            return;
        };
        match (slot.token, direction(slot.sqe.opcode)) {
            (Some(token), Some(direction)) if slot.sqe.opcode != ring::IORING_OP_POLL_ADD => {
                let Some(state) = self.readiness.get_mut(token.index as usize) else {
                    return;
                };
                let mut previous = None;
                let mut next = state.head[direction];
                while let Some(waiting) = next {
                    let after = self.slots[waiting as usize].and_then(|slot| slot.next);
                    if waiting == index {
                        match previous {
                            Some(previous) => {
                                if let Some(slot) = self.slots[previous as usize].as_mut() {
                                    slot.next = after;
                                }
                            }
                            None => state.head[direction] = after,
                        }
                        if state.tail[direction] == Some(index) {
                            state.tail[direction] = previous;
                        }
                        return;
                    }
                    previous = Some(waiting);
                    next = after;
                }
            }
            _ => drop(control(&self.epoll, sys::EPOLL_CTL_DEL, slot.sqe.fd, 0, 0)),
        }
    }

    /// Waits up to `timeout` milliseconds for events, and runs the entries that they make ready.
    /// Gives whether the wake descriptor was readable.
    #[expect(unsafe_code, reason = "the kernel writes the event array")]
    fn poll(&mut self, timeout: c_int) -> io::Result<bool> {
        let capacity = c_int::try_from(self.events.len()).unwrap_or(c_int::MAX);
        // SAFETY: the array has `capacity` events, and the kernel writes at most that many.
        let count = unsafe { sys::epoll_wait(self.epoll.as_raw_fd(), self.events.as_mut_ptr(), capacity, timeout) };
        let Ok(count) = usize::try_from(count) else {
            return Err(io::Error::last_os_error());
        };
        self.full = count == self.events.len();
        let mut woke = false;
        for event in 0..count {
            let sys::EpollEvent { events, data } = self.events[event];
            if data == WAKE {
                woke = true;
            } else if data & ONE_SHOT != 0 {
                self.fired(u32::try_from(data & !ONE_SHOT).unwrap_or(u32::MAX), events);
            } else {
                let token = Token::of(data);
                let Some(readiness) = self.readiness_of(token) else {
                    continue;
                };
                for (direction, mask) in [(READ, READABLE), (WRITE, WRITABLE)] {
                    if events & mask != 0 {
                        self.readiness[readiness].ready[direction] = true;
                        self.run(readiness, direction);
                    }
                }
            }
        }
        Ok(woke)
    }

    /// The one-shot registration of the entry of `index` gave `events`.
    fn fired(&mut self, index: u32, events: u32) {
        let Some(slot) = self.slots.get(index as usize).copied().flatten() else {
            return;
        };
        if slot.sqe.opcode == ring::IORING_OP_POLL_ADD {
            self.post(index, i32::try_from(events & slot.sqe.rw_flags).unwrap_or(0), 0);
            return;
        }
        match self.step(index) {
            Step::Done(result, flags) => self.post(index, result, flags),
            Step::Posted => {}
            Step::Wait(waiting) => {
                let events = if direction(slot.sqe.opcode) == Some(READ) {
                    sys::EPOLLIN
                } else {
                    sys::EPOLLOUT
                };
                self.arm(waiting, events);
            }
        }
    }

    /// Runs the entries that wait in `direction` of the resource, in order, while it stays ready.
    fn run(&mut self, readiness: usize, direction: usize) {
        while self.readiness[readiness].ready[direction] {
            let Some(index) = self.readiness[readiness].head[direction] else {
                return;
            };
            let next = self.slots[index as usize].and_then(|slot| slot.next);
            match self.step(index) {
                step @ (Step::Done(..) | Step::Posted) => {
                    let state = &mut self.readiness[readiness];
                    state.head[direction] = next;
                    if next.is_none() {
                        state.tail[direction] = None;
                    }
                    if let Step::Done(result, flags) = step {
                        self.post(index, result, flags);
                    }
                }
                Step::Wait(waiting) => {
                    // The rest of a chain takes the place of its first entry.
                    if waiting != index {
                        let state = &mut self.readiness[readiness];
                        state.head[direction] = Some(waiting);
                        if state.tail[direction] == Some(index) {
                            state.tail[direction] = Some(waiting);
                        }
                        if let Some(slot) = self.slots[waiting as usize].as_mut() {
                            slot.next = next;
                        }
                    }
                    self.readiness[readiness].ready[direction] = false;
                }
            }
        }
    }

    /// Runs the call of the entry of `index`, or of its chain, one time.
    fn step(&mut self, index: u32) -> Step {
        let Some(slot) = self.slots[index as usize] else {
            return Step::Done(-sys::EINVAL, 0);
        };
        if slot.link.is_some() {
            return match slot.sqe.opcode {
                ring::IORING_OP_SENDMSG => self.send_batch(index),
                ring::IORING_OP_RECVMSG => self.recv_batch(index),
                // The driver links no other entries on this backend.
                _ => Step::Done(-sys::EINVAL, 0),
            };
        }
        if slot.sqe.opcode == ring::IORING_OP_CONNECT {
            return self.connect(index, slot);
        }
        let selected = slot.sqe.flags & ring::IOSQE_BUFFER_SELECT != 0;
        let buffer = if selected {
            match self.group.as_ref().and_then(|group| group.peek(0)) {
                Some(buffer) => Some(buffer),
                None => return Step::Done(-sys::ENOBUFS, 0),
            }
        } else {
            None
        };
        match call(&slot.sqe, buffer) {
            Err(sys::EAGAIN) => Step::Wait(index),
            Err(errno) => Step::Done(-errno, 0),
            Ok(result) => match buffer {
                Some((_, _, bid)) => {
                    if let Some(group) = self.group.as_mut() {
                        group.advance();
                    }
                    Step::Done(
                        result,
                        ring::IORING_CQE_F_BUFFER | (u32::from(bid) << ring::IORING_CQE_BUFFER_SHIFT),
                    )
                }
                None => Step::Done(result, 0),
            },
        }
    }

    /// A nonblocking connect: the first call starts it, and the write edge after `EINPROGRESS`
    /// gives its result in `SO_ERROR`.
    fn connect(&mut self, index: u32, slot: Slot) -> Step {
        if slot.connecting {
            return match pending_error(slot.sqe.fd) {
                0 => Step::Done(0, 0),
                errno => Step::Done(-errno, 0),
            };
        }
        match call(&slot.sqe, None) {
            Ok(result) => Step::Done(result, 0),
            Err(sys::EINPROGRESS) => {
                if let Some(slot) = self.slots[index as usize].as_mut() {
                    slot.connecting = true;
                }
                Step::Wait(index)
            }
            Err(errno) => Step::Done(-errno, 0),
        }
    }

    /// The datagrams of the chain from `index`, in `sendmmsg` calls. Each datagram that went posts
    /// its completion. After a part of the chain went, the next call sends the rest, because the
    /// call reports an error only for its first datagram. The rest waits for the next write edge,
    /// or the error ends it.
    #[expect(unsafe_code, reason = "the kernel reads the messages that the entries name")]
    fn send_batch(&mut self, mut index: u32) -> Step {
        loop {
            let (chain, fd, flags) = self.chain(index);
            // SAFETY: each entry names a message header of its batch, which the driver keeps until
            // its completion.
            let count = unsafe {
                rsnetwork_udp::sys::sendmmsg(
                    fd,
                    self.batch.as_mut_ptr(),
                    u32::try_from(self.batch.len()).unwrap_or(u32::MAX),
                    flags | sys::MSG_DONTWAIT,
                )
            };
            let Ok(count) = usize::try_from(count) else {
                self.chain = chain;
                return match errno() {
                    sys::EAGAIN => Step::Wait(index),
                    // The entry that failed is the first of the rest, and the rest ends with it.
                    errno => {
                        self.post(index, -errno, 0);
                        Step::Posted
                    }
                };
            };
            let rest = self.post_batch(&chain, count, false);
            self.chain = chain;
            match rest {
                Some(rest) => index = rest,
                None => return Step::Posted,
            }
        }
    }

    /// The datagrams of the chain from `index`, in one `recvmmsg` into selected buffers. Each
    /// datagram that came posts its completion, with its buffer. The first entry waits for a
    /// datagram. The entry after the last datagram ends with `EAGAIN`, or with `ENOBUFS` when no
    /// buffer remains, and the rest of the chain ends with it.
    #[expect(unsafe_code, reason = "the kernel writes the messages that the entries name")]
    fn recv_batch(&mut self, index: u32) -> Step {
        let (chain, fd, flags) = self.chain(index);
        let mut buffers = 0;
        if let Some(group) = self.group.as_ref() {
            for header in &mut self.batch {
                let Some((address, len, _)) = group.peek(buffers) else {
                    break;
                };
                // SAFETY: the header names the region of its message, which the driver keeps
                // until the completion. The poller writes it as the kernel writes the region of
                // a selected buffer.
                unsafe {
                    let region = &mut *header.msg_hdr.msg_iov;
                    region.iov_base = pointer(address);
                    region.iov_len = region.iov_len.min(len as usize);
                }
                buffers += 1;
            }
        }
        if buffers == 0 {
            self.chain = chain;
            return Step::Done(-sys::ENOBUFS, 0);
        }
        // SAFETY: as above, and the first `buffers` headers each name a free selected buffer.
        let count = unsafe {
            rsnetwork_udp::sys::recvmmsg(
                fd,
                self.batch.as_mut_ptr(),
                u32::from(buffers),
                flags | sys::MSG_DONTWAIT,
                core::ptr::null_mut(),
            )
        };
        let Ok(count) = usize::try_from(count) else {
            self.chain = chain;
            return match errno() {
                sys::EAGAIN => Step::Wait(index),
                errno => Step::Done(-errno, 0),
            };
        };
        let rest = self.post_batch(&chain, count, true);
        self.chain = chain;
        match rest {
            // The receive after the last datagram ends the chain: no datagram waits, or no buffer.
            Some(rest) => {
                let short = if count < usize::from(buffers) {
                    sys::EAGAIN
                } else {
                    sys::ENOBUFS
                };
                self.post(rest, -short, 0);
                Step::Posted
            }
            None => Step::Posted,
        }
    }

    /// The entries of the chain from `index`, its socket, and its call flags, with the header of
    /// each entry in the batch array.
    #[expect(unsafe_code, reason = "the poller copies the headers that the entries name")]
    fn chain(&mut self, index: u32) -> (Vec<u32>, RawFd, c_int) {
        self.batch.clear();
        let mut chain = core::mem::take(&mut self.chain);
        chain.clear();
        let mut next = Some(index);
        let (mut fd, mut flags) = (-1, 0);
        while let Some(entry) = next {
            let Some(slot) = self.slots[entry as usize] else {
                break;
            };
            if chain.is_empty() {
                (fd, flags) = (slot.sqe.fd, slot.sqe.rw_flags.cast_signed());
            }
            // SAFETY: the entry names a message header that the driver keeps until the completion.
            let header = unsafe { pointer::<MsgHdr>(slot.sqe.addr).cast_const().read() };
            self.batch.push(MmsgHdr {
                msg_hdr: header,
                msg_len: 0,
            });
            chain.push(entry);
            next = slot.link;
        }
        // The later receives of a batch take only the datagrams that wait.
        (chain, fd, flags & !rsnetwork_udp::sys::MSG_DONTWAIT)
    }

    /// Posts the completions of the first `count` datagrams of `chain`. For a receive, it writes
    /// back the fields of each header that the kernel wrote, and takes the selected buffer of each.
    /// Gives the first entry of the rest of the chain.
    #[expect(unsafe_code, reason = "the poller writes back the headers that the kernel wrote")]
    fn post_batch(&mut self, chain: &[u32], count: usize, receive: bool) -> Option<u32> {
        for (position, &entry) in chain.iter().enumerate().take(count) {
            let Some(slot) = self.slots[entry as usize].as_mut() else {
                continue;
            };
            // Each entry posts its own completion: without its link, only this entry ends.
            slot.link = None;
            let (written, address) = (self.batch[position], slot.sqe.addr);
            let mut flags = 0;
            if receive {
                // SAFETY: the entry names a message header that the driver keeps until the
                // completion, and the kernel wrote these fields of its copy.
                unsafe {
                    let header = &mut *pointer::<MsgHdr>(address);
                    header.msg_namelen = written.msg_hdr.msg_namelen;
                    header.msg_controllen = written.msg_hdr.msg_controllen;
                    header.msg_flags = written.msg_hdr.msg_flags;
                }
                if let Some(group) = self.group.as_mut()
                    && let Some((_, _, bid)) = group.peek(0)
                {
                    group.advance();
                    flags = ring::IORING_CQE_F_BUFFER | (u32::from(bid) << ring::IORING_CQE_BUFFER_SHIFT);
                }
            }
            self.post(entry, i32::try_from(written.msg_len).unwrap_or(i32::MAX), flags);
        }
        chain.get(count).copied()
    }

    /// Polls the job of each file entry, and posts the completion of each one that returned.
    fn poll_jobs(&mut self) {
        let Some(waker) = self.waker.as_ref() else {
            return;
        };
        let cx = Context::from_waker(waker);
        let mut index = 0;
        while let Some((user_data, waiter)) = self.jobs.get_mut(index) {
            match waiter.poll(&cx) {
                core::task::Poll::Ready(result) => {
                    let user_data = *user_data;
                    self.jobs.swap_remove(index);
                    self.completions.push_back(Cqe {
                        user_data,
                        res: result.unwrap_or(-ECANCELED),
                        flags: 0,
                    });
                }
                core::task::Poll::Pending => index += 1,
            }
        }
    }
}

// Not derived: the slots and the events are arrays of the size of the limits of the driver.
impl core::fmt::Debug for Poller {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Poller")
            .field("entries", &self.entries)
            .field("unsubmitted", &self.queue.len())
            .field("in_flight", &(self.slots.len() - self.free.len()))
            .field("completions", &self.completions.len())
            .field("jobs", &self.jobs.len())
            .finish_non_exhaustive()
    }
}

/// `ECANCELED` (`include/uapi/asm-generic/errno.h`).
const ECANCELED: i32 = super::ECANCELED;

/// The direction of the call of `opcode`, or `None` for a call that never waits.
const fn direction(opcode: u8) -> Option<usize> {
    match opcode {
        ring::IORING_OP_READ
        | ring::IORING_OP_READV
        | ring::IORING_OP_RECV
        | ring::IORING_OP_RECVMSG
        | ring::IORING_OP_ACCEPT
        | ring::IORING_OP_POLL_ADD => Some(READ),
        ring::IORING_OP_WRITE
        | ring::IORING_OP_WRITEV
        | ring::IORING_OP_SEND
        | ring::IORING_OP_SENDMSG
        | ring::IORING_OP_CONNECT => Some(WRITE),
        _ => None,
    }
}

/// The pointer of the address `value` of an entry, with the provenance that the driver exposed
/// when it made the entry.
fn pointer<T>(value: u64) -> *mut T {
    core::ptr::with_exposed_provenance_mut(usize::try_from(value).unwrap_or(0))
}

/// The result of a system call that returns a count, a descriptor, or 0, or -1.
fn long(result: core::ffi::c_long) -> Result<i32, i32> {
    if result < 0 {
        return Err(errno());
    }
    Ok(i32::try_from(result).unwrap_or(i32::MAX))
}

/// The C `errno` of the last failed call.
fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(sys::EINVAL)
}

/// The result of a call that returns a count or -1.
fn counted(result: isize) -> Result<i32, i32> {
    if result < 0 {
        return Err(errno());
    }
    Ok(i32::try_from(result).unwrap_or(i32::MAX))
}

/// The result of a call that returns 0 or -1, or a descriptor.
fn status(result: c_int) -> Result<i32, i32> {
    if result < 0 { Err(errno()) } else { Ok(result) }
}

/// The pending error of the socket `fd` (`SO_ERROR`), which a read clears.
#[expect(unsafe_code, reason = "the call writes one `int` that lives during the call")]
fn pending_error(fd: RawFd) -> i32 {
    let mut error: c_int = 0;
    let mut len = u32::try_from(size_of::<c_int>()).unwrap_or(4);
    // SAFETY: `error` and `len` are valid for writes during the call, and `len` is its size.
    let result = unsafe {
        rsnetwork_udp::sys::getsockopt(
            fd,
            rsnetwork_udp::sys::SOL_SOCKET,
            sys::SO_ERROR,
            (&raw mut error).cast(),
            &raw mut len,
        )
    };
    if result < 0 { errno() } else { error }
}

/// Runs the nonblocking call of a readiness entry on the thread of the driver, into the selected
/// buffer `buffer` when it has one. Gives the result of the call, or its `errno`.
#[expect(
    unsafe_code,
    reason = "the entry names memory that the driver keeps until the completion"
)]
fn call(sqe: &Sqe, buffer: Option<(u64, u32, u16)>) -> Result<i32, i32> {
    let fd = sqe.fd;
    // The memory of the entry, and the region of a read or a receive: the selected buffer, at
    // most the length of the entry when it names one. The length of a message entry is its count
    // of messages, so a message receive takes the length of its own region.
    let memory = pointer::<core::ffi::c_void>(sqe.addr);
    let (region, len) = match buffer {
        Some((address, len, _)) if sqe.len == 0 => (pointer(address), len as usize),
        Some((address, len, _)) => (pointer(address), len.min(sqe.len) as usize),
        None => (memory, sqe.len as usize),
    };
    let count = c_int::try_from(sqe.len).unwrap_or(c_int::MAX);
    let size = u32::try_from(sqe.off).unwrap_or(0);
    let flags = sqe.rw_flags.cast_signed() | sys::MSG_DONTWAIT;
    // SAFETY: the driver keeps the memory that the entry names, and its descriptor, until the
    // completion of the entry, and nothing else uses the memory that the call writes until then.
    unsafe {
        match sqe.opcode {
            ring::IORING_OP_NOP => Ok(0),
            // A read or a write at the position of a pipe, which only `u64::MAX` names.
            ring::IORING_OP_READ if sqe.off == u64::MAX => counted(sys::read(fd, region, len)),
            ring::IORING_OP_WRITE if sqe.off == u64::MAX => counted(sys::write(fd, memory, len)),
            ring::IORING_OP_READV if sqe.off == u64::MAX => counted(sys::readv(fd, memory.cast(), count)),
            ring::IORING_OP_WRITEV if sqe.off == u64::MAX => counted(sys::writev(fd, memory.cast(), count)),
            ring::IORING_OP_RECV => counted(sys::recv(fd, region, len, flags)),
            ring::IORING_OP_SEND => counted(sys::send(fd, memory, len, flags)),
            ring::IORING_OP_SENDMSG => counted(rsnetwork_udp::sys::sendmsg(fd, memory.cast(), flags)),
            ring::IORING_OP_RECVMSG => {
                let header = memory.cast::<MsgHdr>();
                if let Some((address, capacity, _)) = buffer {
                    // The poller writes the region of the message as the kernel writes the region
                    // of a selected buffer: at most the length of the region and of the buffer.
                    let vector: *mut IoVec = (*header).msg_iov;
                    (*vector).iov_base = pointer(address);
                    (*vector).iov_len = (*vector).iov_len.min(capacity as usize);
                }
                counted(rsnetwork_udp::sys::recvmsg(fd, header, flags))
            }
            ring::IORING_OP_ACCEPT => status(sys::accept4(fd, memory, pointer(sqe.off), sqe.rw_flags.cast_signed())),
            ring::IORING_OP_CONNECT => status(super::sys::connect(fd, memory, size)),
            ring::IORING_OP_SHUTDOWN => status(sys::shutdown(fd, sqe.len.cast_signed())),
            ring::IORING_OP_BIND => status(rsnetwork_udp::sys::bind(fd, memory, size)),
            ring::IORING_OP_LISTEN => status(rsnetwork_udp::sys::listen(fd, sqe.len.cast_signed())),
            // A cross-ring message has no ring here, as for a descriptor that is not a ring.
            ring::IORING_OP_MSG_RING => Err(sys::EBADFD),
            _ => Err(sys::EINVAL),
        }
    }
}

/// Runs a file entry as one blocking positional call: the job of the blocking service for the
/// entry on this backend. Gives the result that a ring posts for the entry.
#[expect(
    unsafe_code,
    reason = "the entry names memory that the driver keeps until the completion"
)]
pub(crate) fn run_file(sqe: &Sqe) -> i32 {
    use super::sys::{SYS_STATX, syscall};

    let fd = sqe.fd;
    let memory = pointer::<core::ffi::c_void>(sqe.addr);
    let flags = sqe.rw_flags.cast_signed();
    let offset = sqe.off.cast_signed();
    let (count, len) = (sqe.len as usize, sqe.len.cast_signed());
    let mut single = IoVec {
        iov_base: memory,
        iov_len: count,
    };
    // The high half of the position of `preadv2` and `pwritev2`, which a 64-bit target ignores.
    let high = 0_usize;
    // SAFETY: the driver keeps the memory that the entry names until the completion of the entry,
    // which comes after the return of this job, and the job owns its descriptor.
    let result = unsafe {
        match sqe.opcode {
            ring::IORING_OP_READ => long(syscall(
                sys::SYS_PREADV2,
                fd,
                &raw mut single,
                1_usize,
                offset,
                high,
                flags,
            )),
            ring::IORING_OP_WRITE => long(syscall(
                sys::SYS_PWRITEV2,
                fd,
                &raw mut single,
                1_usize,
                offset,
                high,
                flags,
            )),
            ring::IORING_OP_READV => long(syscall(sys::SYS_PREADV2, fd, memory, count, offset, high, flags)),
            ring::IORING_OP_WRITEV => long(syscall(sys::SYS_PWRITEV2, fd, memory, count, offset, high, flags)),
            ring::IORING_OP_FSYNC if sqe.rw_flags & ring::IORING_FSYNC_DATASYNC != 0 => status(sys::fdatasync(fd)),
            ring::IORING_OP_FSYNC => status(sys::fsync(fd)),
            ring::IORING_OP_FALLOCATE => status(sys::fallocate(fd, len, offset, sqe.addr.cast_signed())),
            ring::IORING_OP_FTRUNCATE => status(sys::ftruncate(fd, offset)),
            ring::IORING_OP_OPENAT => status(sys::openat(fd, memory.cast(), flags, sqe.len)),
            ring::IORING_OP_STATX => long(syscall(SYS_STATX, fd, memory, flags, sqe.len, pointer::<u8>(sqe.off))),
            ring::IORING_OP_RENAMEAT => long(syscall(
                sys::SYS_RENAMEAT2,
                fd,
                memory,
                len,
                pointer::<u8>(sqe.off),
                flags,
            )),
            ring::IORING_OP_UNLINKAT => status(sys::unlinkat(fd, memory.cast(), flags)),
            _ => Err(sys::EINVAL),
        }
    };
    result.unwrap_or_else(|errno| -errno)
}

/// The ready completions of a poller, in order. Each one leaves the queue as the iterator gives
/// it, so a completion that the iterator does not give stays for the next take.
#[derive(Debug)]
pub(crate) struct Completions<'a>(&'a mut VecDeque<Cqe>);

impl Iterator for Completions<'_> {
    type Item = Cqe;

    fn next(&mut self) -> Option<Cqe> {
        self.0.pop_front()
    }
}

// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
mod tests;
