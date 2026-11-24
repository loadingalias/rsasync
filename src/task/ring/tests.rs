use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::assert_matches;
use core::cell::Cell;
use core::ffi::c_int;
use core::future::{Future, poll_fn};
use core::num::NonZeroU64;
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::{Poll, Waker};
use core::time::Duration;
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::os::fd::OwnedFd;
use std::os::unix::thread::{JoinHandleExt as _, RawPthread};
use std::sync::mpsc;
use std::thread;

use super::RingOwner;
use crate::blocking::{self, Service};
use crate::buffer::{Backing, Buffer as _};
use crate::capacity::Budget;
use crate::driver::tests::interrupts;
use crate::driver::{Backend, Config, Driver};
use crate::table::tests::allocations;
use crate::task::tests::yield_once;
use crate::task::{Outcome, spawn_local};

fn owner(slots: usize) -> RingOwner {
    let slots = u32::try_from(slots).unwrap();
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let driver = Driver::new(Config::new()).expect("the test host allows io_uring");
    RingOwner::new(&budget, slots, driver).unwrap()
}

/// The driver of `owner`, which a worker has from its start.
fn driver(owner: &RingOwner) -> core::cell::RefMut<'_, Driver> {
    core::cell::RefMut::map(owner.local.driver.borrow_mut(), |driver| {
        driver.as_mut().expect("a worker has its driver")
    })
}

/// Takes the writes of the wake descriptor since the last take, from a descriptor that shares
/// its count ([`interrupts`]). A descriptor with no write gives `WouldBlock`.
fn take(interrupts: &File) -> u64 {
    let mut count = [0; 8];
    (&*interrupts).read(&mut count).map_or(0, |_| u64::from_ne_bytes(count))
}

/// Gives its waker to `wakers` at its first poll, and ends at its next poll.
fn pend_once(wakers: mpsc::Sender<Waker>) -> impl Future<Output = ()> {
    let mut sent = false;
    poll_fn(move |cx| {
        if sent {
            return Poll::Ready(());
        }
        sent = true;
        wakers.send(cx.waker().clone()).unwrap();
        Poll::Pending
    })
}

/// Only the wake that finds the remote-ready list empty signals the home, so a burst of wakes
/// from many threads with no ring writes the wake descriptor one time for each batch. No wake
/// allocates.
#[test]
fn a_burst_of_wakes_from_threads_with_no_ring_signals_once_for_each_batch() {
    const TASKS: usize = 16;
    const THREADS: usize = 4;
    let owner = owner(TASKS);
    let interrupts = interrupts(&driver(&owner));
    owner
        .block_on(async move {
            for _batch in 0..2 {
                let (sender, receiver) = mpsc::channel();
                let tasks: Vec<_> = (0..TASKS)
                    .map(|_| spawn_local(pend_once(sender.clone())).unwrap())
                    .collect();
                yield_once().await;
                let wakers: Vec<Waker> = receiver.try_iter().collect();
                assert_eq!(wakers.len(), TASKS);
                // The owner stays in this poll of the root during the burst, so it takes the
                // whole burst as one batch.
                let allocations = thread::scope(|scope| {
                    let threads: Vec<_> = wakers
                        .chunks(TASKS / THREADS)
                        .map(|wakers| {
                            scope.spawn(move || {
                                let before = allocations::count();
                                wakers.iter().for_each(Waker::wake_by_ref);
                                allocations::count() - before
                            })
                        })
                        .collect();
                    threads.into_iter().map(|thread| thread.join().unwrap()).sum::<u64>()
                });
                assert_eq!(allocations, 0);
                assert_eq!(take(&interrupts), 1);
                for task in tasks {
                    assert_matches!(task.join().await, Outcome::Completed(()));
                }
            }
        })
        .unwrap();
}

/// A wake from a thread whose owner has a ring sends a message from that ring, and the message
/// alone ends the wait of the home in its ring: the wake descriptor of the home gets no
/// write. The wakes, the message, and its submit allocate nothing.
#[test]
fn a_wake_from_a_thread_with_a_ring_ends_the_wait_of_its_home_by_a_message() {
    const TASKS: usize = 8;
    let (wakers, received) = mpsc::channel::<Vec<Waker>>();
    let target = thread::spawn(move || {
        let owner = owner(TASKS);
        let interrupts = interrupts(&driver(&owner));
        owner
            .block_on(async move {
                let (sender, receiver) = mpsc::channel();
                let tasks: Vec<_> = (0..TASKS)
                    .map(|_| spawn_local(pend_once(sender.clone())).unwrap())
                    .collect();
                yield_once().await;
                wakers.send(receiver.try_iter().collect()).unwrap();
                // The owner waits in its ring until the wakes come.
                for task in tasks {
                    assert_matches!(task.join().await, Outcome::Completed(()));
                }
                take(&interrupts)
            })
            .unwrap()
    });
    let wakers = received.recv().unwrap();
    assert_eq!(wakers.len(), TASKS);
    // The target then waits in its ring with near certainty. If it does not, it takes the wakes
    // before its wait, and the test still holds.
    thread::sleep(Duration::from_millis(50));
    let sender = owner(1);
    let allocations = sender
        .block_on(async move {
            let before = allocations::count();
            wakers.iter().for_each(Waker::wake_by_ref);
            // The owner sends the message before its next turn of the driver.
            yield_once().await;
            allocations::count() - before
        })
        .unwrap();
    assert_eq!(allocations, 0);
    assert_eq!(target.join().unwrap(), 0);
}

/// A job of the blocking service wakes its local task from a thread with no ring, and the wake
/// reaches the home, which waits in its ring.
#[test]
fn a_wake_from_a_blocking_service_thread_reaches_its_home() {
    let budget = Budget::new(NonZeroU64::new(4 << 20).unwrap());
    let config = blocking::Config::new().threads(1).jobs(1).stack(256 << 10);
    let service = Service::new(&budget, config).unwrap();
    let submitter = service.submitter();
    let owner = owner(1);
    let outcome = owner
        .block_on(async move {
            let task = spawn_local(async move {
                let mut waiter = submitter
                    .submit(|| {
                        // The home then waits in its ring with near certainty.
                        thread::sleep(Duration::from_millis(20));
                        5
                    })
                    .unwrap();
                waiter.wait().await
            })
            .unwrap();
            task.join().await
        })
        .unwrap();
    assert_matches!(outcome, Outcome::Completed(Ok(5)));
    service.teardown();
}

/// A task that is always ready keeps the owner from its wait in the ring, which runs the deferred
/// work of the kernel. A turn that sees the task-run flag enters the kernel to run that work, so
/// the receives complete while the task still runs. A short burst completes its receive with no
/// wait for a full buffer, and a close of the peer completes its receive at the end of the stream.
#[test]
fn a_busy_owner_completes_a_burst_and_a_close_while_its_task_runs() {
    /// The rounds of the busy task, far more than the receives need.
    const ROUNDS: u32 = 1_000_000;
    let mut owner = owner(2);
    let receive = |owner: &mut RingOwner| {
        let (read, write) = io::pipe().expect("the test host makes a pipe");
        let reader = driver(owner)
            .adopt_reader(OwnedFd::from(read))
            .expect("the test driver has room");
        // The future borrows its reader for the rest of the test.
        let reader = Box::leak(Box::new(reader));
        let future = driver(owner)
            .recv(reader, Backing::from(Vec::with_capacity(64)))
            .expect("the test driver has room");
        (future, write)
    };
    let (burst, mut burst_writer) = receive(&mut owner);
    let (close, close_writer) = receive(&mut owner);
    let (received, busy_rounds) = owner
        .block_on(async move {
            let (done, rounds) = (Rc::new(Cell::new(false)), Rc::new(Cell::new(0)));
            let busy = spawn_local({
                let (done, rounds) = (Rc::clone(&done), Rc::clone(&rounds));
                async move {
                    while !done.get() && rounds.get() < ROUNDS {
                        rounds.update(|rounds| rounds + 1);
                        yield_once().await;
                    }
                }
            })
            .unwrap();
            // The turns of the yield submit the two receives, which then wait for data.
            yield_once().await;
            burst_writer.write_all(b"abc").unwrap();
            drop(close_writer);
            let (burst, close) = (burst.await.unwrap(), close.await.unwrap());
            let at = rounds.get();
            done.set(true);
            assert_matches!(busy.join().await, Outcome::Completed(()));
            ((burst.0.unwrap(), burst.1.bytes().to_vec(), close.0.unwrap()), at)
        })
        .unwrap();
    assert_eq!(received, (3, b"abc".to_vec(), 0));
    // The receives completed while the task still ran.
    assert!(busy_rounds < ROUNDS, "{busy_rounds}");
}

/// On a home with a ring, a parked owner wakes from its wait in the ring for the timer of a task
/// when no other work comes, and the timer never fires before its deadline. The check of lateness
/// is only for a missed wake: the runtime does not bound the wake delay of the system.
#[test]
fn a_timer_never_fires_early_and_wakes_its_owner_from_its_ring() {
    let resolution = core::time::Duration::from_millis(1);
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let (clock, timers) = crate::task::tests::timed(&budget, 4, resolution);
    let mut owner = owner(1);
    owner.use_timers(timers, clock).unwrap();
    let lateness = owner
        .block_on(crate::task::tests::wake_for_timers(clock, resolution, Rc::default()))
        .unwrap();
    for late in lateness {
        assert!(late < core::time::Duration::from_secs(1), "{late:?}");
    }
}

/// Handled signals that come faster than the timeout of a park, such as the `SIGPROF` of a
/// profiler, do not keep a parked owner past the deadline of its timer. Each signal ends the wait
/// in the kernel with `EINTR`, also under `SA_RESTART`, and the next park takes the time that
/// remains. The check of lateness is only for a missed wake, as in
/// `a_timer_never_fires_early_and_wakes_its_owner_from_its_ring`.
fn signals_do_not_delay_the_timer_of_a_parked_owner(backend: Backend) {
    /// `SIGUSR1` on x86_64 and aarch64 Linux.
    const SIGUSR1: c_int = 10;
    /// The time from the park to the deadline: many intervals of the signals.
    const TIMEOUT: Duration = Duration::from_millis(100);
    const MISSED_WAKE: Duration = Duration::from_secs(1);
    static HANDLED: AtomicU32 = AtomicU32::new(0);
    extern "C" fn handle(_signal: c_int) {
        HANDLED.fetch_add(1, Ordering::Relaxed);
    }
    #[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
    unsafe extern "C" {
        /// `sighandler_t signal(int sig, sighandler_t handler)`. glibc and musl install the
        /// handler with `SA_RESTART`. `SIG_ERR` is `-1`.
        fn signal(sig: c_int, handler: extern "C" fn(c_int)) -> isize;
        /// `int pthread_kill(pthread_t thread, int sig)`.
        fn pthread_kill(thread: RawPthread, sig: c_int) -> c_int;
    }

    // SAFETY: the handler only adds to an atomic counter, which is async-signal-safe.
    #[expect(unsafe_code, reason = "the handler is async-signal-safe")]
    let installed = unsafe { signal(SIGUSR1, handle) };
    assert_ne!(installed, -1);
    let (ready, started) = mpsc::channel();
    let parked = thread::spawn(move || {
        let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
        let (clock, timers) = crate::task::tests::timed(&budget, 1, Duration::from_millis(1));
        let coarsening = timers.max_coarsening();
        let driver = Driver::new(Config::new().backend(backend)).expect("the test host has the backend");
        let mut owner = RingOwner::new(&budget, 1, driver).unwrap();
        owner.use_timers(timers, clock).unwrap();
        ready.send(()).unwrap();
        let late = owner
            .block_on(async move {
                let deadline = clock.now().checked_add(TIMEOUT).unwrap();
                crate::task::timer(crate::time::Deadline::at(deadline))
                    .unwrap()
                    .await
                    .unwrap();
                clock
                    .now()
                    .duration_since(deadline)
                    .expect("a timer never fires before its deadline")
            })
            .unwrap();
        (late, coarsening)
    });
    started.recv().unwrap();
    // A signal each millisecond until the owner ends. A park that takes its whole timeout again
    // at each signal ends only after the last signal, long after the bound.
    let start = std::time::Instant::now();
    while !parked.is_finished() && start.elapsed() < TIMEOUT + 2 * MISSED_WAKE {
        // SAFETY: the thread is not joined, so its handle is valid.
        #[expect(unsafe_code, reason = "the handle names a thread that is not joined")]
        unsafe {
            pthread_kill(parked.as_pthread_t(), SIGUSR1);
        }
        thread::sleep(Duration::from_millis(1));
    }
    let (late, coarsening) = parked.join().unwrap();
    assert_ne!(HANDLED.load(Ordering::Relaxed), 0, "the signals reach the owner");
    assert!(late < coarsening + MISSED_WAKE, "{late:?}");
}

#[test]
fn signals_do_not_delay_the_timer_of_a_parked_owner_on_io_uring() {
    signals_do_not_delay_the_timer_of_a_parked_owner(Backend::IoUring);
}

#[test]
fn signals_do_not_delay_the_timer_of_a_parked_owner_on_epoll() {
    signals_do_not_delay_the_timer_of_a_parked_owner(Backend::Epoll);
}

/// The charge of an owner with a ring equals the bytes that its construction allocates. A home
/// that boxes its ring in an allocation that no charge counts fails.
#[test]
fn a_ring_owner_charges_each_byte_that_it_allocates() {
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let driver = Driver::new(Config::new()).expect("the test host allows io_uring");
    let before = allocations::bytes();
    let owner = RingOwner::new(&budget, 3, driver).unwrap();
    let allocated = allocations::bytes() - before;
    assert_eq!((1 << 20) - budget.available(), allocated);
    drop(owner);
}
