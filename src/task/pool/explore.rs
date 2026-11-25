//! The named risks: a worker steals a task while another worker polls it, or the location of a
//! task is not the worker that polls it; a close that races a spawn leaves the task uncancelled, or
//! the pool does not end; a cancel from another thread that races the turns of two workers is lost.

use core::cell::Cell;
use core::future::{Future as _, pending, poll_fn};
use core::num::NonZeroU64;
use core::task::Poll;

use super::{Pool, Target, Turn};
use crate::capacity::Budget;
use crate::sync::Arc;
use crate::sync::atomic::{AtomicU32, Ordering};
use crate::task::{JoinHandle, Outcome};

fn budget() -> Budget {
    Budget::new(NonZeroU64::new(1 << 20).unwrap())
}

/// A pool of two workers in one cache domain, with `slots` slots.
fn pool(slots: u32) -> Arc<Pool> {
    Arc::new(Pool::new(&budget(), slots, &[None, None], 128).unwrap())
}

/// The slot of the task of `handle`.
fn slot_of<T>(handle: &JoinHandle<T>) -> u32 {
    match &handle.cancel.0 {
        Target::Movable(task) => task.slot,
        _ => panic!("a pool spawn gives a movable task"),
    }
}

loom::thread_local! {
    /// The lane of the worker of this thread, which the test sets and the pool does not read.
    static WORKER: Cell<u32> = Cell::new(u32::MAX);
}

/// Runs one turn of `lane` on another thread.
fn worker(pool: &Arc<Pool>, lane: u32) -> loom::thread::JoinHandle<Turn> {
    let pool = Arc::clone(pool);
    loom::thread::spawn(move || {
        WORKER.with(|worker| worker.set(lane));
        pool.turn(lane, 0)
    })
}

#[test]
fn a_task_is_polled_by_one_worker_at_a_time_and_its_location_is_that_worker() {
    let mut model = loom::model::Builder::new();
    // Three threads: 2 preemptions, as the other models with three threads.
    model.preemption_bound = Some(2);
    model.check(|| {
        let pool = pool(1);
        let slot = Arc::new(AtomicU32::new(u32::MAX));
        let polls = Arc::new(AtomicU32::new(0));
        let task = {
            let (pool, slot, polls) = (Arc::clone(&pool), Arc::clone(&slot), Arc::clone(&polls));
            poll_fn(move |cx| {
                let lane = WORKER.with(Cell::get);
                let own = slot.load(Ordering::Relaxed);
                assert_eq!(pool.shared.location[own as usize].load(Ordering::Relaxed), lane);
                // The first poll wakes its task, which another worker can then steal.
                if polls.fetch_add(1, Ordering::Relaxed) == 0 {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
        };
        let handle = pool.spawn(task).unwrap();
        slot.store(slot_of(&handle), Ordering::Relaxed);
        let (first, second) = (worker(&pool, 0), worker(&pool, 1));
        first.join().unwrap();
        second.join().unwrap();
        // Each poll that the two turns left runs here, as lane 0.
        WORKER.with(|worker| worker.set(0));
        for _ in 0..2 {
            pool.turn(0, 0);
        }
        assert!(matches!(handle.join_blocking(), Outcome::Completed(())));
        assert_eq!(polls.load(Ordering::Relaxed), 2);
    });
}

#[test]
fn a_close_that_races_a_spawn_cancels_its_task_and_the_pool_ends() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.check(|| {
        let pool = pool(1);
        let spawner = {
            let pool = Arc::clone(&pool);
            loom::thread::spawn(move || pool.spawn(pending::<()>()).ok())
        };
        let worker = worker(&pool, 0);
        pool.close();
        let handle = spawner.join().unwrap();
        worker.join().unwrap();
        // The task never ends by itself: only the cancel of the close ends it.
        let mut ended = false;
        for _ in 0..3 {
            if pool.turn(1, 0) == Turn::Ended {
                ended = true;
                break;
            }
        }
        assert!(ended, "the pool ends after its last task");
        if let Some(handle) = handle {
            assert!(matches!(handle.join_blocking(), Outcome::Cancelled));
        }
    });
}

#[test]
fn a_cancel_from_another_thread_that_races_a_steal_ends_the_task_once_and_the_pool_ends() {
    let mut model = loom::model::Builder::new();
    // Three threads, and each turn of a worker searches the queues of the other: 1 preemption in
    // the default suite, so it stays near the other models in time; the R407 campaign raises it.
    model.preemption_bound = Some(1);
    model.max_branches = 10_000;
    model.check(|| {
        let pool = pool(1);
        let polls = Arc::new(AtomicU32::new(0));
        let task = {
            let polls = Arc::clone(&polls);
            // The first poll wakes its task, so either worker can take it. Then it waits, and only
            // the cancel ends it.
            poll_fn(move |cx| {
                if polls.fetch_add(1, Ordering::Relaxed) == 0 {
                    cx.waker().wake_by_ref();
                }
                Poll::<()>::Pending
            })
        };
        let handle = pool.spawn(task).unwrap();
        // The cancel comes from this thread while both workers turn.
        let (first, second) = (worker(&pool, 0), worker(&pool, 1));
        handle.cancel_handle().cancel();
        first.join().unwrap();
        second.join().unwrap();
        // The turns after the cancel drop the future. A lost cancel leaves the task waiting, and
        // its join pending.
        for _ in 0..3 {
            pool.turn(0, 0);
        }
        let mut join = core::pin::pin!(handle.join());
        let mut cx = core::task::Context::from_waker(core::task::Waker::noop());
        assert!(
            matches!(join.as_mut().poll(&mut cx), Poll::Ready(Outcome::Cancelled)),
            "the cancel was lost"
        );
    });
}
