//! The movable pool on a runtime: refusals, steals and locations, and the outcomes of its tasks.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::assert_matches;
use core::future::{pending, poll_fn};
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::Poll;
use core::time::Duration;
use std::sync::{Mutex, mpsc};
use std::time::Instant;

use super::{Pool, Target};
use crate::runtime::{Outcome as WorkerOutcome, Runtime};
use crate::task::{JoinHandle, Outcome, POOL, Reason, spawn_movable};

/// The pool of this thread: a worker of a runtime with movable workers.
fn current() -> Arc<Pool> {
    POOL.with(|pool| pool.borrow().clone())
        .expect("a worker of the runtime has its pool")
}

/// The slot of the task of `handle`.
fn slot_of<T>(handle: &JoinHandle<T>) -> u32 {
    match &handle.cancel.0 {
        Target::Movable(task) => task.slot,
        _ => panic!("a movable spawn gives a movable task"),
    }
}

/// Work of `micros` microseconds on this thread with no runtime leaf: the work that a steal moves.
fn spin(micros: u64) {
    let end = Instant::now() + Duration::from_micros(micros);
    while Instant::now() < end {
        core::hint::spin_loop();
    }
}

#[test]
fn a_refused_movable_spawn_gives_its_future_back() {
    let refused = spawn_movable(async { 1 }).unwrap_err();
    assert_eq!(refused.reason(), Reason::NoPool, "this thread runs no runtime");

    let runtime = Runtime::builder().local_workers(1).build().unwrap();
    let handle = runtime.handle().unwrap();
    let refused = handle.spawn_movable(async { 1 }).unwrap_err();
    assert_eq!(refused.reason(), Reason::NoPool, "the runtime has no movable worker");
    drop(handle);
    drop(runtime.shutdown());

    let runtime = Runtime::builder().movable_workers(1).task_slots(1).build().unwrap();
    let handle = runtime.handle().unwrap();
    let held = handle.spawn_movable(pending::<()>()).unwrap();
    let refused = handle.spawn_movable(async { 2 }).unwrap_err();
    assert_eq!(refused.reason(), Reason::NoSlot);
    assert_eq!(runtime.block_on(refused.into_future()).unwrap(), 2);
    let report = runtime.shutdown();
    assert_eq!(report.movable_cancelled(), 1);
    assert_matches!(held.join_blocking(), Outcome::Cancelled);
    let refused = handle.spawn_movable(async { 3 }).unwrap_err();
    assert_eq!(refused.reason(), Reason::Closed, "the runtime stopped");
}

/// One worker spawns each task, so each one starts in the queue of that worker. The idle workers
/// steal them. Each poll checks that the location of its task is the worker that polls it.
#[test]
fn idle_workers_steal_an_imbalanced_load_and_each_poll_runs_at_the_location_of_its_task() {
    const WORKERS: u32 = 4;
    const TASKS: usize = 64;
    const POLLS: u32 = 8;
    let runtime = Runtime::builder().movable_workers(WORKERS).build().unwrap();
    let handle = runtime.handle().unwrap();
    let lanes = Arc::new(Mutex::new(Vec::new()));
    let producer = {
        let lanes = Arc::clone(&lanes);
        handle
            .spawn_movable(async move {
                let mut tasks = Vec::new();
                for _ in 0..TASKS {
                    let slot = Arc::new(AtomicU32::new(u32::MAX));
                    let (own, lanes) = (Arc::clone(&slot), Arc::clone(&lanes));
                    let mut polls = 0;
                    let task = poll_fn(move |cx| {
                        let pool = current();
                        // The lane of the thread from the name that the runtime gives it.
                        let lane: u32 = std::thread::current()
                            .name()
                            .and_then(|name| name.strip_prefix("rsasync-movable-"))
                            .and_then(|lane| lane.parse().ok())
                            .expect("a worker of the pool polls");
                        // A poll before the spawn stored the slot has no check.
                        let slot = own.load(Ordering::Relaxed);
                        if slot != u32::MAX {
                            assert_eq!(pool.shared.location[slot as usize].load(Ordering::Relaxed), lane);
                            lanes.lock().unwrap().push(lane);
                        }
                        spin(100);
                        polls += 1;
                        if polls == POLLS {
                            Poll::Ready(())
                        } else {
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    });
                    let task = spawn_movable(task).unwrap();
                    slot.store(slot_of(&task), Ordering::Relaxed);
                    tasks.push(task);
                }
                for task in tasks {
                    assert_matches!(task.join().await, Outcome::Completed(()));
                }
            })
            .unwrap()
    };
    assert_matches!(producer.join_blocking(), Outcome::Completed(()));
    let lanes = lanes.lock().unwrap();
    assert!(lanes.len() > TASKS, "most polls had their check: {}", lanes.len());
    let ran = (0..WORKERS).filter(|worker| lanes.contains(worker)).count();
    assert!(
        ran >= 2,
        "the idle workers stole no task: the polls ran on {ran} worker"
    );
}

/// The outcomes of a movable task are those of a local task (`task::tests::each_outcome_is_reported`).
#[test]
fn a_movable_task_ends_with_each_outcome_of_a_local_task() {
    let runtime = Runtime::builder().movable_workers(2).build().unwrap();
    let handle = runtime.handle().unwrap();
    assert_matches!(
        handle.spawn_movable(async { 7 }).unwrap().join_blocking(),
        Outcome::Completed(7)
    );
    let panicked = handle
        .spawn_movable(async { panic!("the poll of a movable task panics") })
        .unwrap();
    assert_matches!(panicked.join_blocking(), Outcome::Panicked(_));

    let waiting = handle.spawn_movable(pending::<()>()).unwrap();
    let cancel = waiting.cancel_handle();
    std::thread::spawn(move || cancel.cancel()).join().unwrap();
    assert_matches!(waiting.join_blocking(), Outcome::Cancelled);

    // The drop of a join handle does not cancel its task.
    let (sender, receiver) = mpsc::channel();
    drop(handle.spawn_movable(async move { sender.send(9).unwrap() }).unwrap());
    assert_eq!(receiver.recv().unwrap(), 9);

    let live = handle.spawn_movable(pending::<()>()).unwrap();
    let report = runtime.shutdown();
    assert_matches!(live.join_blocking(), Outcome::Cancelled);
    assert_eq!(report.movable_cancelled(), 1);
    assert_eq!(report.workers().len(), 2);
    assert!(
        report
            .workers()
            .iter()
            .all(|worker| *worker.outcome() == WorkerOutcome::Joined)
    );
}

/// A task that another thread spawns runs on the one worker while the queue of that worker never
/// empties: a task there wakes itself at each poll, and the spawn waits in the list of the worker.
#[test]
fn a_spawn_from_another_thread_runs_while_the_queue_of_its_worker_never_empties() {
    let runtime = Runtime::builder().movable_workers(1).build().unwrap();
    let handle = runtime.handle().unwrap();
    let (polls, done) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let busy = {
        let (polls, done) = (Arc::clone(&polls), Arc::clone(&done));
        handle
            .spawn_movable(poll_fn(move |cx| {
                polls.fetch_add(1, Ordering::Relaxed);
                if done.load(Ordering::Acquire) == 1 {
                    return Poll::Ready(());
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            }))
            .unwrap()
    };
    // The busy task is in the queue of the worker at each turn from now.
    let start = Instant::now();
    while polls.load(Ordering::Relaxed) < 2 {
        assert!(start.elapsed() < Duration::from_secs(5), "the busy task does not poll");
        core::hint::spin_loop();
    }
    let late = {
        let done = Arc::clone(&done);
        handle
            .spawn_movable(async move { done.store(1, Ordering::Release) })
            .unwrap()
    };
    assert_matches!(late.join_blocking(), Outcome::Completed(()));
    assert_matches!(busy.join_blocking(), Outcome::Completed(()));
    drop(handle);
    drop(runtime.shutdown());
}
