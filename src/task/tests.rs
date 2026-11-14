use alloc::format;
use alloc::rc::Rc;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use core::future::{Future, pending, poll_fn};
use core::num::NonZeroU64;
use core::panic::AssertUnwindSafe;
use core::pin::{Pin, pin};
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};
use std::panic::catch_unwind;

use super::{
    CancelHandle, JoinHandle, LocalOwner, Outcome, OwnerFailure, Reason, Refused, spawn_local, spawn_local_waiting,
};
use crate::capacity::Budget;
use crate::channel::oneshot;

fn budget() -> Budget {
    Budget::new(NonZeroU64::new(1 << 20).unwrap())
}

/// Gives `Pending` one time, after it wakes its own task, so the owner polls the other tasks.
pub(super) async fn yield_once() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

/// Polls `future` one time, and gives the result of that poll.
async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx: &mut Context<'_>| Poll::Ready(future.as_mut().poll(cx))).await
}

#[test]
fn construction_refuses_with_a_typed_reason() {
    let budget = budget();
    assert_eq!(LocalOwner::new(&budget, 0).unwrap_err(), Refused::Zero);
    assert_eq!(LocalOwner::new(&budget, (1 << 16) + 1).unwrap_err(), Refused::TooLarge);
    let small = Budget::new(NonZeroU64::new(16).unwrap());
    assert!(matches!(LocalOwner::new(&small, 1).unwrap_err(), Refused::Budget(_)));
}

#[test]
fn a_refused_spawn_gives_the_future_back_with_its_reason() {
    let refused = spawn_local(async { 1 }).unwrap_err();
    assert_eq!(refused.reason(), Reason::NoOwner);

    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 2).unwrap();
    owner.block_on(async {
        let _first = spawn_local(pending::<()>()).unwrap();
        // Too few free units for the task.
        let rest = budget.reserve(budget.available()).unwrap();
        let refused = spawn_local(async { 7 }).unwrap_err();
        assert!(matches!(refused.reason(), Reason::Budget(_)));
        drop(rest);
        let _second = spawn_local(pending::<()>()).unwrap();
        // No free slot. The future comes back, and it still runs.
        let refused = spawn_local(async { 7 }).unwrap_err();
        assert_eq!(refused.reason(), Reason::NoSlot);
        assert_eq!(refused.into_future().await, 7);
    });
}

#[test]
fn the_waiting_form_spawns_after_a_slot_frees() {
    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    owner.block_on(async {
        let (sender, mut receiver) = oneshot::channel::<()>(&budget).unwrap();
        let first = spawn_local(async move { receiver.recv().await.unwrap() }).unwrap();
        let mut waiting = pin!(spawn_local_waiting(async { 2 }));
        assert!(poll_once(waiting.as_mut()).await.is_pending());
        // The first task ends, and its slot goes to the waiting spawn.
        sender.send(()).unwrap();
        let second = waiting.await.unwrap();
        assert!(matches!(first.join().await, Outcome::Completed(())));
        assert!(matches!(second.join().await, Outcome::Completed(2)));
    });
}

#[test]
fn a_dropped_join_handle_does_not_cancel_the_task() {
    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    let done = Rc::new(Cell::new(false));
    owner.block_on(async {
        let flag = Rc::clone(&done);
        drop(
            spawn_local(async move {
                yield_once().await;
                flag.set(true);
            })
            .unwrap(),
        );
        while !done.get() {
            yield_once().await;
        }
    });
    assert!(done.get());
}

#[test]
fn a_cancel_from_another_thread_drops_the_future_at_a_poll_boundary() {
    use std::sync::mpsc;

    /// A future that waits in its poll until the cancel came, and records a drop during a poll.
    struct Probe {
        in_poll: Arc<AtomicBool>,
        dropped_in_poll: Arc<AtomicBool>,
        started: mpsc::Sender<()>,
        cancelled: mpsc::Receiver<()>,
    }

    impl Future for Probe {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.in_poll.store(true, Ordering::SeqCst);
            self.started.send(()).unwrap();
            self.cancelled.recv().unwrap();
            self.in_poll.store(false, Ordering::SeqCst);
            Poll::Pending
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            if self.in_poll.load(Ordering::SeqCst) {
                self.dropped_in_poll.store(true, Ordering::SeqCst);
            }
        }
    }

    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    let dropped_in_poll = Arc::new(AtomicBool::new(false));
    let (started, poll_started) = mpsc::channel();
    let (cancel_done, cancelled) = mpsc::channel();
    let probe = Probe {
        in_poll: Arc::new(AtomicBool::new(false)),
        dropped_in_poll: Arc::clone(&dropped_in_poll),
        started,
        cancelled,
    };
    owner.block_on(async {
        let task = spawn_local(probe).unwrap();
        let cancel = task.cancel_handle();
        // The cancel comes while the owner polls the task.
        let canceller = std::thread::spawn(move || {
            poll_started.recv().unwrap();
            cancel.cancel();
            cancel_done.send(()).unwrap();
        });
        assert!(matches!(task.join().await, Outcome::Cancelled));
        canceller.join().unwrap();
    });
    assert!(!dropped_in_poll.load(Ordering::SeqCst));
}

#[test]
fn an_output_stays_charged_until_it_is_joined() {
    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    let before = budget.available();
    owner.block_on(async {
        let task = spawn_local(async { [7_u8; 4096] }).unwrap();
        yield_once().await;
        // The task ended, and its output waits for the join, charged.
        assert!(budget.available() <= before - 4096);
        let Outcome::Completed(bytes) = task.join().await else {
            panic!("the task completes");
        };
        assert_eq!(bytes[0], 7);
        assert_eq!(budget.available(), before);
    });
}

#[test]
fn each_outcome_is_reported() {
    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 4).unwrap();
    owner.block_on(async {
        // Local state: the task holds an `Rc`.
        let state = Rc::new(Cell::new(0));
        let shared = Rc::clone(&state);
        let completed = spawn_local(async move {
            shared.set(1);
            yield_once().await;
            shared.get() + 1
        })
        .unwrap();
        let cancelled = spawn_local(pending::<()>()).unwrap();
        cancelled.cancel_handle().cancel();
        let panicked = spawn_local(async { panic!("the task panics") }).unwrap();
        assert!(matches!(completed.join().await, Outcome::Completed(2)));
        assert!(matches!(cancelled.join().await, Outcome::Cancelled));
        assert!(matches!(panicked.join().await, Outcome::Panicked(_)));
    });

    // The root panics while a task is live.
    let kept: RefCell<Option<JoinHandle<()>>> = RefCell::new(None);
    let ended = catch_unwind(AssertUnwindSafe(|| {
        owner.block_on(async {
            kept.replace(Some(spawn_local(pending::<()>()).unwrap()));
            panic!("the root panics");
        })
    }));
    assert!(ended.is_err());
    let task = kept.take().unwrap();
    assert!(matches!(task.join_blocking(), Outcome::OwnerFailed(OwnerFailure)));
    // The owner runs again after the failure.
    assert_eq!(owner.block_on(async { 3 }), 3);
}

#[test]
fn a_stale_wake_after_slot_reuse_does_not_poll_the_new_task() {
    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    owner.block_on(async {
        let kept: Rc<RefCell<Option<Waker>>> = Rc::default();
        let out = Rc::clone(&kept);
        let first = spawn_local(poll_fn(move |cx| {
            out.replace(Some(cx.waker().clone()));
            Poll::Ready(())
        }))
        .unwrap();
        assert!(matches!(first.join().await, Outcome::Completed(())));
        // The second task takes the one slot, with the next generation.
        let polls = Rc::new(Cell::new(0));
        let count = Rc::clone(&polls);
        let _second = spawn_local(poll_fn(move |_| {
            count.set(count.get() + 1);
            Poll::<()>::Pending
        }))
        .unwrap();
        yield_once().await;
        assert_eq!(polls.get(), 1);
        kept.take().unwrap().wake();
        yield_once().await;
        yield_once().await;
        assert_eq!(polls.get(), 1);
    });
}

#[test]
fn block_on_cancels_each_live_task_before_it_returns() {
    /// Records its drop.
    struct Guard(Rc<Cell<bool>>);

    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    let dropped = Rc::new(Cell::new(false));
    let guard = Guard(Rc::clone(&dropped));
    let task = owner.block_on(async move {
        spawn_local(async move {
            let _guard = guard;
            pending::<()>().await;
        })
        .unwrap()
    });
    assert!(dropped.get());
    assert!(matches!(task.join_blocking(), Outcome::Cancelled));
}

#[test]
fn a_wake_from_another_thread_reaches_a_local_task() {
    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    owner.block_on(async {
        let (sender, mut receiver) = oneshot::channel::<u32>(&budget).unwrap();
        let task = spawn_local(async move { receiver.recv().await.unwrap() }).unwrap();
        let other = std::thread::spawn(move || sender.send(5));
        assert!(matches!(task.join().await, Outcome::Completed(5)));
        other.join().unwrap().unwrap();
    });
}

/// A future that is ready at its first poll if `ready`, and pending if not. Its drop enters the
/// owner again: it spawns a task and records the result in `spawns`, and it cancels `victim`.
struct Reenters {
    ready: bool,
    victim: Option<CancelHandle>,
    spawns: Rc<RefCell<Vec<Result<(), Reason>>>>,
}

impl Future for Reenters {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.ready { Poll::Ready(()) } else { Poll::Pending }
    }
}

impl Drop for Reenters {
    fn drop(&mut self) {
        let spawned = spawn_local(async {}).map(drop).map_err(|refused| refused.reason());
        self.spawns.borrow_mut().push(spawned);
        if let Some(victim) = self.victim.take() {
            victim.cancel();
        }
    }
}

#[test]
fn a_destructor_that_enters_the_owner_again_does_not_deadlock() {
    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 8).unwrap();
    let spawns = Rc::default();
    let reenters = |ready, victim| Reenters {
        ready,
        victim,
        spawns: Rc::clone(&spawns),
    };
    owner.block_on(async {
        // The future drops after its output, in the poll of its task.
        let victim = spawn_local(pending::<()>()).unwrap();
        let task = spawn_local(reenters(true, Some(victim.cancel_handle()))).unwrap();
        assert!(matches!(task.join().await, Outcome::Completed(())));
        assert!(matches!(victim.join().await, Outcome::Cancelled));

        // The future drops at its cancel, at the start of its next poll.
        let victim = spawn_local(pending::<()>()).unwrap();
        let task = spawn_local(reenters(false, Some(victim.cancel_handle()))).unwrap();
        yield_once().await;
        task.cancel_handle().cancel();
        assert!(matches!(task.join().await, Outcome::Cancelled));
        assert!(matches!(victim.join().await, Outcome::Cancelled));

        // The future drops at the cancel at the end of `block_on`.
        drop(spawn_local(reenters(false, None)).unwrap());
    });

    // The future drops when the owner fails.
    let failed = catch_unwind(AssertUnwindSafe(|| {
        owner.block_on(async {
            drop(spawn_local(reenters(false, None)).unwrap());
            yield_once().await;
            panic!("the root panics");
        })
    }));
    assert!(failed.is_err());
    assert_eq!(
        *spawns.borrow(),
        [Ok(()), Ok(()), Err(Reason::Closed), Err(Reason::Closed)]
    );
}

/// A pending future whose drop panics.
struct PanicsOnDrop;

impl Future for PanicsOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl Drop for PanicsOnDrop {
    fn drop(&mut self) {
        panic!("the drop of the future panics");
    }
}

#[test]
fn a_panic_in_the_drop_of_one_task_does_not_skip_the_others() {
    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 4).unwrap();
    let free = budget.available();

    // At the end of `block_on`, the owner cancels each live task.
    let tasks = owner.block_on(async {
        let tasks = [
            spawn_local(pending::<()>()).unwrap(),
            spawn_local(PanicsOnDrop).unwrap(),
            spawn_local(pending::<()>()).unwrap(),
        ];
        yield_once().await;
        tasks
    });
    let [first, panicked, last] = tasks.map(JoinHandle::join_blocking);
    assert!(matches!(first, Outcome::Cancelled));
    assert!(matches!(panicked, Outcome::Panicked(_)));
    assert!(matches!(last, Outcome::Cancelled));
    drop(panicked);
    assert_eq!(budget.available(), free);

    // After a panic of the root, the owner drops each live task with no outcome.
    let kept = RefCell::new(Vec::new());
    let failed = catch_unwind(AssertUnwindSafe(|| {
        owner.block_on(async {
            kept.borrow_mut().extend([
                spawn_local(pending::<()>()).unwrap(),
                spawn_local(PanicsOnDrop).unwrap(),
                spawn_local(pending::<()>()).unwrap(),
            ]);
            yield_once().await;
            panic!("the root panics");
        })
    }));
    assert!(failed.is_err());
    for task in kept.take() {
        assert!(matches!(task.join_blocking(), Outcome::OwnerFailed(OwnerFailure)));
    }
    assert_eq!(budget.available(), free);
}

#[test]
fn the_debug_of_a_panic_holds_no_payload() {
    let budget = budget();
    let mut owner = LocalOwner::new(&budget, 1).unwrap();
    let outcome = owner.block_on(async { spawn_local(async { panic!("secret 7f3a") }).unwrap().join().await });
    let debug = format!("{outcome:?}");
    assert!(matches!(outcome, Outcome::Panicked(_)));
    assert!(!debug.contains("7f3a"), "{debug}");
}
