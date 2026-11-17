//! The blocking service as an application or another library uses it: each submission comes from
//! outside the crate. The waits use no scheduler, as in driver-only use.
#![cfg(feature = "std")]

use core::assert_matches;
use core::future::Future;
use core::num::NonZeroU64;
use core::pin::pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::task::Wake;
use std::thread;
use std::time::Instant;

use rsasync::blocking::{Config, Failed, Job, Reason, Refused, Service};
use rsasync::capacity::Budget;

const STACK: usize = 256 << 10;

/// The longest wait of a test for a job that returns with no waiter to show it.
const RETURN_WAIT: Duration = Duration::from_secs(10);

fn budget() -> Budget {
    Budget::new(NonZeroU64::new(64 << 20).unwrap())
}

/// A gate that jobs wait at until the test opens it.
#[derive(Clone, Default)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn wait(&self) {
        let (open, changed) = &*self.0;
        drop(changed.wait_while(open.lock().unwrap(), |open| !*open).unwrap());
    }

    fn open(&self) {
        let (open, changed) = &*self.0;
        *open.lock().unwrap() = true;
        changed.notify_all();
    }
}

/// Polls `future` on this thread until it is ready, as a caller-owned loop with no scheduler does.
fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(thread::Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

/// A job type of another library, whose input a refusal gives back.
struct Lookup(String);

impl Job for Lookup {
    type Output = usize;

    fn run(self) -> usize {
        self.0.len()
    }
}

#[test]
fn each_refusal_returns_the_job_with_its_reason() {
    let budget = budget();
    let service = Service::new(&budget, Config::new().threads(1).jobs(1).stack(STACK)).unwrap();
    let submitter = service.submitter();

    let rest = budget.reserve(budget.available()).unwrap();
    let Refused { reason, job } = submitter.submit(Lookup("budget.example".into())).unwrap_err();
    assert_matches!(reason, Reason::Budget(_));
    assert_eq!(job.0, "budget.example");
    drop(rest);

    let gate = Gate::default();
    let mut held = submitter
        .submit({
            let gate = gate.clone();
            move || gate.wait()
        })
        .unwrap();
    let Refused { reason, job } = submitter.submit(Lookup("full.example".into())).unwrap_err();
    assert_eq!(reason, Reason::Full);
    assert_eq!(job.0, "full.example");
    gate.open();
    assert_eq!(held.wait_blocking(), Ok(()));

    service.teardown();
    let Refused { reason, job } = submitter.submit(Lookup("ended.example".into())).unwrap_err();
    assert_eq!(reason, Reason::Ended);
    assert_eq!(job.0, "ended.example");
}

/// Sends on its channel when it drops.
struct Probe(mpsc::Sender<()>);

impl Drop for Probe {
    fn drop(&mut self) {
        self.0.send(()).unwrap();
    }
}

#[test]
fn an_abandoned_job_holds_its_slot_until_it_returns_and_then_its_result_drops() {
    let budget = budget();
    // More threads than job slots: only the bound keeps a free thread from a third job.
    let service = Service::new(&budget, Config::new().threads(4).jobs(2).stack(STACK)).unwrap();
    let submitter = service.submitter();
    let gate = Gate::default();
    let (dropped, results) = mpsc::channel();
    let job = || {
        let (gate, dropped) = (gate.clone(), dropped.clone());
        move || {
            gate.wait();
            Probe(dropped)
        }
    };

    // Each waiter drops at once. The jobs still hold their slots, so only two are admitted.
    let admitted = (0..8).filter(|_| submitter.submit(job()).is_ok()).count();
    assert_eq!(admitted, 2);
    gate.open();
    // The abandoned jobs run until they return, and their results then drop.
    results.recv().unwrap();
    results.recv().unwrap();

    // The slots ended when the jobs returned, before their results dropped, so the service admits
    // a job again at once.
    let mut waiter = submitter.submit(|| 7).unwrap();
    assert_eq!(waiter.wait_blocking(), Ok(7));
    service.teardown();
}

#[test]
fn teardown_waits_for_the_running_job_drops_the_queue_and_joins_each_thread() {
    let budget = budget();
    let service = Service::new(&budget, Config::new().threads(1).jobs(3).stack(STACK)).unwrap();
    let submitter = service.submitter();
    let gate = Gate::default();
    let returned = Arc::new(AtomicBool::new(false));
    let (started, start) = mpsc::channel();
    let mut first = submitter
        .submit({
            let (gate, returned) = (gate.clone(), Arc::clone(&returned));
            move || {
                started.send(()).unwrap();
                gate.wait();
                returned.store(true, Ordering::SeqCst);
                1
            }
        })
        .unwrap();
    let mut queued = [submitter.submit(|| 2).unwrap(), submitter.submit(|| 3).unwrap()];
    start.recv().unwrap();

    // The gate opens only after the teardown stopped the admission.
    let opener = thread::spawn(move || {
        loop {
            match submitter.submit(|| 0) {
                Err(refused) if refused.reason == Reason::Ended => break,
                Err(refused) => assert_eq!(refused.reason, Reason::Full),
                Ok(_) => panic!("each job slot is occupied"),
            }
            thread::yield_now();
        }
        gate.open();
    });
    let report = service.teardown();
    assert!(returned.load(Ordering::SeqCst));
    assert_eq!((report.running(), report.dropped()), (1, 2));
    opener.join().unwrap();

    assert_eq!(first.wait_blocking(), Ok(1));
    for waiter in &mut queued {
        assert_eq!(waiter.wait_blocking(), Err(Failed::Ended));
    }
    drop((first, queued));
    // Each charge ended, also the stack of the joined thread.
    assert_eq!(budget.available(), budget.bound());
}

#[test]
fn a_drop_with_no_teardown_keeps_the_charge_of_a_running_thread_until_it_ends() {
    let budget = budget();
    let service = Service::new(&budget, Config::new().threads(1).jobs(1).stack(STACK)).unwrap();
    let gate = Gate::default();
    let (started, start) = mpsc::channel();
    let mut waiter = service
        .submitter()
        .submit({
            let gate = gate.clone();
            move || {
                started.send(()).unwrap();
                gate.wait();
                5
            }
        })
        .unwrap();
    start.recv().unwrap();

    drop(service);
    // The thread still runs its job, so its stack stays charged.
    assert!(budget.bound() - budget.available() >= STACK as u64);
    gate.open();
    assert_eq!(waiter.wait_blocking(), Ok(5));
    drop(waiter);
    // The thread ends after its job, and the charge of its stack then ends.
    while budget.available() != budget.bound() {
        thread::yield_now();
    }
}

#[test]
fn a_panicking_job_gives_panicked_and_its_thread_runs_the_next_job() {
    let budget = budget();
    let service = Service::new(&budget, Config::new().threads(1).jobs(1).stack(STACK)).unwrap();
    let submitter = service.submitter();

    let mut failed = submitter.submit(|| -> u32 { panic!("the job fails") }).unwrap();
    assert_eq!(block_on(failed.wait()), Err(Failed::Panicked));
    drop(failed);
    // The waiter has the result, so the one job slot is free, and the one thread runs the next
    // job.
    let mut next = submitter.submit(|| 7).unwrap();
    assert_eq!(block_on(next.wait()), Ok(7));
    service.teardown();
}

/// Panics when it drops.
struct Bomb;

impl Drop for Bomb {
    fn drop(&mut self) {
        panic!("the drop of the result fails");
    }
}

#[test]
fn a_panic_in_the_drop_of_an_abandoned_result_does_not_end_the_thread() {
    let budget = budget();
    let service = Service::new(&budget, Config::new().threads(1).jobs(1).stack(STACK)).unwrap();
    let submitter = service.submitter();
    let gate = Gate::default();

    // The waiter drops before the job returns, so the result drops on the one thread.
    drop(
        submitter
            .submit({
                let gate = gate.clone();
                move || {
                    gate.wait();
                    Bomb
                }
            })
            .unwrap(),
    );
    gate.open();
    // The one thread still runs the next job. No waiter shows when the job returns and its slot
    // ends, so the test submits until the service admits the job, at most for `RETURN_WAIT`.
    let deadline = Instant::now() + RETURN_WAIT;
    let mut next = loop {
        match submitter.submit(|| 7) {
            Ok(waiter) => break waiter,
            Err(refused) => assert_eq!(refused.reason, Reason::Full),
        }
        assert!(
            Instant::now() < deadline,
            "the job with no waiter did not return in {RETURN_WAIT:?}"
        );
        thread::yield_now();
    };
    assert_eq!(next.wait_blocking(), Ok(7));
    service.teardown();
}
