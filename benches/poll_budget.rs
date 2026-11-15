//! The measurement of the poll budget of a local owner (`rsasync::task::DEFAULT_POLL_BUDGET`).
//!
//! For each budget, one task loops on two channel operations that are always ready: a send and a
//! receive on one SPSC channel. The measurement gives two values:
//!
//! - The cost of each operation of the loop, alone on the owner. A smaller budget yields more
//!   often, and each yield costs a wake and a turn of the owner.
//! - The delay from a send on another thread to the poll of the task that receives it, while the
//!   loop runs. A larger budget lets the loop keep the thread for longer.
//!
//! Run it on a quiet host: `cargo bench --locked --bench poll_budget`.

#![expect(clippy::print_stdout, reason = "a measurement prints its table")]

use core::cell::Cell;
use core::num::{NonZeroU32, NonZeroU64};
use core::time::Duration;
use std::rc::Rc;
use std::thread;
use std::time::Instant;

use rsasync::capacity::Budget;
use rsasync::channel::{mpsc, spsc};
use rsasync::task::{LocalOwner, spawn_local};

/// The budgets to compare. `u32::MAX` is no limit for this loop.
const BUDGETS: [u32; 8] = [16, 32, 64, 128, 256, 512, 1024, u32::MAX];
/// The operations of one cost sample.
const OPERATIONS: u32 = 4_000_000;
/// The cost samples of each budget. The table gives their median.
const SAMPLES: usize = 7;
/// The delay samples of each budget.
const DELAYS: usize = 4_000;
/// The time between two sends of the other thread.
const PERIOD: Duration = Duration::from_micros(100);

fn owner(budget: &Budget, poll_budget: u32) -> LocalOwner {
    let mut owner = LocalOwner::new(budget, 4).expect("the budget has room for the owner");
    owner.set_poll_budget(NonZeroU32::new(poll_budget).expect("a budget is not zero"));
    owner
}

/// The loop: a send and a receive on one channel, until `stop` or `operations`. Gives the
/// operations that it did.
async fn spin(budget: Budget, stop: Rc<Cell<bool>>, operations: u32) -> u32 {
    let (mut sender, mut receiver) = spsc::channel::<u32>(&budget, 1).expect("the budget has room");
    let mut done = 0;
    while done < operations && !stop.get() {
        sender.send(done).await.expect("the receiver is alive");
        receiver.recv().await.expect("the sender is alive");
        done += 2;
    }
    done
}

/// The nanoseconds of each operation of the loop, alone on an owner with `poll_budget`.
fn cost(budget: &Budget, poll_budget: u32) -> f64 {
    let mut owner = owner(budget, poll_budget);
    let start = Instant::now();
    let done = owner.block_on(spin(budget.clone(), Rc::new(Cell::new(false)), OPERATIONS));
    let elapsed = start.elapsed();
    assert_eq!(done, OPERATIONS);
    elapsed.as_nanos() as f64 / f64::from(OPERATIONS)
}

/// The delays from a send on another thread to the poll of the receiver, while the loop runs on
/// the owner, in microseconds and in increasing order.
fn delays(budget: &Budget, poll_budget: u32) -> Vec<f64> {
    let mut owner = owner(budget, poll_budget);
    let (mut sender, mut receiver) = mpsc::channel::<Instant>(budget, 1, 1).expect("the budget has room");
    let other = thread::spawn(move || {
        for _ in 0..DELAYS {
            thread::sleep(PERIOD);
            if sender.send_blocking(Instant::now()).is_err() {
                break;
            }
        }
    });
    let mut delays = owner.block_on({
        let budget = budget.clone();
        async move {
            let stop = Rc::new(Cell::new(false));
            let looper = spawn_local(spin(budget, Rc::clone(&stop), u32::MAX)).expect("the owner has a slot");
            let mut delays = Vec::with_capacity(DELAYS);
            while let Ok(sent) = receiver.recv().await {
                delays.push(sent.elapsed().as_secs_f64() * 1e6);
            }
            stop.set(true);
            drop(looper.join().await);
            delays
        }
    });
    other.join().expect("the sending thread does not panic");
    delays.sort_by(f64::total_cmp);
    delays
}

/// The value at `per_mille` of `sorted`, which is not empty.
fn percentile(sorted: &[f64], per_mille: usize) -> f64 {
    sorted[(sorted.len() - 1) * per_mille / 1000]
}

fn main() {
    let budget = Budget::new(NonZeroU64::new(1 << 24).expect("not zero"));
    println!("| Budget | ns per operation (median of {SAMPLES}) | Delay p50 (µs) | Delay p99 (µs) | Delay max (µs) |");
    println!("| --- | --- | --- | --- | --- |");
    for poll_budget in BUDGETS {
        let mut costs: Vec<f64> = (0..SAMPLES).map(|_| cost(&budget, poll_budget)).collect();
        costs.sort_by(f64::total_cmp);
        if poll_budget == u32::MAX {
            // With no limit, the loop keeps the thread, and the receiver never runs.
            println!("| no limit | {:.2} | - | - | - |", costs[SAMPLES / 2]);
            continue;
        }
        let delays = delays(&budget, poll_budget);
        println!(
            "| {poll_budget} | {:.2} | {:.1} | {:.1} | {:.1} |",
            costs[SAMPLES / 2],
            percentile(&delays, 500),
            percentile(&delays, 990),
            percentile(&delays, 1000),
        );
    }
}
