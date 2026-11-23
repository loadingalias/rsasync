# Tasks

`rsasync::task` runs local tasks: futures that one thread polls, and drops, only on itself. A task
can hold `!Send` state, such as an `Rc`. rsasync is in development, and the tasks on this page are
not in a published release yet. They need the `local` feature, which is on by default and turns on
the `std` feature.

A `LocalOwner` runs tasks with no driver and no other thread. On Linux, the
[runtime](runtime.md) gives each of its workers, and each call of its `block_on`, an owner with a
driver, timers, and a generator. `task::generator` gives a task a generator of its own, and
`Handle::spawn_on` spawns on a worker from any thread. A runtime with movable workers also runs
movable tasks: `spawn_movable` gives a `Send` future to a pool of workers, and an idle worker of the
pool can steal it.

## Where a task runs

Where a task runs and whether its future is `Send` are two separate facts. A local task stays on
its worker, also when its future is `Send`. Only a movable task moves between workers.

| Spawn | Where the task runs | The future | The output |
| --- | --- | --- | --- |
| `spawn_local(future)` | The owner of this thread, for the life of the task | Any, also `!Send` | Any |
| `handle.spawn_on(worker, factory)` | Local worker `worker`, for the life of the task. The `Send` factory makes the future there | Any, also `!Send` | `Send` |
| `spawn_movable(future)`, `handle.spawn_movable(future)` | Each movable worker can poll it, one at a time. An idle worker can steal it | `Send` | `Send` |

Each spawn gives a `JoinHandle` with the same [outcomes](#join-and-cancel), and each refusal gives
the future back. [Spawn movable work](runtime.md#spawn-movable-work) gives the movable spawn, its
refusals, and how the pool runs its tasks.

## Run tasks on a local owner

1. Make a `LocalOwner` with `LocalOwner::new(&budget, slots)`. `slots` is the number of tasks that
   can be live at one time, from 1 to 2^16. The owner charges its tables to the `Budget` (see
   [Capacity](capacity.md)), and it charges each task at its spawn.
2. Call `owner.block_on(root)`. The calling thread is the owner while the call runs: it polls the
   root future and the tasks, and it parks when no task is ready.
3. In the root future or in a task, call `spawn_local(future)`. It gives a `JoinHandle`.
4. When the root future gives its output, `block_on` cancels each task that is still live. It
   returns after each of them ended.

The owner stays on the thread that made it. `LocalOwner::new` refuses with `Refused::Zero` for zero
slots, `Refused::TooLarge` for more than 2^16, `Refused::Budget` with the `Exhausted` value of the
budget, or `Refused::Allocation`.

## A spawn that has no capacity

A spawn claims a free slot and charges the memory of the task before it allocates. When it cannot,
it allocates nothing, and `SpawnRefused` gives the future back with its reason. The spawn then
allocates the memory of the task before the future moves. When the allocator refuses, the spawn
frees what it allocated, gives the slot and the charge back, and gives the future back with
`Allocation`:

| `Reason` | Cause | What to do |
| --- | --- | --- |
| `NoSlot` | Each slot of the owner holds a live task | Join or cancel a task, or use `spawn_local_waiting` |
| `Budget(Exhausted)` | The budget has fewer free units than the task | Free units, or spawn a smaller future |
| `Allocation` | The allocator did not give the memory of the task | Spawn again when memory is free, or run the future in place |
| `Closed` | `block_on` is at its end and cancels its tasks, or it failed and drops them | Do the work in the root future, or before the end |
| `NoOwner` | No `block_on` runs on this thread | Spawn from the root future or from a task |
| `NoWorker` | `Handle::spawn_on` names no worker of the runtime | Use an index below `handle.workers()` |

`SpawnRefused::into_future` gives the future back, so the caller can run it in place, keep it, or
drop it.

`spawn_local_waiting(future)` gives a future that waits for a free slot, and then spawns. The
waiting spawns get the free slots first in, first out. It does not wait for budget units: for each
other reason, it gives the same `SpawnRefused`.

This example fills the one slot of an owner, and gets the refusal of a second spawn:

```rust
use core::num::NonZeroU64;

use rsasync::capacity::Budget;
use rsasync::task::{LocalOwner, Outcome, Reason, spawn_local};

let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
let mut owner = LocalOwner::new(&budget, 1)?;

let sum = owner.block_on(async {
    let task = spawn_local(async { 2 + 2 }).expect("the owner has a free slot");
    // The one slot is in use: the refusal gives the future back, with its reason.
    let refused = spawn_local(async { 3 }).unwrap_err();
    assert_eq!(refused.reason(), Reason::NoSlot);
    let _future = refused.into_future();
    match task.join().await {
        Outcome::Completed(sum) => sum,
        Outcome::Cancelled | Outcome::Panicked(_) | Outcome::OwnerFailed(_) => 0,
    }
});
assert_eq!(sum, 4);
```

## Join and cancel

A `JoinHandle` observes the outcome of its task:

- `join()` gives a future of the `Outcome`. `join_blocking()` waits on the calling thread: use it on
  another thread, never on the thread of the owner, which then cannot poll the task.
- A drop of the join handle does not cancel the task. The task runs on, and the owner drops its
  output.
- The outcome of a task stays in the channel of its join handle, charged to the budget, until the
  join takes it or the handle drops.
- A join handle can move to another thread if the output can.

`JoinHandle::cancel_handle` gives a `CancelHandle`, which works from each thread. Its `cancel`
requests the cancel of the task. The owner drops the future at a poll boundary, never during a
poll: at the start of the next poll, or after the poll that runs now. A cancel after the task ended
does nothing.

| `Outcome` | When |
| --- | --- |
| `Completed(output)` | The future gave its output |
| `Cancelled` | A cancel was requested, or `block_on` ended: the owner dropped the future at a poll boundary |
| `Panicked(panic)` | A poll of the future, or the drop of the future, panicked: the owner dropped the future. `Panic::into_payload` gives the payload |
| `OwnerFailed(OwnerFailure)` | The owner dropped the future with no outcome, because `block_on` ended with a panic |

When the root future panics, each live task ends with `OwnerFailed`, and the panic continues out of
`block_on`. The owner can then run `block_on` again.

## Panics

The owner contains each panic of the state of a task: a poll of its future, and the drop of its
future or of an output that no join takes. That task ends with `Panicked`, and the other tasks
continue. A panic in the drop of one task does not stop the drop of the others, also after a panic
of the root, and each charge returns to the budget.

- The owner drops each panic payload that no join takes. If that drop panics, the owner drops the
  new payload in the same way. If that drop panics too, its payload leaks: one allocation.
- A panic of the root future, or of a waker that the owner calls, ends `block_on` with that panic.
  Each live task then ends with `OwnerFailed`.
- With `panic = "abort"`, each panic ends the process, and no task ends with `Panicked`.

The destructor of a task can use the owner again: it can spawn, wake, and cancel. At the end of
`block_on`, and after a panic of the root, a spawn gives `Reason::Closed` and its future back.
[Errors](errors.md) gives each error of a task and its recovery.

## Turns and the poll budget

The owner polls its tasks in bounded turns, and each poll of a task has a budget of runtime leaves:
the futures of rsasync that wait for the runtime, such as a channel receive. A task that loops on
leaves that are always ready yields its thread at the budget, so the other tasks run.
[Scheduling](scheduling.md) gives the turn, the budget and its measured default, and what the owner
cannot stop.

## Wakes

The waker of a task works from each thread. A wake on another thread schedules the task on its
owner, and unparks the owner thread if it parks. A wake after the task ended does nothing, also when
a later task uses the same slot.
