# Scheduling

A local owner runs its tasks in turns (see [Tasks](tasks.md)). This page tells you what one turn
does, how the poll budget keeps one busy task from the others, and what the owner cannot stop.
rsasync is in development, and this behavior is not in a published release yet.

## The turn of an owner

`block_on` runs turns until the root future gives its output. Each turn does a bounded amount of
work, and then the next turn starts:

1. The owner polls the root future if a wake came for it.
2. It takes the tasks that other threads woke. The list holds at most one entry for each task slot.
3. It polls each task that was ready when the turn started, once, in the order of their wakes. A
   task that a poll wakes again waits for the next turn.
4. If the owner has a timer storage (see [Timers](timers.md#timers-of-a-task)), it fires at most 64
   due timers. When more are due, they fire in the turns that follow, and the owner does not wait.
5. On a home with an io_uring driver, it turns the driver: one bounded `Driver::turn`. The turn
   enters the kernel when the kernel holds deferred work for the ring (the task-run flag), also
   when nothing is to submit and tasks are ready. Thus a receive whose data came completes while
   other tasks keep the owner busy.
6. When no task is ready, no timer is due, and the driver has no work, the owner waits: it parks
   its thread, or it waits in its ring. A wake, a completion, or a request of another thread ends
   the wait. With a waiting timer, the wait ends at the latest at the start of the tick of that
   timer, and the wake delay of the operating system comes on top of it.

A turn does not wait for more work to fill a batch. A short burst of data, or a close of the peer,
completes its operation in the turn that takes its completion.

## Poll budget

Each poll of a task, and of the root future, has a budget of runtime leaves. A runtime leaf is a
future of rsasync that waits for the runtime:

- an operation of the driver, such as a receive, a send, or the wait for a child process;
- a send or a receive of a channel, also the one-shot receive of a job of the blocking service;
- a permit acquisition of `rsasync::acquire`;
- a timer of `rsasync::timer`.

Each leaf that becomes ready in the poll spends one unit. A leaf that gives `Pending` spends
nothing. When no unit is left, the next leaf wakes its task and gives `Pending` one time, with no
change of its state. The owner then polls the other ready tasks before that task, and the next
poll has a full budget again. Thus a task that loops on operations that are always ready, for
example a receive from a channel that always has values, yields its thread.

The budget is `rsasync::task::DEFAULT_POLL_BUDGET`, 128 leaves. `LocalOwner::set_poll_budget`
sets another value for the later polls of the owner.

## What the owner cannot stop

The owner cannot preempt a poll. The budget is not a guarantee of fairness:

- Code that uses no runtime leaf runs until its poll returns. A loop that computes, or that waits
  on a future of another library, keeps the thread until it gives `Pending` or ends.
- A blocking call blocks the owner and each of its tasks. Run blocking host work on the bounded
  blocking service (see [Blocking service](blocking-service.md)).

A long computation that must share the thread gives `Pending` itself from time to time: it wakes
its own waker, and then returns `Pending`.

## The measured value

The default comes from a measurement, `benches/poll_budget.rs`, on two EC2 hosts with Ubuntu 26.04
(`7.0.0-1016-aws`): c8i.4xlarge (x86_64) and c8g.4xlarge (aarch64), on 2026-11-15. For each budget,
one task loops on a send and a receive of one SPSC channel, both always ready. The measurement
gives two values:

- The cost of each operation of the loop, alone on the owner: a smaller budget yields more often.
- The delay from a send on another thread, every 100 µs, to the poll of the task that receives it,
  while the loop runs: a larger budget keeps the thread longer.

| Budget | x86_64 ns per operation | x86_64 delay p50 / p99 (µs) | aarch64 ns per operation | aarch64 delay p50 / p99 (µs) |
| --- | --- | --- | --- | --- |
| 16 | 11.97 | 0.6 / 0.8 | 15.85 | 0.7 / 0.9 |
| 32 | 11.24 | 0.6 / 0.9 | 14.78 | 0.8 / 1.1 |
| 64 | 10.86 | 0.8 / 1.3 | 14.19 | 1.0 / 3.6 |
| 128 | 10.65 | 1.2 / 1.9 | 13.89 | 1.4 / 2.5 |
| 256 | 10.56 | 1.8 / 3.3 | 13.75 | 2.4 / 4.4 |
| 512 | 10.52 | 3.3 / 6.1 | 13.67 | 4.2 / 8.1 |
| 1024 | 10.49 | 5.7 / 12.2 | 13.64 | 8.0 / 15.4 |
| No limit | 10.47 | none: the receiver never runs | 13.64 | none: the receiver never runs |

The rule selects the smallest budget whose cost is within 2% of no limit. On both hosts that is
128: 1.7% above no limit on x86_64 and 1.8% on aarch64, with a p99 delay of 1.9 µs and 2.5 µs. A
budget of 64 costs 3.7% and 4.0%. A leaf that does more work for each poll than this channel makes
the yield a smaller part of its cost. To run the measurement on your host, run
`cargo bench --locked -p rsasync --bench poll_budget` from the repository root on a quiet host.
