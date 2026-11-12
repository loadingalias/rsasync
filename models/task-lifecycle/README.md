# Task admission, wakes, and shutdown

This is the design model of task R100. It checks the rules that the shared wait handshake (R102)
and task scheduling (R301, R302, R305, R401, R404) build on, before that code exists. The sources
are in the local design `docs/specs/dual-async-runtime.md`: Future and Wake Contract, Local and
Movable Work, Startup, Shutdown, and Failures, and Capacity and Progress. The Design Models section
of `docs/specs/family-policy.md` sets the acceptance.

**Status (2026-11-11): all 27 profiles pass at the current inputs.** The declared profile and
the three raise-by-one profiles complete with no violation. All 14 known-bad variants and 9
witnesses give their required violations. See [Evidence record](#evidence-record).

## The promise and a concrete failure

The promise: a spawn reserves a task slot before it admits the task, and no task is admitted after
the shutdown request closes ingress. One owner polls a task at a time. A local task is polled,
queued, and dropped only on its home. A wake from any thread, also from a signal handler that owns
no worker, reaches its task, and a wake or a cancel of an earlier generation changes no task.
Cancellation drops a future at a poll boundary. block_on cancels the tasks that remain on its owner
and waits for them. Shutdown ends each task, and a worker exits only after the last task ends.

Four concrete failures:

- A future checks its event, and then registers its waker. A publish between the two finds no
  waker, so the task waits forever with its event set (`NoRecheck_bad`).
- A wake comes while the task is polled. The poll end does not schedule the task again, so the
  task waits forever (`LoseCoalesced_bad`).
- A waker of a task that ended runs after its slot holds a new task, and it schedules the new task
  (`StaleGen_bad`).
- After the shutdown request, a worker with empty queues exits while a task that it would poll is
  still idle (`EarlyExit_bad`).

## Run it

```sh
cargo xtask model task-lifecycle                                   # every profile
cargo xtask model task-lifecycle/TaskLifecycle.cfg                 # one profile
```

[Check the design models](../../docs/content/design-models.md) tells how to install the checkers,
where the output goes, and what a failure means.

## Files and profiles

| File | Role |
| --- | --- |
| `TaskLifecycle.tla` | The module: the state, the actions, the properties, and the reachability targets |
| `TaskLifecycle.cfg` | The declared profile: every property |
| `TaskLifecycle.Workers3.cfg`, `Slots3`, `Gens3` | One argued bound raised by one: three workers, three task slots, or three generations of each slot |
| `TaskLifecycle.<Name>_bad.cfg` | The known-bad variants: each must violate its one property |
| `TaskLifecycle.<Name>_reach.cfg` | The reachability witnesses: each must violate its `Unreached<Name>` property, which shows a trace to the target |

## What the model contains

| Part | Model |
| --- | --- |
| Workers | `Workers = {w1, w2}`. Each worker polls local and movable tasks. `SH`, the first worker, is the signal home: its driver watches the wake descriptor of the signal handler, and it is the one worker that parks (reduction 2) |
| block_on | The owner `B`, the thread that calls block_on. `bo` is `off`, `on` (the root runs and can spawn local tasks on `B`), or `ending` (the root completed). A call can start again after it returned. Each block_on call starts and ends while the runtime runs |
| Slots | `NSlots = 2` task slots, each with `MaxGen = 2` generations. `ss[s]` is `free`, `rsv` (reserved), `idle`, `sched` (in exactly one queue), or `run`. `gen[s]` names the task in slot `s`; `MaxGen + 1` retires the slot |
| Tasks | `kind[s]` is `local` or `movable`. `home[s]` is the home of a local task, or the location of a movable task: the worker that last polled it. A movable spawn reserves a slot and then admits the task at a worker. A local spawn runs on its owner |
| Queues | `rq[o]`, the run queue of each owner. `rr`, the remote-ready list of the signal home. A push that finds `rr` empty writes the descriptor `sig` |
| Polls | `cur[o]`, the task that `o` polls. A poll can return Ready (`Finish`), register its waker and recheck its event (`Register`), return Pending with its registration or after a wake during the poll (`Keep`), or wake itself and return Pending when its poll budget is spent (`Yield`) |
| Events | `ev[s]`, the event of the task in slot `s`, and `reg[s]`, its registered waker. A publish from any thread sets the event, takes the waker, and wakes it. `nt[s]` records a wake during a poll; `cx[s]` records a cancel request |
| Signal | The handler sets `sf` and writes the descriptor of the signal home. Between polls, the signal home takes the flag and publishes the event of the task that waits for the signal |
| Shutdown | `ing` is `open` or `closed`. After closure, a reservation is refused at its admission, and each task gets its cancel (`Doom`). A worker exits after the last task ended (`wk`) |

## Physical and design assumptions

The model gives fairness only to a step that the design promises to take, or to a physical event.

| Assumption | Source | Fairness in the model, and why |
| --- | --- | --- |
| Each poll returns: a future returns from its poll or reaches a cooperative checkpoint | The design: "User code must return from a poll or reach a cooperative checkpoint before the runtime can schedule it again" (Future and Wake Contract). This is the stated condition of the progress properties | Weak fairness on `PollStep(o)` |
| A running owner polls the tasks in its queues in order, and it harvests its remote-ready list at each turn | The design: "Every drive turn bounds task polls, completion harvest, timers, wakes, cancellation, and maintenance" | Weak fairness on `PollStart(o)`. The run queue is first in, first out |
| A readable descriptor wakes its parked thread | eventfd(2): a read blocks until the counter is nonzero; io_uring and the readiness backends report the readable descriptor | Weak fairness on `Unpark` |
| Each drive turn of the signal home takes the signal flag | The design: the home's native signal; each turn handles the descriptor. Polls between turns disable the step | Strong fairness on `SigHandle` |
| The grace timer fires, and the cancellation of each task follows; the end of block_on cancels each task that remains on its owner | The design: "Grace expiry escalates task cancellation" (Startup, Shutdown, and Failures); "After the root completes, block_on cancels each task that is still on that owner" | Weak fairness on `Doom(s)` for each slot |
| A worker exits, and block_on returns, when no task remains | The design: "A worker exits after the last task ends"; block_on "returns" after terminal release | Weak fairness on `Exit(w)` and `BlockOnEnd` |
| Spawns, publishes, signals, old wakers and cancel handles, the root, the shutdown request, and steals are free | The design gives the caller no obligation; a steal is an optimization | None |

Each fairness condition names the failure that it could hide, and a known-bad variant with that
failure still fails under it:

| Fairness | Failure that it could hide | Known-bad variant that still fails |
| --- | --- | --- |
| `PollStep(o)` | A wake that is lost while a poll never returns | `NoRecheck_bad`, `LoseCoalesced_bad` (`OwedWakeDelivered`) |
| `PollStart(o)` | A queued task that its owner never polls | `NoRecheck_bad`, `LoseCoalesced_bad` |
| `Unpark` | A parked worker with work and no readable descriptor | `ParkBeforeDrainLive_bad` (`OwedWakeDelivered`) |
| `SigHandle` | A signal flag that the home never takes | `ParkWithFlag_bad` (`OwedWakeDelivered`) |
| `Doom(s)` | A cancel that never schedules its task | `NoGraceWake_bad` (`ShutdownJoins`), `BlockOnNoCancel_bad` (`BlockOnReturns`) |
| `Exit(w)`, `BlockOnEnd` | A worker or a block_on call that waits for a task that never ends | `NoGraceWake_bad`, `BlockOnNoCancel_bad` |

## Reductions and their soundness

Each reduction removes states or steps that no property can tell apart, or adds behaviors. After
all of them, each known-bad variant still fails its own property (see the evidence record).

1. Each wake takes the remote path: it pushes its task to the queue of the task's home. A wake on
   the home thread pushes to the home's run queue at once; the model has that behavior as a push
   that the home harvests next. Known-bad: `DoublePush_bad`.
2. Only the signal home parks. A worker that does not park only takes steps sooner. The park
   protocol is the same for each worker, and the signal home checks it with both a remote push
   and the signal flag. Known-bad: `ParkBeforeDrain_bad`, `ParkWithFlag_bad`.
3. An owner that does not park (`B` and each worker other than the signal home) gets each remote
   push in its run queue. Its harvest at each turn only changes the order of a first-in,
   first-out queue, and no property reads the order. A steal can then take a task that the real
   victim has not harvested yet; this only adds behaviors.
4. The harvest of the remote-ready list and the start of the next poll are one step. A push
   between the two lands in the list for the next turn, which is the same as a push just after
   the step.
5. The exit notice and the exit of a worker are one step. When the notice is posted, ingress is
   closed and no task remains, so no spawn, steal, or wake can come between. Known-bad:
   `EarlyExit_bad`.
6. Grace expiry is no separate step. After the shutdown request, `Doom(s)` cancels each task, and
   each task can run until its own cancel, so the model has each drain and each order of the
   cancels. Known-bad: `NoGraceWake_bad`.
7. The end of a task and the release of its slot are one step: the join handle takes the output
   at once. A later release only keeps the slot taken, as a live task does, and delays its reuse.
8. The environment holds old wakers and cancel handles only. A current wake comes from an event
   (`Publish`, the signal) or from the poll budget (`Yield`); a current cancel comes from `Doom`,
   which is the same step as a cancel handle: a wake with the cancel bit. `Doom` runs while the
   runtime runs (at the end of block_on) and after the shutdown request, during polls and before
   slot reuse. Known-bad: `StaleGen_bad`.
9. A spawn takes the first free slot among the free slots of its generation. Two such slots are
   equal in every variable, so each other choice is the mirror image of a kept behavior under a
   permutation of the slots, and each property and fairness condition treats the slots the same.
   This is not TLC symmetry reduction, which can be unsound with liveness: the model keeps one
   choice from each set of images.
10. In the correct model, the registration of a waker and the recheck of the event are one step.
    A publish between the two takes the waker of a running task and sets its event; the recheck
    then sees the event, so the state is the same as after a publish just before the
    registration. The `noRecheck` variant keeps the check and the registration as two steps.
    Known-bad: `NoRecheck_bad`.
11. The signal home hosts no local task. A local task there behaves as a movable task located
    there that no worker steals. Known-bad: `StealLocal_bad` takes a local task from the other
    worker.
12. Each liveness known-bad variant runs at one slot and one generation. TLC checks liveness only
    after the complete search, so these bounds keep the run short. The consumer and the
    environment have no fairness, so a fair behavior at these bounds is also a fair behavior at
    the declared bounds, where the other slot stays free and no slot is reused.

The declared bounds are argued, not proven: two workers, two slots, and two generations. Each one
has a raise-by-one profile.

## Memory-order assumptions

Each action is one atomic step. The code proofs of R102, R301, R302, and R401 must show that the
production code gives each step below its atomicity:

| Step | Assumption |
| --- | --- |
| `Wake`, `WakeEffect`, `Doom` | The slot word holds the generation and the scheduling state (idle, scheduled, running, notified, cancelled). A wake or a cancel is one compare-and-swap on it (acquire-release), so the generation check and the state change are one step. Its success publishes the queue entry |
| `Take` (in `PollStart`, `Steal`) | The poll start is one read-modify-write from scheduled to running (acquire), so the poller sees the task state that the last poll wrote. A steal removes one entry from the victim's queue with one read-modify-write |
| `EndPending` | The poll end is one read-modify-write (release) that reads the notified and cancel bits in the same change to idle or scheduled |
| `Push`, `PollStart` | The remote-ready push is one read-modify-write on the list head (acquire-release) that also tells whether the list was empty. The pusher writes the descriptor after the push. The harvest is one swap of the head (acquire) |
| `Park`, `Unpark` | The descriptor is sticky: a write before the park makes the park return. The worker harvests its list, takes the signal flag, and then parks |
| `Register`, `PublishEffect` | The event and the waiter registration are one atomic word, or a sequentially consistent fence orders each side: the publisher stores the event before it takes the waker, and the waiter registers before it rechecks |
| `Admit`, `SpawnLocal`, `Refuse`, `Exit` | One atomic word holds the closed bit of ingress and the count of live tasks. An admission is one read-modify-write that fails when the bit is set. A worker exits after it reads a zero count with the bit set |
| `SigRaise`, `SigHandle` | The handler stores the flag (release) before it writes the descriptor. The home reads the descriptor, then swaps the flag to false (acquire) |

## Correspondence

Each action is one atomic step. The rule column names the design rule that the action implements.
The last column names the task whose production transition implements the action; that task adds
its transition to this table when the code exists, and a change to either side reopens the check.

| Action | Design rule | Atomicity | Production transition |
| --- | --- | --- | --- |
| `Reserve`, `Admit`, `Refuse` | Local and Movable Work: spawning reserves a task slot before it accepts the future; a refused spawn returns the future with a typed reason. Startup, Shutdown, and Failures: closing ingress refuses admission | The reservation; the admission or refusal | R401 (movable spawn) |
| `SpawnLocal` | Local and Movable Work: spawn on the current local owner keeps polling and destruction there | One step on the owner's thread | R301 |
| `Publish`, `Register`, `Keep`, `Yield`, `Finish` | Channels and Synchronization: a wait registers interest, rechecks the condition, and parks. Future and Wake Contract: the poll budget returns Pending once and schedules its task again | One poll step | R102 |
| `Wake`, `WakeEffect` | Future and Wake Contract: every waker is `Send + Sync`; a wake from another thread publishes the task to its home's remote-ready list; a wake after the task ended or its slot was reused has no effect | One compare-and-swap of the slot word | R302 |
| `PollStart`, `Take`, `EndPending` | Local and Movable Work: cancellation drops the future at a poll boundary, never during a poll. Future and Wake Contract: coalescing avoids duplicate scheduling without losing progress | One step at the owner | R301 (local), R401 (movable) |
| `Steal` | Local and Movable Work: movable work moves; a local task stays on its home | One step at the idle worker | R401 |
| `Park`, `Unpark`, `Push` | Future and Wake Contract: one unpark of the home for each batch, through its native signal | One step each | R302 |
| `SigRaise`, `SigHandle` | Future and Wake Contract: a thread that owns no ring writes an event descriptor that the home watches | One step each | R302 |
| `Shutdown`, `Doom`, `Exit` | Startup, Shutdown, and Failures: close ingress, drain, cancel at grace expiry, and no worker exits while a task of the runtime remains | One step each | R305 (local workers), R404 (both modes) |
| `BlockOnStart`, `RootDone`, `BlockOnEnd` | Startup, Shutdown, and Failures: block_on makes the calling thread a local owner; after the root completes, it cancels each task still on that owner and returns after they end | One step each | R301 |
| `Terminated` | None. The end state stutters, so deadlock checking stays on | One step | None |

## Properties

| Property | Acceptance text | Kind |
| --- | --- | --- |
| `OnePollOwner` | One poll owner: at most one owner polls a task, and a task in the run state has exactly that owner | Invariant |
| `HomeOnly` | Home-only local poll and drop: a local task is polled, queued, and dropped only on its home | Invariant |
| `BoundedReservations` | Bounded reservations: each queue entry is a reserved task in the scheduled state, and each such task has exactly one entry | Invariant |
| `ExitAfterWork` | No worker exits while a task of the runtime remains | Invariant |
| `BlockOnJoins` | block_on returns only after each task on its owner ended | Invariant |
| `ParkedHasSignal` | The parked signal home has a readable descriptor when its remote-ready list holds work or the signal flag is set | Invariant |
| `StaleWakeRejected` | Stale-wake rejection: a wake or a cancel of an earlier generation changes no task | Action property |
| `NoAdmissionAfterClose` | No admission after ingress closure | Action property |
| `StablePhases` | The side condition of the arguments below | Action property |
| `OwedWakeDelivered` | Eventual owed wake delivery: a task whose event happened, or whose cancel was requested, is polled; the signal flag is taken while the signal home runs | `[]<>`, equal to `Owed(s) ~> ~Owed(s)` and to `sf ~> (~sf \/ wk[SH] = "exit")` |
| `ShutdownJoins` | Shutdown: every worker exits | `[]<>`, equal to `ing = "closed" ~> Done` |
| `BlockOnReturns` | block_on returns | `[]<>`, equal to `bo = "ending" ~> bo = "off"` |
| Deadlock | No state deadlocks | TLC deadlock check, on |

`TypeOK` is also an invariant of each main profile. For each progress property, `P ~> Q` gives
`[]<>(~P \/ Q)`. The other direction needs an argument:

- `OwedWakeDelivered`: each target includes the negation of its source, so `[]<>(~P \/ Q)` is
  `[]<>Q`, and `[]<>Q` gives `P ~> Q` with no side condition.
- `ShutdownJoins`: `StablePhases` checks that closed ingress stays closed. So from a closed
  state, `~P` never holds again, and the form gives `Done`.
- `BlockOnReturns`: `StablePhases` checks that "ending" leaves only to "off". So a state that is
  not "ending" after an "ending" state is "off".

## Bounds, faults, and fairness

The declared bounds: two workers and the block_on owner, two task slots, two generations of each
slot, local and movable tasks, and capacity refusal. A larger bound needs only a change of the
constants in a `.cfg` file. The model has no count of wakes, polls, or parks: a task can be woken,
polled, and parked again forever, and repeated cycles use no finite event allowance.

The faults and free choices:

- A spawn, a publish, a signal, an old waker, an old cancel handle, the root of block_on, the
  shutdown request, and a steal can come at any time, or never.
- A future can return Ready, register and recheck, return Pending with its registration, or yield
  at any poll.
- A wake can come during a poll, while the task waits in a queue, or after its slot was reused.

The fairness is weak fairness for each step in [Physical and design assumptions](#physical-and-design-assumptions),
and strong fairness for `SigHandle`.

## Known-bad variants

Each variant changes one rule through the constant `Bug`, and its profile checks only its own
property. The safety variants run at the declared bounds; the liveness variants run at the bounds
of reduction 12.

| Profile | Changed rule | Must violate |
| --- | --- | --- |
| `NoRecheck_bad` | The future checks its event, then registers its waker with no recheck | `OwedWakeDelivered` |
| `LoseCoalesced_bad` | The poll end loses a wake that came during the poll | `OwedWakeDelivered` |
| `StaleGen_bad` | A wake does not check the generation | `StaleWakeRejected` |
| `EarlyExit_bad` | A worker exits after shutdown when its own queues are empty, while accepted work remains | `ExitAfterWork` |
| `DoublePush_bad` | A wake of a queued task pushes it again | `BoundedReservations` |
| `DoublePushPoll_bad` | The same rule change | `OnePollOwner` |
| `StealLocal_bad` | A steal takes a local task | `HomeOnly` |
| `AdmitClosed_bad` | Admission does not check ingress | `NoAdmissionAfterClose` |
| `ParkBeforeDrain_bad` | The signal home parks again before it harvests its remote-ready list | `ParkedHasSignal` |
| `ParkBeforeDrainLive_bad` | The same rule change | `OwedWakeDelivered` |
| `ParkWithFlag_bad` | The signal home parks with the signal flag set | `OwedWakeDelivered` |
| `NoGraceWake_bad` | A cancel after the shutdown request sets the cancel bit of an idle task and does not schedule it | `ShutdownJoins` |
| `BlockOnNoCancel_bad` | After the root completes, block_on waits for its tasks and does not cancel them | `BlockOnReturns` |
| `BlockOnNoWait_bad` | block_on returns before its cancelled tasks end | `BlockOnJoins` |

The counterexamples, as actions:

- `NoRecheck_bad`: `Reserve`; `Admit`; `PollStart`; `Check` (the event is not set); `Publish` (no
  waker is registered, so no wake); `RegisterLate`; `Keep`; then stuttering. The task waits with
  its event set.
- `LoseCoalesced_bad`: `BlockOnStart`; `SpawnLocal(b)`; `PollStart(b)`; `Publish` during the poll;
  `Yield` (the variant loses the wake of the spent poll budget); then stuttering. The task is idle
  with its event set.
- `StaleGen_bad`: `Reserve`; `Admit`; `PollStart`; `Finish` (the slot frees with generation 2);
  `Reserve`; `Admit`; `Wake` of generation 1, which schedules the new task.
- `EarlyExit_bad`: `Reserve`; `Admit` at `w2`; `Shutdown`; `Exit(w1)` while the task waits at `w2`.
- `DoublePushPoll_bad`: a yield leaves the task queued at `w2`; `Publish` pushes it again;
  `PollStart(w2)` takes one entry, and `w1` steals the other.
- Each current trace is retained in `target/model/task-lifecycle/<profile>/<run>/trace.json`.

## Reachability

| Profile | Target | Shows |
| --- | --- | --- |
| `CancelInPoll_reach` | A cancel request comes during a poll, and the poll end schedules the task for its drop at the next poll boundary | Cancellation during a poll |
| `Reuse_reach` | A reused slot holds a live task of the second generation while wakers of the first generation can run | Slot reuse; `StaleWakeRejected` is not vacuous |
| `ShutdownRemoteWake_reach` | After the shutdown request, a wake from another thread publishes a task to a queue | Shutdown with a remote wake |
| `Refusal_reach` | A spawn finds no free slot | Capacity refusal |
| `AdmitRefused_reach` | A reservation is refused at its admission after the shutdown request | `NoAdmissionAfterClose` is not vacuous |
| `Coalesced_reach` | A wake during a poll schedules the task again at the poll end | Wake coalescing |
| `Steal_reach` | An idle worker steals a movable task and polls it | Stealing |
| `SignalWake_reach` | The signal flag waits for the parked signal home, whose descriptor is readable, while a task waits for the signal | A wake from a context that owns no worker |
| `BlockOnCancel_reach` | The end of block_on cancels a task that still waits on its owner | block_on cancels its remaining tasks |

## Limits

The model does not cover these items. Each one is outside the task, or a later task owns it.

- A shutdown request while a block_on call runs. Each block_on call starts and ends while the
  runtime runs.
- The factory that initializes another local owner, typed messages to a state owner, and the
  waiting form of spawn.
- Join handles and their outcomes (completed, cancelled, panicked, owner failure), panics, and
  output that waits for its handle (reduction 7).
- A task with more than one event, and the replacement or the cancel of one waiter among several.
  R102 owns those cases with its bounded exploration.
- Operations, drivers, and I/O: the operation-ownership model checks them.
- Time: the grace period, the poll budget, and the bounds of a drive turn are steps with no
  duration.
- Mixed mode: each worker polls local and movable tasks. A worker of one mode has a subset of
  these behaviors.
- The model proves no Rust code. When R102, R301, R302, R305, R401, and R404 exist, the
  correspondence table gets their transitions, and a change to either side reopens this check.

## Evidence record

All 27 profiles pass for
`sha256:ac5eef2c6974d8e1a5ba6a97b16a860825b9890f8875dca96bf0bcbcb201a04a` (28 files) at Git
revision `7585a9d`. The checks use Temurin JRE 21.0.13+9, TLC 1.8.0+d8fb231de4e4, and
`cargo xtask model task-lifecycle/<profile>`. Each positive search finishes with no violation, an
empty queue, and exit 0.

| Profile | Distinct states | Depth | Seconds | Host |
| --- | --- | --- | --- | --- |
| `TaskLifecycle.cfg` | 115,571 | 25 | 58.82 | Mac |
| `TaskLifecycle.Workers3.cfg` | 253,307 | 26 | 117.18 | Mac |
| `TaskLifecycle.Gens3.cfg` | 275,368 | 31 | 216.02 | Mac |
| `TaskLifecycle.Slots3.cfg` | 9,177,449 | 36 | 1144.55 | EC2 |

The Mac is the shared development Mac (10 cores, 16 GiB, aarch64); other work ran on it, so its
durations are functional evidence, not qualified measurements. All 14 known-bad variants violate
their named property, and all 9 witnesses reach their named target, on the Mac. Their combined
checker time is 44.50 seconds. The logs, traces, and source copies are in
`target/model/task-lifecycle/<profile>/<run>/`; the Mac run of these inputs started at
`target/model/inputs/1794444955435331000-76045/`.

`TaskLifecycle.Slots3.cfg` ran one time on one EC2 c8g.4xlarge (16 vCPUs, 32 GiB, on demand), image
`ami-06b1a03fa3561aefb`, Ubuntu 26.04.1 LTS, Linux 7.0.0-1016-aws, aarch64, with a cap of 3600
seconds of checker time. The search took about 6 minutes and the liveness check 12 minutes 59
seconds. A 10-minute sample on the Mac first found 2,322,618 distinct states at depth 18 with a
growing queue, so the profile does not fit the one-hour budget on the Mac. The EC2 run is
`20261112T011930Z-23921`; its collected archive is
`target/model/remote-r100/node-1-artifacts.tar.gz`, with SHA-256
`f07ad9360f9a03645d0272e40a98a2e268fca71f8bc8ebf62d894ac7e693574e`. Collection and teardown both
exit 0, and `dev-machine status rsasync aws-linux-arm64-ubuntu2604` reports no AWS instance or EBS
volume. The archive excludes TLC state caches; it keeps the inputs, the command, the result, and
the host facts.
