# Task admission, wakes, and shutdown

This is the design model of task R100. It checks the rules that the shared wait handshake (R102)
and task scheduling (R301, R302, R303, R305, R312, R401, R404) build on. R102, R301, R302, R303,
and R312 exist, and the [correspondence](#correspondence) maps each action to their code. The
sources are in the local design `docs/specs/dual-async-runtime.md`: Future and Wake Contract, Local
and Movable Work, Time, Hardware, and Backends (the timers), Startup, Shutdown, and Failures, and
Capacity and Progress. The Design Models section of `docs/specs/family-policy.md` sets the
acceptance.

**Status (2026-11-17): all 31 profiles pass at the current inputs.** The declared profile and
the three raise-by-one profiles complete with no violation. All 16 known-bad variants and 11
witnesses give their required violations. See [Evidence record](#evidence-record).

## The promise and a concrete failure

The promise: a spawn reserves a task slot before it admits the task, and no task is admitted after
the shutdown request closes ingress. One owner polls a task at a time. A local task is polled,
queued, and dropped only on its home. A wake from any thread, also from a signal handler that owns
no worker, reaches its task. This includes a wake whose signal waits in the outbox of the worker
that sent it, until the next turn of that worker. A task that waits for a timer of its owner gets
its wake, because the owner parks with the deadline of its next timer. A wake or a cancel of an
earlier generation changes no task. Cancellation drops a future at a poll boundary. block_on
cancels the tasks that remain on its owner and waits for them. Shutdown ends each task, and a
worker exits only after the last task ends.

Six concrete failures:

- A future checks its event, and then registers its waker. A publish between the two finds no
  waker, so the task waits forever with its event set (`NoRecheck_bad`).
- A wake comes while the task is polled. The poll end does not schedule the task again, so the
  task waits forever (`LoseCoalesced_bad`).
- A waker of a task that ended runs after its slot holds a new task, and it schedules the new task
  (`StaleGen_bad`).
- After the shutdown request, a worker with empty queues exits while a task that it would poll is
  still idle (`EarlyExit_bad`).
- A worker puts the signal of a wake in its outbox and never sends it. The home stays parked, and
  the woken task stays in its list (`NoFlush_bad`).
- A home parks with no deadline while a task waits for its timer. The timer never fires
  (`NoDeadline_bad`).

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
| Workers | `Workers = {w1, w2}`. Each worker polls local and movable tasks. `SH`, the first worker, is the signal home. Its driver watches the wake descriptor of the signal handler, and its storage holds the timers. It is the one worker that parks (reduction 2) |
| block_on | The owner `B`, the thread that calls block_on. `bo` is `off`, `on` (the root runs and can spawn local tasks on `B`), or `ending` (the root completed). A call can start again after it returned. Each block_on call starts and ends while the runtime runs |
| Slots | `NSlots = 2` task slots, each with `MaxGen = 2` generations. `ss[s]` is `free`, `rsv` (reserved), `idle`, `sched` (in exactly one queue), or `run`. `gen[s]` names the task in slot `s`; `MaxGen + 1` retires the slot |
| Tasks | `kind[s]` is `local` or `movable`. `home[s]` is the home of a local task, or the location of a movable task: the worker that last polled it. A movable spawn reserves a slot and then admits the task at a worker. A local spawn runs on its owner |
| Queues | `rq[o]`, the run queue of each owner. `rr`, the remote-ready list of the signal home. A push that finds `rr` empty signals the signal home |
| Senders | Each push to `rr` has a sender. Only a parked home gets a signal (reduction 18). `Ext` writes the descriptor `sig` at once. It is a thread with no ring, the block_on owner (a thread home, whose outbox has no room), or a worker with a full outbox. A worker other than `SH` puts the signal in its outbox. `SH` sends itself no signal |
| Outbox | `ob[w]`: the outbox of worker `w` holds a signal of `SH`. The next drive turn or wait of `w` sends it (`Flush`), and then `sig` is readable. The exit of `w` also sends it. A step that ends the park of `SH` empties each outbox (reduction 18) |
| Polls | `cur[o]`, the task that `o` polls. A poll can return Ready (`Finish`), register its waker and recheck its event (`Register`), or arm its timer (`TimerArm`). It can return Pending with its registration, or after a wake during the poll (`Keep`). It can wake itself and return Pending when its poll budget is spent (`Yield`) |
| Events | `ev[s]`, the event of the task in slot `s`, and `reg[s]`, its registered waker. A publish from any thread sets the event, takes the waker, and wakes it. `nt[s]` records a wake during a poll; `cx[s]` records a cancel request |
| Timers | `tm[s]`: the task in slot `s` waits for its timer in the storage of `SH`, and the timer is its event. A turn of `SH` fires the timer (`TimerFire`). While a timer waits, the park of `SH` has a deadline (reduction 13) |
| Signal | The handler sets `sf` and writes the descriptor of the signal home. A turn of the signal home takes the flag and publishes the event of the task that waits for the signal |
| Shutdown | `ing` is `open` or `closed`. After closure, a reservation is refused at its admission, and each task gets its cancel (`Doom`). A worker exits after the last task ended (`wk`) |

## Physical and design assumptions

The model gives fairness only to a step that the design promises to take, or to a physical event.

| Assumption | Source | Fairness in the model, and why |
| --- | --- | --- |
| Each poll returns: a future returns from its poll or reaches a cooperative checkpoint | The design: "User code must return from a poll or reach a cooperative checkpoint before the runtime can schedule it again" (Future and Wake Contract). This is the stated condition of the progress properties | Weak fairness on `PollStep(o)` |
| A running owner polls the tasks in its queues in order, and it harvests its remote-ready list at each turn | The design: "Every drive turn bounds task polls, completion harvest, timers, wakes, cancellation, and maintenance" | Weak fairness on `PollStart(o)`. The run queue is first in, first out |
| A readable descriptor wakes its parked thread | eventfd(2): a read blocks until the counter is nonzero. `std::thread::park` returns when an unpark came before it. A cross-ring message posts a completion in the ring of its home, and the wait of the home (`io_uring_enter` with `IORING_ENTER_GETEVENTS`) returns. On a 7.0.y kernel with the eventfd defect (R302 note), this holds because the home never parks on the registered eventfd | Weak fairness on `Unpark` |
| Each drive turn of the signal home takes the signal flag | The design: the home's native signal; each turn handles the descriptor | Weak fairness on `SigHandle`, which can come at any step while the home runs (reduction 15) |
| The clock passes each deadline, a timed park ends at its deadline, and the next drive turn of the signal home fires each due timer | The design: "A deadline already in the past fires on the next drive turn". The due timers above the bound of a turn "fire in deadline order in the turns that follow" (Time, Hardware, and Backends). A timed park ends with `thread::park_timeout`, or with `IORING_ENTER_EXT_ARG`, which gives `-ETIME` | Weak fairness on `TimerFire(s)`, which also ends a park and can come at any step while the home runs (reductions 13 and 15) |
| The sender's worker keeps turning: it sends its outbox at its next drive turn or wait. The kernel posts each message at its home | R302: `ring::flush` runs in `Drive::drive` and in `Drive::park`, and the next submit sends each message. The kernel: Linux 6.1 posts the completion of a cross-ring message directly. Later kernels run it as task work on the thread of the target ring, and the wait of that ring runs it. A failed message writes the interrupt descriptor of its home at a later turn of the sender (`Wakes::complete`) | Weak fairness on `Flush(w)` for each worker other than `SH`, which can come at any step while `w` runs (reduction 15) |
| The grace timer fires, and the cancellation of each task follows; the end of block_on cancels each task that remains on its owner | The design: "Grace expiry escalates task cancellation" (Startup, Shutdown, and Failures); "After the root completes, block_on cancels each task that is still on that owner" | Weak fairness on `Doom(s)` for each slot |
| A worker exits, and block_on returns, when no task remains | The design: "A worker exits after the last task ends"; block_on "returns" after terminal release | Weak fairness on `Exit(w)` and `BlockOnEnd` |
| Spawns, publishes, signals, timers, old wakers and cancel handles, the root, the shutdown request, and steals are free | The design gives the caller no obligation; a steal is an optimization | None |

The fairness of `Flush(w)` is an assumption about the sender: the worker `w` keeps turning. It
reaches its next drive turn or wait only when its poll returns. A task that blocks the thread of
`w` delays each signal in the outbox of `w`. With one signal for each batch, it also delays each
later wake of that batch, from any thread, to a task on another home. The progress properties hold
under the poll condition of the first row for each owner, also for the sender. The design states
that condition only for the scheduling of the task itself: "User code must return from a poll or
reach a cooperative checkpoint before the runtime can schedule it again" (Future and Wake
Contract). The design also states the delay of a wake to another home: "A wake from a worker's
thread reaches another home at that worker's next turn, so a poll that does not return also
delays the wakes that its worker sent" (selected on 2026-11-17).

Each fairness condition names the failure that it could hide, and a known-bad variant with that
failure still fails under it:

| Fairness | Failure that it could hide | Known-bad variant that still fails |
| --- | --- | --- |
| `PollStep(o)` | A wake that is lost while a poll never returns | `NoRecheck_bad`, `LoseCoalesced_bad` (`OwedWakeDelivered`) |
| `PollStart(o)` | A queued task that its owner never polls | `NoRecheck_bad`, `LoseCoalesced_bad` |
| `Unpark` | A parked worker with work and no readable descriptor | `ParkBeforeDrainLive_bad` (`OwedWakeDelivered`) |
| `SigHandle` | A signal flag that the home never takes | `ParkWithFlag_bad` (`OwedWakeDelivered`) |
| `TimerFire(s)` | A timer that its home never fires, for example because the home parks with no deadline | `NoDeadline_bad` (`OwedWakeDelivered`) |
| `Flush(w)` | A signal that stays in an outbox | `NoFlush_bad` (`OwedWakeDelivered`) |
| `Doom(s)` | A cancel that never schedules its task | `NoGraceWake_bad` (`ShutdownJoins`), `BlockOnNoCancel_bad` (`BlockOnReturns`) |
| `Exit(w)`, `BlockOnEnd` | A worker or a block_on call that waits for a task that never ends | `NoGraceWake_bad`, `BlockOnNoCancel_bad` |

## Reductions and their soundness

Each reduction removes states or steps that no property can tell apart, or adds behaviors. After
all of them, each known-bad variant still fails its own property (see the evidence record).

1. Each wake pushes its task to the remote-ready list of its home, also a wake on the home thread,
   as `Shared::wake` and `Shared::push` do. In the code, the compare-exchange of the slot word and
   the push of the list are two atomic steps. In the model, they are one step. Between them, the
   task is scheduled and in no list. Another wake or cancel then only sets bits of the word, and
   the owner cannot take the task. A park of the home finds the list empty, so the push signals
   it. So each step between the two gives the same state before or after the push. The model
   gives the run queue of an owner other than `SH` each push (reduction 3). Known-bad:
   `DoublePush_bad`.
2. Only the signal home parks. A worker that does not park only takes steps sooner. The park
   protocol is the same for each owner. The signal home checks it with each kind of signal: an
   immediate signal, a signal in an outbox, the signal flag, and the deadline of a timer. The
   block_on owner is a thread home: each signal to it is an immediate unpark, so its park is a
   case of this protocol. A worker sends its outbox before it parks (`Drive::park`), so a parked
   sender holds no signal. Known-bad: `ParkBeforeDrain_bad`, `ParkWithFlag_bad`, `NoFlush_bad`,
   `NoDeadline_bad`.
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
   (`Publish`, the signal, a timer) or from the poll budget (`Yield`). A current cancel comes from
   `Doom`, which is the same step as a cancel handle: a wake with the cancel bit. `Doom` runs while
   the runtime runs (at the end of block_on) and after the shutdown request, during polls and
   before slot reuse. Known-bad: `StaleGen_bad`.
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
13. Only the signal home has timers, and the deadline of a timer and its fire are one step. The
    other owners do not park (reduction 2), so a timer there is an event that a turn of its owner
    publishes, which `Publish` has. When a deadline passes, no variable of the code changes: the
    storage changes only when a turn fires the timer. A park while a timer waits ends at its
    deadline, and the turn after it fires the timer, so `TimerFire` also ends a park. A
    task arms its timer only in a poll on the signal home, so no timer starts during a park. So
    the model can read the deadline from `tm` at each step. The end of a stolen task can leave a
    park with no timer. In the code, that park ends at its old deadline and finds no work, and the
    next park is a state of the model. So the model has no wake at a deadline with no timer. The
    arm and the registration of the waker are one step: the poll of a timer is the handshake of
    reduction 10 (`Waiter::poll_wait`). A turn fires at most 64 timers, which is more than the
    slots, and a turn with more due timers does not park. Known-bad: `NoDeadline_bad`.
14. A sender that signals at once pushes and writes the descriptor in one step. In the code,
    `Shared::push` changes the head, and then `Shared::signal` writes the descriptor or unparks the
    thread, with no wait between. Only `Unpark` reads `sig`, and a parked home takes no other step.
    So each step of another thread between the two gives the same state before or after the
    write. If the descriptor was readable already, the home can wake between the two, and the
    write then only ends a later park at once. A push on the thread of the signal home itself
    sends no signal (`Shared::signal` returns). `SigHandle` and `TimerFire` have that sender. A
    publish, a spawn, or a cancel on that thread has the sender `Ext` in the model. Its extra
    write only ends a later park at once, and `Park` reads no signal. Known-bad: `NoFlush_bad`
    shows that the properties see a signal that does not come.
15. A step of a turn, `SigHandle` and `TimerFire` of the signal home and `Flush` of a worker, can
    come at any step while its thread runs, also during a poll on that thread. In the code, such a
    step comes between polls. The argument has two parts:
    - Safety: a flush, a timer fire, or a signal during a poll only adds behaviors, and it removes
      none. So each safety result covers the code.
    - Progress: a behavior of the code is a fair behavior of the model while each owner keeps
      turning: its turn steps come at each turn, and weak fairness only asks that a step that
      stays enabled is taken. For `Flush`, this premise is the physical assumption above: the
      sender's worker reaches its next drive turn or wait. A task that blocks the worker's thread
      breaks it. Under the premise, a code behavior that violates a progress property is a fair
      model behavior that violates it.

    Each such step stays enabled until it is taken: `Park` needs the flag clear, `TimerFire` ends
    a park, and an outbox stays full until `Flush` or `Exit`. Strong fairness is then not
    necessary, and it is costly. On the evidence host, the declared profile took 392 s with
    strong fairness on five turn steps, and it takes 24 s with weak fairness, for almost the
    same states. Known-bad: `ParkWithFlag_bad`, `NoDeadline_bad`, and `NoFlush_bad` still fail
    under weak fairness.

16. A batch steal (R401) takes the oldest movable task of the victim's queue and polls it, and it
    moves the next movable task, if one is there, to the thief's run queue. The code moves each
    other task of the older half of the run queue, or of the whole remote-ready list, in queue
    order. Each moved task stays scheduled, with one queue entry, and the thief polls it in queue
    order, as the model does with its one moved task. Each further moved task repeats that step.
    The steal polls the oldest task: in the first form of this action, a steal could poll a newer
    task and move an older one, and two workers then moved one task back and forth with no poll,
    which violated `OwedWakeDelivered` (2026-11-22). So the remote-ready list of the code gives
    its tasks in the order of their pushes.
17. A wake or a cancel of an earlier generation changes no variable in the correct model (reduction
    8): it is a stuttering step, which `[Next]_vars` allows at each state, so the model gives it no
    step of its own (R409, 2026-11-25). The behaviors are the same, and a stuttering step changes
    no fairness condition. Only the `staleGen` variant has the step, because there the stale wake
    changes a task. The distinct states of each profile stay the same (425,220 declared, 1,017,966
    `Gens3`, 2,212,365 `Workers3`), and the generated states fall by a fifth (4,956,462 to
    3,982,218 declared). Known-bad: `StaleGen_bad` still violates `StaleWakeRejected`.
18. A signal waits only for a parked signal home (R409, 2026-11-25). The home reads its
    descriptor only in `Unpark`, and it parks only with an empty list and a clear signal flag. So
    a signal that comes while the home runs carries no work: it only ends the next park at once,
    and the home then finds nothing. The model gives such a signal no state. A push, a flush, or
    the signal handler sets `sig` or an outbox only while the home is parked, and each step that
    ends a park (`Unpark`, `TimerFire`, and the exit of the home) clears `sig` and each outbox.
    `RunningHasNoSignal` states this. The distinct states fall by 60% (425,220 to 168,895
    declared).

    Soundness: each fair behavior of the model before this reduction (the full model) has a fair
    behavior of this model with the same values of each other variable. A full state and a state
    of this model are related when they are equal but for `pk`, `sig`, and `ob`, and one of
    these holds:
    - (a) both homes run, and this home has no signal;
    - (b) both homes are parked, with equal `sig` and `ob`;
    - (c) the full home is parked with a readable descriptor or a signal in an outbox, and this
      home runs with no signal.

    For each step of the full model from a related pair, this model takes the same step or no
    step, and the pair stays related:
    - From (a), each step is the same step. A signal that the full step sets is dropped here. A
      `Flush` of a signal that came while the home ran is no step here. A `Park` with no signal
      gives (b). A `Park` with a signal is no step here, and it gives (c).
    - From (b), each step is the same step: a push, a flush, and the handler signal a parked
      home in both models. `Unpark`, `TimerFire`, and the exit of the home give (a).
    - From (c), the full home takes only `Unpark`, `TimerFire`, or its exit, which give (a):
      `Unpark` is no step here, and the two others are the same step. A step of another thread
      is the same step, or no step for a `Flush`, and the full home keeps a signal: only these
      three steps remove the last one.

    No other guard reads `pk`, `sig`, or `ob`. The steps of the home that need a running home
    are not enabled in a full state of (c), so this model must match none of them there. Each
    property but `ParkedHasSignal` reads only the equal variables. `ParkedHasSignal` holds in a
    full state of (a), where the home runs, and of (c), where it has a signal, and (b) has equal
    states. So a violation in the full model is a violation here.

    Fairness: a stay in (c) ends. A readable descriptor keeps `Unpark` enabled, and a signal in
    the outbox of `w` keeps `Flush(w)` enabled until the flush or the exit of `w` writes the
    descriptor. Weak fairness of both steps is in `Fairness`. Then take a fair step that stays
    enabled in the behavior of this model. If its guard reads none of the three variables, it
    is enabled in the full behavior too, so the full behavior takes it. `Unpark` and `Flush`
    are enabled here only in (b), where the states are equal. A poll start, a poll step, or
    `SigHandle` of the home needs a task in its queue, a running poll, or the signal flag, and
    `Park` is not enabled with one of them. So the full home parks no more, its stay in (c)
    ends, and the step is enabled in the full behavior from then on.

    Known-bad: `ParkBeforeDrain_bad`, `ParkBeforeDrainLive_bad`, `ParkWithFlag_bad`,
    `NoFlush_bad`, and `NoDeadline_bad` still fail. In these variants, a wrong park now fails
    also when a signal came before it: the full model let that signal end the park. Witnesses:
    `SignalWake_reach`, `DeferredSignal_reach`, and `TimerWake_reach` are still reached.

The declared bounds are argued, not proven: two workers, two slots, and two generations. Each one
has a raise-by-one profile.

## Memory-order assumptions

Each action is one atomic step, or two code steps that reductions 1 and 14 join. The code proofs
of R102, R301, R302, and R401 must show that the production code gives each step below its
atomicity:

| Step | Assumption |
| --- | --- |
| `Wake`, `WakeEffect`, `Doom` | The slot word holds the generation and the scheduling bits (scheduled, running, notified, cancelled). A wake or a cancel is one compare-exchange on it (acquire-release), so the generation check and the state change are one step (`Shared::wake`). The push of a task that was idle follows (reduction 1) |
| `Take` (in `PollStart`, `Steal`) | The poll start is one read-modify-write from scheduled to running (acquire), and it also reads the cancel bit (`slot::poll_start`). So the poller sees the task state that the last poll wrote. A steal (R401) claims the older half of the victim's run queue with one compare-exchange of its head (`Deque::steal_into`), or takes its whole remote-ready list with one swap (`Shared::take_list` in `task/pool.rs`) |
| `EndPending` | The poll end is one compare-exchange (release) that reads the notified and cancel bits in the same change to idle or scheduled (`Shared::poll_end`) |
| `Push` | The remote-ready push is one compare-exchange on the list head (release) that also tells whether the list was empty, then the signal (`Shared::push`, `Shared::signal`). The harvest looks at the head with no order, and swaps it (acquire) only when the list is not empty (`Shared::harvest`). A look that reads an empty list comes before the push in the order of the head, and that push found the list empty, so it signals the home: the step is a harvest before the push. The take of the root flag is the same (`Shared::take_root`) |
| `Flush` | The outbox belongs to the owner's thread: only that thread puts a home in it (`ring::post`) and drains it (`ring::flush`). Each message is one `IORING_OP_MSG_RING` entry, which the next submit of the sender's driver sends |
| `Park`, `Unpark` | The signal is sticky: the eventfd counter, the completion of a message, and the token of `Thread::unpark` stay until the home takes them. The owner harvests its list, takes the signal flag, and then reads the list head again (`Shared::has_remote`) before it parks. A push after the harvest found the list empty and signals |
| `TimerArm`, `TimerFire` | The timer storage and the park deadline belong to the owner's thread (`Local::timers`, `Local::until`). The state of a timer and its waiter registration follow the R102 handshake (`Waiter::poll_wait`, `Waiter::wake`) |
| `Register`, `PublishEffect` | The event and the waiter registration are one atomic word, or a sequentially consistent fence orders each side: the publisher stores the event before it takes the waker, and the waiter registers before it rechecks |
| `Admit`, `SpawnLocal`, `Refuse`, `Exit` | A local spawn changes only cells of the owner's thread (`closed`, `free`, `queue`). For a movable spawn and a worker exit (R401, R404), one atomic word holds the closed bit of ingress and the count of live tasks. An admission is one read-modify-write that fails when the bit is set. A worker exits after it reads a zero count with the bit set |
| `SigRaise`, `SigHandle` | The handler adds one to the delivery count of the signal (sequentially consistent) before it writes the descriptor. The turn of the home compares the count with the count of its last wake (`signal::wake`) |

## Correspondence

Each action is one atomic step. The rule column names the design rule that the action implements.
The last column names the production transition at Git revision `00b14a3`, or the task that adds
it. A change to either side reopens the check. All paths are in `src/`.

| Action | Design rule | Atomicity | Production transition |
| --- | --- | --- | --- |
| `Reserve`, `Admit`, `Refuse` | Local and Movable Work: spawning reserves a task slot before it accepts the future; a refused spawn returns the future with a typed reason. Startup, Shutdown, and Failures: closing ingress refuses admission | The reservation; the admission or refusal | R401 (movable spawn), not built |
| `SpawnLocal` | Local and Movable Work: spawn on the current local owner keeps polling and destruction there | One step on the owner's thread | `Local::spawn` (`task/owner.rs`), through `spawn_local` (`task.rs`), with `Shared::start` (`task/slot.rs`) |
| `Publish`, `Register`, `Keep`, `Finish` | Channels and Synchronization: a wait registers interest, rechecks the condition, and parks | One poll step | R102, in `wait.rs`: `Publish` is the change of the condition and then `Waiter::wake`. `Register` is `Waiter::register` and the recheck in `Waiter::poll_wait` (the model's one step is two changes of the state word and a load; reduction 10). `Keep` is `Waiter::poll_wait` that gives `Pending` with its registration. `Finish` is the `Ready` of the future, then `Local::finish` and `Shared::end` |
| `Yield` | Future and Wake Contract: the poll budget returns Pending once and schedules its task again | One poll step | R303: `coop::poll` (`coop.rs`) calls `wake_by_ref` when no unit is left, so `Shared::wake` sets the notified bit of the running task, and `Shared::poll_end` schedules it again |
| `TimerArm`, `TimerFire` | Time, Hardware, and Backends: a timer never fires before its deadline; a deadline in the past fires on the next drive turn | One poll step; one step of a turn | R312: `task::timer` (`task.rs`) and `Timer::poll` (`timer.rs`, through `Waiter::poll_wait`); `Local::fire_timers` (`task/owner.rs`) through `Timers::turn` (`timer.rs`) |
| `Wake`, `WakeEffect` | Future and Wake Contract: every waker is `Send + Sync`; a wake from another thread publishes the task to its home's remote-ready list; a wake after the task ended or its slot was reused has no effect | One compare-exchange of the slot word, then the push (reduction 1) | `Shared::wake` and `Shared::push` (`task/slot.rs`), through the waker of `TaskRef` and `CancelHandle` |
| `PollStart`, `Take`, `EndPending` | Local and Movable Work: cancellation drops the future at a poll boundary, never during a poll. Future and Wake Contract: coalescing avoids duplicate scheduling without losing progress | One step at the owner | `Local::turn` (`task/owner.rs`: `Shared::harvest`, then the polls of the turn) and `Local::poll_slot`, with `Shared::poll_start` and `Shared::poll_end`. A cancelled task ends in `Local::finish` |
| `Steal` | Local and Movable Work: movable work moves; a local task stays on its home | One step at the idle worker | `Pool::steal` and `Pool::steal_from` (`task/pool.rs`), with `Deque::steal_into` (`task/deque.rs`); `Pool::run` starts the poll and records the location |
| `Push`, `Flush` | Future and Wake Contract: one unpark of the home for each batch, through its native signal. A thread that owns a ring sends a cross-ring message. A thread that owns no ring writes an event descriptor | One step each (reduction 14) | `Shared::push` and `Shared::signal` (`task/slot.rs`). `ring::post` (`task/ring.rs`) puts the home in the outbox, and `ring::flush` sends it in `Drive::drive` and `Drive::park` with `Driver::message` (`driver/home.rs`). `Enter::drop` (`task/owner.rs`) writes the interrupt descriptor of each home that is still in the outbox |
| `Park`, `Unpark` | Future and Wake Contract: the home parks until its native signal; Time, Hardware, and Backends: the owner parks at most until its next timer | One step each | The park condition in `Local::settle`, which the loop of `Local::block_on` calls (`task/owner.rs`). It parks only when no timer is due, the local queue is empty, and a look after the drive finds the list empty and the root flag clear (`Shared::has_remote`, `Shared::root_woken`): the drive clears the wake descriptor, so a look before it could miss a push, the deadline from `Local::until`, and `Park::park`: `Thread::park` (`task/owner.rs`) and `Drive::park` (`task/ring.rs`) |
| `SigRaise`, `SigHandle` | Future and Wake Contract: a thread that owns no ring writes an event descriptor that the home watches | One step each | R220: the handler `handle` (`driver/signal.rs`), and `signal::wake` in `Driver::turn` (`driver.rs`) |
| `Shutdown`, `Doom`, `Exit` | Startup, Shutdown, and Failures: close ingress, drain, cancel at grace expiry, and no worker exits while a task of the runtime remains | One step each | R305 (local workers) and R404 (both modes), not built. The cancel at the end of block_on is `Local::cancel_all` (`task/owner.rs`) |
| `BlockOnStart`, `RootDone`, `BlockOnEnd` | Startup, Shutdown, and Failures: block_on makes the calling thread a local owner; after the root completes, it cancels each task still on that owner and returns after they end | One step each | `LocalOwner::block_on` (`task.rs`) and `Local::block_on` (`task/owner.rs`). The wake of the root is `Shared::wake_root` and `Shared::take_root` (`task/slot.rs`) |
| `Terminated` | None. The end state stutters, so deadlock checking stays on | One step | None |

## Properties

| Property | Acceptance text | Kind |
| --- | --- | --- |
| `OnePollOwner` | One poll owner: at most one owner polls a task, and a task in the run state has exactly that owner | Invariant |
| `HomeOnly` | Home-only local poll and drop: a local task is polled, queued, and dropped only on its home | Invariant |
| `BoundedReservations` | Bounded reservations: each queue entry is a reserved task in the scheduled state, and each such task has exactly one entry | Invariant |
| `ExitAfterWork` | No worker exits while a task of the runtime remains | Invariant |
| `BlockOnJoins` | block_on returns only after each task on its owner ended | Invariant |
| `ParkedHasSignal` | The parked signal home has a readable descriptor or a signal in an outbox when its remote-ready list holds work or the signal flag is set | Invariant |
| `RunningHasNoSignal` | The form of reduction 18: the signal home that runs has no readable descriptor and no signal in an outbox | Invariant |
| `StaleWakeRejected` | Stale-wake rejection: a wake or a cancel of an earlier generation changes no task | Action property |
| `NoAdmissionAfterClose` | No admission after ingress closure | Action property |
| `StablePhases` | The side condition of the arguments below | Action property |
| `OwedWakeDelivered` | Eventual owed wake delivery: each task whose event happened, whose cancel was requested, or that waits for its timer, is polled. The signal flag is taken while the signal home runs | `[]<>`, equal to `Owed(s) ~> ~Owed(s)` and to `sf ~> (~sf \/ wk[SH] = "exit")` |
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
constants in a `.cfg` file. The model has no count of wakes, polls, parks, or timers: a task can
be woken, polled, and parked again forever, and repeated cycles use no finite event allowance.

The faults and free choices:

- A spawn, a publish, a signal, a timer, an old waker, an old cancel handle, the root of
  block_on, the shutdown request, and a steal can come at any time, or never.
- The sender of a publish, a spawn, or a cancel is any thread: a thread that signals at once, or
  a running worker that puts the signal in its outbox.
- A future can return Ready, register and recheck, arm its timer on the signal home, return
  Pending with its registration, or yield at any poll.
- A wake can come during a poll, while the task waits in a queue, or after its slot was reused.

The fairness is weak fairness for each step in [Physical and design assumptions](#physical-and-design-assumptions).
The model has no strong fairness (reduction 15).

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
| `NoFlush_bad` | A worker never sends the signal in its outbox while it runs | `OwedWakeDelivered` |
| `NoDeadline_bad` | The signal home parks with no deadline while a task waits for its timer | `OwedWakeDelivered` |

The counterexamples of the evidence run, as actions:

- `NoRecheck_bad`: `Reserve`; `Admit` at `w1`; `PollStart(w1)`; `Check` (the event is not set);
  `Publish` (no waker is registered, so no wake); `RegisterLate`; `Keep`; then a cycle of parks,
  signals, and block_on calls. The task waits with its event set.
- `LoseCoalesced_bad`: `BlockOnStart`; `Park`; `SpawnLocal(b)`; `Publish` (no waker yet);
  `PollStart(b)`; `Yield` (the variant loses the wake of the spent poll budget); then
  stuttering. The task is idle with its event set.
- `StaleGen_bad`: `BlockOnStart`; `SpawnLocal(b)`; `PollStart(b)`; `Finish` (the slot frees with
  generation 2); `SpawnLocal(b)`; a `Wake` of generation 1 with the cancel bit, which changes the
  new task.
- `EarlyExit_bad`: `Reserve`; `Admit` at `w1`; `Shutdown`; `Exit(w2)` while the task waits at
  `w1`.
- `DoublePushPoll_bad`: `Reserve`; `Admit` at `w1`; `PollStart(w1)`; `Register`; a yield leaves
  the task queued at `w1`; `Publish` pushes it again; `w2` steals one entry, and `PollStart(w1)`
  takes the other.
- `NoFlush_bad`: `Park`; `Reserve`; `Admit(1, w1, w2)` (the list was empty, so the signal goes to
  the outbox of `w2`); `Publish(1, w2)`; `Shutdown`; `DoomBy(1, w2)`; then stuttering. The parked
  signal home holds the task in its list, with its event and its cancel set. The signal stays in
  the outbox.
- `NoDeadline_bad`: `Reserve`; `Admit` at `w1`; `PollStart(w1)`; `TimerArm(w1)`; `Keep(w1)`; then
  block_on calls, signals, and parks with no deadline, again and again. The task waits for its
  timer. Each park disables `TimerFire`, so weak fairness does not make the timer fire. In the
  correct model, `TimerFire` also ends a park.
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
| `DeferredSignal_reach` | The signal home is parked, its list holds a task, and the signal of that batch waits in the outbox of another worker | The outbox path; `ParkedHasSignal` uses its new term |
| `TimerWake_reach` | The deadline of its park wakes the signal home, whose descriptor is not readable, while a task waits for its timer | The timed park |

## Limits

The model does not cover these items. Each one is outside the task, or a later task owns it.

- A shutdown request while a block_on call runs. Each block_on call starts and ends while the
  runtime runs.
- The drain of the driver at the end of block_on. block_on drives its driver until each operation
  that it homes reaches terminal release (the design of 2026-11-14). The operation-ownership
  model checks that rule: its `Exit` of a home waits for the terminal release of each operation
  that the home holds ([operation ownership](../operation-ownership/README.md)). In this model,
  `BlockOnEnd` waits only for the tasks of its owner.
- The factory that initializes another local owner, typed messages to a state owner, and the
  waiting form of spawn.
- Join handles and their outcomes (completed, cancelled, panicked, owner failure), panics, and
  output that waits for its handle (reduction 7).
- A task with more than one event, and the replacement or the cancel of one waiter among several.
  R102 owns those cases with its bounded exploration.
- Operations, drivers, and I/O: the operation-ownership model checks them.
- Durations: the grace period, the poll budget, the bounds of a drive turn, and the delay of a
  timer after its deadline are steps with no duration. R312 tests the coarsening of a timer.
- Mixed mode: each worker polls local and movable tasks. A worker of one mode has a subset of
  these behaviors.
- The model proves no Rust code. The loom models of R301 and R302 explore the slot word and the
  remote-ready list, and the correspondence table maps each action to its code.

## Evidence record

All 31 profiles pass for reductions 17 and 18 of R409, at Git revision `49d1ec5`
(`sha256:260207786690f103472b8486d51d354b0635ab27006bb48e50b0f2068ce6a54a`, 32 files). The
checkers were TLC 1.8.0+bc1b506bdcad and Apalache 0.63.0 on each host.

On a Mac (10 cores, 16 GiB, Darwin 27.2.0, Temurin JRE 21.0.13.1+2, 2026-11-25), the 30 profiles
other than `Slots3` pass in 156 seconds of checker time:

| Profile | Distinct states | Before R409 | Seconds |
| --- | --- | --- | --- |
| `TaskLifecycle.cfg` | 168,895 | 425,220 | 13.80 |
| `TaskLifecycle.Gens3.cfg` | 411,771 | 1,017,966 | 39.42 |
| `TaskLifecycle.Workers3.cfg` | 703,666 | 2,212,365 | 82.36 |

All 16 known-bad variants violate their named property, and all 11 witnesses reach their named
target. Before the full `Slots3` run, a 10-minute sample on that Mac found 7,161,960 distinct
states at depth 21, with a queue of 1,706,158 states that still grew.

`Slots3` passes on one EC2 c8g.4xlarge (16 vCPUs, 32 GiB) of the target
`aws-linux-arm64-ubuntu2604`: image `ami-098ede342a5440ac0`, Ubuntu 26.04, Linux 7.0.0-1018-aws,
aarch64, dev-machines run `20261126T005047Z-65673` (2026-11-25), from a clone of a Git bundle of
`49d1ec5` with a clean tree (`models/remote.sh`). The search found 17,655,206 distinct states
(200,035,585 generated) at depth 40, with 0 states left on the queue and no error, in 13 minutes
52 seconds. Its check of the progress properties took 13 minutes 59 seconds. `cargo xtask model`
gave `ok in 1674.03s`: 27 minutes 53 seconds, inside the one hour of
[Large profiles](../../docs/content/design-models.md#large-profiles). The JRE was Temurin
21.0.13+9, the Linux aarch64 build, which matched its published SHA-256. The record of the run
names the profile closure `sha256:228daa837e9d1ac8079f1f85be6f04044dd4f775eb12b084d6ff55b20e3ae4dc`.
After the run, `dev-machine status rsasync aws-linux-arm64-ubuntu2604` reports no AWS instance or
EBS volume.

The starvation variant of R401 fails under the reductions: a steal that can poll a newer task and
move an older one, in a scratch copy of `49d1ec5`, violates `OwedWakeDelivered` in the declared
profile (175,423 distinct states, on that Mac).

The record of the model before R409 follows.

All 31 profiles pass for the R401 change of `Steal`
(`sha256:52cff6cf0c1d8930755ad24a1e3e255feae931810939108cc5428d4d29f03924`, 32 files). On a Mac (10
cores, 16 GiB, 2026-11-22), 30 profiles pass in 10 minutes 35 seconds: the declared profile
(425,220 distinct states), `Gens3` (1,017,966), `Workers3` (2,212,365), each known-bad variant, and
each witness. On that Mac, `Slots3` searched each state, and then TLC ran out of its default heap
of 4 GiB in the check of the progress properties.

`Slots3` passes on one EC2 m8g.4xlarge (16 vCPUs, 64 GiB, so a default heap of 16.5 GB) of the
target `aws-linux-arm64-ubuntu2604-memory`: image `ami-098ede342a5440ac0`, Ubuntu 26.04, Linux
7.0.0-1018-aws, aarch64, dev-machines run `20261123T022711Z-36121` (2026-11-23), from a plain clone
of `9066dc1` with a clean tree. The search found 47,785,001 distinct states at depth 40, with 0
states left on the queue and no error, in 4904.02 seconds. Its check of the progress properties
took 37 minutes 43 seconds. The checkers were Temurin JRE 21.0.13+9 (the Linux aarch64 build,
which matched its published SHA-256), TLC 1.8.0+bc1b506bdcad, and Apalache 0.63.0. The archive of
the run directory, with no TLC state cache, has the SHA-256
`8ef8875dcb993df984d3613d766cdbc5263d869d7a33bcc267ec375898ae5e88`. After the run,
`dev-machine status rsasync aws-linux-arm64-ubuntu2604-memory` reports no AWS instance or EBS
volume. That run took more than the one hour of [Large
profiles](../../docs/content/design-models.md#large-profiles), which R409 corrected.

The record of the model before R401 follows.

All 31 profiles pass for
`sha256:e566767773d31c00515732bb7bffcf6ca755f1a86888833db91b744e3ead2434` (32 files). The checks
use Temurin JRE 21.0.13+9 (the Linux aarch64 build), TLC 1.8.0+bc1b506bdcad (`TLC2 Version
2026.11.15.002146`, rev `bc1b506`), and `cargo xtask model task-lifecycle/<profile>`. Each
download matched its published SHA-256 before the install. Each profile had a cap of 3,600
seconds. Each positive search finishes with no violation, an empty queue, and exit 0.

| Profile | Distinct states | Depth | Seconds | Run |
| --- | --- | --- | --- | --- |
| `TaskLifecycle.cfg` | 337,524 | 29 | 23.68 | 1 |
| `TaskLifecycle.Gens3.cfg` | 797,682 | 35 | 65.27 | 1 |
| `TaskLifecycle.Workers3.cfg` | 1,487,317 | 28 | 132.95 | 1 |
| `TaskLifecycle.Slots3.cfg` | 32,271,537 | 39 | 3332.42 | 2 |

All 16 known-bad variants violate their named property, and all 11 witnesses reach their named
target, in run 1. Their combined checker time is 17.00 seconds. Before the full Slots3 run, a
10-minute sample found 14,093,955 distinct states at depth 21 with a growing queue.

Each run used one EC2 c8g.4xlarge (16 vCPUs, 32 GiB, on demand) of the target
`aws-linux-arm64-debian12`: image `ami-06bcda7571e75165c`, Debian 12, Linux
6.1.0-54-cloud-arm64, aarch64. The runs started from plain clones of the task branch, because a
synced worktree has no Git directory on the host. Collection and teardown exit 0, and
`dev-machine status rsasync aws-linux-arm64-debian12` reports no AWS instance or EBS volume.

| Run | dev-machines run | Git revision | Archive SHA-256 |
| --- | --- | --- | --- |
| 1 | `20261117T033943Z-94578` | `349a56b` | `9dbad66d90723f94ba31aeb71ed12c4ef973e8bd4d6068fd6de6defffc7811cf` |
| 2 | `20261117T042231Z-78072` | `99026ce` | `f5303bc7152fff7faee2f607998af650a2a63eb5580187fcc362917e6f518825` |

Each archive is `node-1-artifacts.tar.gz` in
`~/.local/share/dev-machines/runs/rsasync/aws-linux-arm64-debian12/<run>/`. It keeps the inputs,
the commands, the logs, the traces, and the host facts, and no TLC state cache. The two revisions
have the same model files; between them only this README changed.

The archive of run 2 lacks the end of its liveness check. The search ended at 04:53:59 UTC with 0
states left on the queue. dev-machines stopped the remote script 120 seconds before the end of
its 60-minute lease, at 05:20:31 UTC, and the exit trap of the script took the archive then. TLC
ran in its own process group under `timeout`, so it continued. At about 05:21:10 UTC,
`cargo xtask model` read the complete output and printed `ok in 3332.42s (exit status: 0)`.
dev-machines retains that line in `node-1-recipe.log` (SHA-256
`a7dcaa394821d7571cb65b531ea32c0eb817c3e2295287f882c858dd81d63725`). The archived `run.txt`
still has `state=started`, and the archived `checker.log` ends at the start of the liveness check.
A rerun of this profile needs a lease above 60 minutes, for example
`DEV_MACHINE_MAX_LEASE_MINUTES=90`, because setup and collection come on top of the check.

An earlier revision had strong fairness on five turn steps. Its declared profile passed in 392
seconds on the same target, and reduction 15 replaced it. Its other results are void.
