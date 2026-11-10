# Operation ownership, close, and shutdown

This is the design model of task R000. It checks the rules that the operation table (R203), the
TCP rules (R206), and zero-copy close (R603, R604) build on, before that code exists. The sources
are in the local design `docs/specs/dual-async-runtime.md`: Driver Topology and Operation Home,
Completion I/O and Buffers (the terminal-event table and the cancel rule), Future and Wake Contract
(generations), and Startup, Shutdown, and Failures. The Design Models section of
`docs/specs/family-policy.md` sets the acceptance.

**Status (2026-11-08): complete.** The rule of 2026-11-08 changed the module: a zero-copy send
frees its direction at its send result ([Decisions of 2026-11-08](#decisions-of-2026-11-08)).
Every profile passed with its required verdict on the evidence host at the current digest
`sha256:48cb71030ec27b3eb01114a3970fe0a79081423c7b161f09c6a88cd4fe23abaa` (53 files), in one
command ([Evidence record](#evidence-record)). The evidence-host record of the rule of 2026-11-07
stays below as the record of that design. The design as first written had one defect, a close that
waits for a zero-copy notice; [Decisions of 2026-11-07](#decisions-of-2026-11-07) gives it, the
selected design, and each rejected option.

## The promise and a concrete failure

The promise: an operation has one home driver from its accepted submission to its terminal
release. The consumer of the operation can move to another worker, drop its future, forget it, or
cancel it, and the runtime can close the resource or shut down. The driver releases each buffer
and charge exactly once, and only after the kernel can no longer use it. A stream gives each byte
to the caller once and in order. A zero-copy send frees its direction at its send result, and
each notice releases its own backing once. Close never waits on a peer. Shutdown waits on a peer
for no longer than its grace period. Detached debt ends while the runtime runs.

Four concrete failures:

- A receive future drops while its kernel request is in flight, and the runtime frees the buffer
  at the drop. The kernel then writes into freed memory.
- Close waits for a send that it did not cancel. The peer never reads, so close never returns.
- Close waits for a zero-copy notice. TCP frees the sent pages only when the peer acknowledges
  them, so a peer that stops reading keeps close waiting forever. This was the design before
  2026-11-07 (`ZcWait_bad`).
- The driver releases a zero-copy buffer at the send result, before the notice, and the kernel
  then reads freed pages (`ZcEarlyRelease_bad`).

## Run it

```sh
cargo xtask model operation-ownership                                    # every profile
cargo xtask model operation-ownership/OperationOwnership.Uring.cfg       # one profile
```

The output of a profile goes to `target/model/operation-ownership/<file>/`: the checker output in
`checker.log` and, on a violation, the trace in `trace.json`. The xtask prints one digest over the
checked files of the model; the evidence record names it. [Check the design
models](../../docs/content/design-models.md) tells how to install the checkers and what a
failure means.

## Files and profiles

| File | Role |
| --- | --- |
| `OperationOwnership.tla` | The module: the state, the actions, the properties, and the reachability targets |
| `OperationOwnership.Uring.cfg`, `OperationOwnership.Iocp.cfg` | The main profiles at the declared bounds: every property |
| `OperationOwnership.UringWide.cfg`, `OperationOwnership.IocpWide.cfg` | Two submissions on each direction (four on the stream): the wider reading of the task bound, for the evidence host |
| `OperationOwnership.UringWorkers3.cfg`, `UringBytes4`, `UringConns2`, `UringOps3`, and the same four for IOCP (`IocpWorkers3`, `IocpBytes4`, `IocpConns2`, `IocpOps3`) | One argued bound raised by one on each backend, for the evidence host (family policy, Design Models) |
| `OperationOwnership.<Name>_bad.cfg` | The known-bad variants: each must violate its one property |
| `OperationOwnership.<Name>_reach.cfg` | The reachability witnesses: each must violate its `Unreached<Name>` property, which shows a trace to the target |
| `statechart.md` | Two statecharts derived from the module |

Each profile is a TLC configuration of `OperationOwnership.tla`. The xtask passes a `_bad` or
`_reach` profile only when TLC reports a violation of the one property that the profile names.

## What the model contains

| Part | Model |
| --- | --- |
| Workers | `Workers = {w1, w2}`. Each worker has one driver. A worker runs until it exits at shutdown |
| Consumer | One task, alive until `EndTask`. It moves between the workers, so an operation's home is the worker where it was submitted. While the task is alive, both workers run and no step reads a home, so `EndTask` selects the home of each held slot. The first held slot gets one fixed worker (`W0`); this removes only mirror images |
| Resources | One stream (`rx` receive, `tx` send) and one listener (`ac` accept). One slot for each direction, so at most one outstanding operation for each direction |
| Operations | `MaxOps = 2` submissions. `Submissions = "resource"` (declared): two on each resource, so the receive and the send share the two of the stream. `"direction"`: two on each direction. The generation `gen[c]` names the operation in slot `c` |
| Kernel | `k[c]`: `fly` (an event that the peer controls), `cxl` (the terminal event after a cancel that found its target), `tail` (the IOCP packet after an immediate return, whose result is known), or `none` |
| Requests | `rq` holds the request entries `<<c, g>>`. A cancel from the consumer or from close travels to the home of operation `g`, and the home takes it in a later step |
| Backings | `bk[c][g]`: the buffer and charge of operation `g`. `own`, `held` (received bytes that the stream keeps), `zc` (the driver keeps it for the zero-copy notice of send `g`), `out` (released), or `dbl` (released twice) |
| Stream bytes | `Bytes = 3` from the peer. `rbuf` is the buffer of the receive, `held` the bytes that the stream keeps, and `got` the bytes that the caller received |
| Send progress | `tp`: `part` (partial or failed) or `full`. `wd` is the write direction, and `rep[g]` counts the progress reports of send `g` |
| Pending notices | `pn`: the sends whose result had `IORING_CQE_F_MORE` and whose notice has not arrived. `nh[g]`: the home of the notice of send `g`, selected when the task ends, as for a slot. The submissions bound the set, as each send's charge bounds it in the design |
| Connections | `Conns = 1` connection waits on the listener. `ar` is in the accept, `hc` is held by the listener, and `uc` is with the caller |
| Waker | `wake`: the waker of the latest poll of the receive future. `woke[c]`: the latest waker of the future in slot `c` is woken |
| Detached debt | `dt`: close returned and left the stream's pending notices, with their buffers, charges, and the descriptor, to the driver. `pg`: a reset ended the kernel's hold of the stream's queued bytes, so every pending notice comes from the kernel |

## Physical assumptions

Each assumption is a fact of the kernel, the device, the peer, the clock, or memory that the model
uses. The model gives fairness only to a step that a physical cause forces. Sources were read on
2026-11-07.

| Assumption | Primary source | Fairness in the model, and why |
| --- | --- | --- |
| A zero-copy send has two events: the send result gives the final progress, and the notice comes when the kernel releases the pages. The direction can start the next send after the result; the buffer stays owned until the notice | io_uring_prep_send_zc(3) (the first completion has `IORING_CQE_F_MORE`, and a second completion with `IORING_CQE_F_NOTIF` follows); the design's terminal-event table | The result is a `Complete("tx")` step under the same fairness as any result. The notice is a separate `Notice(g)` step |
| TCP frees sent pages only when the peer acknowledges them, so a zero-copy notice waits for the peer. A peer can keep its window closed forever while it answers probes, and a peer that acknowledges late adds its delayed-acknowledgement time to each notice | Linux v6.1 `net/ipv4/tcp_input.c` ("Remove acknowledged frames from the retransmission queue"); v6.1 `io_uring/notif.c` (the notice is queued when the last reference to the pages drops); `Documentation/networking/msg_zerocopy.rst` ("the kernel returns a completion notification when it releases its hold on the shared pages"); RFC 1122 sections 4.2.2.17 (a sender stays open indefinitely while the receiver answers zero-window probes) and 4.2.3.2 (delayed acknowledgement) | None before a reset. `Notice(g)` is fair only with `pg` |
| A reset purges the send queue of the connection. Every pending notice then comes from the kernel and the device, with no peer | connect(2): a TCP socket "may dissolve the association by connecting to an address with the sa_family member of sockaddr set to AF_UNSPEC"; Linux v6.1 `net/ipv4/af_inet.c` (`__inet_stream_connect` calls `disconnect` for `AF_UNSPEC`) and `net/ipv4/tcp.c` (`tcp_disconnect` sends a reset and calls `tcp_write_queue_purge`) | Weak fairness on `Notice(g)` for each pending notice once `pg` holds |
| After a cancel that finds its target, the kernel posts the target's own completion. An IOCP packet that the system queued after an immediate return arrives | io_uring_prep_cancel(3), NOTES; Microsoft Learn, "CancelIoEx" and "I/O Completion Ports" | Weak fairness on `Complete(c)` for `cxl` and `tail` |
| Every cancel result waits for the target's own terminal event. A current cancel finds each socket operation in flight, because no socket operation runs in io-wq. No cancel ends a pending notice: its send has ended | io_uring_prep_cancel(3); kernel commits `8aa0e5db4845` and `fe6ca13c98b1` (2026-10-15, for Linux 7.4; both defects need io-wq); v6.1 `io_uring/cancel.c` (`io_cancel_req_match` matches only its own ring) | `Process` makes `fly` into `cxl`, and it changes nothing else ("not found"). It is fair because the home keeps running |
| A timer that the runtime owns fires, because its home worker keeps running and its monotonic clock advances. The timers close at grace expiry | The design: "The home is driven until that release, including while it has no runnable task" (Driver Topology); "timer closure" is a distinct error (Startup, Shutdown, and Failures); clock_gettime(2), `CLOCK_MONOTONIC` | The drain limit is weak fairness on `Reset` while `life = "run"`. Grace expiry is weak fairness on `Reset` and on `EndTask` while `life = "down"` |
| The peer, the consumer, and the shutdown request are free | RFC 9293 (the peer controls its window and its data); the design gives the caller no obligation | None. `fly` events, every consumer step, `Shutdown`, and a reset under budget pressure have no fairness |
| Buffer budgets are finite, so admission can refuse, and the pressure reset is a real step | The design: "The owner retains bounded debt, stops admission before that debt exhausts its budget" (Completion I/O and Buffers) | The model has no budget variable. `Reset` can come at any time while `dt` holds, and that step stands for the pressure reset. The order (oldest first) and the refusal record belong to the R603 tests; a full table refuses in R203 |

Each fairness condition names the failure that it could hide, and a known-bad variant with that
failure still fails under it:

| Fairness | Failure that it could hide | Known-bad variant that still fails |
| --- | --- | --- |
| `Process(c, g)` | A close that waits for a cancel that nobody posted | `CloseNoCancel_bad` (`CloseReturns`) |
| `KernelEvent(c) /\ Complete(c)` | A wait on an event that the peer controls, or a backing released at the send result | `CloseNoCancel_bad`, and `ZcWait_bad` for the zero-copy notice (`CloseReturns`); `ZcEarlyRelease_bad` (`ReleaseOnce`) |
| `pg /\ Notice(g)` | Detached debt that no reset ends | `NoDrainLimit_bad` (`DetachedReleased`), `NoGraceReset_bad` (`ShutdownJoins`) |
| `TakeOver(c)` and `CloseDone(r)` while closing | A close that returns before a terminal event, or one that waits for an event that cannot arrive | `CloseNoCancel_bad`, `ZcWait_bad`; `DropRelease_bad` (`ReleaseOnce`) for the release |
| `EndTask` after shutdown | A shutdown that joins while a home still holds a live operation | `CloseNoCancelJoin_bad` (`ShutdownJoins`) |
| `Exit(w)` | A worker that exits with live operations | `ExitLive_bad` (`NoImpossibleWait`) |
| The drain limit, while the runtime runs | Detached debt after the timers closed | `NoGraceReset_bad` (`ShutdownJoins`) |
| The reset at grace expiry | Detached debt while the runtime runs | `NoDrainLimit_bad` (`DetachedReleased`) |
| The wake of the latest waker | A wake that goes to an old waker | `StaleWaker_bad` (`LatestWakerWoken`) |

## Reductions and their soundness

Each reduction removes states or steps that no property can tell apart, or adds behaviors. After
all of them, each known-bad variant still fails its own property (see the evidence record): that
is the check that no reduction hid a failure.

1. The home is selected when the task ends. In the design, the home is the worker that submitted
   the operation. While the task is alive, both workers run, and no step reads a home:
   `Complete` and `Process` need a driven home (`Driven`), which every running worker is, and
   `Exit` needs the task to end. So a selection at submission and a selection at `EndTask` give
   the same behaviors. The design treats the home and another worker differently only for a
   cancel or close request: from another thread it goes through the request entry, and on the
   home it can act at once. The model sends every request through an entry, and the home takes
   it in a later step (`Process`). The direct path is the case where `Process` is the next step.
   On the entry path, a completion, a drop, a forget, or a slot reuse can come between.
   Witnesses: `CancelRacesCompletion_reach`, `CancelRacesDrop_reach`, `StaleRequest_reach`,
   `CancelFound_reach`, `CancelNotFound_reach`, and `ShutdownCross_reach`. Each new operation in
   a direction reuses the same slot, also on another home. This adds stale-request races that
   one slot table for each home does not have; it removes none. Known-bad: `NoGeneration_bad`.
2. Mirror images are removed. Each step and each property treats the workers the same. `EndTask`
   gives the first held slot one fixed worker (`W0`). Each removed behavior is the mirror image
   of a kept behavior, and a property holds for a behavior only if it holds for its mirror image.
   This is not TLC symmetry reduction, which can be unsound with liveness: the model keeps one
   choice from each set of images. With no held slot, the first pending notice gets `W0`.
   Known-bad: `ExitLive_bad`.
3. The abandoned rule applies in the terminal step. The design lets the next operation in the
   direction, the close, or a driver that knows of the drop apply it. The model applies it in
   the step that makes an operation both terminal and abandoned: `Complete` with no future, or
   `Drop` or `Forget` of a terminal result. After the task ends, `TakeOver` applies it. Between
   the two points, the slot holds a result that no future can take. Only `Begin` in the same
   direction and `Exit` of its home read that slot, and both wait for it. A later application
   only delays them, and the model also has each delay. Known-bad: `SubmitFirst_bad`.
4. The shutdown request and grace expiry are one step. During the grace period, the consumer
   can do what it can do before the request; the model has no task admission. So a behavior with
   work in the grace period is also a behavior with that work before `Shutdown`. Progress from
   the request follows from progress from expiry, because the grace timer fires. After this step,
   the timers have closed, so the drain limit has no fairness then. Known-bad:
   `NoGraceReset_bad`.
5. The kernel posts an event and the home harvests it in one step. Between the two, the driver
   does not see the event. A cancel in between finds no target in flight; the model has this
   case, because after a cancel the terminal event can carry any progress. A drop in between is
   the same as a drop before the post. A worker cannot exit in between, because the slot is held.
   Known-bad: `DropRelease_bad`.
6. The receive future changes its waker once. Only the latest waker counts. In the correct model
   each wake goes to the latest waker, so `wake` changes no other step. One change keeps the
   failure of `StaleWaker_bad`; a change back to the first waker would only hide it.
7. "done" and "already running" are one state. After each cancel result, the driver waits for
   the target's own terminal event, and that event can carry any progress. "Not found" changes
   nothing. Known-bad: `NoGeneration_bad` (a request that acts where it must not).
8. A claim returns the result and the slot in one step. A later slot return only delays the next
   operation in the direction and the exit of the home, and the model also has each delay.
9. The domains. One connection keeps each accept rule; a second multishot item repeats the same
   step. The send direction has only partial and full progress, because its rules depend only on
   that. Three bytes give a prefix, a remainder, and a later byte. These are argued, not proven,
   so each one has a raise-by-one profile for the evidence host.
10. The drain limit, the pressure reset, and grace expiry are one `Reset` step, and it purges
    every pending notice of the connection at once, as the kernel's purge of the send queue does.
    The model has no time and no budget. `Reset` can come at any time while `dt` holds (budget
    pressure), it is fair while the runtime runs (the drain limit), and it is fair after shutdown
    (grace expiry). Which of the three comes first changes only the time at which the notices
    follow. Known-bad: `NoDrainLimit_bad` and `NoGraceReset_bad`.
11. Each liveness known-bad variant runs at `Bytes = 1` and `MaxOps = 1`. TLC checks liveness
    only after the complete search, so these bounds keep the run short. Each trace needs one
    submission and no byte. The consumer and the peer have no fairness, so a fair behavior at
    these bounds is also a fair behavior at the declared bounds: there the consumer submits no
    more, and the peer sends no more.
12. The pending set has no bound of its own. The design bounds it by the charge of each pending
    buffer, and the model bounds it by the submissions: each send is one submission and one
    charge, so `pn` has at most `MaxOps` members. Witnesses: `TwoPending_reach` and
    `DetachTwo_reach`.

## Correspondence

Each action is one atomic step. The rule column names the design rule that the action implements;
the R203, R206, and R603 transitions get a column when that code exists.

| Action | Design rule | Atomicity |
| --- | --- | --- |
| `BeginRx` | Driver Topology: one outstanding operation for each direction; the next receive returns held data before it submits, as a prefix with one copy, and keeps the rest. Each operation future borrows its direction exclusively, so no earlier future of the direction exists | One poll: the check of held data, or the submission and the return of the initiating call |
| `BeginTx` | Driver Topology: after a partial or failed abandoned send, the next send returns a typed error with the transferred count. A next send that succeeds shows that each earlier abandoned send transferred all its bytes | One poll |
| `BeginAc` | Driver Topology: the next accept returns a connection that the listener holds before it submits | One poll |
| `Submit` (in `Begin*`) | Completion I/O: accepted submission gives the backing to the driver. Terminal-event table: an io_uring request waits for its completion; IOCP classifies the NT status of the return (pending, `NT_ERROR`, other, skip-on-success) | The submission and the return of the initiating call are one step |
| `Repoll` | Future and Wake Contract: a pending poll gives a new waker, and the future honors the latest one | One poll |
| `Claim` | Future and Wake Contract: the owned result and the slot return exactly once each. The claim of a zero-copy send returns its progress; its notice releases the backing | One step for both returns |
| `Cancel` | Driver Topology: an explicit cancellation returns the original buffer with its exact progress; a send direction stays usable | Posts one request entry |
| `Drop` | Driver Topology: a dropped future ends interest, not the operation. A receive and an accept ask their home to cancel; a send is not cancelled. A terminal result goes by the abandoned rule | Posts at most one request entry |
| `Forget` | Driver Topology: a forgotten future ends interest with no destructor, so no request; `mem::forget` ends the exclusive borrow | One step |
| `Complete` | Terminal-event table: one event at the home. Single completion; multishot item with `IORING_CQE_F_MORE` and its final completion; IOCP packet. A zero-copy send result ends the send and frees its direction (selected on 2026-11-08); with `IORING_CQE_F_MORE`, its backing joins the pending notices. With no waiting future, the abandoned rule applies | The kernel posts the event and the home harvests it in one step |
| `Process` | Driver Topology: the request travels to the home through its entry, and close cancels each operation by its own `user_data`. Future and Wake Contract: the generation check rejects a stale identity. Completion I/O: a cancel result is not a terminal event; "done" and "already running" make the target's own event certain; "not found" changes nothing | One step at the home |
| `TakeOver` | Driver Topology: close takes each result that a dropped future left, by the abandoned rule | One step |
| `CloseDone` | Driver Topology: close waits for the terminal event of each operation, releases the data that no receive took, reports each owed and each cancelled send's progress, and closes each connection that no accept took. It does not wait for the pending notices: it half-closes, returns, and turns every pending notice of the stream into detached debt | One step after the last terminal event |
| `Reset` | Driver Topology: `connect` with `AF_UNSPEC` at the drain limit, under budget pressure, or at grace expiry | One step: the purge of the send queue, for every pending notice of the connection |
| `Notice(g)` | Terminal-event table: the zero-copy notice of send `g` releases its own buffer and charge, once. After the last notice of detached debt, the descriptor closes | The kernel posts the notice and the home of `g` harvests it in one step |
| `Shutdown` | Startup, Shutdown, and Failures: shutdown closes ingress, and grace expiry escalates | The request and grace expiry are one step |
| `EndTask` | Startup, Shutdown, and Failures: the task returns, or grace expiry cancels it. Its futures drop. Driver Topology: close first cancels each outstanding operation, a dropped send included. Each held slot and each pending notice gets the home of its submission | One step for the drops and the closes |
| `Exit` | Driver Topology: a worker with live operations or pending notices cannot exit until they reach terminal release. Startup, Shutdown, and Failures: no worker exits while a task of the runtime remains | One step |
| `Terminated` | None. The end state stutters, so deadlock checking stays on | One step |

## Properties

| Property | Acceptance text | Kind |
| --- | --- | --- |
| `ReleaseOnce` | Each buffer and charge is released exactly once and never before its terminal event. A send's backing stays with the driver exactly while its notice is pending. No accepted connection is lost | Invariant |
| `NoImpossibleWait` | Teardown never waits for an event that cannot arrive: each live operation has an event to come, and the home of each live operation and each pending notice is driven | Invariant |
| `InOrderOnce` | The caller receives each byte once and in order across abandoned receives | Invariant |
| `NoStaleEffect` | A stale identity never changes a reused slot | Invariant |
| `ReportOnce` | Each abandoned send that fails or transfers only part of its bytes, and each send that close cancels, has its progress reported exactly once | Invariant |
| `CloseReturns` | Close returns | `[]<>`, equal to `closing ~> closed` |
| `ShutdownJoins` | Shutdown joins every worker | `[]<>`, equal to `down ~> Done` |
| `DetachedReleased` | While the runtime runs, also with no shutdown request, each detached buffer is released. Detached debt ends only when its last pending notice releases its backing, so this covers each pending backing | `[]<>`, equal to `dt ~> ~dt` |
| `LatestWakerWoken` | A waiting consumer's latest waker is woken | `[]<>`, equal to `Ready ~> (woke \/ ~Ready)` |
| `StablePhases` | The side condition of the two arguments below | Action property |
| Deadlock | No state deadlocks | TLC deadlock check, on |

`TypeOK` is also an invariant of each main profile. The progress properties use the `[]<>` form,
which TLC checks with no tableau. For each property, `P ~> Q` gives `[]<>(~P \/ Q)`. The other
direction needs an argument:

- `CloseReturns`: `P` is `cl[r] = "closing"` and `Q` is `cl[r] = "closed"`. `StablePhases`
  checks that "closing" leaves only to "closed". So when the form gives a state that is not
  "closing" after a "closing" state, that state is "closed".
- `ShutdownJoins`: `P` is `life = "down"` and `Q` is `Done`. `StablePhases` checks that "down"
  stays. So from a "down" state, `~P` never holds again, and the form gives `Done`.
- `DetachedReleased` and `LatestWakerWoken`: `Q` includes `~P`, so `[]<>(~P \/ Q)` is `[]<>Q`,
  and `[]<>Q` gives `P ~> Q` with no side condition.

A full abandoned send reports nothing (selected on 2026-11-07): `ReportOnce` checks the reports
that are owed, and a next send that succeeds is the report of a full transfer.

## Bounds, faults, and fairness

The declared bounds: two workers, one moving consumer, one stream and one listener, two operation
submissions on each resource in one run (the receive and the send of the stream share two), three
bytes on the receive direction, and one connection on the listener. A larger bound needs only a
change of the constants in a `.cfg` file. The wide profiles and the raise-by-one profiles run on
the evidence host.

The faults and free choices:

- The kernel selects each result from the terminal-event table of the backend. A receive takes
  0 to all remaining bytes; 0 is an error or a cancel. A send is partial (or failed) or full. An
  accept gets the connection or an error. On io_uring, a send result can have
  `IORING_CQE_F_MORE` (a zero-copy notice follows), and an accept can give a multishot item. On
  IOCP the return is `STATUS_PENDING`, `NT_ERROR`, or another status, with or without
  skip-on-success.
- A cancel finds its target in flight, or does not find it.
- The peer can refuse to read or to send forever, and it can hold a zero-copy notice forever.
- The consumer can start, poll with a new waker, claim, cancel, drop, forget, and end at any time,
  in any order. It has no fairness.
- Shutdown and a reset under budget pressure can come at any time, or never.

The fairness is weak fairness for each step that a physical cause forces, and for nothing else
(see [Physical assumptions](#physical-assumptions)).

## Known-bad variants

Each variant changes one rule through the constant `Bug`, and its profile checks only its own
property. The safety variants run at the declared bounds; the liveness variants run at the
bounds of reduction 11.

| Profile | Changed rule | Must violate |
| --- | --- | --- |
| `DropRelease_bad` | The driver releases the backing when the future drops | `ReleaseOnce` |
| `Win32_bad` | IOCP classifies the return by the Win32 value: a warning status is taken as terminal, but its packet follows | `ReleaseOnce` |
| `ExitLive_bad` | A worker exits with live operations | `NoImpossibleWait` |
| `SubmitFirst_bad` | A receive submits before it returns held data | `InOrderOnce` |
| `NoGeneration_bad` | The home does not check the generation of a request | `NoStaleEffect` |
| `ReportTwice_bad` | Close reports send progress that a send error already reported | `ReportOnce` |
| `CloseNoCancel_bad` | Close does not cancel, and the peer refuses to read | `CloseReturns` |
| `CloseNoCancelJoin_bad` | The same rule change | `ShutdownJoins` |
| `StaleWaker_bad` | The home wakes the waker of the first poll | `LatestWakerWoken` |
| `ZcWait_bad` | The design before 2026-11-07: close waits for every pending notice, and nothing resets | `CloseReturns` |
| `ZcEarlyRelease_bad` | The mirror error of the rule of 2026-11-08: the driver releases a zero-copy backing at its send result, not at its notice | `ReleaseOnce` |
| `NoDrainLimit_bad` | Option C as first proposed: no drain limit | `DetachedReleased` |
| `NoGraceReset_bad` | Grace expiry does not reset detached debt | `ShutdownJoins` |

The counterexamples, as actions and observations:

- `ZcWait_bad` (`CloseReturns`): `BeginTx`; `Complete("tx")` (a result with
  `IORING_CQE_F_MORE`: the send ends, and its notice is pending); `Drop("tx")` (the abandoned rule
  takes the result); `Shutdown`; `EndTask`; `CloseDone("l")`; `Exit(w2)`; then stuttering. The
  peer does not acknowledge, so the stream stays "closing".
- `NoDrainLimit_bad` (`DetachedReleased`): `BeginTx`; `Complete("tx")` (pending notice);
  `Drop("tx")`; `EndTask` (the task returns while the runtime runs); `CloseDone("s")` (close
  returns, `dt` holds); `CloseDone("l")`; then stuttering. No shutdown comes and the peer does not
  acknowledge, so the buffer stays detached while the runtime runs.
- `NoGraceReset_bad` (`ShutdownJoins`): `BeginTx`; `Drop("tx")` (a send is not cancelled);
  `Shutdown`; `Complete("tx")` (pending notice); `EndTask`; `Exit(w2)`; `CloseDone("s")` (detached
  debt after grace expiry); `CloseDone("l")`; then stuttering. The timers have closed and nothing
  resets, so the home of the notice cannot exit.
- `ZcEarlyRelease_bad` (`ReleaseOnce`): `BeginTx`; `Shutdown`; `EndTask`; `Complete("tx")` (a
  result with `IORING_CQE_F_MORE`, and the variant releases the backing). The notice is still
  pending while the backing is released.
- The other variants keep the traces of the first run (2026-11-06): for example `DropRelease_bad`
  is `BeginRx`, `Drop("rx")`, and the drop releases the receive buffer while the receive is in
  flight. Each trace is in `target/model/operation-ownership/<profile>/trace.json`.

## Reachability

Each witness profile must reach its target.

| Profile | Backend | Target | Shows |
| --- | --- | --- | --- |
| `UringSingle_reach` | io_uring | A receive completion is terminal | Terminal event: single completion |
| `MultishotFinal_reach` | io_uring | An accept ends with its final completion after an item | Terminal event: multishot final |
| `ZcResult_reach` | io_uring | A send result with `IORING_CQE_F_MORE` ends the send and frees its slot, and its notice is pending | Terminal event: zero-copy send result |
| `ZcNotice_reach` | io_uring | A zero-copy notice releases the backing of its send | Release event: zero-copy notice |
| `ZcNoNotice_reach` | io_uring | A send result with no `IORING_CQE_F_MORE` is terminal, and no notice is pending | Terminal event: zero-copy send with no notice |
| `TwoPending_reach` | io_uring | Two zero-copy notices are pending on one stream | The direction is free at the send result |
| `IocpPending_reach` | IOCP | After `STATUS_PENDING`, the packet is terminal | Terminal event: IOCP pending |
| `IocpError_reach` | IOCP | An immediate `NT_ERROR` return is terminal | Terminal event: IOCP immediate error |
| `IocpOtherPacket_reach` | IOCP | After an immediate success, the packet is terminal | Terminal event: IOCP immediate other |
| `IocpSkip_reach` | IOCP | With skip-on-success, the immediate return is terminal | Terminal event: skip-on-success |
| `CancelFound_reach` | io_uring | A cancel finds its target in flight | Cancel result: "done" or "already running" |
| `CancelNotFound_reach` | io_uring | A current request for a send finds no target in flight, because the send result came first, and its notice follows | Cancel result: "not found" |
| `HeldBytes_reach` | io_uring | A receive returns a prefix of held bytes and keeps the rest | Abandoned receive |
| `SendError_reach` | io_uring | The next send reports an abandoned send's progress | Abandoned send |
| `HeldConn_reach` | io_uring | The next accept returns a held connection | Abandoned accept |
| `CloseReport_reach` | io_uring | Close returns and reports an abandoned send's progress | Close with an abandoned send |
| `CancelledData_reach` | io_uring | An explicit cancellation returns received bytes in the original buffer | Explicit cancellation |
| `CloseLive_reach` | io_uring | The stream closes with a live receive and a live send | Close with live operations |
| `ShutdownCross_reach` | io_uring | After shutdown, a close request waits at one home while the other home holds a live operation | Shutdown with live operations across workers |
| `DetachedClose_reach` | io_uring | Close returns with a zero-copy notice pending | Detached debt |
| `DetachTwo_reach` | io_uring | A close detaches two pending notices | Detached debt of the whole set |
| `DetachedAck_reach` | io_uring | The peer's acknowledgement ends detached debt with no reset | Detached debt, healthy peer |
| `DetachedReset_reach` | io_uring | A reset ends detached debt, and the notice follows | Detached debt, reset |
| `StaleRequest_reach` | io_uring | A stale request waits while its slot holds a new operation | `NoStaleEffect` is not vacuous |
| `CancelRacesCompletion_reach` | io_uring | An operation reaches its terminal event while its cancel request waits | A request races with completion |
| `CancelRacesDrop_reach` | io_uring | A future drops or is forgotten while its cancel request waits | A request races with a drop |
| `NewWaker_reach` | io_uring | The home wakes a waker that a later poll gave | `LatestWakerWoken` is not vacuous |

## Limits

The model does not cover these items. Each one is outside the task, or a later task owns it.

- Datagrams: the receive metadata and the truncation rule. Multishot receive, provided buffer
  rings, and bundles. Positional file operations.
- The epoll, kqueue, Windows IoRing, Registered I/O, and blocking-service rows of the
  terminal-event table have no profile of their own. Their shapes are modeled shapes: a return
  that is terminal, or a release event after a known result. The epoll `MSG_ZEROCOPY` notice has
  the shape of the io_uring notice, and the same rules: the call's return ends the send, and the
  notice releases the buffer.
- Time and capacity. The drain limit, grace expiry, and the pressure reset are steps with no
  duration, and the model has no budget, so "oldest first" and the refusal record are R603
  tests.
- Driver-only use has no profile of its own. The model's workers are the homes that the
  schedulers keep driven. In driver-only use, the caller drives the home, so the fairness of the
  steps at a home (`Process`, the kernel's events after a cancel or a reset, and the drain-limit
  reset) holds only while the caller keeps driving. The design states that condition ("An
  outstanding operation's home must remain alive and driven until terminal resource release").
  So each progress result here is a result for driver-only use under that condition. The
  driver's own drain-limit setting and terminal teardown take the place of the grace period and
  grace expiry (selected on 2026-11-08), and R603 tests them.
- The IOCP home rule (a movable resource binds to the pool's port). Each home is the worker that
  submitted the operation, as on io_uring.
- Skip-on-success is a choice for each operation, not for each handle, and a stream receive can
  return a warning status. Both choices only add behaviors.
- After a cancel, the terminal event can carry any progress, also on io_uring "done". This only
  adds behaviors.
- The send direction has no byte identities: its rules depend only on a partial or a full
  transfer. The receive direction has the three bytes.
- One accept at a time; concurrent accepts on one listener are not modeled. An accept has no
  explicit cancellation, and only a receive is forgotten (a forgotten send is the same as a
  dropped send).
- One consumer task. Close happens when the task ends; an explicit close while the task uses the
  other resource is not modeled. Workers exit only after the task ends, which is now a design
  rule (no worker exits while a task of the runtime remains).
- The wake path (the remote-ready list and one unpark for each batch), the waker handshake, and
  the generations of task slots (stale wakes). A wake is part of the step that makes a future
  ready.
- The drain before grace expiry, partial startup, panics, and unwinding.
- The model proves no Rust code. When R203, R206, and R603 exist, the correspondence table gets
  their transitions, and a change to either side reopens this check.

## Decisions of 2026-11-07

The first run (2026-11-06) passed, because the model gave the zero-copy notice a fairness that the
design does not give. The decision round of 2026-11-07 found the defect (`ZcWait_bad`), and the
design owner selected these rules on 2026-11-07. The proposal files of that round were never
committed; this section keeps their record.

**Close with a zero-copy notice: option C with a drain limit (selected).** Close returns; the
driver keeps the send as detached debt; a reset ends it at the drain limit (one grace period
after the close), under budget pressure (oldest first), or at grace expiry. It keeps the
semantics of a copy send, where close returns and the kernel delivers the queued bytes. The
drain limit is needed: without it, a server that runs for months keeps detached debt until it
shuts down (`NoDrainLimit_bad`). The reset at grace expiry is needed: the timers close then
(`NoGraceReset_bad`).

- Rejected, option A (close waits for a close linger, then resets). Close waits on a slow peer for
  the length of a new setting, and that setting truncates a healthy stream whose acknowledgement
  takes longer. Evidence: the proposal profile `ZcReset_proposal` passed every safety property,
  `CloseReturns`, and `ShutdownJoins` in the decision round, so the choice was about cost, not
  safety.
- Rejected, option B (close resets at once). It truncates a healthy stream that closes within
  about one round trip after a zero-copy send. Evidence: the same profile as A, because the model
  has no time.
- Rejected, option C as first proposed (`TCP_USER_TIMEOUT` and the reset at grace expiry only).
  `TCP_USER_TIMEOUT` ends the wait only for a peer that stops completely (Linux 5.11 and later,
  commit `9d9b1ee0b2d1`), and a peer that opens its window a little before each timeout keeps the
  debt until shutdown. Evidence: `NoDrainLimit_bad` violates `DetachedReleased`. The drain limit
  bounds a stopped peer and a slow peer on each supported kernel, so the timeout adds nothing.

**One cancel rule, with a cancel for each operation (selected).** Each cancel result waits for the
target's own terminal event. Close cancels each operation by its own `user_data`, and no socket
operation sets `IOSQE_ASYNC` or `IOSQE_IO_DRAIN`. Rejected: one cancel request for the descriptor
with `IORING_ASYNC_CANCEL_ALL`. Before Linux 7.4, that search can miss a request that io-wq parked
in the poll table (commit `fe6ca13c98b1`, read on the GitHub mirror on 2026-11-07), and its result
is only a count.

**forget (selected).** Each operation future borrows its direction exclusively, and a half has no
`Clone`. A compile check with the pinned toolchain (rustc 1.101.0-nightly 3e92e1a2d 2026-11-01)
showed that `mem::forget` of a `Recv<'a>` that holds `&'a mut ReadHalf` ends the borrow, and that
two live receive futures on one half fail with E0499. Rejected: a `&self` receive, with which a
forgotten future and a live one cannot be told apart.

**A full abandoned send (selected).** It reports nothing; a next send that succeeds is its
report. Rejected: a report for each abandoned send, which costs work on the success path and
which no consumer needs.

**Bounds (selected).** Two submissions on each resource, as declared, and one search at two
submissions on each direction on the evidence host. Rejected: the wider reading as the declared
bound. A scratch run in the decision round had more than 3.59 million states at depth 19 after
120 s, against 507,880 states for the declared bound, so it is too large for the routine run.

**Worker exit (selected).** No worker exits while a task of the runtime remains. Rejected: an
early exit, which needs a refusal on each placement path (spawn, factory, steal, and wake) and
releases only an idle thread and its ring.

## Decisions of 2026-11-08

**A zero-copy send frees its direction at its send result (selected on 2026-11-08).** The send
result gives the final progress and ends the operation; its slot frees, and the next send in the
direction can start. The driver keeps the buffer and its charge in the stream's set of pending
notices until each notice, and the charge bounds the set, so it needs no setting. Each notice
releases its own backing once. Close turns every pending notice into detached debt under the
resets of 2026-11-07, and a reset purges every pending notice of the connection.

- Rejected: the direction stays busy until the notice (the rule of 2026-11-07). The notice waits
  for the peer's acknowledgement, so each zero-copy send costs at least one round trip, and the
  peer's delayed-acknowledgement time, before the next send can start. That defeats zero-copy for
  rsnetwork's attached TCP form. A post-model review found this performance defect; no property of
  the earlier model fails on it, because the model has no time.
- The mirror error of the new rule is a release at the send result, which lets the kernel read
  freed pages. `ZcEarlyRelease_bad` makes it and violates `ReleaseOnce`.
- IOCP: no rule that both backends share changes what IOCP does. The changed actions
  (`CompleteTx`, `Claim`, the abandoned rule, `CloseDone`, `EndTask`, and `Exit`) change only in
  branches that need a result with `IORING_CQE_F_MORE` or a pending notice, and IOCP has neither.
  The IOCP profiles must still run again, because the module text changed; each one should give
  the state count of the record of 2026-11-07.

## Evidence record

### Evidence host record of the rule of 2026-11-08

One complete run of every profile on the evidence host on 2026-11-08, from 19:05:09 to 20:41:49
UTC. It is the main record of the current files: one command, one digest, and the original checker
exit status of each profile. It includes the main, wide, and raise-by-one profiles (rule 4 of
[FP Design Models]) on both backends.

- Command: `cargo xtask model operation-ownership/OperationOwnership.Uring.cfg
  operation-ownership`, from the repository root, with a limit of `timeout 28000`. The first
  selection puts the main profile first; the command runs each of the 52 profiles once. Exit
  status 0.
- Source: the files of this directory, not yet committed, on top of commit
  `3ad84f8813b29badc0e0ff725075aea52d40062f`. The xtask printed
  `sha256:48cb71030ec27b3eb01114a3970fe0a79081423c7b161f09c6a88cd4fe23abaa (53 files)`, the
  digest of the files as they are now.
- Checkers: temurin-jre 21.0.13+9 (Linux aarch64, SHA-256 checked against Adoptium), tla2tools
  1.8.0+d8fb231de4e4 (TLC2 Version 2026.11.06.222357, rev d8fb231; the pinned jar, SHA-256
  `e1f11b75c4b5bc4478ba0ac5cb6ae28e40a01b0016bc33e0aa97da22dfdbcd7f` checked), with `-workers auto
  -lncheck final` (32 workers, 14,028 MB heap).
- Host: EC2 c8g.8xlarge (32 vCPUs, 64 GiB), us-east-1c, image `ami-06b1a03fa3561aefb` (Ubuntu
  26.04 arm64, 20261104.1), kernel 7.0.0-1016-aws, instance `i-02cd2238d360c7437`. No other checker
  ran during the run; one short build of the rsauthnz xtask ran at about 19:59. The instance is
  terminated.
- Replay: the same command writes each profile's output to
  `target/model/operation-ownership/<profile>/` (`checker.log`, and `trace.json` for a
  violation). The host's own logs are not kept; this record holds their figures.
- Exit: the checker's own exit status, as the xtask printed it: 0 for a complete search with no
  violation, 12 for an invariant violation, 13 for an action or temporal property violation. TLC
  stops a known-bad or reachability search at the first violation that a worker finds, so those
  state counts change with the number of workers; the verdicts do not.

| Profile | Bounds | Result (checker exit) | Distinct states | Depth | Duration (s) |
| --- | --- | --- | --- | --- | --- |
| `Uring` | Declared | Pass (0) | 970,534 | 23 | 73.3 |
| `Iocp` | Declared | Pass (0) | 648,946 | 21 | 45.8 |
| `UringWide` | Two submissions on each direction | Pass (0) | 22,058,438 | 30 | 1851.4 |
| `IocpWide` | Two submissions on each direction | Pass (0) | 7,752,242 | 28 | 636.6 |
| `UringWorkers3` | Three workers (raise) | Pass (0) | 2,198,912 | 24 | 170.7 |
| `IocpWorkers3` | Three workers (raise) | Pass (0) | 1,100,658 | 22 | 85.5 |
| `UringBytes4` | Four bytes (raise) | Pass (0) | 1,282,956 | 23 | 103.2 |
| `IocpBytes4` | Four bytes (raise) | Pass (0) | 873,266 | 21 | 62.6 |
| `UringConns2` | Two connections (raise) | Pass (0) | 1,852,358 | 25 | 149.5 |
| `IocpConns2` | Two connections (raise) | Pass (0) | 861,278 | 23 | 62.2 |
| `UringOps3` | Three submissions on each resource (raise) | Pass (0) | 19,962,870 | 30 | 1772.0 |
| `IocpOps3` | Three submissions on each resource (raise) | Pass (0) | 9,357,002 | 28 | 759.8 |
| `CloseNoCancelJoin_bad` | Small (reduction 11) | `ShutdownJoins` violated (13) | 8,938 | 17 | 1.0 |
| `CloseNoCancel_bad` | Small (reduction 11) | `CloseReturns` violated (13) | 8,938 | 17 | 1.0 |
| `DropRelease_bad` | Declared | `ReleaseOnce` violated (12) | 805 | 10 | 0.6 |
| `ExitLive_bad` | Declared | `NoImpossibleWait` violated (12) | 778 | 10 | 0.6 |
| `NoDrainLimit_bad` | Small (reduction 11) | `DetachedReleased` violated (13) | 8,794 | 17 | 1.1 |
| `NoGeneration_bad` | Declared | `NoStaleEffect` violated (12) | 3,907 | 10 | 0.7 |
| `NoGraceReset_bad` | Small (reduction 11) | `ShutdownJoins` violated (13) | 8,794 | 17 | 1.1 |
| `ReportTwice_bad` | Declared | `ReportOnce` violated (12) | 16,145 | 12 | 0.9 |
| `StaleWaker_bad` | Small (reduction 11) | `LatestWakerWoken` violated (13) | 8,794 | 17 | 1.0 |
| `SubmitFirst_bad` | Declared | `InOrderOnce` violated (12) | 10,695 | 12 | 0.9 |
| `Win32_bad` | Declared | `ReleaseOnce` violated (12) | 781 | 8 | 0.6 |
| `ZcEarlyRelease_bad` | Declared | `ReleaseOnce` violated (12) | 600 | 10 | 0.6 |
| `ZcWait_bad` | Small (reduction 11) | `CloseReturns` violated (13) | 7,332 | 15 | 0.9 |
| `CancelFound_reach` | Declared | Reached (12) | 512 | 10 | 0.6 |
| `CancelNotFound_reach` | Declared | Reached (13) | 906 | 10 | 0.6 |
| `CancelRacesCompletion_reach` | Declared | Reached (13) | 300 | 7 | 0.6 |
| `CancelRacesDrop_reach` | Declared | Reached (13) | 1,183 | 10 | 0.6 |
| `CancelledData_reach` | Declared | Reached (13) | 2,970 | 12 | 0.6 |
| `CloseLive_reach` | Declared | Reached (12) | 1,229 | 11 | 0.6 |
| `CloseReport_reach` | Declared | Reached (13) | 933 | 10 | 0.6 |
| `DetachTwo_reach` | Declared | Reached (13) | 41,710 | 12 | 0.9 |
| `DetachedAck_reach` | Declared | Reached (13) | 1,006 | 10 | 0.6 |
| `DetachedClose_reach` | Declared | Reached (12) | 940 | 10 | 0.6 |
| `DetachedReset_reach` | Declared | Reached (13) | 1,857 | 10 | 0.6 |
| `HeldBytes_reach` | Declared | Reached (13) | 2,361 | 11 | 0.6 |
| `HeldConn_reach` | Declared | Reached (13) | 2,560 | 11 | 0.6 |
| `IocpError_reach` | Declared | Reached (13) | 733 | 10 | 0.5 |
| `IocpOtherPacket_reach` | Declared | Reached (13) | 668 | 9 | 0.6 |
| `IocpPending_reach` | Declared | Reached (13) | 904 | 9 | 0.6 |
| `IocpSkip_reach` | Declared | Reached (13) | 1,099 | 10 | 0.6 |
| `MultishotFinal_reach` | Declared | Reached (13) | 995 | 10 | 0.6 |
| `NewWaker_reach` | Declared | Reached (12) | 2,112 | 11 | 0.6 |
| `SendError_reach` | Declared | Reached (13) | 4,070 | 11 | 0.7 |
| `ShutdownCross_reach` | Declared | Reached (12) | 1,391 | 10 | 0.7 |
| `StaleRequest_reach` | Declared | Reached (12) | 2,335 | 11 | 0.7 |
| `TwoPending_reach` | Declared | Reached (12) | 6,171 | 10 | 0.7 |
| `UringSingle_reach` | Declared | Reached (13) | 429 | 10 | 0.6 |
| `ZcNoNotice_reach` | Declared | Reached (13) | 542 | 10 | 0.6 |
| `ZcNotice_reach` | Declared | Reached (13) | 611 | 10 | 0.6 |
| `ZcResult_reach` | Declared | Reached (13) | 745 | 10 | 0.6 |

What the raises show: each argued bound raised by one passed on both backends, and each raise
added states to its declared profile, so each raise exercised its bound. Reading 2 (`UringWide`,
`IocpWide`) also passed. Each IOCP count is equal to the record of 2026-11-07, as the rule of
2026-11-08 predicts: it changes only branches that need a result with `IORING_CQE_F_MORE` or a
pending notice. Each io_uring count is in or below its estimate before the run (for example,
`UringWide` 22,058,438 against 15 to 40 million, and `UringOps3` 19,962,870 against 30 to 70
million).

### Budget check on a 16-vCPU host (2026-11-09)

The budget rule of [FP Design Models] (selected on 2026-11-09) limits each profile to one hour on
one host of the standing approval, an EC2 4xlarge with 16 vCPUs. The record above ran on 32
vCPUs, so the two largest profiles ran again on a 16-vCPU host, each alone on the host, at the
same digest.

- Command: `cargo xtask model operation-ownership/OperationOwnership.UringWide.cfg`, then the same
  command for `OperationOwnership.UringOps3.cfg`, from the repository root, at commit
  `3f28290`. The xtask printed
  `sha256:48cb71030ec27b3eb01114a3970fe0a79081423c7b161f09c6a88cd4fe23abaa (53 files)`.
- Checkers: as in the record above, with 16 workers and a 14,028 MB heap.
- Host: EC2 m8g.4xlarge (16 vCPUs, 64 GiB, AWS Graviton4), us-east-1a, image
  `ami-06b1a03fa3561aefb`, kernel 7.0.0-1016-aws, instance `i-0cc104d10ea31d875`, on demand.
  Before these two runs, the host ran two rsauthnz searches, one at a time; nothing ran beside
  either profile.

| Profile | Result (checker exit) | Distinct states | Depth | Duration (s) |
| --- | --- | --- | --- | --- |
| `UringWide` | Pass (0) | 22,058,438 | 30 | 2125.3 |
| `UringOps3` | Pass (0) | 19,962,870 | 30 | 1954.8 |

Both fit the rule, and the state counts are those of the record above. Each other profile took at
most 760 s on 32 vCPUs, under half the limit, so a run on 16 vCPUs also fits.

### Local runs of the rule of 2026-11-08

Local runs on 2026-11-08, after the rule of 2026-11-08. The Mac ran only the known-bad and
reachability profiles; the large profiles are for the evidence host.

- Command: `cargo xtask model operation-ownership`, from the repository root, with the 12
  evidence-host profiles set aside in a scratch directory for that command and put back after it,
  so the 40 small profiles ran under one host lock. Exit status 0.
- Source: the files of this directory, not yet committed, on top of commit
  `3ad84f8813b29badc0e0ff725075aea52d40062f`. The xtask printed
  `sha256:547ac1439f4f0c0272d1ce0792a011acbaafebf778d0bdfc694cd2f70b58f70c (41 files)`:
  `OperationOwnership.tla` and the 40 profiles below. The same method over all 53 checked files
  gives `sha256:48cb71030ec27b3eb01114a3970fe0a79081423c7b161f09c6a88cd4fe23abaa`, which the
  evidence-host run prints.
- Checkers: temurin-jre 21.0.13+9, tla2tools 1.8.0+d8fb231de4e4 (TLC with `-workers auto
  -lncheck final`), apalache 0.62.4 (not used).
- SANY parsed `OperationOwnership.tla` with no error before the first TLC run.
- Host: macOS 26.6.2 (25G83), arm64, 10 cores, 16 GB, shared with other sessions, so the
  durations are indicative only.
- Replay: `target/model/operation-ownership/<profile>/trace.json`, and the checker output in
  `checker.log` beside it.
- "TLC exit" is the code that TLC gives for that report (12 for an invariant, 13 for an action or
  temporal property); the xtask prints the checker's code only for a failure. Durations are in
  seconds.

| Profile | Backend | Bounds | Result | Distinct states | Depth | Duration |
| --- | --- | --- | --- | --- | --- | --- |
| `CancelFound_reach` | io_uring | Declared | `UnreachedCancelFound` violated (TLC exit 12, xtask exit 0) | 197 | 7 | 0.70 |
| `CancelNotFound_reach` | io_uring | Declared | `UnreachedCancelNotFound` violated (TLC exit 13, xtask exit 0) | 429 | 7 | 0.69 |
| `CancelRacesCompletion_reach` | io_uring | Declared | `UnreachedCancelRacesCompletion` violated (TLC exit 13, xtask exit 0) | 150 | 7 | 0.68 |
| `CancelRacesDrop_reach` | io_uring | Declared | `UnreachedCancelRacesDrop` violated (TLC exit 13, xtask exit 0) | 432 | 7 | 0.68 |
| `CancelledData_reach` | io_uring | Declared | `UnreachedCancelledData` violated (TLC exit 13, xtask exit 0) | 794 | 7 | 0.77 |
| `CloseLive_reach` | io_uring | Declared | `UnreachedCloseLive` violated (TLC exit 12, xtask exit 0) | 253 | 8 | 0.69 |
| `CloseNoCancelJoin_bad` | io_uring | Bytes 1, MaxOps 1 | `ShutdownJoins` violated (TLC exit 13, xtask exit 0) | 8,938 | 17 | 2.75 |
| `CloseNoCancel_bad` | io_uring | Bytes 1, MaxOps 1 | `CloseReturns` violated (TLC exit 13, xtask exit 0) | 8,938 | 17 | 2.22 |
| `CloseReport_reach` | io_uring | Declared | `UnreachedCloseReport` violated (TLC exit 13, xtask exit 0) | 293 | 7 | 0.76 |
| `DetachTwo_reach` | io_uring | Declared | `UnreachedDetachTwo` violated (TLC exit 13, xtask exit 0) | 23,292 | 10 | 1.51 |
| `DetachedAck_reach` | io_uring | Declared | `UnreachedDetachedAck` violated (TLC exit 13, xtask exit 0) | 639 | 7 | 0.72 |
| `DetachedClose_reach` | io_uring | Declared | `UnreachedDetachedClose` violated (TLC exit 12, xtask exit 0) | 670 | 7 | 0.75 |
| `DetachedReset_reach` | io_uring | Declared | `UnreachedDetachedReset` violated (TLC exit 13, xtask exit 0) | 6,829 | 8 | 1.21 |
| `DropRelease_bad` | io_uring | Declared | `ReleaseOnce` violated (TLC exit 12, xtask exit 0) | 175 | 7 | 0.73 |
| `ExitLive_bad` | io_uring | Declared | `NoImpossibleWait` violated (TLC exit 12, xtask exit 0) | 246 | 7 | 0.71 |
| `HeldBytes_reach` | io_uring | Declared | `UnreachedHeldBytes` violated (TLC exit 13, xtask exit 0) | 742 | 7 | 0.76 |
| `HeldConn_reach` | io_uring | Declared | `UnreachedHeldConn` violated (TLC exit 13, xtask exit 0) | 1,567 | 7 | 0.82 |
| `IocpError_reach` | IOCP | Declared | `UnreachedIocpError` violated (TLC exit 13, xtask exit 0) | 213 | 7 | 0.70 |
| `IocpOtherPacket_reach` | IOCP | Declared | `UnreachedIocpOtherPacket` violated (TLC exit 13, xtask exit 0) | 325 | 7 | 0.66 |
| `IocpPending_reach` | IOCP | Declared | `UnreachedIocpPending` violated (TLC exit 13, xtask exit 0) | 344 | 7 | 0.63 |
| `IocpSkip_reach` | IOCP | Declared | `UnreachedIocpSkip` violated (TLC exit 13, xtask exit 0) | 281 | 6 | 0.64 |
| `MultishotFinal_reach` | io_uring | Declared | `UnreachedMultishotFinal` violated (TLC exit 13, xtask exit 0) | 563 | 5 | 0.71 |
| `NewWaker_reach` | io_uring | Declared | `UnreachedNewWaker` violated (TLC exit 12, xtask exit 0) | 590 | 7 | 0.72 |
| `NoDrainLimit_bad` | io_uring | Bytes 1, MaxOps 1 | `DetachedReleased` violated (TLC exit 13, xtask exit 0) | 8,794 | 17 | 2.02 |
| `NoGeneration_bad` | io_uring | Declared | `NoStaleEffect` violated (TLC exit 12, xtask exit 0) | 3,044 | 10 | 0.78 |
| `NoGraceReset_bad` | io_uring | Bytes 1, MaxOps 1 | `ShutdownJoins` violated (TLC exit 13, xtask exit 0) | 8,794 | 17 | 1.67 |
| `ReportTwice_bad` | io_uring | Declared | `ReportOnce` violated (TLC exit 12, xtask exit 0) | 9,230 | 10 | 1.15 |
| `SendError_reach` | io_uring | Declared | `UnreachedSendError` violated (TLC exit 13, xtask exit 0) | 1,396 | 7 | 0.78 |
| `ShutdownCross_reach` | io_uring | Declared | `UnreachedShutdownCross` violated (TLC exit 12, xtask exit 0) | 519 | 7 | 0.69 |
| `StaleRequest_reach` | io_uring | Declared | `UnreachedStaleRequest` violated (TLC exit 12, xtask exit 0) | 1,295 | 7 | 0.78 |
| `StaleWaker_bad` | io_uring | Bytes 1, MaxOps 1 | `LatestWakerWoken` violated (TLC exit 13, xtask exit 0) | 8,794 | 17 | 1.82 |
| `SubmitFirst_bad` | io_uring | Declared | `InOrderOnce` violated (TLC exit 12, xtask exit 0) | 5,951 | 8 | 1.08 |
| `TwoPending_reach` | io_uring | Declared | `UnreachedTwoPending` violated (TLC exit 12, xtask exit 0) | 2,414 | 9 | 0.84 |
| `UringSingle_reach` | io_uring | Declared | `UnreachedUringSingle` violated (TLC exit 13, xtask exit 0) | 119 | 7 | 0.81 |
| `Win32_bad` | IOCP | Declared | `ReleaseOnce` violated (TLC exit 12, xtask exit 0) | 432 | 7 | 0.78 |
| `ZcEarlyRelease_bad` | io_uring | Declared | `ReleaseOnce` violated (TLC exit 12, xtask exit 0) | 204 | 7 | 0.69 |
| `ZcNoNotice_reach` | io_uring | Declared | `UnreachedZcNoNotice` violated (TLC exit 13, xtask exit 0) | 127 | 6 | 0.65 |
| `ZcNotice_reach` | io_uring | Declared | `UnreachedZcNotice` violated (TLC exit 13, xtask exit 0) | 296 | 7 | 0.74 |
| `ZcResult_reach` | io_uring | Declared | `UnreachedZcResult` violated (TLC exit 13, xtask exit 0) | 228 | 7 | 0.68 |
| `ZcWait_bad` | io_uring | Bytes 1, MaxOps 1 | `CloseReturns` violated (TLC exit 13, xtask exit 0) | 7,332 | 15 | 2.03 |

### Evidence host record of the rule of 2026-11-07

This record checked the module before the rule of 2026-11-08. The module has changed since then,
so these rows no longer hold for it. They stay as the record of that design.

One complete run of every profile on the evidence host on 2026-11-08, from 07:47:35 to 08:45:57
UTC. It is the main record of this model: one command, one digest, a host with no other
workload, and the original checker exit status of each profile. It includes the main, wide, and
raise-by-one profiles (rule 4 of [FP Design Models]) on both backends; the IOCP raise-by-one
profiles were added for this run.

- Command: `cargo xtask model operation-ownership`, from the repository root. Exit status 0.
- Source: the files of this directory, not yet committed. The xtask printed
  `45494d286e492a35c7095a9a0fd86221d82150df (49 files)`, the digest of the files as they are now.
  That digest is a Git object ID. From 2026-11-08 the xtask prints a SHA-256 digest from rscrypto
  instead, and the same 49 files give
  `sha256:a535f5931a50bcc19bb6dd867c6b963b92b06c9837649467e35309ad5d460f5a`; `shasum -a 256`
  over the same list gives the same value.
- Checkers: temurin-jre 21.0.13+9 (Linux aarch64, SHA-256 checked against Adoptium), tla2tools
  1.8.0+3ec1cc4f8c24 (TLC2 Version 2026.11.06.005304, rev 3ec1cc4; the pinned jar, SHA-256
  checked), with `-workers auto -lncheck final`.
- Host: EC2 m8g.4xlarge (16 vCPUs, 64 GiB), us-east-1a, image `ami-06b1a03fa3561aefb` (Ubuntu
  26.04 arm64, 20261104.1), kernel 7.0.0-1016-aws, instance `i-0b429594a0faf07f8`. The instance
  is terminated.
- Replay: the same command writes each profile's output to
  `target/model/operation-ownership/<profile>/` (`checker.log`, and `trace.json` for a
  violation). The host's own logs are not kept; this record holds their figures.
- Exit: the checker's own exit status, as the xtask printed it: 0 for a complete search with no
  violation, 12 for an invariant violation, 13 for an action or temporal property violation.

| Profile | Bounds | Result (checker exit) | Distinct states | Depth | Duration (s) |
| --- | --- | --- | --- | --- | --- |
| `Uring` | Declared | Pass (0) | 581,276 | 22 | 44.9 |
| `Iocp` | Declared | Pass (0) | 648,946 | 21 | 50.2 |
| `UringWide` | Two submissions on each direction | Pass (0) | 6,661,700 | 29 | 596.7 |
| `IocpWide` | Two submissions on each direction | Pass (0) | 7,752,242 | 28 | 717.4 |
| `UringWorkers3` | Three workers (raise) | Pass (0) | 1,071,492 | 23 | 92.8 |
| `IocpWorkers3` | Three workers (raise) | Pass (0) | 1,100,658 | 22 | 96.3 |
| `UringBytes4` | Four bytes (raise) | Pass (0) | 782,174 | 22 | 62.9 |
| `IocpBytes4` | Four bytes (raise) | Pass (0) | 873,266 | 21 | 70.6 |
| `UringConns2` | Two connections (raise) | Pass (0) | 1,109,632 | 24 | 88.0 |
| `IocpConns2` | Two connections (raise) | Pass (0) | 861,278 | 23 | 72.4 |
| `UringOps3` | Three submissions on each resource (raise) | Pass (0) | 8,135,220 | 29 | 717.8 |
| `IocpOps3` | Three submissions on each resource (raise) | Pass (0) | 9,357,002 | 28 | 868.2 |
| `DropRelease_bad` | Declared | `ReleaseOnce` violated (12) | 538 | 9 | 0.6 |
| `Win32_bad` | Declared | `ReleaseOnce` violated (12) | 459 | 7 | 0.6 |
| `ExitLive_bad` | Declared | `NoImpossibleWait` violated (12) | 478 | 7 | 0.6 |
| `SubmitFirst_bad` | Declared | `InOrderOnce` violated (12) | 7,512 | 9 | 0.7 |
| `NoGeneration_bad` | Declared | `NoStaleEffect` violated (12) | 3,904 | 10 | 0.6 |
| `ReportTwice_bad` | Declared | `ReportOnce` violated (12) | 10,521 | 10 | 0.7 |
| `CloseNoCancel_bad` | Small (reduction 11) | `CloseReturns` violated (13) | 7,014 | 16 | 1.0 |
| `CloseNoCancelJoin_bad` | Small (reduction 11) | `ShutdownJoins` violated (13) | 7,014 | 16 | 1.0 |
| `StaleWaker_bad` | Small (reduction 11) | `LatestWakerWoken` violated (13) | 6,332 | 16 | 1.0 |
| `ZcWait_bad` | Small (reduction 11) | `CloseReturns` violated (13) | 5,544 | 15 | 0.9 |
| `NoDrainLimit_bad` | Small (reduction 11) | `DetachedReleased` violated (13) | 6,332 | 16 | 1.0 |
| `NoGraceReset_bad` | Small (reduction 11) | `ShutdownJoins` violated (13) | 6,332 | 16 | 1.0 |
| `CancelFound_reach` | Declared | Reached (12) | 258 | 7 | 0.5 |
| `CancelNotFound_reach` | Declared | Reached (13) | 1,247 | 8 | 0.6 |
| `CancelRacesCompletion_reach` | Declared | Reached (13) | 160 | 7 | 0.6 |
| `CancelRacesDrop_reach` | Declared | Reached (13) | 924 | 9 | 0.6 |
| `CancelledData_reach` | Declared | Reached (13) | 2,099 | 9 | 0.6 |
| `CloseLive_reach` | Declared | Reached (12) | 924 | 10 | 0.6 |
| `CloseReport_reach` | Declared | Reached (13) | 820 | 7 | 0.5 |
| `DetachedAck_reach` | Declared | Reached (13) | 966 | 9 | 0.6 |
| `DetachedClose_reach` | Declared | Reached (12) | 714 | 9 | 0.6 |
| `DetachedReset_reach` | Declared | Reached (13) | 6,627 | 8 | 0.7 |
| `HeldBytes_reach` | Declared | Reached (13) | 1,747 | 9 | 0.6 |
| `HeldConn_reach` | Declared | Reached (13) | 1,948 | 10 | 0.6 |
| `IocpError_reach` | Declared | Reached (13) | 679 | 9 | 0.5 |
| `IocpOtherPacket_reach` | Declared | Reached (13) | 532 | 7 | 0.6 |
| `IocpPending_reach` | Declared | Reached (13) | 641 | 7 | 0.6 |
| `IocpSkip_reach` | Declared | Reached (13) | 780 | 8 | 0.5 |
| `MultishotFinal_reach` | Declared | Reached (13) | 1,469 | 8 | 0.6 |
| `NewWaker_reach` | Declared | Reached (12) | 1,207 | 10 | 0.6 |
| `SendError_reach` | Declared | Reached (13) | 1,918 | 9 | 0.6 |
| `ShutdownCross_reach` | Declared | Reached (12) | 954 | 8 | 0.6 |
| `StaleRequest_reach` | Declared | Reached (12) | 1,923 | 10 | 0.6 |
| `UringSingle_reach` | Declared | Reached (13) | 213 | 7 | 0.6 |
| `ZcNoNotice_reach` | Declared | Reached (13) | 407 | 9 | 0.6 |
| `ZcNotice_reach` | Declared | Reached (13) | 471 | 8 | 0.6 |

What the raises show: each argued bound raised by one passed on both backends, and each raise
added states (the state counts above are all larger than those of the declared profiles), so each
raise exercised its bound. Reading 2 (`UringWide`, `IocpWide`) also passed, so the reading of the
task's bound is not hiding a failure that the larger reading has. At that digest every profile was
complete; the rule of 2026-11-08 reopened the large profiles.

[FP Design Models]: ../../docs/specs/family-policy.md#design-models
