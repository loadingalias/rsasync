# Operation ownership, close, and shutdown

This is the design model of task R000. It checks the rules that the operation table (R203), the
TCP rules (R206), and zero-copy close (R603, R604) build on, before that code exists. The sources
are in the local design `docs/specs/dual-async-runtime.md`: Driver Topology and Operation Home
(with the kept receive error of 2026-11-14 and the IOCP abort of 2026-11-17), Completion I/O and
Buffers (the terminal-event table and the cancel rule), Future and Wake Contract (generations), and
Startup, Shutdown, and Failures. The Design Models section of `docs/specs/family-policy.md` sets the
acceptance.

**Status (2026-11-17): all 59 profiles pass at the current inputs.** This change adds the kept
receive error and the IOCP abort. The 12 positive searches complete, and all 18 fault variants and
29 witnesses give their required violations. See [Evidence record](#evidence-record). The design
decisions of 2026-11-07 and 2026-11-08 stay below.

## The promise and a concrete failure

The promise: an operation has one home driver from its accepted submission to its terminal
release. The consumer of the operation can move to another worker, drop its future, forget it, or
cancel it, and the runtime can close the resource or shut down. The driver releases each buffer
and charge exactly once, and only after the kernel can no longer use it. A stream gives each byte
to the caller once and in order, and it gives the error of a failed stream before the end of the
stream. A report of send progress gives a count only when the count is exact. A zero-copy send frees its direction at its send result, and
each notice releases its own backing once. Close never waits on a peer. Shutdown waits on a peer
for no longer than its grace period. Detached debt ends while the runtime runs.

Six concrete failures:

- A receive future drops while its kernel request is in flight, and the runtime frees the buffer
  at the drop. The kernel then writes into freed memory.
- Close waits for a send that it did not cancel. The peer never reads, so close never returns.
- Close waits for a zero-copy notice. TCP frees the sent pages only when the peer acknowledges
  them, so a peer that stops reading keeps close waiting forever. This was the design before
  2026-11-07 (`ZcWait_bad`).
- The driver releases a zero-copy buffer at the send result, before the notice, and the kernel
  then reads freed pages (`ZcEarlyRelease_bad`).
- A receive whose future dropped gets the error of a reset. The kernel reports that error once, so
  if the stream does not keep it, the next receive reads the end of the stream. The caller then
  takes a reset connection for a complete one (`DropRxError_bad`).
- On IOCP, a cancel aborts the connection during a send, and the packet gives no count. A report
  of a count then names bytes that the peer perhaps never got (`AbortCount_bad`).

## Run it

```sh
cargo xtask model operation-ownership                                    # every profile
cargo xtask model operation-ownership/OperationOwnership.Uring.cfg       # one profile
```

The output of a profile goes to `target/model/operation-ownership/<file>/<run>/`: the checker output in
`checker.log`, diagnostics in `stderr.log`, and, on a violation, the trace in `trace.json`.
`run.txt` records the command, tool identities, source hashes, status, and verdict. The checker
reads the exact inputs in `source/`; later runs preserve this directory. The xtask prints one digest over the
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
| Kernel | `k[c]`: `fly` (an event that the peer controls), `cxl` (the terminal event after a cancel that found its target), `tail` (the IOCP packet after an immediate return, whose result is known), or `none`. `ks`: the kernel's stream. `open`; `fail` (an IOCP abort: the next receive result is the error); `end` (the kernel reported the error, and each later receive result is the end of the stream) |
| Requests | `rq` holds the request entries `<<c, g>>`. A cancel from the consumer or from close travels to the home of operation `g`, and the home takes it in a later step |
| Backings | `bk[c][g]`: the buffer and charge of operation `g`. `own`, `held` (received bytes that the stream keeps), `zc` (the driver keeps it for the zero-copy notice of send `g`), `out` (released), or `dbl` (released twice) |
| Stream bytes | `Bytes = 3` from the peer. `rbuf` is the buffer of the receive, `held` the bytes that the stream keeps, and `got` the bytes that the caller received. `ro` is the kind of the result of the receive: `ok` (bytes, or a cancel that took effect), `err`, or `end`. `re`: the stream keeps the error of an abandoned receive. `ce`: the first result with no bytes, other than a cancel, that the caller received |
| Send progress | `tp`: `part` (partial or failed) or `full`. `wd` is the write direction: `ok`, `owe` (a report with the count is owed), `lost` (a report with no count is owed), or `failed`. `required` records which send results require a report, and `rep[g]` counts the reports of send `g`. `ab`: on IOCP, a cancel aborted the connection |
| Pending notices | `pn`: the sends whose result had `IORING_CQE_F_MORE` and whose notice has not arrived. `nh[g]`: the home of the notice of send `g`, selected when the task ends, as for a slot. The submissions bound the set, as each send's charge bounds it in the design |
| Connections | `Conns = 1` connection waits on the listener. `ar` is in the accept, `hc` is held by the listener, and `uc` is with the caller |
| Waker | `wake`: the waker of the latest poll of the receive future. `woke[c]`: the latest waker of the future in slot `c` is woken |
| Detached debt | `debt` holds the exact generations that close left to the driver, with their buffers, charges, and the descriptor. `dt` means that this set is not empty. `pg`: a reset ended the kernel's hold of the stream's queued bytes, so every pending notice comes from the kernel |

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
| The kernel reports the error of a failed stream, such as a reset, to one receive. Each later receive gets the end of the stream. Bytes that came before the error come first | Linux v6.1 `include/net/sock.h` (`sock_error` takes `sk_err` with `xchg(&sk->sk_err, 0)`) and `net/ipv4/tcp.c` (`tcp_recvmsg_locked` returns `sock_error` once, then breaks at `RCV_SHUTDOWN`, which `tcp_done` sets at a reset) | None: the peer controls the reset. The reset is part of the receive result that reports it (reduction 14) |
| On IOCP, `CancelIoEx` of a running send aborts the connection. The packet of the send has an error status and 0 bytes (Win32 995, `ERROR_OPERATION_ABORTED`). The next send fails at once with `WSAECONNABORTED` (10053), and its status is still `STATUS_PENDING`, so no packet follows. A later receive fails | R507 probe run `20261115T084548Z-91515` (aws-windows-x64, Windows Server 2025); `a_cancel_of_a_send_that_runs_aborts_the_connection_and_gives_no_count` in run `20261115T092057Z-52931`. No run records the raw NT status: the code proves an error status with 0 bytes, and 995 is the Win32 code of `STATUS_CANCELLED` | The abort is a branch of the fair `Complete("tx")` after a cancel. The send that follows ends at its return |
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
   so each one has a raise-by-one profile for the evidence host. A receive result has three
   kinds: bytes or a cancel (`ok`), the error, and the end of the stream. The peer never ends the
   stream itself. A peer's end of the stream keeps nothing at the abandoned rule
   (`tcp::abandon` keeps no result of 0 bytes), and each later receive gets the end again. That
   is the state after a reported error, with no error to keep, so it adds no rule. With no
   peer end, `NoFalseEnd` can require that the caller never gets the end first.
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

13. A waiting operation is a later `Begin`. While an operation of a direction runs, a new
    operation of that direction waits in the driver behind it, in start order (`Driver::enqueue`,
    `src/driver/tcp.rs`; selected on 2026-11-14). A waiting operation owns no kernel resource:
    `begin_entries` pushes no entry for it, and `start` pushes an entry only after its room
    check, so a start that finds no room waits again (`wait_first`). It holds only its operation
    slot and its buffer in the driver. A cancel of it ends it at its home with `ECANCELED` and no
    kernel cancel (`Driver::cancel` through `unqueue`), and so do close (`close_resource`) and
    teardown (`Driver::teardown`). So the model starts it with `Begin` after the slot frees. A
    consumer step has no fairness, so the model has each start, and each operation that never
    starts because of a drop, a cancel, or a close while it waits. A dropped send that still runs
    is `Begin` and then `Drop`. A start during the grace period is a start before `Shutdown`, by
    reduction 4. The reduction hides the order of several waiting operations, because the model
    has one operation for each direction. `two_dropped_sends_and_a_third_send_run_in_start_order`
    (`src/driver/tests.rs:1070`) checks that order. On Windows,
    `an_explicit_cancel_of_a_send_gives_its_progress_and_keeps_the_direction_usable`
    (`src/driver/tests.rs:1164`) cancels a send that waits in the driver. The reduction also hides
    a waiting operation that never starts, because the model has no step for it. Its end can
    change a report: a waiting send that close ends takes the abandoned rule (`close_resource`
    ends it with `ECANCELED` after its interest ended), so the write direction owes a count, and
    the close reports one. `close_ends_a_waiting_send_unsent_and_releases_its_lease_once` and
    `teardown_ends_a_waiting_send_unsent_and_releases_its_lease_once` (`src/driver/tests.rs`)
    check that end at close and at teardown: the waiting send never runs, its lease returns once,
    and the close reports a count of 0.
14. A reset from the peer is part of the receive result that reports it. Before that result, no
    step reads the reset: a send result is a free choice in each case, and only the driver's
    `Reset` makes a zero-copy notice come from the kernel. A reset from the peer also purges the
    send queue, so the model leaves out notices that it makes certain; this removes no behavior.
    After the reported error, no bytes come. An IOCP abort is not a peer event: the send result
    that reports it sets `ks = "fail"`, so a receive in flight then gets the error. The design
    says that each later receive on the stream also ends with an error. The model gives the error
    to one receive and then the end of the stream. No property reads a result after the first
    error that the caller gets: `NoFalseEnd` reads only `ce`, the first result with no bytes. So
    the reduction hides no checked behavior. Known-bad: `DropRxError_bad`.

## Correspondence

Each action is one atomic step. The rule column names the design rule that the action implements.
The R203 column names the transition of the operation table (`src/table.rs` of rsasync) for
each action that the table implements, or the later task that implements the action. The R204
column names the transition of the driver-only surface (`src/driver.rs` and `src/completion.rs`).
The R206 column names the transition of the TCP rules (`src/driver/tcp.rs` and `src/driver.rs`).
The R219 column names the transition of child processes on io_uring (`src/driver/process.rs`),
which R520 and R521 extend for their backends. R603 adds its transitions when its code exists.

| Action | Design rule | Atomicity | R203 transition | R204 transition | R206 transition | R219 transition |
| --- | --- | --- | --- | --- | --- | --- |
| `BeginRx` | Driver Topology: one outstanding operation for each direction; the next receive returns held data before it submits, as a prefix with one copy, and keeps the rest. Each operation future borrows its direction exclusively, so no earlier future of the direction exists | One poll: the check of held data, or the submission and the return of the initiating call | R206 | — | `Driver::recv` and `Driver::recv_selected` through `begin`: `forgo` ends the interest of the earlier operations of the direction. While one of them runs, or an operation waits, the new operation waits behind them in start order (`Driver::enqueue`), and `attend` starts the first at that end. `start` gives the kept bytes, with one copy that `Driver::copied` counts, then a kept error (`Kept::Error`), or submits. Reduction 13 maps the operations that wait | — |
| `BeginTx` | Driver Topology: after a partial or failed abandoned send, the next send returns a typed error with the transferred count. A next send that succeeds shows that each earlier abandoned send transferred all its bytes | One poll | R206 | — | `Driver::send` through `begin` and `start`: a direction that owes its count (`Sent::Owe`) gives `WriteFailed` with the count, then with no count (`Sent::Reported`). Otherwise `start` submits | — |
| `BeginAc` | Driver Topology: the next accept returns a connection that the listener holds before it submits | One poll | R206 | — | `Driver::accept` through `begin` and `start`: the listener gives its kept connection (`Kept::Connection`), or `start` submits | — |
| `Submit` (in `Begin*`) | Completion I/O: accepted submission gives the backing to the driver. Terminal-event table: an io_uring request waits for its completion; IOCP classifies the NT status of the return (pending, `NT_ERROR`, other, skip-on-success) | The submission and the return of the initiating call are one step | `Table::begin` records the resources and the `Shape` before the push. A refused push takes them back with `Table::withdraw` | — | `Driver::open` records the operation and its resources; `start` pushes its entry (`Driver::push`). An operation that waits pushes at the end of the one before it | `Driver::spawn` and `Driver::adopt_child` begin the exit operation of the child (`Kind::Exit`). `Driver::register` pushes its entry at the end of the spawn job: `IORING_OP_WAITID`, or a poll of the process descriptor. The wait is a single-completion operation |
| `Repoll` | Future and Wake Contract: a pending poll gives a new waker, and the future honors the latest one | One poll | R204 | `Completion::poll` registers the waker of each poll (`Entry::poll_take`, through `Waiter::poll_wait`), so the wake goes to the latest waker | — | — |
| `Claim` | Future and Wake Contract: the owned result and the slot return exactly once each. The claim of a zero-copy send returns its progress; its notice releases the backing | One step for both returns | `Table::claim`: the result and the resources, once. A zero-copy send gives its result, and `Table::complete` gives its resources at the notice | `Completion::poll` takes the result and the resources once (`Entry`: `READY` to `TAKEN`), then posts `Request::EndInterest`, so that the home frees the slot | `tcp::taken`: a send result that a future took ends the unreported count of the direction | `Wait::poll` takes the status once and keeps it in the `Child`, so a later wait takes no second result |
| `Cancel` | Driver Topology: an explicit cancellation returns the original buffer with its exact progress; a send direction stays usable. On IOCP, a cancel that reaches a running send aborts the connection, and the send gives no count (selected on 2026-11-17) | Posts one request entry | `Requests::post` with `Request::Cancel` | — | `Operation::cancel` posts `Request::Cancel`. An operation that waits ends at the home with `ECANCELED` (`Driver::unqueue`). A running one gets one kernel cancel (`Held::cancelled`). The IOCP abort is in the backend table below | A kill is a cancel request: `Driver::cancel` of an exit operation kills the child by its `End`, through the process descriptor, and pushes no kernel cancel. The operation ends when the child ends |
| `Drop` | Driver Topology: a dropped future ends interest, not the operation. A receive and an accept ask their home to cancel; a send is not cancelled. A terminal result goes by the abandoned rule | Posts at most one request entry | `Requests::post` with `Request::EndInterest`, and with `Request::Cancel` for a receive or an accept (R206) | `Completion::drop` posts `Request::EndInterest`, and `Request::Cancel` with a wake for an operation that asks for it | A send future asks no cancel (`cancel_on_drop` is false for `Kind::Send`) | A drop of a `Child` posts `Request::EndInterest`, and `Request::Cancel` (a kill) only with `End::Kill`. The exit operation runs on, and the home reaps the child at its end |
| `Forget` | Driver Topology: a forgotten future ends interest with no destructor, so no request; `mem::forget` ends the exclusive borrow | One step | `Table::end_interest` when the next operation of the direction starts (R206) | No step: the result waits in its `Entry` until `Driver::teardown` releases it | `forgo` in `begin`: `Table::end_interest` on each earlier operation of the direction, then the abandoned rule (`claimed`), or a cancel of a receive or an accept that runs | — |
| `Complete` | Terminal-event table: one event at the home. Single completion; multishot item with `IORING_CQE_F_MORE` and its final completion; IOCP packet. A zero-copy send result ends the send and frees its direction (selected on 2026-11-08); with `IORING_CQE_F_MORE`, its backing joins the pending notices. With no waiting future, the abandoned rule applies | The kernel posts the event and the home harvests it in one step | `Table::complete` with the `Event` of the backend (`Cqe::event`): `Outcome::Item`, `Ready`, or `Abandoned`. The zero-copy result: R603 | `Driver::turn` gives up to its bound of completions to `Table::complete`, then publishes each result (`Entry::publish`) or drops the resources of an abandoned operation (`Entry::reclaim`) | `tcp::abandon` at an abandoned terminal event: kept bytes (`Kept::Bytes`) or a kept receive error (`Kept::Error`; a cancel and a result of 0 bytes keep nothing), a kept connection, or a failed write direction: `Sent::Owe` with the count of the bytes that went, or, after an IOCP abort, `Sent::Lost` with no count | `harvest` gives the completion of an exit operation to `Children::exited` before `Table::complete`: the reap and the status, with one nonblocking `waitid` after a poll. A poll with no status yet is pushed again and is no event of the table. An exit before the start is an immediate completion |
| `Process` | Driver Topology: the request travels to the home through its entry, and close cancels each operation by its own `user_data`. Future and Wake Contract: the generation check rejects a stale identity. Completion I/O: a cancel result is not a terminal event; "done" and "already running" make the target's own event certain; "not found" changes nothing | One step at the home | `Table::take_requests`: the generation check, then `Taken::Cancel` for a running target. The backend cancels it by its own user data (`Sqe::cancel` with `OpId::cancel_user_data`), and the result is `Outcome::CancelResult`, which changes nothing | `Driver::turn` takes the requests first in each turn | `Driver::cancel`: one kernel cancel for each operation, by its own user data, once | — |
| `TakeOver` | Driver Topology: close takes each result that a dropped future left, by the abandoned rule | One step | R206 | — | `close_resource`: `Table::end_interest` on each operation of the resource, then `claimed` for one that ended, and `Driver::cancel` for one that runs | — |
| `CloseDone` | Driver Topology: close waits for the terminal event of each operation, releases the data that no receive took, reports each owed and each cancelled send's progress, and closes each connection that no accept took. It does not wait for the pending notices: it half-closes, returns, and turns every pending notice of the stream into detached debt | One step after the last terminal event | R206 | — | `close_if_idle`: when no operation of the resource runs, it reports `Closed::sent`, the count that the direction owes (`SendCount::Unknown` after an IOCP abort), releases the kept bytes and connections and the descriptor (`Resources::release`), and completes the close | — |
| `Reset` | Driver Topology: `connect` with `AF_UNSPEC` at the drain limit, under budget pressure, or at grace expiry | One step: the purge of the send queue, for every pending notice of the connection | R603 | — | — | — |
| `Notice(g)` | Terminal-event table: the zero-copy notice of send `g` releases its own buffer and charge, once. After the last notice of detached debt, the descriptor closes | The kernel posts the notice and the home of `g` harvests it in one step | R603 | — | — | — |
| `Shutdown` | Startup, Shutdown, and Failures: shutdown closes ingress, and grace expiry escalates | The request and grace expiry are one step | — | — | — | — |
| `EndTask` | Startup, Shutdown, and Failures: the task returns, or grace expiry cancels it. Its futures drop. Driver Topology: close first cancels each outstanding operation, a dropped send included. Each held slot and each pending notice gets the home of its submission | One step for the drops and the closes | — | Driver-only: `Driver::teardown` cancels each running operation by its own user data | Driver-only: `Driver::teardown` first ends each operation that waits, with `ECANCELED` | `Driver::teardown` waits for each spawn job, kills each child with `End::Kill`, and cancels the exit operation of each child with `End::Keep`, which `Teardown::kept` counts |
| `Exit` | Driver Topology: a worker with live operations or pending notices cannot exit until they reach terminal release. Startup, Shutdown, and Failures: no worker exits while a task of the runtime remains. `block_on` returns only after each operation that its driver homes reaches terminal release (selected on 2026-11-11) | One step | — | Driver-only: `Driver::teardown` drives until `Driver::busy` is false: no operation of the table and no completion of a record runs. Then it closes each resource. The worker exit is R304 and R305. The return of `block_on` is `RingOwner::block_on` (`src/task/ring.rs`): it returns when its tasks end and does not yet drive its driver to terminal release (found on 2026-11-17). R305 makes that side true | — | — |
| `Terminated` | None. The end state stutters, so deadlock checking stays on | One step | — | — | — | — |

The record of a local stream (R317, mechanism 3 of Cost of a Request on a Local Worker). On
io_uring, a local send of a stream (`local::Transmit`) is no operation of the table. The record of
the stream holds its lease, its result, and its waiter (`tcp::Sending`), and the user data of its
entry names the record (`Resource::record`). The actions keep their rules, at these transitions:
`BeginTx` is `Driver::send_record`, which takes the record only when the direction runs, keeps
waiting, and owes nothing, and else gives the send to `Driver::send`. An operation of the write
direction waits behind a send of the record that runs (`begin` and `attend`). `Claim` is
`Directions::take_send`. `Cancel` is `Driver::cancel_record_send`, once. `Drop` is
`Driver::end_send`, and `Forget` is the `forgo_send` of the next send of the direction. `Complete`
is the harvest of the record (`Directions::record_sent`), with the abandoned rule of `Kind::Send`
for a send with no interest. `TakeOver` and `CloseDone` are `start_close` and `close_if_idle`,
which end the interest of the send of the record, cancel it, and wait for its terminal event. A
drop or a cancel in a turn of the driver posts its mark in the close word of the stream
(`home::Mark`), which the next turn takes: the entry path of reduction 1. A local selected receive
with the multishot receive is outside the model (Limits). A send of each byte (`Kind::SendAll`,
`Driver::send_all`, and the `all` send of a record) is one operation whose kernel completions
before its last are no terminal events: a completion that sent part of the bytes goes again with
the rest (`tcp::sent_part`, `Driver::resend`), as the items of a multishot accept do. Its terminal
event gives each byte, or part of them with a failure or a cancel, which are the `full` and `part`
progress of the model, so its rules are those of a send.

The backends. The R507 column names the IOCP transitions (`src/driver/iocp.rs`), and the
R503, R505, and R520 column names the transitions of the readiness poller (`src/driver/poller.rs`),
with the epoll selector on Linux and the kqueue selector on macOS. Each backend takes the submission
entries of the driver and posts the completions that a ring posts, so the other columns apply to
each backend. The readiness backends run under the io_uring profiles: each of their events is an
io_uring event with no `IORING_CQE_F_MORE` and no multishot item, and a cancel that finds its
entry ends it at once, which is `Process` and then `Complete`.

| Action | R507 transition (IOCP) | R503, R505, and R520 transition (epoll and kqueue) |
| --- | --- | --- |
| `Submit` | `Port::call` runs one overlapped call, and `terminal` classifies its return by the NT status: pending; an error, or a status that is still `STATUS_PENDING` (no packet); success or a warning, with skip-on-success (no packet) or without it (a packet) | `Poller::run`: one nonblocking call when the direction is ready. `EAGAIN` waits in the list of the direction until the next edge |
| `Complete` | `Port::poll` takes each packet, reads its NT status, and gives the result to `Port::ended`. After a cancel, the packet of a send with an error status and 0 bytes gives `WSAECONNABORTED`: the abort | The return of the nonblocking call is the terminal event (`Poller::post`) |
| `Process` | `Port::cancel`: `CancelIoEx` with the `OVERLAPPED` of the target, which ends at its own packet | `Poller::cancel` takes the waiting entry out of its list and posts `ECANCELED` at once |
| The abort (`CompleteTx` with `abort`) | `tcp::abandon` sets `Sent::Lost` for an abort that no future took. `WriteFailed::sent` and `Closed::sent` then give `SendCount::Unknown` | — |
| `Submit` and `Complete` of an exit operation (R219) | — | `Poller::arm`: epoll has a one-shot registration of the process descriptor. kqueue has a one-shot `EVFILT_PROC` registration with `NOTE_EXIT`, and a registration that gives `ESRCH` (the child ended first) posts its completion at once: the immediate completion of an exit before the start. A poll with no status yet waits in a one-shot `EVFILT_TIMER` on kqueue (`again_poll` in `src/driver/process.rs`) and is no event of the table |

The movable resources (R402, `src/driver/plain.rs`). A movable resource owns its descriptor and the
state of its directions, and each of its operations has its home at the driver of the thread that
starts it. The table above applies, with these transitions for the actions of a direction:

| Action | R402 transition |
| --- | --- |
| `BeginRx`, `BeginTx`, `BeginAc` | `Direction::begin` gives the operation the state of the direction and the earlier operation whose result no future took, in a `Chain`. `Driver::begin_plain` records the operation at the home of the caller. `Chain::settle` takes the result of the earlier operation from its own home (`Completion::take`) and applies the abandoned rule (`Directions::abandon`). Then `Chain::result` gives the kept result (`Directions::resume`), or `Driver::advance` submits. This is the "next operation in the direction" of reduction 3. An operation whose earlier operation still runs waits in `Driver::following`, which is reduction 13 with two homes |
| `Drop`, `Forget` | The direction keeps the `Completion`, so no interest ends. `Operation::drop` posts `Request::Cancel` for a receive, an accept, or a connect (`Direction::forgo`). The next operation posts it for a forgotten one |
| `Cancel` | `Operation::cancel` posts `Request::Cancel`. An operation that waits ends at its home with `ECANCELED` (`Driver::unfollow`), and its consumer takes the earlier operation of its chain over |
| `TakeOver`, `CloseDone` | `Direction::close` gives the close the operations of both directions, with a cancel for each. `Chain::settle` takes each result, and `step` reports `Closed::sent` and drops the descriptor |
| `EndTask` | The drop of the resource drops each `Tail`: `Request::Cancel` and `Request::EndInterest` go to the home of each operation, which releases its resources and its hold of the descriptor at its terminal event |

## Properties

| Property | Acceptance text | Kind |
| --- | --- | --- |
| `ReleaseOnce` | Each buffer and charge is released exactly once and never before its terminal event. A send's backing stays with the driver exactly while its notice is pending. No accepted connection is lost | Invariant |
| `NoImpossibleWait` | Teardown never waits for an event that cannot arrive: each live operation has an event to come, and the home of each live operation and each pending notice is driven | Invariant |
| `InOrderOnce` | The caller receives each byte once and in order across abandoned receives | Invariant |
| `NoStaleEffect` | A stale identity never changes a reused slot | Invariant |
| `ReportOnce` | Each abandoned send that fails or transfers only part of its bytes, and each send that close cancels, has its progress reported exactly once | Invariant |
| `ExactCount` | After an IOCP abort, no report of send progress gives a count | Invariant |
| `NoFalseEnd` | A receive never gives the end of the stream in place of the error of a failed stream: the first result with no bytes that the caller gets, other than a cancel, is the error | Invariant |
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

`InOrderOnce` checks both the order of returned bytes and conservation of the received prefix:
before close returns, `got \o held \o rbuf` is exactly the prefix delivered by the kernel. A check
of `got` alone cannot detect an unread suffix that the driver drops.

`required` is independent of `rep`: the send result records the obligation, and error reporting
or close discharges it. `ReportOnce` rejects an extra report and a missing required report when
the write direction has failed or close has returned. A full abandoned send reports nothing
unless close cancelled it. `ReleaseOnce` also checks `debt = pn` after close; a Boolean flag cannot
show that close omitted one of several notices.

`ExactCount` reads `wd`: after an abort, an owed report must be `lost`, which reports no count.
When the abort comes, the send runs, so the direction has no earlier owed report: a send starts
only from `wd = "ok"`. `NoFalseEnd` reads `ce`, the first result with no bytes that the caller
got. The peer of the model never ends the stream (reduction 9), so each end that the kernel gives
comes after the kernel reported an error. An end that the caller gets first therefore hides that
error.

## Bounds, faults, and fairness

The declared bounds: two workers, one moving consumer, one stream and one listener, two operation
submissions on each resource in one run (the receive and the send of the stream share two), three
bytes on the receive direction, and one connection on the listener. A larger bound needs only a
change of the constants in a `.cfg` file. The wide profiles and the raise-by-one profiles run on
the evidence host.

The faults and free choices:

- The kernel selects each result from the terminal-event table of the backend. A receive takes
  1 to all remaining bytes, or the error of a failed stream (a reset from the peer), or after that
  error the end of the stream. A cancel that took effect gives no bytes and no error. A send is
  partial (or failed) or full. On IOCP, a cancel that finds a send running can abort the
  connection. An
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
| `DropRxTail_bad` | An abandoned receive drops its unread suffix | `InOrderOnce` |
| `OmitReport_bad` | Error reporting or close omits required send progress | `ReportOnce` |
| `OmitDebt_bad` | Close omits one of two pending notices from detached debt | `ReleaseOnce` |
| `CloseNoCancel_bad` | Close does not cancel, and the peer refuses to read | `CloseReturns` |
| `CloseNoCancelJoin_bad` | The same rule change | `ShutdownJoins` |
| `StaleWaker_bad` | The home wakes the waker of the first poll | `LatestWakerWoken` |
| `ZcWait_bad` | The design before 2026-11-07: close waits for every pending notice, and nothing resets | `CloseReturns` |
| `ZcEarlyRelease_bad` | The mirror error of the rule of 2026-11-08: the driver releases a zero-copy backing at its send result, not at its notice | `ReleaseOnce` |
| `NoDrainLimit_bad` | Option C as first proposed: no drain limit | `DetachedReleased` |
| `NoGraceReset_bad` | Grace expiry does not reset detached debt | `ShutdownJoins` |
| `AbortCount_bad` | After an IOCP abort, the driver takes the abort as an ordinary cancel and owes the count of the send | `ExactCount` |
| `DropRxError_bad` | The design before 2026-11-14: the abandoned rule keeps no error, so the next receive submits and reads the end of the stream | `NoFalseEnd` |

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
- `AbortCount_bad` (`ExactCount`): `BeginTx` on IOCP (pending); `EndTask` (close cancels the
  dropped send); `Process("tx", 1)`; `Complete("tx")` (the abort, no future). The variant owes
  the count (`wd = "owe"`) of a send whose progress is unknown.
- `DropRxError_bad` (`NoFalseEnd`): `BeginRx`; `Complete("rx")` (the error of a reset);
  `Drop("rx")` (the abandoned rule, and the variant keeps no error); `BeginRx`;
  `Complete("rx")` (the end of the stream); `Claim("rx")`. The caller gets the end of the
  stream, and the reset is lost.
- For example, `DropRelease_bad` is `BeginRx`, `Drop("rx")`, and the drop releases the receive
  buffer while the receive is in flight. Each current trace is retained in
  `target/model/operation-ownership/<profile>/<run>/trace.json`.

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
| `KeptError_reach` | io_uring | The next receive returns the error that an abandoned receive kept | Abandoned receive with an error |
| `IocpAbort_reach` | IOCP | A cancel of a running send aborts the connection | The IOCP abort |
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
- The Windows IoRing, Registered I/O, and blocking-service rows of the terminal-event table have
  no profile of their own, and epoll and kqueue run under the io_uring profiles (the backend
  table of [Correspondence](#correspondence)). Their shapes are modeled shapes: a return
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
- The order of several operations that wait in one direction (reduction 13). A test checks it.
- After an IOCP abort, the result of a receive that was in flight has no run evidence. The model
  gives it the error. No run shows whether a later receive on Windows gets the error again or the
  end of the stream. The model gives the end, which `NoFalseEnd` reads as the stricter case.
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
- The model proves no Rust code. The correspondence table has the transitions of R203 and R204;
  R206, R219, and R603 add theirs. A change to either side reopens this check.

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

All 59 profiles pass for
`sha256:8ddd06c769d39dd85170f94a7eadd8492fb6649125bd4c4d9ee98dc620d9a1d1` (60 files), at
commit `7e3c6de`. The rebase onto main left each model file the same, so the digest is that of
the merged files. SANY parsed the module before TLC. The checks use Temurin JRE 21.0.13+9,
tla2tools 1.8.0+bc1b506bdcad (TLC2 Version 2026.11.15.002146, rev bc1b506), and
`cargo xtask model operation-ownership/<profile>`, one command for each profile, each with a
cap of 3600 seconds.

Two quiet ephemeral EC2 hosts, each with 16 vCPUs and Ubuntu 26.04 on kernel 7.0.0-1016-aws,
ran the profiles at the same time: a c8g.4xlarge (aarch64, 32 GiB, run `20261117T030545Z-51205`)
and a c8i.4xlarge (x86_64, 32 GiB, run `20261117T030619Z-55084`). Each host first ran smaller
searches of each backend. The time of each such probe, divided by its recorded time, scaled the
recorded time of each large profile, and a large profile ran only with an estimate of at most
3300 seconds. Every estimate was below that limit, and every profile finished within the
one-hour limit.

The 12 positive searches finish with no violation, an empty queue, and exit 0:

| Profile | Distinct states | Recorded on 2026-11-11 | Change | Depth | Time | Host |
| --- | --- | --- | --- | --- | --- | --- |
| `OperationOwnership.Iocp.cfg` | 815,370 | 648,946 | +25.6 % | 22 | 55 s | x86_64 |
| `OperationOwnership.IocpBytes4.cfg` | 1,075,142 | 873,266 | +23.1 % | 22 | 1 min 17 s | x86_64 |
| `OperationOwnership.IocpConns2.cfg` | 1,083,608 | 861,278 | +25.8 % | 24 | 1 min 17 s | x86_64 |
| `OperationOwnership.IocpOps3.cfg` | 12,097,132 | 9,357,002 | +29.3 % | 29 | 19 min 27 s | aarch64 |
| `OperationOwnership.IocpWide.cfg` | 10,159,624 | 7,752,242 | +31.1 % | 30 | 14 min 28 s | x86_64 |
| `OperationOwnership.IocpWorkers3.cfg` | 1,392,448 | 1,100,658 | +26.5 % | 23 | 2 min 05 s | aarch64 |
| `OperationOwnership.Uring.cfg` | 1,075,462 | 970,534 | +10.8 % | 23 | 1 min 32 s | aarch64 |
| `OperationOwnership.UringBytes4.cfg` | 1,391,202 | 1,282,956 | +8.4 % | 23 | 1 min 56 s | aarch64 |
| `OperationOwnership.UringConns2.cfg` | 2,052,838 | 1,852,358 | +10.8 % | 25 | 2 min 17 s | x86_64 |
| `OperationOwnership.UringOps3.cfg` | 22,667,262 | 19,962,870 | +13.5 % | 30 | 31 min 53 s | x86_64 |
| `OperationOwnership.UringWide.cfg` | 25,858,420 | 22,058,438 | +17.2 % | 31 | 42 min 44 s | aarch64 |
| `OperationOwnership.UringWorkers3.cfg` | 2,429,840 | 2,198,912 | +10.5 % | 24 | 3 min 29 s | aarch64 |

The io_uring counts differ from the record only through the receive results. The abort needs
`Backend = "iocp"`, so on io_uring `ab` stays false, `wd` never becomes `lost`, and each send
step is the step of the record. The receive results changed in three ways. A result with no bytes
and no cancel is now the error of a failed stream, and it ends the stream, so later bytes no
longer follow it. Each later result is the end of the stream, a state that the record did not
have. An abandoned error stays in the stream (`re`) until the next receive or close, and the
state records the kind of the result (`ro`) and the first result with no bytes that the caller
got (`ce`). The net change is +8 % to +17 %, and it grows with the receive submissions. The IOCP
counts also have the abort, and its later sends and receives, so they grow more.

All 18 known-bad variants violate their named property, and all 29 witnesses reach their named
target, on the x86_64 host, in 34.95 seconds of checker time. The four new profiles:

| Profile | Required result | Seconds |
| --- | --- | --- |
| `AbortCount_bad` | `ExactCount` violated | 0.68 |
| `DropRxError_bad` | `NoFalseEnd` violated | 0.90 |
| `IocpAbort_reach` | `UnreachedIocpAbort` violated: the target is reached | 0.76 |
| `KeptError_reach` | `UnreachedKeptError` violated: the target is reached | 0.64 |

The traces of the two new variants are the ones in [Known-bad variants](#known-bad-variants).

The tests that check the premise of reduction 13 ran on the x86_64 host before the profiles, 6
of 6, at `7e3c6de`. The rebase onto main and the change to `assert_matches!` made that result
invalid, so the six tests ran again at the rebased commit on an ephemeral c8i.4xlarge with
Ubuntu 26.04 (run `20261117T041755Z-57337`, clean tree):
`cargo xtask test -E 'test(waiting_send_unsent)'`, 6 tests run, 6 passed, on the kernel, the
injection, and epoll.

The two collected archives keep the logs, traces, manifests, and source copies of each profile,
and the job and the coordinator of each host. They are in
`~/.local/state/model-evidence/rsasync/R000/20261117-iocp-abort/` of the evidence Mac:
`arm64-20261117T030545Z-51205.tar.gz` (SHA-256
`be33ce047052f9e4085a6a33970ce65035b68e31e7db8264cea5330b100fb3c9`) and
`x64-20261117T030619Z-55084.tar.gz` (SHA-256
`7f47002ae94e841ea5e92b9db9bc4af7c47eeb06977dd364f7a6a7a7a3201f69`). The archives exclude
TLC state caches. Collection and teardown exit 0 for each host, and `dev-machine status` shows
no instance or volume for each target.

The earlier record, of 2026-11-11, checked
`sha256:9519fd9c04854f42cb016adb867e3671e135a2ab8bb6333b3073855b31ca6beb` (56 files) with TLC
rev d8fb231. It is a historical baseline: its counts are the "Recorded" column above. Durations
from different hosts are not a speed comparison.

[FP Design Models]: ../../docs/specs/family-policy.md#design-models
