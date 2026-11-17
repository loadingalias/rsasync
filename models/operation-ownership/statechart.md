# Operation ownership: statecharts

These charts come from `OperationOwnership.tla`. The checked model is the authority; the charts
only show its shape. Each label names the action of the model, or the operator that the action
calls: `Submit` is the part of `BeginRx`, `BeginTx`, and `BeginAc` that gives the operation to the
kernel.

## The life of one operation

One slot holds one operation from its accepted submission to its terminal event. `Fly`, `Cxl`,
and `Tail` are the live states (`st = "live"`), named by what the kernel can still deliver (`k`).
`Tail` is the IOCP packet after an immediate return.

```mermaid
stateDiagram-v2
  [*] --> Free
  Free --> Fly: Submit (SQE or STATUS_PENDING)
  Free --> Tail: Submit (IOCP success, packet follows)
  Free --> Term: Submit (IOCP NT_ERROR or skip-on-success)
  Fly --> Fly: Complete (multishot item with MORE)
  Fly --> Cxl: Process (cancel finds the target)
  Fly --> Term: Complete (terminal event, or a send result with MORE)
  Cxl --> Term: Complete (terminal event, or a send result with MORE)
  Tail --> Term: Complete (packet)
  Term --> Free: Claim (result and slot to the caller)
  Term --> Free: Drop, Forget, or TakeOver (abandoned rule)
```

## The backing of a zero-copy send

A send result with `IORING_CQE_F_MORE` ends the send and frees its slot, and its backing joins
the stream's pending notices (`pn`, `bk = "zc"`). Each pending notice has this life, apart from the
slot. `Detached` is a pending notice whose generation is in `debt` after close returned.
`Purged` is the same state after a reset (`pg`). `dt` means that `debt` is not empty.

```mermaid
stateDiagram-v2
  [*] --> Pending: Complete (send result with MORE)
  Pending --> Released: Notice (peer acknowledges)
  Pending --> Detached: CloseDone (close returns; the whole set detaches)
  Detached --> Released: Notice (peer acknowledges)
  Detached --> Purged: Reset (drain limit, budget pressure, or grace expiry)
  Purged --> Released: Notice (from the kernel)
  Released --> [*]
```

## The receive side of the stream

The kernel's stream (`ks`) and the error that the stream keeps (`re`). The kernel reports the
error of a failed stream to one receive, and each later receive gets the end of the stream.

```mermaid
stateDiagram-v2
  state Kernel {
    [*] --> Open
    Open --> Ended: Complete or Submit (a receive gets the error of a reset)
    Open --> Failed: CompleteTx (IOCP: a cancel aborts a running send)
    Failed --> Ended: Complete or Submit (a receive gets the error)
    Ended --> Ended: Complete (a receive gets the end of the stream)
    --
    [*] --> Clear
    Clear --> Kept: Complete, Drop, Forget, or TakeOver (the abandoned rule keeps the error)
    Kept --> Clear: BeginRx (the next receive returns the error before it submits)
    Kept --> Clear: CloseDone (close drops the error)
  }
```

## Runtime, task, resources, and workers

The four regions run at the same time.

```mermaid
stateDiagram-v2
  state Lifecycle {
    [*] --> Run
    Run --> Down: Shutdown
    --
    [*] --> Alive
    Alive --> Gone: EndTask
    --
    [*] --> Open
    Open --> Closing: EndTask (cancel each live operation)
    Closing --> Closed: CloseDone (each slot is free; pending notices detach)
    --
    [*] --> Running
    Running --> Exited: Exit (no held slot and no pending notice at this home)
  }
```

## What the charts hide or merge

- A terminal event with no waiting future goes from a live state directly to `Free`: the
  abandoned rule applies in the same step. The chart shows only the path through `Term`.
- The fairness. `Fly` has no fairness: the peer controls it. `Cxl` and `Tail` end under weak
  fairness. `Pending` and `Detached` wait for the peer, so they have no fairness. `Purged` ends
  under weak fairness. `Reset` is fair while the runtime runs (the drain limit) and after
  `Shutdown` (grace expiry); under budget pressure it has no fairness.
- One `Reset` moves every detached notice of the stream to `Purged` at once.
- The pending notices exist only for the stream's send on io_uring. The claim of a zero-copy send
  returns its progress, not its backing, so `Claim` and the abandoned rule do not release a
  backing that a notice releases.
- The request entries (`rq`), the cancel flag (`cx`), and the generation check are not shown. A
  stale request removes itself and changes nothing.
- The data is not shown: the received bytes (`rbuf`, `held`, `got`), the kind of the receive
  result (`ro`), the first result with no bytes that the caller got (`ce`), the send progress
  (`tp`, `wd`, `required`, `rep`), the IOCP abort (`ab`), the connections (`kc`, `ar`, `hc`, `uc`), each backing (`bk`), and the home of
  each pending notice (`nh`).
- The futures (`fut`) and the waker (`wake`, `woke`) are not shown. `Repoll` changes only the
  waker.
- `Shutdown` merges the request and grace expiry. `EndTask` merges the drop of each future, the
  close of each resource, and the selection of the home of each held slot and each pending
  notice.
- The guards are not shown. `Exit` needs `Down`, `Gone`, no held slot at its home, and no pending
  notice at its home. `CloseDone` needs each slot of the resource to be free. `Notice` needs the
  home of the notice to run. `EndTask` happens
  at any time while the runtime runs, and it is fair after `Shutdown`.
- The branches of the known-bad variants (`Bug`) are not shown.
