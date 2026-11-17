------------------------- MODULE OperationOwnership -------------------------
(***************************************************************************)
(* Operation ownership, close, and shutdown in rsasync (task R000).        *)
(*                                                                         *)
(* One consumer task moves between two workers. It starts operations on   *)
(* a stream (receive "rx" and send "tx") and on a listener (accept "ac").  *)
(* Each operation has one home driver: the worker where it was submitted. *)
(* While the task is alive, both workers run, so no step reads a home.    *)
(* The model therefore selects the home of each held slot when the task   *)
(* ends. This gives the same behaviors as a selection at submission.      *)
(* The kernel is a free choice from the terminal-event table of the        *)
(* selected backend. Each action is one atomic step of the design. The     *)
(* README maps each action to its design rule.                             *)
(*                                                                         *)
(* A zero-copy notice on a stream waits for the peer. A zero-copy send     *)
(* frees its direction at its send result, and the driver keeps its        *)
(* buffer in the stream's set of pending notices (selected on 2026-11-08). *)
(* Close does not wait for the notices: it leaves the set to the driver as *)
(* detached debt, and a reset at the drain limit, under budget pressure,   *)
(* or at grace expiry ends the wait (selected on 2026-11-07).              *)
(*                                                                         *)
(* The kernel reports the error of a failed stream, such as a reset, to    *)
(* one receive, and then it gives the end of the stream. An abandoned      *)
(* receive keeps that error for the next receive (selected on 2026-11-14). *)
(* On IOCP, a cancel that reaches a running send aborts the connection,    *)
(* and its result gives no count (selected on 2026-11-17).                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS
  Workers,  \* The workers. Each worker has one driver.
  Bytes,    \* The bytes that the peer sends on the stream.
  MaxOps,   \* The operations that the consumer can submit: see Submissions.
  Submissions, \* "resource": MaxOps on each resource, so the receive and
               \* the send share the stream's. "direction": MaxOps on each
               \* direction (the wider bound for the evidence host).
  Conns,    \* The connections that wait on the listener.
  Backend,  \* "uring" or "iocp".
  Bug       \* "none", or the defect of one known-bad variant.

ASSUME Backend \in {"uring", "iocp"}
ASSUME Submissions \in {"resource", "direction"}
ASSUME Bug \in {"none", "dropRelease", "win32", "exitLive", "submitFirst",
                "closeNoCancel", "noGeneration", "reportTwice", "staleWaker",
                "zcWait", "noDrainLimit", "noGraceReset", "zcEarlyRelease",
                "dropRxTail", "omitReport", "omitDebt", "abortCount",
                "dropRxError"}

Ch == {"rx", "tx", "ac"}            \* One operation slot for each direction.
Res == {"s", "l"}                   \* The stream and the listener.
ResOf(c) == IF c = "ac" THEN "l" ELSE "s"
ChOf(r) == {c \in Ch : ResOf(c) = r}
Gens == 1..MaxOps                   \* Generation g is the g-th operation in a slot.
NoHome == "none"
BSeq(S, n) == UNION {[1..m -> S] : m \in 0..n}

VARIABLES
  life,   \* "run", or "down": shutdown was requested and its grace expired.
  wk,     \* wk[w]: "run" while worker w drives its driver, then "exit".
  alive,  \* The consumer task is alive.
  cl,     \* cl[r]: "open", "closing" (cancel, then wait), or "closed".
  fut,    \* fut[c]: "att" while a future of the consumer waits for the
          \* operation in slot c, else "none".
  wake,   \* The waker of the latest poll of the receive future, 1 or 2.
  woke,   \* woke[c]: the latest waker of the future is woken.
  gen,    \* gen[c]: the generation of the last operation in slot c.
  st,     \* st[c]: the slot is "free", "live" (accepted, not terminal), or
          \* "term" (terminal event harvested, result not yet taken).
  home,   \* home[c]: the home of the operation in slot c, after the task ends.
  k,      \* k[c]: what the kernel can still deliver for slot c: "none",
          \* "fly" (an event that the peer controls), "cxl" (the terminal
          \* event after a cancel), or "tail" (the IOCP packet after an
          \* immediate return, whose result is known).
  cx,     \* cx[c]: a cancel was requested for the operation now in slot c.
  rq,     \* The request entries <<c, g>> that wait at the home of operation g.
  bk,     \* bk[c][g]: the backing and charge of operation g in slot c.
          \* "zc": the driver keeps it for the zero-copy notice of send g.
  rxk,    \* The next byte that the kernel gives to a receive.
  rbuf,   \* The bytes in the buffer of the receive in slot "rx".
  held,   \* The bytes that the stream holds for the next receive.
  got,    \* The bytes that the caller received, in order of delivery.
  ks,     \* The kernel's stream: "open"; "fail" (an IOCP abort: the next
          \* receive result is the error); or "end" (the kernel reported the
          \* error, and each later receive result is the end of the stream).
  ro,     \* The kind of the result in slot "rx": "ok" (bytes, or a cancel
          \* that took effect), "err" (the error), or "end".
  re,     \* The stream keeps the error of an abandoned receive.
  ce,     \* The first result with no bytes that the caller received, other
          \* than a cancel: "none", "err", or "end".
  tp,     \* The progress of the send in slot "tx": "none", "part", or "full".
  wd,     \* The write direction: "ok", "owe" (failed, a report with the
          \* count is owed), "lost" (failed, a report with no count is
          \* owed), or "failed".
  rep,    \* rep[g]: the progress reports of abandoned send g.
  required, \* The sends whose abandoned terminal result requires a report.
  ab,     \* IOCP: a cancel of a running send aborted the connection.
  kc,     \* The connections that the kernel has not yet accepted.
  ar,     \* The connections that the accept in slot "ac" accepted.
  hc,     \* The connections that the listener holds for the next accept.
  uc,     \* The connections that the caller received.
  debt,   \* The notices that close transferred to the driver as detached debt.
          \* Close returned and left the stream's pending
          \* notices, with their buffers, charges, and the descriptor, to
          \* the driver.
  pg,     \* A reset ended the kernel's hold of the stream's queued bytes.
  pn,     \* The pending notices: the sends g whose result had
          \* IORING_CQE_F_MORE and whose notice has not arrived.
  nh      \* nh[g]: the home of the pending notice of send g, after the
          \* task ends.

lifeVars == <<life, wk, alive, cl>>
futVars == <<fut, wake, woke>>
opVars == <<gen, st, home, k, cx, rq, bk>>
rxVars == <<rxk, rbuf, held, got, ks, ro, re, ce>>
txVars == <<tp, wd, rep, required, ab>>
acVars == <<kc, ar, hc, uc>>
zcVars == <<debt, pg, pn, nh>>
dt == debt # {}
vars == <<lifeVars, futVars, opVars, rxVars, txVars, acVars, zcVars>>

TypeOK ==
  /\ life \in {"run", "down"}
  /\ wk \in [Workers -> {"run", "exit"}]
  /\ alive \in BOOLEAN
  /\ cl \in [Res -> {"open", "closing", "closed"}]
  /\ fut \in [Ch -> {"none", "att"}]
  /\ wake \in {1, 2}
  /\ woke \in [Ch -> BOOLEAN]
  /\ gen \in [Ch -> 0..MaxOps]
  /\ st \in [Ch -> {"free", "live", "term"}]
  /\ home \in [Ch -> Workers \cup {NoHome}]
  /\ k \in [Ch -> {"none", "fly", "cxl", "tail"}]
  /\ cx \in [Ch -> BOOLEAN]
  /\ rq \subseteq Ch \X Gens
  /\ bk \in [Ch -> [Gens -> {"none", "own", "held", "zc", "out", "dbl"}]]
  /\ rxk \in 1..(Bytes + 1)
  /\ rbuf \in BSeq(1..Bytes, Bytes)
  /\ held \in BSeq(1..Bytes, Bytes)
  /\ got \in BSeq(1..Bytes, Bytes)
  /\ ks \in {"open", "fail", "end"}
  /\ ro \in {"ok", "err", "end"}
  /\ re \in BOOLEAN
  /\ ce \in {"none", "err", "end"}
  /\ tp \in {"none", "part", "full"}
  /\ wd \in {"ok", "owe", "lost", "failed"}
  /\ rep \in [Gens -> 0..2]
  /\ required \subseteq Gens
  /\ ab \in BOOLEAN
  /\ {kc, ar, hc, uc} \subseteq 0..Conns
  /\ debt \subseteq Gens
  /\ pg \in BOOLEAN
  /\ pn \subseteq Gens
  /\ nh \in [Gens -> Workers \cup {NoHome}]

-----------------------------------------------------------------------------
(* Helpers *)

\* The consumer runs: it is alive, and grace expiry has not cancelled it.
Runs == alive /\ life = "run"

\* The resource, or the direction, has budget for one more submitted
\* operation.
CanSubmit(c) ==
  IF Submissions = "resource" /\ ResOf(c) = "s"
    THEN gen["rx"] + gen["tx"] < MaxOps
    ELSE gen[c] < MaxOps

\* The home of the pending notice of send g is driven. Before the task
\* ends, both workers run.
NDriven(g) == IF nh[g] = NoHome THEN TRUE ELSE wk[nh[g]] = "run"

\* A future is ready: its operation is terminal.
Ready(c) == fut[c] = "att" /\ st[c] = "term"

\* The wake goes to the latest waker. The "staleWaker" variant wakes the
\* waker of the first poll.
Woken(c) == IF Bug = "staleWaker" /\ c = "rx" THEN wake = 1 ELSE TRUE

\* A new or ended future of slot c has the waker of its first poll. Only
\* the receive future records its waker.
FirstWaker(c) == IF c = "rx" THEN wake' = 1 ELSE UNCHANGED wake

\* One release of a backing. A second release is a defect.
Rel(b) == IF b = "own" THEN "out" ELSE "dbl"
Hold(b) == IF b = "own" THEN "held" ELSE "dbl"
ReleaseHeld(f) == [g \in Gens |-> IF f[g] = "held" THEN "out" ELSE f[g]]

\* The request entry of the operation in slot c, at its home.
Post(c) == <<c, gen[c]>>

\* The home of slot c is driven. Before the task ends, both workers run.
Driven(c) == IF home[c] = NoHome THEN TRUE ELSE wk[home[c]] = "run"

\* Post a cancel request for each slot in s, once for each operation.
Ask(s) ==
  LET new == {c \in s : ~cx[c]}
  IN /\ rq' = rq \cup {Post(c) : c \in new}
     /\ cx' = [c \in Ch |-> cx[c] \/ c \in new]

Avail == Bytes + 1 - rxk

\* The receive results that the kernel can give now, as <<bytes, kind>>.
\* Bytes come only while the stream is open. The kernel reports the error
\* of a failed stream to one receive: a reset from the peer, or an IOCP
\* abort. After that error, each result is the end of the stream. A cancel
\* that took effect gives no bytes and no error.
RxResults(cancel) ==
  (IF ks = "open" THEN {<<n, "ok">> : n \in 1..Avail} ELSE {})
  \cup (IF ks = "end" THEN {<<0, "end">>} ELSE {<<0, "err">>})
  \cup (IF cancel THEN {<<0, "ok">>} ELSE {})

\* The progress that a result carries. Each operator sets all channel data.
\* A receive takes the bytes and the kind of result x.
RxTakes(x) ==
  /\ rbuf' = [i \in 1..x[1] |-> rxk + i - 1]
  /\ rxk' = rxk + x[1]
  /\ ro' = x[2]
  /\ ks' = IF x[2] = "err" THEN "end" ELSE ks
  /\ UNCHANGED <<held, got, re, ce, txVars, acVars>>
TxTakes(t) == tp' = t /\ UNCHANGED <<rxVars, wd, rep, acVars, required, ab>>
AcTakes(m) == kc' = kc - m /\ ar' = ar + m /\ UNCHANGED <<rxVars, txVars, hc, uc>>
NoData == UNCHANGED <<rxVars, txVars, acVars>>

\* A result of a call that succeeds: bytes or the end of the stream, a send
\* count, or a connection.
Some(c) ==
  CASE c = "rx" -> \E x \in RxResults(FALSE) : x[2] # "err" /\ RxTakes(x)
    [] c = "tx" -> \E t \in {"part", "full"} : TxTakes(t)
    [] c = "ac" -> kc > 0 /\ AcTakes(1)
\* A result of a call that fails: an error with no progress. A failed send
\* is a partial send.
Nothing(c) ==
  CASE c = "rx" -> \E x \in RxResults(FALSE) : x[2] = "err" /\ RxTakes(x)
    [] c = "tx" -> TxTakes("part")
    [] c = "ac" -> AcTakes(0)

\* Terminal release of slot c.
FreeSlot(c) ==
  /\ st' = [st EXCEPT ![c] = "free"]
  /\ home' = [home EXCEPT ![c] = NoHome]
  /\ cx' = [cx EXCEPT ![c] = FALSE]

\* Slot c reaches its terminal event. A waiting future is woken, and takes
\* the result later. With no future, the abandoned rule applies at once.
EndSlot(c) ==
  IF fut[c] = "att"
    THEN /\ st' = [st EXCEPT ![c] = "term"]
         /\ woke' = [woke EXCEPT ![c] = Woken(c)]
         /\ UNCHANGED <<home, cx>>
    ELSE FreeSlot(c) /\ UNCHANGED woke

\* The final result of a slot: b received bytes of kind r, t send
\* progress, or m accepted connections. A future takes it (att), or the
\* abandoned rule applies. Received bytes stay with the stream, in the
\* receive's own buffer and charge. The error stays with the stream for
\* the next receive; the "dropRxError" variant keeps no error. A partial
\* or cancelled send owes a progress report: with no count after an IOCP
\* abort (lost), else with the count. The "abortCount" variant owes the
\* count also after an abort. Accepted connections stay with the listener.
EndRx(b, att, r) ==
  IF att THEN rbuf' = b /\ ro' = r /\ UNCHANGED <<held, bk, re>>
  ELSE /\ rbuf' = <<>>
       /\ ro' = "ok"
       /\ held' = IF Bug = "dropRxTail" THEN held ELSE held \o b
       /\ re' = (re \/ (r = "err" /\ Bug # "dropRxError"))
       /\ bk' = [bk EXCEPT !["rx"][gen["rx"]] = IF b = <<>> THEN Rel(@) ELSE Hold(@)]
\* A zero-copy result (zc) ends the send, and its backing stays with the
\* driver ("zc") until its notice. The claim and the abandoned rule release
\* only a backing that the send still owns. The "zcEarlyRelease" variant
\* releases the backing at the send result.
EndTx(t, att, zc, lost) ==
  /\ required' = IF ~att /\ (t = "part" \/ cx["tx"])
                    THEN required \cup {gen["tx"]} ELSE required
  /\ IF att THEN tp' = t /\ UNCHANGED wd
     ELSE /\ tp' = "none"
          /\ wd' = IF t = "part" \/ cx["tx"]
                     THEN IF lost /\ Bug # "abortCount" THEN "lost" ELSE "owe"
                     ELSE wd
  /\ bk' = [bk EXCEPT !["tx"][gen["tx"]] =
              CASE zc /\ Bug = "zcEarlyRelease" -> Rel(@)
                [] zc -> "zc"
                [] att \/ @ # "own" -> @
                [] OTHER -> Rel(@)]
EndAc(m, att) ==
  IF att THEN ar' = m /\ UNCHANGED <<hc, bk>>
  ELSE /\ ar' = 0
       /\ hc' = hc + m
       /\ bk' = [bk EXCEPT !["ac"][gen["ac"]] = Rel(@)]

\* The abandoned rule for the result that slot c holds.
Abandon(c) ==
  CASE c = "rx" -> EndRx(rbuf, FALSE, ro) /\ UNCHANGED <<rxk, got, ks, ce, txVars, acVars>>
    [] c = "tx" -> EndTx(tp, FALSE, FALSE, ab) /\ UNCHANGED <<rxVars, rep, ab, acVars>>
    [] c = "ac" -> EndAc(ar, FALSE) /\ UNCHANGED <<rxVars, txVars, kc, uc>>

-----------------------------------------------------------------------------
(* Submission. The consumer runs on a worker, which becomes the home.     *)

\* The initiating call returns. On io_uring the request waits for its
\* completion. On IOCP the driver classifies the NT status of the return.
\* After an IOCP abort, a send fails at once: the status is still
\* STATUS_PENDING, so no packet follows.
Submit(c) ==
  /\ gen' = [gen EXCEPT ![c] = @ + 1]
  /\ cx' = [cx EXCEPT ![c] = FALSE]
  /\ bk' = [bk EXCEPT ![c][gen[c] + 1] = "own"]
  /\ fut' = [fut EXCEPT ![c] = "att"]
  /\ FirstWaker(c)
  /\ UNCHANGED <<lifeVars, home, rq, zcVars>>
  /\ \/ /\ ~(ab /\ c = "tx")
        /\ st' = [st EXCEPT ![c] = "live"]     \* SQE, or STATUS_PENDING
        /\ k' = [k EXCEPT ![c] = "fly"]
        /\ woke' = [woke EXCEPT ![c] = FALSE]
        /\ NoData
     \/ /\ Backend = "iocp"                     \* NT_ERROR: no packet
        /\ st' = [st EXCEPT ![c] = "term"]
        /\ k' = [k EXCEPT ![c] = "none"]
        /\ woke' = [woke EXCEPT ![c] = TRUE]
        /\ Nothing(c)
     \/ /\ Backend = "iocp" /\ ~(ab /\ c = "tx") \* success or a warning status
        /\ Some(c)
        /\ \E warning, skip \in BOOLEAN :
             \/ /\ skip                         \* skip-on-success: no packet
                /\ st' = [st EXCEPT ![c] = "term"]
                /\ k' = [k EXCEPT ![c] = "none"]
                /\ woke' = [woke EXCEPT ![c] = TRUE]
             \/ /\ ~skip /\ Bug = "win32" /\ warning
                \* The Win32 return value reports a failure, so the
                \* variant takes the return as terminal. A packet follows.
                /\ st' = [st EXCEPT ![c] = "term"]
                /\ k' = [k EXCEPT ![c] = "tail"]
                /\ woke' = [woke EXCEPT ![c] = TRUE]
             \/ /\ ~skip /\ ~(Bug = "win32" /\ warning)
                /\ st' = [st EXCEPT ![c] = "live"]   \* the packet is terminal
                /\ k' = [k EXCEPT ![c] = "tail"]
                /\ woke' = [woke EXCEPT ![c] = FALSE]

\* A new future begins in slot c. The direction is free: an abandoned
\* operation keeps it busy until its terminal release.
CanBegin(c) ==
  /\ Runs
  /\ fut[c] = "none"
  /\ st[c] = "free"
  /\ cl[ResOf(c)] = "open"

\* The future ends with a result and no operation.
Returns(c) ==
  /\ fut' = [fut EXCEPT ![c] = "none"]
  /\ FirstWaker(c)
  /\ woke' = [woke EXCEPT ![c] = FALSE]

\* A receive first returns a prefix of the held bytes, with one copy, and
\* keeps the rest. Then it returns a kept error. With neither, it submits.
\* The "submitFirst" variant submits before it returns the held bytes.
BeginRx ==
  /\ CanBegin("rx")
  /\ \/ /\ held # <<>>
        /\ \E p \in 1..Len(held) :
             /\ got' = got \o SubSeq(held, 1, p)
             /\ held' = SubSeq(held, p + 1, Len(held))
             /\ bk' = IF p = Len(held) THEN [bk EXCEPT !["rx"] = ReleaseHeld(@)]
                      ELSE bk
        /\ Returns("rx")
        /\ UNCHANGED <<lifeVars, gen, st, home, k, cx, rq, rxk, rbuf, ks, ro,
                       re, ce, txVars, acVars, zcVars>>
     \/ /\ held = <<>> /\ re
        /\ re' = FALSE
        /\ ce' = IF ce = "none" THEN "err" ELSE ce
        /\ Returns("rx")
        /\ UNCHANGED <<lifeVars, opVars, rxk, rbuf, held, got, ks, ro, txVars,
                       acVars, zcVars>>
     \/ /\ (held = <<>> /\ ~re) \/ Bug = "submitFirst"
        /\ CanSubmit("rx")
        /\ Submit("rx")

\* A send on a direction that failed returns the typed error. The first
\* such send reports the progress of the abandoned send: its count, or no
\* count after an IOCP abort.
BeginTx ==
  /\ CanBegin("tx")
  /\ \/ /\ wd \in {"owe", "lost"}
        /\ wd' = "failed"
        /\ rep' = [rep EXCEPT ![gen["tx"]] = IF Bug = "omitReport" THEN @ ELSE @ + 1]
        /\ Returns("tx")
        /\ UNCHANGED <<lifeVars, opVars, rxVars, tp, acVars, zcVars, required,
                       ab>>
     \/ /\ wd = "ok"
        /\ CanSubmit("tx")
        /\ Submit("tx")

\* An accept first returns a connection that the listener holds.
BeginAc ==
  /\ CanBegin("ac")
  /\ \/ /\ hc > 0
        /\ hc' = hc - 1
        /\ uc' = uc + 1
        /\ Returns("ac")
        /\ UNCHANGED <<lifeVars, opVars, rxVars, txVars, kc, ar, zcVars>>
     \/ /\ hc = 0
        /\ CanSubmit("ac")
        /\ Submit("ac")

-----------------------------------------------------------------------------
(* The consumer's future. *)

\* A poll of the receive future that returns Pending gives a new waker,
\* for example a child waker of a combinator. The wake rule is the same for
\* each operation, so only the receive future changes its waker. One
\* change is enough: only the latest waker counts.
Repoll ==
  /\ Runs
  /\ fut["rx"] = "att"
  /\ ~Ready("rx")
  /\ wake = 1
  /\ wake' = 2
  /\ UNCHANGED <<lifeVars, fut, woke, opVars, rxVars, txVars, acVars, zcVars>>

\* The future returns the owned result and the slot, once each.
Claim(c) ==
  /\ Runs
  /\ fut[c] = "att"
  /\ st[c] = "term"
  /\ Returns(c)
  /\ st' = [st EXCEPT ![c] = "free"]
  /\ cx' = [cx EXCEPT ![c] = FALSE]
  /\ bk' = IF c = "tx" /\ bk[c][gen[c]] # "own" THEN bk     \* its notice releases it
           ELSE [bk EXCEPT ![c][gen[c]] = Rel(@)]
  /\ UNCHANGED <<lifeVars, gen, home, k, rq, zcVars>>
  /\ CASE c = "rx" ->
            /\ got' = got \o rbuf
            /\ rbuf' = <<>>
            /\ ce' = IF ce = "none" /\ ro # "ok" THEN ro ELSE ce
            /\ ro' = "ok"
            /\ UNCHANGED <<rxk, held, ks, re, txVars, acVars>>
       [] c = "tx" ->
            /\ tp' = "none"
            /\ UNCHANGED <<rxVars, wd, rep, acVars, required, ab>>
       [] c = "ac" ->
            /\ uc' = uc + ar
            /\ ar' = 0
            /\ UNCHANGED <<rxVars, txVars, kc, hc>>

\* An explicit cancellation of a receive or a send. The future keeps its
\* interest and later receives the original buffer with its exact progress.
\* On IOCP, a cancel that finds a running send can abort the connection
\* (CompleteTx): the send then gives no count.
Cancel(c) ==
  /\ Runs
  /\ c # "ac"
  /\ fut[c] = "att"
  /\ st[c] = "live"
  /\ ~cx[c]
  /\ Ask({c})
  /\ UNCHANGED <<lifeVars, futVars, gen, st, home, k, bk, rxVars, txVars,
                 acVars, zcVars>>

\* A dropped future ends interest, not the operation. A receive and an
\* accept ask their home to cancel. A send is not cancelled. The
\* "dropRelease" variant releases the backing at the drop.
Drop(c) ==
  /\ Runs
  /\ fut[c] = "att"
  /\ Returns(c)
  /\ UNCHANGED <<lifeVars, gen, k, zcVars>>
  /\ IF st[c] = "term"
       THEN FreeSlot(c) /\ Abandon(c) /\ UNCHANGED rq
       ELSE /\ IF c # "tx" THEN Ask({c}) ELSE UNCHANGED <<rq, cx>>
            /\ bk' = IF Bug = "dropRelease"
                       THEN [bk EXCEPT ![c][gen[c]] = Rel(@)]
                       ELSE bk
            /\ UNCHANGED <<st, home, rxVars, txVars, acVars>>

\* A forgotten receive future ends interest with no destructor, so no
\* request. A forgotten send is the same as a dropped send.
Forget ==
  /\ Runs
  /\ fut["rx"] = "att"
  /\ Returns("rx")
  /\ UNCHANGED <<lifeVars, gen, k, rq, zcVars>>
  /\ IF st["rx"] = "term"
       THEN FreeSlot("rx") /\ Abandon("rx")
       ELSE UNCHANGED <<st, home, cx, bk, rxVars, txVars, acVars>>

-----------------------------------------------------------------------------
(* The home driver and the kernel. *)

\* The home harvests one event of slot c. The kernel's choice follows the
\* terminal-event table. An event in "fly" depends on the peer, so it has
\* no fairness. The event after a cancel and a release event will arrive.
\* "tail" ends with the result that the slot holds: the zero-copy notice,
\* or the IOCP packet after an immediate return.
CompleteRx ==
  /\ st["rx"] = "live" /\ Driven("rx") /\ k["rx"] # "none"
  /\ k' = [k EXCEPT !["rx"] = "none"]
  /\ EndSlot("rx")
  /\ IF k["rx"] = "tail"
       THEN EndRx(rbuf, fut["rx"] = "att", ro) /\ UNCHANGED <<rxk, ks>>
       ELSE \E x \in RxResults(k["rx"] = "cxl") :
              /\ rxk' = rxk + x[1]
              /\ ks' = IF x[2] = "err" THEN "end" ELSE ks
              /\ EndRx([i \in 1..x[1] |-> rxk + i - 1], fut["rx"] = "att", x[2])
  /\ UNCHANGED <<lifeVars, fut, wake, gen, rq, got, ce, txVars, acVars, zcVars>>

\* The send result is the terminal event of the send (selected on
\* 2026-11-08). On io_uring a result with IORING_CQE_F_MORE (zc) also has a
\* zero-copy notice to come: the send ends, its slot frees, and its backing
\* joins the pending notices with the home of the send. A failed send is a
\* partial send, and it can also have IORING_CQE_F_MORE. On IOCP, "tail" is
\* the packet after an immediate return. On IOCP, a cancel that finds the
\* send running can abort the connection (selected on 2026-11-17): the
\* packet gives an error and no count, so the send fails with unknown
\* progress. The next receive result is then the error (ks = "fail").
CompleteTx ==
  /\ st["tx"] = "live" /\ Driven("tx") /\ k["tx"] # "none"
  /\ k' = [k EXCEPT !["tx"] = "none"]
  /\ EndSlot("tx")
  /\ UNCHANGED <<lifeVars, fut, wake, gen, rq, rxk, rbuf, held, got, ro, re, ce,
                 rep, acVars, debt, pg>>
  /\ IF k["tx"] = "tail"
       THEN EndTx(tp, fut["tx"] = "att", FALSE, ab) /\ UNCHANGED <<pn, nh, ab, ks>>
       ELSE \E t \in {"part", "full"}, zc \in BOOLEAN, abort \in BOOLEAN :
              /\ zc => Backend = "uring"
              /\ abort => Backend = "iocp" /\ k["tx"] = "cxl" /\ t = "part"
              /\ ab' = (ab \/ abort)
              /\ ks' = IF abort /\ ks = "open" THEN "fail" ELSE ks
              /\ EndTx(t, fut["tx"] = "att", zc, ab \/ abort)
              /\ IF zc
                   THEN /\ pn' = pn \cup {gen["tx"]}
                        /\ nh' = [nh EXCEPT ![gen["tx"]] = home["tx"]]
                   ELSE UNCHANGED <<pn, nh>>

\* On io_uring a multishot accept item with IORING_CQE_F_MORE gives one
\* connection; the final completion follows.
CompleteAc ==
  /\ st["ac"] = "live" /\ Driven("ac") /\ k["ac"] # "none"
  /\ UNCHANGED <<lifeVars, fut, wake, gen, rq, rxVars, txVars, uc, zcVars>>
  /\ IF k["ac"] = "tail"
       THEN /\ k' = [k EXCEPT !["ac"] = "none"]
            /\ EndSlot("ac")
            /\ EndAc(ar, fut["ac"] = "att")
            /\ UNCHANGED kc
       ELSE \E m \in 0..(IF kc > 0 THEN 1 ELSE 0) :
              /\ kc' = kc - m
              /\ \/ /\ k' = [k EXCEPT !["ac"] = "none"]
                    /\ EndSlot("ac")
                    /\ EndAc(ar + m, fut["ac"] = "att")
                 \/ /\ Backend = "uring" /\ m = 1
                    /\ ar' = ar + 1
                    /\ UNCHANGED <<k, st, home, cx, woke, hc, bk>>

Complete(c) ==
  CASE c = "rx" -> CompleteRx [] c = "tx" -> CompleteTx [] c = "ac" -> CompleteAc

\* The home takes request entry <<c, g>>. A cancel that finds its target
\* in flight ("done" or "already running") makes the terminal event
\* certain. "Not found" and a stale generation have no effect. The
\* "noGeneration" variant omits the generation check. A request for a live
\* operation waits for its home; a stale request has no effect anywhere.
Process(c, g) ==
  /\ <<c, g>> \in rq
  /\ g = gen[c] /\ st[c] = "live" => Driven(c)
  /\ rq' = rq \ {<<c, g>>}
  /\ k' = IF /\ g = gen[c] \/ Bug = "noGeneration"
             /\ st[c] = "live" /\ k[c] = "fly"
            THEN [k EXCEPT ![c] = "cxl"]
            ELSE k
  /\ UNCHANGED <<lifeVars, futVars, gen, st, home, cx, bk, rxVars, txVars,
                 acVars, zcVars>>

\* After the task ends, close takes each result that a dropped future
\* left, by the abandoned rule.
TakeOver(c) ==
  /\ st[c] = "term"
  /\ fut[c] = "none"
  /\ FreeSlot(c)
  /\ Abandon(c)
  /\ UNCHANGED <<lifeVars, futVars, gen, k, rq, zcVars>>

-----------------------------------------------------------------------------
(* Close and shutdown. *)

\* Close first cancels each outstanding operation, a dropped send included.
\* The "closeNoCancel" variant does not cancel.
CloseCancels(sl) == IF Bug = "closeNoCancel" THEN {} ELSE sl

\* Close then waits for the terminal event of each operation, and returns.
\* It releases the bytes that no receive took, drops a kept error, reports
\* the owed send progress (with no count after an IOCP abort), and closes
\* each connection that no accept took. It does not
\* wait for the pending notices: it half-closes the write side and leaves
\* them to the driver as detached debt. Each send result came before this
\* step, so its progress is final. The "reportTwice" variant also reports
\* progress that a send error already reported. The "zcWait" variant, the
\* design before 2026-11-07, also waits for every pending notice.
CloseDone(r) ==
  /\ cl[r] = "closing"
  /\ \A c \in ChOf(r) : st[c] = "free"
  /\ Bug = "zcWait" /\ r = "s" => pn = {}
  /\ cl' = [cl EXCEPT ![r] = "closed"]
  /\ UNCHANGED <<life, wk, alive, futVars, gen, st, home, k, cx, rq, rxk,
                 rbuf, got, ks, ro, ce, tp, kc, ar, uc, pg, pn, nh, required, ab>>
  /\ IF r = "s"
       THEN /\ held' = <<>>
            /\ re' = FALSE
            /\ bk' = [bk EXCEPT !["rx"] = ReleaseHeld(@)]
            /\ debt' = IF Bug = "omitDebt" /\ (\E g, h \in pn : g # h)
                         THEN pn \ {CHOOSE g \in pn : TRUE} ELSE pn
            /\ IF wd \in {"owe", "lost"} \/ (Bug = "reportTwice" /\ wd = "failed")
                 THEN /\ rep' = [rep EXCEPT ![gen["tx"]] = IF Bug = "omitReport" THEN @ ELSE @ + 1]
                      /\ wd' = "failed"
                 ELSE UNCHANGED <<rep, wd>>
            /\ UNCHANGED hc
       ELSE /\ hc' = 0
            /\ UNCHANGED <<held, re, bk, rep, wd, debt>>

\* A reset ends the kernel's hold of the stream's queued bytes: connect
\* with AF_UNSPEC purges the send queue, so every pending notice of the
\* connection then comes from the kernel and the device, not from the
\* peer. The driver resets at the drain limit, under budget pressure, or at
\* grace expiry. Budget pressure depends on other admissions, so the step
\* can come at any time while the debt is detached; only the drain limit
\* and grace expiry give it fairness (see Fairness).
Reset ==
  /\ dt
  /\ ~pg
  /\ pg' = TRUE
  /\ UNCHANGED <<lifeVars, futVars, opVars, rxVars, txVars, acVars, debt, pn, nh>>

\* The zero-copy notice of send g releases its backing and charge, once.
\* Before a reset, the peer's acknowledgement gives it; after a reset, the
\* kernel does. When the last pending notice of detached debt arrives, the
\* descriptor closes and the debt ends.
Notice(g) ==
  /\ g \in pn
  /\ NDriven(g)
  /\ pn' = pn \ {g}
  /\ nh' = [nh EXCEPT ![g] = NoHome]
  /\ bk' = [bk EXCEPT !["tx"][g] = IF @ = "zc" THEN "out" ELSE "dbl"]
  /\ debt' = debt \ {g}
  /\ UNCHANGED <<lifeVars, futVars, gen, st, home, k, cx, rq, rxVars, txVars,
                 acVars, pg>>

\* Shutdown closes ingress, and its grace period expires. The work of the
\* consumer during the grace period is the same as its work before the
\* request, so the model merges the two steps.
Shutdown ==
  /\ life = "run"
  /\ life' = "down"
  /\ UNCHANGED <<wk, alive, cl, futVars, opVars, rxVars, txVars, acVars, zcVars>>

\* The consumer task ends: it returns, or grace expiry cancels it. Its
\* futures drop, and each resource that it owns closes. Close takes the
\* resource by value, so no future of the resource is left. Each held slot
\* and each pending notice gets the home where the consumer submitted its
\* send. The workers are symmetric, so the first held slot, or with none
\* the first pending notice, gets the first worker: this removes only
\* mirror images of behaviors.
Held == {c \in Ch : st[c] # "free"}
FirstHeld == IF "rx" \in Held THEN "rx" ELSE IF "tx" \in Held THEN "tx" ELSE "ac"
FirstPending == CHOOSE g \in pn : \A x \in pn : g <= x
W0 == CHOOSE w \in Workers : TRUE

EndTask ==
  /\ alive
  /\ alive' = FALSE
  /\ \E h \in [Ch -> Workers], hn \in [pn -> Workers] :
       /\ Held # {} => h[FirstHeld] = W0
       /\ Held = {} /\ pn # {} => hn[FirstPending] = W0
       /\ home' = [c \in Ch |-> IF c \in Held THEN h[c] ELSE NoHome]
       /\ nh' = [g \in Gens |-> IF g \in pn THEN hn[g] ELSE NoHome]
  /\ fut' = [c \in Ch |-> "none"]
  /\ wake' = 1
  /\ woke' = [c \in Ch |-> FALSE]
  /\ cl' = [r \in Res |-> IF cl[r] = "open" THEN "closing" ELSE cl[r]]
  /\ Ask({c \in {"rx", "ac"} : fut[c] = "att" /\ st[c] = "live"}
         \cup CloseCancels({c \in Ch : cl[ResOf(c)] = "open"
                                       /\ st[c] = "live"}))
  /\ UNCHANGED <<life, wk, gen, st, k, bk, rxVars, txVars, acVars, debt, pg, pn>>

\* A worker exits after its task ends, and only when no operation that it
\* homes is live or holds its slot, and no pending notice has it as its
\* home. The "exitLive" variant exits with live operations.
Exit(w) ==
  /\ wk[w] = "run"
  /\ life = "down"
  /\ ~alive
  /\ \/ Bug = "exitLive"
     \/ /\ \A c \in Ch : st[c] # "free" => home[c] # w
        /\ \A g \in pn : nh[g] # w
  /\ wk' = [wk EXCEPT ![w] = "exit"]
  /\ UNCHANGED <<life, alive, cl, futVars, opVars, rxVars, txVars, acVars, zcVars>>

\* Shutdown joined every worker, and every close returned.
Done ==
  /\ \A w \in Workers : wk[w] = "exit"
  /\ \A r \in Res : cl[r] = "closed"

\* The end state stutters, so deadlock checking stays meaningful.
Terminated == Done /\ UNCHANGED vars

-----------------------------------------------------------------------------
(* The specification. *)

Init ==
  /\ life = "run"
  /\ wk = [w \in Workers |-> "run"]
  /\ alive = TRUE
  /\ cl = [r \in Res |-> "open"]
  /\ fut = [c \in Ch |-> "none"]
  /\ wake = 1
  /\ woke = [c \in Ch |-> FALSE]
  /\ gen = [c \in Ch |-> 0]
  /\ st = [c \in Ch |-> "free"]
  /\ home = [c \in Ch |-> NoHome]
  /\ k = [c \in Ch |-> "none"]
  /\ cx = [c \in Ch |-> FALSE]
  /\ rq = {}
  /\ bk = [c \in Ch |-> [g \in Gens |-> "none"]]
  /\ rxk = 1
  /\ rbuf = <<>>
  /\ held = <<>>
  /\ got = <<>>
  /\ ks = "open"
  /\ ro = "ok"
  /\ re = FALSE
  /\ ce = "none"
  /\ tp = "none"
  /\ wd = "ok"
  /\ rep = [g \in Gens |-> 0]
  /\ required = {}
  /\ ab = FALSE
  /\ kc = Conns
  /\ ar = 0
  /\ hc = 0
  /\ uc = 0
  /\ debt = {}
  /\ pg = FALSE
  /\ pn = {}
  /\ nh = [g \in Gens |-> NoHome]

Next ==
  \/ Shutdown \/ EndTask
  \/ \E r \in Res : CloseDone(r)
  \/ BeginRx \/ BeginTx \/ BeginAc
  \/ \E w \in Workers : Exit(w)
  \/ \E c \in Ch :
       \/ Claim(c) \/ Cancel(c) \/ Drop(c) \/ TakeOver(c) \/ Complete(c)
  \/ Repoll \/ Forget
  \/ \E x \in rq : Process(x[1], x[2])
  \/ Reset \/ \E g \in Gens : Notice(g)
  \/ Terminated

\* An event that the kernel delivers with no peer: the event after a cancel
\* that found its target, and an IOCP packet after an immediate return. A
\* zero-copy notice waits for the peer's acknowledgement, so it is not such
\* an event; after a reset, the kernel gives it (Notice with pg).
KernelEvent(c) == k[c] \in {"cxl", "tail"}

\* The progress assumption: each home takes each request entry, the kernel
\* delivers each event that it can deliver with no peer, close takes each
\* result that a dropped future left and then returns, grace expiry cancels
\* the task, and each worker exits when it can. While the runtime runs, the
\* drain-limit timer resets detached debt. The timers close at grace
\* expiry, so then grace expiry resets it. Each of these steps has its own
\* weak fairness. An event that the peer controls ("fly", and a zero-copy
\* notice before a reset) has no fairness, so the peer can refuse to read or
\* to send. Budget pressure and the consumer have no fairness. The
\* "noDrainLimit" variant has no drain limit, and the "noGraceReset"
\* variant has no reset at grace expiry.
Fairness ==
  /\ \A c \in Ch, g \in Gens : WF_vars(Process(c, g))
  /\ \A c \in Ch : WF_vars(KernelEvent(c) /\ Complete(c))
  /\ \A g \in Gens : WF_vars(pg /\ Notice(g))
  /\ \A c \in Ch : WF_vars(cl[ResOf(c)] = "closing" /\ TakeOver(c))
  /\ \A r \in Res : WF_vars(CloseDone(r))
  /\ WF_vars(life = "down" /\ EndTask)
  /\ \A w \in Workers : WF_vars(Exit(w))
  /\ WF_vars(Bug # "noDrainLimit" /\ life = "run" /\ Reset)
  /\ WF_vars(Bug # "noGraceReset" /\ life = "down" /\ Reset)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety properties. *)

\* Each backing and charge is released exactly once and never before its
\* terminal event. No accepted connection is lost before the listener
\* closes. At the end, nothing is held.
ReleaseOnce ==
  /\ \A c \in Ch, g \in Gens :
       /\ bk[c][g] # "dbl"
       /\ bk[c][g] \in {"held", "out"} /\ g = gen[c] => k[c] = "none"
  \* A send's backing stays with the driver exactly while its notice is
  \* pending.
  /\ \A g \in Gens : (g \in pn) = (bk["tx"][g] = "zc")
  /\ cl["s"] = "closed" => debt = pn
  /\ cl["l"] # "closed" => kc + ar + hc + uc = Conns
  /\ Done => /\ \A c \in Ch, g \in Gens : bk[c][g] \in {"none", "out"}
             /\ pn = {}
             /\ held = <<>> /\ rbuf = <<>> /\ ar = 0 /\ hc = 0

\* Teardown never waits for an event that cannot arrive: each live
\* operation has an event to come, and its home is driven.
NoImpossibleWait ==
  /\ \A c \in Ch : st[c] = "live" => k[c] # "none" /\ Driven(c)
  /\ \A g \in pn : NDriven(g)

\* The caller receives each byte once and in order.
InOrderOnce ==
  /\ \A i \in 1..Len(got) : got[i] = i
  \* Close may discard unread bytes. Before close, every received byte has an owner.
  /\ cl["s"] # "closed" =>
       got \o held \o rbuf = [i \in 1..(rxk - 1) |-> i]

\* A stale identity never changes a reused slot: only a request for the
\* operation now in the slot can cancel it.
NoStaleEffect == \A c \in Ch : k[c] = "cxl" => cx[c]

\* An abandoned send's progress is reported exactly once.
ReportOnce ==
  /\ \A g \in Gens : rep[g] <= IF g \in required THEN 1 ELSE 0
  /\ wd = "failed" \/ cl["s"] = "closed" =>
       \A g \in required : rep[g] = 1
  /\ Done => wd \notin {"owe", "lost"}

\* After an IOCP abort, no report claims a count: the progress of the send
\* is unknown, and a count would be false.
ExactCount == ab => wd # "owe"

\* A receive never gives the end of the stream in place of its error. The
\* peer of the model never ends the stream: it resets it, or it keeps it
\* open. So the first result with no bytes that the caller receives, other
\* than a cancel, is the error. An end of the stream first hides the error.
NoFalseEnd == ce # "end"

(* Progress properties. Each one is a []<> form, which TLC checks with no *)
(* tableau. Each form is equal to the leads-to form in its comment;       *)
(* StablePhases checks the side condition that the argument needs.        *)

\* A close does not restart: "closing" leaves only to "closed". Shutdown
\* does not restart: "down" stays.
StablePhases ==
  [][/\ \A r \in Res : cl[r] = "closing" => cl'[r] \in {"closing", "closed"}
     /\ life = "down" => life' = "down"]_vars

\* Each close returns: cl[r] = "closing" ~> cl[r] = "closed". The leads-to
\* form gives this form. Back: by StablePhases, "closing" leaves only to
\* "closed", so a state that is not "closing" after a "closing" state is
\* "closed".
CloseReturns == \A r \in Res : []<>(cl[r] # "closing")

\* Shutdown joins every worker: life = "down" ~> Done. The leads-to form
\* gives this form. Back: by StablePhases, "down" stays, so from a "down"
\* state this form gives Done infinitely often.
ShutdownJoins == []<>(life = "down" => Done)

\* Detached debt ends while the runtime runs, also with no shutdown
\* request: dt ~> ~dt. Detached debt ends only when its last pending
\* notice releases its backing, so this covers each pending backing. The target is ~dt, which is the negation of the
\* source, so the two forms are equal with no side condition.
DetachedReleased == []<>(~dt)

\* A waiting future's latest waker is woken:
\* Ready(c) ~> (woke[c] \/ ~Ready(c)). The target includes ~Ready(c), so
\* the two forms are equal with no side condition.
LatestWakerWoken == \A c \in Ch : []<>(~Ready(c) \/ woke[c])

-----------------------------------------------------------------------------
(* Reachability targets. Each `_reach` profile checks one Unreached       *)
(* property. A pass is a trace that violates it, so the trace reaches the  *)
(* target. A state target is an invariant; an action target is a [][~A]   *)
(* property.                                                               *)

\* Terminal events. An io_uring single completion is terminal.
ReachUringSingle == st["rx"] = "live" /\ k["rx"] \in {"fly", "cxl"} /\ st'["rx"] # "live"
UnreachedUringSingle == [][~ReachUringSingle]_vars
\* A multishot accept ends with its final completion, after an item.
ReachMultishotFinal == st["ac"] = "live" /\ ar > 0 /\ st'["ac"] # "live"
UnreachedMultishotFinal == [][~ReachMultishotFinal]_vars
\* A zero-copy notice releases the backing of its send.
ReachZcNotice == \E g \in pn : g \notin pn' /\ bk'["tx"][g] = "out"
UnreachedZcNotice == [][~ReachZcNotice]_vars
\* A send completion with no IORING_CQE_F_MORE is terminal.
ReachZcNoNotice ==
  st["tx"] = "live" /\ k["tx"] \in {"fly", "cxl"} /\ st'["tx"] # "live" /\ pn' = pn
\* A send result with IORING_CQE_F_MORE ends the send and frees its slot,
\* and its notice is pending.
ReachZcResult ==
  /\ st["tx"] = "live" /\ st'["tx"] # "live"
  /\ gen["tx"] \notin pn /\ gen["tx"] \in pn'
UnreachedZcResult == [][~ReachZcResult]_vars
\* Two zero-copy notices are pending on one stream.
ReachTwoPending == \E g1, g2 \in pn : g1 # g2
UnreachedTwoPending == ~ReachTwoPending
UnreachedZcNoNotice == [][~ReachZcNoNotice]_vars
\* IOCP: after STATUS_PENDING, the packet is terminal.
ReachIocpPending == st["rx"] = "live" /\ k["rx"] \in {"fly", "cxl"} /\ st'["rx"] # "live"
UnreachedIocpPending == [][~ReachIocpPending]_vars
\* IOCP: an immediate NT_ERROR return is terminal.
ReachIocpError ==
  gen'["rx"] = gen["rx"] + 1 /\ st'["rx"] = "term" /\ rbuf' = <<>> /\ ro' = "err"
UnreachedIocpError == [][~ReachIocpError]_vars
\* IOCP: after an immediate success, the packet is terminal.
ReachIocpOtherPacket == st["rx"] = "live" /\ k["rx"] = "tail" /\ st'["rx"] # "live"
UnreachedIocpOtherPacket == [][~ReachIocpOtherPacket]_vars
\* IOCP: with skip-on-success, an immediate success return is terminal.
ReachIocpSkip == gen'["rx"] = gen["rx"] + 1 /\ st'["rx"] = "term" /\ rbuf' # <<>>
UnreachedIocpSkip == [][~ReachIocpSkip]_vars

\* Cancel results. A cancel finds its target in flight ("done" or
\* "already running").
ReachCancelFound == \E c \in Ch : k[c] = "cxl"
UnreachedCancelFound == ~ReachCancelFound
\* A current request for a send finds no target in flight ("not found"),
\* because the send result came first, and its notice follows.
ReachCancelNotFound ==
  \E x \in rq : /\ x \notin rq' /\ x[1] = "tx"
                /\ x[2] = gen["tx"] /\ x[2] \in pn
                /\ k' = k
UnreachedCancelNotFound == [][~ReachCancelNotFound]_vars

\* Abandoned rules. A receive returns a prefix of held bytes and keeps the
\* rest.
ReachHeldBytes == held # <<>> /\ held' # <<>> /\ Len(got') > Len(got)
UnreachedHeldBytes == [][~ReachHeldBytes]_vars
\* The next send reports the progress of an abandoned send.
ReachSendError == wd = "owe" /\ wd' = "failed" /\ cl["s"] = "open"
UnreachedSendError == [][~ReachSendError]_vars
\* The next receive returns the error that an abandoned receive kept.
ReachKeptError == re /\ ~re' /\ ce' = "err" /\ cl["s"] = "open"
UnreachedKeptError == [][~ReachKeptError]_vars
\* IOCP: a cancel of a running send aborts the connection.
ReachIocpAbort == ~ab /\ ab'
UnreachedIocpAbort == [][~ReachIocpAbort]_vars
\* The next accept returns a connection that the listener holds.
ReachHeldConn == hc > 0 /\ uc' = uc + 1
UnreachedHeldConn == [][~ReachHeldConn]_vars
\* The close result reports the progress of an abandoned send.
ReachCloseReport == cl["s"] = "closing" /\ cl'["s"] = "closed" /\ wd = "owe"
UnreachedCloseReport == [][~ReachCloseReport]_vars
\* An explicit cancellation returns received bytes in the original buffer.
ReachCancelledData ==
  /\ cx["rx"] /\ st["rx"] = "term" /\ fut["rx"] = "att" /\ rbuf # <<>>
  /\ fut'["rx"] = "none" /\ Len(got') > Len(got)
UnreachedCancelledData == [][~ReachCancelledData]_vars

\* Close and shutdown with live operations. The stream closes with a live
\* receive and a live send.
ReachCloseLive == cl["s"] = "closing" /\ st["rx"] = "live" /\ st["tx"] = "live"
UnreachedCloseLive == ~ReachCloseLive
\* After shutdown, a close request waits at one home, and the other home
\* holds a live operation.
ReachShutdownCross ==
  /\ life = "down"
  /\ \E c, d \in Ch : /\ st[c] = "live" /\ st[d] = "live" /\ home[c] # home[d]
                      /\ <<c, gen[c]>> \in rq
UnreachedShutdownCross == ~ReachShutdownCross
\* Close returns with a zero-copy notice outstanding: detached debt.
ReachDetachedClose == dt
UnreachedDetachedClose == ~ReachDetachedClose
\* A close detaches two pending notices.
ReachDetachTwo ==
  /\ cl["s"] = "closing" /\ cl'["s"] = "closed" /\ dt'
  /\ \E g1, g2 \in pn : g1 # g2
UnreachedDetachTwo == [][~ReachDetachTwo]_vars
\* The peer's acknowledgement ends detached debt, with no reset.
ReachDetachedAck == dt /\ ~pg /\ ~dt'
UnreachedDetachedAck == [][~ReachDetachedAck]_vars
\* A reset ends detached debt, and the notice follows from the kernel.
ReachDetachedReset == dt /\ pg /\ ~dt'
UnreachedDetachedReset == [][~ReachDetachedReset]_vars

\* Races and the properties that are not vacuous. A stale request waits
\* while its slot holds a new operation.
ReachStaleRequest == \E x \in rq : x[2] < gen[x[1]] /\ st[x[1]] = "live" /\ k[x[1]] = "fly"
UnreachedStaleRequest == ~ReachStaleRequest
\* An operation reaches its terminal event while its cancel request waits.
ReachCancelRacesCompletion ==
  \E c \in Ch : <<c, gen[c]>> \in rq /\ st[c] = "live" /\ st'[c] # "live"
UnreachedCancelRacesCompletion == [][~ReachCancelRacesCompletion]_vars
\* A future drops or is forgotten while its cancel request waits.
ReachCancelRacesDrop ==
  \E c \in Ch : <<c, gen[c]>> \in rq /\ fut[c] = "att" /\ fut'[c] = "none" /\ alive'
UnreachedCancelRacesDrop == [][~ReachCancelRacesDrop]_vars
\* The home wakes a waker that a later poll gave.
ReachNewWaker == wake = 2 /\ Ready("rx") /\ woke["rx"]
UnreachedNewWaker == ~ReachNewWaker

=============================================================================
