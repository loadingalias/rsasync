--------------------------- MODULE TaskLifecycle ---------------------------
(***************************************************************************)
(* Task admission, wake publication, cancellation, slot reuse, and         *)
(* shutdown in rsasync (task R100).                                        *)
(*                                                                         *)
(* Workers poll local tasks, which stay on their home, and movable tasks,  *)
(* which an idle worker can steal. The block_on owner B is the thread that *)
(* calls block_on: it is a local owner for the call. A spawn reserves a    *)
(* task slot before it admits the task. A task waits on its own event: it  *)
(* registers its waker, rechecks the event, and then returns Pending. A    *)
(* wake from any thread is one atomic change of the slot word, which also  *)
(* holds the generation, and it publishes the task to the remote-ready     *)
(* list of its home. The push that finds the list empty signals the home.  *)
(* A worker puts that signal in its outbox, which its next turn sends. A   *)
(* signal handler owns no worker: it sets a flag and writes the wake       *)
(* descriptor of the signal home. A task can wait for a timer of the       *)
(* signal home, which then parks with a deadline. Cancellation drops a     *)
(* future at a poll boundary on its owner. Shutdown closes ingress, grace  *)
(* expiry cancels each task, and a worker exits only after the last task   *)
(* ends. Each action is one atomic step of the design or the code; the     *)
(* README maps each action to its design rule and its code.                *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS
  Workers,  \* The runtime workers. Each one polls local and movable tasks.
  B,        \* The block_on owner.
  NSlots,   \* The number of task slots.
  MaxGen,   \* The generations of each slot: a slot is reused MaxGen - 1 times.
  Bug       \* "none", or the defect of one known-bad variant.

ASSUME B \notin Workers
ASSUME NSlots \in Nat
ASSUME MaxGen \in Nat \ {0}
ASSUME Bug \in {"none", "noRecheck", "loseCoalesced", "staleGen", "earlyExit",
                "doublePush", "stealLocal", "admitClosed", "parkBeforeDrain",
                "parkWithFlag", "noGraceWake", "blockOnNoCancel",
                "blockOnNoWait", "noFlush", "noDeadline"}

Slots == 1..NSlots
Owners == Workers \cup {B}
Gens == 1..MaxGen
Live == {"idle", "sched", "run"}
None == "none"   \* No owner.
NoSlot == 0      \* No slot.
\* The signal home. Its driver watches the wake descriptor that the signal
\* handler writes. It is also the one worker that parks (README, reduction 2).
SH == CHOOSE w \in Workers : TRUE
\* The sender of a push whose signal is immediate: a thread with no ring, the
\* block_on owner (a thread home, whose outbox has no room), or a worker
\* whose outbox is full. The other senders are the workers other than SH,
\* whose signal waits in their outbox, and SH itself, which sends no signal.
Ext == "ext"
Senders == {Ext} \cup (Workers \ {SH})

VARIABLES
  ing,   \* Ingress: "open", or "closed" by the shutdown request.
  bo,    \* block_on: "off", "on" (the root runs), or "ending" (the root
         \* completed). A block_on call can start again after it returned.
  wk,    \* wk[w]: "run", or "exit" after the worker exited.
  pk,    \* The signal home: "run", or "parked" in the kernel until its
         \* descriptor is readable.
  sig,   \* The wake descriptor of the signal home is readable.
  sf,    \* The signal flag that the handler sets.
  ob,    \* ob[w]: the outbox of worker w holds a signal of the signal home.
         \* ob[SH] stays FALSE: the home sends itself no signal.
  cur,   \* cur[o]: the slot whose future o polls, or NoSlot.
  rq,    \* rq[o]: the run queue of o. Only o changes it, except a steal.
  rr,    \* The remote-ready list of the signal home, one intrusive entry for
         \* each slot. A push to an owner that does not park goes to its run
         \* queue (reduction 3).
  ss,    \* ss[s]: "free", "rsv" (reserved, not admitted), "idle" (admitted,
         \* waiting), "sched" (in one queue), or "run" (polled).
  pc,    \* pc[s]: the step of the poll of s: "none", "body", or "chk" (the
         \* "noRecheck" variant: checked, not registered).
  gen,   \* gen[s]: the generation of the task in slot s. MaxGen + 1 retires it.
  kind,  \* kind[s]: "none", "local", or "movable".
  home,  \* home[s]: the home owner of a local task, the location of a
         \* movable task, or None.
  ev,    \* ev[s]: the event of the task in slot s happened.
  reg,   \* reg[s]: the waker of the task in slot s is in its event.
  tm,    \* tm[s]: the task in slot s waits for its timer, in the timer
         \* storage of the signal home. The timer is its event.
  nt,    \* nt[s]: a wake came during the poll of s.
  cx     \* cx[s]: a cancel was requested for the task in slot s.

vars == <<ing, bo, wk, pk, sig, sf, ob, cur, rq, rr, ss, pc, gen, kind, home,
          ev, reg, tm, nt, cx>>

Running(o) ==
  CASE o = B  -> bo \in {"on", "ending"}
    [] o = SH -> wk[o] = "run" /\ pk = "run"
    [] OTHER  -> wk[o] = "run"
Q(o) == IF o = SH THEN rq[SH] \o rr ELSE rq[o]
InQ(o, s) == \E i \in 1..Len(Q(o)) : Q(o)[i] = s
\* A sender p can push now: a worker sends only while it runs. An IF, not a
\* disjunction: TLC takes each disjunct of an action as a branch.
Sends(p) == IF p = Ext THEN TRUE ELSE wk[p] = "run"
RemoveAt(q, i) == SubSeq(q, 1, i - 1) \o SubSeq(q, i + 1, Len(q))

\* A spawn takes the first free slot among the free slots of its
\* generation: two such slots are equal in every variable, so each other
\* choice is a mirror image of this one (reduction 9).
Pick(s) ==
  /\ ss[s] = "free" /\ gen[s] <= MaxGen
  /\ \A t \in 1..(s - 1) : ss[t] = "free" => gen[t] # gen[s]

-----------------------------------------------------------------------------
(* Wakes. A wake from any thread is one atomic change of the slot word,   *)
(* which holds the generation and the scheduling state, and the push of a *)
(* task that was idle (reduction 1).                                       *)

\* Publish slot s to the remote-ready list of o, from the thread of sender
\* p. Only the push that finds the list empty signals the home: one signal
\* for each batch. Ext writes the descriptor in the same step (reduction
\* 14). Another worker puts the signal in its outbox, and Flush sends it.
\* The signal home sends itself no signal: it looks at its list before it
\* parks.
Push(o, s, p) ==
  IF o = SH
    THEN /\ rr' = Append(rr, s)
         /\ CASE rr # <<>> \/ p = SH -> UNCHANGED <<sig, ob>>
              [] p = Ext             -> sig' = TRUE /\ UNCHANGED ob
              [] OTHER               -> /\ ob' = [ob EXCEPT ![p] = TRUE]
                                        /\ UNCHANGED sig
         /\ UNCHANGED rq
    ELSE /\ rq' = [rq EXCEPT ![o] = Append(@, s)]
         /\ UNCHANGED <<rr, sig, ob>>

\* The effect of a current wake of slot s from sender p; c is the cancel
\* bit.
WakeEffect(s, c, p) ==
  CASE ss[s] = "idle" ->
         /\ ss' = [ss EXCEPT ![s] = "sched"]
         /\ cx' = [cx EXCEPT ![s] = @ \/ c]
         /\ Push(home[s], s, p)
         /\ UNCHANGED nt
    [] ss[s] = "run" ->
         \* Coalesced: the poll end schedules the task again.
         /\ nt' = [nt EXCEPT ![s] = TRUE]
         /\ cx' = [cx EXCEPT ![s] = @ \/ c]
         /\ UNCHANGED <<ss, rq, rr, sig, ob>>
    [] ss[s] = "sched" ->
         \* Coalesced: the task is in a queue already.
         /\ cx' = [cx EXCEPT ![s] = @ \/ c]
         /\ IF Bug = "doublePush" THEN Push(home[s], s, p)
                                  ELSE UNCHANGED <<rq, rr, sig, ob>>
         /\ UNCHANGED <<ss, nt>>
    [] OTHER -> UNCHANGED <<ss, rq, rr, sig, ob, nt, cx>>

\* A waker or a cancel handle of an earlier generation, from any thread at
\* any time. The generation check rejects it (reduction 8).
Wake(s, g, c) ==
  /\ g < gen[s]
  /\ IF Bug = "staleGen" THEN WakeEffect(s, c, Ext)
                         ELSE UNCHANGED <<ss, rq, rr, sig, ob, nt, cx>>
  /\ UNCHANGED <<ing, bo, wk, pk, sf, cur, pc, gen, kind, home, ev, reg, tm>>

\* The event of the task in slot s happens on the thread of sender p: set
\* the event, then take the registered waker and wake it.
PublishEffect(s, p) ==
  /\ ss[s] \in Live /\ ~ev[s]
  /\ ev' = [ev EXCEPT ![s] = TRUE]
  /\ IF reg[s] THEN reg' = [reg EXCEPT ![s] = FALSE] /\ WakeEffect(s, FALSE, p)
               ELSE UNCHANGED <<reg, ss, rq, rr, sig, ob, nt, cx>>

\* A publisher on the thread of sender p. A task that waits for its timer
\* waits for no other event.
Publish(s, p) ==
  /\ ~tm[s] /\ Sends(p)
  /\ PublishEffect(s, p)
  /\ UNCHANGED <<ing, bo, wk, pk, sf, cur, pc, gen, kind, home, tm>>

\* The next drive turn or wait of worker w sends the signal in its outbox,
\* and the kernel posts it in the ring of the signal home: the descriptor
\* of the home is readable. A message that fails writes the interrupt
\* descriptor of the home at a later turn of w; this step includes that
\* path. The step can come at any step while w runs (reduction 15).
Flush(w) ==
  /\ ob[w] /\ wk[w] = "run" /\ Bug # "noFlush"
  /\ ob' = [ob EXCEPT ![w] = FALSE]
  /\ sig' = TRUE
  /\ UNCHANGED <<ing, bo, wk, pk, sf, cur, rq, rr, ss, pc, gen, kind, home,
                 ev, reg, tm, nt, cx>>

\* A signal handler sets the flag and writes the descriptor of the signal
\* home. It calls no waker.
SigRaise ==
  /\ ~sf /\ wk[SH] = "run"
  /\ sf' = TRUE
  /\ sig' = TRUE
  /\ UNCHANGED <<ing, bo, wk, pk, ob, cur, rq, rr, ss, pc, gen, kind, home,
                 ev, reg, tm, nt, cx>>

\* A turn of the driver of the signal home takes the flag and publishes the
\* event of the task that waits for the signal, if one waits. The step can
\* come at any step while the home runs (reduction 15).
SigHandle ==
  /\ Running(SH) /\ sf
  /\ sf' = FALSE
  /\ \/ \E s \in Slots : ~tm[s] /\ PublishEffect(s, SH)
     \/ /\ \A s \in Slots : ss[s] \notin Live \/ ev[s] \/ tm[s]
        /\ UNCHANGED <<ev, reg, ss, rq, rr, sig, ob, nt, cx>>
  /\ UNCHANGED <<ing, bo, wk, pk, cur, pc, gen, kind, home, tm>>

\* A turn of the signal home fires the timer of slot s, after its deadline,
\* and wakes its task. The deadline and the fire are one step (reduction
\* 13). A park of the home has the deadline of its next timer, so the step
\* also ends a park; the "noDeadline" variant parks with no deadline. The
\* step can come at any step while the home runs (reduction 15).
TimerFire(s) ==
  /\ wk[SH] = "run" /\ tm[s]
  /\ pk = "run" \/ Bug # "noDeadline"
  /\ pk' = "run"
  /\ tm' = [tm EXCEPT ![s] = FALSE]
  /\ PublishEffect(s, SH)
  /\ UNCHANGED <<ing, bo, wk, sf, cur, pc, gen, kind, home>>

-----------------------------------------------------------------------------
(* Reservation and admission. *)

\* A movable spawn from any thread reserves a slot. With no free slot, the
\* spawn returns its future with the capacity reason, and nothing changes.
Reserve(s) ==
  /\ Pick(s) /\ ing = "open"
  /\ ss' = [ss EXCEPT ![s] = "rsv"]
  /\ kind' = [kind EXCEPT ![s] = "movable"]
  /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, cur, rq, rr, pc, gen, home,
                 ev, reg, tm, nt, cx>>

\* The spawn on the thread of sender p admits the reserved task into the
\* pool at worker w, only while ingress is open: the ingress state and the
\* admission are one atomic word.
Admit(s, w, p) ==
  /\ ss[s] = "rsv" /\ Sends(p)
  /\ ing = "open" \/ Bug = "admitClosed"
  /\ ss' = [ss EXCEPT ![s] = "sched"]
  /\ home' = [home EXCEPT ![s] = w]
  /\ Push(w, s, p)
  /\ UNCHANGED <<ing, bo, wk, pk, sf, cur, pc, gen, kind, ev, reg, tm, nt, cx>>

\* After ingress closed, the spawn returns its future with the shutdown
\* reason and releases the reservation.
Refuse(s) ==
  /\ ss[s] = "rsv" /\ ing = "closed" /\ Bug # "admitClosed"
  /\ ss' = [ss EXCEPT ![s] = "free"]
  /\ kind' = [kind EXCEPT ![s] = "none"]
  /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, cur, rq, rr, pc, gen, home,
                 ev, reg, tm, nt, cx>>

\* A local spawn on the current owner o: from the poll of a task on o, or
\* from the root of block_on. The reservation and the admission are one
\* step on the owner's thread, and the admission checks ingress. The signal
\* home hosts no local task (reduction 11).
SpawnLocal(o, s) ==
  /\ Running(o)
  /\ CASE o = B  -> bo = "on" /\ cur[B] = NoSlot
       [] o = SH -> FALSE
       [] OTHER  -> cur[o] # NoSlot /\ pc[cur[o]] = "body"
  /\ Pick(s)
  /\ ing = "open" \/ Bug = "admitClosed"
  /\ ss' = [ss EXCEPT ![s] = "sched"]
  /\ rq' = [rq EXCEPT ![o] = Append(@, s)]
  /\ kind' = [kind EXCEPT ![s] = "local"]
  /\ home' = [home EXCEPT ![s] = o]
  /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, cur, rr, pc, gen, ev, reg, tm,
                 nt, cx>>

-----------------------------------------------------------------------------
(* Polls. Only the owner o changes cur[o] and the poll state. *)

\* The future of s is gone: it returned Ready, or its owner dropped it.
\* The drop of its timer frees the place of the timer and wakes nothing.
\* The join handle takes the output, and the slot frees with the next
\* generation (reduction 7). A slot at MaxGen retires.
EndSlot(s) ==
  /\ ss' = [ss EXCEPT ![s] = "free"]
  /\ gen' = [gen EXCEPT ![s] = @ + 1]
  /\ pc' = [pc EXCEPT ![s] = "none"]
  /\ kind' = [kind EXCEPT ![s] = "none"]
  /\ home' = [home EXCEPT ![s] = None]
  /\ ev' = [ev EXCEPT ![s] = FALSE]
  /\ reg' = [reg EXCEPT ![s] = FALSE]
  /\ tm' = [tm EXCEPT ![s] = FALSE]
  /\ nt' = [nt EXCEPT ![s] = FALSE]
  /\ cx' = [cx EXCEPT ![s] = FALSE]

\* Take slot s from a queue to o. A cancelled task drops at this poll
\* boundary; any other task starts its poll on o.
Take(o, s) ==
  IF cx[s]
    THEN /\ EndSlot(s)
         /\ UNCHANGED cur
    ELSE /\ cur' = [cur EXCEPT ![o] = s]
         /\ ss' = [ss EXCEPT ![s] = "run"]
         /\ pc' = [pc EXCEPT ![s] = "body"]
         /\ home' = [home EXCEPT ![s] = IF kind[s] = "movable" THEN o ELSE @]
         /\ UNCHANGED <<gen, kind, ev, reg, tm, nt, cx>>

\* A running owner harvests its remote-ready list into its run queue and
\* takes the head (reduction 4).
PollStart(o) ==
  /\ Running(o) /\ cur[o] = NoSlot /\ Q(o) # <<>>
  /\ Take(o, Head(Q(o)))
  /\ rq' = [rq EXCEPT ![o] = Tail(Q(o))]
  /\ rr' = IF o = SH THEN <<>> ELSE rr
  /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob>>

\* q without the slots of S.
Drop(q, S) == SelectSeq(q, LAMBDA x : x \notin S)

\* The movable entries of q, in queue order.
Movables(q) == SelectSeq(q, LAMBDA x : kind[x] = "movable")

\* An idle worker w steals from worker v: one batch steal (R401). The queue
\* of v is its run queue and, for the signal home, its remote-ready list. w
\* takes the oldest movable task of that queue and polls it. With t, it
\* also moves the next movable task to its own run queue, where it stays
\* scheduled; its location changes at its next poll. The "stealLocal"
\* variant takes the head of the queue of any kind, with no batch.
Steal(w, v, t) ==
  LET m == Movables(Q(v))
      s == IF Bug = "stealLocal" THEN Head(Q(v)) ELSE Head(m)
  IN /\ Running(w) /\ cur[w] = NoSlot /\ Q(w) = <<>>
     /\ IF Bug = "stealLocal" THEN Q(v) # <<>> ELSE m # <<>>
     /\ t = NoSlot \/ (Bug # "stealLocal" /\ Len(m) >= 2 /\ t = m[2])
     /\ Take(w, s)
     /\ rq' = [rq EXCEPT ![v] = Drop(@, {s, t}),
                         ![w] = IF t = NoSlot THEN @ ELSE Append(@, t)]
     /\ rr' = IF v = SH THEN Drop(rr, {s, t}) ELSE rr
     /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob>>

\* The poll returns Pending. A wake during the poll, the self-wake of a
\* spent poll budget (y), or a cancel request schedules the task again.
EndPending(o, s, y) ==
  /\ cur' = [cur EXCEPT ![o] = NoSlot]
  /\ pc' = [pc EXCEPT ![s] = "none"]
  /\ nt' = [nt EXCEPT ![s] = FALSE]
  /\ IF ((nt[s] \/ y) /\ Bug # "loseCoalesced") \/ cx[s]
       THEN /\ ss' = [ss EXCEPT ![s] = "sched"]
            /\ rq' = [rq EXCEPT ![o] = Append(@, s)]
       ELSE /\ ss' = [ss EXCEPT ![s] = "idle"]
            /\ UNCHANGED rq
  /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, rr, gen, kind, home, ev, reg,
                 tm, cx>>

Body(o) == cur[o] # NoSlot /\ pc[cur[o]] = "body"

\* The future returns Ready.
Finish(o) ==
  /\ Body(o)
  /\ EndSlot(cur[o])
  /\ cur' = [cur EXCEPT ![o] = NoSlot]
  /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, rq, rr>>

\* The future registers its waker in its event, then rechecks the event.
\* If the event happened, the future returns Ready; otherwise the poll
\* continues. The registration and the recheck are one step (reduction 10).
Register(o) ==
  /\ Body(o) /\ ~reg[cur[o]] /\ Bug # "noRecheck"
  /\ IF ev[cur[o]]
       THEN /\ EndSlot(cur[o])
            /\ cur' = [cur EXCEPT ![o] = NoSlot]
            /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, rq, rr>>
       ELSE /\ reg' = [reg EXCEPT ![cur[o]] = TRUE]
            /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, cur, rq, rr, ss, pc,
                           gen, kind, home, ev, tm, nt, cx>>

\* The future returns Pending with the registration that it holds, or after
\* a wake during this poll.
Keep(o) ==
  /\ Body(o) /\ (reg[cur[o]] \/ nt[cur[o]])
  /\ EndPending(o, cur[o], FALSE)

\* The poll budget is spent: the next leaf wakes its task and returns
\* Pending.
Yield(o) ==
  /\ Body(o)
  /\ EndPending(o, cur[o], TRUE)

\* The "noRecheck" variant: check the event, then register with no recheck.
Check(o) ==
  /\ Body(o) /\ ~reg[cur[o]] /\ ~ev[cur[o]] /\ Bug = "noRecheck"
  /\ pc' = [pc EXCEPT ![cur[o]] = "chk"]
  /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, cur, rq, rr, ss, gen, kind,
                 home, ev, reg, tm, nt, cx>>

RegisterLate(o) ==
  /\ cur[o] # NoSlot /\ pc[cur[o]] = "chk"
  /\ reg' = [reg EXCEPT ![cur[o]] = TRUE]
  /\ pc' = [pc EXCEPT ![cur[o]] = "body"]
  /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, cur, rq, rr, ss, gen, kind,
                 home, ev, tm, nt, cx>>

\* A future of a task on the signal home waits for its timer: it registers
\* its waker in a timer of the storage of the signal home. Each deadline
\* fires at a turn, so the future returns Pending. Only the signal home has
\* timers (reduction 13).
TimerArm(o) ==
  /\ o = SH /\ Body(o) /\ ~reg[cur[o]] /\ ~ev[cur[o]]
  /\ reg' = [reg EXCEPT ![cur[o]] = TRUE]
  /\ tm' = [tm EXCEPT ![cur[o]] = TRUE]
  /\ UNCHANGED <<ing, bo, wk, pk, sig, sf, ob, cur, rq, rr, ss, pc, gen, kind,
                 home, ev, nt, cx>>

PollStep(o) ==
  \/ Finish(o) \/ Register(o) \/ Keep(o) \/ Yield(o) \/ Check(o)
  \/ RegisterLate(o) \/ TimerArm(o)

-----------------------------------------------------------------------------
(* Workers. *)

\* The signal home parks in the kernel when it has no work. Its turn
\* harvested its remote-ready list, so a push after the harvest finds the
\* list empty and signals. It takes the signal flag first. With a timer in
\* its storage, the park has the deadline of the next turn of the storage.
Park ==
  /\ wk[SH] = "run" /\ pk = "run" /\ cur[SH] = NoSlot /\ rq[SH] = <<>>
  /\ rr = <<>> \/ Bug = "parkBeforeDrain"
  /\ ~sf \/ Bug = "parkWithFlag"
  /\ pk' = "parked"
  /\ UNCHANGED <<ing, bo, wk, sig, sf, ob, cur, rq, rr, ss, pc, gen, kind,
                 home, ev, reg, tm, nt, cx>>

\* The readable descriptor wakes the signal home, which reads it. The
\* deadline of a timed park ends it in TimerFire.
Unpark ==
  /\ wk[SH] = "run" /\ pk = "parked" /\ sig
  /\ pk' = "run"
  /\ sig' = FALSE
  /\ UNCHANGED <<ing, bo, wk, sf, ob, cur, rq, rr, ss, pc, gen, kind, home,
                 ev, reg, tm, nt, cx>>

\* No worker exits while a task of the runtime remains. The exit notice
\* and the exit are one step (reduction 5). The exit sends the signals of
\* the outbox of the worker.
Exit(w) ==
  /\ wk[w] = "run" /\ cur[w] = NoSlot /\ ing = "closed"
  /\ IF Bug = "earlyExit" THEN Q(w) = <<>>
                          ELSE \A s \in Slots : ss[s] \notin Live
  /\ wk' = [wk EXCEPT ![w] = "exit"]
  /\ pk' = IF w = SH THEN "run" ELSE pk
  /\ sig' = IF w = SH THEN FALSE ELSE sig \/ ob[w]
  /\ ob' = [ob EXCEPT ![w] = FALSE]
  /\ UNCHANGED <<ing, bo, sf, cur, rq, rr, ss, pc, gen, kind, home, ev,
                 reg, tm, nt, cx>>

-----------------------------------------------------------------------------
(* Shutdown and block_on. *)

\* The shutdown request closes ingress. block_on starts and ends while the
\* runtime runs.
Shutdown ==
  /\ ing = "open" /\ bo = "off"
  /\ ing' = "closed"
  /\ UNCHANGED <<bo, wk, pk, sig, sf, ob, cur, rq, rr, ss, pc, gen, kind,
                 home, ev, reg, tm, nt, cx>>

\* Grace expiry, and the end of block_on for its own tasks, cancel each
\* task: a cancel request is a wake with the cancel bit, from the thread of
\* sender p. Each task can run until its own cancel, so this step is also
\* the end of the drain (reduction 6).
DoomBy(s, p) ==
  /\ ss[s] \in Live /\ ~cx[s] /\ Sends(p)
  /\ ing = "closed" \/ (bo = "ending" /\ home[s] = B /\ Bug # "blockOnNoCancel")
  /\ IF Bug = "noGraceWake"
       THEN /\ cx' = [cx EXCEPT ![s] = TRUE]
            /\ UNCHANGED <<ss, rq, rr, sig, ob, nt>>
       ELSE WakeEffect(s, TRUE, p)
  /\ UNCHANGED <<ing, bo, wk, pk, sf, cur, pc, gen, kind, home, ev, reg, tm>>

Doom(s) == \E p \in Senders : DoomBy(s, p)

BlockOnStart ==
  /\ bo = "off" /\ ing = "open"
  /\ bo' = "on"
  /\ UNCHANGED <<ing, wk, pk, sig, sf, ob, cur, rq, rr, ss, pc, gen, kind,
                 home, ev, reg, tm, nt, cx>>

\* The root future completes on B, between polls of B's tasks.
RootDone ==
  /\ bo = "on" /\ cur[B] = NoSlot
  /\ bo' = "ending"
  /\ UNCHANGED <<ing, wk, pk, sig, sf, ob, cur, rq, rr, ss, pc, gen, kind,
                 home, ev, reg, tm, nt, cx>>

\* block_on returns after the last task on B ended.
BlockOnEnd ==
  /\ bo = "ending" /\ cur[B] = NoSlot
  /\ Bug = "blockOnNoWait" \/ \A s \in Slots : ss[s] \in Live => home[s] # B
  /\ bo' = "off"
  /\ UNCHANGED <<ing, wk, pk, sig, sf, ob, cur, rq, rr, ss, pc, gen, kind,
                 home, ev, reg, tm, nt, cx>>

Done == \A w \in Workers : wk[w] = "exit"

\* The end state stutters, so deadlock checking stays on.
Terminated == Done /\ UNCHANGED vars

-----------------------------------------------------------------------------
(* The specification. *)

Init ==
  /\ ing = "open"
  /\ bo = "off"
  /\ wk = [w \in Workers |-> "run"]
  /\ pk = "run"
  /\ sig = FALSE
  /\ sf = FALSE
  /\ ob = [w \in Workers |-> FALSE]
  /\ cur = [o \in Owners |-> NoSlot]
  /\ rq = [o \in Owners |-> <<>>]
  /\ rr = <<>>
  /\ ss = [s \in Slots |-> "free"]
  /\ pc = [s \in Slots |-> "none"]
  /\ gen = [s \in Slots |-> 1]
  /\ kind = [s \in Slots |-> "none"]
  /\ home = [s \in Slots |-> None]
  /\ ev = [s \in Slots |-> FALSE]
  /\ reg = [s \in Slots |-> FALSE]
  /\ tm = [s \in Slots |-> FALSE]
  /\ nt = [s \in Slots |-> FALSE]
  /\ cx = [s \in Slots |-> FALSE]

Next ==
  \/ \E s \in Slots :
       \/ Reserve(s) \/ Refuse(s) \/ Doom(s) \/ TimerFire(s)
       \/ \E p \in Senders : Publish(s, p)
       \/ \E o \in Owners : SpawnLocal(o, s)
       \/ \E w \in Workers, p \in Senders : Admit(s, w, p)
       \/ \E g \in Gens, c \in BOOLEAN : Wake(s, g, c)
  \/ \E o \in Owners : PollStart(o) \/ PollStep(o)
  \/ \E w, v \in Workers : w # v /\ \E t \in Slots \cup {NoSlot} : Steal(w, v, t)
  \/ Park \/ Unpark \/ \E w \in Workers : Exit(w) \/ Flush(w)
  \/ SigRaise \/ SigHandle
  \/ Shutdown
  \/ BlockOnStart \/ RootDone \/ BlockOnEnd
  \/ Terminated

\* The progress assumption: fair worker steps and cooperative polls. A
\* running owner starts a poll when its queues hold a task, and each poll
\* returns (the stated condition of cooperative polls). A readable
\* descriptor wakes the parked signal home. Each drive turn of the signal
\* home takes the flag and fires each timer after its deadline, and each
\* drive turn or wait of a worker sends its outbox. These steps can come at
\* any step while their thread runs, so weak fairness is enough (reduction
\* 15). The grace timer fires, and the cancellation of grace expiry and of
\* the end of block_on reaches each task. A worker exits, and block_on
\* returns, when it can. A spawn, a publish, a cancel handle, an old waker,
\* a signal, a timer, the release of a slot, the root, the shutdown request,
\* and a steal are free: they have no fairness.
Fairness ==
  /\ \A o \in Owners : WF_vars(PollStart(o)) /\ WF_vars(PollStep(o))
  /\ WF_vars(Unpark) /\ WF_vars(SigHandle)
  /\ \A w \in Workers : WF_vars(Exit(w))
  /\ \A w \in Workers \ {SH} : WF_vars(Flush(w))
  /\ \A s \in Slots : WF_vars(Doom(s)) /\ WF_vars(TimerFire(s))
  /\ WF_vars(BlockOnEnd)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety properties. *)

TypeOK ==
  /\ ing \in {"open", "closed"}
  /\ bo \in {"off", "on", "ending"}
  /\ wk \in [Workers -> {"run", "exit"}]
  /\ pk \in {"run", "parked"}
  /\ sig \in BOOLEAN
  /\ sf \in BOOLEAN
  /\ ob \in [Workers -> BOOLEAN]
  /\ cur \in [Owners -> Slots \cup {NoSlot}]
  /\ \A o \in Owners : rq[o] \in Seq(Slots)
  /\ rr \in Seq(Slots)
  /\ ss \in [Slots -> {"free", "rsv", "idle", "sched", "run"}]
  /\ pc \in [Slots -> {"none", "body", "chk"}]
  /\ gen \in [Slots -> 1..(MaxGen + 1)]
  /\ kind \in [Slots -> {"none", "local", "movable"}]
  /\ home \in [Slots -> Owners \cup {None}]
  /\ ev \in [Slots -> BOOLEAN]
  /\ reg \in [Slots -> BOOLEAN]
  /\ tm \in [Slots -> BOOLEAN]
  /\ nt \in [Slots -> BOOLEAN]
  /\ cx \in [Slots -> BOOLEAN]

\* One poll owner: at most one owner polls a task, and a task in the run
\* state has exactly that owner.
OnePollOwner ==
  /\ \A o1, o2 \in Owners : o1 # o2 /\ cur[o1] # NoSlot => cur[o1] # cur[o2]
  /\ \A s \in Slots : (ss[s] = "run") = (\E o \in Owners : cur[o] = s)

\* A local task is polled and queued only on its home. Its owner drops it
\* when it takes it from its own queue, so it also drops only there.
HomeOnly ==
  \A o \in Owners, s \in Slots :
    kind[s] = "local" /\ (cur[o] = s \/ InQ(o, s)) => home[s] = o

\* Reservations bound the queues: each queue entry is a reserved task in
\* the "sched" state, and each such task has exactly one entry.
BoundedReservations ==
  /\ \A o \in Owners : \A i, j \in 1..Len(Q(o)) : i # j => Q(o)[i] # Q(o)[j]
  /\ \A o1, o2 \in Owners, s \in Slots : o1 # o2 /\ InQ(o1, s) => ~InQ(o2, s)
  /\ \A s \in Slots : (ss[s] = "sched") = (\E o \in Owners : InQ(o, s))

\* No worker exits while a task of the runtime remains.
ExitAfterWork ==
  (\E w \in Workers : wk[w] = "exit") => \A s \in Slots : ss[s] \notin Live

\* block_on returns only after each task on its owner ended.
BlockOnJoins ==
  bo = "off" => \A s \in Slots : ss[s] \in Live => home[s] # B

\* The parked signal home has a readable descriptor, or a signal in the
\* outbox of a worker, when its remote-ready list holds work or the signal
\* flag is set.
ParkedHasSignal ==
  pk = "parked" /\ (rr # <<>> \/ sf) => sig \/ \E w \in Workers : ob[w]

\* A wake or a cancel of a stale generation changes no task.
StaleWakeRejected ==
  [][\A s \in Slots, g \in Gens, c \in BOOLEAN :
       g # gen[s] /\ Wake(s, g, c) => UNCHANGED <<ss[s], nt[s], cx[s], rq, rr>>]_vars

\* No task is admitted after ingress closed.
NoAdmissionAfterClose ==
  [][ing = "closed" => \A s \in Slots : ss[s] \notin Live => ss'[s] \notin Live]_vars

(* Progress properties. Each one is a []<> form, which TLC checks with no *)
(* tableau. StablePhases checks the side condition of the arguments.       *)

\* Closed ingress stays closed, and the end of block_on leaves only to its
\* return.
StablePhases ==
  [][/\ ing = "closed" => ing' = "closed"
     /\ bo = "ending" => bo' \in {"ending", "off"}]_vars

\* A task owes a poll when its event happened, a cancel was requested, or
\* it waits for its timer (each deadline passes), and no poll runs:
\* Owed(s) ~> ~Owed(s). While the signal home runs, the signal flag owes a
\* wake to the task that waits for the signal:
\* sf ~> (~sf \/ wk[SH] = "exit"). Each target includes the negation of its
\* source, so each form is equal to its leads-to form.
Owed(s) == ss[s] \in {"idle", "sched"} /\ (ev[s] \/ cx[s] \/ tm[s])
OwedWakeDelivered ==
  /\ \A s \in Slots : []<>(~Owed(s))
  /\ []<>(~sf \/ wk[SH] = "exit")

\* Shutdown joins every worker: ing = "closed" ~> Done. Back: by
\* StablePhases, closed ingress stays, so from a closed state this form
\* gives Done infinitely often.
ShutdownJoins == []<>(ing = "closed" => Done)

\* block_on returns: bo = "ending" ~> bo = "off". Back: by StablePhases,
\* "ending" leaves only to "off".
BlockOnReturns == []<>(bo # "ending")

-----------------------------------------------------------------------------
(* Reachability targets. Each `_reach` profile checks one Unreached       *)
(* property. A pass is a trace that violates it, so the trace reaches the  *)
(* target. A state target is an invariant; an action target is a [][~A]   *)
(* property.                                                               *)

\* A cancel request comes during a poll. The poll continues to its end,
\* which schedules the task for its drop at the next poll boundary.
ReachCancelInPoll ==
  \E s \in Slots : ss[s] = "run" /\ cx[s] /\ ss'[s] = "sched"
UnreachedCancelInPoll == [][~ReachCancelInPoll]_vars

\* A reused slot holds a live task of the second generation, while wakers
\* of the first generation can still run.
ReachReuse == \E s \in Slots : gen[s] = 2 /\ ss[s] \in Live
UnreachedReuse == ~ReachReuse

\* After the shutdown request, a wake from another thread publishes a task
\* to the remote-ready list of a worker.
ReachShutdownRemoteWake ==
  ing = "closed" /\ \E o \in Owners : Len(Q(o)') > Len(Q(o)) /\ cur'[o] = cur[o]
UnreachedShutdownRemoteWake == [][~ReachShutdownRemoteWake]_vars

\* A spawn finds no free slot: each slot is taken while a task can spawn.
ReachRefusal ==
  /\ ing = "open"
  /\ \A s \in Slots : ss[s] # "free" \/ gen[s] > MaxGen
  /\ \E o \in Owners : cur[o] # NoSlot
UnreachedRefusal == ~ReachRefusal

\* A reservation is refused at its admission after ingress closed.
ReachAdmitRefused == \E s \in Slots : ss[s] = "rsv" /\ ss'[s] = "free"
UnreachedAdmitRefused == [][~ReachAdmitRefused]_vars

\* A wake during a poll schedules the task again at the poll end.
ReachCoalesced ==
  \E s \in Slots : ss[s] = "run" /\ nt[s] /\ ~cx[s] /\ ss'[s] = "sched"
UnreachedCoalesced == [][~ReachCoalesced]_vars

\* An idle worker steals a movable task and polls it.
ReachSteal ==
  \E w \in Workers, s \in Slots :
    cur[w] = NoSlot /\ cur'[w] = s /\ ~InQ(w, s) /\ kind[s] = "movable"
UnreachedSteal == [][~ReachSteal]_vars

\* The signal flag waits for the parked signal home, whose descriptor is
\* readable, and a task waits for the signal.
ReachSignalWake ==
  /\ pk = "parked" /\ sf /\ sig
  /\ \E s \in Slots : ss[s] = "idle" /\ reg[s] /\ ~ev[s]
UnreachedSignalWake == ~ReachSignalWake

\* The end of block_on cancels a task that still waits on its owner.
ReachBlockOnCancel ==
  bo = "ending" /\ \E s \in Slots : ss[s] = "idle" /\ home[s] = B /\ ~cx[s] /\ cx'[s]
UnreachedBlockOnCancel == [][~ReachBlockOnCancel]_vars

\* The signal home is parked, its list holds a task, and the signal of that
\* batch waits in the outbox of another worker.
ReachDeferredSignal ==
  pk = "parked" /\ rr # <<>> /\ ~sig /\ \E w \in Workers : ob[w]
UnreachedDeferredSignal == ~ReachDeferredSignal

\* The deadline of its park wakes the signal home, whose descriptor is not
\* readable, while a task waits for its timer.
ReachTimerWake ==
  /\ pk = "parked" /\ ~sig /\ \E s \in Slots : tm[s]
  /\ pk' = "run" /\ wk'[SH] = "run"
UnreachedTimerWake == [][~ReachTimerWake]_vars

=============================================================================
