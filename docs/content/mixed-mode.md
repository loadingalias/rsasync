# Run local and movable work in one runtime

A runtime can have local workers and movable workers at one time (Linux, the default features
`local` and `movable`). A local worker owns its tasks, so they can hold `!Send` state and stay on
one thread. A movable worker polls the tasks of one pool, and an idle movable worker steals them.
Use local workers for state that one thread owns, such as the connections of a thread-per-core
server. Use movable workers for `Send` work whose load is uneven.

## Start both modes

```rust
use rsasync::runtime::{Mode, Runtime};

let runtime = Runtime::builder().local_workers(2).movable_workers(2).build()?;
for (index, worker) in runtime.report().workers().iter().enumerate() {
    println!("worker {index}: {:?}, shares CPUs {:?}", worker.mode(), worker.shared());
}
assert_eq!(runtime.report().workers()[2].mode(), Mode::Movable);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The local workers come first, so `WorkerId`s 0 and 1 are local and 2 and 3 are movable. One plan
places all of them, and a plan with more workers than CPUs refuses unless you allow it: see
[Share CPUs](runtime.md#share-cpus). `shared()` gives the CPUs that a worker shares with another
worker of either mode.

## Hand values between the modes

A local task and a movable task exchange values through the bounded channels of
[`rsasync::channel`](channels.md). Each channel is `Send` for a `Send` value, so either end can be
in a task of either mode. A request can carry the sender of its reply:

```rust
use rsasync::capacity::Budget;
use rsasync::channel::{mpsc, oneshot};
use rsasync::runtime::{Runtime, WorkerId};
use rsasync::task::Outcome;

let runtime = Runtime::builder().local_workers(1).movable_workers(1).build()?;
let mut handle = runtime.handle()?;
let budget = Budget::new(core::num::NonZeroU64::new(1 << 20).unwrap());
let (mut requests, mut inbox) = mpsc::channel::<(u32, oneshot::Sender<u32>)>(&budget, 4, 1)?;

// A movable task serves the requests until the channel closes.
let server = handle.spawn_movable(async move {
    while let Ok((value, reply)) = inbox.recv().await {
        let _sent = reply.send(value * 2);
    }
})?;
// A local task on worker 0 sends a request and waits for its reply.
let client = handle.spawn_on(WorkerId::new(0), move || async move {
    let (reply, mut answer) = oneshot::channel(&budget).ok()?;
    requests.send((21, reply)).await.ok()?;
    answer.recv().await.ok()
})?;
assert!(matches!(client.join_blocking(), Outcome::Completed(Some(42))));
assert!(matches!(server.join_blocking(), Outcome::Completed(())));
# Ok::<(), Box<dyn std::error::Error>>(())
```

The channel bounds the requests that wait: a full channel makes `send` wait, and `try_send` gives
the value back. A resource stays with its mode. A local stream stays on its worker, and a movable
stream moves with its task (see [TCP](tcp.md#use-tcp-from-a-movable-task)).

## Stop both modes

`runtime.shutdown()` cancels each task of each mode at once, and it returns one `Shutdown` report:

- `workers()` has one `WorkerShutdown` for each worker, local and movable, in the order of their
  `WorkerId`s. `cancelled()` counts the local tasks that the worker cancelled, and `debt()` the
  operations that it still homes at its end.
- `movable_cancelled()` counts the movable tasks that the shutdown cancelled.

Each worker exits after its tasks end and each operation that it homes reaches its terminal event,
so a provider that a consumer needs stays driven until the consumer ends.

## A build with one mode

`default-features = false, features = ["local"]` gives the local mode only: no movable pool, no
movable workers, no `spawn_movable`, and no `rsasync::movable`. The movable mode needs `local`
too, because each movable worker, as each `block_on`, is a local owner that runs the pool.

## Run the examples

Two examples run the movable workers on Linux, on io_uring. Each one prints one line for each
outcome, and it ends with a shutdown that has no live debt.

- `cargo run -p movable_only` runs a runtime with two movable workers and no local worker. 64 echo
  tasks and 8 tasks that use the CPU run in the pool at one time, and an accept loop gives each
  connection to a task of its own. The accept loop joins each of these tasks before it ends: a
  shutdown cancels each task that still runs, and a join handle that drops does not wait for its
  task. Then one task shows partial progress, the reuse of a buffer after a success and of a lease
  after a failed send, and a dropped receive. The shutdown cancels a task whose receive waits.
- `cargo run -p mixed_mode` runs one local worker, two movable workers, and a compute service. The
  local task keeps its state in an `Rc`, and it hands typed requests to a movable task, which sums
  on the compute service and replies. It shows the reuse of a value that a full channel gave back,
  a wait that drops before its reply, TCP from a movable stream to a local stream with partial
  progress, and a cancel of a movable task from the local task. The shutdown joins both modes.

The expected output of `movable_only`:

<!-- The same text as `EXPECTED` of `examples/movable_only/src/main.rs`. Edit both. -->
```text
pressure: 64 echo tasks each got their 4 KiB back in order
pressure: 8 tasks that use the CPU each summed 1000000 numbers
tcp: the receive gave 8 of the 11 sent bytes: "hello wo"
tcp: the same buffer then received the rest: "rld"
tcp: after a dropped receive, the next receive gave "late"
tcp: a send to a peer that reset failed, and the same lease then went on a new stream: "again"
tcp: closed each stream and the listener
shutdown: 1 movable task cancelled, 0 operations of debt, grace period expired: false
```

The expected output of `mixed_mode`:

<!-- The same text as `EXPECTED` of `examples/mixed_mode/src/main.rs`. Edit both. -->
```text
plan: worker 0 is local, workers 1 and 2 are movable, and 1 compute thread runs beside them
handoff: the local task sent 3 requests, and the movable task sent back the sums [45, 4950, 499500]
handoff: a full channel gave the request back, and the same request then went through: 10
tcp: the local stream received 8 of the 11 bytes that the movable stream sent: "hello wo"
tcp: the same buffer then received the rest: "rld"
state: one Rc kept the 4 notes of the local task on worker 0
cancel: the local task cancelled the movable task that waited: Cancelled
handoff: the movable task found that the local task dropped its wait, and the reply came back
shutdown: 1 local and 1 movable task cancelled, 0 operations of debt, 0 compute jobs ran at the end, grace period expired: false
```
