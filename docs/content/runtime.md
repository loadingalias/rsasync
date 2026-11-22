# Runtime

`rsasync::runtime` starts local workers and runs `block_on`. A local worker is a thread that owns
its tasks, its driver, and its timers. Its tasks never move to another thread, so they can hold
`!Send` state, such as an `Rc`. rsasync is in development, and the runtime is not in a published
release yet.

The runtime needs the `local` feature, which is on by default. It is on Linux only, on x86_64 and
aarch64. A worker runs its driver on io_uring, or on epoll when the kernel refuses io_uring (see
[Capability report](capability-report.md)). macOS and Windows get local workers later. Until then,
use [`LocalOwner`](tasks.md) or [driver-only use](driver-only.md) there.

## Start a runtime

1. Call `Runtime::builder()`, and change the settings that you need.
2. Call `build()`. It gives a `Runtime`, or a `StartFailure` that tells what failed.
3. Read `runtime.report()` to see what each worker found and applied.
4. Call `runtime.handle()` to get a `Handle`, which spawns on the workers.
5. Drop the runtime to stop it.

```rust
use rsasync::runtime::{Placement, Runtime, Selection, WorkerId};
use rsasync::task::Outcome;

let runtime = Runtime::builder()
    .local_workers(4)
    .placement(Placement::Domain, Selection::Permissive)
    .build()?;
for (index, worker) in runtime.report().workers().iter().enumerate() {
    println!("worker {index}: {:?} on {:?}", worker.placement().applied(), worker.driver().backend());
}

let mut handle = runtime.handle()?;
let task = handle
    .spawn_on(WorkerId::new(1), || async { 2 + 2 })?;
assert!(matches!(task.join_blocking(), Outcome::Completed(4)));
# Ok::<(), Box<dyn std::error::Error>>(())
```

Each setting has a default:

| Setting | Default | Meaning |
| --- | --- | --- |
| `local_workers(n)` | 0 | The local workers, at most `MAX_WORKERS` (4096). With 0, the runtime gives only `block_on` |
| `task_slots(n)` | 1024 | The task slots of each owner: each worker, and each call of `block_on`. From 1 to 2^16 |
| `budget(budget)` | 256 MiB | The [budget](capacity.md) that each owner charges its tables, its timers, and each task to |
| `driver(config)` | `Config::new()` | The limits and the requirements of the driver of each owner |
| `timers(capacity, resolution)` | 1024 timers, 1 ms | The [timers](timers.md) of each owner, on the native clock that pauses while the system is suspended |
| `placement(placement, selection)` | `Domain`, `Permissive` | Where the workers run. See [Placement](#placement) |
| `seed(seed)` or `generator(generator)` | A seed from the operating system | The generator of the runtime. See [Random values](#random-values) |
| `handles(n)` | 64 | The handles that can exist at one time |
| `blocking(config)` | `Some(blocking::Config::new())`: 4 threads, 64 jobs, 2 MiB of stack each | The blocking service of the runtime, or `None` for no service. See [The blocking service](#the-blocking-service) |
| `grace_period(duration)` | 5 s (`DEFAULT_GRACE`) | The grace period of `shutdown()`. See [Stop a runtime](#stop-a-runtime) |

## How a start works

`build()` does these steps, in this order:

1. It validates the settings, and it reads the facts of the host: the CPU set of the process,
   `/sys/devices/system`, and the CPU limit of the cgroup.
2. It resolves the placement of each worker. A strict placement that falls back refuses here.
3. It reads the seed from the kernel with `getrandom(2)`, unless `seed` or `generator` gives one.
4. It starts the blocking service, unless `blocking(None)` turns it off. A failure gives
   `Cause::Blocking` with the `blocking::Error`, and no worker starts.
5. It starts the workers one at a time. Each worker binds its thread, makes its driver, its task
   tables, and its timers on that thread, and then reports that it is ready. The next worker
   starts only after that report.

The runtime is ready only after each worker is ready. A failure at a worker stops the start there:
no later worker starts.

## A failed start

A failure at worker k stops and joins each worker before it, and joins worker k too. Then
`build()` gives a `StartFailure`. No thread of the failed start remains, and nothing of it can be
used again.

- `cause()` gives the failure that ended the start.
- `cleanup()` gives one `Cleanup` for each worker whose thread started, the failed one too. Each
  has its `worker()` and its `outcome()`.

For example, a process whose descriptor limit (`ulimit -n`) leaves worker 2 of 4 too few
descriptors for its driver gets this report. The Linux tests of the runtime check this case:

| Part | Value |
| --- | --- |
| `cause()` | `Cause::Worker { index: 2, error: WorkerError::Driver(_) }`, with the `io::Error` of `EMFILE` as its source |
| `cleanup()` | Worker 0: `Outcome::Joined`. Worker 1: `Outcome::Joined`. Worker 2: `Outcome::Joined` |
| Worker 3 | Did not start |

```rust
use rsasync::runtime::{Cause, Outcome, Runtime};

match Runtime::builder().local_workers(4).build() {
    Ok(runtime) => drop(runtime),
    Err(failure) => {
        if let Cause::Worker { index, error } = failure.cause() {
            eprintln!("worker {index} did not start: {error}");
        }
        for cleanup in failure.cleanup() {
            if !matches!(cleanup.outcome(), Outcome::Joined) {
                eprintln!("worker {}: {:?}", cleanup.worker().index(), cleanup.outcome());
            }
        }
    }
}
```

| `Cause` | When | Recovery |
| --- | --- | --- |
| `Plan(PlanError)` | A setting is invalid, or a strict placement falls back. No worker started | Correct the setting. See [Placement](#placement) |
| `Seed(io::Error)` | The kernel gave no seed. No worker started | Give a seed with `seed`, or start again |
| `Allocation` | The allocator did not give the memory of the start: the default budget, the facts of the host, the plans, or the tables of the workers. No worker started | Start fewer workers, or start again when memory is free |
| `Worker { index, error }` | Worker `index` did not start | See `WorkerError` below |

| `WorkerError` | When | Recovery |
| --- | --- | --- |
| `Thread(io::Error)` | The system did not start the thread | Raise the thread limit of the process, or start fewer workers |
| `Placement(io::Error)` | The system refused a strict placement | Use a CPU of the CPU set of the process, or `Selection::Permissive` |
| `Driver(driver::Error)` | The driver did not start, for example at the descriptor limit | Raise `ulimit -n`, or give smaller driver limits |
| `Owner(task::Refused)` | The task tables of the worker do not fit in the budget | Give a larger budget or fewer task slots |
| `Inbox(channel::Refused)` | The inbox of the worker does not fit in the budget | Give a larger budget, fewer task slots, or fewer handles |
| `Timers(TimersError)` | The timers of the worker did not start | Give a valid timer capacity and resolution |
| `Ended` | The thread of the worker ended with no report: it panicked | Report a defect |

`Outcome::Failed(io::Error)` in a cleanup means that the teardown of the driver of that worker
failed. The kernel can then still own buffers of that driver, and they leak.

## Run the calling thread with block_on

`runtime.block_on(root)` makes the calling thread a local owner for the call:

- The thread polls `root` and the tasks that `root` spawns with `spawn_local`. They can hold
  `!Send` state.
- The owner makes its driver at the first operation that needs one, so a root that only waits,
  for example on a channel, starts no driver.
- When `root` gives its output, `block_on` cancels each task that is still on the owner, and it
  waits until each one ended. Then it cancels each operation that its driver homes, drives the
  driver until each one reaches its terminal event, and closes each resource that remains. Then it
  returns.

So no buffer of an operation of the call is in the kernel after `block_on` returns. Work that must
outlive the root belongs on a worker. Each call gets its own owner, with its own task slots,
timers, driver, and generator. A waker of the call that you keep after the return holds no
descriptor of that driver, and a wake through it does nothing.

The tasks of an owner reach its driver only through types that own their resources, so no task
can replace the driver that the owner waits in. These are the types of `rsasync::local`:

- [TCP from a task](tcp.md#use-tcp-from-a-task) and [UDP from a task](udp.md#use-udp-from-a-task)
- [Files from a task](files.md#use-files-from-a-task)
- [IPC streams from a task](ipc-streams.md#use-ipc-streams-from-a-task)
- [Pipes from a task](pipes.md#use-pipes-from-a-task)
- [Child processes from a task](processes.md#use-child-processes-from-a-task)
- [Signals from a task](signals.md#use-signals-from-a-task)

`block_on` gives a `BlockOnError` when the owner gets no task tables (`Owner`) or no timers
(`Timers`, `Time`), when its driver does not start (`Start`), or when its driver fails (`Driver`).
The first three come before the first poll of `root`, and `into_root` gives `root` back, unpolled,
so you can run it later. A driver failure during the call ends each live task with
`Outcome::OwnerFailed`. A driver that does not start at the first operation fails the owner in the
same way: that operation refuses with `Reason::NoDriver`, and `block_on` gives `Start` with the
`driver::Error` of the start, also when `root` ended. For example, the limit of open files gives
`Error::System` with `EMFILE`. A failure of the final teardown drops the output of `root`.

The error holds the root, so `?` into `Box<dyn Error>` needs a root that is `'static`, such as an
`async move` block that owns what it uses. For a root that borrows, match the error, or map it
with `into_root` first.

## Spawn on a worker

`handle.spawn_on(worker, factory)` sends `factory` to the worker, which calls it there. The future
of the factory can thus hold `!Send` state. The factory itself and the output of the future are
`Send`, because they cross threads. The spawn gives the same `JoinHandle` and the same outcomes as
`spawn_local` (see [Tasks](tasks.md)).

The spawn reserves the charge of the task, allocates the memory of the task and of the factory,
and reserves a free slot of the worker on the calling thread, before it sends the factory. So the
worker spawns the future with no wait and no allocation, and an accepted spawn cannot fail later
for lack of capacity or memory. The charge also covers the factory while it waits in the
inbox. A refusal keeps no reservation, and `SpawnRefused::into_future` gives the factory back:

| `Reason` | Cause | What to do |
| --- | --- | --- |
| `NoWorker` | The runtime has no worker with this index | Use an index below `handle.workers()` |
| `NoSlot` | Each free slot of the worker is reserved | Join or cancel a task of the worker, or spawn on another worker |
| `Budget(Exhausted)` | The budget has fewer free units than the task and its factory | Free units, or spawn a smaller future |
| `Allocation` | The allocator did not give the memory of the task or of the factory | Spawn again when memory is free |
| `Closed` | The worker stopped | Spawn before the drop of the runtime |

A cancel works before the worker calls the factory and after, and the task ends with
`Outcome::Cancelled`. The worker reads the cancel before it calls the factory: if the cancel came
first, the factory drops and the worker never calls it. A later cancel drops the future before its
first poll. A factory that panics before the worker sees a cancel ends the task with
`Outcome::Panicked`. A factory that waits in the inbox when its worker stops also ends with
`Outcome::Cancelled`, and the worker does not call it.

A `Handle` can move to another thread, also to a task of a worker. Each handle holds a place in the
inbox of each worker, so `handles` bounds them: `runtime.handle()` and `handle.try_clone()` refuse
with `HandleRefused::TooMany` at the limit, and with `HandleRefused::Allocation` when the allocator
does not give the memory of the handle.

## Bind one socket on each worker

`handle.tcp_group(address, options, serve)` binds one TCP listener on each local worker, in one
reuse-port group at `address`, and `handle.udp_group` binds one UDP socket on each worker in the
same way. The kernel spreads the connections, or the datagrams by their addresses, between the
members, so each connection stays on one worker. On each worker, the group runs the future that
`serve` makes with the `WorkerId` and the resource. The group:

1. Binds on worker 0 first, so a port 0 gives one port to each member. `Group::local_addr` gives
   that address.
2. Binds the same address on each other worker, one at a time.
3. Starts `serve` on each worker only after each member bound.

```rust,no_run
use rsasync::buffer::Lease;
use rsasync::driver::TcpOptions;
use rsasync::local::TcpListener;
use rsasync::runtime::{Runtime, WorkerId};

async fn serve(worker: WorkerId, mut listener: TcpListener) {
    while let Ok(accept) = listener.accept() {
        let Ok(Ok((mut stream, _peer))) = accept.await else { return };
        if let Ok(send) = stream.send(Lease::from(format!("worker {}\n", worker.index()).into_bytes())) {
            drop(send.await);
        }
    }
}

let runtime = Runtime::builder().local_workers(4).build()?;
let mut handle = runtime.handle()?;
let address = "127.0.0.1:0".parse()?;
// The root owns the handle, so `?` can give its error.
let group = runtime.block_on(async move { handle.tcp_group(address, TcpOptions::default(), serve).await })??;
println!("listening on {}", group.local_addr());
# Ok::<(), Box<dyn std::error::Error>>(())
```

The future gives `GroupError` with its cause when a member does not start. The state of the start
can refuse: no room in the budget for the start signals (`Budget`), no memory for the lists of the
group (`Allocation`), or no channel in the budget (`Channel`). A worker can refuse, and the error
then names it: no slot on the worker (`Spawn`), a refused bind (`Refused`), a failed bind (`Bind`),
or a stopped worker (`Ended`).

When the future gives the error, each member that bound has closed its socket, so the address is
free again. A drop of the future before its end cancels each member, and each one closes its socket
on its worker soon after. `Group::into_tasks` gives the task of each member, in the order of its
worker.

On Linux, another process of the same effective user ID, in the same network namespace, can bind a
socket into the reuse-port group of a member and receive part of its connections or datagrams (see
[socket(7)](https://man7.org/linux/man-pages/man7/socket.7.html)). Run a group under a user that no
untrusted process shares.

## Placement

`Placement` selects where the workers run. The physics of the choice: a worker that runs far from
its memory or its cache pays on each cache miss, for as long as it stays there. A worker that
moves to another core of the same cache domain pays only a short refill of its small caches. A
worker that is pinned to one core cannot leave that core when it is busy.

| `Placement` | What each worker gets | Use it when |
| --- | --- | --- |
| `Domain` (default) | The CPUs that share one last-level cache. The workers spread over the domains in proportion to their physical cores, and the kernel chooses the core in the domain. On a host with one domain, no binding | The process shares the host, or you do not know |
| `Dedicated` | One CPU: one CPU of each physical core first, and the SMT siblings last, spread over the NUMA nodes and then over the cache domains of each node | The process owns the host, or a CPU set, for example on dedicated hardware or with exclusive CPUs |
| `Exact(cpus)` | CPU `cpus[i]` for worker `i` | You plan the CPUs yourself, for example with `isolcpus` |
| `Unpinned` | No binding | The kernel places each thread |

A wrong pin on a shared host gives a tail latency with no bound, and two processes can pin onto the
same CPUs. A missing pin on a dedicated host costs only the cache warmth of one core and the
alignment with the interrupts of a network card. So the default does not pin. On a host that the
process owns, select `Dedicated` one time, in the configuration of that deployment.

`Selection::Permissive` runs each worker whose placement falls back unpinned, and its report gives
the `Fallback`. `Selection::Strict` refuses a plan that falls back with
`PlanError::Refused { worker, fallback }`, before a worker starts. A worker whose binding the
system refuses then fails its start with `WorkerError::Placement`.

| `Fallback` | When |
| --- | --- |
| `OneDomain` | `Domain` on a host with one cache domain: it binds nothing |
| `NoTopology` | The host gave no CPU set, or no cache or node facts |
| `NotAllowed(cpu)` | `Exact` names a CPU that is not in the CPU set of the process |
| `Refused(code)` | The system refused the binding, with this error code |

`Dedicated` and `Exact` refuse with `PlanError::TooFewCpus` when there are fewer CPUs than workers.
`Exact` refuses with `DuplicateCpu` or `NoSuchCpu` for a CPU that it names two times, or that is
not below `MAX_CPUS` (4096).

### Read the placement report

`runtime.report().workers()[i].placement()` gives the placement of worker `i` in three parts:

- `request()`: what the worker asked for.
- `resolved()`: the CPUs that the plan selected, or `None` for no binding.
- `applied()`: the CPUs that the thread runs on, as the system reports them after the binding, or
  `None` if the worker runs unpinned. `fallback()` then says why.

`node()` gives the NUMA node of the CPUs of the binding, if they share one. Each worker makes its
tables and its driver after its binding, so their memory is on that node.

`runtime.report().host()` gives the facts that the plan used: `online()`, `allowed()`,
`isolated()`, `cores()`, `domains()`, `nodes()`, and `quota()`. A fact that the host did not give is
`None`, or `Quota::Unknown`, which is not the same as no limit. More CPUs than cores means SMT.
`host.shows_dedication()` is true when the operator gave the process fewer CPUs than are online and
no cgroup limits its CPU time, as in a Kubernetes pod with exclusive CPUs, or under `taskset`.
`Dedicated` then pins one worker to each CPU of that set. Several processes that share one
restricted set also show this, and only the operator can tell them apart.

## Random values

The runtime has one generator, rscrypto's `ChaCha20Rng`, which `rsasync::runtime::ChaCha20Rng`
names. Each owner derives its own generator from it: worker `i` takes the child `worker\0\0` with
index `i`, and call `n` of `block_on` takes the child `block_on` with index `n`.

`task::generator(label)` gives a new generator that the caller owns: a child of the generator of
the owner of the thread, with `label` and the next index of that owner. So:

- No two calls share a stream, also two calls with one label.
- No task can read, predict, or replace the generator of its owner, or the generator of another
  task.
- With `seed(seed)`, two runtimes give each call the same generator again when the calls of each
  owner come in the same order. Use it to replay a run, and keep a production seed secret.
- With no seed, the runtime reads a seed from the kernel with `getrandom(2)` at each start.

Derive the generators of the connections or other children of a task from its own generator with
`ChaCha20Rng::child`. `generator` refuses with `GeneratorRefused::NoOwner` on a thread with no
owner, and with `NoGenerator` on a `LocalOwner`, which has no generator.

```rust
use rsasync::runtime::{Runtime, WorkerId};
use rsasync::task::{Outcome, generator};

let runtime = Runtime::builder().local_workers(2).seed([7; 32]).build()?;
let mut handle = runtime.handle()?;
let task = handle.spawn_on(WorkerId::new(1), || async {
    generator(b"example\0").map(|mut generator| generator.next_u64())
})?;
if let Outcome::Completed(draw) = task.join_blocking() {
    // The same seed gives the first generator of worker 1 this value in each run.
    println!("{}", draw?);
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

## The blocking service

A runtime owns one bounded [blocking service](blocking-service.md), which each driver of the
runtime uses: each worker, and the driver of each `block_on`. A spawn of a child runs on it, and
so does each file call that the backend has no operation for (`Route::BlockingService`). On epoll,
for example in a container that refuses io_uring, that is each file call. The budget of the runtime
charges the service, and `runtime.report()` does not list it.

- `handle.blocking()` gives the `blocking::Submitter` of the service for your own blocking work,
  such as a name lookup. A full service gives the job back with the reason.
- `blocking(None)` starts no service. Those calls then refuse with `Reason::Blocking`, and
  `handle.blocking()` gives `None`.

## Run the examples

Three examples use the runtime with the types of `rsasync::local`, on Linux. Each one runs one task
on worker 0, prints one line for each outcome, and ends with a shutdown that has no live debt.

- `cargo run -p local_only` keeps the state of its task in an `Rc` across each wait, and it does
  file, TCP, UDP, and IPC stream I/O. It shows partial progress, the reuse of a buffer after a
  success and after a failure, a dropped receive, and the close of each resource.
- `cargo run -p standard_futures` combines its I/O with the standard library only:
  `core::future::join!`, `poll_fn`, and a small select. Its join borrows data on the stack of the
  task, and its select abandons a receive. It shows partial progress, and the reuse of a lease
  after a failure.
- `cargo run -p process_supervision` supervises two children with piped standard streams. It
  needs `sh`, `tee`, `sleep`, and `kill` on the host. The steps are:
  1. Register a stream of `SIGINT` with `Escalation::At(2)`, so a second interrupt ends a stuck
     process.
  2. Spawn each child with `ChildOptions::new().tree(true).end(End::Kill)`. The child leads its own
     process group, so a Ctrl-C at the terminal reaches only the supervisor.
  3. Write 256 KiB to each child while both of its outputs drain, in one join. That is 4 times the
     capacity of a Linux pipe, so a write before the reads would wait forever.
  4. Send `SIGINT` to the example itself with `kill -s INT`, as Ctrl-C does.
  5. On the interrupt, send `SIGTERM` to the tree of each child. Kill the tree of a child that
     still runs after 1 s. One child obeys `SIGTERM`, and one child ignores it.

The expected output of `local_only`:

<!-- The same text as `EXPECTED` of `examples/local_only/src/main.rs`. Edit both. -->
```text
file: wrote 11 bytes and made them durable
file: an exact read of 16 bytes stopped at the end of the file after 11 bytes: "hello world"
file: the same buffer then read 5 bytes: "world"
file: closed and removed the file
tcp: the receive gave 8 of the 11 sent bytes: "hello wo"
tcp: the same buffer then received the rest: "rld"
tcp: after a dropped receive, the next receive gave "late"
tcp: closed both streams and the listener
udp: a receive into 4 bytes gave "ping", truncated: true
udp: the same buffer then received "pong", truncated: false
udp: closed both sockets
ipc: the server received "hello" from a peer of this user: true
ipc: closed both streams and the listener
state: one Rc kept the 13 notes of the task on worker 0
shutdown: 0 tasks cancelled, 0 operations of debt, grace period expired: false
```

The expected output of `standard_futures`:

<!-- The same text as `EXPECTED` of `examples/standard_futures/src/main.rs`. Edit both. -->
```text
join: a send and a receive ran together, and the receive gave "hello", as the stack data of the task says
partial: a receive into 4 bytes gave "hell"
partial: the same buffer then received the rest: "o"
select: the timer came first, and the receive with no data dropped
select: the next receive on the same stream gave "after"
failure: a send after the half-close gave BrokenPipe and its lease back
failure: the same lease then sent "again" on a new stream
shutdown: 0 tasks cancelled, 0 operations of debt, grace period expired: false
```

The expected output of `process_supervision`:

<!-- The same text as `EXPECTED` of `examples/process_supervision/src/main.rs`. Edit both. -->
```text
interrupt: the stream of SIGINT is ready, and a second SIGINT ends the process
io: the child that obeys SIGTERM took 262144 bytes while its stdout and stderr drained
io: its stdout gave 262144 bytes and its stderr gave 262144 bytes, each the same as the input: true
io: the child that ignores SIGTERM took 262144 bytes while its stdout and stderr drained
io: its stdout gave 262144 bytes and its stderr gave 262144 bytes, each the same as the input: true
interrupt: kill -s INT sent SIGINT to this process, and the stream reported it
stop: the child that obeys SIGTERM ended by signal 15 before the deadline of 1s
stop: the child that ignores SIGTERM ran on to the deadline of 1s, and the kill ended it by signal 9
shutdown: 0 tasks cancelled, 0 operations of debt, 0 children run on, grace period expired: false
```

In its shutdown line, "children run on" is `worker.kept()`.

## Stop a runtime

Call `runtime.shutdown()`. It stops the runtime in one bounded lifecycle, and it gives a
`Shutdown` report:

1. Each spawn refuses from now on, with `Reason::Closed`, and gives its future back.
2. Each worker closes its inbox. Each factory that waits there ends with `Outcome::Cancelled`.
3. Each task is cancelled at once. Its future drops at its next poll, and the drop closes its
   resources, which cancels their operations.
4. Until the end of the grace period, each operation can end by itself. Then each worker closes
   each resource that remains, cancels each operation that still runs, and kills each child with
   `End::Kill`. The grace period is one deadline from the request, for each worker.
5. Each worker waits until each operation reaches its terminal event, with no deadline. No deadline
   frees a buffer that the kernel can still use, so a shutdown can outlast its grace period.
6. The shutdown joins each worker, and then it tears the blocking service down.

A graceful drain of work in flight is your work: end your tasks, for example with a message on a
channel, before you call `shutdown()`. The 5 s default fits inside the 10 s stop timeout of
`docker stop` and the 30 s default grace of Kubernetes.

| Report | Meaning |
| --- | --- |
| `report.expired()` | The grace period ended while an operation of a worker still ran |
| `report.blocking()` | What the teardown of the blocking service did, or `None` with no service |
| `worker.outcome()` | How the thread of the worker ended: `Joined`, `Failed`, or `Panicked` |
| `worker.cancelled()` | The tasks that the shutdown cancelled |
| `worker.debt()` | The operations that still ran at the end of the grace period. Each one ended before the shutdown returned |
| `worker.sent()` | The bytes that dropped sends had sent when the shutdown closed their resources |
| `worker.kept()` | The children with `End::Keep` that run on. Nothing reaps them now |

`report.workers()` gives one entry for each worker, in the order of their indexes.

A drop of the runtime is a backstop, not a bounded or reported stop. A drop stops each worker with
no grace period. The worker closes its inbox, ends each factory that waits there with
`Outcome::Cancelled`, cancels each of its tasks, waits until each one ended, tears its driver down,
and ends its thread. The drop then joins each thread. Then it tears the blocking
service down: a job of its queue drops with no run, and the drop waits for each running job. So a
task that never returns from its poll, or a job that never returns, stops the drop. The drop gives
no report.
