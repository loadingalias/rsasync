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

## How a start works

`build()` does these steps, in this order:

1. It validates the settings, and it reads the facts of the host: the CPU set of the process,
   `/sys/devices/system`, and the CPU limit of the cgroup.
2. It resolves the placement of each worker. A strict placement that falls back refuses here.
3. It reads the seed from the kernel with `getrandom(2)`, unless `seed` or `generator` gives one.
4. It starts the workers one at a time. Each worker binds its thread, makes its driver, its task
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
| `Allocation` | The allocator did not give the tables of the start. No worker started | Start fewer workers, or start again when memory is free |
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
- The owner makes its driver at the first operation that needs one: the first call of
  `task::with_driver`. A root that only waits, for example on a channel, starts no driver.
- When `root` gives its output, `block_on` cancels each task that is still on the owner, and it
  waits until each one ended. Then it cancels each operation that its driver homes, drives the
  driver until each one reaches its terminal event, and closes each resource that remains. Then it
  returns.

So no buffer of an operation of the call is in the kernel after `block_on` returns. Work that must
outlive the root belongs on a worker. Each call gets its own owner, with its own task slots,
timers, driver, and generator.

```rust
use rsasync::driver::OpenOptions;
use rsasync::runtime::Runtime;
use rsasync::task::with_driver;

let runtime = Runtime::builder().build()?;
runtime.block_on(async {
    // The first operation makes the driver of this owner.
    let file = with_driver(|driver| driver.open_file("data.bin".as_ref(), OpenOptions::new().read(true)))??
        .await??;
    with_driver(|driver| driver.close_file(file))??.await?;
    Ok::<_, Box<dyn std::error::Error>>(())
})??;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`task::with_driver(f)` calls `f` with the driver of the owner of the thread. The futures of the
operations do not borrow the driver, so the task awaits them after `f` returns. The
[driver](driver-only.md) pages give each operation. `with_driver` refuses with `DriverRefused`:

| `DriverRefused` | When |
| --- | --- |
| `NoOwner` | No owner runs on this thread |
| `NoDriver` | The owner is a `LocalOwner`, which has no driver |
| `InUse` | The driver is in use on this thread: inside the closure of `with_driver`, or in a waker that a turn of the driver calls |
| `Start(driver::Error)` | The driver did not start. A later call tries again |

`block_on` gives a `BlockOnError` when the owner gets no task tables (`Owner`) or no timers
(`Timers`, `Time`), or when its driver fails (`Driver`). A driver failure during the call ends each
live task with `Outcome::OwnerFailed`. A failure of the final teardown drops the output of `root`.

## Spawn on a worker

`handle.spawn_on(worker, factory)` sends `factory` to the worker, which calls it there. The future
of the factory can thus hold `!Send` state. The factory itself and the output of the future are
`Send`, because they cross threads. The spawn gives the same `JoinHandle` and the same outcomes as
`spawn_local` (see [Tasks](tasks.md)).

The spawn reserves the charge of the task and a free slot of the worker on the calling thread,
before it sends the factory. So the worker spawns the future with no wait, and an accepted spawn
cannot fail later for lack of capacity. The charge also covers the factory while it waits in the
inbox. A refusal keeps no reservation, and `SpawnRefused::into_future` gives the factory back:

| `Reason` | Cause | What to do |
| --- | --- | --- |
| `NoWorker` | The runtime has no worker with this index | Use an index below `handle.workers()` |
| `NoSlot` | Each free slot of the worker is reserved | Join or cancel a task of the worker, or spawn on another worker |
| `Budget(Exhausted)` | The budget has fewer free units than the task and its factory | Free units, or spawn a smaller future |
| `Closed` | The worker stopped | Spawn before the drop of the runtime |

A panic of the factory ends the task with `Outcome::Panicked`. A cancel works before the worker
calls the factory and after: the task ends with `Outcome::Cancelled`. A factory that waits in the
inbox when its worker stops also ends with `Outcome::Cancelled`, and the worker does not call it.

A `Handle` can move to another thread, also to a task of a worker. Each handle holds a place in the
inbox of each worker, so `handles` bounds them: `runtime.handle()` and `handle.try_clone()` refuse
with `TooManyHandles` at the limit.

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
index `i`, and call `n` of `block_on` takes the child `block_on` with index `n`. So two owners
never share a stream.

- With `seed(seed)`, two runtimes give each owner the same values. Use it to replay a run, and keep
  a production seed secret.
- With no seed, the runtime reads a seed from the kernel with `getrandom(2)` at each start.

`task::with_generator(f)` calls `f` with the generator of the owner of the thread. Derive the
generator of a connection or of another child with `ChaCha20Rng::child`. `with_generator` refuses
with `GeneratorRefused::NoOwner`, `NoGenerator` (a `LocalOwner` has none), or `InUse` (inside its
own closure).

```rust
use rsasync::runtime::{Runtime, WorkerId};
use rsasync::task::{Outcome, with_generator};

let runtime = Runtime::builder().local_workers(2).seed([7; 32]).build()?;
let mut handle = runtime.handle()?;
let task = handle
    .spawn_on(WorkerId::new(1), || async { with_generator(|generator| generator.next_u64()) })?;
if let Outcome::Completed(draw) = task.join_blocking() {
    // The same seed gives worker 1 this value again in each run.
    println!("{}", draw?);
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Stop a runtime

A drop of the runtime stops each worker. The worker closes its inbox, ends each factory that waits
there with `Outcome::Cancelled`, cancels each of its tasks, waits until each one ended, tears its
driver down, and ends its thread. The drop then joins each thread. A drop waits for each task, so a
task that never returns from its poll stops the drop. The drop gives no report.
