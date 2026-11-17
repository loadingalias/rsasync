# Run blocking work on the blocking service

Some host calls block their thread, for example a name lookup through the system resolver or a
host trust check that reads the system trust store. Run such a call on the blocking service, so
that no task or driver thread waits for it. The service has a fixed number of threads and a fixed
bound of jobs. Its submission is public, so the application and each library can share one
service. The type is `rsasync::blocking::Service`. rsasync is in development, and its alpha
releases are on crates.io.

The service needs the `std` feature (on by default) and a target where `std` starts threads. It
needs no scheduler and no driver. The driver uses it too, for a file call that the kernel has no
ring operation for, and for each file call on [epoll](epoll.md) and [kqueue](kqueue.md): see
[Length and space](files.md#length-and-space).

## Start a service and submit a job

`Service::new` starts the threads. `Service::submitter` gives a `Submitter`, which each library
can clone and keep. `Submitter::submit` never waits: it admits the job and gives its `Waiter`, or
it refuses and gives the job back. `Service::teardown` joins each thread.

<!-- The same text as the doctest of `rsasync::blocking`. Edit both. -->
```rust
use core::num::NonZeroU64;
use std::net::ToSocketAddrs as _;

use rsasync::blocking::{Config, Service};
use rsasync::capacity::Budget;

let budget = Budget::new(NonZeroU64::new(4 << 20).unwrap());
// 2 threads of 256 KiB of stack each, and at most 8 jobs at one time.
let service = Service::new(&budget, Config::new().threads(2).jobs(8).stack(256 << 10))?;
let submitter = service.submitter();

// A name lookup blocks in the system resolver, so it runs on the service.
let mut lookup = submitter.submit(|| "localhost:443".to_socket_addrs().map(Iterator::count))?;
let addresses = lookup.wait_blocking()??;
assert!(addresses > 0);

// The teardown joins each thread.
let report = service.teardown();
assert_eq!((report.running(), report.dropped()), (0, 0));
```

`Config` holds the limits:

| Setting | Default | Limits | Meaning |
| --- | --- | --- | --- |
| `Config::threads` | 4 | 1 to 65 535 | The threads. The service starts them all in `new` and keeps them until it ends |
| `Config::jobs` | 64 | 1 to 2^27 | The jobs that the service admits at one time, in its queue and on its threads together |
| `Config::stack` | 2 MiB | Not 0 | The bytes of stack of each thread |

`Service::new` gives one of these errors. An error keeps no charge, and no thread of it remains:

| Error | Cause |
| --- | --- |
| `Error::Config` | A limit is 0 or too large |
| `Error::Budget` | The budget has fewer free units than the service, its queue, and its stacks |
| `Error::Allocation` | The allocator did not give the memory of the queue |
| `Error::Thread` | The system did not start a thread. The service joins the threads that started before it returns |

## Submit a job from a library

Each closure `FnOnce() -> R` that is `Send + 'static` is a job. A library can also make its own job
type with the `Job` trait. A refusal then gives back the input of the job, for example the host
of a lookup or a command that the library must keep:

<!-- The same text as the doctest of `rsasync::blocking::Job`. Edit both. -->
```rust
use std::io;
use std::net::{SocketAddr, ToSocketAddrs as _};

use rsasync::blocking::{Job, Refused, Submitter};

/// A name lookup of a networking library. A refusal gives its host back.
struct Resolve {
    host: String,
    port: u16,
}

impl Job for Resolve {
    type Output = io::Result<Vec<SocketAddr>>;

    fn run(self) -> Self::Output {
        Ok((self.host.as_str(), self.port).to_socket_addrs()?.collect())
    }
}

/// Resolves `host` on the service of `submitter`, and waits on this thread for the addresses.
fn resolve(
    submitter: &Submitter,
    host: String,
    port: u16,
) -> Result<Vec<SocketAddr>, Box<dyn core::error::Error>> {
    match submitter.submit(Resolve { host, port }) {
        Ok(mut lookup) => Ok(lookup.wait_blocking()??),
        // The refusal gives the lookup back, so the error can name its host.
        Err(Refused { reason, job }) => Err(format!("{}: {reason}", job.host).into()),
    }
}
```

A submission refuses with `Refused`, which holds the job and its `Reason`. A refusal keeps no
charge:

| Reason | Cause | What to do |
| --- | --- | --- |
| `Reason::Full` | Each job slot is occupied | Try again after a job returns, or report the overload |
| `Reason::Budget` | The budget has fewer free units than the job and its result | Free memory in the budget, or report the overload |
| `Reason::Ended` | The service stopped | Do not submit again. No later submission can succeed |

## Wait for the result

A `Waiter` gives the result of its job in two forms. `Waiter::wait` gives a future, so a scheduler,
another executor, or your own loop can poll it. A drop of that future keeps the result for a later
wait. `Waiter::wait_blocking` waits on the current thread. Both give `Result<R, Failed>`:

| Result | Cause |
| --- | --- |
| `Ok(output)` | The job returned `output` |
| `Err(Failed::Ended)` | The service stopped before the job ran, so the job dropped. A wait after the waiter took the result also gives it |
| `Err(Failed::Panicked)` | The job panicked. The panic hook reported the panic, and the thread dropped its payload and runs the next job. With `panic = "abort"`, a panic ends the process instead |

## Drop a waiter

No job is cancelled, because a running closure cannot be made to stop. A drop of the waiter ends
only your interest in the result. The job runs until it returns, and it holds its job slot until
then. Its result then drops on the thread of the service, still under the charge of the job. A
panic in that drop does not end the thread. Thus a job that never returns holds its slot for the
life of the service, and each such job leaves one job slot less for other jobs.

## Stop the service

`Service::teardown` stops the admission of jobs and joins each thread:

- Each later submission gives `Reason::Ended`.
- A thread drops each job of the queue that it takes after the stop, and the waiter of that job
  gives `Failed::Ended`. A submission that races the stop can admit its job and then drop it in the
  same way.
- A job that runs when the teardown starts runs until it returns, and the teardown waits for it.
  So a job that never returns stops the teardown.

The `Teardown` report gives two counts:

| Count | Meaning |
| --- | --- |
| `Teardown::running()` | The jobs that ran when the teardown started. The teardown waited until each one returned |
| `Teardown::dropped()` | The jobs of the queue that did not run |

Only the teardown joins the threads. A drop of the service also stops it and drops the jobs of the
queue, but it does not wait: each thread ends after its running job returns.

## Threads and memory

The service starts all of its threads in `Service::new`, and it starts no other thread later. It
charges its memory to the `Budget` of `new` before it allocates (see [Capacity](capacity.md)):

| Charge | Units | Ends |
| --- | --- | --- |
| The service and its queue | Their size in memory | When the service, each submitter, and each thread are gone |
| The stack of each thread | `Config::stack` | When that thread ends, also after a drop of the service with no teardown |
| Each job | The size of the job in memory | When the job returns |
| The result of each job | The size of the result in memory | When the waiter and the job are both done with it |

The budget does not count what a job allocates, the stack that it uses, or a rounding of the stack
size by the platform. Keep headroom in the budget for them, or charge them in your own code.

`Service::threads`, `Service::jobs`, and `Service::stack` give the limits of a running service, so
that a host can put its threads and its stack memory in its plan.
