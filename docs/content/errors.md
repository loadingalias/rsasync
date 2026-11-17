# Errors

Each fallible operation of rsasync gives a small typed error. This page names each public error and
its recovery. rsasync is in development, and these errors are not in a published release yet.

## Rules for each error

- Match the type and its variant. The `Display` text and the Rust type path are not stable
  identities, so do not match them.
- A refusal changes nothing, and it keeps no charge. A refusal of a value gives the value back:
  `SpawnRefused::into_future`, the `job` field of `blocking::Refused`, `driver::Refused::into_inner`,
  and the value in `TrySendError` and `SendError`.
- `Error::source` gives the cause of an error that has one: `capacity::Exhausted` for a budget
  refusal, and the `io::Error` for a failure of the system.
- A refusal of `rsasync::task` and its text need no allocation, so a report completes when the
  allocator refuses each request.
- The `Debug` form of each error of `rsasync::task`, `rsasync::channel`, and `rsasync::blocking`,
  and of `driver::Refused`, shows no output, value, job, or panic message.

## Tasks

The errors of `rsasync::task` (see [Tasks](tasks.md)).

| Error | When | Recovery |
| --- | --- | --- |
| `Refused::Zero` | `LocalOwner::new` got 0 slots | Give at least 1 slot |
| `Refused::TooLarge` | More than 2^16 slots | Give fewer slots |
| `Refused::Budget(Exhausted)` | The budget cannot charge the tables of the owner | Give a larger budget or fewer slots |
| `Refused::Allocation` | The allocator did not give the memory of the owner | Give fewer slots, or make the owner when memory is free |
| `SpawnRefused` with `Reason::NoSlot` | Each slot of the owner holds a live task | Join or cancel a task, or use `spawn_local_waiting` |
| `SpawnRefused` with `Reason::Budget(Exhausted)` | The budget has fewer free units than the task | Free units, or spawn a smaller future |
| `SpawnRefused` with `Reason::Closed` | `block_on` is at its end and cancels its tasks, or it failed and drops them | Do the work in the root future, or before the end |
| `SpawnRefused` with `Reason::NoOwner` | No `block_on` runs on this thread | Spawn from the root future or from a task |
| `Outcome::Panicked(Panic)` | A poll of the future, or its drop, panicked | `Panic::into_payload` gives the payload. The other tasks continue |
| `Outcome::OwnerFailed(OwnerFailure)` | `block_on` ended with a panic before the task ended | Spawn the work again. The owner can run `block_on` again |

## Channels

The errors of `rsasync::channel` (see [Channels](channels.md)).

| Error | When | Recovery |
| --- | --- | --- |
| `Refused::Zero` | The capacity is 0 | Give a capacity of at least 1 |
| `Refused::TooLarge` | The capacity is too large for the topology or for one allocation | Give a smaller capacity |
| `Refused::Budget(Exhausted)` | The budget cannot charge the slots | Give a larger budget or a smaller capacity |
| `Refused::Allocation` | The allocator did not give the memory | Give a smaller capacity, or make the channel when memory is free |
| `TrySendError::Full(value)` | No slot is free | Send the value later, or use the waiting send |
| `TrySendError::Closed(value)`, `SendError(value)` | The channel is closed | Terminal: keep or drop the value, and stop the sends |
| `TryRecvError::Empty` | No value is ready | Receive later, or use the waiting receive |
| `TryRecvError::Closed`, `Closed` | The channel is closed, and no value is left | Terminal: stop the receives |
| `Aborted` | The receiver aborted the channel before the publication of a reservation | Terminal: the channel dropped the values of the reservation |
| `ReserveError::Full` | Fewer slots are free than the reservation asks for | Reserve later |
| `ReserveError::Closed` | The channel is closed | Terminal: stop the reservations |
| `ReserveError::TooLarge` | The reservation asks for more slots than the capacity | Reserve at most the capacity |
| `TooManySenders`, `TooManyReceivers` | Each place for a sender or a receiver is in use | Clone after a sender or a receiver drops |

## Blocking service

The errors of `rsasync::blocking` (see [Blocking service](blocking-service.md)).

| Error | When | Recovery |
| --- | --- | --- |
| `Error::Config` | A limit of `Config` is 0 or too large | Give each limit a value in its range |
| `Error::Budget(Exhausted)` | The budget cannot charge the service, its queue, and its stacks | Give a larger budget, or fewer threads, jobs, or stack bytes |
| `Error::Allocation` | The allocator did not give the memory of the queue | Give fewer jobs, or start the service when memory is free |
| `Error::Thread(io::Error)` | The system did not start a thread | Read the `io::Error`, and give fewer threads or a smaller stack |
| `Refused` with `Reason::Full` | Each job slot is in use | Take the `job` back, and submit it after a job returns |
| `Refused` with `Reason::Budget(Exhausted)` | The budget cannot charge the job and its result | Submit it after a charge returns |
| `Refused` with `Reason::Ended` | The service stopped | Terminal: run the job in another place |
| `Failed::Ended` | The service stopped before the job ran, and the job dropped | Run the work again in another place |
| `Failed::Panicked` | The job panicked. The panic hook reported it | Correct the job. The service continues with the next job |

## Timers and clocks

The errors of `rsasync::timer`, `rsasync::clock`, and `rsasync::time` (see [Timers](timers.md),
[Clocks](clocks.md), and [Physical time](physical-time.md)).

| Error | When | Recovery |
| --- | --- | --- |
| `timer::Invalid::Resolution` | The resolution is 0, or longer than `u64::MAX` nanoseconds | Give a resolution in that range |
| `timer::Invalid::Capacity` | The memory of the capacity is larger than the address space | Give a smaller capacity |
| `timer::Invalid::Exhausted(Exhausted)` | The budget has less free capacity than `Timers::memory` | Give a larger budget or a smaller capacity |
| `timer::Refused::Full` | Each place of the storage holds a timer | Insert after a timer expires or is cancelled |
| `timer::Refused::Range` | The deadline is after `Timers::latest` | Give an earlier deadline, or a storage with a longer range |
| `timer::Refused::Closed` | The storage is closed | Terminal: stop the inserts |
| `timer::Refused::Domain` | The deadline is on another clock domain than the storage | Give a deadline on the domain of the storage |
| `timer::Closed` | The storage closed before the timer expired | Terminal: the timer does not fire |
| `clock::Unsupported` | The target has no native clock for the domain | Use a domain that the target has, or inject a clock |
| `time::TimeError::DomainMismatch` | The values are on different clock domains | Compare values of one domain |
| `time::TimeError::Overflow` | The result is out of the range of its type | Give values with a result in range |
| `time::TimeError::UnknownBound` | The operation needs an uncertainty bound, and the bound is unknown | Use a source that gives a bound |

## Driver

The errors of `rsasync::driver`, on Linux (see [Driver-only use](driver-only.md)). An operation
that the kernel ran and that failed gives the `io::Error` of the kernel.

| Error | When | Recovery |
| --- | --- | --- |
| `Error::Refused(io::Error)` | The kernel or its policy refuses io_uring (`EPERM` or `ENOSYS`), and `Config::backend` requires `Backend::IoUring`. On macOS, `Config::backend` requires a backend other than `Backend::Kqueue`, and on Windows one other than `Backend::Iocp`. The error then has the kind `Unsupported` | Allow io_uring for the process, for example in the seccomp profile of the container or in `kernel.io_uring_disabled`, or remove the requirement: the driver then runs [epoll](epoll.md). On macOS, require `Backend::Kqueue` or no backend, and on Windows `Backend::Iocp` or no backend |
| `Error::Config` | A limit of `Config` is 0 or above what the kernel takes | Give each limit a value in its range |
| `Error::System(io::Error)` | Another failure of the system | Read the `io::Error` |
| `Error::Unsupported(Features)` | The kernel has no feature of these, which `Config::require` required | Run on a kernel with each feature, or remove the requirement and use the route that `Driver::report` gives |
| `Refused` with `Reason::Operations` | Each operation slot is in use | Turn the driver until an operation ends, then submit again |
| `Refused` with `Reason::Queue` | The submission queue is full | Turn the driver, then submit again |
| `Refused` with `Reason::Resources` | Each resource slot is in use | Close a resource, then try again |
| `Refused` with `Reason::Resource` | The descriptor is not one that this driver owns | Do not use the descriptor: it closed, or another driver owns it |
| `Refused` with `Reason::Buffers` | The driver has no provided buffers | Call `Driver::provide` |
| `Refused` with `Reason::NotPipe`, `Reason::Direction` | The descriptor is not a pipe, or its direction is wrong | Give a pipe end with the direction of the adoption |
| `Refused` with `Reason::Unsupported` | The operation does not apply to the resource | Use an operation of that resource |
| `Refused` with `Reason::Alignment` | A direct-I/O buffer, length, or offset is not aligned | Align each one to the `Alignment` of the file |
| `Refused` with `Reason::Register` | epoll did not take the descriptor, for example at the limit of `fs.epoll.max_user_watches`. On macOS, [kqueue](kqueue.md) refused the registration. On Windows, the association of a socket with the [port](iocp.md) failed | The refusal gives the descriptor back, open. Close other resources, or raise the limit |
| `Refused` with `Reason::Blocking` | The operation needs the blocking service, and it has none or it refused | Give a service with `Driver::use_blocking`. `Driver::report` gives the route of each call |
| `Ended` | The driver ended before the operation gave its result | Keep the driver driven until each future takes its result |
| `SignalError::Fault`, `Uncatchable`, `Invalid` | No stream can take the signal | Use another signal |
| `SignalError::Full` | Each of the 64 streams of the process is in use | Close a stream, then try again |
| `SignalError::Escalation` | An `Escalation::At` on a signal whose default action does not end the process | Register the signal with `Escalation::Never` |
| `Incomplete` | An exact read ended before the end of its buffer | Continue at the offset of the read plus `Incomplete::read` |
| `WriteFailed` | A send whose future dropped failed, so the framing of the stream is unknown | Close the stream. `WriteFailed::of` finds it in the `io::Error` of a send |

## Capacity

The errors of `rsasync::capacity` and `rsasync::acquire` (see [Capacity](capacity.md)).

| Error | When | Recovery |
| --- | --- | --- |
| `Exhausted` | The budget has fewer free units than the request | Request again after a charge returns, use the waiting reservation, or give a larger budget |
| `NotCharged` | A refund is larger than the units that the credit charged | Correct the refund: it is a defect of the caller |
| `acquire::Refused::TooManyPlaces` | `acquire::budget` got more than `acquire::MAX_PLACES` waiter places | Give fewer places |
| `acquire::Refused::Memory(Exhausted)` | The memory budget cannot charge the waiter places | Give a larger memory budget or fewer places |
| `TooManyWaiters` | Each waiter place of the budget is in use | Clone after an acquirer drops |
| `AboveBound` | A wait asks for more units than the bound of the budget | Ask for at most the bound: no release can make such a wait end |
