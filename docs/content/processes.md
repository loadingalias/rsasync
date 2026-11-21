# Child processes on the driver

This page is the reference for child processes on the driver, `rsasync::driver::Driver`, on
io_uring and epoll on Linux and on kqueue on macOS: how to start a child or give one to the driver,
the wait for its exit, signals, a process tree, and the end policy.
[Drive the I/O driver yourself](driver-only.md) tells how to start, turn, and end the driver.

On Linux, the driver holds a process descriptor (pidfd) for each child. macOS has no process
descriptor, so there the driver holds the process ID, which names the child only while the driver
holds it unreaped. The driver reaps each child exactly once and reports the status once. It installs no `SIGCHLD` handler, and it never waits for a child that it
does not own, so it shares a process with other libraries that start children. No thread waits for
a child.

## Spawn a child

`Driver::spawn(command, options)` takes a `std::process::Command`. The spawn runs as one job of the
[blocking service](blocking-service.md) that `Driver::use_blocking` gives, because process creation
blocks and its cost grows with the memory of the parent. The future gives the `Child`, or the error
of the spawn, for example `NotFound` for a program that does not exist.

`ChildOptions::new()` has no process tree and `End::Keep`. `.tree(true)` makes the child lead a
process tree, and `.end(End::Kill)` sets the end policy.

A spawn refuses, gives the command back in its `Refused`, and changes nothing:

| `Refused::reason()` | Cause |
| --- | --- |
| `Reason::Children` | Each child slot of the driver is in use (`Config::children`) |
| `Reason::Blocking` | The driver has no blocking service, or the service refused the job, for example because each job slot is in use |
| `Reason::Resources` | Fewer than three resource slots are free for the pipe ends of the child |
| `Reason::Unsupported` | On Linux, the kernel gives no process descriptor, for example under a seccomp filter |
| `Reason::Operations`, `Reason::Queue` | The driver has no room for the operations |

A drop of the future of a spawn ends only the interest: the child starts, and its end policy
applies as at a drop of the `Child`.

This example runs `echo`, receives its output, and waits for its exit, with `run` as in
`Operation`:

<!-- The same text as the doctest of `rsasync::driver::Child`. Edit both. -->
```rust
use std::process::{Command, Stdio};

use rsasync::blocking::{self, Service};
use rsasync::buffer::{Backing, Buffer as _};
use rsasync::capacity::Budget;
use rsasync::driver::{ChildOptions, Config, Driver};

let budget = Budget::new(core::num::NonZeroU64::new(4 << 20).unwrap());
let service = Service::new(&budget, blocking::Config::new().threads(1).stack(256 << 10))?;
let mut driver = Driver::new(Config::new())?;
driver.use_blocking(service.submitter());

let mut command = Command::new("echo");
command.arg("hello").stdout(Stdio::piped());
let spawn = driver.spawn(command, ChildOptions::new())?;
let mut child = run(&mut driver, spawn)???;

let mut stdout = child.stdout.take().unwrap();
let receive = driver.recv(&mut stdout, Backing::from(Vec::with_capacity(64)))?;
let (received, buffer) = run(&mut driver, receive)??;
assert_eq!((received?, buffer.bytes()), (6, &b"hello\n"[..]));
let status = run(&mut driver, child.wait())??;
assert!(status?.success());

let close = driver.close_reader(stdout)?;
run(&mut driver, close)??;
driver.teardown()?;
service.teardown();
```

## Adopt a child

A caller that must start the child itself, for example suspended or with its own launcher, gives
the `std::process::Child` to `Driver::adopt_child(child, end)`. The driver takes its piped standard
streams as pipe ends, and it clears `O_NONBLOCK` of each one, as [Pipes on the driver](pipes.md)
does. An adopted child leads no process tree for the driver.

An adoption refuses, gives the `std::process::Child` back, and changes nothing:

| `Refused::reason()` | Cause |
| --- | --- |
| `Reason::Children` | Each child slot of the driver is in use |
| `Reason::Exited` | The child ended. The `std::process::Child` has its status: call its `wait` |
| `Reason::Resources` | The driver has no resource slot for a pipe end of the child |
| `Reason::Unsupported` | On Linux, the kernel gave no process descriptor |
| `Reason::Operations`, `Reason::Queue` | The driver has no room for the operation |

Give the driver only a child that nothing reaped. The standard library keeps the status that its
own `wait` or `try_wait` took, and the driver refuses such a child with `Reason::Exited`.

## Wait for the exit

`Child::wait()` gives a future of the exit status, a `std::process::ExitStatus`. The exit is an
operation of the driver from the spawn or the adoption on, and it ends when the driver reaps the
child:

| Backend | `report().route(Call::WaitChild)` | The exit operation |
| --- | --- | --- |
| io_uring on Linux 6.7 and later | `Route::Ring` | `IORING_OP_WAITID` on the process descriptor, which reaps the child and writes the status |
| io_uring on Linux 6.1 to 6.6 | `Route::Poll` | A poll of the process descriptor on the ring. When the descriptor is readable, the driver reaps the child with one nonblocking `waitid` |
| [epoll](epoll.md) | `Route::Poll` | epoll waits for the process descriptor to become readable, then the driver reaps the child with one nonblocking `waitid` |
| [kqueue](kqueue.md), on macOS | `Route::Poll` | kqueue waits for the exit event of the process (`EVFILT_PROC` with `NOTE_EXIT`), then the driver reaps the child with one nonblocking `waitid` of its process ID. A registration that finds no process, because the child ended first, is the exit event |

- A child that ended before the operation started gives its status at once.
- A drop of the wait future ends nothing: the next wait gives the status. A wait after the status
  gives the same status again at once.
- A drop of the `Child` ends the interest only. The driver still reaps the child when it ends, so
  no zombie remains.
- The wait gives `ECHILD` when something else in the process took the status first. This occurs
  when the process sets the disposition of `SIGCHLD` to `SIG_IGN`, because the kernel then reaps
  each child at its end.
- A poll can find no status yet. On a `Route::Poll` backend of Linux, this occurs when another
  process traces the child, for example under `strace -f`: its descriptor is readable after its
  end, and the tracer gets the status first. On macOS, the exit event can come before the status:
  on macOS 27.2.1, in 16 % to 56 % of the exits, in four runs of 2000.
- After such a poll, the driver waits 100 µs, and the wait doubles at each poll that finds no
  status, up to 10 ms. Then it polls once each 10 ms until the status comes, the kernel gives
  `ECHILD`, or teardown. So a waiting child costs at most one poll each 10 ms, and the child slots
  bound the waiting children. No thread waits and nothing spins. On Linux, one timer of the
  driver (a `timerfd`) ends each wait. On macOS, a one-shot kqueue timer ends it, and the status
  came 1 ms after the exit event on average and 12 ms after it at most (measured).

## Standard streams

A stream that the command pipes (`Stdio::piped()`) is a pipe end of the driver: `child.stdin` is a
`PipeWriter`, and `child.stdout` and `child.stderr` are `PipeReader`s. Each one is a resource of its
own, under the rules of [Pipes on the driver](pipes.md). So you write the input while both outputs
drain, and no full pipe stops the child. Close each pipe end that you take: a dropped pipe end
keeps its descriptor until teardown.

## Signals and kill

No call reaches a process ID that the system gave to another process. On Linux, each call goes
through the process descriptor. On macOS, the driver signals the process ID only while it holds the
child unreaped, and it first looks, with no reap, that no other waiter of the process took the
status: a `SIGCHLD` disposition of `SIG_IGN`, or a wait for any child in another library, can take
it. A `Signal` is the type of [signal streams](signals.md), for example
`Signal::TERMINATE`, or `Signal::from_raw(number)`:

| Call | Effect |
| --- | --- |
| `Driver::signal_child(&child, signal)` | Sends `signal` to the child |
| `Driver::kill_child(&child)` | Sends `SIGKILL` to the child |
| `Driver::signal_tree(&child, signal)` | Sends `signal` to each process of the tree of the child |
| `Driver::kill_tree(&child)` | Sends `SIGKILL` to each process of the tree of the child |

A signal ends no wait: the wait ends when the child ends, and it then gives the status, for example
the signal that ended the child. Each call gives `SignalChildError::Reaped` after the reap,
`SignalChildError::OtherDriver` for a child of another driver, `SignalChildError::NoTree` for a
tree call on a child with no tree, and `SignalChildError::System` when the system refuses the
signal.

## Process tree

With `ChildOptions::tree(true)`, the child leads a new process group, its tree, and the group ID is
the process ID of the child. Its descendants join the group unless they leave it. The tree calls and
`End::Kill` signal the group.

- The driver signals the group only while it holds the child unreaped. After the reap, the group
  ID can name another group, so a tree call refuses with `SignalChildError::Reaped`. A descendant
  that runs after the end of the child is then outside the reach of the driver.
- A descendant that leaves the group, for example with `setsid` or `setpgid`, is outside the tree.
- Containers and control groups stay with the caller.

## End policy and teardown

The end policy is stated at the spawn or the adoption:

| `End` | `Driver::close_child(child)` or a drop of the `Child` | `Driver::teardown()` |
| --- | --- | --- |
| `End::Keep` (the default) | The child runs on, and the driver reaps it when it ends | The child runs on. `Teardown::kept()` counts it |
| `End::Kill` | The driver kills the child, or each process of its tree, and then reaps it | The same, and teardown waits for the reap |

`Driver::close_child` applies the policy at once. A drop applies it at the next turn. A close on
a driver that did not make the child refuses with `Reason::Resource` and gives the `Child` back:
close or drop it on its own driver. After
teardown, nothing reaps a kept child: it stays a zombie after its end until this process ends.

## Capacity

- Each child holds a child slot until the driver reaps it (`Config::children`, default 64).
- Each child holds an operation slot until its status is taken or the `Child` drops.
- While its job runs, a spawn also holds an operation slot, a job slot of the blocking service, and
  three resource slots, one for each standard stream. The driver reserves them before the job
  starts, because `Command` does not tell which streams it pipes, and a child that the driver cannot
  give to its caller must not run. The resource slots that the child does not use come back when the
  job ends.
