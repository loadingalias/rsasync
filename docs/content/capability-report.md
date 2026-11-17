# Capability report

`Driver::report()` states what a driver found and applied when it started: the backend that you
required and the backend that runs, the features that you required, the features that the probes of
the kernel found, the route of each call that depends on them, and the threads that the driver
starts. The driver builds the report one time, in
`Driver::new`, and the report does not change. rsasync is in development, and the report is not in
a published release yet.

## Read the report

```rust
use rsasync::driver::{Call, Config, Driver, Feature, Route};

let driver = Driver::new(Config::new())?;
let report = driver.report();
if report.route(Call::SetFileLen) == Route::BlockingService {
    // This kernel has no `IORING_OP_FTRUNCATE`: give the driver a blocking service.
}
println!("truncate on the ring: {}", report.probed().contains(Feature::Truncate));
println!("{report:#?}");
```

| Part | Method | What it gives |
| --- | --- | --- |
| Requested | `requested()` | The backend of `Config::backend`, or `None` for the permissive selection |
| Resolved | `backend()` | The backend that runs: `Backend::IoUring` or `Backend::Epoll` ([the epoll backend](epoll.md)) on Linux, `Backend::Kqueue` ([the kqueue backend](kqueue.md)) on macOS, and `Backend::Iocp` ([the IOCP backend](iocp.md)) on Windows. The report states no segmentation offload: check each path with `datagram::Probe` ([validate segmentation offload](datagram-io.md#validate-segmentation-offload)). On `Backend::Kqueue`, the send of the probe gives `Refused::NoSegmentation` |
| Resolved | `fallback()` | The error number of the ring creation (`EPERM` or `ENOSYS`) when the permissive selection runs epoll for that reason, else `None` |
| Requested | `required()` | The features of `Config::require`. The driver has each of them |
| Resolved | `probed()` | The features that the probes found on the ring of the driver. A probe runs the feature or reads it from the kernel. The version of the kernel proves nothing. On epoll, kqueue, and IOCP, no probe runs, and the set is empty |
| Resolved | `route(call)` | How `call` runs on this kernel: `Route::Ring`, `Route::SystemCall`, `Route::BlockingService`, or `Route::Poll` |
| Applied | `helper_threads()` | The threads that the driver starts: 0. A call on `Route::BlockingService` runs on a thread of your service |
| Applied | `rearms_wake()` | Whether the driver registers its wake descriptor again in each turn that takes a wake. It does so on a kernel whose registered event descriptor signals deferred work one time, such as Ubuntu 26.04 `7.0.0-1016-aws`. It also does so when a deferred signal came late during the probe of the kernel |

The features are `ChildWait` (Linux 6.7), `Truncate` (6.9), `Bundles` (6.10), `Bind` and `Listen`
(6.11), `IncrementalBufferRings` (6.12), `RinglessMessages` (6.13), and `ZeroCopyReceive` (6.15).

## Routes

| Call | `Route::Ring` when the kernel has | Else |
| --- | --- | --- |
| `Call::Bind` (`Driver::bind`, `Driver::bind_local`) | `Bind` and `Listen` | `Route::SystemCall`: the bind and the listen are system calls, and the future is ready at once |
| `Call::BindUdp` (`Driver::bind_udp`) | `Bind` | `Route::SystemCall` |
| `Call::SetFileLen` (`Driver::set_file_len`) | `Truncate` | `Route::BlockingService`: the call is a job of the service of `Driver::use_blocking`. With no service, it refuses with `Reason::Blocking` |
| `Call::WaitChild` (the exit of a `Child`, see [child processes](processes.md)) | `ChildWait` | `Route::Poll`: the ring or epoll polls the process descriptor of the child, and when it is readable, the driver takes the status with one nonblocking `waitid` |
| `Call::File` (each other file and directory operation, see [files](files.md)) | The io_uring backend | `Route::BlockingService`, on epoll |

On epoll, no feature is probed, so each call takes the route of the last column.

## Strict and permissive selection

With no requirement, selection is permissive: the driver starts on each supported kernel, and the
report gives the backend and the route that each call takes. Where the kernel refuses io_uring, the
driver runs epoll, and `fallback()` gives the error number. `Config::new().backend(Backend::IoUring)`
makes `Driver::new` refuse there with `Error::Refused` instead, before a driver exists. A requirement is strict:
`Config::new().require(Feature::Truncate)` makes `Driver::new` refuse on a kernel with no such
feature. It gives `Error::Unsupported(missing)`, with each missing feature, and no driver exists.
Use it when a fallback route is not acceptable to you.

```rust
use rsasync::driver::{Config, Driver, Error, Feature};

match Driver::new(Config::new().require(Feature::Truncate)) {
    Ok(driver) => assert!(driver.report().required().contains(Feature::Truncate)),
    Err(Error::Unsupported(missing)) => eprintln!("this kernel has no {missing:?}"),
    Err(error) => eprintln!("the driver did not start: {error}"),
}
```

## Accelerations

The driver registers no file table and no fixed buffers, and it starts no submission poller
(`SQPOLL`). The view of the kernel of a new ring (`/proc/self/fdinfo/<descriptor>`) shows
`UserFiles: 0`, `UserBufs: 0`, and `SqThread: -1`. The one acceleration today is the provided buffer
ring, which only `Driver::provide` registers.

A check that an applied fact agrees with its probe comes with the first registered acceleration.

## Reports on two kernels

On Debian 12, `6.1.0-54-cloud-arm64`, `println!("{report:#?}")` prints this report. No probe finds
its feature, so each call takes its fallback route:

```text
Report {
    requested: None,
    backend: IoUring,
    fallback: None,
    required: {},
    probed: {},
    routes: {
        Bind: SystemCall,
        BindUdp: SystemCall,
        SetFileLen: BlockingService,
        WaitChild: Poll,
        File: Ring,
    },
    helper_threads: 0,
    rearms_wake: false,
}
```

On Ubuntu 26.04, `7.0.0-1016-aws`, the probes find each feature:

```text
probed: {ChildWait, Truncate, Bundles, Bind, Listen, IncrementalBufferRings, RinglessMessages, ZeroCopyReceive}
```

There, each call has `Route::Ring`, and `rearms_wake` is `true`.
