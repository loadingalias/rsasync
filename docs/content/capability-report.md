# Capability report

`Driver::report()` states what an io_uring driver found and applied when it started: the features
that you required, the features that the probes of the kernel found, the route of each call that
depends on them, and the threads that the driver starts. The driver builds the report one time, in
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
| Requested | `required()` | The features of `Config::require`. The driver has each of them |
| Resolved | `probed()` | The features that the probes found on the ring of the driver. A probe runs the feature or reads it from the kernel. The version of the kernel proves nothing |
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
| `Call::WaitChild` (the exit of a `Child`, see [child processes](processes.md)) | `ChildWait` | `Route::Poll`: the ring polls the process descriptor of the child, and when it is readable, the driver takes the status with one nonblocking `waitid` |

## Strict and permissive selection

With no requirement, selection is permissive: the driver starts on each supported kernel, and the
report gives the route that each call takes. A requirement is strict:
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

The report is for the io_uring driver. A backend field and a check that an applied fact agrees with
its probe come with the second backend and the first registered acceleration.

## Reports on two kernels

On Debian 12, `6.1.0-54-cloud-arm64`, `println!("{report:#?}")` prints this report. No probe finds
its feature, so each call takes its fallback route:

```text
Report {
    required: {},
    probed: {},
    routes: {
        Bind: SystemCall,
        BindUdp: SystemCall,
        SetFileLen: BlockingService,
        WaitChild: Poll,
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
