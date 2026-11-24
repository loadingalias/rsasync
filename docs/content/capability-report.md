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
| Resolved | `backend()` | The backend that runs: `Backend::IoUring` or `Backend::Epoll` ([the epoll backend](epoll.md)) on Linux, `Backend::Kqueue` ([the kqueue backend](kqueue.md)) on macOS, and `Backend::IoRing` or `Backend::Iocp` ([the IOCP and IoRing backends](iocp.md)) on Windows. The report states no segmentation offload: check each path with `datagram::Probe` ([validate segmentation offload](datagram-io.md#validate-segmentation-offload)). On `Backend::Kqueue`, the send of the probe gives `Refused::NoSegmentation` |
| Resolved | `fallback()` | The error number of the ring creation (`EPERM` or `ENOSYS`) when the permissive selection runs epoll for that reason, else `None`. On Windows, the `HRESULT` of the step of the IoRing probe that refused the ring when the permissive selection runs IOCP for that reason, for example `0x80460003` (`IORING_E_VERSION_NOT_SUPPORTED`) for a ring before version 3 (see [IoRing files](iocp.md#ioring-files)) |
| Requested | `required()` | The features of `Config::require`. The driver has each of them |
| Resolved | `probed()` | The features that the probes found on the ring of the driver. A probe runs the feature or reads it from the kernel. The version of the kernel proves nothing. On epoll, kqueue, IOCP, and IoRing, no feature probe runs, and the set is empty |
| Resolved | `route(call)` | How `call` runs on this kernel: `Route::Ring` (an entry of io_uring, or of the I/O ring on `Backend::IoRing`), `Route::SystemCall`, `Route::BlockingService`, `Route::Poll`, or on Windows `Route::Port` and `Route::Unsupported` |
| Applied | `helper_threads()` | The threads that the driver starts: 0. A call on `Route::BlockingService` runs on a thread of your service |
| Applied | `rearms_wake()` | Whether the driver registers its wake descriptor again in each turn that takes a wake. It does so on a kernel whose registered event descriptor signals deferred work one time, such as Ubuntu 26.04 `7.0.0-1016-aws`. It also does so when a deferred signal came late during the probe of the kernel. A driver registers its wake descriptor only at its first use (`AsFd`), so this gives `false` before that use. It always gives `false` for a driver whose caller waits only in `Driver::wait` or `Driver::wait_until`, such as the driver of a runtime worker |

The features are `ChildWait` (Linux 6.7), `Truncate` (6.9), `Bundles` (6.10), `Bind` and `Listen`
(6.11), `IncrementalBufferRings` (6.12), `RinglessMessages` (6.13), and `ZeroCopyReceive` (6.15).

## Routes

| Call | `Route::Ring` when the kernel has | Else |
| --- | --- | --- |
| `Call::Bind` (`Driver::bind`) | `Bind` and `Listen` | `Route::SystemCall`: the bind and the listen are system calls, and the future is ready at once |
| `Call::BindUdp` (`Driver::bind_udp`) | `Bind` | `Route::SystemCall` |
| `Call::SetFileLen` (`Driver::set_file_len`) | `Truncate` | `Route::BlockingService`: the call is a job of the service of `Driver::use_blocking`. With no service, it refuses with `Reason::Blocking` |
| `Call::WaitChild` (the exit of a `Child`, see [child processes](processes.md)) | `ChildWait` | `Route::Poll`: the ring or epoll polls the process descriptor of the child, and kqueue waits for the exit event of its process. Then the driver takes the status with one nonblocking `waitid` |
| `Call::FileData` (the positional and vectored reads and writes of a file, see [files](files.md)) | The io_uring and IoRing backends | `Route::Port` on IOCP: overlapped calls on the completion port. `Route::BlockingService` on epoll and kqueue |
| `Call::PunchHole` (`Driver::punch_hole`) | The io_uring backend | `Route::Port` on IOCP. `Route::BlockingService` on epoll, kqueue, and IoRing, whose ring has no hole punch |
| `Call::SyncFile` (`Driver::sync`) | The io_uring and IoRing backends | `Route::BlockingService` on epoll, kqueue, and IOCP |
| `Call::SyncDirectory` (`Driver::sync_directory`) | The io_uring backend | `Route::BlockingService` on epoll and kqueue. `Route::Unsupported` on IOCP and IoRing: the call gives `Unsupported` at once, because no source states that a flush of a directory makes its namespace changes durable on Windows |
| `Call::File` (each other file and directory operation: open, length, preallocation, rename, and unlink, and on Windows the NVMe report) | The io_uring backend | `Route::BlockingService`, on epoll, kqueue, IOCP, and IoRing |

On epoll, kqueue, IOCP, and IoRing, no feature is probed. So `Call::Bind`, `Call::BindUdp`,
`Call::SetFileLen`, and `Call::WaitChild` take the route of the last column there.

The identity of the peer of an IPC stream (`Driver::ipc_peer`) has one rule that only Windows
has: Windows gives the user of a named pipe client only after its security context exists. A
client of the driver gives its user at the accept. A client of dynamic tracking, the default of
`CreateFileW`, gives it at the first receive that takes its data, and before it `ipc_peer` gives
`ERROR_CANNOT_IMPERSONATE` (1368): receive the first bytes, then check the user (see
[IPC streams on Windows](ipc-streams.md#windows)).
On IOCP and IoRing, `Call::Bind` and `Call::BindUdp` are `Route::SystemCall`, and
`Call::SetFileLen` is `Route::BlockingService`, because Windows has no overlapped form of a length
change.

## Strict and permissive selection

With no requirement, selection is permissive: the driver starts on each supported kernel, and the
report gives the backend and the route that each call takes. Where the kernel refuses io_uring, the
driver runs epoll, and `fallback()` gives the error number. `Config::new().backend(Backend::IoUring)`
makes `Driver::new` refuse there with `Error::Refused` instead, before a driver exists. On Windows,
the permissive selection runs IoRing where its probe passes, and IOCP else, with the `HRESULT` of
the refusal in `fallback()`. `Config::new().backend(Backend::IoRing)` refuses there with
`Error::Refused`, and `Config::new().backend(Backend::Iocp)` runs the files on the port also where
the ring exists. A requirement is strict:
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

## Reports on three systems

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
        FileData: Ring,
        SyncDirectory: Ring,
        File: Ring,
    },
    helper_threads: 0,
    rearms_wake: false,
}
```

That run came before the calls `PunchHole` and `SyncFile`, which this revision also lists. Each one
takes `Route::Ring` on io_uring, as the calls that they came from do.

On Ubuntu 26.04, `7.0.0-1016-aws`, the probes find each feature:

```text
probed: {ChildWait, Truncate, Bundles, Bind, Listen, IncrementalBufferRings, RinglessMessages, ZeroCopyReceive}
```

There, each call has `Route::Ring`, and `rearms_wake` is `true`.

On Windows Server 2025, build 10.0.26100.33822, the permissive selection runs IoRing, and the report
gives the routes of [IoRing files](iocp.md#ioring-files):

```text
Report {
    requested: None,
    backend: IoRing,
    fallback: None,
    required: {},
    probed: {},
    routes: {
        Bind: SystemCall,
        BindUdp: SystemCall,
        SetFileLen: BlockingService,
        WaitChild: Poll,
        FileData: Ring,
        PunchHole: BlockingService,
        SyncFile: Ring,
        SyncDirectory: Unsupported,
        File: BlockingService,
    },
    helper_threads: 0,
    rearms_wake: false,
}
```

`WaitChild` has no Windows form yet: no child process runs on Windows.
