# The IOCP and IoRing backends

On Windows, the driver runs on an I/O completion port (IOCP). Where the system has an I/O ring
(IoRing) of version 3 or later, the reads, writes, and syncs of files run on the ring, and all else
stays on the port. TCP, UDP, files, and local streams have the same API and the same ownership rules
as on io_uring and epoll: the terminal event of an operation releases its buffer and its charge, and
a dropped or forgotten future ends only the interest. This page tells how each TCP, UDP, file, and
local stream operation runs, when the port skips a packet, which file operations run on your
blocking service, and what differs from Linux. rsasync is in development, and these backends are
not in a published release yet.

## Select the backend

The driver has two backends on Windows x86_64 (`x86_64-pc-windows-msvc`):

| `Config` | The system has IoRing version 3 or later | The system has no such ring |
| --- | --- | --- |
| `Config::new()`, the permissive selection | `Backend::IoRing` | `Backend::Iocp`, and `report().fallback()` gives the `HRESULT` of the refusal |
| `Config::new().backend(Backend::IoRing)` | `Backend::IoRing` | `Driver::new` gives `Error::Refused` with that `HRESULT`, and no driver exists |
| `Config::new().backend(Backend::Iocp)` | `Backend::Iocp` | `Backend::Iocp` |

`Backend::IoRing` differs from `Backend::Iocp` only in its files ([IoRing files](#ioring-files)).
Require `Backend::Iocp` to run each file operation on the port also on a system with a ring, for
example to measure one against the other. `Config::new().backend(Backend::IoUring)`,
`Backend::Epoll`, or `Backend::Kqueue` makes `Driver::new` refuse with `Error::Refused`, with the
kind `Unsupported`. A feature of `Config::require` is a feature of io_uring, so a Windows driver
with a required feature refuses with `Error::Unsupported`.

```rust
use rsasync::driver::{Backend, Config, Driver};

let driver = Driver::new(Config::new())?;
let report = driver.report();
if report.backend() == Backend::Iocp {
    // No IoRing of version 3: the files run on the port.
    println!("IoRing refused with {:?}", report.fallback().map(|error| format!("{error:#010X}")));
}
```

## How each operation runs

| Operation | On IOCP |
| --- | --- |
| Bind and listen | System calls on the thread of the driver (`Route::SystemCall`). A listener does not set `SO_REUSEADDR`, which on Windows lets another socket take a bound port |
| Accept | One `AcceptEx` with a socket that the driver makes. The accepted socket then takes the state of its listener (`SO_UPDATE_ACCEPT_CONTEXT`) |
| Connect | One `ConnectEx`, after a bind of the new socket to the wildcard address of its family |
| Receive into your buffer | One overlapped `WSARecv` |
| Receive into a selected buffer (`Driver::provide`) | A receive of zero bytes, which waits for data and holds no buffer. When data comes, one nonblocking `WSARecv` reads into a free slot of the pool |
| Send and vectored send | One overlapped `WSASend` of the lease, or of the `WSABUF` array of the regions |
| Half-close | One `shutdown` with `SD_SEND`, which ends at its return |
| Close | The driver cancels each operation of the socket that runs with `CancelIoEx`, waits for each terminal event, and then closes the socket |

The driver makes each socket itself, overlapped and with no inheritance
(`WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT`), and it associates each socket with its port
when it takes it. A socket stays associated with that port until its close. When the association
fails, the operation gives `OutOfMemory`, as with no free resource slot, and an accepted connection
stays with its listener for the next accept.

## The terminal event

Before each call, the driver sets the NT status of the `OVERLAPPED` of the call to
`STATUS_PENDING`. The Win32 return separates three cases, and the NT status decides the third:

| Return of the call | Terminal event |
| --- | --- |
| Pending (`WSA_IO_PENDING`) | The completion packet. After `CancelIoEx`, the `OVERLAPPED` and the buffer stay owned until that packet comes, whatever status it gives |
| Success at once | The completion packet, or on a socket that skips packets, the return |
| Failure at once, with a warning status such as `STATUS_BUFFER_OVERFLOW` | The completion packet, or on a socket that skips packets, the return. Win32 reports this warning as a failure (`WSAEMSGSIZE`), and its packet still comes. The I/O manager skips the packet of each status that is not an error, so a skipping socket gets none. This case has no run evidence: on Windows Server 2025, an overlapped receive of a datagram larger than its buffer returns pending, also when the datagram waits |
| Failure at once, with an error status (`NT_ERROR`), or with the status still `STATUS_PENDING` because the call did not reach the I/O manager | The return. No packet comes |

The driver never classifies a failed return as terminal by the Win32 result alone: a warning status
means that the kernel still writes the buffer and queues a packet.

## UDP

UDP runs on the port with the API of [UDP on the driver](udp.md), and the datagram calls and control
messages of [datagram I/O](datagram-io.md#windows):

| Operation | On IOCP |
| --- | --- |
| Bind (`Driver::bind_udp`) | System calls on the thread of the driver. `UdpOptions::gro` turns on coalesced receive (URO) with `UDP_RECV_MAX_COALESCED_SIZE` |
| One datagram, with its metadata and segment size (USO) | One overlapped `WSASendMsg` or `WSARecvMsg`, with the control messages of `rsnetwork-udp` |
| Receive into a selected buffer | A receive of zero bytes with `MSG_PEEK`, which waits for a datagram and holds no buffer. When one waits, one nonblocking `WSARecvMsg` reads it into a free slot of the pool |
| Batches: `send_batch` and `recv_batch` | One call for each datagram, one at a time. A send that the stack refuses ends the batch, and the report counts the datagrams before it. A batch receive waits for its first datagram, and then takes only the datagrams that wait |

A datagram larger than its buffer fills the buffer, and the receive reports `truncated`. The stack
gives the warning status `STATUS_BUFFER_OVERFLOW` for it, which Win32 reports as the failure
`WSAEMSGSIZE`. The driver keeps the buffer until the terminal event of the receive and reports the
truncation, never an error. On Windows Server 2025, an overlapped receive of a datagram that waits
returns pending, on both forms of socket, so the receive ends at its packet, which carries the
warning. No run shows the immediate form of this warning.

The validation of USO and URO on a path is the [probe of datagram I/O](datagram-io.md#validate-segmentation-offload): send
its segments through the driver, give each received datagram to `Probe::observe`, and turn the
offload off when the verdict is `Verdict::Fallback`, which names the reason. Windows loopback keeps
the segment sizes and each datagram, so no run shows a fallback.

Windows refuses to send the CE codepoint (`WSAEINVAL`), and it has one IPv4 loopback address.

## Files

On `Backend::Iocp`, files run on the port with the API of [files on the driver](files.md). A read,
a write, and a hole punch are overlapped calls on the port (`Route::Port` for `Call::FileData` and
`Call::PunchHole`). Windows has no overlapped form of the other file calls, so each one runs as a
job of the [blocking service](blocking-service.md) that `Driver::use_blocking` gives
(`Route::BlockingService` for `Call::SyncFile`, `Call::File`, and `Call::SetFileLen`). With no
service, such a call refuses with `Reason::Blocking`. On `Backend::IoRing`, the reads, the writes,
and the syncs are entries of the ring instead ([IoRing files](#ioring-files)). Give the driver a
service before the first open, on both backends:

```rust
use core::num::NonZeroU64;

use rsasync::blocking::{self, Service};
use rsasync::capacity::Budget;
use rsasync::driver::{Config, Driver};

let mut driver = Driver::new(Config::new())?;
let budget = Budget::new(NonZeroU64::new(16 << 20).unwrap());
let service = Service::new(&budget, blocking::Config::new())?;
driver.use_blocking(service.submitter());
```

| Operation | On IOCP |
| --- | --- |
| Open and directory open | A job: one `CreateFileW` with `FILE_FLAG_OVERLAPPED`, which lets other handles read, write, rename, and delete the file. A file with writes gets a second handle of the same file with `FILE_FLAG_WRITE_THROUGH` (`ReOpenFile`), for the durable writes. A directory open uses `FILE_FLAG_BACKUP_SEMANTICS`. On `Backend::Iocp`, the driver associates each handle with its port |
| Read and write | One overlapped `ReadFile` or `WriteFile` at the offset of the `OVERLAPPED`. A read at or after the end of the file gives 0 |
| Durable write (`WriteMode::DataSync`) | One overlapped `WriteFile` on the write-through handle |
| Vectored read and write | One overlapped call for each region, in order: the next region starts at the packet of the one before it. A short or failed region ends the operation, with the bytes before it |
| Length and its change | A job: `GetFileInformationByHandleEx` with `FileStandardInfo`, and `SetFileInformationByHandle` with `FileEndOfFileInfo` |
| Preallocation | A job: `FileAllocationInfo`, and `FileEndOfFileInfo` for `Length::Extend`. It runs alone on its file (see [Length and space](files.md#length-and-space)) |
| Hole punch | Two overlapped `DeviceIoControl` calls: `FSCTL_SET_SPARSE`, then `FSCTL_SET_ZERO_DATA` |
| Sync | A job: `FlushFileBuffers` |
| Directory sync | `Unsupported` at once, with no call (`Route::Unsupported` for `Call::SyncDirectory`): no source states that a flush of a directory makes its creates, renames, and unlinks durable on NTFS |
| Rename and unlink | A job: `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`, and `DeleteFileW` |
| NVMe report (`Driver::nvme_path`) | A job that reads the driver of each disk of the volume of the file (see [Native NVMe](#native-nvme)) |
| Close | `CloseHandle` of each handle of the file, on the thread of the driver, after each operation of the file ended |

A file never skips packets: each job owns a duplicate of the file handle, so that it never names a
handle that the driver closed, and the port skips packets only on a handle that is never
duplicated. So each overlapped file call that does not fail at once ends at its packet, and the file
cases pass the same on both forms of the suites. A transfer moves at most `i32::MAX` bytes in one
operation, and the rest stays for the next one, as on Linux.

### IoRing files

On `Backend::IoRing`, the driver keeps one I/O ring beside its port, and no file is on the port:
the ring refuses a read or a write of a file on a port with `E_INVALIDARG` (`0x80070057`). Sockets
and local streams stay on the port, as on `Backend::Iocp`.

| Operation | On IoRing |
| --- | --- |
| Read and write | One entry of the ring (`BuildIoRingReadFile`, `BuildIoRingWriteFile`) at the offset of the operation. Its completion queue entry ends it and gives its count of bytes. A read at or after the end of the file gives 0 |
| Durable write (`WriteMode::DataSync`) | One write entry on the write-through handle. The ring refuses its own write-through flag on a handle that the cache holds (`ERROR_NOT_SUPPORTED_WITH_CACHED_HANDLE`), so the driver keeps the write-through handle |
| Vectored read and write | One entry for each region, in order: the next region starts at the completion queue entry of the one before it |
| Sync | One flush entry with `FILE_FLUSH_DEFAULT` for each `SyncMode`, which the SDK header `winbase.h` defines as "same as WIN32 FlushFileBuffers(); Flushes data, metadata, AND sends a SYNC command to the hardware" (`Route::Ring` for `Call::SyncFile`) |
| Hole punch | A job: `FSCTL_SET_SPARSE`, then `FSCTL_SET_ZERO_DATA`, each an overlapped `DeviceIoControl` on a handle that no port holds, which the job waits for on its own event (`Route::BlockingService` for `Call::PunchHole`) |
| Each other file call | A job, as on `Backend::Iocp` |
| Cancel | One cancel entry (`BuildIoRingCancelRequest`). The operation still ends at its own completion queue entry, with `ERROR_OPERATION_ABORTED` (995) when the cancel stopped it, and the driver keeps its buffer until then |

The kernel signals an event when the completion queue of the ring goes from empty to not empty. A
wait completion packet turns that signal into one packet on the port, so the one port wait of the
driver also wakes for the ring, with no thread. At that packet, the driver takes each entry of the
completion queue before it waits again. The completion queue has an entry for each operation that
the driver can have in flight, so it never fills.

The probe runs at `Driver::new`, and it reads facts of the system, never its version. It takes the
functions of the ring from `kernel32.dll` at run time, so a program still loads on a system with no
ring. Each step that fails gives the `HRESULT` of `report().fallback()`:

| Step | Refusal |
| --- | --- |
| The functions of the ring and of the wait completion packet exist | `0x8007007F` (`ERROR_PROC_NOT_FOUND`): no IoRing, as before Windows build 22000 |
| `QueryIoRingCapabilities` gives a kernel ring with a completion event | `0x80070032` (`ERROR_NOT_SUPPORTED`) for a ring that the system emulates in user mode, or one with no completion event |
| `CreateIoRing` makes a ring of version 3 | `0x80460003` (`IORING_E_VERSION_NOT_SUPPORTED`) on a system with only version 1 or 2, which have no write or flush |
| `IsIoRingOpSupported` gives the read, the write, the flush, and the cancel | `0x80070032` |
| The completion event and the wait completion packet start | The `HRESULT` of the call |

The probed versions:

| System | IoRing | Files |
| --- | --- | --- |
| Windows Server 2025, build 10.0.26100.33822 | Version 4 at most, with each operation of the probe | Reads, writes, and syncs on the ring; the rest as on IOCP |
| Windows 11 | Not run | Not run |

A close of the ring ends no operation: the kernel still writes the buffer of an operation after the
close. So the drop of a driver closes its ring only when no operation of the ring runs, and else
leaks the ring, as it leaks the buffers of such an operation. `Driver::teardown` cancels each
operation and waits for each completion queue entry first.

### Native NVMe

`Driver::nvme_path(&file)` gives `NvmePath::Native` when the native NVMe path of Windows Server
2025 serves each disk of the volume of the file, and `NvmePath::Other` when another driver serves a
disk, or when the volume has no local disk. A job reads the driver service of the device node of
each disk (`nvmedisk` for the native path, `disk` for the disk class driver over the SCSI
translation of the port driver). It never infers the path from the version of the system. When
the allocator refuses the list of the devices, the report gives `OutOfMemory`.

## Local streams

A local stream is a byte-mode named pipe, with the API of
[local streams on the driver](local-streams.md#windows). The bind (`CreateNamedPipeW`) and the
connect (`CreateFileW`) are calls on the thread of the driver, which end at once. An accept is one
overlapped `ConnectNamedPipe` on the instance of the listener, and a receive, a send, and a
vectored send are overlapped `ReadFile` and `WriteFile` calls, as for a file: a vectored send is one
call for each region, in order. A pipe never skips packets, as a file does not. A cancel of a
waiting accept, for example at the close of its listener, is `CancelIoEx`, and the instance stays
owned until the packet of the accept. A read after the close of the other end gives the end of the
stream (`ERROR_BROKEN_PIPE`), and a write that moved bytes before it failed gives their count.

## Skip-on-success

A socket skips the packet of a call that ends at once (`FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`) only
when both checks pass:

- Its provider gives handles of the installable file system (`XP1_IFS_HANDLES` in
  `SO_PROTOCOL_INFOW`).
- No layered provider is above the base provider: `SIO_BASE_HANDLE` gives the same socket.

The rule needs one more fact: no other handle of the socket exists. The driver makes each socket
itself with no inheritance, and no API of the driver duplicates a socket or adopts one, so this
holds for each socket of the driver. A socket that fails a check gets a packet for each call, and the
driver ends each call at its packet. Both forms pass the same suites. Under
[controlled delivery](controlled-delivery.md), no socket skips, so each completion comes through the
port.

## Drive it

`turn`, `wait`, and `teardown` work as on Linux ([driver-only use](driver-only.md)). Each turn
removes up to 128 packets from the port with no wait. A wait blocks in
`GetQueuedCompletionStatusEx`, and a wake from another thread (`WakeHandle::wake`) posts a packet
that ends it. At most one wake packet waits in the port.

## Helper threads

The driver starts no thread, and `report().helper_threads()` gives 0. The file calls with no
overlapped form run on the threads of your blocking service: open, directory open, length
inspection and change, preallocation, sync, rename, unlink, and the NVMe report. On
`Backend::IoRing`, the sync is an entry of the ring, and the hole punch is a job too.

## Differences from Linux

- A send goes whole or fails. The stack takes each byte of a send while its send buffer has room, and
  with the default buffer it takes a send of 64 MiB at once, also when the peer does not read. A
  send ends with part of its bytes only when the connection fails, for example at a reset: the
  packet then gives the count of the bytes that went, and the driver reports it as the progress of
  the send, as on Linux.
- A cancel of a send that runs in the kernel aborts the connection, and its packet gives no count of
  the bytes that went. So the driver never reports a count for it: an explicit cancel gives the
  error `WSAECONNABORTED` with the whole lease, and the read direction fails too. When a cancel
  aborts a dropped send that runs, at close or before its future dropped, the write direction
  fails with no count: the next send gives `WriteFailed` and the close gives `Closed`, whose
  `sent()` is `SendCount::Unknown`. A send that waits behind another send in the
  driver has not started, so its cancel gives the lease back with `ERROR_OPERATION_ABORTED`, and
  the direction stays usable.
- `TcpOptions::reuse_port` has no Windows option: a value gives the error `WSAENOPROTOOPT` (10042),
  and `Driver::options` gives `None`. Windows keeps the buffer sizes that you set, with no doubling.
- The driver has no `AsFd` on Windows: a port handle is not a handle that another reactor can wait
  on. Turn and wait the driver on its own thread.
- A cancelled operation gives `ERROR_OPERATION_ABORTED` (995), where Linux gives `ECANCELED`, and a
  selecting receive with no free buffer gives `WSAENOBUFS` (10055), where Linux gives `ENOBUFS`.

## Limits

- TCP, UDP, files, and local streams only. Pipes, processes, and signals have no Windows form yet.
- Windows x86_64 only. Windows on aarch64 has no run evidence.
- IoRing has run evidence only on Windows Server 2025. Windows 11 is not run.
- The ring registers no file handle and no buffer: each entry names its handle and its buffer.
