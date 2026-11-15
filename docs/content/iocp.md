# The IOCP backend

On Windows, the driver runs on an I/O completion port (IOCP). TCP has the same API and the same
ownership rules as on io_uring and epoll: the terminal event of an operation releases its buffer and
its charge, and a dropped or forgotten future ends only the interest. This page tells how each TCP
operation runs on the port, when the port skips a packet, and what differs from Linux. rsasync is in
development, and the IOCP backend is not in a published release yet.

## Select the backend

IOCP is the one backend of the driver on Windows x86_64 (`x86_64-pc-windows-msvc`).
`Config::new()` selects it, and `report().backend()` gives `Backend::Iocp`.
`Config::new().backend(Backend::IoUring)` or `Backend::Epoll` makes `Driver::new` refuse with
`Error::Refused`. A feature of `Config::require` is a feature of io_uring, so a Windows driver with a
required feature refuses with `Error::Unsupported`.

```rust
use rsasync::driver::{Backend, Config, Driver};

let driver = Driver::new(Config::new())?;
assert_eq!(driver.report().backend(), Backend::Iocp);
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
| Failure at once, with a warning status such as `STATUS_BUFFER_OVERFLOW` | The completion packet, or on a socket that skips packets, the return. Win32 reports this warning as a failure (`WSAEMSGSIZE`), and its packet still comes |
| Failure at once, with an error status (`NT_ERROR`), or with the status still `STATUS_PENDING` because the call did not reach the I/O manager | The return. No packet comes |

The driver never classifies a failed return as terminal by the Win32 result alone: a warning status
means that the kernel still writes the buffer and queues a packet.

## Skip-on-success

A socket skips the packet of a call that ends at once (`FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`) only
when both checks pass:

- Its provider gives handles of the installable file system (`XP1_IFS_HANDLES` in
  `SO_PROTOCOL_INFOW`).
- No layered provider is above the base provider: `SIO_BASE_HANDLE` gives the same socket.

The rule needs one more fact: no other handle of the socket exists. The driver makes each socket
itself with no inheritance, and no API of the driver duplicates a socket or adopts one, so this
holds for each socket of the driver. A socket that fails a check gets a packet for each call, and the
driver ends each call at its packet. Both forms pass the same suites.

## Drive it

`turn`, `wait`, and `teardown` work as on Linux ([driver-only use](driver-only.md)). Each turn
removes up to 128 packets from the port with no wait. A wait blocks in
`GetQueuedCompletionStatusEx`, and a wake from another thread (`WakeHandle::wake`) posts a packet
that ends it. At most one wake packet waits in the port.

## Helper threads

The driver starts no thread, and `report().helper_threads()` gives 0.

## Differences from Linux

- A send goes whole or fails. The stack takes each byte of a send while its send buffer has room, and
  with the default buffer it takes a send of 64 MiB at once, also when the peer does not read. A
  send ends with part of its bytes only when the connection fails, for example at a reset: the
  packet then gives the count of the bytes that went, and the driver reports it as the progress of
  the send, as on Linux.
- A cancel of a send that runs in the kernel aborts the connection, and its packet gives no count of
  the bytes that went. So the driver never reports a count for it: an explicit cancel gives the
  error `WSAECONNABORTED` with the whole lease, and the read direction fails too. When close or
  teardown cancels a dropped send that runs, the write direction fails with no count:
  `WriteFailed::sent` and `Closed::sent` give `None`. A send that waits behind another send in the
  driver has not started, so its cancel gives the lease back with `ERROR_OPERATION_ABORTED`, and
  the direction stays usable.
- `TcpOptions::reuse_port` has no Windows option: a value gives the error `WSAENOPROTOOPT` (10042),
  and `Driver::options` gives `None`. Windows keeps the buffer sizes that you set, with no doubling.
- The driver has no `AsFd` on Windows: a port handle is not a handle that another reactor can wait
  on. Turn and wait the driver on its own thread.
- A cancelled operation gives `ERROR_OPERATION_ABORTED` (995), where Linux gives `ECANCELED`, and a
  selecting receive with no free buffer gives `WSAENOBUFS` (10055), where Linux gives `ENOBUFS`.

## Limits

- TCP only. UDP, files, pipes, local streams, processes, and signals have no Windows form yet.
- Windows x86_64 only. Windows on aarch64 has no run evidence.
