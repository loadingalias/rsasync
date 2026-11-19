# Files on the driver

This page is the reference for files on the driver, `rsasync::driver::Driver`: open, positional and
vectored reads and writes, the exact read, direct I/O, length and space, durability, names and
directories, and close. It also tells what happens when you drop a future. Files run on io_uring
and [epoll](epoll.md) on Linux, on [kqueue](kqueue.md) on macOS, and on an
[I/O completion port](iocp.md#files) on Windows, with the reads, writes, and syncs on an
[I/O ring](iocp.md#ioring-files) where the system has one, with the same API.
[Drive the I/O driver yourself](driver-only.md) tells how to start, turn, and end the driver.

Each file operation is positional, with an explicit offset. So several operations of one file can
run at one time, and the file has no position that they share.

## Open and use a file

This example makes a file, writes to it, makes the bytes durable, reads them, closes the file, and
removes it. `run` turns the driver until a future is ready, as in the docs of `Operation`. On epoll
and kqueue, each file call is a job of a [blocking service](blocking-service.md), and on IOCP each
file call with no overlapped form, such as the open, so the example gives the driver one:

<!-- The same text as the doctest of `rsasync::driver::File`. Edit both. -->
```rust
use core::num::NonZeroU64;

use rsasync::blocking::{self, Service};
use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::capacity::Budget;
use rsasync::driver::{Config, Driver, OpenOptions, SyncMode, WriteMode};

let mut driver = Driver::new(Config::new())?;
// On epoll and kqueue, each file call is a job of a blocking service, and on IOCP each file
// call with no overlapped form.
let budget = Budget::new(NonZeroU64::new(16 << 20).unwrap());
let service = Service::new(&budget, blocking::Config::new())?;
driver.use_blocking(service.submitter());
let path = std::env::temp_dir().join(format!("rsasync-file-{}", std::process::id()));

let options = OpenOptions::new().read(true).write(true).create(true);
let open = driver.open_file(&path, options)?;
let file = run(&mut driver, open)???;
let write = driver.write_at(&file, Lease::from(b"hello".to_vec()), 0, WriteMode::Plain)?;
let (written, _lease) = run(&mut driver, write)??;
assert_eq!(written?, 5);
// The written bytes become durable.
let sync = driver.sync(&file, SyncMode::Data)?;
run(&mut driver, sync)???;

// A read gives the exact count, which is fewer at the end of the file.
let read = driver.read_at(&file, Backing::from(Vec::with_capacity(64)), 0)?;
let (read, buffer) = run(&mut driver, read)??;
assert_eq!((read?, buffer.bytes()), (5, &b"hello"[..]));

let close = driver.close_file(file)?;
run(&mut driver, close)??;
let unlink = driver.unlink(&path)?;
run(&mut driver, unlink)???;
```

`OpenOptions` sets the open. Its defaults are read only, the page cache, and mode `0o666`:

| Setting | Meaning |
| --- | --- |
| `read(bool)` and `write(bool)` | The access. At least one of them is set |
| `create(bool)` | Make the file if it does not exist. It needs `write` |
| `create_new(bool)` | Make the file, and fail with `AlreadyExists` if it exists. It needs `write` |
| `truncate(bool)` | Set the length of an existing file to 0. It needs `write` |
| `mode(u32)` | The mode of a file that the open makes, less the umask. Windows has no mode: a new file takes the security of its directory |
| `cache(Cache)` | `Cache::Buffered`, through the page cache, or `Cache::Direct` (`O_DIRECT`, `F_NOCACHE` on macOS, and `FILE_FLAG_NO_BUFFERING` on Windows). See [Direct I/O](#direct-io) |

The descriptor closes on `exec`, and a Windows handle is not inherited. Options that no open can
take, and a path with a NUL byte, give `InvalidInput`. On Windows, the open lets other handles read,
write, rename, and delete the file, as on Linux and macOS. A relative path, or a path that can be
longer than `MAX_PATH` (260 units), goes to the system in its absolute verbatim form (`\\?\`, or
`\\?\UNC\` for a network path), as `std::fs` gives it, so each path that `std::fs` opens also opens
on the driver. A rename and an unlink take their paths in the same way.

## Operations

Each operation gives an `Operation` future. Turn the driver until the future is ready. The outer
`Result` of the output is `Err(Ended)` only if the driver ended first.

| Call | Borrows | Output | Refusal |
| --- | --- | --- | --- |
| `Driver::open_file(path, options)` | Nothing | `io::Result<File>` | `Refused<()>` |
| `Driver::read_at(&file, backing, offset)` | The file, shared | `(io::Result<usize>, Backing)`: the count, and the buffer with the bytes after its earlier bytes | `Refused<Backing>` |
| `Driver::read_exact_at(&file, backing, offset)` | The file, shared | `(io::Result<()>, Backing)`. See [Exact read](#exact-read) | `Refused<Backing>` |
| `Driver::read_vectored_at(&file, buffers, offset)` | The file, shared | `(io::Result<usize>, Buffers)`: the count across the buffers | `Refused<Buffers>` |
| `Driver::write_at(&file, lease, offset, mode)` | The file, shared | `(io::Result<usize>, Lease)`: the count, and the lease | `Refused<Lease>` |
| `Driver::write_vectored_at(&file, regions, offset, mode)` | The file, shared | `(io::Result<usize>, Regions)`: the count across the regions | `Refused<Regions>` |
| `Driver::file_len(&file)` | The file, shared | `io::Result<u64>` | `Refused<()>` |
| `Driver::set_file_len(&file, len)` | The file, shared | `io::Result<()>`. See [Length and space](#length-and-space) | `Refused<()>` |
| `Driver::allocate(&file, offset, len, length)` | The file, shared | `io::Result<()>` | `Refused<()>` |
| `Driver::punch_hole(&file, offset, len)` | The file, shared | `io::Result<()>` | `Refused<()>` |
| `Driver::sync(&file, mode)` | The file, shared | `io::Result<()>`. See [Durability](#durability) | `Refused<()>` |
| `Driver::rename(from, to)` | Nothing | `io::Result<()>` | `Refused<()>` |
| `Driver::unlink(path)` | Nothing | `io::Result<()>` | `Refused<()>` |
| `Driver::open_directory(path)` | Nothing | `io::Result<Directory>` | `Refused<()>` |
| `Driver::sync_directory(&directory)` | The directory, shared | `io::Result<()>` | `Refused<()>` |
| `Driver::close_file(file)` | Takes the file | `()`. See [Close](#close) | `Refused<File>` |
| `Driver::close_directory(directory)` | Takes the directory | `()` | `Refused<Directory>` |
| `Driver::nvme_path(&file)`, on Windows | The file, shared | `io::Result<NvmePath>`. See [Native NVMe on Windows](#native-nvme-on-windows) | `Refused<()>` |

A refusal gives a `Reason` and changes nothing: `Operations`, `Queue`, `Resource`, `Alignment`
(see [Direct I/O](#direct-io)), or `Blocking` (see [Length and space](#length-and-space)).

A read fills the capacity of the buffer after its initialized bytes. It gives 0 at or after the end
of the file. `Buffers` holds the buffers of a vectored read: the read fills each one to its
capacity before the next one, and `Buffers::into_buffers` gives them back in order. `Regions`, the
type of a vectored send, holds the leases of a vectored write.

## Exact read

`Driver::read_exact_at` reads into the whole capacity of the buffer after its initialized bytes, in
one read. It gives `Ok(())` when the read filled that capacity.

A read that put in fewer bytes gives an error of the kind `UnexpectedEof`. Its inner error is
`Incomplete`, and `Incomplete::read` gives the count. This happens at the end of the file, or on a
file system that gives short reads. The buffer holds the bytes that the read put in. To read the
rest, read again at the offset of the read plus that count.

## Direct I/O

`Cache::Direct` opens the file with `O_DIRECT`: the reads and writes move bytes between the device
and your buffers, with no copy in the page cache. The open reads the alignment of the file with
`statx` (`STATX_DIOALIGN`, Linux 6.1). `File::alignment` gives it: `Alignment::memory` for the
address of each buffer, and `Alignment::offset` for each file offset and each length. A file system
that does not report it gets 4096 bytes for both. A file system with no direct I/O for the file
makes the open give `Unsupported`. macOS has no `O_DIRECT`: the open turns the data cache of the
file off with `F_NOCACHE`, and the alignment is one page for both values, 16 KiB on Apple silicon.
On Windows, the open uses `FILE_FLAG_NO_BUFFERING`, and reads the sector sizes of the device with
`FileStorageInfo`: `Alignment::offset` is the logical sector, which the flag requires for each
offset and length, and `Alignment::memory` is the physical sector, which its documentation asks for
each buffer. A device that reports no sector size gets 4096 bytes.

A read or write whose buffer, length, or offset does not have that alignment refuses with
`Reason::Alignment` before the submission, and gives the buffer back. A read uses the capacity of
the buffer after its initialized bytes, so that part needs the alignment. Use a `Pool` whose slots
have the alignment:

<!-- The same text as the doctest of `rsasync::driver::Alignment`. Edit both. -->
```rust
use core::alloc::Layout;
use core::num::{NonZeroU32, NonZeroU64};

use rsasync::buffer::{Buffer as _, Pool};
use rsasync::capacity::Budget;
use rsasync::driver::{Cache, Config, Driver, OpenOptions};

let mut driver = Driver::new(Config::new())?;
let path = std::env::temp_dir().join(format!("rsasync-direct-{}", std::process::id()));
std::fs::write(&path, vec![7; 16384])?;

let open = driver.open_file(&path, OpenOptions::new().cache(Cache::Direct))?;
let file = run(&mut driver, open)???;
let alignment = file.alignment().expect("a direct-I/O file has its alignment");
// Each slot starts at the memory alignment, and its size is a multiple of the offset
// alignment.
let align = alignment.memory().max(alignment.offset()) as usize;
let slot = Layout::from_size_align(align.max(4096), align)?;
let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
let mut pool = Pool::new(&budget, NonZeroU32::MIN, slot)?;
let read = driver.read_at(&file, pool.acquire()?, 0)?;
let (read, buffer) = run(&mut driver, read)??;
assert_eq!(read?, slot.size());
assert!(buffer.bytes().iter().all(|&byte| byte == 7));
```

## Length and space

`Driver::file_len` gives the length of the file (`statx`). `Driver::set_file_len` sets it: a
longer file reads as zeros after its old end.

From Linux 6.9, `set_file_len` runs on the ring (`IORING_OP_FTRUNCATE`). Before Linux 6.9, it runs
as a job of the [blocking service](blocking-service.md), because the kernel has no ring operation
for it:

- `Driver::report().route(Call::SetFileLen)` is `Route::BlockingService` on such a kernel. The
  driver knows it when it starts, before the first operation (see the
  [capability report](capability-report.md)). `Config::require(Feature::Truncate)` makes such a
  kernel refuse the driver.
- `Driver::use_blocking(submitter)` gives the driver a `Submitter` of your service. Until then, the
  change refuses with `Reason::Blocking`. A full or stopped service also refuses with
  `Reason::Blocking`.
- The job owns a duplicate of the descriptor. The operation ends when the job returns, as an
  operation of the ring ends at its completion, so a close and a teardown wait for it.

On epoll and kqueue, each file operation is such a job, `set_file_len` too. On IOCP, each file
operation with no overlapped form is such a job, `set_file_len` too (see [Files](iocp.md#files)).
On IoRing, each file operation that the ring does not have is such a job, the hole punch too (see
[IoRing files](iocp.md#ioring-files)).

`Driver::allocate` reserves the blocks of a range (`fallocate`), so that a later write there does
not fail for space. `Length::Keep` keeps the length. `Length::Extend` makes the length the end of
the range when the range ends after it. `Driver::punch_hole` frees the blocks of a range, which then
reads as zeros, and keeps the length (`FALLOC_FL_PUNCH_HOLE` with `FALLOC_FL_KEEP_SIZE`). A block
that the range holds only in part keeps its blocks, and the bytes of the range in it become zeros.
A file system without the operation gives `Unsupported` (`EOPNOTSUPP`), as a hole punch on ramfs
does, and a range of 0 bytes gives `InvalidInput`.

On macOS, the calls are `F_PREALLOCATE` and `F_PUNCHHOLE`, with the same results and two limits: a
hole inside the file stays a hole after a preallocation, and an allocation with `Length::Extend`
runs alone on its file. exFAT and FAT32 have no hole punching there. See
[Preallocation](kqueue.md#preallocation) and [Hole punching](kqueue.md#hole-punching).

On Windows, a preallocation is `SetFileInformationByHandle` with `FileAllocationInfo`, and then
`FileEndOfFileInfo` for `Length::Extend`. `FileAllocationInfo` sets the allocation to its value,
and a value below the length cuts the file, so the job reads the allocation and the length first
and never sets less than either. Thus each preallocation runs alone on its file: it starts when
each other operation of the file ended, and each later one waits until it ends. Another process
that writes the file at the same time is outside that order. NTFS frees the clusters after the end
of the file at the last close of the file, so `Length::Keep` reserves them only while the file is
open, and `Length::Extend` keeps them. In a sparse file, a hole can stay a hole after a
preallocation, so a later write into it can still fail for space. A hole punch makes the file sparse (`FSCTL_SET_SPARSE`), and
then `FSCTL_SET_ZERO_DATA` frees the clusters of the range, which then reads as zeros, and keeps the
length, also for a range that ends after the end of the file. A file system without sparse files
gives `Unsupported`.

The tests check both operations on ext4, on APFS, and on NTFS.

## Durability

The end of a write tells only that the kernel has the bytes. Durability is a separate result, as
this table gives:

| Result | Call | Linux | macOS | Windows |
| --- | --- | --- | --- | --- |
| Durable contents | `Driver::sync(&file, SyncMode::Data)` or `SyncMode::All` after the writes, or each write with `WriteMode::DataSync` | `fdatasync`, `fsync`, and `RWF_DSYNC` | `F_FULLFSYNC` for each of them | `FlushFileBuffers` for a sync, or on IoRing a flush entry with `FILE_FLUSH_DEFAULT`, which the SDK defines as `FlushFileBuffers`, and a write through the write-through handle of the file (`FILE_FLAG_WRITE_THROUGH`) for `WriteMode::DataSync` |
| Durable namespace change | `Driver::sync_directory(&directory)` on the parent directory, after a create, a rename, or an unlink | `fsync` of the directory | `F_FULLFSYNC` of the directory | Unsupported: the sync gives `Unsupported` at once |

On macOS, a plain `fsync` leaves the bytes in the cache of the device, so the driver never uses it
for durability. A file system with no `F_FULLFSYNC` gives `Unsupported` (see
[Durability](kqueue.md#durability)).

On Windows, no source states that a flush of a directory makes its creates, renames, and unlinks
durable on NTFS, so `Driver::sync_directory` gives `Unsupported`, and the
[capability report](capability-report.md#routes) gives `Route::Unsupported` for
`Call::SyncDirectory`. A program that needs a durable namespace change on Windows cannot get it
from the driver. The open of a file with writes makes the write-through handle, so that a
`WriteMode::DataSync` write is one overlapped call, or one entry of the ring on IoRing.

A sync covers the writes that ended before it. Start it after their futures are ready.

## Native NVMe on Windows

Windows Server 2025 has a native NVMe path, which sends NVMe commands to the device with no SCSI
translation, so the same reads and writes cost less. `Driver::nvme_path(&file)` tells whether that
path serves the device of a file. It runs as a job of the blocking service, and reads the driver
service of each disk of the volume of the file: `nvmedisk` for the native path. It never infers the
path from the version of Windows.

| Result | Meaning |
| --- | --- |
| `NvmePath::Native` | The native NVMe driver serves each disk of the volume |
| `NvmePath::Other` | Another driver serves a disk of the volume, for example the disk class driver `disk` over the SCSI translation of `stornvme` or of the NVMe driver of a cloud provider, or the volume has no local disk |

## Dropped, forgotten, and cancelled operations

The driver keeps the buffer of each operation until its terminal event, also after a drop or a
forget of its future.

An operation that changes the file system runs to its end when its future drops: a write, a sync,
a change of length or space, a rename, an unlink, and an open with `create`, `create_new`, or
`truncate`. Thus its effect never depends on a race with the drop. The driver keeps the lease of a
write until the kernel ends the write.

The other operations ask for a cancel when their future drops: a read, an open with none of those
options, and a length inspection. The kernel can still run such an operation. The driver releases
the buffer of a read after its end. The driver closes the file that an open gives after a drop of
its future.

`Operation::cancel` asks for a cancel and keeps the future: it gives the result later, as usual.

## Close

`Driver::close_file` takes the file. It waits until each operation of the file ends, also each
operation whose future dropped, and then closes the descriptor. A close makes nothing durable: sync
before it. `Driver::close_directory` closes a directory in the same way.
