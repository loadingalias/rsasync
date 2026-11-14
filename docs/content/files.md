# Files on the driver

This page is the reference for files on the io_uring driver, `rsasync::driver::Driver`: open,
positional and vectored reads and writes, the exact read, direct I/O, length and space, durability,
names and directories, and close. It also tells what happens when you drop a future.
[Drive the I/O driver yourself](driver-only.md) tells how to start, turn, and end the driver.

Each file operation is positional, with an explicit offset. So several operations of one file can
run at one time, and the file has no position that they share.

## Open and use a file

This example makes a file, writes to it, makes the bytes durable, reads them, closes the file, and
removes it. `run` turns the driver until a future is ready, as in the docs of `Operation`:

<!-- The same text as the doctest of `rsasync::driver::File`. Edit both. -->
```rust
use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::driver::{Config, Driver, OpenOptions, SyncMode, WriteMode};

let mut driver = Driver::new(Config::new())?;
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
| `mode(u32)` | The mode of a file that the open makes, less the umask |
| `cache(Cache)` | `Cache::Buffered`, through the page cache, or `Cache::Direct` (`O_DIRECT`). See [Direct I/O](#direct-io) |

The descriptor closes on `exec`. Options that no open can take, and a path with a NUL byte, give
`InvalidInput`.

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
makes the open give `Unsupported`.

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

`Driver::allocate` reserves the blocks of a range (`fallocate`), so that a later write there does
not fail for space. `Length::Keep` keeps the length. `Length::Extend` makes the length the end of
the range when the range ends after it. `Driver::punch_hole` frees the blocks of a range, which then
reads as zeros, and keeps the length (`FALLOC_FL_PUNCH_HOLE` with `FALLOC_FL_KEEP_SIZE`). A file
system without the operation gives `Unsupported` (`EOPNOTSUPP`), as a hole punch on ramfs does.
The tests check both operations on ext4.

## Durability

The end of a write tells only that the kernel has the bytes. Durability is a separate result, as
this table gives for Linux:

| Result | Call |
| --- | --- |
| Durable contents | `Driver::sync(&file, SyncMode::Data)` (`fdatasync`) or `SyncMode::All` (`fsync`) after the writes, or each write with `WriteMode::DataSync` (`RWF_DSYNC`) |
| Durable namespace change | `Driver::sync_directory(&directory)` on the parent directory, after a create, a rename, or an unlink |

A sync covers the writes that ended before it. Start it after their futures are ready.

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
