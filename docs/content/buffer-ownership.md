# Buffer ownership

An rsasync I/O operation takes each buffer by value and gives it back when the operation ends. This
page tells who owns the bytes before, during, and after an operation, and which buffers obey the
contract. The types are in `rsasync::buffer`. rsasync is in development, and its alpha releases
are on crates.io. Today the Linux driver (`rsasync::driver`) uses them: its TCP receive takes a
`Backing`, and its send takes a `Lease`.

## Who owns the bytes

| When | Owner of the bytes | What the caller can do |
| --- | --- | --- |
| Before the operation | The caller | Read and change the buffer |
| During the operation | The driver, and the kernel through the driver | Nothing: the buffer moved into the operation |
| After the operation | The caller again | Read the initialized bytes, then use the buffer again or drop it |

The driver keeps the buffer until the backend releases it. If the caller drops or forgets the
future of the operation, the driver still owns the buffer. It frees the buffer only after the
backend releases it. Thus no dropped future frees memory that the kernel still uses, and no
deadline releases the buffer before the backend does.

## Buffers that I/O accepts

A buffer obeys an unsafe trait: its address stays the same when the buffer value moves. A send
reads the initialized bytes of a `Buffer`. A receive writes into the capacity of a `BufferMut`
after its initialized bytes.

| Value | `Buffer` (send) | `BufferMut` (receive) |
| --- | --- | --- |
| `Vec<u8>` | Yes | Yes |
| `&'static [u8]` | Yes | No: its bytes are shared, so nothing can write them |
| `Backing`, also a pool slot | Yes | Yes |
| `Lease` | Yes | No: its bytes do not change |
| `Box<[u8]>` | No: use `Backing::from(boxed)`, which keeps the same bytes | No |
| An inline array, for example `[u8; 4096]` | No: its bytes move when the array moves | No |

A pointer from a `Box` is not valid after the `Box` moves (the
[`Box` documentation](https://doc.rust-lang.org/std/boxed/index.html#considerations-for-unsafe-code)).
Thus `Box<[u8]>` is not a buffer itself. `Backing::from` makes it a `Vec<u8>` and then a backing,
with no copy and no allocation.

A `Backing` holds a writable region from any source in one type: a `Vec<u8>`, a `Box<[u8]>`, or
memory that a library allocates. When the backing drops, it calls its one release hook, on the
thread that drops it.

## The four facts of a buffer

A buffer gives four separate facts. Do not use one in place of another.

| Fact | Method | Meaning |
| --- | --- | --- |
| Capacity | `BufferMut::capacity` | The number of bytes that the buffer can hold |
| Initialized length | `Buffer::initialized` | The number of bytes from the start that are initialized. `Buffer::bytes` gives them |
| Allocation size | `Buffer::allocation_size` | The size of the allocation that the release of the buffer frees or returns. It can be larger than the capacity. A static region gives zero |
| Alignment | `Buffer::alignment` | The largest power of two that divides the address of the first byte. Compare it with the alignment that an operation needs, for example for direct I/O |

## A receive fills only the capacity after the initialized bytes

A receive holds a `WriteLoan`, an exclusive loan over the capacity after the initialized bytes.
The loan owns the buffer, so no other code reads or writes the region while the backend writes.
When the backend gives the region back, the loan records the exact number of bytes that the
backend filled. Only those bytes become readable:

- A short receive adds only the bytes that it filled.
- A receive that fails adds no bytes. The buffer keeps only its earlier initialized bytes.
- The bytes that were initialized before the receive stay the same.

To receive into the full capacity again, first call `BufferMut::truncate(0)` (for a `Vec<u8>`,
`Vec::clear` does the same).

In this example, `WriteLoan::fill_from` does the work of the backend. It copies bytes into the
loan. A driver uses it to give data that it holds already.

```rust
use rsasync::buffer::WriteLoan;

// Before the receive, the caller owns the buffer.
let mut buf = Vec::with_capacity(64);
buf.extend_from_slice(b"GET ");

// During the receive, the loan owns the buffer. The backend writes into the capacity after the
// initialized bytes. Here, a copy does the work of the backend.
let mut loan = WriteLoan::new(buf);
assert_eq!(loan.fill_from(b"/index.html"), 11);
assert_eq!(loan.filled(), b"/index.html");

// After the receive, the caller owns the buffer again, with exactly the filled bytes.
let buf = loan.into_inner();
assert_eq!(buf, b"GET /index.html");
```

Code that writes into uninitialized memory, for example a cipher, fills a loan with
`WriteLoan::fill_with` and no unsafe code. The loan lends its capacity after the filled bytes to
a closure, as `&mut [MaybeUninit<u8>]`. The closure gives back the `&mut [u8]` that it
initialized:

- The loan records the slice only when it starts at the first byte that the closure got. Then it
  gives the number of bytes that it recorded.
- For each other slice, the loan records no byte and gives 0. Compare the count with the number
  of bytes that you expect.
- Each fill starts after the bytes that the loan filled before. Thus a header, a body, and a tag
  go in three fills.
- To give out an error or a value, for example a tag, set a variable that the closure captures.
  On an error, give back an empty slice, for example with `unwrap_or_default()`.

```rust
use rsasync::buffer::WriteLoan;

let mut loan = WriteLoan::new(Vec::with_capacity(64));
loan.fill_from(b"HDR|");

// The closure initializes the first 4 spare bytes, here with a copy, and gives them back.
let count = loan.fill_with(|spare| spare[..4].write_copy_of_slice(b"BODY"));
assert_eq!(count, 4);
assert_eq!(loan.filled(), b"HDR|BODY");
```

A backend writes through `WriteLoan::as_mut_ptr`, for example from the kernel, and then records
its extent with the unsafe `WriteLoan::set_filled`.

## Share bytes with leases and views

A `Lease` freezes the initialized bytes of a buffer, for example after a receive and a change in
place, such as a decryption. Make it with `Lease::from` from a `Backing`, a `Vec<u8>`, or a
`&'static [u8]`, or with `Lease::from_owner` from a different owner (see
[Lease the bytes of a different owner](#lease-the-bytes-of-a-different-owner)). It is the one way to
give the same bytes to more than one owner:

- `Lease::view` gives a lease of a sub-range, for example one record, with no copy. It gives
  `None` if the range is not in the lease.
- A clone gives one more lease of the same bytes.
- Each lease and view keeps the backing alive, also after the first lease, the buffer, and the
  operation drop. The bytes do not change, and their address stays the same.
- Leases and views can move to other threads and drop there. The last one that drops releases the
  backing through its hook, one time, on its thread.

A lease made from a backing costs one allocation, for its reference count. Views and clones cost
no allocation. A lease of a pool slot costs no allocation, because the slot counts the leases (see
[Receive into pool slots](#receive-into-pool-slots)). A lease of a static region has no reference
count.

`Backing::lease_layout()` gives the layout of that one allocation before you make it, or `None`
for a pool slot and for a part of a split, which make none. To charge it to a budget, reserve a
`Permit` of its size first, and keep the permit where it drops with the backing: in the data of
the release hook of your backing (see [Put your own memory in a backing](#put-your-own-memory-in-a-backing)).
A `Vec<u8>` or a `Box<[u8]>` has no such place, so lease it with `Lease::from_owner` and an owner
that holds the charge (see [Lease the bytes of a different owner](#lease-the-bytes-of-a-different-owner)).

A small view keeps the whole allocation. Its `Buffer::allocation_size` is that of the backing, not
its own length. Thus a view of 4 bytes can keep a receive buffer of 64 KiB. To free a large
allocation early, copy the small part into a new buffer and drop the view.

```rust
use rsasync::buffer::{Backing, Buffer, BufferMut, Lease};

// A received record, changed in place, as a decryption does.
let mut received = Vec::with_capacity(4096);
received.extend_from_slice(b"HEAD:BODY");
let mut record = Backing::from(received);
record.bytes_mut().make_ascii_lowercase();

// Freeze the record, then give its parts to two owners, with no copy.
let lease = Lease::from(record);
let head = lease.view(..4).unwrap();
let body = lease.view(5..).unwrap();
drop(lease);
assert_eq!(head.bytes(), b"head");

// The view of 4 bytes keeps the whole allocation of 4096 bytes.
assert!(body.allocation_size() >= 4096);

// A view can move to a different thread. The last view that drops releases the backing.
let owner = std::thread::spawn(move || body.bytes() == b"body");
assert!(owner.join().unwrap());
drop(head);
```

## Freeze a prefix and keep receiving

`Backing::freeze_prefix(len)` freezes the first `len` initialized bytes of a backing into a
`Lease`, and gives the rest of its region as a writable `Backing`. Use it when the protocol is done
with the start of a region, but the next receive continues in the same region. For example, a
stream gives one complete record to its parser while the rest of the next record arrives.

- The lease and its views own the prefix. The rest owns the bytes after it: its initialized bytes
  first, and then the capacity after them.
- A receive fills the rest. A later `freeze_prefix` or `Lease::from` freezes the next part. Each
  part keeps its address in the one allocation.
- The allocation is released one time, when the last view and the last rest drop, on any thread.
  Each part reports the allocation size of the whole.
- A pool slot splits with no allocation. Another backing allocates one reference count at its
  first split, and each later split of its rest shares it.
- When `len` is more than the initialized bytes, `freeze_prefix` gives the backing back with no
  change.

```rust
use rsasync::buffer::{Backing, Buffer, Lease, WriteLoan};

// A receive filled one record and the start of the next one.
let mut loan = WriteLoan::new(Backing::from(Vec::with_capacity(64)));
loan.fill_from(b"RECORD|NE");
let (record, rest) = loan.into_inner().freeze_prefix(7).unwrap();
assert_eq!(record.bytes(), b"RECORD|");

// The next receive fills the rest of the same allocation while the record is shared.
let mut loan = WriteLoan::new(rest);
loan.fill_from(b"XT");
let next = Lease::from(loan.into_inner());
assert_eq!(next.bytes(), b"NEXT");
assert_eq!(next.as_ptr(), record.as_ptr().wrapping_add(7));
```

## Receive into pool slots

A `Pool` holds slots of one size and alignment, and gives each free slot as a `Backing`. Use it on
a path that receives often: an acquire allocates nothing, and a slot comes back for the next
receive.

- `Pool::new(&budget, slots, layout)` charges each allocation of the pool to the budget before it
  allocates. The slots take the slot count times the rounded slot size, which is the slot size
  rounded up to its alignment. Each slot also has an entry of 16 bytes, and the pool has one shared
  state. Idle slots stay charged. The charge ends when the memory is freed, after the pool and each
  of its slots drop. It refuses with a `PoolRefused` value: `Layout` for a slot size of zero or a
  size that is too large, `Budget` when the budget has too few free units, and `Allocation` when
  the allocator gives no memory for one of its allocations. A refusal keeps no charge and no
  memory.
- `Pool::new_in(&budget, slots, layout, allocator)` makes the same pool with all of its slots in one
  block from `allocator`, an `Allocator + Send + 'static`. Use it when the slots need a special
  kind of memory, for example a mapping that a child process does not inherit. The pool gives the
  block back to `allocator` one time, after the pool and each slot drop, on the thread of the last
  drop. The pool keeps the allocator in a box, and the charge includes the size of the allocator:
  a zero-sized allocator needs no allocation. The charge counts the layout of the slots, not a
  larger block that the allocator gives. A refusal of `allocator` gives `Allocation`.
- `Pool::acquire` gives a free slot, or refuses with `PoolEmpty`. It allocates nothing. The backing
  has the capacity of the slot size and the allocation size of the rounded slot size, and none of
  its bytes are initialized.
- The slot is exclusive: only its backing writes it. A `Lease` of the backing counts its views in
  the slot, so the lease and its views allocate nothing.
- The last backing, lease, or view of a slot returns the slot, on any thread, with no allocation
  and no lock, so the drop never blocks or fails. A slot never waits for an epoch or a reclaimer.
- The owner of the pool is the home of its slots: only it acquires, through `&mut Pool`. A slot
  that drops, on any thread, goes into the return ring of the pool, which has one cell for each
  slot. Each acquire takes the returned slots and gives the slot that came back last first, so a
  busy pool reuses slots whose memory is still in the cache.
- A backing from `acquire`, and a lease of it, count nothing in the slot until their first clone
  or view. So their drop returns the slot with no read of the count, and only a lease with views
  pays for the count.

```rust
use core::alloc::Layout;
use core::num::{NonZeroU32, NonZeroU64};

use rsasync::buffer::{Buffer, BufferMut, Lease, Pool, PoolEmpty, WriteLoan};
use rsasync::capacity::Budget;

let budget = Budget::new(NonZeroU64::new(1024 * 1024).unwrap());
// 2 slots of 4000 bytes, aligned to 4096 bytes: the pool charges 2 rounded slots, and its
// entries and shared state.
let slot = Layout::from_size_align(4000, 4096).unwrap();
let mut pool = Pool::new(&budget, NonZeroU32::new(2).unwrap(), slot).unwrap();
assert!(budget.available() < 1024 * 1024 - 2 * 4096);

// A receive fills a slot. Its lease and views need no allocation.
let mut loan = WriteLoan::new(pool.acquire().unwrap());
loan.fill_from(b"HEAD:BODY");
let lease = Lease::from(loan.into_inner());
let body = lease.view(5..).unwrap();
drop(lease);

// The view keeps its slot, so one slot is free.
let other = pool.acquire().unwrap();
assert_eq!(pool.acquire().unwrap_err(), PoolEmpty);

// A view can drop on a different thread. Then its slot returns to the pool.
std::thread::spawn(move || assert_eq!(body.bytes(), b"BODY")).join().unwrap();
let again = pool.acquire().unwrap();
assert_eq!((again.capacity(), again.allocation_size()), (4000, 4096));
```

## Put your own memory in a backing

A library that allocates its own memory, for example memory that it locks or clears, makes a
`Backing` with `Backing::from_raw_parts`. The library gives the region, the initialized length,
the allocation size, and a `Release` hook. The backing calls the hook one time, with the pointer
and the capacity of the region, when it drops. The hook can free the region, return it to a pool,
or clear it first.

The call is unsafe. The library makes sure of these conditions:

- The region is valid for reads and writes of its capacity, in one allocation, until the hook
  runs. The given initialized length is correct.
- Until the hook runs, no other code reads, writes, or frees the region.
- Each thread can use the region and call the hook. A backing can drop on any thread, so the hook
  must not block.

This example puts one page, aligned to the page, in a backing that can receive 1000 bytes:

```rust
use std::alloc::{Allocator, Global, Layout, handle_alloc_error};
use std::ptr::{self, NonNull};

use rsasync::buffer::{Backing, Buffer, BufferMut, Release};

/// One page, aligned to the page.
#[repr(C, align(4096))]
struct Page([u8; 4096]);
const PAGE: Layout = Layout::new::<Page>();

/// The release hook: frees the page. A library can also clear or unlock its memory here.
///
/// # Safety
///
/// `ptr` is from `page`, and nothing uses it after this call.
unsafe fn free_page(_: *const (), ptr: NonNull<u8>, _: usize) {
    // SAFETY: `page` allocated `ptr` from `Global` with `PAGE`.
    unsafe { Global.deallocate(ptr, PAGE) };
}

/// A backing of 1000 writable bytes in one page.
fn page() -> Backing {
    let Ok(page) = Global.allocate(PAGE) else { handle_alloc_error(PAGE) };
    // SAFETY: the page is valid for reads and writes of 4096 bytes. Only the backing uses
    // it, and `free_page` frees it on any thread.
    unsafe { Backing::from_raw_parts(page.cast(), 1000, 0, PAGE.size(), Release::new(ptr::null(), free_page)) }
}

let buf = page();
assert_eq!((buf.capacity(), buf.allocation_size()), (1000, 4096));
assert!(buf.alignment() >= 4096);
drop(buf); // calls `free_page` one time
```

The capacity (1000), the allocation size (4096), and the alignment (at least 4096) are separate
facts of this backing.

To share the bytes of your memory, receive into the backing or change its bytes with
`BufferMut::bytes_mut`, then make a lease with `Lease::from(backing)`. The hook then runs when the
last lease or view drops. Your own rules for the memory, for example when other code can read it,
stay in your code: the backing and the lease have no field for them.

## Lease the bytes of a different owner

`Lease::from_owner(owner, allocation_size)` lends the bytes of a value that owns them, with no copy.
The owner gives its bytes through `AsRef<[u8]>`, and it is `Send` and `Sync`: for example a
`String`, an `Arc<[u8]>`, a `bytes::Bytes`, or a type of your library that owns a memory map. The
last lease or view that drops drops the owner one time, on its thread. Thus the drop of the owner is
the one release of its bytes.

This is also the path for memory that your library can only read, for example a read-only memory
map. A `Backing` needs memory that it can write, so give the type that owns the map instead. Its
`Drop` unmaps the memory.

The owner does not tell the size of the allocation that it keeps, so the caller gives it as
`allocation_size`, and the leases report it. Give the true size. A smaller value makes a budget
charge too little for the memory that the lease keeps. The length of the bytes is the minimum.

The conversion costs one allocation: the reference count, with the owner in it.
`Lease::owner_layout::<T>()` gives its layout for an owner of type `T`. To charge each byte that the
lease keeps, reserve a `Permit` for the bytes of the owner and the size of that layout first, and
keep the permit in the owner. The last lease or view then drops the owner, the permit, and the
allocation at one time:

```rust
use core::num::NonZeroU64;

use rsasync::buffer::Lease;
use rsasync::capacity::{Budget, Permit};

struct Charged {
    bytes: Vec<u8>,
    _charge: Permit,
}

impl AsRef<[u8]> for Charged {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
let response = b"HTTP/1.1 200 OK\r\n\r\n";
// The charge comes before each allocation: the bytes, and then the reference count.
let charged = response.len() as u64 + Lease::owner_layout::<Charged>().size() as u64;
let charge = budget.reserve(charged)?;
let mut bytes = Vec::new();
bytes.try_reserve_exact(response.len())?;
bytes.extend_from_slice(response);
let size = bytes.capacity();
let lease = Lease::from_owner(Charged { bytes, _charge: charge }, size);
assert_eq!(budget.available(), budget.bound() - charged);
drop(lease);
assert_eq!(budget.available(), budget.bound());
# Ok::<(), Box<dyn std::error::Error>>(())
```

```rust
use rsasync::buffer::{Buffer, Lease};

let mut text = String::with_capacity(64);
text.push_str("key=value");
let (address, capacity) = (text.as_ptr(), text.capacity());

let lease = Lease::from_owner(text, capacity);
let value = lease.view(4..).unwrap();
assert_eq!(value.bytes(), b"value");
assert_eq!(value.as_ptr(), address.wrapping_add(4)); // no copy
assert_eq!(value.allocation_size(), capacity);
```

## Convert to and from the `bytes` crate

`rstypes` does not depend on the `bytes` crate. These conversions use `AsRef<[u8]>` and
`Bytes::from_owner`, which `bytes` 1.9 and later has. Each conversion that exists keeps the address of
the bytes:

| From | To | Exists | How, or why not |
| --- | --- | --- | --- |
| `Lease` or `Backing` | `bytes::Bytes` | Yes, no copy, one allocation | `Bytes::from_owner(lease)`. The last `Bytes` that drops drops the lease, and the backing is then released as for each lease |
| `bytes::Bytes` | `Lease` | Yes, no copy, one allocation | `Lease::from_owner(bytes, allocation_size)`. A `Bytes` does not tell the size of the allocation that it keeps, so give it |
| `Lease` or `Backing` | `bytes::BytesMut` | No, it would copy | A `BytesMut` must own an allocation that it can grow and free as a `Vec<u8>`. Other memory cannot become one |
| `bytes::Bytes` | `Backing` | No | A `Bytes` is shared and its bytes cannot change. A `Backing` needs memory that only it can write |
| `bytes::BytesMut` | `Backing`, or a `BufferMut` | No | `BytesMut` gives no pointer that is valid for its full capacity, and it does not say that its bytes keep their address when it moves |

## Check the examples and the unsafe code

The six examples on this page are doctests of `rsasync::buffer`. From the repository root,
`cargo xtask test` runs them with the other tests.

Miri runs the tests of the buffer code and stops at undefined behavior, for example a read of
uninitialized bytes or a release that frees memory two times. `rust-toolchain.toml` includes the
`miri` component. To run the tests with and without `std`:

```sh
rustup component add miri   # when rustup did not install it with the toolchain
cargo miri test --locked -p rstypes
cargo miri test --locked -p rstypes --no-default-features
```
