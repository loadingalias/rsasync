# Buffer ownership

An rsasync I/O operation takes each buffer by value and gives it back when the operation ends. This
page tells who owns the bytes before, during, and after an operation, and which buffers obey the
contract. The types are in `rsasync::buffer`. rsasync is in development and has no release. It
has the buffer types, but no I/O operation yet.

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
| `Backing` | Yes | Yes |
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

To receive into the full capacity again, first clear the buffer, for example with `Vec::clear`.

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

A backend writes through `WriteLoan::as_mut_ptr` and then records its extent with the unsafe
`WriteLoan::set_filled`.

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
use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
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
    // SAFETY: `page` allocated `ptr` with `PAGE`.
    unsafe { dealloc(ptr.as_ptr(), PAGE) };
}

/// A backing of 1000 writable bytes in one page.
fn page() -> Backing {
    // SAFETY: the size of `PAGE` is not zero.
    let Some(ptr) = NonNull::new(unsafe { alloc(PAGE) }) else { handle_alloc_error(PAGE) };
    // SAFETY: the page is valid for reads and writes of 4096 bytes. Only the backing uses
    // it, and `free_page` frees it on any thread.
    unsafe { Backing::from_raw_parts(ptr, 1000, 0, PAGE.size(), Release::new(ptr::null(), free_page)) }
}

let buf = page();
assert_eq!((buf.capacity(), buf.allocation_size()), (1000, 4096));
assert!(buf.alignment() >= 4096);
drop(buf); // calls `free_page` one time
```

The capacity (1000), the allocation size (4096), and the alignment (at least 4096) are separate
facts of this backing.

## Check the examples and the unsafe code

The two examples on this page are the doctests of `rsasync::buffer`. From the repository root,
`cargo xtask test` runs them with the other tests.

Miri runs the tests of the buffer code and stops at undefined behavior, for example a read of
uninitialized bytes or a release that frees memory two times. `rust-toolchain.toml` includes the
`miri` component. To run the tests with and without `std`:

```sh
rustup component add miri   # when rustup did not install it with the toolchain
cargo miri test --locked -p rstypes
cargo miri test --locked -p rstypes --no-default-features
```
