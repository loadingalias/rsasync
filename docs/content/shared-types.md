# Shared types

rsasync shares three groups of types with networking libraries and with applications:

| Module | Contents |
| --- | --- |
| `rsasync::buffer` | Owned buffers, and the leases and pool buffers that share their backing |
| `rsasync::capacity` | Capacity permits, and the caller-owned budget that contains them |
| `rsasync::time` | Clock domains, instants and deadlines, and physical-time readings |

rsasync is in development and has no release. These modules have no items yet.

## Use the types through `rsasync`

```rust
use rsasync::{buffer, capacity, time};
```

The modules come from the `rstypes` crate, and `rsasync` re-exports them. Do not add `rstypes` to
your dependencies, and do not name it in your code. A library that uses these types in its public
API re-exports them, so you use them through that library or through `rsasync`. Each path gives
the same type, so a value goes from a library to the runtime with no conversion.

## Why the types are in a separate crate

- A library, for example a protocol implementation, can use these types on a different runtime.
  It depends on `rstypes`, not on rsasync.
- `rstypes` has its own version. It changes only when a shared type changes, so different releases
  of rsasync and of a library can use the same types.

## Build without `std`

`rstypes` has a `std` feature, which is on by default. Without it, the crate needs only `core` and
`alloc`. `rsasync` needs `std`, and it turns on this feature. To build `rstypes` without `std`, run
this command from the repository root:

```sh
cargo build --locked -p rstypes --no-default-features
```
