# Unstable features

rsasync and `rstypes` build with a dated nightly Rust toolchain, which `rust-toolchain.toml` pins.
This page lists each unstable Rust feature that their public API or a default feature uses, and
the reason for it.

## Rule

A change that enables an unstable feature adds its entry to this list in the same change. The
entry gives the feature name, the crate, the public item or default feature that uses it, and the
reason. A change that removes the last use of a feature removes its entry.

## Features in use

| Feature | Crate | Uses | Reason |
| --- | --- | --- | --- |
| `allocator_ext` | `rstypes`, `rsasync` | `Arc::try_new`, `Arc::try_new_uninit`, and `Box::try_new_uninit`, in `Pool::new`, the channel constructors, `LocalOwner::new`, each spawn, and the blocking service (default features) | A refusal of the allocator returns a typed refusal: `Allocation`. A spawn and a submission give their future, factory, or job back. Stable Rust has no fallible constructor of an `Arc`, so each of these calls would abort the process |
