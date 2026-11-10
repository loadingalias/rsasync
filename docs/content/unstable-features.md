# Unstable features

rsasync and `rstypes` build with a dated nightly Rust toolchain, which `rust-toolchain.toml` pins.
This page lists each unstable Rust feature that their public API or a default feature uses, and
the reason for it.

## Rule

A change that enables an unstable feature adds its entry to this list in the same change. The
entry gives the feature name, the crate, the public item or default feature that uses it, and the
reason. A change that removes the last use of a feature removes its entry.

## Features in use

None. rsasync and `rstypes` enable no unstable feature.
