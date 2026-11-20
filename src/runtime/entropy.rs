//! The seed of the generator of a runtime: rscrypto's `ChaCha20Rng`, with a seed that the runtime
//! reads from the kernel itself (`getrandom(2)`), so no entropy crate enters the closure.

use std::io;

use rscrypto::ChaCha20Rng;

/// A generator with a seed from the kernel. A failed read leaves no part of the seed.
pub(super) fn generator() -> io::Result<ChaCha20Rng> {
    ChaCha20Rng::try_generate_with(super::sys::random)
}
