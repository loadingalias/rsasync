//! The parts that the MPSC and MPMC channels share: the positions of a ring with laps, a word on
//! its own cache line, and the table of endpoint places with their waiters.
//!
//! # Places
//!
//! A channel makes a fixed number of places for each kind of endpoint that can be cloned. An
//! endpoint holds one place, and each place has its own waiter (`crate::wait`). One bitset marks
//! the places in use, and a second one marks the places that wait.
//!
//! - A wait registers its waker, and then sets its waiting bit with an `AcqRel` change. A wake
//!   clears the bit with an `Acquire` change before it wakes the waiter. Thus the wake sees the
//!   registration, and the clear of the bit decides which wake owns the waiter.
//! - A wait that ends after a wake cleared its bit, and that does not use the condition, wakes
//!   another place: that wake was for a condition that it does not use.
//! - A close wakes each waiting place with a swap of each waiting word. A wait that sets its bit
//!   after that swap reads it, so the close happens before its recheck.

use alloc::boxed::Box;

use super::Refused;
use crate::memory::items;
use crate::sync::atomic::{AtomicU32, Ordering};
use crate::wait::Waiter;

/// The bits of a position in the words of the claimed positions.
pub(super) const POSITION: u64 = (1 << 62) - 1;
/// In the word of the sent position: the channel accepts no new claim.
pub(super) const CLOSED: u64 = 1 << 62;
/// In the word of the sent position: the receiving side is gone, so no value can be published.
pub(super) const ABORTED: u64 = 1 << 63;
/// The largest capacity, as in the SPSC channel.
pub(super) const MAX_CAPACITY: u32 = 1 << 27;
/// The largest number of places of one kind.
pub(super) const MAX_PLACES: u32 = 1 << 16;

/// The places in each word of a bitset.
const BITS: u32 = u32::BITS;

/// The positions of a ring of `capacity` slots. One lap is the next power of two of the
/// capacity: the bits below `shift` are the index of a slot, and the bits above are the lap. A
/// position wraps only after 2^62 positions.
#[derive(Clone, Copy)]
pub(super) struct Ring {
    pub(super) capacity: u64,
    shift: u32,
}

impl Ring {
    pub(super) fn new(capacity: u32) -> Self {
        Self {
            capacity: u64::from(capacity),
            shift: capacity.next_power_of_two().trailing_zeros(),
        }
    }

    fn index_bits(self) -> u64 {
        (1 << self.shift) - 1
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "the index is below the capacity, which is at most 2^27"
    )]
    pub(super) fn index(self, position: u64) -> usize {
        (position & self.index_bits()) as usize
    }

    /// The lap of `position`.
    pub(super) fn lap(self, position: u64) -> u64 {
        position >> self.shift
    }

    /// The position `count` slots after `position`. The count is at most the capacity.
    pub(super) fn advance(self, position: u64, count: u64) -> u64 {
        // No overflow: the index is below 2^27, and so is the count.
        let index = (position & self.index_bits()) + count;
        if index < self.capacity {
            position + count
        } else {
            let lap = (position & !self.index_bits()).wrapping_add(1 << self.shift);
            (lap + (index - self.capacity)) & POSITION
        }
    }

    /// The number of free slots when the reads are at `head` and the claims end at `tail`. A
    /// `head` that is older than the true one gives fewer free slots, never more.
    pub(super) fn free(self, head: u64, tail: u64) -> u64 {
        let laps = ((tail & !self.index_bits()).wrapping_sub(head & !self.index_bits()) & POSITION) >> self.shift;
        let used = laps
            .saturating_mul(self.capacity)
            .saturating_add(tail & self.index_bits())
            .saturating_sub(head & self.index_bits());
        self.capacity.saturating_sub(used)
    }
}

/// A word on its own cache line, also on targets with lines of 128 bytes.
#[repr(align(128))]
pub(super) struct Line<T>(pub(super) T);

/// The word and the bit of a place in a bitset.
fn place_bit(place: u32) -> (usize, u32) {
    ((place / BITS) as usize, 1 << (place % BITS))
}

/// The first place of a word of a bitset.
#[expect(
    clippy::cast_possible_truncation,
    reason = "a bitset has at most `MAX_PLACES / BITS` words"
)]
fn first_place(word: usize) -> u32 {
    word as u32 * BITS
}

/// The places of one kind of endpoint.
pub(super) struct Places {
    waiters: Box<[Waiter]>,
    /// One bit for each place in use. The bits after the last place are set.
    used: Box<[AtomicU32]>,
    /// One bit for each place that waits.
    waiting: Box<[AtomicU32]>,
    /// The number of places in use.
    live: AtomicU32,
    /// The number of waiting bits that are set, or that a wait sets now.
    waits: AtomicU32,
    /// The place from which the next wake starts.
    next: AtomicU32,
}

impl Places {
    /// The memory of `count` places, outside the `Places` value itself.
    pub(super) fn size(count: u32) -> Option<usize> {
        let words = count.div_ceil(BITS) as usize;
        size_of::<Waiter>()
            .checked_mul(count as usize)?
            .checked_add(size_of::<AtomicU32>().checked_mul(2 * words)?)
    }

    /// Makes `count` places, with place 0 in use.
    pub(super) fn new(count: u32) -> Result<Self, Refused> {
        let words = count.div_ceil(BITS);
        // The bits after the last place are set, so no endpoint takes them.
        let used = items(words, |index| {
            let first = index * BITS;
            let unused = if first + BITS > count {
                u32::MAX << (count - first)
            } else {
                0
            };
            AtomicU32::new(unused | u32::from(index == 0))
        });
        let refused = |_| Refused::Allocation;
        Ok(Self {
            waiters: items(count, |_| Waiter::new()).map_err(refused)?,
            used: used.map_err(refused)?,
            waiting: items(words, |_| AtomicU32::new(0)).map_err(refused)?,
            live: AtomicU32::new(1),
            waits: AtomicU32::new(0),
            next: AtomicU32::new(0),
        })
    }

    pub(super) fn waiter(&self, place: u32) -> &Waiter {
        &self.waiters[place as usize]
    }

    /// Takes a free place for a new endpoint.
    pub(super) fn take(&self) -> Option<u32> {
        for (index, word) in self.used.iter().enumerate() {
            // Ordering: `Relaxed`: the exchange below checks the value.
            let mut bits = word.load(Ordering::Relaxed);
            while bits != u32::MAX {
                let bit = (!bits).isolate_lowest_one();
                // Ordering: `Acquire` reads the release of the last endpoint of the place, so its
                // use of the waiter happens before the use of the new endpoint.
                match word.compare_exchange_weak(bits, bits | bit, Ordering::Acquire, Ordering::Relaxed) {
                    Ok(_) => {
                        // Ordering: `Relaxed`, as for the count of an `Arc`: the endpoint that
                        // clones is alive, so the count is not zero.
                        self.live.fetch_add(1, Ordering::Relaxed);
                        return Some(first_place(index) + bit.trailing_zeros());
                    }
                    Err(actual) => bits = actual,
                }
            }
        }
        None
    }

    /// Frees the place of an endpoint that drops. Gives `true` for the last endpoint.
    pub(super) fn release(&self, place: u32) -> bool {
        let (word, bit) = place_bit(place);
        // Ordering: `Release` orders the use of the waiter before the next endpoint of the place.
        self.used[word].fetch_and(!bit, Ordering::Release);
        // Ordering: `AcqRel`, as for the count of an `Arc`.
        self.live.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// Marks `place` as waiting. Call it after the registration of the waker, and before the
    /// recheck of the condition.
    pub(super) fn wait(&self, place: u32) {
        let (word, bit) = place_bit(place);
        // Ordering: `Relaxed`: the count only tells whether a wake can find a bit.
        self.waits.fetch_add(1, Ordering::Relaxed);
        // Ordering: `Release` orders the registration before the wake that clears the bit.
        // `Acquire` reads the release of a close that cleared the word before (module docs).
        if self.waiting[word].fetch_or(bit, Ordering::AcqRel) & bit != 0 {
            self.waits.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Ends the interest of a wait of `place`. A wait that ends with no use of its condition, after
    /// a wake cleared its bit, wakes another place.
    pub(super) fn unwait(&self, place: u32, cancelled: bool) {
        let (word, bit) = place_bit(place);
        let waiter = self.waiter(place);
        // Ordering: `Relaxed`: only this endpoint sets the bit, and the change publishes no data.
        if self.waiting[word].fetch_and(!bit, Ordering::Relaxed) & bit != 0 {
            self.waits.fetch_sub(1, Ordering::Relaxed);
            waiter.cancel();
        } else if !waiter.cancel() && cancelled {
            self.wake_one();
        }
    }

    /// Whether a place waits, or starts to wait now.
    pub(super) fn waiting(&self) -> bool {
        // Ordering: `Relaxed`: the caller orders this load after its change of the condition with
        // a read-modify-write that the waits also change (the users' docs).
        self.waits.load(Ordering::Relaxed) != 0
    }

    /// Wakes one waiting place that has a registered waker, in rotating order. Gives the place.
    pub(super) fn wake_one(&self) -> Option<u32> {
        if !self.waiting() {
            return None;
        }
        let words = self.waiting.len();
        // Ordering: `Relaxed`: the place only selects where the search starts.
        let from = self.next.load(Ordering::Relaxed);
        let first = place_bit(from).0 % words;
        let from_bit = from % BITS;
        // The first word two times: its bits from `from` on, and at the end its bits before.
        for step in 0..=words {
            let index = (first + step) % words;
            let word = &self.waiting[index];
            // Ordering: `Relaxed`: the `Acquire` change below checks each bit.
            let mut bits = word.load(Ordering::Relaxed);
            if step == 0 {
                bits &= u32::MAX << from_bit;
            } else if step == words {
                bits &= !(u32::MAX << from_bit);
            }
            while bits != 0 {
                let bit = bits.isolate_lowest_one();
                bits &= !bit;
                // Ordering: `Acquire` reads the release of the wait, so its waker is registered.
                if word.fetch_and(!bit, Ordering::Acquire) & bit != 0 {
                    self.waits.fetch_sub(1, Ordering::Relaxed);
                    let place = first_place(index) + bit.trailing_zeros();
                    if self.waiter(place).wake() {
                        self.next.store(place + 1, Ordering::Relaxed);
                        return Some(place);
                    }
                }
            }
        }
        None
    }

    /// Wakes each waiting place, after a close.
    pub(super) fn wake_all(&self) {
        for (index, word) in self.waiting.iter().enumerate() {
            // Ordering: `Acquire` reads the release of each wait, so its waker is registered.
            // `Release` orders the close before the recheck of a wait that sets its bit after this
            // change.
            let mut bits = word.swap(0, Ordering::AcqRel);
            self.waits.fetch_sub(bits.count_ones(), Ordering::Relaxed);
            while bits != 0 {
                let bit = bits.isolate_lowest_one();
                bits &= !bit;
                self.waiter(first_place(index) + bit.trailing_zeros()).wake();
            }
        }
    }
}
