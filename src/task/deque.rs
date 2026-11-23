//! The run queue of a movable worker: a fixed ring of slot indices. The worker pushes at the tail
//! and pops at the head, first in, first out. An idle worker steals the older half from the head.
//!
//! - `tail` is the index after the last entry. Only the owner changes it, and it publishes each
//!   entry with `Release`.
//! - `head` holds two indices in one word: `real`, the next entry to take, and `steal`, the first
//!   entry that a steal still copies. They are equal when no steal runs. A steal claims the
//!   entries from `real` with one compare-exchange that moves only `real`. It copies them, and
//!   then a second compare-exchange moves `steal` up to `real`. While the two differ, no other
//!   steal starts, and the owner still pops: its pop moves `real` alone.
//! - The room counts from `steal`, so the owner never writes a place that a steal still copies.
//!   The owner loads `head` with `Acquire`, which reads the `Release` of the end of the steal, so
//!   each copy ends before the owner writes its place again.
//!
//! Each change of `head` is one read-modify-write, so the owner and the thieves agree on one order
//! of the claims: each entry goes to one taker. The entries are atomic words, so the queue has no
//! unsafe code. The indices wrap, and only their differences count.

use alloc::boxed::Box;
use core::alloc::AllocError;

use crate::memory::items;
use crate::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// The entries of a queue.
pub(super) const CAPACITY: u32 = 256;

/// The bytes of a queue that its owner charges.
pub(super) const SIZE: usize = size_of::<Deque>() + CAPACITY as usize * size_of::<AtomicU32>();

pub(super) struct Deque {
    /// `steal` in the high half and `real` in the low half.
    head: AtomicU64,
    tail: AtomicU32,
    entries: Box<[AtomicU32]>,
}

fn unpack(head: u64) -> (u32, u32) {
    #[expect(clippy::cast_possible_truncation, reason = "each half of the word is one index")]
    ((head >> 32) as u32, head as u32)
}

fn pack(steal: u32, real: u32) -> u64 {
    (u64::from(steal) << 32) | u64::from(real)
}

impl Deque {
    pub(super) fn new() -> Result<Self, AllocError> {
        Ok(Self {
            head: AtomicU64::new(0),
            tail: AtomicU32::new(0),
            entries: items(CAPACITY, |_| AtomicU32::new(0))?,
        })
    }

    fn place(&self, index: u32) -> &AtomicU32 {
        &self.entries[(index % CAPACITY) as usize]
    }

    /// Pushes `slot` at the tail. Only the owner calls it. A full queue gives `slot` back.
    pub(super) fn push(&self, slot: u32) -> Result<(), u32> {
        // Ordering: `Relaxed`: only this thread changes the tail.
        let tail = self.tail.load(Ordering::Relaxed);
        // Ordering: `Acquire` reads the end of each steal, so its copy ended.
        let (steal, _) = unpack(self.head.load(Ordering::Acquire));
        if tail.wrapping_sub(steal) >= CAPACITY {
            return Err(slot);
        }
        // Ordering: `Relaxed`: the store of the tail publishes it.
        self.place(tail).store(slot, Ordering::Relaxed);
        // Ordering: `Release` publishes the entry to a thief, which loads the tail with `Acquire`.
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Takes the oldest entry. Only the owner calls it.
    pub(super) fn pop(&self) -> Option<u32> {
        // Ordering: `Acquire`, as in `push`.
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let (steal, real) = unpack(head);
            // Ordering: `Relaxed`: only this thread changes the tail.
            if real == self.tail.load(Ordering::Relaxed) {
                return None;
            }
            let next = real.wrapping_add(1);
            let changed = if steal == real {
                pack(next, next)
            } else {
                pack(steal, next)
            };
            // Ordering: `AcqRel`: the claim is in the one order of `head`, and it reads each steal.
            match self
                .head
                .compare_exchange_weak(head, changed, Ordering::AcqRel, Ordering::Acquire)
            {
                // Ordering: `Relaxed`: this thread wrote the entry.
                Ok(_) => return Some(self.place(real).load(Ordering::Relaxed)),
                Err(actual) => head = actual,
            }
        }
    }

    /// The entries in the queue, at one time. A thief and an idle worker use it as a hint.
    pub(super) fn len(&self) -> u32 {
        // Ordering: `Relaxed`: the count is a hint, and each take reads `head` again.
        let (_, real) = unpack(self.head.load(Ordering::Relaxed));
        self.tail.load(Ordering::Relaxed).wrapping_sub(real)
    }

    /// Steals the older half of this queue, rounded up: gives the oldest entry, for the thief to
    /// poll, and pushes the others to `into`, the queue of the thief, in their order. Only the owner
    /// of `into` calls it. Gives `None` if the queue is empty, another steal of it runs, or `into`
    /// has no room for half of a queue.
    pub(super) fn steal_into(&self, into: &Self) -> Option<u32> {
        // Ordering: `Relaxed`: only this thread changes the tail of `into`.
        let tail_into = into.tail.load(Ordering::Relaxed);
        // Ordering: `Acquire`, as in `push`.
        let (steal_into, _) = unpack(into.head.load(Ordering::Acquire));
        // A thief of `into` that has not ended its copy keeps the room of its entries, also when
        // `into` looks empty to its owner.
        if tail_into.wrapping_sub(steal_into) > CAPACITY / 2 {
            return None;
        }
        // Ordering: `Acquire` reads the end of the last steal.
        let mut head = self.head.load(Ordering::Acquire);
        let (real, count) = loop {
            let (steal, real) = unpack(head);
            if steal != real {
                return None;
            }
            // Ordering: `Acquire` reads the `Release` of each push, so the copy sees its entry.
            let queued = self.tail.load(Ordering::Acquire).wrapping_sub(real);
            let count = queued - queued / 2;
            if count == 0 {
                return None;
            }
            // Ordering: `AcqRel`: the claim is in the one order of `head`.
            match self.head.compare_exchange_weak(
                head,
                pack(steal, real.wrapping_add(count)),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break (real, count),
                Err(actual) => head = actual,
            }
        };
        // Ordering: `Relaxed`: the `Acquire` of the tail above read the push of each entry.
        let first = self.place(real).load(Ordering::Relaxed);
        for index in 1..count {
            let slot = self.place(real.wrapping_add(index)).load(Ordering::Relaxed);
            // Ordering: `Relaxed`: the store of the tail publishes it.
            into.place(tail_into.wrapping_add(index - 1))
                .store(slot, Ordering::Relaxed);
        }
        // The copy ended: `steal` moves up to `real`, which the owner can have moved meanwhile.
        // Ordering: `Relaxed`: the exchange below reads the latest word.
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            let (_, now) = unpack(head);
            // Ordering: `Release` orders the copy before the next write of the owner to a place
            // of the copy, which loads `head` with `Acquire`.
            match self
                .head
                .compare_exchange_weak(head, pack(now, now), Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => head = actual,
            }
        }
        if count > 1 {
            // Ordering: `Release`, as in `push`.
            into.tail.store(tail_into.wrapping_add(count - 1), Ordering::Release);
        }
        Some(first)
    }
}

#[cfg(all(test, loom))]
mod explore;
