//! The wheel of a timer storage: the lists of its timers, which only the home changes.
//!
//! The wheel has six levels of 64 slots. A slot of level `l` holds the timers whose ticks share
//! one value of the tick bits from `6l` up, so a slot of level `l` covers `64^l` ticks. A timer goes
//! to the level of the highest 6-bit group in which its tick and the current tick differ, and to
//! the slot of its tick bits in that group. When the current tick comes to the start of a slot,
//! the turn makes each timer of the slot whose tick it reached due, and moves each other timer of
//! the slot down to a finer level. Thus insert and cancel cost a fixed number of steps, and a turn
//! costs the timers that it fires or moves, not the ticks that it passes: a bit for each slot tells
//! which slots hold a timer.
//!
//! The due timers wait in one list until they fire, and a turn fires at most its limit of them.
//! The current tick moves to the next slot only when no timer is due, so each due timer has the
//! current tick, and the timers fire in the order of their ticks.
//!
//! The top level takes each tick up to [`RANGE`] ticks after the current tick. Its slots are a
//! ring: a slot before the current slot of the top level holds timers of the next turn of the
//! ring. A tick further out does not fit, so the storage refuses it.
//!
//! The arguments for the order of the turn:
//!
//! - A timer of level `l < 5` shares the tick bits from `6(l + 1)` up with the current tick, and
//!   its slot starts after the current tick. The turn moves the current tick only up to the start
//!   of the next slot that holds a timer, so the current tick stays in that range of bits until
//!   the turn takes the slot.
//! - A timer of the top level starts its slot after the current tick and at most [`RANGE`] ticks
//!   after it, so each slot of the ring has one start for its timers. The current slot of the top
//!   level holds only timers of the next turn of the ring, so it is the last slot of that level.
//! - The start of a slot at level `l` is before the start of each slot at a higher level, so the
//!   lowest level that holds a timer has the next slot to take.
//! - Each timer of a taken slot is due, or moves to a lower level: its tick and the start of its
//!   slot differ only in bits below `6l`. A timer moves down at most five times.

use alloc::boxed::Box;
use core::alloc::AllocError;

use crate::memory::items;

/// The bits of a tick for each level.
const SLOT_BITS: u32 = 6;
const SLOTS: u32 = 1 << SLOT_BITS;
const LEVELS: u32 = 6;

/// The most ticks from the current tick to the tick of a timer: one turn of the top level, less
/// one tick.
pub(super) const RANGE: u64 = (1 << (SLOT_BITS * LEVELS)) - 1;

/// The list of the timers whose ticks the wheel reached: they fire at the next turn.
const DUE: u32 = LEVELS * SLOTS;
/// The list of the free entries.
const FREE: u32 = DUE + 1;
const LISTS: usize = FREE as usize + 1;
/// In no list: a closed timer whose consumer still holds it.
const NONE: u32 = u32::MAX;
/// The end of a list.
const NIL: u32 = u32::MAX;

/// The place of one entry in a list. The index of an entry is the index of its timer.
struct Link {
    tick: u64,
    list: u32,
    prev: u32,
    next: u32,
}

pub(super) struct Wheel {
    links: Box<[Link]>,
    heads: [u32; LISTS],
    /// Bit `s` of `occupied[l]` tells that slot `s` of level `l` holds a timer.
    occupied: [u64; LEVELS as usize],
    /// The current tick. Each timer of this tick or an earlier one is due.
    elapsed: u64,
}

impl Wheel {
    /// A wheel of `capacity` free entries at the tick `elapsed`.
    pub(super) fn new(capacity: u32, elapsed: u64) -> Result<Self, AllocError> {
        let mut wheel = Self {
            links: items(capacity, |_| Link {
                tick: 0,
                list: NONE,
                prev: NIL,
                next: NIL,
            })?,
            heads: [NIL; LISTS],
            occupied: [0; LEVELS as usize],
            elapsed,
        };
        for index in (0..capacity).rev() {
            wheel.push(FREE, index);
        }
        Ok(wheel)
    }

    /// The memory of the entries of a wheel, in bytes, for each timer.
    pub(super) const fn entry_size() -> usize {
        size_of::<Link>()
    }

    pub(super) fn elapsed(&self) -> u64 {
        self.elapsed
    }

    /// Takes a free entry, or gives `None` when each entry holds a timer.
    pub(super) fn alloc(&mut self) -> Option<u32> {
        self.pop(FREE)
    }

    /// Removes the timer of `index` from the wheel, if it is there, and frees its entry.
    pub(super) fn free(&mut self, index: u32) {
        self.unlink(index);
        self.push(FREE, index);
    }

    /// Adds the timer of `index`, which [`Wheel::alloc`] gave, at `tick`. The tick is at or after
    /// the current tick, and at most [`RANGE`] ticks after it. The current tick is due.
    pub(super) fn insert(&mut self, index: u32, tick: u64) {
        self.links[index as usize].tick = tick;
        self.push(self.list_for(tick), index);
    }

    /// The next tick at which a turn has work: the current tick when a timer is due, else the
    /// start of the next slot that holds a timer. `None` when the wheel holds no timer.
    pub(super) fn next(&self) -> Option<u64> {
        if self.heads[DUE as usize] != NIL {
            return Some(self.elapsed);
        }
        self.next_slot().map(|(_, start)| start)
    }

    /// Moves the current tick towards `to`, and gives at most `limit` due timers to `fire`, in
    /// the order of their ticks. Gives `true` when more timers are due: the current tick then
    /// stays at the tick of the last fired timer, so the next call continues in order. Each fired
    /// timer leaves the lists, and its entry stays taken until [`Wheel::free`].
    ///
    /// Each due timer has the current tick: the current tick moves only when no timer is due, to
    /// the start of the next slot, whose due timers have that tick.
    pub(super) fn advance(&mut self, to: u64, limit: u32, mut fire: impl FnMut(u32)) -> bool {
        let mut fired = 0;
        loop {
            let next = if self.heads[DUE as usize] == NIL {
                self.next_slot().filter(|&(_, start)| start <= to)
            } else {
                None
            };
            if fired == limit {
                return self.heads[DUE as usize] != NIL || next.is_some();
            }
            if let Some(index) = self.pop(DUE) {
                fire(index);
                fired += 1;
            } else if let Some((list, start)) = next {
                self.elapsed = start;
                // Each timer of the slot is due or moves to a lower level, so the loop ends.
                while let Some(index) = self.pop(list) {
                    let tick = self.links[index as usize].tick;
                    self.push(self.list_for(tick), index);
                }
            } else {
                self.elapsed = self.elapsed.max(to);
                return false;
            }
        }
    }

    /// Removes each timer from the lists and gives it to `end`. Each entry stays taken until
    /// [`Wheel::free`].
    pub(super) fn clear(&mut self, mut end: impl FnMut(u32)) {
        for list in 0..FREE {
            while let Some(index) = self.pop(list) {
                end(index);
            }
        }
    }

    /// The list of a timer at `tick`.
    fn list_for(&self, tick: u64) -> u32 {
        if tick <= self.elapsed {
            return DUE;
        }
        // The highest bit in which the ticks differ. A difference above the top level, which a
        // tick of the next turn of the ring has, stays at the top level.
        let bit = (tick ^ self.elapsed).ilog2();
        let level = (bit / SLOT_BITS).min(LEVELS - 1);
        level * SLOTS + slot(tick, level)
    }

    /// The list of the next slot that holds a timer, and the tick at which that slot starts.
    fn next_slot(&self) -> Option<(u32, u64)> {
        (0..LEVELS).find_map(|level| {
            let occupied = self.occupied[level as usize];
            if occupied == 0 {
                return None;
            }
            let shift = SLOT_BITS * level;
            let current = slot(self.elapsed, level);
            // The slots from the current slot to the next slot that holds a timer. A timer in the
            // current slot can only be of the next turn of the top-level ring, so that slot is
            // the last one: bit 0 of the rotated bits is the current slot.
            let distance = match occupied.rotate_right(current) & !1 {
                0 => SLOTS,
                later => later.trailing_zeros(),
            };
            // No overflow: the start is at or before the tick of each timer in the slot.
            let start = (self.elapsed >> shift << shift) + (u64::from(distance) << shift);
            Some((level * SLOTS + (current + distance) % SLOTS, start))
        })
    }

    fn push(&mut self, list: u32, index: u32) {
        let head = self.heads[list as usize];
        if let Some(next) = self.links.get_mut(head as usize) {
            next.prev = index;
        }
        let link = &mut self.links[index as usize];
        link.list = list;
        link.prev = NIL;
        link.next = head;
        self.heads[list as usize] = index;
        if list < DUE {
            self.occupied[(list / SLOTS) as usize] |= 1 << (list % SLOTS);
        }
    }

    fn pop(&mut self, list: u32) -> Option<u32> {
        let head = self.heads[list as usize];
        (head != NIL).then(|| {
            self.unlink(head);
            head
        })
    }

    /// Removes the entry of `index` from its list, if it is in one.
    fn unlink(&mut self, index: u32) {
        let Link { list, prev, next, .. } = self.links[index as usize];
        if list == NONE {
            return;
        }
        match self.links.get_mut(prev as usize) {
            Some(link) => link.next = next,
            None => self.heads[list as usize] = next,
        }
        if let Some(link) = self.links.get_mut(next as usize) {
            link.prev = prev;
        }
        if list < DUE && self.heads[list as usize] == NIL {
            self.occupied[(list / SLOTS) as usize] &= !(1 << (list % SLOTS));
        }
        self.links[index as usize].list = NONE;
    }
}

/// The slot of `tick` at `level`: its tick bits from `6 * level` to `6 * level + 5`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the cast keeps the low 32 bits, and the mask then keeps the 6 bits of the slot"
)]
const fn slot(tick: u64, level: u32) -> u32 {
    (tick >> (SLOT_BITS * level)) as u32 & (SLOTS - 1)
}
