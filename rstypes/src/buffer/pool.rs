//! A pool of slots that return to their home with no allocation, no lock, and no epoch.

use alloc::alloc::Global;
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::alloc::{Allocator, Layout};
use core::fmt;
use core::num::NonZeroU32;
use core::ptr::NonNull;

use super::{Backing, Hook, Release};
use crate::capacity::{Budget, Exhausted, Permit};
use crate::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering, fence};
use crate::sync::{Arc, arc_size, try_arc};

/// The bit of a cell of the return ring that holds the lap of the return that wrote it. A pool has
/// fewer slots, so no slot index has this bit.
const LAP: u32 = 1 << 31;

/// A bounded pool of slots of one size, which it charges to a budget for its whole life.
///
/// The pool takes its memory and its charge when it is made. The charge is each allocation of the
/// pool: its slots, an entry of 16 bytes for each slot, and its shared state. The slots take the
/// slot count times the rounded slot size, which is the slot size rounded up to the slot
/// alignment. The charge includes the idle slots. The budget gets it back when the memory of the
/// pool is freed, after the pool and each slot drop.
///
/// [`acquire`](Self::acquire) gives a free slot as a [`Backing`], with no allocation. The slot is
/// exclusive: only that backing writes it. The backing can become a [`Lease`](super::Lease) whose
/// views count in the slot, so the lease needs no allocation either. When the last backing, lease,
/// or view of a slot drops, on any thread, the slot returns to the pool with no allocation and no
/// lock, and a later `acquire` can give it again. No slot waits for an epoch.
///
/// The owner of the pool is the home of its slots: only it acquires, through `&mut self`. A slot
/// that drops, on any thread, goes into the return ring of the pool, which has one cell for each
/// slot. Each acquire takes the returned slots, and gives the slot that came back last first, so
/// a busy pool reuses slots whose memory is still in the cache.
///
/// ```
/// # extern crate rstypes as rsasync;
/// use core::alloc::Layout;
/// use core::num::{NonZeroU32, NonZeroU64};
///
/// use rsasync::buffer::{Buffer, BufferMut, Lease, Pool, PoolEmpty, WriteLoan};
/// use rsasync::capacity::Budget;
///
/// let budget = Budget::new(NonZeroU64::new(1024 * 1024).unwrap());
/// // 2 slots of 4000 bytes, aligned to 4096 bytes: the pool charges 2 rounded slots, and its
/// // entries and shared state.
/// let slot = Layout::from_size_align(4000, 4096).unwrap();
/// let mut pool = Pool::new(&budget, NonZeroU32::new(2).unwrap(), slot).unwrap();
/// assert!(budget.available() < 1024 * 1024 - 2 * 4096);
///
/// // A receive fills a slot. Its lease and views need no allocation.
/// let mut loan = WriteLoan::new(pool.acquire().unwrap());
/// loan.fill_from(b"HEAD:BODY");
/// let lease = Lease::from(loan.into_inner());
/// let body = lease.view(5..).unwrap();
/// drop(lease);
///
/// // The view keeps its slot, so one slot is free.
/// let other = pool.acquire().unwrap();
/// assert_eq!(pool.acquire().unwrap_err(), PoolEmpty);
///
/// // A view can drop on a different thread. Then its slot returns to the pool.
/// std::thread::spawn(move || assert_eq!(body.bytes(), b"BODY")).join().unwrap();
/// let again = pool.acquire().unwrap();
/// assert_eq!((again.capacity(), again.allocation_size()), (4000, 4096));
/// # drop((other, again));
/// ```
pub struct Pool {
    shared: Arc<Shared>,
    /// The free slots of the home: the first `top` entries, the last one first.
    free: Box<[u32]>,
    top: u32,
    /// The next cell of the return ring that the home reads, and the lap bit of its next return.
    cell: u32,
    lap: u32,
}

/// The memory and the slots of a pool. The pool and each slot in use keep it alive.
struct Shared {
    arena: NonNull<u8>,
    arena_layout: Layout,
    /// The rounded slot size: the distance from one slot to the next.
    stride: usize,
    /// The slot size that the caller asked for: the capacity of each backing.
    capacity: usize,
    slots: Box<[Slot]>,
    returned: ReturnRing,
    /// The allocator of the arena, which gets the arena back in `Drop::drop`.
    allocator: Box<dyn Allocator + Send>,
    /// The charge of the pool. Fields drop after `Drop::drop` and in order, so the charge ends
    /// after the arena and the allocator are freed.
    _charge: Permit,
}

/// One slot: the count of the owners of the slot that a clone made.
///
/// The first owner of a slot, the backing that `acquire` gives, counts nothing while it has no
/// clone ([`SlotRef::alone`]), so its drop reads no count. Its first clone counts it and itself.
/// Thus a free slot counts 0, and `acquire` writes nothing here.
struct Slot {
    count: AtomicU64,
}

/// The slots that their owners returned on any thread, in the order of their returns: a ring of
/// one cell for each slot.
///
/// A slot is in the ring at most one time: only its last owner returns it, and the home takes it
/// out before it gives the slot again. So the returns that the home did not take fill at most each
/// cell, and a return needs no allocation and no lock. Each cell holds the lap of the return that
/// wrote it (`LAP`), so the home reads a cell of the next lap as not written, and writes no cell.
struct ReturnRing {
    cells: Box<[AtomicU32]>,
    /// The returns so far. Return `n` writes cell `n % cells` in lap `n / cells`. 64 bits do not
    /// wrap.
    tail: AtomicU64,
}

impl ReturnRing {
    /// Adds slot `index`, on any thread, with no lock and no allocation.
    fn push(&self, index: u32) {
        // Ordering: `Relaxed`: the count gives each return its own cell, and the store below
        // publishes the slot.
        let tail = self.tail.fetch_add(1, Ordering::Relaxed);
        let len = self.cells.len() as u64;
        let lap = if (tail / len) % 2 == 1 { LAP } else { 0 };
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the remainder is less than the cell count, a `u32`"
        )]
        let cell = (tail % len) as usize;
        // Ordering: `Release` publishes each use of the slot before its return to the `Acquire` of
        // the home that reads the cell.
        self.cells[cell].store(index | lap, Ordering::Release);
    }
}

/// A refusal to make a pool. A refusal keeps no charge and no memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolRefused {
    /// The slot size is zero, or the memory of the pool is too large for its allocations.
    Layout,
    /// The budget has fewer free units than the size of all of the slots.
    Budget(Exhausted),
    /// The allocator did not give the memory.
    Allocation,
}

impl fmt::Display for PoolRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Layout => f.write_str("the slots of the pool have no valid layout"),
            Self::Budget(refused) => write!(f, "the budget cannot charge the pool: {refused}"),
            Self::Allocation => f.write_str("the allocator did not give the memory of the pool"),
        }
    }
}

impl core::error::Error for PoolRefused {}

/// A refusal to acquire: no slot of the pool is free. A refusal allocates nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolEmpty;

impl fmt::Display for PoolEmpty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no slot of the pool is free")
    }
}

impl core::error::Error for PoolEmpty {}

impl Pool {
    /// Makes a pool of `slots` slots, each with the size and the alignment of `slot`. It charges
    /// each allocation of the pool to `budget` before it allocates: all of the slots at their
    /// rounded size, their entries, and its shared state.
    pub fn new(budget: &Budget, slots: NonZeroU32, slot: Layout) -> Result<Self, PoolRefused> {
        Self::new_in(budget, slots, slot, Global)
    }

    /// Makes a pool as [`new`](Self::new) does, with all of its slots in one block from
    /// `allocator`. For example, a pool of direct-I/O buffers takes its slots from an allocator
    /// that maps memory that a child process does not inherit.
    ///
    /// The pool gives the block back to `allocator` one time, after the pool and each slot drop, on
    /// the thread of the last drop. Until then it keeps `allocator` in a box, which it charges: a
    /// zero-sized allocator needs no allocation. The charge counts the layout of the slots, not a
    /// larger block that `allocator` gives.
    pub fn new_in<A: Allocator + Send + 'static>(
        budget: &Budget,
        slots: NonZeroU32,
        slot: Layout,
        allocator: A,
    ) -> Result<Self, PoolRefused> {
        let count = slots.get();
        // Each slot index is below `LAP`, the lap bit of a cell of the return ring.
        if slot.size() == 0 || count > LAP {
            return Err(PoolRefused::Layout);
        }
        // Each slot takes its rounded size, also the last slot. `repeat` adds no padding after its
        // last copy, so it gets the rounded layout.
        let (arena_layout, stride) = slot
            .pad_to_align()
            .repeat(count as usize)
            .map_err(|_| PoolRefused::Layout)?;
        // The entries of the slots: a count, a cell of the return ring, and a place on the free
        // list of the home, 16 bytes for each slot.
        let entries = [
            Layout::array::<Slot>(count as usize),
            Layout::array::<AtomicU32>(count as usize),
            Layout::array::<u32>(count as usize),
        ];
        let charge = entries
            .iter()
            .try_fold(0_usize, |sum, entry| {
                entry.as_ref().ok().and_then(|entry| sum.checked_add(entry.size()))
            })
            .and_then(|entries| entries.checked_add(arena_layout.size()))
            .and_then(|size| size.checked_add(size_of::<A>()))
            .and_then(|size| size.checked_add(arc_size::<Shared>()))
            .and_then(|size| u64::try_from(size).ok())
            .ok_or(PoolRefused::Layout)?;
        let charge = budget.reserve(charge).map_err(PoolRefused::Budget)?;

        let allocator: Box<dyn Allocator + Send> = Box::try_new(allocator).map_err(|_| PoolRefused::Allocation)?;
        let slots = filled(count, |_| Slot {
            count: AtomicU64::new(0),
        })?;
        // No cell is written in the first lap.
        let cells = filled(count, |_| AtomicU32::new(LAP))?;
        // At first, each slot is free at the home, and the first slot comes first.
        let free = filled(count, |place| count - 1 - place)?;
        let arena = allocator
            .allocate(arena_layout)
            .map_err(|_| PoolRefused::Allocation)?
            .cast();

        // A refusal drops the shared state, which frees the arena and ends the charge.
        let shared = try_arc(Shared {
            arena,
            arena_layout,
            stride,
            capacity: slot.size(),
            slots,
            returned: ReturnRing {
                cells,
                tail: AtomicU64::new(0),
            },
            allocator,
            _charge: charge,
        })
        .map_err(|_| PoolRefused::Allocation)?;
        Ok(Self {
            shared,
            free,
            top: count,
            cell: 0,
            lap: 0,
        })
    }

    /// Gives a free slot as an exclusive backing, with no allocation, or refuses if no slot is free.
    ///
    /// The backing has the capacity of the slot size, and the allocation size of the rounded slot
    /// size. None of its bytes are initialized.
    #[expect(unsafe_code, reason = "the backing owns its slot until its last count drops")]
    pub fn acquire(&mut self) -> Result<Backing, PoolEmpty> {
        self.take_returns();
        let top = self.top.checked_sub(1).ok_or(PoolEmpty)?;
        let index = self.free[top as usize];
        self.top = top;

        // The reference of the slot to its pool, which the drop of the last count of the slot gives
        // back.
        let pool = Arc::into_raw(Arc::clone(&self.shared));
        // SAFETY: the slot is in the arena: its index is less than the slot count.
        let ptr = unsafe { self.shared.arena.add(index as usize * self.shared.stride) };
        let count = SlotRef {
            // SAFETY: `Arc::into_raw` gives a pointer that is not null.
            pool: unsafe { NonNull::new_unchecked(pool.cast_mut()) },
            index,
            alone: AtomicBool::new(true),
        };
        let release = Release(Hook::Shared(super::Shared::Slot(count)));
        // SAFETY: the slot is `capacity` bytes in the arena, which its reference to the pool keeps
        // allocated. The pool gives a slot again only after its last count drops, so only this
        // backing uses it. The count of the backing drops on any thread: the count and the return
        // list are atomic.
        Ok(unsafe { Backing::from_raw_parts(ptr, self.shared.capacity, 0, self.shared.stride, release) })
    }

    /// Moves each returned slot onto the free list of the home, in the order of the returns, so
    /// the slot that came back last is on top.
    fn take_returns(&mut self) {
        let cells = &self.shared.returned.cells;
        // Each pass takes one return, and the ring holds at most one return for each cell.
        for _ in 0..cells.len() {
            // Ordering: `Acquire` reads the `Release` of the return, and each use of the slot
            // before it.
            let value = cells[self.cell as usize].load(Ordering::Acquire);
            if value & LAP != self.lap {
                return;
            }
            // The free list has a place for each slot, and the returned slot is not on it.
            self.free[self.top as usize] = value & !LAP;
            self.top += 1;
            self.cell += 1;
            if self.cell as usize == cells.len() {
                self.cell = 0;
                self.lap ^= LAP;
            }
        }
    }
}

/// `count` items that `make` gives for each index, or `Allocation` when the allocator refuses.
fn filled<T>(count: u32, make: impl FnMut(u32) -> T) -> Result<Box<[T]>, PoolRefused> {
    let mut items = Vec::new();
    items
        .try_reserve_exact(count as usize)
        .map_err(|_| PoolRefused::Allocation)?;
    items.extend((0..count).map(make));
    Ok(items.into_boxed_slice())
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("slots", &self.shared.slots.len())
            .field("capacity", &self.shared.capacity)
            .field("allocation_size", &self.shared.stride)
            .finish_non_exhaustive()
    }
}

impl Drop for Shared {
    #[expect(unsafe_code, reason = "the pool frees its arena when no slot is in use")]
    fn drop(&mut self) {
        // SAFETY: `Pool::new_in` allocated the arena from `allocator` with `arena_layout`. Each slot
        // in use keeps a reference to the pool, so no slot is in use now.
        unsafe { self.allocator.deallocate(self.arena, self.arena_layout) };
    }
}

#[expect(unsafe_code, reason = "the arena pointer of a pool is for each thread")]
// SAFETY: the pool itself uses its arena only to free it. Each slot of the arena is for the owners
// of that slot, which `Backing` and `Lease` make safe on each thread. The allocator is `Send`. The
// other fields are atomic, immutable, or a `Permit`, which each thread can use.
unsafe impl Send for Shared {}

#[expect(unsafe_code, reason = "the arena pointer of a pool is for each thread")]
// SAFETY: see `Send`. A shared `Shared` gives only atomic operations and immutable facts. Only
// `Drop::drop`, which has the only reference, uses the allocator, so the allocator needs no `Sync`.
unsafe impl Sync for Shared {}

/// One count of a slot in use, which each backing, lease, and view of the slot holds. The index,
/// not the address of a region, names the slot, so each part of a split keeps the slot.
#[derive(Debug)]
pub(super) struct SlotRef {
    /// The pool, from `Arc::into_raw`. The slot holds one strong count of it while it is in use.
    pool: NonNull<Shared>,
    index: u32,
    /// This is the first owner of the slot, which `acquire` gave, and no clone of it was made: it is
    /// the only owner, and the count of the slot does not count it.
    alone: AtomicBool,
}

impl SlotRef {
    #[expect(unsafe_code, reason = "the count of a slot keeps its pool alive")]
    fn shared(&self) -> &Shared {
        // SAFETY: this count keeps the slot in use, and the slot keeps its pool alive.
        unsafe { self.pool.as_ref() }
    }

    /// The address of the first byte of the slot.
    pub(super) fn as_ptr(&self) -> *const u8 {
        let shared = self.shared();
        shared
            .arena
            .as_ptr()
            .cast_const()
            .wrapping_add(self.index as usize * shared.stride)
    }

    /// The rounded slot size, which the slot keeps.
    pub(super) fn allocation_size(&self) -> usize {
        self.shared().stride
    }
}

impl Clone for SlotRef {
    fn clone(&self) -> Self {
        let count = &self.shared().slots[self.index as usize].count;
        // Ordering: as for an `Arc`. `self` keeps the slot in use, so a new count needs no order.
        // The clone counts itself first, and then the first owner when this is its first clone. A
        // clone that returned thus counted the first owner, so the count does not fall to 0 while
        // an owner lives. The drop of `self` comes after each clone that borrows it.
        let before = count.fetch_add(1, Ordering::Relaxed);
        if self.alone.swap(false, Ordering::Relaxed) {
            count.fetch_add(1, Ordering::Relaxed);
        }
        // Forgotten clones can raise the count without a drop. As `Arc` does, stop far below the
        // wrap, which would return the slot while a lease uses it. Other threads can each add one
        // more before they stop, which half of the range holds.
        assert!(before < u64::MAX / 2, "a slot has fewer than 2^63 counts");
        Self {
            pool: self.pool,
            index: self.index,
            alone: AtomicBool::new(false),
        }
    }
}

impl Drop for SlotRef {
    /// Drops this count. The last count returns the slot to its home, and then drops the reference
    /// of the slot to its pool.
    #[expect(unsafe_code, reason = "the last count of a slot drops its reference to the pool")]
    fn drop(&mut self) {
        {
            let shared = self.shared();
            // A first owner with no clone is the only owner: it returns the slot with no access to
            // the cold line of the count, which is 0. Each clone of it came before this drop.
            if !self.alone.load(Ordering::Relaxed) {
                // Ordering: as for an `Arc`. `Release` orders each use of the slot by this owner
                // before the drop of its count, and the `Acquire` fence of the last owner orders all
                // of them before the return. A load could not stand in for the read-modify-write: a
                // clone on another thread adds with no order, so a load can miss it.
                if shared.slots[self.index as usize].count.fetch_sub(1, Ordering::Release) != 1 {
                    return;
                }
                fence(Ordering::Acquire);
            }
            shared.returned.push(self.index);
        }
        // SAFETY: `Pool::acquire` took this reference with `Arc::into_raw`, and the slot kept it
        // until its last count, which is gone now. Nothing uses `pool` after this.
        unsafe { Arc::decrement_strong_count(self.pool.as_ptr().cast_const()) };
    }
}

#[expect(unsafe_code, reason = "a count of a slot is for each thread")]
// SAFETY: a `SlotRef` gives reads of a slot that nothing writes while it is leased, and its clone
// and its drop use only the atomic count, the atomic `alone`, and the atomic return ring. The pool
// is `Sync`.
unsafe impl Send for SlotRef {}

#[expect(unsafe_code, reason = "a count of a slot is for each thread")]
// SAFETY: see `Send`. A shared `SlotRef` gives only reads.
unsafe impl Sync for SlotRef {}

#[cfg(test)]
pub(super) mod tests;

#[cfg(all(test, loom))]
mod explore;
