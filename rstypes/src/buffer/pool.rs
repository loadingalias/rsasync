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
use crate::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};
use crate::sync::{Arc, arc_size, try_arc};

/// The end of a list of slots. A pool has fewer slots, so no slot has this index.
const NONE: u32 = u32::MAX;

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
/// that drops on a different thread goes into the return list of the pool. The list has at most one
/// entry for each slot, in the slot itself, and the home takes the list when it has no free slot.
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
    /// The first slot of the free list of the home, or `NONE`.
    free: u32,
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
    returned: ReturnList,
    /// The allocator of the arena, which gets the arena back in `Drop::drop`.
    allocator: Box<dyn Allocator + Send>,
    /// The charge of the pool. Fields drop after `Drop::drop` and in order, so the charge ends
    /// after the arena and the allocator are freed.
    _charge: Permit,
}

/// One slot: the count of its owners, and its entry in the free list or in the return list.
struct Slot {
    count: AtomicU64,
    next: AtomicU32,
}

/// The slots that their owners returned on any thread, linked through the entries of the slots.
///
/// A slot is in the list at most one time: only the drop of its last count adds it, and the home
/// takes it out before it gives the slot again. Thus an addition needs no allocation, and the list
/// holds at most the slot count. A driver pool that returns its slots to a home uses the same list.
struct ReturnList {
    head: AtomicU32,
}

impl ReturnList {
    /// Adds slot `index`, on any thread, with no lock and no allocation.
    fn push(&self, slots: &[Slot], index: u32) {
        let entry = &slots[index as usize].next;
        // Ordering: `Relaxed`: the exchange below checks the value.
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            entry.store(head, Ordering::Relaxed);
            // Ordering: `Release` publishes the entry, and each use of the slot before its return,
            // to the `Acquire` of `take`.
            match self
                .head
                .compare_exchange_weak(head, index, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(now) => head = now,
            }
        }
    }

    /// Takes all of the returned slots, at the home. Gives the first of them, or `NONE`.
    fn take(&self) -> u32 {
        // Ordering: see `push`. The home takes the whole list in one exchange. Thus no slot can
        // leave the list and come back between a read of the head and its change.
        self.head.swap(NONE, Ordering::Acquire)
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
        if slot.size() == 0 || count == NONE {
            return Err(PoolRefused::Layout);
        }
        // Each slot takes its rounded size, also the last slot. `repeat` adds no padding after its
        // last copy, so it gets the rounded layout.
        let (arena_layout, stride) = slot
            .pad_to_align()
            .repeat(count as usize)
            .map_err(|_| PoolRefused::Layout)?;
        let charge = Layout::array::<Slot>(count as usize)
            .ok()
            .and_then(|entries| entries.size().checked_add(arena_layout.size()))
            .and_then(|size| size.checked_add(size_of::<A>()))
            .and_then(|size| size.checked_add(arc_size::<Shared>()))
            .and_then(|size| u64::try_from(size).ok())
            .ok_or(PoolRefused::Layout)?;
        let charge = budget.reserve(charge).map_err(PoolRefused::Budget)?;

        let allocator: Box<dyn Allocator + Send> = Box::try_new(allocator).map_err(|_| PoolRefused::Allocation)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count as usize)
            .map_err(|_| PoolRefused::Allocation)?;
        // At first, each slot is in the free list of the home, in order.
        entries.extend((1..=count).map(|next| Slot {
            count: AtomicU64::new(0),
            next: AtomicU32::new(if next == count { NONE } else { next }),
        }));
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
            slots: entries.into_boxed_slice(),
            returned: ReturnList {
                head: AtomicU32::new(NONE),
            },
            allocator,
            _charge: charge,
        })
        .map_err(|_| PoolRefused::Allocation)?;
        Ok(Self { shared, free: 0 })
    }

    /// Gives a free slot as an exclusive backing, with no allocation, or refuses if no slot is free.
    ///
    /// The backing has the capacity of the slot size, and the allocation size of the rounded slot
    /// size. None of its bytes are initialized.
    #[expect(unsafe_code, reason = "the backing owns its slot until its last count drops")]
    pub fn acquire(&mut self) -> Result<Backing, PoolEmpty> {
        if self.free == NONE {
            self.free = self.shared.returned.take();
        }
        let index = self.free;
        // `NONE` is not the index of a slot.
        let Some(slot) = self.shared.slots.get(index as usize) else {
            return Err(PoolEmpty);
        };
        self.free = slot.next.load(Ordering::Relaxed);
        // Ordering: the slot is free, so only the home uses its count until the backing exists.
        slot.count.store(1, Ordering::Relaxed);

        // The reference of the slot to its pool, which the drop of the last count of the slot gives
        // back.
        let pool = Arc::into_raw(Arc::clone(&self.shared));
        // SAFETY: the slot is in the arena: its index is less than the slot count.
        let ptr = unsafe { self.shared.arena.add(index as usize * self.shared.stride) };
        let count = SlotRef {
            // SAFETY: `Arc::into_raw` gives a pointer that is not null.
            pool: unsafe { NonNull::new_unchecked(pool.cast_mut()) },
            index,
        };
        let release = Release(Hook::Shared(super::Shared::Slot(count)));
        // SAFETY: the slot is `capacity` bytes in the arena, which its reference to the pool keeps
        // allocated. The pool gives a slot again only after its last count drops, so only this
        // backing uses it. The count of the backing drops on any thread: the count and the return
        // list are atomic.
        Ok(unsafe { Backing::from_raw_parts(ptr, self.shared.capacity, 0, self.shared.stride, release) })
    }
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
        // Ordering: as for an `Arc`. The count of `self` keeps the slot in use, so a new count
        // needs no order.
        let before = self.shared().slots[self.index as usize]
            .count
            .fetch_add(1, Ordering::Relaxed);
        // Forgotten clones can raise the count without a drop. As `Arc` does, stop far below the
        // wrap, which would return the slot while a lease uses it. Other threads can each add one
        // more before they stop, which half of the range holds.
        assert!(before < u64::MAX / 2, "a slot has fewer than 2^63 counts");
        Self {
            pool: self.pool,
            index: self.index,
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
            let count = &shared.slots[self.index as usize].count;
            // Ordering: as for an `Arc`. A count of 1 under `Acquire` is this count alone: each
            // other count dropped with `Release` before the load, and only a holder of a count can
            // make one more, so the last count returns the slot with no read-modify-write, which
            // would wait for each earlier store of this thread. The acquire of the slot sets the
            // count again. Else `Release` orders each use of the slot by this owner before the drop
            // of its count, and the `Acquire` fence of the last owner orders all of them before
            // the return.
            if count.load(Ordering::Acquire) != 1 {
                if count.fetch_sub(1, Ordering::Release) != 1 {
                    return;
                }
                fence(Ordering::Acquire);
            }
            shared.returned.push(&shared.slots, self.index);
        }
        // SAFETY: `Pool::acquire` took this reference with `Arc::into_raw`, and the slot kept it
        // until its last count, which is gone now. Nothing uses `pool` after this.
        unsafe { Arc::decrement_strong_count(self.pool.as_ptr().cast_const()) };
    }
}

#[expect(unsafe_code, reason = "a count of a slot is for each thread")]
// SAFETY: a `SlotRef` gives reads of a slot that nothing writes while it is leased, and its drop
// uses only the atomic count and the atomic return list. The pool is `Sync`.
unsafe impl Send for SlotRef {}

#[expect(unsafe_code, reason = "a count of a slot is for each thread")]
// SAFETY: see `Send`. A shared `SlotRef` gives only reads.
unsafe impl Sync for SlotRef {}

#[cfg(test)]
pub(super) mod tests;

#[cfg(all(test, loom))]
mod explore;
