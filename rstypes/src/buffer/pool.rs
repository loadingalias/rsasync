//! A pool of slots that return to their home with no allocation, no lock, and no epoch.

use alloc::alloc::{alloc, dealloc};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::alloc::Layout;
use core::fmt;
use core::mem::ManuallyDrop;
use core::num::NonZeroU32;
use core::ptr::NonNull;

use super::{Backing, Release};
use crate::capacity::{Budget, Exhausted, Permit};
use crate::sync::Arc;
use crate::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};

/// The end of a list of slots. A pool has fewer slots, so no slot has this index.
const NONE: u32 = u32::MAX;

/// A bounded pool of slots of one size, which it charges to a budget for its whole life.
///
/// The pool takes its memory and its charge when it is made: the slot count times the rounded slot
/// size, which is the slot size rounded up to the slot alignment. The charge includes the idle
/// slots. The budget gets it back when the memory of the pool is freed, after the pool and each
/// slot drop.
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
/// // 2 slots of 4000 bytes, aligned to 4096 bytes: the pool charges 2 rounded slots.
/// let slot = Layout::from_size_align(4000, 4096).unwrap();
/// let mut pool = Pool::new(&budget, NonZeroU32::new(2).unwrap(), slot).unwrap();
/// assert_eq!(budget.available(), 1024 * 1024 - 2 * 4096);
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
    /// The charge of the arena. Fields drop after `Drop::drop`, so the charge ends after the arena
    /// is freed.
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
    /// The slot size is zero, or the size of all of the slots is too large for one allocation.
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
    /// all of the slots to `budget`, at their rounded size, before it allocates.
    #[expect(
        unsafe_code,
        reason = "the pool allocates its arena with the layout that it frees it with"
    )]
    pub fn new(budget: &Budget, slots: NonZeroU32, slot: Layout) -> Result<Self, PoolRefused> {
        let count = slots.get();
        if slot.size() == 0 || count == NONE {
            return Err(PoolRefused::Layout);
        }
        let stride = slot.pad_to_align().size();
        let arena_layout = stride
            .checked_mul(count as usize)
            .and_then(|size| Layout::from_size_align(size, slot.align()).ok())
            .ok_or(PoolRefused::Layout)?;
        let charge = u64::try_from(arena_layout.size()).map_err(|_| PoolRefused::Layout)?;
        let charge = budget.reserve(charge).map_err(PoolRefused::Budget)?;

        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count as usize)
            .map_err(|_| PoolRefused::Allocation)?;
        // At first, each slot is in the free list of the home, in order.
        entries.extend((1..=count).map(|next| Slot {
            count: AtomicU64::new(0),
            next: AtomicU32::new(if next == count { NONE } else { next }),
        }));
        // SAFETY: the size of `arena_layout` is not zero: the slot size and the count are not zero.
        let arena = NonNull::new(unsafe { alloc(arena_layout) }).ok_or(PoolRefused::Allocation)?;

        Ok(Self {
            shared: Arc::new(Shared {
                arena,
                arena_layout,
                stride,
                capacity: slot.size(),
                slots: entries.into_boxed_slice(),
                returned: ReturnList {
                    head: AtomicU32::new(NONE),
                },
                _charge: charge,
            }),
            free: 0,
        })
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

        let pool = Arc::as_ptr(&self.shared);
        // SAFETY: `pool` is from a live `Arc`. This is the reference of the slot to its pool, which
        // `release_count` drops when the last count of the slot drops.
        unsafe { Arc::increment_strong_count(pool) };
        // SAFETY: the slot is in the arena: its index is less than the slot count.
        let ptr = unsafe { self.shared.arena.add(index as usize * self.shared.stride) };
        let release = Release {
            data: pool.cast(),
            release: release_slot,
            pool_slot: true,
        };
        // SAFETY: the slot is `capacity` bytes in the arena, which its reference to the pool keeps
        // allocated. The pool gives a slot again only after its last count drops, so only this
        // backing uses it. `release_slot` drops the count of the backing on any thread: the count
        // and the return list are atomic.
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

impl Shared {
    /// The index of the slot at `ptr`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the index is less than the slot count, which is a u32"
    )]
    fn index(&self, ptr: NonNull<u8>) -> u32 {
        ((ptr.addr().get() - self.arena.addr().get()) / self.stride) as u32
    }
}

impl Drop for Shared {
    #[expect(unsafe_code, reason = "the pool frees its arena when no slot is in use")]
    fn drop(&mut self) {
        // SAFETY: `Pool::new` allocated the arena with `arena_layout`. Each slot in use keeps a
        // reference to the pool, so no slot is in use now.
        unsafe { dealloc(self.arena.as_ptr(), self.arena_layout) };
    }
}

#[expect(unsafe_code, reason = "the arena pointer of a pool is for each thread")]
// SAFETY: the pool itself uses its arena only to free it. Each slot of the arena is for the owners
// of that slot, which `Backing` and `Lease` make safe on each thread. The other fields are atomic,
// immutable, or a `Permit`, which each thread can use.
unsafe impl Send for Shared {}

#[expect(unsafe_code, reason = "the arena pointer of a pool is for each thread")]
// SAFETY: see `Send`. A shared `Shared` gives only atomic operations and immutable facts.
unsafe impl Sync for Shared {}

/// The release hook of a backing from `Pool::acquire`: drops the count of the backing.
///
/// # Safety
///
/// `data` and `ptr` are from `Pool::acquire`, and the backing gives up its count with this call.
#[expect(unsafe_code, reason = "the hook drops the count that keeps the pool alive")]
unsafe fn release_slot(data: *const (), ptr: NonNull<u8>, _: usize) {
    let pool = data.cast::<Shared>();
    // SAFETY: the count of the backing keeps the pool alive.
    let index = unsafe { (*pool).index(ptr) };
    // SAFETY: the backing gives up its count of slot `index`.
    unsafe { release_count(pool, index) };
}

/// Drops one count of slot `index`. The last count returns the slot to its home, and then drops the
/// reference of the slot to its pool.
///
/// # Safety
///
/// The caller gives up one count of the slot, which keeps `pool` alive until this call.
#[expect(unsafe_code, reason = "the last count of a slot drops its reference to the pool")]
unsafe fn release_count(pool: *const Shared, index: u32) {
    {
        // SAFETY: the count that the caller gives up keeps the pool alive in this block.
        let shared = unsafe { &*pool };
        // Ordering: as for an `Arc`. `Release` orders each use of the slot by this owner before the
        // drop of its count, and the `Acquire` fence of the last owner orders all of them before
        // the return.
        if shared.slots[index as usize].count.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        fence(Ordering::Acquire);
        shared.returned.push(&shared.slots, index);
    }
    // SAFETY: the slot kept this reference since `Pool::acquire`, and its last count is gone.
    // Nothing uses `pool` after this.
    unsafe { Arc::decrement_strong_count(pool) };
}

/// One count of a slot in use, which a lease of the slot holds.
#[derive(Debug)]
pub(super) struct SlotRef {
    pool: NonNull<Shared>,
    index: u32,
}

impl SlotRef {
    /// Takes the count of `backing` if it is from `Pool::acquire`, or gives the backing back.
    #[expect(unsafe_code, reason = "only `Pool::acquire` marks a release as a pool slot")]
    pub(super) fn try_from(backing: Backing) -> Result<Self, Backing> {
        if !backing.release.pool_slot {
            return Err(backing);
        }
        // The count of the backing moves to the new `SlotRef`, so the backing does not drop.
        let backing = ManuallyDrop::new(backing);
        let pool = backing.release.data.cast::<Shared>();
        // SAFETY: only `Pool::acquire` sets `pool_slot`, with the pool as `data` and the slot at
        // `ptr`. The count of the backing keeps the pool alive.
        let index = unsafe { (*pool).index(backing.ptr) };
        Ok(Self {
            // SAFETY: `pool` is from `Arc::as_ptr`, which is not null.
            pool: unsafe { NonNull::new_unchecked(pool.cast_mut()) },
            index,
        })
    }

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
        // needs no order. A 64-bit count cannot overflow.
        self.shared().slots[self.index as usize]
            .count
            .fetch_add(1, Ordering::Relaxed);
        Self {
            pool: self.pool,
            index: self.index,
        }
    }
}

impl Drop for SlotRef {
    #[expect(unsafe_code, reason = "a lease gives up its count of the slot")]
    fn drop(&mut self) {
        // SAFETY: this is the one drop of this count.
        unsafe { release_count(self.pool.as_ptr(), self.index) };
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
mod tests;

#[cfg(all(test, loom))]
mod explore;
