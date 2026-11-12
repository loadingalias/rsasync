//! A provided buffer ring (`IORING_REGISTER_PBUF_RING`, Linux 5.19): the buffers that the kernel
//! selects for a receive when its data comes.
//!
//! The ring is memory that the driver writes and the kernel reads: an array of entries, each the
//! address, the length, and the identifier of one buffer, and a tail. The driver writes an entry
//! and then publishes it with a release store of the tail. The kernel keeps its own head, and a
//! completion names the buffer that it took. Linux 6.1 needs the ring in memory of the caller, at a
//! page boundary, so the ring is an anonymous mapping.

use core::mem::ManuallyDrop;
use core::sync::atomic::{AtomicU16, Ordering};
use std::io;

use super::{Mapping, Ring, register, sys};

/// A provided buffer ring that is registered with one ring.
#[derive(Debug)]
pub(crate) struct BufferRing {
    /// The kernel can read the memory until `unregister`, so a ring that drops first leaks it.
    memory: ManuallyDrop<Mapping>,
    /// A power of two.
    entries: u16,
    /// The tail after the last added entry. The kernel sees it at the next publish.
    tail: u16,
    group: u16,
}

impl BufferRing {
    /// Registers a buffer ring of `entries` entries, a power of two up to 32768, as the buffer
    /// group `group` of `ring`.
    #[expect(unsafe_code, reason = "the kernel reads the registration")]
    pub(crate) fn register(ring: &Ring, entries: u16, group: u16) -> io::Result<Self> {
        let memory = Mapping::anonymous(usize::from(entries) * sys::BUF_SIZE)?;
        let mut registration = sys::BufReg {
            ring_addr: memory.base.as_ptr().addr() as u64,
            ring_entries: u32::from(entries),
            bgid: group,
            ..sys::BufReg::default()
        };
        // SAFETY: `registration` is a valid `io_uring_buf_reg` during the call. The ring memory
        // stays mapped until `unregister`, or for the life of the process when the ring drops
        // first.
        unsafe {
            register(
                ring.fd(),
                sys::IORING_REGISTER_PBUF_RING,
                (&raw mut registration).cast(),
                1,
            )
        }?;
        Ok(Self {
            memory: ManuallyDrop::new(memory),
            entries,
            tail: 0,
            group,
        })
    }

    /// The buffer group of the ring.
    pub(crate) fn group(&self) -> u16 {
        self.group
    }

    /// Adds the buffer `bid` of `len` bytes at `address`. The kernel sees it at the next
    /// [`publish`](Self::publish). The caller adds at most as many buffers as the ring has entries
    /// that the kernel has not taken.
    #[expect(unsafe_code, reason = "the driver writes an entry that the kernel does not read")]
    #[expect(
        clippy::cast_ptr_alignment,
        reason = "the mapping starts at a page, and each field is at its natural offset in a 16-byte entry"
    )]
    pub(crate) fn add(&mut self, bid: u16, address: *mut u8, len: u32) {
        let entry = usize::from(self.tail & (self.entries - 1)) * sys::BUF_SIZE;
        let base = self.memory.base.as_ptr();
        // SAFETY: the entry is in the mapping. The kernel reads no entry at or after the published
        // tail, so these writes race with no read. They do not touch the `resv` field, which is
        // the tail in the first entry.
        unsafe {
            base.add(entry).cast::<u64>().write(address.addr() as u64);
            base.add(entry + sys::BUF_LEN).cast::<u32>().write(len);
            base.add(entry + sys::BUF_BID).cast::<u16>().write(bid);
        }
        self.tail = self.tail.wrapping_add(1);
    }

    /// Gives the added buffers to the kernel.
    #[expect(unsafe_code, reason = "the kernel reads the tail of the ring")]
    pub(crate) fn publish(&self) {
        // SAFETY: the tail is a `u16` at a 2-aligned offset of the page-aligned mapping, which
        // lives as long as `self`. The kernel reads it only with an acquire load.
        let tail = unsafe { AtomicU16::from_ptr(self.memory.base.as_ptr().add(sys::BUF_RING_TAIL).cast()) };
        // Release: the kernel reads each added entry after its acquire of the tail.
        tail.store(self.tail, Ordering::Release);
    }

    /// Removes the ring from `ring`, after which the kernel takes no buffer of it, and frees its
    /// memory.
    /// A failure leaves the ring registered, and its memory leaks.
    #[expect(unsafe_code, reason = "the kernel reads the registration")]
    pub(crate) fn unregister(self, ring: &Ring) -> io::Result<()> {
        let mut registration = sys::BufReg {
            bgid: self.group,
            ..sys::BufReg::default()
        };
        // SAFETY: `registration` is a valid `io_uring_buf_reg` during the call.
        unsafe {
            register(
                ring.fd(),
                sys::IORING_UNREGISTER_PBUF_RING,
                (&raw mut registration).cast(),
                1,
            )
        }?;
        drop(ManuallyDrop::into_inner(self.memory));
        Ok(())
    }
}
