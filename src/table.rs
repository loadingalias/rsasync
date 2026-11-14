//! The operation table of one home driver.
//!
//! Each accepted operation has a slot from before its submission to its terminal event. The slot
//! holds the resources of the operation, `T`, by value: the buffer that the kernel uses, its
//! charge, and any other memory that the submission names. The table returns or drops them exactly
//! once, and never before their terminal event. Thus a dropped or forgotten consumer frees nothing
//! that the kernel still uses, and no destructor of a future has to run.
//!
//! - Identity: an [`OpId`] is a slot and the generation of the slot. A slot gets the next
//!   generation when it frees, so an identity that outlives its operation is stale, and the table
//!   ignores it.
//! - Debt and the terminal event: [`Table::begin`] records the resources and the [`Shape`] of an
//!   operation before the backend submits it. The shape gives the event that releases the
//!   resources, by the terminal-event table of the design; a zero-copy send selects between its two
//!   rows at its first completion.
//! - Interest apart from ownership: when the consumer ends its interest, the operation and its
//!   resources stay. At the terminal event, the table gives them back to the driver, which applies
//!   the abandoned rule.
//! - Requests from other threads: [`Requests::post`] records a cancel or the end of interest in the
//!   one request entry of the slot, with no allocation, and the home takes them with
//!   [`Table::take_requests`]. A cancel result changes nothing: the operation waits for its own
//!   terminal event.
//!
//! The backends give each completion to [`Table::complete`] as a backend-neutral [`Event`]. A test
//! gives the same transitions its own events, in any order, and any cancel result.
//!
//! A slot holds `T` inline, in a slice that the table allocates once, so `T` keeps its address
//! while the operation runs. The table of a driver holds a caller's buffer in `T` as a
//! [`Backing`](crate::buffer::Backing) or a [`Lease`](crate::buffer::Lease), with no allocation.

#[cfg(all(test, loom))]
mod explore;
#[cfg(test)]
pub(crate) mod tests;

use alloc::boxed::Box;
use core::mem::{self, ManuallyDrop};

use crate::sync::Arc;
use crate::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// The largest number of slots: a slot index has 30 bits of the user data.
pub(crate) const MAX_SLOTS: u32 = 1 << 30;

/// The user data of a cancel result: this tag and the user data of the target.
const CANCEL_RESULT: u64 = 1 << 63;

/// The user data of another completion of the driver, for example a wake message.
pub(crate) const CONTROL: u64 = 0b11 << 62;

/// No slot: the end of a list.
const NIL: u32 = u32::MAX;

/// The request bits of a request entry. The upper 32 bits hold the generation of the target.
const CANCEL: u64 = 1 << 0;
const END_INTEREST: u64 = 1 << 1;
/// The entry is in the list of the home.
const QUEUED: u64 = 1 << 31;

/// The identity of an operation: its slot and the generation of the slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpId {
    index: u32,
    generation: u32,
}

impl OpId {
    /// The user data of the submission. Bits 62 and 63 are clear.
    pub(crate) fn user_data(self) -> u64 {
        u64::from(self.index) << 32 | u64::from(self.generation)
    }

    /// The slot of the operation.
    pub(crate) fn index(self) -> u32 {
        self.index
    }

    /// The generation of the slot that the operation has.
    pub(crate) fn generation(self) -> u32 {
        self.generation
    }

    /// The operation that `user_data` names, or `None` for the user data of a cancel result or of
    /// another completion of the driver.
    pub(crate) fn from_user_data(user_data: u64) -> Option<Self> {
        (user_data & CONTROL == 0).then(|| Self::from_bits(user_data))
    }

    /// The user data of a cancel of this operation, so that its result reaches
    /// [`Table::complete`] as a cancel result.
    pub(crate) fn cancel_user_data(self) -> u64 {
        CANCEL_RESULT | self.user_data()
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "the generation is the low 32 bits of the user data, and the index fits in 30 bits"
    )]
    fn from_bits(bits: u64) -> Self {
        Self {
            index: (bits >> 32) as u32 & (MAX_SLOTS - 1),
            generation: bits as u32,
        }
    }
}

/// The completions that an operation posts, by the terminal-event table of the design.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shape {
    /// One completion, which is terminal.
    Single,
    /// Items while each completion has `more`; the completion without it is terminal.
    Multishot,
    /// A zero-copy send. A result with `more` ends the operation, and the notice that follows
    /// releases the resources. A result without `more` is terminal, because no notice follows.
    ZeroCopy,
}

/// A completion, in the terms of every backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Event {
    pub(crate) result: i32,
    /// The operation posts more completions (`IORING_CQE_F_MORE`).
    pub(crate) more: bool,
    /// The completion is the notice of a zero-copy send (`IORING_CQE_F_NOTIF`).
    pub(crate) notice: bool,
}

/// What [`Table::complete`] did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome<T> {
    /// The identity names no live operation, for example after its slot was reused. Nothing
    /// changed.
    Stale,
    /// The result of a cancel. It is not a terminal event, so nothing changed: the target waits
    /// for its own terminal event.
    CancelResult,
    /// A completion of the driver that names no operation. The driver handles it.
    Control(u64),
    /// A nonterminal item of a multishot operation.
    Item(i32),
    /// The operation ended, and its consumer can claim the result: the driver wakes the consumer.
    Ready,
    /// The operation ended with no consumer. The driver applies the abandoned rule.
    Abandoned(Ended<T>),
    /// The notice of an ended zero-copy send released its resources.
    Released(T),
    /// The terminal-event table has no row for this event in this state, so nothing changed. The
    /// driver reports it.
    Unclassified,
}

/// The result of an ended operation, and its resources if they are free.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Ended<T> {
    pub(crate) result: i32,
    /// `None` for a zero-copy send whose notice has not released them.
    pub(crate) resources: Option<T>,
}

/// The table has no free slot. The caller keeps its resources.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Full<T>(pub(crate) T);

/// A request that another thread posts to the home of an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Request {
    /// Ask the backend to cancel the operation. Its consumer stays.
    Cancel,
    /// The consumer ends its interest. The operation and its resources stay until the terminal
    /// event.
    EndInterest,
}

/// What [`Table::take_requests`] gives to the driver.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Taken<T> {
    /// Submit a cancel of the operation, with [`OpId::cancel_user_data`].
    Cancel(OpId),
    /// The consumer ended its interest after the operation ended.
    Abandoned(OpId, Ended<T>),
}

enum State<T> {
    Free,
    Live {
        shape: Shape,
        resources: T,
        interest: bool,
    },
    /// A zero-copy send ended, and its resources wait for the notice. `result` waits for the
    /// consumer while it has interest.
    Notice {
        resources: T,
        result: Option<i32>,
    },
    /// The operation ended, and its result waits for the consumer.
    Done {
        resources: Option<T>,
        result: i32,
    },
}

struct Slot<T> {
    generation: u32,
    next_free: u32,
    state: State<T>,
}

/// The operation table of one home driver. It stays on that thread.
pub(crate) struct Table<T> {
    slots: Box<[Slot<T>]>,
    free: u32,
    requests: Requests,
}

impl<T> Table<T> {
    /// A table of `slots` operations. It allocates here, one time.
    ///
    /// # Panics
    ///
    /// If `slots` is above [`MAX_SLOTS`].
    pub(crate) fn new(slots: u32) -> Self {
        assert!(slots <= MAX_SLOTS, "an operation table has at most 2^30 slots");
        Self {
            slots: (0..slots)
                .map(|index| Slot {
                    generation: 0,
                    next_free: if index + 1 < slots { index + 1 } else { NIL },
                    state: State::Free,
                })
                .collect(),
            free: if slots > 0 { 0 } else { NIL },
            requests: Requests(Arc::new(Shared {
                entries: (0..slots)
                    .map(|_| Entry {
                        word: AtomicU64::new(0),
                        next: AtomicU32::new(NIL),
                    })
                    .collect(),
                head: AtomicU32::new(NIL),
            })),
        }
    }

    /// The handle that posts requests to this table from any thread.
    pub(crate) fn requests(&self) -> Requests {
        self.requests.clone()
    }

    /// Records an operation before its submission: its resources and its shape. Its consumer has
    /// interest. Refuses with the resources when no slot is free.
    pub(crate) fn begin(&mut self, resources: T, shape: Shape) -> Result<OpId, Full<T>> {
        let index = self.free;
        let Some(slot) = self.slots.get_mut(index as usize) else {
            return Err(Full(resources));
        };
        self.free = slot.next_free;
        slot.state = State::Live {
            shape,
            resources,
            interest: true,
        };
        Ok(OpId {
            index,
            generation: slot.generation,
        })
    }

    /// Takes back the resources of an operation that the backend refused to submit, so that no
    /// completion of it can come.
    pub(crate) fn withdraw(&mut self, id: OpId) -> Option<T> {
        let slot = self.live(id)?;
        match mem::replace(&mut slot.state, State::Free) {
            State::Live { resources, .. } => {
                self.free_slot(id.index);
                Some(resources)
            }
            state => {
                slot.state = state;
                None
            }
        }
    }

    /// Applies one completion of the backend, by the user data of its submission.
    pub(crate) fn complete(&mut self, user_data: u64, event: Event) -> Outcome<T> {
        match user_data & CONTROL {
            0 => {}
            CANCEL_RESULT => return Outcome::CancelResult,
            _ => return Outcome::Control(user_data),
        }
        let id = OpId::from_bits(user_data);
        let Some(slot) = self.live(id) else {
            return Outcome::Stale;
        };
        let (state, outcome) = match (mem::replace(&mut slot.state, State::Free), event) {
            (
                State::Live {
                    shape: Shape::Multishot,
                    resources,
                    interest,
                },
                Event {
                    result,
                    more: true,
                    notice: false,
                },
            ) => (
                State::Live {
                    shape: Shape::Multishot,
                    resources,
                    interest,
                },
                Outcome::Item(result),
            ),
            (
                State::Live {
                    shape: Shape::ZeroCopy,
                    resources,
                    interest,
                },
                Event {
                    result,
                    more: true,
                    notice: false,
                },
            ) if interest => (
                State::Notice {
                    resources,
                    result: Some(result),
                },
                Outcome::Ready,
            ),
            (
                State::Live {
                    shape: Shape::ZeroCopy,
                    resources,
                    ..
                },
                Event {
                    result,
                    more: true,
                    notice: false,
                },
            ) => (
                State::Notice {
                    resources,
                    result: None,
                },
                Outcome::Abandoned(Ended {
                    result,
                    resources: None,
                }),
            ),
            (
                State::Live {
                    resources, interest, ..
                },
                Event {
                    result,
                    more: false,
                    notice: false,
                },
            ) if interest => (
                State::Done {
                    resources: Some(resources),
                    result,
                },
                Outcome::Ready,
            ),
            (
                State::Live { resources, .. },
                Event {
                    result,
                    more: false,
                    notice: false,
                },
            ) => (
                State::Free,
                Outcome::Abandoned(Ended {
                    result,
                    resources: Some(resources),
                }),
            ),
            (
                State::Notice { resources, result },
                Event {
                    more: false,
                    notice: true,
                    ..
                },
            ) => (
                result.map_or(State::Free, |result| State::Done {
                    resources: None,
                    result,
                }),
                Outcome::Released(resources),
            ),
            (state, _) => (state, Outcome::Unclassified),
        };
        let free = matches!(state, State::Free);
        slot.state = state;
        if free {
            self.free_slot(id.index);
        }
        outcome
    }

    /// Gives the result of an ended operation to its consumer, one time. `None` while the
    /// operation runs.
    pub(crate) fn claim(&mut self, id: OpId) -> Option<Ended<T>> {
        let slot = self.live(id)?;
        match mem::replace(&mut slot.state, State::Free) {
            State::Done { resources, result } => {
                self.free_slot(id.index);
                Some(Ended { result, resources })
            }
            State::Notice {
                resources,
                result: Some(result),
            } => {
                slot.state = State::Notice {
                    resources,
                    result: None,
                };
                Some(Ended {
                    result,
                    resources: None,
                })
            }
            state => {
                slot.state = state;
                None
            }
        }
    }

    /// Ends the interest of the consumer. A running operation keeps its resources until its
    /// terminal event. Gives the result of an ended operation, for the abandoned rule.
    pub(crate) fn end_interest(&mut self, id: OpId) -> Option<Ended<T>> {
        if let Some(Slot {
            state: State::Live { interest, .. },
            ..
        }) = self.live(id)
        {
            *interest = false;
            return None;
        }
        self.claim(id)
    }

    /// Takes the requests that other threads posted, and applies each one that names a live
    /// operation. An end of interest applies first. A cancel of a running operation goes to `on`
    /// for the backend.
    pub(crate) fn take_requests(&mut self, mut on: impl FnMut(Taken<T>)) {
        let shared = Arc::clone(&self.requests.0);
        // Acquire: each poster wrote the `next` of its entry before its release of the head.
        let mut index = shared.head.swap(NIL, Ordering::Acquire);
        while let Some(entry) = shared.entries.get(index as usize) {
            // Read before the take of the word: after it, a poster can link the entry again.
            let next = entry.next.load(Ordering::Relaxed);
            // AcqRel: a poster that sees the entry empty after this swap links it again, and the
            // read of `next` above happens before that link.
            let word = entry.word.swap(0, Ordering::AcqRel);
            let id = OpId::from_bits(u64::from(index) << 32 | word >> 32);
            if word & END_INTEREST != 0
                && let Some(ended) = self.end_interest(id)
            {
                on(Taken::Abandoned(id, ended));
            }
            if word & CANCEL != 0
                && matches!(
                    self.live(id),
                    Some(Slot {
                        state: State::Live { .. },
                        ..
                    })
                )
            {
                on(Taken::Cancel(id));
            }
            index = next;
        }
    }

    /// The resources of the operation `id` while its slot holds them, or `None` for a stale
    /// identity.
    pub(crate) fn resources(&self, id: OpId) -> Option<&T> {
        let slot = self
            .slots
            .get(id.index as usize)
            .filter(|slot| slot.generation == id.generation)?;
        match &slot.state {
            State::Live { resources, .. } | State::Notice { resources, .. } => Some(resources),
            State::Done { resources, .. } => resources.as_ref(),
            State::Free => None,
        }
    }

    /// Whether the operation `id` runs: its terminal event can still come.
    pub(crate) fn runs(&self, id: OpId) -> bool {
        self.slots
            .get(id.index as usize)
            .is_some_and(|slot| slot.generation == id.generation && matches!(slot.state, State::Live { .. }))
    }

    /// Whether the consumer of the running operation `id` has interest in its result.
    pub(crate) fn interest(&self, id: OpId) -> bool {
        self.slots.get(id.index as usize).is_some_and(|slot| {
            slot.generation == id.generation && matches!(slot.state, State::Live { interest: true, .. })
        })
    }

    /// The running operations: each one whose terminal event can still come.
    pub(crate) fn running(&self) -> impl Iterator<Item = OpId> {
        self.slots.iter().zip(0..).filter_map(|(slot, index)| {
            matches!(slot.state, State::Live { .. }).then_some(OpId {
                index,
                generation: slot.generation,
            })
        })
    }

    /// Whether an event can still come: an operation runs, or a zero-copy send waits for its
    /// notice.
    pub(crate) fn busy(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| matches!(slot.state, State::Live { .. } | State::Notice { .. }))
    }

    /// The slot of a live operation, or `None` for a stale identity.
    fn live(&mut self, id: OpId) -> Option<&mut Slot<T>> {
        self.slots
            .get_mut(id.index as usize)
            .filter(|slot| slot.generation == id.generation && !matches!(slot.state, State::Free))
    }

    /// Frees a slot that `live` gave and that holds `State::Free`.
    fn free_slot(&mut self, index: u32) {
        if let Some(slot) = self.slots.get_mut(index as usize) {
            slot.generation = slot.generation.wrapping_add(1);
            slot.next_free = self.free;
            self.free = index;
        }
    }
}

impl<T> Drop for Table<T> {
    /// The kernel can still use the resources of a running operation and of a zero-copy send
    /// that waits for its notice, so they leak. The other resources drop.
    fn drop(&mut self) {
        for slot in &mut self.slots {
            if let State::Live { .. } | State::Notice { .. } = slot.state {
                let _leaked = ManuallyDrop::new(mem::replace(&mut slot.state, State::Free));
            }
        }
    }
}

/// The request entry of one slot.
struct Entry {
    /// `generation << 32 | QUEUED | requests`, or 0 when the entry is empty.
    word: AtomicU64,
    /// The next entry in the list of the home.
    next: AtomicU32,
}

struct Shared {
    entries: Box<[Entry]>,
    /// The list of the entries that hold requests, linked by `next`.
    head: AtomicU32,
}

/// Posts requests to the home of a table, from any thread. It never allocates or blocks.
#[derive(Clone)]
pub(crate) struct Requests(Arc<Shared>);

impl Requests {
    /// Whether no request waits for the home.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.head.load(Ordering::Acquire) == NIL
    }

    /// Records `request` in the entry of the slot of `id`. Requests for one operation combine. A
    /// request for an older generation than the entry holds is stale, and it changes nothing.
    pub(crate) fn post(&self, id: OpId, request: Request) {
        let Some(entry) = self.0.entries.get(id.index as usize) else {
            return;
        };
        let bit = match request {
            Request::Cancel => CANCEL,
            Request::EndInterest => END_INTEREST,
        };
        let own = u64::from(id.generation) << 32 | QUEUED;
        let mut word = entry.word.load(Ordering::Relaxed);
        loop {
            let held = (word >> 32) as u32;
            let new = if word == 0 {
                own | bit
            } else if held == id.generation {
                word | bit
            } else if held.wrapping_sub(id.generation).cast_signed() > 0 {
                // The entry holds a newer operation of the slot: this request is stale.
                return;
            } else {
                // The entry holds a stale request. It is in the list already.
                own | bit
            };
            // AcqRel: see `Table::take_requests`.
            match entry
                .word
                .compare_exchange_weak(word, new, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(now) => word = now,
            }
        }
        if word == 0 {
            self.link(id.index, entry);
        }
    }

    /// Adds the entry of `index` to the list of the home.
    fn link(&self, index: u32, entry: &Entry) {
        let mut head = self.0.head.load(Ordering::Relaxed);
        loop {
            entry.next.store(head, Ordering::Relaxed);
            // Release: the home reads `next` after its acquire of the head.
            match self
                .0
                .head
                .compare_exchange_weak(head, index, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(now) => head = now,
            }
        }
    }
}
