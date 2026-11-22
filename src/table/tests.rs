use alloc::rc::Rc;
use alloc::vec::Vec;
use core::assert_matches;
use core::cell::RefCell;
use core::num::NonZeroU64;

use super::{Event, Full, Outcome, Request, Shape, Table, Taken};
use crate::capacity::{Budget, Permit};

/// `ENOENT`, `EALREADY`, and `ECANCELED` (`include/uapi/asm-generic/errno-base.h` and `errno.h`).
pub(crate) const ENOENT: i32 = 2;
pub(crate) const EALREADY: i32 = 114;
pub(crate) const ECANCELED: i32 = 125;

/// The order of the deliveries and the releases of a test.
#[derive(Debug, Clone, Default)]
pub(crate) struct Log(Rc<RefCell<Vec<&'static str>>>);

impl Log {
    pub(crate) fn note(&self, what: &'static str) {
        self.0.borrow_mut().push(what);
    }

    pub(crate) fn take(&self) -> Vec<&'static str> {
        self.0.take()
    }
}

/// The resources of a test operation: a buffer and its charge. Its drop records the release.
#[derive(Debug)]
pub(crate) struct Recorded {
    pub(crate) buffer: Vec<u8>,
    _charge: Permit,
    log: Log,
}

impl Recorded {
    pub(crate) fn new(log: &Log, budget: &Budget, buffer: Vec<u8>) -> Self {
        Self {
            buffer,
            _charge: budget.reserve(CHARGE).expect("the test budget has room"),
            log: log.clone(),
        }
    }
}

impl Drop for Recorded {
    fn drop(&mut self) {
        self.log.note("released");
    }
}

pub(crate) const CHARGE: u64 = 10;

pub(crate) fn budget() -> Budget {
    Budget::new(NonZeroU64::MIN.saturating_add(99))
}

/// Does what a driver does with an outcome: it records what came, and it drops the resources that
/// the table gives back.
pub(crate) fn apply(outcome: Outcome<Recorded>, log: &Log) {
    match outcome {
        Outcome::Stale => log.note("stale"),
        Outcome::CancelResult => log.note("cancel result"),
        Outcome::Control(_) => log.note("control"),
        Outcome::Item(_) => log.note("item"),
        Outcome::Ready => log.note("ready"),
        Outcome::Abandoned(ended) => {
            log.note("abandoned");
            drop(ended);
        }
        Outcome::Unclassified => log.note("unclassified"),
    }
}

const fn done(result: i32) -> Event {
    Event { result, more: false }
}

const fn more(result: i32) -> Event {
    Event { result, more: true }
}

fn taken(table: &mut Table<Recorded>) -> Vec<Taken<Recorded>> {
    let mut taken = Vec::new();
    table.take_requests(|request| taken.push(request));
    taken
}

/// The consumer ends its interest while the operation runs: the buffer and the charge stay until
/// the terminal completion. A post from another thread does the same (`posting_a_request_never_allocates`).
#[test]
fn interest_that_ends_first_releases_the_buffer_and_charge_at_the_terminal_completion() {
    let (log, budget) = (Log::default(), budget());
    let mut table = Table::new(4).unwrap();
    let id = table
        .begin(Recorded::new(&log, &budget, Vec::new()), Shape::Single)
        .unwrap();
    table.requests().post(id, Request::EndInterest);
    assert!(taken(&mut table).is_empty());
    assert_eq!(budget.available(), 100 - CHARGE);

    apply(table.complete(id.user_data(), done(5)), &log);
    assert_eq!(log.take(), ["abandoned", "released"]);
    assert_eq!(budget.available(), 100);
}

/// After its slot holds another operation, each transition with an old identity changes nothing,
/// also a second delivery of the old terminal event.
#[test]
fn a_stale_identity_after_slot_reuse_changes_nothing() {
    let (log, budget) = (Log::default(), budget());
    let mut table = Table::new(1).unwrap();
    let old = table
        .begin(Recorded::new(&log, &budget, Vec::new()), Shape::Single)
        .unwrap();
    apply(table.complete(old.user_data(), done(1)), &log);
    apply(table.complete(old.user_data(), done(1)), &log);
    drop(table.claim(old));
    let new = table
        .begin(Recorded::new(&log, &budget, Vec::new()), Shape::Single)
        .unwrap();
    assert_ne!(new, old);
    assert_eq!(log.take(), ["ready", "unclassified", "released"]);

    apply(table.complete(old.user_data(), done(2)), &log);
    assert!(table.claim(old).is_none());
    assert!(table.end_interest(old).is_none());
    assert!(table.withdraw(old).is_none());
    table.requests().post(old, Request::Cancel);
    table.requests().post(old, Request::EndInterest);
    assert!(taken(&mut table).is_empty());
    assert_eq!(log.take(), ["stale"]);

    // The new operation still runs, with its consumer.
    assert!(table.claim(new).is_none());
    apply(table.complete(new.user_data(), done(3)), &log);
    assert_eq!(table.claim(new).map(|ended| ended.result), Some(3));
    assert_eq!(log.take(), ["ready", "released"]);
}

/// A cancel result is not a terminal event, whatever it reports and wherever it comes: only the
/// completion of the target releases its buffer.
#[test]
fn each_cancel_result_waits_for_the_completion_of_its_target() {
    for result in [0, -EALREADY, -ENOENT] {
        for result_first in [true, false] {
            let (log, budget) = (Log::default(), budget());
            let mut table = Table::new(1).unwrap();
            let id = table
                .begin(Recorded::new(&log, &budget, Vec::new()), Shape::Single)
                .unwrap();
            // A dropped receive ends its interest and asks for a cancel.
            table.requests().post(id, Request::EndInterest);
            table.requests().post(id, Request::Cancel);
            assert_matches!(taken(&mut table)[..], [Taken::Cancel(cancel)] if cancel == id);
            let cancel_result = done(result);
            if result_first {
                apply(table.complete(id.cancel_user_data(), cancel_result), &log);
            }
            apply(table.complete(id.user_data(), done(-ECANCELED)), &log);
            if !result_first {
                apply(table.complete(id.cancel_user_data(), cancel_result), &log);
            }
            let expected: &[&str] = if result_first {
                &["cancel result", "abandoned", "released"]
            } else {
                &["abandoned", "released", "cancel result"]
            };
            assert_eq!(log.take(), expected, "cancel result {result}");
        }
    }
}

/// A full table refuses the operation with a typed reason and gives back its resources.
#[test]
fn a_full_table_refuses_and_returns_the_resources() {
    let (log, budget) = (Log::default(), budget());
    let mut table = Table::new(1).unwrap();
    let first = table.begin(Recorded::new(&log, &budget, Vec::new()), Shape::Single);
    let Err(Full(second)) = table.begin(Recorded::new(&log, &budget, b"second".to_vec()), Shape::Single) else {
        panic!("a full table accepts no operation");
    };
    assert_eq!(second.buffer, b"second");
    assert!(log.take().is_empty());
    drop((first, second));

    let Err(Full(resources)) = Table::new(0)
        .unwrap()
        .begin(Recorded::new(&log, &budget, Vec::new()), Shape::Single)
    else {
        panic!("a table with no slot accepts no operation");
    };
    drop(resources);
    assert_eq!(log.take(), ["released", "released"]);
}

/// Each shape of the terminal-event table releases its resources once, at its own terminal
/// event, and the table refuses an event that has no row.
#[test]
fn each_shape_releases_its_resources_once_at_its_terminal_event() {
    let (log, budget) = (Log::default(), budget());
    let mut table = Table::new(1).unwrap();
    let begin =
        |table: &mut Table<Recorded>, shape| table.begin(Recorded::new(&log, &budget, Vec::new()), shape).unwrap();

    // Multishot: items while `more` is set; the last completion is terminal.
    let id = begin(&mut table, Shape::Multishot);
    apply(table.complete(id.user_data(), more(1)), &log);
    assert!(table.end_interest(id).is_none());
    apply(table.complete(id.user_data(), more(2)), &log);
    apply(table.complete(id.user_data(), done(-ECANCELED)), &log);
    assert_eq!(log.take(), ["item", "item", "abandoned", "released"]);

    // A single completion with `more` has no row; the real completion still ends it.
    let id = begin(&mut table, Shape::Single);
    apply(table.complete(id.user_data(), more(1)), &log);
    apply(table.complete(id.user_data(), done(1)), &log);
    drop(table.claim(id));
    assert_eq!(log.take(), ["unclassified", "ready", "released"]);
}

/// A table that drops keeps each resource that the kernel can still use: those of a running
/// operation leak, and the others drop.
#[test]
fn a_dropped_table_leaks_the_resources_that_the_kernel_can_still_use() {
    let (log, budget) = (Log::default(), budget());
    let mut table = Table::new(2).unwrap();
    let mut begin = |shape| table.begin(Recorded::new(&log, &budget, Vec::new()), shape).unwrap();
    let _running = begin(Shape::Single);
    let ended = begin(Shape::Single);
    apply(table.complete(ended.user_data(), done(1)), &log);
    drop(table);
    assert_eq!(log.take(), ["ready", "released"]);
    assert_eq!(budget.available(), 100 - CHARGE);
}

/// The allocation test needs threads and the system allocator. The allocator counts for each
/// unit test of the crate, and `count` gives its count to them.
#[cfg(feature = "std")]
pub(crate) mod allocations {
    use alloc::vec::Vec;
    use core::alloc::{GlobalAlloc, Layout};
    use core::assert_matches;
    use core::cell::Cell;
    use std::alloc::System;
    use std::thread;

    use super::{Log, Recorded, apply, budget, done, taken};
    use crate::table::{Request, Shape, Table, Taken};

    /// The allocations of the current thread, and their bytes. It refuses each allocation from
    /// the count in `REFUSE_FROM` on.
    struct Counting;

    std::thread_local! {
        static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
        static BYTES: Cell<u64> = const { Cell::new(0) };
        static REFUSE_FROM: Cell<u64> = const { Cell::new(u64::MAX) };
        /// The bytes that this thread allocated, less the bytes that it freed.
        static HELD: Cell<isize> = const { Cell::new(0) };
    }

    /// The most runs of [`each_refusal`]: more than the allocations of each operation that a test
    /// gives it.
    const RUNS: u64 = 64;

    #[expect(
        unsafe_code,
        reason = "the test allocator counts each allocation and forwards it to the system allocator"
    )]
    // SAFETY: each method forwards its arguments to the system allocator, which meets the contract.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let count = ALLOCATIONS.get();
            ALLOCATIONS.set(count + 1);
            if count >= REFUSE_FROM.get() {
                return core::ptr::null_mut();
            }
            BYTES.set(BYTES.get() + layout.size() as u64);
            HELD.set(HELD.get() + layout.size().cast_signed());
            // SAFETY: the caller meets the contract of `alloc`.
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            HELD.set(HELD.get() - layout.size().cast_signed());
            // SAFETY: the caller meets the contract of `dealloc`.
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    #[global_allocator]
    static COUNTING: Counting = Counting;

    /// The allocations of the current thread so far.
    pub(crate) fn count() -> u64 {
        ALLOCATIONS.get()
    }

    /// The bytes that the current thread allocated so far.
    pub(crate) fn bytes() -> u64 {
        BYTES.get()
    }

    /// The bytes that the current thread allocated and did not free so far.
    pub(crate) fn held() -> isize {
        HELD.get()
    }

    /// Runs `f`, which this thread runs with each allocation refused. Its one user is a driver
    /// test, which loom does not build.
    #[cfg(all(target_os = "macos", not(loom)))]
    pub(crate) fn refused<T>(f: impl FnOnce() -> T) -> T {
        REFUSE_FROM.set(ALLOCATIONS.get());
        let output = f();
        REFUSE_FROM.set(u64::MAX);
        output
    }

    /// Runs `make` until a run fails no allocation. Run `n` refuses allocation `n` of this thread,
    /// counted from 0, and each later one. Gives the error of each run that failed, and the bytes
    /// that the run kept. The value of the last run drops after the refusals end.
    pub(crate) fn each_refusal<T, E>(mut make: impl FnMut() -> Result<T, E>) -> Vec<(E, isize)> {
        let mut refusals = Vec::new();
        for n in 0..RUNS {
            let held = HELD.get();
            REFUSE_FROM.set(ALLOCATIONS.get() + n);
            let made = make();
            REFUSE_FROM.set(u64::MAX);
            match made {
                Ok(_) => return refusals,
                Err(error) => refusals.push((error, HELD.get() - held)),
            }
        }
        panic!("each of {RUNS} runs failed");
    }

    /// A post from another thread allocates nothing, and the home gets both requests.
    #[test]
    fn posting_a_request_never_allocates() {
        let (log, budget) = (Log::default(), budget());
        let mut table = Table::new(2).unwrap();
        let _other = table
            .begin(Recorded::new(&log, &budget, Vec::new()), Shape::Single)
            .unwrap();
        let id = table
            .begin(Recorded::new(&log, &budget, Vec::new()), Shape::Single)
            .unwrap();
        let requests = table.requests();
        let (counted, allocations) = thread::spawn(move || {
            // The counter sees an allocation of this thread.
            let before = count();
            drop(Vec::<u8>::with_capacity(1));
            let counted = count() - before;
            let before = count();
            requests.post(id, Request::Cancel);
            requests.post(id, Request::EndInterest);
            (counted, count() - before)
        })
        .join()
        .unwrap();
        assert_eq!((counted, allocations), (1, 0));
        assert_matches!(taken(&mut table)[..], [Taken::Cancel(cancel)] if cancel == id);
        assert!(table.claim(id).is_none(), "the end of interest applied");
        apply(table.complete(id.user_data(), done(0)), &log);
        assert_eq!(log.take(), ["abandoned", "released"]);
    }
}
