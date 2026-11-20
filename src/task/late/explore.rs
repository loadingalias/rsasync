//! The named risks: a read that finds the value of the cell, with no other order with its set, reads
//! the value before the writes that made it; the detach frees the value while a read uses it.
//! Each value is a loom `UnsafeCell` that the setter writes before the set, and whose drop writes
//! it. Loom reports a read that is not ordered after those writes.

use super::{Late, Read};
use crate::sync::{Arc, UnsafeCell};

/// A value whose making and whose drop each write its cell.
struct Value(UnsafeCell<u32>);

// SAFETY: loom checks the order of each access to the cell, which is what the tests check.
#[expect(unsafe_code, reason = "loom checks the order of each access to the cell")]
unsafe impl Sync for Value {}

impl Value {
    fn new() -> Self {
        let cell = UnsafeCell::new(0);
        cell.with_mut(|value| {
            // SAFETY: the value is new, so only this thread has it.
            #[expect(unsafe_code, reason = "the value is new, so only this thread has it")]
            unsafe {
                *value = 7;
            }
        });
        Self(cell)
    }

    #[expect(unsafe_code, reason = "loom checks the order of each access to the cell")]
    fn read(&self) -> u32 {
        // SAFETY: loom reports an access that is not ordered after the last write.
        self.0.with(|value| unsafe { *value })
    }
}

impl Drop for Value {
    #[expect(unsafe_code, reason = "loom checks the order of each access to the cell")]
    fn drop(&mut self) {
        // SAFETY: loom reports a write that a read can race.
        self.0.with_mut(|value| unsafe { *value = 0 });
    }
}

/// Checks `f` in each thread order within `bound` preemptions.
fn model(bound: usize, f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(bound);
    model.check(f);
}

#[test]
fn a_read_that_finds_the_value_sees_each_write_that_made_it() {
    model(3, || {
        let late = Arc::new(Late::<Value>::new(None).unwrap());
        let reader = loom::thread::spawn({
            let late = Arc::clone(&late);
            move || late.with(Value::read)
        });
        late.set(Value::new()).unwrap();
        let read = reader.join().unwrap();
        assert!(matches!(read, Read::Empty | Read::Value(7)), "{read:?}");
    });
}

#[test]
fn the_detach_frees_the_value_only_after_each_read_ends() {
    model(3, || {
        let late = Arc::new(Late::new(Some(Value::new())).unwrap());
        let reader = loom::thread::spawn({
            let late = Arc::clone(&late);
            move || late.with(Value::read)
        });
        late.detach();
        let read = reader.join().unwrap();
        assert!(matches!(read, Read::Value(7) | Read::Detached), "{read:?}");
        assert_eq!(late.with(Value::read), Read::Detached);
    });
}
