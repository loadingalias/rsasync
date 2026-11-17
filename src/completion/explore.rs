use alloc::task::Wake;
use core::assert_matches;
use core::task::{Context, Poll, Waker};

use super::{Entry, Gone};
use crate::sync::{Arc, thread};
use crate::table::{OpId, Shape, Table};

/// An operation of a table with one slot.
fn operation() -> OpId {
    Table::new(1).begin((), Shape::Single).expect("one free slot")
}

/// Unparks a loom thread.
struct Unpark(thread::Thread);

impl Wake for Unpark {
    fn wake(self: alloc::sync::Arc<Self>) {
        self.0.unpark();
    }
}

/// The consumer: polls `entry` for `id` until it gives the result, and parks between polls.
fn take(entry: &Entry<u8>, id: OpId) -> Result<(i32, u8), Gone> {
    let waker = Waker::from(alloc::sync::Arc::new(Unpark(thread::current())));
    let cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(taken) = entry.poll_take(&cx, id) {
            return taken;
        }
        thread::park();
    }
}

fn model(test: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(test);
}

/// The risk: the consumer on another thread misses the wake of the home and waits for ever, or
/// it reads the resources before the home wrote them.
#[test]
fn a_consumer_on_another_thread_takes_the_published_result() {
    model(|| {
        let id = operation();
        let entry = Arc::new(Entry::new());
        entry.start(id, 7_u8, |_| ());
        let consumer = {
            let entry = Arc::clone(&entry);
            thread::spawn(move || take(&entry, id))
        };
        entry.publish(id, 5);
        assert_eq!(consumer.join().expect("the consumer ends"), Ok((5, 7)));
    });
}

/// The risk: the driver ends while the consumer waits, and the consumer waits for ever, or the
/// home and the consumer both take the resources.
#[test]
fn a_driver_that_ends_wakes_its_consumer_and_one_of_the_two_takes_the_resources() {
    model(|| {
        let id = operation();
        let entry = Arc::new(Entry::new());
        entry.start(id, 7_u8, |_| ());
        let consumer = {
            let entry = Arc::clone(&entry);
            thread::spawn(move || take(&entry, id))
        };
        entry.publish(id, 5);
        entry.close();
        let taken = consumer.join().expect("the consumer ends");
        assert_matches!(taken, Ok((5, 7)) | Err(Gone));
    });
}
