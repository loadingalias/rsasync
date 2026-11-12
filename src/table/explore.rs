use alloc::vec::Vec;

use super::{Event, Request, Shape, Table, Taken};

/// The risk: a request that another thread posts is lost, or it changes the operation that reused
/// its slot. Two posters race the home, which takes requests, ends the first operation, and
/// begins a second one in the same slot.
#[test]
fn a_posted_request_reaches_its_operation_and_never_the_next_one_of_its_slot() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(|| {
        let mut table = Table::new(1);
        let first = table.begin(1_u8, Shape::Single).expect("one free slot");
        let requests = table.requests();
        let early = loom::thread::spawn(move || requests.post(first, Request::Cancel));

        let mut taken_early = Vec::new();
        table.take_requests(|taken| taken_early.push(taken));
        let ended = Event {
            result: 0,
            more: false,
            notice: false,
        };
        assert_eq!(table.complete(first.user_data(), ended), super::Outcome::Ready);
        assert!(table.claim(first).is_some());
        let second = table.begin(2_u8, Shape::Single).expect("the slot is free again");
        let requests = table.requests();
        let late = loom::thread::spawn(move || requests.post(second, Request::Cancel));

        early.join().expect("the first poster ends");
        late.join().expect("the second poster ends");
        let mut taken_late = Vec::new();
        table.take_requests(|taken| taken_late.push(taken));

        // The first cancel applies only while the first operation runs, and at most once.
        assert!(taken_early.iter().all(|taken| *taken == Taken::Cancel(first)));
        assert!(taken_early.len() <= 1);
        // The second cancel is never lost, and nothing else reaches the second operation.
        assert_eq!(taken_late, [Taken::Cancel(second)]);
    });
}
