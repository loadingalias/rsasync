# Channels

`rsasync::channel` moves values between tasks and threads through a bounded channel. A channel
needs no scheduler, driver, or runtime, so it works also with another executor or with plain
threads. rsasync is in development, and the channels on this page are not in a published release
yet. Select the topology from the transfer that you have:

| Transfer | Module | Constructor |
| --- | --- | --- |
| One sender and one receiver | `rsasync::channel::spsc` | `spsc::channel(&budget, capacity)` |
| Many senders and one receiver | `rsasync::channel::mpsc` | `mpsc::channel(&budget, capacity, senders)` |
| One value, such as a reply (see [One-shot](#one-shot)) | `rsasync::channel::oneshot` | `oneshot::channel(&budget)` |

The SPSC channel costs less for each value, so use it when one owner sends.

## Make a channel

The SPSC and MPSC constructors give a `Sender` and a `Receiver`, or a refusal:

- The channel charges its slots and its shared state to the `Budget` (see [Capacity](capacity.md))
  before it allocates. The charge ends when each endpoint has dropped.
- The capacity is the number of slots, from 1 to 2^27. A capacity of zero is a refusal,
  `Refused::Zero`. It does not make a rendezvous channel or an unbounded channel.
- For MPSC, `senders` is the number of sender places, from 1 to 2^16. Zero is also
  `Refused::Zero`.
- Other refusals are `Refused::TooLarge`, `Refused::Budget` with the `Exhausted` value of the
  budget, and `Refused::Allocation`. A refusal keeps no charge and no memory.

The channel does not examine its values. If a value holds memory, such as a `Vec`, put a `Permit`
for that memory in the value, so that the charge moves with the value.

## Send and receive

Each operation borrows its endpoint mutably, so each endpoint has at most one operation at a
time. No endpoint can be cloned, except an MPSC `Sender` (see [Many senders](#many-senders)). The
receiver gets the values of each sender in the order that the sender published them.

Each operation has a nonblocking form, which never waits, an asynchronous form, which gives a
future, and a blocking form, which needs the `std` feature (on by default). The three forms obey
the same ownership and close rules.

| Operation | Nonblocking | Asynchronous | Blocking |
| --- | --- | --- | --- |
| Send one value | `Sender::try_send` | `Sender::send` gives a `SendFuture` | `Sender::send_blocking` |
| Send a batch | `Sender::try_reserve` gives a `Reservation` | None | None |
| Receive one value | `Receiver::try_recv` | `Receiver::recv` gives a `RecvFuture` | `Receiver::recv_blocking` |

A `Reservation` holds free slots for the sender. `Reservation::push` writes the next slot.
`Reservation::publish` gives the receiver only the slots that are written, in order, and frees the
other slots. Thus one publication sends a batch of values. A reservation larger than the capacity
fails with `ReserveError::TooLarge`. A reservation that does not fit now fails with
`ReserveError::Full`.

This example sends one value and a batch from a thread. The receiver takes each value until the
sender drops:

```rust
use core::num::NonZeroU64;
use std::thread;

use rsasync::capacity::Budget;
use rsasync::channel::spsc;

type Error = Box<dyn core::error::Error + Send + Sync>;

let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
let (mut sender, mut receiver) = spsc::channel::<u32>(&budget, 8)?;

let producer = thread::spawn(move || {
    // One value: wait for a free slot, then send.
    sender.send_blocking(1)?;
    // A batch: reserve 4 slots, write 2, and publish only the 2 written values.
    let mut batch = sender.try_reserve(4)?;
    for value in [2, 3] {
        batch.push(value).expect("the reservation has 4 slots");
    }
    batch.publish()?;
    // The sender drops here, which closes the channel.
    Ok::<(), Error>(())
});

// Each receive waits for the next value. After the last value, it gives `Closed`.
let mut values = Vec::new();
while let Ok(value) = receiver.recv_blocking() {
    values.push(value);
}
assert_eq!(values, [1, 2, 3]);
producer.join().expect("the producer does not panic")?;
```

## Many senders

An MPSC channel has one ring of exactly `capacity` slots, which all of its senders share. A slot
is in use from the send or the reservation that claims it until the receiver takes the value or
passes the slot. A clone of a sender adds no slot, so the bound holds with any number of senders.

- `Sender::try_clone` makes another sender in a free sender place. When each place is in use, it
  gives `TooManySenders`. A place is free again when its sender drops.
- The channel closes when the last sender drops.
- Order: the values of one sender reach the receiver in the order of its sends and publications.
  The channel states no order between the values of different senders.
- Fairness: for each slot that it frees, the receiver wakes one waiting sender, in rotating order
  of the sender places. A sender that does not wait can take a free slot first, so a woken sender
  can find the ring full and wait again.

A `Reservation` holds its place in the order of the channel: the receiver cannot pass its slots
until it ends. Thus one slow reservation stops the receiver for every sender. Fill and publish a
reservation with no wait between `try_reserve` and `publish`.

This example sends from two threads, each with a clone of the sender:

```rust
use core::num::NonZeroU64;
use std::thread;

use rsasync::capacity::Budget;
use rsasync::channel::mpsc;

type Error = Box<dyn core::error::Error + Send + Sync>;

let budget = Budget::new(NonZeroU64::new(64 * 1024).unwrap());
// 4 slots for all senders, and 3 sender places.
let (sender, mut receiver) = mpsc::channel::<(u32, u32)>(&budget, 4, 3)?;

let producers: Vec<_> = (0..2)
    .map(|producer| {
        let mut sender = sender.try_clone()?;
        Ok(thread::spawn(move || {
            for number in 0..10 {
                // Wait for a free slot, then send.
                sender.send_blocking((producer, number))?;
            }
            Ok::<(), Error>(())
        }))
    })
    .collect::<Result<_, Error>>()?;
// The channel closes when the last sender drops.
drop(sender);

let mut next = [0, 0];
while let Ok((producer, number)) = receiver.recv_blocking() {
    // The values of each sender come in the order of its sends.
    assert_eq!(number, next[producer as usize]);
    next[producer as usize] += 1;
}
assert_eq!(next, [10, 10]);
for producer in producers {
    producer.join().expect("a producer does not panic")?;
}
```

## Who owns the value

Each send that fails gives the value back to the caller, or this table names the party that drops
it.

| Outcome | Owner of the value |
| --- | --- |
| A send succeeds | The channel, until the receiver takes it or the receiver drops |
| `try_send` finds the channel full or closed | The caller: `TrySendError::Full` or `TrySendError::Closed` gives it back |
| A waiting send finds the channel closed | The caller: `SendError` gives it back |
| A pending `SendFuture` drops | The future drops it. It was never sent |
| `SendFuture::cancel` | The caller: it gives `Some(value)` if the send did not move the value into the channel, and `None` if it did |
| A value is written into a `Reservation` | The reservation, until `Reservation::publish` |
| A reservation drops with no publication | The reservation drops its values. They were never sent |
| The receiver drops | The receiver drops each value that is still in the channel |
| A failed send: the publication of an accepted reservation comes after the abort | The channel drops the written values, as the abort drops the values in the channel. `Reservation::publish` gives `Aborted` |

A success means that the channel accepted the value. It does not mean that the receiver took the
value. A reservation larger than the capacity is a refusal, `ReserveError::TooLarge`, and moves no
value.

Use `SendFuture::cancel` to stop a waiting send and keep the value, for example when a timeout
comes first. A `SendFuture` is `Unpin`, so poll it through `&mut`, and call `cancel` after that. A
poll of a `SendFuture` after its result gives `Pending`.

## Close the channel

The channel has two close paths: the drain, which delivers each accepted value, and the abort,
which drops them.

| Event | Later sends and reservations | Values in the channel | A pending send |
| --- | --- | --- | --- |
| The last sender drops | None can start | The receiver gets them, then `Closed` | None can exist |
| `Receiver::close` (the drain) | Fail. A send gives its value back | The receiver gets them, then `Closed` | Ends with `SendError` and its value |
| The receiver drops (the abort) | Fail. A send gives its value back | The receiver drops them | Ends with `SendError` and its value |

A `Reservation` that the channel accepted before `Receiver::close` can still publish its values.
The receiver gets `TryRecvError::Empty`, not `TryRecvError::Closed`, while that reservation can
still publish. `Closed` from a receive is terminal: no value is left, and no value can still come.

A `Reservation` that the channel accepted before the abort cannot publish. Its `publish` is a
failed send: it gives `Aborted`, and the channel drops the written values of the reservation.

## Waits

A waiting send waits for a free slot or a close. A waiting receive waits for a value, a close, or
the end of an accepted reservation. The channel keeps the waker of the latest poll of a wait, and
the send, receive, or close that ends the wait wakes that waker. A dropped `SendFuture` or
`RecvFuture` ends its interest.

With no waiting endpoint, an SPSC transfer costs one write of a slot and one atomic
read-modify-write on each side, and it wakes nothing. The read-modify-write also carries the close
flags and the waiting bit, so a close and a send have one order. [rtrb](https://docs.rs/rtrb/0.4.1/rtrb/)
publishes each position with a plain store, but it has no close and no wait. A wait needs a
store-load order on the publication path: the publisher stores the position and then loads the
waiting bit. One read-modify-write takes the place of the store, a fence, and the load.

An MPSC send costs one compare-exchange of the claimed position, which all senders share, and
one swap of the state of its slot. An MPSC receive costs one swap of the read position.

## One-shot

`rsasync::channel::oneshot` moves one value from one sender to one receiver, such as a reply, a
join result, or a startup outcome. `oneshot::channel(&budget)` gives a `Sender` and a `Receiver`.
It charges its shared state to the budget before it allocates, and the charge ends when both
endpoints drop. A refusal is `Refused::Budget`.

`Sender::send(value)` takes the sender, so it sends at most one value. It never waits: the channel
has room for the one value. The receiver has the three forms of a receive: `try_recv`, the future
of `recv`, and `recv_blocking`, which needs the `std` feature.

| Outcome | Owner of the value |
| --- | --- |
| `send` succeeds | The channel, until the receiver takes it or the receiver drops |
| `send` finds the receiver gone | The caller: `SendError` gives the value back |
| The receiver drops before it takes a sent value | The receiver drops the value |
| A `RecvFuture` drops | The channel keeps the value: a later receive takes it |

A drop of the sender with no send closes the channel: a receive gives `Closed`, and
`try_recv` gives `TryRecvError::Closed`. After the receiver took the value, each later receive
gives `Closed` too. A send and a drop of the receiver change one word with one atomic
read-modify-write each, so the first of them decides the owner: the value never has two owners
and is never lost.

```rust
use core::num::NonZeroU64;
use std::thread;

use rsasync::capacity::Budget;
use rsasync::channel::{Closed, oneshot};

let budget = Budget::new(NonZeroU64::new(4096).unwrap());
let (sender, mut receiver) = oneshot::channel::<u32>(&budget)?;

let worker = thread::spawn(move || sender.send(7));
assert_eq!(receiver.recv_blocking(), Ok(7));
assert!(worker.join().expect("the worker does not panic").is_ok());
// The one value is taken, and no other can come.
assert_eq!(receiver.recv_blocking(), Err(Closed));
```

The example is the example of the `rsasync::channel::oneshot` documentation, which `cargo xtask
test` runs.
