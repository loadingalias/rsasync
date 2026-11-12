//! The operation table with the completions of a real ring. Each case also runs with injected
//! completions in `table::tests`, with the same release record.

use std::io::{Write as _, pipe};
use std::net::UdpSocket;
use std::os::fd::{AsRawFd as _, RawFd};
use std::vec::Vec;

use super::{Ring, Sqe};
use crate::table::tests::{ECANCELED, ENOENT, Log, Recorded, apply, budget};
use crate::table::{CONTROL, OpId, Request, Shape, Table, Taken};

/// The opcodes and flags of the test operations (`include/uapi/linux/io_uring.h` of Linux 6.1),
/// and `POLLIN` (`include/uapi/asm-generic/poll.h`).
const IORING_OP_POLL_ADD: u8 = 6;
const IORING_OP_SEND_ZC: u8 = 47;
const IORING_POLL_ADD_MULTI: u32 = 1 << 0;
const POLLIN: u32 = 0x1;

/// A ring and its table, driven as a home driver drives them.
struct Driver {
    ring: Ring,
    table: Table<Recorded>,
    log: Log,
}

impl Driver {
    fn new(slots: u32) -> Self {
        Self {
            ring: Ring::new(8, 16).expect("the test host allows io_uring"),
            table: Table::new(slots),
            log: Log::default(),
        }
    }

    /// Records the operation in the table, then pushes its entry with its user data.
    #[expect(unsafe_code, reason = "the table keeps the memory that the entry names")]
    fn start(&mut self, resources: Recorded, shape: Shape, entry: Sqe) -> OpId {
        let id = self
            .table
            .begin(resources, shape)
            .expect("the test table has a free slot");
        let entry = Sqe {
            user_data: id.user_data(),
            ..entry
        };
        // SAFETY: the table keeps the resources, and with them the buffer that the entry names,
        // until the terminal event of the operation. Each descriptor stays open until the end of
        // the test.
        unsafe { self.ring.push(&entry) }.expect("the test ring has a free entry");
        id
    }

    /// Takes the posted requests and pushes a cancel for each running target.
    #[expect(unsafe_code, reason = "a cancel names no memory")]
    fn take_requests(&mut self) {
        let mut cancels = Vec::new();
        let log = &self.log;
        self.table.take_requests(|taken| match taken {
            Taken::Cancel(id) => cancels.push(id),
            Taken::Abandoned(_, ended) => {
                log.note("abandoned");
                drop(ended);
            }
        });
        for id in cancels {
            // SAFETY: a cancel names no memory.
            unsafe { self.ring.push(&Sqe::cancel(id.user_data(), id.cancel_user_data())) }
                .expect("the test ring has a free entry");
        }
    }

    /// Submits, waits for `wait` completions, and applies each one. Gives their results.
    fn turn(&mut self, wait: u32) -> Vec<i32> {
        self.ring.submit(wait).expect("the kernel takes the entries");
        let mut results = Vec::new();
        for cqe in self.ring.completions() {
            results.push(cqe.res);
            apply(self.table.complete(cqe.user_data, cqe.event()), &self.log);
        }
        results.sort_unstable();
        results
    }
}

fn read(fd: RawFd, buffer: &mut Vec<u8>) -> Sqe {
    let len = u32::try_from(buffer.capacity()).expect("a test buffer is small");
    Sqe::read(fd, buffer.as_mut_ptr(), len)
}

/// The two orders in which a cancel result and the completion of its target can come.
fn released_after_the_target(log: &Log) -> bool {
    let log = log.take();
    log == ["cancel result", "abandoned", "released"] || log == ["abandoned", "released", "cancel result"]
}

/// A consumer ends its interest while a read waits for data: the buffer and the charge stay until
/// the kernel completes the read.
#[test]
fn interest_that_ends_first_releases_at_the_completion_of_the_kernel() {
    let (mut driver, budget) = (Driver::new(4), budget());
    let (reader, mut writer) = pipe().unwrap();
    let mut resources = Recorded::new(&driver.log, &budget, Vec::with_capacity(16));
    let entry = read(reader.as_raw_fd(), &mut resources.buffer);
    let id = driver.start(resources, Shape::Single, entry);
    assert!(driver.turn(0).is_empty());
    driver.table.requests().post(id, Request::EndInterest);
    driver.take_requests();
    assert!(driver.turn(0).is_empty());
    assert_eq!(budget.available(), 90);

    writer.write_all(b"bytes").unwrap();
    assert_eq!(driver.turn(1), [5]);
    assert_eq!(driver.log.take(), ["abandoned", "released"]);
    assert_eq!(budget.available(), 100);
}

/// The kernel posts a completion with the identity of an ended operation, whose slot now holds a
/// read: the completion changes nothing, and the read still completes to its consumer.
#[test]
#[expect(unsafe_code, reason = "the message names no memory")]
fn a_stale_identity_from_the_kernel_changes_nothing() {
    let (mut driver, budget) = (Driver::new(1), budget());
    let old = driver.start(
        Recorded::new(&driver.log, &budget, Vec::new()),
        Shape::Single,
        Sqe::nop(),
    );
    assert_eq!(driver.turn(1), [0]);
    drop(driver.table.claim(old));
    let (reader, mut writer) = pipe().unwrap();
    let mut resources = Recorded::new(&driver.log, &budget, Vec::with_capacity(16));
    let entry = read(reader.as_raw_fd(), &mut resources.buffer);
    let new = driver.start(resources, Shape::Single, entry);

    let message = Sqe::message(driver.ring.fd(), old.user_data(), 7, CONTROL);
    // SAFETY: the message names no memory.
    unsafe { driver.ring.push(&message) }.unwrap();
    assert_eq!(driver.turn(2), [0, 7]);
    let mut log = driver.log.take();
    log.sort_unstable();
    assert_eq!(log, ["control", "ready", "released", "stale"]);

    writer.write_all(b"bytes").unwrap();
    assert_eq!(driver.turn(1), [5]);
    assert_eq!(driver.table.claim(new).map(|ended| ended.result), Some(5));
    assert_eq!(driver.log.take(), ["ready", "released"]);
}

/// A cancel that finds its target ("done") and one whose target already completed ("not found"):
/// each time, only the completion of the target releases its buffer.
#[test]
fn a_cancel_result_waits_for_the_completion_of_its_target() {
    let (mut driver, budget) = (Driver::new(2), budget());
    let (reader, _writer) = pipe().unwrap();
    let mut resources = Recorded::new(&driver.log, &budget, Vec::with_capacity(16));
    let entry = read(reader.as_raw_fd(), &mut resources.buffer);
    let reading = driver.start(resources, Shape::Single, entry);
    assert!(driver.turn(0).is_empty());
    driver.table.requests().post(reading, Request::EndInterest);
    driver.table.requests().post(reading, Request::Cancel);
    driver.take_requests();
    assert_eq!(driver.turn(2), [-ECANCELED, 0]);
    assert!(released_after_the_target(&driver.log));

    // The no-op completes in the same submission, before its cancel looks for it.
    let nop = driver.start(
        Recorded::new(&driver.log, &budget, Vec::new()),
        Shape::Single,
        Sqe::nop(),
    );
    driver.table.requests().post(nop, Request::EndInterest);
    driver.table.requests().post(nop, Request::Cancel);
    driver.take_requests();
    assert_eq!(driver.turn(2), [-ENOENT, 0]);
    assert!(released_after_the_target(&driver.log));
    assert_eq!(budget.available(), 100);
}

/// A multishot poll gives items while the kernel sets `IORING_CQE_F_MORE`; its last completion,
/// after a cancel, releases its resources.
#[test]
fn a_multishot_operation_releases_at_its_last_completion() {
    let (mut driver, budget) = (Driver::new(2), budget());
    let (reader, mut writer) = pipe().unwrap();
    let id = driver.start(
        Recorded::new(&driver.log, &budget, Vec::new()),
        Shape::Multishot,
        Sqe {
            opcode: IORING_OP_POLL_ADD,
            fd: reader.as_raw_fd(),
            len: IORING_POLL_ADD_MULTI,
            rw_flags: POLLIN,
            ..Sqe::default()
        },
    );
    for byte in [b"x", b"y"] {
        writer.write_all(byte).unwrap();
        assert_eq!(driver.turn(1), [i32::try_from(POLLIN).unwrap()]);
    }
    driver.table.requests().post(id, Request::EndInterest);
    driver.table.requests().post(id, Request::Cancel);
    driver.take_requests();
    assert_eq!(driver.turn(2), [-ECANCELED, 0]);
    let log = driver.log.take();
    assert!(
        log == ["item", "item", "cancel result", "abandoned", "released"]
            || log == ["item", "item", "abandoned", "released", "cancel result"],
        "{log:?}"
    );
}

/// A zero-copy send ends at its send result, and its notice releases its buffer.
#[test]
fn a_zero_copy_send_releases_its_buffer_at_the_notice() {
    let (mut driver, budget) = (Driver::new(1), budget());
    let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    sender.connect(receiver.local_addr().unwrap()).unwrap();
    let mut resources = Recorded::new(&driver.log, &budget, b"datagram".to_vec());
    let entry = Sqe {
        opcode: IORING_OP_SEND_ZC,
        fd: sender.as_raw_fd(),
        addr: resources.buffer.as_mut_ptr().addr() as u64,
        len: 8,
        ..Sqe::default()
    };
    let id = driver.start(resources, Shape::ZeroCopy, entry);
    assert_eq!(driver.turn(2), [0, 8]);
    assert_eq!(driver.log.take(), ["ready", "notice", "released"]);
    let claim = driver.table.claim(id).unwrap();
    assert_eq!((claim.result, claim.resources.is_none()), (8, true));
    let mut received = [0; 16];
    assert_eq!(receiver.recv(&mut received).unwrap(), 8);
    assert_eq!(&received[..8], b"datagram");
}
