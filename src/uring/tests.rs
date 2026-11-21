use core::assert_matches;
use std::thread;
use std::vec::Vec;

use super::{Cqe, Ring, SetupError, Sqe, enter, sys};

const ENTRIES: u32 = 8;

/// `EEXIST` and `EINVAL` (`include/uapi/asm-generic/errno-base.h`).
const EEXIST: i32 = 17;
const EINVAL: i32 = 22;

fn ring() -> Ring {
    Ring::new(ENTRIES, 2 * ENTRIES).unwrap()
}

/// Single issuer and deferred task running are in effect: the kernel refuses a wait from another
/// thread, and the creating thread can wait.
#[test]
fn only_the_creating_thread_can_wait_on_a_ring() {
    let mut ring = ring();
    let fd = ring.fd();
    let other = thread::scope(|scope| {
        scope
            .spawn(|| enter(fd, 0, 0, sys::IORING_ENTER_GETEVENTS))
            .join()
            .unwrap()
    });
    assert_eq!(other.unwrap_err().raw_os_error(), Some(EEXIST));
    assert_eq!(ring.submit(0).unwrap(), 0);
}

/// A message from a ring on another thread reaches a ring with deferred task running. A kernel
/// posts the completion at once, or it queues task work and sets the task-run flag, so that the
/// next submit of the target posts it.
#[test]
#[expect(unsafe_code, reason = "the sender pushes an entry that names no memory")]
fn a_message_from_another_ring_reaches_a_deferred_ring() {
    const DATA: u64 = 0x5eed;
    const RESULT: i32 = 7;
    const SENT: u64 = 1;
    let mut target = ring();
    let fd = target.fd();
    let sent: Vec<Cqe> = thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut sender = ring();
                // SAFETY: the message names no memory, and the target ring stays open until the
                // scope ends.
                unsafe { sender.push(&Sqe::message(fd, DATA, RESULT, SENT)) }.unwrap();
                assert_eq!(sender.submit(1).unwrap(), 1);
                sender.completions().collect()
            })
            .join()
            .unwrap()
    });
    assert_eq!(
        sent,
        [Cqe {
            user_data: SENT,
            res: 0,
            flags: 0
        }]
    );

    let mut received: Vec<Cqe> = target.completions().collect();
    let posted_at_once = !received.is_empty();
    if !posted_at_once {
        assert!(target.needs_enter(), "the kernel shows the deferred message");
        target.submit(1).unwrap();
        received = target.completions().collect();
        assert!(!target.needs_enter());
    }
    std::println!("cross-ring message posted at once: {posted_at_once}");
    assert_eq!(
        received,
        [Cqe {
            user_data: DATA,
            res: RESULT,
            flags: 0
        }]
    );
}

/// The probe reads the opcode table of the kernel, and it leaves the ring as it found it: a second
/// probe gives the same features, and no completion remains.
#[test]
fn the_probe_reads_the_opcode_table_and_leaves_the_ring_empty() {
    let mut ring = ring();
    let opcodes = ring.opcodes().unwrap();
    // Linux 6.1 has both opcodes, and no kernel has an opcode at 255.
    assert!(opcodes.supports(sys::IORING_OP_NOP));
    assert!(opcodes.supports(sys::IORING_OP_MSG_RING));
    assert!(!opcodes.supports(u8::MAX));

    let features = ring.probe().unwrap();
    std::println!("{features:?}");
    assert_eq!(ring.probe().unwrap(), features);
    ring.submit(0).unwrap();
    assert_eq!(ring.completions().count(), 0);
}

/// A failure that is not a refusal stays a failure, so that it does not select epoll: the kernel
/// refuses a ring with no entries with `EINVAL`.
#[test]
fn an_invalid_ring_is_a_failure_and_not_a_refusal() {
    let result = Ring::new(0, 0);
    assert_matches!(&result, Err(SetupError::Failed(error)) if error.raw_os_error() == Some(EINVAL));
}

/// With `kernel.io_uring_disabled` set to 2 (Linux 6.6), creation is the typed refusal that
/// selects epoll.
#[test]
#[ignore = "needs kernel.io_uring_disabled=2, which only root can set"]
fn a_disabled_kernel_refuses_creation() {
    let setting = std::fs::read_to_string("/proc/sys/kernel/io_uring_disabled").unwrap();
    assert_eq!(setting.trim(), "2", "run this test with kernel.io_uring_disabled=2");
    let result = Ring::new(ENTRIES, 2 * ENTRIES);
    assert_matches!(&result, Err(SetupError::Refused(error)) if error.raw_os_error() == Some(sys::EPERM));
}
