//! The negative-path suite of the R2 exit: the index of the cases that show each failure and
//! abandon path of the driver surface, with each case on the kernel and under injected
//! completions.
//!
//! [`Backend::Kernel`] gives each completion to the driver as the kernel posts it.
//! [`Backend::Injected`] uses the completion-injection seam of the ring (`Ring::inject`), at the
//! boundary of the backend. The ring keeps each completion that the kernel posts and gives it at
//! the next take, after the driver applied the requests of that turn. So a drop, a cancel, or a
//! close always comes before the completion of its operation, with no dependence on the timing of
//! the kernel. A case can also hold the completions of one operation, replace a result, or add a
//! completion that the kernel did not post. The seam supplies inputs only: the table, the harvest,
//! and the stream rules run as in production. The seam exists only in the tests, and no public
//! injection API exists.
//!
//! The body of a case stays beside the code of its surface, and this index gives each body its
//! tests: `kernel::<case>` and `injected::<case>`. A refusal makes no completion, so its case runs
//! once, as `once::<case>`. Two cases run only under injection, as `injected_only::<case>`,
//! because a kernel cannot make them on demand: a completion with a stale generation, and a cancel
//! result that says that its target already runs. The table cases of the same rules, with each
//! cancel result and a completion after "not found", are in `table::tests` and
//! `uring::table_tests`.

use super::{Config, Driver};

/// Where the completions of a case come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Backend {
    /// The kernel, as it posts them.
    Kernel,
    /// The completion-injection seam: each completion comes at the take after the one that found
    /// it, unless the case holds, replaces, or adds a completion.
    Injected,
}

/// A driver of `config` that takes its completions from `backend`.
pub(super) fn driver(config: Config, backend: Backend) -> Driver {
    let mut driver = super::tests::driver(config);
    if backend == Backend::Injected {
        driver.ring.inject();
    }
    driver
}

/// Gives each case a test on the kernel and a test under injection.
macro_rules! cases {
    ($($($module:ident)::+ => $case:ident),* $(,)?) => {
        mod kernel {
            $(
                #[test]
                fn $case() {
                    crate::driver::$($module::)+$case(super::Backend::Kernel);
                }
            )*
        }

        mod injected {
            $(
                #[test]
                fn $case() {
                    crate::driver::$($module::)+$case(super::Backend::Injected);
                }
            )*
        }
    };
}

cases! {
    // Short I/O.
    tests => a_dropped_partial_send_fails_the_write_direction_and_reports_its_count_once,
    tests => a_vectored_send_gives_its_exact_progress_across_regions,
    file::tests => a_short_read_gives_its_exact_extent_and_the_exact_read_reports_incomplete_progress,
    // Failed I/O.
    tests => a_connect_to_a_port_with_no_listener_is_refused,
    tests => a_selecting_receive_with_no_free_buffer_gives_enobufs,
    tests => a_dropped_receive_that_failed_leaves_its_error_to_the_next_receive,
    pipe::tests => a_send_after_the_reader_closed_gives_broken_pipe,
    // The return of a writable region, with its exact extent, at its own address.
    tests => a_request_into_an_owned_buffer_and_a_response_from_a_lease_keep_their_addresses,
    // Forgotten futures.
    tests => a_forgotten_future_releases_its_buffer_and_charge_only_after_the_terminal_completion,
    tests => a_forgotten_receive_leaves_its_bytes_to_the_next_receive_which_gives_them_before_it_submits,
    // An abandoned stream receive and send.
    tests => a_dropped_receive_leaves_its_bytes_to_the_next_receives_in_order,
    tests => a_dropped_send_that_sends_all_its_bytes_reports_nothing_and_the_next_send_succeeds,
    tests => two_dropped_sends_and_a_third_send_run_in_start_order,
    pipe::tests => a_numbered_stream_crosses_a_pipe_with_no_gap_and_ends_at_the_close_of_the_writer,
    local::tests => a_numbered_stream_crosses_a_local_stream_with_no_gap_through_the_abandoned_operations,
    // An abandoned accept.
    tests => a_dropped_accept_leaves_its_connection_to_the_next_accept,
    // A late completion: it comes after the end of the interest in it.
    tests => a_dropped_connect_that_completes_closes_its_connection,
    // Cancel while running.
    tests => an_explicit_cancel_of_a_receive_gives_its_buffer_and_bytes_with_no_copy,
    tests => an_explicit_cancel_of_a_send_gives_its_progress_and_keeps_the_direction_usable,
    // Close while a peer refuses to read.
    tests => close_cancels_each_dropped_operation_by_its_own_request_and_reports_the_send,
    pipe::tests => a_close_returns_while_the_other_end_is_idle_and_reports_the_cancelled_send,
    // Close with no live debt.
    tests => close_releases_the_bytes_and_the_connection_that_no_operation_took,
    tests => teardown_cancels_a_read_with_no_data_closes_its_pipe_and_reports_it,
}

/// The refused submissions, which make no completion.
mod once {
    #[test]
    fn a_full_driver_refuses_a_submission_and_returns_its_resources() {
        crate::driver::tests::a_full_driver_refuses_a_submission_and_returns_its_resources();
    }

    #[test]
    fn a_batch_larger_than_the_submission_queue_refuses_and_gives_its_datagrams_back() {
        crate::driver::tests::a_batch_larger_than_the_submission_queue_refuses_and_gives_its_datagrams_back();
    }
}

/// The cases that a kernel cannot make on demand.
mod injected_only {
    use alloc::vec::Vec;
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Waker};
    use std::io::Write as _;
    use std::os::fd::OwnedFd;

    use super::super::Config;
    use super::super::tests::{drive, posted};
    use super::{Backend, driver};
    use crate::buffer::{Backing, Buffer as _};
    use crate::table::OpId;

    /// `EALREADY` (`include/uapi/asm-generic/errno.h`): the cancel found its target running.
    const EALREADY: i32 = 114;

    /// Whether one poll of `future` leaves it pending.
    fn pending<F: Future + Unpin>(future: &mut F) -> bool {
        Pin::new(future)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    }

    /// A completion whose user data names a slot that a later operation holds now changes nothing,
    /// because its generation is stale. The later operation gives its own result.
    #[test]
    fn a_stale_completion_after_the_slot_is_used_again_changes_nothing() {
        let mut driver = driver(Config::new().operations(1), Backend::Injected);
        let (read, mut write) = std::io::pipe().expect("the test host makes a pipe");
        let mut reader = driver.adopt_reader(OwnedFd::from(read)).expect("the driver has room");
        let first = driver
            .recv(&mut reader, Backing::from(Vec::with_capacity(8)))
            .expect("the driver has room");
        write.write_all(b"a").expect("the pipe has room");
        let stale = posted(&mut driver, 1)[0];
        assert_eq!(drive(&mut driver, first).expect("the driver runs").1.bytes(), b"a");

        // The one operation slot takes the next receive, with a new generation.
        let mut second = driver
            .recv(&mut reader, Backing::from(Vec::with_capacity(8)))
            .expect("the driver has room");
        let running = driver.table.running().next().expect("the receive runs");
        let old = OpId::from_user_data(stale.user_data).expect("the completion of an operation");
        assert_eq!(running.index(), old.index());
        assert_ne!(running.generation(), old.generation());
        driver.ring.add(stale);
        while driver.turn().expect("a turn works").more() {}
        assert!(pending(&mut second), "the stale completion ended the receive");

        write.write_all(b"b").expect("the pipe has room");
        assert_eq!(drive(&mut driver, second).expect("the driver runs").1.bytes(), b"b");
    }

    /// A cancel whose result says that its receive already runs ends nothing: the receive ends at
    /// its own completion, which comes later, and gives its own buffer with its bytes, with no copy.
    #[test]
    fn a_cancel_that_finds_its_receive_running_waits_for_the_completion_of_the_receive() {
        let mut driver = driver(Config::new(), Backend::Injected);
        let (read, mut write) = std::io::pipe().expect("the test host makes a pipe");
        let mut reader = driver.adopt_reader(OwnedFd::from(read)).expect("the driver has room");
        let buffer = Backing::from(Vec::with_capacity(64));
        let address = buffer.as_ptr();
        let mut receive = driver.recv(&mut reader, buffer).expect("the driver has room");
        write.write_all(b"hello").expect("the pipe has room");
        let completion = posted(&mut driver, 1)[0];
        let id = OpId::from_user_data(completion.user_data).expect("the completion of an operation");

        // The completion waits, and the result of the cancel says that the receive already runs.
        driver.ring.hold(completion.user_data);
        driver.ring.replace(id.cancel_user_data(), -EALREADY);
        receive.cancel();
        while driver.turn().expect("a turn works").more() {}
        assert!(pending(&mut receive), "the result of the cancel ended the receive");

        driver.ring.release(completion.user_data);
        let (count, buffer) = drive(&mut driver, receive).expect("the driver runs");
        let count = count.expect("the receive works");
        assert_eq!((count, buffer.as_ptr(), buffer.bytes()), (5, address, &b"hello"[..]));
        assert_eq!(driver.copied(), 0);
    }
}
