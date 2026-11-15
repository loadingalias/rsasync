//! The two backend-neutral suites of the driver: the ownership law, and one case for each
//! operation of the initial surface. Each case runs on each backend of the driver.
//!
//! # Backends
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
//! The suites use only the driver-only surface, because no other surface has I/O types yet. The
//! local I/O types (R307) and the movable I/O types (R403) each add a surface parameter and run both
//! suites on it.
//!
//! # Forms of a case
//!
//! The body of a case stays beside the code of its surface, and this index gives each body its
//! tests: `<suite>::kernel::<case>` and `<suite>::injected::<case>`. A case runs once, as
//! `<suite>::once::<case>`, when the seam can change none of its inputs: a refusal makes no
//! completion, and a signal reaches its stream through the wake descriptor of the driver, not
//! through a completion. Two cases run only under injection, as `law::injected_only::<case>`,
//! because a kernel cannot make them on demand: a completion with a stale generation, and a cancel
//! result that says that its target already runs.
//!
//! # The ownership law
//!
//! The suite `law` holds the negative paths of the R2 exit, and the forgotten futures, abandoned
//! operations, cancellation, close, and teardown of each resource. Each row of the terminal-event
//! table of the design maps to a case of the suite, or to the reason that it does not apply to an
//! io_uring driver:
//!
//! | Terminal-event row | Case, or the reason that no case applies |
//! | --- | --- |
//! | io_uring, single-completion request | `a_forgotten_future_releases_its_buffer_and_charge_only_after_the_terminal_completion` |
//! | io_uring, multishot request | No driver operation is multishot. The table rule runs in `uring::table_tests::a_multishot_operation_releases_at_its_last_completion` on the kernel, and in `table::tests::each_shape_releases_its_resources_once_at_its_terminal_event` under injection |
//! | io_uring, zero-copy send | No driver operation is a zero-copy send before R611. The table rule runs in `table::tests::each_shape_releases_its_resources_once_at_its_terminal_event` |
//! | epoll, `MSG_ZEROCOPY` send | No epoll backend: R503 adds it, and R6 adds its zero-copy send |
//! | IOCP, each of its three rows | No IOCP backend: the Windows backend task of R5 adds it |
//! | Windows IoRing | No IoRing backend: the Windows file task of R5 adds it |
//! | Windows Registered I/O | No Registered I/O: R6 adds it |
//! | epoll and kqueue readiness operations | No readiness backend: R503 and R505 add them |
//! | The blocking service | `a_dropped_spawn_ends_at_the_return_of_its_job_and_its_child_ends_by_its_policy`. Before Linux 6.9, also the length change of the operation suite |
//! | Child wait | `a_dropped_wait_and_child_leave_no_zombie_after_the_exit` |
//!
//! A cancel result is not a terminal event: `law::injected_only` and the table cases show it, with
//! each cancel result and a completion after "not found" (`table::tests` and
//! `uring::table_tests`).
//!
//! # The operations
//!
//! The suite `operation` holds one case or more for each operation of the initial surface. A
//! case of `law` that already does an operation is its case, and this index does not give it a
//! second test.
//!
//! - File
//!   - Open with an explicit cache mode and direct I/O:
//!     `a_misaligned_direct_io_buffer_refuses_before_submission`.
//!   - Positional reads and writes (`law`):
//!     `a_short_read_gives_its_exact_extent_and_the_exact_read_reports_incomplete_progress` and
//!     `a_dropped_write_changes_the_file_and_keeps_its_lease_until_its_completion`.
//!   - Vectored reads and writes: `vectored_writes_and_reads_keep_the_order_of_their_regions`.
//!   - Inspect and change the length:
//!     `a_length_change_runs_on_the_ring_from_linux_6_9_and_on_the_blocking_service_before`.
//!   - Preallocation: `a_preallocation_keeps_or_extends_the_length_and_allocates_its_blocks`.
//!   - Hole punching: `a_punched_hole_reads_as_zeros_keeps_the_length_and_frees_its_blocks`.
//!   - Sync, rename, unlink, directory open and sync, and close:
//!     `a_rename_an_unlink_and_a_directory_sync_complete`.
//! - TCP
//!   - Bind and listen with socket options, and the options:
//!     `each_socket_option_reads_back_the_value_that_was_set`.
//!   - Accept, connect, receive, send, and close (`law`): the cases of their abandoned and failed
//!     forms.
//!   - Vectored send (`law`): `a_vectored_send_gives_its_exact_progress_across_regions`.
//!   - Receive into a selected buffer:
//!     `a_selected_buffer_is_decrypted_in_place_and_returns_to_its_ring_when_its_last_view_drops_elsewhere`
//!     and `idle_connections_with_a_waiting_selecting_receive_hold_no_receive_buffer`.
//!   - Half-close: `a_half_close_ends_the_peer_receive_at_the_sent_count_and_local_receives_go_on`
//!     and `a_half_close_on_a_failed_write_direction_sends_no_end_of_stream`.
//!   - Zero-copy send: no case before R611 adds the operation.
//!   - A listener group for each worker: no case before the local workers of R307.
//! - Local stream
//!   - Bind with mode 0600: `the_socket_file_of_a_local_listener_has_mode_0600_when_its_bind_ends`.
//!   - Bind on an existing path:
//!     `a_bind_refuses_a_live_listener_path_and_replaces_only_a_stale_socket_file` and
//!     `a_path_that_does_not_fit_a_socket_address_gives_invalid_input`.
//!   - Accept, connect, receive, send, vectored send, half-close, and close (`law`):
//!     `a_numbered_stream_crosses_a_local_stream_with_no_gap_through_the_abandoned_operations`.
//!   - The identity of the peer:
//!     `an_accepted_stream_reports_the_user_group_and_process_of_this_process`.
//! - UDP
//!   - Bind with options, and whole datagrams with their metadata:
//!     `send_metadata_sets_the_source_and_ecn_that_the_receive_metadata_reports`.
//!   - Segmented send and coalesced receive:
//!     `a_segmented_send_arrives_as_its_datagrams_and_a_coalesced_receive_reports_its_segment_size`.
//!   - Truncation: `an_oversized_datagram_gives_the_truncation_outcome`.
//!   - Receive into a selected buffer:
//!     `a_selected_datagram_arrives_in_the_slot_of_the_pool_which_returns_at_its_drop`.
//!   - Batches: `a_refused_datagram_ends_its_batch_and_the_report_counts_the_datagrams_before_it`
//!     and `a_batch_receive_returns_several_datagrams_each_with_its_metadata`.
//!   - Half-close: `a_half_close_of_a_udp_socket_refuses_and_changes_nothing`.
//!   - Close (`law`): `a_dropped_receive_leaves_its_datagram_metadata_and_truncation_to_the_next_receive`.
//! - Pipe
//!   - Receive, send, and close (`law`): the pipe cases.
//!   - Adoption: `an_adopted_nonblocking_read_end_waits_for_data` and
//!     `an_adoption_refuses_a_socket_a_wrong_direction_and_a_full_driver`.
//!   - Receive into a selected buffer: `idle_read_ends_with_a_waiting_selecting_receive_hold_no_buffer`.
//!   - Half-close: `a_half_close_of_a_pipe_writer_refuses_and_changes_nothing`.
//!   - The piped streams of a child: `the_caller_writes_the_input_while_both_outputs_drain`.
//! - Child process
//!   - Spawn and wait:
//!     `a_child_exit_status_is_reported_once_with_no_sigchld_handler_and_no_helper_thread` and
//!     `a_full_child_table_and_a_full_blocking_service_give_the_command_back`.
//!   - Adoption: `the_driver_never_reaps_a_child_that_the_test_starts_itself`.
//!   - Signal and kill: `a_kill_ends_the_child_and_a_signal_after_the_reap_refuses`.
//!   - Process tree: `a_tree_kill_ends_a_grandchild_and_a_tree_signal_after_the_reap_refuses`.
//!   - End policy: `the_kill_policy_close_kills_and_reaps_the_child`, and
//!     `the_keep_policy_leaves_a_closed_and_a_dropped_child_running_and_teardown_counts_them`
//!     (`law`).
//! - Signal
//!   - A stream of a signal: `several_raises_before_one_poll_give_one_report` and
//!     `two_streams_of_one_signal_on_two_drivers_both_report`.
//!   - Close: `a_close_restores_the_earlier_disposition_and_other_signals_keep_theirs`.
//!   - Refusals: `a_fault_an_uncatchable_or_a_reserved_signal_and_a_full_registry_refuse`.
//!   - Escalation: `escalation_at_the_second_delivery_ends_a_child_that_never_polls`.

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
                    crate::driver::$($module::)+$case(crate::driver::suite::Backend::Kernel);
                }
            )*
        }

        mod injected {
            $(
                #[test]
                fn $case() {
                    crate::driver::$($module::)+$case(crate::driver::suite::Backend::Injected);
                }
            )*
        }
    };
}

/// Gives each case one test, with no backend parameter.
macro_rules! once {
    ($($($module:ident)::+ => $case:ident),* $(,)?) => {
        mod once {
            $(
                #[test]
                fn $case() {
                    crate::driver::$($module::)+$case();
                }
            )*
        }
    };
}

/// The ownership law.
mod law {
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
        // Abandoned operations: a stream receive and send, a datagram receive, a file write, a
        // spawn, and a child wait.
        tests => a_dropped_receive_leaves_its_bytes_to_the_next_receives_in_order,
        tests => a_dropped_send_that_sends_all_its_bytes_reports_nothing_and_the_next_send_succeeds,
        tests => a_dropped_partial_vectored_send_fails_the_write_direction_with_its_count,
        tests => two_dropped_sends_and_a_third_send_run_in_start_order,
        pipe::tests => a_numbered_stream_crosses_a_pipe_with_no_gap_and_ends_at_the_close_of_the_writer,
        local::tests => a_numbered_stream_crosses_a_local_stream_with_no_gap_through_the_abandoned_operations,
        tests => a_dropped_receive_leaves_its_datagram_metadata_and_truncation_to_the_next_receive,
        file::tests => a_dropped_write_changes_the_file_and_keeps_its_lease_until_its_completion,
        process::tests => a_dropped_spawn_ends_at_the_return_of_its_job_and_its_child_ends_by_its_policy,
        process::tests => a_dropped_wait_and_child_leave_no_zombie_after_the_exit,
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
        // Close and teardown with no live debt.
        tests => close_releases_the_bytes_and_the_connection_that_no_operation_took,
        tests => teardown_cancels_a_read_with_no_data_closes_its_pipe_and_reports_it,
        tests => a_dropped_driver_ends_its_futures_and_keeps_the_buffers_of_running_operations,
        process::tests => the_keep_policy_leaves_a_closed_and_a_dropped_child_running_and_teardown_counts_them,
    }

    // The refused submissions.
    once! {
        tests => a_full_driver_refuses_a_submission_and_returns_its_resources,
        tests => a_batch_larger_than_the_submission_queue_refuses_and_gives_its_datagrams_back,
    }

    /// The cases that a kernel cannot make on demand.
    mod injected_only {
        use alloc::vec::Vec;
        use core::future::Future;
        use core::pin::Pin;
        use core::task::{Context, Waker};
        use std::io::Write as _;
        use std::os::fd::OwnedFd;

        use crate::buffer::{Backing, Buffer as _};
        use crate::driver::Config;
        use crate::driver::suite::{Backend, driver};
        use crate::driver::tests::{drive, posted};
        use crate::table::OpId;

        /// `EALREADY` (`include/uapi/asm-generic/errno.h`): the cancel found its target running.
        const EALREADY: i32 = 114;

        /// Whether one poll of `future` leaves it pending.
        fn pending<F: Future + Unpin>(future: &mut F) -> bool {
            Pin::new(future)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        }

        /// A completion whose user data names a slot that a later operation holds now changes
        /// nothing, because its generation is stale. The later operation gives its own result.
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

        /// A cancel whose result says that its receive already runs ends nothing: the receive
        /// ends at its own completion, which comes later, and gives its own buffer with its bytes,
        /// with no copy.
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

            // The completion waits, and the result of the cancel says that the receive already
            // runs.
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
}

/// The operations of the initial surface.
mod operation {
    cases! {
        // File.
        file::tests => a_misaligned_direct_io_buffer_refuses_before_submission,
        file::tests => vectored_writes_and_reads_keep_the_order_of_their_regions,
        file::tests => a_length_change_runs_on_the_ring_from_linux_6_9_and_on_the_blocking_service_before,
        file::tests => a_preallocation_keeps_or_extends_the_length_and_allocates_its_blocks,
        file::tests => a_punched_hole_reads_as_zeros_keeps_the_length_and_frees_its_blocks,
        file::tests => a_rename_an_unlink_and_a_directory_sync_complete,
        // TCP.
        tests => each_socket_option_reads_back_the_value_that_was_set,
        tests => a_selected_buffer_is_decrypted_in_place_and_returns_to_its_ring_when_its_last_view_drops_elsewhere,
        tests => idle_connections_with_a_waiting_selecting_receive_hold_no_receive_buffer,
        tests => a_half_close_ends_the_peer_receive_at_the_sent_count_and_local_receives_go_on,
        tests => a_half_close_on_a_failed_write_direction_sends_no_end_of_stream,
        // Local stream.
        local::tests => the_socket_file_of_a_local_listener_has_mode_0600_when_its_bind_ends,
        local::tests => a_bind_refuses_a_live_listener_path_and_replaces_only_a_stale_socket_file,
        local::tests => a_path_that_does_not_fit_a_socket_address_gives_invalid_input,
        local::tests => an_accepted_stream_reports_the_user_group_and_process_of_this_process,
        // UDP.
        tests => send_metadata_sets_the_source_and_ecn_that_the_receive_metadata_reports,
        tests => a_segmented_send_arrives_as_its_datagrams_and_a_coalesced_receive_reports_its_segment_size,
        tests => an_oversized_datagram_gives_the_truncation_outcome,
        tests => a_selected_datagram_arrives_in_the_slot_of_the_pool_which_returns_at_its_drop,
        tests => a_refused_datagram_ends_its_batch_and_the_report_counts_the_datagrams_before_it,
        tests => a_batch_receive_returns_several_datagrams_each_with_its_metadata,
        tests => a_half_close_of_a_udp_socket_refuses_and_changes_nothing,
        // Pipe.
        pipe::tests => an_adopted_nonblocking_read_end_waits_for_data,
        pipe::tests => idle_read_ends_with_a_waiting_selecting_receive_hold_no_buffer,
        pipe::tests => a_half_close_of_a_pipe_writer_refuses_and_changes_nothing,
        // Child process.
        process::tests => a_child_exit_status_is_reported_once_with_no_sigchld_handler_and_no_helper_thread,
        process::tests => a_full_child_table_and_a_full_blocking_service_give_the_command_back,
        process::tests => the_driver_never_reaps_a_child_that_the_test_starts_itself,
        process::tests => a_kill_ends_the_child_and_a_signal_after_the_reap_refuses,
        process::tests => a_tree_kill_ends_a_grandchild_and_a_tree_signal_after_the_reap_refuses,
        process::tests => the_caller_writes_the_input_while_both_outputs_drain,
        process::tests => the_kill_policy_close_kills_and_reaps_the_child,
    }

    // A refused adoption, and the signal streams.
    once! {
        pipe::tests => an_adoption_refuses_a_socket_a_wrong_direction_and_a_full_driver,
        signal::tests => several_raises_before_one_poll_give_one_report,
        signal::tests => two_streams_of_one_signal_on_two_drivers_both_report,
        signal::tests => a_close_restores_the_earlier_disposition_and_other_signals_keep_theirs,
        signal::tests => a_fault_an_uncatchable_or_a_reserved_signal_and_a_full_registry_refuse,
        signal::tests => escalation_at_the_second_delivery_ends_a_child_that_never_polls,
    }
}
