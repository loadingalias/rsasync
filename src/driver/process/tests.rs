//! Each test runs in a process of its own (nextest), so the children, the threads, and the signal
//! state of one test do not reach another.

use alloc::string::String;
use alloc::vec::Vec;
use core::assert_matches;
use core::cell::RefCell;
use core::ffi::c_int;
use core::future::Future;
use core::num::NonZeroU64;
use core::pin::{Pin, pin};
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::io::{BufRead as _, BufReader};
use std::os::fd::{AsRawFd as _, OwnedFd};
use std::os::unix::process::ExitStatusExt as _;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use super::super::suite::{self, Backend};
use super::super::tests::{drive, driver, numbered};
use super::super::{Call, Config, Driver, PipeReader, Reason, Route, Signal, sys};
use super::{Child, ChildOptions, End, SignalChildError, pidfd_open};
use crate::blocking::{self, Service};
use crate::buffer::{Backing, Buffer as _, Lease};
use crate::capacity::Budget;
use crate::sync::Arc;

/// `P_PID` (`include/uapi/linux/wait.h`): a `waitid` of one process ID.
const P_PID: c_int = 1;

/// `ESRCH` (`include/uapi/asm-generic/errno-base.h`).
const ESRCH: i32 = 3;

/// A driver on `backend` with a blocking service of one thread and `jobs` job slots, for its
/// spawns.
fn with_service(config: Config, jobs: u32, backend: Backend) -> (Driver, Service) {
    let budget = Budget::new(NonZeroU64::new(64 << 20).expect("not 0"));
    let service = Service::new(&budget, blocking::Config::new().threads(1).jobs(jobs).stack(256 << 10))
        .expect("the test budget holds the service");
    let mut driver = suite::driver(config, backend);
    driver.use_blocking(service.submitter());
    (driver, service)
}

/// A command that runs `script` in the shell.
fn sh(script: &str) -> Command {
    let mut command = Command::new("sh");
    command.args(["-c", script]);
    command
}

/// A command that runs for 10 minutes, or until a signal ends it.
fn sleeper() -> Command {
    let mut command = Command::new("sleep");
    command.arg("600");
    command
}

/// Spawns `command` with `options` and drives the driver until the spawn ends.
fn spawn(driver: &mut Driver, command: Command, options: ChildOptions) -> Child {
    let spawn = driver
        .spawn(command, options)
        .expect("the test driver admits the spawn");
    drive(driver, spawn)
        .expect("the driver runs")
        .expect("the test host starts the command")
}

/// The state of the process `pid` for this process, as `waitid` shows it with no reap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Running,
    Zombie,
    /// No child of this process has the process ID: something reaped it.
    Reaped,
}

#[expect(unsafe_code, reason = "the call writes only the structure that it gets")]
fn state(pid: u32) -> State {
    let mut info = sys::SigInfo::default();
    let options = sys::WEXITED | sys::WNOHANG | sys::WNOWAIT;
    // SAFETY: the call writes only `info`.
    match unsafe { sys::waitid(P_PID, pid, &raw mut info, options) } {
        0 if info.pid == 0 => State::Running,
        0 => State::Zombie,
        _ => State::Reaped,
    }
}

/// The threads of this process.
fn threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("the test host has procfs")
        .count()
}

/// Whether this process catches or ignores `SIGCHLD`: bit 16 of `SigCgt` or of `SigIgn` in
/// `/proc/self/status`.
fn sigchld_changed() -> bool {
    let status = std::fs::read_to_string("/proc/self/status").expect("the test host has procfs");
    status
        .lines()
        .filter_map(|line| line.strip_prefix("SigCgt:").or_else(|| line.strip_prefix("SigIgn:")))
        .map(|mask| u64::from_str_radix(mask.trim(), 16).expect("procfs gives a hexadecimal mask"))
        .any(|mask| mask & (1 << 16) != 0)
}

/// Whether `fd` becomes readable within `timeout` milliseconds.
#[expect(unsafe_code, reason = "a poll writes only the structure that it gets")]
fn readable(fd: &OwnedFd, timeout: c_int) -> bool {
    let mut poll = sys::PollFd {
        fd: fd.as_raw_fd(),
        events: sys::POLLIN,
        revents: 0,
    };
    // SAFETY: the call writes only `poll`.
    (unsafe { sys::poll(&raw mut poll, 1, timeout) }) == 1
}

/// Turns the driver until `done` holds, at most 10 s. A thread wakes the driver each 10 ms, so a
/// wait never blocks for longer when no event comes. Gives whether `done` held.
fn drive_until(driver: &mut Driver, mut done: impl FnMut() -> bool) -> bool {
    let (wake, stop) = (driver.wake_handle(), Arc::new(AtomicBool::new(false)));
    let ticker = {
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(10));
                wake.wake();
            }
        })
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let held = loop {
        let more = driver.turn().expect("a turn of the test driver works").more();
        if done() {
            break true;
        }
        if Instant::now() > deadline {
            break false;
        }
        if !more {
            driver.wait().expect("the wait of the test driver works");
        }
    };
    stop.store(true, Ordering::Relaxed);
    ticker.join().expect("the ticker ends");
    held
}

/// Receives on `reader` until the end of the stream.
async fn receive_all(driver: &RefCell<Driver>, reader: &mut PipeReader) -> Vec<u8> {
    let mut received = Vec::new();
    loop {
        let receive = driver
            .borrow_mut()
            .recv(reader, Backing::from(Vec::with_capacity(1 << 16)))
            .expect("the test driver has room");
        let (count, buffer) = receive.await.expect("the driver runs");
        if count.expect("the receive works") == 0 {
            return received;
        }
        received.extend_from_slice(buffer.bytes());
    }
}

/// The output of `future` when one poll gives it.
fn poll_ready<F: Future>(future: Pin<&mut F>, cx: &mut Context<'_>) -> Option<F::Output> {
    match future.poll(cx) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

/// The exit status is the one of the child, the driver installs no `SIGCHLD` handler, and no
/// thread waits for the child. A second wait gives the same status at once.
pub(in crate::driver) fn a_child_exit_status_is_reported_once_with_no_sigchld_handler_and_no_helper_thread(
    backend: Backend,
) {
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let before = threads();
    let mut command = sh("read line; exit 3");
    command.stdin(Stdio::piped());
    let mut child = spawn(&mut driver, command, ChildOptions::new());
    assert!(poll_ready(pin!(child.wait()), &mut Context::from_waker(Waker::noop())).is_none());
    assert_eq!(threads(), before, "no thread waits for the child");
    assert!(!sigchld_changed(), "the disposition of SIGCHLD is the default");

    let stdin = child.stdin.take().expect("the command pipes its input");
    let close = driver.close_writer(stdin).expect("the test driver has room");
    drive(&mut driver, close).expect("the driver runs");
    let status = drive(&mut driver, child.wait()).expect("the driver runs");
    let status = status.expect("the wait works");
    assert_eq!(status.code(), Some(3));
    assert_eq!(state(child.id()), State::Reaped);
    let again = poll_ready(pin!(child.wait()), &mut Context::from_waker(Waker::noop()));
    assert_matches!(again, Some(Ok(Ok(again))) if again == status);
    assert_eq!(threads(), before);
    assert!(!sigchld_changed());
}

/// A drop of the wait and of the child ends the interest only: the driver still reaps the child
/// when it ends, so no zombie remains.
pub(in crate::driver) fn a_dropped_wait_and_child_leave_no_zombie_after_the_exit(backend: Backend) {
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let mut command = sh("read line");
    command.stdin(Stdio::piped());
    let mut child = spawn(&mut driver, command, ChildOptions::new());
    let pid = child.id();
    let stdin = child.stdin.take().expect("the command pipes its input");
    assert!(poll_ready(pin!(child.wait()), &mut Context::from_waker(Waker::noop())).is_none());
    drop(child);
    // The home applies the requests of the drop while the child still runs.
    let _turn = driver.turn().expect("a turn works");
    assert_eq!(state(pid), State::Running);

    let close = driver.close_writer(stdin).expect("the test driver has room");
    drive(&mut driver, close).expect("the driver runs");
    let reaped = drive_until(&mut driver, || state(pid) == State::Reaped);
    assert!(reaped, "the child stays a {:?}", state(pid));
}

/// A drop of a spawn ends the interest only: the job of the blocking service still starts the
/// child, and the return of the job is the terminal event of the spawn. The driver then applies the
/// end policy of the child, as at a drop of the child: with [`End::Kill`], it kills the child and
/// reaps it.
pub(in crate::driver) fn a_dropped_spawn_ends_at_the_return_of_its_job_and_its_child_ends_by_its_policy(
    backend: Backend,
) {
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    // The child writes its process ID into a pipe of the test, not into a pipe end of the driver.
    let (reader, writer) = std::io::pipe().expect("the test host makes a pipe");
    let mut command = sh("echo $$; exec sleep 600");
    command.stdout(writer);
    let options = ChildOptions::new().end(End::Kill);
    drop(
        driver
            .spawn(command, options)
            .expect("the test driver admits the spawn"),
    );
    let mut line = String::new();
    BufReader::new(reader)
        .read_line(&mut line)
        .expect("the child writes its process ID");
    let pid = line.trim().parse::<u32>().expect("the child gives a process ID");
    let reaped = drive_until(&mut driver, || state(pid) == State::Reaped);
    assert!(reaped, "the child stays {:?}", state(pid));
    assert_eq!(driver.teardown().expect("the teardown works").kept(), 0);
}

/// A wait for any child, or a `SIGCHLD` handler that reaps, would take the zombie of the test's
/// own child. The driver reaps only its own children: a spawned one and an adopted one.
pub(in crate::driver) fn the_driver_never_reaps_a_child_that_the_test_starts_itself(backend: Backend) {
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let mut own = Command::new("true").spawn().expect("the test host runs true");
    let own_pidfd = pidfd_open(own.id()).expect("the kernel gives process descriptors");
    assert!(readable(&own_pidfd, 10_000), "the test's own child ends");
    assert_eq!(state(own.id()), State::Zombie);

    let adopted = sh("exit 4").spawn().expect("the test host runs sh");
    let mut adopted = driver
        .adopt_child(adopted, End::Keep)
        .expect("the test driver has room");
    let status = drive(&mut driver, adopted.wait()).expect("the driver runs");
    assert_eq!(status.expect("the wait works").code(), Some(4));
    let mut spawned = spawn(&mut driver, sh("exit 5"), ChildOptions::new());
    let status = drive(&mut driver, spawned.wait()).expect("the driver runs");
    assert_eq!(status.expect("the wait works").code(), Some(5));

    assert_eq!(state(own.id()), State::Zombie);
    assert!(own.wait().expect("the test's own wait takes the status").success());
}

/// A spawn that finds each child slot in use, and one that finds the service full, refuse and
/// give the command back.
pub(in crate::driver) fn a_full_child_table_and_a_full_blocking_service_give_the_command_back(backend: Backend) {
    let (mut driver, service) = with_service(Config::new().children(1), 1, backend);
    let mut first = spawn(&mut driver, sleeper(), ChildOptions::new());
    let refused = driver
        .spawn(Command::new("true"), ChildOptions::new())
        .expect_err("each child slot is in use");
    assert_eq!(refused.reason(), Reason::Children);
    assert_eq!(refused.into_inner().get_program(), "true");
    driver.kill_child(&first).expect("the child runs");
    let status = drive(&mut driver, first.wait()).expect("the driver runs");
    assert_eq!(status.expect("the wait works").signal(), Some(9));

    // The one job slot of the service holds a job that waits for the test.
    let (release, held) = mpsc::channel::<()>();
    let mut job = service
        .submitter()
        .submit(move || held.recv().is_ok())
        .map_err(|refused| refused.reason)
        .expect("the service has a job slot");
    let refused = driver
        .spawn(Command::new("false"), ChildOptions::new())
        .expect_err("the service is full");
    assert_eq!(refused.reason(), Reason::Blocking);
    assert_eq!(refused.into_inner().get_program(), "false");
    release.send(()).expect("the job waits");
    job.wait_blocking().expect("the job returns");
}

/// A kill ends the child, and a signal after the reap refuses: no process has the identity for
/// the driver now.
pub(in crate::driver) fn a_kill_ends_the_child_and_a_signal_after_the_reap_refuses(backend: Backend) {
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let mut child = spawn(&mut driver, sleeper(), ChildOptions::new());
    driver.kill_child(&child).expect("the child runs");
    let status = drive(&mut driver, child.wait()).expect("the driver runs");
    assert_eq!(status.expect("the wait works").signal(), Some(9));
    let signal = driver.signal_child(&child, Signal::TERMINATE);
    assert_matches!(signal, Err(SignalChildError::Reaped));
}

/// A tree kill ends each process of the group of the child, also a grandchild. A tree signal
/// after the reap of the leader refuses, because its group ID can name another group then.
pub(in crate::driver) fn a_tree_kill_ends_a_grandchild_and_a_tree_signal_after_the_reap_refuses(backend: Backend) {
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let mut command = sh("sleep 600 & echo $!; wait");
    command.stdout(Stdio::piped());
    let mut child = spawn(&mut driver, command, ChildOptions::new().tree(true));
    let mut stdout = child.stdout.take().expect("the command pipes its output");
    let mut line = Vec::new();
    while !line.ends_with(b"\n") {
        let receive = driver
            .recv(&mut stdout, Backing::from(Vec::with_capacity(64)))
            .expect("the test driver has room");
        let (count, buffer) = drive(&mut driver, receive).expect("the driver runs");
        assert!(count.expect("the receive works") > 0, "the shell gives the grandchild");
        line.extend_from_slice(buffer.bytes());
    }
    let grandchild = core::str::from_utf8(&line).expect("text").trim().parse::<u32>();
    let grandchild = pidfd_open(grandchild.expect("a process ID"));

    driver.kill_tree(&child).expect("the leader runs");
    let status = drive(&mut driver, child.wait()).expect("the driver runs");
    assert_eq!(status.expect("the wait works").signal(), Some(9));
    match grandchild {
        Ok(grandchild) => assert!(readable(&grandchild, 10_000), "the grandchild ends"),
        Err(error) => assert_eq!(error.raw_os_error(), Some(ESRCH), "the grandchild ended already"),
    }
    let signal = driver.signal_tree(&child, Signal::TERMINATE);
    assert_matches!(signal, Err(SignalChildError::Reaped));
}

/// The input and both outputs are separate pipe ends, so the caller writes the input while both
/// outputs drain, and no full pipe stops the child.
pub(in crate::driver) fn the_caller_writes_the_input_while_both_outputs_drain(backend: Backend) {
    let (driver, _service) = with_service(Config::new(), 4, backend);
    let driver = RefCell::new(driver);
    let mut command = sh("tee /dev/stderr");
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = spawn(&mut driver.borrow_mut(), command, ChildOptions::new());
    let input = numbered(0, 1 << 20);
    let mut stdin = child.stdin.take().expect("the command pipes its input");
    let mut stdout = child.stdout.take().expect("the command pipes its output");
    let mut stderr = child.stderr.take().expect("the command pipes its error");
    let write = async {
        let mut sent = 0;
        while sent < input.len() {
            let chunk = input.iter().skip(sent).take(1 << 16).copied().collect::<Vec<u8>>();
            let send = driver
                .borrow_mut()
                .send(&mut stdin, Lease::from(chunk))
                .expect("the test driver has room");
            sent += send.await.expect("the driver runs").0.expect("the send works");
        }
        let close = driver
            .borrow_mut()
            .close_writer(stdin)
            .expect("the test driver has room");
        close.await.expect("the driver runs");
    };
    let mut write = pin!(write);
    let mut out = pin!(receive_all(&driver, &mut stdout));
    let mut err = pin!(receive_all(&driver, &mut stderr));
    let (mut wrote, mut output, mut error) = (false, None, None);
    let mut cx = Context::from_waker(Waker::noop());
    while !wrote || output.is_none() || error.is_none() {
        let more = driver.borrow_mut().turn().expect("a turn works").more();
        wrote = wrote || poll_ready(write.as_mut(), &mut cx).is_some();
        output = output.or_else(|| poll_ready(out.as_mut(), &mut cx));
        error = error.or_else(|| poll_ready(err.as_mut(), &mut cx));
        if !more && !(wrote && output.is_some() && error.is_some()) {
            driver.borrow_mut().wait().expect("a wait works");
        }
    }
    assert!(output.as_ref() == Some(&input), "the output is the input");
    assert!(error.as_ref() == Some(&input), "the error stream is the input");
    let status = drive(&mut driver.borrow_mut(), child.wait()).expect("the driver runs");
    assert!(status.expect("the wait works").success());
}

/// With [`End::Keep`], a close and a drop leave the child running, and teardown counts each one.
#[expect(
    unsafe_code,
    reason = "a kill names no memory, and the wait writes only its structure"
)]
pub(in crate::driver) fn the_keep_policy_leaves_a_closed_and_a_dropped_child_running_and_teardown_counts_them(
    backend: Backend,
) {
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let closed = spawn(&mut driver, sleeper(), ChildOptions::new());
    let dropped = spawn(&mut driver, sleeper(), ChildOptions::new());
    let pids = [closed.id(), dropped.id()];
    driver.close_child(closed);
    drop(dropped);
    // A turn applies the requests of the drop.
    let _turn = driver.turn().expect("a turn works");
    assert_eq!(pids.map(state), [State::Running; 2]);
    assert_eq!(driver.teardown().expect("the teardown works").kept(), 2);
    assert_eq!(pids.map(state), [State::Running; 2]);

    // The driver no longer owns them, so the test ends them.
    for pid in pids {
        // SAFETY: `kill` names no memory.
        assert_eq!(unsafe { sys::kill(pid.cast_signed(), 9) }, 0);
        let mut info = sys::SigInfo::default();
        // SAFETY: the call writes only `info`.
        assert_eq!(unsafe { sys::waitid(P_PID, pid, &raw mut info, sys::WEXITED) }, 0);
    }
}

/// With [`End::Kill`], a close kills the child, and the driver reaps it.
pub(in crate::driver) fn the_kill_policy_close_kills_and_reaps_the_child(backend: Backend) {
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let options = ChildOptions::new().end(End::Kill);
    let child = spawn(&mut driver, sleeper(), options);
    let pid = child.id();
    driver.close_child(child);
    let reaped = drive_until(&mut driver, || state(pid) == State::Reaped);
    assert!(reaped, "the child stays {:?}", state(pid));
    assert_eq!(driver.teardown().expect("the teardown works").kept(), 0);
}

/// The report names the wait path of the kernel: `IORING_OP_WAITID` from Linux 6.7, and a poll of
/// the process descriptor before.
#[test]
fn the_report_names_the_wait_path_of_the_kernel() {
    let release = std::fs::read_to_string("/proc/sys/kernel/osrelease").expect("the test host has procfs");
    let mut numbers = release.split(['.', '-']).map(|part| part.parse::<u32>().unwrap_or(0));
    let version = (numbers.next().unwrap_or(0), numbers.next().unwrap_or(0));
    let route = driver(Config::new()).report().route(Call::WaitChild);
    std::println!("Linux {}: the wait of a child takes {route:?}", release.trim());
    let expected = if version >= (6, 7) { Route::Ring } else { Route::Poll };
    assert_eq!(route, expected);
}
