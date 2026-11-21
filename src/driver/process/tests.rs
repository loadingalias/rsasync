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
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd as _;
use std::os::unix::process::ExitStatusExt as _;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use super::super::suite::{self, Backend};
#[cfg(target_os = "linux")]
use super::super::tests::driver;
use super::super::tests::{drive, numbered};
#[cfg(target_os = "linux")]
use super::super::{Call, Route};
use super::super::{Config, Driver, PipeReader, Reason, Signal};
use super::{Child, ChildOptions, End, SignalChildError, sys};
use crate::blocking::{self, Service};
use crate::buffer::{Backing, Buffer as _, Lease};
use crate::capacity::Budget;
use crate::sync::Arc;

/// `P_ALL` and `P_PID` (`include/uapi/linux/wait.h`, and `sys/wait.h` of macOS): a `waitid` of
/// each child, and of one process ID.
const P_ALL: c_int = 0;
const P_PID: c_int = 1;

/// `ESRCH` (`include/uapi/asm-generic/errno-base.h`).
#[cfg(target_os = "linux")]
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

/// Turns nothing, and waits until `done` holds, at most 10 s. Gives whether it held.
fn wait_until(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        if Instant::now() > deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
    true
}

/// Whether a child of this process ended and waits for its reap. The look reaps nothing.
#[expect(unsafe_code, reason = "the call writes only the structure that it gets")]
fn a_zombie_waits() -> bool {
    let mut info = sys::SigInfo::default();
    let options = sys::WEXITED | sys::WNOHANG | sys::WNOWAIT;
    // SAFETY: the call writes only `info`.
    (unsafe { sys::waitid(P_ALL, 0, &raw mut info, options) }) == 0 && info.pid != 0
}

/// The threads of this process.
#[cfg(target_os = "linux")]
fn threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("the test host has procfs")
        .count()
}

/// The threads of this process: `pti_threadnum` of `struct proc_taskinfo` (`sys/proc_info.h`),
/// which `proc_pidinfo` of `libproc.h` writes for `PROC_PIDTASKINFO`.
#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "the call writes only the structure that it gets")]
fn threads() -> usize {
    const PROC_PIDTASKINFO: c_int = 4;
    /// Six `uint64_t` counts, then twelve `int32_t` counts, of which `pti_threadnum` is the tenth.
    #[repr(C)]
    struct TaskInfo {
        times: [u64; 6],
        counts: [i32; 12],
    }
    #[expect(unsafe_code, reason = "libSystem of macOS gives this function")]
    unsafe extern "C" {
        fn proc_pidinfo(pid: c_int, flavor: c_int, arg: u64, buffer: *mut core::ffi::c_void, size: c_int) -> c_int;
    }
    let mut info = TaskInfo {
        times: [0; 6],
        counts: [0; 12],
    };
    let size = c_int::try_from(size_of::<TaskInfo>()).expect("96 bytes");
    let pid = c_int::try_from(std::process::id()).expect("a process ID");
    // SAFETY: the call writes at most `size` bytes into `info`.
    let written = unsafe { proc_pidinfo(pid, PROC_PIDTASKINFO, 0, (&raw mut info).cast(), size) };
    assert_eq!(written, size, "proc_pidinfo");
    usize::try_from(info.counts[9]).expect("a count")
}

/// Whether this process catches or ignores `SIGCHLD`: bit 16 of `SigCgt` or of `SigIgn` in
/// `/proc/self/status`.
#[cfg(target_os = "linux")]
fn sigchld_changed() -> bool {
    let status = std::fs::read_to_string("/proc/self/status").expect("the test host has procfs");
    status
        .lines()
        .filter_map(|line| line.strip_prefix("SigCgt:").or_else(|| line.strip_prefix("SigIgn:")))
        .map(|mask| u64::from_str_radix(mask.trim(), 16).expect("procfs gives a hexadecimal mask"))
        .any(|mask| mask & (1 << 16) != 0)
}

/// Whether this process catches or ignores `SIGCHLD` (20): its handler is not `SIG_DFL` (0) in
/// `struct sigaction` of `sys/signal.h`.
#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "the call writes only the structure that it gets")]
fn sigchld_changed() -> bool {
    #[repr(C)]
    struct SigAction {
        handler: usize,
        mask: u32,
        flags: c_int,
    }
    #[expect(unsafe_code, reason = "libSystem of macOS gives this function")]
    unsafe extern "C" {
        fn sigaction(sig: c_int, act: *const SigAction, oact: *mut SigAction) -> c_int;
    }
    let mut action = SigAction {
        handler: 0,
        mask: 0,
        flags: 0,
    };
    // SAFETY: a null `act` only reads the disposition into `action`.
    assert_eq!(unsafe { sigaction(20, core::ptr::null(), &raw mut action) }, 0);
    action.handler != 0
}

/// Whether the process `pid`, which is no child of this process, ends within 10 s: its process
/// descriptor becomes readable, or no process has its ID.
#[cfg(target_os = "linux")]
#[expect(unsafe_code, reason = "a poll writes only the structure that it gets")]
fn ends(pid: u32) -> bool {
    let fd = match super::unsafe_call::open(pid) {
        Ok(fd) => fd,
        Err(error) => return error.raw_os_error() == Some(ESRCH),
    };
    let mut poll = crate::driver::sys::PollFd {
        fd: fd.as_raw_fd(),
        events: crate::driver::sys::POLLIN,
        revents: 0,
    };
    // SAFETY: the call writes only `poll`.
    (unsafe { crate::driver::sys::poll(&raw mut poll, 1, 10_000) }) == 1
}

/// Whether the process `pid`, which is no child of this process, ends within 10 s: no process has
/// its ID, after `launchd` reaped it. macOS has no process descriptor.
#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "a kill of signal 0 names no memory")]
fn ends(pid: u32) -> bool {
    let pid = c_int::try_from(pid).expect("a process ID");
    // SAFETY: `kill` names no memory, and signal 0 sends nothing.
    wait_until(|| unsafe { sys::kill(pid, 0) } != 0)
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
    assert!(
        wait_until(|| state(own.id()) == State::Zombie),
        "the test's own child ends"
    );

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

/// Each driver spawns its first child in slot 0 with generation 0. On the other driver, a kill of
/// that child gives `OtherDriver`, so the kill reaches neither child. Each child runs on until a
/// kill of its own driver ends it.
pub(in crate::driver) fn a_child_of_another_driver_gives_other_driver_and_no_child_gets_the_kill() {
    let backend = cfg_select! {
        target_os = "linux" => Backend::Kernel,
        _ => Backend::Kqueue,
    };
    let (mut first, _first_service) = with_service(Config::new(), 4, backend);
    let (mut second, _second_service) = with_service(Config::new(), 4, backend);
    let mut theirs = spawn(&mut first, sleeper(), ChildOptions::new());
    let mut ours = spawn(&mut second, sleeper(), ChildOptions::new());

    assert_matches!(second.kill_child(&theirs), Err(SignalChildError::OtherDriver));
    assert_eq!([theirs.id(), ours.id()].map(state), [State::Running; 2]);

    for (driver, child) in [(&mut first, &mut theirs), (&mut second, &mut ours)] {
        driver.kill_child(child).expect("the child runs");
        let status = drive(driver, child.wait()).expect("the driver runs");
        assert_eq!(status.expect("the wait works").signal(), Some(9));
    }
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
    let grandchild = grandchild.expect("a process ID");

    driver.kill_tree(&child).expect("the leader runs");
    let status = drive(&mut driver, child.wait()).expect("the driver runs");
    assert_eq!(status.expect("the wait works").signal(), Some(9));
    assert!(ends(grandchild), "the grandchild ends");
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

/// A child that ends before the driver registers its exit still has its exit reported once: on
/// kqueue, the registration then finds no process, and the home takes the status at once. The
/// spawn ends its job, and the child its run, before the turn that registers the exit.
pub(in crate::driver) fn a_child_that_ends_before_its_registration_has_its_exit_reported_once(backend: Backend) {
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let spawn = driver
        .spawn(sh("exit 6"), ChildOptions::new())
        .expect("the test driver admits the spawn");
    assert!(wait_until(a_zombie_waits), "the child ends before the next turn");
    let mut child = drive(&mut driver, spawn)
        .expect("the driver runs")
        .expect("the test host starts the command");
    let status = drive(&mut driver, child.wait()).expect("the driver runs");
    assert_eq!(status.expect("the wait works").code(), Some(6));
    assert_eq!(state(child.id()), State::Reaped);
}

/// With `SIGCHLD` ignored, the kernel reaps each child at its end, so the status is lost for the
/// driver: the wait ends with `ECHILD`, and it does not wait for a status that cannot come. On
/// macOS, the exit event can come before the kernel reaps the child, so the driver waits again
/// with its bound.
pub(in crate::driver) fn an_ignored_sigchld_ends_the_wait_with_echild(backend: Backend) {
    /// `SIGCHLD` and `ECHILD`.
    #[cfg(target_os = "linux")]
    const SIGCHLD: c_int = 17;
    #[cfg(target_os = "macos")]
    const SIGCHLD: c_int = 20;
    const ECHILD: i32 = 10;
    crate::driver::signal::tests::ignore(SIGCHLD);
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let mut command = sh("read line; exit 3");
    command.stdin(Stdio::piped());
    let mut child = spawn(&mut driver, command, ChildOptions::new());
    let stdin = child.stdin.take().expect("the command pipes its input");
    let close = driver.close_writer(stdin).expect("the test driver has room");
    drive(&mut driver, close).expect("the driver runs");
    let status = drive(&mut driver, child.wait()).expect("the driver runs");
    let error = status.expect_err("the kernel took the status");
    assert_eq!(error.raw_os_error(), Some(ECHILD));
}

/// A child whose status a tracer holds after its exit does not make the driver spin. Its process
/// descriptor is readable, and each poll of the driver finds no status, so the driver waits with
/// the backoff of its children before each poll: from 100 µs, doubling to 10 ms, then one poll
/// each 10 ms. A driver that polls again at once turns thousands of times in `WINDOW`. The child
/// lets any process of its user trace it (`PR_SET_PTRACER_ANY`), so the tracer needs no
/// privilege. On io_uring from Linux 6.7, `IORING_OP_WAITID` waits in the kernel and makes no poll.
#[cfg(target_os = "linux")]
#[expect(unsafe_code, reason = "the calls name no memory, or only the status that they write")]
pub(in crate::driver) fn a_child_whose_status_a_tracer_holds_does_not_make_the_driver_spin(backend: Backend) {
    // The test of this case in the index of `driver::suite`, which the child and the tracer run.
    const NAME: &str =
        "driver::suite::operation::epoll::a_child_whose_status_a_tracer_holds_does_not_make_the_driver_spin";
    const ROLE: &str = "RSASYNC_TRACED_ROLE";
    const WINDOW_MS: u64 = 300;
    /// `prctl` and `ptrace` requests (`include/uapi/linux/prctl.h` and `ptrace.h`), and the
    /// option of `waitpid` that waits for a traced process that is no child (`__WALL`).
    const PR_SET_PTRACER: c_int = 0x5961_6D61;
    const PR_SET_PTRACER_ANY: core::ffi::c_ulong = core::ffi::c_ulong::MAX;
    const PTRACE_SEIZE: c_int = 0x4206;
    const WALL: c_int = 0x4000_0000;
    #[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
    unsafe extern "C" {
        fn prctl(option: c_int, ...) -> c_int;
        fn ptrace(request: c_int, ...) -> core::ffi::c_long;
        fn waitpid(pid: c_int, status: *mut c_int, options: c_int) -> c_int;
    }
    match std::env::var(ROLE).as_deref() {
        // The child lets a tracer in, and it ends when its input ends.
        Ok("child") => {
            // SAFETY: the option names no memory.
            let allowed = unsafe { prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0_u64, 0_u64, 0_u64) };
            assert_eq!(allowed, 0, "prctl");
            std::io::copy(&mut std::io::stdin(), &mut std::io::sink()).expect("the child reads its input");
            std::process::exit(7);
        }
        // The tracer seizes the child, holds its status while its own input lasts, and then takes
        // the status, which goes on to the parent.
        Ok(pid) => {
            let pid = pid.parse::<c_int>().expect("a process ID");
            let null = core::ptr::null_mut::<core::ffi::c_void>();
            let seize = || {
                // SAFETY: a seize names no memory.
                unsafe { ptrace(PTRACE_SEIZE, pid, null, null) == 0 }
            };
            assert!(
                wait_until(seize),
                "the tracer seizes the child: {}",
                std::io::Error::last_os_error()
            );
            std::println!("seized");
            std::io::copy(&mut std::io::stdin(), &mut std::io::sink()).expect("the tracer reads its input");
            let mut status = 0;
            // SAFETY: the call writes only `status`.
            assert_eq!(unsafe { waitpid(pid, &raw mut status, WALL) }, pid);
            std::process::exit(0);
        }
        Err(_) => {}
    }
    let (mut driver, _service) = with_service(Config::new(), 4, backend);
    let exe = std::env::current_exe().expect("the test binary");
    // `--nocapture`: libtest keeps the output of a test from its pipe otherwise.
    let mut command = Command::new(&exe);
    command.args(["--exact", NAME, "--nocapture"]).env(ROLE, "child");
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = spawn(&mut driver, command, ChildOptions::new());
    let mut tracer = Command::new(&exe)
        .args(["--exact", NAME, "--nocapture"])
        .env(ROLE, alloc::format!("{}", child.id()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("the test host runs the tracer");
    let mut lines = BufReader::new(tracer.stdout.take().expect("the tracer pipes its output")).lines();
    assert!(
        lines.any(|line| line.is_ok_and(|line| line == "seized")),
        "the tracer seizes the child"
    );

    // The child ends, and the tracer holds its status.
    let stdin = child.stdin.take().expect("the command pipes its input");
    let close = driver.close_writer(stdin).expect("the test driver has room");
    drive(&mut driver, close).expect("the driver runs");
    let deadline = Instant::now() + Duration::from_millis(WINDOW_MS);
    let wake = driver.wake_handle();
    let ticker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(WINDOW_MS));
        wake.wake();
    });
    let mut turns = 0_u64;
    while Instant::now() < deadline {
        let more = driver.turn().expect("a turn works").more();
        turns += 1;
        if !more && Instant::now() < deadline {
            driver.wait().expect("a wait works");
        }
    }
    ticker.join().expect("the ticker ends");
    // The child ended and nothing reaped it, so its descriptor is readable and each poll missed.
    let stat = std::fs::read_to_string(alloc::format!("/proc/{}/stat", child.id())).expect("procfs");
    let state = stat.rsplit_once(") ").and_then(|(_, rest)| rest.chars().next());
    assert_eq!(state, Some('Z'), "the tracer holds the status of the ended child");
    // Seven waits take 12.7 ms, and then one poll comes each 10 ms. Each poll takes three turns:
    // the end of the timer, the poll of the descriptor, which finds no status, and the next poll
    // of the timer. The bound allows twice that. A driver that polls again at once turns thousands
    // of times.
    let polls = 8 + WINDOW_MS / 10;
    assert!(turns <= 6 * polls, "{turns} turns in {WINDOW_MS} ms");

    // The tracer gives the status back, and the wait gives it.
    drop(tracer.stdin.take());
    let status = drive(&mut driver, child.wait()).expect("the driver runs");
    assert_eq!(status.expect("the wait works").code(), Some(7));
    assert!(tracer.wait().expect("the tracer ends").success());
}

/// The report names the wait path of the kernel: `IORING_OP_WAITID` from Linux 6.7, and a poll of
/// the process descriptor before.
#[cfg(target_os = "linux")]
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
