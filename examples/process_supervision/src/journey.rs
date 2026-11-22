//! The steps of the example. Each step gives one line for each outcome.

use core::error::Error;
use core::future::{Future, join, poll_fn};
use core::num::NonZeroU32;
use core::pin::pin;
use core::task::Poll;
use core::time::Duration;
use std::io::Write;
use std::os::unix::process::ExitStatusExt as _;
use std::process::{Command, ExitStatus, Stdio};

use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::clock::NativeClock;
use rsasync::driver::{ChildOptions, End, Escalation, Signal};
use rsasync::local::{self, Child, PipeReader, PipeWriter};
use rsasync::runtime::{Runtime, WorkerId};
use rsasync::task::{Outcome, timer};
use rsasync::time::{Deadline, Suspend};

/// One line of the input of a child: 16 bytes.
const LINE: &[u8] = b"0123456789abcde\n";

/// The lines of the input of each child: 256 KiB, 4 times the 64 KiB that a Linux pipe holds by
/// default. A caller that wrote all of it before it read would wait forever, because the child
/// stops reading when the pipe of one of its outputs is full.
const LINES: usize = 16 << 10;

/// The time that a child gets to end after `SIGTERM`. Then the supervisor kills it.
const STOP_DEADLINE: Duration = Duration::from_secs(1);

/// The two children, and what each one does with `SIGTERM`. Each one copies its input to its
/// standard output and its standard error, and closes both. Then it runs until a signal ends it.
/// The second one ignores `SIGTERM`, and `exec` keeps that disposition for `sleep`.
const CHILDREN: [(&str, &str); 2] = [
    ("obeys", "tee /dev/stderr; exec sleep 600 >/dev/null 2>&1"),
    (
        "ignores",
        "trap '' TERM; tee /dev/stderr; exec sleep 600 >/dev/null 2>&1",
    ),
];

/// Runs the task on worker 0, prints its lines, and stops the runtime with a report.
pub(crate) fn run(out: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::builder().local_workers(1).build()?;
    let mut handle = runtime.handle()?;
    let task = handle
        .spawn_on(WorkerId::new(0), || async {
            supervise().await.map_err(|error| error.to_string())
        })
        .map_err(|refused| format!("the spawn refused: {:?}", refused.reason()))?;
    let lines = match task.join_blocking() {
        Outcome::Completed(lines) => lines?,
        other => return Err(format!("the task ended with {other:?}").into()),
    };
    for line in lines {
        writeln!(out, "{line}")?;
    }
    let report = runtime.shutdown();
    let worker = report.workers().first().ok_or("the runtime has no worker")?;
    writeln!(
        out,
        "shutdown: {} tasks cancelled, {} operations of debt, {} children run on, grace period expired: {}",
        worker.cancelled(),
        worker.debt(),
        worker.kept(),
        report.expired()
    )?;
    Ok(())
}

/// The steps of the task.
async fn supervise() -> Result<Vec<String>, Box<dyn Error>> {
    let mut lines = Vec::new();
    // The first interrupt goes to the stream. From the second one on, the default action ends the
    // process, also when the stop is stuck.
    let mut interrupt = local::signal(Signal::INTERRUPT, Escalation::At(NonZeroU32::MIN.saturating_add(1)))?;
    lines.push(String::from(
        "interrupt: the stream of SIGINT is ready, and a second SIGINT ends the process",
    ));

    let input = Lease::from(LINE.repeat(LINES));
    let mut children = Vec::new();
    for (name, script) in CHILDREN {
        let mut child = spawn(script).await?;
        let (stdout, stderr) = exchange(&mut child, &input).await?;
        lines.push(format!(
            "io: the child that {name} SIGTERM took {} bytes while its stdout and stderr drained",
            input.bytes().len()
        ));
        lines.push(format!(
            "io: its stdout gave {} bytes and its stderr gave {} bytes, each the same as the input: {}",
            stdout.bytes().len(),
            stderr.bytes().len(),
            stdout.bytes() == input.bytes() && stderr.bytes() == input.bytes()
        ));
        children.push((name, child));
    }

    // `kill -s INT` stands in for Ctrl-C at a terminal, or for a service manager.
    let mut command = Command::new("kill");
    command.args(["-s", "INT", &std::process::id().to_string()]);
    let mut kill = Child::spawn(command, ChildOptions::new())?.await??;
    let status = kill.wait().await??;
    if !status.success() {
        return Err(format!("kill ended with {status}").into());
    }
    kill.close()?;
    interrupt.recv().await?;
    lines.push(String::from(
        "interrupt: kill -s INT sent SIGINT to this process, and the stream reported it",
    ));

    let clock = NativeClock::new(Suspend::Pauses)?;
    for (name, mut child) in children {
        lines.push(stop(&mut child, &clock, name).await?);
        child.close()?;
    }
    Ok(lines)
}

/// Spawns `sh -c script` with each standard stream piped. The child leads a process tree, a new
/// process group: an interrupt from the terminal reaches only this process, which then stops the
/// tree. With `End::Kill`, a close or a drop of the child kills its tree, so an early return of the
/// task leaves no process.
async fn spawn(script: &str) -> Result<Child, Box<dyn Error>> {
    let mut command = Command::new("sh");
    command
        .args(["-c", script])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let options = ChildOptions::new().tree(true).end(End::Kill);
    Ok(Child::spawn(command, options)?.await??)
}

/// Writes `input` to the standard input of `child` while both of its outputs drain, in one join,
/// and gives both outputs.
async fn exchange(child: &mut Child, input: &Lease) -> Result<(Backing, Backing), Box<dyn Error>> {
    let stdin = child.stdin.take().ok_or("the child has no standard input")?;
    let stdout = child.stdout.take().ok_or("the child has no standard output")?;
    let stderr = child.stderr.take().ok_or("the child has no standard error")?;
    let capacity = input.bytes().len().checked_add(1).ok_or("the input is too large")?;
    let (fed, stdout, stderr) = join!(feed(stdin, input), drain(stdout, capacity), drain(stderr, capacity)).await;
    fed?;
    Ok((stdout?, stderr?))
}

/// Sends each byte of `input`, and then closes `stdin`: the child then reads the end of its input.
/// A send can take part of its bytes, so the next send starts after them, with no copy.
async fn feed(mut stdin: PipeWriter, input: &Lease) -> Result<(), Box<dyn Error>> {
    let mut rest = input.clone();
    while !rest.bytes().is_empty() {
        let (sent, lease) = stdin.send(rest)?.await?;
        rest = lease.view(sent?..).ok_or("a send gave a count larger than its bytes")?;
    }
    stdin.close()?.await?;
    Ok(())
}

/// Receives from `output` until the end of its stream, all into one buffer, and then closes
/// `output`. The buffer has room for `capacity` bytes, one more than the input: so a receive of 0
/// bytes is the end of the stream, not a full buffer.
async fn drain(mut output: PipeReader, capacity: usize) -> Result<Backing, Box<dyn Error>> {
    let mut buffer = Backing::from(Vec::with_capacity(capacity));
    loop {
        let (received, filled) = output.recv(buffer)?.await?;
        buffer = filled;
        if received? == 0 {
            break;
        }
    }
    output.close()?.await?;
    Ok(buffer)
}

/// Sends `SIGTERM` to the tree of `child`, and waits for its exit until the deadline. At the
/// deadline, it kills the tree and waits for that exit. Gives the line of the outcome.
async fn stop(child: &mut Child, clock: &NativeClock, name: &str) -> Result<String, Box<dyn Error>> {
    child.signal_tree(Signal::TERMINATE)?;
    let deadline = Deadline::at(clock.now().checked_add(STOP_DEADLINE)?);
    match select(child.wait(), timer(deadline)?).await {
        First::Left(status) => Ok(format!(
            "stop: the child that {name} SIGTERM ended by signal {} before the deadline of {STOP_DEADLINE:?}",
            signal(status??)?
        )),
        First::Right(fired) => {
            fired?;
            child.kill_tree()?;
            let status = child.wait().await??;
            Ok(format!(
                "stop: the child that {name} SIGTERM ran on to the deadline of {STOP_DEADLINE:?}, and the kill ended it by signal {}",
                signal(status)?
            ))
        }
    }
}

/// The signal that ended a child, or an error for a child that exited by itself.
fn signal(status: ExitStatus) -> Result<i32, String> {
    status
        .signal()
        .ok_or_else(|| format!("the child exited by itself, with {status}"))
}

/// What [`select`] gives: the output of the future that was ready first.
enum First<A, B> {
    Left(A),
    Right(B),
}

/// Polls `left` and then `right` in one task, and gives the first output. The other future drops
/// with the call: a dropped wait leaves the exit of the child to the next wait, and a dropped timer
/// is cancelled.
async fn select<A: Future, B: Future>(left: A, right: B) -> First<A::Output, B::Output> {
    let (mut left, mut right) = (pin!(left), pin!(right));
    poll_fn(|cx| {
        if let Poll::Ready(output) = left.as_mut().poll(cx) {
            return Poll::Ready(First::Left(output));
        }
        right.as_mut().poll(cx).map(First::Right)
    })
    .await
}
