//! The steps of the example. The local task runs on worker 0; the movable tasks run on any
//! movable worker, so no output names a movable worker.

use core::cell::RefCell;
use core::error::Error;
use core::future::pending;
use core::net::SocketAddr;
use core::num::NonZeroU64;
use std::io::Write;
use std::rc::Rc;

use rsasync::blocking::{self, Submitter};
use rsasync::buffer::{Backing, BufferMut as _, Lease};
use rsasync::capacity::Budget;
use rsasync::channel::{TrySendError, mpsc, oneshot};
use rsasync::driver::TcpOptions;
use rsasync::local::TcpListener;
use rsasync::runtime::{Mode, Runtime, WorkerId};
use rsasync::task::{self, CancelHandle, Outcome};

/// A request from the local task to the movable task: the numbers to sum, and the sender of the
/// sum.
struct Request {
    numbers: u64,
    reply: oneshot::Sender<u64>,
}

/// The state of the local task: its notes, which only its worker uses. An `Rc` is not `Send`.
type Notes = Rc<RefCell<Vec<String>>>;

/// Runs each step, prints the outcomes, and stops the runtime with a report.
pub(crate) fn run(out: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::builder()
        .local_workers(1)
        .movable_workers(2)
        .compute(Some(blocking::Config::new().threads(1).jobs(8).stack(256 << 10)))
        .build()?;
    let report = runtime.report();
    let modes: Vec<Mode> = report.workers().iter().map(|worker| worker.mode()).collect();
    if modes != [Mode::Local, Mode::Movable, Mode::Movable] {
        return Err(format!("the workers have the modes {modes:?}").into());
    }
    writeln!(
        out,
        "plan: worker 0 is local, workers 1 and 2 are movable, and {} compute thread runs beside them",
        report.compute_threads()
    )?;
    let mut handle = runtime.handle()?;
    let budget = Budget::new(NonZeroU64::new(1 << 20).ok_or("the budget is not zero")?);
    let compute = handle.compute().ok_or("the runtime has a compute service")?.clone();
    let (requests, inbox) = mpsc::channel(&budget, 1, 1)?;
    let server = handle.spawn_movable(serve(inbox, compute)).map_err(refused)?;
    let (_never, never) = oneshot::channel::<()>(&budget)?;
    let waiting = handle.spawn_movable(wait(never)).map_err(refused)?;
    let cancel = waiting.cancel_handle();
    let local = handle
        .spawn_on(WorkerId::new(0), move || local_task(requests, budget, cancel))
        .map_err(|refused| format!("the spawn refused: {:?}", refused.reason()))?;
    match local.join_blocking() {
        Outcome::Completed(notes) => {
            for note in notes? {
                writeln!(out, "{note}")?;
            }
        }
        other => return Err(format!("the local task ended with {other:?}").into()),
    }
    writeln!(
        out,
        "cancel: the local task cancelled the movable task that waited: {:?}",
        waiting.join_blocking()
    )?;
    // The end of the local task dropped its sender, so the movable task ends.
    match server.join_blocking() {
        Outcome::Completed(1) => writeln!(
            out,
            "handoff: the movable task found that the local task dropped its wait, and the reply came back"
        )?,
        other => return Err(format!("the movable task ended with {other:?}").into()),
    }

    // A local task and a movable task that wait at the shutdown, which cancels them both.
    drop(handle.spawn_on(WorkerId::new(0), pending::<()>).map_err(refused)?);
    drop(handle.spawn_movable(pending::<()>()).map_err(refused)?);
    let report = runtime.shutdown();
    let local: u32 = report.workers().iter().map(|worker| worker.cancelled()).sum();
    let debt: u32 = report.workers().iter().map(|worker| worker.debt()).sum();
    let jobs = report.compute().map_or(0, blocking::Teardown::running);
    writeln!(
        out,
        "shutdown: {local} local and {} movable task cancelled, {debt} operations of debt, {jobs} compute jobs ran at the end, grace period expired: {}",
        report.movable_cancelled(),
        report.expired()
    )?;
    Ok(())
}

/// The movable task: it sums the numbers of each request on the compute service, which runs CPU
/// work that cannot yield, and replies. Gives the replies that came back because the requester
/// dropped its wait.
async fn serve(mut inbox: mpsc::Receiver<Request>, compute: Submitter) -> u32 {
    let mut returned = 0;
    while let Ok(Request { numbers, reply }) = inbox.recv().await {
        let Ok(mut job) = compute.submit(move || (0..numbers).sum::<u64>()) else {
            break;
        };
        let Ok(sum) = job.wait().await else {
            break;
        };
        if reply.send(sum).is_err() {
            returned += 1;
        }
    }
    returned
}

/// A movable task that waits for a value that never comes.
async fn wait(mut never: oneshot::Receiver<()>) {
    let _never = never.recv().await;
}

fn note(notes: &Notes, note: String) {
    notes.borrow_mut().push(note);
}

/// The text of the initialized bytes of `buffer`.
fn text(buffer: &impl rsasync::buffer::Buffer) -> String {
    String::from_utf8_lossy(buffer.bytes()).into_owned()
}

/// The local task: the handoff, the reuse of a value after a failed handoff, a dropped wait, TCP
/// from a movable stream to a local stream, and the cancel of a movable task.
async fn local_task(
    mut requests: mpsc::Sender<Request>,
    budget: Budget,
    cancel: CancelHandle,
) -> Result<Vec<String>, String> {
    let notes = Notes::default();
    let steps = async {
        let mut sums = Vec::new();
        for numbers in [10, 100, 1000] {
            let (reply, mut answer) = oneshot::channel(&budget)?;
            requests.send(Request { numbers, reply }).await?;
            sums.push(answer.recv().await?);
        }
        note(
            &notes,
            format!("handoff: the local task sent 3 requests, and the movable task sent back the sums {sums:?}"),
        );

        // A bounded channel with one place: the second value comes back, and it goes through after
        // the first one leaves.
        let (mut full, mut taken) = mpsc::channel::<u32>(&budget, 1, 1)?;
        full.try_send(1)?;
        let Err(TrySendError::Full(again)) = full.try_send(10) else {
            return Err("the full channel took the second value".into());
        };
        taken.recv().await?;
        full.send(again).await?;
        note(
            &notes,
            format!(
                "handoff: a full channel gave the request back, and the same request then went through: {}",
                taken.recv().await?
            ),
        );

        // The local task drops its wait before the reply: the movable task gets the reply back.
        let (reply, answer) = oneshot::channel(&budget)?;
        drop(answer);
        requests.send(Request { numbers: 5, reply }).await?;

        tcp(&notes).await?;

        cancel.cancel();
        let count = notes.borrow().len();
        note(
            &notes,
            format!("state: one Rc kept the {count} notes of the local task on worker 0"),
        );
        Ok::<(), Box<dyn Error>>(())
    };
    steps.await.map_err(|error| error.to_string())?;
    Ok(notes.take())
}

/// A movable task sends to a stream of the local task: partial progress, and the reuse of the
/// buffer.
async fn tcp(notes: &Notes) -> Result<(), Box<dyn Error>> {
    let mut listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)), TcpOptions::default())?.await??;
    let address = listener.local_addr();
    let sender = task::spawn_movable(async move {
        let mut stream = rsasync::movable::TcpStream::connect(address, TcpOptions::default())
            .ok()?
            .await
            .ok()?
            .ok()?;
        let (sent, _lease) = stream.send(Lease::from(b"hello world".to_vec())).ok()?.await.ok()?;
        stream.close().ok()?.await.ok()?;
        sent.ok()
    })
    .map_err(|refused| format!("the spawn refused: {:?}", refused.reason()))?;
    let (mut stream, _peer) = listener.accept()?.await??;
    let sent = match sender.join().await {
        Outcome::Completed(Some(sent)) => sent,
        other => return Err(format!("the movable sender ended with {other:?}").into()),
    };
    let (received, mut buffer) = stream.recv(Backing::from(Vec::with_capacity(8)))?.await?;
    note(
        notes,
        format!(
            "tcp: the local stream received {} of the {sent} bytes that the movable stream sent: {:?}",
            received?,
            text(&buffer)
        ),
    );
    buffer.truncate(0);
    let (received, buffer) = stream.recv(buffer)?.await?;
    received?;
    note(
        notes,
        format!("tcp: the same buffer then received the rest: {:?}", text(&buffer)),
    );
    stream.close()?.await?;
    listener.close()?.await?;
    Ok(())
}

/// The text of a refused spawn, for any future.
fn refused<F>(refused: task::SpawnRefused<F>) -> String {
    format!("the spawn refused: {:?}", refused.reason())
}
