//! The steps of the example. Each step runs in movable tasks, which an idle worker can steal, so no
//! output names a worker.

use core::error::Error;
use core::future::poll_fn;
use core::net::SocketAddr;
use core::task::Poll;
use std::io::Write;
use std::sync::mpsc;

use rsasync::buffer::{Backing, Buffer as _, BufferMut as _, Lease};
use rsasync::driver::TcpOptions;
use rsasync::movable::{TcpListener, TcpStream};
use rsasync::runtime::Runtime;
use rsasync::task::{self, Outcome};

/// The echo tasks of the pressure step, and the bytes of each one.
const ECHOES: u32 = 64;
const ECHO_BYTES: usize = 4096;
/// The tasks that use the CPU, and the numbers that each one sums.
const SUMS: u32 = 8;
const NUMBERS: u64 = 1_000_000;

/// Runs each step on a runtime with two movable workers, prints its outcomes, and stops the
/// runtime with a report.
pub(crate) fn run(out: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::builder().movable_workers(2).build()?;
    let handle = runtime.handle()?;

    // Task and I/O pressure: an accept loop gives each connection to a task of its own, and the
    // echo tasks and the tasks that use the CPU all run in the pool at one time.
    let (addressed, address) = mpsc::channel();
    let server = handle.spawn_movable(serve(addressed)).map_err(refused)?;
    let address = address.recv()?;
    let mut echoes = Vec::new();
    for index in 0..ECHOES {
        echoes.push(handle.spawn_movable(echo(address, index)).map_err(refused)?);
    }
    let mut sums = Vec::new();
    for _ in 0..SUMS {
        sums.push(handle.spawn_movable(sum()).map_err(refused)?);
    }
    let echoed = echoes
        .into_iter()
        .map(task::JoinHandle::join_blocking)
        .filter(|echoed| matches!(echoed, Outcome::Completed(Ok(()))))
        .count();
    writeln!(out, "pressure: {echoed} echo tasks each got their 4 KiB back in order")?;
    let summed = sums
        .into_iter()
        .map(task::JoinHandle::join_blocking)
        .filter(|summed| matches!(summed, Outcome::Completed(total) if *total == NUMBERS * (NUMBERS - 1) / 2))
        .count();
    writeln!(
        out,
        "pressure: {summed} tasks that use the CPU each summed {NUMBERS} numbers"
    )?;
    match server.join_blocking() {
        Outcome::Completed(Ok(())) => {}
        other => return Err(format!("the accept loop ended with {other:?}").into()),
    }

    match handle.spawn_movable(tcp()).map_err(refused)?.join_blocking() {
        Outcome::Completed(notes) => {
            for note in notes? {
                writeln!(out, "{note}")?;
            }
        }
        other => return Err(format!("the TCP task ended with {other:?}").into()),
    }

    // A task whose receive waits for bytes that never come: the shutdown cancels it, and the drop
    // of its stream ends the receive.
    let (ready, waiting) = mpsc::channel();
    let _waiting = handle.spawn_movable(wait_forever(ready)).map_err(refused)?;
    waiting.recv()?;
    let report = runtime.shutdown();
    let debt: u32 = report.workers().iter().map(|worker| worker.debt()).sum();
    writeln!(
        out,
        "shutdown: {} movable task cancelled, {debt} operations of debt, grace period expired: {}",
        report.movable_cancelled(),
        report.expired()
    )?;
    Ok(())
}

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// The text of the initialized bytes of `buffer`.
fn text(buffer: &impl rsasync::buffer::Buffer) -> String {
    String::from_utf8_lossy(buffer.bytes()).into_owned()
}

/// The 4 KiB of echo task `index`, which differ for each task.
fn message(index: u32) -> Vec<u8> {
    (0..ECHO_BYTES)
        .map(|position| u8::try_from((position + index as usize) % 251).unwrap_or(0))
        .collect()
}

/// Accepts `ECHOES` connections, gives each one to a reply task of its own, and waits for the end of
/// each reply task. So no task of this step still runs when the accept loop ends, and the shutdown
/// finds only the task that it must cancel.
async fn serve(addressed: mpsc::Sender<SocketAddr>) -> Result<(), String> {
    let mut listener = TcpListener::bind(loopback(), TcpOptions::default())
        .map_err(|refused| format!("{:?}", refused.reason()))?
        .await
        .map_err(|_| "the bind ended")?
        .map_err(|error| error.to_string())?;
    addressed
        .send(listener.local_addr())
        .map_err(|_| "no one waits for the address")?;
    let mut replies = Vec::new();
    for _ in 0..ECHOES {
        let (stream, _peer) = listener
            .accept()
            .map_err(|refused| format!("{:?}", refused.reason()))?
            .await
            .map_err(|_| "the accept ended")?
            .map_err(|error| error.to_string())?;
        replies.push(task::spawn_movable(reply(stream)).map_err(|refused| format!("{:?}", refused.reason()))?);
    }
    listener
        .close()
        .map_err(|_| "the close refused")?
        .await
        .map_err(|_| "the close ended")?;
    // A reply task ends after its peer closed the connection.
    for reply in replies {
        if !matches!(reply.join().await, Outcome::Completed(())) {
            return Err(String::from("a reply task did not complete"));
        }
    }
    Ok(())
}

/// Sends back each byte that `stream` receives, until the end of its stream, and closes it.
async fn reply(mut stream: TcpStream) {
    loop {
        let Ok(receive) = stream.recv(Backing::from(Vec::with_capacity(ECHO_BYTES))) else {
            break;
        };
        let Ok((Ok(received), buffer)) = receive.await else {
            break;
        };
        if received == 0 {
            break;
        }
        let Ok(send) = stream.send(Lease::from(buffer)) else {
            break;
        };
        if !matches!(send.await, Ok((Ok(_), _))) {
            break;
        }
    }
    if let Ok(close) = stream.close() {
        let _closed = close.await;
    }
}

/// Connects, sends the message of `index`, and receives it back in order.
async fn echo(address: SocketAddr, index: u32) -> Result<(), String> {
    let mut stream = TcpStream::connect(address, TcpOptions::default())
        .map_err(|refused| format!("{:?}", refused.reason()))?
        .await
        .map_err(|_| "the connect ended")?
        .map_err(|error| error.to_string())?;
    let sent = message(index);
    let (count, _lease) = stream
        .send(Lease::from(sent.clone()))
        .map_err(|_| "the send refused")?
        .await
        .map_err(|_| "the send ended")?;
    count.map_err(|error| error.to_string())?;
    let mut received = Vec::new();
    while received.len() < ECHO_BYTES {
        let (count, buffer) = stream
            .recv(Backing::from(Vec::with_capacity(ECHO_BYTES)))
            .map_err(|_| "the receive refused")?
            .await
            .map_err(|_| "the receive ended")?;
        if count.map_err(|error| error.to_string())? == 0 {
            return Err(String::from("the echo ended early"));
        }
        received.extend_from_slice(buffer.bytes());
    }
    stream
        .close()
        .map_err(|_| "the close refused")?
        .await
        .map_err(|_| "the close ended")?;
    if received == sent {
        Ok(())
    } else {
        Err(String::from("the echo changed"))
    }
}

/// Sums the numbers below `NUMBERS`, and yields to the pool after each 10,000.
async fn sum() -> u64 {
    let mut total = 0;
    for number in 0..NUMBERS {
        total += number;
        if number % 10_000 == 0 {
            yield_once().await;
        }
    }
    total
}

/// Returns `Pending` one time, after a wake, so the worker polls other tasks first.
async fn yield_once() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// A TCP connection between two movable streams: partial progress, the reuse of a buffer, a
/// dropped receive, a failed send whose lease goes on another stream, and close.
async fn tcp() -> Result<Vec<String>, String> {
    let steps = async {
        let mut notes = Vec::new();
        let mut listener = TcpListener::bind(loopback(), TcpOptions::default())?.await??;
        let connect = TcpStream::connect(listener.local_addr(), TcpOptions::default())?;
        let (mut server, _peer) = listener.accept()?.await??;
        let mut client = connect.await??;

        let (sent, _lease) = client.send(Lease::from(b"hello world".to_vec()))?.await?;
        let sent = sent?;
        let (received, mut buffer) = server.recv(Backing::from(Vec::with_capacity(8)))?.await?;
        notes.push(format!(
            "tcp: the receive gave {} of the {sent} sent bytes: {:?}",
            received?,
            text(&buffer)
        ));
        buffer.truncate(0);
        let (received, buffer) = server.recv(buffer)?.await?;
        received?;
        notes.push(format!(
            "tcp: the same buffer then received the rest: {:?}",
            text(&buffer)
        ));

        // A receive whose future drops leaves the bytes that come later to the next receive.
        drop(server.recv(Backing::from(Vec::with_capacity(8)))?);
        client.send(Lease::from(b"late".to_vec()))?.await?.0?;
        let (received, buffer) = server.recv(Backing::from(Vec::with_capacity(8)))?.await?;
        received?;
        notes.push(format!(
            "tcp: after a dropped receive, the next receive gave {:?}",
            text(&buffer)
        ));

        // The server closes with bytes that it did not read, so the kernel resets the connection:
        // the receive of the client fails, and then its send fails and gives its lease back.
        client.send(Lease::from(b"unread".to_vec()))?.await?.0?;
        server.close()?.await?;
        let (reset, _buffer) = client.recv(Backing::from(Vec::with_capacity(8)))?.await?;
        reset.err().ok_or("the reset did not fail the receive")?;
        let (failed, lease) = client.send(Lease::from(b"again".to_vec()))?.await?;
        failed.err().ok_or("the send to a peer that reset did not fail")?;
        let connect = TcpStream::connect(listener.local_addr(), TcpOptions::default())?;
        let (mut other, _peer) = listener.accept()?.await??;
        let mut retry = connect.await??;
        retry.send(lease)?.await?.0?;
        let (received, buffer) = other.recv(Backing::from(Vec::with_capacity(8)))?.await?;
        received?;
        notes.push(format!(
            "tcp: a send to a peer that reset failed, and the same lease then went on a new stream: {:?}",
            text(&buffer)
        ));

        client.close()?.await?;
        retry.close()?.await?;
        other.close()?.await?;
        listener.close()?.await?;
        notes.push(String::from("tcp: closed each stream and the listener"));
        Ok::<_, Box<dyn Error>>(notes)
    };
    steps.await.map_err(|error| error.to_string())
}

/// Waits for bytes that never come: the peer neither sends nor closes until the end of the task.
async fn wait_forever(ready: mpsc::Sender<()>) -> Result<(), String> {
    let steps = async {
        let mut listener = TcpListener::bind(loopback(), TcpOptions::default())?.await??;
        let connect = TcpStream::connect(listener.local_addr(), TcpOptions::default())?;
        let (_server, _peer) = listener.accept()?.await??;
        let mut client = connect.await??;
        let receive = client.recv(Backing::from(Vec::with_capacity(8)))?;
        ready.send(()).map_err(|_| "no one waits for the receive")?;
        receive.await?.0?;
        Ok::<(), Box<dyn Error>>(())
    };
    steps.await.map_err(|error| error.to_string())
}

/// The text of a refused spawn, for any future.
fn refused<F>(refused: task::SpawnRefused<F>) -> String {
    format!("the spawn refused: {:?}", refused.reason())
}
