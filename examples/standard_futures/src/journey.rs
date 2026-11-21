//! The steps of the example. Each step gives one line for each outcome.

use core::error::Error;
use core::future::{Future, join, poll_fn};
use core::net::SocketAddr;
use core::pin::pin;
use core::task::Poll;
use core::time::Duration;
use std::io::Write;

use rsasync::buffer::{Backing, Buffer as _, BufferMut as _, Lease};
use rsasync::clock::NativeClock;
use rsasync::driver::TcpOptions;
use rsasync::local::{TcpListener, TcpStream};
use rsasync::runtime::{Runtime, WorkerId};
use rsasync::task::{Outcome, timer};
use rsasync::time::{Deadline, Suspend};

/// Runs the task on worker 0, prints its lines, and stops the runtime with a report.
pub(crate) fn run(out: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::builder().local_workers(1).build()?;
    let mut handle = runtime.handle()?;
    let task = handle
        .spawn_on(WorkerId::new(0), || async {
            steps().await.map_err(|error| error.to_string())
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
        "shutdown: {} tasks cancelled, {} operations of debt, grace period expired: {}",
        worker.cancelled(),
        worker.debt(),
        report.expired()
    )?;
    Ok(())
}

/// What [`select`] gives: the output of the future that was ready first.
enum First<A, B> {
    Left(A),
    Right(B),
}

/// Polls `left` and then `right` in one task, and gives the first output. The other future drops
/// with the call, as a future that a select abandons.
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

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// A listener, the stream that it accepted, and the stream that connected to it.
async fn pair() -> Result<(TcpListener, TcpStream, TcpStream), Box<dyn Error>> {
    let mut listener = TcpListener::bind(loopback(), TcpOptions::default())?.await??;
    let connect = TcpStream::connect(listener.local_addr(), TcpOptions::default())?;
    let (server, _peer) = listener.accept()?.await??;
    let client = connect.await??;
    Ok((listener, server, client))
}

/// The text of the initialized bytes of `buffer`.
fn text(buffer: &Backing) -> String {
    String::from_utf8_lossy(buffer.bytes()).into_owned()
}

/// The steps of the task.
async fn steps() -> Result<Vec<String>, Box<dyn Error>> {
    let mut lines = Vec::new();
    let (listener, mut server, mut client) = pair().await?;

    // One join runs a send and a receive together. Each future borrows a stream on the stack of
    // the task, and the check borrows the greeting there too.
    let greeting = String::from("hello");
    let send = client.send(Lease::from(greeting.as_bytes().to_vec()))?;
    let receive = server.recv(Backing::from(Vec::with_capacity(16)))?;
    let (sent, received) = join!(send, receive).await;
    sent?.0?;
    let (received, buffer) = received?;
    received?;
    let matches = buffer.bytes() == greeting.as_bytes();
    lines.push(format!(
        "join: a send and a receive ran together, and the receive gave {:?}, as the stack data of the task says",
        if matches {
            text(&buffer)
        } else {
            String::from("other bytes")
        }
    ));

    // Partial progress, and the reuse of the buffer after the success.
    client.send(Lease::from(b"hello".to_vec()))?.await?.0?;
    let (received, mut buffer) = server.recv(Backing::from(Vec::with_capacity(4)))?.await?;
    received?;
    lines.push(format!("partial: a receive into 4 bytes gave {:?}", text(&buffer)));
    buffer.truncate(0);
    let (received, buffer) = server.recv(buffer)?.await?;
    received?;
    lines.push(format!(
        "partial: the same buffer then received the rest: {:?}",
        text(&buffer)
    ));

    // A select abandons a receive that gets no data: its future drops, and the stream gives the
    // bytes that come later to the next receive.
    {
        let clock = NativeClock::new(Suspend::Pauses)?;
        let deadline = clock.now().checked_add(Duration::from_millis(20))?;
        let receive = server.recv(Backing::from(Vec::with_capacity(16)))?;
        match select(timer(Deadline::at(deadline))?, receive).await {
            First::Left(fired) => {
                fired?;
                lines.push(String::from(
                    "select: the timer came first, and the receive with no data dropped",
                ));
            }
            First::Right(_received) => return Err("the receive with no data ended first".into()),
        }
    }
    client.send(Lease::from(b"after".to_vec()))?.await?.0?;
    let (received, buffer) = server.recv(Backing::from(Vec::with_capacity(16)))?.await?;
    received?;
    lines.push(format!(
        "select: the next receive on the same stream gave {:?}",
        text(&buffer)
    ));

    // A failure gives the lease back. After the half-close of the server, its send fails, and the
    // same lease then goes on a new stream.
    server.shutdown_write()?.await??;
    let (sent, lease) = server.send(Lease::from(b"again".to_vec()))?.await?;
    let kind = sent.err().ok_or("the send after the half-close did not fail")?.kind();
    lines.push(format!(
        "failure: a send after the half-close gave {kind:?} and its lease back"
    ));
    server.close()?.await?;
    client.close()?.await?;
    let (listener_two, mut again, mut sender) = pair().await?;
    sender.send(lease)?.await?.0?;
    let (received, buffer) = again.recv(Backing::from(Vec::with_capacity(16)))?.await?;
    received?;
    lines.push(format!(
        "failure: the same lease then sent {:?} on a new stream",
        text(&buffer)
    ));

    for stream in [again, sender] {
        stream.close()?.await?;
    }
    for listener in [listener, listener_two] {
        listener.close()?.await?;
    }
    Ok(lines)
}
