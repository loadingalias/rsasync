//! The movable TCP types on a runtime with two movable workers: an operation stays at the worker
//! that started it while its task goes to the other worker.

use alloc::vec::Vec;
use core::assert_matches;
use core::future::{Future, pending, poll_fn};
use core::mem;
use core::net::SocketAddr;
use core::pin::Pin;
use core::task::Poll;
use core::time::Duration;
use std::collections::HashSet;
use std::io::{Read as _, Write as _};
use std::net;
use std::sync::{Mutex, mpsc};
use std::thread::{self, ThreadId};
use std::time::Instant;

use super::{TcpListener, TcpStream};
use crate::buffer::{Backing, Buffer as _, Lease};
use crate::driver::{Backend, Config, ECANCELED, Reason, SendCount, TcpOptions};
use crate::runtime::Runtime;
use crate::sync::Arc;
use crate::task::{self, JoinHandle, Outcome};

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn buffer(capacity: usize) -> Backing {
    Backing::from(Vec::with_capacity(capacity))
}

/// A runtime with two movable workers. Each one runs on io_uring, which the movable types need.
fn runtime() -> Runtime {
    let driver = Config::new().backend(Backend::IoUring);
    Runtime::builder().movable_workers(2).driver(driver).build().unwrap()
}

/// A listener, which sends its address to `address`, and the stream of its first connection.
async fn accepted(address: mpsc::Sender<SocketAddr>) -> (TcpListener, TcpStream) {
    let mut listener = TcpListener::bind(loopback(), TcpOptions::default())
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    address.send(listener.local_addr()).unwrap();
    let (stream, _peer) = listener.accept().unwrap().await.unwrap().unwrap();
    (listener, stream)
}

/// A client of the standard library that connects to `address`. A read with no data ends after
/// 20 s, so a failed test does not wait for its time limit.
fn connect(address: SocketAddr) -> net::TcpStream {
    let client = net::TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    client
}

/// A client that connects to the address that `addressed` gives.
fn client(addressed: &mpsc::Receiver<SocketAddr>) -> net::TcpStream {
    connect(addressed.recv().unwrap())
}

/// The worker that polled the task before [`leave`] stays blocked until this value drops.
struct Blocked(#[expect(dead_code, reason = "the drop of the sender ends the block")] mpsc::Sender<()>);

/// Moves the task that awaits it to the other worker of the pool, and keeps the worker that it
/// left blocked until the result drops. A worker that is blocked turns no driver, so an operation
/// that it homes makes no progress.
///
/// The task spawns a local task, which stays on the worker of this poll. That worker polls it
/// after its movable tasks: it starts a thread and then blocks the worker. The thread wakes the
/// movable task. The wake comes from no worker, so it queues the task at its last worker, which is
/// blocked, and it wakes one parked worker. That one can be the blocked worker, which counts
/// itself parked. So the thread also cancels a second movable task: the second wake of the pool
/// starts at the next worker, and the other worker then takes the task.
async fn leave() -> Blocked {
    let from = thread::current().id();
    let (release, blocked) = mpsc::channel::<()>();
    let mut blocked = Some(blocked);
    poll_fn(|cx| {
        if thread::current().id() != from {
            return Poll::Ready(());
        }
        if let Some(blocked) = blocked.take() {
            let waker = cx.waker().clone();
            let second = task::spawn_movable(pending::<()>()).unwrap().cancel_handle();
            let blocker = task::spawn_local(async move {
                thread::spawn(move || {
                    waker.wake();
                    second.cancel();
                });
                // The drop of the sender ends the wait, with an error.
                assert!(blocked.recv().is_err());
            });
            drop(blocker.unwrap());
        }
        Poll::Pending
    })
    .await;
    Blocked(release)
}

/// [`leave`] when the thread is `home`, so that the task runs on the other worker.
async fn away_from(home: ThreadId) -> Option<Blocked> {
    if thread::current().id() == home {
        Some(leave().await)
    } else {
        None
    }
}

/// Polls `future` one time.
async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx))).await
}

/// Waits for the next wake of the task.
async fn next_wake() {
    let mut waited = false;
    poll_fn(|_| {
        if waited {
            Poll::Ready(())
        } else {
            waited = true;
            Poll::Pending
        }
    })
    .await;
}

/// Gives the output of `future`, and the thread of the poll that gave it, which is never `home`:
/// each wake from `home` queues the task there, and the task then leaves it again.
async fn elsewhere<F: Future + Unpin>(home: ThreadId, mut future: F) -> (F::Output, ThreadId) {
    loop {
        let blocked = away_from(home).await;
        if let Poll::Ready(output) = poll_once(&mut future).await {
            return (output, thread::current().id());
        }
        drop(blocked);
        next_wake().await;
    }
}

/// R402: the home of an operation is the worker that started it. The task goes to the other
/// worker while its receive runs. The home has no task then, and it still completes the receive.
/// The task takes the result on the other worker.
#[test]
fn a_task_that_another_worker_takes_during_its_receive_gets_the_bytes_on_that_worker() {
    let runtime = runtime();
    let (address, addressed) = mpsc::channel();
    let (waits, waiting) = mpsc::channel();
    let task = runtime
        .handle()
        .unwrap()
        .spawn_movable(async move {
            let (_listener, mut stream) = accepted(address).await;
            let home = thread::current().id();
            let mut receive = stream.recv(buffer(64)).unwrap();
            let blocked = leave().await;
            assert_ne!(thread::current().id(), home, "the other worker took the task");
            assert!(poll_once(&mut receive).await.is_pending());
            // The home turns its driver again, with no task.
            drop(blocked);
            waits.send(()).unwrap();
            let (output, taker) = elsewhere(home, receive).await;
            let (received, bytes) = output.unwrap();
            (received.unwrap(), bytes.bytes().to_vec(), taker != home)
        })
        .unwrap();
    let mut client = client(&addressed);
    waiting.recv().unwrap();
    client.write_all(b"ping").unwrap();
    let Outcome::Completed((received, bytes, elsewhere)) = task.join_blocking() else {
        panic!("the task ended with no output");
    };
    assert_eq!((received, bytes.as_slice()), (4, b"ping".as_slice()));
    assert!(elsewhere, "the other worker took the result");
}

/// R402: a cancel on another worker reaches the home of the operation, which ends it. The peer
/// sends nothing, so only the cancel ends the receive.
#[test]
fn a_cancel_on_another_worker_ends_the_receive_at_its_home() {
    let runtime = runtime();
    let (address, addressed) = mpsc::channel();
    let task = runtime
        .handle()
        .unwrap()
        .spawn_movable(async move {
            let (_listener, mut stream) = accepted(address).await;
            let home = thread::current().id();
            let mut receive = stream.recv(buffer(64)).unwrap();
            let blocked = leave().await;
            assert!(poll_once(&mut receive).await.is_pending());
            let canceller = thread::current().id();
            receive.cancel();
            drop(blocked);
            let (received, bytes) = receive.await.unwrap();
            (
                received.unwrap_err().raw_os_error(),
                bytes.initialized(),
                canceller != home,
            )
        })
        .unwrap();
    let _client = client(&addressed);
    assert_matches!(task.join_blocking(), Outcome::Completed((Some(ECANCELED), 0, true)));
}

/// R402: a close on another worker cancels the receive at its home, ends after it, and closes the
/// socket, so the peer reads the end of the stream. The receive has no future: the stream keeps
/// it.
#[test]
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
fn a_close_on_another_worker_ends_the_receive_at_its_home_and_closes_the_socket() {
    let runtime = runtime();
    let (address, addressed) = mpsc::channel();
    let task = runtime
        .handle()
        .unwrap()
        .spawn_movable(async move {
            let (_listener, mut stream) = accepted(address).await;
            mem::forget(stream.recv(buffer(64)).unwrap());
            let blocked = leave().await;
            let close = stream.close().unwrap();
            drop(blocked);
            close.await.unwrap().sent()
        })
        .unwrap();
    let mut client = client(&addressed);
    assert_matches!(task.join_blocking(), Outcome::Completed(SendCount::Settled));
    assert_eq!(
        client.read(&mut [0; 1]).unwrap(),
        0,
        "the peer reads the end of the stream"
    );
}

/// The bytes that a dropped receive took stay with the stream. The next receives start on the
/// other worker, whose driver takes the bytes from the home of the dropped receive: they come in
/// order, and none is lost.
#[test]
fn the_bytes_of_a_dropped_receive_go_to_the_next_receives_on_another_worker_in_order() {
    let runtime = runtime();
    let (address, addressed) = mpsc::channel();
    let (waits, waiting) = mpsc::channel();
    let task = runtime
        .handle()
        .unwrap()
        .spawn_movable(async move {
            let (_listener, mut stream) = accepted(address).await;
            let home = thread::current().id();
            let mut first = stream.recv(buffer(64)).unwrap();
            assert!(poll_once(&mut first).await.is_pending());
            waits.send(()).unwrap();
            // The end of the receive is the only wake of the task. The task does not take the
            // result.
            next_wake().await;
            drop(first);
            let blocked = away_from(home).await;
            let (received, start) = stream.recv(buffer(2)).unwrap().await.unwrap();
            assert_eq!(received.unwrap(), 2);
            let (received, rest) = stream.recv(buffer(64)).unwrap().await.unwrap();
            assert_eq!(received.unwrap(), 1);
            drop(blocked);
            [start.bytes(), rest.bytes()].concat()
        })
        .unwrap();
    let mut client = client(&addressed);
    waiting.recv().unwrap();
    client.write_all(b"abc").unwrap();
    assert_matches!(task.join_blocking(), Outcome::Completed(bytes) if bytes == b"abc");
}

/// A dropped send runs on at its home. A send that starts on the other worker waits for it, so
/// the bytes of the stream keep their order when the two sends have different homes. The home of
/// the first send is blocked, so that send is not in the kernel when the second one starts.
#[test]
fn a_send_on_another_worker_starts_after_the_dropped_send_of_its_direction() {
    let runtime = runtime();
    let (address, addressed) = mpsc::channel();
    let (blocks, blocking) = mpsc::channel();
    let task = runtime
        .handle()
        .unwrap()
        .spawn_movable(async move {
            let (_listener, mut stream) = accepted(address).await;
            drop(stream.send(Lease::from(b"first".to_vec())).unwrap());
            let blocked = leave().await;
            let second = stream.send(Lease::from(b"second".to_vec())).unwrap();
            // The test ends the block after it saw that no byte came.
            blocks.send(blocked).unwrap();
            let (sent, _lease) = second.await.unwrap();
            sent.unwrap()
        })
        .unwrap();
    let mut client = client(&addressed);
    let blocked = blocking.recv().unwrap();
    client.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
    let early = client.read(&mut [0; 16]);
    assert!(
        early.is_err(),
        "a send ran before the first send of its direction: {early:?}"
    );
    client.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    drop(blocked);
    let mut bytes = [0; 11];
    client.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"firstsecond");
    assert_matches!(task.join_blocking(), Outcome::Completed(6));
}

/// A receive that starts while an earlier one runs waits at its own worker. A cancel ends it at
/// once, also while the home of the earlier receive is blocked, and so does the drop of its
/// future. Each one gives the earlier receive back, so the next receive still waits for it and
/// then gets each byte.
#[test]
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
fn a_receive_that_ends_before_its_start_leaves_the_earlier_receive_to_the_next_one() {
    let runtime = runtime();
    let (address, addressed) = mpsc::channel();
    let (waits, waiting) = mpsc::channel();
    let task = runtime
        .handle()
        .unwrap()
        .spawn_movable(async move {
            let (_listener, mut stream) = accepted(address).await;
            mem::forget(stream.recv(buffer(64)).unwrap());
            let blocked = leave().await;
            let cancelled = stream.recv(buffer(64)).unwrap();
            cancelled.cancel();
            let (received, bytes) = cancelled.await.unwrap();
            assert_eq!(received.unwrap_err().raw_os_error(), Some(ECANCELED));
            assert_eq!(bytes.initialized(), 0);
            drop(stream.recv(buffer(64)).unwrap());
            let receive = stream.recv(buffer(64)).unwrap();
            drop(blocked);
            waits.send(()).unwrap();
            let (received, bytes) = receive.await.unwrap();
            (received.unwrap(), bytes.bytes().to_vec())
        })
        .unwrap();
    let mut client = client(&addressed);
    waiting.recv().unwrap();
    client.write_all(b"abc").unwrap();
    assert_matches!(task.join_blocking(), Outcome::Completed((3, bytes)) if bytes == b"abc");
}

/// A close takes the dropped sends of both workers. The second send waits for the first one, so
/// the close ends it before its start. The first send ran, and the close cancelled it, so the
/// close reports its bytes: the caller cannot know its framing. The peer reads only those bytes.
#[test]
fn a_close_on_another_worker_reports_the_bytes_of_the_dropped_sends_of_both_workers() {
    let runtime = runtime();
    let (address, addressed) = mpsc::channel();
    let task = runtime
        .handle()
        .unwrap()
        .spawn_movable(async move {
            let (_listener, mut stream) = accepted(address).await;
            drop(stream.send(Lease::from(b"first".to_vec())).unwrap());
            let blocked = leave().await;
            drop(stream.send(Lease::from(b"second".to_vec())).unwrap());
            let close = stream.close().unwrap();
            drop(blocked);
            close.await.unwrap().sent()
        })
        .unwrap();
    let mut client = client(&addressed);
    assert_matches!(task.join_blocking(), Outcome::Completed(SendCount::Sent(5)));
    let mut bytes = Vec::new();
    client.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"first");
}

/// The completions of one turn of a driver make several tasks ready at one worker. That worker
/// wakes the other one, which takes a part of them. Each receive has its home at one worker,
/// because the other one is blocked while they start.
#[test]
fn the_other_worker_takes_a_part_of_the_tasks_that_one_turn_of_a_driver_made_ready() {
    const TASKS: usize = 8;
    let runtime = runtime();
    let (address, addressed) = mpsc::channel();
    let (waits, waiting) = mpsc::channel();
    let pollers = Arc::new(Mutex::new(Vec::new()));
    let parent = {
        let pollers = Arc::clone(&pollers);
        runtime.handle().unwrap().spawn_movable(async move {
            let (mut listener, first) = accepted(address).await;
            let mut streams = Vec::from([first]);
            while streams.len() < TASKS {
                streams.push(listener.accept().unwrap().await.unwrap().unwrap().0);
            }
            // No other worker takes a task before its receive started here.
            let blocked = leave().await;
            let mut tasks = Vec::new();
            for mut stream in streams {
                let (waits, pollers) = (waits.clone(), Arc::clone(&pollers));
                let task = task::spawn_movable(async move {
                    let mut receive = stream.recv(buffer(1)).unwrap();
                    assert!(poll_once(&mut receive).await.is_pending());
                    waits.send(()).unwrap();
                    let (received, _byte) = receive.await.unwrap();
                    assert_eq!(received.unwrap(), 1);
                    pollers.lock().unwrap().push(thread::current().id());
                    // Work with no wake, which only a steal moves to the other worker.
                    let end = Instant::now() + Duration::from_millis(20);
                    while Instant::now() < end {
                        core::hint::spin_loop();
                    }
                });
                tasks.push(task.unwrap());
            }
            (blocked, tasks)
        })
    };
    let address = addressed.recv().unwrap();
    let mut clients: Vec<_> = (0..TASKS).map(|_| connect(address)).collect();
    for _ in 0..TASKS {
        waiting.recv().unwrap();
    }
    let Outcome::Completed((blocked, tasks)) = parent.unwrap().join_blocking() else {
        panic!("the parent ended with no output");
    };
    drop(blocked);
    for client in &mut clients {
        client.write_all(b"x").unwrap();
    }
    for task in tasks {
        let task: JoinHandle<()> = task;
        assert_matches!(task.join_blocking(), Outcome::Completed(()));
    }
    let workers: HashSet<ThreadId> = pollers.lock().unwrap().iter().copied().collect();
    assert_eq!(
        workers.len(),
        2,
        "one worker ran each task, and the other one stayed parked"
    );
}

/// A shutdown cancels a movable task, whose stream drops with a receive that runs. The home of the
/// receive ends it before the worker exits, and the socket closes then, so the peer reads the end
/// of the stream.
#[test]
fn a_shutdown_ends_the_receive_of_a_dropped_movable_stream_and_closes_its_socket() {
    let runtime = runtime();
    let (address, addressed) = mpsc::channel();
    let (waits, waiting) = mpsc::channel();
    let task = runtime
        .handle()
        .unwrap()
        .spawn_movable(async move {
            let (_listener, mut stream) = accepted(address).await;
            let mut receive = stream.recv(buffer(64)).unwrap();
            assert!(poll_once(&mut receive).await.is_pending());
            waits.send(()).unwrap();
            drop(receive.await);
        })
        .unwrap();
    let mut client = client(&addressed);
    waiting.recv().unwrap();
    let report = runtime.shutdown();
    assert_eq!(report.movable_cancelled(), 1);
    assert_matches!(task.join_blocking(), Outcome::Cancelled);
    assert_eq!(
        client.read(&mut [0; 1]).unwrap(),
        0,
        "the peer reads the end of the stream"
    );
}

/// Only io_uring runs the operations of a movable resource. A driver on epoll refuses each call,
/// so no operation waits there with no progress.
#[test]
fn a_worker_on_epoll_refuses_each_movable_call() {
    let driver = Config::new().backend(Backend::Epoll);
    let runtime = Runtime::builder().driver(driver).build().unwrap();
    runtime
        .block_on(async {
            let bind = TcpListener::bind(loopback(), TcpOptions::default()).unwrap_err();
            assert_eq!(bind.reason(), Reason::Unsupported);
            let connect = TcpStream::connect(loopback(), TcpOptions::default()).unwrap_err();
            assert_eq!(connect.reason(), Reason::Unsupported);
        })
        .unwrap();
}
