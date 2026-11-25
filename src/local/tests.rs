use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;
use core::assert_matches;
use core::cell::RefCell;
use core::future::{Future as _, pending, poll_fn};
use core::net::SocketAddr;
use core::num::NonZeroU64;
use core::pin::Pin;
use core::task::Poll;
use core::time::Duration;
use std::io::Read as _;
use std::sync::mpsc;
use std::thread::{self, ThreadId};
use std::thread_local;
use std::time::Instant;

use std::fs;
use std::os::fd::OwnedFd;
use std::os::unix::fs::MetadataExt as _;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use super::{
    Child, Directory, File, IpcListener, IpcStream, PipeReader, PipeWriter, TcpListener, TcpStream, UdpSocket, rename,
    signal, unlink,
};
use crate::buffer::{Backing, Buffer as _, Lease};
use crate::capacity::Budget;
use crate::channel::oneshot;
use crate::driver::{
    ChildOptions, Config, Datagram, End, Escalation, OpenOptions, Reason, SendCount, Shot, Signal, Stale, SyncMode,
    TcpOptions, UdpOptions, WriteMode,
};
use crate::runtime::{GroupError, Runtime, WorkerId};
use crate::task;
use crate::task::tests::yield_once;

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn buffer() -> Backing {
    Backing::from(Vec::with_capacity(64))
}

/// A listener, the stream that it accepted, and the stream that connected to it.
async fn pair() -> (TcpListener, TcpStream, TcpStream) {
    let mut listener = TcpListener::bind(loopback(), TcpOptions::default())
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    let connect = TcpStream::connect(listener.local_addr(), TcpOptions::default()).unwrap();
    let (server, _peer) = listener.accept().unwrap().await.unwrap().unwrap();
    let client = connect.await.unwrap().unwrap();
    (listener, server, client)
}

/// The local TCP types run each operation on the driver of their owner, with no driver argument:
/// a send and a receive on a stream, both at one time on its halves, a half-close, and each close.
#[test]
fn bytes_cross_local_tcp_streams_and_their_halves() {
    let runtime = Runtime::builder().build().unwrap();
    runtime
        .block_on(async {
            let (listener, mut server, mut client) = pair().await;
            let (sent, _lease) = client.send(Lease::from(b"ping".to_vec())).unwrap().await.unwrap();
            assert_eq!(sent.unwrap(), 4);
            let (received, ping) = server.recv(buffer()).unwrap().await.unwrap();
            assert_eq!((received.unwrap(), ping.bytes()), (4, &b"ping"[..]));

            let (mut read, mut write) = client.split();
            let receive = read.recv(buffer()).unwrap();
            let send = write.send(Lease::from(b"data".to_vec())).unwrap();
            let (sent, _lease) = server.send(Lease::from(b"pong".to_vec())).unwrap().await.unwrap();
            assert_eq!(sent.unwrap(), 4);
            let (received, pong) = receive.await.unwrap();
            assert_eq!((received.unwrap(), pong.bytes()), (4, &b"pong"[..]));
            assert_eq!(send.await.unwrap().0.unwrap(), 4);
            let (received, data) = server.recv(buffer()).unwrap().await.unwrap();
            assert_eq!((received.unwrap(), data.bytes()), (4, &b"data"[..]));

            client.shutdown_write().unwrap().await.unwrap().unwrap();
            let (end, _buffer) = server.recv(buffer()).unwrap().await.unwrap();
            assert_eq!(end.unwrap(), 0, "the half-close ends the stream of the peer");
            server.close().unwrap().await.unwrap();
            client.close().unwrap().await.unwrap();
            listener.close().unwrap().await.unwrap();
        })
        .unwrap();
}

/// A local stream receives into the provided buffers of the driver of its owner, with one
/// submission for each receive and with the multishot receive of its stream, and the bytes come
/// in order. A runtime with no provided buffers refuses the receive with `Reason::Buffers`.
#[test]
fn a_local_stream_receives_into_the_provided_buffers_of_its_owner() {
    for shot in [Shot::Single, Shot::Multi] {
        let runtime = Runtime::builder().driver(Config::new().receives(shot)).build().unwrap();
        runtime
            .block_on(async {
                let (_listener, mut server, mut client) = pair().await;
                let (mut read, _write) = server.split();
                for bytes in [&b"first"[..], b"second"] {
                    let (sent, _lease) = client.send(Lease::from(bytes.to_vec())).unwrap().await.unwrap();
                    assert_eq!(sent.unwrap(), bytes.len());
                    let received = read.recv_selected().unwrap().await.unwrap().unwrap();
                    assert_eq!(received.bytes(), bytes);
                }
            })
            .unwrap();
    }
    let runtime = Runtime::builder().receive_buffers(None).build().unwrap();
    runtime
        .block_on(async {
            let (_listener, mut server, _client) = pair().await;
            assert_eq!(server.recv_selected().unwrap_err().reason(), Reason::Buffers);
        })
        .unwrap();
}

/// A receive that the root submits and polls, and that a task then awaits, wakes that task at its
/// completion: the wake goes to the latest waker, not to the task that submitted. A wake of the
/// root only would leave the task pending, and the join would never end.
#[test]
fn a_receive_that_another_task_awaits_wakes_that_task() {
    let runtime = Runtime::builder().build().unwrap();
    runtime
        .block_on(async {
            let (_listener, server, mut client) = pair().await;
            // The task needs a `'static` future, so the stream outlives `block_on`, whose end
            // closes it.
            let server = Box::leak(Box::new(server));
            let mut receive = server.recv(buffer()).unwrap();
            let polled = poll_fn(|cx| Poll::Ready(Pin::new(&mut receive).poll(cx).is_pending())).await;
            assert!(polled, "no byte came before the send");
            let waiter = task::spawn_local(receive).unwrap();
            yield_once().await;
            let (sent, _lease) = client.send(Lease::from(b"x".to_vec())).unwrap().await.unwrap();
            assert_eq!(sent.unwrap(), 1);
            let task::Outcome::Completed(Ok((received, bytes))) = waiter.join().await else {
                panic!("the task that awaits the receive completes");
            };
            assert_eq!((received.unwrap(), bytes.bytes()), (1, &b"x"[..]));
        })
        .unwrap();
}

/// The local UDP socket sends and receives whole datagrams with their source, also both at one
/// time on its halves, and closes.
#[test]
fn datagrams_cross_local_udp_sockets_and_their_halves() {
    let runtime = Runtime::builder().build().unwrap();
    runtime
        .block_on(async {
            let mut first = UdpSocket::bind(loopback(), UdpOptions::default())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let mut second = UdpSocket::bind(loopback(), UdpOptions::default())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let one = Datagram::new(Lease::from(b"one".to_vec()), second.local_addr());
            assert_eq!(first.send_to(one).unwrap().await.unwrap().0.unwrap(), 1);
            let (received, bytes) = second.recv_from(buffer()).unwrap().await.unwrap();
            let received = received.unwrap();
            assert_eq!(
                (received.len, received.source, bytes.bytes()),
                (3, first.local_addr(), &b"one"[..])
            );

            let second_address = second.local_addr();
            let (mut read, mut write) = second.split();
            let receive = read.recv_from(buffer()).unwrap();
            let two = Datagram::new(Lease::from(b"two".to_vec()), first.local_addr());
            assert_eq!(write.send_to(two).unwrap().await.unwrap().0.unwrap(), 1);
            let three = Datagram::new(Lease::from(b"three".to_vec()), second_address);
            assert_eq!(first.send_to(three).unwrap().await.unwrap().0.unwrap(), 1);
            let (received, bytes) = receive.await.unwrap();
            assert_eq!((received.unwrap().len, bytes.bytes()), (5, &b"three"[..]));
            let (received, bytes) = first.recv_from(buffer()).unwrap().await.unwrap();
            assert_eq!((received.unwrap().len, bytes.bytes()), (3, &b"two"[..]));

            first.close().unwrap().await.unwrap();
            second.close().unwrap().await.unwrap();
        })
        .unwrap();
}

/// The bytes of one send that fills the socket buffers.
const FILL: usize = 64 << 20;

/// Waits until the bytes that wait at `peer` stay the same for 100 ms, while the owner turns: the
/// receive window of the peer is closed, so no acknowledgment frees room in the send buffer.
async fn until_window_closes(peer: &std::net::TcpStream) {
    let mut probe = vec![0; FILL];
    let mut before = None;
    loop {
        // A turn of the owner submits a new send, and its bytes then reach the peer.
        yield_once().await;
        thread::sleep(Duration::from_millis(100));
        let waiting = peer.peek(&mut probe).unwrap();
        if before == Some(waiting) {
            return;
        }
        before = Some(waiting);
    }
}

/// A close of a stream whose peer neither reads nor sends cancels a dropped receive and a dropped
/// send that waits in the kernel. A send waits there when the send buffer is full and the receive
/// window of the peer is closed, so each send before it took only what fit. The future of the close
/// ends after the terminal events, and its report gives the count of the dropped send: 0.
#[test]
fn a_close_cancels_a_dropped_send_to_a_peer_that_never_reads_and_ends() {
    /// The sends that can end before one waits: each one fills the room that the last one left.
    const FILLS: usize = 16;
    let runtime = Runtime::builder().build().unwrap();
    let sent = runtime
        .block_on(async {
            let mut listener = TcpListener::bind(loopback(), TcpOptions::default())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
            peer.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            let (mut stream, _peer) = listener.accept().unwrap().await.unwrap().unwrap();
            let (mut read, mut write) = stream.split();
            let mut waits = None;
            for _ in 0..FILLS {
                let mut send = write.send(Lease::from(vec![7; FILL])).unwrap();
                until_window_closes(&peer).await;
                if poll_fn(|cx| Poll::Ready(Pin::new(&mut send).poll(cx)))
                    .await
                    .is_pending()
                {
                    waits = Some(send);
                    break;
                }
            }
            let send = waits.expect("a send waits in the kernel");
            let receive = read.recv(buffer()).unwrap();
            // A turn of the owner submits the receive, and the kernel holds both.
            yield_once().await;
            drop((receive, send));
            let closed = stream.close().unwrap().await.unwrap();
            drop(peer);
            closed.sent()
        })
        .unwrap();
    assert_eq!(sent, SendCount::Sent(0));
}

/// A stream that leaves its `block_on` refuses each call and gives the buffer back: with no owner
/// on the thread with `Reason::NoDriver`, and in the next `block_on`, whose driver has its own
/// stream in the same slot, with `Reason::Resource`. An option read with no owner gives an error
/// of the kind `NotFound`, which allocates nothing.
#[test]
fn a_stream_after_its_block_on_refuses_and_gives_its_buffer_back() {
    let runtime = Runtime::builder().build().unwrap();
    let (_listener, mut stream, _client) = runtime.block_on(pair()).unwrap();

    let given = buffer();
    let address = given.as_ptr();
    let refused = stream.recv(given).unwrap_err();
    assert_eq!(refused.reason(), Reason::NoDriver);
    assert_eq!(refused.into_inner().as_ptr(), address);
    assert_eq!(stream.options().unwrap_err().kind(), std::io::ErrorKind::NotFound);

    runtime
        .block_on(async {
            let (_listener, _server, _client) = pair().await;
            let given = buffer();
            let address = given.as_ptr();
            let refused = stream.recv(given).unwrap_err();
            assert_eq!(refused.reason(), Reason::Resource);
            assert_eq!(refused.into_inner().as_ptr(), address);
        })
        .unwrap();
}

/// A receive that a `block_on` started ends with `Ended` at a poll outside that owner, and does not
/// wait: no turn of its driver comes there, so a wait would never end.
#[test]
fn a_receive_polled_outside_its_owner_ends_and_does_not_wait() {
    let runtime = Runtime::builder().build().unwrap();
    let (_listener, _client, mut receive) = runtime
        .block_on(async {
            let (listener, server, client) = pair().await;
            // The receive needs a stream that outlives `block_on`.
            let server = Box::leak(Box::new(server));
            (listener, client, server.recv(buffer()).unwrap())
        })
        .unwrap();
    let polled = Pin::new(&mut receive).poll(&mut core::task::Context::from_waker(core::task::Waker::noop()));
    assert_matches!(polled, Poll::Ready(Err(_)));
}

/// A stream that drops while its owner runs closes, so its peer reads the end of the stream before
/// `block_on` ends.
#[test]
fn a_dropped_stream_closes_while_its_owner_runs() {
    let runtime = Runtime::builder().build().unwrap();
    let budget = Budget::new(NonZeroU64::new(1 << 16).unwrap());
    runtime
        .block_on(async {
            let mut listener = TcpListener::bind(loopback(), TcpOptions::default())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let mut peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
            let (stream, _peer) = listener.accept().unwrap().await.unwrap().unwrap();
            drop(stream);
            let (sender, mut receiver) = oneshot::channel(&budget).unwrap();
            let reader = thread::spawn(move || {
                peer.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                let read = peer.read(&mut [0; 8]).map_err(|error| error.kind());
                sender.send(read).unwrap();
            });
            assert_eq!(
                receiver.recv().await.unwrap(),
                Ok(0),
                "the peer reads the end of the stream"
            );
            reader.join().unwrap();
        })
        .unwrap();
}

/// A listener that drops while each operation slot of its driver is in use closes at its drop, so a
/// plain listener binds its address at once.
#[test]
fn a_listener_that_drops_while_each_operation_slot_is_in_use_closes_at_its_drop() {
    let runtime = Runtime::builder().driver(Config::new().operations(1)).build().unwrap();
    runtime
        .block_on(async {
            let listener = TcpListener::bind(loopback(), TcpOptions::default())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let address = listener.local_addr();
            let mut socket = UdpSocket::bind(loopback(), UdpOptions::default())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            // A receive that no datagram ends holds the one operation slot.
            let _receive = socket.recv_from(buffer()).unwrap();
            let refused = TcpStream::connect(address, TcpOptions::default()).unwrap_err();
            assert_eq!(refused.reason(), Reason::Operations, "each operation slot is in use");
            drop(listener);
            assert!(
                std::net::TcpListener::bind(address).is_ok(),
                "the listener closed at its drop"
            );
        })
        .unwrap();
}

thread_local! {
    /// A listener that a test keeps on its thread.
    static KEPT: RefCell<Option<TcpListener>> = const { RefCell::new(None) };
}

/// The bytes of a lease whose drop drops the listener in [`KEPT`].
struct DropsKept;

impl AsRef<[u8]> for DropsKept {
    fn as_ref(&self) -> &[u8] {
        b"drop"
    }
}

impl Drop for DropsKept {
    fn drop(&mut self) {
        drop(KEPT.take());
    }
}

/// A listener that drops in a destructor that a turn of its driver runs closes in a later turn, so
/// a plain listener then binds its address. The driver drops the lease of a send whose future
/// dropped at the end of the send, and the drop of the lease drops the listener.
#[test]
fn a_listener_that_drops_in_a_turn_of_its_driver_closes_in_a_later_turn() {
    let runtime = Runtime::builder().build().unwrap();
    runtime
        .block_on(async {
            let listener = TcpListener::bind(loopback(), TcpOptions::default())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let address = listener.local_addr();
            KEPT.set(Some(listener));
            let mut socket = UdpSocket::bind(loopback(), UdpOptions::default())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let datagram = Datagram::new(Lease::from_owner(DropsKept, 4), socket.local_addr());
            drop(socket.send_to(datagram).unwrap());
            let deadline = Instant::now() + Duration::from_secs(10);
            while KEPT.with_borrow(Option::is_some) || std::net::TcpListener::bind(address).is_err() {
                assert!(Instant::now() < deadline, "the listener stays open");
                yield_once().await;
            }
        })
        .unwrap();
}

thread_local! {
    /// A socket that a thread keeps after its `block_on`, until the thread ends.
    static CACHED: RefCell<Option<UdpSocket>> = const { RefCell::new(None) };
}

/// A socket that a thread-local value keeps after its `block_on` drops at the exit of the thread,
/// after the destructor of the owner of the thread, and the process does not abort.
#[test]
fn a_socket_that_a_thread_local_value_keeps_drops_at_the_exit_of_the_thread() {
    thread::spawn(|| {
        // The first use registers the destructor of the cache before the destructor of the owner,
        // so the destructor of the owner runs first at the exit of the thread.
        CACHED.with(|_| ());
        let runtime = Runtime::builder().build().unwrap();
        let socket = runtime
            .block_on(async {
                UdpSocket::bind(loopback(), UdpOptions::default())
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap()
            })
            .unwrap();
        CACHED.set(Some(socket));
    })
    .join()
    .unwrap();
}

/// Takes the report of each member of a group of four, in the order of the probes that `probe`
/// sends: one probe, then its report. Each member closes its socket before its report, so the next
/// probe reaches another member. Gives the threads of the four members.
fn four_threads(reports: &mpsc::Receiver<ThreadId>, mut probe: impl FnMut()) -> Vec<ThreadId> {
    (0..4)
        .map(|_| {
            probe();
            reports.recv_timeout(Duration::from_secs(10)).unwrap()
        })
        .collect()
}

/// Asserts that `threads` are distinct, and that none of them is this thread.
fn assert_distinct(threads: &[ThreadId]) {
    for (index, thread) in threads.iter().enumerate() {
        assert_ne!(
            *thread,
            thread::current().id(),
            "a member runs on the thread of the test"
        );
        assert!(
            !threads[index + 1..].contains(thread),
            "two members run on one thread: {threads:?}"
        );
    }
}

/// A group of four workers binds one listener on each worker at one port, and each listener
/// accepts a connection on its own worker thread. Each member accepts one connection, closes its
/// listener, and then reports its thread, so the kernel gives the next connection to another
/// member.
#[test]
fn a_tcp_group_of_four_workers_accepts_connections_on_each_worker() {
    let runtime = Runtime::builder().local_workers(4).build().unwrap();
    let mut handle = runtime.handle().unwrap();
    let (report, reports) = mpsc::channel();
    let serve = move |_worker, mut listener: TcpListener| {
        let report = report.clone();
        async move {
            let (_stream, _peer) = listener.accept().unwrap().await.unwrap().unwrap();
            listener.close().unwrap().await.unwrap();
            report.send(thread::current().id()).unwrap();
        }
    };
    let group = runtime
        .block_on(handle.tcp_group(loopback(), TcpOptions::default(), serve))
        .unwrap()
        .unwrap();
    let threads = four_threads(&reports, || {
        std::net::TcpStream::connect(group.local_addr()).unwrap();
    });
    assert_distinct(&threads);
}

/// A group of four workers binds one UDP socket on each worker at one port, and each socket
/// receives a datagram on its own worker thread. Each member receives one datagram, closes its
/// socket, and then reports its thread, so the kernel gives the next datagram to another member.
#[test]
fn a_udp_group_of_four_workers_receives_datagrams_on_each_worker() {
    let runtime = Runtime::builder().local_workers(4).build().unwrap();
    let mut handle = runtime.handle().unwrap();
    let (report, reports) = mpsc::channel();
    let serve = move |_worker, mut socket: UdpSocket| {
        let report = report.clone();
        async move {
            let (received, _buffer) = socket.recv_from(buffer()).unwrap().await.unwrap();
            received.unwrap();
            socket.close().unwrap().await.unwrap();
            report.send(thread::current().id()).unwrap();
        }
    };
    let group = runtime
        .block_on(handle.udp_group(loopback(), UdpOptions::default(), serve))
        .unwrap()
        .unwrap();
    let client = std::net::UdpSocket::bind(loopback()).unwrap();
    let threads = four_threads(&reports, || {
        client.send_to(b"which", group.local_addr()).unwrap();
    });
    assert_distinct(&threads);
}

/// A group start that the budget or the allocator refuses gives a typed refusal: the budget pays
/// for the list of the members before each channel, and each allocation of the start on this
/// thread can fail with no abort.
#[test]
fn a_group_start_that_the_budget_or_the_allocator_refuses_gives_a_typed_refusal() {
    use crate::table::tests::allocations::each_refusal;

    let budget = Budget::new(NonZeroU64::new(1 << 24).unwrap());
    let runtime = Runtime::builder()
        .local_workers(2)
        .budget(budget.clone())
        .build()
        .unwrap();
    let mut handle = runtime.handle().unwrap();
    let serve = |_worker, _listener| pending::<()>();

    // The owner of `block_on` charges its tables first, so the root takes each free unit after it.
    let started = runtime
        .block_on(async {
            let _all = budget.reserve(budget.available()).unwrap();
            handle.tcp_group(loopback(), TcpOptions::default(), serve).await
        })
        .unwrap();
    assert_matches!(started, Err(GroupError::Budget(_)));

    let refusals =
        each_refusal(
            || match runtime.block_on(handle.tcp_group(loopback(), TcpOptions::default(), serve)) {
                Ok(started) => started.map_err(Some),
                // A refusal of the owner of `block_on`, which its own test covers.
                Err(_owner) => Err(None),
            },
        );
    assert!(
        refusals
            .iter()
            .any(|(refused, _held)| matches!(refused, Some(GroupError::Allocation))),
        "the allocator can refuse the lists of the group"
    );
    for (refused, _held) in refusals {
        assert_matches!(
            refused,
            None | Some(
                GroupError::Allocation
                    | GroupError::Channel(crate::channel::Refused::Allocation)
                    | GroupError::Spawn(_, task::Reason::Allocation)
            )
        );
    }
}

/// A group that worker 1 refuses, because two tasks hold its two slots, fails with that worker and
/// reason, and only after the listener that worker 0 bound closed. A task on worker 0 blocks its
/// thread from the bind of the member until a release `HOLD` later, so the close of that listener
/// comes after the release. Each driver has one resource slot, so a bind on worker 0 after the
/// error gets that slot at once.
#[test]
fn a_group_that_worker_1_refuses_fails_only_after_the_listener_of_worker_0_closed() {
    const HOLD: Duration = Duration::from_millis(300);
    let address = std::net::TcpListener::bind(loopback()).unwrap().local_addr().unwrap();
    let runtime = Runtime::builder()
        .local_workers(2)
        .task_slots(2)
        .driver(Config::new().resources(1))
        .build()
        .unwrap();
    let mut handle = runtime.handle().unwrap();
    let _busy = [0, 1].map(|_| handle.spawn_on(WorkerId::new(1), pending::<()>).unwrap());
    let (release, held) = mpsc::channel::<()>();
    let blocker = handle
        .spawn_on(WorkerId::new(0), move || async move {
            // The member binds in its first poll, which comes after a yield of this task.
            while std::net::TcpStream::connect(address).is_err() {
                yield_once().await;
            }
            held.recv().unwrap();
        })
        .unwrap();
    let start = Instant::now();
    let releaser = thread::spawn(move || {
        thread::sleep(HOLD);
        release.send(()).unwrap();
    });
    let failed = runtime
        .block_on(handle.tcp_group(address, TcpOptions::default(), |_worker, _listener| pending::<()>()))
        .unwrap();
    assert!(
        start.elapsed() >= HOLD,
        "the group failed before worker 0 closed its listener"
    );
    releaser.join().unwrap();
    assert_matches!(failed, Err(GroupError::Spawn(worker, task::Reason::NoSlot)) if worker == WorkerId::new(1));
    assert_matches!(blocker.join_blocking(), task::Outcome::Completed(()));
    let rebind = handle
        .spawn_on(WorkerId::new(0), || async {
            TcpListener::bind(loopback(), TcpOptions::default())
                .err()
                .map(|refused| refused.reason())
        })
        .unwrap();
    assert_matches!(
        rebind.join_blocking(),
        task::Outcome::Completed(None),
        "the listener of worker 0 holds its slot after the error"
    );
}

/// A directory of a test, which its drop removes.
struct Scratch(PathBuf);

impl Scratch {
    /// A directory beside the test binary, so on the file system of the build and not on a
    /// `tmpfs`.
    fn new(name: &str) -> Self {
        let mut path = std::env::current_exe().unwrap();
        path.pop();
        Self::within(path, name)
    }

    /// A directory in the temporary directory, for a socket file. A socket path has at most 107
    /// bytes, and a socket beside the test binary had a path of 111 bytes on CI.
    fn short(name: &str) -> Self {
        Self::within(std::env::temp_dir(), name)
    }

    fn within(mut path: PathBuf, name: &str) -> Self {
        path.push(alloc::format!("rsasync-local-{name}-{}", std::process::id()));
        drop(fs::remove_dir_all(&path));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}

/// A file and a directory run each operation on the driver of their owner, with no driver
/// argument: a write, a read, the length, a sync, a rename, a directory sync, an unlink, and each
/// close.
#[test]
fn a_file_and_a_directory_run_their_operations_with_no_driver_argument() {
    let scratch = Scratch::new("file");
    let runtime = Runtime::builder().build().unwrap();
    runtime
        .block_on(async {
            let path = scratch.join("data");
            let options = OpenOptions::new().read(true).write(true).create(true);
            let file = File::open(&path, options).unwrap().await.unwrap().unwrap();
            let (written, _lease) = file
                .write_at(Lease::from(b"hello".to_vec()), 0, WriteMode::Plain)
                .unwrap()
                .await
                .unwrap();
            assert_eq!(written.unwrap(), 5);
            let (read, data) = file.read_at(buffer(), 0).unwrap().await.unwrap();
            assert_eq!((read.unwrap(), data.bytes()), (5, &b"hello"[..]));
            assert_eq!(file.len().unwrap().await.unwrap().unwrap(), 5);
            file.sync(SyncMode::Data).unwrap().await.unwrap().unwrap();
            file.close().unwrap().await.unwrap();

            let moved = scratch.join("moved");
            rename(&path, &moved).unwrap().await.unwrap().unwrap();
            let directory = Directory::open(&scratch.0).unwrap().await.unwrap().unwrap();
            directory.sync().unwrap().await.unwrap().unwrap();
            directory.close().unwrap().await.unwrap();
            unlink(&moved).unwrap().await.unwrap().unwrap();
            assert!(!moved.exists(), "the unlink left the name");
        })
        .unwrap();
}

/// A write whose future dropped runs to its end, and the close of its file returns only after it:
/// the file then holds each byte of the write.
#[test]
fn the_close_of_a_file_returns_after_its_dropped_write_ends() {
    const LEN: usize = 4 << 20;
    let scratch = Scratch::new("dropped");
    let path = scratch.join("data");
    let runtime = Runtime::builder().build().unwrap();
    runtime
        .block_on(async {
            let options = OpenOptions::new().write(true).create(true);
            let file = File::open(&path, options).unwrap().await.unwrap().unwrap();
            drop(file.write_at(Lease::from(vec![7; LEN]), 0, WriteMode::Plain).unwrap());
            file.close().unwrap().await.unwrap();
        })
        .unwrap();
    assert!(
        fs::read(&path).unwrap() == vec![7; LEN],
        "the close came before the end of the write"
    );
}

/// An IPC listener and its streams run each operation with no driver argument, and the peer has
/// the user of the socket file. The close of a stream cancels a receive that waits in the kernel
/// and returns after its terminal event: the receive is forgotten, so only the close can end it.
#[test]
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
fn an_ipc_stream_carries_bytes_and_its_close_cancels_a_waiting_receive() {
    let scratch = Scratch::short("ipc");
    let runtime = Runtime::builder().build().unwrap();
    runtime
        .block_on(async {
            let path = scratch.join("socket");
            let mut listener = IpcListener::bind(&path, Stale::Refuse).unwrap().await.unwrap().unwrap();
            let connect = IpcStream::connect(&path).unwrap();
            let mut server = listener.accept().unwrap().await.unwrap().unwrap();
            let mut client = connect.await.unwrap().unwrap();
            let (sent, _lease) = client.send(Lease::from(b"ping".to_vec())).unwrap().await.unwrap();
            assert_eq!(sent.unwrap(), 4);
            let (received, ping) = server.recv(buffer()).unwrap().await.unwrap();
            assert_eq!((received.unwrap(), ping.bytes()), (4, &b"ping"[..]));
            assert_eq!(server.peer().unwrap().user(), fs::metadata(&path).unwrap().uid());

            core::mem::forget(server.recv(buffer()).unwrap());
            server.close().unwrap().await.unwrap();
            client.close().unwrap().await.unwrap();
            listener.close().unwrap().await.unwrap();
        })
        .unwrap();
}

/// Pipe ends that a task adopts run their operations with no driver argument, and the close of the
/// write end is the end of the stream for the read end.
#[test]
fn a_pipe_carries_bytes_and_the_close_of_its_writer_ends_its_reader() {
    let runtime = Runtime::builder().build().unwrap();
    runtime
        .block_on(async {
            let (read, write) = std::io::pipe().unwrap();
            let mut reader = PipeReader::adopt(OwnedFd::from(read)).unwrap();
            let mut writer = PipeWriter::adopt(OwnedFd::from(write)).unwrap();
            let (sent, _lease) = writer.send(Lease::from(b"pipe".to_vec())).unwrap().await.unwrap();
            assert_eq!(sent.unwrap(), 4);
            writer.close().unwrap().await.unwrap();
            let (received, bytes) = reader.recv(buffer()).unwrap().await.unwrap();
            assert_eq!((received.unwrap(), bytes.bytes()), (4, &b"pipe"[..]));
            assert_eq!(reader.recv(buffer()).unwrap().await.unwrap().0.unwrap(), 0);
            reader.close().unwrap().await.unwrap();
        })
        .unwrap();
}

/// On a worker, a child spawns on the blocking service of the runtime with no driver argument, its
/// piped output arrives, and its status comes. A child with the kill policy dies at its close, and
/// the driver reaps it, so its process entry goes.
#[test]
fn a_child_on_a_worker_gives_its_output_and_a_child_with_the_kill_policy_dies_at_its_close() {
    let runtime = Runtime::builder().local_workers(1).build().unwrap();
    let mut handle = runtime.handle().unwrap();
    let task = handle
        .spawn_on(WorkerId::new(0), || async {
            let mut command = Command::new("sh");
            command.args(["-c", "printf out"]).stdout(Stdio::piped());
            let mut child = Child::spawn(command, ChildOptions::new())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let mut stdout = child.stdout.take().unwrap();
            let mut output = Vec::new();
            loop {
                let (received, bytes) = stdout.recv(buffer()).unwrap().await.unwrap();
                if received.unwrap() == 0 {
                    break;
                }
                output.extend_from_slice(bytes.bytes());
            }
            assert_eq!(output, b"out");
            assert!(child.wait().await.unwrap().unwrap().success());

            let mut command = Command::new("sleep");
            command.arg("600");
            let child = Child::spawn(command, ChildOptions::new().end(End::Kill))
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let entry = PathBuf::from(alloc::format!("/proc/{}", child.id()));
            child.close().unwrap();
            let start = Instant::now();
            while entry.exists() {
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "the child of the kill policy runs on"
                );
                yield_once().await;
            }
        })
        .unwrap();
    assert_matches!(task.join_blocking(), task::Outcome::Completed(()));
}

// `raise` of the C library.
#[expect(unsafe_code, reason = "the C library gives this function")]
unsafe extern "C" {
    fn raise(signal: core::ffi::c_int) -> core::ffi::c_int;
}

/// A signal stream of a task registers on the driver of its owner, and a raised signal reaches
/// it. `SIGWINCH` ends no process by default.
#[test]
#[expect(unsafe_code, reason = "raise names no memory")]
fn a_signal_stream_of_a_task_reports_a_raised_signal() {
    let runtime = Runtime::builder().build().unwrap();
    runtime
        .block_on(async {
            let mut stream = signal(Signal::WINDOW_CHANGE, Escalation::Never).unwrap();
            // SAFETY: `raise` names no memory.
            assert_eq!(unsafe { raise(28) }, 0);
            stream.recv().await.unwrap();
        })
        .unwrap();
}

/// A runtime with no blocking service refuses a spawn with `Reason::Blocking` and gives the command
/// back. The service of a runtime runs a job of the caller.
#[test]
fn a_runtime_with_no_blocking_service_refuses_a_spawn_and_its_service_runs_a_job() {
    let runtime = Runtime::builder().blocking(None).build().unwrap();
    assert!(runtime.handle().unwrap().blocking().is_none());
    runtime
        .block_on(async {
            let refused = Child::spawn(Command::new("true"), ChildOptions::new()).unwrap_err();
            assert_eq!(refused.reason(), Reason::Blocking);
            assert_eq!(refused.into_inner().get_program(), "true");
        })
        .unwrap();

    let runtime = Runtime::builder().build().unwrap();
    let handle = runtime.handle().unwrap();
    let mut waiter = handle.blocking().unwrap().submit(|| 6 * 7).unwrap();
    assert_eq!(waiter.wait_blocking().unwrap(), 42);
}
