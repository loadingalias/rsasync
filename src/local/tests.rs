use alloc::vec;
use alloc::vec::Vec;
use core::assert_matches;
use core::cell::RefCell;
use core::future::pending;
use core::net::SocketAddr;
use core::num::NonZeroU64;
use core::time::Duration;
use std::io::Read as _;
use std::time::Instant;
use std::{thread, thread_local};

use super::{TcpListener, TcpStream, UdpSocket};
use crate::buffer::{Backing, Buffer as _, Lease};
use crate::capacity::Budget;
use crate::channel::oneshot;
use crate::driver::{Config, Datagram, Reason, SendCount, TcpOptions, UdpOptions};
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

/// A close of a stream whose peer neither reads nor sends cancels a dropped receive and a dropped
/// send that waits in the kernel, because the send before it filled the socket buffers. Its future
/// ends after their terminal events, and its report gives the count of the dropped send: 0.
#[test]
fn a_close_cancels_a_dropped_send_to_a_peer_that_never_reads_and_ends() {
    const FILL: usize = 64 << 20;
    let runtime = Runtime::builder().build().unwrap();
    let sent = runtime
        .block_on(async {
            let mut listener = TcpListener::bind(loopback(), TcpOptions::default())
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            let peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
            let (mut stream, _peer) = listener.accept().unwrap().await.unwrap().unwrap();
            let (filled, _lease) = stream.send(Lease::from(vec![7; FILL])).unwrap().await.unwrap();
            assert!(filled.unwrap() < FILL, "the socket buffers held the whole send");
            let (mut read, mut write) = stream.split();
            let receive = read.recv(buffer()).unwrap();
            let send = write.send(Lease::from(vec![8; 4096])).unwrap();
            // A turn of the owner submits both, and the kernel holds them.
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

/// Each member of a TCP group accepts, and sends the index of its worker.
async fn serve_tcp(worker: WorkerId, mut listener: TcpListener) {
    let index = u8::try_from(worker.index()).unwrap();
    while let Ok(accept) = listener.accept() {
        let Ok(Ok((mut stream, _peer))) = accept.await else {
            return;
        };
        let Ok(send) = stream.send(Lease::from(vec![index])) else {
            return;
        };
        drop(send.await);
    }
}

/// A group of four workers binds one listener on each worker at one port, and the kernel gives
/// connections to each of them.
#[test]
fn a_tcp_group_of_four_workers_accepts_connections_on_each_worker() {
    let runtime = Runtime::builder().local_workers(4).build().unwrap();
    let mut handle = runtime.handle().unwrap();
    let group = runtime
        .block_on(handle.tcp_group(loopback(), TcpOptions::default(), serve_tcp))
        .unwrap()
        .unwrap();
    let mut seen = [false; 4];
    for _ in 0..400 {
        if seen == [true; 4] {
            break;
        }
        let mut stream = std::net::TcpStream::connect(group.local_addr()).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut index = [0];
        stream.read_exact(&mut index).unwrap();
        seen[usize::from(index[0])] = true;
    }
    assert_eq!(seen, [true; 4], "each worker accepts");
}

/// Each member of a UDP group answers each datagram with the index of its worker.
async fn serve_udp(worker: WorkerId, mut socket: UdpSocket) {
    let index = u8::try_from(worker.index()).unwrap();
    while let Ok(receive) = socket.recv_from(buffer()) {
        let Ok((Ok(received), _buffer)) = receive.await else {
            return;
        };
        let Ok(send) = socket.send_to(Datagram::new(Lease::from(vec![index]), received.source)) else {
            return;
        };
        drop(send.await);
    }
}

/// A group of four workers binds one UDP socket on each worker at one port, and the kernel gives
/// datagrams to each of them.
#[test]
fn a_udp_group_of_four_workers_receives_datagrams_on_each_worker() {
    let runtime = Runtime::builder().local_workers(4).build().unwrap();
    let mut handle = runtime.handle().unwrap();
    let group = runtime
        .block_on(handle.udp_group(loopback(), UdpOptions::default(), serve_udp))
        .unwrap()
        .unwrap();
    let mut seen = [false; 4];
    for _ in 0..400 {
        if seen == [true; 4] {
            break;
        }
        let client = std::net::UdpSocket::bind(loopback()).unwrap();
        client.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        client.send_to(b"which", group.local_addr()).unwrap();
        let mut index = [0];
        assert_eq!(client.recv(&mut index).unwrap(), 1);
        seen[usize::from(index[0])] = true;
    }
    assert_eq!(seen, [true; 4], "each worker receives");
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

/// A group that worker 1 refuses, because a task holds its one slot, fails with that worker and
/// reason. The listener that worker 0 bound closed before the error, so a plain listener binds its
/// address at once.
#[test]
fn a_group_that_worker_1_refuses_closes_the_listener_of_worker_0() {
    let runtime = Runtime::builder().local_workers(2).task_slots(1).build().unwrap();
    let mut handle = runtime.handle().unwrap();
    let _busy = handle.spawn_on(WorkerId::new(1), pending::<()>).unwrap();
    let address = std::net::TcpListener::bind(loopback()).unwrap().local_addr().unwrap();
    let failed = runtime
        .block_on(handle.tcp_group(address, TcpOptions::default(), |_worker, _listener| pending::<()>()))
        .unwrap();
    assert_matches!(failed, Err(GroupError::Spawn(worker, task::Reason::NoSlot)) if worker == WorkerId::new(1));
    assert!(
        std::net::TcpListener::bind(address).is_ok(),
        "the listener of worker 0 is open after the error"
    );
}
