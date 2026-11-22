use core::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::io;
#[cfg(unix)]
use std::os::fd::{AsFd as _, BorrowedFd};
#[cfg(windows)]
use std::os::windows::io::{AsSocket as _, BorrowedSocket};
use std::vec::Vec;

use tokio::io::Interest;
use tokio::net::UdpSocket;

#[cfg(target_os = "linux")]
use crate::set_gro;
#[cfg(windows)]
use crate::set_uro;
#[cfg(windows)]
use crate::sys::IPPROTO_IP;
#[cfg(any(target_os = "linux", windows))]
use crate::sys::IPPROTO_IPV6;
#[cfg(target_os = "linux")]
use crate::sys::IPV6_MTU;
#[cfg(unix)]
use crate::unix::io::set;
#[cfg(windows)]
use crate::windows::io::set;
use crate::{Ecn, Received, Refused, Transmit, configure, recv, recv_batch, send, send_batch, set_dont_fragment};

/// The Winsock items that only these tests use, so the crate declares none of them. They agree with
/// `winsock2.h` and `ws2ipdef.h` of the SDK that `windows/sys.rs` names.
#[cfg(windows)]
#[expect(unsafe_code, reason = "ws2_32.dll gives the function")]
mod winsock {
    use core::ffi::c_int;

    use crate::sys::Socket;

    /// `INVALID_SOCKET`: the result of a `socket` call that failed.
    pub(super) const INVALID_SOCKET: Socket = !0;
    /// `IP_USER_MTU`: the upper bound of the IPv4 path MTU that the socket sets, with no privilege.
    pub(super) const IP_USER_MTU: c_int = 76;

    #[link(name = "ws2_32")]
    unsafe extern "system" {
        /// `SOCKET socket(int af, int type, int protocol)`: a dual-stack socket, whose
        /// `IPV6_V6ONLY` must be off before the bind.
        pub(super) fn socket(af: c_int, kind: c_int, protocol: c_int) -> Socket;
    }
}
#[cfg(windows)]
use winsock::{INVALID_SOCKET, IP_USER_MTU, socket};

/// The borrowed socket that the calls of this crate take on this target.
#[cfg(unix)]
type Borrowed<'a> = BorrowedFd<'a>;
/// The borrowed socket that the calls of this crate take on this target.
#[cfg(windows)]
type Borrowed<'a> = BorrowedSocket<'a>;

/// The socket of a Tokio socket, as the calls of this crate take it.
trait Handle {
    fn handle(&self) -> Borrowed<'_>;
}

impl Handle for UdpSocket {
    #[cfg(unix)]
    fn handle(&self) -> Borrowed<'_> {
        self.as_fd()
    }

    #[cfg(windows)]
    fn handle(&self) -> Borrowed<'_> {
        self.as_socket()
    }
}

/// Turns coalesced receive on: GRO on Linux, URO on Windows.
#[cfg(any(target_os = "linux", windows))]
fn coalesce(socket: &UdpSocket) {
    #[cfg(target_os = "linux")]
    set_gro(socket.handle(), true).unwrap();
    #[cfg(windows)]
    set_uro(socket.handle(), true).unwrap();
}

/// Runs `test` on a Tokio runtime of one thread, with its I/O driver: each socket is a Tokio socket.
fn tokio(test: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap()
        .block_on(test);
}

async fn bind(address: &str) -> UdpSocket {
    UdpSocket::bind(address).await.unwrap()
}

/// A dual-stack IPv6 socket on the wildcard address. macOS gives a dual-stack socket an ephemeral
/// port that an IPv4 socket can hold already, and an IPv4 datagram to the port then goes to that
/// IPv4 socket. A bind to a stated port checks the IPv4 sockets too. So the socket binds the port of
/// an IPv4 socket that just closed, and it takes another port when a socket took that one first.
#[cfg(not(windows))]
async fn dual_stack() -> UdpSocket {
    const ATTEMPTS: usize = 16;
    for _ in 0..ATTEMPTS {
        let port = std::net::UdpSocket::bind("0.0.0.0:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        match UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port)).await {
            Ok(socket) => return socket,
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => {}
            Err(error) => panic!("the dual-stack bind failed: {error}"),
        }
    }
    panic!("another socket took each of {ATTEMPTS} free IPv4 ports before the dual-stack bind");
}

/// A dual-stack IPv6 socket on the wildcard address. A Windows IPv6 socket takes no IPv4 traffic
/// unless `IPV6_V6ONLY` is off before the bind, and Tokio binds at once, so the test makes the
/// socket itself.
#[cfg(windows)]
#[expect(unsafe_code, reason = "the test makes and binds a socket that it then owns")]
async fn dual_stack() -> UdpSocket {
    use std::os::windows::io::FromRawSocket as _;

    use crate::sys::{IPPROTO_UDP, IPV6_V6ONLY, SOCK_DGRAM, bind};
    use crate::{AF_INET6, SockAddrStorage};

    // The standard library starts Winsock with its first socket.
    drop(std::net::UdpSocket::bind("[::1]:0").unwrap());
    // SAFETY: the call takes no pointer.
    let raw = unsafe { socket(core::ffi::c_int::from(AF_INET6), SOCK_DGRAM, IPPROTO_UDP) };
    assert_ne!(raw, INVALID_SOCKET);
    // SAFETY: nothing else owns `raw`, and the standard socket closes it.
    let owned = unsafe { std::net::UdpSocket::from_raw_socket(raw) };
    set(owned.as_socket(), IPPROTO_IPV6, IPV6_V6ONLY, 0).unwrap();
    let (any, len) = SockAddrStorage::new(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0));
    // SAFETY: the address and its length live during the call.
    let bound = unsafe { bind(raw, core::ptr::from_ref(&any).cast(), i32::try_from(len).unwrap()) };
    assert_eq!(bound, 0);
    owned.set_nonblocking(true).unwrap();
    UdpSocket::from_std(owned).unwrap()
}

/// A marked ECN codepoint that a sender sets: CE, or ECT(1) on Windows, which refuses to send CE
/// (`WSAEINVAL`).
const MARKED: Ecn = if cfg!(windows) { Ecn::Ect1 } else { Ecn::Ce };

/// Sends on a Tokio socket: Tokio waits until the socket is writable, then calls `send`.
async fn send_on(socket: &UdpSocket, transmit: &Transmit<'_>) -> io::Result<usize> {
    socket
        .async_io(Interest::WRITABLE, || send(socket.handle(), transmit))
        .await
}

/// Receives on a Tokio socket: Tokio waits until the socket is readable, then calls `recv`.
async fn recv_on(socket: &UdpSocket, buffer: &mut [u8]) -> Received {
    socket
        .async_io(Interest::READABLE, || recv(socket.handle(), buffer))
        .await
        .unwrap()
}

/// `len` bytes that differ from one segment to the next.
fn bytes(len: usize) -> Vec<u8> {
    (0..len).map(|index| u8::try_from(index % 251).unwrap()).collect()
}

/// The IPv4 loopback address `127.0.0.<octet>`. macOS has only 127.0.0.1 on its loopback
/// interface unless an administrator adds more, so there each octet gives 127.0.0.1. Windows takes
/// the same address, so that a test depends on no other address of the host.
fn loopback(octet: u8) -> Ipv4Addr {
    Ipv4Addr::new(127, 0, 0, if cfg!(target_os = "linux") { octet } else { 1 })
}

/// Prepares `socket` to send a datagram above its MTU, and gives the size of such a datagram and
/// of one below the MTU. Linux checks the path MTU, which an IPv6 socket can lower to 1280 bytes
/// with no privilege; an IPv4 path MTU on loopback cannot be lowered without one. macOS checks the
/// MTU of the interface, 16,384 bytes on loopback, and a larger send buffer lets such a datagram
/// reach the IP layer. Windows checks no MTU on loopback: there the socket lowers its IPv4 bound to
/// 1280 bytes with `IP_USER_MTU`, and the datagram takes a path off the host.
fn above_the_mtu(socket: &UdpSocket) -> (usize, usize) {
    #[cfg(target_os = "linux")]
    {
        set(socket.handle(), IPPROTO_IPV6, IPV6_MTU, 1280).unwrap();
        (1400, 1000)
    }
    #[cfg(target_os = "macos")]
    {
        // `SOL_SOCKET` and `SO_SNDBUF` of `sys/socket.h` in the macOS 27.2 SDK.
        set(socket.handle(), 0xffff, 0x1001, 65536).unwrap();
        (20_000, 16_000)
    }
    #[cfg(windows)]
    {
        set(socket.handle(), IPPROTO_IP, IP_USER_MTU, 1280).unwrap();
        (1400, 1000)
    }
}

fn datagram<'a>(destination: SocketAddr, contents: &'a [u8]) -> Transmit<'a> {
    Transmit {
        destination: Some(destination),
        contents,
        segment_size: None,
        ecn: None,
        source: None,
    }
}

/// Three full segments of 1000 bytes and a short last one: each arrives as its own datagram.
#[cfg(any(target_os = "linux", windows))]
#[test]
fn one_segmented_send_arrives_as_one_datagram_for_each_segment() {
    tokio(async {
        let (sender, receiver) = (bind("127.0.0.1:0").await, bind("127.0.0.1:0").await);
        let contents = bytes(3500);
        let transmit = Transmit {
            segment_size: Some(1000),
            ..datagram(receiver.local_addr().unwrap(), &contents)
        };
        assert_eq!(send_on(&sender, &transmit).await.unwrap(), 4);

        let mut buffer = [0; 4096];
        for segment in contents.chunks(1000) {
            let received = recv_on(&receiver, &mut buffer).await;
            assert_eq!(&buffer[..received.len], segment);
            assert_eq!(received.metadata.segment_size, None);
        }
    });
}

/// With coalesced receive (GRO, URO), the same batch arrives as one coalesced receive with its
/// segment size, and no other datagram follows it.
#[cfg(any(target_os = "linux", windows))]
#[test]
fn a_coalesced_receive_reports_its_segment_size() {
    tokio(async {
        let (sender, receiver) = (bind("127.0.0.1:0").await, bind("127.0.0.1:0").await);
        coalesce(&receiver);
        let contents = bytes(3500);
        let transmit = Transmit {
            segment_size: Some(1000),
            ..datagram(receiver.local_addr().unwrap(), &contents)
        };
        send_on(&sender, &transmit).await.unwrap();

        let mut buffer = [0; 8192];
        let received = recv_on(&receiver, &mut buffer).await;
        assert_eq!((received.len, received.metadata.segment_size), (3500, Some(1000)));
        assert_eq!(&buffer[..received.len], contents);
        let next = recv(receiver.handle(), &mut buffer).unwrap_err();
        assert_eq!(next.kind(), io::ErrorKind::WouldBlock);
    });
}

/// A send sets the ECN codepoint and the source address. The receiver reports them, with the local
/// address that the datagram came to, on IPv4, on IPv6, and in each direction of a dual-stack
/// socket.
#[test]
fn ecn_and_the_local_addresses_round_trip() {
    async fn round_trip(
        sender: &UdpSocket,
        receiver: &UdpSocket,
        destination: SocketAddr,
        source: IpAddr,
        ecn: Ecn,
    ) -> Received {
        configure(receiver.handle()).unwrap();
        let contents = bytes(100);
        let transmit = Transmit {
            ecn: Some(ecn),
            source: Some(source),
            ..datagram(destination, &contents)
        };
        assert_eq!(send_on(sender, &transmit).await.unwrap(), 1);
        let mut buffer = [0; 256];
        let received = recv_on(receiver, &mut buffer).await;
        assert_eq!(&buffer[..received.len], contents);
        received
    }
    let v4 = |octet| IpAddr::V4(loopback(octet));
    let mapped = |octet| IpAddr::V6(loopback(octet).to_ipv6_mapped());
    let at = |ip, socket: &UdpSocket| SocketAddr::new(ip, socket.local_addr().unwrap().port());
    tokio(async {
        // IPv4: the receiver is bound to each address.
        let (sender, receiver) = (bind("0.0.0.0:0").await, bind("0.0.0.0:0").await);
        let received = round_trip(&sender, &receiver, at(v4(3), &receiver), v4(2), Ecn::Ect0).await;
        assert_eq!(received.source, at(v4(2), &sender));
        assert_eq!(
            (received.metadata.destination, received.metadata.ecn),
            (Some(v4(3)), Some(Ecn::Ect0))
        );

        // IPv6.
        let loopback = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let (sender, receiver) = (bind("[::1]:0").await, bind("[::1]:0").await);
        let received = round_trip(&sender, &receiver, receiver.local_addr().unwrap(), loopback, MARKED).await;
        assert_eq!(received.source, at(loopback, &sender));
        assert_eq!(
            (received.metadata.destination, received.metadata.ecn),
            (Some(loopback), Some(MARKED))
        );

        // Dual-stack: an IPv4 datagram to an IPv6 socket, and from it.
        let (ipv4, dual) = (bind("0.0.0.0:0").await, dual_stack().await);
        let received = round_trip(&ipv4, &dual, at(v4(4), &dual), v4(5), Ecn::Ect1).await;
        assert_eq!(received.source, at(mapped(5), &ipv4));
        assert_eq!(
            (received.metadata.destination, received.metadata.ecn),
            (Some(v4(4)), Some(Ecn::Ect1))
        );
        let received = round_trip(&dual, &ipv4, at(mapped(6), &ipv4), mapped(7), Ecn::Ect0).await;
        assert_eq!(received.source, at(v4(7), &dual));
        assert_eq!(
            (received.metadata.destination, received.metadata.ecn),
            (Some(v4(6)), Some(Ecn::Ect0))
        );
    });
}

/// A datagram larger than the buffer fills the buffer and reports the truncation.
#[test]
fn an_oversized_datagram_reports_truncation() {
    tokio(async {
        let (sender, receiver) = (bind("127.0.0.1:0").await, bind("127.0.0.1:0").await);
        let contents = bytes(2000);
        send_on(&sender, &datagram(receiver.local_addr().unwrap(), &contents))
            .await
            .unwrap();

        let mut buffer = [0; 100];
        let received = recv_on(&receiver, &mut buffer).await;
        assert_eq!((received.len, received.truncated), (100, true));
        assert_eq!(buffer[..], contents[..100]);
    });
}

/// With the don't-fragment option, a datagram above the MTU is refused with a typed error, and a
/// smaller one goes. Without the option, the same large datagram goes, in fragments. The test checks
/// IPv6 on Linux, IPv4 and IPv6 on macOS, and IPv4 on Windows. Windows checks no MTU on loopback, so
/// there the datagrams go to TEST-NET-1 (RFC 5737), which no network routes: the host refuses the
/// large datagram before it leaves, and the network drops the two datagrams that go. On Windows the
/// test needs an IPv4 default route, and with none it fails with a message that names that need.
#[test]
fn the_dont_fragment_option_refuses_a_datagram_above_the_mtu() {
    let families: &[&str] = if cfg!(target_os = "macos") {
        &["127.0.0.1:0", "[::1]:0"]
    } else if cfg!(windows) {
        &["0.0.0.0:0"]
    } else {
        &["[::1]:0"]
    };
    tokio(async {
        for &family in families {
            let (sender, receiver) = (bind(family).await, bind(family).await);
            let destination = if cfg!(windows) {
                let test_net = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 9);
                // A connect of a UDP socket sends nothing; it fails when no route reaches the address.
                std::net::UdpSocket::bind("0.0.0.0:0")
                    .and_then(|route| route.connect(test_net))
                    .unwrap_or_else(|error| {
                        panic!("this test needs an IPv4 default route on Windows, to reach {test_net}: {error}")
                    });
                test_net
            } else {
                receiver.local_addr().unwrap()
            };
            set_dont_fragment(sender.handle()).unwrap();
            let (refused, sent) = above_the_mtu(&sender);
            let large = bytes(refused);
            let error = send_on(&sender, &datagram(destination, &large)).await.unwrap_err();
            assert_eq!(Refused::of(&error), Some(Refused::TooLarge), "{family}");
            assert_eq!(send_on(&sender, &datagram(destination, &bytes(sent))).await.unwrap(), 1);

            let plain = bind(family).await;
            above_the_mtu(&plain);
            assert_eq!(
                send_on(&plain, &datagram(destination, &large)).await.unwrap(),
                1,
                "{family}"
            );
        }
    });
}

/// A send with no destination goes to the peer of a connected socket, with its ECN codepoint, alone
/// and in a batch: on IPv4, on IPv6, and on a dual-stack socket that is connected to the
/// IPv4-mapped address of an IPv4 peer.
#[test]
fn a_connected_socket_sends_to_its_peer_with_its_ecn_codepoint() {
    let cases = [
        ("127.0.0.1:0", "127.0.0.1:0", false),
        ("[::1]:0", "[::1]:0", false),
        ("", "127.0.0.1:0", true),
    ];
    tokio(async {
        for (sender, receiver, mapped) in cases {
            let sender = if mapped { dual_stack().await } else { bind(sender).await };
            let receiver = bind(receiver).await;
            configure(receiver.handle()).unwrap();
            let peer = match receiver.local_addr().unwrap() {
                SocketAddr::V4(address) if mapped => {
                    SocketAddr::new(IpAddr::V6(address.ip().to_ipv6_mapped()), address.port())
                }
                address => address,
            };
            sender.connect(peer).await.unwrap();
            let contents = bytes(100);
            let transmit = |ecn| Transmit {
                destination: None,
                ecn: Some(ecn),
                ..datagram(peer, &contents)
            };
            assert_eq!(send_on(&sender, &transmit(Ecn::Ect0)).await.unwrap(), 1);
            let batch = [transmit(Ecn::Ect1), transmit(MARKED)];
            let sent = sender
                .async_io(Interest::WRITABLE, || send_batch(sender.handle(), &batch))
                .await
                .unwrap();
            assert_eq!(sent, 2);

            let port = sender.local_addr().unwrap().port();
            let mut buffer = [0; 256];
            for ecn in [Ecn::Ect0, Ecn::Ect1, MARKED] {
                let received = recv_on(&receiver, &mut buffer).await;
                assert_eq!(&buffer[..received.len], &contents[..], "{peer}");
                assert_eq!(
                    (received.source.port(), received.metadata.ecn),
                    (port, Some(ecn)),
                    "{peer}"
                );
            }
        }
    });
}

/// Waits until the receive buffer of `socket` holds `count` datagrams, for at most 2 s. macOS
/// delivers a loopback datagram after its send returns, so the socket can be readable before the
/// last datagram of a batch arrived, and one receive then gives only those that are ready.
#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "the kernel writes one `int` option into owned storage")]
fn wait_for_datagrams(socket: Borrowed<'_>, count: core::ffi::c_int) {
    use std::os::fd::AsRawFd as _;

    /// `SO_NUMRCVPKT` of `sys/socket.h` of the macOS SDK: the datagrams in the receive buffer.
    const SO_NUMRCVPKT: core::ffi::c_int = 0x1112;
    let start = std::time::Instant::now();
    loop {
        let mut queued: core::ffi::c_int = 0;
        let mut len = u32::try_from(size_of::<core::ffi::c_int>()).unwrap();
        // SAFETY: the option and its length live during the call, and the kernel writes at most
        // `len` bytes.
        let result = unsafe {
            crate::sys::getsockopt(
                socket.as_raw_fd(),
                crate::sys::SOL_SOCKET,
                SO_NUMRCVPKT,
                (&raw mut queued).cast(),
                &raw mut len,
            )
        };
        assert_eq!(result, 0, "{}", io::Error::last_os_error());
        if queued >= count {
            return;
        }
        assert!(
            start.elapsed() < core::time::Duration::from_secs(2),
            "{queued} of {count} datagrams arrived"
        );
        std::thread::yield_now();
    }
}

/// One batch send and one batch receive keep each datagram, its size, its ECN codepoint, its peer, and
/// its local address, and report the truncation of a datagram that does not fit its buffer.
#[test]
fn a_batch_keeps_each_datagram_and_its_metadata() {
    tokio(async {
        let (sender, receiver) = (bind("127.0.0.1:0").await, bind("127.0.0.1:0").await);
        configure(receiver.handle()).unwrap();
        let destination = receiver.local_addr().unwrap();
        let contents = [bytes(10), bytes(200), bytes(3000)];
        let ecn = [Some(Ecn::Ect0), None, Some(MARKED)];
        let transmits: Vec<_> = contents
            .iter()
            .zip(ecn)
            .map(|(contents, ecn)| Transmit {
                ecn,
                ..datagram(destination, contents)
            })
            .collect();
        let sent = sender
            .async_io(Interest::WRITABLE, || send_batch(sender.handle(), &transmits))
            .await
            .unwrap();
        assert_eq!(sent, 3);
        #[cfg(target_os = "macos")]
        wait_for_datagrams(receiver.handle(), 3);

        // The third buffer holds 100 bytes of the third datagram.
        let mut storage = [[0; 4096]; 4];
        let mut received = [Received::default(); 4];
        let count = {
            let mut buffers: Vec<&mut [u8]> = storage
                .iter_mut()
                .enumerate()
                .map(|(index, buffer)| {
                    if index == 2 {
                        &mut buffer[..100]
                    } else {
                        &mut buffer[..]
                    }
                })
                .collect();
            receiver
                .async_io(Interest::READABLE, || {
                    recv_batch(receiver.handle(), &mut buffers, &mut received)
                })
                .await
                .unwrap()
        };
        assert_eq!(count, 3);
        for (index, contents) in contents.iter().enumerate() {
            let truncated = index == 2;
            let len = if truncated { 100 } else { contents.len() };
            assert_eq!(&storage[index][..received[index].len], &contents[..len]);
            assert_eq!(received[index].truncated, truncated);
            assert_eq!(received[index].source, sender.local_addr().unwrap());
            assert_eq!(received[index].metadata.ecn, ecn[index]);
            assert_eq!(
                received[index].metadata.destination,
                Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
            );
        }
    });
}

/// A batch with a datagram above the path MTU, with the don't-fragment option on: the kernel sends
/// the datagrams before it, the report names only them, and the next send of the rest refuses it.
/// Windows checks no MTU on loopback, so there the refused datagram is larger than the largest UDP
/// payload of IPv6, 65,527 bytes.
#[test]
fn a_batch_reports_the_datagrams_before_a_refused_one() {
    tokio(async {
        let (sender, receiver) = (bind("[::1]:0").await, bind("[::1]:0").await);
        set_dont_fragment(sender.handle()).unwrap();
        let refused = if cfg!(windows) {
            65_528
        } else {
            above_the_mtu(&sender).0
        };
        let destination = receiver.local_addr().unwrap();
        let contents = [bytes(1000), bytes(1001), bytes(refused), bytes(1002)];
        let transmits: Vec<_> = contents
            .iter()
            .map(|contents| datagram(destination, contents))
            .collect();
        let sent = sender
            .async_io(Interest::WRITABLE, || send_batch(sender.handle(), &transmits))
            .await
            .unwrap();
        assert_eq!(sent, 2);
        let refused = sender
            .async_io(Interest::WRITABLE, || send_batch(sender.handle(), &transmits[sent..]))
            .await
            .unwrap_err();
        assert_eq!(Refused::of(&refused), Some(Refused::TooLarge));

        let mut buffer = [0; 2048];
        for contents in &contents[..sent] {
            let received = recv_on(&receiver, &mut buffer).await;
            assert_eq!(&buffer[..received.len], &contents[..]);
        }
        let next = recv(receiver.handle(), &mut buffer).unwrap_err();
        assert_eq!(next.kind(), io::ErrorKind::WouldBlock);
    });
}

/// macOS has no segmentation offload. A segment size is refused with a typed error, alone or in a
/// batch, and nothing of it goes out; a batch sends the datagrams before it. So the send of a
/// `Probe`, the first step of its validation, gives the typed error on each macOS path.
#[cfg(target_os = "macos")]
#[test]
fn a_segment_size_is_refused_and_sends_nothing() {
    use core::num::{NonZeroU8, NonZeroU16};

    use crate::Probe;

    tokio(async {
        let (sender, receiver) = (bind("127.0.0.1:0").await, bind("127.0.0.1:0").await);
        let destination = receiver.local_addr().unwrap();
        let (single, contents) = (bytes(10), bytes(3000));
        let segmented = Transmit {
            segment_size: Some(1000),
            ..datagram(destination, &contents)
        };
        let probe = Probe::new(NonZeroU8::new(3).unwrap(), NonZeroU16::new(1000).unwrap());
        let refused = send(sender.handle(), &probe.transmit(Some(destination))).unwrap_err();
        assert_eq!(Refused::of(&refused), Some(Refused::NoSegmentation));
        let sent = sender
            .async_io(Interest::WRITABLE, || {
                send_batch(sender.handle(), &[datagram(destination, &single), segmented])
            })
            .await
            .unwrap();
        assert_eq!(sent, 1);
        let refused = send_batch(sender.handle(), &[segmented]).unwrap_err();
        assert_eq!(Refused::of(&refused), Some(Refused::NoSegmentation));

        let mut buffer = [0; 4096];
        let received = recv_on(&receiver, &mut buffer).await;
        assert_eq!(&buffer[..received.len], &single[..]);
        let next = recv(receiver.handle(), &mut buffer).unwrap_err();
        assert_eq!(next.kind(), io::ErrorKind::WouldBlock);
    });
}

/// A probe over loopback, with segmentation and coalesced receive on, finds the path valid: each
/// segment arrives once with its boundaries, as its own datagram or in a coalesced receive with
/// its segment size.
#[cfg(any(target_os = "linux", windows))]
#[test]
fn a_probe_over_loopback_finds_the_offload_valid() {
    use core::num::{NonZeroU8, NonZeroU16};

    use crate::{Probe, Verdict};

    tokio(async {
        for family in ["127.0.0.1:0", "[::1]:0"] {
            let (sender, receiver) = (bind(family).await, bind(family).await);
            coalesce(&receiver);
            let mut probe = Probe::new(NonZeroU8::new(8).unwrap(), NonZeroU16::new(1200).unwrap());
            let sent = send_on(&sender, &probe.transmit(Some(receiver.local_addr().unwrap())))
                .await
                .unwrap();
            assert_eq!(sent, 8);
            let mut buffer = [0; 65_536];
            let verdict = loop {
                let received = recv_on(&receiver, &mut buffer).await;
                if let Some(verdict) = probe.observe(&received, &buffer[..received.len]) {
                    break verdict;
                }
            };
            assert_eq!(verdict, Verdict::Valid, "{family}");
        }
    });
}
