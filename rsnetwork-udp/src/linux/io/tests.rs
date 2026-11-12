use core::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::io;
use std::os::fd::AsFd as _;
use std::vec::Vec;

use tokio::io::Interest;
use tokio::net::UdpSocket;

use super::super::sys::{IPPROTO_IPV6, IPV6_MTU};
use super::{Received, Refused, configure, recv, recv_batch, send, send_batch, set, set_dont_fragment, set_gro};
use crate::{Ecn, Transmit};

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

/// Sends on a Tokio socket: Tokio waits until the socket is writable, then calls `send`.
async fn send_on(socket: &UdpSocket, transmit: &Transmit<'_>) -> io::Result<usize> {
    socket
        .async_io(Interest::WRITABLE, || send(socket.as_fd(), transmit))
        .await
}

/// Receives on a Tokio socket: Tokio waits until the socket is readable, then calls `recv`.
async fn recv_on(socket: &UdpSocket, buffer: &mut [u8]) -> Received {
    socket
        .async_io(Interest::READABLE, || recv(socket.as_fd(), buffer))
        .await
        .unwrap()
}

/// `len` bytes that differ from one segment to the next.
fn bytes(len: usize) -> Vec<u8> {
    (0..len).map(|index| u8::try_from(index % 251).unwrap()).collect()
}

fn datagram<'a>(destination: SocketAddr, contents: &'a [u8]) -> Transmit<'a> {
    Transmit {
        destination,
        contents,
        segment_size: None,
        ecn: None,
        source: None,
    }
}

/// Three full segments of 1000 bytes and a short last one: each arrives as its own datagram.
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

/// With GRO, the same batch arrives as one coalesced receive with its segment size, and no other
/// datagram follows it.
#[test]
fn a_coalesced_receive_reports_its_segment_size() {
    tokio(async {
        let (sender, receiver) = (bind("127.0.0.1:0").await, bind("127.0.0.1:0").await);
        set_gro(receiver.as_fd(), true).unwrap();
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
        let next = recv(receiver.as_fd(), &mut buffer).unwrap_err();
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
        configure(receiver.as_fd()).unwrap();
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
    let v4 = |octet| IpAddr::V4(Ipv4Addr::new(127, 0, 0, octet));
    let mapped = |octet| IpAddr::V6(Ipv4Addr::new(127, 0, 0, octet).to_ipv6_mapped());
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
        let received = round_trip(&sender, &receiver, receiver.local_addr().unwrap(), loopback, Ecn::Ce).await;
        assert_eq!(received.source, at(loopback, &sender));
        assert_eq!(
            (received.metadata.destination, received.metadata.ecn),
            (Some(loopback), Some(Ecn::Ce))
        );

        // Dual-stack: an IPv4 datagram to an IPv6 socket, and from it.
        let (ipv4, dual) = (bind("0.0.0.0:0").await, bind("[::]:0").await);
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

/// With the don't-fragment option, a datagram above the path MTU is refused with a typed error,
/// and a smaller one goes. The test sets the path MTU of the IPv6 socket to 1280 bytes, which needs
/// no privilege; an IPv4 path MTU on loopback cannot be lowered without one.
#[test]
fn the_dont_fragment_option_refuses_a_datagram_above_the_path_mtu() {
    tokio(async {
        let (sender, receiver) = (bind("[::1]:0").await, bind("[::1]:0").await);
        set_dont_fragment(sender.as_fd()).unwrap();
        set(sender.as_fd(), IPPROTO_IPV6, IPV6_MTU, 1280).unwrap();
        let destination = receiver.local_addr().unwrap();

        let refused = send_on(&sender, &datagram(destination, &bytes(1400)))
            .await
            .unwrap_err();
        assert_eq!(Refused::of(&refused), Some(Refused::TooLarge));
        assert_eq!(send_on(&sender, &datagram(destination, &bytes(1000))).await.unwrap(), 1);
    });
}

/// One `sendmmsg` and one `recvmmsg` keep each datagram, its size, and its ECN codepoint.
#[test]
fn a_batch_keeps_each_datagram_and_its_metadata() {
    tokio(async {
        let (sender, receiver) = (bind("127.0.0.1:0").await, bind("127.0.0.1:0").await);
        configure(receiver.as_fd()).unwrap();
        let destination = receiver.local_addr().unwrap();
        let contents = [bytes(10), bytes(200), bytes(3000)];
        let ecn = [Some(Ecn::Ect0), None, Some(Ecn::Ce)];
        let transmits: Vec<_> = contents
            .iter()
            .zip(ecn)
            .map(|(contents, ecn)| Transmit {
                ecn,
                ..datagram(destination, contents)
            })
            .collect();
        let sent = sender
            .async_io(Interest::WRITABLE, || send_batch(sender.as_fd(), &transmits))
            .await
            .unwrap();
        assert_eq!(sent, 3);

        let mut storage = [[0; 4096]; 4];
        let mut received = [Received::default(); 4];
        let count = {
            let mut buffers: Vec<&mut [u8]> = storage.iter_mut().map(|buffer| &mut buffer[..]).collect();
            receiver
                .async_io(Interest::READABLE, || {
                    recv_batch(receiver.as_fd(), &mut buffers, &mut received)
                })
                .await
                .unwrap()
        };
        assert_eq!(count, 3);
        for (index, contents) in contents.iter().enumerate() {
            assert_eq!(&storage[index][..received[index].len], &contents[..]);
            assert_eq!(received[index].metadata.ecn, ecn[index]);
        }
    });
}
