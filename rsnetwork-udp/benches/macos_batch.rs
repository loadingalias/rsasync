//! Measures the batches of macOS: `send_batch` and `recv_batch` with one system call for each
//! datagram, which is the default, or with one call for the batch, which the `macos-private-batch`
//! feature selects. Run it on a quiet host, once for each build:
//!
//! ```text
//! cargo bench --locked -p rsnetwork-udp --bench macos_batch
//! cargo bench --locked -p rsnetwork-udp --bench macos_batch --features macos-private-batch
//! ```
//!
//! Each run sends 50,000 rounds of 32 datagrams of 1200 bytes between two IPv4 loopback sockets,
//! receives each datagram, and checks its length. It prints the time of the send calls and of the
//! receive calls for each datagram. On another target it does nothing.

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[expect(clippy::print_stdout, reason = "the measurement reports its result")]
fn main() -> std::io::Result<()> {
    use core::time::Duration;
    use std::io;
    use std::net::UdpSocket;
    use std::os::fd::AsFd as _;
    use std::time::Instant;

    use rsnetwork_udp::{BATCH, Received, Transmit, recv_batch, send_batch};

    const ROUNDS: usize = 50_000;
    const SIZE: usize = 1200;

    let (sender, receiver) = (UdpSocket::bind("127.0.0.1:0")?, UdpSocket::bind("127.0.0.1:0")?);
    sender.set_nonblocking(true)?;
    receiver.set_nonblocking(true)?;
    let contents = [7; SIZE];
    let transmits = [Transmit {
        destination: Some(receiver.local_addr()?),
        contents: &contents,
        segment_size: None,
        ecn: None,
        source: None,
    }; BATCH];
    let mut storage = [[0; 2048]; BATCH];
    let mut buffers: Vec<&mut [u8]> = storage.iter_mut().map(|buffer| &mut buffer[..]).collect();
    let mut received = [Received::default(); BATCH];
    let (mut sending, mut receiving) = (Duration::ZERO, Duration::ZERO);
    for _ in 0..ROUNDS {
        let mut sent = 0;
        while sent < BATCH {
            let start = Instant::now();
            let result = send_batch(sender.as_fd(), &transmits[sent..]);
            sending += start.elapsed();
            match result {
                Ok(count) => sent += count,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        let mut done = 0;
        while done < BATCH {
            let start = Instant::now();
            let result = recv_batch(receiver.as_fd(), &mut buffers[done..], &mut received[done..]);
            receiving += start.elapsed();
            match result {
                Ok(count) => {
                    if received[done..done + count]
                        .iter()
                        .any(|datagram| datagram.len != SIZE || datagram.truncated)
                    {
                        return Err(io::Error::other("a received datagram does not have the sent length"));
                    }
                    done += count;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
    }
    let datagrams = u32::try_from(ROUNDS * BATCH).map_err(io::Error::other)?;
    let calls = if cfg!(feature = "macos-private-batch") {
        "sendmsg_x and recvmsg_x"
    } else {
        "one call for each datagram"
    };
    println!(
        "{calls}: send {} ns, receive {} ns for each datagram",
        (sending / datagrams).as_nanos(),
        (receiving / datagrams).as_nanos()
    );
    Ok(())
}

#[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
fn main() {}
