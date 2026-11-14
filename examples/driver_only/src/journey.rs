//! The steps of the example. Each step writes one line for each outcome.

use core::error::Error;
use core::future::Future;
use core::net::SocketAddr;
use core::num::NonZeroU64;
use core::pin::pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::{Context, Poll, Waker};
use std::io::{self, Write};
use std::sync::Arc;
use std::task::Wake;

use rsasync::blocking::{self, Service};
use rsasync::buffer::{Backing, Buffer as _, BufferMut as _, Lease};
use rsasync::capacity::Budget;
use rsasync::driver::{
    Call, Config, Datagram, Driver, Incomplete, OpenOptions, SyncMode, TcpOptions, UdpOptions, WriteMode,
};

/// The error number of an operation that a cancel ended, on Linux.
const ECANCELED: i32 = 125;

/// Runs each step on one driver, then ends the driver and the blocking service.
pub(crate) fn run(out: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let mut driver = Driver::new(Config::new())?;
    let report = driver.report();
    writeln!(
        out,
        "report: {} helper threads; set_file_len route: {:?}",
        report.helper_threads(),
        report.route(Call::SetFileLen)
    )?;
    // The driver gives the service only the calls whose route is `Route::BlockingService`.
    let budget = Budget::new(NonZeroU64::try_from(1_u64 << 20)?);
    let service = Service::new(&budget, blocking::Config::new().threads(1).jobs(4).stack(256 << 10))?;
    driver.use_blocking(service.submitter());

    file(&mut driver, out)?;
    tcp(&mut driver, out)?;
    udp(&mut driver, out)?;

    // Each close ended each operation of its resource, so teardown has nothing to cancel.
    let teardown = driver.teardown()?;
    writeln!(
        out,
        "driver: teardown cancelled {} operations and kept {} children",
        teardown.cancelled(),
        teardown.kept()
    )?;
    let teardown = service.teardown();
    writeln!(
        out,
        "blocking service: teardown found {} running jobs and dropped {} jobs",
        teardown.running(),
        teardown.dropped()
    )?;
    Ok(())
}

/// A file: a write, a sync, an exact read that ends early, a change of the length, and a read
/// into the same buffer.
fn file(driver: &mut Driver, out: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let waiter = Arc::default();
    let path = std::env::temp_dir().join(format!("rsasync-driver-only-{}", std::process::id()));
    let options = OpenOptions::new().read(true).write(true).create(true).truncate(true);
    let open = driver.open_file(&path, options)?;
    let file = drive(driver, &waiter, open)???;

    let write = driver.write_at(&file, Lease::from(b"hello world".to_vec()), 0, WriteMode::Plain)?;
    let (written, _lease) = drive(driver, &waiter, write)??;
    let sync = driver.sync(&file, SyncMode::Data)?;
    drive(driver, &waiter, sync)???;
    writeln!(out, "file: wrote {} bytes and made them durable", written?)?;

    // An exact read of more bytes than the file has fails, and its buffer holds the bytes that
    // the read put in.
    let read = driver.read_exact_at(&file, Backing::from(Vec::with_capacity(16)), 0)?;
    let (read, mut buffer) = drive(driver, &waiter, read)??;
    let error = read.err().ok_or("the exact read of 16 bytes did not fail")?;
    let incomplete = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<Incomplete>().copied());
    let count = incomplete.ok_or(error)?.read();
    writeln!(
        out,
        "file: an exact read of 16 bytes stopped at the end of the file after {count} bytes: {:?}",
        text(&buffer)
    )?;

    // On a kernel with no ring operation for it, the change of the length runs on the service.
    let set = driver.set_file_len(&file, 5)?;
    drive(driver, &waiter, set)???;
    let len = driver.file_len(&file)?;
    let len = drive(driver, &waiter, len)???;
    writeln!(out, "file: set the length to 5, and the length is {len}")?;

    buffer.truncate(0);
    let read = driver.read_at(&file, buffer, 0)?;
    let (read, buffer) = drive(driver, &waiter, read)??;
    writeln!(
        out,
        "file: the same buffer then read {} bytes: {:?}",
        read?,
        text(&buffer)
    )?;

    let close = driver.close_file(file)?;
    drive(driver, &waiter, close)??;
    let unlink = driver.unlink(&path)?;
    drive(driver, &waiter, unlink)???;
    writeln!(out, "file: closed and removed the file")?;
    Ok(())
}

/// A TCP connection: a receive that wakes its current waiter, partial progress, a cancel, a
/// dropped receive, a dropped accept, and close.
fn tcp(driver: &mut Driver, out: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let waiter = Arc::default();
    let bind = driver.bind(loopback(), TcpOptions::default())?;
    let mut listener = drive(driver, &waiter, bind)???;
    let connect = driver.connect(listener.local_addr(), TcpOptions::default())?;
    let mut client = drive(driver, &waiter, connect)???;
    let accept = driver.accept(&mut listener)?;
    let (mut server, _peer) = drive(driver, &waiter, accept)???;

    // The receive gives its completion through the waker of its latest poll.
    let (earlier, current) = (Arc::new(Count::default()), Arc::new(Count::default()));
    let receive = driver.recv(&mut server, Backing::from(Vec::with_capacity(8)))?;
    let mut receive = pin!(receive);
    for count in [&earlier, &current] {
        let waker = Waker::from(Arc::clone(count));
        if receive.as_mut().poll(&mut Context::from_waker(&waker)).is_ready() {
            return Err("the receive was ready before the send".into());
        }
    }
    let send = driver.send(&mut client, Lease::from(b"hello world".to_vec()))?;
    let (sent, _lease) = drive(driver, &waiter, send)??;
    let sent = sent?;
    let (received, mut buffer) = drive(driver, &current, receive)??;
    writeln!(
        out,
        "tcp: the receive woke its current waiter {} time and its earlier waiter {} times",
        current.wakes(),
        earlier.wakes()
    )?;
    writeln!(
        out,
        "tcp: the receive gave {} of the {sent} sent bytes: {:?}",
        received?,
        text(&buffer)
    )?;

    buffer.truncate(0);
    let receive = driver.recv(&mut server, buffer)?;
    let (received, mut buffer) = drive(driver, &waiter, receive)??;
    received?;
    writeln!(out, "tcp: the same buffer then received the rest: {:?}", text(&buffer))?;

    // A cancel keeps the interest: the future gives the result and the buffer.
    buffer.truncate(0);
    let receive = driver.recv(&mut server, buffer)?;
    receive.cancel();
    let (received, buffer) = drive(driver, &waiter, receive)??;
    let outcome = match received {
        Err(error) if error.raw_os_error() == Some(ECANCELED) => "ECANCELED".to_owned(),
        Err(error) => error.to_string(),
        Ok(count) => format!("{count} bytes"),
    };
    writeln!(out, "tcp: the cancelled receive gave {outcome} and its buffer back")?;
    let send = driver.send(&mut client, Lease::from(b"again".to_vec()))?;
    drive(driver, &waiter, send)??.0?;
    let receive = driver.recv(&mut server, buffer)?;
    let (received, buffer) = drive(driver, &waiter, receive)??;
    received?;
    writeln!(out, "tcp: the same buffer then received {:?}", text(&buffer))?;

    // A drop ends the interest, not the operation: the driver keeps the buffer until the kernel
    // ends the receive, and the bytes stay with the stream.
    drop(driver.recv(&mut server, Backing::from(Vec::with_capacity(8)))?);
    let send = driver.send(&mut client, Lease::from(b"late".to_vec()))?;
    drive(driver, &waiter, send)??.0?;
    let receive = driver.recv(&mut server, Backing::from(Vec::with_capacity(8)))?;
    let (received, buffer) = drive(driver, &waiter, receive)??;
    received?;
    writeln!(
        out,
        "tcp: after a dropped receive, the next receive gave {:?}",
        text(&buffer)
    )?;

    drop(driver.accept(&mut listener)?);
    let close = driver.close_listener(listener)?;
    drive(driver, &waiter, close)??;
    writeln!(out, "tcp: closed the listener after a dropped accept")?;
    // No dropped send owes a count of sent bytes.
    let mut sent = [None; 2];
    for (stream, sent) in [server, client].into_iter().zip(&mut sent) {
        let close = driver.close(stream)?;
        *sent = drive(driver, &waiter, close)??.sent();
    }
    writeln!(out, "tcp: closed both streams; Closed::sent() gives {sent:?}")?;
    Ok(())
}

/// UDP datagrams: a truncated receive, the same buffer for the next datagram, a dropped receive,
/// and close.
fn udp(driver: &mut Driver, out: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let waiter = Arc::default();
    let bind = driver.bind_udp(loopback(), UdpOptions::default())?;
    let mut receiver = drive(driver, &waiter, bind)???;
    let bind = driver.bind_udp(loopback(), UdpOptions::default())?;
    let mut sender = drive(driver, &waiter, bind)???;
    let to = receiver.local_addr();

    let send = driver.send_to(&mut sender, Datagram::new(Lease::from(b"ping!".to_vec()), to))?;
    drive(driver, &waiter, send)??.0?;
    let receive = driver.recv_from(&mut receiver, Backing::from(Vec::with_capacity(4)))?;
    let (received, mut buffer) = drive(driver, &waiter, receive)??;
    writeln!(
        out,
        "udp: a receive into 4 bytes gave {:?}, truncated: {}",
        text(&buffer),
        received?.truncated
    )?;

    let send = driver.send_to(&mut sender, Datagram::new(Lease::from(b"pong".to_vec()), to))?;
    drive(driver, &waiter, send)??.0?;
    buffer.truncate(0);
    let receive = driver.recv_from(&mut receiver, buffer)?;
    let (received, buffer) = drive(driver, &waiter, receive)??;
    writeln!(
        out,
        "udp: the same buffer then received {:?}, truncated: {}",
        text(&buffer),
        received?.truncated
    )?;

    // A drop ends the interest: a datagram that the dropped receive took stays with the socket.
    drop(driver.recv_from(&mut receiver, Backing::from(Vec::with_capacity(8)))?);
    let send = driver.send_to(&mut sender, Datagram::new(Lease::from(b"late".to_vec()), to))?;
    drive(driver, &waiter, send)??.0?;
    let receive = driver.recv_from(&mut receiver, Backing::from(Vec::with_capacity(8)))?;
    let (received, buffer) = drive(driver, &waiter, receive)??;
    received?;
    writeln!(
        out,
        "udp: after a dropped receive, the next receive gave {:?}",
        text(&buffer)
    )?;

    for socket in [receiver, sender] {
        let close = driver.close_udp(socket)?;
        drive(driver, &waiter, close)??;
    }
    writeln!(out, "udp: closed both sockets")?;
    Ok(())
}

/// Drives `driver` until `future` is ready: the loop of the caller. The loop polls the future
/// again only after the future woke `waiter`, because the driver gives each completion through
/// the waker of the latest poll.
fn drive<F: Future>(driver: &mut Driver, waiter: &Arc<Count>, future: F) -> io::Result<F::Output> {
    let waker = Waker::from(Arc::clone(waiter));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        let seen = waiter.wakes();
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return Ok(output);
        }
        while waiter.wakes() == seen {
            if !driver.turn()?.more() && waiter.wakes() == seen {
                driver.wait()?;
            }
        }
    }
}

/// A waker that counts its wakes.
#[derive(Debug, Default)]
struct Count(AtomicU32);

impl Count {
    fn wakes(&self) -> u32 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// An address on the loopback interface, with a port that the system selects.
fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// The bytes of `buffer`, as text.
fn text(buffer: &Backing) -> String {
    String::from_utf8_lossy(buffer.bytes()).into_owned()
}
