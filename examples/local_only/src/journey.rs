//! The steps of the example. Each step adds one note for each outcome to the state of the task.

use core::cell::RefCell;
use core::error::Error;
use core::net::SocketAddr;
use std::io::Write;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use rsasync::buffer::{Backing, BufferMut as _, Lease};
use rsasync::driver::{Datagram, Incomplete, OpenOptions, Stale, SyncMode, TcpOptions, UdpOptions, WriteMode};
use rsasync::local::{File, IpcListener, IpcStream, TcpListener, TcpStream, UdpSocket, unlink};
use rsasync::runtime::{Runtime, WorkerId};
use rsasync::task::Outcome;

/// The state of the task: its notes, which only its worker uses. An `Rc` is not `Send`, and the task
/// keeps it across each wait.
type Notes = Rc<RefCell<Vec<String>>>;

/// Runs the task on worker 0, prints its notes, and stops the runtime with a report.
pub(crate) fn run(out: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let directory = std::env::temp_dir().join(format!("rsasync-local-only-{}", std::process::id()));
    std::fs::create_dir_all(&directory)?;
    let runtime = Runtime::builder().local_workers(1).build()?;
    let mut handle = runtime.handle()?;
    let task = handle
        .spawn_on(WorkerId::new(0), move || task(directory))
        .map_err(|refused| format!("the spawn refused: {:?}", refused.reason()))?;
    let notes = match task.join_blocking() {
        Outcome::Completed(notes) => notes?,
        other => return Err(format!("the task ended with {other:?}").into()),
    };
    for note in notes {
        writeln!(out, "{note}")?;
    }
    // Each step closed what it made, so no operation remains for the shutdown to cancel.
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

/// The task: each step, then the notes. An error of a step ends the task with its text, because
/// the output of a task goes to another thread.
async fn task(directory: PathBuf) -> Result<Vec<String>, String> {
    let notes = Notes::default();
    let steps = async {
        file(&directory, &notes).await?;
        tcp(&notes).await?;
        udp(&notes).await?;
        ipc(&directory, &notes).await?;
        Ok::<(), Box<dyn Error>>(())
    };
    steps.await.map_err(|error| error.to_string())?;
    drop(std::fs::remove_dir(&directory));
    let count = notes.borrow().len();
    note(
        &notes,
        format!("state: one Rc kept the {count} notes of the task on worker 0"),
    );
    Ok(notes.take())
}

fn note(notes: &Notes, note: String) {
    notes.borrow_mut().push(note);
}

/// The text of the initialized bytes of `buffer`.
fn text(buffer: &impl rsasync::buffer::Buffer) -> String {
    String::from_utf8_lossy(buffer.bytes()).into_owned()
}

/// A file: a write and a sync, an exact read that ends early, and a read into the same buffer.
async fn file(directory: &Path, notes: &Notes) -> Result<(), Box<dyn Error>> {
    let path = directory.join("data");
    let options = OpenOptions::new().read(true).write(true).create(true).truncate(true);
    let file = File::open(&path, options)?.await??;
    let (written, _lease) = file
        .write_at(Lease::from(b"hello world".to_vec()), 0, WriteMode::Plain)?
        .await?;
    file.sync(SyncMode::Data)?.await??;
    note(notes, format!("file: wrote {} bytes and made them durable", written?));

    // An exact read of more bytes than the file has fails, and its buffer holds the bytes that the
    // read put in.
    let (read, mut buffer) = file.read_exact_at(Backing::from(Vec::with_capacity(16)), 0)?.await?;
    let error = read.err().ok_or("the exact read of 16 bytes did not fail")?;
    let incomplete = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<Incomplete>().copied());
    let count = incomplete.ok_or(error)?.read();
    note(
        notes,
        format!(
            "file: an exact read of 16 bytes stopped at the end of the file after {count} bytes: {:?}",
            text(&buffer)
        ),
    );
    buffer.truncate(0);
    let (read, buffer) = file.read_at(buffer, 6)?.await?;
    note(
        notes,
        format!("file: the same buffer then read {} bytes: {:?}", read?, text(&buffer)),
    );
    file.close()?.await?;
    unlink(&path)?.await??;
    note(notes, String::from("file: closed and removed the file"));
    Ok(())
}

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// A TCP connection: partial progress, the reuse of a buffer, a dropped receive, and close.
async fn tcp(notes: &Notes) -> Result<(), Box<dyn Error>> {
    let mut listener = TcpListener::bind(loopback(), TcpOptions::default())?.await??;
    let connect = TcpStream::connect(listener.local_addr(), TcpOptions::default())?;
    let (mut server, _peer) = listener.accept()?.await??;
    let mut client = connect.await??;

    let (sent, _lease) = client.send(Lease::from(b"hello world".to_vec()))?.await?;
    let sent = sent?;
    let (received, mut buffer) = server.recv(Backing::from(Vec::with_capacity(8)))?.await?;
    note(
        notes,
        format!(
            "tcp: the receive gave {} of the {sent} sent bytes: {:?}",
            received?,
            text(&buffer)
        ),
    );
    buffer.truncate(0);
    let (received, buffer) = server.recv(buffer)?.await?;
    received?;
    note(
        notes,
        format!("tcp: the same buffer then received the rest: {:?}", text(&buffer)),
    );

    // A receive whose future drops leaves the bytes that come later to the next receive.
    drop(server.recv(Backing::from(Vec::with_capacity(8)))?);
    client.send(Lease::from(b"late".to_vec()))?.await?.0?;
    let (received, buffer) = server.recv(Backing::from(Vec::with_capacity(8)))?.await?;
    received?;
    note(
        notes,
        format!(
            "tcp: after a dropped receive, the next receive gave {:?}",
            text(&buffer)
        ),
    );

    server.close()?.await?;
    client.close()?.await?;
    listener.close()?.await?;
    note(notes, String::from("tcp: closed both streams and the listener"));
    Ok(())
}

/// UDP: a truncated datagram, the reuse of its buffer, and close.
async fn udp(notes: &Notes) -> Result<(), Box<dyn Error>> {
    let mut sender = UdpSocket::bind(loopback(), UdpOptions::default())?.await??;
    let mut receiver = UdpSocket::bind(loopback(), UdpOptions::default())?.await??;
    let to = receiver.local_addr();
    sender
        .send_to(Datagram::new(Lease::from(b"pinged".to_vec()), to))?
        .await?
        .0?;
    let (received, mut buffer) = receiver.recv_from(Backing::from(Vec::with_capacity(4)))?.await?;
    let received = received?;
    note(
        notes,
        format!(
            "udp: a receive into 4 bytes gave {:?}, truncated: {}",
            text(&buffer),
            received.truncated
        ),
    );
    buffer.truncate(0);
    sender
        .send_to(Datagram::new(Lease::from(b"pong".to_vec()), to))?
        .await?
        .0?;
    let (received, buffer) = receiver.recv_from(buffer)?.await?;
    let received = received?;
    note(
        notes,
        format!(
            "udp: the same buffer then received {:?}, truncated: {}",
            text(&buffer),
            received.truncated
        ),
    );
    sender.close()?.await?;
    receiver.close()?.await?;
    note(notes, String::from("udp: closed both sockets"));
    Ok(())
}

/// An IPC stream at a path in `directory`: a send, a receive, the user of the peer, and close.
async fn ipc(directory: &Path, notes: &Notes) -> Result<(), Box<dyn Error>> {
    let path = directory.join("socket");
    let mut listener = IpcListener::bind(&path, Stale::Refuse)?.await??;
    let connect = IpcStream::connect(&path)?;
    let mut server = listener.accept()?.await??;
    let mut client = connect.await??;
    client.send(Lease::from(b"hello".to_vec()))?.await?.0?;
    let (received, buffer) = server.recv(Backing::from(Vec::with_capacity(16)))?.await?;
    received?;
    let ours = server.peer()?.user() == std::fs::metadata(&path)?.uid();
    note(
        notes,
        format!(
            "ipc: the server received {:?} from a peer of this user: {ours}",
            text(&buffer)
        ),
    );
    server.close()?.await?;
    client.close()?.await?;
    listener.close()?.await?;
    std::fs::remove_file(&path)?;
    note(notes, String::from("ipc: closed both streams and the listener"));
    Ok(())
}
