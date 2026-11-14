//! Pipe ends on the driver: the read end and the write end of a pipe or a FIFO, under the stream
//! rules of TCP.

use std::os::fd::{AsRawFd as _, OwnedFd};

use super::tcp::{Closed, ReadHalf, WriteHalf};
use super::{Driver, Medium, Operation, Reason, Refused, Resource, sys};

/// The read end of a pipe, which the driver owns. [`Driver::recv`] and [`Driver::recv_selected`]
/// borrow it, as they borrow a [`TcpStream`](super::TcpStream), under the same rules: one
/// operation at a time, and the bytes of a dropped receive go to the next receive. A receive gives
/// 0 at the end of the stream: after the close of each write end.
///
/// A send, the close of the writer, and the receives until the end of the stream, with `run` as in
/// [`Operation`]:
///
/// ```
/// # fn run<F: std::future::Future>(driver: &mut rsasync::driver::Driver, future: F) -> std::io::Result<F::Output> {
/// #     use std::future::Future as _;
/// #     let mut future = std::pin::pin!(future);
/// #     let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
/// #     loop {
/// #         if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #             return Ok(output);
/// #         }
/// #         if !driver.turn()?.more() {
/// #             if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
/// #                 return Ok(output);
/// #             }
/// #             driver.wait()?;
/// #         }
/// #     }
/// # }
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::os::fd::OwnedFd;
///
/// use rsasync::buffer::{Backing, Buffer as _, Lease};
/// use rsasync::driver::{Config, Driver};
///
/// let mut driver = Driver::new(Config::new())?;
/// let (read, write) = std::io::pipe()?;
/// let mut reader = driver.adopt_reader(OwnedFd::from(read))?;
/// let mut writer = driver.adopt_writer(OwnedFd::from(write))?;
///
/// let send = driver.send(&mut writer, Lease::from(b"hello".to_vec()))?;
/// let (sent, _lease) = run(&mut driver, send)??;
/// assert_eq!(sent?, 5);
/// let close = driver.close_writer(writer)?;
/// run(&mut driver, close)??;
///
/// let receive = driver.recv(&mut reader, Backing::from(Vec::with_capacity(64)))?;
/// let (received, buffer) = run(&mut driver, receive)??;
/// assert_eq!((received?, buffer.bytes()), (5, &b"hello"[..]));
/// let receive = driver.recv(&mut reader, Backing::from(Vec::with_capacity(64)))?;
/// let (end, _buffer) = run(&mut driver, receive)??;
/// assert_eq!(end?, 0, "the end of the stream");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct PipeReader {
    pub(super) read: ReadHalf,
}

/// The write end of a pipe, which the driver owns. [`Driver::send`] and [`Driver::send_vectored`]
/// borrow it, as they borrow a [`TcpStream`](super::TcpStream), under the same rules: a dropped
/// send still runs, and a failed or partial dropped send fails the write direction. Its close is
/// the end of the stream for the reader.
///
/// A send to a pipe whose read end closed gives `EPIPE` ([`std::io::ErrorKind::BrokenPipe`])
/// where the process ignores `SIGPIPE`, as a Rust program does by default. Elsewhere the signal
/// ends the process. The driver changes no disposition.
#[derive(Debug)]
pub struct PipeWriter {
    pub(super) write: WriteHalf,
}

impl AsMut<ReadHalf> for PipeReader {
    fn as_mut(&mut self) -> &mut ReadHalf {
        &mut self.read
    }
}

impl AsMut<WriteHalf> for PipeWriter {
    fn as_mut(&mut self) -> &mut WriteHalf {
        &mut self.write
    }
}

impl Driver {
    /// Takes `fd`, the read end of a pipe or a FIFO, for example the standard output of a child.
    /// The driver clears `O_NONBLOCK` of its open file description, because a nonblocking read
    /// ends at once with no data instead of waiting.
    ///
    /// # Errors
    ///
    /// A refusal, with `fd`, that gives [`Reason::NotPipe`], [`Reason::Direction`] for a write
    /// end, or [`Reason::Resources`].
    pub fn adopt_reader(&mut self, fd: OwnedFd) -> Result<PipeReader, Refused<OwnedFd>> {
        let resource = self.adopt_pipe(fd, true)?;
        Ok(PipeReader {
            read: ReadHalf { resource },
        })
    }

    /// Takes `fd`, the write end of a pipe or a FIFO, for example the standard input of a child.
    /// The driver clears `O_NONBLOCK` of its open file description, because a nonblocking write
    /// to a full pipe ends at once instead of waiting.
    ///
    /// # Errors
    ///
    /// A refusal, with `fd`, that gives [`Reason::NotPipe`], [`Reason::Direction`] for a read
    /// end, or [`Reason::Resources`].
    pub fn adopt_writer(&mut self, fd: OwnedFd) -> Result<PipeWriter, Refused<OwnedFd>> {
        let resource = self.adopt_pipe(fd, false)?;
        Ok(PipeWriter {
            write: WriteHalf { resource },
        })
    }

    /// Closes `reader`. The close cancels the receive that runs, waits until it ends, releases
    /// the bytes that no receive took, and closes the descriptor. A writer then gets `EPIPE`.
    ///
    /// # Errors
    ///
    /// A refusal, with the reader, when the driver has no room for the operation.
    pub fn close_reader(&mut self, reader: PipeReader) -> Result<Operation<'static, Closed>, Refused<PipeReader>> {
        let resource = reader.read.resource;
        let completion = self.close_resource(resource, reader)?;
        Ok(Operation::new(completion, |_, held| held.closed))
    }

    /// Closes `writer`. The close cancels the send that runs, a dropped send too, waits until it
    /// ends, and closes the descriptor: the reader then reads the end of the stream. The future
    /// gives the progress that dropped sends owe.
    ///
    /// # Errors
    ///
    /// A refusal, with the writer, when the driver has no room for the operation.
    pub fn close_writer(&mut self, writer: PipeWriter) -> Result<Operation<'static, Closed>, Refused<PipeWriter>> {
        let resource = writer.write.resource;
        let completion = self.close_resource(resource, writer)?;
        Ok(Operation::new(completion, |_, held| held.closed))
    }

    /// Takes the pipe end `fd`, which must be readable when `read` holds, and writable when it
    /// does not.
    #[expect(unsafe_code, reason = "fcntl names no memory with these commands")]
    fn adopt_pipe(&mut self, fd: OwnedFd, read: bool) -> Result<Resource, Refused<OwnedFd>> {
        let refuse = |reason, value| Err(Refused { reason, value });
        let raw = fd.as_raw_fd();
        // SAFETY: the command names no memory. Only a pipe or a FIFO has a capacity.
        if unsafe { sys::fcntl(raw, sys::F_GETPIPE_SZ) } < 0 {
            return refuse(Reason::NotPipe, fd);
        }
        // SAFETY: the command names no memory.
        let flags = unsafe { sys::fcntl(raw, sys::F_GETFL) };
        let mode = flags & sys::O_ACCMODE;
        let wanted = if read { sys::O_RDONLY } else { sys::O_WRONLY };
        if flags < 0 || (mode != wanted && mode != sys::O_RDWR) {
            return refuse(Reason::Direction, fd);
        }
        if !blocks(&fd) {
            return refuse(Reason::Unsupported, fd);
        }
        self.resources
            .adopt(fd, Medium::Pipe)
            .or_else(|fd| refuse(Reason::Resources, fd))
    }
}

/// Clears `O_NONBLOCK` of the open file description of `fd`, because a nonblocking read or write
/// ends at once instead of waiting. Gives whether the description blocks now.
#[expect(unsafe_code, reason = "fcntl names no memory with these commands")]
pub(super) fn blocks(fd: &OwnedFd) -> bool {
    let raw = fd.as_raw_fd();
    // SAFETY: the command names no memory.
    let flags = unsafe { sys::fcntl(raw, sys::F_GETFL) };
    // SAFETY: the command names no memory, and its argument is an `int`.
    flags >= 0
        && (flags & sys::O_NONBLOCK == 0 || unsafe { sys::fcntl(raw, sys::F_SETFL, flags & !sys::O_NONBLOCK) } >= 0)
}

#[cfg(all(test, not(loom)))]
mod tests;
