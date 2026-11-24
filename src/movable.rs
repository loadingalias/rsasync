//! The I/O types of a movable task: TCP resources that are `Send`, so that a task keeps them on
//! each worker that polls it.
//!
//! A call of this module submits its operation to the driver of the owner of the thread that makes
//! the call: a worker of a [`Runtime`](crate::runtime::Runtime), or the owner of
//! [`Runtime::block_on`](crate::runtime::Runtime::block_on). That driver is the home of the
//! operation until its terminal event. The resource has no home. It owns its socket, and it moves
//! with its task. When an idle worker steals the task:
//!
//! - An operation that runs stays at its home, which stays driven. Its future gives the result on
//!   the worker that polls the task then.
//! - The next operation goes to the driver of that worker, with no message to another thread.
//! - A cancel, a drop, or a close sends a request to the home of each operation that runs. Only
//!   that home cancels the operation in its ring.
//!
//! Each call has the meaning of the call of [`rsasync::local`](crate::local) with the same name:
//!
//! - The call gives `Err(Refused)` before the wait, with what the caller gave.
//!   [`Reason::NoDriver`](crate::driver::Reason::NoDriver) says that no driver runs on this thread
//!   for the call. An accepted operation never refuses later.
//! - The future gives the buffer back with each outcome. The driver keeps each buffer until the
//!   terminal event of its operation.
//! - A direction runs one operation at a time. A dropped receive leaves its bytes to the next
//!   receive, and a dropped send runs on. The next operation of the direction starts after the
//!   dropped one ended, also when the two have different homes.
//! - `close` cancels each operation of the resource, and its future ends after their terminal
//!   events. A drop of the resource cancels them too, with no future, and the socket closes when
//!   they ended.
//!
//! The types run on io_uring. On a worker that runs on epoll, each call refuses with
//! [`Reason::Unsupported`](crate::driver::Reason::Unsupported).
//!
//! ```no_run
//! use rsasync::buffer::{Backing, Buffer as _, Lease};
//! use rsasync::driver::TcpOptions;
//! use rsasync::movable::{TcpListener, TcpStream};
//! use rsasync::runtime::Runtime;
//! use rsasync::task::{self, Outcome};
//!
//! let runtime = Runtime::builder().movable_workers(2).build()?;
//! runtime.block_on(async {
//!     let mut listener = TcpListener::bind("127.0.0.1:0".parse()?, TcpOptions::default())?.await??;
//!     let address = listener.local_addr();
//!     // The listener and its stream go with the task to each worker that polls it.
//!     let server = task::spawn_movable(async move {
//!         let (mut stream, _peer) = listener.accept().ok()?.await.ok()?.ok()?;
//!         let (received, buffer) = stream.recv(Backing::from(Vec::with_capacity(64))).ok()?.await.ok()?;
//!         let (sent, _lease) = stream.send(Lease::from(buffer)).ok()?.await.ok()?;
//!         stream.close().ok()?.await.ok()?;
//!         Some((received.ok()?, sent.ok()?))
//!     });
//!
//!     let mut client = TcpStream::connect(address, TcpOptions::default())?.await??;
//!     let (sent, _lease) = client.send(Lease::from(b"ping".to_vec()))?.await?;
//!     assert_eq!(sent?, 4);
//!     let (received, buffer) = client.recv(Backing::from(Vec::with_capacity(64)))?.await?;
//!     assert_eq!(&buffer.bytes()[..received?], b"ping");
//!     client.close()?.await?;
//!
//!     let Ok(server) = server else {
//!         return Err("the runtime has no movable worker".into());
//!     };
//!     assert!(matches!(server.join().await, Outcome::Completed(Some((4, 4)))));
//!     Ok::<(), Box<dyn std::error::Error>>(())
//! })??;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use core::fmt;
use core::future::Future;
use core::net::SocketAddr;
use core::pin::Pin;
use core::task::{Context, Poll, ready};
use std::io;

use crate::buffer::{Backing, Lease};
use crate::driver::{self, Closed, Direction, Ended, Held, PlainSocket, Refused, TcpOptions};
use crate::local::{rewrap, submit};

/// A listening TCP socket that moves with its task.
///
/// An accept borrows the listener, so one accept runs at a time. A connection that a dropped
/// accept took stays with the listener, and the next accept gives it. A drop of the listener
/// cancels its accept, and closes the socket.
#[derive(Debug)]
pub struct TcpListener {
    socket: PlainSocket,
    local: SocketAddr,
    accepts: Direction,
}

impl TcpListener {
    /// Binds a TCP socket to `address` with `options`, and listens on it, as
    /// [`local::TcpListener::bind`](crate::local::TcpListener::bind) does.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, or when no driver runs on this
    /// thread. The future gives an error of the system, or `OutOfMemory` when the allocator does
    /// not give the memory of the listener.
    pub fn bind(address: SocketAddr, options: TcpOptions) -> Result<Operation<'static, io::Result<Self>>, Refused<()>> {
        let place = submit((), |driver, ()| driver.bind_plain(address, options))?;
        Ok(Operation::owned(place, |result, held| {
            driver::plain_socket(result, held).map(|made| {
                made.map(|(socket, local)| Self {
                    socket,
                    local,
                    accepts: Direction::default(),
                })
            })
        }))
    }

    /// The local address of the listener.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local
    }

    /// Accepts a connection. The future gives the stream and the address of its peer. A drop of
    /// the future asks for a cancel.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, or when no driver runs on this
    /// thread. The future gives an error of the system, or `OutOfMemory` when the allocator does
    /// not give the memory of the stream. The connection closes then.
    pub fn accept(&mut self) -> Result<Operation<'_, io::Result<(TcpStream, SocketAddr)>>, Refused<()>> {
        let Self { socket, accepts, .. } = self;
        submit((), |driver, ()| accepts.accept(driver, socket))?;
        Ok(Operation::borrowed(accepts, |result, held| {
            driver::plain_socket(result, held).map(|made| made.map(|(socket, peer)| (TcpStream::new(socket), peer)))
        }))
    }

    /// Closes the listener: the close cancels the accept that runs, waits until it ends, and
    /// closes the socket and each connection that no accept took.
    ///
    /// # Errors
    ///
    /// A refusal, with the listener, when the driver has no room for the operation or when no
    /// driver runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        let Self { socket, local, accepts } = self;
        let parts = (socket, accepts, Direction::default());
        match submit(parts, |driver, (socket, accepts, none)| {
            Direction::close(driver, socket, accepts, none)
        }) {
            Ok(place) => Ok(Operation::owned(place, |_, held| held.closed)),
            Err(refused) => Err(rewrap(refused, |(socket, accepts, _)| Self { socket, local, accepts })),
        }
    }
}

/// A connected TCP stream that moves with its task.
///
/// A receive and a send each borrow the stream, so one operation runs at a time. The rules of
/// [`driver::TcpStream`] apply: a dropped receive leaves its bytes to the next receive, and a
/// dropped send runs on. A drop of the stream cancels each of its operations, and closes the
/// socket when they ended.
#[derive(Debug)]
pub struct TcpStream {
    socket: PlainSocket,
    read: Direction,
    write: Direction,
}

impl TcpStream {
    fn new(socket: PlainSocket) -> Self {
        Self {
            socket,
            read: Direction::default(),
            write: Direction::default(),
        }
    }

    /// Connects a TCP socket with `options` to `address`, as
    /// [`local::TcpStream::connect`](crate::local::TcpStream::connect) does. A drop of the future
    /// asks for a cancel, and closes the socket.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, or when no driver runs on this
    /// thread. The future gives an error of the system, or `OutOfMemory` when the allocator does
    /// not give the memory of the stream.
    pub fn connect(
        address: SocketAddr,
        options: TcpOptions,
    ) -> Result<Operation<'static, io::Result<Self>>, Refused<()>> {
        let place = submit((), |driver, ()| driver.connect_plain(address, options))?;
        Ok(Operation::owned(place, |result, held| {
            driver::plain_socket(result, held).map(|made| made.map(|(socket, _)| Self::new(socket)))
        }))
    }

    /// Receives into the capacity of `buffer` after its initialized bytes, as
    /// [`local::TcpStream::recv`](crate::local::TcpStream::recv) does. A drop of the future asks
    /// for a cancel.
    ///
    /// # Errors
    ///
    /// A refusal, with the buffer, when the driver has no room for the operation, or when no
    /// driver runs on this thread.
    pub fn recv(&mut self, buffer: Backing) -> Result<Operation<'_, (io::Result<usize>, Backing)>, Refused<Backing>> {
        let Self { socket, read, .. } = self;
        submit(buffer, |driver, buffer| read.recv(driver, socket, buffer))?;
        Ok(Operation::borrowed(read, driver::received))
    }

    /// Sends the bytes of `lease`, as [`local::TcpStream::send`](crate::local::TcpStream::send)
    /// does. A drop of the future does not cancel the send.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`], with the lease.
    pub fn send(&mut self, lease: Lease) -> Result<Operation<'_, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let Self { socket, write, .. } = self;
        submit(lease, |driver, lease| write.send(driver, socket, lease))?;
        Ok(Operation::borrowed(write, driver::sent))
    }

    /// Closes the stream: the close cancels each operation of the stream, a dropped send too, and
    /// its future ends after their terminal events. The future gives the progress that dropped
    /// sends owe.
    ///
    /// # Errors
    ///
    /// A refusal, with the stream, when the driver has no room for the operation or when no driver
    /// runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        let Self { socket, read, write } = self;
        match submit((socket, read, write), |driver, (socket, read, write)| {
            Direction::close(driver, socket, read, write)
        }) {
            Ok(place) => Ok(Operation::owned(place, |_, held| held.closed)),
            Err(refused) => Err(rewrap(refused, |(socket, read, write)| Self { socket, read, write })),
        }
    }
}

/// The place of the operation of a future: the direction that the future borrows, or its own
/// place when the operation has no resource yet.
enum Place<'a> {
    Borrowed(&'a mut Direction),
    Owned(Direction),
}

/// The future of one operation of a movable resource. It is `Send`, and it gives the result on the
/// thread that polls it, while the home of the operation stays driven. It wakes the waker of its
/// latest poll.
///
/// The future borrows the direction of its resource for `'a`, so no other operation starts in that
/// direction while it exists. The direction keeps the operation: a drop of the future ends no
/// interest, and the next operation of the direction, or the close, takes the result.
///
/// # Panics
///
/// A poll after the future gave `Ready` panics, as for the futures of the standard library.
pub struct Operation<'a, T> {
    place: Place<'a>,
    output: fn(i32, Held) -> Option<T>,
    done: bool,
}

impl<'a, T> Operation<'a, T> {
    const fn borrowed(direction: &'a mut Direction, output: fn(i32, Held) -> Option<T>) -> Self {
        Self {
            place: Place::Borrowed(direction),
            output,
            done: false,
        }
    }

    /// Asks the home of the operation to cancel it. The future keeps its interest: it later gives
    /// the result, with the original buffer and its exact progress.
    pub fn cancel(&self) {
        if !self.done {
            self.direction().cancel();
        }
    }

    const fn direction(&self) -> &Direction {
        match &self.place {
            Place::Borrowed(direction) => direction,
            Place::Owned(direction) => direction,
        }
    }
}

impl<T> Operation<'static, T> {
    const fn owned(place: Direction, output: fn(i32, Held) -> Option<T>) -> Self {
        Self {
            place: Place::Owned(place),
            output,
            done: false,
        }
    }
}

impl<T> Future for Operation<'_, T> {
    type Output = Result<T, Ended>;

    #[expect(
        clippy::panic,
        reason = "a poll after `Ready` breaks the contract of `Future`, and the caller has no recovery path"
    )]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.done {
            panic!("an `Operation` was polled after it gave `Ready`");
        }
        let direction = match &mut self.place {
            Place::Borrowed(direction) => &mut **direction,
            Place::Owned(direction) => direction,
        };
        let taken = ready!(direction.poll(cx));
        self.done = true;
        let output = taken.ok().and_then(|(result, held)| (self.output)(result, held));
        Poll::Ready(output.ok_or(Ended))
    }
}

impl<T> Drop for Operation<'_, T> {
    /// The direction keeps the operation. A receive, an accept, or a connect gets a cancel.
    fn drop(&mut self) {
        if !self.done {
            self.direction().forgo();
        }
    }
}

impl<T> fmt::Debug for Operation<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Operation")
            .field("direction", self.direction())
            .finish_non_exhaustive()
    }
}

// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
mod tests;
