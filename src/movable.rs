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
//!   dropped one ended, also when the two have different homes. A dropped `recv_selected` with
//!   no cancel runs on, and the next `recv_selected` takes it over with no submission.
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
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, ready};
use std::io;

use crate::buffer::{Backing, Lease};
use crate::driver::{self, Closed, Direction, Ended, Held, PlainSocket, Refused, Regions, TcpOptions};
use crate::local::{rewrap, submit_any};

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
        let place = submit_any((), |driver, ()| driver.bind_plain(address, options))?;
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
        submit_any((), |driver, ()| accepts.accept(driver, socket))?;
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
        match submit_any(parts, |driver, (socket, accepts, none)| {
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
        let place = submit_any((), |driver, ()| driver.connect_plain(address, options))?;
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
        submit_any(buffer, |driver, buffer| read.recv(driver, socket, buffer))?;
        Ok(Operation::borrowed(read, driver::received))
    }

    /// Receives into a buffer that the kernel selects from the provided buffers of the driver of
    /// this thread when the bytes come, as
    /// [`local::TcpStream::recv_selected`](crate::local::TcpStream::recv_selected) does: a receive
    /// that waits holds no buffer. A drop of the future with no [`Operation::cancel`] does not
    /// cancel the receive: it runs on at its home, and the next `recv_selected` of the stream, on
    /// any worker, takes it over with no submission and gives its bytes. A plain receive or the
    /// close cancels it.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`], and `Reason::Buffers` when the driver of this thread has no
    /// provided buffers.
    pub fn recv_selected(&mut self) -> Result<Operation<'_, io::Result<Backing>>, Refused<()>> {
        recv_selected(&self.socket, &mut self.read)
    }

    /// Sends the bytes of `lease`, as [`local::TcpStream::send`](crate::local::TcpStream::send)
    /// does. A drop of the future does not cancel the send.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`], with the lease.
    pub fn send(&mut self, lease: Lease) -> Result<Operation<'_, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let Self { socket, write, .. } = self;
        submit_any(lease, |driver, lease| write.send(driver, socket, lease))?;
        Ok(Operation::borrowed(write, driver::sent))
    }

    /// Sends the bytes of `regions` in order, in one operation, as
    /// [`local::TcpStream::send_vectored`](crate::local::TcpStream::send_vectored) does. A drop of
    /// the future does not cancel the send.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`], with the regions.
    pub fn send_vectored(
        &mut self,
        regions: Regions,
    ) -> Result<Operation<'_, (io::Result<usize>, Regions)>, Refused<Regions>> {
        send_vectored(&self.socket, &mut self.write, regions)
    }

    /// Ends the write side of the stream after the sends before it, as
    /// [`local::TcpStream::shutdown_write`](crate::local::TcpStream::shutdown_write) does: the peer
    /// reads the end of the stream, and the receives go on.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`].
    pub fn shutdown_write(&mut self) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        shutdown_write(&self.socket, &mut self.write)
    }

    /// Gives a read half and a write half, so that a receive and a send run at one time, each in
    /// its own direction. The halves borrow the stream.
    pub fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        let Self { socket, read, write } = self;
        (ReadHalf { socket, read }, WriteHalf { socket, write })
    }

    /// Reads each option of the stream, with the value of the kernel, as
    /// [`local::TcpStream::options`](crate::local::TcpStream::options) does. It needs no driver.
    ///
    /// # Errors
    ///
    /// An error of the system.
    pub fn options(&self) -> io::Result<TcpOptions> {
        driver::plain_options(&self.socket)
    }

    /// Sets each option of `options` that is not `None`, as
    /// [`local::TcpStream::set_options`](crate::local::TcpStream::set_options) does. It needs no
    /// driver.
    ///
    /// # Errors
    ///
    /// An error of the system, or `InvalidInput` for a buffer size above `i32::MAX`.
    pub fn set_options(&self, options: TcpOptions) -> io::Result<()> {
        driver::set_plain_options(&self.socket, options)
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
        match submit_any((socket, read, write), |driver, (socket, read, write)| {
            Direction::close(driver, socket, read, write)
        }) {
            Ok(place) => Ok(Operation::owned(place, |_, held| held.closed)),
            Err(refused) => Err(rewrap(refused, |(socket, read, write)| Self { socket, read, write })),
        }
    }
}

/// The read half of a [`TcpStream`]: its receives borrow it.
#[derive(Debug)]
pub struct ReadHalf<'a> {
    socket: &'a PlainSocket,
    read: &'a mut Direction,
}

impl ReadHalf<'_> {
    /// Receives into the capacity of `buffer` after its initialized bytes, as
    /// [`TcpStream::recv`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`].
    pub fn recv(&mut self, buffer: Backing) -> Result<Operation<'_, (io::Result<usize>, Backing)>, Refused<Backing>> {
        let Self { socket, read } = self;
        submit_any(buffer, |driver, buffer| read.recv(driver, socket, buffer))?;
        Ok(Operation::borrowed(read, driver::received))
    }

    /// Receives into a buffer that the kernel selects, as [`TcpStream::recv_selected`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv_selected`].
    pub fn recv_selected(&mut self) -> Result<Operation<'_, io::Result<Backing>>, Refused<()>> {
        recv_selected(self.socket, self.read)
    }
}

/// The write half of a [`TcpStream`]: its sends borrow it.
#[derive(Debug)]
pub struct WriteHalf<'a> {
    socket: &'a PlainSocket,
    write: &'a mut Direction,
}

impl WriteHalf<'_> {
    /// Sends the bytes of `lease`, as [`TcpStream::send`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::send`].
    pub fn send(&mut self, lease: Lease) -> Result<Operation<'_, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let Self { socket, write } = self;
        submit_any(lease, |driver, lease| write.send(driver, socket, lease))?;
        Ok(Operation::borrowed(write, driver::sent))
    }

    /// Sends the bytes of `regions` in order, as [`TcpStream::send_vectored`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::send_vectored`].
    pub fn send_vectored(
        &mut self,
        regions: Regions,
    ) -> Result<Operation<'_, (io::Result<usize>, Regions)>, Refused<Regions>> {
        send_vectored(self.socket, self.write, regions)
    }

    /// Ends the write side of the stream, as [`TcpStream::shutdown_write`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::shutdown_write`].
    pub fn shutdown_write(&mut self) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        shutdown_write(self.socket, self.write)
    }
}

/// Starts a receive into a selected buffer on `socket` in its direction `read`.
fn recv_selected<'a>(
    socket: &PlainSocket,
    read: &'a mut Direction,
) -> Result<Operation<'a, io::Result<Backing>>, Refused<()>> {
    submit_any((), |driver, ()| read.recv_selected(driver, socket))?;
    Ok(Operation::borrowed(read, driver::received_selected))
}

/// Starts a vectored send of `regions` on `socket` in its direction `write`.
fn send_vectored<'a>(
    socket: &PlainSocket,
    write: &'a mut Direction,
    regions: Regions,
) -> Result<Operation<'a, (io::Result<usize>, Regions)>, Refused<Regions>> {
    submit_any(regions, |driver, regions| write.send_vectored(driver, socket, regions))?;
    Ok(Operation::borrowed(write, driver::sent_regions))
}

/// Starts a half-close of `socket` in its direction `write`.
fn shutdown_write<'a>(
    socket: &PlainSocket,
    write: &'a mut Direction,
) -> Result<Operation<'a, io::Result<()>>, Refused<()>> {
    submit_any((), |driver, ()| write.shutdown_write(driver, socket))?;
    Ok(Operation::borrowed(write, driver::shut))
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
    output: fn(i32, &mut Held) -> Option<T>,
    done: bool,
    /// [`Operation::cancel`] asked the home to cancel the operation.
    cancelled: AtomicBool,
}

impl<'a, T> Operation<'a, T> {
    const fn borrowed(direction: &'a mut Direction, output: fn(i32, &mut Held) -> Option<T>) -> Self {
        Self {
            place: Place::Borrowed(direction),
            output,
            done: false,
            cancelled: AtomicBool::new(false),
        }
    }

    /// The identity of the operation that the future waits for, or `None` after the future gave
    /// its output. A selected receive that takes over the receive of a dropped future has the
    /// identity of that receive.
    #[must_use]
    pub fn id(&self) -> Option<driver::OperationId> {
        if self.done { None } else { self.direction().id() }
    }

    /// Asks the home of the operation to cancel it. The future keeps its interest: it later gives
    /// the result, with the original buffer and its exact progress.
    pub fn cancel(&self) {
        if !self.done {
            self.cancelled.store(true, Ordering::Relaxed);
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
    const fn owned(place: Direction, output: fn(i32, &mut Held) -> Option<T>) -> Self {
        Self {
            place: Place::Owned(place),
            output,
            done: false,
            cancelled: AtomicBool::new(false),
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
        let output = self.output;
        let direction = match &mut self.place {
            Place::Borrowed(direction) => &mut **direction,
            Place::Owned(direction) => direction,
        };
        let taken = ready!(direction.poll(cx, output));
        self.done = true;
        Poll::Ready(taken.ok().flatten().ok_or(Ended))
    }
}

impl<T> Drop for Operation<'_, T> {
    /// The direction keeps the operation. A selected receive with no cancel runs on for the next
    /// selected receive. Each other receive, an accept, or a connect gets a cancel.
    fn drop(&mut self) {
        if !self.done {
            let cancelled = *self.cancelled.get_mut();
            match &mut self.place {
                Place::Borrowed(direction) => direction.forgo(cancelled),
                Place::Owned(direction) => direction.forgo(cancelled),
            }
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
