//! TCP on a local owner: a listener, a stream, and the two halves of a stream.
//!
//! Each call has the meaning of the [`Driver`] call of the same name. A
//! direction of a stream runs one operation at a time: a receive borrows the read half, and a send
//! the write half. [`TcpStream::split`] gives both halves, so a receive and a send run at one time.

use core::net::SocketAddr;
use std::io;
use std::os::fd::OwnedFd;

use super::{Home, Homed, Operation, ReadHalf, Receive, Transmit, WriteHalf, call, rewrap, submit};
use crate::buffer::{Backing, Lease};
use crate::driver::{self, Closed, Completion, Driver, Refused, Regions, TcpOptions};

/// A listening TCP socket whose home is the driver of the owner of this thread.
///
/// An accept borrows the listener, so one accept runs at a time. A drop of the listener closes it,
/// as [`TcpListener::close`] does, with no future.
#[derive(Debug)]
pub struct TcpListener {
    inner: Homed<driver::TcpListener>,
}

impl TcpListener {
    /// The listener that `operation` made.
    fn new(listener: driver::TcpListener, operation: &Completion) -> Self {
        let cleanup = operation.cleanup(listener.resource());
        Self {
            inner: Homed::new(listener, cleanup),
        }
    }

    /// Binds a TCP socket to `address` with `options`, and listens on it, as
    /// [`Driver::bind`](crate::driver::Driver::bind) does.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, or when no driver runs on this
    /// thread. The future gives an error of the system.
    pub fn bind(address: SocketAddr, options: TcpOptions) -> Result<Operation<'static, io::Result<Self>>, Refused<()>> {
        let bind = submit((), |driver, ()| driver.bind(address, options))?;
        Ok(Operation::home(bind.with_output(|result, held, bind| {
            driver::tcp_bound(result, held).map(|bound| bound.map(|listener| Self::new(listener, bind)))
        })))
    }

    /// The local address of the listener.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.resource.local_addr()
    }

    /// Accepts a connection, as [`Driver::accept`](crate::driver::Driver::accept) does. The future
    /// gives the stream, whose home is the driver of the listener, and the address of its peer.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, when no driver runs on this
    /// thread, or with `Reason::Resource` when the listener has another home.
    pub fn accept(&mut self) -> Result<Operation<'_, io::Result<(TcpStream, SocketAddr)>>, Refused<()>> {
        let listener = &mut self.inner.resource;
        let accept = submit((), |driver, ()| driver.accept(listener))?;
        Ok(Operation::home(accept.with_output(|result, held, accept| {
            driver::accepted(result, held)
                .map(|accepted| accepted.map(|(stream, peer)| (TcpStream::new(stream, accept), peer)))
        })))
    }

    /// The options of the socket, with the values of the kernel.
    ///
    /// # Errors
    ///
    /// An error of the system, or of the kind `NotFound` when no driver of this thread owns the
    /// listener.
    pub fn options(&self) -> io::Result<TcpOptions> {
        call(|driver| driver.options(&self.inner.resource))
    }

    /// Sets each option of `options` that is not `None`.
    ///
    /// # Errors
    ///
    /// An error of the system, or of the kind `NotFound` when no driver of this thread owns the
    /// listener.
    pub fn set_options(&self, options: TcpOptions) -> io::Result<()> {
        call(|driver| driver.set_options(&self.inner.resource, options))
    }

    /// Closes the listener, as [`Driver::close_listener`](crate::driver::Driver::close_listener)
    /// does.
    ///
    /// # Errors
    ///
    /// A refusal, with the listener, when the driver has no room for the operation or when no
    /// driver runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        self.inner
            .close(Driver::close_listener)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
    }
}

/// A connected TCP stream whose home is the driver of the owner of this thread.
///
/// The rules of [`driver::TcpStream`] apply: a dropped receive leaves its bytes to the next
/// receive, and a dropped send runs on. A drop of the stream closes it, as [`TcpStream::close`]
/// does, with no future.
#[derive(Debug)]
pub struct TcpStream {
    inner: Homed<driver::TcpStream>,
}

impl TcpStream {
    /// The stream that `operation` made.
    fn new(stream: driver::TcpStream, operation: &Completion) -> Self {
        let cleanup = operation.cleanup(stream.resource());
        Self {
            inner: Homed::new(stream, cleanup),
        }
    }

    /// Connects a TCP socket with `options` to `address`, as
    /// [`Driver::connect`](crate::driver::Driver::connect) does.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, or when no driver runs on this
    /// thread. The future gives an error of the system.
    pub fn connect(
        address: SocketAddr,
        options: TcpOptions,
    ) -> Result<Operation<'static, io::Result<Self>>, Refused<()>> {
        let connect = submit((), |driver, ()| driver.connect(address, options))?;
        Ok(Operation::home(connect.with_output(|result, held, connect| {
            driver::connected(result, held).map(|stream| stream.map(|stream| Self::new(stream, connect)))
        })))
    }

    /// Takes `fd`, a connected TCP socket, as a stream whose home is the driver of this thread, as
    /// [`Driver::adopt_stream`] does: for example a connection that another worker accepted and
    /// gave with [`TcpStream::into_fd`].
    ///
    /// # Errors
    ///
    /// A refusal, with `fd`, with the reason of [`Driver::adopt_stream`], or `Reason::NoDriver`
    /// when no driver runs on this thread.
    pub fn adopt(fd: OwnedFd) -> Result<Self, Refused<OwnedFd>> {
        submit(fd, |driver, fd| {
            let stream = driver.adopt_stream(fd)?;
            let cleanup = driver.cleanup(stream.resource());
            Ok(Self {
                inner: Homed::new(stream, cleanup),
            })
        })
    }

    /// Gives the descriptor of the stream, with no close, as [`Driver::release_stream`] does: for
    /// example to give a connection that this worker accepted to another worker, which takes it
    /// with [`TcpStream::adopt`]. An accepted stream that ran no operation has no state, so its
    /// release succeeds.
    ///
    /// ```
    /// use rsasync::buffer::{Backing, Buffer as _, Lease};
    /// use rsasync::driver::TcpOptions;
    /// use rsasync::local::{TcpListener, TcpStream};
    /// use rsasync::runtime::{Runtime, WorkerId};
    /// use rsasync::task::Outcome;
    ///
    /// let runtime = Runtime::builder().local_workers(1).build()?;
    /// let mut handle = runtime.handle()?;
    /// runtime.block_on(async move {
    ///     let mut listener = TcpListener::bind("127.0.0.1:0".parse()?, TcpOptions::default())?.await??;
    ///     let connect = TcpStream::connect(listener.local_addr(), TcpOptions::default())?;
    ///     let (accepted, _peer) = listener.accept()?.await??;
    ///     let mut client = connect.await??;
    ///
    ///     // The acceptor gives the connection to worker 0, which echoes one message.
    ///     let fd = accepted.into_fd()?;
    ///     let echo = handle.spawn_on(WorkerId::new(0), move || async move {
    ///         let mut stream = TcpStream::adopt(fd).ok()?;
    ///         let (received, buffer) = stream.recv(Backing::from(Vec::with_capacity(64))).ok()?.await.ok()?;
    ///         let bytes = buffer.bytes()[..received.ok()?].to_vec();
    ///         let (sent, _lease) = stream.send(Lease::from(bytes)).ok()?.await.ok()?;
    ///         sent.ok()
    ///     })?;
    ///
    ///     client.send(Lease::from(b"ping".to_vec()))?.await?.0?;
    ///     let (received, buffer) = client.recv(Backing::from(Vec::with_capacity(64)))?.await?;
    ///     assert_eq!(&buffer.bytes()[..received?], b"ping");
    ///     assert!(matches!(echo.join().await, Outcome::Completed(Some(4))));
    ///     client.close()?.await?;
    ///     Ok::<(), Box<dyn std::error::Error>>(())
    /// })??;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// A refusal, with the stream, with the reason of [`Driver::release_stream`], or
    /// `Reason::NoDriver` when no driver runs on this thread.
    pub fn into_fd(self) -> Result<OwnedFd, Refused<Self>> {
        self.inner
            .end(Driver::release_stream)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
    }

    /// The read half and the write half, so that a receive and a send run at one time.
    pub fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        let (read, write) = self.inner.resource.split();
        (
            ReadHalf {
                read,
                home: &self.inner.cleanup,
                _home: Home::default(),
            },
            WriteHalf {
                write,
                home: &self.inner.cleanup,
                _home: Home::default(),
            },
        )
    }

    /// Receives into the capacity of `buffer` after its initialized bytes, as
    /// [`Driver::recv`](crate::driver::Driver::recv) does.
    ///
    /// # Errors
    ///
    /// A refusal, with the buffer, when the driver has no room for the operation, when no driver
    /// runs on this thread, or with `Reason::Resource` when the stream has another home.
    pub fn recv(&mut self, buffer: Backing) -> Result<Operation<'_, (io::Result<usize>, Backing)>, Refused<Backing>> {
        let stream = &mut self.inner.resource;
        submit(buffer, |driver, buffer| driver.recv(stream, buffer)).map(Operation::home)
    }

    /// Receives into a buffer that the kernel selects from the provided buffers of the worker when
    /// the bytes come, as [`Driver::recv_selected`](crate::driver::Driver::recv_selected) does: a
    /// receive that waits holds no buffer. The future gives the buffer with the received bytes,
    /// and an empty buffer at the end of the stream. A worker has these buffers by
    /// [`Builder::receive_buffers`](crate::runtime::Builder::receive_buffers).
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`], and `Reason::Buffers` when the worker has no provided buffers.
    pub fn recv_selected(&mut self) -> Result<Receive<'_>, Refused<()>> {
        Receive::start(self.inner.resource.as_mut(), &self.inner.cleanup)
    }

    /// Sends the bytes of `lease`, as [`Driver::send`](crate::driver::Driver::send) does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`], with the lease.
    pub fn send(&mut self, lease: Lease) -> Result<Transmit<'_>, Refused<Lease>> {
        Transmit::start(self.inner.resource.as_mut(), &self.inner.cleanup, lease)
    }

    /// Sends the bytes of `regions` in order, in one operation, as
    /// [`Driver::send_vectored`](crate::driver::Driver::send_vectored) does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`], with the regions.
    pub fn send_vectored(
        &mut self,
        regions: Regions,
    ) -> Result<Operation<'_, (io::Result<usize>, Regions)>, Refused<Regions>> {
        let stream = &mut self.inner.resource;
        submit(regions, |driver, regions| driver.send_vectored(stream, regions)).map(Operation::home)
    }

    /// Ends the write direction after the sent bytes, as
    /// [`Driver::shutdown_write`](crate::driver::Driver::shutdown_write) does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`].
    pub fn shutdown_write(&mut self) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let stream = &mut self.inner.resource;
        submit((), |driver, ()| driver.shutdown_write(stream)).map(Operation::home)
    }

    /// The options of the socket, with the values of the kernel.
    ///
    /// # Errors
    ///
    /// An error of the system, or of the kind `NotFound` when no driver of this thread owns the
    /// stream.
    pub fn options(&self) -> io::Result<TcpOptions> {
        call(|driver| driver.options(&self.inner.resource))
    }

    /// Sets each option of `options` that is not `None`.
    ///
    /// # Errors
    ///
    /// An error of the system, or of the kind `NotFound` when no driver of this thread owns the
    /// stream.
    pub fn set_options(&self, options: TcpOptions) -> io::Result<()> {
        call(|driver| driver.set_options(&self.inner.resource, options))
    }

    /// Closes the stream, as [`Driver::close`](crate::driver::Driver::close) does: it cancels each
    /// operation of the stream, a dropped send too, and the future ends after their terminal events.
    ///
    /// # Errors
    ///
    /// A refusal, with the stream, when the driver has no room for the operation or when no driver
    /// runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        self.inner
            .close(Driver::close)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
    }
}
