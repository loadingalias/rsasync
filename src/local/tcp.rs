//! TCP on a local owner: a listener, a stream, and the two halves of a stream.
//!
//! Each call has the meaning of the [`Driver`](crate::driver::Driver) call of the same name. A
//! direction of a stream runs one operation at a time: a receive borrows the read half, and a send
//! the write half. [`TcpStream::split`] gives both halves, so a receive and a send run at one time.

use core::mem::DropGuard;
use core::net::SocketAddr;
use std::io;

use super::{Home, call, rewrap, submit};
use crate::buffer::{Backing, Lease};
use crate::driver::{self, Closed, Operation, Refused, Regions, TcpOptions};
use crate::task::with_driver;

/// A listening TCP socket whose home is the driver of the owner of this thread.
///
/// An accept borrows the listener, so one accept runs at a time. A drop of the listener closes it,
/// as [`TcpListener::close`] does, with no future.
#[derive(Debug)]
pub struct TcpListener {
    inner: DropGuard<driver::TcpListener, fn(driver::TcpListener)>,
    _home: Home,
}

impl TcpListener {
    fn new(inner: driver::TcpListener) -> Self {
        Self {
            inner: DropGuard::new(inner, |listener| {
                drop(with_driver(listener, |driver, listener| {
                    driver.close_listener(listener)
                }));
            }),
            _home: Home::default(),
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
        Ok(bind.with_output(|result, held| driver::tcp_bound(result, held).map(|bound| bound.map(Self::new))))
    }

    /// The local address of the listener.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr()
    }

    /// Accepts a connection, as [`Driver::accept`](crate::driver::Driver::accept) does. The future
    /// gives the stream, whose home is the driver of the listener, and the address of its peer.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, when no driver runs on this
    /// thread, or with `Reason::Resource` when the listener has another home.
    pub fn accept(&mut self) -> Result<Operation<'_, io::Result<(TcpStream, SocketAddr)>>, Refused<()>> {
        let listener = &mut *self.inner;
        let accept = submit((), |driver, ()| driver.accept(listener))?;
        Ok(accept.with_output(|result, held| {
            driver::accepted(result, held).map(|accepted| accepted.map(|(stream, peer)| (TcpStream::new(stream), peer)))
        }))
    }

    /// The options of the socket, with the values of the kernel.
    ///
    /// # Errors
    ///
    /// An error of the system, or when the listener has no driver on this thread.
    pub fn options(&self) -> io::Result<TcpOptions> {
        call(|driver| driver.options(&*self.inner))
    }

    /// Sets each option of `options` that is not `None`.
    ///
    /// # Errors
    ///
    /// An error of the system, or when the listener has no driver on this thread.
    pub fn set_options(&self, options: TcpOptions) -> io::Result<()> {
        call(|driver| driver.set_options(&*self.inner, options))
    }

    /// Closes the listener, as [`Driver::close_listener`](crate::driver::Driver::close_listener)
    /// does.
    ///
    /// # Errors
    ///
    /// A refusal, with the listener, when the driver has no room for the operation or when no
    /// driver runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        let listener = DropGuard::dismiss(self.inner);
        submit(listener, |driver, listener| driver.close_listener(listener))
            .map_err(|refused| rewrap(refused, Self::new))
    }
}

/// A connected TCP stream whose home is the driver of the owner of this thread.
///
/// The rules of [`driver::TcpStream`] apply: a dropped receive leaves its bytes to the next
/// receive, and a dropped send runs on. A drop of the stream closes it, as [`TcpStream::close`]
/// does, with no future.
#[derive(Debug)]
pub struct TcpStream {
    inner: DropGuard<driver::TcpStream, fn(driver::TcpStream)>,
    _home: Home,
}

impl TcpStream {
    fn new(inner: driver::TcpStream) -> Self {
        Self {
            inner: DropGuard::new(inner, |stream| {
                drop(with_driver(stream, |driver, stream| driver.close(stream)));
            }),
            _home: Home::default(),
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
        Ok(connect.with_output(|result, held| driver::connected(result, held).map(|stream| stream.map(Self::new))))
    }

    /// The read half and the write half, so that a receive and a send run at one time.
    pub fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        let (read, write) = self.inner.split();
        (
            ReadHalf {
                read,
                _home: Home::default(),
            },
            WriteHalf {
                write,
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
        let stream = &mut *self.inner;
        submit(buffer, |driver, buffer| driver.recv(stream, buffer))
    }

    /// Sends the bytes of `lease`, as [`Driver::send`](crate::driver::Driver::send) does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`], with the lease.
    pub fn send(&mut self, lease: Lease) -> Result<Operation<'_, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let stream = &mut *self.inner;
        submit(lease, |driver, lease| driver.send(stream, lease))
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
        let stream = &mut *self.inner;
        submit(regions, |driver, regions| driver.send_vectored(stream, regions))
    }

    /// Ends the write direction after the sent bytes, as
    /// [`Driver::shutdown_write`](crate::driver::Driver::shutdown_write) does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`].
    pub fn shutdown_write(&mut self) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let stream = &mut *self.inner;
        submit((), |driver, ()| driver.shutdown_write(stream))
    }

    /// The options of the socket, with the values of the kernel.
    ///
    /// # Errors
    ///
    /// An error of the system, or when the stream has no driver on this thread.
    pub fn options(&self) -> io::Result<TcpOptions> {
        call(|driver| driver.options(&*self.inner))
    }

    /// Sets each option of `options` that is not `None`.
    ///
    /// # Errors
    ///
    /// An error of the system, or when the stream has no driver on this thread.
    pub fn set_options(&self, options: TcpOptions) -> io::Result<()> {
        call(|driver| driver.set_options(&*self.inner, options))
    }

    /// Closes the stream, as [`Driver::close`](crate::driver::Driver::close) does: it cancels each
    /// operation of the stream, a dropped send too, and the future ends after their terminal events.
    ///
    /// # Errors
    ///
    /// A refusal, with the stream, when the driver has no room for the operation or when no driver
    /// runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        let stream = DropGuard::dismiss(self.inner);
        submit(stream, |driver, stream| driver.close(stream)).map_err(|refused| rewrap(refused, Self::new))
    }
}

/// The read half of a [`TcpStream`]: its receives borrow it.
#[derive(Debug)]
pub struct ReadHalf<'a> {
    read: &'a mut driver::ReadHalf,
    _home: Home,
}

impl ReadHalf<'_> {
    /// Receives into the capacity of `buffer` after its initialized bytes, as
    /// [`TcpStream::recv`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv`].
    pub fn recv(&mut self, buffer: Backing) -> Result<Operation<'_, (io::Result<usize>, Backing)>, Refused<Backing>> {
        let read = &mut *self.read;
        submit(buffer, |driver, buffer| driver.recv(read, buffer))
    }
}

/// The write half of a [`TcpStream`]: its sends borrow it.
#[derive(Debug)]
pub struct WriteHalf<'a> {
    write: &'a mut driver::WriteHalf,
    _home: Home,
}

impl WriteHalf<'_> {
    /// Sends the bytes of `lease`, as [`TcpStream::send`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::send`].
    pub fn send(&mut self, lease: Lease) -> Result<Operation<'_, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let write = &mut *self.write;
        submit(lease, |driver, lease| driver.send(write, lease))
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
        let write = &mut *self.write;
        submit(regions, |driver, regions| driver.send_vectored(write, regions))
    }

    /// Ends the write direction after the sent bytes, as [`TcpStream::shutdown_write`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::shutdown_write`].
    pub fn shutdown_write(&mut self) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let write = &mut *self.write;
        submit((), |driver, ()| driver.shutdown_write(write))
    }
}
