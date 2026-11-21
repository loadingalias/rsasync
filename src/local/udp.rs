//! UDP on a local owner: a socket, and its two halves.
//!
//! Each call has the meaning of the [`Driver`](crate::driver::Driver) call of the same name. A
//! direction of a socket runs one operation at a time: a receive borrows the read half, and a send
//! the write half. [`UdpSocket::split`] gives both halves, so a receive and a send run at one time.

use core::mem::DropGuard;
use core::net::SocketAddr;
use std::io;

use super::{Home, rewrap, submit};
use crate::buffer::Backing;
use crate::datagram::Received;
use crate::driver::{self, Arrivals, Closed, Datagram, Datagrams, Operation, Refused, UdpOptions};
use crate::task::with_driver;

/// A UDP socket whose home is the driver of the owner of this thread.
///
/// A drop of the socket closes it, as [`UdpSocket::close`] does, with no future.
#[derive(Debug)]
pub struct UdpSocket {
    inner: DropGuard<driver::UdpSocket, fn(driver::UdpSocket)>,
    _home: Home,
}

impl UdpSocket {
    fn new(inner: driver::UdpSocket) -> Self {
        Self {
            inner: DropGuard::new(inner, |socket| {
                drop(with_driver(socket, |driver, socket| driver.close_udp(socket)));
            }),
            _home: Home::default(),
        }
    }

    /// Binds a UDP socket to `address` with `options`, as
    /// [`Driver::bind_udp`](crate::driver::Driver::bind_udp) does.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, or when no driver runs on this
    /// thread. The future gives an error of the system.
    pub fn bind(address: SocketAddr, options: UdpOptions) -> Result<Operation<'static, io::Result<Self>>, Refused<()>> {
        let bind = submit((), |driver, ()| driver.bind_udp(address, options))?;
        Ok(bind.with_output(|result, held| driver::udp_bound(result, held).map(|bound| bound.map(Self::new))))
    }

    /// The local address of the socket.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr()
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

    /// Sends `datagram`, as [`Driver::send_to`](crate::driver::Driver::send_to) does.
    ///
    /// # Errors
    ///
    /// A refusal, with the datagram, when the driver has no room for the operation, when no driver
    /// runs on this thread, or with `Reason::Resource` when the socket has another home.
    pub fn send_to(
        &mut self,
        datagram: Datagram,
    ) -> Result<Operation<'_, (io::Result<usize>, Datagram)>, Refused<Datagram>> {
        let socket = &mut *self.inner;
        submit(datagram, |driver, datagram| driver.send_to(socket, datagram))
    }

    /// Sends the datagrams of `datagrams` in one operation, as
    /// [`Driver::send_batch`](crate::driver::Driver::send_batch) does.
    ///
    /// # Errors
    ///
    /// As for [`UdpSocket::send_to`], with the datagrams.
    pub fn send_batch(
        &mut self,
        datagrams: Datagrams,
    ) -> Result<Operation<'_, (io::Result<usize>, Datagrams)>, Refused<Datagrams>> {
        let socket = &mut *self.inner;
        submit(datagrams, |driver, datagrams| driver.send_batch(socket, datagrams))
    }

    /// Receives one datagram into the capacity of `buffer` after its initialized bytes, as
    /// [`Driver::recv_from`](crate::driver::Driver::recv_from) does.
    ///
    /// # Errors
    ///
    /// As for [`UdpSocket::send_to`], with the buffer.
    pub fn recv_from(
        &mut self,
        buffer: Backing,
    ) -> Result<Operation<'_, (io::Result<Received>, Backing)>, Refused<Backing>> {
        let socket = &mut *self.inner;
        submit(buffer, |driver, buffer| driver.recv_from(socket, buffer))
    }

    /// Receives the datagrams that wait, into the buffers of `arrivals`, as
    /// [`Driver::recv_batch`](crate::driver::Driver::recv_batch) does.
    ///
    /// # Errors
    ///
    /// As for [`UdpSocket::send_to`], with the arrivals.
    pub fn recv_batch(
        &mut self,
        arrivals: Arrivals,
    ) -> Result<Operation<'_, (io::Result<usize>, Arrivals)>, Refused<Arrivals>> {
        let socket = &mut *self.inner;
        submit(arrivals, |driver, arrivals| driver.recv_batch(socket, arrivals))
    }

    /// Closes the socket, as [`Driver::close_udp`](crate::driver::Driver::close_udp) does.
    ///
    /// # Errors
    ///
    /// A refusal, with the socket, when the driver has no room for the operation or when no driver
    /// runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        let socket = DropGuard::dismiss(self.inner);
        submit(socket, |driver, socket| driver.close_udp(socket)).map_err(|refused| rewrap(refused, Self::new))
    }
}

/// The read half of a [`UdpSocket`]: its receives borrow it.
#[derive(Debug)]
pub struct ReadHalf<'a> {
    read: &'a mut driver::ReadHalf,
    _home: Home,
}

impl ReadHalf<'_> {
    /// Receives one datagram, as [`UdpSocket::recv_from`] does.
    ///
    /// # Errors
    ///
    /// As for [`UdpSocket::recv_from`].
    pub fn recv_from(
        &mut self,
        buffer: Backing,
    ) -> Result<Operation<'_, (io::Result<Received>, Backing)>, Refused<Backing>> {
        let read = &mut *self.read;
        submit(buffer, |driver, buffer| driver.recv_from(read, buffer))
    }

    /// Receives the datagrams that wait, as [`UdpSocket::recv_batch`] does.
    ///
    /// # Errors
    ///
    /// As for [`UdpSocket::recv_batch`].
    pub fn recv_batch(
        &mut self,
        arrivals: Arrivals,
    ) -> Result<Operation<'_, (io::Result<usize>, Arrivals)>, Refused<Arrivals>> {
        let read = &mut *self.read;
        submit(arrivals, |driver, arrivals| driver.recv_batch(read, arrivals))
    }
}

/// The write half of a [`UdpSocket`]: its sends borrow it.
#[derive(Debug)]
pub struct WriteHalf<'a> {
    write: &'a mut driver::WriteHalf,
    _home: Home,
}

impl WriteHalf<'_> {
    /// Sends `datagram`, as [`UdpSocket::send_to`] does.
    ///
    /// # Errors
    ///
    /// As for [`UdpSocket::send_to`].
    pub fn send_to(
        &mut self,
        datagram: Datagram,
    ) -> Result<Operation<'_, (io::Result<usize>, Datagram)>, Refused<Datagram>> {
        let write = &mut *self.write;
        submit(datagram, |driver, datagram| driver.send_to(write, datagram))
    }

    /// Sends the datagrams of `datagrams` in one operation, as [`UdpSocket::send_batch`] does.
    ///
    /// # Errors
    ///
    /// As for [`UdpSocket::send_batch`].
    pub fn send_batch(
        &mut self,
        datagrams: Datagrams,
    ) -> Result<Operation<'_, (io::Result<usize>, Datagrams)>, Refused<Datagrams>> {
        let write = &mut *self.write;
        submit(datagrams, |driver, datagrams| driver.send_batch(write, datagrams))
    }
}
