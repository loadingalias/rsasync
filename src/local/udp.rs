//! UDP on a local owner: a socket, and its two halves.
//!
//! Each call has the meaning of the [`Driver`] call of the same name. A
//! direction of a socket runs one operation at a time: a receive borrows the read half, and a send
//! the write half. [`UdpSocket::split`] gives both halves, so a receive and a send run at one time.

use core::net::SocketAddr;
use std::io;

use super::{Home, Homed, rewrap, submit};
use crate::buffer::Backing;
use crate::datagram::Received;
use crate::driver::{self, Closed, Completion, Datagram, Datagrams, Driver, Operation, Refused, UdpOptions};

/// A UDP socket whose home is the driver of the owner of this thread.
///
/// A drop of the socket closes it, as [`UdpSocket::close`] does, with no future.
#[derive(Debug)]
pub struct UdpSocket {
    inner: Homed<driver::UdpSocket>,
}

impl UdpSocket {
    /// The socket that `operation` made.
    fn new(socket: driver::UdpSocket, operation: &Completion) -> Self {
        let cleanup = operation.cleanup(socket.resource());
        Self {
            inner: Homed::new(socket, cleanup),
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
        Ok(bind.with_output(|result, held, bind| {
            driver::udp_bound(result, held).map(|bound| bound.map(|socket| Self::new(socket, bind)))
        }))
    }

    /// The local address of the socket.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.resource.local_addr()
    }

    /// The read half and the write half, so that a receive and a send run at one time.
    pub fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        let (read, write) = self.inner.resource.split();
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
        let socket = &mut self.inner.resource;
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
        let socket = &mut self.inner.resource;
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
        let socket = &mut self.inner.resource;
        submit(buffer, |driver, buffer| driver.recv_from(socket, buffer))
    }

    /// Closes the socket, as [`Driver::close_udp`](crate::driver::Driver::close_udp) does.
    ///
    /// # Errors
    ///
    /// A refusal, with the socket, when the driver has no room for the operation or when no driver
    /// runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        self.inner
            .close(Driver::close_udp)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
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
