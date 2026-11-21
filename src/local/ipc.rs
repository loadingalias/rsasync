//! IPC (inter-process communication) streams on a local owner: Unix-domain stream sockets at a
//! path.
//!
//! Each call has the meaning of the [`Driver`] call of the same name, and the stream rules of TCP
//! apply: a direction runs one operation at a time, a dropped receive leaves its bytes to the next
//! receive, and a dropped send runs on.

use std::io;
use std::path::Path;

use super::{Home, Homed, ReadHalf, WriteHalf, call, rewrap, submit};
use crate::buffer::{Backing, Lease};
use crate::driver::{self, Closed, Completion, Driver, Operation, Peer, Refused, Regions, Stale};

/// A listening IPC socket at a path, whose home is the driver of the owner of this thread.
///
/// An accept borrows the listener, so one accept runs at a time. A drop of the listener closes it,
/// as [`IpcListener::close`] does, with no future, and the socket file stays at its path.
#[derive(Debug)]
pub struct IpcListener {
    inner: Homed<driver::IpcListener>,
}

impl IpcListener {
    /// Binds an IPC listener at `path`, as [`Driver::bind_ipc`] does.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, or when no driver runs on this
    /// thread. The future gives an error of the system.
    pub fn bind(path: &Path, stale: Stale) -> Result<Operation<'static, io::Result<Self>>, Refused<()>> {
        let bind = submit((), |driver, ()| driver.bind_ipc(path, stale))?;
        Ok(bind.with_output(|result, held, bind| {
            driver::ipc_bound(result, held).map(|bound| {
                bound.map(|listener| {
                    let cleanup = bind.cleanup(listener.resource());
                    Self {
                        inner: Homed::new(listener, cleanup),
                    }
                })
            })
        }))
    }

    /// Accepts a connection, as [`Driver::accept_ipc`] does. The future gives the stream, whose
    /// home is the driver of the listener.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, when no driver runs on this
    /// thread, or with `Reason::Resource` when the listener has another home.
    pub fn accept(&mut self) -> Result<Operation<'_, io::Result<IpcStream>>, Refused<()>> {
        let listener = &mut self.inner.resource;
        let accept = submit((), |driver, ()| driver.accept_ipc(listener))?;
        Ok(accept.with_output(|result, held, accept| {
            driver::ipc_connected(result, held).map(|accepted| accepted.map(|stream| IpcStream::new(stream, accept)))
        }))
    }

    /// Closes the listener, as [`Driver::close_ipc_listener`] does.
    ///
    /// # Errors
    ///
    /// A refusal, with the listener, when the driver has no room for the operation or when no
    /// driver runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        self.inner
            .close(Driver::close_ipc_listener)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
    }
}

/// A connected IPC stream whose home is the driver of the owner of this thread.
///
/// A drop of the stream closes it, as [`IpcStream::close`] does, with no future.
#[derive(Debug)]
pub struct IpcStream {
    inner: Homed<driver::IpcStream>,
}

impl IpcStream {
    /// The stream that `operation` made.
    fn new(stream: driver::IpcStream, operation: &Completion) -> Self {
        let cleanup = operation.cleanup(stream.resource());
        Self {
            inner: Homed::new(stream, cleanup),
        }
    }

    /// Connects to the listener at `path`, as [`Driver::connect_ipc`] does.
    ///
    /// # Errors
    ///
    /// As for [`IpcListener::bind`].
    pub fn connect(path: &Path) -> Result<Operation<'static, io::Result<Self>>, Refused<()>> {
        let connect = submit((), |driver, ()| driver.connect_ipc(path))?;
        Ok(connect.with_output(|result, held, connect| {
            driver::ipc_connected(result, held).map(|connected| connected.map(|stream| Self::new(stream, connect)))
        }))
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

    /// The OS identity of the peer, as [`Driver::ipc_peer`] gives it.
    ///
    /// # Errors
    ///
    /// An error of the system, or of the kind `NotFound` when no driver of this thread owns the
    /// stream.
    pub fn peer(&self) -> io::Result<Peer> {
        call(|driver| driver.ipc_peer(&self.inner.resource))
    }

    /// Receives into the capacity of `buffer` after its initialized bytes, as
    /// [`Driver::recv`] does.
    ///
    /// # Errors
    ///
    /// A refusal, with the buffer, when the driver has no room for the operation, when no driver
    /// runs on this thread, or with `Reason::Resource` when the stream has another home.
    pub fn recv(&mut self, buffer: Backing) -> Result<Operation<'_, (io::Result<usize>, Backing)>, Refused<Backing>> {
        let stream = &mut self.inner.resource;
        submit(buffer, |driver, buffer| driver.recv(stream, buffer))
    }

    /// Sends the bytes of `lease`, as [`Driver::send`] does.
    ///
    /// # Errors
    ///
    /// As for [`IpcStream::recv`], with the lease.
    pub fn send(&mut self, lease: Lease) -> Result<Operation<'_, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let stream = &mut self.inner.resource;
        submit(lease, |driver, lease| driver.send(stream, lease))
    }

    /// Sends the bytes of `regions` in order, in one operation, as [`Driver::send_vectored`] does.
    ///
    /// # Errors
    ///
    /// As for [`IpcStream::recv`], with the regions.
    pub fn send_vectored(
        &mut self,
        regions: Regions,
    ) -> Result<Operation<'_, (io::Result<usize>, Regions)>, Refused<Regions>> {
        let stream = &mut self.inner.resource;
        submit(regions, |driver, regions| driver.send_vectored(stream, regions))
    }

    /// Ends the write direction after the sent bytes, as [`Driver::shutdown_write`] does.
    ///
    /// # Errors
    ///
    /// As for [`IpcStream::recv`].
    pub fn shutdown_write(&mut self) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let stream = &mut self.inner.resource;
        submit((), |driver, ()| driver.shutdown_write(stream))
    }

    /// Closes the stream, as [`Driver::close_ipc`] does: it cancels each operation of the stream,
    /// and the future ends after their terminal events.
    ///
    /// # Errors
    ///
    /// A refusal, with the stream, when the driver has no room for the operation or when no driver
    /// runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        self.inner
            .close(Driver::close_ipc)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
    }
}
