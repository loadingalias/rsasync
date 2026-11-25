//! The I/O types of a local owner: TCP, UDP, file, IPC stream, pipe, and child process resources,
//! and signal streams, that a task uses with no driver argument.
//!
//! A worker of a [`Runtime`](crate::runtime::Runtime), and the owner of
//! [`Runtime::block_on`](crate::runtime::Runtime::block_on), each have a driver. A call of this
//! module submits its operation to the driver of the owner of its thread, and that driver is the
//! home of the resource that the call makes. The types are not `Send`, so a resource stays on the
//! thread of its home. The future of each call is not `Send` either, so a resource starts there,
//! and each operation ends there. To use a resource on each worker, make it there: see
//! [`Handle::tcp_group`](crate::runtime::Handle::tcp_group) and
//! [`Handle::udp_group`](crate::runtime::Handle::udp_group).
//!
//! ```compile_fail,E0277
//! use rsasync::driver::TcpOptions;
//! use rsasync::local::TcpListener;
//!
//! fn to_other_thread<T: Send>(_: T) {}
//!
//! // The future of the bind cannot go to another thread.
//! to_other_thread(TcpListener::bind("127.0.0.1:0".parse().unwrap(), TcpOptions::default()));
//! ```
//!
//! Each call has the meaning of its [`driver`] call, and gives an [`Operation`] future with the
//! outputs of the driver. The future is not `Send`, so it ends on the thread of its home:
//!
//! - The call gives `Err(Refused)` before the wait, with what the caller gave, such as the buffer
//!   of a receive. [`Reason::NoDriver`] says that no driver runs on this thread for the call, and
//!   [`Reason::Resource`] that the resource has another home, for example after its `block_on`.
//! - The future gives the buffer back with each outcome. A drop of the future asks the driver to
//!   cancel a receive, and the driver keeps each buffer until the terminal event of its operation.
//! - `close` cancels each operation of the resource, and its future ends after their terminal
//!   events. A drop of the resource starts the same close, with no future and with no free
//!   operation slot: the driver closes the socket when its operations end. When the drop cannot
//!   use the driver at once, for example in a destructor that a turn of the driver runs, the next
//!   turn of the home starts the close.
//!
//! ```no_run
//! use rsasync::buffer::{Backing, Buffer as _, Lease};
//! use rsasync::driver::TcpOptions;
//! use rsasync::local::{TcpListener, TcpStream};
//! use rsasync::runtime::Runtime;
//!
//! let runtime = Runtime::builder().build()?;
//! runtime.block_on(async {
//!     let mut listener = TcpListener::bind("127.0.0.1:0".parse()?, TcpOptions::default())?.await??;
//!     let connect = TcpStream::connect(listener.local_addr(), TcpOptions::default())?;
//!     let (mut server, _peer) = listener.accept()?.await??;
//!     let mut client = connect.await??;
//!
//!     let (sent, _lease) = client.send(Lease::from(b"ping".to_vec()))?.await?;
//!     assert_eq!(sent?, 4);
//!     let (received, buffer) = server.recv(Backing::from(Vec::with_capacity(64)))?.await?;
//!     assert_eq!(&buffer.bytes()[..received?], b"ping");
//!
//!     server.close()?.await?;
//!     client.close()?.await?;
//!     Ok::<(), Box<dyn std::error::Error>>(())
//! })??;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::mem::DropGuard;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use crate::buffer::{Backing, Lease};
use crate::driver::{self, Cleanup, Driver, Reason, Refused, Regions};
use crate::task::with_driver;

pub mod file;
pub mod ipc;
pub mod pipe;
pub mod process;
pub mod tcp;
pub mod udp;

pub use file::{Directory, File, rename, unlink};
pub use ipc::{IpcListener, IpcStream};
pub use pipe::{PipeReader, PipeWriter};
pub use process::Child;
pub use tcp::{TcpListener, TcpStream};
pub use udp::UdpSocket;

/// The marker of a type that stays on the thread of its home: it is neither `Send` nor `Sync`.
type Home = PhantomData<*const ()>;

/// The future of an operation of a type of this module. It has the meaning of the
/// [`Operation`](driver::Operation) of the driver, and it stays on the thread of its home: it is
/// not `Send`, so its poll reaches the driver of its home, which takes the result in place and
/// wakes the task that polls it, with no atomic change.
///
/// A poll outside the owner of its home, or inside a turn of its driver, refuses the context: the
/// future gives [`Ended`](driver::Ended), and the drop of the future ends the interest.
///
/// ```compile_fail,E0277
/// use rsasync::buffer::Backing;
/// use rsasync::local::TcpStream;
///
/// fn to_other_thread<T: Send>(_: T) {}
///
/// fn receive(stream: &mut TcpStream) {
///     // The receive cannot go to another thread.
///     to_other_thread(stream.recv(Backing::from(Vec::new())));
/// }
/// ```
pub struct Operation<'a, T> {
    inner: driver::Operation<'a, T>,
    _home: Home,
}

impl<'a, T> Operation<'a, T> {
    /// The home operation of a call of this module.
    const fn home(inner: driver::Operation<'a, T>) -> Self {
        Self {
            inner,
            _home: PhantomData,
        }
    }

    /// The identity of the operation, as [`driver::Operation::id`] gives it.
    #[must_use]
    pub fn id(&self) -> driver::OperationId {
        self.inner.id()
    }

    /// Asks the driver to cancel the operation, as [`driver::Operation::cancel`] does.
    pub fn cancel(&self) {
        self.inner.cancel();
    }
}

impl<T> Future for Operation<'_, T> {
    type Output = Result<T, driver::Ended>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<T> fmt::Debug for Operation<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Operation").field(&self.inner).finish()
    }
}

/// A resource of a driver that a type of this module owns, with the home of its close.
///
/// A drop closes the resource with no future. The drop runs on the thread of the home. When the
/// driver of this thread is the home and is free, the close starts at once. Else the drop records
/// it at the home, whose next turn starts it: the driver is in a turn, or the thread runs another
/// owner, or the home ended.
#[derive(Debug)]
struct Homed<R> {
    resource: R,
    cleanup: DropGuard<Cleanup, fn(Cleanup)>,
    _home: Home,
}

impl<R> Homed<R> {
    fn new(resource: R, cleanup: Cleanup) -> Self {
        Self {
            resource,
            cleanup: DropGuard::new(cleanup, |cleanup| {
                if let Err(cleanup) | Ok(Err(cleanup)) = with_driver(cleanup, Driver::close_dropped) {
                    cleanup.post();
                }
            }),
            _home: Home::default(),
        }
    }

    /// Closes the resource with `close`, whose future gets the report. A refusal gives the resource
    /// back with its home, so its drop still closes it.
    fn close<T>(
        self,
        close: impl FnOnce(&mut Driver, R) -> Result<driver::Operation<'static, T>, Refused<R>>,
    ) -> Result<Operation<'static, T>, Refused<Self>> {
        let Self {
            resource,
            cleanup,
            _home,
        } = self;
        match submit(resource, close) {
            Ok(close) => {
                drop(DropGuard::dismiss(cleanup));
                Ok(Operation::home(close))
            }
            Err(refused) => Err(rewrap(refused, |resource| Self {
                resource,
                cleanup,
                _home,
            })),
        }
    }
}

/// The read half of a stream of this module, a [`TcpStream`] or an [`IpcStream`]: its receives
/// borrow it.
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
        submit(buffer, |driver, buffer| driver.recv(read, buffer)).map(Operation::home)
    }

    /// Receives into a buffer that the kernel selects from the provided buffers of the worker, as
    /// [`TcpStream::recv_selected`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::recv_selected`].
    pub fn recv_selected(&mut self) -> Result<Operation<'_, io::Result<Backing>>, Refused<()>> {
        let read = &mut *self.read;
        submit((), |driver, ()| driver.recv_selected(read)).map(Operation::home)
    }
}

/// The write half of a stream of this module, a [`TcpStream`] or an [`IpcStream`]: its sends
/// borrow it.
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
        submit(lease, |driver, lease| driver.send(write, lease)).map(Operation::home)
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
        submit(regions, |driver, regions| driver.send_vectored(write, regions)).map(Operation::home)
    }

    /// Ends the write direction after the sent bytes, as [`TcpStream::shutdown_write`] does.
    ///
    /// # Errors
    ///
    /// As for [`TcpStream::shutdown_write`].
    pub fn shutdown_write(&mut self) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let write = &mut *self.write;
        submit((), |driver, ()| driver.shutdown_write(write)).map(Operation::home)
    }
}

/// A stream of `signal`, whose home is the driver of the owner of this thread, as
/// [`Driver::signal`] gives it. The stream can move to another thread: a turn of its home wakes it.
///
/// # Errors
///
/// The [`SignalError`](driver::SignalError) of the driver, and `NoDriver` when no driver runs on
/// this thread.
pub fn signal(
    signal: driver::Signal,
    escalation: driver::Escalation,
) -> Result<driver::SignalStream, driver::SignalError> {
    with_driver((), |driver, ()| driver.signal(signal, escalation)).unwrap_or(Err(driver::SignalError::NoDriver))
}

/// Calls `f` with the driver of the owner of this thread and `value`. With no driver, it refuses
/// with [`Reason::NoDriver`] and gives `value` back.
/// Each operation that `f` starts has its consumer at the home: a type of this module.
pub(crate) fn submit<V, T>(value: V, f: impl FnOnce(&mut Driver, V) -> Result<T, Refused<V>>) -> Result<T, Refused<V>> {
    submit_any(value, |driver, value| driver.home_call(|driver| f(driver, value)))
}

/// [`submit`] for an operation whose consumer can move to another thread: a type of
/// [`movable`](crate::movable).
pub(crate) fn submit_any<V, T>(
    value: V,
    f: impl FnOnce(&mut Driver, V) -> Result<T, Refused<V>>,
) -> Result<T, Refused<V>> {
    with_driver(value, f).unwrap_or_else(|value| Err(Refused::new(Reason::NoDriver, value)))
}

/// Calls `f` with the driver of the owner of this thread. With no driver, it gives an error of the
/// kind `NotFound`, as the driver does for a socket of another home: both have one recovery, and
/// the error allocates nothing.
fn call<T>(f: impl FnOnce(&mut Driver) -> io::Result<T>) -> io::Result<T> {
    with_driver((), |driver, ()| f(driver)).unwrap_or_else(|()| Err(io::ErrorKind::NotFound.into()))
}

/// Gives `refused` with `wrap` applied to its value.
pub(crate) fn rewrap<T, U>(refused: Refused<T>, wrap: impl FnOnce(T) -> U) -> Refused<U> {
    let reason = refused.reason();
    Refused::new(reason, wrap(refused.into_inner()))
}

// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
mod tests;
