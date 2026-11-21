//! The I/O types of a local owner: TCP and UDP resources that a task uses with no driver argument.
//!
//! A worker of a [`Runtime`](crate::runtime::Runtime), and the owner of
//! [`Runtime::block_on`](crate::runtime::Runtime::block_on), each have a driver. A call of this
//! module submits its operation to the driver of the owner of its thread, and that driver is the
//! home of the resource that the call makes. The types are not `Send`, so a resource stays on the
//! thread of its home. The future of a call that makes a resource is not `Send` either, so the
//! resource starts there. To use a resource on each worker, make it there: see
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
//! Each call has the meaning of its [`driver`](crate::driver) call, and gives the same [`Operation`]
//! future:
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

use core::marker::PhantomData;
use core::mem::DropGuard;
use std::io;

use crate::driver::{Cleanup, Closed, Driver, Reason, Refused};
use crate::task::with_driver;

pub mod tcp;
pub mod udp;

pub use crate::driver::Operation;
pub use tcp::{TcpListener, TcpStream};
pub use udp::UdpSocket;

/// The marker of a type that stays on the thread of its home: it is neither `Send` nor `Sync`.
type Home = PhantomData<*const ()>;

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
    fn close(
        self,
        close: impl FnOnce(&mut Driver, R) -> Result<Operation<'static, Closed>, Refused<R>>,
    ) -> Result<Operation<'static, Closed>, Refused<Self>> {
        let Self {
            resource,
            cleanup,
            _home,
        } = self;
        match submit(resource, close) {
            Ok(close) => {
                drop(DropGuard::dismiss(cleanup));
                Ok(close)
            }
            Err(refused) => Err(rewrap(refused, |resource| Self {
                resource,
                cleanup,
                _home,
            })),
        }
    }
}

/// Calls `f` with the driver of the owner of this thread and `value`. With no driver, it refuses
/// with [`Reason::NoDriver`] and gives `value` back.
fn submit<V, T>(value: V, f: impl FnOnce(&mut Driver, V) -> Result<T, Refused<V>>) -> Result<T, Refused<V>> {
    with_driver(value, f).unwrap_or_else(|value| Err(Refused::new(Reason::NoDriver, value)))
}

/// Calls `f` with the driver of the owner of this thread. With no driver, it gives an error of the
/// kind `NotFound`, as the driver does for a socket of another home: both have one recovery, and
/// the error allocates nothing.
fn call<T>(f: impl FnOnce(&mut Driver) -> io::Result<T>) -> io::Result<T> {
    with_driver((), |driver, ()| f(driver)).unwrap_or_else(|()| Err(io::ErrorKind::NotFound.into()))
}

/// Gives `refused` with `wrap` applied to its value.
fn rewrap<T, U>(refused: Refused<T>, wrap: impl FnOnce(T) -> U) -> Refused<U> {
    let reason = refused.reason();
    Refused::new(reason, wrap(refused.into_inner()))
}

// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
mod tests;
