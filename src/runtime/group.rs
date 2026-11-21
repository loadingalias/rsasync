//! Groups: one TCP listener, or one UDP socket, on each local worker, at one address of a
//! reuse-port group.

use alloc::vec::Vec;
use core::fmt;
use core::future::{Future, pending};
use core::mem::DropGuard;
use core::net::SocketAddr;
use std::io;

use super::{Handle, WorkerId};
use crate::channel::{self, oneshot};
use crate::driver::{self, Operation, TcpOptions, UdpOptions};
use crate::local::{TcpListener, UdpSocket};
use crate::task::{self, CancelHandle, JoinHandle};

/// The members of a group, one for each local worker. [`Group::local_addr`] is the address of
/// each member.
#[derive(Debug)]
pub struct Group<T> {
    local_addr: SocketAddr,
    tasks: Vec<JoinHandle<T>>,
}

impl<T> Group<T> {
    /// The address of each member: with port 0, the port that the bind of worker 0 got.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The task of each member, in the order of its worker.
    #[must_use]
    pub fn into_tasks(self) -> Vec<JoinHandle<T>> {
        self.tasks
    }
}

/// Why a group did not start. Each member that bound before closes.
#[derive(Debug)]
#[non_exhaustive]
pub enum GroupError {
    /// The budget of the runtime did not hold the channels that start a member.
    Channel(channel::Refused),
    /// The worker did not take the task of its member, for example with no free slot.
    Spawn(WorkerId, task::Reason),
    /// The driver of the worker refused the bind.
    Refused(WorkerId, driver::Reason),
    /// The bind on the worker failed, for example because another socket has the address.
    Bind(WorkerId, io::Error),
    /// The task of the member ended before its bind: its worker stopped.
    Ended(WorkerId),
}

impl fmt::Display for GroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Channel(refused) => write!(f, "the group got no channel: {refused}"),
            Self::Spawn(worker, reason) => write!(f, "worker {} took no member: {reason:?}", worker.index()),
            Self::Refused(worker, reason) => write!(
                f,
                "the driver of worker {} refused the bind: {reason:?}",
                worker.index()
            ),
            Self::Bind(worker, error) => write!(f, "the bind on worker {} failed: {error}", worker.index()),
            Self::Ended(worker) => write!(f, "the member on worker {} ended before its bind", worker.index()),
        }
    }
}

impl core::error::Error for GroupError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Channel(refused) => Some(refused),
            Self::Bind(_, error) => Some(error),
            Self::Spawn(..) | Self::Refused(..) | Self::Ended(_) => None,
        }
    }
}

/// A resource that a group binds on each worker.
trait Member: Sized + 'static {
    type Options: Copy + Send + 'static;

    /// `options` with the reuse-port option on.
    fn shared(options: Self::Options) -> Self::Options;

    fn bind(
        address: SocketAddr,
        options: Self::Options,
    ) -> Result<Operation<'static, io::Result<Self>>, driver::Refused<()>>;

    fn local_addr(&self) -> SocketAddr;
}

impl Member for TcpListener {
    type Options = TcpOptions;

    fn shared(options: TcpOptions) -> TcpOptions {
        TcpOptions {
            reuse_port: Some(true),
            ..options
        }
    }

    fn bind(
        address: SocketAddr,
        options: TcpOptions,
    ) -> Result<Operation<'static, io::Result<Self>>, driver::Refused<()>> {
        Self::bind(address, options)
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr()
    }
}

impl Member for UdpSocket {
    type Options = UdpOptions;

    fn shared(options: UdpOptions) -> UdpOptions {
        UdpOptions {
            reuse_port: Some(true),
            ..options
        }
    }

    fn bind(
        address: SocketAddr,
        options: UdpOptions,
    ) -> Result<Operation<'static, io::Result<Self>>, driver::Refused<()>> {
        Self::bind(address, options)
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr()
    }
}

impl Handle {
    /// Binds one TCP listener on each local worker, in one reuse-port group at `address`, and then
    /// runs the future that `serve` makes there with the index of the worker and its listener. The
    /// kernel spreads the connections between the listeners.
    ///
    /// The bind of worker 0 comes first, so a port 0 gives one port to each member. No member runs
    /// `serve` before each member bound. A drop of the future before its end closes each listener.
    ///
    /// # Errors
    ///
    /// [`GroupError`] with the worker and its cause. Each listener that bound before closes.
    pub async fn tcp_group<S, F>(
        &mut self,
        address: SocketAddr,
        options: TcpOptions,
        serve: S,
    ) -> Result<Group<F::Output>, GroupError>
    where
        S: Fn(WorkerId, TcpListener) -> F + Clone + Send + 'static,
        F: Future + 'static,
        F::Output: Send + 'static,
    {
        group(self, address, options, serve).await
    }

    /// Binds one UDP socket on each local worker, in one reuse-port group at `address`, as
    /// [`Handle::tcp_group`] binds listeners. The kernel spreads the datagrams between the sockets
    /// by their addresses.
    ///
    /// # Errors
    ///
    /// As for [`Handle::tcp_group`].
    pub async fn udp_group<S, F>(
        &mut self,
        address: SocketAddr,
        options: UdpOptions,
        serve: S,
    ) -> Result<Group<F::Output>, GroupError>
    where
        S: Fn(WorkerId, UdpSocket) -> F + Clone + Send + 'static,
        F: Future + 'static,
        F::Output: Send + 'static,
    {
        group(self, address, options, serve).await
    }
}

/// Binds a member of `M` on each worker of `handle`, one at a time, and then starts each one.
async fn group<M, S, F>(
    handle: &mut Handle,
    mut address: SocketAddr,
    options: M::Options,
    serve: S,
) -> Result<Group<F::Output>, GroupError>
where
    M: Member,
    S: Fn(WorkerId, M) -> F + Clone + Send + 'static,
    F: Future + 'static,
    F::Output: Send + 'static,
{
    let options = M::shared(options);
    let workers = handle.workers();
    let Some(budget) = handle.routes.first().map(|route| route.placer.budget().clone()) else {
        return Err(GroupError::Spawn(WorkerId::new(0), task::Reason::NoWorker));
    };
    // Until the start, a failure or a drop of this future cancels each member, and a member closes
    // its socket at its drop.
    let mut started = DropGuard::new(
        Vec::with_capacity(workers as usize),
        |members: Vec<(CancelHandle, _)>| {
            for (cancel, _start) in members {
                cancel.cancel();
            }
        },
    );
    let mut tasks = Vec::with_capacity(workers as usize);
    for index in 0..workers {
        let worker = WorkerId::new(index);
        let (bound, mut binding) = oneshot::channel(&budget).map_err(GroupError::Channel)?;
        let (start, waiting) = oneshot::channel::<()>(&budget).map_err(GroupError::Channel)?;
        let serve = serve.clone();
        let task = handle
            .spawn_on(worker, move || {
                member::<M, S, F>(worker, address, options, serve, bound, waiting)
            })
            .map_err(|refused| GroupError::Spawn(worker, refused.reason()))?;
        started.push((task.cancel_handle(), start));
        tasks.push(task);
        match binding.recv().await {
            Ok(Ok(local)) => address = local,
            Ok(Err(cause)) => return Err(cause.at(worker)),
            Err(channel::Closed) => return Err(GroupError::Ended(worker)),
        }
    }
    for (_cancel, start) in DropGuard::dismiss(started) {
        // A member whose worker stopped since its bind ends with `Cancelled`, so a failed send needs
        // nothing more.
        let _sent = start.send(());
    }
    Ok(Group {
        local_addr: address,
        tasks,
    })
}

/// Why the bind of a member failed.
enum Cause {
    Refused(driver::Reason),
    Bind(io::Error),
    Ended,
}

impl Cause {
    fn at(self, worker: WorkerId) -> GroupError {
        match self {
            Self::Refused(reason) => GroupError::Refused(worker, reason),
            Self::Bind(error) => GroupError::Bind(worker, error),
            Self::Ended => GroupError::Ended(worker),
        }
    }
}

/// The task of the member on `worker`: it binds, gives its local address to the group, waits for
/// the start, and then runs `serve`. A member that the group does not start waits until its
/// cancel drops it.
async fn member<M, S, F>(
    worker: WorkerId,
    address: SocketAddr,
    options: M::Options,
    serve: S,
    bound: oneshot::Sender<Result<SocketAddr, Cause>>,
    mut waiting: oneshot::Receiver<()>,
) -> F::Output
where
    M: Member,
    S: Fn(WorkerId, M) -> F,
    F: Future,
{
    let made = match M::bind(address, options) {
        Ok(bind) => match bind.await {
            Ok(Ok(member)) => Ok(member),
            Ok(Err(error)) => Err(Cause::Bind(error)),
            Err(driver::Ended) => Err(Cause::Ended),
        },
        Err(refused) => Err(Cause::Refused(refused.reason())),
    };
    match made {
        Ok(member) => {
            if bound.send(Ok(member.local_addr())).is_ok() && waiting.recv().await.is_ok() {
                return serve(worker, member).await;
            }
        }
        Err(cause) => drop(bound.send(Err(cause))),
    }
    pending().await
}
