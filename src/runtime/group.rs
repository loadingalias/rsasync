//! Groups: one TCP listener, or one UDP socket, on each local worker, at one address of a
//! reuse-port group.

use alloc::vec::Vec;
use core::alloc::Layout;
use core::fmt;
use core::future::{Future, pending};
use core::mem::DropGuard;
use core::net::SocketAddr;
use std::io;

use super::{Handle, WorkerId};
use crate::capacity::Exhausted;
use crate::channel::{self, oneshot};
use crate::driver::{self, TcpOptions, UdpOptions};
use crate::local::{Operation, TcpListener, UdpSocket};
use crate::memory::room;
use crate::task::{self, JoinHandle};

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

/// Why a group did not start. Each member that bound closed its socket before the error.
#[derive(Debug)]
#[non_exhaustive]
pub enum GroupError {
    /// The budget of the runtime did not hold the list of the members that the group starts.
    Budget(Exhausted),
    /// The allocator did not give the memory of the list of the members, or of their tasks.
    Allocation,
    /// The budget of the runtime did not hold the channels that start a member.
    Channel(channel::Refused),
    /// The worker did not take the task of its member, for example with no free slot.
    Spawn(WorkerId, task::Reason),
    /// The driver of the worker refused the bind.
    Refused(WorkerId, driver::Reason),
    /// The bind on the worker failed, for example because another socket has the address.
    Bind(WorkerId, io::Error),
    /// The task of the member ended before its start: its worker stopped.
    Ended(WorkerId),
}

impl fmt::Display for GroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget(exhausted) => write!(f, "the budget cannot charge the list of the members: {exhausted}"),
            Self::Allocation => f.write_str("the allocator did not give the lists of the group"),
            Self::Channel(refused) => write!(f, "the group got no channel: {refused}"),
            Self::Spawn(worker, reason) => write!(f, "worker {} took no member: {reason:?}", worker.index()),
            Self::Refused(worker, reason) => write!(
                f,
                "the driver of worker {} refused the bind: {reason:?}",
                worker.index()
            ),
            Self::Bind(worker, error) => write!(f, "the bind on worker {} failed: {error}", worker.index()),
            Self::Ended(worker) => write!(f, "the member on worker {} ended before its start", worker.index()),
        }
    }
}

impl core::error::Error for GroupError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Budget(exhausted) => Some(exhausted),
            Self::Channel(refused) => Some(refused),
            Self::Bind(_, error) => Some(error),
            Self::Allocation | Self::Spawn(..) | Self::Refused(..) | Self::Ended(_) => None,
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
    /// `serve` before each member bound and got its start. A drop of the future before its end
    /// cancels each member, and each one closes its listener on its worker soon after.
    ///
    /// # Errors
    ///
    /// [`GroupError`] with its cause, and the worker when one failed. Each listener that bound
    /// closed before the error, so its address is free again.
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
    // The start signals are state of the runtime, so the budget pays for their list. The list of
    // the tasks goes to the caller, so no charge pays for it, but a refusal of its memory still
    // refuses the group.
    let list = Layout::array::<oneshot::Sender<()>>(workers as usize)
        .ok()
        .and_then(|list| u64::try_from(list.size()).ok())
        .ok_or(GroupError::Allocation)?;
    let _charge = budget.reserve(list).map_err(GroupError::Budget)?;
    let mut starts = room(workers as usize).map_err(|_| GroupError::Allocation)?;
    // Until the start, a drop of this future cancels each member, and a member closes its socket at
    // its drop, on its worker.
    let mut tasks = DropGuard::new(
        room(workers as usize).map_err(|_| GroupError::Allocation)?,
        |tasks: Vec<JoinHandle<F::Output>>| {
            for task in &tasks {
                task.cancel_handle().cancel();
            }
        },
    );
    let bound = async {
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
            tasks.push(task);
            starts.push(start);
            match binding.recv().await {
                Ok(Ok(local)) => address = local,
                Ok(Err(cause)) => return Err(cause.at(worker)),
                Err(channel::Closed) => return Err(GroupError::Ended(worker)),
            }
        }
        Ok(())
    }
    .await;
    // A member whose task ended after its bind never serves, so the group fails then too.
    let failed = match bound {
        Ok(()) => (0..).zip(starts).find_map(|(index, start)| {
            start
                .send(())
                .err()
                .map(|_unsent| GroupError::Ended(WorkerId::new(index)))
        }),
        Err(error) => Some(error),
    };
    let tasks = DropGuard::dismiss(tasks);
    let Some(error) = failed else {
        return Ok(Group {
            local_addr: address,
            tasks,
        });
    };
    // A task ends after the drop of its member, which closes its socket. So when the error comes,
    // the address of each member is free again.
    for task in &tasks {
        task.cancel_handle().cancel();
    }
    for task in tasks {
        let _outcome = task.join().await;
    }
    Err(error)
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
