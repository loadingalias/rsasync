//! The echo server: local workers with `Placement::Dedicated`, one reuse-port listener on each
//! worker, and one task for each connection, which sends back each received byte. Each receive
//! takes a buffer from the pool of its worker, and the send of its bytes gives the buffer back.

use core::cell::RefCell;
use core::net::{Ipv4Addr, SocketAddr};
use core::num::{NonZeroU32, NonZeroU64};
use std::io::Write as _;
use std::rc::Rc;

use rsasync::buffer::{Backing, Lease, Pool};
use rsasync::capacity::Budget;
use rsasync::driver::{Config, TcpOptions};
use rsasync::local::{TcpListener, TcpStream};
use rsasync::runtime::{Placement, Runtime, Selection, WorkerId};
use rsasync::task::{self, Outcome};

use crate::client::send_all;
use crate::value;

/// The bytes of each receive buffer.
const BUFFER: usize = 16 << 10;
/// The connections of each worker: 10,000 connections on 4 workers, with room.
const CONNECTIONS: u32 = 4096;

/// The settings of a server.
pub(crate) struct Plan {
    workers: u32,
    port: u16,
    /// The completions that each turn of a driver takes (`Config::completions_per_turn`).
    completions: u32,
}

impl Plan {
    /// The plan of `--workers`, `--port`, and `--completions`.
    pub(crate) fn parse(arguments: &[String]) -> Result<Self, String> {
        Ok(Self {
            workers: value(arguments, "workers")?,
            port: value(arguments, "port")?,
            completions: value(arguments, "completions")?,
        })
    }
}

/// Runs the server until the process ends. It prints the backend and the placement of each worker.
pub(crate) fn run(plan: &Plan) -> Result<(), String> {
    let slots = CONNECTIONS + 16;
    let runtime = Runtime::builder()
        .local_workers(plan.workers)
        .placement(Placement::Dedicated, Selection::Permissive)
        .task_slots(slots)
        .driver(
            Config::new()
                .resources(slots)
                .operations(2 * slots)
                .submissions(1024)
                .completions_per_turn(plan.completions),
        )
        .build()
        .map_err(|failure| format!("start: {failure:?}"))?;
    let mut output = std::io::stdout().lock();
    for (index, worker) in runtime.report().workers().iter().enumerate() {
        writeln!(
            output,
            "worker {index}: {:?} on {:?}",
            worker.placement().applied(),
            worker.driver().backend()
        )
        .map_err(|error| format!("output: {error}"))?;
    }
    let mut handle = runtime.handle().map_err(|error| format!("handle: {error:?}"))?;
    let address = SocketAddr::from((Ipv4Addr::UNSPECIFIED, plan.port));
    let options = TcpOptions {
        no_delay: Some(true),
        ..TcpOptions::default()
    };
    let group = runtime
        .block_on(async move { handle.tcp_group(address, options, serve).await })
        .map_err(|error| format!("block_on: {error:?}"))?
        .map_err(|error| format!("group: {error:?}"))?;
    writeln!(output, "listening on {}", group.local_addr()).map_err(|error| format!("output: {error}"))?;
    drop(output);
    for member in group.into_tasks() {
        if let Outcome::Completed(Err(error)) = member.join_blocking() {
            return Err(error);
        }
    }
    Ok(())
}

/// Accepts each connection of `listener`, and spawns its echo task on this worker.
async fn serve(_worker: WorkerId, mut listener: TcpListener) -> Result<(), String> {
    let budget = Budget::new(NonZeroU64::new(256 << 20).ok_or("budget")?);
    let layout = core::alloc::Layout::from_size_align(BUFFER, 64).map_err(|error| error.to_string())?;
    let slots = NonZeroU32::new(CONNECTIONS).ok_or("slots")?;
    let pool = Rc::new(RefCell::new(
        Pool::new(&budget, slots, layout).map_err(|error| format!("pool: {error:?}"))?,
    ));
    loop {
        let accept = listener.accept().map_err(|error| format!("accept: {error:?}"))?;
        let (stream, _peer) = accept
            .await
            .map_err(|error| format!("accept: {error:?}"))?
            .map_err(|error| format!("accept: {error}"))?;
        // A refused spawn gives the future back, and its drop closes the stream.
        drop(task::spawn_local(echo(stream, Rc::clone(&pool))));
    }
}

/// Sends back each byte that `stream` receives, until the end of its stream.
async fn echo(mut stream: TcpStream, pool: Rc<RefCell<Pool>>) {
    let (mut read, mut write) = stream.split();
    loop {
        // A pool with no free buffer gives way to one allocation.
        let buffer = pool
            .borrow_mut()
            .acquire()
            .unwrap_or_else(|_| Backing::from(Vec::with_capacity(BUFFER)));
        let Ok(receive) = read.recv(buffer) else { break };
        let Ok((Ok(count), buffer)) = receive.await else { break };
        if count == 0 {
            break;
        }
        if send_all(&mut write, Lease::from(buffer)).await.is_err() {
            break;
        }
    }
    if let Ok(close) = stream.close() {
        let _closed = close.await;
    }
}
