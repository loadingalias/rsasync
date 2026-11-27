//! The echo server: local workers with `Placement::Dedicated`, one reuse-port listener on each
//! worker, and one task for each connection, which sends back each received byte. Each receive
//! takes a buffer from the pool of its worker, and the send of its bytes gives the buffer back.
//!
//! `--files registered` uses the file table of the ring. `--receives single|multi` receives into the
//! provided buffers of the worker (`recv_selected`), with one submission for each receive or with
//! one multishot receive for each stream. The default, `--receives pool`, is the receive into a
//! buffer of the task.
//!
//! With `--movable N`, the runtime also has N movable workers, and `--bursts` gives them work in
//! bursts of 100 ms, with 100 ms between two bursts: tasks that use the CPU, tasks that send
//! 128 KiB blocks over loopback TCP to other movable tasks, or both. The echo path stays on the
//! local workers, so a client measures what the bursts cost it.

use core::cell::RefCell;
use core::future::poll_fn;
use core::net::{Ipv4Addr, SocketAddr};
use core::num::{NonZeroU32, NonZeroU64};
use core::task::Poll;
use core::time::Duration;
use std::io::Write as _;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Instant;

use rsasync::buffer::{Backing, Buffer as _, Lease, Pool};
use rsasync::capacity::Budget;
use rsasync::driver::{Config, Files, Shot, TcpOptions};
use rsasync::local::{ReadHalf, TcpListener, TcpStream};
use rsasync::movable;
use rsasync::runtime::{Handle, Placement, ReceiveBuffers, Runtime, Selection, WorkerId};
use rsasync::task::{self, Outcome};

use crate::client::send_all;
use crate::value;

/// The bytes of each receive buffer.
const BUFFER: usize = 16 << 10;
/// The connections of each worker: 10,000 connections on 4 workers, with room.
const CONNECTIONS: u32 = 4096;
/// The time of a burst of the movable work, and of the pause after it.
const BURST: Duration = Duration::from_millis(100);
/// The streams of one I/O burst, and the bytes of each of their sends.
const FLOODS: u32 = 32;
const FLOOD_BYTES: usize = 128 << 10;

/// The provided buffers of each worker for `recv_selected`: 4096 slots of 4 KiB.
const PROVIDED: u32 = 4096;
const PROVIDED_BYTES: usize = 4096;

/// How the echo task receives.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Receive {
    /// `recv` into a buffer that the task takes from its pool.
    Pool,
    /// `recv_selected` into a provided buffer, with the form of the submissions.
    Selected(Shot),
}

/// The work that the movable workers get in bursts.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Bursts {
    None,
    /// One task for each movable worker, which uses the CPU and yields.
    Cpu,
    /// `FLOODS` tasks, which each send blocks of `FLOOD_BYTES` over loopback TCP to a movable task.
    Io,
    Both,
}

/// The settings of a server.
pub(crate) struct Plan {
    workers: u32,
    port: u16,
    /// The completions that each turn of a driver takes (`Config::completions_per_turn`).
    completions: u32,
    /// The movable workers, which run only the bursts.
    movable: u32,
    bursts: Bursts,
    files: Files,
    receive: Receive,
}

impl Plan {
    /// The plan of `--workers`, `--port`, and `--completions`, of `--files plain|registered` and
    /// `--receives pool|single|multi` (plain and pool when absent), and of `--movable N --bursts
    /// none|cpu|io|both` for a server with movable workers (none when absent).
    pub(crate) fn parse(arguments: &[String]) -> Result<Self, String> {
        let given = |name: &str| arguments.iter().any(|argument| argument == name);
        let files = if given("--files") {
            match value::<String>(arguments, "files")?.as_str() {
                "plain" => Files::Plain,
                "registered" => Files::Registered,
                other => return Err(format!("--files: {other:?} is not plain or registered")),
            }
        } else {
            Files::Plain
        };
        let receive = if given("--receives") {
            match value::<String>(arguments, "receives")?.as_str() {
                "pool" => Receive::Pool,
                "single" => Receive::Selected(Shot::Single),
                "multi" => Receive::Selected(Shot::Multi),
                other => return Err(format!("--receives: {other:?} is not pool, single, or multi")),
            }
        } else {
            Receive::Pool
        };
        let movable = if given("--movable") {
            value(arguments, "movable")?
        } else {
            0
        };
        let bursts = if given("--bursts") {
            match value::<String>(arguments, "bursts")?.as_str() {
                "none" => Bursts::None,
                "cpu" => Bursts::Cpu,
                "io" => Bursts::Io,
                "both" => Bursts::Both,
                other => return Err(format!("--bursts: {other:?} is not none, cpu, io, or both")),
            }
        } else {
            Bursts::None
        };
        if bursts != Bursts::None && movable == 0 {
            return Err("--bursts: the bursts need --movable with at least one worker".to_owned());
        }
        Ok(Self {
            workers: value(arguments, "workers")?,
            port: value(arguments, "port")?,
            completions: value(arguments, "completions")?,
            movable,
            bursts,
            files,
            receive,
        })
    }
}

/// Runs the server until the process ends. It prints the backend and the placement of each worker.
pub(crate) fn run(plan: &Plan) -> Result<(), String> {
    let slots = CONNECTIONS + 16;
    let shot = match plan.receive {
        Receive::Pool => Shot::Single,
        Receive::Selected(shot) => shot,
    };
    let provided = core::alloc::Layout::from_size_align(PROVIDED_BYTES, 64).map_err(|error| error.to_string())?;
    let mut builder = Runtime::builder()
        .local_workers(plan.workers)
        .movable_workers(plan.movable)
        .placement(Placement::Dedicated, Selection::Permissive)
        .task_slots(slots)
        .driver(
            Config::new()
                .resources(slots)
                .operations(2 * slots)
                .submissions(1024)
                .completions_per_turn(plan.completions)
                .files(plan.files)
                .receives(shot),
        );
    if plan.receive != Receive::Pool {
        let slots = NonZeroU32::new(PROVIDED).ok_or("slots")?;
        builder = builder.receive_buffers(Some(ReceiveBuffers::new(slots, provided)));
    }
    let runtime = builder.build().map_err(|failure| format!("start: {failure:?}"))?;
    let mut output = std::io::stdout().lock();
    for (index, worker) in runtime.report().workers().iter().enumerate() {
        writeln!(
            output,
            "worker {index}: {:?}, {:?} on {:?}, files {:?}, receives {:?}",
            worker.mode(),
            worker.placement().applied(),
            worker.driver().backend(),
            worker.driver().files(),
            worker.driver().receives()
        )
        .map_err(|error| format!("output: {error}"))?;
    }
    let mut handle = runtime.handle().map_err(|error| format!("handle: {error:?}"))?;
    let address = SocketAddr::from((Ipv4Addr::UNSPECIFIED, plan.port));
    let options = TcpOptions {
        no_delay: Some(true),
        ..TcpOptions::default()
    };
    let selected = plan.receive != Receive::Pool;
    let group = runtime
        .block_on(async move {
            handle
                .tcp_group(address, options, move |worker, listener| {
                    serve(worker, listener, selected)
                })
                .await
        })
        .map_err(|error| format!("block_on: {error:?}"))?
        .map_err(|error| format!("group: {error:?}"))?;
    writeln!(output, "listening on {}", group.local_addr()).map_err(|error| format!("output: {error}"))?;
    drop(output);
    if plan.bursts != Bursts::None {
        return burst(&runtime.handle().map_err(|error| format!("handle: {error:?}"))?, plan);
    }
    for member in group.into_tasks() {
        if let Outcome::Completed(Err(error)) = member.join_blocking() {
            return Err(error);
        }
    }
    Ok(())
}

/// Accepts each connection of `listener`, and spawns its echo task on this worker. With `selected`,
/// the task receives into the provided buffers of the worker.
async fn serve(_worker: WorkerId, mut listener: TcpListener, selected: bool) -> Result<(), String> {
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
        if selected {
            drop(task::spawn_local(echo_selected(stream)));
        } else {
            drop(task::spawn_local(echo(stream, Rc::clone(&pool))));
        }
    }
}

/// Sends back each byte that `stream` receives into a provided buffer, until the end of its stream.
/// The send of each response runs while the task waits for the next request, and the task takes
/// its result after that request: a send that ended by then gives it with no wake of the task, and
/// a peer that reads slowly holds the echo back by one response.
async fn echo_selected(mut stream: TcpStream) {
    let (mut read, mut write) = stream.split();
    let mut request = receive_selected(&mut read).await;
    while let Some(buffer) = request {
        let Ok(send) = write.send_all(Lease::from(buffer)) else {
            break;
        };
        request = receive_selected(&mut read).await;
        if !matches!(send.await, Ok((Ok(_), _))) {
            break;
        }
    }
    if let Ok(close) = stream.close() {
        let _closed = close.await;
    }
}

/// The next request of `read`, in a provided buffer, or `None` at the end of the stream or at an
/// error.
async fn receive_selected(read: &mut ReadHalf<'_>) -> Option<Backing> {
    loop {
        let receive = read.recv_selected().ok()?;
        match receive.await {
            Ok(Ok(buffer)) if buffer.bytes().is_empty() => return None,
            Ok(Ok(buffer)) => return Some(buffer),
            // No provided buffer is free: the bytes stay in the socket, and a buffer comes back
            // after a send of another task.
            Ok(Err(error)) if error.raw_os_error() == Some(ENOBUFS) => yield_once().await,
            _ => return None,
        }
    }
}

/// The error number of a receive with no free provided buffer, on Linux.
const ENOBUFS: i32 = 105;

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

/// Gives the movable workers the bursts of `plan` until the process ends: this thread starts the
/// tasks of each burst, which end by themselves, and sleeps to the next burst. A refused spawn
/// leaves that task out of its burst.
fn burst(handle: &Handle, plan: &Plan) -> Result<(), String> {
    let sink = if plan.bursts == Bursts::Cpu {
        None
    } else {
        let (addressed, address) = mpsc::channel();
        drop(
            handle
                .spawn_movable(sink(addressed))
                .map_err(|refused| format!("spawn: {:?}", refused.reason()))?,
        );
        Some(address.recv().map_err(|_| "the sink did not start")??)
    };
    loop {
        let until = Instant::now() + BURST;
        if plan.bursts != Bursts::Io {
            for _ in 0..plan.movable {
                drop(handle.spawn_movable(spin(until)));
            }
        }
        if let Some(address) = sink {
            for _ in 0..FLOODS {
                drop(handle.spawn_movable(flood(address, until)));
            }
        }
        std::thread::sleep(2 * BURST);
    }
}

/// Uses the CPU until `until`, and yields to the pool after each 10,000 numbers.
async fn spin(until: Instant) -> u64 {
    let mut total = 0_u64;
    while Instant::now() < until {
        for number in 0..10_000_u64 {
            total = total.wrapping_add(core::hint::black_box(number).wrapping_mul(number));
        }
        yield_once().await;
    }
    total
}

/// Returns `Pending` one time, after a wake, so the worker polls other tasks first.
async fn yield_once() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// Accepts each stream of a flood on a loopback listener, whose address it gives first, and
/// starts a movable task that receives the bytes of the stream.
async fn sink(addressed: mpsc::Sender<Result<SocketAddr, String>>) {
    let bound = async {
        movable::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), TcpOptions::default())
            .map_err(|refused| format!("bind: {:?}", refused.reason()))?
            .await
            .map_err(|_| "the bind ended".to_owned())?
            .map_err(|error| format!("bind: {error}"))
    };
    let mut listener = match bound.await {
        Ok(listener) => listener,
        Err(error) => {
            drop(addressed.send(Err(error)));
            return;
        }
    };
    if addressed.send(Ok(listener.local_addr())).is_err() {
        return;
    }
    loop {
        let Ok(accept) = listener.accept() else { return };
        let Ok(Ok((stream, _peer))) = accept.await else { return };
        // A refused spawn gives the future back, and its drop closes the stream.
        drop(task::spawn_movable(drain(stream)));
    }
}

/// Receives each byte of `stream` until its end, and closes it.
async fn drain(mut stream: movable::TcpStream) {
    let mut buffer = Backing::from(Vec::with_capacity(FLOOD_BYTES));
    loop {
        let Ok(receive) = stream.recv(buffer) else { break };
        let Ok((Ok(count), mut received)) = receive.await else {
            break;
        };
        if count == 0 {
            break;
        }
        rsasync::buffer::BufferMut::truncate(&mut received, 0);
        buffer = received;
    }
    if let Ok(close) = stream.close() {
        let _closed = close.await;
    }
}

/// Connects to the sink at `address`, sends blocks of `FLOOD_BYTES` until `until`, and closes.
async fn flood(address: SocketAddr, until: Instant) {
    let Ok(connect) = movable::TcpStream::connect(address, TcpOptions::default()) else {
        return;
    };
    let Ok(Ok(mut stream)) = connect.await else { return };
    let mut lease = Lease::from(vec![0x5a_u8; FLOOD_BYTES]);
    while Instant::now() < until {
        let Ok(send) = stream.send(lease) else { break };
        match send.await {
            Ok((Ok(_), sent)) => lease = sent,
            _ => break,
        }
    }
    if let Ok(close) = stream.close() {
        let _closed = close.await;
    }
}
