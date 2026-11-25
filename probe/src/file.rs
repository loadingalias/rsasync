//! `probe file`: random reads or durable writes of one block at queue depth 1 on a direct-I/O
//! file, from a loop that owns its driver, as `driver-only.md` shows. It prints one JSON line, with
//! the turns, waits, and voluntary context switches of all of the operations.

use core::alloc::Layout;
use core::future::Future;
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::io::Write as _;
use std::path::PathBuf;
use std::time::Instant;

use rsasync::buffer::{Lease, Pool, WriteLoan};
use rsasync::capacity::Budget;
use rsasync::driver::{Backend, Cache, Config, Driver, OpenOptions, WriteMode};

use crate::histogram::Histogram;
use crate::value;

/// The operation of each request.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Read,
    /// A write with `WriteMode::DataSync`.
    Write,
}

/// How the loop drives the driver until an operation ends.
#[derive(Clone, Copy)]
enum Drive {
    /// Poll, turn, poll again, and wait when the turn has no more work: the loop of
    /// `driver-only.md`.
    Turn,
    /// Poll and turn, and never wait.
    Spin,
}

/// The arguments of one run.
pub(crate) struct Plan {
    path: PathBuf,
    kind: Kind,
    block: usize,
    /// The bytes of the file that the offsets cover.
    size: u64,
    count: u64,
    seed: u64,
    drive: Drive,
}

impl Plan {
    /// Parses `--path P --op read|write --block BYTES --size BYTES --count N --seed N
    /// --drive turn|spin`.
    pub(crate) fn parse(arguments: &[String]) -> Result<Self, String> {
        let kind = match value::<String>(arguments, "op")?.as_str() {
            "read" => Kind::Read,
            "write" => Kind::Write,
            other => return Err(format!("--op: {other:?} is not read or write")),
        };
        let drive = match value::<String>(arguments, "drive")?.as_str() {
            "turn" => Drive::Turn,
            "spin" => Drive::Spin,
            other => return Err(format!("--drive: {other:?} is not turn or spin")),
        };
        Ok(Self {
            path: value(arguments, "path")?,
            kind,
            block: value(arguments, "block")?,
            size: value(arguments, "size")?,
            count: value(arguments, "count")?,
            seed: value(arguments, "seed")?,
            drive,
        })
    }
}

/// The work of the loop for the operations of a run.
#[derive(Default)]
struct Counts {
    turns: u64,
    waits: u64,
}

/// Polls `future` and drives `driver` in the form of `drive`, until the future gives its output.
fn run_to_end<F: Future>(
    driver: &mut Driver,
    future: F,
    drive: Drive,
    counts: &mut Counts,
) -> Result<F::Output, String> {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return Ok(output);
        }
        counts.turns += 1;
        let more = driver.turn().map_err(|error| format!("turn: {error}"))?.more();
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return Ok(output);
        }
        if !more && matches!(drive, Drive::Turn) {
            counts.waits += 1;
            driver.wait().map_err(|error| format!("wait: {error}"))?;
        }
    }
}

/// The voluntary context switches of this thread.
fn switches() -> Result<u64, String> {
    let status = std::fs::read_to_string("/proc/thread-self/status").map_err(|error| format!("status: {error}"))?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("voluntary_ctxt_switches:"))
        .and_then(|count| count.trim().parse().ok())
        .ok_or_else(|| "status: no voluntary_ctxt_switches".to_owned())
}

/// Runs `plan`, and prints its JSON line.
pub(crate) fn run(plan: &Plan) -> Result<(), String> {
    let mut driver =
        Driver::new(Config::new().backend(Backend::IoUring)).map_err(|error| format!("driver: {error}"))?;
    let options = OpenOptions::new().write(plan.kind == Kind::Write).cache(Cache::Direct);
    let open = driver
        .open_file(&plan.path, options)
        .map_err(|refused| format!("open: {refused}"))?;
    let file = run_to_end(&mut driver, open, Drive::Turn, &mut Counts::default())?
        .map_err(|ended| format!("open: {ended}"))?
        .map_err(|error| format!("open: {error}"))?;
    let alignment = file.alignment().ok_or("the file has no direct-I/O alignment")?;
    let align = (alignment.memory().max(alignment.offset()) as usize).max(4096);
    let slot = Layout::from_size_align(plan.block, align).map_err(|error| format!("slot: {error}"))?;
    let blocks = plan.size / plan.block as u64;
    if blocks == 0 || !plan.block.is_multiple_of(align) {
        return Err(format!(
            "--block {} needs a multiple of {align} below --size",
            plan.block
        ));
    }
    let budget = Budget::new(NonZeroU64::new(4 * plan.block as u64 + (1 << 20)).ok_or("budget")?);
    let mut pool = Pool::new(&budget, NonZeroU32::MIN, slot).map_err(|error| format!("pool: {error}"))?;
    let pattern = vec![0x5a; plan.block];

    let mut latency = Histogram::default();
    // xorshift64: a seed of 0 would stay 0.
    let mut state = plan.seed.max(1);
    let mut counts = Counts::default();
    let switches_before = switches()?;
    for _ in 0..plan.count {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let offset = state % blocks * plan.block as u64;
        let buffer = pool.acquire().map_err(|error| format!("acquire: {error}"))?;
        let count = match plan.kind {
            Kind::Read => {
                let start = Instant::now();
                let read = driver
                    .read_at(&file, buffer, offset)
                    .map_err(|refused| format!("read: {refused}"))?;
                let (count, _buffer) = run_to_end(&mut driver, read, plan.drive, &mut counts)?
                    .map_err(|ended| format!("read: {ended}"))?;
                latency.record(nanos(start));
                count
            }
            Kind::Write => {
                let mut loan = WriteLoan::new(buffer);
                loan.fill_from(&pattern);
                let lease = Lease::from(loan.into_inner());
                let start = Instant::now();
                let write = driver
                    .write_at(&file, lease, offset, WriteMode::DataSync)
                    .map_err(|refused| format!("write: {refused}"))?;
                let (count, _lease) = run_to_end(&mut driver, write, plan.drive, &mut counts)?
                    .map_err(|ended| format!("write: {ended}"))?;
                latency.record(nanos(start));
                count
            }
        };
        let count = count.map_err(|error| format!("at {offset}: {error}"))?;
        if count != plan.block {
            return Err(format!("at {offset}: {count} of {} bytes", plan.block));
        }
    }
    let switches = switches()? - switches_before;
    writeln!(
        std::io::stdout().lock(),
        "{{\"op\":\"{}\",\"block\":{},\"drive\":\"{}\",\"count\":{},\"latency_ns\":{{\"p50\":{},\"p99\":{},\
         \"p999\":{},\"max\":{}}},\"turns\":{},\"waits\":{},\"switches\":{switches}}}",
        match plan.kind {
            Kind::Read => "read",
            Kind::Write => "write",
        },
        plan.block,
        match plan.drive {
            Drive::Turn => "turn",
            Drive::Spin => "spin",
        },
        latency.total(),
        latency.percentile(1, 2),
        latency.percentile(99, 100),
        latency.percentile(999, 1000),
        latency.max(),
        counts.turns,
        counts.waits,
    )
    .map_err(|error| format!("output: {error}"))?;
    let close = driver.close_file(file).map_err(|refused| format!("close: {refused}"))?;
    run_to_end(&mut driver, close, Drive::Turn, &mut Counts::default())?.map_err(|ended| format!("close: {ended}"))
}

/// The nanoseconds since `start`.
fn nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
