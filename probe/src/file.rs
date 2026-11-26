//! `probe file`: random reads at a queue depth, or durable writes of one block at queue depth 1,
//! on a direct-I/O file, from a loop that owns its driver, as `driver-only.md` shows. It prints one
//! JSON line, with the turns, waits, and voluntary context switches of all of the operations, and
//! the CPU time of the process and the elapsed time of the run.

use core::alloc::Layout;
use core::future::Future;
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::{Pin, pin};
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::{Context, Poll, Waker};
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::sync::Arc;
use std::task::Wake;
use std::time::Instant;

use rsasync::buffer::{Lease, Pool, WriteLoan};
use rsasync::capacity::Budget;
use rsasync::driver::{Backend, Cache, Config, Driver, File, OpenOptions, WriteMode};

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
    /// The operations in flight at one time. Writes run at depth 1.
    depth: NonZeroU32,
}

impl Plan {
    /// Parses `--path P --op read|write --block BYTES --size BYTES --count N --seed N
    /// --drive turn|spin`, and `--depth N` for reads (1 when absent).
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
        let depth = if arguments.iter().any(|argument| argument == "--depth") {
            value(arguments, "depth")?
        } else {
            NonZeroU32::MIN
        };
        if kind == Kind::Write && depth != NonZeroU32::MIN {
            return Err("--depth: a write runs at depth 1".to_owned());
        }
        Ok(Self {
            path: value(arguments, "path")?,
            kind,
            block: value(arguments, "block")?,
            size: value(arguments, "size")?,
            count: value(arguments, "count")?,
            seed: value(arguments, "seed")?,
            drive,
            depth,
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

/// The CPU time of each thread of this process, user and system, in nanoseconds
/// (`/proc/self/stat`, in clock ticks of 10 ms on Linux).
fn cpu_ns() -> Result<u64, String> {
    let stat = std::fs::read_to_string("/proc/self/stat").map_err(|error| format!("stat: {error}"))?;
    // The fields after the name, which ends at the last `)`: utime and stime are 14 and 15.
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest.split_whitespace().collect())
        .unwrap_or_default();
    let ticks = |index: usize| fields.get(index).and_then(|field| field.parse::<u64>().ok());
    let (Some(user), Some(system)) = (ticks(11), ticks(12)) else {
        return Err("stat: no utime and stime".to_owned());
    };
    Ok((user + system) * 10_000_000)
}

/// The offsets of a run: a xorshift64 generator, at a block of the file.
struct Offsets {
    state: u64,
    blocks: u64,
    block: u64,
}

impl Offsets {
    fn next(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state % self.blocks * self.block
    }
}

/// Gives an error unless the operation at `offset` moved one whole block of `plan`.
fn whole(count: io::Result<usize>, offset: u64, plan: &Plan) -> Result<(), String> {
    let count = count.map_err(|error| format!("at {offset}: {error}"))?;
    if count == plan.block {
        Ok(())
    } else {
        Err(format!("at {offset}: {count} of {} bytes", plan.block))
    }
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
    let budget = Budget::new(
        NonZeroU64::new((u64::from(plan.depth.get()) + 4) * plan.block as u64 + (1 << 20)).ok_or("budget")?,
    );
    let mut pool = Pool::new(&budget, plan.depth, slot).map_err(|error| format!("pool: {error}"))?;
    // xorshift64: a seed of 0 would stay 0.
    let mut offsets = Offsets {
        state: plan.seed.max(1),
        blocks,
        block: plan.block as u64,
    };

    let mut latency = Histogram::default();
    let mut counts = Counts::default();
    let switches_before = switches()?;
    let (cpu_before, started) = (cpu_ns()?, Instant::now());
    match plan.kind {
        Kind::Read => read(
            &mut driver,
            &file,
            &mut pool,
            plan,
            &mut offsets,
            &mut latency,
            &mut counts,
        )?,
        Kind::Write => write(
            &mut driver,
            &file,
            &mut pool,
            plan,
            &mut offsets,
            &mut latency,
            &mut counts,
        )?,
    }
    let switches = switches()? - switches_before;
    let (cpu, elapsed) = (cpu_ns()? - cpu_before, nanos(started));
    writeln!(
        std::io::stdout().lock(),
        "{{\"op\":\"{}\",\"block\":{},\"drive\":\"{}\",\"depth\":{},\"count\":{},\"latency_ns\":{{\"p50\":{},\
         \"p99\":{},\"p999\":{},\"max\":{}}},\"turns\":{},\"waits\":{},\"switches\":{switches},\"cpu_ns\":{cpu},\
         \"elapsed_ns\":{elapsed}}}",
        match plan.kind {
            Kind::Read => "read",
            Kind::Write => "write",
        },
        plan.block,
        match plan.drive {
            Drive::Turn => "turn",
            Drive::Spin => "spin",
        },
        plan.depth,
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

/// The places whose read the driver woke: one bit for each place.
struct Woken(Box<[AtomicU64]>);

impl Woken {
    fn set(&self, place: usize) {
        // Ordering: `Relaxed`: the driver wakes on the thread of the loop.
        self.0[place / 64].fetch_or(1 << (place % 64), Ordering::Relaxed);
    }
}

/// The waker of the read in one place of the loop.
struct Place {
    woken: Arc<Woken>,
    index: usize,
}

impl Wake for Place {
    fn wake(self: Arc<Self>) {
        self.woken.set(self.index);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.set(self.index);
    }
}

/// Keeps `plan.depth` reads in flight on `file` until `plan.count` reads ended. Each place has its
/// own waker, so the loop polls a read only at its start and after its completion, as an executor
/// polls a task: the cost of a pass does not grow with the depth. A read that ends starts the next
/// one before the next turn submits.
fn read(
    driver: &mut Driver,
    file: &File,
    pool: &mut Pool,
    plan: &Plan,
    offsets: &mut Offsets,
    latency: &mut Histogram,
    counts: &mut Counts,
) -> Result<(), String> {
    let depth = plan.depth.get() as usize;
    let woken = Arc::new(Woken((0..depth.div_ceil(64)).map(|_| AtomicU64::new(0)).collect()));
    let wakers: Vec<Waker> = (0..depth)
        .map(|index| {
            Waker::from(Arc::new(Place {
                woken: Arc::clone(&woken),
                index,
            }))
        })
        .collect();
    let mut reads: Vec<_> = (0..depth).map(|_| None).collect();
    let mut free: Vec<usize> = (0..depth).collect();
    let (mut started, mut ended) = (0, 0);
    // The last turn had no more work, and no read ended after it: the loop waits.
    let mut idle = false;
    while ended < plan.count {
        while started < plan.count
            && let Some(place) = free.pop()
        {
            let offset = offsets.next();
            let buffer = pool.acquire().map_err(|error| format!("acquire: {error}"))?;
            let start = Instant::now();
            let read = driver
                .read_at(file, buffer, offset)
                .map_err(|refused| format!("read: {refused}"))?;
            reads[place] = Some((start, offset, read));
            // The first poll gives the read the waker of its place.
            woken.set(place);
            started += 1;
        }
        let mut progress = false;
        for (word, bits) in woken.0.iter().enumerate() {
            // Ordering: `Relaxed`, as in `Woken::set`.
            let mut bits = bits.swap(0, Ordering::Relaxed);
            while bits != 0 {
                let place = word * 64 + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let Some((start, offset, read)) = &mut reads[place] else {
                    continue;
                };
                let Poll::Ready(output) = Pin::new(read).poll(&mut Context::from_waker(&wakers[place])) else {
                    continue;
                };
                latency.record(nanos(*start));
                let (count, _buffer) = output.map_err(|ended| format!("read: {ended}"))?;
                whole(count, *offset, plan)?;
                reads[place] = None;
                free.push(place);
                ended += 1;
                progress = true;
            }
        }
        if progress {
            idle = false;
            continue;
        }
        if idle && matches!(plan.drive, Drive::Turn) {
            counts.waits += 1;
            driver.wait().map_err(|error| format!("wait: {error}"))?;
        }
        counts.turns += 1;
        idle = !driver.turn().map_err(|error| format!("turn: {error}"))?.more();
    }
    Ok(())
}

/// Runs `plan.count` durable writes on `file`, one at a time.
fn write(
    driver: &mut Driver,
    file: &File,
    pool: &mut Pool,
    plan: &Plan,
    offsets: &mut Offsets,
    latency: &mut Histogram,
    counts: &mut Counts,
) -> Result<(), String> {
    let pattern = vec![0x5a; plan.block];
    for _ in 0..plan.count {
        let offset = offsets.next();
        let mut loan = WriteLoan::new(pool.acquire().map_err(|error| format!("acquire: {error}"))?);
        loan.fill_from(&pattern);
        let lease = Lease::from(loan.into_inner());
        let start = Instant::now();
        let write = driver
            .write_at(file, lease, offset, WriteMode::DataSync)
            .map_err(|refused| format!("write: {refused}"))?;
        let (count, _lease) =
            run_to_end(driver, write, plan.drive, counts)?.map_err(|ended| format!("write: {ended}"))?;
        latency.record(nanos(start));
        whole(count, offset, plan)?;
    }
    Ok(())
}

/// The nanoseconds since `start`.
fn nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
