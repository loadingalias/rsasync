//! The open-loop client. Each connection sends a request of [`SIZE`] bytes at each instant of its
//! own seeded Poisson schedule, also while its earlier responses are late. Each latency counts from
//! the scheduled instant, so a stall of the server cannot hide behind a late send. The send lag,
//! from the scheduled instant to the start of the send, tells whether the client kept its schedule.
//! With `--slice MILLISECONDS`, the client also gives the 99.9th percentile of each slice of the
//! window, by the scheduled instant of each request, so a short stall of the server shows.
//!
//! The client runs on local workers with epoll, not on the io_uring path of the server. On each
//! worker, one pacer task polls in a loop and wakes each connection at its next instant.

use core::cell::{Cell, RefCell};
use core::cmp::Reverse;
use core::future::{Future, poll_fn};
use core::net::SocketAddr;
use core::pin::pin;
use core::task::{Poll, Waker};
use core::time::Duration;
use std::collections::{BinaryHeap, VecDeque};
use std::io::Write as _;
use std::rc::Rc;
use std::time::Instant;

use rsasync::buffer::{Backing, Buffer as _, BufferMut as _, Lease};
use rsasync::driver::{Backend, Config, TcpOptions};
use rsasync::local::{ReadHalf, TcpStream, WriteHalf};
use rsasync::runtime::{ChaCha20Rng, Placement, Runtime, Selection, WorkerId};
use rsasync::task::{self, Outcome};

use crate::histogram::Histogram;
use crate::{SIZE, value};

/// The time from the start of the process to the start of the schedules, for the connects.
const CONNECT_TIME: Duration = Duration::from_secs(3);

/// The settings of a run.
#[derive(Clone, Copy)]
pub(crate) struct Plan {
    target: SocketAddr,
    connections: u32,
    /// The offered requests each second, over all connections.
    rate: f64,
    workers: u32,
    warmup: Duration,
    window: Duration,
    seed: u64,
    /// The time of each slice of the window, or zero for no slices.
    slice: Duration,
}

impl Plan {
    /// The plan of the arguments.
    pub(crate) fn parse(arguments: &[String]) -> Result<Self, String> {
        let plan = Self {
            target: value(arguments, "target")?,
            connections: value(arguments, "connections")?,
            rate: value(arguments, "rate")?,
            workers: value(arguments, "workers")?,
            warmup: Duration::from_secs(value(arguments, "warmup")?),
            window: Duration::from_secs(value(arguments, "window")?),
            seed: value(arguments, "seed")?,
            slice: if arguments.iter().any(|argument| argument == "--slice") {
                Duration::from_millis(value(arguments, "slice")?)
            } else {
                Duration::ZERO
            },
        };
        if plan.workers == 0 || plan.connections < plan.workers || !plan.rate.is_finite() || plan.rate <= 0.0 {
            return Err("give at least one worker, a connection for each worker, and a rate".to_owned());
        }
        Ok(plan)
    }
}

/// The measurements of one worker, or of all of them.
#[derive(Debug, Default)]
struct Stats {
    /// The latency of each request that the window scheduled.
    latency: Histogram,
    /// The send lag of each request that the window scheduled.
    lag: Histogram,
    /// The responses that came during the window.
    completed: u64,
    /// The slice and the latency of each request that the window scheduled, with slices: the
    /// nanoseconds of a latency stop at `u32::MAX`, about 4.3 s.
    samples: Vec<(u32, u32)>,
}

impl Stats {
    fn merge(&mut self, other: &Self) {
        self.latency.merge(&other.latency);
        self.lag.merge(&other.lag);
        self.completed = self.completed.saturating_add(other.completed);
        self.samples.extend_from_slice(&other.samples);
    }
}

/// The 99.9th percentile of the latency of each slice in `samples`, as a JSON object: the count of
/// the slices, and the median, the 99th percentile, and the largest of these values.
fn slices(samples: &mut [(u32, u32)], slice: Duration) -> String {
    samples.sort_unstable();
    let mut tails: Vec<u32> = samples
        .chunk_by(|one, next| one.0 == next.0)
        .map(|slice| slice[(slice.len() * 999).div_ceil(1000).max(1) - 1].1)
        .collect();
    tails.sort_unstable();
    let at = |numerator: usize, denominator: usize| {
        tails
            .get((tails.len() * numerator).div_ceil(denominator).max(1) - 1)
            .copied()
            .unwrap_or(0)
    };
    format!(
        "{{\"ms\":{},\"count\":{},\"p999_ns\":{{\"p50\":{},\"p99\":{},\"max\":{}}}}}",
        slice.as_millis(),
        tails.len(),
        at(1, 2),
        at(99, 100),
        at(1, 1)
    )
}

/// Runs the plan, and prints its result as one JSON line.
pub(crate) fn run(plan: Plan) -> Result<(), String> {
    let each = plan.connections.div_ceil(plan.workers);
    let slots = each.saturating_add(16);
    let mut seed = [0; 32];
    seed[..8].copy_from_slice(&plan.seed.to_le_bytes());
    let runtime = Runtime::builder()
        .local_workers(plan.workers)
        .placement(Placement::Dedicated, Selection::Permissive)
        .task_slots(slots)
        .driver(
            Config::new()
                .backend(Backend::Epoll)
                .resources(slots)
                .operations(slots.saturating_mul(2))
                .submissions(1024),
        )
        .seed(seed)
        .build()
        .map_err(|failure| format!("start: {failure:?}"))?;
    let mut handle = runtime.handle().map_err(|error| format!("handle: {error:?}"))?;
    let start = Instant::now() + CONNECT_TIME;
    let mut workers = Vec::new();
    for index in 0..plan.workers {
        let task = handle
            .spawn_on(WorkerId::new(index), move || worker(plan, index, start))
            .map_err(|refused| format!("spawn: {:?}", refused.reason()))?;
        workers.push(task);
    }
    let mut total = Stats::default();
    for task in workers {
        match task.join_blocking() {
            Outcome::Completed(Ok(stats)) => total.merge(&stats),
            Outcome::Completed(Err(error)) => return Err(error),
            _ => return Err("a worker did not complete".to_owned()),
        }
    }
    let seconds = plan.window.as_secs_f64();
    #[expect(clippy::cast_precision_loss, reason = "a count of responses is far below 2^52")]
    let achieved = total.completed as f64 / seconds;
    let sliced = if plan.slice.is_zero() {
        String::new()
    } else {
        format!(",\"slices\":{}", slices(&mut total.samples, plan.slice))
    };
    writeln!(
        std::io::stdout().lock(),
        "{{\"connections\":{},\"workers\":{},\"offered\":{:.0},\"achieved\":{achieved:.0},\
         \"requests\":{},\"latency_ns\":{},\"lag_ns\":{}{sliced}}}",
        plan.connections,
        plan.workers,
        plan.rate,
        total.latency.total(),
        percentiles(&total.latency),
        percentiles(&total.lag),
    )
    .map_err(|error| format!("output: {error}"))
}

/// The percentiles of `histogram` as a JSON object.
fn percentiles(histogram: &Histogram) -> String {
    format!(
        "{{\"p50\":{},\"p99\":{},\"p999\":{},\"max\":{}}}",
        histogram.percentile(1, 2),
        histogram.percentile(99, 100),
        histogram.percentile(999, 1000),
        histogram.max()
    )
}

/// The nanoseconds from `start` to now, or 0 before `start`.
fn elapsed(start: Instant) -> u64 {
    u64::try_from(Instant::now().saturating_duration_since(start).as_nanos()).unwrap_or(u64::MAX)
}

/// Runs the connections of worker `index`, and gives their measurements.
async fn worker(plan: Plan, index: u32, start: Instant) -> Result<Stats, String> {
    let count = plan.connections / plan.workers + u32::from(index < plan.connections % plan.workers);
    let count = usize::try_from(count).map_err(|error| error.to_string())?;
    let nanos = |duration: Duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
    let schedule = Schedule {
        start,
        warm: nanos(plan.warmup),
        end: nanos(plan.warmup + plan.window),
        mean: 1e9 * f64::from(plan.connections) / plan.rate,
        slice: nanos(plan.slice),
    };
    let pacer = Rc::new(Pacer::new(count));
    let stats = Rc::new(RefCell::new(Stats::default()));
    let pace = task::spawn_local(Rc::clone(&pacer).run(start)).map_err(|refused| format!("{:?}", refused.reason()))?;
    let options = TcpOptions {
        no_delay: Some(true),
        ..TcpOptions::default()
    };
    let mut connections = Vec::with_capacity(count);
    for slot in 0..count {
        let connect = TcpStream::connect(plan.target, options).map_err(|error| format!("connect: {error:?}"))?;
        let stream = connect
            .await
            .map_err(|error| format!("connect: {error:?}"))?
            .map_err(|error| format!("connect: {error}"))?;
        let generator = task::generator(b"probe\0\0\0").map_err(|error| format!("generator: {error:?}"))?;
        let connection = Connection {
            slot,
            schedule,
            pacer: Rc::clone(&pacer),
            stats: Rc::clone(&stats),
        };
        let task = task::spawn_local(connection.run(stream, generator))
            .map_err(|refused| format!("spawn: {:?}", refused.reason()))?;
        connections.push(task);
    }
    for connection in connections {
        if let Outcome::Completed(Err(error)) = connection.join().await {
            return Err(error);
        }
    }
    drop(pace.join().await);
    Rc::try_unwrap(stats)
        .map(RefCell::into_inner)
        .map_err(|_| "a task still holds the measurements".to_owned())
}

/// The instants of a run, in nanoseconds from `start`, and the mean gap of each connection.
#[derive(Clone, Copy)]
struct Schedule {
    start: Instant,
    /// The end of the warm-up, which is the start of the window.
    warm: u64,
    /// The end of the window, and of the schedule.
    end: u64,
    /// The mean gap between two requests of one connection.
    mean: f64,
    /// The nanoseconds of each slice of the window, or 0 for no slices.
    slice: u64,
}

impl Schedule {
    /// A gap of the Poisson schedule: an exponential value with the mean of the schedule.
    fn gap(&self, generator: &mut ChaCha20Rng) -> u64 {
        // A uniform value in (0, 1], from the top 53 bits.
        #[expect(clippy::cast_precision_loss, reason = "a value of 53 bits is exact in an f64")]
        let unit = ((generator.next_u64() >> 11) + 1) as f64 / (1_u64 << 53) as f64;
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the gap is positive, and a cast saturates at u64::MAX"
        )]
        let gap = (-unit.ln() * self.mean) as u64;
        gap
    }
}

/// One connection of a worker: its slot in the pacer, and the measurements that it adds to.
struct Connection {
    slot: usize,
    schedule: Schedule,
    pacer: Rc<Pacer>,
    stats: Rc<RefCell<Stats>>,
}

impl Connection {
    /// Sends and receives at one time until the end of the schedule, and closes the stream.
    async fn run(self, mut stream: TcpStream, mut generator: ChaCha20Rng) -> Result<(), String> {
        // The scheduled instant of each request that has no response yet, in order.
        let waiting = RefCell::new(VecDeque::new());
        let result = {
            let (read, write) = stream.split();
            join(self.send(write, &mut generator, &waiting), self.receive(read, &waiting)).await
        };
        if let Ok(close) = stream.close() {
            let _closed = close.await;
        }
        result
    }

    /// Sends a request at each instant of the schedule, then half-closes the stream.
    async fn send(
        &self,
        mut write: WriteHalf<'_>,
        generator: &mut ChaCha20Rng,
        waiting: &RefCell<VecDeque<u64>>,
    ) -> Result<(), String> {
        let schedule = &self.schedule;
        let mut lease = Lease::from(vec![0x5a; SIZE]);
        let mut at = schedule.gap(generator);
        let mut result = Ok(());
        while at < schedule.end {
            self.pacer.until(self.slot, at, schedule.start).await;
            if at >= schedule.warm {
                self.stats
                    .borrow_mut()
                    .lag
                    .record(elapsed(schedule.start).saturating_sub(at));
            }
            waiting.borrow_mut().push_back(at);
            match send_all(&mut write, lease).await {
                Ok(sent) => lease = sent,
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
            at = at.saturating_add(schedule.gap(generator));
        }
        self.pacer.finish();
        // The server closes its stream after the end of this one, which ends the receive.
        let shutdown = write.shutdown_write().map_err(|error| format!("shutdown: {error:?}"))?;
        shutdown
            .await
            .map_err(|error| format!("shutdown: {error:?}"))?
            .map_err(|error| format!("shutdown: {error}"))?;
        result
    }

    /// Receives each response until the end of the stream, and records its latency.
    async fn receive(&self, mut read: ReadHalf<'_>, waiting: &RefCell<VecDeque<u64>>) -> Result<(), String> {
        let schedule = &self.schedule;
        let mut buffer = Backing::from(Vec::with_capacity(64 << 10));
        let mut partial = 0_usize;
        loop {
            buffer.truncate(0);
            let receive = read.recv(buffer).map_err(|error| format!("recv: {error:?}"))?;
            let (count, back) = receive.await.map_err(|error| format!("recv: {error:?}"))?;
            buffer = back;
            let count = count.map_err(|error| format!("recv: {error}"))?;
            if count == 0 {
                return Ok(());
            }
            let now = elapsed(schedule.start);
            partial = partial.saturating_add(count);
            while let Some(rest) = partial.checked_sub(SIZE) {
                partial = rest;
                let at = waiting
                    .borrow_mut()
                    .pop_front()
                    .ok_or("a response came with no request")?;
                let mut stats = self.stats.borrow_mut();
                if (schedule.warm..schedule.end).contains(&at) {
                    let latency = now.saturating_sub(at);
                    stats.latency.record(latency);
                    if let Some(slice) = (at - schedule.warm).checked_div(schedule.slice) {
                        stats.samples.push((
                            u32::try_from(slice).unwrap_or(u32::MAX),
                            u32::try_from(latency).unwrap_or(u32::MAX),
                        ));
                    }
                }
                if (schedule.warm..schedule.end).contains(&now) {
                    stats.completed = stats.completed.saturating_add(1);
                }
            }
        }
    }
}

/// Sends each byte of `lease` on `write`, and gives the lease back.
pub(crate) async fn send_all(write: &mut WriteHalf<'_>, lease: Lease) -> Result<Lease, String> {
    let send = write
        .send(lease)
        .map_err(|error| format!("send: {:?}", error.reason()))?;
    let (mut result, lease) = send.await.map_err(|error| format!("send: {error:?}"))?;
    let mut sent = 0usize;
    loop {
        let count = result.map_err(|error| format!("send: {error}"))?;
        sent = sent.checked_add(count).ok_or("send count overflow")?;
        if sent == lease.initialized() {
            return Ok(lease);
        }
        if count == 0 {
            return Err("send returned zero with bytes left".into());
        }
        let rest = lease.view(sent..).ok_or("send exceeded the buffer length")?;
        let send = write
            .send(rest)
            .map_err(|error| format!("send: {:?}", error.reason()))?;
        (result, _) = send.await.map_err(|error| format!("send: {error:?}"))?;
    }
}

/// Polls `first` and `second` at one time, and gives the first error, else `Ok`.
async fn join(
    first: impl Future<Output = Result<(), String>>,
    second: impl Future<Output = Result<(), String>>,
) -> Result<(), String> {
    let (mut first, mut second) = (pin!(first), pin!(second));
    let (mut one, mut two) = (None, None);
    poll_fn(|context| {
        if one.is_none()
            && let Poll::Ready(result) = first.as_mut().poll(context)
        {
            one = Some(result);
        }
        if two.is_none()
            && let Poll::Ready(result) = second.as_mut().poll(context)
        {
            two = Some(result);
        }
        if one.is_some() && two.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    match (one, two) {
        (Some(Err(error)), _) | (_, Some(Err(error))) => Err(error),
        _ => Ok(()),
    }
}

/// The wakes of the connections of one worker at their scheduled instants. Its task polls in a
/// loop, so each wake comes within one turn of the worker after its instant.
struct Pacer {
    /// The next instant of each connection that waits, earliest first.
    due: RefCell<BinaryHeap<Reverse<(u64, usize)>>>,
    /// The waker of each connection that waits.
    wakers: RefCell<Vec<Option<Waker>>>,
    /// The connections that still send.
    senders: Cell<usize>,
}

impl Pacer {
    fn new(connections: usize) -> Self {
        Self {
            due: RefCell::new(BinaryHeap::with_capacity(connections)),
            wakers: RefCell::new(vec![None; connections]),
            senders: Cell::new(connections),
        }
    }

    /// Wakes each connection whose instant came, until no connection sends.
    async fn run(self: Rc<Self>, start: Instant) {
        while self.senders.get() > 0 {
            let now = elapsed(start);
            loop {
                let mut due = self.due.borrow_mut();
                let Some(&Reverse((at, slot))) = due.peek() else { break };
                if at > now {
                    break;
                }
                due.pop();
                drop(due);
                if let Some(waker) = self.wakers.borrow_mut().get_mut(slot).and_then(Option::take) {
                    waker.wake();
                }
            }
            yield_now().await;
        }
    }

    /// Waits until `at`, in nanoseconds from `start`, for the connection `slot`.
    fn until(&self, slot: usize, at: u64, start: Instant) -> impl Future<Output = ()> + '_ {
        poll_fn(move |context| {
            if elapsed(start) >= at {
                return Poll::Ready(());
            }
            if let Some(waker) = self.wakers.borrow_mut().get_mut(slot) {
                // A connection is in the heap while its waker is there.
                if waker.replace(context.waker().clone()).is_none() {
                    self.due.borrow_mut().push(Reverse((at, slot)));
                }
            }
            Poll::Pending
        })
    }

    /// Records the end of the schedule of one connection.
    fn finish(&self) {
        self.senders.set(self.senders.get().saturating_sub(1));
    }
}

/// Gives way to the other tasks of this worker one time.
fn yield_now() -> impl Future<Output = ()> {
    let mut yielded = false;
    poll_fn(move |context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
}
