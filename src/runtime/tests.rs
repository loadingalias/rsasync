use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::assert_matches;
use core::ffi::c_int;
use core::future::pending;
use core::num::NonZeroU64;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};
use std::sync::{Mutex, mpsc};
use std::thread;

use super::topology::{Host, Quota};
use super::*;
use crate::capacity::Budget;
use crate::channel::oneshot;
use crate::driver::tests::{Log, pipe, recorded, taken};
use crate::driver::{Backend, Config};
use crate::task::tests::yield_once;
use crate::task::{DriverRefused, Outcome as TaskOutcome, Reason, generator, spawn_local, with_driver};

fn budget() -> Budget {
    Budget::new(NonZeroU64::new(1 << 20).unwrap())
}

fn completed<T: core::fmt::Debug>(outcome: TaskOutcome<T>) -> T {
    match outcome {
        TaskOutcome::Completed(value) => value,
        other => panic!("the task did not complete: {other:?}"),
    }
}

/// The open descriptors of this process. The descriptor of the listing itself is closed again, so
/// it is not in the list.
fn descriptors() -> Vec<u32> {
    let listed: Vec<u32> = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_str().unwrap().parse().unwrap())
        .collect();
    listed
        .into_iter()
        .filter(|fd| std::path::Path::new(&alloc::format!("/proc/self/fd/{fd}")).exists())
        .collect()
}

/// The io_uring rings of this process.
fn rings() -> usize {
    descriptors()
        .into_iter()
        .filter(|fd| {
            std::fs::read_link(alloc::format!("/proc/self/fd/{fd}"))
                .is_ok_and(|target| target.as_os_str() == "anon_inode:[io_uring]")
        })
        .count()
}

/// The threads of this process: each thread ID with its name.
fn threads() -> Vec<(String, String)> {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = std::fs::read_to_string(entry.path().join("comm")).ok()?;
            Some((
                entry.file_name().to_string_lossy().into_owned(),
                String::from(name.trim()),
            ))
        })
        .collect()
}

/// Waits until this process has no thread that `before` does not have. A joined thread can stay in
/// `/proc` for a short time: the join returns when the kernel clears the thread ID, early in the
/// exit of the thread, and the kernel removes the thread later. The bound is far above that time,
/// and a thread that never ends stays past it.
fn assert_no_new_thread(before: &[(String, String)]) {
    let deadline = std::time::Instant::now() + core::time::Duration::from_secs(5);
    loop {
        let new: Vec<_> = threads()
            .into_iter()
            .filter(|thread| !before.contains(thread))
            .collect();
        if new.is_empty() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "threads of the failed start remain: {new:?}"
        );
        thread::sleep(core::time::Duration::from_millis(1));
    }
}

#[repr(C)]
struct Limit {
    current: u64,
    max: u64,
}

/// `RLIMIT_NOFILE` of `include/uapi/asm-generic/resource.h`, which x86_64 and aarch64 use.
const RLIMIT_NOFILE: c_int = 7;

#[expect(unsafe_code, reason = "the C library of each Linux target gives these functions")]
unsafe extern "C" {
    fn getrlimit(resource: c_int, limit: *mut Limit) -> c_int;
    fn setrlimit(resource: c_int, limit: *const Limit) -> c_int;
}

/// Runs `f` while a new descriptor of this process gets a number below `limit`.
#[expect(unsafe_code, reason = "each call names one valid `rlimit`")]
fn with_descriptor_limit<R>(limit: u64, f: impl FnOnce() -> R) -> R {
    let mut before = Limit { current: 0, max: 0 };
    // SAFETY: `before` is one writable `rlimit`.
    assert_eq!(unsafe { getrlimit(RLIMIT_NOFILE, &raw mut before) }, 0);
    let lowered = Limit {
        current: limit,
        max: before.max,
    };
    // SAFETY: `lowered` is one readable `rlimit`.
    assert_eq!(unsafe { setrlimit(RLIMIT_NOFILE, &raw const lowered) }, 0);
    let result = f();
    // SAFETY: `before` is one readable `rlimit`.
    assert_eq!(unsafe { setrlimit(RLIMIT_NOFILE, &raw const before) }, 0);
    result
}

/// The smallest limit below which exactly `free` descriptor numbers are free, with `open` open.
fn limit_for(open: &[u32], free: usize) -> u64 {
    (0_u32..)
        .find(|limit| *limit as usize - open.iter().filter(|fd| *fd < limit).count() == free)
        .map(u64::from)
        .unwrap()
}

/// A worker that the descriptor limit leaves no room for its driver fails the start. The start
/// stops and joins each worker before it, reports the cleanup of each one, and leaves no thread.
#[test]
fn a_failure_at_worker_k_stops_and_joins_each_worker_before_it() {
    const WORKERS: u32 = 4;
    const FAILED: u32 = 2;
    let before = descriptors().len();
    let runtime = Runtime::builder().local_workers(1).build().unwrap();
    let held = descriptors().len() - before;
    drop(runtime);
    assert!(held > 1, "a worker holds the descriptors of its driver");
    let threads_before = threads();
    // Worker FAILED gets one descriptor fewer than a worker holds. Each worker before it gets at
    // least twice that while it starts, which covers the pipe of the probe of its driver.
    let limit = limit_for(&descriptors(), held * FAILED as usize + held - 1);
    let failure = with_descriptor_limit(limit, || Runtime::builder().local_workers(WORKERS).build()).unwrap_err();
    assert_matches!(
        failure.cause(),
        Cause::Worker {
            index: FAILED,
            error: WorkerError::Driver(_)
        }
    );
    let cleanup: Vec<_> = failure
        .cleanup()
        .iter()
        .map(|cleanup| (cleanup.worker().index(), cleanup.outcome() == &Outcome::Joined))
        .collect();
    assert_eq!(cleanup, [(0, true), (1, true), (FAILED, true)]);
    assert_no_new_thread(&threads_before);
}

/// The factory runs on the thread of its worker, which makes a future that holds an `Rc`. A spawn
/// beyond the free slots of the worker comes back with its factory.
fn a_factory_makes_its_future_on_its_worker_and_a_spawn_beyond_the_free_slots_comes_back(backend: Backend) {
    let runtime = Runtime::builder()
        .local_workers(2)
        .task_slots(1)
        .driver(Config::new().backend(backend))
        .build()
        .unwrap();
    assert!(
        runtime
            .report()
            .workers()
            .iter()
            .all(|worker| worker.driver().backend() == backend)
    );
    let mut handle = runtime.handle().unwrap();
    let budget = budget();
    let (release, mut released) = oneshot::channel::<()>(&budget).unwrap();
    let held = handle
        .spawn_on(WorkerId::new(1), move || {
            let local = Rc::new(thread::current().name().map(String::from));
            async move {
                released.recv().await.unwrap();
                (*local).clone()
            }
        })
        .unwrap();
    let refused = handle.spawn_on(WorkerId::new(1), || async {}).unwrap_err();
    assert_eq!(refused.reason(), Reason::NoSlot);
    let _factory = refused.into_future();
    let missing = handle.spawn_on(WorkerId::new(2), || async {}).unwrap_err();
    assert_eq!(missing.reason(), Reason::NoWorker);
    release.send(()).unwrap();
    assert_eq!(completed(held.join_blocking()).as_deref(), Some("rsasync-local-1"));
}

#[test]
fn a_factory_makes_its_future_on_its_worker_and_a_spawn_beyond_the_free_slots_comes_back_on_io_uring() {
    a_factory_makes_its_future_on_its_worker_and_a_spawn_beyond_the_free_slots_comes_back(Backend::IoUring);
}

#[test]
fn a_factory_makes_its_future_on_its_worker_and_a_spawn_beyond_the_free_slots_comes_back_on_epoll() {
    a_factory_makes_its_future_on_its_worker_and_a_spawn_beyond_the_free_slots_comes_back(Backend::Epoll);
}

/// A cancel ends a factory spawn whether it comes before the worker calls the factory, which then
/// drops uncalled, or after the task runs.
#[test]
fn a_cancel_ends_a_factory_spawn_before_or_after_its_worker_places_it() {
    let runtime = Runtime::builder().local_workers(1).build().unwrap();
    let mut handle = runtime.handle().unwrap();
    // The first factory holds the worker in its call, so the next spawn waits in the inbox.
    let (unblock, blocked) = mpsc::channel::<()>();
    let blocker = handle
        .spawn_on(WorkerId::new(0), move || {
            blocked.recv().unwrap();
            async {}
        })
        .unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let early = handle
        .spawn_on(WorkerId::new(0), {
            let called = Arc::clone(&called);
            move || {
                called.store(true, Ordering::Relaxed);
                pending::<()>()
            }
        })
        .unwrap();
    early.cancel_handle().cancel();
    unblock.send(()).unwrap();
    assert_matches!(early.join_blocking(), TaskOutcome::Cancelled);
    assert!(
        !called.load(Ordering::Relaxed),
        "a cancel that came first drops the factory uncalled"
    );
    completed(blocker.join_blocking());
    let (polled, mut started) = oneshot::channel::<()>(&budget()).unwrap();
    let late = handle
        .spawn_on(WorkerId::new(0), move || async move {
            polled.send(()).unwrap();
            pending::<()>().await;
        })
        .unwrap();
    started.recv_blocking().unwrap();
    late.cancel_handle().cancel();
    assert_matches!(late.join_blocking(), TaskOutcome::Cancelled);
}

/// The first CPU of the physical core of `cpu`, from the topology files of the kernel: a list in
/// ascending order begins with it.
fn first_sibling(cpu: u32) -> u32 {
    let path = alloc::format!("/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list");
    let siblings = std::fs::read_to_string(path).unwrap();
    siblings.trim().split([',', '-']).next().unwrap().parse().unwrap()
}

/// Each worker of `Dedicated` runs on one CPU, of a physical core that no other worker has, and its
/// thread reports the same CPU as the start.
#[test]
fn dedicated_placement_pins_each_worker_to_one_cpu_of_its_own_physical_core() {
    let mut cores: Vec<u32> = sys::affinity().unwrap().iter().map(first_sibling).collect();
    cores.sort_unstable();
    cores.dedup();
    let workers = u32::try_from(cores.len()).unwrap();
    let runtime = Runtime::builder()
        .local_workers(workers)
        .placement(Placement::Dedicated, Selection::Strict)
        .build()
        .unwrap();
    let mut handle = runtime.handle().unwrap();
    let mut pinned = Vec::new();
    for (index, report) in (0..).zip(runtime.report().workers()) {
        let applied = report.placement().applied().unwrap().clone();
        assert_eq!(applied.len(), 1, "worker {index} runs on {applied:?}");
        let inside = handle
            .spawn_on(WorkerId::new(index), || async { sys::affinity().unwrap() })
            .unwrap();
        assert_eq!(completed(inside.join_blocking()), applied);
        pinned.push(first_sibling(applied.iter().next().unwrap()));
    }
    pinned.sort_unstable();
    assert_eq!(pinned, cores, "each worker has its own physical core");
}

/// An exact CPU that is not in the CPU set of the process runs its worker unpinned with the
/// fallback in the report, and a strict selection refuses the plan before a worker starts.
#[test]
fn an_exact_cpu_outside_the_cpu_set_runs_unpinned_and_a_strict_plan_refuses() {
    let allowed = sys::affinity().unwrap();
    let inside = allowed.iter().next().unwrap();
    let outside = allowed.iter().last().unwrap() + 1;
    let exact = || Placement::Exact([inside, outside].into());
    let runtime = Runtime::builder()
        .local_workers(2)
        .placement(exact(), Selection::Permissive)
        .build()
        .unwrap();
    let [first, second] = runtime.report().workers() else {
        panic!("two workers");
    };
    assert_eq!(
        first.placement().applied().map(|set| set.iter().collect()),
        Some(alloc::vec![inside])
    );
    assert_eq!(second.placement().applied(), None);
    assert_eq!(second.placement().fallback(), Some(Fallback::NotAllowed(outside)));
    drop(runtime);
    let threads_before = threads();
    let refused = Runtime::builder()
        .local_workers(2)
        .placement(exact(), Selection::Strict)
        .build()
        .unwrap_err();
    assert_matches!(
        refused.cause(),
        Cause::Plan(PlanError::Refused {
            worker: 1,
            fallback: Fallback::NotAllowed(cpu)
        }) if *cpu == outside
    );
    assert!(refused.cleanup().is_empty());
    assert_no_new_thread(&threads_before);
}

/// A host of 16 CPUs: 8 physical cores with SMT siblings `c` and `c + 8`, two NUMA nodes, and
/// four cache domains of two cores each.
fn two_node_host() -> Host {
    let mut allowed = CpuSet::new();
    for cpu in 0..16 {
        allowed.insert(cpu);
    }
    Host {
        online: Some(allowed.clone()),
        allowed: Some(allowed),
        isolated: None,
        core: (0..16).map(|cpu| Some(cpu % 8)).collect(),
        domain: (0..16).map(|cpu| Some(cpu % 8 / 2 * 2)).collect(),
        node: (0..16).map(|cpu| Some(cpu % 8 / 4)).collect(),
        quota: Quota::None,
    }
}

fn cpus(set: &CpuSet) -> Vec<u32> {
    set.iter().collect()
}

/// `Dedicated` takes one CPU of each physical core first, in turn over the nodes and then over the
/// domains of each node, and the SMT siblings last. `Domain` spreads the workers over the domains
/// in proportion to their cores.
#[test]
fn the_plans_spread_over_nodes_and_domains_and_take_smt_siblings_last() {
    let host = two_node_host();
    let dedicated = placement::resolve(&Placement::Dedicated, Selection::Strict, 16, &host).unwrap();
    let order: Vec<u32> = dedicated
        .iter()
        .map(|plan| plan.resolved().unwrap().iter().next().unwrap())
        .collect();
    assert_eq!(order, [0, 4, 2, 6, 1, 5, 3, 7, 8, 12, 10, 14, 9, 13, 11, 15]);
    assert_eq!(dedicated[1].node(), Some(1));
    let domain = placement::resolve(&Placement::Domain, Selection::Strict, 6, &host).unwrap();
    let targets: Vec<Vec<u32>> = domain.iter().map(|plan| cpus(plan.resolved().unwrap())).collect();
    let domains = [[0, 1, 8, 9], [2, 3, 10, 11], [4, 5, 12, 13], [6, 7, 14, 15]];
    assert_eq!(
        targets,
        [
            &domains[0],
            &domains[1],
            &domains[2],
            &domains[3],
            &domains[0],
            &domains[1]
        ]
    );
    let too_many = placement::resolve(&Placement::Dedicated, Selection::Permissive, 17, &host);
    assert_matches!(
        too_many,
        Err(Cause::Plan(PlanError::TooFewCpus { workers: 17, cpus: 16 }))
    );
    let mut one_domain = host;
    one_domain.domain.fill(Some(0));
    let plans = placement::resolve(&Placement::Domain, Selection::Permissive, 2, &one_domain).unwrap();
    assert!(
        plans
            .iter()
            .all(|plan| plan.fallback() == Some(Fallback::OneDomain) && plan.resolved().is_none())
    );
}

/// The root completes while its task waits and while a receive that it started runs. `block_on`
/// cancels the task, waits for it, and releases the buffer of the receive before it returns. The
/// root forgets the future of the receive and never drops the reader, so no cancel request and no
/// close exists: only the teardown of `block_on` ends the receive.
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
fn block_on_returns_after_its_task_is_cancelled_and_its_receive_is_released(backend: Backend) {
    let runtime = Runtime::builder()
        .driver(Config::new().backend(backend))
        .build()
        .unwrap();
    let log = Log::default();
    // The write end stays open, so the receive never completes by itself.
    let (read, _write) = pipe();
    let task = runtime
        .block_on(async {
            let local = Rc::new(());
            let task = spawn_local({
                let local = Rc::clone(&local);
                async move {
                    pending::<()>().await;
                    drop(local);
                }
            })
            .unwrap();
            // A drop of the reader would close the pipe, and the turns that wait for the task
            // would then end the receive before the teardown.
            let reader = Box::leak(Box::new(
                with_driver(|driver| driver.adopt_reader(read)).unwrap().unwrap(),
            ));
            let receive = with_driver(|driver| driver.recv(reader, recorded(&log, 64)))
                .unwrap()
                .unwrap();
            // The root holds the `Rc` across an await. A drop of the future would request the
            // cancel of the receive, which the turns that wait for the task would end.
            yield_once().await;
            core::mem::forget(receive);
            drop(local);
            task
        })
        .unwrap();
    assert_eq!(taken(&log), ["released"], "the release came before block_on returned");
    assert_matches!(task.join_blocking(), TaskOutcome::Cancelled);
}

#[test]
fn block_on_returns_after_its_task_is_cancelled_and_its_receive_is_released_on_io_uring() {
    block_on_returns_after_its_task_is_cancelled_and_its_receive_is_released(Backend::IoUring);
}

#[test]
fn block_on_returns_after_its_task_is_cancelled_and_its_receive_is_released_on_epoll() {
    block_on_returns_after_its_task_is_cancelled_and_its_receive_is_released(Backend::Epoll);
}

/// A root that only waits for a value from another thread makes no driver.
#[test]
fn a_root_that_only_waits_starts_no_driver() {
    let runtime = Runtime::builder().build().unwrap();
    let before = rings();
    let (sender, mut receiver) = oneshot::channel::<u32>(&budget()).unwrap();
    let sending = thread::spawn(move || sender.send(7).unwrap());
    let (value, during) = runtime
        .block_on(async { (receiver.recv().await.unwrap(), rings()) })
        .unwrap();
    sending.join().unwrap();
    assert_eq!((value, during), (7, before));
}

/// The owner of `block_on` makes its driver at its first operation, and then waits in it. A wake
/// from a thread with no ring then ends that wait.
fn a_wake_from_another_thread_ends_the_wait_after_the_first_operation_made_the_driver(backend: Backend) {
    let runtime = Runtime::builder()
        .driver(Config::new().backend(backend))
        .build()
        .unwrap();
    let (sender, mut receiver) = oneshot::channel::<()>(&budget()).unwrap();
    let (made, wait) = mpsc::channel::<()>();
    let sending = thread::spawn(move || {
        wait.recv().unwrap();
        sender.send(()).unwrap();
    });
    let made_backend = runtime
        .block_on(async {
            let made_backend = with_driver(|driver| driver.report().backend()).unwrap();
            made.send(()).unwrap();
            receiver.recv().await.unwrap();
            made_backend
        })
        .unwrap();
    sending.join().unwrap();
    assert_eq!(made_backend, backend);
}

#[test]
fn a_wake_from_another_thread_ends_the_wait_after_the_first_operation_made_the_driver_on_io_uring() {
    a_wake_from_another_thread_ends_the_wait_after_the_first_operation_made_the_driver(Backend::IoUring);
}

#[test]
fn a_wake_from_another_thread_ends_the_wait_after_the_first_operation_made_the_driver_on_epoll() {
    a_wake_from_another_thread_ends_the_wait_after_the_first_operation_made_the_driver(Backend::Epoll);
}

/// One value of a new generator of the owner of this thread.
fn draw() -> u64 {
    generator(b"draw\0\0\0\0").unwrap().next_u64()
}

/// Draws one value on each of two workers, and two in `block_on`.
fn draws(builder: Builder) -> Vec<u64> {
    let runtime = builder.local_workers(2).build().unwrap();
    let mut handle = runtime.handle().unwrap();
    let mut draws: Vec<u64> = (0..2)
        .map(|index| {
            let task = handle.spawn_on(WorkerId::new(index), || async { draw() }).unwrap();
            completed(task.join_blocking())
        })
        .collect();
    // Two calls on one owner give two children.
    let (first, second) = runtime.block_on(async { (draw(), draw()) }).unwrap();
    draws.extend([first, second]);
    draws
}

/// Two starts with one seed give each generator of each owner the same values, no two generators
/// of one start share a stream, and two starts with a seed from the system differ.
#[test]
fn one_seed_gives_each_generator_the_same_values_and_no_two_generators_share_a_stream() {
    let seed = [7; ChaCha20Rng::SEED_SIZE];
    let first = draws(Runtime::builder().seed(seed));
    assert_eq!(first, draws(Runtime::builder().seed(seed)));
    let mut distinct = first.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), first.len(), "{first:?}");
    assert_ne!(draws(Runtime::builder()), draws(Runtime::builder()));
}

/// Asks for the driver when the teardown of `block_on` wakes it, and records whether the driver was
/// in use.
struct AsksForTheDriver(Mutex<Option<bool>>);

impl alloc::task::Wake for AsksForTheDriver {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let in_use = matches!(with_driver(|_driver| ()), Err(DriverRefused::InUse));
        *self.0.lock().unwrap() = Some(in_use);
    }
}

/// The teardown of `block_on` ends a receive whose future the root forgot, and calls its waker. A
/// waker that then asks for the driver gets `InUse`, and the owner makes no second driver.
#[test]
#[expect(clippy::mem_forget, reason = "the test forgets a future, as a caller can")]
fn a_waker_that_the_teardown_calls_makes_no_second_driver() {
    let runtime = Runtime::builder().build().unwrap();
    let asks = Arc::new(AsksForTheDriver(Mutex::new(None)));
    let (read, _write) = pipe();
    runtime
        .block_on(async {
            let reader = Box::leak(Box::new(
                with_driver(|driver| driver.adopt_reader(read)).unwrap().unwrap(),
            ));
            let mut receive = Box::pin(
                with_driver(|driver| driver.recv(reader, crate::buffer::Backing::from(Vec::with_capacity(64))))
                    .unwrap()
                    .unwrap(),
            );
            let waker = Waker::from(Arc::clone(&asks));
            assert!(receive.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
            core::mem::forget(receive);
        })
        .unwrap();
    assert_eq!(*asks.0.lock().unwrap(), Some(true));
}

/// A waker of the root that the caller keeps after `block_on` returns holds no descriptor of the
/// driver of the call, and a wake then does nothing.
#[test]
fn a_kept_waker_holds_no_descriptor_after_block_on_returns() {
    let runtime = Runtime::builder().build().unwrap();
    let before = descriptors().len();
    let waker = runtime
        .block_on(async {
            with_driver(|_driver| ()).unwrap();
            core::future::poll_fn(|cx| Poll::Ready(cx.waker().clone())).await
        })
        .unwrap();
    assert_eq!(descriptors().len(), before, "the descriptors of the driver closed");
    waker.wake();
}

/// A cancel handle of a factory spawn that the caller keeps after the task ended, and after the
/// runtime dropped, keeps its charge until it drops.
#[test]
fn a_kept_cancel_handle_of_a_factory_spawn_keeps_its_charge_until_it_drops() {
    let budget = Budget::new(NonZeroU64::new(DEFAULT_BUDGET).unwrap());
    let start = budget.available();
    let runtime = Runtime::builder()
        .local_workers(1)
        .budget(budget.clone())
        .build()
        .unwrap();
    let mut handle = runtime.handle().unwrap();
    let task = handle.spawn_on(WorkerId::new(0), || async {}).unwrap();
    let cancel = task.cancel_handle();
    completed(task.join_blocking());
    drop((handle, runtime));
    assert!(budget.available() < start, "the kept cancel handle holds its charge");
    drop(cancel);
    assert_eq!(budget.available(), start);
}

/// Each refusal of the allocator gives `Allocation` with the factory, and keeps no memory on the
/// thread of the spawn. A factory spawn whose allocation aborts the process fails.
#[test]
fn a_factory_spawn_that_the_allocator_refuses_gives_its_factory_back() {
    use crate::table::tests::allocations::each_refusal;

    let budget = Budget::new(NonZeroU64::new(DEFAULT_BUDGET).unwrap());
    let runtime = Runtime::builder()
        .local_workers(1)
        .budget(budget.clone())
        .build()
        .unwrap();
    let mut handle = runtime.handle().unwrap();
    let before = budget.available();
    let refusals = each_refusal(|| {
        handle
            .spawn_on(WorkerId::new(0), || async { 7 })
            .map(drop)
            .map_err(|refused| (refused, budget.available()))
    });
    assert!(!refusals.is_empty());
    for ((refused, available), held) in refusals {
        assert_eq!((refused.reason(), available, held), (Reason::Allocation, before, 0));
        let _factory = refused.into_future();
    }
}

/// The job of a factory spawn holds the charge of its box until the box drops, when the worker
/// places the task. A task that keeps that charge holds the units of a large factory after its
/// memory is free.
#[test]
fn a_placed_factory_spawn_keeps_no_charge_of_its_factory() {
    const CAPTURE: usize = 16 << 10;
    let budget = Budget::new(NonZeroU64::new(DEFAULT_BUDGET).unwrap());
    let runtime = Runtime::builder()
        .local_workers(1)
        .budget(budget.clone())
        .build()
        .unwrap();
    let mut handle = runtime.handle().unwrap();
    let before = budget.available();
    let (polled, first_poll) = mpsc::channel();
    let capture = [0_u8; CAPTURE];
    let _task = handle
        .spawn_on(WorkerId::new(0), move || {
            let len = capture.len();
            async move {
                polled.send(len).unwrap();
                pending::<()>().await;
            }
        })
        .unwrap();
    assert_eq!(first_poll.recv().unwrap(), CAPTURE);
    let held = before - budget.available();
    assert!(
        held < CAPTURE as u64,
        "the task holds {held} units, also those of its factory"
    );
}

/// Each refusal of the allocator in a start with no worker gives `Cause::Allocation`, also the
/// refusal of the default budget and of the facts of the host, and keeps no memory. A start
/// whose allocation aborts the process fails.
#[test]
fn a_start_that_the_allocator_refuses_gives_allocation() {
    use crate::table::tests::allocations::each_refusal;

    let refusals = each_refusal(|| Runtime::builder().local_workers(0).build().map(drop));
    assert!(!refusals.is_empty());
    for (failure, held) in refusals {
        assert_matches!(failure.cause(), Cause::Allocation);
        assert_eq!(held, 0);
    }
}

/// Each refusal of the allocator in the plans of `Dedicated` and of `Domain` gives
/// `Cause::Allocation`, and keeps no memory.
#[test]
fn a_plan_that_the_allocator_refuses_gives_allocation() {
    use crate::table::tests::allocations::each_refusal;

    let host = two_node_host();
    for (placement, workers) in [(Placement::Dedicated, 16), (Placement::Domain, 6)] {
        let refusals = each_refusal(|| placement::resolve(&placement, Selection::Strict, workers, &host));
        assert!(!refusals.is_empty());
        for (cause, held) in refusals {
            assert_matches!(cause, Cause::Allocation);
            assert_eq!(held, 0);
        }
    }
}

/// Each refusal of the allocator before the first poll of the root of `block_on` gives the root
/// back, unpolled, and keeps no memory and no charge. A refusal that drops the root fails.
#[test]
fn a_block_on_that_the_allocator_refuses_gives_its_root_back() {
    use crate::table::tests::allocations::each_refusal;

    let budget = Budget::new(NonZeroU64::new(DEFAULT_BUDGET).unwrap());
    let runtime = Runtime::builder()
        .local_workers(0)
        .budget(budget.clone())
        .build()
        .unwrap();
    let refusals = each_refusal(|| runtime.block_on(async { 7 }));
    assert!(!refusals.is_empty());
    for (error, held) in refusals {
        assert_matches!(error, BlockOnError::Owner(..) | BlockOnError::Timers(..));
        assert_eq!((held, budget.available()), (0, DEFAULT_BUDGET));
        let mut root = core::pin::pin!(error.into_root().expect("a setup refusal gives the root back"));
        let polled = root.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        assert_eq!(polled, Poll::Ready(7));
    }
}

/// A refusal of the allocator in a clone of a handle gives `Allocation`, not `TooMany`, and keeps
/// no memory.
#[test]
fn a_handle_that_the_allocator_refuses_gives_allocation() {
    use crate::table::tests::allocations::each_refusal;

    let runtime = Runtime::builder().local_workers(1).build().unwrap();
    let handle = runtime.handle().unwrap();
    let refusals = each_refusal(|| handle.try_clone());
    assert!(!refusals.is_empty());
    assert!(
        refusals
            .iter()
            .all(|refusal| *refusal == (HandleRefused::Allocation, 0))
    );
}
