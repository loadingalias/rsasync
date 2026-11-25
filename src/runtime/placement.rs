//! The placement of the workers: the request, its resolution on the facts of the host, and the
//! report of each worker.
//!
//! The resolution reads only the [`Host`], so it makes the same plan from the same facts. It runs
//! before a worker starts, and a strict request that falls back refuses there. Each worker then
//! applies its own plan on its thread (`worker`).

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::alloc::AllocError;
use core::fmt;

use super::Cause;
use super::topology::{Host, Quota};
use crate::driver::filled;
use crate::memory::{push, room};

/// The most CPUs, and the largest CPU index plus 1, that a placement handles.
pub const MAX_CPUS: u32 = 4096;

/// The words of a CPU set.
const WORDS: usize = MAX_CPUS as usize / 64;

/// A set of CPUs, each below [`MAX_CPUS`], in the form of the affinity mask of Linux.
#[derive(Clone, PartialEq, Eq)]
pub struct CpuSet {
    words: [u64; WORDS],
}

impl CpuSet {
    /// The empty set.
    #[must_use]
    pub const fn new() -> Self {
        Self { words: [0; WORDS] }
    }

    /// Adds `cpu`. Gives `false`, and adds nothing, if `cpu` is not below [`MAX_CPUS`].
    pub fn insert(&mut self, cpu: u32) -> bool {
        let Some(word) = self.words.get_mut((cpu / 64) as usize) else {
            return false;
        };
        *word |= 1 << (cpu % 64);
        true
    }

    /// Whether the set has `cpu`.
    #[must_use]
    pub fn contains(&self, cpu: u32) -> bool {
        self.words
            .get((cpu / 64) as usize)
            .is_some_and(|word| word & (1 << (cpu % 64)) != 0)
    }

    /// The CPUs of the set.
    #[must_use]
    pub fn len(&self) -> u32 {
        self.words.iter().map(|word| word.count_ones()).sum()
    }

    /// Whether the set has no CPU.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|word| *word == 0)
    }

    /// The CPUs of the set, in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        (0..).zip(self.words).flat_map(|(index, word)| {
            (0..64_u32)
                .filter(move |bit| word & (1 << bit) != 0)
                .map(move |bit| index * 64 + bit)
        })
    }

    /// The words of the mask, for the system calls of affinity.
    pub(super) const fn words(&self) -> &[u64; WORDS] {
        &self.words
    }

    /// The words of the mask, for `sched_getaffinity`.
    pub(super) const fn words_mut(&mut self) -> &mut [u64; WORDS] {
        &mut self.words
    }

    /// The CPUs of both sets.
    fn and(&self, other: &Self) -> Self {
        let mut words = self.words;
        for (word, other) in words.iter_mut().zip(other.words) {
            *word &= other;
        }
        Self { words }
    }
}

impl Default for CpuSet {
    fn default() -> Self {
        Self::new()
    }
}

/// The CPU list form of Linux, such as `0-3,8`.
impl fmt::Debug for CpuSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cpus = self.iter().peekable();
        let mut first = true;
        while let Some(start) = cpus.next() {
            let mut end = start;
            while cpus.next_if(|cpu| Some(*cpu) == end.checked_add(1)).is_some() {
                end += 1;
            }
            if !first {
                f.write_str(",")?;
            }
            first = false;
            if start == end {
                write!(f, "{start}")?;
            } else {
                write!(f, "{start}-{end}")?;
            }
        }
        Ok(())
    }
}

/// Where the workers run.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Placement {
    /// Binds each worker to the CPUs that share one last-level cache, and spreads the workers over
    /// those domains in proportion to their physical cores. The kernel chooses the core in the
    /// domain, so a worker can leave a busy core. On a host with one domain, it binds nothing. With
    /// no cache facts, the NUMA nodes are the domains.
    Domain,
    /// Pins each worker to one CPU: one CPU of each physical core first, and the SMT siblings
    /// last, spread over the NUMA nodes and then over the cache domains of each node. Use it for a
    /// host, or a CPU set, that the process owns: a pinned worker cannot leave a busy core.
    Dedicated,
    /// Pins worker `i` to CPU `cpus[i]`.
    Exact(Box<[u32]>),
    /// Binds nothing.
    Unpinned,
}

/// Whether a placement can fall back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Selection {
    /// A plan that falls back refuses before a worker starts, and a worker whose placement the
    /// system refuses fails its start.
    Strict,
    /// A worker whose plan falls back runs unpinned, and its report gives the [`Fallback`].
    Permissive,
}

/// The placement that one worker asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Request {
    /// [`Placement::Domain`].
    Domain,
    /// [`Placement::Dedicated`].
    Dedicated,
    /// [`Placement::Exact`], with the CPU of this worker.
    Exact(u32),
    /// [`Placement::Unpinned`].
    Unpinned,
}

/// Why a worker runs unpinned although it asked for a binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fallback {
    /// The host has one cache domain, so `Domain` binds nothing.
    OneDomain,
    /// The host gave no CPU set or no topology.
    NoTopology,
    /// The CPU is not in the CPU set of the process.
    NotAllowed(u32),
    /// The system refused the binding, with this error code.
    Refused(i32),
}

/// The requested, resolved, and applied placement of one worker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placed {
    pub(super) request: Request,
    pub(super) target: Option<CpuSet>,
    pub(super) applied: Option<CpuSet>,
    pub(super) fallback: Option<Fallback>,
    pub(super) node: Option<u32>,
}

impl Placed {
    /// What the worker asked for.
    #[must_use]
    pub const fn request(&self) -> Request {
        self.request
    }

    /// The CPUs that the plan resolved for the worker, or `None` for no binding.
    #[must_use]
    pub const fn resolved(&self) -> Option<&CpuSet> {
        self.target.as_ref()
    }

    /// The CPUs that the thread of the worker runs on, as the system reports them after the
    /// binding, or `None` if the worker runs unpinned.
    #[must_use]
    pub const fn applied(&self) -> Option<&CpuSet> {
        self.applied.as_ref()
    }

    /// Why the worker runs unpinned although it asked for a binding.
    #[must_use]
    pub const fn fallback(&self) -> Option<Fallback> {
        self.fallback
    }

    /// The NUMA node of each CPU of the binding, if they share one. The worker allocates its
    /// tables and its driver after the binding, so their memory is on this node.
    #[must_use]
    pub const fn node(&self) -> Option<u32> {
        self.node
    }
}

/// Whether the workers of a plan can outnumber the CPUs of the process
/// ([`Builder::oversubscription`](super::Builder::oversubscription)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Oversubscription {
    /// A plan with more workers than CPUs refuses with [`PlanError::Oversubscribed`].
    #[default]
    Refuse,
    /// The workers share the CPUs, and the kernel divides their time. A worker then waits for
    /// its CPU, which adds to the latency of each of its tasks.
    Allow,
}

/// Why a plan is invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlanError {
    /// The task slots are 0, or more than 2^16.
    TaskSlots,
    /// More workers than [`super::MAX_WORKERS`].
    TooManyWorkers,
    /// `Dedicated` or `Exact` has fewer CPUs than workers.
    TooFewCpus {
        /// The workers.
        workers: u32,
        /// The CPUs.
        cpus: u32,
    },
    /// `Exact` names this CPU two times.
    DuplicateCpu(u32),
    /// The local and the movable workers outnumber the CPUs that the process can use: the CPUs of
    /// its CPU set, and at most the CPU quota of its cgroup, rounded up. Give fewer workers, or
    /// [`Oversubscription::Allow`].
    Oversubscribed {
        /// The workers.
        workers: u32,
        /// The CPUs.
        cpus: u32,
    },
    /// `Exact` names a CPU that is not below [`MAX_CPUS`].
    NoSuchCpu(u32),
    /// The selection is strict, and the plan of this worker falls back.
    Refused {
        /// The worker.
        worker: u32,
        /// Its fallback.
        fallback: Fallback,
    },
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TaskSlots => f.write_str("an owner has from 1 to 2^16 task slots"),
            Self::TooManyWorkers => write!(f, "a runtime has at most {} workers", super::MAX_WORKERS),
            Self::TooFewCpus { workers, cpus } => write!(f, "{workers} workers need more than {cpus} CPUs"),
            Self::DuplicateCpu(cpu) => write!(f, "the CPU list names CPU {cpu} two times"),
            Self::Oversubscribed { workers, cpus } => {
                write!(f, "{workers} workers outnumber the {cpus} CPUs of the process")
            }
            Self::NoSuchCpu(cpu) => write!(f, "CPU {cpu} is not below {MAX_CPUS}"),
            Self::Refused { worker, fallback } => {
                write!(f, "the strict placement of worker {worker} falls back: {fallback:?}")
            }
        }
    }
}

impl core::error::Error for PlanError {}

/// Resolves `placement` for `workers` workers on `host`: the plans, or [`Cause::Plan`], or
/// [`Cause::Allocation`] when the allocator refuses the tables of the plans. Each plan pushes into
/// the room of `workers` plans.
pub(super) fn resolve(
    placement: &Placement,
    selection: Selection,
    workers: u32,
    host: &Host,
) -> Result<Vec<Placed>, Cause> {
    let refused = |_| Cause::Allocation;
    let count = workers as usize;
    let mut plans = room(count).map_err(refused)?;
    match placement {
        Placement::Unpinned => plans.extend((0..count).map(|_| unpinned(Request::Unpinned, None))),
        Placement::Domain => domain(host, count, &mut plans).map_err(refused)?,
        Placement::Dedicated if host.allowed.is_none() => {
            plans.extend((0..count).map(|_| unpinned(Request::Dedicated, Some(Fallback::NoTopology))));
        }
        Placement::Dedicated => {
            let order = dedicated(host).map_err(refused)?;
            if order.len() < count {
                return Err(Cause::Plan(PlanError::TooFewCpus {
                    workers,
                    cpus: u32::try_from(order.len()).unwrap_or(u32::MAX),
                }));
            }
            plans.extend(
                order
                    .iter()
                    .take(count)
                    .map(|cpu| pinned(Request::Dedicated, *cpu, host)),
            );
        }
        Placement::Exact(cpus) => {
            if cpus.len() < count {
                return Err(Cause::Plan(PlanError::TooFewCpus {
                    workers,
                    cpus: u32::try_from(cpus.len()).unwrap_or(u32::MAX),
                }));
            }
            for (index, cpu) in cpus.iter().copied().enumerate().take(count) {
                if cpu >= MAX_CPUS {
                    return Err(Cause::Plan(PlanError::NoSuchCpu(cpu)));
                }
                if cpus[..index].contains(&cpu) {
                    return Err(Cause::Plan(PlanError::DuplicateCpu(cpu)));
                }
                plans.push(match &host.allowed {
                    Some(allowed) if !allowed.contains(cpu) => {
                        unpinned(Request::Exact(cpu), Some(Fallback::NotAllowed(cpu)))
                    }
                    _ => pinned(Request::Exact(cpu), cpu, host),
                });
            }
        }
    }
    if selection == Selection::Strict
        && let Some((worker, fallback)) = (0..)
            .zip(&plans)
            .find_map(|(worker, plan)| Some((worker, plan.fallback?)))
    {
        return Err(Cause::Plan(PlanError::Refused { worker, fallback }));
    }
    Ok(plans)
}

/// The CPUs that the process can use for its time: the CPUs of its CPU set, and at most its CPU
/// quota, rounded up. `None` when the host gave no CPU set.
fn usable(host: &Host) -> Option<u32> {
    let cpus = host.allowed.as_ref()?.len();
    Some(match host.quota {
        Quota::Millis(millis) => cpus.min(u32::try_from(millis.div_ceil(1000)).unwrap_or(u32::MAX).max(1)),
        Quota::None | Quota::Unknown => cpus,
    })
}

/// Refuses `workers` workers that outnumber the CPUs that the process can use, unless
/// `oversubscription` allows it. A host with no CPU set gives no count, so nothing refuses.
pub(super) fn check(workers: u32, host: &Host, oversubscription: Oversubscription) -> Result<(), PlanError> {
    match usable(host) {
        Some(cpus) if workers > cpus && oversubscription == Oversubscription::Refuse => {
            Err(PlanError::Oversubscribed { workers, cpus })
        }
        _ => Ok(()),
    }
}

/// For each plan, the CPUs of its worker that another worker of the plans can also run on. An
/// unpinned worker can run on each CPU of the process. Empty when the host gave no CPU set.
pub(super) fn shared(plans: &[Placed], host: &Host) -> Result<Vec<CpuSet>, AllocError> {
    let all = host.allowed.clone().unwrap_or_default();
    let runs_on = |plan: &Placed| plan.target.as_ref().unwrap_or(&all).clone();
    let mut workers = filled(MAX_CPUS as usize, || 0_u32)?;
    for plan in plans {
        for cpu in runs_on(plan).iter() {
            if let Some(count) = workers.get_mut(cpu as usize) {
                *count = count.saturating_add(1);
            }
        }
    }
    let mut shared = room(plans.len())?;
    shared.extend(plans.iter().map(|plan| {
        let mut cpus = CpuSet::new();
        for cpu in runs_on(plan).iter() {
            if workers.get(cpu as usize).is_some_and(|count| *count > 1) {
                cpus.insert(cpu);
            }
        }
        cpus
    }));
    Ok(shared)
}

fn unpinned(request: Request, fallback: Option<Fallback>) -> Placed {
    Placed {
        request,
        target: None,
        applied: None,
        fallback,
        node: None,
    }
}

fn pinned(request: Request, cpu: u32, host: &Host) -> Placed {
    let mut target = CpuSet::new();
    target.insert(cpu);
    Placed {
        request,
        node: host.node_of(&target),
        target: Some(target),
        applied: None,
        fallback: None,
    }
}

/// Binds each worker to one domain, which the domain with the fewest workers for each of its
/// physical cores gets, the first of them on a tie.
fn domain(host: &Host, count: usize, plans: &mut Vec<Placed>) -> Result<(), AllocError> {
    let Some(allowed) = &host.allowed else {
        plans.extend((0..count).map(|_| unpinned(Request::Domain, Some(Fallback::NoTopology))));
        return Ok(());
    };
    let domains = host.domains_of(allowed)?;
    if domains.len() < 2 {
        let fallback = if domains.is_empty() {
            Fallback::NoTopology
        } else {
            Fallback::OneDomain
        };
        plans.extend((0..count).map(|_| unpinned(Request::Domain, Some(fallback))));
        return Ok(());
    }
    let mut cores = room(domains.len())?;
    cores.extend(
        domains
            .iter()
            .map(|(_, domain)| u64::from(host.cores_in(domain).max(1))),
    );
    let mut assigned = filled(domains.len(), || 0_u64)?;
    for _ in 0..count {
        let mut best = 0;
        for candidate in 1..domains.len() {
            // `assigned / cores` is smaller for the candidate, in exact integer form.
            if (assigned[candidate] + 1) * cores[best] < (assigned[best] + 1) * cores[candidate] {
                best = candidate;
            }
        }
        assigned[best] += 1;
        let target = domains[best].1.clone();
        plans.push(Placed {
            request: Request::Domain,
            node: host.node_of(&target),
            target: Some(target),
            applied: None,
            fallback: None,
        });
    }
    Ok(())
}

/// The CPUs of `Dedicated` in the order of the workers: one CPU of each physical core, spread over
/// the nodes and then over the domains of each node, and then the next sibling of each core in the
/// same order.
fn dedicated(host: &Host) -> Result<Vec<u32>, AllocError> {
    let Some(allowed) = &host.allowed else {
        return Ok(Vec::new());
    };
    let mut nodes: Vec<Node> = Vec::new();
    let mut seen = CpuSet::new();
    for cpu in allowed.iter() {
        if seen.contains(cpu) {
            continue;
        }
        let core = host.core_of(cpu).map_or_else(
            || {
                let mut core = CpuSet::new();
                core.insert(cpu);
                core
            },
            |core| core.and(allowed),
        );
        let mut siblings = room(core.len() as usize)?;
        siblings.extend(core.iter());
        for sibling in &siblings {
            seen.insert(*sibling);
        }
        let node = host.node.get(cpu as usize).copied().flatten();
        let domain = host.domain.get(cpu as usize).copied().flatten();
        let at = match nodes.iter().position(|(id, _)| *id == node) {
            Some(at) => at,
            None => {
                push(&mut nodes, (node, Vec::new()))?;
                nodes.len() - 1
            }
        };
        let domains = &mut nodes[at].1;
        match domains.iter_mut().find(|(id, _)| *id == domain) {
            Some((_, cores)) => push(cores, siblings)?,
            None => {
                let mut cores = room(1)?;
                cores.push(siblings);
                push(domains, (domain, cores))?;
            }
        }
    }
    let mut spread = room(nodes.len())?;
    for (_, domains) in nodes {
        let mut cores = room(domains.len())?;
        cores.extend(domains.into_iter().map(|(_, cores)| cores));
        spread.push(interleave(cores)?);
    }
    let cores = interleave(spread)?;
    let rounds = cores.iter().map(Vec::len).max().unwrap_or(0);
    let mut order = room(cores.iter().map(Vec::len).sum())?;
    order.extend((0..rounds).flat_map(|round| cores.iter().filter_map(move |core| core.get(round).copied())));
    Ok(order)
}

/// The CPUs of one physical core, in ascending order.
type Core = Vec<u32>;
/// The cores of one cache domain, with the label of the domain.
type Domain = (Option<u32>, Vec<Core>);
/// The domains of one NUMA node, with the node.
type Node = (Option<u32>, Vec<Domain>);

/// The items of `groups` in turn: the first of each group, then the second of each, and so on.
fn interleave<T>(mut groups: Vec<Vec<T>>) -> Result<Vec<T>, AllocError> {
    let total = groups.iter().map(Vec::len).sum();
    let mut items = room(total)?;
    // Each group gives its items from its end, so it turns around first.
    groups.iter_mut().for_each(|group| group.reverse());
    // Each pass takes at least one item, so the loop ends after at most `total` passes.
    while items.len() < total {
        items.extend(groups.iter_mut().filter_map(Vec::pop));
    }
    Ok(items)
}
