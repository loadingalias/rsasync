//! The facts of the host that the placement uses, from the CPU set of the process
//! (`sched_getaffinity`), `/sys/devices/system`, and the CPU limit of the cgroup.
//!
//! A fact that the host does not give stays unknown, and the report says so: an unknown fact is
//! not the same as no limit. Each read is one small file, and the discovery reads the file of a
//! core, a cache domain, or a node one time, not one time for each of its CPUs.

use alloc::string::String;
use alloc::vec::Vec;
use std::io::Read as _;

use super::placement::{CpuSet, MAX_CPUS};
use super::sys;

/// The most bytes of one file of `/sys` or `/proc` that the discovery reads. A list of 4096 CPUs
/// in its longest form is about 20 KiB.
const READ_LIMIT: u64 = 64 << 10;

/// The most cache indexes of one CPU that the discovery reads.
const CACHE_INDEXES: u32 = 16;

/// The most levels of the cgroup tree that the discovery reads for the CPU limit.
const CGROUP_DEPTH: usize = 64;

/// The CPU limit of the cgroup of the process (`cpu.max` of cgroup v2), the smallest on its path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Quota {
    /// No level of the path limits the CPU time.
    None,
    /// The process can use this many thousandths of a CPU in each period.
    Millis(u64),
    /// The host gave no cgroup v2 limit to read.
    Unknown,
}

/// The facts of the host that a start read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    pub(super) online: Option<CpuSet>,
    pub(super) allowed: Option<CpuSet>,
    pub(super) isolated: Option<CpuSet>,
    /// For each CPU index: the first CPU of its physical core.
    pub(super) core: Vec<Option<u32>>,
    /// For each CPU index: the first CPU of the CPUs that share its last-level cache.
    pub(super) domain: Vec<Option<u32>>,
    /// For each CPU index: its NUMA node.
    pub(super) node: Vec<Option<u32>>,
    pub(super) quota: Quota,
}

impl Host {
    /// The CPUs that are online, or `None` if the host did not say.
    #[must_use]
    pub const fn online(&self) -> Option<&CpuSet> {
        self.online.as_ref()
    }

    /// The CPU set of the process, which each worker starts with, or `None` if the system did not
    /// give it.
    #[must_use]
    pub const fn allowed(&self) -> Option<&CpuSet> {
        self.allowed.as_ref()
    }

    /// The CPUs that the kernel isolates from its scheduler (`isolcpus`), or `None` if the host did
    /// not say.
    #[must_use]
    pub const fn isolated(&self) -> Option<&CpuSet> {
        self.isolated.as_ref()
    }

    /// The physical cores of the CPU set of the process, or `None` if a CPU has no core fact. More
    /// CPUs than cores means SMT.
    #[must_use]
    pub fn cores(&self) -> Option<u32> {
        let allowed = self.allowed.as_ref()?;
        distinct(allowed, &self.core)
    }

    /// The last-level cache domains of the CPU set of the process, or `None` if a CPU has no cache
    /// fact.
    #[must_use]
    pub fn domains(&self) -> Option<u32> {
        let allowed = self.allowed.as_ref()?;
        distinct(allowed, &self.domain)
    }

    /// The NUMA nodes of the CPU set of the process, or `None` if a CPU has no node fact.
    #[must_use]
    pub fn nodes(&self) -> Option<u32> {
        let allowed = self.allowed.as_ref()?;
        distinct(allowed, &self.node)
    }

    /// The CPU limit of the cgroup of the process.
    #[must_use]
    pub const fn quota(&self) -> Quota {
        self.quota
    }

    /// Whether the facts show a CPU set that the process owns: the operator gave the process fewer
    /// CPUs than are online, and no cgroup limits its CPU time. A Kubernetes pod with exclusive
    /// CPUs, or a service under `taskset`, shows this. [`Placement::Dedicated`] then pins one
    /// worker to each CPU of the set. Several processes that share one restricted set also show
    /// it, and only the operator can tell them apart.
    ///
    /// [`Placement::Dedicated`]: super::Placement::Dedicated
    #[must_use]
    pub fn shows_dedication(&self) -> bool {
        matches!((&self.allowed, &self.online), (Some(allowed), Some(online)) if allowed != online)
            && self.quota == Quota::None
    }

    /// The CPUs of the physical core of `cpu`, if the host gave its core.
    pub(super) fn core_of(&self, cpu: u32) -> Option<CpuSet> {
        let label = self.core.get(cpu as usize).copied().flatten()?;
        Some(labelled(&self.core, label))
    }

    /// The cache domains of the CPUs of `allowed`, each with its CPUs of `allowed`. With no cache
    /// fact for a CPU, the NUMA nodes are the domains. With no node fact either, it is empty.
    pub(super) fn domains_of(&self, allowed: &CpuSet) -> Vec<CpuSet> {
        let labels = if covers(allowed, &self.domain) {
            &self.domain
        } else if covers(allowed, &self.node) {
            &self.node
        } else {
            return Vec::new();
        };
        let mut domains: Vec<(u32, CpuSet)> = Vec::new();
        for cpu in allowed.iter() {
            let Some(label) = labels.get(cpu as usize).copied().flatten() else {
                continue;
            };
            match domains.iter_mut().find(|(id, _)| *id == label) {
                Some((_, set)) => {
                    set.insert(cpu);
                }
                None => {
                    let mut set = CpuSet::new();
                    set.insert(cpu);
                    domains.push((label, set));
                }
            }
        }
        domains.into_iter().map(|(_, set)| set).collect()
    }

    /// The physical cores of the CPUs of `set`. A CPU with no core fact counts as one core.
    pub(super) fn cores_in(&self, set: &CpuSet) -> u32 {
        let mut cores = CpuSet::new();
        for cpu in set.iter() {
            // A labelled core has its first CPU as its label, so no other CPU has that number.
            cores.insert(self.core.get(cpu as usize).copied().flatten().unwrap_or(cpu));
        }
        cores.len()
    }

    /// The NUMA node of each CPU of `set`, if they share one.
    pub(super) fn node_of(&self, set: &CpuSet) -> Option<u32> {
        let mut nodes = set.iter().map(|cpu| self.node.get(cpu as usize).copied().flatten());
        let first = nodes.next()??;
        nodes.all(|node| node == Some(first)).then_some(first)
    }
}

/// The distinct labels of the CPUs of `set`, or `None` if a CPU has no label.
fn distinct(set: &CpuSet, labels: &[Option<u32>]) -> Option<u32> {
    let mut seen = CpuSet::new();
    for cpu in set.iter() {
        seen.insert(labels.get(cpu as usize).copied().flatten()?);
    }
    Some(seen.len())
}

/// Whether each CPU of `set` has a label.
fn covers(set: &CpuSet, labels: &[Option<u32>]) -> bool {
    set.iter()
        .all(|cpu| labels.get(cpu as usize).copied().flatten().is_some())
}

/// The CPUs with `label`.
fn labelled(labels: &[Option<u32>], label: u32) -> CpuSet {
    let mut set = CpuSet::new();
    for cpu in (0..)
        .zip(labels)
        .filter_map(|(cpu, each)| (*each == Some(label)).then_some(cpu))
    {
        set.insert(cpu);
    }
    set
}

/// Reads the facts of this host.
pub(super) fn discover() -> Host {
    let mut buffer = Vec::new();
    let allowed = sys::affinity().ok();
    let online = read(&mut buffer, "/sys/devices/system/cpu/online").and_then(cpu_list);
    let isolated = read(&mut buffer, "/sys/devices/system/cpu/isolated").and_then(cpu_list);
    let size = allowed
        .as_ref()
        .and_then(|allowed| allowed.iter().last())
        .map_or(0, |last| last as usize + 1);
    let mut core = alloc::vec![None; size];
    let mut domain = alloc::vec![None; size];
    let mut node = alloc::vec![None; size];
    if let Some(allowed) = &allowed {
        for cpu in allowed.iter() {
            if core[cpu as usize].is_none() {
                let path = alloc::format!("/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list");
                label(&mut core, read(&mut buffer, &path).and_then(cpu_list));
            }
            if domain[cpu as usize].is_none() {
                label(&mut domain, last_level_cache(&mut buffer, cpu));
            }
        }
        if let Some(nodes) = read(&mut buffer, "/sys/devices/system/node/online").and_then(cpu_list) {
            for id in nodes.iter() {
                let path = alloc::format!("/sys/devices/system/node/node{id}/cpulist");
                if let Some(cpus) = read(&mut buffer, &path).and_then(cpu_list) {
                    for cpu in cpus.iter() {
                        if let Some(entry) = node.get_mut(cpu as usize) {
                            *entry = Some(id);
                        }
                    }
                }
            }
        }
    }
    Host {
        online,
        allowed,
        isolated,
        core,
        domain,
        node,
        quota: quota(&mut buffer),
    }
}

/// Gives each CPU of `set` the first CPU of `set` as its label.
fn label(labels: &mut [Option<u32>], set: Option<CpuSet>) {
    let Some(set) = set else {
        return;
    };
    let Some(first) = set.iter().next() else {
        return;
    };
    for cpu in set.iter() {
        if let Some(entry) = labels.get_mut(cpu as usize) {
            *entry = Some(first);
        }
    }
}

/// The CPUs that share the cache of the highest level of `cpu`, other than an instruction cache.
fn last_level_cache(buffer: &mut Vec<u8>, cpu: u32) -> Option<CpuSet> {
    let mut highest: Option<(u32, u32)> = None;
    for index in 0..CACHE_INDEXES {
        let base = alloc::format!("/sys/devices/system/cpu/cpu{cpu}/cache/index{index}");
        let Some(level) = read(buffer, &alloc::format!("{base}/level")).and_then(|text| text.parse::<u32>().ok())
        else {
            break;
        };
        if read(buffer, &alloc::format!("{base}/type")).is_some_and(|kind| kind == "Instruction") {
            continue;
        }
        if highest.is_none_or(|(top, _)| level > top) {
            highest = Some((level, index));
        }
    }
    let (_, index) = highest?;
    let path = alloc::format!("/sys/devices/system/cpu/cpu{cpu}/cache/index{index}/shared_cpu_list");
    read(buffer, &path).and_then(cpu_list)
}

/// The smallest CPU limit on the cgroup v2 path of the process.
fn quota(buffer: &mut Vec<u8>) -> Quota {
    let Some(path) = read(buffer, "/proc/self/cgroup")
        .and_then(|groups| groups.lines().find_map(|line| line.strip_prefix("0::")))
        .map(String::from)
    else {
        return Quota::Unknown;
    };
    let mut directory = alloc::format!("/sys/fs/cgroup{}", path.trim_end_matches('/'));
    let mut smallest = None;
    let mut read_any = false;
    for _ in 0..CGROUP_DEPTH {
        if let Some(limit) = read(buffer, &alloc::format!("{directory}/cpu.max")) {
            read_any = true;
            let mut fields = limit.split_whitespace();
            if let (Some(max), Some(period)) = (fields.next(), fields.next())
                && let (Ok(max), Ok(period)) = (max.parse::<u64>(), period.parse::<u64>())
                && let Some(millis) = max.checked_mul(1000).and_then(|max| max.checked_div(period))
            {
                smallest = Some(smallest.map_or(millis, |smallest: u64| smallest.min(millis)));
            }
        }
        if directory == "/sys/fs/cgroup" {
            break;
        }
        match directory.rsplit_once('/') {
            Some((parent, _)) => directory.truncate(parent.len()),
            None => break,
        }
    }
    match (smallest, read_any) {
        (Some(millis), _) => Quota::Millis(millis),
        (None, true) => Quota::None,
        (None, false) => Quota::Unknown,
    }
}

/// The trimmed text of the file at `path`, or `None` if it is missing, larger than `READ_LIMIT`,
/// or not text.
fn read<'a>(buffer: &'a mut Vec<u8>, path: &str) -> Option<&'a str> {
    buffer.clear();
    let file = std::fs::File::open(path).ok()?;
    file.take(READ_LIMIT + 1).read_to_end(buffer).ok()?;
    if buffer.len() as u64 > READ_LIMIT {
        return None;
    }
    core::str::from_utf8(buffer).ok().map(str::trim)
}

/// A CPU list of Linux, such as `0-3,8`, or `None` if it is malformed or names a CPU that is not
/// below `MAX_CPUS`.
fn cpu_list(text: &str) -> Option<CpuSet> {
    let mut set = CpuSet::new();
    for part in text.split(',').map(str::trim).filter(|part| !part.is_empty()) {
        let (first, last) = match part.split_once('-') {
            Some((first, last)) => (first.parse::<u32>().ok()?, last.parse::<u32>().ok()?),
            None => {
                let cpu = part.parse::<u32>().ok()?;
                (cpu, cpu)
            }
        };
        if first > last || last >= MAX_CPUS {
            return None;
        }
        for cpu in first..=last {
            set.insert(cpu);
        }
    }
    Some(set)
}
