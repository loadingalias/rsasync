use alloc::vec::Vec;
use core::alloc::Layout;
use core::future::Future as _;
use core::num::{NonZeroU32, NonZeroU64};
use core::pin::Pin;
#[cfg(target_os = "linux")]
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use super::super::suite::{self, Backend};
#[cfg(target_os = "linux")]
use super::super::tests::driver;
#[cfg(any(target_os = "linux", target_os = "windows"))]
use super::super::tests::held;
use super::super::tests::{Log, drive, hold, note, numbered, posted, recorded, taken};
use super::super::{Call, Config, Delivery, Driver, Reason, Route};
use super::{Buffers, Cache, File, Incomplete, Length, OpenOptions, SyncMode, WriteMode};
use crate::blocking::{self, Service};
use crate::buffer::{Backing, Buffer as _, Lease, Pool, WriteLoan};
use crate::capacity::Budget;
use crate::driver::Regions;

/// A new empty directory for one test, next to the test binary, so on the file system of the
/// build and not on a `tmpfs`. Its drop removes it.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let mut path = std::env::current_exe().expect("the test knows its binary");
        path.pop();
        path.push(alloc::format!("rsasync-file-{name}-{}", std::process::id()));
        drop(fs::remove_dir_all(&path));
        fs::create_dir_all(&path).expect("the test makes its directory");
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}

/// Opens `path` on the driver with `options`.
fn open(driver: &mut Driver, path: &Path, options: OpenOptions) -> File {
    let open = driver.open_file(path, options).expect("the test driver has room");
    drive(driver, open)
        .expect("the driver runs")
        .expect("the test file opens")
}

/// Read and write, and made when it does not exist.
fn writable() -> OpenOptions {
    OpenOptions::new().write(true).create(true)
}

/// A backing with `bytes` as its initialized bytes and `capacity` bytes in all.
fn filled(bytes: &[u8], capacity: usize) -> Backing {
    let mut loan = WriteLoan::new(Backing::from(Vec::with_capacity(capacity)));
    assert_eq!(loan.fill_from(bytes), bytes.len());
    loan.into_inner()
}

fn write(driver: &mut Driver, file: &File, bytes: &[u8], offset: u64) {
    let write = driver
        .write_at(file, Lease::from(bytes.to_vec()), offset, WriteMode::Plain)
        .expect("the test driver has room");
    let written = drive(driver, write).expect("the driver runs").0;
    assert_eq!(written.expect("the write works"), bytes.len());
}

fn file_len(driver: &mut Driver, file: &File) -> u64 {
    let len = driver.file_len(file).expect("the test driver has room");
    drive(driver, len).expect("the driver runs").expect("the statx works")
}

fn sync(driver: &mut Driver, file: &File) {
    let sync = driver.sync(file, SyncMode::All).expect("the test driver has room");
    drive(driver, sync).expect("the driver runs").expect("the sync works");
}

/// The bytes that the file system allocated for the file at `path`: `st_blocks`, which counts
/// 512-byte units.
#[cfg(unix)]
fn allocated(path: &Path) -> u64 {
    fs::metadata(path).expect("the test file exists").blocks() * 512
}

/// The bytes that the file system allocated for the file at `path`: the `AllocationSize` of
/// `FILE_STANDARD_INFO`, which counts the clusters of a sparse file that hold data.
#[cfg(target_os = "windows")]
#[expect(unsafe_code, reason = "the call writes one structure of the test")]
fn allocated(path: &Path) -> u64 {
    use std::os::windows::io::AsRawHandle as _;

    use super::super::iocp::sys;

    let file = fs::File::open(path).expect("the test file exists");
    let mut info = sys::FileStandardInfo::default();
    let len = u32::try_from(size_of::<sys::FileStandardInfo>()).unwrap();
    // SAFETY: `info` holds `len` bytes, and the call writes at most that many.
    let read = unsafe {
        sys::GetFileInformationByHandleEx(
            file.as_raw_handle(),
            sys::FILE_STANDARD_INFO,
            core::ptr::from_mut(&mut info).cast(),
            len,
        )
    };
    assert_ne!(read, 0, "{}", io::Error::last_os_error());
    u64::try_from(info.allocation).unwrap()
}

/// A read gives the exact count of the bytes that it put in, fewer at the end of the file and 0
/// after it. The exact read of the same range reports that it is incomplete, with that count.
pub(in crate::driver) fn a_short_read_gives_its_exact_extent_and_the_exact_read_reports_incomplete_progress(
    backend: Backend,
) {
    let scratch = Scratch::new("short");
    fs::write(scratch.join("data"), numbered(0, 10)).unwrap();
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), OpenOptions::new());

    let read = driver.read_at(&file, Backing::from(Vec::with_capacity(64)), 4).unwrap();
    let (count, buffer) = drive(&mut driver, read).unwrap();
    assert_eq!(count.unwrap(), 6);
    assert_eq!(buffer.bytes(), &numbered(0, 10)[4..]);
    let read = driver
        .read_at(&file, Backing::from(Vec::with_capacity(64)), 10)
        .unwrap();
    assert_eq!(drive(&mut driver, read).unwrap().0.unwrap(), 0);

    let exact = driver
        .read_exact_at(&file, Backing::from(Vec::with_capacity(64)), 0)
        .unwrap();
    let (result, buffer) = drive(&mut driver, exact).unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    let incomplete = error.get_ref().and_then(|inner| inner.downcast_ref::<Incomplete>());
    assert_eq!(incomplete.map(|incomplete| incomplete.read()), Some(10));
    assert_eq!(buffer.bytes(), numbered(0, 10));

    let exact = driver.read_exact_at(&file, filled(b"x", 11), 0).unwrap();
    let (result, buffer) = drive(&mut driver, exact).unwrap();
    result.unwrap();
    assert_eq!(&buffer.bytes()[1..], numbered(0, 10));
}

/// Two reads of one file that start in one turn each end with their own bytes: the end of one does
/// not hide the end of the other. A read of a file in the cache ends in the turn that submits it,
/// so on IoRing both completion queue entries wait before one packet of the ring.
pub(in crate::driver) fn two_reads_started_in_one_turn_each_end_with_their_bytes(backend: Backend) {
    /// The turns, 1 ms apart, in which both reads end: a read in the cache ends in its first turn,
    /// and a job of the blocking service in a few.
    const TURNS: usize = 1000;
    let scratch = Scratch::new("together");
    fs::write(scratch.join("data"), numbered(0, 8192)).unwrap();
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), OpenOptions::new());
    let mut reads = [0, 4096].map(|offset| {
        driver
            .read_at(&file, Backing::from(Vec::with_capacity(4096)), offset)
            .unwrap()
    });
    let mut ended = [None, None];
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..TURNS {
        driver.turn().unwrap();
        for (read, ended) in reads.iter_mut().zip(&mut ended) {
            if ended.is_none()
                && let Poll::Ready(output) = Pin::new(read).poll(&mut cx)
            {
                *ended = Some(output);
            }
        }
        if ended.iter().all(Option::is_some) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let [Some(first), Some(second)] = ended else {
        let count = ended.iter().filter(|ended| ended.is_some()).count();
        panic!("{count} of 2 reads ended in {TURNS} turns");
    };
    for ((count, buffer), offset) in [first, second].map(Result::unwrap).into_iter().zip([0, 4096]) {
        assert_eq!(count.unwrap(), 4096);
        assert_eq!(buffer.bytes(), numbered(offset, 4096));
    }
}

/// A direct-I/O file refuses a buffer, a length, or an offset that does not have its alignment,
/// before the submission, and gives the buffer back. An aligned read runs.
pub(in crate::driver) fn a_misaligned_direct_io_buffer_refuses_before_submission(backend: Backend) {
    // 256 KiB holds a read of four blocks after the first block, also with pages of 16 KiB.
    const LEN: usize = 1 << 18;
    let scratch = Scratch::new("direct");
    fs::write(scratch.join("data"), numbered(0, LEN)).unwrap();
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(
        &mut driver,
        &scratch.join("data"),
        OpenOptions::new().cache(Cache::Direct),
    );
    let alignment = file.alignment().expect("a direct-I/O file has an alignment");
    let (memory, offset) = (alignment.memory() as usize, alignment.offset() as usize);
    let block = memory.max(offset);
    let budget = Budget::new(NonZeroU64::new(1 << 20).unwrap());
    let layout = Layout::from_size_align(4 * block, block).unwrap();
    let mut pool = Pool::new(&budget, NonZeroU32::new(1).unwrap(), layout).unwrap();
    let buffer = pool.acquire().unwrap();

    // One byte at the start moves the read one byte past an aligned address, and its length is
    // one byte short.
    let mut loan = WriteLoan::new(buffer);
    loan.fill_from(&[0]);
    let refused = driver.read_at(&file, loan.into_inner(), 0).unwrap_err();
    assert_eq!(refused.reason(), Reason::Alignment);
    assert_eq!(refused.into_inner().bytes(), [0]);

    let refused = driver.read_at(&file, pool.acquire().unwrap(), 1).unwrap_err();
    assert_eq!(refused.reason(), Reason::Alignment);
    let read = driver.read_at(&file, refused.into_inner(), offset as u64).unwrap();
    let (count, buffer) = drive(&mut driver, read).unwrap();
    assert_eq!(count.unwrap(), 4 * block);
    assert_eq!(buffer.bytes(), &numbered(0, LEN)[offset..offset + 4 * block]);
}

/// The kernel release, as `(major, minor)`.
#[cfg(target_os = "linux")]
fn kernel() -> (u32, u32) {
    let release = fs::read_to_string("/proc/sys/kernel/osrelease").expect("Linux gives its release");
    let mut parts = release.trim().split(['.', '-']);
    let mut next = || {
        parts
            .next()
            .and_then(|part| part.parse().ok())
            .expect("a release starts with its major and minor numbers")
    };
    (next(), next())
}

/// The status of this thread, at a path that another thread can read.
#[cfg(target_os = "linux")]
fn thread_status() -> PathBuf {
    let link = fs::read_link("/proc/thread-self").expect("Linux names the thread");
    let thread = link.file_name().expect("the link ends with the thread");
    Path::new("/proc/self/task").join(thread).join("status")
}

/// The sleeps in the kernel of the thread of `status`: its voluntary context switches.
#[cfg(target_os = "linux")]
fn sleeps(status: &Path) -> u64 {
    fs::read_to_string(status)
        .expect("Linux gives the status of a thread")
        .lines()
        .find_map(|line| line.strip_prefix("voluntary_ctxt_switches:"))
        .and_then(|count| count.trim().parse().ok())
        .expect("the status counts the voluntary context switches")
}

/// Waits in `driver`, and gives the sleeps of the wait in the kernel. Another thread wakes the
/// wait when it has slept `slept` times, or after `at_most`, so the count does not depend on the
/// speed of the host. The wait must end at that wake, not before it.
#[cfg(target_os = "linux")]
fn sleeps_of_a_wait(driver: &mut Driver, slept: u64, at_most: Duration) -> u64 {
    let status = thread_status();
    let before = sleeps(&status);
    let woken = Arc::new(AtomicBool::new(false));
    let waker = {
        let (status, woken, wake) = (status.clone(), Arc::clone(&woken), driver.wake_handle());
        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            while sleeps(&status) - before < slept && start.elapsed() < at_most {
                std::thread::sleep(Duration::from_micros(100));
            }
            woken.store(true, Ordering::SeqCst);
            wake.wake();
        })
    };
    driver.wait().expect("the wait of the test driver works");
    let (ended_at_its_wake, slept) = (woken.load(Ordering::SeqCst), sleeps(&status) - before);
    waker.join().expect("the waker ends");
    assert!(
        ended_at_its_wake,
        "the wait ended after {slept} sleeps, before its wake"
    );
    slept
}

/// Holds the one job slot of a blocking service until its drop.
struct Hold(Arc<(Mutex<bool>, Condvar)>, blocking::Waiter<()>);

impl Hold {
    fn new(service: &Service) -> Self {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let job = {
            let gate = Arc::clone(&gate);
            move || {
                let open = gate.0.lock().expect("no test thread panics with the gate");
                drop(
                    gate.1
                        .wait_while(open, |open| !*open)
                        .expect("no test thread panics with the gate"),
                );
            }
        };
        let waiter = service
            .submitter()
            .submit(job)
            .expect("the service has a free job slot");
        Self(gate, waiter)
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        *self.0.0.lock().expect("no test thread panics with the gate") = true;
        self.0.1.notify_all();
        assert_eq!(self.1.wait_blocking(), Ok(()));
    }
}

/// A length change runs on the ring from Linux 6.9 (`IORING_OP_FTRUNCATE`), and on the blocking
/// service before it, on epoll, and on kqueue, and the report names the route before the first
/// operation. On the blocking service, a driver with no service and a full service refuse. On epoll
/// and kqueue, each file operation needs the service, so the case shows only the full service.
pub(in crate::driver) fn a_length_change_runs_on_the_ring_from_linux_6_9_and_on_the_blocking_service_before(
    backend: Backend,
) {
    let scratch = Scratch::new("length");
    let mut driver = suite::driver(Config::new(), backend);
    // A readiness backend runs each file operation on the service of the case.
    #[cfg(target_os = "linux")]
    let (readiness, on_service) = (
        backend == Backend::Epoll,
        backend == Backend::Epoll || kernel() < (6, 9),
    );
    // On Windows, the length change has no overlapped form, and the open needs the service too.
    #[cfg(not(target_os = "linux"))]
    let (readiness, on_service) = (true, true);
    let route = driver.report().route(Call::SetFileLen);
    assert_eq!(route == Route::BlockingService, on_service);
    let file = open(&mut driver, &scratch.join("data"), writable());
    let budget = Budget::new(NonZeroU64::new(8 << 20).unwrap());
    let config = blocking::Config::new().threads(1).jobs(1).stack(256 << 10);
    let service = Service::new(&budget, config).unwrap();
    if on_service {
        if !readiness {
            let refused = driver.set_file_len(&file, 100).unwrap_err();
            assert_eq!(refused.reason(), Reason::Blocking);
        }
        driver.use_blocking(service.submitter());
        let hold = Hold::new(&service);
        let refused = driver.set_file_len(&file, 100).unwrap_err();
        assert_eq!(refused.reason(), Reason::Blocking);
        drop(hold);
    } else {
        // The ring changes the length with no thread, also with each job slot occupied.
        driver.use_blocking(service.submitter());
        let _hold = Hold::new(&service);
        let change = driver.set_file_len(&file, 100).unwrap();
        drive(&mut driver, change).unwrap().unwrap();
        assert_eq!(file_len(&mut driver, &file), 100);
    }
    // The waiter of the held job has its result, so the job slot is free.
    let change = driver.set_file_len(&file, 4000).unwrap();
    drive(&mut driver, change).unwrap().unwrap();
    assert_eq!(file_len(&mut driver, &file), 4000);
    assert_eq!(fs::metadata(scratch.join("data")).unwrap().len(), 4000);
    let close = driver.close_file(file).unwrap();
    drive(&mut driver, close).unwrap();
}

/// Syncs `directory`. On Windows, the report gives the sync no qualified form, and the sync gives
/// `Unsupported`.
fn sync_directory(driver: &mut Driver, directory: &super::Directory) {
    let sync = driver.sync_directory(directory).unwrap();
    let synced = drive(driver, sync).unwrap();
    #[cfg(unix)]
    synced.unwrap();
    #[cfg(target_os = "windows")]
    {
        assert_eq!(driver.report().route(Call::SyncDirectory), Route::Unsupported);
        assert_eq!(synced.unwrap_err().kind(), io::ErrorKind::Unsupported);
    }
}

/// A file and a directory of another driver refuse with `Reason::Resource` before each other check,
/// so no call reaches the slot of either on this driver. This driver has no blocking service, so a
/// length change on the service would refuse with `Reason::Blocking` first. On Windows, a directory
/// sync has no qualified form, so it would give an operation that fails with `Unsupported`.
pub(in crate::driver) fn a_file_of_another_driver_refuses_before_each_other_check(backend: Backend) {
    let scratch = Scratch::new("foreign");
    let mut first = suite::driver(Config::new(), backend);
    let mut second = super::super::tests::driver(Config::new());
    let file = open(&mut first, &scratch.join("data"), writable());
    let open = first.open_directory(&scratch.0).unwrap();
    let directory = drive(&mut first, open).unwrap().unwrap();

    assert_eq!(second.set_file_len(&file, 100).unwrap_err().reason(), Reason::Resource);
    assert_eq!(
        second.sync_directory(&directory).unwrap_err().reason(),
        Reason::Resource
    );

    let close = first.close_file(file).unwrap();
    drive(&mut first, close).unwrap();
    let close = first.close_directory(directory).unwrap();
    drive(&mut first, close).unwrap();
}

/// A rename, an unlink, and the sync of their directory complete, and the names change as they
/// say. On Windows, the sync of the directory gives `Unsupported`.
pub(in crate::driver) fn a_rename_an_unlink_and_a_directory_sync_complete(backend: Backend) {
    let scratch = Scratch::new("names");
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("first"), writable().create_new(true));
    write(&mut driver, &file, b"contents", 0);
    sync(&mut driver, &file);
    let open = driver.open_directory(&scratch.0).unwrap();
    let directory = drive(&mut driver, open).unwrap().unwrap();

    let rename = driver.rename(&scratch.join("first"), &scratch.join("second")).unwrap();
    drive(&mut driver, rename).unwrap().unwrap();
    sync_directory(&mut driver, &directory);
    assert!(!scratch.join("first").exists());
    assert_eq!(fs::read(scratch.join("second")).unwrap(), b"contents");

    let unlink = driver.unlink(&scratch.join("second")).unwrap();
    drive(&mut driver, unlink).unwrap().unwrap();
    sync_directory(&mut driver, &directory);
    assert!(!scratch.join("second").exists());
    let unlink = driver.unlink(&scratch.join("second")).unwrap();
    assert_eq!(
        drive(&mut driver, unlink).unwrap().unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    let close = driver.close_directory(directory).unwrap();
    drive(&mut driver, close).unwrap();
}

/// A preallocation keeps or extends the length as asked, and the file system allocates its blocks.
pub(in crate::driver) fn a_preallocation_keeps_or_extends_the_length_and_allocates_its_blocks(backend: Backend) {
    const MIB: u64 = 1 << 20;
    let scratch = Scratch::new("allocate");
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), writable());
    let allocate = driver.allocate(&file, 0, MIB, Length::Keep).unwrap();
    drive(&mut driver, allocate).unwrap().unwrap();
    assert_eq!(fs::metadata(scratch.join("data")).unwrap().len(), 0);
    let bytes = allocated(&scratch.join("data"));
    assert!(bytes >= MIB, "{bytes} bytes");

    let allocate = driver.allocate(&file, 0, 2 * MIB, Length::Extend).unwrap();
    drive(&mut driver, allocate).unwrap().unwrap();
    assert_eq!(fs::metadata(scratch.join("data")).unwrap().len(), 2 * MIB);
    let bytes = allocated(&scratch.join("data"));
    assert!(bytes >= 2 * MIB, "{bytes} bytes");
}

/// A punched hole reads as zeros, keeps the length, and frees its blocks.
pub(in crate::driver) fn a_punched_hole_reads_as_zeros_keeps_the_length_and_frees_its_blocks(backend: Backend) {
    const KIB: usize = 1 << 10;
    let scratch = Scratch::new("hole");
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), writable().read(true));
    write(&mut driver, &file, &alloc::vec![0xab; 1024 * KIB], 0);
    sync(&mut driver, &file);
    let before = allocated(&scratch.join("data"));

    let punch = driver.punch_hole(&file, 256 * 1024, 512 * 1024).unwrap();
    drive(&mut driver, punch).unwrap().unwrap();
    sync(&mut driver, &file);
    assert_eq!(fs::metadata(scratch.join("data")).unwrap().len(), 1024 * 1024);
    let after = allocated(&scratch.join("data"));
    assert!(before - after >= 512 * 1024, "{before} bytes before, {after} after");
    let read = driver
        .read_at(&file, Backing::from(Vec::with_capacity(1024 * KIB)), 0)
        .unwrap();
    let (count, buffer) = drive(&mut driver, read).unwrap();
    assert_eq!(count.unwrap(), 1024 * KIB);
    let bytes = buffer.bytes();
    assert!(bytes[..256 * KIB].iter().all(|&byte| byte == 0xab));
    assert!(bytes[256 * KIB..768 * KIB].iter().all(|&byte| byte == 0));
    assert!(bytes[768 * KIB..].iter().all(|&byte| byte == 0xab));
}

/// A hole punch of a range that starts and ends inside blocks reads as zeros over the whole range,
/// and each byte outside it stays. So does a range inside one block, and a range that ends after
/// the end of the file, whose length stays.
pub(in crate::driver) fn an_unaligned_hole_reads_as_zeros_and_the_bytes_around_it_stay(backend: Backend) {
    const LEN: usize = 1 << 16;
    let scratch = Scratch::new("unaligned");
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), writable().read(true));
    write(&mut driver, &file, &numbered(0, LEN), 0);
    let mut expected = numbered(0, LEN);
    // Across whole blocks, inside one block, and after the end of the file.
    for (offset, len) in [(1000, 20_000), (30_001, 99), (60_000, 10_000)] {
        let punch = driver.punch_hole(&file, offset as u64, len as u64).unwrap();
        drive(&mut driver, punch).unwrap().unwrap();
        expected[offset..(offset + len).min(LEN)].fill(0);
    }
    assert_eq!(file_len(&mut driver, &file), LEN as u64);
    let read = driver
        .read_at(&file, Backing::from(Vec::with_capacity(LEN)), 0)
        .unwrap();
    let (count, buffer) = drive(&mut driver, read).unwrap();
    assert_eq!(count.unwrap(), LEN);
    assert_eq!(buffer.bytes(), expected);
}

/// A write that ends after the end of an allocation that extends the file, and that starts while
/// the allocation runs, keeps each of its bytes. On macOS, the allocation is a preallocation and
/// then a length change, so it runs alone on its file, and the write waits for it.
pub(in crate::driver) fn a_write_after_the_end_of_an_extending_allocation_keeps_each_byte(backend: Backend) {
    const MIB: u64 = 1 << 20;
    let scratch = Scratch::new("extend");
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), writable());
    // A large allocation runs long, and the small write after it ends 4 KiB after its end.
    let allocate = driver.allocate(&file, 0, 64 * MIB, Length::Extend).unwrap();
    let bytes = numbered(0, 8192);
    let write = driver
        .write_at(&file, Lease::from(bytes.clone()), 64 * MIB - 4096, WriteMode::Plain)
        .unwrap();
    let (written, _lease) = drive(&mut driver, write).unwrap();
    assert_eq!(written.unwrap(), 8192);
    drive(&mut driver, allocate).unwrap().unwrap();

    assert_eq!(file_len(&mut driver, &file), 64 * MIB + 4096);
    let read = driver
        .read_at(&file, Backing::from(Vec::with_capacity(8192)), 64 * MIB - 4096)
        .unwrap();
    let (count, buffer) = drive(&mut driver, read).unwrap();
    assert_eq!(count.unwrap(), 8192);
    assert_eq!(buffer.bytes(), bytes);
}

/// On ramfs, which has no `fallocate`, a hole punch gives `Unsupported`. A mount needs root.
#[cfg(target_os = "linux")]
#[test]
#[ignore = "mounts a ramfs, which only root can do"]
fn a_file_system_without_hole_punching_refuses_with_unsupported() {
    let scratch = Scratch::new("ramfs");
    let mount = scratch.join("mount");
    fs::create_dir(&mount).unwrap();
    let status = std::process::Command::new("mount")
        .args(["-t", "ramfs", "ramfs"])
        .arg(&mount)
        .status()
        .unwrap();
    assert!(status.success(), "mount gave {status}");
    let result = {
        let mut driver = driver(Config::new());
        let file = open(&mut driver, &mount.join("data"), writable());
        write(&mut driver, &file, &[1; 8192], 0);
        let punch = driver.punch_hole(&file, 0, 4096).unwrap();
        let result = drive(&mut driver, punch).unwrap();
        let close = driver.close_file(file).unwrap();
        drive(&mut driver, close).unwrap();
        result
    };
    let status = std::process::Command::new("umount").arg(&mount).status().unwrap();
    assert!(status.success(), "umount gave {status}");
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Unsupported);
}

/// A dropped write still runs and changes the file, and the driver keeps its lease until the
/// completion: the release comes after it in the release record.
pub(in crate::driver) fn a_dropped_write_changes_the_file_and_keeps_its_lease_until_its_completion(backend: Backend) {
    let scratch = Scratch::new("dropped");
    let log = Log::default();
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let file = open(&mut driver, &scratch.join("data"), writable());
    let mut loan = WriteLoan::new(recorded(&log, 4096));
    loan.fill_from(&numbered(0, 4096));
    let write = driver
        .write_at(&file, Lease::from(loan.into_inner()), 0, WriteMode::DataSync)
        .unwrap();
    let id = hold(&mut driver, &write);
    drop(write);
    posted(&mut driver, id).unwrap();
    note(&log, "completed");
    while driver.turn().unwrap().more() {}
    assert_eq!(taken(&log), ["completed", "released"]);
    assert_eq!(fs::read(scratch.join("data")).unwrap(), numbered(0, 4096));
}

/// A vectored write writes its regions in order at its offset, and a vectored read fills its
/// buffers in order, each up to its capacity.
pub(in crate::driver) fn vectored_writes_and_reads_keep_the_order_of_their_regions(backend: Backend) {
    let scratch = Scratch::new("vectored");
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), writable().read(true));
    let mut regions = Regions::with_capacity(2);
    regions.push(Lease::from(numbered(0, 300)));
    regions.push(Lease::from(numbered(300, 700)));
    let write = driver.write_vectored_at(&file, regions, 100, WriteMode::Plain).unwrap();
    assert_eq!(drive(&mut driver, write).unwrap().0.unwrap(), 1000);

    let mut buffers = Buffers::with_capacity(3);
    buffers.push(Backing::from(Vec::with_capacity(250)));
    buffers.push(filled(b"kept", 504));
    buffers.push(Backing::from(Vec::with_capacity(1000)));
    let read = driver.read_vectored_at(&file, buffers, 100).unwrap();
    let (count, buffers) = drive(&mut driver, read).unwrap();
    assert_eq!(count.unwrap(), 1000);
    let buffers = buffers.into_buffers();
    assert_eq!(buffers[0].bytes(), numbered(0, 250));
    assert_eq!(&buffers[1].bytes()[..4], b"kept");
    assert_eq!(&buffers[1].bytes()[4..], numbered(250, 500));
    assert_eq!(buffers[2].bytes(), numbered(750, 250));
}

/// A backing of `capacity` bytes at an address that is a multiple of 4096, which holds the sector
/// alignment of direct I/O, whose release, through its release hook, records "released".
#[cfg(target_os = "windows")]
#[expect(
    unsafe_code,
    reason = "the test maps its own block into a backing, as a library does"
)]
fn recorded_aligned(log: &super::super::tests::Log, capacity: usize) -> Backing {
    use crate::buffer::Release;

    let layout = Layout::from_size_align(capacity, ALIGNED).unwrap();
    // SAFETY: the layout has a size above 0.
    let ptr = core::ptr::NonNull::new(unsafe { alloc::alloc::alloc(layout) }).expect("the test host has memory");
    let data = Arc::into_raw(Arc::clone(log)).cast::<()>();
    // SAFETY: the block is valid for `capacity` bytes and only the backing uses it.
    // `release_aligned` frees it, on any thread.
    unsafe { Backing::from_raw_parts(ptr, capacity, 0, capacity, Release::new(data, release_aligned)) }
}

/// The alignment of the blocks of `recorded_aligned`.
#[cfg(target_os = "windows")]
const ALIGNED: usize = 4096;

/// The release hook of `recorded_aligned`: records the release, then frees the block.
///
/// # Safety
///
/// `data` is from `Arc::into_raw` of a `Log`, and `ptr` and `capacity` are a block of
/// `recorded_aligned` that nothing freed. Nothing uses them after this call.
#[cfg(target_os = "windows")]
#[expect(
    unsafe_code,
    reason = "the hook takes back the log and the block that `recorded_aligned` gave"
)]
unsafe fn release_aligned(data: *const (), ptr: core::ptr::NonNull<u8>, capacity: usize) {
    // SAFETY: `recorded_aligned` made `data` with `Arc::into_raw`, and this call is its only use.
    let log = unsafe { Arc::from_raw(data.cast::<Mutex<Vec<&'static str>>>()) };
    note(&log, "released");
    // SAFETY: the caller gives a block of `recorded_aligned` with this layout that nothing freed.
    unsafe { alloc::alloc::dealloc(ptr.as_ptr(), Layout::from_size_align(capacity, ALIGNED).unwrap()) };
}

/// On Windows, a dropped write that returns pending keeps its lease until its packet. A write
/// with no cache inside the length of the file goes to the device, so its call returns pending.
/// While the control holds its packet, turns of the driver release nothing. The release comes
/// after the packet in the release record (R203).
#[cfg(target_os = "windows")]
pub(in crate::driver) fn a_dropped_write_that_returns_pending_keeps_its_lease_until_its_packet(backend: Backend) {
    const LEN: usize = 1 << 16;
    let scratch = Scratch::new("pending");
    let log = Log::default();
    // The bytes are on the device and not in the cache, so the write does not wait for a flush.
    let data = fs::File::create(scratch.join("data")).unwrap();
    data.set_len(16 * LEN as u64).unwrap();
    data.sync_all().unwrap();
    drop(data);
    let mut driver = suite::driver(Config::new().delivery(Delivery::Controlled), backend);
    let file = open(
        &mut driver,
        &scratch.join("data"),
        OpenOptions::new().write(true).cache(Cache::Direct),
    );
    let mut loan = WriteLoan::new(recorded_aligned(&log, LEN));
    loan.fill_from(&numbered(0, LEN));
    let write = driver
        .write_at(&file, Lease::from(loan.into_inner()), 4 * LEN as u64, WriteMode::Plain)
        .unwrap();
    let id = hold(&mut driver, &write);
    drop(write);
    held(&mut driver, id).unwrap();
    for _ in 0..4 {
        driver.turn().unwrap();
    }
    assert!(taken(&log).is_empty(), "the lease returned before its packet");

    driver.control().unwrap().deliver(id).unwrap();
    note(&log, "completed");
    while driver.turn().unwrap().more() {}
    assert_eq!(taken(&log), ["completed", "released"]);
    let close = driver.close_file(file).unwrap();
    drive(&mut driver, close).unwrap();
    let bytes = fs::read(scratch.join("data")).unwrap();
    assert_eq!(&bytes[4 * LEN..5 * LEN], numbered(0, LEN));
}

/// On Windows, the report of a file states whether the native NVMe path serves its disk, as the
/// driver service of that disk, which PowerShell reads through CIM, says.
#[cfg(target_os = "windows")]
pub(in crate::driver) fn the_nvme_report_of_a_file_follows_the_driver_of_its_disk(backend: Backend) {
    let scratch = Scratch::new("nvme");
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), writable());
    let report = driver.nvme_path(&file).unwrap();
    let path = drive(&mut driver, report).unwrap().unwrap();

    let letter = scratch.0.to_str().unwrap().chars().next().unwrap();
    let script = alloc::format!(
        "$n = (Get-Partition -DriveLetter {letter}).DiskNumber; \
         $d = Get-CimInstance Win32_DiskDrive | Where-Object Index -eq $n; \
         (Get-PnpDeviceProperty -InstanceId $d.PNPDeviceID -KeyName DEVPKEY_Device_Service).Data"
    );
    let output = std::process::Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        alloc::string::String::from_utf8_lossy(&output.stderr)
    );
    let service: alloc::string::String = alloc::string::String::from_utf8_lossy(&output.stdout).trim().into();
    assert!(
        !service.is_empty(),
        "PowerShell found no driver service for drive {letter}"
    );
    let expected = if service.eq_ignore_ascii_case("nvmedisk") {
        super::NvmePath::Native
    } else {
        super::NvmePath::Other
    };
    assert_eq!(
        path, expected,
        "the disk of drive {letter} has the driver service {service}"
    );
}

/// On Windows, a file at a path longer than `MAX_PATH` (260 units) opens, takes a write, and reads
/// it back, as with `std::fs`: the driver gives the system the verbatim form of the path.
#[cfg(target_os = "windows")]
pub(in crate::driver) fn a_file_at_a_path_longer_than_max_path_opens_writes_and_reads_back(backend: Backend) {
    let scratch = Scratch::new("long");
    let directory = scratch.join(&"d".repeat(120)).join("e".repeat(120));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("data");
    assert!(path.as_os_str().len() > 260, "{}", path.display());
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &path, writable().read(true));
    write(&mut driver, &file, b"long", 0);
    let read = driver.read_at(&file, Backing::from(Vec::with_capacity(8)), 0).unwrap();
    let (count, buffer) = drive(&mut driver, read).unwrap();
    assert_eq!((count.unwrap(), buffer.bytes()), (4, &b"long"[..]));
    let close = driver.close_file(file).unwrap();
    drive(&mut driver, close).unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"long");
    let unlink = driver.unlink(&path).unwrap();
    drive(&mut driver, unlink).unwrap().unwrap();
    assert!(!path.exists());
}

/// Each refusal of the allocator at the admission of a file job is `Allocation`, gives the lease
/// back, and keeps no memory. A refusal that reads `Blocking` fails.
#[cfg(target_os = "macos")]
#[test]
fn a_file_job_that_the_allocator_refuses_gives_allocation_and_its_lease() {
    use crate::table::tests::allocations::each_refusal;

    let scratch = Scratch::new("refused");
    let mut driver = suite::driver(Config::new(), Backend::Kqueue);
    let file = open(&mut driver, &scratch.join("data"), writable());
    // A lease for each run, made before the allocator refuses.
    let mut leases: Vec<_> = (0..16).map(|_| Lease::from(numbered(0, 64))).collect();
    let refusals = each_refusal(|| {
        driver
            .write_at(&file, leases.pop().unwrap(), 0, WriteMode::Plain)
            .map(drop)
    });
    assert!(!refusals.is_empty());
    for (refused, held) in refusals {
        assert_eq!((refused.reason(), held), (Reason::Allocation, 0));
        assert_eq!(refused.into_inner().bytes(), numbered(0, 64));
    }
}

/// A file operation that waits behind an operation that runs alone on its file keeps its
/// admission, so it starts when that operation ends with no allocation. A start that reserves
/// again then fails, and nothing wakes the driver after it: the drive never ends.
#[cfg(target_os = "macos")]
#[test]
fn a_waiting_file_operation_starts_with_the_admission_that_it_kept() {
    use crate::table::tests::allocations::refused;

    let scratch = Scratch::new("waiting");
    let mut driver = suite::driver(Config::new(), Backend::Kqueue);
    let file = open(&mut driver, &scratch.join("data"), writable());
    let allocate = driver.allocate(&file, 0, 1 << 20, Length::Extend).unwrap();
    // The allocation runs alone on the file, so the write waits for it.
    let write = driver
        .write_at(&file, Lease::from(numbered(0, 4096)), 0, WriteMode::Plain)
        .unwrap();
    // Each turn of the driver refuses each allocation of this thread.
    let (written, _lease) = refused(|| drive(&mut driver, write)).unwrap();
    assert_eq!(written.unwrap(), 4096);
    drive(&mut driver, allocate).unwrap().unwrap();
}

/// Each refusal of the allocator in the NVMe report gives `ERROR_NOT_ENOUGH_MEMORY`, which reads as
/// `OutOfMemory`, and keeps no memory. A report whose allocation aborts the process fails.
#[cfg(target_os = "windows")]
#[test]
fn an_nvme_report_that_the_allocator_refuses_keeps_no_memory() {
    use std::os::windows::io::AsRawHandle as _;

    use crate::driver::iocp::sys;
    use crate::table::tests::allocations::each_refusal;

    let scratch = Scratch::new("nvme-refused");
    let file = fs::File::create(scratch.join("data")).unwrap();
    let refusals = each_refusal(|| super::windows::nvme_path(file.as_raw_handle()));
    assert!(
        !refusals.is_empty(),
        "the volume of the test has a disk, so the report lists the devices"
    );
    assert!(
        refusals
            .iter()
            .all(|refusal| *refusal == (-sys::ERROR_NOT_ENOUGH_MEMORY, 0))
    );
}

/// While a file operation runs on the ring, a wait enters the kernel with a short timeout, so the
/// CPU takes no idle state that wakes later than the device ends. The wait enters again after
/// each timeout, so it still ends only at a completion or a wake. After the last file operation
/// ends, a wait sleeps until its wake, so an idle driver does not wake the CPU. The control holds
/// the completion of the read, so the driver counts the read as running, and only the wake ends
/// the wait.
#[cfg(target_os = "linux")]
#[test]
fn a_wait_enters_the_kernel_again_after_a_short_timeout_only_while_a_file_operation_runs() {
    let scratch = Scratch::new("bounded-wait");
    let path = scratch.join("data");
    fs::write(&path, b"data").expect("the test writes its file");
    let mut driver = driver(Config::new().delivery(Delivery::Controlled));
    let file = open(&mut driver, &path, OpenOptions::new());
    let read = driver
        .read_at(&file, Backing::from(Vec::with_capacity(4)), 0)
        .expect("the test driver has room");
    let id = hold(&mut driver, &read);
    assert_eq!(held(&mut driver, id).expect("the read works"), 4);
    // A wait with no bound sleeps one time, until the wake after 5 s.
    let slept = sleeps_of_a_wait(&mut driver, 4, Duration::from_secs(5));
    assert!(slept >= 4, "the wait slept {slept} times while a read ran");

    assert_eq!(posted(&mut driver, id).expect("the read works"), 4);
    let (read, _) = drive(&mut driver, read).expect("the driver runs");
    assert_eq!(read.expect("the read works"), 4);
    // 50 ms is 200 entries of 250 µs.
    let slept = sleeps_of_a_wait(&mut driver, u64::MAX, Duration::from_millis(50));
    assert!(slept <= 3, "the wait slept {slept} times with no file operation");
}
