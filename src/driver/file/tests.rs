use alloc::vec::Vec;
use core::alloc::Layout;
use core::num::{NonZeroU32, NonZeroU64};
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use super::super::suite::{self, Backend};
#[cfg(target_os = "linux")]
use super::super::tests::driver;
use super::super::tests::{Log, drive, note, numbered, posted, recorded, taken};
use super::super::{Call, Config, Driver, Reason, Route};
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
    #[cfg(target_os = "macos")]
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

/// A rename, an unlink, and the sync of their directory complete, and the names change as they
/// say.
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
    let sync = driver.sync_directory(&directory).unwrap();
    drive(&mut driver, sync).unwrap().unwrap();
    assert!(!scratch.join("first").exists());
    assert_eq!(fs::read(scratch.join("second")).unwrap(), b"contents");

    let unlink = driver.unlink(&scratch.join("second")).unwrap();
    drive(&mut driver, unlink).unwrap().unwrap();
    let sync = driver.sync_directory(&directory).unwrap();
    drive(&mut driver, sync).unwrap().unwrap();
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
    let metadata = fs::metadata(scratch.join("data")).unwrap();
    assert_eq!(metadata.len(), 0);
    // `st_blocks` counts 512-byte units.
    assert!(metadata.blocks() * 512 >= MIB, "{} blocks", metadata.blocks());

    let allocate = driver.allocate(&file, 0, 2 * MIB, Length::Extend).unwrap();
    drive(&mut driver, allocate).unwrap().unwrap();
    let metadata = fs::metadata(scratch.join("data")).unwrap();
    assert_eq!(metadata.len(), 2 * MIB);
    assert!(metadata.blocks() * 512 >= 2 * MIB, "{} blocks", metadata.blocks());
}

/// A punched hole reads as zeros, keeps the length, and frees its blocks.
pub(in crate::driver) fn a_punched_hole_reads_as_zeros_keeps_the_length_and_frees_its_blocks(backend: Backend) {
    const KIB: usize = 1 << 10;
    let scratch = Scratch::new("hole");
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), writable().read(true));
    write(&mut driver, &file, &alloc::vec![0xab; 1024 * KIB], 0);
    sync(&mut driver, &file);
    let before = fs::metadata(scratch.join("data")).unwrap().blocks();

    let punch = driver.punch_hole(&file, 256 * 1024, 512 * 1024).unwrap();
    drive(&mut driver, punch).unwrap().unwrap();
    sync(&mut driver, &file);
    let metadata = fs::metadata(scratch.join("data")).unwrap();
    assert_eq!(metadata.len(), 1024 * 1024);
    assert!(
        (before - metadata.blocks()) * 512 >= 512 * 1024,
        "{before} blocks before, {} after",
        metadata.blocks()
    );
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
    let mut driver = suite::driver(Config::new(), backend);
    let file = open(&mut driver, &scratch.join("data"), writable());
    let mut loan = WriteLoan::new(recorded(&log, 4096));
    loan.fill_from(&numbered(0, 4096));
    drop(
        driver
            .write_at(&file, Lease::from(loan.into_inner()), 0, WriteMode::DataSync)
            .unwrap(),
    );
    posted(&mut driver, 1);
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
