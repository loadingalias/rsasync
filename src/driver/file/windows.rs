//! The file calls of the IOCP backend that have no overlapped form. Each one runs as a job of the
//! blocking service, as on epoll and kqueue ([`run_file`]): an open, a length inspection or
//! change, a preallocation, a sync, a rename, and an unlink. The device report of a file
//! ([`nvme_path`]) runs there too. The reads, the writes, and the hole punches are overlapped calls
//! on the port (`iocp`). On IoRing, the reads, the writes, and the syncs are entries of the I/O ring
//! (`iocp::ring`), and a hole punch is a job too, because the file is not on the port.

use alloc::vec::Vec;
use core::ffi::{c_int, c_void};
use core::ptr;
use std::ffi::OsStr;
use std::io;
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, IntoRawHandle as _, OwnedHandle};
use std::path::Path;

use super::super::iocp::sys;
use super::{Alignment, Cache, NvmePath, OpenOptions};
use crate::uring::Sqe;
use crate::uring::sys as ring;

/// The alignment of direct I/O when the device does not report its sector sizes.
const SECTOR: u32 = 4096;

/// `ERROR_TOO_MANY_OPEN_FILES` (`winerror.h`): the handle of an open has significant bits above the
/// low 32, so no entry can hold it.
const ERROR_TOO_MANY_OPEN_FILES: c_int = 4;

/// The disk extents of a volume that the device report reads: more is a volume of more disks
/// than a report takes, and it gives [`NvmePath::Other`].
const EXTENTS: usize = 16;

/// The UTF-16 units of a device path or a property that the device report reads.
const NAME: usize = 1024;

/// The open of a file or a directory: what its job reads, and what it gives the home.
#[derive(Debug, Default)]
pub(crate) struct Opening {
    /// The path, as a NUL-terminated UTF-16 string.
    path: Vec<u16>,
    /// `dwDesiredAccess`.
    access: u32,
    /// `dwCreationDisposition`.
    disposition: u32,
    /// `dwFlagsAndAttributes`.
    flags: u32,
    /// The handle of the write-through writes of a file with writes, which the job makes.
    pub(super) through: Option<OwnedHandle>,
    /// The alignment of direct I/O, which the job reads for a file that the cache does not hold.
    pub(super) alignment: Option<Alignment>,
}

impl Opening {
    /// The open of the file at `path` with `options`, or `None` for options that no open can take
    /// or a path with a NUL unit.
    pub(super) fn file(path: &Path, options: OpenOptions) -> Option<Self> {
        let access = match (options.read, options.write) {
            (true, false) => sys::GENERIC_READ,
            (false, true) => sys::GENERIC_WRITE,
            (true, true) => sys::GENERIC_READ | sys::GENERIC_WRITE,
            (false, false) => return None,
        };
        if !options.write && (options.create || options.create_new || options.truncate) {
            return None;
        }
        let disposition = match (options.create, options.create_new, options.truncate) {
            (_, true, _) => sys::CREATE_NEW,
            (true, false, true) => sys::CREATE_ALWAYS,
            (true, false, false) => sys::OPEN_ALWAYS,
            (false, false, true) => sys::TRUNCATE_EXISTING,
            (false, false, false) => sys::OPEN_EXISTING,
        };
        let mut flags = sys::FILE_FLAG_OVERLAPPED | sys::FILE_ATTRIBUTE_NORMAL;
        if options.cache == Cache::Direct {
            flags |= sys::FILE_FLAG_NO_BUFFERING;
        }
        Some(Self {
            path: wide(path)?,
            access,
            disposition,
            flags,
            ..Self::default()
        })
    }

    /// The open of the directory at `path`, or `None` for a path with a NUL unit.
    pub(super) fn directory(path: &Path) -> Option<Self> {
        Some(Self {
            path: wide(path)?,
            access: sys::GENERIC_READ,
            disposition: sys::OPEN_EXISTING,
            flags: sys::FILE_FLAG_OVERLAPPED | sys::FILE_FLAG_BACKUP_SEMANTICS,
            ..Self::default()
        })
    }
}

/// `path` as a NUL-terminated UTF-16 string, or `None` when it holds a NUL unit. A path that
/// can be longer than `MAX_PATH` becomes absolute with the verbatim prefix ([`verbatim`]), so the
/// driver opens each path that `std::fs` opens.
pub(super) fn wide(path: &Path) -> Option<Vec<u16>> {
    let mut wide: Vec<u16> = OsStr::encode_wide(path.as_os_str()).collect();
    if wide.contains(&0) {
        return None;
    }
    wide.push(0);
    Some(verbatim(wide))
}

/// The length below which an absolute path is short for each call: `MAX_PATH` is 260 units with
/// the NUL, and `CreateDirectoryW` takes 248 (`fileapi.h`).
const SHORT: usize = 248;

/// The NUL-terminated UTF-16 path `path`, as `std::fs` gives it to the system: a short absolute
/// path as it is, and each other path made absolute (`GetFullPathNameW`) with the verbatim prefix
/// `\\?\`, or `\\?\UNC\` for a network path. The system then takes it with no length limit.
/// A path that has a verbatim prefix stays as it is, and a path that `GetFullPathNameW` refuses
/// stays as it is, so its call gives the error.
#[expect(unsafe_code, reason = "the call reads the path and writes the buffer of its length")]
fn verbatim(path: Vec<u16>) -> Vec<u16> {
    const SEPARATOR: u16 = b'\\' as u16;
    const SLASH: u16 = b'/' as u16;
    const COLON: u16 = b':' as u16;
    const QUERY: u16 = b'?' as u16;
    const DOT: u16 = b'.' as u16;
    const VERBATIM: [u16; 4] = [SEPARATOR, SEPARATOR, QUERY, SEPARATOR];
    const NT: [u16; 4] = [SEPARATOR, QUERY, QUERY, SEPARATOR];
    const UNC: [u16; 8] = [
        SEPARATOR,
        SEPARATOR,
        QUERY,
        SEPARATOR,
        b'U' as u16,
        b'N' as u16,
        b'C' as u16,
        SEPARATOR,
    ];
    if path.starts_with(&VERBATIM) || path.starts_with(&NT) || path == [0] {
        return path;
    }
    let short_absolute = path.len() < SHORT
        && match path.as_slice() {
            [drive, COLON, 0] | [drive, COLON, SEPARATOR | SLASH, ..] => *drive != SEPARATOR && *drive != SLASH,
            [SEPARATOR | SLASH, SEPARATOR | SLASH, ..] => true,
            _ => false,
        };
    if short_absolute {
        return path;
    }
    // The working directory can change between the calls, so the loop tries a few times.
    let mut absolute = Vec::new();
    for _ in 0..ATTEMPTS {
        let room = u32::try_from(absolute.capacity()).unwrap_or(u32::MAX);
        // SAFETY: the path is NUL-terminated, and the call writes at most `room` units.
        let len = unsafe { sys::GetFullPathNameW(path.as_ptr(), room, absolute.as_mut_ptr(), ptr::null_mut()) };
        match usize::try_from(len) {
            Ok(0) | Err(_) => return path,
            // The units and the NUL fitted: the call wrote `len` units before the NUL.
            Ok(len) if len < absolute.capacity() => {
                // SAFETY: the call initialized `len` units.
                unsafe { absolute.set_len(len) };
                break;
            }
            // The buffer needs `len` units with the NUL.
            Ok(len) => absolute.reserve_exact(len),
        }
    }
    if absolute.is_empty() {
        return path;
    }
    let (prefix, rest): (&[u16], &[u16]) = match absolute.as_slice() {
        [_, COLON, SEPARATOR, ..] => (&VERBATIM, &absolute),
        // `\\.\` names a device: the verbatim form keeps it.
        [SEPARATOR, SEPARATOR, DOT, SEPARATOR, rest @ ..] => (&VERBATIM, rest),
        [SEPARATOR, SEPARATOR, QUERY, SEPARATOR, ..] | [SEPARATOR, QUERY, QUERY, SEPARATOR, ..] => (&[], &absolute),
        [SEPARATOR, SEPARATOR, rest @ ..] => (&UNC, rest),
        _ => (&[], &absolute),
    };
    let mut verbatim = Vec::with_capacity(prefix.len() + rest.len() + 1);
    verbatim.extend_from_slice(prefix);
    verbatim.extend_from_slice(rest);
    verbatim.push(0);
    verbatim
}

/// The calls of `GetFullPathNameW` for one path: the second one has the length that the first one
/// gave, and a third one serves a change of the working directory between them.
const ATTEMPTS: usize = 3;

/// The handle of the number `number` of an entry: the number extended with its sign, as for a
/// socket.
pub(crate) fn handle(number: i32) -> sys::Handle {
    ptr::without_provenance_mut(usize::try_from(super::super::iocp::socket(number)).unwrap_or(usize::MAX))
}

/// The number of `handle` in an entry, or `None` for a handle with significant bits above the low
/// 32.
pub(crate) fn number(handle: sys::Handle) -> Option<i32> {
    let number = i32::try_from(handle.addr().cast_signed()).ok()?;
    (number >= 0).then_some(number)
}

/// The negative Win32 error of the last call of this thread, as a completion gives it.
fn last_error() -> i32 {
    io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(sys::ERROR_INVALID_PARAMETER)
        .saturating_neg()
}

/// 0 for a call that succeeded, or the negative error of the last call.
fn status(succeeded: i32) -> i32 {
    if succeeded == 0 { last_error() } else { 0 }
}

/// Runs a file entry as one blocking call, or a few for a preallocation: the job of the blocking
/// service for the entry on IOCP. Gives the result that a ring posts for the entry, with a Win32
/// error in place of an error number. The entry holds the values of Linux: the modes of
/// `fallocate` select the preallocation and its length change.
#[expect(
    unsafe_code,
    reason = "the entry names memory that the driver keeps until the completion"
)]
pub(crate) fn run_file(sqe: &Sqe) -> i32 {
    let file = handle(sqe.fd);
    // SAFETY: the driver keeps the memory that the entry names until the completion of the entry,
    // which comes after the return of this job, and the job owns the handle of the entry.
    unsafe {
        match sqe.opcode {
            ring::IORING_OP_OPENAT => open(&mut *pointer::<Opening>(sqe.addr)),
            ring::IORING_OP_FSYNC => status(sys::FlushFileBuffers(file)),
            ring::IORING_OP_FTRUNCATE => set_end(file, sqe.off),
            ring::IORING_OP_FALLOCATE if sqe.len & ring::FALLOC_FL_PUNCH_HOLE != 0 => punch(file, sqe.off, sqe.addr),
            ring::IORING_OP_FALLOCATE => allocate(file, sqe.off, sqe.addr, sqe.len & ring::FALLOC_FL_KEEP_SIZE == 0),
            ring::IORING_OP_STATX => match standard(file) {
                Ok(info) => {
                    pointer::<sys::FileStandardInfo>(sqe.off).write(info);
                    0
                }
                Err(error) => error,
            },
            ring::IORING_OP_RENAMEAT => status(sys::MoveFileExW(
                pointer(sqe.addr),
                pointer(sqe.off),
                sys::MOVEFILE_REPLACE_EXISTING,
            )),
            ring::IORING_OP_UNLINKAT => status(sys::DeleteFileW(pointer(sqe.addr))),
            _ => -sys::ERROR_INVALID_PARAMETER,
        }
    }
}

/// The pointer of an address in an entry.
fn pointer<T>(address: u64) -> *mut T {
    ptr::with_exposed_provenance_mut(usize::try_from(address).unwrap_or(0))
}

/// Opens the file of `opening`, and for a file with writes its write-through handle, and for a
/// file that the cache does not hold the alignment of its direct I/O. Gives the number of the
/// handle, or the negative error. A directory gives `ERROR_DIRECTORY` when the path is not one.
#[expect(unsafe_code, reason = "the calls read the path and give new handles")]
fn open(opening: &mut Opening) -> i32 {
    // SAFETY: the path is a NUL-terminated UTF-16 string, and the call names no other memory.
    let raw = unsafe {
        sys::CreateFileW(
            opening.path.as_ptr(),
            opening.access,
            sys::FILE_SHARE_ALL,
            ptr::null_mut(),
            opening.disposition,
            opening.flags,
            ptr::null_mut(),
        )
    };
    if raw.addr() == sys::INVALID_HANDLE_VALUE {
        return last_error();
    }
    // SAFETY: the call gave a new handle that nothing else owns.
    let file = unsafe { OwnedHandle::from_raw_handle(raw) };
    let Some(result) = number(raw) else {
        return -ERROR_TOO_MANY_OPEN_FILES;
    };
    let directory = opening.flags & sys::FILE_FLAG_BACKUP_SEMANTICS != 0;
    match standard(raw) {
        Ok(info) if directory && info.directory == 0 => return -ERROR_DIRECTORY,
        Ok(_) => {}
        Err(error) => return error,
    }
    if !directory && opening.access & sys::GENERIC_WRITE != 0 {
        // `ReOpenFile` takes the flags of the open, and no attribute.
        let flags = (opening.flags & !sys::FILE_ATTRIBUTE_NORMAL) | sys::FILE_FLAG_WRITE_THROUGH;
        // SAFETY: the call takes the open handle and gives a new one.
        let through = unsafe { sys::ReOpenFile(raw, opening.access, sys::FILE_SHARE_ALL, flags) };
        if through.addr() == sys::INVALID_HANDLE_VALUE {
            return last_error();
        }
        // SAFETY: the call gave a new handle that nothing else owns.
        let through = unsafe { OwnedHandle::from_raw_handle(through) };
        if number(through.as_raw_handle()).is_none() {
            return -ERROR_TOO_MANY_OPEN_FILES;
        }
        opening.through = Some(through);
    }
    if opening.flags & sys::FILE_FLAG_NO_BUFFERING != 0 {
        match alignment(raw) {
            Ok(alignment) => opening.alignment = Some(alignment),
            Err(error) => return error,
        }
    }
    // The home takes the handle from the result, as from an open on Linux.
    let _raw = file.into_raw_handle();
    result
}

/// `ERROR_DIRECTORY` (`winerror.h`): the path of a directory open is not a directory. It reads as
/// `NotADirectory`.
const ERROR_DIRECTORY: c_int = 267;

/// The alignment of direct I/O of the open file `file`: each offset and length is a multiple of
/// the logical sector, which `FILE_FLAG_NO_BUFFERING` requires, and each buffer starts at a
/// multiple of the physical sector, which its documentation asks for. A device that reports no
/// sector size gets 4096 bytes.
fn alignment(file: sys::Handle) -> Result<Alignment, i32> {
    let mut storage = sys::FileStorageInfo::default();
    query(file, sys::FILE_STORAGE_INFO, &mut storage)?;
    let offset = Some(storage.logical).filter(|&logical| logical > 0).unwrap_or(SECTOR);
    let memory = storage.physical_performance.max(offset);
    Ok(Alignment { memory, offset })
}

/// The allocation and the length of the open file `file`.
fn standard(file: sys::Handle) -> Result<sys::FileStandardInfo, i32> {
    let mut info = sys::FileStandardInfo::default();
    query(file, sys::FILE_STANDARD_INFO, &mut info)?;
    Ok(info)
}

/// Writes the information `class` of the open file `file` into `information`.
#[expect(unsafe_code, reason = "the call writes at most the size of the structure")]
fn query<T>(file: sys::Handle, class: c_int, information: &mut T) -> Result<(), i32> {
    let len = u32::try_from(size_of::<T>()).unwrap_or(0);
    // SAFETY: `information` holds `len` bytes, and the call writes at most that many.
    let read = unsafe { sys::GetFileInformationByHandleEx(file, class, ptr::from_mut(information).cast(), len) };
    if read == 0 { Err(last_error()) } else { Ok(()) }
}

/// Sets the information `class` of the open file `file` to the `i64` `value`.
#[expect(unsafe_code, reason = "the call reads one `LARGE_INTEGER`")]
fn set(file: sys::Handle, class: c_int, value: i64) -> Result<(), i32> {
    // SAFETY: the structure of each class that the module sets is one `LARGE_INTEGER`, which lives
    // during the call.
    let set = unsafe { sys::SetFileInformationByHandle(file, class, ptr::from_ref(&value).cast::<c_void>(), 8) };
    if set == 0 { Err(last_error()) } else { Ok(()) }
}

/// Sets the length of the file `file` to `len` bytes.
fn set_end(file: sys::Handle, len: u64) -> i32 {
    match i64::try_from(len) {
        Ok(len) => set(file, sys::FILE_END_OF_FILE_INFO, len).err().unwrap_or(0),
        Err(_) => -sys::ERROR_INVALID_PARAMETER,
    }
}

/// Reserves the clusters up to the end of the `len` bytes from `offset` of the file `file`, and
/// with `extend` makes that end the length when the length is less. `FileAllocationInfo` sets the
/// allocation to its value, and a value below the length cuts the file there, so the job reads the
/// allocation and the length first and never sets less than either. The driver runs such an
/// allocation alone on its file (`Driver::allocate`), so no write of the driver can end between
/// the calls. A range of 0 bytes, or a range past the largest offset, gives
/// `ERROR_INVALID_PARAMETER`.
fn allocate(file: sys::Handle, offset: u64, len: u64, extend: bool) -> i32 {
    let Some(end) = offset
        .checked_add(len)
        .filter(|_| len > 0)
        .and_then(|end| i64::try_from(end).ok())
    else {
        return -sys::ERROR_INVALID_PARAMETER;
    };
    let result = standard(file).and_then(|info| {
        if end > info.allocation {
            set(file, sys::FILE_ALLOCATION_INFO, end.max(info.end))?;
        }
        if extend && end > info.end {
            set(file, sys::FILE_END_OF_FILE_INFO, end)?;
        }
        Ok(())
    });
    result.err().unwrap_or(0)
}

/// Punches a hole of `len` bytes at `offset` in the file `file`, which no port holds, on
/// [`Backend::IoRing`](crate::driver::Backend::IoRing): it makes the file sparse
/// (`FSCTL_SET_SPARSE`), and then frees the clusters of the range (`FSCTL_SET_ZERO_DATA`). The
/// results are those of the punch on the port: a range of 0 bytes, or a range past the largest
/// offset, gives `ERROR_INVALID_PARAMETER`, and a file system with no sparse files gives
/// `ERROR_CALL_NOT_IMPLEMENTED`, which `std::io` reads as `Unsupported`.
fn punch(file: sys::Handle, offset: u64, len: u64) -> i32 {
    let Some(end) = offset
        .checked_add(len)
        .filter(|_| len > 0)
        .and_then(|end| i64::try_from(end).ok())
    else {
        return -sys::ERROR_INVALID_PARAMETER;
    };
    let range = sys::FileZeroDataInformation {
        offset: offset.cast_signed(),
        end,
    };
    match zero_control(file, sys::FSCTL_SET_SPARSE, None) {
        Err(sys::ERROR_INVALID_FUNCTION | sys::ERROR_NOT_SUPPORTED) => -sys::ERROR_CALL_NOT_IMPLEMENTED,
        Err(error) => error.saturating_neg(),
        Ok(()) => zero_control(file, sys::FSCTL_SET_ZERO_DATA, Some(&range)).map_or_else(i32::saturating_neg, |()| 0),
    }
}

/// Runs the control `code` of a hole punch, with `range` as its input, on the overlapped file
/// `file`, which no port holds, and waits for its end on an event of its own. Gives the Win32
/// error of a failure.
#[expect(
    unsafe_code,
    reason = "the call reads the input and writes the OVERLAPPED, which live until its end"
)]
fn zero_control(file: sys::Handle, code: u32, range: Option<&sys::FileZeroDataInformation>) -> Result<(), i32> {
    // SAFETY: the call names no memory, and gives a new handle or null.
    let event = unsafe { sys::CreateEventW(ptr::null_mut(), 1, 0, ptr::null()) };
    if event.is_null() {
        return Err(last_error().saturating_neg());
    }
    // SAFETY: the call gave a new handle that nothing else owns.
    let event = unsafe { OwnedHandle::from_raw_handle(event) };
    let mut overlapped = sys::Overlapped {
        internal: 0,
        internal_high: 0,
        offset: [0; 2],
        event: event.as_raw_handle(),
    };
    let (input, input_len) = range.map_or((ptr::null(), 0), |range| {
        (
            ptr::from_ref(range).cast::<c_void>(),
            u32::try_from(size_of_val(range)).unwrap_or(0),
        )
    });
    let mut returned = 0;
    // SAFETY: the input and the `OVERLAPPED` live until the wait below ends the call, the control
    // writes no output, and the event signals the end of the call, because no port holds the file.
    let done = unsafe {
        sys::DeviceIoControl(
            file,
            code,
            input,
            input_len,
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            &raw mut overlapped,
        )
    };
    if done == 0 {
        let error = last_error().saturating_neg();
        if error != sys::ERROR_IO_PENDING {
            return Err(error);
        }
    }
    // SAFETY: as above. The wait returns at the end of the call.
    if unsafe { sys::GetOverlappedResult(file, &raw mut overlapped, &raw mut returned, 1) } == 0 {
        return Err(last_error().saturating_neg());
    }
    Ok(())
}

/// Whether the native NVMe path of Windows Server 2025 serves each disk of the volume of the open
/// file `file`: the driver service of the device node of each disk is `nvmedisk`. A disk under
/// the disk class driver, `disk`, goes through the SCSI translation of its port driver. The report
/// reads the devices, not the version of the system. Gives the path, or the negative error.
pub(crate) fn nvme_path(file: sys::Handle) -> Result<NvmePath, i32> {
    let disks = disks(file)?;
    if disks.is_empty() {
        return Ok(NvmePath::Other);
    }
    let services = services()?;
    let native = disks.iter().all(|disk| {
        services
            .iter()
            .any(|(number, service)| number == disk && service.eq_ignore_ascii_case("nvmedisk"))
    });
    Ok(if native { NvmePath::Native } else { NvmePath::Other })
}

/// The numbers of the disks of the volume of the open file `file`. A volume with no disk, such as
/// a network share, has none.
#[expect(unsafe_code, reason = "the calls write into storage that lives during the calls")]
fn disks(file: sys::Handle) -> Result<Vec<u32>, i32> {
    let mut path = [0_u16; NAME];
    // SAFETY: `path` holds `NAME` units, and the call writes at most that many.
    let len = unsafe {
        sys::GetFinalPathNameByHandleW(
            file,
            path.as_mut_ptr(),
            u32::try_from(path.len()).unwrap_or(0),
            sys::VOLUME_NAME_GUID,
        )
    };
    let path = path.get(..len as usize).ok_or(-sys::ERROR_INVALID_PARAMETER)?;
    if len == 0 {
        return Err(last_error());
    }
    // `\\?\Volume{GUID}\…`: the volume is the path up to its fourth separator, opened as `\\.\`.
    let Some(end) = path
        .iter()
        .enumerate()
        .filter(|&(_, &unit)| unit == u16::from(b'\\'))
        .nth(3)
        .map(|(index, _)| index)
    else {
        return Ok(Vec::new());
    };
    let mut volume: Vec<u16> = path[..end].to_vec();
    if let Some(unit) = volume.get_mut(2) {
        *unit = u16::from(b'.');
    }
    volume.push(0);
    let volume = open_device(&volume)?;
    let mut extents = sys::VolumeDiskExtents::<EXTENTS> {
        count: 0,
        extents: [sys::DiskExtent::default(); EXTENTS],
    };
    // A volume with no disk, such as a network share, has no extents.
    if control(&volume, sys::IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS, &mut extents).is_err() {
        return Ok(Vec::new());
    }
    let count = (extents.count as usize).min(EXTENTS);
    let mut disks: Vec<u32> = extents.extents[..count].iter().map(|extent| extent.disk).collect();
    disks.sort_unstable();
    disks.dedup();
    Ok(disks)
}

/// Opens the device at the NUL-terminated path `path` with no access, which device queries need.
#[expect(unsafe_code, reason = "the call reads the path and gives a new handle")]
fn open_device(path: &[u16]) -> Result<OwnedHandle, i32> {
    // SAFETY: the path is a NUL-terminated UTF-16 string, and the call names no other memory.
    let raw = unsafe {
        sys::CreateFileW(
            path.as_ptr(),
            0,
            sys::FILE_SHARE_ALL,
            ptr::null_mut(),
            sys::OPEN_EXISTING,
            0,
            ptr::null_mut(),
        )
    };
    if raw.addr() == sys::INVALID_HANDLE_VALUE {
        return Err(last_error());
    }
    // SAFETY: the call gave a new handle that nothing else owns.
    Ok(unsafe { OwnedHandle::from_raw_handle(raw) })
}

/// Writes the output of the control `code` of the device `device` into `output`.
#[expect(unsafe_code, reason = "the call writes at most the size of the output")]
fn control<T>(device: &OwnedHandle, code: u32, output: &mut T) -> Result<(), i32> {
    let mut returned = 0;
    // SAFETY: the handle is open with no overlapped mode, `output` holds its size in bytes, and the
    // call writes at most that many.
    let done = unsafe {
        sys::DeviceIoControl(
            device.as_raw_handle(),
            code,
            ptr::null(),
            0,
            ptr::from_mut(output).cast(),
            u32::try_from(size_of::<T>()).unwrap_or(0),
            &raw mut returned,
            ptr::null_mut(),
        )
    };
    if done == 0 { Err(last_error()) } else { Ok(()) }
}

/// The number and the driver service of each present disk.
#[expect(unsafe_code, reason = "the calls write into storage that lives during the calls")]
fn services() -> Result<Vec<(u32, alloc::string::String)>, i32> {
    let mut len = 0;
    // SAFETY: the call writes one count.
    let result = unsafe {
        sys::CM_Get_Device_Interface_List_SizeW(
            &raw mut len,
            &sys::GUID_DEVINTERFACE_DISK,
            ptr::null(),
            sys::CM_GET_DEVICE_INTERFACE_LIST_PRESENT,
        )
    };
    if result != sys::CR_SUCCESS {
        return Err(-sys::ERROR_NOT_SUPPORTED);
    }
    let mut list = alloc::vec![0_u16; len as usize];
    // SAFETY: `list` holds `len` units, and the call writes at most that many.
    let result = unsafe {
        sys::CM_Get_Device_Interface_ListW(
            &sys::GUID_DEVINTERFACE_DISK,
            ptr::null(),
            list.as_mut_ptr(),
            len,
            sys::CM_GET_DEVICE_INTERFACE_LIST_PRESENT,
        )
    };
    if result != sys::CR_SUCCESS {
        return Err(-sys::ERROR_NOT_SUPPORTED);
    }
    let mut services = Vec::new();
    // The list holds NUL-terminated paths, and an empty path ends it.
    for interface in list
        .split_inclusive(|&unit| unit == 0)
        .take_while(|path| path.len() > 1)
    {
        let Ok(device) = open_device(interface) else {
            continue;
        };
        let mut number = sys::StorageDeviceNumber::default();
        if control(&device, sys::IOCTL_STORAGE_GET_DEVICE_NUMBER, &mut number).is_err() {
            continue;
        }
        let mut instance = [0_u16; NAME];
        let mut size = u32::try_from(size_of_val(&instance)).unwrap_or(0);
        let mut kind = 0;
        // SAFETY: `instance` holds `size` bytes, and the call writes at most that many.
        let found = unsafe {
            sys::CM_Get_Device_Interface_PropertyW(
                interface.as_ptr(),
                &sys::DEVPKEY_DEVICE_INSTANCE_ID,
                &raw mut kind,
                instance.as_mut_ptr().cast(),
                &raw mut size,
                0,
            )
        };
        if found != sys::CR_SUCCESS || kind != sys::DEVPROP_TYPE_STRING {
            continue;
        }
        let mut node = 0;
        // SAFETY: the identifier is a NUL-terminated string that the call wrote.
        if unsafe { sys::CM_Locate_DevNodeW(&raw mut node, instance.as_ptr(), sys::CM_LOCATE_DEVNODE_NORMAL) }
            != sys::CR_SUCCESS
        {
            continue;
        }
        let mut service = [0_u16; NAME];
        let mut size = u32::try_from(size_of_val(&service)).unwrap_or(0);
        // SAFETY: `service` holds `size` bytes, and the call writes at most that many.
        let found = unsafe {
            sys::CM_Get_DevNode_PropertyW(
                node,
                &sys::DEVPKEY_DEVICE_SERVICE,
                &raw mut kind,
                service.as_mut_ptr().cast(),
                &raw mut size,
                0,
            )
        };
        if found != sys::CR_SUCCESS || kind != sys::DEVPROP_TYPE_STRING {
            continue;
        }
        let end = service.iter().position(|&unit| unit == 0).unwrap_or(NAME);
        services.push((number.number, alloc::string::String::from_utf16_lossy(&service[..end])));
    }
    Ok(services)
}
