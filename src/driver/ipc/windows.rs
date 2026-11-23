//! IPC streams on Windows: byte-mode named pipes at `\\.\pipe\<name>`.
//!
//! A listener holds [`BACKLOG`] instances of its pipe, which wait for clients, as the queue of a
//! listening socket does. The first instance takes the name (`FILE_FLAG_FIRST_PIPE_INSTANCE`), so a
//! second listener of the name, or a process that took the name first, makes the bind fail. Each
//! instance rejects remote clients, and its security descriptor grants access only to the user of
//! this process. A client takes the first instance that waits, in the order of their make, and an
//! accept takes the first instance. The listener then makes a new instance, with the same
//! descriptor, after the others.

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::ffi::c_void;
use core::fmt::{self, Write as _};
use core::ptr::{self, NonNull};
use std::io;
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::path::Path;

use super::super::file::windows::number;
use super::super::iocp::sys;
use crate::memory::room;

/// The prefix of the name of a local pipe.
const PREFIX: &str = r"\\.\pipe\";

/// The most UTF-16 units of a pipe name with its prefix (`CreateNamedPipeW`).
const NAME_MAX: usize = 256;

/// The bytes that each direction of an instance buffers.
const BUFFER: u32 = 1 << 16;

/// `ERROR_TOO_MANY_OPEN_FILES` (`winerror.h`): the handle of a pipe has significant bits above the
/// low 32, so no entry can hold it.
const ERROR_TOO_MANY_OPEN_FILES: i32 = 4;

/// The security identifier (SID) of a Windows user, as the token of the user holds it. Its
/// `Display` form is the string form, for example `S-1-5-21-…-500`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sid {
    len: u8,
    bytes: [u8; sys::SECURITY_MAX_SID_SIZE],
}

impl Sid {
    /// The SID of the user of `token`.
    #[expect(unsafe_code, reason = "the call writes the user of the token into owned storage")]
    fn of_token(token: &OwnedHandle) -> Result<Self, i32> {
        // `TOKEN_USER` and its SID: at most 16 and 68 bytes, with the alignment of a pointer.
        let mut buffer = [0_usize; 16];
        let mut returned = 0;
        let len = u32::try_from(size_of_val(&buffer)).unwrap_or(0);
        // SAFETY: the buffer holds `len` bytes with the alignment of `TOKEN_USER`, and the call
        // writes at most that many.
        let read = unsafe {
            sys::GetTokenInformation(
                token.as_raw_handle(),
                sys::TOKEN_USER,
                buffer.as_mut_ptr().cast(),
                len,
                &raw mut returned,
            )
        };
        if read == 0 {
            return Err(last_error());
        }
        // SAFETY: the call wrote a `TOKEN_USER` at the start of the buffer, whose SID is in the
        // buffer and valid.
        unsafe { Self::copy(buffer.as_ptr().cast::<sys::TokenUser>().read().sid) }
    }

    /// A copy of the valid SID at `sid`.
    ///
    /// # Safety
    ///
    /// `sid` points to a valid SID.
    #[expect(unsafe_code, reason = "the caller gives a valid SID")]
    unsafe fn copy(sid: *mut c_void) -> Result<Self, i32> {
        // SAFETY: the caller's contract.
        let len = unsafe { sys::GetLengthSid(sid) } as usize;
        let mut copy = Self {
            len: u8::try_from(len).map_err(|_| sys::ERROR_INVALID_PARAMETER.saturating_neg())?,
            bytes: [0; sys::SECURITY_MAX_SID_SIZE],
        };
        let bytes = copy
            .bytes
            .get_mut(..len)
            .ok_or(sys::ERROR_INVALID_PARAMETER.saturating_neg())?;
        // SAFETY: the SID has `len` bytes, and `bytes` has room for them.
        unsafe { ptr::copy_nonoverlapping(sid.cast::<u8>(), bytes.as_mut_ptr(), len) };
        Ok(copy)
    }

    /// The bytes of the SID, in the binary form of the system.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.get(..usize::from(self.len)).unwrap_or_default()
    }
}

impl fmt::Display for Sid {
    /// The string form `S-<revision>-<authority>-<subauthority>…` of `ConvertSidToStringSidW`: the
    /// authority in decimal below 2^32, and else in hexadecimal.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.as_bytes();
        let (Some(&revision), Some(authority)) = (bytes.first(), bytes.get(2..8)) else {
            return f.write_str("S-?");
        };
        let authority = authority
            .iter()
            .fold(0_u64, |value, &byte| (value << 8) | u64::from(byte));
        if authority >> 32 == 0 {
            write!(f, "S-{revision}-{authority}")?;
        } else {
            write!(f, "S-{revision}-0x{authority:012X}")?;
        }
        for part in bytes.get(8..).unwrap_or_default().as_chunks::<4>().0 {
            write!(f, "-{}", u32::from_le_bytes(*part))?;
        }
        Ok(())
    }
}

impl fmt::Debug for Sid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sid({self})")
    }
}

/// The instances of a pipe listener that wait for clients: a burst of this many clients connects
/// before an accept, and each client after them gets `ResourceBusy`, the Windows form of a full
/// backlog. Each instance is one kernel object and one handle on the port. The 64 KiB buffer of
/// each direction is a quota that only bytes in the pipe use, and an instance takes no resource
/// slot before its accept.
pub(crate) const BACKLOG: usize = 64;

/// What a pipe listener keeps to make its next instances: the name, the security descriptor, and
/// the instances after the first, in the order of their make.
#[derive(Debug)]
pub(crate) struct Listening {
    name: Vec<u16>,
    descriptor: Descriptor,
    pub(crate) waiting: VecDeque<OwnedHandle>,
}

/// A self-relative security descriptor that `LocalAlloc` gave, which grants access only to the
/// user of this process. Its drop frees it.
#[derive(Debug)]
struct Descriptor(NonNull<c_void>);

impl Drop for Descriptor {
    #[expect(
        unsafe_code,
        reason = "the descriptor came from LocalAlloc, and nothing else uses it"
    )]
    fn drop(&mut self) {
        // SAFETY: the conversion allocated the descriptor with `LocalAlloc`, and this is its one free.
        unsafe { sys::LocalFree(self.0.as_ptr()) };
    }
}

/// The name of the pipe at `path` as a NUL-terminated UTF-16 string: `\\.\pipe\` and a name of at
/// least one unit, at most 256 units in all. The negative `ERROR_FILENAME_EXCED_RANGE` for a
/// longer pipe name, and the negative `ERROR_INVALID_PARAMETER` for each other path.
pub(crate) fn name(path: &Path) -> Result<Vec<u16>, i32> {
    let mut name: Vec<u16> = path.as_os_str().encode_wide().collect();
    let prefix: Vec<u16> = PREFIX.encode_utf16().collect();
    let starts = name.get(..prefix.len()).is_some_and(|start| {
        start
            .iter()
            .zip(&prefix)
            .all(|(&unit, &expected)| lower(unit) == expected)
    });
    if !starts || name.len() <= prefix.len() || name.contains(&0) {
        return Err(sys::ERROR_INVALID_PARAMETER.saturating_neg());
    }
    if name.len() > NAME_MAX {
        return Err(sys::ERROR_FILENAME_EXCED_RANGE.saturating_neg());
    }
    name.push(0);
    Ok(name)
}

/// `unit` in lower case, for an ASCII letter.
fn lower(unit: u16) -> u16 {
    u8::try_from(unit).map_or(unit, |byte| u16::from(byte.to_ascii_lowercase()))
}

/// The negative Win32 error of the last call of this thread.
fn last_error() -> i32 {
    io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(sys::ERROR_INVALID_PARAMETER)
        .saturating_neg()
}

/// Makes the first instance of the pipe `name`, which takes the name. Gives the instance and what
/// the listener keeps, or the negative error: `WSAEADDRINUSE` when an instance of the name exists.
pub(crate) fn listen(name: Vec<u16>) -> Result<(OwnedHandle, Listening), i32> {
    let waiting = room(BACKLOG - 1).map_err(|_| sys::ERROR_NOT_ENOUGH_MEMORY.saturating_neg())?;
    let mut listening = Listening {
        name,
        descriptor: owner_only()?,
        waiting: VecDeque::from(waiting),
    };
    let first = match instance(&listening, sys::FILE_FLAG_FIRST_PIPE_INSTANCE) {
        Ok(instance) => instance,
        // The first-instance flag refuses a name that another instance holds.
        Err(error) if error == sys::ERROR_ACCESS_DENIED.saturating_neg() => {
            return Err(sys::WSAEADDRINUSE.saturating_neg());
        }
        Err(error) => return Err(error),
    };
    for _ in 1..BACKLOG {
        let next = instance(&listening, 0)?;
        listening.waiting.push_back(next);
    }
    Ok((first, listening))
}

/// Makes an instance of the pipe of `listening`, overlapped, in byte mode, with no remote client,
/// with the descriptor of `listening`, and with the open flags `flags`.
#[expect(
    unsafe_code,
    reason = "the call reads the name and the descriptor and gives a new handle"
)]
pub(crate) fn instance(listening: &Listening, flags: u32) -> Result<OwnedHandle, i32> {
    let mut security = sys::SecurityAttributes {
        len: u32::try_from(size_of::<sys::SecurityAttributes>()).unwrap_or(0),
        descriptor: listening.descriptor.0.as_ptr(),
        inherit: 0,
    };
    // SAFETY: the name is NUL-terminated, and the descriptor lives during the call.
    let raw = unsafe {
        sys::CreateNamedPipeW(
            listening.name.as_ptr(),
            sys::PIPE_ACCESS_DUPLEX | sys::FILE_FLAG_OVERLAPPED | flags,
            sys::PIPE_BYTES | sys::PIPE_REJECT_REMOTE_CLIENTS,
            sys::PIPE_UNLIMITED_INSTANCES,
            BUFFER,
            BUFFER,
            0,
            &raw mut security,
        )
    };
    owned(raw)
}

/// The handle `raw` that a call gave, or the negative error of the call. A handle with
/// significant bits above the low 32 closes and gives `ERROR_TOO_MANY_OPEN_FILES`.
#[expect(unsafe_code, reason = "the call gave a new handle that nothing else owns")]
fn owned(raw: sys::Handle) -> Result<OwnedHandle, i32> {
    if raw.addr() == sys::INVALID_HANDLE_VALUE {
        return Err(last_error());
    }
    // SAFETY: the call gave a new handle that nothing else owns.
    let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
    number(raw).ok_or(ERROR_TOO_MANY_OPEN_FILES.saturating_neg())?;
    Ok(handle)
}

/// Opens the pipe `name` as a client, overlapped, at the identification level: the server can
/// read the identity of the client and cannot act as the client. Gives the negative error, with
/// `ERROR_BUSY` when each instance of the pipe has a client.
#[expect(unsafe_code, reason = "the call reads the name and gives a new handle")]
pub(crate) fn connect(name: &[u16]) -> Result<OwnedHandle, i32> {
    // SAFETY: the name is NUL-terminated, and the call names no other memory.
    let raw = unsafe {
        sys::CreateFileW(
            name.as_ptr(),
            sys::GENERIC_READ | sys::GENERIC_WRITE,
            0,
            ptr::null_mut(),
            sys::OPEN_EXISTING,
            sys::FILE_FLAG_OVERLAPPED | sys::SECURITY_IDENTIFICATION,
            ptr::null_mut(),
        )
    };
    owned(raw).map_err(|error| {
        if error == sys::ERROR_PIPE_BUSY.saturating_neg() {
            sys::ERROR_BUSY.saturating_neg()
        } else {
            error
        }
    })
}

/// What the driver knows of the user of the client of an accepted pipe stream. Windows has the
/// user of a client when it has its security context: a client of static tracking, as one of
/// `connect_ipc`, gives it at the open, and a client of dynamic tracking, the default of
/// `CreateFileW`, at its first write. The driver reads the user one time, at the accept or at the
/// first receive that takes data, and keeps it for the life of the stream, as Unix records it at
/// the connect.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Client {
    /// The system has no context of the client yet: the first receive that takes data reads it.
    Pending,
    /// The user that the driver read.
    Known(Sid),
    /// The Win32 error of the read of the user, which `ipc_peer` gives.
    Failed(i32),
}

impl Client {
    /// The user of the client of the pipe instance `instance`, read now, at the accept.
    pub(crate) fn at_accept(instance: &OwnedHandle) -> Self {
        match client_user(instance) {
            Ok(user) => Self::Known(user),
            Err(ERROR_CANNOT_IMPERSONATE) => Self::Pending,
            Err(error) => Self::Failed(error),
        }
    }

    /// The user of the client of the pipe instance `instance`, read after the first data.
    pub(crate) fn after_data(instance: &OwnedHandle) -> Self {
        match client_user(instance) {
            Ok(user) => Self::Known(user),
            Err(error) => Self::Failed(error),
        }
    }
}

/// `ERROR_CANNOT_IMPERSONATE` (`winerror.h`): the server has read no data of a client whose
/// security context the system takes at its writes.
pub(crate) const ERROR_CANNOT_IMPERSONATE: i32 = 1368;

/// The process ID of the client of the pipe instance `instance`, which the system recorded at the
/// connect.
#[expect(unsafe_code, reason = "the call writes one `ULONG`")]
pub(crate) fn client_process(instance: &OwnedHandle) -> io::Result<u32> {
    let mut process = 0;
    // SAFETY: the call writes one `ULONG`.
    if unsafe { sys::GetNamedPipeClientProcessId(instance.as_raw_handle(), &raw mut process) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(process)
}

/// The user of the token of the client of the pipe instance `instance`, which the thread reads
/// while it impersonates the client, at the identification level. Gives the Win32 error of a
/// failed call.
///
/// # Panics
///
/// When `RevertToSelf` fails, so the thread would go on as the client: the process must not go on.
#[expect(unsafe_code, reason = "the calls write into storage that lives during them")]
fn client_user(instance: &OwnedHandle) -> Result<Sid, i32> {
    let error = || {
        io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(sys::ERROR_INVALID_PARAMETER)
    };
    // SAFETY: the instance is open. From here to `RevertToSelf`, the thread acts as the client.
    if unsafe { sys::ImpersonateNamedPipeClient(instance.as_raw_handle()) } == 0 {
        return Err(error());
    }
    let mut token = ptr::null_mut();
    // SAFETY: the call writes one handle. The access check of the open uses the process, not the
    // client.
    let opened = unsafe { sys::OpenThreadToken(sys::GetCurrentThread(), sys::TOKEN_QUERY, 1, &raw mut token) };
    let opened = if opened == 0 { Err(error()) } else { Ok(token) };
    // SAFETY: the call takes no argument.
    let reverted = unsafe { sys::RevertToSelf() } != 0;
    assert!(
        reverted,
        "the driver thread goes back to its own identity after it reads a client"
    );
    // SAFETY: the call gave a new handle that nothing else owns.
    let token = unsafe { OwnedHandle::from_raw_handle(opened?) };
    Sid::of_token(&token).map_err(i32::saturating_neg)
}

/// The process ID and the user of the server of the pipe client `client`: the process ID that the
/// system recorded at the bind, and the owner of the pipe, which its server set. A process with no
/// privilege to restore or take ownership cannot set another user as an owner, and the server
/// of the driver sets the user of its process.
#[expect(unsafe_code, reason = "the calls write into storage that lives during them")]
pub(crate) fn server(client: &OwnedHandle) -> io::Result<(u32, Sid)> {
    let mut process = 0;
    // SAFETY: the call writes one `ULONG`.
    if unsafe { sys::GetNamedPipeServerProcessId(client.as_raw_handle(), &raw mut process) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let (mut owner, mut descriptor) = (ptr::null_mut(), ptr::null_mut());
    // SAFETY: the call writes the owner, which points into the descriptor, and the descriptor, which
    // it allocates with `LocalAlloc`.
    let error = unsafe {
        sys::GetSecurityInfo(
            client.as_raw_handle(),
            sys::SE_FILE_OBJECT,
            sys::OWNER_SECURITY_INFORMATION,
            &raw mut owner,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &raw mut descriptor,
        )
    };
    if error != 0 {
        return Err(io::Error::from_raw_os_error(i32::try_from(error).unwrap_or(i32::MAX)));
    }
    // The drop frees the descriptor, after the copy of its owner.
    let descriptor = NonNull::new(descriptor).map(Descriptor);
    if owner.is_null() || descriptor.is_none() {
        return Err(io::ErrorKind::InvalidData.into());
    }
    // SAFETY: the call gave a valid owner SID in the descriptor, which lives until the copy ends.
    let user = unsafe { Sid::copy(owner) }.map_err(|error| io::Error::from_raw_os_error(error.saturating_neg()))?;
    drop(descriptor);
    Ok((process, user))
}

/// The security descriptor that grants access only to the user of this process, who also owns
/// it, and that no inherited entry changes: `O:<user>D:P(A;;GA;;;<user>)`.
#[expect(
    unsafe_code,
    reason = "the calls give owned strings and a descriptor that this function frees"
)]
fn owner_only() -> Result<Descriptor, i32> {
    let mut token = ptr::null_mut();
    // SAFETY: the call writes one handle of the token of this process.
    if unsafe { sys::OpenProcessToken(sys::GetCurrentProcess(), sys::TOKEN_QUERY, &raw mut token) } == 0 {
        return Err(last_error());
    }
    // SAFETY: the call gave a new handle that nothing else owns.
    let token = unsafe { OwnedHandle::from_raw_handle(token) };
    let user = Sid::of_token(&token)?;
    let mut sddl = Wide::new();
    write!(sddl, "O:{user}D:P(A;;GA;;;{user})").map_err(|_| sys::ERROR_INVALID_PARAMETER.saturating_neg())?;
    let mut descriptor = ptr::null_mut();
    // SAFETY: the string is NUL-terminated, and the call writes one pointer to a descriptor that it
    // allocates.
    let converted = unsafe {
        sys::ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.units.as_ptr(),
            sys::SDDL_REVISION_1,
            &raw mut descriptor,
            ptr::null_mut(),
        )
    };
    if converted == 0 {
        return Err(last_error());
    }
    NonNull::new(descriptor)
        .map(Descriptor)
        .ok_or(sys::ERROR_INVALID_PARAMETER.saturating_neg())
}

/// The most UTF-16 units of the descriptor string of [`owner_only`]: two SIDs of at most 185
/// characters each, 15 more characters, and the NUL.
const SDDL_UNITS: usize = 400;

/// A NUL-terminated UTF-16 string in fixed storage, which `write!` fills.
struct Wide {
    units: [u16; SDDL_UNITS],
    len: usize,
}

impl Wide {
    const fn new() -> Self {
        Self {
            units: [0; SDDL_UNITS],
            len: 0,
        }
    }
}

impl fmt::Write for Wide {
    /// Adds `text`, or gives an error if the storage has no room for it and the NUL.
    fn write_str(&mut self, text: &str) -> fmt::Result {
        for unit in text.encode_utf16() {
            // The last unit stays for the NUL.
            if self.len + 1 >= SDDL_UNITS {
                return Err(fmt::Error);
            }
            self.units[self.len] = unit;
            self.len += 1;
        }
        Ok(())
    }
}

// Two attributes, so that Clippy sees the test module (`allow-unwrap-in-tests`).
#[cfg(test)]
#[cfg(not(loom))]
pub(in crate::driver) mod tests;
