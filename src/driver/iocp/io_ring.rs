//! The I/O ring of Windows (IoRing), on which a driver of
//! [`Backend::IoRing`](crate::driver::Backend::IoRing) runs the reads, the
//! writes, and the syncs of its files. The port owns one ring. Each call is one entry of the ring,
//! and the completion queue entry of the call is its terminal event: the port keeps the buffer of
//! each entry until that entry.
//!
//! # The wait
//!
//! The kernel signals the event of the ring only when its completion queue goes from empty to not
//! empty (`SetIoRingCompletionEvent`). A wait completion packet watches the event and queues one
//! packet with the key of the ring on the port, so the one port wait of the driver also wakes for
//! the ring, with no thread. At that packet, the port takes each entry until the queue is empty,
//! and only then associates the packet again ([`Ring::arm`]): an entry that the port leaves makes
//! the next completion signal nothing. An association while the event is signaled queues the
//! packet at once (probed on Windows Server 2025, 2026-11-19), so a completion between the last
//! take and the association still wakes the port.
//!
//! # The probe
//!
//! The functions come from `kernel32.dll` and `ntdll.dll` at the first probe of the process,
//! because a build of Windows before 22000 has no IoRing, and a static import would stop the load
//! of each program there. [`Ring::new`] refuses the ring with the `HRESULT` of its first step that
//! fails, and the driver then runs its files on the port.
//!
//! # Bounds
//!
//! The ring rounds its submission queue up to a power of two, at least 8, and its completion queue
//! is twice its submission queue (probed). The port asks for a completion queue with an entry for
//! each of its slots, and each entry keeps its slot until its completion queue entry, so the
//! completion queue never fills. The kernel refuses a whole submit that could overflow it
//! (`IORING_E_COMPLETION_QUEUE_TOO_FULL`), and its entries stay, but the bound makes that case
//! unreachable. At most the entries of the submission queue wait for a submit: the next one
//! submits first.
//!
//! # The close
//!
//! A close of the ring ends no entry: the kernel still writes the buffer of an entry after the
//! close (probed on Windows Server 2025, 2026-11-19). So the drop closes the ring only when no
//! entry is in flight, and else leaks it, as the port leaks its `OVERLAPPED` array.
//!
//! # Declarations
//!
//! Each item agrees with the Windows 11 SDK that the Visual Studio Build Tools 18.11.1 install:
//! `ioringapi.h` (the functions, `HIORING`, `IORING_CAPABILITIES`, `IORING_CREATE_FLAGS`,
//! `IORING_INFO`, `IORING_HANDLE_REF`, `IORING_BUFFER_REF`, `IORING_CQE`, `IORING_REF_KIND`,
//! `IORING_SQE_FLAGS`), `ntioring_x.h` (`IORING_VERSION`, `IORING_OP_CODE`,
//! `IORING_FEATURE_FLAGS`), `winbase.h` (`FILE_WRITE_FLAGS`, `FILE_FLUSH_MODE`), and `winerror.h`
//! (the codes). The wait completion packet functions agree with their pages in the Windows
//! developer notes, which give no header: they come from `ntdll.dll` through `GetProcAddress`.

use core::ffi::{c_int, c_void};
use core::mem::{offset_of, size_of};
use core::ptr;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::sync::OnceLock;

use super::sys::{self, Handle};

/// `HIORING`: the handle of a ring, which only `CloseIoRing` closes.
type Hioring = *mut c_void;

/// `IORING_VERSION_3`: the version that has the write, the flush, and the cancel.
const IORING_VERSION_3: i32 = 300;

/// `IORING_OP_READ`, `IORING_OP_CANCEL`, `IORING_OP_WRITE`, and `IORING_OP_FLUSH`
/// (`IORING_OP_CODE`): the operations that the driver uses, each of which the probe requires.
const OPERATIONS: [i32; 4] = [1, 4, 5, 6];

/// `IORING_FEATURE_UM_EMULATION`: the system emulates the ring in user mode, with no kernel ring.
const IORING_FEATURE_UM_EMULATION: i32 = 1;

/// `IORING_FEATURE_SET_COMPLETION_EVENT`: the ring takes a completion event.
const IORING_FEATURE_SET_COMPLETION_EVENT: i32 = 2;

/// `IORING_REF_RAW`: a handle or a buffer that is not registered.
const IORING_REF_RAW: i32 = 0;

/// `IOSQE_FLAGS_NONE`.
const IOSQE_FLAGS_NONE: i32 = 0;

/// `FILE_WRITE_FLAGS_NONE`.
const FILE_WRITE_FLAGS_NONE: i32 = 0;

/// `FILE_FLUSH_DEFAULT`, which `winbase.h` describes as "same as WIN32 FlushFileBuffers(); Flushes
/// data, metadata, AND sends a SYNC command to the hardware".
const FILE_FLUSH_DEFAULT: i32 = 0;

/// `S_OK`: `PopIoRingCompletion` took an entry. `S_FALSE` means that the queue is empty.
const S_OK: i32 = 0;

/// `GENERIC_ALL` (`winnt.h`): the access of the wait completion packet.
const GENERIC_ALL: u32 = 0x1000_0000;

/// `HRESULT_FROM_WIN32(ERROR_PROC_NOT_FOUND)`: the system has no such function.
const PROC_NOT_FOUND: i32 = 0x8007_007F_u32.cast_signed();

/// `HRESULT_FROM_WIN32(ERROR_NOT_SUPPORTED)`: the ring lacks a feature or an operation that the
/// driver uses.
const NOT_SUPPORTED: i32 = 0x8007_0032_u32.cast_signed();

/// The prefix of the `HRESULT` of a Win32 error (`FACILITY_WIN32` with the failure bit).
const FACILITY_WIN32: u32 = 0x8007_0000;

/// `FACILITY_NT_BIT`: `HRESULT_FROM_NT` sets it in an NT status.
const FACILITY_NT_BIT: u32 = 0x1000_0000;

/// `kernel32.dll` and `ntdll.dll`, as NUL-terminated UTF-16 strings.
const KERNEL32: [u16; 13] = wide(b"kernel32.dll\0");
const NTDLL: [u16; 10] = wide(b"ntdll.dll\0");

/// `IORING_CAPABILITIES`.
#[repr(C)]
#[derive(Debug, Default)]
struct Capabilities {
    max_version: i32,
    max_submissions: u32,
    max_completions: u32,
    features: i32,
}

/// `IORING_CREATE_FLAGS`: no required and no advisory flag.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
struct CreateFlags {
    required: i32,
    advisory: i32,
}

/// `IORING_INFO`: the version and the actual queue sizes of a ring.
#[repr(C)]
#[derive(Debug, Default)]
struct Info {
    version: i32,
    flags: CreateFlags,
    submissions: u32,
    completions: u32,
}

/// `IORING_HANDLE_REF` with the kind `IORING_REF_RAW`: its union holds a `HANDLE`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct HandleRef {
    kind: i32,
    handle: Handle,
}

/// `IORING_BUFFER_REF` with the kind `IORING_REF_RAW`: its union holds an address.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct BufferRef {
    kind: i32,
    address: *mut c_void,
}

/// `IORING_CQE`: the user data of an entry, its `HRESULT`, and its count of bytes.
#[repr(C)]
#[derive(Debug, Default)]
struct Completion {
    user_data: usize,
    result: i32,
    information: usize,
}

const _: () = {
    assert!(size_of::<Capabilities>() == 16);
    assert!(offset_of!(Capabilities, features) == 12);
    assert!(size_of::<CreateFlags>() == 8);
    assert!(size_of::<Info>() == 20);
    assert!(offset_of!(Info, flags) == 4);
    assert!(offset_of!(Info, submissions) == 12);
    assert!(offset_of!(Info, completions) == 16);
    assert!(size_of::<HandleRef>() == 16);
    assert!(offset_of!(HandleRef, handle) == 8);
    assert!(size_of::<BufferRef>() == 16);
    assert!(offset_of!(BufferRef, address) == 8);
    assert!(size_of::<Completion>() == 24);
    assert!(offset_of!(Completion, result) == 8);
    assert!(offset_of!(Completion, information) == 16);
};

/// `HRESULT QueryIoRingCapabilities(IORING_CAPABILITIES* capabilities)`.
type QueryIoRingCapabilities = unsafe extern "system" fn(capabilities: *mut Capabilities) -> i32;
/// `BOOL IsIoRingOpSupported(HIORING ioRing, IORING_OP_CODE op)`.
type IsIoRingOpSupported = unsafe extern "system" fn(ring: Hioring, op: i32) -> i32;
/// `HRESULT CreateIoRing(IORING_VERSION ioringVersion, IORING_CREATE_FLAGS flags, UINT32
/// submissionQueueSize, UINT32 completionQueueSize, HIORING* h)`.
type CreateIoRing = unsafe extern "system" fn(
    version: i32,
    flags: CreateFlags,
    submissions: u32,
    completions: u32,
    ring: *mut Hioring,
) -> i32;
/// `HRESULT GetIoRingInfo(HIORING ioRing, IORING_INFO* info)`.
type GetIoRingInfo = unsafe extern "system" fn(ring: Hioring, info: *mut Info) -> i32;
/// `HRESULT SubmitIoRing(HIORING ioRing, UINT32 waitOperations, UINT32 milliseconds, UINT32*
/// submittedEntries)`.
type SubmitIoRing = unsafe extern "system" fn(ring: Hioring, wait: u32, milliseconds: u32, submitted: *mut u32) -> i32;
/// `HRESULT CloseIoRing(HIORING ioRing)`.
type CloseIoRing = unsafe extern "system" fn(ring: Hioring) -> i32;
/// `HRESULT PopIoRingCompletion(HIORING ioRing, IORING_CQE* cqe)`.
type PopIoRingCompletion = unsafe extern "system" fn(ring: Hioring, completion: *mut Completion) -> i32;
/// `HRESULT SetIoRingCompletionEvent(HIORING ioRing, HANDLE hEvent)`.
type SetIoRingCompletionEvent = unsafe extern "system" fn(ring: Hioring, event: Handle) -> i32;
/// `HRESULT BuildIoRingCancelRequest(HIORING ioRing, IORING_HANDLE_REF file, UINT_PTR opToCancel,
/// UINT_PTR userData)`.
type BuildIoRingCancelRequest =
    unsafe extern "system" fn(ring: Hioring, file: HandleRef, target: usize, user_data: usize) -> i32;
/// `HRESULT BuildIoRingReadFile(HIORING ioRing, IORING_HANDLE_REF fileRef, IORING_BUFFER_REF
/// dataRef, UINT32 numberOfBytesToRead, UINT64 fileOffset, UINT_PTR userData, IORING_SQE_FLAGS
/// sqeFlags)`.
type BuildIoRingReadFile = unsafe extern "system" fn(
    ring: Hioring,
    file: HandleRef,
    buffer: BufferRef,
    len: u32,
    offset: u64,
    user_data: usize,
    flags: i32,
) -> i32;
/// `HRESULT BuildIoRingWriteFile(HIORING ioRing, IORING_HANDLE_REF fileRef, IORING_BUFFER_REF
/// bufferRef, UINT32 numberOfBytesToWrite, UINT64 fileOffset, FILE_WRITE_FLAGS writeFlags, UINT_PTR
/// userData, IORING_SQE_FLAGS sqeFlags)`.
type BuildIoRingWriteFile = unsafe extern "system" fn(
    ring: Hioring,
    file: HandleRef,
    buffer: BufferRef,
    len: u32,
    offset: u64,
    write_flags: i32,
    user_data: usize,
    flags: i32,
) -> i32;
/// `HRESULT BuildIoRingFlushFile(HIORING ioRing, IORING_HANDLE_REF fileRef, FILE_FLUSH_MODE
/// flushMode, UINT_PTR userData, IORING_SQE_FLAGS sqeFlags)`.
type BuildIoRingFlushFile =
    unsafe extern "system" fn(ring: Hioring, file: HandleRef, mode: i32, user_data: usize, flags: i32) -> i32;
/// `NTSTATUS NtCreateWaitCompletionPacket(PHANDLE WaitCompletionPacketHandle, ACCESS_MASK
/// DesiredAccess, POBJECT_ATTRIBUTES ObjectAttributes)`.
type NtCreateWaitCompletionPacket =
    unsafe extern "system" fn(packet: *mut Handle, access: u32, attributes: *mut c_void) -> i32;
/// `NTSTATUS NtAssociateWaitCompletionPacket(HANDLE WaitCompletionPacketHandle, HANDLE
/// IoCompletionHandle, HANDLE TargetObjectHandle, PVOID KeyContext, PVOID ApcContext, NTSTATUS
/// IoStatus, ULONG_PTR IoStatusInformation, PBOOLEAN AlreadySignaled)`.
type NtAssociateWaitCompletionPacket = unsafe extern "system" fn(
    packet: Handle,
    port: Handle,
    target: Handle,
    key: *mut c_void,
    context: *mut c_void,
    status: i32,
    information: usize,
    signaled: *mut u8,
) -> i32;
/// `NTSTATUS NtCancelWaitCompletionPacket(HANDLE WaitCompletionPacketHandle, BOOLEAN
/// RemoveSignaledPacket)`.
type NtCancelWaitCompletionPacket = unsafe extern "system" fn(packet: Handle, remove: u8) -> i32;

/// The functions of the ring and of the wait completion packet, which the first probe of the
/// process takes from the system.
#[derive(Debug)]
struct Api {
    query: QueryIoRingCapabilities,
    supported: IsIoRingOpSupported,
    create: CreateIoRing,
    info: GetIoRingInfo,
    submit: SubmitIoRing,
    close: CloseIoRing,
    pop: PopIoRingCompletion,
    set_event: SetIoRingCompletionEvent,
    cancel: BuildIoRingCancelRequest,
    read: BuildIoRingReadFile,
    write: BuildIoRingWriteFile,
    flush: BuildIoRingFlushFile,
    create_packet: NtCreateWaitCompletionPacket,
    associate: NtAssociateWaitCompletionPacket,
    cancel_packet: NtCancelWaitCompletionPacket,
}

/// The functions of [`Api`], or the `HRESULT` of the search that found one missing.
static API: OnceLock<Result<Api, i32>> = OnceLock::new();

/// The functions of the system, which the first call takes.
fn api() -> Result<&'static Api, i32> {
    API.get_or_init(resolve).as_ref().map_err(|&error| error)
}

/// Takes each function of [`Api`] from `kernel32.dll` and `ntdll.dll`, which each process has.
#[expect(
    unsafe_code,
    reason = "each address is the export of its name, whose declaration is its type"
)]
fn resolve() -> Result<Api, i32> {
    let (kernel32, ntdll) = (module(&KERNEL32)?, module(&NTDLL)?);
    // The export `$name` of `$module` as the function type `$type`.
    macro_rules! export {
        ($module:expr, $name:literal, $type:ty) => {{
            // SAFETY: the module stays loaded, and the name is a NUL-terminated string.
            let address = unsafe { sys::GetProcAddress($module, $name.as_ptr()) };
            if address.is_null() {
                return Err(PROC_NOT_FOUND);
            }
            // SAFETY: the export of the name is the function that its SDK declaration, `$type`,
            // describes, and a function pointer has the size of an address.
            unsafe { core::mem::transmute::<*const c_void, $type>(address) }
        }};
    }
    Ok(Api {
        query: export!(kernel32, c"QueryIoRingCapabilities", QueryIoRingCapabilities),
        supported: export!(kernel32, c"IsIoRingOpSupported", IsIoRingOpSupported),
        create: export!(kernel32, c"CreateIoRing", CreateIoRing),
        info: export!(kernel32, c"GetIoRingInfo", GetIoRingInfo),
        submit: export!(kernel32, c"SubmitIoRing", SubmitIoRing),
        close: export!(kernel32, c"CloseIoRing", CloseIoRing),
        pop: export!(kernel32, c"PopIoRingCompletion", PopIoRingCompletion),
        set_event: export!(kernel32, c"SetIoRingCompletionEvent", SetIoRingCompletionEvent),
        cancel: export!(kernel32, c"BuildIoRingCancelRequest", BuildIoRingCancelRequest),
        read: export!(kernel32, c"BuildIoRingReadFile", BuildIoRingReadFile),
        write: export!(kernel32, c"BuildIoRingWriteFile", BuildIoRingWriteFile),
        flush: export!(kernel32, c"BuildIoRingFlushFile", BuildIoRingFlushFile),
        create_packet: export!(ntdll, c"NtCreateWaitCompletionPacket", NtCreateWaitCompletionPacket),
        associate: export!(
            ntdll,
            c"NtAssociateWaitCompletionPacket",
            NtAssociateWaitCompletionPacket
        ),
        cancel_packet: export!(ntdll, c"NtCancelWaitCompletionPacket", NtCancelWaitCompletionPacket),
    })
}

/// The module of the loaded library `name`, a NUL-terminated UTF-16 string.
#[expect(unsafe_code, reason = "the call reads a NUL-terminated string")]
fn module(name: &[u16]) -> Result<Handle, i32> {
    // SAFETY: the name is a NUL-terminated UTF-16 string, and the call names no other memory.
    let module = unsafe { sys::GetModuleHandleW(name.as_ptr()) };
    if module.is_null() {
        return Err(from_win32(last_error()));
    }
    Ok(module)
}

/// The I/O ring of a port, its completion event, and the wait completion packet that watches the
/// event.
#[derive(Debug)]
pub(crate) struct Ring {
    api: &'static Api,
    ring: Hioring,
    event: OwnedHandle,
    packet: OwnedHandle,
    /// The key of the packet of the ring on the port.
    key: usize,
    /// The entries of the submission queue.
    submissions: u32,
    /// The entries built since the last submit.
    built: u32,
    /// The entries built whose completion queue entry the port has not taken.
    in_flight: u32,
}

impl Ring {
    /// A ring with a completion queue entry for each of `slots` entries in flight, whose event
    /// queues a packet with `key` on `port`. Refuses with the `HRESULT` of the first step that
    /// fails: no such function (`0x8007007F`), a version before 3
    /// (`IORING_E_VERSION_NOT_SUPPORTED`), an emulated ring, a ring with no completion event or
    /// with no read, write, flush, or cancel (`0x80070032`), or a failure of the system.
    #[expect(
        unsafe_code,
        reason = "the calls write structures and handles that live during the calls"
    )]
    pub(crate) fn new(port: Handle, slots: u32, key: usize) -> Result<Self, i32> {
        let api = api()?;
        let mut capabilities = Capabilities::default();
        // SAFETY: the call writes one structure.
        check(unsafe { (api.query)(&raw mut capabilities) })?;
        if capabilities.features & IORING_FEATURE_UM_EMULATION != 0
            || capabilities.features & IORING_FEATURE_SET_COMPLETION_EVENT == 0
        {
            return Err(NOT_SUPPORTED);
        }
        // An event that resets when the packet takes its signal.
        // SAFETY: the call names no memory, and gives a new handle or null.
        let event = unsafe { sys::CreateEventW(ptr::null_mut(), 0, 0, ptr::null()) };
        if event.is_null() {
            return Err(from_win32(last_error()));
        }
        // SAFETY: the call gave a new handle that nothing else owns.
        let event = unsafe { OwnedHandle::from_raw_handle(event) };
        let mut packet = ptr::null_mut();
        // SAFETY: the call writes one handle.
        let status = unsafe { (api.create_packet)(&raw mut packet, GENERIC_ALL, ptr::null_mut()) };
        if status < 0 {
            return Err(from_nt(status));
        }
        // SAFETY: the call gave a new handle that nothing else owns.
        let packet = unsafe { OwnedHandle::from_raw_handle(packet) };
        let mut handle = ptr::null_mut();
        // SAFETY: the call writes one handle.
        let created = unsafe {
            (api.create)(
                IORING_VERSION_3,
                CreateFlags::default(),
                slots.div_ceil(2),
                slots,
                &raw mut handle,
            )
        };
        check(created)?;
        let mut ring = Self {
            api,
            ring: handle,
            event,
            packet,
            key,
            submissions: 0,
            built: 0,
            in_flight: 0,
        };
        let mut info = Info::default();
        // SAFETY: the ring is open, and the call writes one structure.
        check(unsafe { (api.info)(handle, &raw mut info) })?;
        // The completion queue holds an entry for each slot, so it never fills.
        if info.completions < slots || info.submissions == 0 {
            return Err(NOT_SUPPORTED);
        }
        ring.submissions = info.submissions;
        // SAFETY: the ring is open.
        if OPERATIONS.iter().any(|&op| unsafe { (api.supported)(handle, op) } == 0) {
            return Err(NOT_SUPPORTED);
        }
        // SAFETY: the ring and the event are open, and the kernel takes a duplicate of the event.
        check(unsafe { (api.set_event)(handle, ring.event.as_raw_handle()) })?;
        ring.arm(port)?;
        Ok(ring)
    }

    /// The entries of the completion queue that the port can take now, at most: the entries in
    /// flight.
    pub(crate) const fn in_flight(&self) -> u32 {
        self.in_flight
    }

    /// Associates the wait completion packet with the event again, after the port took each entry
    /// of the completion queue: the next signal queues one packet with the key of the ring on
    /// `port`. Gives the error as an `HRESULT`.
    #[expect(unsafe_code, reason = "the call takes open handles and no memory")]
    pub(crate) fn arm(&self, port: Handle) -> Result<(), i32> {
        // SAFETY: the packet, the port, and the event are open, and the packet is not in use: the
        // port took its packet, or it was never associated.
        let status = unsafe {
            (self.api.associate)(
                self.packet.as_raw_handle(),
                port,
                self.event.as_raw_handle(),
                ptr::without_provenance_mut(self.key),
                ptr::null_mut(),
                0,
                0,
                ptr::null_mut(),
            )
        };
        if status < 0 { Err(from_nt(status)) } else { Ok(()) }
    }

    /// Builds a read of `len` bytes at `offset` of `file` into `buffer`, with the user data `user`.
    /// Gives the Win32 error of a refusal: then no entry exists.
    ///
    /// # Safety
    ///
    /// `buffer` holds `len` bytes that nothing reads or writes until the completion queue entry of
    /// `user`, and `file` stays open until then.
    #[expect(unsafe_code, reason = "the caller keeps the buffer and the file")]
    pub(crate) unsafe fn read(
        &mut self,
        file: Handle,
        buffer: *mut c_void,
        len: u32,
        offset: u64,
        user: u32,
    ) -> Result<(), c_int> {
        self.room()?;
        // SAFETY: the ring is open, and the caller keeps the buffer and the file.
        let built = unsafe {
            (self.api.read)(
                self.ring,
                raw(file),
                BufferRef {
                    kind: IORING_REF_RAW,
                    address: buffer,
                },
                len,
                offset,
                user as usize,
                IOSQE_FLAGS_NONE,
            )
        };
        self.built(built)
    }

    /// Builds a write of the `len` bytes at `buffer` at `offset` of `file`, with the user data
    /// `user`. Gives the Win32 error of a refusal: then no entry exists.
    ///
    /// # Safety
    ///
    /// `buffer` holds `len` bytes that nothing writes until the completion queue entry of `user`,
    /// and `file` stays open until then.
    #[expect(unsafe_code, reason = "the caller keeps the bytes and the file")]
    pub(crate) unsafe fn write(
        &mut self,
        file: Handle,
        buffer: *const c_void,
        len: u32,
        offset: u64,
        user: u32,
    ) -> Result<(), c_int> {
        self.room()?;
        // SAFETY: the ring is open, and the caller keeps the bytes and the file. The kernel only
        // reads the buffer of a write.
        let built = unsafe {
            (self.api.write)(
                self.ring,
                raw(file),
                BufferRef {
                    kind: IORING_REF_RAW,
                    address: buffer.cast_mut(),
                },
                len,
                offset,
                FILE_WRITE_FLAGS_NONE,
                user as usize,
                IOSQE_FLAGS_NONE,
            )
        };
        self.built(built)
    }

    /// Builds a flush of `file` with `FILE_FLUSH_DEFAULT`, with the user data `user`. Gives the
    /// Win32 error of a refusal: then no entry exists.
    #[expect(unsafe_code, reason = "the call names a handle and no memory")]
    pub(crate) fn flush(&mut self, file: Handle, user: u32) -> Result<(), c_int> {
        self.room()?;
        // SAFETY: the ring is open, and the entry names no memory.
        let built = unsafe {
            (self.api.flush)(
                self.ring,
                raw(file),
                FILE_FLUSH_DEFAULT,
                user as usize,
                IOSQE_FLAGS_NONE,
            )
        };
        self.built(built)
    }

    /// Builds a cancel of the entry with the user data `target` on `file`, with the user data
    /// `user`. Its own completion queue entry gives its result, which ends no entry: the target
    /// ends at its own entry. Gives the Win32 error of a refusal: then no entry exists.
    #[expect(unsafe_code, reason = "the call names a handle and no memory")]
    pub(crate) fn cancel(&mut self, file: Handle, target: u32, user: u32) -> Result<(), c_int> {
        self.room()?;
        // SAFETY: the ring is open, and the entry names no memory.
        let built = unsafe { (self.api.cancel)(self.ring, raw(file), target as usize, user as usize) };
        self.built(built)
    }

    /// Submits the built entries, if any. Gives the error as an `HRESULT`: then the entries stay
    /// for the next submit.
    #[expect(unsafe_code, reason = "the call takes the open ring and writes no memory")]
    pub(crate) fn submit(&mut self) -> Result<(), i32> {
        if self.built == 0 {
            return Ok(());
        }
        // SAFETY: the ring is open, and a submit with no wait writes no count.
        check(unsafe { (self.api.submit)(self.ring, 0, 0, ptr::null_mut()) })?;
        self.built = 0;
        Ok(())
    }

    /// Takes the next completion queue entry: its user data, and its count of bytes or its Win32
    /// error. `None` when the queue is empty.
    #[expect(unsafe_code, reason = "the call writes one structure")]
    pub(crate) fn pop(&mut self) -> Option<(u32, Result<u32, c_int>)> {
        let mut completion = Completion::default();
        // SAFETY: the ring is open, and the call writes one structure.
        if unsafe { (self.api.pop)(self.ring, &raw mut completion) } != S_OK {
            return None;
        }
        assert!(
            self.in_flight > 0,
            "each completion queue entry is an entry that the ring built"
        );
        self.in_flight -= 1;
        let result = if completion.result < 0 {
            Err(win32(completion.result))
        } else {
            Ok(u32::try_from(completion.information).unwrap_or(u32::MAX))
        };
        Some((u32::try_from(completion.user_data).unwrap_or(u32::MAX), result))
    }

    /// Submits first when the submission queue is full, so the next entry has room.
    fn room(&mut self) -> Result<(), c_int> {
        if self.built >= self.submissions {
            self.submit().map_err(win32)?;
        }
        Ok(())
    }

    /// Counts the entry that a build gave with `result`, or gives the Win32 error of a refusal.
    fn built(&mut self, result: i32) -> Result<(), c_int> {
        if result < 0 {
            return Err(win32(result));
        }
        self.built += 1;
        self.in_flight += 1;
        Ok(())
    }
}

impl Drop for Ring {
    /// Ends the association of the packet. Closes the ring only when no entry is in flight: a close
    /// ends no entry, and the kernel still writes its buffer.
    #[expect(unsafe_code, reason = "the calls take open handles")]
    fn drop(&mut self) {
        // SAFETY: the packet is open, and the call removes a packet that waits in the port.
        let _status = unsafe { (self.api.cancel_packet)(self.packet.as_raw_handle(), 1) };
        if self.in_flight == 0 {
            // SAFETY: the ring is open, and no entry of it is in flight.
            let _closed = unsafe { (self.api.close)(self.ring) };
        }
    }
}

/// The reference of the handle `file`, which is not registered.
const fn raw(file: Handle) -> HandleRef {
    HandleRef {
        kind: IORING_REF_RAW,
        handle: file,
    }
}

/// `Ok` for an `HRESULT` that is not a failure.
const fn check(result: i32) -> Result<(), i32> {
    if result < 0 { Err(result) } else { Ok(()) }
}

/// The Win32 error of the failed `HRESULT` `result`: its code for `FACILITY_WIN32`, as each
/// failure of an entry that the probe saw gives it, and `ERROR_INVALID_FUNCTION` for another
/// facility, which no probe saw.
fn win32(result: i32) -> c_int {
    let result = result.cast_unsigned();
    if result & 0xFFFF_0000 == FACILITY_WIN32 {
        c_int::try_from(result & 0xFFFF).unwrap_or(sys::ERROR_INVALID_FUNCTION)
    } else {
        sys::ERROR_INVALID_FUNCTION
    }
}

/// `HRESULT_FROM_WIN32(error)`.
const fn from_win32(error: c_int) -> i32 {
    if error <= 0 {
        error
    } else {
        (error.cast_unsigned() & 0xFFFF | FACILITY_WIN32).cast_signed()
    }
}

/// `HRESULT_FROM_NT(status)`.
const fn from_nt(status: i32) -> i32 {
    (status.cast_unsigned() | FACILITY_NT_BIT).cast_signed()
}

/// The Win32 error of the last call of this thread.
fn last_error() -> c_int {
    std::io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(sys::ERROR_INVALID_FUNCTION)
}

/// The ASCII string `name` as UTF-16.
const fn wide<const N: usize>(name: &[u8; N]) -> [u16; N] {
    let mut wide = [0; N];
    let mut index = 0;
    while index < N {
        wide[index] = name[index] as u16;
        index += 1;
    }
    wide
}
