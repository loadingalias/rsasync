//! The declarations of the completion port, the overlapped socket calls, and the NT statuses that
//! the IOCP backend uses, on Windows x86_64.
//!
//! Each item agrees with the Windows 11 SDK that the Visual Studio Build Tools 18.11.1 install:
//! `minwinbase.h` (`OVERLAPPED`, `OVERLAPPED_ENTRY`), `ioapiset.h` (`CreateIoCompletionPort`,
//! `GetQueuedCompletionStatusEx`, `PostQueuedCompletionStatus`, `CancelIoEx`), `winbase.h`
//! (`SetFileCompletionNotificationModes` and its flags), `winsock2.h` (the socket functions,
//! `WSADATA`, `WSAPROTOCOL_INFOW`, the socket options, `SIO_BASE_HANDLE`, `FIONBIO`),
//! `ws2def.h` (`SOL_SOCKET`, `IPPROTO_TCP`), `ws2ipdef.h` (`TCP_NODELAY`), `mswsock.h`
//! (`WSAID_ACCEPTEX`, `WSAID_CONNECTEX`, `LPFN_ACCEPTEX`, `LPFN_CONNECTEX`, and the update
//! options), `ntstatus.h` (the statuses), and `winerror.h` (the error codes). The file items agree
//! with `fileapi.h` (`CreateFileW`, `ReadFile`, `WriteFile`, `FlushFileBuffers`, `DeleteFileW`,
//! `GetFinalPathNameByHandleW`, `GetFullPathNameW`), `winbase.h` (`ReOpenFile`, `MoveFileExW`,
//! `SetFileInformationByHandle`, `GetFileInformationByHandleEx`, and the file flags),
//! `minwinbase.h` (`FILE_INFO_BY_HANDLE_CLASS` and its structures), `winnt.h` (the access rights
//! and the share modes), `synchapi.h` (`CreateEventW`), `libloaderapi.h` (`GetModuleHandleW`,
//! `GetProcAddress`), `ioapiset.h` (`DeviceIoControl`, `GetOverlappedResult`), `winioctl.h`
//! (the control codes and their structures), `cfgmgr32.h` (the device functions), `devpkey.h`
//! (the device properties), and `ntddstor.h` (`GUID_DEVINTERFACE_DISK`). The named pipe items agree
//! with `namedpipeapi.h` and `winbase.h` (the pipe functions and modes), `processthreadsapi.h`
//! (`OpenProcessToken`, `OpenThreadToken`, `GetCurrentProcess`, `GetCurrentThread`),
//! `securitybaseapi.h` (`GetTokenInformation`, `RevertToSelf`, `GetLengthSid`), `aclapi.h`
//! (`GetSecurityInfo`, `SE_OBJECT_TYPE`), `sddl.h` (the
//! string form of a security descriptor), `winnt.h` (`TOKEN_USER`, `TOKEN_QUERY`), and
//! `minwinbase.h` (`SECURITY_ATTRIBUTES`). `kernel32.dll`, `ws2_32.dll`, `advapi32.dll`, and
//! `cfgmgr32.dll` give the functions; `AcceptEx` and `ConnectEx` come only as
//! pointers from `WSAIoctl`. The datagram declarations and the shared socket calls are in
//! `rsnetwork_udp::sys`.

use core::ffi::{c_char, c_int, c_void};
use core::mem::{offset_of, size_of};

pub(crate) use rsnetwork_udp::sys::{
    Guid, IPPROTO_IPV6, IPV6_V6ONLY, SIO_GET_EXTENSION_FUNCTION_POINTER, SO_PROTOCOL_INFOW, SOCK_DGRAM, SOCKET_ERROR,
    SOL_SOCKET, Socket, WSAIoctl, WsaBuf, WsaProtocolInfoW, bind, getpeername, getsockname, getsockopt, setsockopt,
};

/// `HANDLE`.
pub(crate) type Handle = *mut c_void;

/// `INVALID_HANDLE_VALUE`, the handle -1: with it, `CreateIoCompletionPort` makes a new port.
pub(crate) const INVALID_HANDLE_VALUE: usize = usize::MAX;

/// `OVERLAPPED`: the state of one overlapped call. The kernel writes the NT status of the call into
/// `internal` and its transferred bytes into `internal_high`.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct Overlapped {
    /// `Internal`: the NT status (`ULONG_PTR`).
    pub(crate) internal: usize,
    /// `InternalHigh`: the transferred bytes (`ULONG_PTR`).
    pub(crate) internal_high: usize,
    /// `Offset` and `OffsetHigh`, a union with `Pointer`: 0 for a socket.
    pub(crate) offset: [u32; 2],
    /// `hEvent`: no event.
    pub(crate) event: Handle,
}

/// `OVERLAPPED_ENTRY`: one packet that `GetQueuedCompletionStatusEx` removed from the port.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct OverlappedEntry {
    /// `lpCompletionKey`: the key of the association, or of a posted packet (`ULONG_PTR`).
    pub(crate) key: usize,
    /// `lpOverlapped`: the overlapped structure of the call, or null for a posted packet.
    pub(crate) overlapped: *mut Overlapped,
    /// `Internal`: reserved.
    pub(crate) internal: usize,
    /// `dwNumberOfBytesTransferred`.
    pub(crate) bytes: u32,
}

/// `WSADATA` on a 64-bit target: the Winsock version and facts that `WSAStartup` writes.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct WsaData {
    pub(crate) version: u16,
    pub(crate) high_version: u16,
    pub(crate) max_sockets: u16,
    pub(crate) max_udp_dg: u16,
    pub(crate) vendor_info: *mut c_char,
    /// `szDescription` (`WSADESCRIPTION_LEN + 1`) and `szSystemStatus` (`WSASYS_STATUS_LEN + 1`).
    pub(crate) description_and_status: [u8; 257 + 129],
}

const _: () = {
    assert!(size_of::<Overlapped>() == 32);
    assert!(offset_of!(Overlapped, internal_high) == 8);
    assert!(offset_of!(Overlapped, offset) == 16);
    assert!(offset_of!(Overlapped, event) == 24);
    assert!(size_of::<OverlappedEntry>() == 32);
    assert!(offset_of!(OverlappedEntry, overlapped) == 8);
    assert!(offset_of!(OverlappedEntry, internal) == 16);
    assert!(offset_of!(OverlappedEntry, bytes) == 24);
    assert!(size_of::<WsaData>() == 408);
    assert!(offset_of!(WsaData, vendor_info) == 8);
    assert!(offset_of!(WsaData, description_and_status) == 16);
    assert!(size_of::<Socket>() == size_of::<usize>());
};

/// `STATUS_PENDING`: the call runs, and its packet comes later.
pub(crate) const STATUS_PENDING: u32 = 0x0000_0103;

/// `NT_ERROR`: the severity of `status` is an error. No packet comes for a call that returns an
/// error status.
pub(crate) const fn nt_error(status: u32) -> bool {
    status >> 30 == 3
}

/// `NT_WARNING`: the severity of `status` is a warning, for example `STATUS_BUFFER_OVERFLOW`
/// (0x80000005) for a datagram larger than its buffer. A packet comes for it.
pub(crate) const fn nt_warning(status: u32) -> bool {
    status >> 30 == 2
}

/// `WSAEMSGSIZE`: a datagram did not fit its buffer.
pub(crate) const WSAEMSGSIZE: c_int = 10040;

/// `ERROR_IO_PENDING`, also `WSA_IO_PENDING`: the overlapped call runs.
pub(crate) const ERROR_IO_PENDING: c_int = 997;
/// `ERROR_NOT_FOUND`: `CancelIoEx` found no call of the overlapped structure.
pub(crate) const ERROR_NOT_FOUND: c_int = 1168;
/// `WAIT_TIMEOUT`: `GetQueuedCompletionStatusEx` removed no packet in its time.
pub(crate) const WAIT_TIMEOUT: c_int = 258;
/// `WSAEWOULDBLOCK`: a nonblocking call would wait.
pub(crate) const WSAEWOULDBLOCK: c_int = 10035;
/// `WSAECONNABORTED`: the stack aborted the connection, for example at the cancel of a send that
/// runs.
pub(crate) const WSAECONNABORTED: c_int = 10053;
/// `WSAENOPROTOOPT`: the socket has no such option, for example `SO_REUSEPORT` on Windows.
pub(crate) const WSAENOPROTOOPT: c_int = 10042;
/// `WSAEINVAL`: an argument is not valid, for example a `ConnectEx` on a socket with no bind.
pub(crate) const WSAEINVAL: c_int = 10022;

/// `INFINITE`: a wait with no limit.
pub(crate) const INFINITE: u32 = u32::MAX;

/// `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`: a call that returns at once queues no packet.
pub(crate) const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: u8 = 0x1;
/// `FILE_SKIP_SET_EVENT_ON_HANDLE`: a call does not signal the handle.
pub(crate) const FILE_SKIP_SET_EVENT_ON_HANDLE: u8 = 0x2;

/// `SOCK_STREAM`.
pub(crate) const SOCK_STREAM: c_int = 1;
/// `INVALID_SOCKET`: the result of a socket call that failed.
pub(crate) const INVALID_SOCKET: Socket = !0;
/// `WSA_FLAG_OVERLAPPED`: the socket takes overlapped calls.
pub(crate) const WSA_FLAG_OVERLAPPED: u32 = 0x01;
/// `WSA_FLAG_NO_HANDLE_INHERIT`: no child process inherits the socket.
pub(crate) const WSA_FLAG_NO_HANDLE_INHERIT: u32 = 0x80;
/// `FIONBIO`: `_IOW('f', 126, u_long)`, the nonblocking mode of a socket.
pub(crate) const FIONBIO: c_int = 0x8004_667E_u32.cast_signed();
/// `SD_SEND`: the half-close of the send side.
pub(crate) const SD_SEND: c_int = 1;

/// `SO_KEEPALIVE`.
pub(crate) const SO_KEEPALIVE: c_int = 0x0008;
/// `SO_SNDBUF`.
pub(crate) const SO_SNDBUF: c_int = 0x1001;
/// `SO_RCVBUF`.
pub(crate) const SO_RCVBUF: c_int = 0x1002;
/// `SO_UPDATE_ACCEPT_CONTEXT`: an accepted socket takes the state of its listener.
pub(crate) const SO_UPDATE_ACCEPT_CONTEXT: c_int = 0x700B;
/// `SO_UPDATE_CONNECT_CONTEXT`: a socket that `ConnectEx` connected takes its connected state.
pub(crate) const SO_UPDATE_CONNECT_CONTEXT: c_int = 0x7010;
/// `IPPROTO_TCP`, the level of the TCP options.
pub(crate) const IPPROTO_TCP: c_int = 6;
/// `TCP_NODELAY`.
pub(crate) const TCP_NODELAY: c_int = 0x0001;

/// `XP1_IFS_HANDLES`: the provider gives handles of the installable file system, which the I/O
/// manager completes.
pub(crate) const XP1_IFS_HANDLES: u32 = 0x0002_0000;
/// `SIO_BASE_HANDLE`: `_WSAIOR(IOC_WS2, 34)`, the socket of the base provider under each layered
/// provider.
pub(crate) const SIO_BASE_HANDLE: u32 = 0x4800_0022;

/// `WSAID_ACCEPTEX`: the identifier of `AcceptEx`.
pub(crate) const WSAID_ACCEPTEX: Guid = Guid {
    data1: 0xb536_7df1,
    data2: 0xcbac,
    data3: 0x11cf,
    data4: [0x95, 0xca, 0x00, 0x80, 0x5f, 0x48, 0xa1, 0x92],
};
/// `WSAID_CONNECTEX`: the identifier of `ConnectEx`.
pub(crate) const WSAID_CONNECTEX: Guid = Guid {
    data1: 0x25a2_07b9,
    data2: 0xddf3,
    data3: 0x4660,
    data4: [0x8e, 0xe9, 0x76, 0xe5, 0x8c, 0x74, 0x06, 0x3e],
};

/// `LPFN_ACCEPTEX`: `BOOL AcceptEx(SOCKET sListenSocket, SOCKET sAcceptSocket, PVOID
/// lpOutputBuffer, DWORD dwReceiveDataLength, DWORD dwLocalAddressLength, DWORD
/// dwRemoteAddressLength, LPDWORD lpdwBytesReceived, LPOVERLAPPED lpOverlapped)`.
pub(crate) type AcceptEx = unsafe extern "system" fn(
    listener: Socket,
    accepted: Socket,
    output: *mut c_void,
    receive_len: u32,
    local_len: u32,
    remote_len: u32,
    received: *mut u32,
    overlapped: *mut Overlapped,
) -> i32;

/// `LPFN_CONNECTEX`: `BOOL ConnectEx(SOCKET s, const struct sockaddr *name, int namelen, PVOID
/// lpSendBuffer, DWORD dwSendDataLength, LPDWORD lpdwBytesSent, LPOVERLAPPED lpOverlapped)`.
pub(crate) type ConnectEx = unsafe extern "system" fn(
    s: Socket,
    name: *const c_void,
    name_len: c_int,
    send: *mut c_void,
    send_len: u32,
    sent: *mut u32,
    overlapped: *mut Overlapped,
) -> i32;

#[expect(unsafe_code, reason = "kernel32.dll gives these functions")]
#[link(name = "kernel32")]
unsafe extern "system" {
    /// `HANDLE CreateIoCompletionPort(HANDLE FileHandle, HANDLE ExistingCompletionPort, ULONG_PTR
    /// CompletionKey, DWORD NumberOfConcurrentThreads)`.
    pub(crate) fn CreateIoCompletionPort(file: Handle, port: Handle, key: usize, threads: u32) -> Handle;
    /// `BOOL GetQueuedCompletionStatusEx(HANDLE CompletionPort, LPOVERLAPPED_ENTRY
    /// lpCompletionPortEntries, ULONG ulCount, PULONG ulNumEntriesRemoved, DWORD dwMilliseconds,
    /// BOOL fAlertable)`.
    pub(crate) fn GetQueuedCompletionStatusEx(
        port: Handle,
        entries: *mut OverlappedEntry,
        count: u32,
        removed: *mut u32,
        milliseconds: u32,
        alertable: i32,
    ) -> i32;
    /// `BOOL PostQueuedCompletionStatus(HANDLE CompletionPort, DWORD dwNumberOfBytesTransferred,
    /// ULONG_PTR dwCompletionKey, LPOVERLAPPED lpOverlapped)`.
    pub(crate) fn PostQueuedCompletionStatus(port: Handle, bytes: u32, key: usize, overlapped: *mut Overlapped) -> i32;
    /// `BOOL CancelIoEx(HANDLE hFile, LPOVERLAPPED lpOverlapped)`.
    pub(crate) fn CancelIoEx(file: Handle, overlapped: *mut Overlapped) -> i32;
    /// `BOOL SetFileCompletionNotificationModes(HANDLE FileHandle, UCHAR Flags)`.
    pub(crate) fn SetFileCompletionNotificationModes(file: Handle, flags: u8) -> i32;
}

#[expect(unsafe_code, reason = "ws2_32.dll gives these functions")]
#[link(name = "ws2_32")]
unsafe extern "system" {
    /// `int WSAStartup(WORD wVersionRequired, LPWSADATA lpWSAData)`.
    pub(crate) fn WSAStartup(version: u16, data: *mut WsaData) -> c_int;
    /// `SOCKET WSASocketW(int af, int type, int protocol, LPWSAPROTOCOL_INFOW lpProtocolInfo, GROUP
    /// g, DWORD dwFlags)`.
    pub(crate) fn WSASocketW(
        af: c_int,
        kind: c_int,
        protocol: c_int,
        info: *mut WsaProtocolInfoW,
        group: u32,
        flags: u32,
    ) -> Socket;
    /// `int WSARecv(SOCKET s, LPWSABUF lpBuffers, DWORD dwBufferCount, LPDWORD
    /// lpNumberOfBytesRecvd, LPDWORD lpFlags, LPWSAOVERLAPPED lpOverlapped,
    /// LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine)`.
    pub(crate) fn WSARecv(
        s: Socket,
        buffers: *mut WsaBuf,
        count: u32,
        received: *mut u32,
        flags: *mut u32,
        overlapped: *mut Overlapped,
        completion: *mut c_void,
    ) -> c_int;
    /// `int WSASend(SOCKET s, LPWSABUF lpBuffers, DWORD dwBufferCount, LPDWORD
    /// lpNumberOfBytesSent, DWORD dwFlags, LPWSAOVERLAPPED lpOverlapped,
    /// LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine)`.
    pub(crate) fn WSASend(
        s: Socket,
        buffers: *const WsaBuf,
        count: u32,
        sent: *mut u32,
        flags: u32,
        overlapped: *mut Overlapped,
        completion: *mut c_void,
    ) -> c_int;
    /// `BOOL WSAGetOverlappedResult(SOCKET s, LPWSAOVERLAPPED lpOverlapped, LPDWORD lpcbTransfer,
    /// BOOL fWait, LPDWORD lpdwFlags)`.
    pub(crate) fn WSAGetOverlappedResult(
        s: Socket,
        overlapped: *mut Overlapped,
        transferred: *mut u32,
        wait: i32,
        flags: *mut u32,
    ) -> i32;
    /// `int listen(SOCKET s, int backlog)`.
    pub(crate) fn listen(s: Socket, backlog: c_int) -> c_int;
    /// `int shutdown(SOCKET s, int how)`.
    pub(crate) fn shutdown(s: Socket, how: c_int) -> c_int;
    /// `int ioctlsocket(SOCKET s, long cmd, u_long *argp)`, with a 32-bit `long`.
    pub(crate) fn ioctlsocket(s: Socket, cmd: c_int, argp: *mut u32) -> c_int;
}

/// `GENERIC_READ`.
pub(crate) const GENERIC_READ: u32 = 0x8000_0000;
/// `GENERIC_WRITE`.
pub(crate) const GENERIC_WRITE: u32 = 0x4000_0000;
/// `FILE_SHARE_READ`, `FILE_SHARE_WRITE`, and `FILE_SHARE_DELETE`: other handles can read, write,
/// rename, and delete the file, as on Linux and macOS.
pub(crate) const FILE_SHARE_ALL: u32 = 0x1 | 0x2 | 0x4;
/// `CREATE_NEW`: make the file, and fail when it exists.
pub(crate) const CREATE_NEW: u32 = 1;
/// `CREATE_ALWAYS`: make the file, or set the length of an existing file to 0.
pub(crate) const CREATE_ALWAYS: u32 = 2;
/// `OPEN_EXISTING`: open the file, and fail when it does not exist.
pub(crate) const OPEN_EXISTING: u32 = 3;
/// `OPEN_ALWAYS`: open the file, or make it when it does not exist.
pub(crate) const OPEN_ALWAYS: u32 = 4;
/// `TRUNCATE_EXISTING`: open the file and set its length to 0, and fail when it does not exist.
pub(crate) const TRUNCATE_EXISTING: u32 = 5;
/// `FILE_FLAG_WRITE_THROUGH`: a write ends when its bytes are on the media.
pub(crate) const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
/// `FILE_FLAG_OVERLAPPED`: the handle takes overlapped calls.
pub(crate) const FILE_FLAG_OVERLAPPED: u32 = 0x4000_0000;
/// `FILE_FLAG_NO_BUFFERING`: reads and writes go between the device and the buffer, with no copy
/// in the cache.
pub(crate) const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
/// `FILE_FLAG_BACKUP_SEMANTICS`: the open of a directory.
pub(crate) const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
/// `FILE_ATTRIBUTE_NORMAL`.
pub(crate) const FILE_ATTRIBUTE_NORMAL: u32 = 0x80;
/// `MOVEFILE_REPLACE_EXISTING`: a rename replaces a file at its target.
pub(crate) const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
/// `VOLUME_NAME_GUID`: `GetFinalPathNameByHandleW` gives the path with the volume GUID name.
pub(crate) const VOLUME_NAME_GUID: u32 = 0x1;

/// `FileStandardInfo` of `FILE_INFO_BY_HANDLE_CLASS`.
pub(crate) const FILE_STANDARD_INFO: c_int = 1;
/// `FileAllocationInfo`.
pub(crate) const FILE_ALLOCATION_INFO: c_int = 5;
/// `FileEndOfFileInfo`.
pub(crate) const FILE_END_OF_FILE_INFO: c_int = 6;
/// `FileStorageInfo`.
pub(crate) const FILE_STORAGE_INFO: c_int = 16;

/// `FILE_STANDARD_INFO`: the allocated bytes and the length of a file.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct FileStandardInfo {
    /// `AllocationSize` (`LARGE_INTEGER`).
    pub(crate) allocation: i64,
    /// `EndOfFile` (`LARGE_INTEGER`).
    pub(crate) end: i64,
    /// `NumberOfLinks`.
    pub(crate) links: u32,
    /// `DeletePending` (`BOOLEAN`).
    pub(crate) delete_pending: u8,
    /// `Directory` (`BOOLEAN`).
    pub(crate) directory: u8,
}

/// `FILE_STORAGE_INFO`: the sector sizes of the device of a file.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct FileStorageInfo {
    /// `LogicalBytesPerSector`.
    pub(crate) logical: u32,
    /// `PhysicalBytesPerSectorForAtomicity`.
    pub(crate) physical_atomicity: u32,
    /// `PhysicalBytesPerSectorForPerformance`.
    pub(crate) physical_performance: u32,
    /// `FileSystemEffectivePhysicalBytesPerSectorForAtomicity`.
    pub(crate) effective_atomicity: u32,
    /// `Flags`.
    pub(crate) flags: u32,
    /// `ByteOffsetForSectorAlignment`.
    pub(crate) sector_offset: u32,
    /// `ByteOffsetForPartitionAlignment`.
    pub(crate) partition_offset: u32,
}

/// `FSCTL_SET_SPARSE`: `CTL_CODE(FILE_DEVICE_FILE_SYSTEM, 49, METHOD_BUFFERED,
/// FILE_SPECIAL_ACCESS)`. With no input, the file becomes sparse.
pub(crate) const FSCTL_SET_SPARSE: u32 = 0x0009_00C4;
/// `FSCTL_SET_ZERO_DATA`: `CTL_CODE(FILE_DEVICE_FILE_SYSTEM, 50, METHOD_BUFFERED,
/// FILE_WRITE_DATA)`. A range reads as zeros, and a sparse file frees its clusters.
pub(crate) const FSCTL_SET_ZERO_DATA: u32 = 0x0009_80C8;
/// `IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS`: `CTL_CODE(IOCTL_VOLUME_BASE, 0, METHOD_BUFFERED,
/// FILE_ANY_ACCESS)`.
pub(crate) const IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS: u32 = 0x0056_0000;
/// `IOCTL_STORAGE_GET_DEVICE_NUMBER`: `CTL_CODE(IOCTL_STORAGE_BASE, 0x0420, METHOD_BUFFERED,
/// FILE_ANY_ACCESS)`.
pub(crate) const IOCTL_STORAGE_GET_DEVICE_NUMBER: u32 = 0x002D_1080;

/// `FILE_ZERO_DATA_INFORMATION`: the range of `FSCTL_SET_ZERO_DATA`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct FileZeroDataInformation {
    /// `FileOffset` (`LARGE_INTEGER`).
    pub(crate) offset: i64,
    /// `BeyondFinalZero` (`LARGE_INTEGER`): the end of the range.
    pub(crate) end: i64,
}

/// `DISK_EXTENT`: a range of a volume on one disk.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DiskExtent {
    /// `DiskNumber`.
    pub(crate) disk: u32,
    /// `StartingOffset` (`LARGE_INTEGER`).
    pub(crate) start: i64,
    /// `ExtentLength` (`LARGE_INTEGER`).
    pub(crate) len: i64,
}

/// `VOLUME_DISK_EXTENTS` with room for `EXTENTS` extents in place of `ANYSIZE_ARRAY`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct VolumeDiskExtents<const EXTENTS: usize> {
    /// `NumberOfDiskExtents`.
    pub(crate) count: u32,
    /// `Extents`.
    pub(crate) extents: [DiskExtent; EXTENTS],
}

/// `STORAGE_DEVICE_NUMBER`: the number of a disk.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StorageDeviceNumber {
    /// `DeviceType` (`DEVICE_TYPE`).
    pub(crate) kind: u32,
    /// `DeviceNumber`.
    pub(crate) number: u32,
    /// `PartitionNumber`.
    pub(crate) partition: u32,
}

/// `DEVPROPKEY`: the key of a device property.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct DevPropKey {
    /// `fmtid`.
    pub(crate) fmtid: Guid,
    /// `pid`.
    pub(crate) pid: u32,
}

/// `GUID_DEVINTERFACE_DISK`: the interface class of disks.
pub(crate) const GUID_DEVINTERFACE_DISK: Guid = Guid {
    data1: 0x53f5_6307,
    data2: 0xb6bf,
    data3: 0x11d0,
    data4: [0x94, 0xf2, 0x00, 0xa0, 0xc9, 0x1e, 0xfb, 0x8b],
};
/// `DEVPKEY_Device_InstanceId`: the instance identifier of the device of an interface.
pub(crate) const DEVPKEY_DEVICE_INSTANCE_ID: DevPropKey = DevPropKey {
    fmtid: Guid {
        data1: 0x78c3_4fc8,
        data2: 0x104a,
        data3: 0x4aca,
        data4: [0x9e, 0xa4, 0x52, 0x4d, 0x52, 0x99, 0x6e, 0x57],
    },
    pid: 256,
};
/// `DEVPKEY_Device_Service`: the name of the driver service of a device.
pub(crate) const DEVPKEY_DEVICE_SERVICE: DevPropKey = DevPropKey {
    fmtid: Guid {
        data1: 0xa45c_254e,
        data2: 0xdf1c,
        data3: 0x4efd,
        data4: [0x80, 0x20, 0x67, 0xd1, 0x46, 0xa8, 0x50, 0xe0],
    },
    pid: 6,
};
/// `CR_SUCCESS`.
pub(crate) const CR_SUCCESS: u32 = 0;
/// `CM_GET_DEVICE_INTERFACE_LIST_PRESENT`: only the interfaces of present devices.
pub(crate) const CM_GET_DEVICE_INTERFACE_LIST_PRESENT: u32 = 0;
/// `CM_LOCATE_DEVNODE_NORMAL`.
pub(crate) const CM_LOCATE_DEVNODE_NORMAL: u32 = 0;
/// `DEVPROP_TYPE_STRING`: a NUL-terminated UTF-16 string.
pub(crate) const DEVPROP_TYPE_STRING: u32 = 0x12;

/// `ERROR_HANDLE_EOF`: a read at or after the end of the file.
pub(crate) const ERROR_HANDLE_EOF: c_int = 38;
/// `ERROR_INVALID_FUNCTION`: the file system has no such operation, for example no sparse files.
pub(crate) const ERROR_INVALID_FUNCTION: c_int = 1;
/// `ERROR_INVALID_PARAMETER`.
pub(crate) const ERROR_INVALID_PARAMETER: c_int = 87;
/// `ERROR_NOT_SUPPORTED`.
pub(crate) const ERROR_NOT_SUPPORTED: c_int = 50;
/// `ERROR_CALL_NOT_IMPLEMENTED`: the error that `std::io` reads as `Unsupported`, which a file
/// operation gives for a file system without it.
pub(crate) const ERROR_CALL_NOT_IMPLEMENTED: c_int = 120;

const _: () = {
    assert!(size_of::<FileStandardInfo>() == 24);
    assert!(offset_of!(FileStandardInfo, end) == 8);
    assert!(offset_of!(FileStandardInfo, links) == 16);
    assert!(offset_of!(FileStandardInfo, delete_pending) == 20);
    assert!(offset_of!(FileStandardInfo, directory) == 21);
    assert!(size_of::<FileStorageInfo>() == 28);
    assert!(offset_of!(FileStorageInfo, physical_performance) == 8);
    assert!(size_of::<FileZeroDataInformation>() == 16);
    assert!(size_of::<DiskExtent>() == 24);
    assert!(offset_of!(DiskExtent, start) == 8);
    assert!(size_of::<VolumeDiskExtents<1>>() == 32);
    assert!(offset_of!(VolumeDiskExtents<1>, extents) == 8);
    assert!(size_of::<StorageDeviceNumber>() == 12);
    assert!(size_of::<DevPropKey>() == 20);
};

#[expect(unsafe_code, reason = "kernel32.dll gives these functions")]
#[link(name = "kernel32")]
unsafe extern "system" {
    /// `HANDLE CreateFileW(LPCWSTR lpFileName, DWORD dwDesiredAccess, DWORD dwShareMode,
    /// LPSECURITY_ATTRIBUTES lpSecurityAttributes, DWORD dwCreationDisposition, DWORD
    /// dwFlagsAndAttributes, HANDLE hTemplateFile)`.
    pub(crate) fn CreateFileW(
        name: *const u16,
        access: u32,
        share: u32,
        security: *mut c_void,
        disposition: u32,
        flags: u32,
        template: Handle,
    ) -> Handle;
    /// `HANDLE ReOpenFile(HANDLE hOriginalFile, DWORD dwDesiredAccess, DWORD dwShareMode, DWORD
    /// dwFlagsAndAttributes)`.
    pub(crate) fn ReOpenFile(file: Handle, access: u32, share: u32, flags: u32) -> Handle;
    /// `BOOL ReadFile(HANDLE hFile, LPVOID lpBuffer, DWORD nNumberOfBytesToRead, LPDWORD
    /// lpNumberOfBytesRead, LPOVERLAPPED lpOverlapped)`.
    pub(crate) fn ReadFile(
        file: Handle,
        buffer: *mut c_void,
        len: u32,
        read: *mut u32,
        overlapped: *mut Overlapped,
    ) -> i32;
    /// `BOOL WriteFile(HANDLE hFile, LPCVOID lpBuffer, DWORD nNumberOfBytesToWrite, LPDWORD
    /// lpNumberOfBytesWritten, LPOVERLAPPED lpOverlapped)`.
    pub(crate) fn WriteFile(
        file: Handle,
        buffer: *const c_void,
        len: u32,
        written: *mut u32,
        overlapped: *mut Overlapped,
    ) -> i32;
    /// `BOOL DeviceIoControl(HANDLE hDevice, DWORD dwIoControlCode, LPVOID lpInBuffer, DWORD
    /// nInBufferSize, LPVOID lpOutBuffer, DWORD nOutBufferSize, LPDWORD lpBytesReturned,
    /// LPOVERLAPPED lpOverlapped)`.
    pub(crate) fn DeviceIoControl(
        device: Handle,
        code: u32,
        input: *const c_void,
        input_len: u32,
        output: *mut c_void,
        output_len: u32,
        returned: *mut u32,
        overlapped: *mut Overlapped,
    ) -> i32;
    /// `BOOL GetOverlappedResult(HANDLE hFile, LPOVERLAPPED lpOverlapped, LPDWORD
    /// lpNumberOfBytesTransferred, BOOL bWait)`.
    pub(crate) fn GetOverlappedResult(
        file: Handle,
        overlapped: *mut Overlapped,
        transferred: *mut u32,
        wait: i32,
    ) -> i32;
    /// `BOOL FlushFileBuffers(HANDLE hFile)`.
    pub(crate) fn FlushFileBuffers(file: Handle) -> i32;
    /// `HANDLE CreateEventW(LPSECURITY_ATTRIBUTES lpEventAttributes, BOOL bManualReset, BOOL
    /// bInitialState, LPCWSTR lpName)`.
    pub(crate) fn CreateEventW(security: *mut c_void, manual: i32, initial: i32, name: *const u16) -> Handle;
    /// `HMODULE GetModuleHandleW(LPCWSTR lpModuleName)`.
    pub(crate) fn GetModuleHandleW(name: *const u16) -> Handle;
    /// `FARPROC GetProcAddress(HMODULE hModule, LPCSTR lpProcName)`: the address of an export, or
    /// null.
    pub(crate) fn GetProcAddress(module: Handle, name: *const c_char) -> *const c_void;
    /// `BOOL SetFileInformationByHandle(HANDLE hFile, FILE_INFO_BY_HANDLE_CLASS
    /// FileInformationClass, LPVOID lpFileInformation, DWORD dwBufferSize)`.
    pub(crate) fn SetFileInformationByHandle(file: Handle, class: c_int, information: *const c_void, len: u32) -> i32;
    /// `BOOL GetFileInformationByHandleEx(HANDLE hFile, FILE_INFO_BY_HANDLE_CLASS
    /// FileInformationClass, LPVOID lpFileInformation, DWORD dwBufferSize)`.
    pub(crate) fn GetFileInformationByHandleEx(file: Handle, class: c_int, information: *mut c_void, len: u32) -> i32;
    /// `BOOL MoveFileExW(LPCWSTR lpExistingFileName, LPCWSTR lpNewFileName, DWORD dwFlags)`.
    pub(crate) fn MoveFileExW(from: *const u16, to: *const u16, flags: u32) -> i32;
    /// `BOOL DeleteFileW(LPCWSTR lpFileName)`.
    pub(crate) fn DeleteFileW(name: *const u16) -> i32;
    /// `DWORD GetFinalPathNameByHandleW(HANDLE hFile, LPWSTR lpszFilePath, DWORD cchFilePath, DWORD
    /// dwFlags)`.
    pub(crate) fn GetFinalPathNameByHandleW(file: Handle, path: *mut u16, len: u32, flags: u32) -> u32;
    /// `DWORD GetFullPathNameW(LPCWSTR lpFileName, DWORD nBufferLength, LPWSTR lpBuffer, LPWSTR
    /// *lpFilePart)`.
    pub(crate) fn GetFullPathNameW(name: *const u16, len: u32, buffer: *mut u16, part: *mut *mut u16) -> u32;
}

#[expect(unsafe_code, reason = "cfgmgr32.dll gives these functions")]
#[link(name = "cfgmgr32")]
unsafe extern "system" {
    /// `CONFIGRET CM_Get_Device_Interface_List_SizeW(PULONG pulLen, LPGUID InterfaceClassGuid,
    /// DEVINSTID_W pDeviceID, ULONG ulFlags)`.
    pub(crate) fn CM_Get_Device_Interface_List_SizeW(
        len: *mut u32,
        class: *const Guid,
        device: *const u16,
        flags: u32,
    ) -> u32;
    /// `CONFIGRET CM_Get_Device_Interface_ListW(LPGUID InterfaceClassGuid, DEVINSTID_W pDeviceID,
    /// PZZWSTR Buffer, ULONG BufferLen, ULONG ulFlags)`.
    pub(crate) fn CM_Get_Device_Interface_ListW(
        class: *const Guid,
        device: *const u16,
        buffer: *mut u16,
        len: u32,
        flags: u32,
    ) -> u32;
    /// `CONFIGRET CM_Get_Device_Interface_PropertyW(LPCWSTR pszDeviceInterface, const DEVPROPKEY
    /// *PropertyKey, DEVPROPTYPE *PropertyType, PBYTE PropertyBuffer, PULONG PropertyBufferSize,
    /// ULONG ulFlags)`.
    pub(crate) fn CM_Get_Device_Interface_PropertyW(
        interface: *const u16,
        key: *const DevPropKey,
        kind: *mut u32,
        buffer: *mut u8,
        len: *mut u32,
        flags: u32,
    ) -> u32;
    /// `CONFIGRET CM_Locate_DevNodeW(PDEVINST pdnDevInst, DEVINSTID_W pDeviceID, ULONG ulFlags)`.
    pub(crate) fn CM_Locate_DevNodeW(node: *mut u32, device: *const u16, flags: u32) -> u32;
    /// `CONFIGRET CM_Get_DevNode_PropertyW(DEVINST dnDevInst, const DEVPROPKEY *PropertyKey,
    /// DEVPROPTYPE *PropertyType, PBYTE PropertyBuffer, PULONG PropertyBufferSize, ULONG ulFlags)`.
    pub(crate) fn CM_Get_DevNode_PropertyW(
        node: u32,
        key: *const DevPropKey,
        kind: *mut u32,
        buffer: *mut u8,
        len: *mut u32,
        flags: u32,
    ) -> u32;
}

/// `PIPE_ACCESS_DUPLEX`: the server reads and writes the pipe.
pub(crate) const PIPE_ACCESS_DUPLEX: u32 = 0x3;
/// `FILE_FLAG_FIRST_PIPE_INSTANCE`: the call fails when an instance of the name exists.
pub(crate) const FILE_FLAG_FIRST_PIPE_INSTANCE: u32 = 0x0008_0000;
/// `PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT`: a byte stream, in the mode that waits.
pub(crate) const PIPE_BYTES: u32 = 0;
/// `PIPE_REJECT_REMOTE_CLIENTS`: a client on another computer cannot connect.
pub(crate) const PIPE_REJECT_REMOTE_CLIENTS: u32 = 0x8;
/// `PIPE_UNLIMITED_INSTANCES`.
pub(crate) const PIPE_UNLIMITED_INSTANCES: u32 = 255;
/// `SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION`: the server of a pipe that the client opens
/// can read the identity of the client, and cannot act as the client.
pub(crate) const SECURITY_IDENTIFICATION: u32 = 0x0010_0000 | 0x0001_0000;
/// `TOKEN_QUERY`.
pub(crate) const TOKEN_QUERY: u32 = 0x8;
/// `TokenUser` of `TOKEN_INFORMATION_CLASS`.
pub(crate) const TOKEN_USER: c_int = 1;
/// `SDDL_REVISION_1`.
pub(crate) const SDDL_REVISION_1: u32 = 1;
/// `SE_FILE_OBJECT` of `SE_OBJECT_TYPE`: the object of a file handle, also of a pipe.
pub(crate) const SE_FILE_OBJECT: c_int = 1;
/// `OWNER_SECURITY_INFORMATION`.
pub(crate) const OWNER_SECURITY_INFORMATION: u32 = 0x1;
/// `SECURITY_MAX_SID_SIZE`: the most bytes of a SID.
pub(crate) const SECURITY_MAX_SID_SIZE: usize = 68;

/// `ERROR_ACCESS_DENIED`: for a pipe, an instance of the name exists, or the security descriptor of
/// the pipe denies the caller.
pub(crate) const ERROR_ACCESS_DENIED: c_int = 5;
/// `ERROR_BROKEN_PIPE`: the other end of the pipe closed. A read gives the end of the stream.
pub(crate) const ERROR_BROKEN_PIPE: c_int = 109;
/// `ERROR_PIPE_BUSY`: each instance of the pipe has a client.
pub(crate) const ERROR_PIPE_BUSY: c_int = 231;
/// `ERROR_NO_DATA`: the client of an instance closed before the server took it.
pub(crate) const ERROR_NO_DATA: c_int = 232;
/// `ERROR_PIPE_CONNECTED`: a client connected before the server waited for it.
pub(crate) const ERROR_PIPE_CONNECTED: c_int = 535;
/// `ERROR_BUSY`: the error that `std::io` reads as `ResourceBusy`.
pub(crate) const ERROR_BUSY: c_int = 170;
/// `WSAEADDRINUSE`: the error that `std::io` reads as `AddrInUse`.
pub(crate) const WSAEADDRINUSE: c_int = 10048;

/// `SECURITY_ATTRIBUTES`: the security descriptor of a new object.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct SecurityAttributes {
    /// `nLength`.
    pub(crate) len: u32,
    /// `lpSecurityDescriptor`.
    pub(crate) descriptor: *mut c_void,
    /// `bInheritHandle`.
    pub(crate) inherit: i32,
}

/// `TOKEN_USER`: the `SID_AND_ATTRIBUTES` of the user of a token. The SID follows it in the buffer
/// of `GetTokenInformation`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct TokenUser {
    /// `User.Sid`.
    pub(crate) sid: *mut c_void,
    /// `User.Attributes`.
    pub(crate) attributes: u32,
}

const _: () = {
    assert!(size_of::<SecurityAttributes>() == 24);
    assert!(offset_of!(SecurityAttributes, descriptor) == 8);
    assert!(offset_of!(SecurityAttributes, inherit) == 16);
    assert!(size_of::<TokenUser>() == 16);
    assert!(offset_of!(TokenUser, attributes) == 8);
};

#[expect(unsafe_code, reason = "kernel32.dll gives these functions")]
#[link(name = "kernel32")]
unsafe extern "system" {
    /// `HANDLE CreateNamedPipeW(LPCWSTR lpName, DWORD dwOpenMode, DWORD dwPipeMode, DWORD
    /// nMaxInstances, DWORD nOutBufferSize, DWORD nInBufferSize, DWORD nDefaultTimeOut,
    /// LPSECURITY_ATTRIBUTES lpSecurityAttributes)`.
    pub(crate) fn CreateNamedPipeW(
        name: *const u16,
        open: u32,
        mode: u32,
        instances: u32,
        out_len: u32,
        in_len: u32,
        timeout: u32,
        security: *mut SecurityAttributes,
    ) -> Handle;
    /// `BOOL ConnectNamedPipe(HANDLE hNamedPipe, LPOVERLAPPED lpOverlapped)`.
    pub(crate) fn ConnectNamedPipe(pipe: Handle, overlapped: *mut Overlapped) -> i32;
    /// `BOOL GetNamedPipeClientProcessId(HANDLE Pipe, PULONG ClientProcessId)`.
    pub(crate) fn GetNamedPipeClientProcessId(pipe: Handle, process: *mut u32) -> i32;
    /// `BOOL GetNamedPipeServerProcessId(HANDLE Pipe, PULONG ServerProcessId)`.
    pub(crate) fn GetNamedPipeServerProcessId(pipe: Handle, process: *mut u32) -> i32;
    /// `HANDLE GetCurrentProcess(void)`: a pseudo handle that needs no close.
    pub(crate) fn GetCurrentProcess() -> Handle;
    /// `HANDLE GetCurrentThread(void)`: a pseudo handle that needs no close.
    pub(crate) fn GetCurrentThread() -> Handle;
    /// `HLOCAL LocalFree(HLOCAL hMem)`.
    pub(crate) fn LocalFree(memory: *mut c_void) -> *mut c_void;
}

#[expect(unsafe_code, reason = "advapi32.dll gives these functions")]
#[link(name = "advapi32")]
unsafe extern "system" {
    /// `BOOL OpenProcessToken(HANDLE ProcessHandle, DWORD DesiredAccess, PHANDLE TokenHandle)`.
    pub(crate) fn OpenProcessToken(process: Handle, access: u32, token: *mut Handle) -> i32;
    /// `BOOL OpenThreadToken(HANDLE ThreadHandle, DWORD DesiredAccess, BOOL OpenAsSelf, PHANDLE
    /// TokenHandle)`.
    pub(crate) fn OpenThreadToken(thread: Handle, access: u32, as_self: i32, token: *mut Handle) -> i32;
    /// `BOOL GetTokenInformation(HANDLE TokenHandle, TOKEN_INFORMATION_CLASS TokenInformationClass,
    /// LPVOID TokenInformation, DWORD TokenInformationLength, PDWORD ReturnLength)`.
    pub(crate) fn GetTokenInformation(
        token: Handle,
        class: c_int,
        information: *mut c_void,
        len: u32,
        returned: *mut u32,
    ) -> i32;
    /// `BOOL ImpersonateNamedPipeClient(HANDLE hNamedPipe)`.
    pub(crate) fn ImpersonateNamedPipeClient(pipe: Handle) -> i32;
    /// `BOOL RevertToSelf(void)`.
    pub(crate) fn RevertToSelf() -> i32;
    /// `DWORD GetLengthSid(PSID pSid)`.
    pub(crate) fn GetLengthSid(sid: *mut c_void) -> u32;
    /// `DWORD GetSecurityInfo(HANDLE handle, SE_OBJECT_TYPE ObjectType, SECURITY_INFORMATION
    /// SecurityInfo, PSID *ppsidOwner, PSID *ppsidGroup, PACL *ppDacl, PACL *ppSacl,
    /// PSECURITY_DESCRIPTOR *ppSecurityDescriptor)`. It gives a Win32 error, not a `BOOL`.
    pub(crate) fn GetSecurityInfo(
        handle: Handle,
        kind: c_int,
        information: u32,
        owner: *mut *mut c_void,
        group: *mut *mut c_void,
        dacl: *mut *mut c_void,
        sacl: *mut *mut c_void,
        descriptor: *mut *mut c_void,
    ) -> u32;
    /// `BOOL ConvertStringSecurityDescriptorToSecurityDescriptorW(LPCWSTR
    /// StringSecurityDescriptor, DWORD StringSDRevision, PSECURITY_DESCRIPTOR *SecurityDescriptor,
    /// PULONG SecurityDescriptorSize)`.
    pub(crate) fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
        string: *const u16,
        revision: u32,
        descriptor: *mut *mut c_void,
        len: *mut u32,
    ) -> i32;
}
