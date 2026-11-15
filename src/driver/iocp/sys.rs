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
//! options), `ntstatus.h` (the statuses), and `winerror.h` (the error codes). `kernel32.dll` and
//! `ws2_32.dll` give the functions; `AcceptEx` and `ConnectEx` come only as pointers from
//! `WSAIoctl`. The datagram declarations and the shared socket calls are in `rsnetwork_udp::sys`.

use core::ffi::{c_char, c_int, c_void};
use core::mem::{offset_of, size_of};

pub(crate) use rsnetwork_udp::sys::{
    Guid, IPPROTO_IPV6, IPV6_V6ONLY, SIO_GET_EXTENSION_FUNCTION_POINTER, SOCK_DGRAM, SOCKET_ERROR, Socket, WSAIoctl,
    WsaBuf, bind, getpeername, getsockname, getsockopt, setsockopt,
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

/// `WSAPROTOCOL_INFOW`, 628 bytes: the provider of a socket. The driver reads only
/// `dwServiceFlags1`.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct WsaProtocolInfoW {
    /// `dwServiceFlags1`, for example [`XP1_IFS_HANDLES`].
    pub(crate) service_flags1: u32,
    /// The other fields, from `dwServiceFlags2` to `szProtocol`.
    pub(crate) rest: [u32; 156],
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
    assert!(size_of::<WsaProtocolInfoW>() == 628);
    assert!(size_of::<Socket>() == size_of::<usize>());
};

/// `STATUS_PENDING`: the call runs, and its packet comes later.
pub(crate) const STATUS_PENDING: u32 = 0x0000_0103;

/// `NT_ERROR`: the severity of `status` is an error. No packet comes for a call that returns an
/// error status.
pub(crate) const fn nt_error(status: u32) -> bool {
    status >> 30 == 3
}

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

/// `SOL_SOCKET`, the level of the socket options.
pub(crate) const SOL_SOCKET: c_int = 0xFFFF;
/// `SO_KEEPALIVE`.
pub(crate) const SO_KEEPALIVE: c_int = 0x0008;
/// `SO_SNDBUF`.
pub(crate) const SO_SNDBUF: c_int = 0x1001;
/// `SO_RCVBUF`.
pub(crate) const SO_RCVBUF: c_int = 0x1002;
/// `SO_PROTOCOL_INFOW`: the `WSAPROTOCOL_INFOW` of the provider of a socket.
pub(crate) const SO_PROTOCOL_INFOW: c_int = 0x2005;
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
