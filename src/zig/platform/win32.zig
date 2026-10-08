//! The kernel32 and advapi32 functions and types the library uses on Windows, declared here because
//! std's Windows layer is built on ntdll and doesn't cover them.

const std = @import("std");

pub const BOOL = c_int;
pub const DWORD = u32;
pub const HANDLE = *anyopaque;
pub const WCHAR = u16;
/// A security identifier (variable length; handled by pointer only).
pub const SID = opaque {};
/// An access-control list (variable length; handled by pointer only).
pub const ACL = opaque {};

pub const FALSE: BOOL = 0;
pub const TRUE: BOOL = 1;
pub const INVALID_HANDLE_VALUE: HANDLE = @ptrFromInt(std.math.maxInt(usize));
pub const INFINITE: DWORD = 0xFFFF_FFFF;
pub const WAIT_OBJECT_0: DWORD = 0;
pub const WAIT_TIMEOUT: DWORD = 258;
pub const STD_ERROR_HANDLE: DWORD = @bitCast(@as(i32, -12));
pub const PAGE_READWRITE: DWORD = 0x04;
pub const FILE_MAP_READ: DWORD = 0x0004;

// Error codes
pub const ERROR_FILE_NOT_FOUND: DWORD = 2;
pub const ERROR_ACCESS_DENIED: DWORD = 5;
pub const ERROR_NOT_ENOUGH_MEMORY: DWORD = 8;
pub const ERROR_OUTOFMEMORY: DWORD = 14;
pub const ERROR_ALREADY_EXISTS: DWORD = 183;
pub const ERROR_COMMITMENT_LIMIT: DWORD = 1455;
pub const ERROR_BROKEN_PIPE: DWORD = 109;
pub const ERROR_PIPE_BUSY: DWORD = 231;
pub const ERROR_NO_DATA: DWORD = 232;
pub const ERROR_PIPE_NOT_CONNECTED: DWORD = 233;
pub const ERROR_PIPE_CONNECTED: DWORD = 535;
pub const ERROR_OPERATION_ABORTED: DWORD = 995;
pub const ERROR_IO_PENDING: DWORD = 997;
pub const ERROR_NO_SYSTEM_RESOURCES: DWORD = 1450;

// Named pipes, sections, events and their security (platform/windows.zig)
pub const GENERIC_READ: DWORD = 0x8000_0000;
pub const GENERIC_WRITE: DWORD = 0x4000_0000;
pub const SYNCHRONIZE: DWORD = 0x0010_0000;
pub const EVENT_MODIFY_STATE: DWORD = 0x0002;
pub const FILE_MAP_WRITE: DWORD = 0x0002;
pub const OPEN_EXISTING: DWORD = 3;
pub const FILE_FLAG_OVERLAPPED: DWORD = 0x4000_0000;
pub const FILE_FLAG_FIRST_PIPE_INSTANCE: DWORD = 0x0008_0000;
/// A pipe client's quality of service: the server may identify the client, not impersonate it (`CreateFileW`'s flags).
pub const SECURITY_SQOS_PRESENT: DWORD = 0x0010_0000;
pub const SECURITY_IDENTIFICATION: DWORD = 0x0001_0000;
pub const PIPE_ACCESS_DUPLEX: DWORD = 0x0000_0003;
/// Byte type, byte read mode, blocking (PIPE_TYPE_BYTE, PIPE_READMODE_BYTE and PIPE_WAIT are 0).
pub const PIPE_TYPE_BYTE_READMODE_BYTE_WAIT: DWORD = 0;
pub const PIPE_REJECT_REMOTE_CLIENTS: DWORD = 0x0000_0008;
pub const TOKEN_QUERY: DWORD = 0x0008;
/// `TOKEN_INFORMATION_CLASS.TokenUser`.
pub const TokenUser: c_int = 1;
/// `TOKEN_INFORMATION_CLASS.TokenImpersonationLevel` (a `SECURITY_IMPERSONATION_LEVEL`: 1 identification, 2
/// impersonation).
pub const TokenImpersonationLevel: c_int = 9;
pub const SDDL_REVISION_1: DWORD = 1;
/// `SE_OBJECT_TYPE.SE_KERNEL_OBJECT`.
pub const SE_KERNEL_OBJECT: c_int = 6;
pub const OWNER_SECURITY_INFORMATION: DWORD = 0x01;
pub const DACL_SECURITY_INFORMATION: DWORD = 0x04;
pub const LABEL_SECURITY_INFORMATION: DWORD = 0x10;
/// `ACL_INFORMATION_CLASS.AclSizeInformation`.
pub const AclSizeInformation: c_int = 2;
pub const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
/// SECURITY_MAX_SID_SIZE.
pub const max_sid_size = 68;

pub const SECURITY_ATTRIBUTES = extern struct {
    nLength: DWORD,
    lpSecurityDescriptor: ?*anyopaque,
    bInheritHandle: BOOL,
};

pub const SID_AND_ATTRIBUTES = extern struct {
    Sid: *SID,
    Attributes: DWORD,
};

pub const TOKEN_USER = extern struct {
    User: SID_AND_ATTRIBUTES,
};

pub const ACL_SIZE_INFORMATION = extern struct {
    AceCount: DWORD,
    AclBytesInUse: DWORD,
    AclBytesFree: DWORD,
};

/// An `ACCESS_ALLOWED_ACE`: the header, the access mask, then the SID, which starts at `SidStart`.
pub const ACCESS_ALLOWED_ACE = extern struct {
    AceType: u8,
    AceFlags: u8,
    AceSize: u16,
    Mask: DWORD,
    SidStart: DWORD,
};

// Processes, threads and time
pub extern "kernel32" fn GetCurrentProcessId() callconv(.winapi) DWORD;
pub extern "kernel32" fn GetCurrentThread() callconv(.winapi) HANDLE;
pub extern "kernel32" fn Sleep(dwMilliseconds: DWORD) callconv(.winapi) void;
pub extern "kernel32" fn GetLastError() callconv(.winapi) DWORD;
pub extern "kernel32" fn CloseHandle(hObject: HANDLE) callconv(.winapi) BOOL;
pub extern "kernel32" fn GetStdHandle(nStdHandle: DWORD) callconv(.winapi) ?HANDLE;
pub extern "kernel32" fn WriteFile(
    hFile: HANDLE,
    lpBuffer: [*]const u8,
    nNumberOfBytesToWrite: DWORD,
    lpNumberOfBytesWritten: ?*DWORD,
    lpOverlapped: ?*anyopaque,
) callconv(.winapi) BOOL;

// Events and waits
pub extern "kernel32" fn SetEvent(hEvent: HANDLE) callconv(.winapi) BOOL;
pub extern "kernel32" fn WaitForSingleObject(hHandle: HANDLE, dwMilliseconds: DWORD) callconv(.winapi) DWORD;
pub extern "kernel32" fn WaitForMultipleObjects(nCount: DWORD, lpHandles: [*]const HANDLE, bWaitAll: BOOL, dwMilliseconds: DWORD) callconv(.winapi) DWORD;

// Overlapped I/O on a pipe (the handshake's waits on the caller's thread)
pub const OVERLAPPED = extern struct {
    Internal: usize = 0,
    InternalHigh: usize = 0,
    Offset: DWORD = 0,
    OffsetHigh: DWORD = 0,
    hEvent: ?HANDLE = null,
};
pub extern "kernel32" fn ConnectNamedPipe(hNamedPipe: HANDLE, lpOverlapped: *OVERLAPPED) callconv(.winapi) BOOL;
pub extern "kernel32" fn ReadFile(
    hFile: HANDLE,
    lpBuffer: [*]u8,
    nNumberOfBytesToRead: DWORD,
    lpNumberOfBytesRead: ?*DWORD,
    lpOverlapped: *OVERLAPPED,
) callconv(.winapi) BOOL;
pub extern "kernel32" fn CancelIoEx(hFile: HANDLE, lpOverlapped: *OVERLAPPED) callconv(.winapi) BOOL;
pub extern "kernel32" fn GetOverlappedResult(hFile: HANDLE, lpOverlapped: *OVERLAPPED, lpNumberOfBytesTransferred: *DWORD, bWait: BOOL) callconv(.winapi) BOOL;

// File mappings (paging-file backed shared memory)
pub extern "kernel32" fn MapViewOfFile(
    hFileMappingObject: HANDLE,
    dwDesiredAccess: DWORD,
    dwFileOffsetHigh: DWORD,
    dwFileOffsetLow: DWORD,
    dwNumberOfBytesToMap: usize,
) callconv(.winapi) ?*anyopaque;
pub extern "kernel32" fn UnmapViewOfFile(lpBaseAddress: *const anyopaque) callconv(.winapi) BOOL;

// Named pipes, sections and events by name, threads by id, handle counts
pub extern "kernel32" fn CreateNamedPipeW(
    lpName: [*:0]const WCHAR,
    dwOpenMode: DWORD,
    dwPipeMode: DWORD,
    nMaxInstances: DWORD,
    nOutBufferSize: DWORD,
    nInBufferSize: DWORD,
    nDefaultTimeOut: DWORD,
    lpSecurityAttributes: ?*const SECURITY_ATTRIBUTES,
) callconv(.winapi) HANDLE;
pub extern "kernel32" fn CreateFileW(
    lpFileName: [*:0]const WCHAR,
    dwDesiredAccess: DWORD,
    dwShareMode: DWORD,
    lpSecurityAttributes: ?*const SECURITY_ATTRIBUTES,
    dwCreationDisposition: DWORD,
    dwFlagsAndAttributes: DWORD,
    hTemplateFile: ?HANDLE,
) callconv(.winapi) HANDLE;
pub extern "kernel32" fn DisconnectNamedPipe(hNamedPipe: HANDLE) callconv(.winapi) BOOL;
pub extern "kernel32" fn GetNamedPipeClientProcessId(Pipe: HANDLE, ClientProcessId: *u32) callconv(.winapi) BOOL;
pub extern "kernel32" fn GetNamedPipeServerProcessId(Pipe: HANDLE, ServerProcessId: *u32) callconv(.winapi) BOOL;
pub extern "kernel32" fn ProcessIdToSessionId(dwProcessId: DWORD, pSessionId: *DWORD) callconv(.winapi) BOOL;
pub extern "kernel32" fn CreateFileMappingW(
    hFile: HANDLE,
    lpFileMappingAttributes: ?*const SECURITY_ATTRIBUTES,
    flProtect: DWORD,
    dwMaximumSizeHigh: DWORD,
    dwMaximumSizeLow: DWORD,
    lpName: ?[*:0]const WCHAR,
) callconv(.winapi) ?HANDLE;
pub extern "kernel32" fn OpenFileMappingW(dwDesiredAccess: DWORD, bInheritHandle: BOOL, lpName: [*:0]const WCHAR) callconv(.winapi) ?HANDLE;
pub extern "kernel32" fn CreateEventW(
    lpEventAttributes: ?*const SECURITY_ATTRIBUTES,
    bManualReset: BOOL,
    bInitialState: BOOL,
    lpName: ?[*:0]const WCHAR,
) callconv(.winapi) ?HANDLE;
pub extern "kernel32" fn OpenEventW(dwDesiredAccess: DWORD, bInheritHandle: BOOL, lpName: [*:0]const WCHAR) callconv(.winapi) ?HANDLE;
pub extern "kernel32" fn OpenThread(dwDesiredAccess: DWORD, bInheritHandle: BOOL, dwThreadId: DWORD) callconv(.winapi) ?HANDLE;
pub extern "kernel32" fn GetCurrentProcess() callconv(.winapi) HANDLE;
pub extern "kernel32" fn TerminateProcess(hProcess: HANDLE, uExitCode: c_uint) callconv(.winapi) BOOL;
pub extern "kernel32" fn GetProcessHandleCount(hProcess: HANDLE, pdwHandleCount: *DWORD) callconv(.winapi) BOOL;
pub extern "kernel32" fn GetHandleInformation(hObject: HANDLE, lpdwFlags: *DWORD) callconv(.winapi) BOOL;
pub const HANDLE_FLAG_INHERIT: DWORD = 0x0000_0001;
pub extern "kernel32" fn LocalFree(hMem: ?*anyopaque) callconv(.winapi) ?*anyopaque;

// The process token's user SID and the security of every object (advapi32)
pub extern "advapi32" fn OpenProcessToken(ProcessHandle: HANDLE, DesiredAccess: DWORD, TokenHandle: *HANDLE) callconv(.winapi) BOOL;
pub extern "advapi32" fn OpenThreadToken(ThreadHandle: HANDLE, DesiredAccess: DWORD, OpenAsSelf: BOOL, TokenHandle: *HANDLE) callconv(.winapi) BOOL;
pub extern "advapi32" fn ImpersonateNamedPipeClient(hNamedPipe: HANDLE) callconv(.winapi) BOOL;
pub extern "advapi32" fn RevertToSelf() callconv(.winapi) BOOL;
pub extern "advapi32" fn GetTokenInformation(
    TokenHandle: HANDLE,
    TokenInformationClass: c_int,
    TokenInformation: ?*anyopaque,
    TokenInformationLength: DWORD,
    ReturnLength: *DWORD,
) callconv(.winapi) BOOL;
pub extern "advapi32" fn GetLengthSid(pSid: *SID) callconv(.winapi) DWORD;
pub extern "advapi32" fn EqualSid(pSid1: *SID, pSid2: *SID) callconv(.winapi) BOOL;
pub extern "advapi32" fn ConvertSidToStringSidW(Sid: *SID, StringSid: *?[*:0]WCHAR) callconv(.winapi) BOOL;
pub extern "advapi32" fn ConvertStringSidToSidW(StringSid: [*:0]const WCHAR, Sid: *?*SID) callconv(.winapi) BOOL;
pub extern "advapi32" fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
    StringSecurityDescriptor: [*:0]const WCHAR,
    StringSDRevision: DWORD,
    SecurityDescriptor: *?*anyopaque,
    SecurityDescriptorSize: ?*u32,
) callconv(.winapi) BOOL;
pub extern "advapi32" fn GetSecurityInfo(
    handle: HANDLE,
    ObjectType: c_int,
    SecurityInfo: DWORD,
    ppsidOwner: ?*?*SID,
    ppsidGroup: ?*?*SID,
    ppDacl: ?*?*ACL,
    ppSacl: ?*?*ACL,
    ppSecurityDescriptor: *?*anyopaque,
) callconv(.winapi) DWORD;
pub extern "advapi32" fn GetAclInformation(
    pAcl: *ACL,
    pAclInformation: *anyopaque,
    nAclInformationLength: DWORD,
    dwAclInformationClass: c_int,
) callconv(.winapi) BOOL;
pub extern "advapi32" fn GetAce(pAcl: *ACL, dwAceIndex: DWORD, pAce: *?*anyopaque) callconv(.winapi) BOOL;
pub extern "advapi32" fn CheckTokenMembership(TokenHandle: ?HANDLE, SidToCheck: *SID, IsMember: *BOOL) callconv(.winapi) BOOL;

pub const MEMORY_BASIC_INFORMATION = extern struct {
    BaseAddress: ?*anyopaque,
    AllocationBase: ?*anyopaque,
    AllocationProtect: DWORD,
    PartitionId: u16,
    RegionSize: usize,
    State: DWORD,
    Protect: DWORD,
    Type: DWORD,
};
pub const MEM_MAPPED: DWORD = 0x40000;
pub extern "kernel32" fn VirtualQuery(lpAddress: ?*const anyopaque, lpBuffer: *MEMORY_BASIC_INFORMATION, dwLength: usize) callconv(.winapi) usize;
