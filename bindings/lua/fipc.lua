--[[
FastIPC: messages and RPC between two processes on one machine, through shared memory. The LuaJIT binding of
FastIPC's C API (include/fipc.h), through LuaJIT's FFI: one Lua file and the native library, no C module.

    local fipc = require("fipc")

    local listener = assert(fipc.listen("my_channel", 1024 * 1024)) -- the server: rings of 1 MiB each way
    local conn = assert(listener:accept())                         -- waits for a client
    local conn = assert(fipc.connect("my_channel", 5000))          -- the client, in another process: waits up to 5 s

The client and the server run in different processes: connect waits for the server's accept, and a Lua state has one
thread.

Results. A call that succeeds returns its value (true when there is none). A call the library refuses returns nil and
the error's name: "timeout", "disconnected", "cancelled", "too_large", "invalid", "no_memory" or "addr_in_use" (the
C API's results; fipc.TIMEOUT ... are the same strings). A receive that is "too_large" for the caller's buffer
returns the message's length third. Wrap a call in assert() to raise instead. A misuse (a closed handle, an argument
of the wrong type) raises an error.

Timeouts are milliseconds, the last argument of every call that can wait: 0 (fipc.NO_WAIT) doesn't wait, nil or a
negative number (fipc.FOREVER) waits as long as it takes.

The header's rules hold: on a connection, one thread at a time sends and one receives (a Lua state is one thread);
close() needs the handle to itself. A listener or connection the program drops is closed when the garbage collector
frees it; close() closes it at once. Pointers that a connection returns (zero-copy) are valid only while it is open.
]]

local found_ffi, ffi = pcall(require, "ffi")
if not found_ffi then
    error("fipc needs LuaJIT 2.1 (its FFI), not " .. _VERSION)
end

local M = {}

M._VERSION = "1.0.0"
M.NO_WAIT = 0
M.FOREVER = -1
M.RPC_REQUEST = 1
M.RPC_RESPONSE = 2

-- The C API's results other than FIPC_OK, by value, as the names a failed call returns
local NAMES = { "timeout", "disconnected", "cancelled", "too_large", "invalid", "no_memory", "addr_in_use" }
local CODES = { ok = 0 }
for code, name in ipairs(NAMES) do
    CODES[name] = code
    M[name:upper()] = name
end
local TOO_LARGE, DISCONNECTED = 4, 2

if not pcall(ffi.typeof, "fipc_rpc_msg_t") then -- once, however many times this file is loaded
    ffi.cdef [[
        typedef struct fipc_listener fipc_listener_t;
        typedef struct fipc_conn fipc_conn_t;
        typedef int fipc_result_t;

        typedef struct {
            uint64_t id;
            uint32_t kind;
            uint32_t opcode;
            int32_t status;
            uint32_t reserved;
            uint64_t len;
        } fipc_rpc_msg_t;

        const char* fipc_result_str(fipc_result_t result);

        fipc_result_t fipc_listen(const char* name, size_t capacity, fipc_listener_t** out_listener);
        fipc_result_t fipc_accept(fipc_listener_t* listener, fipc_conn_t** out_conn, int timeout_ms);
        void fipc_listener_cancel(fipc_listener_t* listener);
        void fipc_listener_close(fipc_listener_t* listener);
        fipc_result_t fipc_connect(const char* name, fipc_conn_t** out_conn, int timeout_ms);
        size_t fipc_max_piece(const fipc_conn_t* conn);
        void fipc_cancel(fipc_conn_t* conn);
        void fipc_close(fipc_conn_t* conn);

        fipc_result_t fipc_send(fipc_conn_t* conn, const void* data, size_t len, int timeout_ms);
        fipc_result_t fipc_recv(fipc_conn_t* conn, void* buf, size_t buf_len, size_t* out_len, int timeout_ms);
        fipc_result_t fipc_send_acquire(fipc_conn_t* conn, size_t len, void** out_buf, int timeout_ms);
        fipc_result_t fipc_send_commit(fipc_conn_t* conn, size_t len);
        fipc_result_t fipc_recv_acquire(fipc_conn_t* conn, const void** out_data, size_t* out_len, int timeout_ms);
        void fipc_recv_release(fipc_conn_t* conn);

        fipc_result_t fipc_rpc_submit(fipc_conn_t* conn, uint32_t opcode, const void* data, size_t len,
                                      uint64_t* out_id, int timeout_ms);
        fipc_result_t fipc_rpc_respond(fipc_conn_t* conn, uint64_t id, uint32_t opcode, int32_t status,
                                       const void* data, size_t len, int timeout_ms);
        fipc_result_t fipc_rpc_recv(fipc_conn_t* conn, void* buf, size_t buf_len, fipc_rpc_msg_t* msg,
                                    int timeout_ms);
    ]]
end

-- === The native library ===

local WINDOWS = ffi.os == "Windows"
local MACOS = ffi.os == "OSX"
local LINUX_ARM64 = ffi.os == "Linux" and ffi.arch == "arm64"
local LIBRARY = WINDOWS and "fastipc.dll" or MACOS and "libfastipc.dylib" or "libfastipc.so"

local X64 = ffi.arch == "x64" and (WINDOWS or ffi.os == "Linux")
if not (X64 or (MACOS and ffi.arch == "arm64") or LINUX_ARM64) then
    error("fipc runs on Linux and Windows on x86-64 and on Linux and macOS on arm64, not " .. ffi.os .. " on "
        .. ffi.arch)
end

local function is_file(path)
    local file = io.open(path, "rb")
    if file then
        file:close()
    end
    return file ~= nil
end

-- The folder of this file, if it was loaded from one
local function module_folder()
    local source = debug.getinfo(1, "S").source
    return source:sub(1, 1) == "@" and (source:sub(2):match("^(.*)[/\\]") or ".") or nil
end

--[[ The library's path, in this order: the folder the environment variable FASTIPC_LIB_DIR names; in a checkout of
the repository (this file is bindings/lua/fipc.lua), its zig-out build; the folder of this file (a copy
vendored into a project); the rock's copy, fipc/<library> on package.cpath (on macOS, whose cpath names C
modules ?.so, the same folders with ?.dylib; on Linux on arm64, fipc/arm64/<library>: the rock installs both
Linux libraries). nil: the system's search. ]]
local function library_path()
    local dir = os.getenv("FASTIPC_LIB_DIR")
    if dir and dir ~= "" then
        local path = dir .. "/" .. LIBRARY
        if not is_file(path) then
            error("fipc: FASTIPC_LIB_DIR names " .. dir .. ", which holds no " .. LIBRARY)
        end
        return path
    end
    local here = module_folder()
    if here then
        local repository = here .. "/../.."
        if is_file(repository .. "/build.zig") and is_file(repository .. "/include/fipc.h") then
            for _, folder in ipairs({ "/zig-out/lib/", "/zig-out/bin/" }) do
                if is_file(repository .. folder .. LIBRARY) then
                    return repository .. folder .. LIBRARY
                end
            end
        end
        if is_file(here .. "/" .. LIBRARY) then
            return here .. "/" .. LIBRARY
        end
    end
    local cpath = MACOS and (package.cpath:gsub("%?%.so", "?.dylib")) or package.cpath
    local folder = LINUX_ARM64 and "fipc.arm64." or "fipc."
    return package.searchpath(folder .. LIBRARY:match("^(.-)%.%w+$"), cpath)
end

local function native_path(path)
    return WINDOWS and path and (path:gsub("/", "\\")) or path
end

--[[ Keeps the library loaded until the process ends. LuaJIT unloads a library when its namespace is collected, and
when the Lua state closes it does so before the finalizers that would close the handles still open; the C API
allows unloading only once every handle is closed, and never on Linux or macOS (its fork handlers stay
registered). ]]
local function pin(path, lib)
    if WINDOWS then -- the module that holds fipc_close, pinned
        pcall(ffi.cdef, "int GetModuleHandleExA(unsigned long flags, const void* name, void** module);")
        local FLAG_PIN, FLAG_FROM_ADDRESS = 0x1, 0x4
        local address = ffi.cast("const void*", lib.fipc_close)
        return ffi.C.GetModuleHandleExA(FLAG_PIN + FLAG_FROM_ADDRESS, address, ffi.new("void*[1]")) ~= 0
    end
    pcall(ffi.cdef, "void* dlopen(const char* file, int mode);")
    local RTLD_NOW, RTLD_NODELETE = 0x2, MACOS and 0x80 or 0x1000
    return ffi.C.dlopen(path, RTLD_NOW + RTLD_NODELETE) ~= nil
end

local path = native_path(library_path())
local ok, C = pcall(ffi.load, path or LIBRARY)
if not ok then
    error("fipc: can't load " .. (path or LIBRARY) .. " (" .. tostring(C) .. "); set FASTIPC_LIB_DIR to "
        .. "the folder that holds it")
end
if not pin(path or LIBRARY, C) then
    error("fipc: can't keep " .. (path or LIBRARY) .. " loaded")
end

--- The library's namespace: the 18 functions of include/fipc.h, for code that calls the C API itself.
M.C = C
--- Where the library was loaded from (a path, or the bare file name the system's search found).
M.library = path or LIBRARY

--- The C name of a result: an error name ("timeout") or a fipc_result_t value; "FIPC_UNKNOWN" for any other.
function M.result_str(result)
    local code = type(result) == "string" and CODES[result] or result
    if type(code) ~= "number" then
        return "FIPC_UNKNOWN"
    end
    return ffi.string(C.fipc_result_str(code))
end

-- === Arguments ===

local INT_MAX = 2147483647

-- int milliseconds: nil or negative waits for ever; a fraction of a millisecond counts as a whole one
local function millis(timeout)
    if timeout == nil then
        return -1
    end
    if type(timeout) ~= "number" or timeout ~= timeout then
        error("fipc: a timeout is milliseconds (a number) or nil, not " .. tostring(timeout), 3)
    end
    if timeout < 0 then
        return -1
    end
    return timeout >= INT_MAX and INT_MAX or math.ceil(timeout)
end

-- A message's bytes: a string (then the timeout follows), or a cdata pointer and its length (then the timeout)
local function bytes(data, a, b, empty_ok)
    local kind = type(data)
    if kind == "string" then
        return data, #data, a
    elseif kind == "cdata" then
        if type(a) ~= "number" then
            error("fipc: a cdata message needs its length after it", 3)
        end
        return data, a, b
    elseif kind == "nil" and empty_ok then
        return "", 0, a
    end
    error("fipc: a message is a string, or a cdata pointer and its length, not " .. kind, 3)
end

local function length(n, what)
    if type(n) ~= "number" or n < 0 or n ~= math.floor(n) then
        error("fipc: " .. what .. " is a whole number of bytes, not " .. tostring(n), 3)
    end
    return n
end

-- === Connection ===

local Connection = {}
Connection.__index = Connection
M.Connection = Connection

local INITIAL_BUFFER = 4096

local function new_connection(handle)
    return setmetatable({
        _handle = ffi.gc(handle, C.fipc_close),
        _len = ffi.new("size_t[1]"),
        _ptr = ffi.new("void*[1]"),
        _cptr = ffi.new("const void*[1]"),
        _id = ffi.new("uint64_t[1]"),
        _msg = ffi.new("fipc_rpc_msg_t"),
        _buf = nil, -- the receive buffer of recv and rpc_recv, grown for a message that doesn't fit
        _cap = 0,
        _ended = false,
    }, Connection)
end

local function conn_handle(self)
    local handle = self._handle
    if handle == nil then
        error("fipc: the connection is closed", 3)
    end
    return handle
end

-- nil and the result's name; "disconnected" is remembered (ended())
local function failed(self, result)
    if result == DISCONNECTED then
        self._ended = true
    end
    return nil, NAMES[result] or "unknown"
end

-- The connection's receive buffer, at least `size` bytes
local function buffer(self, size)
    if self._cap < size then
        local cap = math.max(size, INITIAL_BUFFER)
        self._buf, self._cap = ffi.new("uint8_t[?]", cap), cap
    end
    return self._buf, self._cap
end

--- The client: connects to the server listening on `name` and sets the connection up, waiting up to `timeout` ms for
--- it to listen and to call accept: the server runs in another process. nil (fipc.FOREVER) waits for a server however
--- late it starts. Returns the connection, or nil and the error's name.
function M.connect(name, timeout)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    local out = ffi.new("fipc_conn_t*[1]")
    local result = C.fipc_connect(name, out, millis(timeout))
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return new_connection(out[0])
end

--- Sends one message of any size, at least 1 byte: `conn:send(string, timeout)` or `conn:send(ptr, len, timeout)`.
--- The timeout bounds the wait for room for its first piece. Returns true.
function Connection:send(data, a, b)
    local handle = conn_handle(self)
    local ptr, len, timeout = bytes(data, a, b, false)
    local result = C.fipc_send(handle, ptr, len, millis(timeout))
    if result ~= 0 then
        return failed(self, result)
    end
    return true
end

--- Receives one message of any size, as a string.
function Connection:recv(timeout)
    local handle = conn_handle(self)
    local buf, cap = buffer(self, INITIAL_BUFFER)
    local result = C.fipc_recv(handle, buf, cap, self._len, millis(timeout))
    if result == TOO_LARGE then -- the message stays queued: take it into a buffer that fits
        buf, cap = buffer(self, tonumber(self._len[0]))
        result = C.fipc_recv(handle, buf, cap, self._len, 0)
    end
    if result ~= 0 then
        return failed(self, result)
    end
    return ffi.string(buf, tonumber(self._len[0]))
end

--- Receives one message into `buf` (a cdata pointer to `size` bytes) and returns its length. A message longer than
--- `size` stays queued: nil, "too_large" and its length.
function Connection:recv_into(buf, size, timeout)
    local handle = conn_handle(self)
    local result = C.fipc_recv(handle, buf, length(size, "a buffer's size"), self._len, millis(timeout))
    if result == 0 then
        return tonumber(self._len[0])
    elseif result == TOO_LARGE then
        return nil, "too_large", tonumber(self._len[0])
    end
    return failed(self, result)
end

--- Zero-copy send, step 1: room for `len` bytes (1 to max_piece()) in the ring, as a uint8_t pointer (16-byte
--- aligned). Write the message there, then send_commit. The next send or acquire drops a reservation never
--- committed.
function Connection:send_acquire(len, timeout)
    local handle = conn_handle(self)
    local result = C.fipc_send_acquire(handle, length(len, "a length"), self._ptr, millis(timeout))
    if result ~= 0 then
        return failed(self, result)
    end
    return ffi.cast("uint8_t*", self._ptr[0])
end

--- Zero-copy send, step 2: sends the first `len` bytes of the acquired room (1 to its length) as one message.
--- Doesn't wait.
function Connection:send_commit(len)
    local result = C.fipc_send_commit(conn_handle(self), length(len, "a length"))
    if result ~= 0 then
        return failed(self, result)
    end
    return true
end

--- Zero-copy receive, step 1: the next message in the ring, read-only: a const uint8_t pointer and its length,
--- valid until recv_release, the next receive or close. A message of several pieces stays queued for recv: nil,
--- "too_large" and its length.
function Connection:recv_acquire(timeout)
    local handle = conn_handle(self)
    local result = C.fipc_recv_acquire(handle, self._cptr, self._len, millis(timeout))
    if result == 0 then
        return ffi.cast("const uint8_t*", self._cptr[0]), tonumber(self._len[0])
    elseif result == TOO_LARGE then
        return nil, "too_large", tonumber(self._len[0])
    end
    return failed(self, result)
end

--- Zero-copy receive, step 2: frees the acquired message's room (without one, does nothing).
function Connection:recv_release()
    C.fipc_recv_release(conn_handle(self))
end

--- Sends a request with a payload of any size, possibly empty (nil): `conn:rpc_submit(opcode, string, timeout)` or
--- `conn:rpc_submit(opcode, ptr, len, timeout)`. Returns its id (a connection numbers its requests from 1).
function Connection:rpc_submit(opcode, data, a, b)
    local handle = conn_handle(self)
    local ptr, len, timeout = bytes(data, a, b, true)
    local result = C.fipc_rpc_submit(handle, opcode, ptr, len, self._id, millis(timeout))
    if result ~= 0 then
        return failed(self, result)
    end
    return tonumber(self._id[0])
end

--- Sends the response to request `id`, with the application's `status` (an int32) and a payload of any size,
--- possibly empty: `conn:rpc_respond(id, opcode, status, string, timeout)` or `(id, opcode, status, ptr, len,
--- timeout)`. Returns true.
function Connection:rpc_respond(id, opcode, status, data, a, b)
    local handle = conn_handle(self)
    local ptr, len, timeout = bytes(data, a, b, true)
    local result = C.fipc_rpc_respond(handle, id, opcode, status or 0, ptr, len, millis(timeout))
    if result ~= 0 then
        return failed(self, result)
    end
    return true
end

local function header(msg)
    return {
        id = tonumber(msg.id),
        kind = msg.kind,
        opcode = msg.opcode,
        status = msg.status,
        len = tonumber(msg.len),
    }
end

--- Receives one request or response: a table { id, kind (RPC_REQUEST or RPC_RESPONSE), opcode, status, payload }.
function Connection:rpc_recv(timeout)
    local handle = conn_handle(self)
    local msg = self._msg
    local buf, cap = buffer(self, INITIAL_BUFFER)
    local result = C.fipc_rpc_recv(handle, buf, cap, msg, millis(timeout))
    if result == TOO_LARGE then
        buf, cap = buffer(self, tonumber(msg.len))
        result = C.fipc_rpc_recv(handle, buf, cap, msg, 0)
    end
    if result ~= 0 then
        return failed(self, result)
    end
    local message = header(msg)
    message.payload = ffi.string(buf, message.len)
    message.len = nil
    return message
end

--- Receives one request or response with its payload in `buf` (a cdata pointer to `size` bytes): a table { id,
--- kind, opcode, status, len }. A payload longer than `size` stays queued: nil, "too_large" and its length.
function Connection:rpc_recv_into(buf, size, timeout)
    local handle = conn_handle(self)
    local msg = self._msg
    local result = C.fipc_rpc_recv(handle, buf, length(size, "a buffer's size"), msg, millis(timeout))
    if result == 0 then
        return header(msg)
    elseif result == TOO_LARGE then
        return nil, "too_large", tonumber(msg.len)
    end
    return failed(self, result)
end

--- The longest message the zero-copy calls take: the ring's capacity less 64 bytes.
function Connection:max_piece()
    return tonumber(C.fipc_max_piece(conn_handle(self)))
end

--- Makes every call of the connection that waits, now or later, return nil, "cancelled"; calls that needn't wait
--- still work. Final.
function Connection:cancel()
    C.fipc_cancel(conn_handle(self))
end

--- Whether a call has returned "disconnected": the peer closed the connection or its process ended. Final.
function Connection:ended()
    return self._ended
end

--- Ends the connection (the peer gets "disconnected" once it has received what this side sent) and frees it. Every
--- pointer it returned becomes invalid. Closing it again does nothing.
function Connection:close()
    local handle = self._handle
    if handle ~= nil then
        self._handle = nil
        ffi.gc(handle, nil)
        C.fipc_close(handle)
    end
end

--- Whether close() has been called.
function Connection:closed()
    return self._handle == nil
end

-- === Listener ===

local Listener = {}
Listener.__index = Listener
M.Listener = Listener

local function listener_handle(self)
    local handle = self._handle
    if handle == nil then
        error("fipc: the listener is closed", 3)
    end
    return handle
end

--- The server: claims `name` (1-245 characters of [A-Za-z0-9_.-]) and listens on it, with rings of `capacity` bytes
--- each way (a power of two, 1 KiB to 2 GiB). Doesn't wait. Returns the listener, or nil and "addr_in_use" if
--- another listener holds the name ("invalid" for a bad name or capacity).
function M.listen(name, capacity)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    local out = ffi.new("fipc_listener_t*[1]")
    local result = C.fipc_listen(name, length(capacity, "a capacity"), out)
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return setmetatable({ _handle = ffi.gc(out[0], C.fipc_listener_close), name = name }, Listener)
end

--- Waits up to `timeout` ms for a client, sets its connection up and returns it. A call that times out in the middle of
--- a client's setup keeps it for the next call, so fipc.NO_WAIT polls (a client then takes one or two calls). One
--- client at a time: "invalid" while the connection it returned last is open.
function Listener:accept(timeout)
    local out = ffi.new("fipc_conn_t*[1]")
    local result = C.fipc_accept(listener_handle(self), out, millis(timeout))
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return new_connection(out[0])
end

--- Makes every accept that waits, now or later, return nil, "cancelled"; a client whose setup an accept began is
--- dropped. Final.
function Listener:cancel()
    C.fipc_listener_cancel(listener_handle(self))
end

--- Stops listening and frees the listener; the connections it accepted stay open. Closing it again does nothing.
function Listener:close()
    local handle = self._handle
    if handle ~= nil then
        self._handle = nil
        ffi.gc(handle, nil)
        C.fipc_listener_close(handle)
    end
end

--- Whether close() has been called.
function Listener:closed()
    return self._handle == nil
end

return M
