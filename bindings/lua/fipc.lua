--[[
FastIPC: messages between two processes on one machine, broadcasts from one publisher to many subscribers, work
queues from many producers to many consumers, and services from many clients to a pool of servers, with replies,
through shared memory. The LuaJIT binding of FastIPC's C API (include/fipc.h), through LuaJIT's FFI: one Lua file and
the native library, no C module.

    local fipc = require("fipc")

    local listener = assert(fipc.stream_create("my_channel", 1024 * 1024)) -- the server: rings of 1 MiB each way
    local conn = assert(listener:accept())                                 -- waits for a client
    local conn = assert(fipc.stream_connect("my_channel", 5000))           -- the client, in another process

    local publisher = assert(fipc.broadcast_create("my_broadcast", 64 * 1024, 8, 5000))   -- a broadcast
    local subscriber = assert(fipc.broadcast_join("my_broadcast", fipc.SUBSCRIBER_GATED, 5000))

    local queue = assert(fipc.queue_create("my_queue", 4096, 8, 8))                        -- a work queue
    local producer = assert(fipc.queue_join("my_queue", fipc.PRODUCER, 5000))
    local consumer = assert(fipc.queue_join("my_queue", fipc.CONSUMER, 5000))

    local service = assert(fipc.service_create("my_service", 4096, 256, 8, 8))           -- a service
    local server = assert(fipc.service_join("my_service", fipc.SERVER, 5000))
    local client = assert(fipc.service_join("my_service", fipc.CLIENT, 5000))

A stream's client and server run in different processes (or Lua states on different threads): stream_connect waits
for the server's accept, and a Lua state has one thread. A broadcast's, a queue's or a service's members may join from
the owner's own Lua state: the owner's process admits each joiner on a thread of the library's.

Results. A call that succeeds returns its value (true when there is none). A call the library refuses returns nil and
the error's name: "timeout", "disconnected", "cancelled", "too_large", "invalid", "no_memory", "addr_in_use",
"overrun", "unsubscribed" or "abandoned" (the C API's results; fipc.TIMEOUT ... are the same strings). A third value
says more: a receive that is "too_large" for the caller's buffer returns the message's length, one that is "overrun" (a
lossy subscriber's) the number of messages lost, and wait_for_members at "timeout", "cancelled" or "disconnected" the
count. Wrap a call in assert() to raise instead. A misuse (a closed handle, an argument of the wrong type) raises an error.

Timeouts are milliseconds, the last argument of every call that can wait: 0 (fipc.NO_WAIT) doesn't wait, nil or a
negative number (fipc.FOREVER) waits as long as it takes.

The header's rules hold: on a connection, one thread at a time sends and one receives (a Lua state is one thread);
close() needs the handle to itself. A listener or connection the program drops is closed when the garbage collector
frees it; close() closes it at once. A queue or a service is not: it lives until close() or the end of its process,
and dropping the object doesn't end it. Pointers that a connection returns (zero-copy) are valid only while it is open.
]]

local found_ffi, ffi = pcall(require, "ffi")
if not found_ffi then
    error("fipc needs LuaJIT 2.1 (its FFI), not " .. _VERSION)
end

local M = {}

M._VERSION = "1.0.0"
M.NO_WAIT = 0
M.FOREVER = -1

-- The C API's results other than FIPC_OK, by value, as the names a failed call returns
local NAMES = { "timeout", "disconnected", "cancelled", "too_large", "invalid", "no_memory", "addr_in_use", "overrun",
    "unsubscribed", "abandoned" }
local CODES = { ok = 0 }
for code, name in ipairs(NAMES) do
    CODES[name] = code
    M[name:upper()] = name
end
local TOO_LARGE, DISCONNECTED, INVALID, OVERRUN, ABANDONED = 4, 2, 5, 8, 10

-- broadcast_join's roles: a lossy subscriber (never holds the publisher back) and a gated one (misses nothing);
-- queue_join's: a work queue's producer and consumer, which a queue's wait_for_members counts; service_join's: a
-- service's client and server, which a service's wait_for_members counts
M.SUBSCRIBER_LOSSY = 1
M.SUBSCRIBER_GATED = 2
M.PRODUCER = 3
M.CONSUMER = 4
M.CLIENT = 5
M.SERVER = 6

if not pcall(ffi.typeof, "fipc_conn_t") then -- once, however many times this file is loaded
    ffi.cdef [[
        typedef struct fipc_listener fipc_listener_t;
        typedef struct fipc_conn fipc_conn_t;
        typedef struct fipc_queue fipc_queue_t;
        typedef struct fipc_service fipc_service_t;
        typedef int fipc_result_t;

        const char* fipc_result_str(fipc_result_t result);

        fipc_result_t fipc_stream_create(const char* name, size_t capacity, fipc_listener_t** out_listener);
        fipc_result_t fipc_stream_accept(fipc_listener_t* listener, fipc_conn_t** out_conn, int timeout_ms);
        void fipc_listener_cancel(fipc_listener_t* listener);
        void fipc_listener_close(fipc_listener_t* listener);
        fipc_result_t fipc_stream_connect(const char* name, fipc_conn_t** out_conn, int timeout_ms);
        size_t fipc_max_piece(const fipc_conn_t* conn);
        void fipc_cancel(fipc_conn_t* conn);
        void fipc_close(fipc_conn_t* conn);

        fipc_result_t fipc_send(fipc_conn_t* conn, const void* data, size_t len, int timeout_ms);
        fipc_result_t fipc_recv(fipc_conn_t* conn, void* buf, size_t buf_len, size_t* out_len, int timeout_ms);
        fipc_result_t fipc_send_many(fipc_conn_t* conn, const void* const* datas, const size_t* lens, size_t count,
                                     size_t* out_sent, int timeout_ms);
        fipc_result_t fipc_recv_many(fipc_conn_t* conn, void* buf, size_t buf_len, size_t* lens, size_t max_count,
                                     size_t* out_count, int timeout_ms);
        fipc_result_t fipc_send_acquire(fipc_conn_t* conn, size_t len, void** out_buf, int timeout_ms);
        fipc_result_t fipc_send_commit(fipc_conn_t* conn, size_t len);
        fipc_result_t fipc_recv_acquire(fipc_conn_t* conn, const void** out_data, size_t* out_len, int timeout_ms);
        void fipc_recv_release(fipc_conn_t* conn);

        fipc_result_t fipc_broadcast_create(const char* name, size_t capacity, uint32_t max_subscribers,
                                            int max_stall_ms, fipc_conn_t** out_publisher);
        fipc_result_t fipc_broadcast_join(const char* name, int role, fipc_conn_t** out_conn, int timeout_ms);
        fipc_result_t fipc_broadcast_wait_for_members(fipc_conn_t* publisher, uint32_t at_least,
                                                      uint32_t* out_count, int timeout_ms);

        fipc_result_t fipc_queue_create(const char* name, size_t capacity, uint32_t max_producers,
                                        uint32_t max_consumers, fipc_queue_t** out_queue);
        fipc_result_t fipc_queue_join(const char* name, int role, fipc_conn_t** out_conn, int timeout_ms);
        fipc_result_t fipc_queue_wait_for_members(fipc_queue_t* queue, int role, uint32_t at_least,
                                                  uint32_t* out_count, int timeout_ms);
        void fipc_queue_cancel(fipc_queue_t* queue);
        void fipc_queue_close(fipc_queue_t* queue);

        fipc_result_t fipc_service_create(const char* name, size_t capacity, size_t max_message,
                                          uint32_t max_clients, uint32_t max_servers, fipc_service_t** out_service);
        fipc_result_t fipc_service_join(const char* name, int role, fipc_conn_t** out_conn, int timeout_ms);
        fipc_result_t fipc_service_wait_for_members(fipc_service_t* service, int role, uint32_t at_least,
                                                    uint32_t* out_count, int timeout_ms);
        void fipc_service_cancel(fipc_service_t* service);
        void fipc_service_close(fipc_service_t* service);
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

--[[ The library's path, the first of these that holds it:
1. the folder the environment variable FASTIPC_LIB_DIR names;
2. in a checkout of the repository (this file is bindings/lua/fipc.lua), its zig-out build;
3. this file's folder (a copy vendored into a project);
4. the rock's copy, fipc/<library> on package.cpath. On macOS, whose cpath names C modules ?.so, the same folders
   with ?.dylib. On Linux arm64, fipc/arm64/<library>, because the rock installs both Linux libraries.
nil: the system's search. ]]
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

-- Seconds on a monotonic clock, for the deadline of a receive that tries again. The declarations name the system's
-- functions through asm() under names of their own, so a program's declarations of them don't clash with these.
local now
if WINDOWS then
    pcall(ffi.cdef, [[
        int fipc_lua_query_counter(int64_t* count) __asm__("QueryPerformanceCounter");
        int fipc_lua_query_frequency(int64_t* frequency) __asm__("QueryPerformanceFrequency");
    ]])
    local count, frequency = ffi.new("int64_t[1]"), ffi.new("int64_t[1]")
    ffi.C.fipc_lua_query_frequency(frequency)
    local ticks_per_second = tonumber(frequency[0])
    function now()
        ffi.C.fipc_lua_query_counter(count)
        return tonumber(count[0]) / ticks_per_second
    end
else
    pcall(ffi.cdef, [[
        typedef struct { long seconds; long nanoseconds; } fipc_lua_timespec;
        int fipc_lua_clock_gettime(int clock, fipc_lua_timespec* now) __asm__("clock_gettime");
    ]])
    local MONOTONIC = MACOS and 6 or 1
    local time = ffi.new("fipc_lua_timespec")
    function now()
        ffi.C.fipc_lua_clock_gettime(MONOTONIC, time)
        return tonumber(time.seconds) + tonumber(time.nanoseconds) / 1e9
    end
end

--- The library's namespace: every function of include/fipc.h, for code that calls the C API itself.
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

-- What is left of a wait of `milliseconds` (millis()) that began at `start` (now()), as the next call's timeout;
-- fipc.NO_WAIT and fipc.FOREVER stay as they are
local function remaining(milliseconds, start)
    if milliseconds <= 0 then
        return milliseconds
    end
    return math.max(0, math.ceil(milliseconds - (now() - start) * 1000))
end

-- A message's bytes: a string (then the timeout follows), or a cdata pointer and its length (then the timeout)
local function bytes(data, a, b)
    local kind = type(data)
    if kind == "string" then
        return data, #data, a
    elseif kind == "cdata" then
        if type(a) ~= "number" then
            error("fipc: a cdata message needs its length after it", 3)
        end
        return data, a, b
    end
    error("fipc: a message is a string, or a cdata pointer and its length, not " .. kind, 3)
end

local function length(n, what)
    if type(n) ~= "number" or n < 0 or n ~= math.floor(n) then
        error("fipc: " .. what .. " is a whole number of bytes, not " .. tostring(n), 3)
    end
    return n
end

-- A number of subscribers, a uint32_t: a larger one goes as 2^32 - 1, which is over every limit too
local function subscriber_count(n)
    if type(n) ~= "number" or n < 0 or n ~= math.floor(n) then
        error("fipc: a number of subscribers is a whole number, not " .. tostring(n), 3)
    end
    return math.min(n, 0xFFFFFFFF)
end

-- A number of members (`what`: of a queue, producers or consumers; of a service, clients or servers), a uint32_t: a
-- larger one goes as 2^32 - 1, which is over the limit too
local function member_count(n, what)
    if type(n) ~= "number" or n < 0 or n ~= math.floor(n) then
        error("fipc: a number of " .. what .. " is a whole number, not " .. tostring(n), 3)
    end
    return math.min(n, 0xFFFFFFFF)
end
local QUEUE_MEMBERS, SERVICE_MEMBERS = "producers or consumers", "clients or servers"

-- === Connection ===

local Connection = {}
Connection.__index = Connection
M.Connection = Connection

local INITIAL_BUFFER = 4096
local BATCH_BUFFER = 65536 -- the least room recv_many receives into

--[[ A connection; `client` for a service's client, which counts the replies it is owed, oldest first: those owed to
calls that gave up on them (call's receive timed out or was cancelled), which the next call receives and drops, then
those owed to send and send_commit, which are the caller's to receive (a call refuses while one is owed, so none of
call's comes after them). A reply, or "abandoned", answers the oldest. A Lua state is one thread: a request is counted
once its send has succeeded. ]]
local function new_connection(handle, client)
    return setmetatable({
        _handle = ffi.gc(handle, C.fipc_close),
        _len = ffi.new("size_t[1]"),
        _ptr = ffi.new("void*[1]"),
        _cptr = ffi.new("const void*[1]"),
        _count = ffi.new("uint32_t[1]"),
        _buf = nil, -- recv's receive buffer, grown for a message that doesn't fit
        _cap = 0,
        _batch = nil, -- send_many's addresses and lengths, and recv_many's lengths, grown for a longer batch
        _batch_cap = 0,
        _ended = false,
        _client = client or false,
        _calls_owed = 0,
        _sends_owed = 0,
        _cancelled = false, -- cancel() ran: a call sends nothing, as it could never receive the reply
    }, Connection)
end

-- On a service's client: a receive's result that answers its oldest request (a reply, or "abandoned") takes it off
local function count_answer(self, result)
    if self._client and (result == 0 or result == ABANDONED) then
        if self._calls_owed > 0 then
            self._calls_owed = self._calls_owed - 1
        elseif self._sends_owed > 0 then
            self._sends_owed = self._sends_owed - 1
        end
    end
end

-- On a service's client: a request of send's or send_commit's, sent
local function count_send(self)
    if self._client then
        self._sends_owed = self._sends_owed + 1
    end
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

-- A receive's failure: "overrun" with the number of messages lost, a number (exact up to 2^53, as every length here)
local function receive_failed(self, result)
    if result == OVERRUN then
        return nil, "overrun", tonumber(self._len[0])
    end
    return failed(self, result)
end

-- The connection's receive buffer, at least `size` bytes
local function buffer(self, size)
    if self._cap < size then
        local cap = math.max(size, INITIAL_BUFFER)
        self._buf, self._cap = ffi.new("uint8_t[?]", cap), cap
    end
    return self._buf, self._cap
end

--- Sends one message of any size, at least 1 byte: `conn:send(string, timeout)` or `conn:send(ptr, len, timeout)`.
--- The timeout bounds the wait for room for its first piece. Returns true. On a service's client: a request (of one
--- piece, up to max_piece()), whose reply a receive returns; on a server: the reply to the request it holds, which
--- never waits ("invalid" with no request held).
function Connection:send(data, a, b)
    local handle = conn_handle(self)
    local ptr, len, timeout = bytes(data, a, b)
    local result = C.fipc_send(handle, ptr, len, millis(timeout))
    if result ~= 0 then
        return failed(self, result)
    end
    count_send(self)
    return true
end

-- The connection's arrays for a batch of `count` messages: each one's address and length
local function batch_arrays(self, count)
    if self._batch_cap < math.max(count, 1) then
        local cap = math.max(count, 64)
        self._batch = { datas = ffi.new("const void*[?]", cap), lens = ffi.new("size_t[?]", cap) }
        self._batch_cap = cap
    end
    return self._batch.datas, self._batch.lens
end

--- Sends `messages`, a table of strings (at least one, each of at least 1 byte), in order in one call, published
--- together with at most one wake of the receivers, and returns how many went: all of them. The timeout bounds the
--- whole call (NO_WAIT: only what fits now). A failure is the first message that didn't go, as send would return it,
--- with how many went (all those before it) as the third value. Every message is checked first, before any goes:
--- "invalid" for an empty one or an empty table, "too_large" over max_piece() on a broadcast, a work queue or a
--- service (a stream sends a longer message in pieces). A service's client sends a batch of requests, whose replies
--- come in their order and are owed as send's are; a server's batch is "invalid". Batch both ends: a receiver that
--- takes one message per call falls behind a batching sender.
function Connection:send_many(messages, timeout)
    local handle = conn_handle(self)
    if type(messages) ~= "table" then
        error("fipc: send_many takes a table of strings, not " .. type(messages), 2)
    end
    local count = #messages
    local datas, lens = batch_arrays(self, count)
    for i = 1, count do
        local message = messages[i]
        if type(message) ~= "string" then
            error("fipc: send_many takes a table of strings, not one with a " .. type(message), 2)
        end
        datas[i - 1], lens[i - 1] = message, #message -- the strings stay in `messages` for the call
    end
    local result = C.fipc_send_many(handle, datas, lens, count, self._len, millis(timeout))
    local sent = tonumber(self._len[0])
    if self._client then
        self._sends_owed = self._sends_owed + sent
    end
    if result ~= 0 then
        local _, name = failed(self, result)
        return nil, name, sent
    end
    return sent
end

--- Waits up to `timeout` ms for the next message, as recv, then takes every further message already there, up to
--- `max_count` (default 16, at least 1) and as many as fit the connection's buffer (64 KiB, grown for a longer first
--- message), in one call, and returns them as a table of strings, in order. After the first it never waits, and stops
--- before a message that isn't there yet, doesn't fit, is in pieces, or would bring another result: the next receive
--- returns it. A failure is the first message's, as recv returns it, with nothing else taken: "overrun" with the number
--- lost, a service client's "abandoned", ...; "invalid" for a max_count of 0, on a publisher, a producer and a
--- service's server (which holds one request at a time). A broadcast's subscriber takes, after its first, only the
--- messages its publisher has moved past (the newest come in the next calls); a work queue's consumer acknowledges each
--- message before the call returns; a service's client receives its replies in its requests' order.
function Connection:recv_many(max_count, timeout)
    local handle = conn_handle(self)
    max_count = max_count or 16
    if type(max_count) ~= "number" or max_count < 0 or max_count ~= math.floor(max_count) then
        error("fipc: a number of messages is a whole number, not " .. tostring(max_count), 2)
    end
    local _, lens = batch_arrays(self, max_count)
    local milliseconds = millis(timeout)
    local start = milliseconds > 0 and now()
    local buf, cap = buffer(self, BATCH_BUFFER)
    local result = C.fipc_recv_many(handle, buf, cap, lens, max_count, self._len, milliseconds)
    -- The first message stays queued: take it into a buffer that fits (on a work queue another consumer may take it
    -- first, as in recv)
    while result == TOO_LARGE do
        buf, cap = buffer(self, tonumber(lens[0]))
        result = C.fipc_recv_many(handle, buf, cap, lens, max_count, self._len,
            start and remaining(milliseconds, start) or milliseconds)
    end
    if result ~= 0 then
        count_answer(self, result)
        if result == OVERRUN then
            return nil, "overrun", tonumber(lens[0])
        end
        return failed(self, result)
    end
    local messages, at = {}, 0
    for i = 0, tonumber(self._len[0]) - 1 do
        local n = tonumber(lens[i])
        messages[i + 1] = ffi.string(buf + at, n)
        at = at + n
        count_answer(self, 0)
    end
    return messages
end

-- recv's receive, of `milliseconds` (millis())
local function receive(self, handle, milliseconds)
    local start = milliseconds > 0 and now()
    local buf, cap = buffer(self, INITIAL_BUFFER)
    local result = C.fipc_recv(handle, buf, cap, self._len, milliseconds)
    -- The message stays queued: take it into a buffer that fits. On a work queue another consumer may take it first,
    -- so the next receive waits for what is left of the timeout, and may find a longer message.
    while result == TOO_LARGE do
        buf, cap = buffer(self, tonumber(self._len[0]))
        result = C.fipc_recv(handle, buf, cap, self._len, start and remaining(milliseconds, start) or milliseconds)
    end
    count_answer(self, result)
    if result ~= 0 then
        return receive_failed(self, result)
    end
    return ffi.string(buf, tonumber(self._len[0]))
end

--- Receives one message of any size, as a string. A lossy subscriber that fell a lap behind gets nil, "overrun" and
--- the number of messages it lost (at least 1) instead; its next receive goes on from a recent message. On a
--- service's client: the reply to its oldest request whose reply it hasn't received yet, or nil, "abandoned" if the
--- server that took that request ended before it replied (the request counts as answered); after a call that gave
--- up, that call's late reply (or "abandoned") comes first, as replies come in request order. On a server: the next
--- request of any client, which it holds until its reply ("invalid" while it holds one).
function Connection:recv(timeout)
    return receive(self, conn_handle(self), millis(timeout))
end

--- Receives one message into `buf` (a cdata pointer to `size` bytes) and returns its length. A message longer than
--- `size` stays queued: nil, "too_large" and its length. A lossy subscriber: nil, "overrun" and the number lost.
function Connection:recv_into(buf, size, timeout)
    local handle = conn_handle(self)
    local result = C.fipc_recv(handle, buf, length(size, "a buffer's size"), self._len, millis(timeout))
    count_answer(self, result)
    if result == 0 then
        return tonumber(self._len[0])
    elseif result == TOO_LARGE then
        return nil, "too_large", tonumber(self._len[0])
    end
    return receive_failed(self, result)
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
    count_send(self)
    return true
end

--- Zero-copy receive, step 1: the next message in the ring, read-only: a const uint8_t pointer and its length,
--- valid until recv_release, the next receive or close. A message of several pieces stays queued for recv: nil,
--- "too_large" and its length.
--- On a broadcast only a gated subscriber receives in place ("invalid" for a lossy one), and its message holds the
--- publisher back until recv_release. Unsafe there: a subscriber that has held the publisher back for the
--- broadcast's max_stall is unsubscribed, at a moment the publisher's clock picks, and the pointer's memory becomes
--- inaccessible: reading it then crashes the process. Release it well within max_stall, or receive in place only on
--- a broadcast created with max_stall nil (fipc.FOREVER), which never unsubscribes; recv is always safe.
--- A work queue's consumer takes the message in place: no other consumer gets it, and until recv_release it holds its
--- producer back once that producer is a ring ahead of it.
--- A service's client takes its next reply in place (or nil, "abandoned", as recv); a server the next request, which
--- stays valid until its reply's send or send_commit (recv_release does nothing there).
function Connection:recv_acquire(timeout)
    local handle = conn_handle(self)
    local result = C.fipc_recv_acquire(handle, self._cptr, self._len, millis(timeout))
    count_answer(self, result)
    if result == 0 then
        return ffi.cast("const uint8_t*", self._cptr[0]), tonumber(self._len[0])
    elseif result == TOO_LARGE then
        return nil, "too_large", tonumber(self._len[0])
    end
    return failed(self, result)
end

--- Zero-copy receive, step 2: frees the acquired message's room (without one, does nothing). On a service's server it
--- does nothing: the request stays held until the reply.
function Connection:recv_release()
    C.fipc_recv_release(conn_handle(self))
end

--- A service's client: sends a request (`conn:call(string, timeout)` or `conn:call(ptr, len, timeout)`) and returns its
--- reply, as a string, waiting up to `timeout` ms in all; nil, "abandoned" if the server that took it ended before it
--- replied. A call checks its request first: "invalid" for an empty one, "too_large" over max_piece(), at once, even
--- while a reply is owed. Then a call on a cancelled connection returns nil, "cancelled" at once, whatever its timeout,
--- and sends nothing: it could never receive the reply. A call never returns another request's reply: one whose receive
--- timed out or was cancelled leaves its reply owed, and the next call first receives and drops that reply (at most one
--- is owed, since every call drops it first; an "abandoned" in its place counts too), within its own timeout and before
--- it sends. A call that had to drop an owed reply first sends its request only if its timeout has time left: once a
--- positive timeout has run out, it returns nil, "timeout" without sending, so no server sees that request; with
--- NO_WAIT (0) no step waits, and it sends if there is room. After a call that gave up, a plain recv returns that
--- call's late reply (or "abandoned") first: replies come in request order. "invalid" while a request of send's (or
--- send_commit's) still has its reply to be received (that reply is the caller's: receive it first), and on any other
--- connection.
function Connection:call(data, a, b)
    local handle = conn_handle(self)
    local ptr, len, timeout = bytes(data, a, b)
    local milliseconds, start = millis(timeout), now()
    if not self._client then
        return nil, "invalid"
    end
    if len < 1 then
        return nil, "invalid"
    elseif len > tonumber(C.fipc_max_piece(handle)) then
        return nil, "too_large"
    end
    if self._cancelled then -- its reply could never be received: the request never goes
        return nil, "cancelled"
    end
    if self._sends_owed > 0 then
        return nil, "invalid"
    end
    local dropped = false
    while self._calls_owed > 0 do -- replies to calls that gave up on them: dropped
        local reply, err = receive(self, handle, remaining(milliseconds, start))
        if not reply and err ~= "abandoned" then
            return nil, err
        end
        dropped = true
    end
    -- Its deadline ran out while it dropped: the request never goes (a server that saw it would do its work for a
    -- call that has already returned "timeout")
    if dropped and milliseconds > 0 and (now() - start) * 1000 >= timeout then
        return nil, "timeout"
    end
    local result = C.fipc_send(handle, ptr, len, remaining(milliseconds, start))
    if result ~= 0 then
        return failed(self, result)
    end
    self._calls_owed = 1 -- its reply is owed until a receive answers it
    return receive(self, handle, remaining(milliseconds, start))
end

--- On a broadcast's publisher: the number of subscribers joined, once there are at least `at_least` (default 0: at
--- once), waiting up to `timeout` ms for them. Joined: admitted by the publisher's process, and not yet left, dropped
--- or unsubscribed; one that left is counted until the publisher's process has seen its end, soon after. Wait for the
--- subscribers before the first send: a send reaches those joined. At "timeout" and "cancelled" the count is the third
--- value; "invalid" on any other connection, or for `at_least` over the broadcast's max_subscribers.
function Connection:wait_for_members(at_least, timeout)
    local handle = conn_handle(self)
    at_least = subscriber_count(at_least or 0)
    local result = C.fipc_broadcast_wait_for_members(handle, at_least, self._count, millis(timeout))
    if result == 0 then
        return self._count[0]
    elseif result == INVALID then
        return nil, "invalid"
    end
    local _, name = failed(self, result)
    return nil, name, self._count[0]
end

--- The longest message the zero-copy calls take: the ring's capacity less 64 bytes; a broadcast's or a work queue's: a
--- quarter of its capacity, the longest message it takes; a service's: its max_message.
function Connection:max_piece()
    return tonumber(C.fipc_max_piece(conn_handle(self)))
end

--- Makes every call of the connection that waits, now or later, return nil, "cancelled"; calls that needn't wait
--- still work. Final. A service client's call, which waits for its reply, returns nil, "cancelled" at once from then
--- on, sending nothing.
function Connection:cancel()
    local handle = conn_handle(self)
    self._cancelled = true
    C.fipc_cancel(handle)
end

--- Whether a call has returned "disconnected": on a stream, the peer closed the connection or its process ended; on a
--- broadcast's subscriber, the publisher closed or its process ended; on a work queue's or a service's member, the
--- queue or the service has ended (its close, or the end of its owner's process: another member's close never ends
--- it). Final.
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

-- === Streams ===

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

--- The server: claims `name` (1-245 characters of [A-Za-z0-9_.-]) for a stream and listens on it, with rings of
--- `capacity` bytes each way (a power of two, 1 KiB to 2 GiB). Doesn't wait. Returns the listener, or nil and
--- "addr_in_use" if another stream, broadcast, queue or service holds the name ("invalid" for a bad name or
--- capacity).
function M.stream_create(name, capacity)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    local out = ffi.new("fipc_listener_t*[1]")
    local result = C.fipc_stream_create(name, length(capacity, "a capacity"), out)
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return setmetatable({ _handle = ffi.gc(out[0], C.fipc_listener_close), name = name }, Listener)
end

--- The client: connects to the server of the stream on `name` and sets the connection up, waiting up to `timeout` ms
--- for it to create the stream and to call accept: the server runs in another process (or a Lua state on another
--- thread). nil (fipc.FOREVER) waits for a server however late it starts. Returns the connection, or nil and the
--- error's name ("invalid" on a name a broadcast, a queue or a service holds).
function M.stream_connect(name, timeout)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    local out = ffi.new("fipc_conn_t*[1]")
    local result = C.fipc_stream_connect(name, out, millis(timeout))
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return new_connection(out[0])
end

--- Waits up to `timeout` ms for a client, sets its connection up and returns it. A call that times out in the middle of
--- a client's setup keeps it for the next call, so fipc.NO_WAIT polls (a client then takes one or two calls). One
--- client at a time: "invalid" while the connection it returned last is open.
function Listener:accept(timeout)
    local out = ffi.new("fipc_conn_t*[1]")
    local result = C.fipc_stream_accept(listener_handle(self), out, millis(timeout))
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return new_connection(out[0])
end

--- Makes every accept that waits, now or later, return nil, "cancelled"; a client whose setup one began is dropped.
--- Final.
function Listener:cancel()
    C.fipc_listener_cancel(listener_handle(self))
end

--- Stops listening and frees the listener and the name; the connections it accepted stay open. Closing it again does
--- nothing.
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

-- === Broadcasts ===

--- The publisher: claims `name` for a broadcast with `capacity` bytes of messages (a power of two, 4 KiB to 2 GiB; a
--- message is of one piece, up to a quarter of it) and room for `max_subscribers` subscribers (1 to 1024), and returns
--- the publisher, a connection whose sends go to every joined subscriber and the broadcast's one handle. Subscribers
--- may join from then on: the publisher's process admits them on a thread of the library's. A gated subscriber that
--- holds the publisher back for `max_stall` ms is unsubscribed and the publisher goes on; nil (fipc.FOREVER) never
--- unsubscribes, 0 refuses gated subscribers. Doesn't wait. Closing the publisher ends the broadcast and frees the
--- name: its subscribers get "disconnected" after its messages. Failures as stream_create's.
function M.broadcast_create(name, capacity, max_subscribers, max_stall)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    capacity = length(capacity, "a capacity")
    max_subscribers = subscriber_count(max_subscribers)
    local out = ffi.new("fipc_conn_t*[1]")
    local result = C.fipc_broadcast_create(name, capacity, max_subscribers, millis(max_stall), out)
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return new_connection(out[0])
end

--- A subscriber: joins the broadcast on `name` as `role`, fipc.SUBSCRIBER_GATED (misses nothing: the publisher waits
--- for it) or fipc.SUBSCRIBER_LOSSY (never holds the publisher back, receives copies only, and one that falls a lap
--- behind gets "overrun"), waiting up to `timeout` ms for the publisher to create the broadcast, to have a free slot
--- and to admit it; no call of the publisher's need wait for it, so the publisher's own Lua state may join. It
--- receives the messages sent after it joined, then "disconnected" once the publisher closed or its process ended.
--- Returns the connection, or nil and the error's name ("invalid" for another role, or a name a queue or a service
--- holds; on a name a stream holds, "invalid" once its server accepts the join, and "timeout" while none does).
function M.broadcast_join(name, role, timeout)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    if type(role) ~= "number" then
        error("fipc: a role is fipc.SUBSCRIBER_LOSSY or fipc.SUBSCRIBER_GATED, not " .. tostring(role), 2)
    end
    local out = ffi.new("fipc_conn_t*[1]")
    local result = C.fipc_broadcast_join(name, role, out, millis(timeout))
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return new_connection(out[0])
end

-- === Work queues ===

local Queue = {}
Queue.__index = Queue
M.Queue = Queue

local function queue_handle(self)
    local handle = self._handle
    if handle == nil then
        error("fipc: the queue is closed", 3)
    end
    return handle
end

--- The owner: claims `name` for a work queue of up to `max_producers` producers (1 to 63) and `max_consumers` consumers
--- (1 to 64), each producer with a ring of `capacity` bytes of messages (a power of two, 4 KiB to 2 GiB; a message is
--- of one piece, up to a quarter of it), and returns the queue. Producers and consumers may join from then on, the
--- owner's too (queue_join, from this Lua state as well): the owner's process admits them on a thread of the library's.
--- Doesn't wait. A queue lives until close() or the end of its process, and dropping the object doesn't end it: the
--- garbage collector never closes a queue. Failures as stream_create's ("invalid" also for a number of members out of
--- range).
function M.queue_create(name, capacity, max_producers, max_consumers)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    capacity = length(capacity, "a capacity")
    max_producers = member_count(max_producers, QUEUE_MEMBERS)
    max_consumers = member_count(max_consumers, QUEUE_MEMBERS)
    local out = ffi.new("fipc_queue_t*[1]")
    local result = C.fipc_queue_create(name, capacity, max_producers, max_consumers, out)
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return setmetatable({ _handle = out[0], _count = ffi.new("uint32_t[1]"), name = name }, Queue)
end

--- A producer or a consumer: joins the work queue on `name` as fipc.PRODUCER (sends) or fipc.CONSUMER (receives each
--- message no other consumer took, then "disconnected" once the queue has ended), waiting up to `timeout` ms for the
--- owner to create the queue, to have a free place of the role and to admit it; no call of the owner's need wait for
--- it, so the owner's own Lua state may join. Returns the connection, or nil and the error's name ("invalid" for
--- another role, or a name a broadcast or a service holds; on a name a stream holds, "invalid" once its server
--- accepts the join, and "timeout" while none does).
function M.queue_join(name, role, timeout)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    if type(role) ~= "number" then
        error("fipc: a role is fipc.PRODUCER or fipc.CONSUMER, not " .. tostring(role), 2)
    end
    local out = ffi.new("fipc_conn_t*[1]")
    local result = C.fipc_queue_join(name, role, out, millis(timeout))
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return new_connection(out[0])
end

--- The number of members of `role` joined, fipc.PRODUCER or fipc.CONSUMER (nil: both), once there are at least
--- `at_least` (default 0: at once), waiting up to `timeout` ms for them. Joined: admitted, and not yet left; one that
--- left is counted until the owner's process has seen its end, soon after. At "timeout", "cancelled" and
--- "disconnected" (the queue has ended) the count is the third value; "invalid" for another role, or for `at_least`
--- over the role's maximum (max_producers, max_consumers, or their sum for nil).
function Queue:wait_for_members(role, at_least, timeout)
    local handle = queue_handle(self)
    if role ~= nil and type(role) ~= "number" then
        error("fipc: a queue's role is fipc.PRODUCER, fipc.CONSUMER or nil, not " .. tostring(role), 2)
    end
    at_least = member_count(at_least or 0, QUEUE_MEMBERS)
    local result = C.fipc_queue_wait_for_members(handle, role or 0, at_least, self._count, millis(timeout))
    if result == 0 then
        return self._count[0]
    elseif result == INVALID then
        return nil, "invalid"
    end
    return nil, NAMES[result] or "unknown", self._count[0]
end

--- Makes every wait_for_members that waits, now or later, return nil, "cancelled" and the count. The queue goes on:
--- its members and their joins are unaffected. Final.
function Queue:cancel()
    C.fipc_queue_cancel(queue_handle(self))
end

--- Ends the queue and frees it and the name: producers' sends then return nil, "disconnected", and consumers receive
--- every message still in the queue, then "disconnected". The members' connections, the owner's own included, stay
--- open until their close(). A producer's or a consumer's close never ends the queue. Closing it again does nothing.
function Queue:close()
    local handle = self._handle
    if handle ~= nil then
        self._handle = nil
        C.fipc_queue_close(handle)
    end
end

--- Whether close() has been called.
function Queue:closed()
    return self._handle == nil
end

-- === Services ===

local Service = {}
Service.__index = Service
M.Service = Service

local function service_handle(self)
    local handle = self._handle
    if handle == nil then
        error("fipc: the service is closed", 3)
    end
    return handle
end

--- The owner: claims `name` for a service of up to `max_clients` clients (1 to 64) and `max_servers` servers (1 to
--- 64), each client with a ring of `capacity` bytes (a power of two, 4 KiB to 2 GiB) for its requests and their
--- replies, each of 1 to `max_message` bytes (1 to a quarter of the capacity), and returns the service. A request
--- holds its length rounded up to 64 bytes, plus max_message rounded up to 64, of its client's ring from its send
--- until its reply has been received. Clients and servers may join from then on, the owner's too (service_join, from
--- this Lua state as well): the owner's process admits them on a thread of the library's, and answers "abandoned" the
--- requests of a server that ends. Doesn't wait. A service lives until close() or the end of its process, and dropping
--- the object doesn't end it: the garbage collector never closes a service. Failures as stream_create's ("invalid"
--- also for a max_message or a number of members out of range).
function M.service_create(name, capacity, max_message, max_clients, max_servers)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    capacity, max_message = length(capacity, "a capacity"), length(max_message, "a max_message")
    max_clients, max_servers = member_count(max_clients, SERVICE_MEMBERS), member_count(max_servers, SERVICE_MEMBERS)
    local out = ffi.new("fipc_service_t*[1]")
    local result = C.fipc_service_create(name, capacity, max_message, max_clients, max_servers, out)
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return setmetatable({ _handle = out[0], _count = ffi.new("uint32_t[1]"), name = name }, Service)
end

--- A client or a server: joins the service on `name` as fipc.CLIENT or fipc.SERVER, waiting up to `timeout` ms for
--- the owner to create the service, to have a free place of the role and to admit it; no call of the owner's need
--- wait for it, so the owner's own Lua state may join. Returns the connection, or nil and the error's name ("invalid"
--- for another role, or a name a broadcast or a queue holds; on a name a stream holds, "invalid" once its server
--- accepts the join, and "timeout" while none does).
--- A client's send (or send_acquire and send_commit) sends a request, and its receive returns the reply to its oldest
--- request whose reply it hasn't received yet (the n-th reply answers the n-th request), or nil, "abandoned"; call
--- does both. A server's receive takes the next request of any client and holds it ("invalid" while it holds one),
--- and its send replies to it, never waiting ("invalid" with none held); its close abandons the request it holds. A
--- server never closed holds its request, and its client waits for the reply, until the garbage collector frees the
--- connection or the server's process exits.
--- Once the service has ended, clients' sends and servers' receives return "disconnected", and a client receives the
--- replies that came before the end, then "disconnected".
function M.service_join(name, role, timeout)
    if type(name) ~= "string" then
        error("fipc: a name is a string, not " .. type(name), 2)
    end
    if type(role) ~= "number" then
        error("fipc: a role is fipc.CLIENT or fipc.SERVER, not " .. tostring(role), 2)
    end
    local out = ffi.new("fipc_conn_t*[1]")
    local result = C.fipc_service_join(name, role, out, millis(timeout))
    if result ~= 0 then
        return nil, NAMES[result] or "unknown"
    end
    return new_connection(out[0], role == M.CLIENT)
end

--- The number of members of `role` joined, fipc.CLIENT or fipc.SERVER (nil: both), once there are at least
--- `at_least` (default 0: at once), waiting up to `timeout` ms for them. Joined: admitted, and not yet left; one that
--- left is counted until the owner's process has seen its end, soon after. At "timeout", "cancelled" and
--- "disconnected" (the service has ended) the count is the third value; "invalid" for another role, or for
--- `at_least` over the role's maximum (max_clients, max_servers, or their sum for nil).
function Service:wait_for_members(role, at_least, timeout)
    local handle = service_handle(self)
    if role ~= nil and type(role) ~= "number" then
        error("fipc: a service's role is fipc.CLIENT, fipc.SERVER or nil, not " .. tostring(role), 2)
    end
    at_least = member_count(at_least or 0, SERVICE_MEMBERS)
    local result = C.fipc_service_wait_for_members(handle, role or 0, at_least, self._count, millis(timeout))
    if result == 0 then
        return self._count[0]
    elseif result == INVALID then
        return nil, "invalid"
    end
    return nil, NAMES[result] or "unknown", self._count[0]
end

--- Makes every wait_for_members that waits, now or later, return nil, "cancelled" and the count. The service goes on:
--- its members and their joins are unaffected. Final.
function Service:cancel()
    C.fipc_service_cancel(service_handle(self))
end

--- Ends the service and frees it and the name: clients' sends and servers' receives then return nil, "disconnected";
--- a client receives the replies that came before the end, then "disconnected", and a server's reply returns
--- "disconnected". The members' connections, the owner's own included, stay open until their close(). A client's or a
--- server's close never ends the service. Closing it again does nothing.
function Service:close()
    local handle = self._handle
    if handle ~= nil then
        self._handle = nil
        C.fipc_service_close(handle)
    end
end

--- Whether close() has been called.
function Service:closed()
    return self._handle == nil
end

return M
