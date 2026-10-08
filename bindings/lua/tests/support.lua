--[[
What the tests share: the binding (this checkout's bindings/lua, against its zig-out build), unique names, test data,
other processes (this interpreter running tests/peer.lua, an example, or a Python peer), sleeping, and a small test
suite: each test file returns a suite, and tests/run.lua runs them.
]]

local ffi = require("ffi")

local support = {}

-- The folder of this file, the binding's and the repository's
local tests = (debug.getinfo(1, "S").source:sub(2):match("^(.*)[/\\]") or ".")
support.tests = tests
support.binding = tests .. "/.."
support.repository = tests .. "/../../.."
support.windows = ffi.os == "Windows"
support.macos = ffi.os == "OSX"

package.path = support.binding .. "/?.lua;" .. package.path
local fipc = require("fipc")

-- The interpreter running the tests: the other Lua processes run with it too
local function interpreter()
    local i = 0
    while arg and arg[i - 1] do
        i = i - 1
    end
    return arg and arg[i] or "luajit"
end
support.luajit = interpreter()

-- === Names and data ===

local counter = 0

--- A name no other test (or run) uses.
function support.unique_name(prefix)
    counter = counter + 1
    return string.format("fipc_lua_%s_%d_%d_%d", prefix, support.pid(), counter, math.floor(support.now() * 1000) % 1000000)
end

--- `len` bytes of a pattern that differs at every offset of a ring's frame.
function support.pattern(len)
    local buf = ffi.new("uint8_t[?]", math.max(len, 1))
    for i = 0, len - 1 do
        buf[i] = (i * 31 + 7) % 256
    end
    return ffi.string(buf, len)
end

-- === The platform: time, sleeping, processes ===

if support.windows then
    pcall(ffi.cdef, [[
        typedef struct { uint32_t nLength; void* lpSecurityDescriptor; int bInheritHandle; } FIPC_SECURITY_ATTRIBUTES;
        typedef struct {
            uint32_t cb; char* lpReserved; char* lpDesktop; char* lpTitle;
            uint32_t dwX, dwY, dwXSize, dwYSize, dwXCountChars, dwYCountChars, dwFillAttribute, dwFlags;
            uint16_t wShowWindow, cbReserved2; uint8_t* lpReserved2;
            void* hStdInput; void* hStdOutput; void* hStdError;
        } FIPC_STARTUPINFOA;
        typedef struct { void* hProcess; void* hThread; uint32_t dwProcessId, dwThreadId; } FIPC_PROCESS_INFORMATION;
        int CreateProcessA(const char* app, char* line, FIPC_SECURITY_ATTRIBUTES* pa, FIPC_SECURITY_ATTRIBUTES* ta,
                           int inherit, uint32_t flags, void* env, const char* cwd, FIPC_STARTUPINFOA* startup,
                           FIPC_PROCESS_INFORMATION* info);
        void* CreateFileA(const char* name, uint32_t access, uint32_t share, FIPC_SECURITY_ATTRIBUTES* sa,
                          uint32_t disposition, uint32_t flags, void* template);
        void* GetStdHandle(uint32_t which);
        uint32_t WaitForSingleObject(void* handle, uint32_t ms);
        int GetExitCodeProcess(void* process, uint32_t* code);
        int TerminateProcess(void* process, uint32_t code);
        int CloseHandle(void* handle);
        void Sleep(uint32_t ms);
        uint32_t GetCurrentProcessId(void);
        int QueryPerformanceCounter(int64_t* count);
        int QueryPerformanceFrequency(int64_t* frequency);
    ]])
else
    pcall(ffi.cdef, [[
        // glibc's posix_spawn_file_actions_t; macOS's is one pointer, which this holds as well
        typedef struct { int allocated; int used; void* actions; int pad[16]; } fipc_spawn_actions_t;
        int posix_spawn(int* pid, const char* path, const fipc_spawn_actions_t* actions, const void* attr,
                        char* const argv[], char* const envp[]);
        int posix_spawn_file_actions_init(fipc_spawn_actions_t* actions);
        int posix_spawn_file_actions_addopen(fipc_spawn_actions_t* actions, int fd, const char* path, int flags,
                                             unsigned int mode);
        int posix_spawn_file_actions_destroy(fipc_spawn_actions_t* actions);
        int waitpid(int pid, int* status, int options);
        int kill(int pid, int sig);
        int getpid(void);
        int poll(void* fds, unsigned long n, int timeout);
        struct fipc_timespec { long tv_sec; long tv_nsec; };
        int clock_gettime(int clock, struct fipc_timespec* now);
        extern char** environ;
    ]])
end

local C = ffi.C

--- Seconds on a monotonic clock.
function support.now()
    if support.windows then
        local count, frequency = ffi.new("int64_t[1]"), ffi.new("int64_t[1]")
        C.QueryPerformanceCounter(count)
        C.QueryPerformanceFrequency(frequency)
        return tonumber(count[0]) / tonumber(frequency[0])
    end
    local now = ffi.new("struct fipc_timespec")
    C.clock_gettime(support.macos and 6 or 1, now) -- CLOCK_MONOTONIC
    return tonumber(now.tv_sec) + tonumber(now.tv_nsec) / 1e9
end

function support.sleep(ms)
    if support.windows then
        C.Sleep(ms)
    else
        C.poll(nil, 0, ms)
    end
end

function support.pid()
    return support.windows and C.GetCurrentProcessId() or C.getpid()
end

-- A Windows command line that hands each argument over unchanged (the C runtime's parsing rules)
local function quote(argument)
    if argument ~= "" and not argument:find('[%s"]') then
        return argument
    end
    local quoted = argument:gsub('(\\*)"', '%1%1\\"'):gsub("(\\+)$", "%1%1")
    return '"' .. quoted .. '"'
end

local Process = {}
Process.__index = Process

--[[ Starts a process: `argv` is the program and its arguments; options.stdout, a file its standard output goes to
(else this process's). ]]
function support.spawn(argv, options)
    options = options or {}
    if support.windows then
        local parts = { quote((argv[1]:gsub("/", "\\"))) } -- the program: a path Windows searches
        for i = 2, #argv do
            parts[i] = quote(argv[i])
        end
        local line = table.concat(parts, " ")
        local sa = ffi.new("FIPC_SECURITY_ATTRIBUTES", { nLength = ffi.sizeof("FIPC_SECURITY_ATTRIBUTES"), bInheritHandle = 1 })
        local out = C.GetStdHandle(-11 % 2 ^ 32) -- STD_OUTPUT_HANDLE
        local file = nil
        if options.stdout then
            file = C.CreateFileA(options.stdout, 0x40000000, 3, sa, 2, 0x80, nil) -- GENERIC_WRITE, CREATE_ALWAYS
            assert(file ~= ffi.cast("void*", -1), "can't create " .. options.stdout)
            out = file
        end
        local startup = ffi.new("FIPC_STARTUPINFOA")
        startup.cb = ffi.sizeof(startup)
        startup.dwFlags = 0x100 -- STARTF_USESTDHANDLES
        startup.hStdInput = C.GetStdHandle(-10 % 2 ^ 32)
        startup.hStdOutput = out
        startup.hStdError = C.GetStdHandle(-12 % 2 ^ 32)
        local info = ffi.new("FIPC_PROCESS_INFORMATION")
        local buffer = ffi.new("char[?]", #line + 1, line)
        local ok = C.CreateProcessA(nil, buffer, nil, nil, 1, 0, nil, nil, startup, info)
        if file then
            C.CloseHandle(file)
        end
        assert(ok ~= 0, "can't start " .. line)
        C.CloseHandle(info.hThread)
        return setmetatable({ handle = info.hProcess, argv = argv }, Process)
    end
    local args = ffi.new("char*[?]", #argv + 1)
    local keep = {}
    for i, a in ipairs(argv) do
        keep[i] = ffi.new("char[?]", #a + 1, a)
        args[i - 1] = keep[i]
    end
    local actions = ffi.new("fipc_spawn_actions_t")
    C.posix_spawn_file_actions_init(actions)
    if options.stdout then
        local flags = support.macos and 0x601 or 0x241 -- O_WRONLY|O_CREAT|O_TRUNC
        C.posix_spawn_file_actions_addopen(actions, 1, options.stdout, flags, 420) -- 0644
    end
    local pid = ffi.new("int[1]")
    local program = argv[1]
    if not program:find("/") then -- a bare name: the PATH's
        program = assert(io.popen("command -v " .. program)):read("*l") or program
    end
    local failed = C.posix_spawn(pid, program, actions, nil, args, C.environ)
    C.posix_spawn_file_actions_destroy(actions)
    assert(failed == 0, "can't start " .. table.concat(argv, " "))
    return setmetatable({ pid = pid[0], argv = argv }, Process)
end

--- Waits up to `ms` (default 30 s) for the process to exit: its exit code, or nil if it hasn't (it is then killed).
function Process:wait(ms)
    ms = ms or 30000
    if self.code then
        return self.code
    end
    if support.windows then
        if C.WaitForSingleObject(self.handle, ms) ~= 0 then
            self:kill()
            return nil
        end
        local code = ffi.new("uint32_t[1]")
        C.GetExitCodeProcess(self.handle, code)
        C.CloseHandle(self.handle)
        self.code = tonumber(code[0])
        return self.code
    end
    local status = ffi.new("int[1]")
    local deadline = support.now() + ms / 1000
    while true do
        local got = C.waitpid(self.pid, status, 1) -- WNOHANG
        if got == self.pid then
            local s = status[0]
            self.code = (s % 128 == 0) and math.floor(s / 256) % 256 or 128 + s % 128
            return self.code
        end
        if support.now() > deadline then
            self:kill()
            return nil
        end
        support.sleep(5)
    end
end

--- Kills the process and waits for it.
function Process:kill()
    if self.code then
        return
    end
    if support.windows then
        C.TerminateProcess(self.handle, 137)
        C.WaitForSingleObject(self.handle, 0xFFFFFFFF)
        C.CloseHandle(self.handle)
        self.code = 137
    else
        C.kill(self.pid, 9)
        C.waitpid(self.pid, ffi.new("int[1]"), 0)
        self.code = 137
    end
end

--- A Lua peer: this interpreter running tests/peer.lua with the role and its arguments.
function support.peer(role, ...)
    return support.spawn({ support.luajit, tests .. "/peer.lua", role, ... })
end

--- A temporary file's path (removed by the caller).
function support.temp_file(suffix)
    local name = os.tmpname()
    if support.windows then -- os.tmpname gives a name in the root of the drive on Windows; use TEMP
        os.remove(name)
        name = (os.getenv("TEMP") or ".") .. "\\" .. support.unique_name("out") .. (suffix or "")
    end
    return name
end

function support.read_file(path)
    local file = assert(io.open(path, "rb"))
    local text = file:read("*a")
    file:close()
    return text
end

-- === Checks ===

--- Fails unless `a == b`, saying what each was.
function support.eq(a, b, what)
    if a ~= b then
        local function show(v)
            if type(v) == "string" and #v > 60 then
                return string.format("a string of %d bytes", #v)
            end
            return string.format("%q", tostring(v))
        end
        error((what and what .. ": " or "") .. "expected " .. show(b) .. ", got " .. show(a), 2)
    end
end

--- Fails unless the call returned nil and the error `name` (and, if given, `third`).
function support.fails(name, third, ...)
    local value, err, extra = ...
    if value ~= nil or err ~= name then
        error(string.format("expected nil, %q; got %s, %s", name, tostring(value), tostring(err)), 2)
    end
    if third ~= nil and extra ~= third then
        error(string.format("expected %s third, got %s", tostring(third), tostring(extra)), 2)
    end
end

--- Fails unless `fn` raises an error whose message contains `text`.
function support.raises(text, fn, ...)
    local ok, err = pcall(fn, ...)
    if ok or not tostring(err):find(text, 1, true) then
        error("expected an error with '" .. text .. "', got " .. (ok and "none" or tostring(err)), 2)
    end
end

--- A suite of tests in order: suite.test(name, fn); tests/run.lua runs it.
function support.suite(name)
    local suite = { name = name, tests = {} }
    function suite.test(test_name, fn)
        suite.tests[#suite.tests + 1] = { name = test_name, fn = fn }
    end
    return suite
end

support.fipc = fipc
support.RING = 64 * 1024
support.TEN_SECONDS = 10000

return support
