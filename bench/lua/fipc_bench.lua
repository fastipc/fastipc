--[[
FastIPC's Lua benchmark, through the binding (bindings/lua, fipc) on LuaJIT: one-way throughput between this
process, a server that receives and times, and a client process (this script again) that sends. The same cases,
output and checks as the C, C++, Python, C#, Java, Rust, JavaScript and Go benchmarks.

  luajit bench/lua/fipc_bench.lua copy|zerocopy|rpc

  copy      conn:send(string) / conn:recv_into a buffer the server reuses
  zerocopy  send_acquire + ffi.copy + send_commit / recv_acquire, copied out, + recv_release (a message of several
            pieces through the copying calls)
  rpc       rpc_submit / rpc_recv_into a buffer the server reuses

The JIT compiles the code only after it has run a while, and a fresh process starts slowly, so each case first sends
messages untimed for half a second (the warm-up; at least its count), then a start marker, then its count, and the
server times from the last start marker to the end marker (see client). Each case prints "Test i/n: name" and "Throughput: n messages/sec" (devtool bench-compare reads them).
]]

local here = arg[0]:match("^(.*)[/\\]") or "."
package.path = here .. "/../../bindings/lua/?.lua;" .. here .. "/../../bindings/lua/tests/?.lua;" .. package.path
local ffi = require("ffi")
local fipc = require("fipc")
local support = require("support") -- the client process and the clock

local CONNECT_MS = 15000
local DATA_OPCODE = 1
local END_OPCODE = 0xFB000001
local GO_OPCODE = 0xFB000002
local WARM_UP = 0.5 -- seconds
local MARKER_EVERY = 1000

local CASES = {
    { count = 2000000, ring = 512 * 1024, size = 16, name = "Tiny messages (16B)" },
    { count = 2000000, ring = 512 * 1024, size = 64, name = "Small messages (64B)" },
    { count = 1000000, ring = 512 * 1024, size = 256, name = "Medium messages (256B)" },
    { count = 1000, ring = 512 * 1024, size = 64 * 1024, name = "Large messages (64KB)" },
    { count = 1000, ring = 2 * 1024 * 1024, size = 512 * 1024, name = "Large messages (512KB)" },
    { count = 10, ring = 512 * 1024, size = 1024 * 1024, name = "Exceeds buffer (1MB msg, 512KB buffer)" },
}

-- "end" or "go" for a marker, else nil
local function marker(ptr, len)
    if len ~= 3 then
        return nil
    end
    local text = ffi.string(ptr, 3)
    return (text == "END" and "end") or (text == "GO!" and "go") or nil
end

-- The server: skips the warm-up up to the last start marker, then receives until the end marker; the messages,
-- their bytes and the seconds (or nil and the error)
local function serve(mode, conn, size)
    local cap = math.max(size, 3)
    local buf = ffi.new("uint8_t[?]", cap)
    local sink = ffi.new("uint8_t[?]", cap)
    local timing, messages, bytes, start = false, 0, 0, 0
    while true do
        local len, err, mark
        if mode == "rpc" then
            local header
            header, err = conn:rpc_recv_into(buf, cap)
            if header then
                mark = (header.opcode == END_OPCODE and "end") or (header.opcode == GO_OPCODE and "go") or nil
                len = header.len
            end
        elseif mode == "zerocopy" then
            local data
            data, len = conn:recv_acquire()
            if data then
                mark = marker(data, len)
                if not mark then
                    ffi.copy(sink, data, len) -- a consumer copies the message out
                end
                conn:recv_release()
            elseif len == fipc.TOO_LARGE then -- several pieces
                len, err = conn:recv_into(buf, cap)
            else
                err = len
                len = nil
            end
        else
            len, err = conn:recv_into(buf, cap)
            if len then
                mark = marker(buf, len)
            end
        end
        if not len then
            return nil, err
        end
        if mark == "end" then
            break
        elseif mark == "go" then -- the last one starts the timed messages
            timing, messages, bytes, start = true, 0, 0, support.now()
        elseif timing then
            messages = messages + 1
            bytes = bytes + len
        end
    end
    return messages, bytes, support.now() - start
end

-- A start or end marker: an empty RPC of its opcode, else its 3 bytes through the copying send
local function send_marker(mode, conn, text, opcode)
    if mode == "rpc" then
        assert(conn:rpc_submit(opcode, nil))
    else
        assert(conn:send(text))
    end
end

-- One message of `size` bytes
local function send_one(mode, conn, payload, size, one_piece)
    if mode == "rpc" then
        assert(conn:rpc_submit(DATA_OPCODE, payload))
    elseif mode == "zerocopy" and one_piece then
        local slot = assert(conn:send_acquire(size))
        ffi.copy(slot, payload, size)
        assert(conn:send_commit(size))
    else
        assert(conn:send(payload))
    end
end

-- The client process: connects; sends messages untimed for the warm-up (the count, and for at least WARM_UP), with a
-- start marker every MARKER_EVERY messages, so that the real one takes no path the JIT hasn't seen; then the start
-- marker, `count` messages of `size` bytes and the end marker; and closes (the server still receives everything
-- sent before)
local function client(mode, name, size, count)
    local conn = assert(fipc.connect(name, CONNECT_MS))
    local payload = string.rep("x", size)
    local one_piece = size <= conn:max_piece()
    local warm = support.now() + WARM_UP
    local i = 0
    while i < count or support.now() < warm do
        if i % MARKER_EVERY == 0 then
            send_marker(mode, conn, "GO!", GO_OPCODE)
        end
        send_one(mode, conn, payload, size, one_piece)
        i = i + 1
    end
    send_marker(mode, conn, "GO!", GO_OPCODE)
    for _ = 1, count do
        send_one(mode, conn, payload, size, one_piece)
    end
    send_marker(mode, conn, "END", END_OPCODE)
    conn:close()
end

local function run_case(mode, case, index)
    local name = string.format("luabench_%d_%d", support.pid(), index)
    local listener, err = fipc.listen(name, case.ring)
    if not listener then
        io.stderr:write("listen: ", err, "\n")
        return false
    end
    local process = support.spawn({ support.luajit, arg[0], "client", mode, name, tostring(case.size),
        tostring(case.count) }, { stdout = support.windows and "NUL" or "/dev/null" })
    local conn
    conn, err = listener:accept(CONNECT_MS)
    local messages, bytes, seconds
    if conn then
        messages, bytes, seconds = serve(mode, conn, case.size)
        if not messages then
            err = bytes
        end
        conn:close()
    end
    listener:close()
    if not messages then
        io.stderr:write("server: ", tostring(err), "\n")
        process:kill()
        return false
    end
    local code = process:wait(600000)
    if code ~= 0 then
        io.stderr:write("the client failed: exit code ", tostring(code), "\n")
        return false
    end
    if messages ~= case.count or bytes ~= case.count * case.size then
        io.stderr:write(string.format("expected %d messages of %d B, got %d (%d B)\n", case.count, case.size,
            messages, bytes))
        return false
    end
    print(string.format("Messages: %d | Ring: %dKB | Size: %dB", case.count, case.ring / 1024, case.size))
    print(string.format("Duration: %.3fs", seconds))
    print(string.format("Throughput: %.0f messages/sec, %.1f MB/sec", messages / seconds,
        bytes / seconds / (1024 * 1024)))
    return true
end

local mode = arg[1] or "copy"
if mode == "client" then
    client(arg[2], arg[3], tonumber(arg[4]), tonumber(arg[5]))
    os.exit(0)
end
if mode ~= "copy" and mode ~= "zerocopy" and mode ~= "rpc" then
    io.stderr:write("usage: luajit fipc_bench.lua copy|zerocopy|rpc\n")
    os.exit(2)
end
print("FastIPC Lua benchmark: " .. mode .. "\n")
local passed = 0
for i, case in ipairs(CASES) do
    print(string.format("Test %d/%d: %s", i, #CASES, case.name))
    io.stdout:flush()
    if run_case(mode, case, i) then
        passed = passed + 1
    else
        print("FAILED")
    end
    print()
    io.stdout:flush()
end
print(string.format("Summary: %d/%d tests passed", passed, #CASES))
os.exit(passed == #CASES and 0 or 1)
