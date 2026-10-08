-- Cross-language: this binding against the Python binding (bindings/python, fipc) in another process, both
-- ways, with plain messages and RPC. The interpreter is FIPC_TEST_PYTHON, else the repository's venv, else python;
-- it needs cffi. Without one the tests say so and pass, unless FIPC_REQUIRE_INTEROP is set.

local support = require("support")
local fipc = support.fipc
local eq, fails = support.eq, support.fails

local suite = support.suite("interop")
local test = suite.test

-- The Python peer: peer.py client|server <name> <bindings/python>
local PYTHON_PEER = [[
import sys
sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import Conn, FipcError, Listener, Result, RPC_REQUEST

mode, name = sys.argv[1], sys.argv[2]
if mode == "client":
    # A plain connection: echo each message reversed until the server's end
    with Conn.connect(name, timeout_ms=20000) as conn:
        try:
            while True:
                conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
    # An RPC connection: call the Lua server, then answer its call
    with Conn.connect(name, timeout_ms=20000) as conn:
        request_id = conn.rpc_submit(7, "hello from Python".encode(), timeout_ms=20000)
        reply = conn.rpc_recv(timeout_ms=20000)
        assert (reply.id, reply.status, reply.payload) == (request_id, 42, b"HELLO FROM PYTHON"), reply
        request = conn.rpc_recv(timeout_ms=20000)
        assert request.kind == RPC_REQUEST and request.opcode == 9, request
        conn.rpc_respond(request.id, 9, status=len(request.payload), data=request.payload * 2, timeout_ms=20000)
        try:
            conn.rpc_recv(timeout_ms=20000)
            raise AssertionError("expected the server's end")
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
else:
    with Listener(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
        while True:
            try:
                request = conn.rpc_recv(timeout_ms=20000)
            except FipcError as e:
                if e.result == Result.DISCONNECTED:
                    break
                raise
            conn.rpc_respond(request.id, request.opcode, status=-1, data=request.payload.upper(), timeout_ms=20000)
]]

-- A Python interpreter with cffi, or nil (the test is then skipped, unless FIPC_REQUIRE_INTEROP is set)
local python_found
local function python()
    if python_found ~= nil then
        return python_found or nil
    end
    local venv = support.repository .. (support.windows and "/venv/Scripts/python.exe" or "/venv/bin/python")
    local candidates = { os.getenv("FIPC_TEST_PYTHON"), venv, support.windows and "python" or "python3" }
    for i = 1, 3 do
        local candidate = candidates[i]
        if candidate and candidate ~= "" then
            local started, probe = pcall(support.spawn, { candidate, "-c", "import cffi" })
            if started and probe:wait() == 0 then
                python_found = candidate
                return candidate
            end
        end
    end
    assert(not os.getenv("FIPC_REQUIRE_INTEROP"), "no Python with cffi (set FIPC_TEST_PYTHON)")
    print("      interop: skipped, no Python with cffi (set FIPC_TEST_PYTHON)")
    python_found = false
    return nil
end

local function start_python(interpreter, mode, name)
    local script = support.temp_file(".py")
    local file = assert(io.open(script, "wb"))
    file:write(PYTHON_PEER)
    file:close()
    return support.spawn({ interpreter, script, mode, name, support.repository .. "/bindings/python" }), script
end

test("a Lua server and a Python client: plain messages, then RPC both ways", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_client")
    local listener = assert(fipc.listen(name, 64 * 1024))
    local peer, script = start_python(interpreter, "client", name)

    local conn = assert(listener:accept(30000))
    for _, size in ipairs({ 1, 100, 70000, 200000 }) do
        local message = support.pattern(size)
        assert(conn:send(message, support.TEN_SECONDS)) -- longer than the ring: Python receives it as it comes
        eq(conn:recv(support.TEN_SECONDS), message:reverse(), "size " .. size)
    end
    conn:close()

    conn = assert(listener:accept(30000))
    local request = assert(conn:rpc_recv(support.TEN_SECONDS))
    eq(request.kind, fipc.RPC_REQUEST)
    eq(request.opcode, 7)
    eq(request.payload, "hello from Python")
    assert(conn:rpc_respond(request.id, 7, 42, "HELLO FROM PYTHON", support.TEN_SECONDS))

    local id = assert(conn:rpc_submit(9, "Lua", support.TEN_SECONDS))
    local reply = assert(conn:rpc_recv(support.TEN_SECONDS))
    eq(reply.kind, fipc.RPC_RESPONSE)
    eq(reply.id, id)
    eq(reply.status, 3)
    eq(reply.payload, "LuaLua")
    conn:close()
    listener:close()

    eq(peer:wait(), 0, "the Python peer's exit code")
    os.remove(script)
end)

test("a Python server and a Lua client over RPC; the client's end ends the server", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_server")
    local peer, script = start_python(interpreter, "server", name)

    local conn = assert(fipc.connect(name, 30000))
    local first = assert(conn:rpc_submit(1, "shared memory", support.TEN_SECONDS))
    local big = support.pattern(300000)
    local second = assert(conn:rpc_submit(2, big, support.TEN_SECONDS))
    local one = assert(conn:rpc_recv(support.TEN_SECONDS))
    eq(one.id, first)
    eq(one.status, -1)
    eq(one.payload, "SHARED MEMORY")
    local two = assert(conn:rpc_recv(support.TEN_SECONDS))
    eq(two.id, second)
    eq(two.payload, big:upper())
    fails("timeout", nil, conn:rpc_recv(fipc.NO_WAIT))
    conn:close()

    eq(peer:wait(), 0, "the Python peer's exit code")
    os.remove(script)
end)

return suite
