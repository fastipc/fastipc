-- Cross-language: this binding against the Python binding (bindings/python, fipc) in another process, both ways, on
-- a stream, on a broadcast, on a work queue and on a service. The interpreter is FIPC_TEST_PYTHON, else the
-- repository's venv, else python; it needs cffi. Without one the tests say so and pass, unless FIPC_REQUIRE_INTEROP is
-- set.

local support = require("support")
local broadcast = require("broadcast")
local queue = require("queue")
local service = require("service")
local fipc = support.fipc
local eq, fails = support.eq, support.fails

local suite = support.suite("interop")
local test = suite.test

-- The Python peer: peer.py client|server <name> <bindings/python>
local PYTHON_PEER = [[
import sys
sys.path.insert(0, sys.argv[3])  # the repository's bindings/python
from fipc import FipcError, Result, stream_connect, stream_create

mode, name = sys.argv[1], sys.argv[2]
if mode == "client":
    # A stream's client: echo each message reversed until the server's end
    with stream_connect(name, timeout_ms=20000) as conn:
        try:
            while True:
                conn.send(conn.recv(timeout_ms=20000)[::-1], timeout_ms=20000)
        except FipcError as e:
            if e.result != Result.DISCONNECTED:
                raise
else:
    # Answer each message in upper case until the client's end
    with stream_create(name, 1 << 16) as listener, listener.accept(timeout_ms=20000) as conn:
        while True:
            try:
                message = conn.recv(timeout_ms=20000)
            except FipcError as e:
                if e.result == Result.DISCONNECTED:
                    break
                raise
            conn.send(message.upper(), timeout_ms=20000)
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

test("a Lua server and a Python client: plain messages; the server's end ends the client", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_client")
    local listener = assert(fipc.stream_create(name, 64 * 1024))
    local peer, script = start_python(interpreter, "client", name)

    local conn = assert(listener:accept(30000))
    for _, size in ipairs({ 1, 100, 70000, 200000 }) do
        local message = support.pattern(size)
        assert(conn:send(message, support.TEN_SECONDS)) -- longer than the ring: Python receives it as it comes
        eq(conn:recv(support.TEN_SECONDS), message:reverse(), "size " .. size)
    end
    conn:close()
    listener:close()

    eq(peer:wait(), 0, "the Python peer's exit code")
    os.remove(script)
end)

test("a Python server and a Lua client; the client's end ends the server", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_server")
    local peer, script = start_python(interpreter, "server", name)

    local conn = assert(fipc.stream_connect(name, 30000))
    assert(conn:send("shared memory", support.TEN_SECONDS))
    local big = support.pattern(300000)
    assert(conn:send(big, support.TEN_SECONDS)) -- longer than the ring: the server receives it as it comes
    eq(conn:recv(support.TEN_SECONDS), "SHARED MEMORY")
    eq(conn:recv(support.TEN_SECONDS), big:upper())
    fails("timeout", nil, conn:recv(fipc.NO_WAIT))
    conn:close()

    eq(peer:wait(), 0, "the Python peer's exit code")
    os.remove(script)
end)

-- The Python binding's broadcast peer (bindings/python/tests/broadcast_peer.py): the same broadcast, messages and
-- checks as tests/broadcast.lua
local BROADCAST_PEER = support.repository .. "/bindings/python/tests/broadcast_peer.py"

test("a broadcast: a Lua publisher, a gated and a lossy Python subscriber", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_bc_sub")
    local peers = {}
    for i, role in ipairs({ "gated", "lossy" }) do
        peers[i] = support.spawn({ interpreter, BROADCAST_PEER, "subscriber", name, role, "300" })
    end
    broadcast.publish(name, 300, 2)
    for _, peer in ipairs(peers) do
        eq(peer:wait(), 0, "a Python subscriber's exit code")
    end
end)

test("a broadcast: a Python publisher, a gated and a lossy Lua subscriber", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_bc_pub")
    local peer = support.spawn({ interpreter, BROADCAST_PEER, "publisher", name, "300", "2" })
    broadcast.subscribe(name, { "gated", "lossy" }, 300)
    eq(peer:wait(), 0, "the Python publisher's exit code")
end)

-- The Python binding's queue peer (bindings/python/tests/queue_peer.py): the same queue and jobs as tests/queue.lua
local QUEUE_PEER = support.repository .. "/bindings/python/tests/queue_peer.py"

test("a work queue: a Python owner's producer, two Lua consumers", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_q_prod")
    local peer = support.spawn({ interpreter, QUEUE_PEER, "producer", name, "100", "2" })
    local consumers = {}
    for c = 1, 2 do
        consumers[c] = assert(fipc.queue_join(name, fipc.CONSUMER, queue.TIMEOUT))
    end
    local lists = queue.receive_until_the_end(consumers)
    for c, list in ipairs(lists) do
        for k, message in ipairs(list) do
            list[k] = assert(queue.job_number(message), "consumer " .. c .. " took " .. message)
        end
    end
    queue.check_split(lists, 100)
    eq(peer:wait(), 0, "the Python producer's exit code")
end)

test("a work queue: a Lua owner's producer, a Python consumer", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_q_cons")
    local peer = support.spawn({ interpreter, QUEUE_PEER, "consumer", name, "100" })
    queue.produce(name, 100, 1)
    eq(peer:wait(), 0, "the Python consumer's exit code")
end)

test("a work queue: two joined Python producers, a Lua owner's consumer", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_q_jprod")
    local peers = {}
    for i = 1, 2 do
        peers[i] = support.spawn({ interpreter, QUEUE_PEER, "joined-producer", name, "100" })
    end
    queue.own_and_consume(name, 100, 2)
    for _, peer in ipairs(peers) do
        eq(peer:wait(), 0, "a Python producer's exit code")
    end
end)

test("a work queue: a Python owner's consumer, two joined Lua producers", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_q_owncons")
    local peer = support.spawn({ interpreter, QUEUE_PEER, "owner-consumer", name, "100", "2" })
    local producers = {}
    for p = 1, 2 do
        producers[p] = assert(fipc.queue_join(name, fipc.PRODUCER, queue.TIMEOUT))
    end
    for i = 1, 100 do -- interleaved: job i of each producer in turn
        for _, producer in ipairs(producers) do
            assert(producer:send(queue.job(i), queue.TIMEOUT))
        end
    end
    for _, producer in ipairs(producers) do
        producer:close()
    end
    eq(peer:wait(), 0, "the Python owner's exit code")
end)

-- The Python binding's service peer (bindings/python/tests/service_peer.py): the same service and requests as
-- tests/service.lua
local SERVICE_PEER = support.repository .. "/bindings/python/tests/service_peer.py"

test("a service: a Python owner's server, a Lua client", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_s_srv")
    local peer = support.spawn({ interpreter, SERVICE_PEER, "server", name, "300", "1" })
    service.join_and_call(name, 300)
    eq(peer:wait(), 0, "the Python server's exit code")
end)

test("a service: a Lua owner's server, two Python clients", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_s_cli")
    local peers = {}
    for i = 1, 2 do
        peers[i] = support.spawn({ interpreter, SERVICE_PEER, "client", name, "300" })
    end
    service.serve(name, 300, 2)
    for _, peer in ipairs(peers) do
        eq(peer:wait(), 0, "a Python client's exit code")
    end
end)

test("a service: a Python owner's client, a joined Lua server", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_s_ocli")
    local peer = support.spawn({ interpreter, SERVICE_PEER, "owner-client", name, "300", "1" })
    service.join_and_answer(name)
    eq(peer:wait(), 0, "the Python owner's exit code")
end)

test("a service: two joined Python servers, a Lua owner's client", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_s_jsrv")
    local peers = {}
    for i = 1, 2 do
        peers[i] = support.spawn({ interpreter, SERVICE_PEER, "joined-server", name })
    end
    service.own_and_call(name, 300, 2)
    for _, peer in ipairs(peers) do
        eq(peer:wait(), 0, "a Python server's exit code")
    end
end)

test("a service: a Python server killed holding a request abandons it; the next call is answered", function()
    local interpreter = python()
    if not interpreter then
        return
    end
    local name = support.unique_name("py_s_kill")
    local owned = assert(fipc.service_create(name, 4096, 64, 1, 2))
    local client = assert(fipc.service_join(name, fipc.CLIENT, service.TIMEOUT))
    local output = support.temp_file(".txt")
    local peer = support.spawn({ interpreter, SERVICE_PEER, "holding-server", name }, { stdout = output })
    assert(client:send("doomed", service.TIMEOUT))
    local deadline = support.now() + service.TIMEOUT / 1000
    while not support.read_file(output):find("holding", 1, true) do -- it holds the request
        assert(support.now() < deadline, "the holding server didn't take the request within 20 s")
        support.sleep(5)
    end
    peer:kill()
    os.remove(output)
    fails("abandoned", nil, client:recv(service.TIMEOUT)) -- once the owner's process has seen its end
    fails("timeout", nil, client:recv(fipc.NO_WAIT)) -- answered once
    local server = support.spawn({ interpreter, SERVICE_PEER, "joined-server", name })
    eq(client:call("next", service.TIMEOUT), "NEXT")
    client:close()
    owned:close() -- ends the service: the Python server's receive raises DISCONNECTED
    eq(server:wait(), 0, "the Python server's exit code")
end)

return suite
