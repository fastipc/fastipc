-- The module itself, in one process: its constants and declarations (every function of include/fipc.h), names,
-- stream_create, stream_connect and accept without a peer, timeouts, cancel, close, and what raises.

local support = require("support")
local fipc = support.fipc
local eq, fails, raises = support.eq, support.fails, support.raises

local suite = support.suite("api")
local test = suite.test

test("the constants and the error names", function()
    eq(fipc._VERSION, "1.0.0")
    eq(fipc.NO_WAIT, 0)
    eq(fipc.FOREVER, -1)
    eq(fipc.SUBSCRIBER_LOSSY, 1)
    eq(fipc.SUBSCRIBER_GATED, 2)
    eq(fipc.PRODUCER, 3)
    eq(fipc.CONSUMER, 4)
    eq(fipc.CLIENT, 5)
    eq(fipc.SERVER, 6)
    local names = { "timeout", "disconnected", "cancelled", "too_large", "invalid", "no_memory", "addr_in_use",
        "overrun", "unsubscribed", "abandoned" }
    for code, name in ipairs(names) do
        eq(fipc[name:upper()], name)
        eq(fipc.result_str(name), "FIPC_" .. name:upper(), name)
        eq(fipc.result_str(code), "FIPC_" .. name:upper(), name)
    end
    eq(fipc.result_str(0), "FIPC_OK")
    eq(fipc.result_str("ok"), "FIPC_OK")
    eq(fipc.result_str(11), "FIPC_UNKNOWN")
    eq(fipc.result_str("nonsense"), "FIPC_UNKNOWN")
end)

test("the declarations match include/fipc.h", function()
    local header = support.read_file(support.repository .. "/include/fipc.h")
    local declared = 0
    for name in header:gmatch("FIPC_API[^;]-(fipc_[%w_]+)%s*%(") do
        declared = declared + 1
        assert(pcall(function() return fipc.C[name] end), name .. " is not declared")
    end
    eq(declared, 30, "the header's functions")
    for code, name in ipairs({ "TIMEOUT", "DISCONNECTED", "CANCELLED", "TOO_LARGE", "INVALID", "NO_MEMORY",
        "ADDR_IN_USE", "OVERRUN", "UNSUBSCRIBED", "ABANDONED" }) do
        assert(header:find("FIPC_" .. name .. " = " .. code .. ",", 1, true), name .. " is not " .. code)
    end
    for _, role in ipairs({ "SUBSCRIBER_LOSSY", "SUBSCRIBER_GATED", "PRODUCER", "CONSUMER", "CLIENT", "SERVER" }) do
        assert(header:find("#define FIPC_" .. role .. " " .. fipc[role] .. " ", 1, true), role)
    end
end)

test("the library comes from this checkout's zig-out", function()
    assert(fipc.library:find("zig-out", 1, true), fipc.library)
end)

test("stream_create refuses a bad name or capacity", function()
    fails("invalid", nil, fipc.stream_create("", support.RING))
    fails("invalid", nil, fipc.stream_create("bad name", support.RING))
    fails("invalid", nil, fipc.stream_create(".hidden", support.RING))
    fails("invalid", nil, fipc.stream_create("-dash", support.RING))
    fails("invalid", nil, fipc.stream_create(string.rep("a", 246), support.RING))
    fails("invalid", nil, fipc.stream_create(support.unique_name("cap"), 1000))
    fails("invalid", nil, fipc.stream_create(support.unique_name("cap"), 512))
    local longest = support.unique_name("long")
    local listener = assert(fipc.stream_create(longest .. string.rep("a", 245 - #longest), 1024))
    listener:close()
end)

test("a name has one stream at a time", function()
    local name = support.unique_name("inuse")
    local listener = assert(fipc.stream_create(name, support.RING))
    eq(listener.name, name)
    fails("addr_in_use", nil, fipc.stream_create(name, support.RING))
    fails("addr_in_use", nil, fipc.broadcast_create(name, 4096, 1))
    fails("addr_in_use", nil, fipc.queue_create(name, 4096, 1, 1))
    fails("addr_in_use", nil, fipc.service_create(name, 4096, 64, 1, 1))
    -- A join learns the kind once the stream's server accepts it; while no accept runs, it times out
    fails("timeout", nil, fipc.broadcast_join(name, fipc.SUBSCRIBER_GATED, fipc.NO_WAIT))
    fails("timeout", nil, fipc.queue_join(name, fipc.CONSUMER, fipc.NO_WAIT))
    fails("timeout", nil, fipc.service_join(name, fipc.SERVER, fipc.NO_WAIT))
    listener:close()
    local again = assert(fipc.stream_create(name, support.RING)) -- free again at once
    again:close()
end)

test("accept and stream_connect time out", function()
    local listener = assert(fipc.stream_create(support.unique_name("timeout"), support.RING))
    fails("timeout", nil, listener:accept(fipc.NO_WAIT))
    local t0 = support.now()
    fails("timeout", nil, listener:accept(60))
    local waited = (support.now() - t0) * 1000
    assert(waited >= 50 and waited < 5000, "waited " .. waited .. " ms")
    listener:close()
    fails("timeout", nil, fipc.stream_connect(support.unique_name("nobody"), fipc.NO_WAIT))
    t0 = support.now()
    fails("timeout", nil, fipc.stream_connect(support.unique_name("nobody"), 0.5)) -- a fraction counts as 1 ms
    assert(support.now() - t0 < 5)
end)

test("a cancelled listener's accept returns at once", function()
    local listener = assert(fipc.stream_create(support.unique_name("cancel"), support.RING))
    listener:cancel()
    local t0 = support.now()
    fails("cancelled", nil, listener:accept(fipc.FOREVER))
    fails("cancelled", nil, listener:accept(10000))
    assert(support.now() - t0 < 5)
    listener:close()
end)

test("close twice, and a closed handle raises", function()
    local listener = assert(fipc.stream_create(support.unique_name("close"), support.RING))
    assert(not listener:closed())
    listener:close()
    listener:close()
    assert(listener:closed())
    raises("the listener is closed", listener.accept, listener, 0)
    raises("the listener is closed", listener.cancel, listener)
end)

test("arguments of the wrong type raise", function()
    raises("a name is a string", fipc.stream_create, nil, 1024)
    raises("a capacity is a whole number", fipc.stream_create, "x", "1024")
    raises("a capacity is a whole number", fipc.stream_create, "x", 1024.5)
    raises("a name is a string", fipc.stream_connect, 42)
    local listener = assert(fipc.stream_create(support.unique_name("args"), support.RING))
    raises("a timeout is milliseconds", listener.accept, listener, "soon")
    raises("a timeout is milliseconds", listener.accept, listener, 0 / 0)
    listener:close()
end)

test("a dropped listener is closed by the garbage collector", function()
    local name = support.unique_name("gc")
    do
        local _ = assert(fipc.stream_create(name, support.RING))
    end
    collectgarbage()
    collectgarbage()
    local listener = assert(fipc.stream_create(name, support.RING)) -- the name is free again
    listener:close()
end)

return suite
