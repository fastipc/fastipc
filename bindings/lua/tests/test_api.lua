-- The module itself, in one process: its constants and declarations, names, listen, connect and accept without a
-- peer, timeouts, cancel, close, and what raises.

local ffi = require("ffi")
local support = require("support")
local fipc = support.fipc
local eq, fails, raises = support.eq, support.fails, support.raises

local suite = support.suite("api")
local test = suite.test

test("the constants and the error names", function()
    eq(fipc._VERSION, "1.0.0")
    eq(fipc.NO_WAIT, 0)
    eq(fipc.FOREVER, -1)
    eq(fipc.RPC_REQUEST, 1)
    eq(fipc.RPC_RESPONSE, 2)
    local names = { "timeout", "disconnected", "cancelled", "too_large", "invalid", "no_memory", "addr_in_use" }
    for code, name in ipairs(names) do
        eq(fipc[name:upper()], name)
        eq(fipc.result_str(name), "FIPC_" .. name:upper(), name)
        eq(fipc.result_str(code), "FIPC_" .. name:upper(), name)
    end
    eq(fipc.result_str(0), "FIPC_OK")
    eq(fipc.result_str("ok"), "FIPC_OK")
    eq(fipc.result_str(8), "FIPC_UNKNOWN")
    eq(fipc.result_str("nonsense"), "FIPC_UNKNOWN")
end)

test("the declarations match include/fipc.h", function()
    local header = support.read_file(support.repository .. "/include/fipc.h")
    local declared = 0
    for name in header:gmatch("FIPC_API[^;]-(fipc_[%w_]+)%s*%(") do
        declared = declared + 1
        assert(pcall(function() return fipc.C[name] end), name .. " is not declared")
    end
    eq(declared, 18, "the header's functions")
    -- fipc_rpc_msg_t: 32 bytes, its fields where the header has them
    eq(ffi.sizeof("fipc_rpc_msg_t"), 32)
    local offsets = { id = 0, kind = 8, opcode = 12, status = 16, reserved = 20, len = 24 }
    for field, offset in pairs(offsets) do
        eq(ffi.offsetof("fipc_rpc_msg_t", field), offset, field)
    end
    for code, name in ipairs({ "TIMEOUT", "DISCONNECTED", "CANCELLED", "TOO_LARGE", "INVALID", "NO_MEMORY",
        "ADDR_IN_USE" }) do
        assert(header:find("FIPC_" .. name .. " = " .. code .. ",", 1, true), name .. " is not " .. code)
    end
end)

test("the library comes from this checkout's zig-out", function()
    assert(fipc.library:find("zig-out", 1, true), fipc.library)
end)

test("listen refuses a bad name or capacity", function()
    fails("invalid", nil, fipc.listen("", support.RING))
    fails("invalid", nil, fipc.listen("bad name", support.RING))
    fails("invalid", nil, fipc.listen(".hidden", support.RING))
    fails("invalid", nil, fipc.listen("-dash", support.RING))
    fails("invalid", nil, fipc.listen(string.rep("a", 246), support.RING))
    fails("invalid", nil, fipc.listen(support.unique_name("cap"), 1000))
    fails("invalid", nil, fipc.listen(support.unique_name("cap"), 512))
    local longest = support.unique_name("long")
    local listener = assert(fipc.listen(longest .. string.rep("a", 245 - #longest), 1024))
    listener:close()
end)

test("a name has one listener at a time", function()
    local name = support.unique_name("inuse")
    local listener = assert(fipc.listen(name, support.RING))
    eq(listener.name, name)
    fails("addr_in_use", nil, fipc.listen(name, support.RING))
    listener:close()
    local again = assert(fipc.listen(name, support.RING)) -- free again at once
    again:close()
end)

test("accept and connect time out", function()
    local listener = assert(fipc.listen(support.unique_name("timeout"), support.RING))
    fails("timeout", nil, listener:accept(fipc.NO_WAIT))
    local t0 = support.now()
    fails("timeout", nil, listener:accept(60))
    local waited = (support.now() - t0) * 1000
    assert(waited >= 50 and waited < 5000, "waited " .. waited .. " ms")
    listener:close()
    fails("timeout", nil, fipc.connect(support.unique_name("nobody"), fipc.NO_WAIT))
    t0 = support.now()
    fails("timeout", nil, fipc.connect(support.unique_name("nobody"), 0.5)) -- a fraction counts as 1 ms
    assert(support.now() - t0 < 5)
end)

test("a cancelled listener's accept returns at once", function()
    local listener = assert(fipc.listen(support.unique_name("cancel"), support.RING))
    listener:cancel()
    local t0 = support.now()
    fails("cancelled", nil, listener:accept(fipc.FOREVER))
    fails("cancelled", nil, listener:accept(10000))
    assert(support.now() - t0 < 5)
    listener:close()
end)

test("close twice, and a closed handle raises", function()
    local listener = assert(fipc.listen(support.unique_name("close"), support.RING))
    assert(not listener:closed())
    listener:close()
    listener:close()
    assert(listener:closed())
    raises("the listener is closed", listener.accept, listener, 0)
    raises("the listener is closed", listener.cancel, listener)
end)

test("arguments of the wrong type raise", function()
    raises("a name is a string", fipc.listen, nil, 1024)
    raises("a capacity is a whole number", fipc.listen, "x", "1024")
    raises("a capacity is a whole number", fipc.listen, "x", 1024.5)
    raises("a name is a string", fipc.connect, 42)
    local listener = assert(fipc.listen(support.unique_name("args"), support.RING))
    raises("a timeout is milliseconds", listener.accept, listener, "soon")
    raises("a timeout is milliseconds", listener.accept, listener, 0 / 0)
    listener:close()
end)

test("a dropped listener is closed by the garbage collector", function()
    local name = support.unique_name("gc")
    do
        local _ = assert(fipc.listen(name, support.RING))
    end
    collectgarbage()
    collectgarbage()
    local listener = assert(fipc.listen(name, support.RING)) -- the name is free again
    listener:close()
end)

return suite
