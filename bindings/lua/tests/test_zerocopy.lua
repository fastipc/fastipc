-- Zero-copy messages: written and read in the ring (send_acquire / send_commit, recv_acquire / recv_release), against
-- a Lua peer in another process that does the same (tests/peer.lua zerocopy_echo).

local ffi = require("ffi")
local support = require("support")
local fipc = support.fipc
local eq, fails = support.eq, support.fails

local suite = support.suite("zerocopy")
local test = suite.test

local function serve(role)
    local name = support.unique_name(role)
    local listener = assert(fipc.listen(name, support.RING))
    local peer = support.peer(role, name)
    local conn = assert(listener:accept(support.TEN_SECONDS))
    return listener, conn, peer
end

local function finish(listener, conn, peer)
    conn:close()
    listener:close()
    eq(peer:wait(), 0, "the peer's exit code")
end

-- Sends `message` in place
local function send_in_place(conn, message)
    local slot = assert(conn:send_acquire(#message, support.TEN_SECONDS))
    ffi.copy(slot, message, #message)
    assert(conn:send_commit(#message))
end

-- Receives the next message in place, as a string
local function recv_in_place(conn)
    local data, len = assert(conn:recv_acquire(support.TEN_SECONDS))
    local message = ffi.string(data, len)
    conn:recv_release()
    return message
end

local function address(ptr)
    return tonumber(ffi.cast("uintptr_t", ptr))
end

test("messages of one piece, in place both ways", function()
    local listener, conn, peer = serve("zerocopy_echo")
    local max = conn:max_piece()
    for _, size in ipairs({ 1, 15, 16, 17, 1000, math.floor(max / 2), max - 1, max }) do
        local message = support.pattern(size)
        send_in_place(conn, message)
        eq(recv_in_place(conn), message, "size " .. size)
    end
    finish(listener, conn, peer)
end)

test("the room is 16-byte aligned, and a commit may be shorter", function()
    local listener, conn, peer = serve("zerocopy_echo")
    for _, size in ipairs({ 1, 3, 17, 100 }) do
        local slot = assert(conn:send_acquire(size, support.TEN_SECONDS))
        eq(address(slot) % 16, 0, "the send room's address")
        ffi.fill(slot, size, 65)
        assert(conn:send_commit(1)) -- only the first byte
        local data, len = assert(conn:recv_acquire(support.TEN_SECONDS))
        eq(address(data) % 16, 0, "the received message's address")
        eq(len, 1)
        eq(ffi.string(data, len), "A")
        conn:recv_release()
    end
    finish(listener, conn, peer)
end)

test("commits out of range, and without a reservation, are invalid", function()
    local listener, conn, peer = serve("zerocopy_echo")
    fails("invalid", nil, conn:send_commit(1)) -- nothing acquired
    fails("invalid", nil, conn:send_acquire(0, support.TEN_SECONDS))
    fails("too_large", nil, conn:send_acquire(conn:max_piece() + 1, support.TEN_SECONDS))
    local slot = assert(conn:send_acquire(8, support.TEN_SECONDS))
    ffi.copy(slot, "reserved", 8)
    fails("invalid", nil, conn:send_commit(0))
    fails("invalid", nil, conn:send_commit(9))
    assert(conn:send_commit(8)) -- the reservation stayed for a valid commit
    eq(recv_in_place(conn), "reserved")
    -- A reservation never committed is dropped by the next send
    assert(conn:send_acquire(4, support.TEN_SECONDS))
    assert(conn:send("plain", support.TEN_SECONDS))
    eq(conn:recv(support.TEN_SECONDS), "plain")
    finish(listener, conn, peer)
end)

test("a message of several pieces is received with recv", function()
    local listener, conn, peer = serve("zerocopy_echo")
    local long = support.pattern(3 * support.RING)
    assert(conn:send(long, support.TEN_SECONDS)) -- the peer takes it with recv_into, sends it back with send
    fails("too_large", #long, conn:recv_acquire(support.TEN_SECONDS))
    eq(conn:recv(support.TEN_SECONDS), long)
    conn:recv_release() -- nothing acquired: does nothing
    -- A receive releases an acquired message first
    send_in_place(conn, "first")
    send_in_place(conn, "second")
    local data, len = assert(conn:recv_acquire(support.TEN_SECONDS))
    eq(ffi.string(data, len), "first")
    eq(conn:recv(support.TEN_SECONDS), "second")
    finish(listener, conn, peer)
end)

return suite
