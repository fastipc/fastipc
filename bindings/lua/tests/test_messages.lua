-- Plain messages between this process (the server) and a Lua peer in another (tests/peer.lua): every size, buffers,
-- TooLarge, timeouts and cancel.

local ffi = require("ffi")
local support = require("support")
local fipc = support.fipc
local eq, fails, raises = support.eq, support.fails, support.raises

local suite = support.suite("messages")
local test = suite.test

-- A listener, a peer in another process that connects to it, and the server's connection
local function serve(role, ring, ...)
    local name = support.unique_name(role)
    local listener = assert(fipc.listen(name, ring or support.RING))
    local peer = support.peer(role, name, ...)
    local conn, err = listener:accept(support.TEN_SECONDS)
    if not conn then
        peer:kill()
        error("accept: " .. err)
    end
    return listener, conn, peer
end

-- Closes the connection and the listener; the peer must exit with 0
local function finish(listener, conn, peer)
    conn:close()
    listener:close()
    eq(peer:wait(), 0, "the peer's exit code")
end

test("messages of every size go there and back", function()
    local listener, conn, peer = serve("echo")
    local max = conn:max_piece()
    eq(max, support.RING - 64)
    for _, size in ipairs({ 1, 2, 15, 16, 17, 100, 1000, 4096, 4097, max - 1, max, max + 1, 3 * support.RING + 5,
        1024 * 1024 }) do
        local message = support.pattern(size)
        assert(conn:send(message, support.TEN_SECONDS)) -- longer than the ring: the peer receives it as it comes
        eq(conn:recv(support.TEN_SECONDS), message, "size " .. size)
    end
    finish(listener, conn, peer)
end)

test("a cdata buffer and its length", function()
    local listener, conn, peer = serve("echo")
    local buf = ffi.new("uint8_t[?]", 200000)
    for i = 0, 199999 do
        buf[i] = i % 251
    end
    assert(conn:send(buf, 200000, support.TEN_SECONDS))
    eq(conn:recv(support.TEN_SECONDS), ffi.string(buf, 200000))
    assert(conn:send(buf + 5, 3)) -- the default timeout: as long as it takes
    eq(conn:recv(), ffi.string(buf + 5, 3))
    raises("needs its length", conn.send, conn, buf)
    raises("a message is a string", conn.send, conn, {})
    finish(listener, conn, peer)
end)

test("recv_into fills a buffer, and leaves a message too long for it", function()
    local listener, conn, peer = serve("echo")
    local buf = ffi.new("uint8_t[?]", 100)
    assert(conn:send("hello", support.TEN_SECONDS))
    eq(conn:recv_into(buf, 100, support.TEN_SECONDS), 5)
    eq(ffi.string(buf, 5), "hello")
    local long = support.pattern(1000)
    assert(conn:send(long, support.TEN_SECONDS))
    fails("too_large", 1000, conn:recv_into(buf, 100, support.TEN_SECONDS))
    fails("too_large", 1000, conn:recv_into(nil, 0, support.TEN_SECONDS)) -- the next message's length only
    eq(conn:recv(support.TEN_SECONDS), long) -- still queued
    -- The connection's own buffer grows for a long message and still takes short ones
    for _, size in ipairs({ 10000, 5, 300000, 7 }) do
        local message = support.pattern(size)
        assert(conn:send(message, support.TEN_SECONDS))
        eq(conn:recv(support.TEN_SECONDS), message, "size " .. size)
    end
    raises("a buffer's size is a whole number", conn.recv_into, conn, buf, -1)
    finish(listener, conn, peer)
end)

test("an empty message is invalid", function()
    local listener, conn, peer = serve("echo")
    fails("invalid", nil, conn:send("", support.TEN_SECONDS))
    assert(conn:send("still up", support.TEN_SECONDS))
    eq(conn:recv(support.TEN_SECONDS), "still up")
    finish(listener, conn, peer)
end)

test("receives time out", function()
    local listener, conn, peer = serve("echo")
    fails("timeout", nil, conn:recv(fipc.NO_WAIT))
    local t0 = support.now()
    fails("timeout", nil, conn:recv(80))
    local waited = (support.now() - t0) * 1000
    assert(waited >= 70 and waited < 5000, "waited " .. waited .. " ms")
    fails("timeout", nil, conn:recv_into(ffi.new("uint8_t[16]"), 16, 1))
    assert(not conn:ended())
    finish(listener, conn, peer)
end)

test("sends time out when the ring is full", function()
    local listener, conn, peer = serve("hang", 1024)
    eq(conn:recv(support.TEN_SECONDS), "ready") -- the peer receives nothing more
    local sent, err = 0, nil
    repeat
        local ok
        ok, err = conn:send(string.rep("x", 100), fipc.NO_WAIT)
        if ok then
            sent = sent + 1
        end
    until not ok
    eq(err, "timeout")
    assert(sent > 0 and sent < 20, sent .. " messages fit")
    fails("timeout", nil, conn:send("x", 30))
    peer:kill()
    fails("disconnected", nil, conn:send("x", support.TEN_SECONDS))
    conn:close()
    listener:close()
end)

test("cancel ends every wait, now and later", function()
    local listener, conn, peer = serve("echo")
    conn:cancel()
    local t0 = support.now()
    fails("cancelled", nil, conn:recv(fipc.FOREVER))
    fails("cancelled", nil, conn:recv(support.TEN_SECONDS))
    assert(support.now() - t0 < 5)
    -- A call that needn't wait still works: a send into an empty ring, a receive of a message already there
    assert(conn:send("after the cancel", fipc.NO_WAIT))
    local echo
    local deadline = support.now() + 10
    repeat
        echo = conn:recv(fipc.NO_WAIT)
    until echo or support.now() > deadline
    eq(echo, "after the cancel")
    assert(not conn:ended())
    finish(listener, conn, peer)
end)

return suite
