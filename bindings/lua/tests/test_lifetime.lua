-- Lifetimes across processes: the peer's end (it closes, exits or is killed), ended(), one client at a time, a
-- listener's close, and handles the garbage collector closes.

local support = require("support")
local fipc = support.fipc
local eq, fails, raises = support.eq, support.fails, support.raises

local suite = support.suite("lifetime")
local test = suite.test

test("the messages a peer sent before it closed arrive, then its end", function()
    local name = support.unique_name("end")
    local listener = assert(fipc.listen(name, support.RING))
    local peer = support.peer("send_and_exit", name, "3")
    local conn = assert(listener:accept(support.TEN_SECONDS))
    eq(peer:wait(), 0, "the peer's exit code")
    for i = 1, 3 do
        eq(conn:recv(support.TEN_SECONDS), "message " .. i)
    end
    assert(not conn:ended())
    fails("disconnected", nil, conn:recv(support.TEN_SECONDS))
    assert(conn:ended())
    fails("disconnected", nil, conn:recv(fipc.NO_WAIT)) -- final
    fails("disconnected", nil, conn:send("anyone?", support.TEN_SECONDS))
    conn:close()
    assert(conn:closed())
    raises("the connection is closed", conn.recv, conn)
    conn:close()
    listener:close()
end)

test("a killed peer ends the connection", function()
    local name = support.unique_name("killed")
    local listener = assert(fipc.listen(name, support.RING))
    local peer = support.peer("hang", name)
    local conn = assert(listener:accept(support.TEN_SECONDS))
    eq(conn:recv(support.TEN_SECONDS), "ready")
    peer:kill()
    local t0 = support.now()
    fails("disconnected", nil, conn:recv(fipc.FOREVER))
    assert(support.now() - t0 < 5)
    assert(conn:ended())
    conn:close()
    listener:close()
end)

test("closing this side ends the peer's connection", function()
    local name = support.unique_name("close")
    local listener = assert(fipc.listen(name, support.RING))
    local peer = support.peer("expect_end", name)
    local conn = assert(listener:accept(support.TEN_SECONDS))
    conn:close()
    eq(peer:wait(), 0, "the peer's exit code")
    listener:close()
end)

test("one client at a time; a dropped connection is closed by the garbage collector", function()
    local name = support.unique_name("one")
    local listener = assert(fipc.listen(name, support.RING))
    local first = support.peer("expect_end", name)
    do
        local _ = assert(listener:accept(support.TEN_SECONDS))
        fails("invalid", nil, listener:accept(fipc.NO_WAIT)) -- the last connection is open
    end
    collectgarbage()
    collectgarbage()
    eq(first:wait(), 0, "the first peer's exit code") -- it saw the end
    local second = support.peer("expect_end", name)
    local conn = assert(listener:accept(support.TEN_SECONDS)) -- the next client
    conn:close()
    eq(second:wait(), 0, "the second peer's exit code")
    listener:close()
end)

test("a listener's close leaves its connections open", function()
    local name = support.unique_name("listener_close")
    local listener = assert(fipc.listen(name, support.RING))
    local peer = support.peer("echo", name)
    local conn = assert(listener:accept(support.TEN_SECONDS))
    listener:close()
    assert(conn:send("still here", support.TEN_SECONDS))
    eq(conn:recv(support.TEN_SECONDS), "still here")
    conn:close()
    eq(peer:wait(), 0, "the peer's exit code")
end)

test("an accept that polls lets a client in; what the client sent before, and its end, wait in the ring", function()
    local name = support.unique_name("early")
    local listener = assert(fipc.listen(name, support.RING))
    local peer = support.peer("send_and_exit", name, "2")
    -- A call with NO_WAIT goes as far as it can without waiting, and the next goes on from there: the call that
    -- offers the client's rings times out, the client's connect returns, and the client sends and exits before the
    -- call that returns its connection
    local conn
    for _ = 1, 200 do
        local err
        conn, err = listener:accept(fipc.NO_WAIT)
        if conn then
            break
        end
        eq(err, "timeout", "a polling accept")
        support.sleep(100)
    end
    assert(conn, "the client got in")
    eq(peer:wait(), 0, "the peer's exit code")
    eq(conn:recv(support.TEN_SECONDS), "message 1")
    eq(conn:recv(support.TEN_SECONDS), "message 2")
    fails("disconnected", nil, conn:recv(support.TEN_SECONDS))
    conn:close()
    listener:close()
end)

test("a client waits for a server that listens later", function()
    local name = support.unique_name("later")
    local peer = support.peer("echo", name)
    support.sleep(200)
    local listener = assert(fipc.listen(name, support.RING))
    local conn = assert(listener:accept(support.TEN_SECONDS))
    assert(conn:send("late", support.TEN_SECONDS))
    eq(conn:recv(support.TEN_SECONDS), "late")
    conn:close()
    listener:close()
    eq(peer:wait(), 0, "the peer's exit code")
end)

return suite
