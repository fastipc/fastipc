-- The broadcast: the publisher's calls, a publisher and its subscribers in this Lua state, a publisher here with a
-- lossy and a gated subscriber in other processes and the other way round, the member count and its wait, the
-- overrun's lost count, and an unsubscribed gated subscriber. The Python binding's side is in test_interop.lua.

local support = require("support")
local broadcast = require("broadcast")
local fipc = support.fipc
local eq, fails, raises = support.eq, support.fails, support.raises

local suite = support.suite("broadcast")
local test = suite.test

test("broadcast_create and the publisher without subscribers", function()
    local name = support.unique_name("bc_api")
    fails("invalid", nil, fipc.broadcast_create(name, 2048, 1)) -- under 4096 bytes
    fails("invalid", nil, fipc.broadcast_create(name, 4096, 0))
    fails("invalid", nil, fipc.broadcast_create(name, 4096, 1025))
    raises("a number of subscribers is a whole number", fipc.broadcast_create, name, 4096, "8")
    raises("a name is a string", fipc.broadcast_create, 42, 4096, 8)
    local publisher = assert(fipc.broadcast_create(name, 4096, 2))
    fails("addr_in_use", nil, fipc.broadcast_create(name, 4096, 2))
    fails("addr_in_use", nil, fipc.stream_create(name, support.RING))
    fails("addr_in_use", nil, fipc.queue_create(name, 4096, 1, 1))
    eq(publisher:max_piece(), 1024)

    eq(publisher:wait_for_members(), 0)
    fails("timeout", 0, publisher:wait_for_members(1, fipc.NO_WAIT))
    local t0 = support.now()
    fails("timeout", 0, publisher:wait_for_members(2, 50))
    local waited = (support.now() - t0) * 1000
    assert(waited >= 40 and waited < 5000, "waited " .. waited .. " ms")
    local value, err, count = publisher:wait_for_members(3) -- over max_subscribers
    eq(value, nil)
    eq(err, "invalid")
    eq(count, nil)
    raises("a number of subscribers is a whole number", publisher.wait_for_members, publisher, -1)

    -- What a publisher can't do
    fails("invalid", nil, publisher:recv(fipc.NO_WAIT))
    fails("too_large", nil, publisher:send(string.rep("x", 1025)))

    -- The roles: a queue's and unknown ones are refused
    fails("invalid", nil, fipc.broadcast_join(name, 5, fipc.NO_WAIT)) -- 5 is no role
    fails("invalid", nil, fipc.broadcast_join(name, fipc.PRODUCER, support.TEN_SECONDS))
    fails("invalid", nil, fipc.broadcast_join(name, fipc.CONSUMER, support.TEN_SECONDS))
    raises("a role is fipc.SUBSCRIBER_LOSSY or fipc.SUBSCRIBER_GATED", fipc.broadcast_join, name, "gated")

    -- The cancel: a wait, now or later, returns "cancelled" with the count
    publisher:cancel()
    fails("cancelled", 0, publisher:wait_for_members(1, fipc.FOREVER))
    publisher:close()
    raises("the connection is closed", publisher.wait_for_members, publisher)

    -- The publisher's close frees the name at once
    publisher = assert(fipc.broadcast_create(name, 4096, 1))
    publisher:close()
end)

test("a publisher and a gated and a lossy subscriber in this Lua state", function()
    local name = support.unique_name("bc_one")
    local publisher = assert(fipc.broadcast_create(name, 4096, 2, broadcast.TIMEOUT))
    local subscribers = broadcast.join(name, { "gated", "lossy" }, 300)
    eq(assert(publisher:wait_for_members(2, support.TEN_SECONDS)), 2)
    -- 300 messages through 64 cells: a send that finds the gated subscriber's room full lets the subscribers take some
    local deadline = support.now() + 20
    local i = 0
    while i < 300 do
        local sent, err = broadcast.send(publisher, i, fipc.NO_WAIT)
        if sent then
            i = i + 1
        else
            eq(err, fipc.TIMEOUT, "the publisher's send")
            broadcast.step(subscribers)
        end
        assert(support.now() < deadline, "only " .. i .. " messages sent within 20 s")
    end
    publisher:close()
    repeat
        local _, open = broadcast.step(subscribers)
        assert(support.now() < deadline, "the broadcast didn't end within 20 s")
    until open == 0
end)

test("a publisher here; a gated and a lossy subscriber in other processes", function()
    local name = support.unique_name("bc_pub")
    local peers = { support.peer("subscriber", name, "gated", "300"), support.peer("subscriber", name, "lossy", "300") }
    broadcast.publish(name, 300, 2)
    for _, peer in ipairs(peers) do
        eq(peer:wait(), 0, "a subscriber peer's exit code")
    end
end)

test("a publisher in another process; a gated and a lossy subscriber here", function()
    local name = support.unique_name("bc_sub")
    local peer = support.peer("publisher", name, "300", "2")
    broadcast.subscribe(name, { "gated", "lossy" }, 300)
    eq(peer:wait(), 0, "the publisher peer's exit code")
end)

test("what a subscriber can't do", function()
    local name = support.unique_name("bc_misuse")
    local publisher = assert(fipc.broadcast_create(name, 4096, 2))
    local gated = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_GATED, support.TEN_SECONDS))
    local lossy = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_LOSSY, support.TEN_SECONDS))
    eq(gated:max_piece(), 1024)
    fails("invalid", nil, lossy:recv_acquire(fipc.NO_WAIT)) -- a lossy subscriber copies and checks
    fails("invalid", nil, gated:send("x"))
    fails("invalid", nil, lossy:send_acquire(1))
    fails("invalid", nil, gated:wait_for_members())
    publisher:close() -- it sent nothing
    fails("disconnected", nil, gated:recv(support.TEN_SECONDS))
    fails("disconnected", nil, lossy:recv(support.TEN_SECONDS))
    assert(gated:ended() and lossy:ended())
    gated:close()
    lossy:close()
end)

test("the member count follows joins and leaves; a full broadcast's joiner waits for a slot", function()
    local name = support.unique_name("bc_count")
    local publisher = assert(fipc.broadcast_create(name, 4096, 2))
    local first = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_GATED, support.TEN_SECONDS))
    eq(assert(publisher:wait_for_members(1, support.TEN_SECONDS)), 1)
    local second = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_LOSSY, support.TEN_SECONDS))
    eq(assert(publisher:wait_for_members(2, support.TEN_SECONDS)), 2)
    fails("timeout", nil, fipc.broadcast_join(name, fipc.SUBSCRIBER_LOSSY, 100)) -- every slot is held
    -- A subscriber that leaves is counted until the publisher's process has seen its end, soon after
    first:close()
    local deadline = support.now() + 10
    while publisher:wait_for_members() ~= 1 do
        assert(support.now() < deadline, "the count didn't drop within 10 s")
        support.sleep(1)
    end
    local third = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_GATED, support.TEN_SECONDS)) -- the free slot
    eq(assert(publisher:wait_for_members(2, support.TEN_SECONDS)), 2)
    assert(publisher:send(broadcast.message(0), support.TEN_SECONDS))
    eq(second:recv(support.TEN_SECONDS), broadcast.message(0))
    eq(third:recv(support.TEN_SECONDS), broadcast.message(0))
    publisher:close()
    second:close()
    third:close()
end)

test("a lapped lossy subscriber gets \"overrun\" and the number of messages lost", function()
    local name = support.unique_name("bc_lap")
    local peer = support.peer("lap", name)
    local subscriber = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_LOSSY, support.TEN_SECONDS))
    eq(peer:wait(), 0, "the publisher peer's exit code") -- every message sent, the broadcast ended
    -- 144 messages of 48 bytes lap the broadcast's 64 cells twice; the subscriber goes on from message 140
    -- (docs/protocol.md §9.6), so 0 to 139 are lost
    local message, err, lost = subscriber:recv(support.TEN_SECONDS)
    eq(message, nil)
    eq(err, "overrun")
    eq(lost, 140)
    eq(type(lost), "number")
    eq(fipc.result_str(err), "FIPC_OVERRUN")
    eq(subscriber:recv(support.TEN_SECONDS):byte(1), 140)
    local buf = require("ffi").new("uint8_t[64]")
    for i = 141, 143 do
        eq(subscriber:recv_into(buf, 64, support.TEN_SECONDS), 48)
        eq(buf[0], i)
    end
    eq(lost, 140, "the count, after the receives") -- a value of its own, not the connection's state
    fails("disconnected", nil, subscriber:recv(support.TEN_SECONDS))
    subscriber:close()
end)

test("a gated subscriber that holds the publisher back is unsubscribed", function()
    local name = support.unique_name("bc_stall")
    local sent = support.temp_file(".txt")
    os.remove(sent)
    local publisher = assert(fipc.broadcast_create(name, 4096, 1, 100))
    local peer = support.peer("stall", name, sent)
    eq(publisher:wait_for_members(1, support.TEN_SECONDS), 1)
    -- The 65th message waits for room the subscriber holds; 100 ms later it is unsubscribed and the publisher goes on
    for _ = 1, 100 do
        assert(publisher:send(string.rep("\0", 48), support.TEN_SECONDS))
    end
    eq(publisher:wait_for_members(), 0)
    local file = assert(io.open(sent, "wb"))
    file:close()
    eq(peer:wait(), 0, "the subscriber peer's exit code")
    os.remove(sent)
    publisher:close()
end)

return suite
