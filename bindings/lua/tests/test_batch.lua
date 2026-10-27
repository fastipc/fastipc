-- Batches (send_many, recv_many): a stream's batch to an echoing Lua peer in another process (tests/peer.lua), in
-- order and whole, max_count, a first message longer than the batch's buffer; a broadcast's and a work queue's batches
-- in this Lua state, a partial send on NO_WAIT with how many went, "too_large" and an empty message before any goes;
-- a service client's batch of requests, owed as its sends are, with their replies in order, and a server's batches
-- refused.

local support = require("support")
local fipc = support.fipc
local eq, fails, raises = support.eq, support.fails, support.raises

local suite = support.suite("batch")
local test = suite.test

-- `count` messages, received in batches of up to `max_count` (a broadcast's subscriber may take fewer than are there)
local function receive_all(conn, count, max_count)
    local received = {}
    while #received < count do
        local batch = assert(conn:recv_many(max_count or 16, support.TEN_SECONDS))
        assert(#batch >= 1 and #batch <= (max_count or 16), "a batch of " .. #batch)
        for _, message in ipairs(batch) do
            received[#received + 1] = message
        end
    end
    return received
end

local function same(a, b, what)
    eq(#a, #b, what .. ": the count")
    for i = 1, #a do
        eq(a[i], b[i], what .. ": message " .. i)
    end
end

test("a stream's batch goes there and back in order and whole", function()
    local name = support.unique_name("batch_echo")
    local listener = assert(fipc.stream_create(name, support.RING))
    local peer = support.peer("echo", name)
    local conn = assert(listener:accept(support.TEN_SECONDS))
    local messages = {}
    for i = 1, 6 do
        messages[i] = support.pattern(10 + i * 20)
    end
    eq(conn:send_many(messages, support.TEN_SECONDS), 6)
    same(receive_all(conn, 6, 2), messages, "max_count 2")
    -- A message longer than one piece goes in pieces, and the batch's buffer grows for it
    local batch = { "a", support.pattern(3 * support.RING), "b" }
    eq(conn:send_many(batch, support.TEN_SECONDS), 3)
    same(receive_all(conn, 3), batch, "a long message")
    fails("invalid", 0, conn:send_many({ "x", "" }, fipc.NO_WAIT))
    fails("invalid", 0, conn:send_many({}, fipc.NO_WAIT))
    fails("invalid", nil, conn:recv_many(0, fipc.NO_WAIT))
    fails("timeout", nil, conn:recv_many(16, fipc.NO_WAIT)) -- nothing went
    raises("send_many takes a table of strings", conn.send_many, conn, "x")
    raises("send_many takes a table of strings", conn.send_many, conn, { "x", 1 })
    raises("a number of messages is a whole number", conn.recv_many, conn, -1)
    conn:close()
    listener:close()
    eq(peer:wait(), 0, "the peer's exit code")
end)

test("a broadcast's batches reach every subscriber", function()
    local name = support.unique_name("bc_batch")
    local publisher = assert(fipc.broadcast_create(name, 64 * 1024, 4, fipc.FOREVER))
    local lossy = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_LOSSY, support.TEN_SECONDS))
    local gated = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_GATED, support.TEN_SECONDS))
    eq(publisher:wait_for_members(2, support.TEN_SECONDS), 2)
    local ticks = {}
    for i = 1, 40 do
        ticks[i] = "tick " .. i
    end
    eq(publisher:send_many(ticks, support.TEN_SECONDS), 40)
    fails("too_large", 0, publisher:send_many({ "a", string.rep("x", publisher:max_piece() + 1) }, fipc.NO_WAIT))
    fails("invalid", nil, publisher:recv_many(16, fipc.NO_WAIT))
    fails("invalid", 0, gated:send_many(ticks, fipc.NO_WAIT))
    publisher:close() -- what it sent is received first
    for _, subscriber in ipairs({ lossy, gated }) do
        same(receive_all(subscriber, 40), ticks, "a subscriber")
        fails("disconnected", nil, subscriber:recv_many(16, support.TEN_SECONDS))
        subscriber:close()
    end
end)

test("a work queue's batches, and a partial send on NO_WAIT", function()
    local name = support.unique_name("q_batch")
    local queue = assert(fipc.queue_create(name, 4096, 1, 1))
    local producer = assert(fipc.queue_join(name, fipc.PRODUCER, support.TEN_SECONDS))
    local consumer = assert(fipc.queue_join(name, fipc.CONSUMER, support.TEN_SECONDS))
    local jobs = {}
    for i = 1, 20 do
        jobs[i] = "job " .. i
    end
    eq(producer:send_many(jobs, support.TEN_SECONDS), 20)
    same(receive_all(consumer, 20), jobs, "one producer: its messages in order")
    -- More than the producer's ring of 4 KiB holds: the rest doesn't go, and the third value says how many did
    local many = {}
    for i = 1, 100 do
        many[i] = string.rep("m", 100)
    end
    local sent, err, went = producer:send_many(many, fipc.NO_WAIT)
    eq(sent, nil)
    eq(err, "timeout")
    assert(went >= 1 and went < 100, "went: " .. tostring(went))
    eq(#receive_all(consumer, went), went)
    fails("timeout", nil, consumer:recv_many(16, fipc.NO_WAIT))
    fails("too_large", 0, producer:send_many({ "a", string.rep("x", producer:max_piece() + 1) }, fipc.NO_WAIT))
    fails("invalid", 0, consumer:send_many(jobs, fipc.NO_WAIT))
    fails("invalid", nil, producer:recv_many(16, fipc.NO_WAIT))
    queue:close()
    fails("disconnected", nil, consumer:recv_many(16, support.TEN_SECONDS))
    fails("disconnected", 0, producer:send_many(jobs, support.TEN_SECONDS))
    producer:close()
    consumer:close()
end)

-- A server's answer to the next request: the request with " done"
local function answer(server)
    local request = assert(server:recv(support.TEN_SECONDS))
    assert(server:send(request .. " done", fipc.NO_WAIT))
end

test("a service client's batch is owed as its sends are, and a server's batches are invalid", function()
    local name = support.unique_name("s_batch")
    local service = assert(fipc.service_create(name, 16 * 1024, 64, 1, 1))
    local server = assert(fipc.service_join(name, fipc.SERVER, support.TEN_SECONDS))
    local client = assert(fipc.service_join(name, fipc.CLIENT, support.TEN_SECONDS))
    local requests = { "a", "bb", "ccc" }
    eq(client:send_many(requests, support.TEN_SECONDS), 3)
    fails("invalid", nil, client:call("d", fipc.NO_WAIT)) -- those replies are the caller's
    for _ = 1, 3 do
        answer(server)
    end
    same(receive_all(client, 3), { "a done", "bb done", "ccc done" }, "the replies")
    -- Nothing owed now: a call sends (with no server thread here, it times out waiting for its reply, which a plain
    -- receive then returns)
    fails("timeout", nil, client:call("e", 100))
    answer(server)
    eq(client:recv(support.TEN_SECONDS), "e done")
    fails("invalid", 0, server:send_many(requests, fipc.NO_WAIT))
    fails("invalid", nil, server:recv_many(16, fipc.NO_WAIT))

    -- A batch whose requests don't all fit counts only those that went. Each request holds 64 + 64 bytes of the
    -- 16 KiB ring: at most 128 fit
    local many = {}
    for i = 1, 200 do
        many[i] = "f"
    end
    local sent, err, went = client:send_many(many, fipc.NO_WAIT)
    eq(sent, nil)
    eq(err, "timeout")
    assert(went >= 1 and went <= 128, "went: " .. tostring(went))
    fails("invalid", nil, client:call("g", fipc.NO_WAIT)) -- their replies are owed
    for _ = 1, went do
        answer(server)
    end
    eq(#receive_all(client, went), went)
    fails("timeout", nil, client:call("h", 100)) -- nothing owed: the call sends again
    server:close()
    client:close()
    service:close()
end)

return suite
