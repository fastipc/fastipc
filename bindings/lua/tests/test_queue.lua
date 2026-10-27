-- The work queue: queue_create, the member count and what each role refuses, the end at the queue's close, a queue the
-- program drops (the collector never closes it), producers and consumers interleaved in this Lua state with the owner
-- here and in another process, owners here with members in other processes, and two consumers racing for messages
-- longer than recv's first buffer. The queue promises no order: the checks are that each message came exactly once.
-- The Python binding's side is in test_interop.lua.

local ffi = require("ffi")
local support = require("support")
local queue = require("queue")
local fipc = support.fipc
local eq, fails, raises = support.eq, support.fails, support.raises

local suite = support.suite("queue")
local test = suite.test

-- Fails unless the result is nil, "invalid", with no count after it
local function invalid(...)
    fails("invalid", nil, ...)
    eq(select("#", ...), 2, "INVALID's values (nil and the name, no count)")
end

-- A file that, once it exists, tells a queue_owner peer to close its queue
local function owner_signal()
    local path = support.temp_file(".txt")
    os.remove(path)
    return path, function()
        local file = assert(io.open(path, "wb"))
        file:close()
    end
end

test("queue_create, the member count, and what the roles refuse", function()
    local name = support.unique_name("q_api")
    fails("invalid", nil, fipc.queue_create(name, 2048, 1, 1)) -- under 4096 bytes
    fails("invalid", nil, fipc.queue_create(name, 4096, 0, 1))
    fails("invalid", nil, fipc.queue_create(name, 4096, 65, 1))
    fails("invalid", nil, fipc.queue_create(name, 4096, 1, 0))
    fails("invalid", nil, fipc.queue_create(name, 4096, 1, 65))
    raises("a number of producers or consumers is a whole number", fipc.queue_create, name, 4096, "1", 1)
    raises("a name is a string", fipc.queue_create, nil, 4096, 1, 1)

    local owned = assert(fipc.queue_create(name, 4096, 1, 2))
    eq(owned.name, name)
    fails("addr_in_use", nil, fipc.queue_create(name, 4096, 1, 1))
    fails("addr_in_use", nil, fipc.broadcast_create(name, 4096, 1))
    fails("addr_in_use", nil, fipc.stream_create(name, support.RING))

    -- The count: at once, a poll, a wait that times out, over a role's maximum
    eq(owned:wait_for_members(), 0)
    eq(owned:wait_for_members(fipc.PRODUCER), 0)
    fails("timeout", 0, owned:wait_for_members(fipc.CONSUMER, 1, fipc.NO_WAIT))
    local t0 = support.now()
    fails("timeout", 0, owned:wait_for_members(nil, 1, 50))
    local waited = (support.now() - t0) * 1000
    assert(waited >= 40 and waited < 5000, "waited " .. waited .. " ms")
    invalid(owned:wait_for_members(fipc.PRODUCER, 2))
    invalid(owned:wait_for_members(fipc.CONSUMER, 3))
    invalid(owned:wait_for_members(nil, 4))
    fails("timeout", 0, owned:wait_for_members(nil, 3, fipc.NO_WAIT)) -- both roles: up to 1 + 2
    invalid(owned:wait_for_members(fipc.SUBSCRIBER_GATED, 0))
    raises("a queue's role is fipc.PRODUCER, fipc.CONSUMER or nil", owned.wait_for_members, owned, "producer")
    raises("a number of producers or consumers is a whole number", owned.wait_for_members, owned, nil, -1)

    -- The roles: a broadcast's and unknown ones are refused
    fails("invalid", nil, fipc.queue_join(name, 5, fipc.NO_WAIT))
    fails("invalid", nil, fipc.queue_join(name, fipc.SUBSCRIBER_LOSSY, support.TEN_SECONDS))
    fails("invalid", nil, fipc.queue_join(name, fipc.SUBSCRIBER_GATED, support.TEN_SECONDS))
    raises("a role is fipc.PRODUCER or fipc.CONSUMER", fipc.queue_join, name, "producer")

    -- The owner's producer, joined from this Lua state: it sends, nothing else
    local producer = assert(fipc.queue_join(name, fipc.PRODUCER, support.TEN_SECONDS))
    eq(assert(owned:wait_for_members(fipc.PRODUCER, 1, support.TEN_SECONDS)), 1)
    fails("timeout", nil, fipc.queue_join(name, fipc.PRODUCER, 100)) -- the one producer's place is held
    eq(producer:max_piece(), 1024)
    fails("invalid", nil, producer:recv(fipc.NO_WAIT))
    fails("invalid", nil, producer:recv_into(ffi.new("uint8_t[8]"), 8, fipc.NO_WAIT))
    fails("invalid", nil, producer:recv_acquire(fipc.NO_WAIT))
    producer:recv_release() -- does nothing
    invalid(producer:wait_for_members())
    fails("too_large", nil, producer:send(string.rep("x", 1025)))
    assert(producer:send("job 1", fipc.NO_WAIT))
    local slot = assert(producer:send_acquire(5, fipc.NO_WAIT))
    ffi.copy(slot, "job 2", 5)
    assert(producer:send_commit(5))
    local sent = 2
    while true do -- until the ring is full, as no consumer takes anything
        local ok, err = producer:send("job " .. sent + 1, fipc.NO_WAIT)
        if not ok then
            eq(err, fipc.TIMEOUT)
            break
        end
        sent = sent + 1
    end
    eq(sent, 64, "the messages a ring of 4096 bytes holds")
    producer:cancel()
    fails("cancelled", nil, producer:send("job 65", fipc.FOREVER))

    -- A consumer receives; nothing else
    local consumer = assert(fipc.queue_join(name, fipc.CONSUMER, support.TEN_SECONDS))
    eq(assert(owned:wait_for_members(nil, 2, support.TEN_SECONDS)), 2)
    eq(consumer:max_piece(), 1024)
    fails("invalid", nil, consumer:send("x"))
    fails("invalid", nil, consumer:send_acquire(1))
    invalid(consumer:wait_for_members())
    local received = {}
    for i = 1, 64 do
        received[i] = assert(consumer:recv(support.TEN_SECONDS))
    end
    queue.check_jobs(received, 1, 64)
    fails("timeout", nil, consumer:recv(fipc.NO_WAIT))
    local t1 = support.now()
    fails("timeout", nil, consumer:recv(50)) -- nothing left: it waits
    waited = (support.now() - t1) * 1000
    assert(waited >= 40 and waited < 5000, "waited " .. waited .. " ms")
    fails("timeout", nil, consumer:recv_acquire(fipc.NO_WAIT))

    -- The cancel: a wait, now or later, returns "cancelled" with the count; the queue goes on
    owned:cancel()
    fails("cancelled", 2, owned:wait_for_members(nil, 3, fipc.FOREVER))
    fails("cancelled", 1, owned:wait_for_members(fipc.CONSUMER, 2, support.TEN_SECONDS))
    producer:close()
    consumer:close()
    local again = assert(fipc.queue_join(name, fipc.CONSUMER, support.TEN_SECONDS)) -- joins still work
    again:close()
    owned:close()
    owned:close()
    assert(owned:closed())
    raises("the queue is closed", owned.wait_for_members, owned)
    raises("the queue is closed", owned.cancel, owned)
    owned = assert(fipc.queue_create(name, 4096, 1, 1)) -- the name is free again at once
    owned:close()
end)

test("the queue's close ends it; a member's close doesn't", function()
    local name = support.unique_name("q_end")
    local owned = assert(fipc.queue_create(name, 4096, 1, 1))
    local producer = assert(fipc.queue_join(name, fipc.PRODUCER, support.TEN_SECONDS))
    local consumer = assert(fipc.queue_join(name, fipc.CONSUMER, support.TEN_SECONDS))
    assert(producer:send("job 1", fipc.NO_WAIT))
    -- A member's close leaves the queue; it goes on, and the member's place is free again
    consumer:close()
    local deadline = support.now() + 10
    while owned:wait_for_members(fipc.CONSUMER) ~= 0 do
        assert(support.now() < deadline, "the count didn't drop within 10 s")
        support.sleep(1)
    end
    consumer = assert(fipc.queue_join(name, fipc.CONSUMER, support.TEN_SECONDS))
    owned:close()
    fails("disconnected", nil, producer:send("job 2", support.TEN_SECONDS))
    assert(producer:ended())
    eq(consumer:recv(support.TEN_SECONDS), "job 1") -- what is left, then the end
    fails("disconnected", nil, consumer:recv(support.TEN_SECONDS))
    assert(consumer:ended())
    producer:close()
    consumer:close()
end)

-- Creates a queue on `name` and returns a producer joined to it; the queue itself is dropped
local function producer_of_a_dropped_queue(name)
    local _ = assert(fipc.queue_create(name, 4096, 1, 1))
    return assert(fipc.queue_join(name, fipc.PRODUCER, support.TEN_SECONDS))
end

test("a queue the program drops goes on: the garbage collector never closes it", function()
    local name = support.unique_name("q_gc")
    local producer = producer_of_a_dropped_queue(name)
    collectgarbage()
    collectgarbage()
    assert(producer:send("job 1", support.TEN_SECONDS))
    local consumer = assert(fipc.queue_join(name, fipc.CONSUMER, support.TEN_SECONDS))
    eq(consumer:recv(support.TEN_SECONDS), "job 1")
    -- The queue holds its name until this process ends
    fails("addr_in_use", nil, fipc.queue_create(name, 4096, 1, 1))
    producer:close()
    consumer:close()
end)

-- Three producers and three consumers in this Lua state, interleaved, each consumer receiving its own way (copied,
-- into a buffer, in place); every message must come exactly once. The producers and consumers stay open.
local function interleave(producers, consumers, count)
    local buf = ffi.new("uint8_t[64]")
    local function receive(c)
        local consumer = consumers[c]
        if c == 1 then
            return consumer:recv(fipc.NO_WAIT)
        elseif c == 2 then
            local len, err = consumer:recv_into(buf, 64, fipc.NO_WAIT)
            return len and ffi.string(buf, len), err
        end
        local data, len = consumer:recv_acquire(fipc.NO_WAIT)
        if not data then
            return nil, len
        end
        local message = ffi.string(data, len)
        consumer:recv_release()
        return message
    end

    local sent, received, lists = { 0, 0, 0 }, 0, { {}, {}, {} }
    local deadline = support.now() + 20
    while received < 3 * count do
        for p = 1, 3 do
            if sent[p] < count then
                local ok, err = producers[p]:send("producer " .. p .. " job " .. sent[p] + 1, fipc.NO_WAIT)
                if ok then
                    sent[p] = sent[p] + 1
                else
                    eq(err, fipc.TIMEOUT, "producer " .. p .. "'s send") -- its ring is full
                end
            end
        end
        for c = 1, 3 do
            local message, err = receive(c)
            if message then
                lists[c][#lists[c] + 1] = message
                received = received + 1
            else
                eq(err, fipc.TIMEOUT, "consumer " .. c .. "'s receive")
            end
        end
        assert(support.now() < deadline, "only " .. received .. " messages received within 20 s")
    end

    local seen = {}
    for c, list in ipairs(lists) do
        assert(#list > 0, "consumer " .. c .. " took nothing")
        for _, message in ipairs(list) do
            local p, i = message:match("^producer (%d) job (%d+)$")
            p, i = tonumber(p), tonumber(i)
            assert(p and i and i >= 1 and i <= count, "a message of no producer: " .. message)
            assert(not seen[message], message .. " went to two consumers")
            seen[message] = true
        end
    end
    for _, consumer in ipairs(consumers) do
        fails("timeout", nil, consumer:recv(fipc.NO_WAIT)) -- nothing more
    end
end

-- Joins three producers and three consumers to `name`
local function join_three_each(name)
    local producers, consumers = {}, {}
    for i = 1, 3 do
        producers[i] = assert(fipc.queue_join(name, fipc.PRODUCER, support.TEN_SECONDS))
        consumers[i] = assert(fipc.queue_join(name, fipc.CONSUMER, support.TEN_SECONDS))
    end
    eq(producers[1]:max_piece(), 1024)
    eq(consumers[1]:max_piece(), 1024)
    return producers, consumers
end

-- After the queue's end: producers get "disconnected"; consumers receive "left over" (once, in all), then the end
local function check_the_end(producers, consumers)
    for p, producer in ipairs(producers) do
        fails("disconnected", nil, producer:send("after the end", support.TEN_SECONDS))
        producer:close()
        assert(producer:closed(), "producer " .. p)
    end
    local left = 0
    for _, consumer in ipairs(consumers) do
        while true do
            local message, err = consumer:recv(support.TEN_SECONDS)
            if not message then
                eq(err, fipc.DISCONNECTED)
                break
            end
            eq(message, "left over")
            left = left + 1
        end
        consumer:close()
    end
    eq(left, 1, "the message left in the queue")
end

test("the owner, three producers and three consumers in this Lua state: every message once", function()
    local name = support.unique_name("q_one")
    local owned = assert(fipc.queue_create(name, 4096, 3, 3))
    local producers, consumers = join_three_each(name)
    eq(assert(owned:wait_for_members(nil, 6, support.TEN_SECONDS)), 6)
    interleave(producers, consumers, 200) -- each producer's: more than its ring holds (64), so sends find it full
    assert(producers[1]:send("left over", support.TEN_SECONDS))
    owned:close()
    check_the_end(producers, consumers)
end)

test("the owner in another process; three producers and three consumers here: every message once", function()
    local name = support.unique_name("q_mix")
    local signal, close_owner = owner_signal()
    local owner = support.peer("queue_owner", name, "6", signal)
    local producers, consumers = join_three_each(name)
    interleave(producers, consumers, 200)
    -- The owner closes its queue: producers get "disconnected", consumers the end after the messages left
    assert(producers[1]:send("left over", support.TEN_SECONDS))
    close_owner()
    eq(owner:wait(), 0, "the owner peer's exit code")
    os.remove(signal)
    check_the_end(producers, consumers)
end)

test("a broadcast and a queue refuse each other's roles and names", function()
    local broadcast_name, queue_name = support.unique_name("q_kind_bc"), support.unique_name("q_kind_q")
    local publisher = assert(fipc.broadcast_create(broadcast_name, 4096, 1))
    local owned = assert(fipc.queue_create(queue_name, 4096, 1, 1))
    fails("invalid", nil, fipc.queue_join(broadcast_name, fipc.PRODUCER, support.TEN_SECONDS))
    fails("invalid", nil, fipc.queue_join(broadcast_name, fipc.CONSUMER, support.TEN_SECONDS))
    fails("invalid", nil, fipc.broadcast_join(queue_name, fipc.SUBSCRIBER_GATED, support.TEN_SECONDS))
    fails("invalid", nil, fipc.broadcast_join(queue_name, fipc.SUBSCRIBER_LOSSY, support.TEN_SECONDS))
    -- A stream's connect, from another process (in this one, connecting to its own names logs a warning)
    for _, other in ipairs({ broadcast_name, queue_name }) do
        eq(support.peer("connect_invalid", other):wait(), 0, "a stream's connect to " .. other)
    end
    local subscriber = assert(fipc.broadcast_join(broadcast_name, fipc.SUBSCRIBER_GATED, support.TEN_SECONDS))
    local consumer = assert(fipc.queue_join(queue_name, fipc.CONSUMER, support.TEN_SECONDS))
    publisher:close()
    owned:close()
    fails("disconnected", nil, subscriber:recv(support.TEN_SECONDS))
    fails("disconnected", nil, consumer:recv(support.TEN_SECONDS))
    subscriber:close()
    consumer:close()
end)

test("the owner's consumer here; two joined producers in other processes", function()
    local name = support.unique_name("q_own_c")
    local peers = {}
    for i = 1, 2 do
        peers[i] = support.peer("queue_joined_producer", name, "100")
    end
    queue.own_and_consume(name, 100, 2)
    for _, peer in ipairs(peers) do
        eq(peer:wait(), 0, "a producer peer's exit code")
    end
end)

-- Runs two queue_consumer peers on a queue the owner's producer here fills with `count` messages of `size(i)` bytes,
-- once both have joined, and returns each consumer's job numbers, checked against their lengths
local function two_consumer_peers(name, capacity, count, size)
    local owned = assert(fipc.queue_create(name, capacity, 1, 2))
    local producer = assert(fipc.queue_join(name, fipc.PRODUCER, queue.TIMEOUT))
    local outputs, peers = {}, {}
    for c = 1, 2 do
        outputs[c] = support.temp_file(".txt")
        peers[c] = support.spawn({ support.luajit, support.tests .. "/peer.lua", "queue_consumer", name },
            { stdout = outputs[c] })
    end
    eq(assert(owned:wait_for_members(fipc.CONSUMER, 2, queue.TIMEOUT)), 2)
    for i = 1, count do
        assert(producer:send(queue.long_job(i, size(i)), queue.TIMEOUT))
    end
    producer:close()
    owned:close() -- ends the queue: the consumers take what is left, then get "disconnected"
    local lists = {}
    for c = 1, 2 do
        eq(peers[c]:wait(), 0, "consumer peer " .. c .. "'s exit code")
        lists[c] = {}
        for number, len in support.read_file(outputs[c]):gmatch("(%d+) (%d+)") do
            number = tonumber(number)
            eq(tonumber(len), size(number), "job " .. number .. "'s length")
            lists[c][#lists[c] + 1] = number
        end
        os.remove(outputs[c])
    end
    return lists
end

test("the owner's producer here; two consumers in other processes split its jobs", function()
    local lists = two_consumer_peers(support.unique_name("q_own_p"), 4096, 300, function(i)
        return #queue.job(i)
    end)
    queue.check_split(lists, 300)
end)

test("two consumers race for messages longer than recv's first buffer: each received once and whole", function()
    -- Each message is longer than the one before, so a consumer's recv finds most of them too long for its buffer
    -- (4096 bytes at first) and receives again into a longer one, while the other consumer may take that message
    local lists = two_consumer_peers(support.unique_name("q_race"), 64 * 1024, 300, function(i)
        return 4097 + 40 * i -- up to 16097 bytes, under max_piece (16 KiB)
    end)
    queue.check_split(lists, 300)
end)

return suite
