-- The service: service_create, what it refuses and the member count, many clients and one server, a pool of servers
-- holding requests at once, each client's replies in the order of its requests, "abandoned" when a server closes
-- holding a request (here and in another process), the calls each role refuses and the zero-copy calls on both roles,
-- the other kinds' roles and names, call's owed replies after a timeout, its deadline and its checks, cancels (a call
-- after one sends nothing), the end at the service's close (and call after it), a service the program drops, messages
-- longer than recv's first buffer, and clients and servers in other processes.
-- A Lua state is one thread: a call that must get its reply has its server in another process. The Python binding's
-- side is in test_interop.lua.

local ffi = require("ffi")
local support = require("support")
local service = require("service")
local fipc = support.fipc
local eq, fails, raises = support.eq, support.fails, support.raises
local T = support.TEN_SECONDS

local suite = support.suite("service")
local test = suite.test

-- Fails unless the result is nil and the error `name`, with nothing after it
local function only(name, ...)
    fails(name, nil, ...)
    eq(select("#", ...), 2, name .. "'s values (nil and the name, nothing more)")
end

-- A service on a new name with a client and a server (or `servers` servers) joined from this Lua state
local function service_with(what, capacity, max_message, max_clients, servers)
    local name = support.unique_name(what)
    local owned = assert(fipc.service_create(name, capacity, max_message, max_clients, servers or 1))
    local client = assert(fipc.service_join(name, fipc.CLIENT, T))
    local joined = {}
    for i = 1, servers or 1 do
        joined[i] = assert(fipc.service_join(name, fipc.SERVER, T))
    end
    return name, owned, client, joined
end

local function close_all(...)
    for _, handle in ipairs({ ... }) do
        handle:close()
    end
end

test("service_create, the member count, and what it refuses", function()
    local name = support.unique_name("s_api")
    for _, args in ipairs({ { 1000, 64, 1, 1 }, { 4096, 0, 1, 1 }, { 4096, 1025, 1, 1 }, { 4096, 64, 0, 1 },
        { 4096, 64, 65, 1 }, { 4096, 64, 1, 0 }, { 4096, 64, 1, 65 } }) do
        fails("invalid", nil, fipc.service_create(name, unpack(args)))
    end
    raises("a number of clients or servers is a whole number", fipc.service_create, name, 4096, 64, "1", 1)
    raises("a max_message is a whole number", fipc.service_create, name, 4096, 6.5, 1, 1)
    raises("a capacity is a whole number", fipc.service_create, name, "4096", 64, 1, 1)
    raises("a name is a string", fipc.service_create, nil, 4096, 64, 1, 1)
    raises("a name is a string", fipc.service_join, 42, fipc.CLIENT)
    raises("a role is fipc.CLIENT or fipc.SERVER", fipc.service_join, name, "client")

    local owned = assert(fipc.service_create(name, 4096, 64, 2, 3))
    eq(owned.name, name)
    fails("addr_in_use", nil, fipc.service_create(name, 4096, 64, 1, 1))
    fails("addr_in_use", nil, fipc.queue_create(name, 4096, 1, 1))
    fails("addr_in_use", nil, fipc.broadcast_create(name, 4096, 1))
    fails("addr_in_use", nil, fipc.stream_create(name, support.RING))

    -- The count: at once, a poll, a wait that times out, over a role's maximum, another role
    eq(owned:wait_for_members(), 0)
    eq(owned:wait_for_members(fipc.CLIENT), 0)
    eq(owned:wait_for_members(fipc.SERVER), 0)
    fails("timeout", 0, owned:wait_for_members(nil, 1, fipc.NO_WAIT))
    local t0 = support.now()
    fails("timeout", 0, owned:wait_for_members(fipc.SERVER, 1, 50))
    local waited = (support.now() - t0) * 1000
    assert(waited >= 40 and waited < 5000, "waited " .. waited .. " ms")
    only("invalid", owned:wait_for_members(fipc.CLIENT, 3))
    only("invalid", owned:wait_for_members(fipc.SERVER, 4))
    only("invalid", owned:wait_for_members(nil, 6))
    fails("timeout", 0, owned:wait_for_members(nil, 5, fipc.NO_WAIT)) -- both roles: up to 2 + 3
    only("invalid", owned:wait_for_members(fipc.PRODUCER, 0))
    raises("a service's role is fipc.CLIENT, fipc.SERVER or nil", owned.wait_for_members, owned, "server")
    raises("a number of clients or servers is a whole number", owned.wait_for_members, owned, nil, -1)

    -- The other kinds' roles, and unknown ones
    for _, role in ipairs({ fipc.SUBSCRIBER_LOSSY, fipc.SUBSCRIBER_GATED, fipc.PRODUCER, fipc.CONSUMER, 7 }) do
        fails("invalid", nil, fipc.service_join(name, role, fipc.NO_WAIT))
    end

    -- Joined from this Lua state; the count goes up, and a role with no free place waits
    local client = assert(fipc.service_join(name, fipc.CLIENT, T))
    local server = assert(fipc.service_join(name, fipc.SERVER, T))
    eq(assert(owned:wait_for_members(nil, 2, T)), 2)
    eq(owned:wait_for_members(fipc.CLIENT), 1)
    fails("timeout", 1, owned:wait_for_members(fipc.CLIENT, 2, 50))
    local second = assert(fipc.service_join(name, fipc.CLIENT, T))
    fails("timeout", nil, fipc.service_join(name, fipc.CLIENT, 100)) -- both clients' places are held

    -- The cancel: a wait, now or later, returns "cancelled" with the count; the service goes on
    owned:cancel()
    fails("cancelled", 3, owned:wait_for_members(nil, 5, fipc.FOREVER))
    fails("cancelled", 1, owned:wait_for_members(fipc.SERVER, 2, T))
    close_all(client, second, server)
    local again = assert(fipc.service_join(name, fipc.SERVER, T)) -- joins still work
    again:close()
    owned:close()
    owned:close()
    assert(owned:closed())
    raises("the service is closed", owned.wait_for_members, owned)
    raises("the service is closed", owned.cancel, owned)
    owned = assert(fipc.service_create(name, 4096, 64, 1, 1)) -- the name is free again at once
    owned:close()
end)

test("eight clients and one server in this Lua state: each reply to its own client, in the order of its requests",
    function()
        local name = support.unique_name("s_many")
        local owned = assert(fipc.service_create(name, 4096, 64, 8, 1))
        local clients = {}
        for k = 1, 8 do
            clients[k] = assert(fipc.service_join(name, fipc.CLIENT, T))
        end
        local server = assert(fipc.service_join(name, fipc.SERVER, T))
        eq(assert(owned:wait_for_members(nil, 9, T)), 9)
        eq(server:max_piece(), 64)
        eq(clients[1]:max_piece(), 64)
        local taken = {} -- each client's requests, in the order the server took them
        for round = 0, 29 do -- ten requests of each client in flight, then the server answers them, then the replies
            for k, client in ipairs(clients) do
                for i = round * 10 + 1, round * 10 + 10 do
                    assert(client:send("client " .. k .. " request " .. i, T))
                end
            end
            for _ = 1, 80 do
                local request = assert(server:recv(T))
                local k, i = request:match("^client (%d) request (%d+)$")
                k, i = tonumber(k), tonumber(i)
                eq(i, (taken[k] or 0) + 1, "the request the server took of client " .. tostring(k))
                taken[k] = i
                assert(server:send(request:upper()))
            end
            fails("timeout", nil, server:recv(fipc.NO_WAIT))
            for k, client in ipairs(clients) do
                for i = round * 10 + 1, round * 10 + 10 do
                    eq(client:recv(T), "CLIENT " .. k .. " REQUEST " .. i)
                end
                fails("timeout", nil, client:recv(fipc.NO_WAIT))
            end
        end
        owned:close() -- ends the service: the server's receive is "disconnected"
        fails("disconnected", nil, server:recv(T))
        assert(server:ended())
        close_all(server, unpack(clients))
    end)

test("a pool of four servers holds four requests at once; each client's replies come in order", function()
    local name = support.unique_name("s_pool")
    local owned = assert(fipc.service_create(name, 16384, 64, 4, 4))
    local servers, clients = {}, {}
    for k = 1, 4 do
        servers[k] = assert(fipc.service_join(name, fipc.SERVER, T))
        clients[k] = assert(fipc.service_join(name, fipc.CLIENT, T))
    end
    eq(assert(owned:wait_for_members(fipc.SERVER, 4, T)), 4)
    for k, client in ipairs(clients) do
        assert(client:send("first of " .. k, T))
        assert(client:send("second of " .. k, T))
    end
    local held = {}
    for s, server in ipairs(servers) do
        held[s] = assert(server:recv(T)) -- every server holds one
        fails("invalid", nil, server:recv(fipc.NO_WAIT)) -- and only one
    end
    for s = 4, 1, -1 do -- the last taken is answered first
        assert(servers[s]:send(held[s]:upper()))
    end
    local seen = {}
    for s = 1, 4 do -- the second requests: each server takes one more
        local request = assert(servers[s]:recv(T))
        assert(not seen[request], request .. " taken twice")
        seen[request] = true
        assert(servers[s]:send(request:upper()))
    end
    for k, client in ipairs(clients) do
        eq(client:recv(T), "FIRST OF " .. k)
        eq(client:recv(T), "SECOND OF " .. k)
    end
    owned:close()
    close_all(unpack(servers))
    close_all(unpack(clients))
end)

test("a client's replies come in the order of its requests, whoever replies first", function()
    local _, owned, client, servers = service_with("s_order", 4096, 64, 1, 2)
    local slow, fast = servers[1], servers[2]
    assert(client:send("first", T))
    assert(client:send("second", T))
    eq(slow:recv(T), "first") -- each client's requests are taken in the order it sent them
    eq(fast:recv(T), "second")
    assert(fast:send("SECOND"))
    fails("timeout", nil, client:recv(50)) -- the reply to the first request comes first: it isn't there yet
    assert(slow:send("FIRST"))
    eq(client:recv(T), "FIRST")
    eq(client:recv(T), "SECOND")
    fails("timeout", nil, client:recv(fipc.NO_WAIT))
    close_all(client, slow, fast, owned)
end)

test("a server that closes holding a request abandons it; the next request is answered", function()
    local name = support.unique_name("s_abandon")
    local owned = assert(fipc.service_create(name, 4096, 64, 1, 2))
    local client = assert(fipc.service_join(name, fipc.CLIENT, T))
    local other = assert(fipc.service_join(name, fipc.SERVER, T)) -- takes nothing until the end
    local buf = ffi.new("uint8_t[64]")
    -- The client's receives each way: copied, into a buffer, in place
    local receives = {
        function() return client:recv(T) end,
        function() return client:recv_into(buf, 64, T) end,
        function() return client:recv_acquire(T) end,
    }
    for way, receive in ipairs(receives) do
        local quitter = assert(fipc.service_join(name, fipc.SERVER, T))
        assert(client:send("request " .. way, T))
        if way == 3 then
            local data, len = assert(quitter:recv_acquire(T)) -- held in place, then the close
            eq(ffi.string(data, len), "request 3")
        else
            eq(quitter:recv(T), "request " .. way)
        end
        quitter:close() -- holding it: the request is answered "abandoned"
        only("abandoned", receive())
        fails("timeout", nil, client:recv(fipc.NO_WAIT)) -- answered once: nothing more is owed
    end

    -- A call that gave up on a reply that is then abandoned: the next call drops the answer and gets its own reply
    local quitter = assert(fipc.service_join(name, fipc.SERVER, T))
    fails("timeout", nil, client:call("two", 50))
    eq(quitter:recv(T), "two")
    quitter:close()
    fails("timeout", nil, client:call("three", 100)) -- drops two's answer, sends three, gives up on its reply
    eq(other:recv(T), "three")
    assert(other:send("THREE"))
    eq(client:recv(T), "THREE")
    close_all(client, other, owned)
end)

test("call returns \"abandoned\" when the server in another process closes holding its request", function()
    local name = support.unique_name("s_quit")
    local owned = assert(fipc.service_create(name, 4096, 64, 1, 1))
    local client = assert(fipc.service_join(name, fipc.CLIENT, T))
    local peer = support.peer("service_quitter", name)
    only("abandoned", client:call("doomed", 20000))
    eq(peer:wait(), 0, "the quitter's exit code")
    -- The next call gets its own reply, from a server in another process
    local server = support.peer("service_joined_server", name)
    eq(client:call("next", 20000), "NEXT")
    owned:close() -- ends the service: the server peer's receive is "disconnected", and it exits
    eq(server:wait(), 0, "the server peer's exit code")
    client:close()
end)

test("each role's calls: what it can't use is \"invalid\"; max_piece is max_message; zero-copy both ways", function()
    local _, owned, client, servers = service_with("s_roles", 16384, 1000, 1, 1)
    local server = servers[1]
    eq(client:max_piece(), 1000)
    eq(server:max_piece(), 1000)
    fails("too_large", nil, client:send(string.rep("x", 1001)))
    fails("invalid", nil, client:send(""))
    fails("invalid", nil, client:call(""))
    fails("too_large", nil, client:call(string.rep("x", 1001)))
    fails("too_large", nil, client:send_acquire(1001))
    fails("invalid", nil, client:send_commit(1)) -- no reservation
    fails("timeout", nil, client:recv(fipc.NO_WAIT)) -- no reply to come
    only("invalid", client:wait_for_members()) -- a broadcast publisher's call
    client:recv_release() -- nothing held: does nothing
    fails("invalid", nil, server:send("x")) -- no request held
    fails("invalid", nil, server:send_acquire(1))
    fails("invalid", nil, server:send_commit(1))
    fails("timeout", nil, server:recv(fipc.NO_WAIT))
    only("invalid", server:wait_for_members())
    only("invalid", server:call("x")) -- call is a client's
    raises("a message is a string", client.call, client, 42)

    -- A server holds one request: its receives are "invalid" until it replies; recv_release does nothing
    assert(client:send(string.rep("\7", 1000), T))
    fails("too_large", 1000, server:recv_into(ffi.new("uint8_t[999]"), 999, T)) -- takes nothing
    local data, len = assert(server:recv_acquire(T))
    eq(len, 1000)
    fails("invalid", nil, server:recv(fipc.NO_WAIT))
    fails("invalid", nil, server:recv_acquire(fipc.NO_WAIT))
    server:recv_release() -- does nothing: the request is still held
    fails("too_large", nil, server:send(string.rep("x", 1001)))
    local room = assert(server:send_acquire(1000))
    ffi.fill(room, 1000, 3)
    eq(ffi.string(data, len), string.rep("\7", 1000)) -- the request held in place stays whole until the commit
    fails("invalid", nil, server:send_commit(1001))
    assert(server:send_commit(1000))
    fails("invalid", nil, server:send("x")) -- nothing held any more
    fails("too_large", 1000, client:recv_into(ffi.new("uint8_t[10]"), 10, T)) -- takes nothing
    eq(client:recv(T), string.rep("\3", 1000))

    -- The zero-copy calls the other way: a request committed in place, its reply received in place
    local slot = assert(client:send_acquire(5, T))
    ffi.copy(slot, "hello", 5)
    assert(client:send_commit(5))
    eq(server:recv(T), "hello")
    assert(server:send("HELLO"))
    local reply, reply_len = assert(client:recv_acquire(T))
    eq(ffi.string(reply, reply_len), "HELLO")
    client:recv_release()
    fails("timeout", nil, client:recv(fipc.NO_WAIT))
    eq(owned:wait_for_members(fipc.CLIENT), 1)
    eq(owned:wait_for_members(fipc.SERVER), 1)

    -- call on the other kinds' connections, and on a closed one
    local queue_name = support.unique_name("s_roles_q")
    local queue = assert(fipc.queue_create(queue_name, 4096, 1, 1))
    local producer = assert(fipc.queue_join(queue_name, fipc.PRODUCER, T))
    only("invalid", producer:call("x"))
    close_all(producer, queue, client, server, owned)
    raises("the connection is closed", client.call, client, "x")
end)

test("a service, a queue and a broadcast refuse each other's roles and names", function()
    local service_name = support.unique_name("s_kind_s")
    local queue_name, broadcast_name = support.unique_name("s_kind_q"), support.unique_name("s_kind_bc")
    local owned = assert(fipc.service_create(service_name, 4096, 64, 1, 1))
    local queue = assert(fipc.queue_create(queue_name, 4096, 1, 1))
    local publisher = assert(fipc.broadcast_create(broadcast_name, 4096, 1))
    fails("invalid", nil, fipc.broadcast_join(service_name, fipc.SUBSCRIBER_GATED, T))
    fails("invalid", nil, fipc.queue_join(service_name, fipc.CONSUMER, T))
    fails("invalid", nil, fipc.queue_join(service_name, fipc.CLIENT, T))
    for _, role in ipairs({ fipc.CLIENT, fipc.SERVER }) do
        fails("invalid", nil, fipc.service_join(queue_name, role, T))
        fails("invalid", nil, fipc.service_join(broadcast_name, role, T))
    end
    -- A stream's connect, from another process (in this one, connecting to its own names logs a warning)
    eq(support.peer("connect_invalid", service_name):wait(), 0, "a stream's connect to a service")
    -- A join learns the kind once the stream's server accepts it; while no accept runs, it times out
    local stream_name = support.unique_name("s_kind_st")
    local listener = assert(fipc.stream_create(stream_name, support.RING))
    fails("timeout", nil, fipc.service_join(stream_name, fipc.CLIENT, fipc.NO_WAIT))
    local client = assert(fipc.service_join(service_name, fipc.CLIENT, T)) -- the service goes on
    close_all(listener, client, publisher, queue, owned)
end)

test("call's owed replies: one that gave up leaves its reply owed, and the next call drops it", function()
    local _, owned, client, servers = service_with("s_owed", 4096, 64, 1, 1)
    local server = servers[1]
    fails("timeout", nil, client:call("late", 50)) -- the request went: no server replied in time
    eq(server:recv(T), "late")
    assert(server:send("LATE")) -- the late reply, owed to the call that gave up
    fails("timeout", nil, client:call("next", fipc.NO_WAIT)) -- drops LATE, sends next, gives up on its reply
    eq(server:recv(T), "next")
    assert(server:send("NEXT"))
    eq(client:recv(T), "NEXT") -- a receive of the caller's own takes the reply owed, in request order: not LATE
    fails("timeout", nil, client:recv(fipc.NO_WAIT)) -- nothing more owed

    -- A call whose owed reply hasn't come waits for it within its own timeout, and gives up before it sends
    fails("timeout", nil, client:call("a", fipc.NO_WAIT)) -- a went; its reply is owed
    local t0 = support.now()
    fails("timeout", nil, client:call("b", 50)) -- a's reply hasn't come: b never goes
    local waited = (support.now() - t0) * 1000
    assert(waited >= 40 and waited < 5000, "waited " .. waited .. " ms")
    eq(server:recv(T), "a")
    assert(server:send("A"))
    fails("timeout", nil, server:recv(fipc.NO_WAIT)) -- no b

    -- A request of send's whose reply hasn't been received: call refuses, and leaves it for recv
    assert(client:send("mine", T))
    only("invalid", client:call("x", T))
    eq(client:recv(T), "A") -- the reply owed to the call that gave up comes first
    eq(server:recv(T), "mine")
    assert(server:send("MINE"))
    eq(client:recv(T), "MINE")
    local slot = assert(client:send_acquire(4, T)) -- a request of send_commit's: the same
    ffi.copy(slot, "ours", 4)
    assert(client:send_commit(4))
    only("invalid", client:call("x", T))
    eq(server:recv(T), "ours")
    assert(server:send("OURS"))
    eq(client:recv(T), "OURS")

    -- A call whose send fails isn't owed a reply
    fails("too_large", nil, client:call(string.rep("x", 65), T))
    fails("timeout", nil, client:call("y", fipc.NO_WAIT)) -- nothing to drop: y goes at once
    eq(server:recv(T), "y")
    assert(server:send("Y"))
    eq(client:recv(T), "Y")
    fails("timeout", nil, client:recv(fipc.NO_WAIT))
    close_all(client, server, owned)
end)

test("a call whose deadline ran out while it dropped an owed reply returns \"timeout\" without sending", function()
    local _, owned, client, servers = service_with("s_deadline", 262144, 65536, 1, 1)
    local server = servers[1]
    fails("timeout", nil, client:call("first", 50)) -- the request went: no server replied in time
    eq(server:recv(T), "first")
    assert(server:send(string.rep("L", 60000))) -- the late reply is in: the next call's drop needn't wait
    -- A deadline of a nanosecond is past once the call has dropped tens of KiB: its request never goes
    only("timeout", client:call("second", 1e-6))
    fails("timeout", nil, server:recv(50)) -- no server saw second
    fails("timeout", nil, client:call("third", fipc.NO_WAIT)) -- nothing to drop: third goes at once
    eq(server:recv(T), "third")
    assert(server:send("THIRD"))
    eq(client:recv(T), "THIRD") -- its own reply: nothing of second's is owed
    fails("timeout", nil, client:recv(fipc.NO_WAIT))
    close_all(client, server, owned)
end)

test("a call checks its request first: \"invalid\" and \"too_large\" at once, even while a reply is owed", function()
    local _, owned, client, servers = service_with("s_check_first", 4096, 64, 1, 1)
    local server = servers[1]
    fails("timeout", nil, client:call("held", 50)) -- its reply is owed, and the server holds it without replying
    eq(server:recv(T), "held")
    local t0 = support.now()
    only("invalid", client:call("", fipc.FOREVER)) -- no wait for the owed reply, which would never come
    only("invalid", client:call(ffi.new("uint8_t[1]"), 0, fipc.FOREVER))
    only("too_large", client:call(string.rep("x", client:max_piece() + 1), fipc.FOREVER))
    raises("a message is a string", client.call, client, 42, fipc.FOREVER) -- raised at once, nothing counted
    assert(support.now() - t0 < 1, "the checks waited")
    assert(server:send("HELD")) -- still owed: the next call drops it
    fails("timeout", nil, client:call("after", fipc.NO_WAIT)) -- drops HELD, sends after
    eq(server:recv(T), "after")
    assert(server:send("AFTER"))
    eq(client:recv(T), "AFTER")
    fails("timeout", nil, client:recv(fipc.NO_WAIT)) -- nothing more owed
    close_all(client, server, owned)
end)

test("a cancel ends a client's and a server's waits; a cancelled call leaves its reply owed", function()
    local name, owned, client, servers = service_with("s_cancel", 4096, 64, 2, 1)
    local server = servers[1]
    -- The native cancel, as a thread holding the handle would cancel the connection while a call of this state waits:
    -- the call's request went, and its wait for the reply was cancelled (after client:cancel(), a call sends nothing)
    fipc.C.fipc_cancel(client._handle)
    local t0 = support.now()
    fails("cancelled", nil, client:call("cancelled", fipc.FOREVER)) -- it went; its wait for the reply was cancelled
    assert(support.now() - t0 < 5)
    eq(server:recv(T), "cancelled")
    assert(server:send("CANCELLED")) -- owed to the cancelled call
    eq(client:recv(fipc.NO_WAIT), "CANCELLED") -- a receive that needn't wait still works
    fails("cancelled", nil, client:recv(T)) -- final
    client:close()

    client = assert(fipc.service_join(name, fipc.CLIENT, T)) -- the server goes on
    server:cancel()
    fails("cancelled", nil, server:recv(fipc.FOREVER))
    assert(client:send("after", T))
    eq(server:recv(T), "after") -- a request there: the receive needn't wait
    assert(server:send("AFTER")) -- a reply never waits
    eq(client:recv(T), "AFTER")
    fails("cancelled", nil, server:recv(T)) -- final
    close_all(client, server, owned)
end)

test("a call after cancel() returns \"cancelled\" at once and sends nothing", function()
    local _, owned, client, servers = service_with("s_call_cancelled", 4096, 64, 1, 1)
    local server = servers[1]
    client:cancel()
    only("invalid", client:call("", T)) -- the request's checks come first
    only("too_large", client:call(string.rep("x", 65), T))
    for _, timeout in ipairs({ fipc.FOREVER, fipc.NO_WAIT, T }) do
        only("cancelled", client:call("never sent", timeout))
    end
    fails("timeout", nil, server:recv(200)) -- no request went
    close_all(client, server, owned)
end)

test("the service's close ends it for clients and servers", function()
    local name = support.unique_name("s_end")
    local owned = assert(fipc.service_create(name, 4096, 64, 1, 2))
    local client = assert(fipc.service_join(name, fipc.CLIENT, T))
    local server = assert(fipc.service_join(name, fipc.SERVER, T))
    local holder = assert(fipc.service_join(name, fipc.SERVER, T))
    assert(client:send("done", T))
    assert(client:send("held", T))
    assert(server:send(assert(server:recv(T)):upper()))
    eq(holder:recv(T), "held")
    owned:close()
    owned:close() -- a second close does nothing
    fails("disconnected", nil, client:send("after", T))
    assert(client:ended())
    fails("disconnected", nil, server:recv(T))
    eq(client:recv(T), "DONE") -- the replies that came before the end, then "disconnected"
    fails("disconnected", nil, client:recv(T))
    only("invalid", client:call("after", T)) -- held's reply is still owed to its send: never to come, but the caller's
    fails("disconnected", nil, holder:send("HELD")) -- a reply after the end
    owned = assert(fipc.service_create(name, 4096, 64, 1, 1)) -- the name is free again at once
    close_all(owned, client, server, holder)
end)

-- Creates a service on `name` and returns a server joined to it; the service itself is dropped
test("call after the service's end: \"disconnected\" from the drop and from the send", function()
    local name = support.unique_name("s_call_end")
    local owned = assert(fipc.service_create(name, 4096, 64, 2, 1))
    local gave_up = assert(fipc.service_join(name, fipc.CLIENT, T))
    local idle = assert(fipc.service_join(name, fipc.CLIENT, T))
    local server = assert(fipc.service_join(name, fipc.SERVER, T))
    fails("timeout", nil, gave_up:call("held", 50))
    eq(server:recv(T), "held") -- held, never replied to
    owned:close()
    only("disconnected", gave_up:call("next", T)) -- its drop: the owed reply never comes
    assert(gave_up:ended())
    only("disconnected", idle:call("next", T)) -- nothing owed: its send
    assert(idle:ended())
    only("invalid", idle:call("", T)) -- the request is checked first, end or not
    close_all(gave_up, idle, server)
end)

local function server_of_a_dropped_service(name)
    local _ = assert(fipc.service_create(name, 4096, 64, 1, 1))
    return assert(fipc.service_join(name, fipc.SERVER, T))
end

test("a service the program drops goes on: the garbage collector never closes it", function()
    local name = support.unique_name("s_gc")
    local server = server_of_a_dropped_service(name)
    collectgarbage()
    collectgarbage()
    local client = assert(fipc.service_join(name, fipc.CLIENT, T))
    assert(client:send("still here", T))
    assert(server:send(assert(server:recv(T)):upper()))
    eq(client:recv(T), "STILL HERE")
    -- The service holds its name until this process ends
    fails("addr_in_use", nil, fipc.service_create(name, 4096, 64, 1, 1))
    close_all(client, server)
end)

test("requests and replies longer than recv's first buffer are received whole on both sides", function()
    local _, owned, client, servers = service_with("s_large", 65536, 16384, 1, 1)
    local server = servers[1]
    local request = support.pattern(16384) -- more than recv's first 4 KiB
    assert(client:send(request, T))
    eq(server:recv(T), request)
    assert(server:send(request:reverse()))
    eq(client:recv(T), request:reverse())
    close_all(client, server, owned)
end)

test("a server in another process; the client here calls", function()
    local name = support.unique_name("s_srv_peer")
    local peer = support.peer("service_server", name, "300", "1")
    service.join_and_call(name, 300)
    eq(peer:wait(), 0, "the server peer's exit code")
end)

test("the owner's server here; a client in another process", function()
    local name = support.unique_name("s_cli_peer")
    local peer = support.peer("service_client", name, "300")
    service.serve(name, 300, 1)
    eq(peer:wait(), 0, "the client peer's exit code")
end)

test("the owner's server here; two clients in other processes", function()
    local name = support.unique_name("s_clis_peer")
    local peers = {}
    for i = 1, 2 do
        peers[i] = support.peer("service_client", name, "300")
    end
    service.serve(name, 300, 2)
    for _, peer in ipairs(peers) do
        eq(peer:wait(), 0, "a client peer's exit code")
    end
end)

test("the owner's client here; two joined servers in other processes", function()
    local name = support.unique_name("s_jsrv_peer")
    local peers = {}
    for i = 1, 2 do
        peers[i] = support.peer("service_joined_server", name)
    end
    service.own_and_call(name, 300, 2)
    for _, peer in ipairs(peers) do
        eq(peer:wait(), 0, "a server peer's exit code")
    end
end)

test("the owner's client in another process; a joined server here", function()
    local name = support.unique_name("s_ocli_peer")
    local peer = support.peer("service_owner_client", name, "300", "1")
    service.join_and_answer(name)
    eq(peer:wait(), 0, "the owner peer's exit code")
end)

return suite
