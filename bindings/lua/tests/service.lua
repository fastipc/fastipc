--[[
A service's sides, as the tests run them here and in other processes (tests/peer.lua), in the shape of the Python
binding's service_peer.py, so that either side can be the other language's: a client calls `count` times, ping 1 to
ping <count> (the texts `ping <i>`), one at a time, and checks that each reply is its request's uppercase, PING <i>;
the owner's service has rings of 4096 bytes, requests and replies of up to 256 bytes, and room for 8 clients and 8
servers; each step waits at most 20 s. A failed check raises.
]]

local support = require("support")
local fipc = support.fipc

local service = {}

local TIMEOUT = 20000
service.TIMEOUT = TIMEOUT
service.CAPACITY = 4096
service.MAX_MESSAGE = 256
service.MEMBERS = 8

function service.request(i)
    return "ping " .. i
end

--- Answers `count` requests, each with its uppercase.
function service.answer(server, count)
    for _ = 1, count do
        assert(server:send(assert(server:recv(TIMEOUT)):upper()))
    end
end

--- Answers requests until the service ends, and returns how many it answered.
function service.answer_until_the_end(server)
    local answered = 0
    while true do
        local request, err = server:recv(TIMEOUT)
        if not request then
            assert(err == fipc.DISCONNECTED, "a server's receive: " .. tostring(err))
            return answered
        end
        local ok, failed = server:send(request:upper())
        if not ok then -- the service ended between the request and its reply
            assert(failed == fipc.DISCONNECTED, "a server's reply: " .. tostring(failed))
            return answered
        end
        answered = answered + 1
    end
end

--- Calls ping 1 to ping <count> and checks each reply.
function service.call(client, count)
    for i = 1, count do
        local reply, err = client:call(service.request(i), TIMEOUT)
        assert(reply == service.request(i):upper(), "the reply to ping " .. i .. ": " .. tostring(reply or err))
    end
end

--[[ The owner and its server, as the examples' server: creates the service on `name` and joins it as a server,
answers `count` requests of each of `clients` clients, and closes its server and the service, which ends it. ]]
function service.serve(name, count, clients)
    local owned = assert(fipc.service_create(name, service.CAPACITY, service.MAX_MESSAGE, service.MEMBERS,
        service.MEMBERS))
    local server = assert(fipc.service_join(name, fipc.SERVER, TIMEOUT))
    service.answer(server, count * clients)
    server:close()
    owned:close()
end

--- A client: joins `name`, calls `count` times and checks each reply, then closes.
function service.join_and_call(name, count)
    local client = assert(fipc.service_join(name, fipc.CLIENT, TIMEOUT))
    service.call(client, count)
    client:close()
end

--[[ The owner and its client: creates the service on `name` and joins it as a client, waits for `servers` servers
to have joined (in other processes), calls `count` times, checks each reply, and closes its client and the service,
which ends it. ]]
function service.own_and_call(name, count, servers)
    local owned = assert(fipc.service_create(name, service.CAPACITY, service.MAX_MESSAGE, service.MEMBERS,
        service.MEMBERS))
    local client = assert(fipc.service_join(name, fipc.CLIENT, TIMEOUT))
    local joined, err = owned:wait_for_members(fipc.SERVER, servers, TIMEOUT)
    assert(joined and joined >= servers, "the servers' count: " .. tostring(joined or err))
    service.call(client, count)
    client:close()
    owned:close()
end

--- A joined server: answers requests until the service's end.
function service.join_and_answer(name)
    local server = assert(fipc.service_join(name, fipc.SERVER, TIMEOUT))
    service.answer_until_the_end(server)
    server:close()
end

return service
