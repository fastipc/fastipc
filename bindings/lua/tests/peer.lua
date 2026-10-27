--[[
The tests' peer in another process: `luajit tests/peer.lua <role> <name> [arguments]`. Exit code 0 when the role
went as expected; an error ends it with another code.

  echo <name>               connects; sends back every message it receives until the peer's end
  send_and_exit <name> <n>  connects; sends "message 1" ... "message n", then exits
  hang <name>               connects, sends "ready", then waits until it is killed
  expect_end <name>         connects; receives until the peer's end ("disconnected"), and nothing else may come
  zerocopy_echo <name>      connects; receives each message in place (recv_acquire, or recv for one of several
                            pieces) and sends it back through send_acquire (send for a long one)

A broadcast's (tests/broadcast.lua says what the two sides check):
  publisher <name> <count> <subscribers>
                            claims the broadcast, waits for the subscribers, sends "message 0" ... and closes
  subscriber <name> <gated|lossy> <count>
                            joins, receives every message (or counts the lost ones) until the broadcast's end
  lap <name>                claims a broadcast of 4096 bytes for one subscriber, waits for it, sends 144 messages of 48
                            bytes (byte 1: the message's number, 0 to 143) without waiting, and closes
  stall <name> <file>       joins as a gated subscriber, receives nothing until <file> exists, then expects
                            "unsubscribed", twice

A work queue's (tests/queue.lua says what the jobs are):
  queue_consumer <name>     joins as a consumer and receives (recv) until the queue's end, printing each message's job
                            number and length ("<i> <len>"); each message must be a job, plain or padded with dots
  queue_joined_producer <name> <count>
                            joins as a producer, sends job 1 to job <count> and closes
  queue_owner <name> <members> <file>
                            claims a queue with no member of its own, waits for <members> producers and consumers to
                            join, and once <file> exists closes the queue, which ends it

A service's (tests/service.lua says what the requests are):
  service_server <name> <count> <clients>
                            creates the service and joins it as a server, answers <count> requests of each of
                            <clients> clients with their uppercase, and closes, which ends the service
  service_client <name> <count>
                            joins as a client, calls ping 1 to ping <count> and checks each reply
  service_owner_client <name> <count> <servers>
                            creates the service and joins it as a client, waits for <servers> servers, calls ping 1 to
                            ping <count>, checks each reply, and closes, which ends the service
  service_joined_server <name>
                            joins as a server and answers requests until the service's end
  service_quitter <name>    joins as a server, takes one request and closes without replying: its client gets
                            "abandoned"

Any kind's:
  connect_invalid <name>    connects as a stream's client to a broadcast's, a queue's or a service's name: it must
                            fail with "invalid"
]]

local here = arg[0]:match("^(.*)[/\\]") or "."
package.path = here .. "/?.lua;" .. here .. "/../?.lua;" .. package.path
local support = require("support")
local broadcast = require("broadcast")
local queue = require("queue")
local service = require("service")
local fipc = support.fipc
local ffi = require("ffi")

local role, name = arg[1], arg[2]
local TIMEOUT = 20000

local function connect()
    return assert(fipc.stream_connect(name, TIMEOUT))
end

-- Waits up to 20 s for a file to exist
local function wait_for_file(path)
    local deadline = support.now() + TIMEOUT / 1000
    local file = io.open(path, "rb")
    while not file do
        assert(support.now() < deadline, "no " .. path .. " within 20 s")
        support.sleep(5)
        file = io.open(path, "rb")
    end
    file:close()
end

-- Receives until the peer's end; fails on anything else
local function until_end(receive)
    while true do
        local value, err = receive()
        if value == nil then
            assert(err == fipc.DISCONNECTED, "expected the peer's end, got " .. tostring(err))
            return
        end
    end
end

if role == "echo" then
    local conn = connect()
    until_end(function()
        local message, err = conn:recv(TIMEOUT)
        if message then
            assert(conn:send(message, TIMEOUT))
        end
        return message, err
    end)
    conn:close()
elseif role == "send_and_exit" then
    local conn = connect()
    for i = 1, tonumber(arg[3]) do
        assert(conn:send("message " .. i, TIMEOUT))
    end
    conn:close()
elseif role == "hang" then
    local conn = connect()
    assert(conn:send("ready", TIMEOUT))
    while true do
        support.sleep(1000)
    end
elseif role == "expect_end" then
    local conn = connect()
    local message, err = conn:recv(TIMEOUT)
    assert(message == nil and err == fipc.DISCONNECTED, "expected the peer's end, got " .. tostring(err))
    assert(conn:ended())
    conn:close()
elseif role == "zerocopy_echo" then
    local conn = connect()
    local max = conn:max_piece()
    local held = ffi.new("uint8_t[?]", 4 * 1024 * 1024)
    until_end(function()
        local len
        local data, err, long = conn:recv_acquire(TIMEOUT)
        if data then
            ffi.copy(held, data, err)
            len = err
            conn:recv_release()
        elseif err == fipc.TOO_LARGE then
            len = assert(conn:recv_into(held, 4 * 1024 * 1024, TIMEOUT))
            assert(len == long)
        else
            return nil, err
        end
        if len <= max then
            local slot = assert(conn:send_acquire(len, TIMEOUT))
            ffi.copy(slot, held, len)
            assert(conn:send_commit(len))
        else
            assert(conn:send(held, len, TIMEOUT))
        end
        return true
    end)
    conn:close()
elseif role == "publisher" then
    broadcast.publish(name, tonumber(arg[3]), tonumber(arg[4]))
elseif role == "subscriber" then
    broadcast.subscribe(name, { arg[3] }, tonumber(arg[4]))
elseif role == "lap" then
    local publisher = assert(fipc.broadcast_create(name, 4096, 1, fipc.FOREVER))
    assert(publisher:wait_for_members(1, TIMEOUT))
    for i = 0, 143 do
        assert(publisher:send(string.char(i) .. string.rep("\0", 47), fipc.NO_WAIT))
    end
    publisher:close()
elseif role == "stall" then
    local conn = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_GATED, TIMEOUT))
    wait_for_file(arg[3])
    for _ = 1, 2 do
        local message, err = conn:recv(fipc.NO_WAIT)
        assert(message == nil and err == fipc.UNSUBSCRIBED, "expected unsubscribed, got " .. tostring(err))
    end
    conn:close()
elseif role == "queue_consumer" then
    local consumer = assert(fipc.queue_join(name, fipc.CONSUMER, TIMEOUT))
    until_end(function()
        local message, err = consumer:recv(TIMEOUT)
        if message then
            local number = assert(queue.job_number(message), "not a job: " .. message:sub(1, 40))
            print(number .. " " .. #message)
        end
        return message, err
    end)
    consumer:close()
elseif role == "queue_joined_producer" then
    local producer = assert(fipc.queue_join(name, fipc.PRODUCER, TIMEOUT))
    queue.send_jobs(producer, tonumber(arg[3]))
    producer:close()
elseif role == "queue_owner" then
    local owned = assert(fipc.queue_create(name, queue.CAPACITY, queue.MEMBERS, queue.MEMBERS))
    local members = tonumber(arg[3])
    local count, err = owned:wait_for_members(nil, members, TIMEOUT)
    assert(count and count >= members, "the members' count: " .. tostring(count or err))
    wait_for_file(arg[4])
    owned:close()
elseif role == "service_server" then
    service.serve(name, tonumber(arg[3]), tonumber(arg[4]))
elseif role == "service_client" then
    service.join_and_call(name, tonumber(arg[3]))
elseif role == "service_owner_client" then
    service.own_and_call(name, tonumber(arg[3]), tonumber(arg[4]))
elseif role == "service_joined_server" then
    service.join_and_answer(name)
elseif role == "service_quitter" then
    local server = assert(fipc.service_join(name, fipc.SERVER, TIMEOUT))
    assert(server:recv(TIMEOUT))
    server:close() -- holding the request: it is answered "abandoned"
elseif role == "connect_invalid" then
    local conn, err = fipc.stream_connect(name, TIMEOUT)
    assert(conn == nil and err == "invalid", "expected invalid, got " .. tostring(conn or err))
else
    error("unknown role " .. tostring(role))
end
