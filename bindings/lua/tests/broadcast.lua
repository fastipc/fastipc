--[[
A broadcast's two sides, as the tests run them here and in other processes (tests/peer.lua), in the shape of the
Python binding's broadcast_peer.py, so that either side can be the other language's: message i, from 0 to count - 1,
is the text `message <i>`; the broadcast has 4096 bytes and room for 8 subscribers; each step waits at most 20 s. A
failed check raises.
]]

local ffi = require("ffi")
local support = require("support")
local fipc = support.fipc

local broadcast = {}

local TIMEOUT = 20000
broadcast.TIMEOUT = TIMEOUT

function broadcast.message(i)
    return "message " .. i
end

--- Sends message i (every other one zero-copy): true, or nil and the error's name.
function broadcast.send(publisher, i, timeout)
    local message = broadcast.message(i)
    if i % 2 == 0 then
        return publisher:send(message, timeout)
    end
    local slot, err = publisher:send_acquire(#message, timeout)
    if not slot then
        return nil, err
    end
    ffi.copy(slot, message, #message)
    return publisher:send_commit(#message)
end

--[[ The publisher: claims `name`, waits until `subscribers` subscribers have joined (in other processes), sends the
count messages and closes, which ends the broadcast. ]]
function broadcast.publish(name, count, subscribers)
    local publisher = assert(fipc.broadcast_create(name, 4096, 8, TIMEOUT))
    support.eq(assert(publisher:wait_for_members(subscribers, TIMEOUT)), subscribers, "the subscriber count")
    for i = 0, count - 1 do
        assert(broadcast.send(publisher, i, TIMEOUT))
    end
    publisher:close()
end

-- One subscriber's receives and checks: receive() takes the next message or reports why not
local function subscriber(name, role, count)
    local gated = role == "gated"
    local conn = assert(fipc.broadcast_join(name, gated and fipc.SUBSCRIBER_GATED or fipc.SUBSCRIBER_LOSSY, TIMEOUT))
    local state = { conn = conn, expected = 0, received = 0, lost = 0, ended = false }
    local buf = ffi.new("uint8_t[64]")

    -- A gated subscriber takes its messages three ways in turn: copied, into a buffer, in place
    local function receive(timeout)
        local turn = state.received % 3
        if not gated or turn == 0 then
            return conn:recv(timeout)
        elseif turn == 1 then
            local len, err, extra = conn:recv_into(buf, 64, timeout)
            if not len then
                return nil, err, extra
            end
            return ffi.string(buf, len)
        end
        local data, len = conn:recv_acquire(timeout)
        if not data then
            return nil, len
        end
        local message = ffi.string(data, len)
        conn:recv_release()
        return message
    end

    -- Takes what has come: false when nothing has (yet)
    function state.step(timeout)
        local message, err, lost = receive(timeout)
        if message then
            support.eq(message, broadcast.message(state.expected), role)
            state.expected = state.expected + 1
            state.received = state.received + 1
        elseif err == fipc.DISCONNECTED then
            assert(conn:ended())
            support.eq(state.expected, count, role .. ": received " .. state.received .. " and lost " .. state.lost
                .. " of " .. count .. " messages")
            assert(not gated or state.received == count, "a gated subscriber lost messages")
            conn:close()
            state.ended = true
        elseif err == fipc.OVERRUN and not gated then
            assert(type(lost) == "number" and lost >= 1, tostring(lost))
            state.lost = state.lost + lost
            state.expected = state.expected + lost
        elseif err == fipc.TIMEOUT then
            return false
        else
            error(role .. ": " .. tostring(err))
        end
        return true
    end
    return state
end

--[[ The subscribers: joins `name` once for each of `roles` ("gated" or "lossy"), in order. Each one's step(timeout)
takes what has come (false when nothing has) and checks it: a gated one that it receives every message in order; a
lossy one that each message it receives is the one after the last, or that many after it as an "overrun" between them
reported lost, and at the broadcast's end, that received and lost add up to the count. ]]
function broadcast.join(name, roles, count)
    local subscribers = {}
    for i, role in ipairs(roles) do
        subscribers[i] = subscriber(name, role, count)
    end
    return subscribers
end

--- Steps every subscriber that hasn't seen the broadcast's end, without waiting: whether any took something, and how
--- many haven't seen the end.
function broadcast.step(subscribers)
    local busy, open = false, 0
    for _, s in ipairs(subscribers) do
        if not s.ended then
            busy = s.step(fipc.NO_WAIT) or busy
            open = open + (s.ended and 0 or 1)
        end
    end
    return busy, open
end

--- The subscribers (broadcast.join), receiving on all of them until each sees the broadcast's end.
function broadcast.subscribe(name, roles, count)
    local subscribers = broadcast.join(name, roles, count)
    local deadline = support.now() + TIMEOUT / 1000
    while true do
        local busy, open = broadcast.step(subscribers)
        if open == 0 then
            return
        end
        assert(support.now() < deadline, "the broadcast didn't end within 20 s")
        if not busy then
            support.sleep(1)
        end
    end
end

return broadcast
