--[[
The tests' peer in another process: `luajit tests/peer.lua <role> <name> [arguments]`. Exit code 0 when the role
went as expected; an error ends it with another code.

  echo <name>               connects; sends back every message it receives until the peer's end
  echo_rpc <name>           connects; answers every request with its payload reversed, status = its length, until
                            the peer's end
  send_and_exit <name> <n>  connects; sends "message 1" ... "message n", then exits
  hang <name>               connects, then waits until it is killed
  expect_end <name>         connects; receives until the peer's end ("disconnected"), and nothing else may come
  rpc_server <name>         listens with 64 KiB rings; accepts one client and answers every request in upper case,
                            status -1, until its end
  rpc_statuses <name>       connects; answers four requests with the statuses 0, -1, 2^31 - 1 and -2^31, no payload
  zerocopy_echo <name>      connects; receives each message in place (recv_acquire, or recv for one of several
                            pieces) and sends it back through send_acquire (send for a long one)
]]

local here = arg[0]:match("^(.*)[/\\]") or "."
package.path = here .. "/?.lua;" .. here .. "/../?.lua;" .. package.path
local support = require("support")
local fipc = support.fipc
local ffi = require("ffi")

local role, name = arg[1], arg[2]
local TIMEOUT = 20000

local function connect()
    return assert(fipc.connect(name, TIMEOUT))
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
elseif role == "echo_rpc" then
    local conn = connect()
    until_end(function()
        local request, err = conn:rpc_recv(TIMEOUT)
        if request then
            assert(request.kind == fipc.RPC_REQUEST)
            assert(conn:rpc_respond(request.id, request.opcode, #request.payload, request.payload:reverse(), TIMEOUT))
        end
        return request, err
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
elseif role == "rpc_server" then
    local listener = assert(fipc.listen(name, 64 * 1024))
    local conn = assert(listener:accept(TIMEOUT))
    until_end(function()
        local request, err = conn:rpc_recv(TIMEOUT)
        if request then
            assert(conn:rpc_respond(request.id, request.opcode, -1, request.payload:upper(), TIMEOUT))
        end
        return request, err
    end)
    conn:close()
    listener:close()
elseif role == "rpc_statuses" then
    local conn = connect()
    for _, status in ipairs({ 0, -1, 2147483647, -2147483648 }) do
        local request = assert(conn:rpc_recv(TIMEOUT))
        assert(conn:rpc_respond(request.id, request.opcode, status, nil, TIMEOUT))
    end
    until_end(function() return conn:rpc_recv(TIMEOUT) end)
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
else
    error("unknown role " .. tostring(role))
end
