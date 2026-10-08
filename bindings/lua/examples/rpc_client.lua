-- rpc_client.lua
local fipc = require("fipc")

local UPPER = 1

local conn = assert(fipc.connect("my_channel", 5000))
local id = assert(conn:rpc_submit(UPPER, "ping"))
local reply = assert(conn:rpc_recv())
assert(reply.kind == fipc.RPC_RESPONSE and reply.id == id)
print(reply.status, reply.payload) -- 0	PING
conn:close()
