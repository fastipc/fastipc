-- client.lua
local fipc = require("fipc")

local conn = assert(fipc.connect("my_channel", 5000)) -- waits up to 5 s for the server
assert(conn:rpc_submit(1, "ping")) -- opcode 1
print(assert(conn:rpc_recv()).payload) -- PING
conn:close()
