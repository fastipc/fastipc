-- server.lua
local fipc = require("fipc")

local listener = assert(fipc.listen("my_channel", 1024 * 1024)) -- rings of 1 MiB each way
local conn = assert(listener:accept()) -- waits for a client
local request = assert(conn:rpc_recv())
assert(conn:rpc_respond(request.id, request.opcode, 0, request.payload:upper()))
conn:close()
listener:close()
