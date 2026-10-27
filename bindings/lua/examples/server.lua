-- server.lua
local fipc = require("fipc")

local listener = assert(fipc.stream_create("my_channel", 1024 * 1024)) -- rings of 1 MiB each way
local conn = assert(listener:accept()) -- waits for a client
local message = assert(conn:recv())
assert(conn:send(message:upper()))
conn:close()
listener:close()
