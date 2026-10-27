-- client.lua
local fipc = require("fipc")

local conn = assert(fipc.stream_connect("my_channel", 5000)) -- waits up to 5 s for the server
assert(conn:send("ping"))
print(assert(conn:recv())) -- PING
conn:close()
