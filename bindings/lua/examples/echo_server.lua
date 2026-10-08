-- echo_server.lua: sends every message back until its client leaves
local fipc = require("fipc")

-- rings of 1 MiB each way: a power of two, 1 KiB to 2 GiB
local listener = assert(fipc.listen("demo", 1024 * 1024))
local conn = assert(listener:accept()) -- waits for a client
while true do
    local message, err = conn:recv()
    if message then
        assert(conn:send(message)) -- echo, any size
    elseif err == fipc.DISCONNECTED then
        break
    else
        error(err)
    end
end
print("client gone")
conn:close()
listener:close()
