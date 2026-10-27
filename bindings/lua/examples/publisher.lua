-- publisher.lua
local fipc = require("fipc")

-- 64 KiB of messages, up to 8 subscribers; one that holds the publisher back 5 s is dropped
local publisher = assert(fipc.broadcast_create("my_broadcast", 64 * 1024, 8, 5000))
assert(publisher:wait_for_members(1)) -- waits for the first subscriber
for i = 1, 3 do
    assert(publisher:send("tick " .. i))
end
publisher:close() -- ends the broadcast: subscribers get "disconnected" after the ticks
