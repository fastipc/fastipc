-- subscriber.lua
local fipc = require("fipc")

-- gated: misses nothing (the publisher waits for it)
local subscriber = assert(fipc.broadcast_join("my_broadcast", fipc.SUBSCRIBER_GATED, 5000))
while true do
    local tick, err = subscriber:recv()
    if not tick then
        assert(err == fipc.DISCONNECTED, err) -- the publisher closed
        break
    end
    print(tick) -- tick 1, tick 2, tick 3
end
subscriber:close()
