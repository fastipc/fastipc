-- consumer.lua
local fipc = require("fipc")

local consumer = assert(fipc.queue_join("my_queue", fipc.CONSUMER, 5000))
while true do
    local job, err = consumer:recv()
    if not job then
        assert(err == fipc.DISCONNECTED, err) -- the queue ended
        break
    end
    print(job) -- job 1, job 2, job 3
end
consumer:close()
