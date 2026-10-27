-- producer.lua
local fipc = require("fipc")

-- 4 KiB of messages per producer, 1 producer and 1 consumer: this process owns the queue and is its producer
local queue = assert(fipc.queue_create("my_queue", 4096, 1, 1))
local producer = assert(fipc.queue_join("my_queue", fipc.PRODUCER))
for i = 1, 3 do
    assert(producer:send("job " .. i)) -- waits in the queue until a consumer takes it
end
assert(queue:wait_for_members(fipc.CONSUMER, 1)) -- waits for the consumer to join
producer:close()
queue:close() -- ends the queue: the consumer gets "disconnected" after the jobs
