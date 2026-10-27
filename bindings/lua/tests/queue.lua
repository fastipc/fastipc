--[[
A work queue's sides, as the tests run them here and in other processes (tests/peer.lua), in the shape of the Python
binding's queue_peer.py, so that either side can be the other language's: a producer sends job 1 to job <count>, the
texts `job <i>`; the owner's queue has rings of 4096 bytes and room for 8 producers and 8 consumers; each step waits at
most 20 s. A failed check raises.
]]

local support = require("support")
local fipc = support.fipc

local queue = {}

local TIMEOUT = 20000
queue.TIMEOUT = TIMEOUT
queue.CAPACITY = 4096
queue.MEMBERS = 8

function queue.job(i)
    return "job " .. i
end

--- Job `i` padded with dots to `size` bytes: a message that checks its own length.
function queue.long_job(i, size)
    local text = queue.job(i)
    return text .. string.rep(".", size - #text)
end

--- The number of a job, plain or padded with dots; nil for anything else.
function queue.job_number(message)
    return tonumber(message:match("^job (%d+)%.*$"))
end

function queue.send_jobs(producer, count)
    for i = 1, count do
        assert(producer:send(queue.job(i), TIMEOUT))
    end
end

--[[ That `received` (messages, in any order) is the jobs of `producers` producers, each sending job 1 to job
<count>: each job `producers` times. The queue promises no order. ]]
function queue.check_jobs(received, producers, count)
    local times = {}
    for _, message in ipairs(received) do
        local i = queue.job_number(message)
        assert(i and i >= 1 and i <= count and message == queue.job(i), "received " .. message .. ", no job")
        times[i] = (times[i] or 0) + 1
    end
    for i = 1, count do
        support.eq(times[i] or 0, producers, "the times job " .. i .. " came")
    end
end

--[[ That `lists` (each consumer's job numbers) split one producer's job 1 to job <count>: every job went to exactly
one consumer. ]]
function queue.check_split(lists, count)
    local taken = {}
    for c, numbers in ipairs(lists) do
        for _, i in ipairs(numbers) do
            assert(not taken[i], "job " .. i .. " went to two consumers, the second consumer " .. c)
            assert(i >= 1 and i <= count, "consumer " .. c .. " took job " .. i)
            taken[i] = true
        end
    end
    for i = 1, count do
        assert(taken[i], "job " .. i .. " went to no consumer")
    end
end

--[[ Receives on every consumer in turn, without waiting, until each gets "disconnected" (closing it then): each
consumer's messages, in the order it took them. ]]
function queue.receive_until_the_end(consumers)
    local lists, open = {}, #consumers
    for c = 1, #consumers do
        lists[c] = {}
    end
    local deadline = support.now() + TIMEOUT / 1000
    while open > 0 do
        local busy = false
        for c, consumer in ipairs(consumers) do
            if not consumer:closed() then
                local message, err = consumer:recv(fipc.NO_WAIT)
                if message then
                    lists[c][#lists[c] + 1] = message
                    busy = true
                elseif err == fipc.DISCONNECTED then
                    consumer:close()
                    open = open - 1
                else
                    support.eq(err, fipc.TIMEOUT, "consumer " .. c .. "'s receive")
                end
            end
        end
        assert(support.now() < deadline, "the queue didn't end within 20 s")
        if not busy then
            support.sleep(1)
        end
    end
    return lists
end

--[[ The owner and its producer, as the examples' producer: claims `name` and joins it as a producer, sends job 1 to
job <count> (more than its ring holds wait for a consumer to take some), waits for `consumers` consumers to have
joined (in other processes), and closes its producer and the queue, which ends it. ]]
function queue.produce(name, count, consumers)
    local owned = assert(fipc.queue_create(name, queue.CAPACITY, queue.MEMBERS, queue.MEMBERS))
    local producer = assert(fipc.queue_join(name, fipc.PRODUCER, TIMEOUT))
    queue.send_jobs(producer, count)
    local count, err = owned:wait_for_members(fipc.CONSUMER, consumers, TIMEOUT)
    assert(count and count >= consumers, "the consumers' count: " .. tostring(count or err))
    producer:close()
    owned:close()
end

--[[ The owner and its consumer: claims `name` and joins it as a consumer, receives job 1 to job <count> of each of
`producers` producers (they join in other processes), checks that each came once, and closes its consumer and the
queue, which ends it. ]]
function queue.own_and_consume(name, count, producers)
    local owned = assert(fipc.queue_create(name, queue.CAPACITY, queue.MEMBERS, queue.MEMBERS))
    local consumer = assert(fipc.queue_join(name, fipc.CONSUMER, TIMEOUT))
    local received = {}
    for i = 1, producers * count do
        received[i] = assert(consumer:recv(TIMEOUT))
    end
    queue.check_jobs(received, producers, count)
    consumer:close()
    owned:close()
end

return queue
