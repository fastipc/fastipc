-- The documentation's Lua blocks that aren't examples (examples/*.lua), as code that runs: each block of the READMEs
-- and of the website appears here (or in an example) line for line, which tests/test_docs.lua checks.

local ffi = require("ffi")
local support = require("support")
local fipc = support.fipc
local eq = support.eq

local suite = support.suite("snippets")
local test = suite.test

-- A connection to an echo peer in another process
local function echo_pair()
    local name = support.unique_name("snippet")
    local listener = assert(fipc.stream_create(name, support.RING))
    local peer = support.peer("echo", name)
    local conn = assert(listener:accept(support.TEN_SECONDS))
    return conn, function()
        conn:close()
        listener:close()
        eq(peer:wait(), 0, "the peer's exit code")
    end
end

test("errors and timeouts", function()
    local conn, finish = echo_pair()
    local handled = {}
    local function handle(message)
        handled[#handled + 1] = message
    end
    for _, sent in ipairs({ false, true }) do
        if sent then
            assert(conn:send("hello", support.TEN_SECONDS))
        end
        local message, err = conn:recv(100) -- waits at most 100 ms
        if message then
            handle(message)
        elseif err ~= fipc.TIMEOUT then -- "timeout": nothing yet, try again later
            error(err) -- "disconnected": the peer is gone
        end
    end
    eq(#handled, 1)
    eq(handled[1], "hello")
    finish()
end)

test("a buffer of your own", function()
    local conn, finish = echo_pair()
    assert(conn:send(support.pattern(10000), support.TEN_SECONDS))
    local buf = ffi.new("uint8_t[?]", 4096)
    local len, err, needed = conn:recv_into(buf, 4096)
    if err == fipc.TOO_LARGE then -- the message stays queued: make room, then take it
        buf = ffi.new("uint8_t[?]", needed)
        len = assert(conn:recv_into(buf, needed))
    end
    eq(len, 10000)
    eq(ffi.string(buf, len), support.pattern(10000))
    finish()
end)

test("zero-copy", function()
    local conn, finish = echo_pair()
    local slot = assert(conn:send_acquire(5)) -- room in the ring: a uint8_t*
    ffi.copy(slot, "hello", 5)
    assert(conn:send_commit(5))

    local got
    local function handle(data, len)
        got = ffi.string(data, len)
    end
    local data, len = assert(conn:recv_acquire()) -- a const uint8_t* into the ring
    handle(data, len) -- read it in place
    conn:recv_release() -- frees its room
    eq(got, "hello")
    finish()
end)

test("a coroutine that yields while nothing has arrived", function()
    local conn, finish = echo_pair()
    -- In a coroutine scheduler: poll, and yield until a message (or an error) comes
    local function receive(conn)
        while true do
            local message, err = conn:recv(fipc.NO_WAIT)
            if message or err ~= fipc.TIMEOUT then
                return message, err
            end
            coroutine.yield()
        end
    end
    local reader = coroutine.wrap(function()
        return receive(conn)
    end)
    eq(reader(), nil) -- nothing yet: it yielded
    assert(conn:send("later", support.TEN_SECONDS))
    local message
    local deadline = support.now() + 10
    repeat
        message = reader()
    until message or support.now() > deadline
    eq(message, "later")
    finish()
end)

test("a lossy subscriber that fell a lap behind", function()
    local name = support.unique_name("snippet_lap")
    local peer = support.peer("lap", name) -- 144 messages through a broadcast of 64, then its end
    local subscriber = assert(fipc.broadcast_join(name, fipc.SUBSCRIBER_LOSSY, support.TEN_SECONDS))
    eq(peer:wait(), 0, "the peer's exit code")
    local handled, printed = {}, {}
    local function handle(message)
        handled[#handled + 1] = message:byte(1)
    end
    local function print(text)
        printed[#printed + 1] = text
    end
    local ok, ended = pcall(function()
        while true do
            local message, err, lost = subscriber:recv()
            if message then
                handle(message)
            elseif err == fipc.OVERRUN then -- fell a lap behind: the next receive goes on from a recent message
                print(lost .. " messages lost")
            else
                error(err) -- "disconnected": the broadcast ended
            end
        end
    end)
    assert(not ok and ended:find("disconnected", 1, true), tostring(ended))
    eq(table.concat(printed, ","), "140 messages lost")
    eq(table.concat(handled, ","), "140,141,142,143")
    subscriber:close()
end)

test("a batch each way", function()
    local conn, finish = echo_pair()
    local handled = {}
    local function handle(message)
        handled[#handled + 1] = message
    end
    assert(conn:send("zero", support.TEN_SECONDS))
    eq(conn:recv(support.TEN_SECONDS), "zero") -- the echo peer is ready: the batch comes back in one piece of time
    local sent = assert(conn:send_many({ "one", "two", "three" })) -- published together
    local batch = assert(conn:recv_many(64)) -- every message already there, up to 64
    for _, message in ipairs(batch) do
        handle(message)
    end
    eq(sent, 3)
    while #handled < 3 do -- the echoes may come over more than one batch
        for _, message in ipairs(assert(conn:recv_many(64, support.TEN_SECONDS))) do
            handle(message)
        end
    end
    eq(table.concat(handled, ","), "one,two,three")
    finish()
end)

return suite
