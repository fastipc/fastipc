-- game_loop.lua: polls the echo server once a frame, never waiting
local ffi = require("ffi")
local fipc = require("fipc")

-- The rest of a frame (a game engine's loop does this for you)
local sleep
if ffi.os == "Windows" then
    ffi.cdef("void Sleep(uint32_t ms);")
    sleep = function(ms) ffi.C.Sleep(ms) end
else
    ffi.cdef("int usleep(uint32_t us);")
    sleep = function(ms) ffi.C.usleep(ms * 1000) end
end

local conn = assert(fipc.connect("demo", 5000))
local echoes = 0
for frame = 1, 60 do
    assert(conn:send(tostring(frame), fipc.NO_WAIT)) -- "timeout" only if the ring is full
    while true do
        local echo, err = conn:recv(fipc.NO_WAIT)
        if echo then
            echoes = echoes + 1
        elseif err == fipc.TIMEOUT then
            break -- nothing more this frame
        else
            error(err) -- "disconnected": the peer is gone
        end
    end
    sleep(16)
end
print(echoes .. " echoes in 60 frames")
conn:close()
