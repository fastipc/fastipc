-- service_server.lua
local fipc = require("fipc")

-- 4 KiB per client, requests and replies of up to 256 bytes, 1 client and 1 server: this process owns the service
-- and is its server
local service = assert(fipc.service_create("my_service", 4096, 256, 1, 1))
local server = assert(fipc.service_join("my_service", fipc.SERVER, 5000))
for _ = 1, 3 do -- three requests, then the end
    local request = assert(server:recv()) -- the next request of any client
    assert(server:send(request:upper())) -- the reply goes to its client, and never waits
end
server:close()
service:close() -- ends the service: the client still receives the replies sent
