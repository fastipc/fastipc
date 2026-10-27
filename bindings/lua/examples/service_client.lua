-- service_client.lua
local fipc = require("fipc")

local client = assert(fipc.service_join("my_service", fipc.CLIENT, 5000))
for i = 1, 3 do
    -- sends the request and returns its reply; nil, "abandoned" if the server ended before it replied
    print(assert(client:call("ping " .. i))) -- PING 1, PING 2, PING 3
end
client:close()
