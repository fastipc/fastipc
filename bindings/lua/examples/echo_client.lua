-- echo_client.lua
local fipc = require("fipc")

local conn = assert(fipc.connect("demo", 5000))
for _, word in ipairs({ "hello", "shared", "memory" }) do
    assert(conn:send(word))
    print(assert(conn:recv()))
end
conn:close()
