-- rpc_server.lua: answers requests until its client leaves
local fipc = require("fipc")

local UPPER = 1

local listener = assert(fipc.listen("my_channel", 1024 * 1024))
local conn = assert(listener:accept())
while true do
    local req, err = conn:rpc_recv()
    if not req then
        if err == fipc.DISCONNECTED then
            break
        end
        error(err)
    end
    if req.kind == fipc.RPC_REQUEST and req.opcode == UPPER then
        assert(conn:rpc_respond(req.id, req.opcode, 0, req.payload:upper()))
    end
end
conn:close()
listener:close()
