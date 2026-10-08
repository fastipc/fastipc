-- The examples (examples/*.lua) run in pairs, as the README and the website run them: a server, then its client, each
-- in a process of its own, in bindings/lua (where require("fipc") finds the binding).

local support = require("support")
local eq = support.eq

local suite = support.suite("examples")
local test = suite.test

-- Runs examples/<server>.lua, then examples/<client>.lua; both must succeed. Their outputs.
local function run_pair(server, client)
    local outputs = {}
    local processes = {}
    for i, example in ipairs({ server, client }) do
        outputs[i] = support.temp_file(".txt")
        local script = support.binding .. "/examples/" .. example .. ".lua"
        local env = "package.path = '" .. support.binding:gsub("\\", "/") .. "/?.lua;' .. package.path"
        processes[i] = support.spawn({ support.luajit, "-e", env, script }, { stdout = outputs[i] })
    end
    local client_code = processes[2]:wait()
    local server_code = processes[1]:wait()
    local texts = {}
    for i, path in ipairs(outputs) do
        texts[i] = support.read_file(path):gsub("\r\n", "\n")
        os.remove(path)
    end
    eq(client_code, 0, client .. "'s exit code")
    eq(server_code, 0, server .. "'s exit code")
    return texts[1], texts[2]
end

test("server and client: ping, PING", function()
    local _, client = run_pair("server", "client")
    eq(client, "PING\n")
end)

test("echo_server and echo_client", function()
    local server, client = run_pair("echo_server", "echo_client")
    eq(client, "hello\nshared\nmemory\n")
    eq(server, "client gone\n")
end)

test("echo_server and game_loop", function()
    local server, client = run_pair("echo_server", "game_loop")
    local echoes = tonumber(client:match("^(%d+) echoes in 60 frames\n$"))
    assert(echoes and echoes >= 50 and echoes <= 60, client)
    eq(server, "client gone\n")
end)

test("rpc_server and rpc_client, and across the pairs", function()
    local _, client = run_pair("rpc_server", "rpc_client")
    eq(client, "0\tPING\n")
    _, client = run_pair("rpc_server", "client")
    eq(client, "PING\n")
    _, client = run_pair("server", "rpc_client")
    eq(client, "0\tPING\n")
end)

return suite
