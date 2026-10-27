-- The examples (examples/*.lua) run in pairs, as the README and the website run them: a server, then its client (a
-- publisher and its subscriber, a producer and its consumer, a service's server and its client, in either order),
-- each in a process of its own, in bindings/lua (where require("fipc") finds the binding).

local support = require("support")
local eq = support.eq

local suite = support.suite("examples")
local test = suite.test

-- Runs examples/<server>.lua, then examples/<client>.lua (the client first if `client_first`); both must succeed.
-- Their outputs.
local function run_pair(server, client, client_first)
    local outputs = {}
    local processes = {}
    for _, i in ipairs(client_first and { 2, 1 } or { 1, 2 }) do
        local example = i == 1 and server or client
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

test("publisher and subscriber: tick 1 to tick 3 on a broadcast, in either order", function()
    for _, subscriber_first in ipairs({ false, true }) do
        local publisher, subscriber = run_pair("publisher", "subscriber", subscriber_first)
        eq(subscriber, "tick 1\ntick 2\ntick 3\n")
        eq(publisher, "")
    end
end)

-- The lines of `text`, sorted
local function sorted_lines(text)
    local lines = {}
    for line in text:gmatch("[^\n]+") do
        lines[#lines + 1] = line
    end
    table.sort(lines)
    return table.concat(lines, "\n")
end

test("producer and consumer: job 1 to job 3 on a work queue, in either order", function()
    for _, consumer_first in ipairs({ false, true }) do
        local producer, consumer = run_pair("producer", "consumer", consumer_first)
        eq(sorted_lines(consumer), "job 1\njob 2\njob 3", "the jobs, in any order (the queue promises none)")
        eq(producer, "")
    end
end)

test("service_server and service_client: ping 1 to ping 3 and their replies, in order, in either order", function()
    for _, client_first in ipairs({ false, true }) do
        local server, client = run_pair("service_server", "service_client", client_first)
        eq(client, "PING 1\nPING 2\nPING 3\n")
        eq(server, "")
    end
end)

return suite
