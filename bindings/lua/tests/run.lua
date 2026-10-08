--[[
The Lua binding's tests: `luajit tests/run.lua [text]` in bindings/lua runs every suite (or only the tests whose name
contains `text`) against this checkout's library (zig-out), and exits 1 if any failed. Nothing but LuaJIT is needed;
the cross-language tests also want a Python with cffi (FIPC_TEST_PYTHON, else the repository's venv, else python),
and pass without one unless FIPC_REQUIRE_INTEROP is set.
]]

local here = arg[0]:match("^(.*)[/\\]") or "."
package.path = here .. "/?.lua;" .. package.path
local support = require("support")

local SUITES = { "test_api", "test_messages", "test_zerocopy", "test_rpc", "test_lifetime", "test_interop",
    "test_examples", "snippets", "test_docs" }

local filter = arg[1]
local passed, failed, skipped = 0, {}, 0
local started = support.now()
print(string.format("fipc %s for Lua, %s, library %s", support.fipc._VERSION, jit.version,
    support.fipc.library))
for _, file in ipairs(SUITES) do
    local loaded, suite = pcall(require, file)
    if not loaded then
        failed[#failed + 1] = file
        print("FAIL  " .. file .. ": " .. tostring(suite))
        suite = { tests = {} }
    end
    for _, test in ipairs(suite.tests) do
        local full = suite.name .. ": " .. test.name
        if filter and not full:find(filter, 1, true) then
            skipped = skipped + 1
        else
            local t0 = support.now()
            local ok, err = xpcall(test.fn, debug.traceback)
            collectgarbage() -- a test's dropped handles close now, not in a later test
            local ms = (support.now() - t0) * 1000
            if ok then
                passed = passed + 1
                print(string.format("ok    %s (%.0f ms)", full, ms))
            else
                failed[#failed + 1] = full
                print(string.format("FAIL  %s (%.0f ms)\n%s", full, ms, err))
            end
        end
    end
end
print(string.format("\n%d passed, %d failed%s in %.1f s", passed, #failed,
    skipped > 0 and string.format(", %d filtered out", skipped) or "", support.now() - started))
for _, name in ipairs(failed) do
    print("  failed: " .. name)
end
os.exit(#failed == 0 and 0 or 1)
