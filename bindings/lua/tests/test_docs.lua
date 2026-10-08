-- Every Lua block of the documentation is code that runs: each block of the READMEs and of the website's pages must
-- appear, line for line, in examples/ or in tests/snippets.lua (which run). Blank lines and indentation don't count;
-- a document that isn't there is skipped.

local support = require("support")

local suite = support.suite("docs")
local test = suite.test

-- The lines that count: trimmed, without blank ones
local function lines(text)
    local out = {}
    for line in (text .. "\n"):gmatch("([^\n]*)\n") do
        line = line:gsub("\r$", ""):match("^%s*(.-)%s*$")
        if line ~= "" then
            out[#out + 1] = line
        end
    end
    return out
end

local function unescape_html(text)
    return (text:gsub("&lt;", "<"):gsub("&gt;", ">"):gsub("&quot;", '"'):gsub("&#39;", "'"):gsub("&amp;", "&"))
end

-- The Lua blocks of a Markdown file (```lua) or an HTML page (<code class="language-lua">); none if it's missing
local function lua_blocks(path)
    local file = io.open(path, "rb")
    if not file then
        return {}
    end
    local text = file:read("*a"):gsub("\r\n", "\n")
    file:close()
    local blocks = {}
    local open, close, html = "```lua\n", "\n```", path:match("%.html$")
    if html then
        open, close = '<code class="language-lua">', "</code>"
    end
    local from = 1
    while true do
        local start, after = text:find(open, from, true)
        if not start then
            break
        end
        local finish = assert(text:find(close, after + 1, true), path .. ": an unclosed block")
        local block = text:sub(after + 1, finish - 1)
        blocks[#blocks + 1] = html and unescape_html(block) or block
        from = finish
    end
    return blocks
end

-- Whether `block` (lines) appears in `source` (lines) as consecutive lines
local function contains(source, block)
    for i = 1, #source - #block + 1 do
        local all = true
        for j = 1, #block do
            if source[i + j - 1] ~= block[j] then
                all = false
                break
            end
        end
        if all then
            return true
        end
    end
    return false
end

test("every Lua block of the docs runs", function()
    local sources = { lines(support.read_file(support.tests .. "/snippets.lua")) }
    local examples = support.binding .. "/examples/"
    for _, name in ipairs({ "server", "client", "echo_server", "echo_client", "rpc_server", "rpc_client",
        "game_loop" }) do
        sources[#sources + 1] = lines(support.read_file(examples .. name .. ".lua"))
    end
    local docs = {
        support.binding .. "/README.md",
        support.repository .. "/README.md",
        support.repository .. "/website/lua.html",
        support.repository .. "/website/index.html",
    }
    local checked = 0
    for _, doc in ipairs(docs) do
        for _, block in ipairs(lua_blocks(doc)) do
            local wanted = lines(block)
            local found = false
            for _, source in ipairs(sources) do
                found = found or contains(source, wanted)
            end
            assert(found, doc .. ": this block runs nowhere (examples/, tests/snippets.lua):\n" .. block)
            checked = checked + 1
        end
    end
    local page = io.open(docs[3]) -- the website is there (a checkout): its page has most of the blocks
    if page then
        page:close()
        assert(checked >= 15, "only " .. checked .. " blocks found")
    end
end)

return suite
