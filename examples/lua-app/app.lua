-- claviron.app/lua: a Lua application served by portico.
-- Loaded once per worker; `served` counts this worker's requests.

local router = claviron.router()
local log = claviron.log.channel("lua")
local served = 0

local function json(value, status)
  return {
    status = status or 200,
    headers = { ["content-type"] = "application/json" },
    body = claviron.json.encode(value),
  }
end

-- What this request looked like from inside the application.
router:get("/lua", function(request)
  served = served + 1
  log:info("Lua demo request", { path = request.path, served = served })
  return json({
    hello = "lua",
    served = served,
    path = request.path,
    query = request.query,
    agent = request.headers["user-agent"],
  })
end)

router:get("/lua/hello/{name}", function(_, params)
  return "hello " .. params.name .. "\n"
end)

-- A reply in pieces: one line per call, until nil.
router:get("/lua/count", function()
  local n = 0
  return {
    headers = { ["content-type"] = "text/plain; charset=utf-8" },
    body = function()
      n = n + 1
      if n > 5 then return nil end
      return "line " .. n .. "\n"
    end,
  }
end)

-- The request body as JSON, read whole, and a summary of it back.
router:post("/lua/echo", function(request)
  local ok, value = pcall(claviron.json.decode, request:body())
  if not ok then
    return json({ error = "the body is not JSON" }, 400)
  end
  local keys = {}
  if type(value) == "table" then
    for key in pairs(value) do keys[#keys + 1] = tostring(key) end
    table.sort(keys)
  end
  return json({ received = value, keys = claviron.json.array(keys) })
end)

-- The shared demo uses SQLite. This native adapter does not expose it yet;
-- never invent a connection or fall back to a remote PostgreSQL pool.
router:get("/lua/db/{n}", function(_, params)
  local n = math.tointeger(tonumber(params.n))
  if not n or n < 1 or n > 20 then
    return json({ error = "n is a whole number from 1 to 20" }, 400)
  end
  return json({ error = "the native Lua SQLite adapter is not available" }, 501)
end)

-- Benchmark: the same two statements as form/bench/*.php, on MySQL test_db
-- (claviron_bench, 10 000 seeded rows), through portico's MySQL pool.
-- Lua's own `math.random` picks the row, so every request reads one row.
local BENCH_ROWS = 10000

router:get("/lua/bench/select", function()
  local row = claviron.db.mysql():first([[
    select id, name, score, payload, cast(created_at as char) as created_at
      from claviron_bench where id = ?
  ]], math.random(1, BENCH_ROWS))
  return json({ row = row })
end)

router:post("/lua/bench/insert", function()
  local done = claviron.db.mysql():execute(
    "insert into claviron_bench (name, score, payload) values (?, ?, ?)",
    "lua", math.random(0, 999), { from = "lua" })
  return json({ id = done.insert_id })
end)

-- Words and numbers in a text, with Rust's regex under claviron.regex.
local word = claviron.regex([[\p{L}+]])
local number = claviron.regex([[\d+(?:\.\d+)?]])

router:post("/lua/words", function(request)
  local text = request:body()
  local words, numbers = {}, {}
  for found in word:gmatch(text) do
    local w = found[0]:lower()
    words[w] = (words[w] or 0) + 1
  end
  for found in number:gmatch(text) do numbers[#numbers + 1] = found[0] end
  local top = {}
  for w, n in pairs(words) do top[#top + 1] = { word = w, count = n } end
  table.sort(top, function(a, b)
    if a.count ~= b.count then return a.count > b.count end
    return a.word < b.word
  end)
  while #top > 10 do table.remove(top) end
  return json({ top = claviron.json.array(top), numbers = claviron.json.array(numbers) })
end)

-- A WebSocket at /lua/socket: a chat among the sockets of this worker.
-- The state is shared by every request and socket, so a plain table holds
-- who is connected.
local sockets = {}
local function count()
  local n = 0
  for _ in pairs(sockets) do n = n + 1 end
  return n
end

router:websocket({
  open = function(socket)
    sockets[socket.id] = socket
    socket:send(claviron.json.encode({ hello = "socket " .. socket.id, here = count() }))
  end,

  message = function(socket, data, binary)
    if binary then
      socket:send_binary(data)                  -- bytes come back as they went
      return
    end
    data = data:gsub("%s+$", "")                -- a line from a terminal ends in \n
    if data == "/who" then
      socket:send(claviron.json.encode({ here = count() }))
    else
      local line = claviron.json.encode({ from = socket.id, text = data })
      for _, other in pairs(sockets) do other:send(line) end
    end
  end,

  close = function(socket, code, reason, clean)
    sockets[socket.id] = nil
  end,
})

return router
