local requests = 0
local router = claviron.router()

local function answer(request)
  requests = requests + 1
  return {
    headers = { ["content-type"] = "application/json" },
    body = claviron.json.encode({
      runtime = "lua", engine = "Olua", method = request.method,
      path = request.path, body = request:body(), requests = requests,
    }),
  }
end

router:get("/lua", answer)
router:post("/lua/echo", answer)
return router
