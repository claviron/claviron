import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from database import database_summary


try:
    import gufi
except ImportError:
    # Served by a plain ASGI server — uvicorn, for a measurement beside gufi —
    # there is no Claviron to greet, and the app serves the same without it.
    gufi = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    if gufi is not None:
        response = await gufi.invoke(
            "claviron.ping.v1",
            {"echo": "FastApiApp"},
        )
        print(f"FastApiApp connected to Claviron: {response}")
    yield


app = FastAPI(lifespan=lifespan)
ROOT = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=ROOT / "public" / "static"), name="static")

if os.environ.get("EXAMPLES_BENCH") == "1":
    from claviron_bench.fastapi import router as bench_router

    app.include_router(bench_router)

    bench_headers = {"x-gufi-mode": "asgi", "x-gufi-process-id": str(os.getpid())}

    @app.get("/bench/python/plain")
    async def python_bench_plain():
        return Response("ok\n", media_type="text/plain", headers=bench_headers)

    @app.get("/bench/python/json")
    async def python_bench_json():
        return JSONResponse({"message": "ok"}, headers=bench_headers)


@app.get("/", response_class=FileResponse)
async def root():
    return FileResponse(ROOT / "public" / "index.html")


@app.get("/api/db")
def shared_database():
    # FastAPI runs this synchronous SQLite read off the async event loop.
    return database_summary()


@app.get("/python/hits")
async def python_hits():
    # The same count Rust's `/hits` adds to: one value for every worker, kept
    # by Claviron in shared memory. Nothing to register — the key is the name.
    if gufi is None:
        return {"hits": None, "from": "python"}
    return {"hits": gufi.counter("hits").increment(), "from": "python"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}", "from": "python", "app": "FastApi"}


# Server-side rendering, the way a Python site does it — a Jinja template and a
# page of rows built per request — and the same rows as JSON, the way an API
# sends them for a client to render. `rows` sets the size: about 330 bytes a
# row as HTML and half that as JSON — 300 rows is ~99 KB of page, ~49 KB of
# data. Together they measure the transport on what a Python app
# actually returns: large, dynamic HTML that crosses to the edge uncompressed,
# and the API reply that stands beside it.
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
SECTIONS = ["Books", "Music", "Garden", "Kitchen", "Tools", "Toys"]


def catalog_items(rows: int, page: int) -> list[dict]:
    start = (page - 1) * rows
    return [
        {
            "sku": f"SKU-{start + n:06d}",
            "name": f"{SECTIONS[n % len(SECTIONS)]} item {start + n}",
            "blurb": "A well made thing that does what it says, shipped in two days.",
            "category": SECTIONS[n % len(SECTIONS)],
            "stock": (start + n) * 7 % 23,
            "price": 4.99 + (start + n) % 97 * 1.25,
        }
        for n in range(rows)
    ]


@app.get("/ssr")
async def catalog_page(request: Request, rows: int = 50, page: int = 1):
    """The page, rendered: what a server-side site sends."""
    return templates.TemplateResponse(
        request,
        "catalog.html",
        {
            "title": "Catalog",
            "sections": SECTIONS,
            "current": "Books",
            "items": catalog_items(rows, page),
            "total": 10_000,
            "sort": "relevance",
            "page": page,
            "client": request.client.host if request.client else "unknown",
        },
    )


@app.get("/api/catalog")
async def catalog_data(rows: int = 50, page: int = 1):
    """The same rows, not rendered: what an API sends for a client to render."""
    return {"total": 10_000, "page": page, "items": catalog_items(rows, page)}


@app.get("/events")
async def events(count: int = 3, every: float = 0.0):
    """Server-sent events, one per `every` seconds: a reply that streams.

    The first event must reach the client before the last is produced; a
    runner that collects the whole reply first delivers them all at the end.
    """

    async def stream():
        for n in range(count):
            yield f"data: {n}\n\n"
            if every:
                await asyncio.sleep(every)

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.websocket("/ws")
async def echo(websocket: WebSocket):
    """ASGI text/binary echo, also used by Gufi's reliable WT message adapter."""
    await websocket.accept()
    source = websocket.scope.get("extensions", {}).get("gufi.transport", {})
    identity = {
        "socket_id": uuid4().hex,
        "pid": os.getpid(),
        "transport": source.get("name", "websocket"),
        "http_version": source.get("http_version", websocket.scope.get("http_version", "1.1")),
    }
    try:
        await websocket.send_json({"event": "connected", **identity})
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                break
            if message.get("bytes") is not None:
                await websocket.send_bytes(message["bytes"])
                continue
            text = message.get("text", "")
            if text == "/hello":
                continue  # Activates a browser-created WT stream without a chat message.
            if identity["transport"] == "webtransport" and text in (
                "/concurrent-stream-test A", "/concurrent-stream-test B"
            ):
                await websocket.send_json({
                    "event": "concurrent_stream_test", "stream": text[-1], **identity,
                })
            else:
                await websocket.send_text(f"echo: {text}")
    except WebSocketDisconnect:
        pass
