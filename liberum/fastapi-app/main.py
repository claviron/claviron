import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates


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

if os.environ.get("LIBERUM_BENCH") == "1":
    from liberum_bench.fastapi import router as bench_router

    app.include_router(bench_router)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/orders")
async def orders():
    return {"message": "no orders found"}


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
    """A WebSocket in FastAPI's own terms: accept, then echo until the client
    leaves. Nothing here knows which server or transport carries it."""
    await websocket.accept()
    try:
        while True:
            text = await websocket.receive_text()
            await websocket.send_text(f"echo: {text}")
    except WebSocketDisconnect:
        pass
