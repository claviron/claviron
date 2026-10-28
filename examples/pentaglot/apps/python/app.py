"""One standard-library ASGI application with an explicit lifespan."""

import json

requests = 0


async def app(scope, receive, send):
    global requests
    if scope["type"] == "lifespan":
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                print("Pentaglot Python startup", flush=True)
                await send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                print("Pentaglot Python shutdown", flush=True)
                await send({"type": "lifespan.shutdown.complete"})
                return
    elif scope["type"] == "http":
        body = bytearray()
        while True:
            event = await receive()
            if event["type"] == "http.disconnect":
                return
            body.extend(event.get("body", b""))
            if not event.get("more_body", False):
                break
        path = scope["path"]
        if path == "/python/events":
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-type", b"text/event-stream")]})
            for n in range(3):
                await send({"type": "http.response.body",
                            "body": f"data: {n}\n\n".encode(), "more_body": True})
            await send({"type": "http.response.body", "body": b""})
            return
        status = 200 if path in ("/python", "/python/echo") else 404
        requests += 1
        response = json.dumps({
            "runtime": "python", "engine": "Gufi-ASGI", "method": scope["method"],
            "path": path, "body": body.decode(), "requests": requests,
        }, separators=(",", ":")).encode()
        await send({"type": "http.response.start", "status": status,
                    "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": response})
