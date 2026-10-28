"""Check an explicitly selected, already running Pentaglot instance."""

import argparse
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--url", required=True, help="Base URL of the isolated demo")
args = parser.parse_args()
base = args.url.rstrip("/")
pids = set()

for runtime in ("rust", "lua", "deno", "php", "python"):
    for echo in (False, True):
        path = f"/{runtime}" + ("/echo" if echo else "")
        body = "Hello, five languages ✓".encode() if echo else None
        with urlopen(Request(base + path, data=body)) as response:
            value = json.load(response)
            assert response.status == 200, (path, response.status)
            assert value["runtime"] == runtime, value
            assert value["path"] == path, value
            assert value["method"] == ("POST" if echo else "GET"), value
            assert value["body"] == (body.decode() if body else ""), value
            pid = response.headers.get("x-pentaglot-pid")
            assert pid and pid.isdecimal(), (path, pid)
            pids.add(pid)
            print(f"PASS {path} ({value['engine']}, Edge PID {pid})")
    try:
        urlopen(base + f"/{runtime}-outside")
    except HTTPError as error:
        assert error.code == 404, error.code
    else:
        raise AssertionError(f"/{runtime} captured an unrelated prefix")

with urlopen(base + "/python/events") as response:
    assert response.read() == b"data: 0\n\ndata: 1\n\ndata: 2\n\n"
print("PASS Python streaming body")
print(f"Serving process IDs: {', '.join(sorted(pids))}")
