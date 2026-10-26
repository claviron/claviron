# Runner benchmark listener

This configuration serves HTTP/1.1 on `127.0.0.1:8080`. Each host selects one
already-running application over Orbit Link. Access logging is disabled on
all benchmark hosts. The Edge and runner binaries must use the same Claviron
and Orbitive sources and transport geometry.

| Host | Plain path | JSON path | Handler |
| --- | --- | --- | --- |
| `edge.bench.test` | `/bench/plain` | `/bench/json` | Native Rust in Edge |
| `rust.bench.test` | `/bench/plain` | `/bench/json` | Native Rust over Link |
| `fastapi.bench.test` | `/bench/plain` | `/bench/json` | Native Rust in Gufi |
| `fastapi.bench.test` | `/bench/python/plain` | `/bench/python/json` | Python/FastAPI |
| `octane.bench.test` | `/bench/plain` | `/bench/json` | Native Rust in Lumenize |
| `octane.bench.test` | `/bench/laravel/plain` | `/bench/laravel/json` | Persistent PHP/Laravel |
| `symfony.bench.test` | `/bench/plain` | `/bench/json` | Persistent PHP/Symfony |
| `node.bench.test` | `/bench/javascript/plain` | `/bench/javascript/json` | JavaScript/Hono on Node |
| `bun.bench.test` | `/bench/javascript/plain` | `/bench/javascript/json` | JavaScript/Hono on Bun |
| `deno.bench.test` | `/bench/javascript/plain` | `/bench/javascript/json` | JavaScript/Hono on Deno |

`native.yaml` registers the Edge Rust control on its own host. Other hosts
are ordinary `vhost.yaml` declarations; they cannot be intercepted by that
control. Start Skene without its optional native `bench` feature, and select
the distinct guest-language paths when measuring Python and Laravel.

From the Claviron repository root:

```sh
CONFIG_DIR="$PWD/examples/edge-server/config-matrix" FLEET=benchedg WORKERS=4 \
  examples/scripts/edge.sh start --release --bench
```

Use the independent example launchers in `examples/scripts/` for the runners.
`bench-app/bench-orbit.yaml` selects eight Rust workers without a socket
listener. JavaScript launchers accept `ORBIT_WORKERS`, `ORBIT_FLEET` and
`ORBIT_APP`; use the distinct fleet/application pairs in `vhost.yaml`.

The measurement client starts no servers and performs no builds:

```sh
python3 examples/scripts/measure-matrix.py \
  --output examples/edge-server/logs/matrix-run \
  --connections 256 --threads 4 --duration 5m --sample-seconds 10
```

It verifies exact response bodies before each case: plain is three bytes
(`ok` and a newline), JSON is 16 bytes (`{"message":"ok"}`). It records raw
h2load output, response identity headers and a JSON summary. Use a recent
h2load whose output includes p99; an older tool produces a null percentile,
not an estimate. Repeat `--family PID` to capture RSS and cumulative CPU time
for owned master processes and their descendants at the declared sample
interval. `--check-only` verifies responses without generating timed load;
repeat `--case NAME` to select a subset. Each output directory must be new.

Before measurement, verify eight live serving lanes and actual request
participation for every application; merely configuring eight workers is
insufficient. Retain per-process resource samples and identity headers when
reporting distribution. Build and prepare framework caches before measuring;
keep production/debug, warmup, logging and capacity policies constant.

`edge.sh stop` requests graceful shutdown of its recorded process. The other
launchers have the same `status`/`stop` lifecycle. Confirm exit before clearing
only the stopped test fleets. Never clear a fleet mapped by a live process.
