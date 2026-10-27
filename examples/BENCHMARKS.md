# Edge and Orbit application benchmarks

These measurements exercise real HTTP requests through Claviron Edge, either
handled in Rust at the edge or forwarded over Orbit Link to a separate
application. PHP and Python rows execute framework handlers; their native Rust
controls are labeled separately.

Results were collected on 25–27 October 2026. macOS and Linux are separate
campaigns with different hardware, builds and request paths. Read each table
with its configuration; they are not a cross-platform ranking.

## Workload and method

```text
h2load → HTTP/1.1 → Claviron Edge → Orbit SHM → application → Edge → client
                              └→ native Rust handler → client
```

Every timed path ran for five minutes with four generator threads, persistent
connections and one outstanding request per connection. Client and server ran
on the same machine. Cases ran sequentially, with builds and framework-cache
preparation outside measurement. Per-request access logging was disabled;
warning/error logging remained enabled. The workloads add no rate limit,
authentication or database query. These are closed-loop measurements without
coordinated-omission correction.

The Linux plain body is `ok\n` (3 bytes); JSON is `{"message":"ok"}` (16 bytes),
serialized by the selected handler. Response headers differ between adapters.
Neither payload measures substantial application logic or large-body streaming.

The Symfony fixture is a **minimal Symfony application**. The Laravel example
runs the full Laravel application lifecycle through Octane/Lumenize. FastAPI
and Hono use simple handlers. Middleware and service composition are not
equivalent: these results measure the supplied application setups, not a
like-for-like framework ranking.

## Linux x86-64: complete runner matrix

Hardware: two Intel Xeon E5-2680 v2 CPUs, 20 physical cores / 40 SMT threads,
two NUMA nodes. HTTP/1.1 cleartext entered the actual
[edge-server example](edge-server/) on loopback port 8080. Tests used private
network, PID, mount and IPC namespaces; existing host workloads remained active.
No explicit CPU or NUMA affinity was applied.

| Connections | Edge workers | Application workers / Link lanes | Rust build | Claviron | Orbitive |
| ---: | ---: | ---: | --- | --- | --- |
| 64 | 4 | 8 | Release, CPU-native | `6152fbc` | `971df61` |
| 256 | 4 | 8 | Release, default x86-64 target | `b488fa0` | `971df61` |
| 512 | 16 | 16 | Release, CPU-native | `6152fbc` | `971df61` |

CPU-native means `target-cpu=native` for Rust binaries and the shared JavaScript
addon. Installed language runtimes were not rebuilt. The measured JavaScript
runtimes were Node 24.22.0, Deno 2.9.8 and, in the 256-client campaign only,
Bun 1.3.12. The Python environment used CPython 3.15. Its exact patch version,
GIL mode and build options have not been reconciled with the macOS runtime;
the Python difference cannot be attributed to hardware alone. The Node 26
follow-up below is a separate experiment, not a replacement row in this matrix.

Laravel ran in persistent Lumenize application mode, with `APP_ENV=production`,
`APP_DEBUG=false` and successful `artisan optimize` for configuration, events,
routes and views. OPcache warmup compiled 2,959 files per serving worker before
timing. Minimal Symfony ran in persistent Skene application mode with its
production cache and OPcache warmup; Skene's Rust benchmark interceptor was disabled.
Native PHP INI settings were retained rather than applying a new timestamp/JIT
tuning profile.

### Plain

Each cell is **successful thousands of requests/s / p99 milliseconds**.

| Handler | 64 connections | 256 connections | 512 connections |
| --- | ---: | ---: | ---: |
| Edge Rust | 213.30 / 1.33 | 242.29 / 4.15 | 231.08 / 4.55 |
| Orbit Rust | 122.56 / 1.93 | 149.08 / 6.25 | 155.25 / 14.08 |
| Gufi Rust control | 122.15 / 1.95 | 149.55 / 6.27 | 152.23 / 14.28 |
| FastAPI / Python | 30.88 / 7.81 | 34.58 / 33.39 | 51.79 / 31.25 |
| Lumenize Rust control | 105.66 / 2.13 | 131.23 / 7.26 | 131.12 / 15.73 |
| Laravel / PHP | 13.72 / 9.52 | 14.36 / 134.97 | 12.41 / 371.54 |
| Minimal Symfony / PHP | 44.75 / 4.87 | 50.61 / 19.42 | 41.76 / 45.65 |
| Node / JavaScript | 32.52 / 4.38 | 30.65 / 29.43 | 34.38 / 53.79 |
| Deno / JavaScript | 37.11 / 4.99 | 38.44 / 12.04 | 40.03 / 28.14 |
| Bun / JavaScript | — | 24.08 / 22.73 | — |

### JSON

Same units and configuration as the plain table.

| Handler | 64 connections | 256 connections | 512 connections |
| --- | ---: | ---: | ---: |
| Edge Rust | 212.32 / 1.35 | 244.12 / 4.19 | 229.18 / 4.93 |
| Orbit Rust | 122.37 / 1.94 | 149.79 / 6.28 | 154.05 / 14.13 |
| Gufi Rust control | 121.84 / 1.94 | 151.07 / 6.20 | 152.76 / 14.16 |
| FastAPI / Python | 28.89 / 8.42 | 32.62 / 35.10 | 49.12 / 33.65 |
| Lumenize Rust control | 105.38 / 2.13 | 131.96 / 7.27 | 132.05 / 15.61 |
| Laravel / PHP | 13.73 / 9.32 | 14.35 / 135.30 | 12.47 / 374.36 |
| Minimal Symfony / PHP | 44.27 / 4.75 | 50.65 / 19.08 | 42.32 / 45.28 |
| Node / JavaScript | 32.56 / 4.16 | 30.41 / 31.59 | 34.85 / 53.54 |
| Deno / JavaScript | 36.89 / 4.92 | 38.33 / 12.09 | 40.07 / 28.04 |
| Bun / JavaScript | — | 27.19 / 15.25 | — |

### Errors and interpretation

- 64 clients: no failed requests, transport errors or timeouts.
- 256 clients: one HTTP 502 in Gufi Rust/plain; no transport errors or timeouts.
- 512 clients: four HTTP 503 responses: Orbit Rust/plain (one), Gufi Rust/plain
  (two), Lumenize Rust/JSON (one); no transport errors or timeouts.
- All three configurations recorded **zero new Link wake warnings**. HTTP
  status alone did not establish inbox exhaustion or the cause of those failures.
- All selected application lanes participated, and owned process families
  exited normally after the runs.

Worker counts, CPU build flags and Claviron revisions differ between columns.
Increasing concurrency did not uniformly increase throughput: PHP framework
rows completed fewer requests at 512, with higher p99. These single samples
cannot isolate concurrency scaling or the benefit of native compilation.
The Rust controls measure their serving compositions, not guest-language speed.

## macOS ARM64: M3 Max application paths

Hardware: Apple M3 Max, 16 CPU cores, 128 GiB RAM; macOS 27.0.1.
The generator was h2load 1.70.0, with 64 connections and the common five-minute,
four-thread settings. Frontends had four Edge workers. These were release
builds on a development machine with normal background activity.

### HTTP JSON: same minimal Symfony application, two frontends

Both rows used the same eight Skene workers, production/debug=false, warmed
OPcache and the same 16-byte PHP JSON controller. Skene's native Rust bench
feature was disabled. One frontend was an already-running Claviron downstream
application with its services and HTTP policies active; the other was the
isolated edge-server example. No quantified background-traffic claim is made.

| Frontend → Orbit → minimal Symfony | Successful req/s | p99 | Failed / error / timeout | New wake warnings |
| --- | ---: | ---: | --- | ---: |
| Existing downstream application | 59,497.43 | 3.79 ms | 0 / 0 / 0 | 0 |
| Isolated edge-server | 63,155.34 | 4.30 ms | 0 / 0 / 0 | 0 |

All eight Skene workers participated; process identities stayed stable. The
isolated Edge exited gracefully. The existing application composition did not
produce a large throughput gap in this pair, but the samples do not establish
an overhead bound or a latency improvement.

Build provenance differs from the Linux matrix: the recorded Claviron base was
`427667e`, Skene `d69f917`. These binaries used registry `orbit-link 0.5.6` with
only the inbox/watcher wake-drain correction overlaid. They did not contain
every later Orbitive change. This table records the corrected-wake runs only.

### Earlier HTTPS application references

HTTP/1.1 over TLS through the existing downstream application; no client
certificate. All rows returned the same 3-byte plain body. Orbit targets had
eight serving workers. Embedded Symfony instead used per-Edge-worker PHP pools,
with four initially warm contexts per worker and a ceiling of sixteen.

| Actual handler / composition | Successful req/s | p99 | Failed / error / timeout | New wake warnings |
| --- | ---: | ---: | --- | ---: |
| Minimal Symfony / embedded persistent PHP | 89,660.57 | 2.63 ms | 0 / 0 / 0 | 0 |
| Minimal Symfony / Skene over Orbit | 60,327.88 | 4.22 ms | 0 / 0 / 0 | 4 |
| FastAPI / Gufi over Orbit | 60,883.37 | 2.82 ms | 0 / 0 / 0 | 23 |
| Laravel / Lumenize over Orbit | 18,287.83 | 47.84 ms | 0 / 0 / 0 | 0 |

These earlier references precede the wake correction. The warnings did not
become measured HTTP failures; the corrected HTTP JSON pair above is separate.
PHP was 8.5.11 ZTS, Python 3.15.1 and FastAPI 0.143.0. Framework production
caches were prepared and serving-worker OPcache was warmed; native
`opcache.validate_timestamps=1` was retained. PHP context recycling remained
enabled. Placement, pool sizes and runner composition differ, so the embedded
versus Orbit gap is not an isolated transport-overhead measurement.

The earlier native Rust controls used the same HTTPS frontend and 3-byte body:

| Native control | Successful req/s | p99 | Failed / error / timeout |
| --- | ---: | ---: | --- |
| Direct Edge Rust | 132,810.96 | 0.769 ms | 0 / 0 / 0 |
| Rust bench-app over Orbit, eight workers | 49,607.04 | 2.84 ms | 0 / 0 / 0 |

Both recorded no new warning/error messages. This native pair used registry
Orbitive 0.5.8 / orbit-link 0.5.6, before the later Linux source matrix.
Ingress and most frontend CPU concentrated in one of the four Edge workers;
all eight application lanes participated. The direct Edge generator approached
four-thread saturation. Neither figure establishes a maximum server capacity,
and the pair does not isolate Link cost from routing and runner composition.

## Node memory follow-up on Linux

A separate controlled comparison used 64 clients, **8 Edge + 8 Node workers**,
the same native Edge/addon binaries, sources and settings. Only the installed
Node runtime changed. JSON results:

| Runtime | Successful req/s | p99 | Application-family peak RSS |
| --- | ---: | ---: | ---: |
| Node 24.22.0 | 30,729.80 | 4.71 ms | 15,969.84 MiB |
| Node 26.12.1 | 31,929.25 | 4.86 ms | 1,737.12 MiB |

Both completed without failed requests, transport errors, timeouts or wake
warnings. RSS is a sampled sum over serving processes and can count shared
pages repeatedly; it is not an exclusive allocation total.

Additional Node 24 diagnostics found a small post-load live JS heap alongside
large retained reusable V8 page pools. `malloc_trim` returned only about 162 MiB
from 12.11 GiB aggregate RSS in the census run. Native allocation profiling
found no multi-GiB outstanding malloc footprint in the addon path. This supports
V8 page retention as the main explanation, without proving every retained byte's
owner or general leak freedom. Profiling substantially changed throughput and
GC behavior; those instrumented runs are not throughput baselines.

## Configuration and reproduction

The [matrix configuration](edge-server/config-matrix/README.md) identifies each
host and real handler path. Its version at Claviron `b488fa0` describes the
256-client setup; the 64/512 runs changed worker counts and native build flags
as recorded above. The source snapshot uses a listener limit of 2,048
connections and a host request-concurrency limit of 5,000. Rust and PHP runner
examples declare 128 in-flight and concurrent requests per serving worker.

The PHP runner pool policy retained `min_idle=1`, `max_idle=1`, `max_size=8`,
`idle_timeout=60s`, `max_life=5m`, `max_requests=1000`,
`maintenance_interval=100ms` and `max_waiting=256`. Context recycling is part
of the measured application path. These are experiment settings, not tuning
recommendations for every deployment.

Use the [launchers](scripts/) to prepare an isolated Edge and selected runners,
then verify ready workers and handler identity before timing. The
[measurement client](scripts/measure-matrix.py) verifies bodies and saves raw
h2load output, identity headers and resource samples. It starts no servers:

```sh
python3 examples/scripts/measure-matrix.py \
  --output examples/edge-server/logs/matrix-run \
  --connections 256 --threads 4 --duration 5m --sample-seconds 10
```

For one JSON path, the underlying load command is:

```sh
h2load --h1 -c 256 -t 4 -m 1 -D 5m \
  -H 'host: symfony.bench.test' http://127.0.0.1:8080/bench/json
```

The retained campaign reports include raw response/error counts, per-PID CPU
and RSS, lane participation, binary/source identities and cleanup observations;
this file is their summary, not a bundled raw-data archive. No statistical
confidence interval or multi-run speedup is claimed. Later source changes need
fresh measurements; these results do not characterize the current checkout
automatically. Transport microbenchmarks and comparisons against other servers
are outside this HTTP application matrix.
