# Pentaglot

Five applications embedded in one Claviron Edge. The serving process hosts
Rust handlers, a persistent Lua application, a Deno Core/V8 application, a
persistent minimal Symfony/PHP application and one Gufi-ASGI Python application.

| URL prefix | Application | Source |
| --- | --- | --- |
| `/rust` | Native Rust | `src/app/native.rs` |
| `/lua` | Lua / Olua | `apps/lua/app.lua` |
| `/deno` | JavaScript/TypeScript / Deno Core | `apps/deno/app.ts` |
| `/php` | Minimal Symfony on persistent embedded PHP | `apps/php/app.php` |
| `/python` | Python / Gufi-ASGI | `apps/python/app.py` |

Every application answers JSON at its prefix and accepts `POST <prefix>/echo`.
The original URL remains visible to the application. The home page calls all
five. `X-Pentaglot-Pid` identifies the Edge process serving each response;
standalone mode uses one PID for every language.

## Run

This is an independent executable workspace. From the repository root:

```sh
examples/scripts/pentaglot.sh --sync
```

Open `http://127.0.0.1:8080`, as declared in the example's `.env` file. Claviron
loads that file and its selected environment variant; the launcher does not
inject a listener address. The launcher also supports `start`, `status`,
`stop`, `--release`, `--native` and `--no-build`, like the other examples.
Use `--standalone` for one serving process or `--workers N` for a master and
N serving workers. The default is standalone; `WORKERS` remains supported.
`--native` applies `target-cpu=native` to the Rust host and rebuilt Rust
dependencies; language engines use the selected runtime builds.
`--production` explicitly selects Claviron's production environment instead
of a named development environment.
`PENTAGLOT_LISTEN` selects the loopback listener; `FLEET`, `CONFIG_DIR` and
`WORKERS` select this instance's runtime resources. Server arguments after `--`
can select a distinct instance name, fleet, environment or configuration.

Building requires a compatible PHP ZTS embed installation (`PHP` and
`PHP_CONFIG`), CPython 3.15 (`PYO3_PYTHON`) and the Deno Core/V8 build
prerequisites. The Python example uses only the standard library. It creates
no virtual environment and installs no Python packages; `config/python.yaml`
can select a venv for an application that needs one.

`--sync` installs the PHP application's locked Composer dependencies using
the selected `PHP` executable. Later runs can omit it. The Symfony micro-kernel
declares only two routes and one controller; the existing `claviron/symfony`
adapter owns its persistent request/terminate loop. Deno enables its standard
Fetch and Streams implementation for request and response bodies.

`--optimize` builds an authoritative Composer classmap and warms Symfony's
production container and router cache. The kernel runs with `prod` and debug
off. PHP scans the demo's `config/php.d` instead of the host CLI extension
directory: OPcache is enabled, `opcache.validate_timestamps=0`, and Xdebug is
not loaded from that directory. The selected PHP build must provide OPcache
as a built-in module or load it from its primary INI. The pool keeps eight
persistent PHP instances warm and can grow to 64 under demand. `engine.warmup`
compiles the application, vendor and generated cache
files into the serving process's OPcache, excluding test directories. This
background warmup is separate from Symfony's generated production cache;
wait for its completion log before benchmarking. Restart after changing PHP
source because timestamp validation is disabled.

All configuration paths follow the process working directory, which the
launcher sets to this example. Rust, Lua, Deno and PHP use their existing
public composition APIs. The example's ASGI provider owns startup and final
shutdown through `gufi-asgi`, using a blocking executor for native boot/join.

## Check

Validate configuration without starting the listener:

```sh
examples/scripts/pentaglot.sh -- --check-config
```

After launching an isolated instance, run the small acceptance check:

```sh
python3 examples/pentaglot/scripts/smoke.py --url http://127.0.0.1:8080
```

It checks GET/POST dispatch in every language, UTF-8 body round trips, segment
boundaries and the Python streamed response. This is an executable demo check,
not a library test or a performance benchmark. Stop the server with Ctrl-C or
the launcher's `stop` command after clients have finished.

## Benchmark

The bottom of the home page shows a recorded throughput summary, loaded from
`public/assets/benchmark.json`. It reports successful requests/s, concurrency,
p99 latency, CPU usage, process RSS before/after each case and errors. Opening
the page reads the saved report; it does not
start a load test.

Build and start an isolated release instance with
`examples/scripts/pentaglot.sh start --standalone --release --native --optimize --production`,
then measure its five JSON
paths from the repository root:

```sh
python3 examples/pentaglot/scripts/benchmark.py \
  --url http://127.0.0.1:8080 \
  --connections 64 --threads 4 --duration 1m \
  --standalone \
  --binary examples/pentaglot/target/release/pentaglot \
  --native \
  --label "Your CPU, RAM and OS" \
  --output examples/pentaglot/public/assets/benchmark.json \
  --raw-dir examples/pentaglot/logs/benchmark
```

The script requires `wrk` and a running instance. It checks each
handler's JSON response and serving PID before timing. Cases run sequentially
with persistent HTTP/1.1 connections while all five runtimes remain loaded.
Only the final summary uses a Lua reporting hook; there is no per-response
Lua callback in the generator. HTTP error responses are excluded from
successful throughput; socket errors and timeouts remain in the report. A
run with errors produces a report and exits unsuccessfully.

For a worker-process measurement, start with `--workers 16` and pass
`--workers 16` to `benchmark.py` instead of `--standalone`. The sampler includes
the master and its serving workers and rejects changes to that process set
during the measurement. Runtime capacity is per serving process: sixteen
workers with this PHP configuration keep eight PHP instances warm each, and
each pool can grow to its configured ceiling.

The workload is run with settings suited to each runtime. The Lua handler
computes JSON rather than awaiting external I/O: concurrency 1 prevents
multiple Tokio tasks competing for its one VM. Deno uses eight independent
isolates, PHP keeps eight application instances warm with a configured pool
ceiling, and Python uses one ASGI event loop with asynchronous request tasks.
Rust serializes a typed response directly; Python also emits compact JSON.
PHP uses the minimal Symfony production kernel with debug off
and the OPcache settings described above. Access logging is off.
Each handler serializes its own small JSON response, including a request
counter; payload sizes and execution paths differ.

Equal instance counts would not imply equal execution capacity: Lua concurrency
is coroutine admission inside one VM, Deno instances are separate isolates,
PHP instances are separate Zend contexts, and ASGI overlaps asynchronous
requests on an event loop. All cases use the same machine, listener, client
concurrency and duration, with the other applications still loaded. The report
records CPU use and RSS alongside throughput, so additional parallelism is
visible together with its resource cost. Framework and adapter work remains
part of each labeled application.

For focused tuning, repeat `--runtime` to measure selected paths and write to a
separate report file. The default still measures all five. Keep profiled
diagnostic runs separate from the final unprofiled throughput measurements.

The report records the machine description, source revision, release binary
hash, exact elapsed time, completed requests, CPU-time and RSS samples, and
wrk error counters. CPU uses cumulative process time at the two boundaries,
not an instantaneous `ps %cpu` value; 100% means one fully used core. Resource
sampling runs outside the timed request loop. RSS
includes all five loaded runtimes; the sequential per-path deltas do not
isolate a language's allocations. Raw
generator output stays under the ignored logs directory. The sample uses
one minute and 64 connections per path, with client and server sharing a
development machine, without CPU affinity. It describes this demo configuration; it is not a
like-for-like language or framework ranking.

## Ownership

Each serving worker embeds all five applications. Worker processes can scale
the composition; they are not language-specific processes. PHP instances have
their own Zend contexts, Deno owns its isolate threads, Lua owns its managed
state, and ASGI owns an event-loop thread. They share the host process and are
not crash/security isolation boundaries.

CPython and its environment are process-wide. The host drains ingress before
ASGI lifespan shutdown and joining its loop. No forced drain deadline,
filesystem watcher or automatic interpreter reload is added. Restart the demo
to apply Python source or environment changes. Lifecycle controls for each
runtime remain owned by the corresponding adapter; this example adds no
management server.
