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
`PENTAGLOT_LISTEN` is resolved from the environment-file recipe; the example's
`.env` overrides an inherited value. For an isolated listener, copy the config
and set `listeners.demo.listen` explicitly. `FLEET`, `CONFIG_DIR` and `WORKERS`
select this instance's runtime resources. Server arguments after `--`
can select a distinct instance name, fleet, environment or configuration.

Pentaglot uses MiMalloc for Rust allocations. `--mimalloc release` sets immediate
unused-page purging with decommit; `--mimalloc retain` disables purging.
The default policy preserves inherited allocator settings.
`--mimalloc-purge-delay-ms N` overrides the selected policy (`-1` disables,
`0` purges immediately). These options take effect at process startup.

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

The PHP example selects Symfony's `8.2.x-dev` branch and locks its resolved
revisions. `--optimize` builds an authoritative Composer classmap and runs
`cache:clear --env=prod --no-debug`, including container and router warmup.
The same command is available through `apps/php/bin/console`.
The kernel runs with `prod` and debug
off. PHP scans the demo's `config/php.d` instead of the host CLI extension
directory: OPcache is enabled with 256 MiB, 20,000 accelerated files,
16 MiB for interned strings, `opcache.validate_timestamps=0`, and Xdebug is
not loaded from that directory. The selected PHP build must provide OPcache
as a built-in module or load it from its primary INI. The pool keeps eight
persistent PHP instances warm and can grow to 64 under demand. `engine.warmup`
compiles the application, vendor and generated cache
files into the serving process's OPcache, excluding test directories. This
background warmup is separate from Symfony's generated production cache;
wait for its completion log before benchmarking. Restart after changing PHP
source because timestamp validation is disabled.

For the framework-free PHP variant, change `script_path` in `config/php.yaml`
to `apps/php/plain.php` and remove `frameworks: [Symfony]`. Keep the same pool
and `/php` route. This selects one entry script for the existing application;
it does not start another pool or a separate PHP server. The script loads no
Composer autoloader or Symfony kernel. It boots once per PHP instance, receives
request messages with `claviron_application_receive()`, and returns buffered
JSON with `claviron_application_respond()`. Claviron owns HTTP ingress and
drain; the receive loop ends when its application stops. The default entry
remains Symfony.

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

Linux x86-64; two Xeon E5-2650 v2 CPUs, 16 physical cores / 32 logical CPUs,
125 GiB RAM. Release/native with mimalloc release; Tokio default multi_thread.
Each language uses a fresh master and 16 workers, 128 connections and 5 minutes
of load, followed by 120 seconds idle with the server still running.

| Language | Successful req/s | CPU µs/request | p99 (ms) | RSS start → end → +120s (MiB) | Errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| Rust | 280,113 | 69.39 | 8.151 | 2,942.3 → 2,994.8 → 2,994.8 | 0 |
| Lua | 108,810 | 198.69 | 5.072 | 2,936.6 → 3,014.9 → 3,014.9 | 0 |
| Deno | 95,398 | 236.77 | 8.110 | 2,464.0 → 3,326.5 → 3,307.9 | 0 |
| PHP / Symfony 8.2-dev | 32,547 | 919.39 | 12.005 | 3,439.3 → 3,548.5 → 3,482.3 | 0 |
| PHP / plain | 134,978 | 156.60 | 10.391 | 2,798.5 → 2,905.3 → 2,901.2 | 0 |
| Python / Gufi-ASGI | 123,932 | 194.95 | 6.694 | 2,936.7 → 3,015.9 → 3,018.0 | 0 |

CPU is aggregate master/worker CPU time per completed request. RSS sums those
17 processes with all five runtimes loaded; shared pages count per process.
The Deno row uses one isolate per worker. Rust, Lua and Python retain the
original binary; both PHP rows use the optimized embed bridge and eight Deno
isolates per worker. PHP starts with eight warm contexts in every case.
Plain PHP retains its earlier pool and warmup packages. Symfony uses 8.2-dev,
production cache warmup and the explicit OPcache settings above.
The [original measurements](public/assets/benchmark.json),
[earlier Plain PHP report](public/assets/benchmark-php-plain.json),
[updated Plain PHP report](public/assets/benchmark-php-plain-optimized.json),
[Symfony 8.2 report](public/assets/benchmark-symfony82.json),
and [updated Deno report](public/assets/benchmark-deno-multiplexed.json) include
settings, source revisions, CPU totals and idle RSS samples. See
[BENCHMARK.md](BENCHMARK.md) for the full history.

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
