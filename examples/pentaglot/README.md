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
