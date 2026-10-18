# claviron-lua

Lua 5.5 scripting and managed applications embedded in Claviron.

The engine runs text scripts with explicit Rust-provided capabilities. Short
scripts get a fresh state per execution; HTTP applications retain one state per
application per worker. Those are different lifetime contracts, not a pool of
interchangeable request states.

Applications normally use `claviron::lua` through the root `lua` feature. Lua is
vendored through `mlua`: the build compiles its interpreter rather than depending
on a system Lua installation. This crate is an engine, not a standalone runner
binary, and does not require Edge to execute scripts.

## Script execution and extensions

`Engine` holds registered extensions, a module search path, and an explicit
execution environment. `Script` carries a name, source, arguments, and optional
output callback. `Engine::execute` creates a state, runs the script as a future,
and drops its state when execution finishes.

The `Extension` trait supplies a named module to `require`; it builds a table
against the current Lua state and `Execution`. An extension can also expose its
module under the `claviron` global. Register extensions once on the engine, not
by performing runtime discovery inside each script operation.

`Engine::new()` is a minimal engine. The managed `host::engine` builder installs
JSON, regex, and routing support, plus enabled capabilities for which the host
has supplied runtime resources. A low-level caller can choose its own extensions.

The sandbox opens selected standard libraries: base, string, table, math, UTF-8,
and coroutines. It removes `dofile` and `loadfile`, restricts `load` to text, and
does not expose `io`, `os`, or `debug`. `require` searches registered extensions,
bundled pure-Lua packages, and explicitly configured source directories, in that
order. It does not load arbitrary C modules or use the host's `package.path`.

This is an in-process capability boundary, not process isolation. Async waits
can yield; CPU-bound Lua is still CPU-bound. Cancellation wakes registered waits
and lets execution finish cooperatively; it is not preemption of arbitrary Lua
code and does not create an implicit execution deadline.

## Managed HTTP applications

`lua.yaml` declares application modules and source search directories. Relative
paths are process-working-directory-relative, not relative to the YAML file.
A minimal application declaration is:

```yaml
applications:
  example:
    module: apps/example.lua
```

The module returns a value with a `fetch` method, including a `claviron.router`:

```lua
local router = claviron.router()
router:get("/hello/{name}", function(request, params)
  return "Hello " .. params.name
end)
return router
```

Declaring the application does not mount an HTTP path by itself. Edge or another
HTTP consumer supplies typed Lua route declarations naming that application.
The `app` implementation surface owns the raw `Request<RouteBody>` handler;
the optional `axum` surface adds route builders without moving engine ownership
into Edge.

Requests carry method, path, query, headers, and body-reading methods. Responses
can be strings or tables with status, headers, and a body; a function-valued body
produces streamed chunks until it returns `nil`. Route request/response buffering
policy determines buffering at the HTTP boundary.

Globals and loaded modules survive between application requests. Keep
request-specific values in locals. `concurrency` bounds in-flight coroutines
and defaults to one; `max_waiting` bounds the permit queue and defaults to 256.
Excess queued work is rejected rather than growing an unbounded backlog.
Concurrency overlaps waiting coroutines; it does not make Lua computation
parallel within one state. A streamed reply retains its permit through completion.

The application manager reports runtime `lua` and, by default, framework `Olua`.
Start, stop, restart, and reload operate on application generations. Replacement
loads a new state before publication; a failed replacement leaves the previous
state serving. Requests using a retired state finish there. An explicitly stopped
application stays stopped across reloads.

## Shared capabilities and configuration

The root facade composes the engine's local features rather than exposing every
implementation feature as a separate application choice:

| Facade selection | Capability |
| --- | --- |
| `lua` | Engine, managed configuration/applications, and Orbit integration. |
| `lua` + `tasks` | Lua scripts as task-service executions. |
| `lua` + `websocket` | HTTP/1 Upgrade and HTTP/2 extended-CONNECT socket handlers. |
| `lua-fetch` | Outbound HTTP, subject to `lua.yaml` host policy. |
| `lua` + `postgres` or `mysql` | Query adapters using Claviron's configured database pools. |
| `lua` + `log-pipeline` | Records to declared log channels. |

Orbit extensions provide cells, streams, events, and invocations over the host's
registered resources. Shared state belongs there, not in another script's Lua
globals. Bounded event and invocation history is not a durable exactly-once queue.

Outbound fetch is closed unless `fetch.allow` permits a destination; redirects
are checked too. Its pooled client and optional response-size bound come from
configuration. Host allow rules are not a complete network-isolation mechanism.
Database statements use bind parameters and configured pools; the current
SQLite feature selects pools but does not implement a Lua SQLite query adapter.
Unsupported SQL value types fail explicitly rather than receiving guessed Lua
representations.

Application `env_file` and inline `env` declarations prepare detached execution
values through the shared [Dotenw](https://crates.io/crates/dotenw) infrastructure.
`claviron.getenv` reads those values, not the server's process environment. An
environment update replaces only the affected application's state; invalid
replacement data leaves the previous generation serving.

## Tools and current boundaries

The managed engine installs `claviron.json`, Rust-backed byte regexes, and the
router. Bundled `inspect`, `lume`, `middleclass`, and `serpent` modules require no
runtime installation; functions needing unavailable standard libraries still
cannot bypass the sandbox. Their sources retain their own MIT notices.

[claviron.d.lua](claviron.d.lua) supplies lua-language-server annotations for
editor completion. This is tooling metadata, not a runtime module.

WebSocket behavior is opt-in and uses declared endpoint paths and queue/message
limits. WebTransport and a dedicated Lua runner owner are not implemented by
this crate. Compiling an optional capability does not configure its external
resources or mount it publicly.

## Related crates

- [App](../app/README.md): managed application identity and lifecycle.
- [Tasks](../tasks/README.md): triggers, placement, and execution policy.
- [Orbit](../orbit/README.md): shared state and same-host coordination.
- [HTTP](../http/README.md): transport-neutral policy and routing declarations.
- [DB](../db/README.md): runtime-owned connection pools.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
Bundled third-party Lua sources retain their own license notices.
