# claviron-module Architecture

## Boundary

This crate loads compiled libraries into the process and exposes Claviron to them through a C
ABI. It is the only crate that knows that ABI: `http`, `pipeline`, `app` and `transport` have no
C types and no `abi` feature. It uses their ordinary Rust APIs, the way the PHP engine's functions
do. A module links against nothing of ours, so all it can reach is the host table.

A module is in the process and is not an upstream: no gateway stands in front of it, and it is not
reached over Orbit. A library in its own process is the C binding over Orbit, listed as a linked application.
That process's runnables are not Continuo services here, and nothing pretends they are.

## Sources

- [`include/claviron_module.h`](include/claviron_module.h) — The contract.
- [`abi.rs`](src/abi.rs) — The same shapes in Rust. No Tokio and no HTTP.
- [`registrar.rs`](src/registrar.rs) — The host table, and what one library's register function registered. Every call is contained.
- [`config.rs`](src/config.rs) — `modules.yaml`. Nothing has a default.
- [`provider.rs`](src/provider.rs) — The one component: loads, validates, boots, finalizes.
- [`pool.rs`](src/pool.rs) — The OS threads handlers run on.
- [`application.rs`](src/application.rs) — The backend a `module:` route selects, and the controller that lists and manages applications.
- [`request.rs`](src/request.rs) — One exchange: the request's head in place, the reply being built, a session's messages.
- [`service.rs`](src/service.rs) — A service as a runnable, and its cancel signal.
- [`handlers.rs`](src/handlers.rs) — Operations and event subscribers, and the host calls that need the process.

## One component

The registry takes one instance per Rust type, and every native component would be the same type,
so one `ModuleProvider` holds them all and a registrar fills its lists. Services become named
instances through `Component::runnables()`, each with its own `InstancePlacement`.

The provider owns `modules.yaml` through `Configurable`. It loads libraries in `boot`, not
`configure`, which can run again and would register twice. Continuo lets a preparation component
publish complete services this way, and refuses a new config owner during boot, so a library's own
settings are its own files. `HostAppResolver` boots late, so a `module:` route resolves against
what was registered. The factory runs several times per host and only looks up.

A configuration check loads the libraries and validates names, and runs nothing of theirs but
register. A master boots a module only if one of its services can run there, and runs no application
or handler. A process with nothing of a module to run boots nothing of it.

A changed `modules.yaml` is refused by `configure`: a library cannot be unloaded, and half applying
a change is worse than saying so. Nothing reloads it.

## Execution

A library is compiled code, not a language to host. A request is a function call on one of the
application's OS threads, reached from Tokio through a queue:

```text
Tokio   connection, plan, policy
  | queue (max_waiting; queue_timeout waits here and nowhere else)
thread  a handler (one of `instances`)
  | oneshot, bounded channel
Tokio   the response
```

Native code never runs on a Tokio worker: `boot`, `run`, `start`, `stop` and handlers all have
threads. A body stays on Tokio and each frame is handed over, the next after the last callback
returns. A handler may answer later from any thread; `instances` bounds only the callbacks running
at once.

There is no `execution_timeout` and no `retry`. PHP interrupts its VM and replaces a failed worker.
Native code cannot be interrupted, and a thread calling a function has nothing to replace.

## One boundary

The Rust side holds `Request<RouteBody>`'s parts and builds a `RouteResponse` directly. C gets an
opaque handle and calls accessors, which return bytes in place, so nothing is copied or serialized
unless the module asks. A reply's bytes become `Bytes` once.

A handler runs on another thread, so `HostContext`'s borrow cannot reach it. The backend declares
`BackendIdentity::Request`, and the connection and effective policy travel in the request's extensions.

Invocations and events cross as JSON bytes: `InvocationRouter` speaks JSON already, and `EventValue`
has its JSON form.

## An exchange's end

`closed` runs exactly once for what reached a handler, whichever way it ended: the reply's body ended,
the body was dropped because the client left, the client left before the head, or the module failed.
The handle stays valid until it returns, because the queued task holds an `Arc`. An exchange that
never started is not heard of.

A handler failing before the head is a 500. After the head, the reply is cut with an error rather
than looking whole. A session's proposal goes through `Offer` and `Claim` like any other, so what stands
in front applies, an unclaimed attempt is refused, and only the door commits. Stopping an application
tells its open sessions with a close of 1001 and then waits for them, with no bound of its own.

`Error::Recoverable` carries a service's failure: that service reads failed and the server carries on.

## Tests

`tests/provider.rs` pins the validation against `modules.yaml` with functions standing in for a library.
What needs a real library, threads and a client runs from the C module example, in C and C++.
