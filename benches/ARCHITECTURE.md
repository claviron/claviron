# Benchmarks

Each entry point is a `[[bench]]` target in [Cargo.toml](../Cargo.toml).
[scripts/bench-http.py](../scripts/bench-http.py) selects, builds and runs a target;
it keeps results under ignored `.benchmarks/http/`. [README.md](README.md) covers
commands and comparison controls. This file indexes the implementation.

## Measurement boundaries

| Path | Timed work | Prepared before timing |
| --- | --- | --- |
| `dispatch/` | Decoded request through response consumption. Plain Edge cases also isolate request binding. | Host plans, application registration, request construction. |
| `transport/http1/` serial targets | Client/server protocol work, request construction, socket IO and response consumption on one established connection. | Host publication, bind/accept, connection setup and preflight. |
| `transport/http1/concurrent.rs` | Concurrent requests; server process user and system CPU divided by completed requests, alternating with an optional control. | Separate processes/runtimes, connection setup and preflight. Warmup is untimed and carries the allocation count. |

Edge targets execute production `HostExecution` or `ConnectionHandler` code.
Native controls call Hyper or Axum directly. Plain and JSON workloads remain
separate. No TLS benchmark is registered.

## Entry points

### `dispatch/native/`

| File | Cargo target | Workload |
| --- | --- | --- |
| [hyper.rs](dispatch/native/hyper.rs) | `dispatch-hyper` | Hyper service, JSON. |
| [axum.rs](dispatch/native/axum.rs) | `dispatch-axum` | Axum router, JSON. |

### `dispatch/edge/`

Plain targets compare `native`, `bind` and `host` through the same response
workload. `bind` retains the request without executing host layers; `host`
executes the compiled plan.

| File | Cargo target | Registered owner |
| --- | --- | --- |
| [other.rs](dispatch/edge/other.rs) | `dispatch-edge-other` | `OtherHandler`, plain. |
| [axum.rs](dispatch/edge/axum.rs) | `dispatch-edge-axum` | Axum router, plain. |
| [pipeline.rs](dispatch/edge/pipeline.rs) | `dispatch-edge-pipeline` | `Routable` contribution, plain. |
| [json.rs](dispatch/edge/json.rs) | `dispatch-edge-json` | `Routable` contribution, JSON. |

### `transport/http1/native/`

| File | Cargo target | Server / workload |
| --- | --- | --- |
| [hyper.rs](transport/http1/native/hyper.rs) | `http1-hyper` | Hyper / JSON. |
| [axum.rs](transport/http1/native/axum.rs) | `http1-axum` | `axum::serve` / JSON. |
| [hyper_plain.rs](transport/http1/native/hyper_plain.rs) | `http1-plain-hyper` | Hyper / plain. |
| [axum_plain.rs](transport/http1/native/axum_plain.rs) | `http1-plain-axum` | `axum::serve` / plain. |

### `transport/http1/edge/`

All three use the native Edge accepted-stream handler, including host selection.

| File | Cargo target | Registered owner |
| --- | --- | --- |
| [other.rs](transport/http1/edge/other.rs) | `http1-plain-edge-other` | Other handler. |
| [axum.rs](transport/http1/edge/axum.rs) | `http1-plain-edge-axum` | Axum router. |
| [routable.rs](transport/http1/edge/routable.rs) | `http1-plain-edge-routable` | `Routable` contribution. |

### Concurrent entry point

[transport/http1/concurrent.rs](transport/http1/concurrent.rs) defines the
`http1-concurrent` executable on Unix. Its `--target` selects Hyper, Axum,
Edge Other, Edge Axum or Edge Routable; all use the plain workload.

```text
concurrent::run
  Side::start, for the target and then the optional control
    process::Server::spawn -> server::run
      execution::Running::start -> server::start
    client::Clients -> preflight -> counted warmup
  measured windows, alternating between the sides
  Side::finish: client shutdown -> server shutdown -> executor drain/finalize
```

## `support/`

| File | Responsibility / callers |
| --- | --- |
| [mod.rs](support/mod.rs) | Shared module declarations, imported by every entry point; Axum helpers follow the `axum` feature. |
| [decoded.rs](support/decoded.rs) | Criterion JSON dispatch timing and response consumption; native JSON and Edge JSON targets. |
| [plain.rs](support/plain.rs) | Static plain request/response checks and paired dispatch timing; plain dispatch and HTTP/1 targets. |
| [json.rs](support/json.rs) | Shared hostname, JSON payload, request/response construction and checks. |
| [axum.rs](support/axum.rs) | Plain and JSON Axum routers for native and registered Edge owners. |
| [edge.rs](support/edge.rs) | Register Other/Axum/Routable owners, compile host plans and prepare production HTTP/1 connection handlers. |
| [setup.rs](support/setup.rs) | Parse embedded HTTP/vhost fixtures and prepare dispatch connection facts without provider boot. |
| [http1.rs](support/http1.rs) | Serial loopback servers, Hyper client, host preflight and Criterion RTT timing; concurrent clients reuse preflight and reply checks. |

### `support/http1/`

| File | Responsibility |
| --- | --- |
| [concurrent.rs](support/http1/concurrent.rs) | CLI types, targets, execution mode, alternating measurement windows, per-owner totals, paired comparison and report assembly. |
| [concurrent/process.rs](support/http1/concurrent/process.rs) | Spawn/join one server child per owner and exchange typed control messages over stdin/stdout. |
| [concurrent/server.rs](support/http1/concurrent/server.rs) | Server control loop, CPU sample and allocation-count boundaries, target selection and connection drain. |
| [concurrent/allocation.rs](support/http1/concurrent/allocation.rs) | Counting wrapper over the system allocator; enabled only around the warm-up load. |
| [concurrent/execution.rs](support/http1/concurrent/execution.rs) | Own the server runtime and optional `CurrentThreadDriver`; retain the submitted task until shutdown, then drain/finalize. |
| [concurrent/client.rs](support/http1/concurrent/client.rs) | Persistent connections, synchronized request loops that wait for connection readiness, complete-response counts and client teardown. |
| [concurrent/cpu.rs](support/http1/concurrent/cpu.rs) | Unix `getrusage(RUSAGE_SELF)` user and system CPU, kept separate, for all server process threads, excluding the client process. |

`shared` serves on the main multi-thread runtime. `current_thread` keeps that
runtime and places HTTP on one additional Claviron current-thread executor.
Both apply to every concurrent target; execution setup/teardown is not timed.

### `support/fixtures/`

All four files are embedded by `setup.rs` and deserialized through production
configuration types.

| File | Use |
| --- | --- |
| [http.yaml](support/fixtures/http.yaml) | Plain listener declaration and disabled body limits for decoded dispatch setup. |
| [vhosts.yaml](support/fixtures/vhosts.yaml) | Minimal dispatch host, without optional policy/backend layers. |
| [http1.yaml](support/fixtures/http1.yaml) | Loopback ephemeral listener and H1 settings matched to native controls. |
| [http1-vhosts.yaml](support/fixtures/http1-vhosts.yaml) | H1 host and alias used by known/unknown-host preflight. |
