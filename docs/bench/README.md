# claviron::bench

Opt-in native HTTP workloads for comparing Claviron serving compositions.

The same Rust handlers can run through an Edge host, a standalone runner, or a
caller-owned router. Changing ingress does not change the workload or turn it
into a PHP, Python, Lua, or framework benchmark. This module is not a load generator
and does not open a listener.

Applications use `claviron::bench` through the root `bench` feature. That feature
selects Axum support but neither the Edge nor runner composition. The facade
exposes each adapter only when its corresponding composition is also selected;
registration remains explicit.

## Shared workloads

| Method and path | Work performed |
| --- | --- |
| `GET /bench/plain` | Return static `ok` text with a newline. |
| `GET /bench/json` | Serialize a small JSON reply on each request. |
| `POST /bench/echo` | Return the incoming body as a stream without collecting it whole. |
| `GET` or `POST /_bench/rust` | Collect the input, optionally delay, and return a native baseline reply. |
| `GET /_bench/stats` | Read process-local activity counters. |

GET routes also receive Axum's HEAD behavior. `routes()` builds the router,
the same everywhere.

The native baseline accepts optional `min` and `max` delay bounds in milliseconds
and a `payload` size. Without either delay bound, it adds no wait. Its `x-received`
header reports collected input bytes; ingress and extractor body limits still
apply. This is intentionally different from the streaming echo workload.

Synthetic payloads are capped at one MiB. One reply per requested size is retained
for the process lifetime, avoiding repeated generation on the measured path.
Use fixed payload sizes when comparing memory retention. Activity counters are
local observations, not a fleet-wide accounting or benchmark-results service.

## Register with the existing composition

`claviron::bench::BenchService` is one service for an Edge and a runner alike.
It is a `HostRoutable` (`claviron::pipeline::routing`): routes that also select
the hosts they answer on. Register it in the application's service-registration
callback:

```rust
use claviron::runtime::{AppState, Register, registry};

fn register_bench(state: &AppState) -> registry::Result<()> {
    <claviron::bench::BenchService as Register<AppState>>::register(state)
}
```

It reads optional `bench.yaml` configuration under the runtime's
configuration directory. Its `vhosts` field names hosts declared in `vhost.yaml`;
it declares none itself. An absent file selects no hosts. `register_with_path`
selects another source explicitly. Listener, TLS, ACL and HTTP policy are the
host's, as `vhost.yaml` declares them.

Where the process serves hosts, an Edge, the routes stand in front of each
selected host's own owner, and a path or method they do not declare goes on to
it. A runner serves the same routes host-free. Application interceptors retain
precedence.

It registers shared handlers; it does not start a background task,
select an interpreter, add a database dependency, or choose deployment admission
policy. Keep benchmark exposure an explicit deployment decision.

## Benchmark reports

Measurements of complete serving paths, Claviron beside native Hyper, on one
Linux machine that client and server share.

- [Where a response's serving CPU goes](response-cpu.md): the budget of one
  plain response, row by row, and where each row is decided.
- [HTTP/1.1 plain text with 128 connections](http1-plain-128.md)
- [HTTP/1.1 plain text with 1,024 connections](http1-plain-1024.md)
- [HTTP/1.1 over TLS with 1,024 connections](http1-tls-1024.md)
- [TLS connections: what establishing one costs](tls-connections-1024.md)
- [HTTP/2 over TLS with 1,024 connections](http2-tls-1024.md)

Three things hold for all of them:

- **A boundary belongs to client and server together.** They run on the same
  40 CPUs. The level at which the SLA breaks compares targets under the same
  conditions; it is not a server's capacity.
- **Serving CPU per response is the figure that travels.** It sums user and
  system time of the serving processes and leaves out the supervisor and the
  load generator, so it can be compared at any level both targets accept.
- **The load generator matters.** Reports made with `break` 0.2 say so; their
  figures are not comparable with earlier ones at a pipeline depth above one.

## Related crates

- [Edge](../../crates/edge/README.md): virtual-host routing and common admission.
- [Runner](../../crates/runner/README.md): single-application ingress and composition.
- [HTTP](../../crates/http/README.md): request/response policy and body contracts.
- [Runtime](../../crates/runtime/README.md): registration and process-local activity.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
