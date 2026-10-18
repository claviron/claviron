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

GET routes also receive Axum's HEAD behavior. `routes()` builds the full router;
`baseline_routes()` exposes `/rust` and `/stats` relative to a caller-selected
mount.

The native baseline accepts optional `min` and `max` delay bounds in milliseconds
and a `payload` size. Without either delay bound, it adds no wait. Its `x-received`
header reports collected input bytes; ingress and extractor body limits still
apply. This is intentionally different from the streaming echo workload.

Synthetic payloads are capped at one MiB. One reply per requested size is retained
for the process lifetime, avoiding repeated generation on the measured path.
Use fixed payload sizes when comparing memory retention. Activity counters are
local observations, not a fleet-wide accounting or benchmark-results service.

## Register with the existing composition

For an Edge application, enable `bench` and `edge` and register the adapter in the
application's service-registration callback:

```rust
use claviron::runtime::{AppState, Register, registry};

fn register_bench(state: &AppState) -> registry::Result<()> {
    <claviron::bench::edge::BenchService as Register<AppState>>::register(state)
}
```

The Edge adapter reads optional `bench.yaml` configuration under the runtime's
configuration directory. Its `hosting` field uses Edge's `VhostDocument` schema.
An absent file declares no benchmark hosts. `register_with_path` selects another
source explicitly. Prepared host declarations participate in Edge publication
and reload, while existing listener, TLS, ACL, and HTTP policy remain authoritative.

For a host-free runner, enable `bench` and `runner`, then register
`claviron::bench::runner::BenchService` through the same `Register<AppState>`
contract. It contributes routes to the existing routing registry and declares
no vhost or configuration file. `register_with_alias_prefix` optionally adds a
caller-selected mount for the baseline routes. Application interceptors retain
precedence.

These adapters register shared handlers; they do not start a background task,
select an interpreter, add a database dependency, or choose deployment admission
policy. Keep benchmark exposure an explicit deployment decision.

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
