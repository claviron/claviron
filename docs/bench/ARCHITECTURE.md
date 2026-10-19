# Native benchmark capability

The facade's opt-in `bench` module owns one language-neutral native HTTP workload implementation.
It depends on HTTP and runtime primitives, never on a language runner, engine,
database, cache, Orbit transport or load generator. The facade exposes it as
`claviron::bench` with its opt-in `bench` feature. Ordinary builds omit it.

The facade's independent `edge` and `runner` features expose adapters inside
the selected `bench` module, so enabling bench alone selects neither serving
composition. Both adapters may compile;
the embedding application explicitly chooses which one to register at boot.
No runtime caller detection, request-time mode branch or second routing registry
is introduced. Gufi/Lumenize register the runner adapter; Revolution registers
the Edge adapter. Registration uses the existing Register/Component/Routable
contracts, with no Runnable or background Task.

## Hosting

The Edge adapter retains Revolution's `bench.yaml` contract: an optional file
under `AppState::config_dir()` containing `hosting: VhostDocument`. Missing
configuration declares no benchmark hosts. Host declarations are read from
Congif's prepared snapshot through Edge's existing `register_hosts` API, so
resolver-before-provider reload order does not expose old hosting. The provider
keeps only prepared canonical keys for cold route selection. Full runtime
reload applies host changes; an explicit `register_with_path` can select another
source. Existing listeners, TLS, ACL and request policy remain authoritative.
No database-backed HostingService is required merely to declare these hosts.

The runner adapter declares no host or config source. It contributes routes
to the existing host-free registry; application interceptors retain precedence.
An optional code-selected alias prefix preserves old application URLs at route
construction, without another set of handlers. Raw custom services can compose
the shared router directly.

## Workloads

- GET/HEAD `/bench/plain`: static `ok` text.
- GET/HEAD `/bench/json`: per-request JSON serialization of `{message: ok}`.
- POST `/bench/echo`: incoming streaming body returned without whole-body collection.
- GET/POST `/_bench/rust`: existing native baseline, optional caller-declared
  `min`/`max` millisecond delay and synthetic `payload`; POST reports received
  bytes through `x-received` after collecting its input under the ingress/Axum limits.
- GET `/_bench/stats`: existing process-local activity, not aggregate fleet state.

The same handlers run over Edge, TCP/UDS runner ingress and Orbit ingress.
Only the ingress changes; these are not Python/PHP/framework measurements.
There is no invented wait or deadline when the caller supplies no delay.
Synthetic messages support the existing one-MiB fixture ceiling and cache one
body per requested size for the process lifetime. Use fixed payload sizes when
comparing retained memory. Echo retains no completed request body. No per-peer
history, independent admission policy, transport loop or SHM kind is added.

Verification is source review/formatting only until the operator rebuilds;
no library test boots a server or language runtime.
