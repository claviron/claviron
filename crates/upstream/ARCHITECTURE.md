# claviron-upstream — Architecture

The former library tests, fixtures and benchmark harnesses have been removed.
Measurements recorded below are historical evidence, not a current Cargo test
suite. System boot, network listeners and performance harnesses belong outside
the library's tests; any future regression test must pin a necessary behavior.

`upstream` is the reverse-upstream primitive for
Claviron vhosts. It lets an existing HTTP application remain behind a
Claviron host while Claviron owns edge transport, host policy, local
file/static handling, availability signals, and the step-by-step Rust
migration path.

The crate is not a second web server and not a generic proxy sidecar.
The optional `edge` module registers the `upstream` BackendFactory. It compiles
selected application/profile handles at boot/reload and declares identity and
external SSE ownership through Edge's common Backend contract. Edge depends on
no Upstream implementation. There is no Axum dependency or Upstream RustRouter
variant. Low-level outbound services remain available without the `edge` feature.

## Boundary

```text
Edge host policy -> Rust application -> Static -> TryFiles
  -> prepared Upstream backend -> origin response
```

The selected service retains request/response IO and live profile ownership.
The origin may be any supported HTTP server, including another Claviron edge;
ingress admission and outbound authority/TLS policy remain independent.

---

## 2. Protocol Boundary

```text
client -> Claviron:
  H1 / H2 / H3

Claviron -> upstream:
  H1
  H2C
  H2 over TLS
  H3: not planned
```

H3 is an edge ingress capability, not an upstream transport. The pool
must not carry QUIC/H3 concepts into backend connection identity,
capacity, or metrics. `tcp://` and `unix:` origins speak HTTP/2 without TLS. `tls://` IP targets use
profile-prepared ALPN, SNI, trust and an optional client identity.
`https://` remains rejected: protocol is explicit.

### Certificates come from the process certificate manager

There is no upstream TLS feature and no certificate directory in this crate.
`tls.yaml` already loads this deployment's certificates and names the
authority it issues them under; a compile borrows that material through
`claviron_tls::{CertResolver, CertAuthority}` and asks for it by name:

```text
verify: native      the operating system trust store — somebody else's backend
verify: authority   the CA tls.yaml issues under — our edge, our backend, our CA
verify: ca + ca:    an explicit PEM trust store a third party gave us

client: <name>              an identity tls.yaml loaded, read again per handshake
client: {cert_path, key_path}  a third party's own material, read from its files
```

The two client spellings differ in what a change means. A named identity is
resolved at each handshake, so renewing the certificate behind it reaches the
next connection and is *not* a profile change: live pools are retained. Paths
are read at compile time and their bytes take part in profile equality, so
rotating a file replaces the epoch exactly as a trust store rotation does.

A name is proved present once, at compile time, so a misspelling fails the
boot rather than an unauthenticated handshake much later. A process with no
certificate manager registered lends nothing: `verify: native` and plain
origins still compile, while a profile that asks for a name or the authority
fails with its own message instead of inventing trust.

---

## 2b. Public surface

The crate root holds one thing: `register` and `UpstreamProvider`, because
registering the provider is the whole entry point. Everything else is reached
through the module that owns it, so a consumer reads the surface the way the
crate is arranged:

```text
claviron::upstream::register / UpstreamProvider   boot and reload owner
claviron::upstream::app        UpstreamApp
claviron::upstream::edge       optional BackendFactory registration
claviron::upstream::service    UpstreamService, UpstreamLifecycle,
                               UpstreamRequestPolicy, MembershipError/Target
claviron::upstream::origin     OriginId, OriginHealth, PoolGeneration, ConnectTarget
claviron::upstream::config     the typed declarations an operator writes
claviron::upstream::resolve    resolution status, tables and publication
claviron::upstream::metrics    the recorded surface
```

One name has one public path: nothing is re-exported twice, and the request
path, the compiler and the process-local state stay private. A future
management surface (an `UpstreamManager`, a gRPC service) composes these
modules rather than reaching past them.

## 3. Layered Type Model

There is no "client pool" in the capacity/lifecycle sense.

```text
UpstreamService (one Arc<ServiceInner>):
  cheap logical handle; immutable Arc<Profile>, health, protocol pools and runtime context

TargetPool:
  physical pool for one prepared Origin; owns real H1 connections

H1Connection:
  one real TCP/UDS, optionally TLS, + Hyper H1 connection

H2TargetPool:
  physical connection pool for one OriginId; each connection owns stream capacity

H2Connection:
  one real TCP/UDS, optionally TLS, + Hyper H2 connection with many concurrent streams
```

`UpstreamService` is cheap and cloneable. `TargetPool` is where
idle, leased, connecting, live capacity, generation, passive health,
and dirty-drop behavior live.

`H2TargetPool` does not reuse the H1 idle/lease lifecycle. H2 capacity
is stream based within each connection. `H2Permit` retains the connection
and its stream reservation through response EOF/error/drop. Nonempty request
bodies share that reservation with their InputLease, so an early response
cannot free capacity or close an unfinished upload. This is also the SSE
cancellation contract: SSE is a normal response body, not an upgrade;
dropping the downstream body closes the H1 lease dirty or releases the
H2 stream permit.

```text
live = idle + leased + connecting
```

There is no pool member that is merely "created but not connected".
Configured origins and client factories are runtime plans, not pool
members.

### Pool management

Each serving process owns one pool per profile/origin. A profile chooses H1 or
H2; each origin chooses its transport.

| HTTP mode | TCP target | UDS target | Admission per connection |
| --- | --- | --- | --- |
| H1 plain | `tcp://` | `unix:` | One request |
| H1 TLS | `tls://`, ALPN `http/1.1` | Not supported | One request |
| H2c | `tcp://`, prior knowledge | `unix:`, prior knowledge | Effective local/peer stream limit |
| H2 TLS | `tls://`, ALPN `h2` | Not supported | Effective local/peer stream limit |

```text
request -> profile balance policy -> origin A -> pool A -> connection -> stream
                                 -> origin B -> pool B -> connection -> stream
```

| Setting | Effective scope | H1 | H2c / H2 TLS |
| --- | --- | --- | --- |
| `balance.algorithm` | Across the profile's eligible origins | Selects an origin | Selects an origin |
| `capacity.max_live` | Each origin's pool, per process | Physical connection ceiling | Physical connection ceiling, including dialing/draining connections |
| `capacity.max_idle` | Each origin's pool, per process | Retention ceiling for empty connections | Retention ceiling for empty connections |
| `capacity.min_idle` | Each H1 pool | Live floor, including leased/connecting connections | Rejected |
| `capacity.max_streams` | Each H2 connection | Rejected | Optional local ceiling; effective limit also respects peer SETTINGS |
| `capacity.warmup` | Lifecycle activation of each origin pool | Async preparation of the min_idle floor | Async preparation of one connection |
| `connection.idle_timeout` | Each connection | Expiry checked on reuse above the floor | Driver-owned idle expiry |
| Origin `max_live` | `balance.algorithm: first` only | In-flight request threshold | In-flight stream threshold, not a physical connection limit |

`warmup` defaults to false. `max_idle` defaults to max_live and is a ceiling,
not a target to replenish. H2 reuses available streams first, opens another
connection when all are full and the pool can grow, and otherwise queues.

| Balance algorithm | Origin-selection input |
| --- | --- |
| `round_robin` (default) | Origin order and weights |
| `least_connections` | Total in-flight work across each origin's pool, adjusted by weight; worker or fleet scope |
| `peak_ewma` | Origin latency estimate and total pool in-flight work |
| `consistent_hash` | Configured request key and prepared origin hash ring |
| `first` | Worker-relative origin order; total pool load, available streams and remaining physical capacity |

Balance policy does not select connections inside a pool. H1 reuses an empty
connection or opens one; H2 reserves a stream on a reusable connection before
considering another dial. Empty retention and idle expiry never discard an
accepted request/body writer.

### Allocation sizing policy

Meaningfully tunable preallocation/reserve sizes are build-time parameters.
The library owns their defaults and validation; downstream applications may
override them through their own Cargo build environment, for example `[env]`
in `.cargo/config.toml`. They are not runtime YAML or reload settings. This
policy supersedes the earlier blanket decision against allocation sizing
configuration. `CLAVIRON_UPSTREAM_HEADER_RESERVE` is the additional spare entry
count (default `0`), read through `option_env!` with decimal/overflow validation.
Preparation reserves computed missing static rewrite entries plus required
Host/Connection restoration and that headroom, after forwarding cleanup and
static unset. Existing replacement keys and restoration keys already in `set`
are not counted twice. Zero keeps this computed reservation with no spare space.
`try_reserve` failure is ignored, preserving normal insertion behavior rather
than rejecting a request because its storage hint could not be fulfilled.

The former `header_reserve_sweep` measured only preparation, excluding request
construction identically for all candidates. The selected zero-headroom hint reduced
allocation counts in 28 of 108 measured shapes without increasing allocated
bytes or allocation counts in any shape. Latency samples are debug-build
microbenchmarks, not a throughput acceptance claim. Recorded evidence lives in
`docs/verification/upstream-header-reserve.bison.tsv`.

Storage hints must preserve forwarding, body bytes and protocol behavior, and
must not become request limits. The measured allocation-count acceptance
budgets remain independent and are accepted in plan §6.5. Cache/SHM geometry
is a separate contract; a local reserve does not by itself require fleet-wide
ABI compatibility.

### Module Layout

`src/service.rs` owns service construction, request policy checks and protocol
dispatch. Its constructor starts no tasks and requires no Tokio runtime.
`provider/registry.rs` owns one current service per profile in each process;
apps clone that service handle instead of creating pools or maintenance loops.
`UpstreamActiveProbe` is an Automatic Runnable ordered after UpstreamProvider.
Its one RunContext generation polls origin schedules inside the future; no
per-origin tasks are spawned. Epoch-tracked futures attach under admission and
release their maintenance tokens on completion or generation cancellation.
Targeted active stop leaves profile admission, requests and recovery untouched;
profile stop cancels and joins the attached futures, while restart attaches the
fresh epoch. Registry changes are broadcast; observers register before reading
the current snapshot to avoid losing reload/start wake-ups. UpstreamHealthBridge independently owns its subscriptions and startup replay.
The supervised master never claims worker probe duties or primes serving sockets;
its registry may observe current state and expire bounded policy bans. Detached/
standalone and worker roles may claim probe duties. The role is frozen in each
service runtime from AppState.

Fleet-less embedders boot without FleetProvider: origin state and probe duty
are process-local and no SHM is opened. Server standalone mode instead joins
the `clv` SHM fleet at node zero with the compiled fleet capacity and kind 222
origin table, but registers no FleetEventRegistry. Its serving process owns
both coordinator and consumer work; health events stay on the local bus.

The process driver owns only retirement/drain work; it never dispatches probes
or applies health events.

```text
config.rs, config/*               file declarations, built-in policy and profile-local overrides
config/profile/*                  capacity, connection, timeout and health options
compile.rs, compile/*             profile/origin/app validation and compiled construction
compile/profile.rs                immutable Profile and its capacity
app.rs, app/matcher.rs            compiled and runtime app matching
app/current.rs                    DNS-feature atomic current service behind a captured app
origin.rs, origin/*               origin identity, connect targets, generations and health

proxy.rs                          the request path, in the order one request walks it
proxy/service.rs                  request policy checks, protocol dispatch and service construction
proxy/service/policy.rs           request upgrade and extended CONNECT policy
proxy/service/kind.rs             request predicates
proxy/service/lifecycle.rs        admission, bounded profile/view drain and fresh epochs
proxy/service/control.rs          stable profile lifetime and active/draining native owners
proxy/service/membership.rs       DNS-feature local member transfer, policy checks and pool reuse
proxy/service/runtime.rs          protocol pools and the detached/process runtime context
proxy/service/request.rs          shared URI/Host/hop/forward preparation and RequestScope
proxy/service/attempt.rs          generic pre-byte acquisition and origin failover
proxy/service/retry.rs            explicit replay, budget window and native send legs
proxy/service/breaker.rs          optional worker/profile admission reservations
proxy/service/transport.rs        independent per-origin native IO cancellation and joined ownership
proxy/service/tunnel.rs           shared listener-owned upgraded IO submission
proxy/service/error.rs            HTTP error mapping
proxy/balance.rs                  Balancer contract and allocation-free selectable candidates
proxy/balance/key.rs              borrowed header/cookie/path/trusted-IP request keys
proxy/balance/round_robin.rs      atomic configured-origin ordering
proxy/balance/consistent_hash.rs  immutable compiled virtual-node ring
proxy/balance/weight.rs           smooth RR and fleet-clock slow-start weights
proxy/balance/peak_ewma.rs        worker response-header peaks and idle decay
proxy/connect.rs, connect/*       bounded TCP/Unix dial and protocol handshake selection
proxy/connect/io.rs               optional H1 peer HTTP-byte evidence
proxy/h1.rs, h1/*                 handler, pool, conn, body, lease and tunnel owners
proxy/h2.rs, h2/*                 handler, pool, conn, body, permit and tunnel owners
proxy/h2/executor.rs              origin-owned Hyper transport/request-writer tasks
proxy/body.rs, body/timeout.rs    body errors and request read-idle wrapper
proxy/body/buffer.rs              bounded opt-in replay payload/trailers and overflow stream
proxy/lease.rs                    what one request holds on the service while it runs
proxy/failure.rs                  RequestFailure and DirtyReason

health.rs                         slot-backed health and local pool/metrics projections
health/active.rs                  Automatic active-health Runnable and registry rounds
health/active/origin.rs           origin schedule polled by the active generation
health/tasks.rs                   shared registry rounds/cold futures; no child spawn
health/drop.rs                    driver termination probes
health/recovery.rs                Automatic recovery Runnable over current slot health
health/recovery/origin.rs         owned recovery schedule/reservation and backoff policy
health/lock.rs                    typed acquisition/renewal of probe duties
health/lock/held.rs               exact RAII/provider release point and vacancy wake-up
health/probe.rs                   typed probe kinds/outcomes and stable labels
health/probe/failure.rs           owned typed reachability failure facts
health/probe/message.rs           streaming early-EOF diagnostic classification
health/probe/outcome.rs           one owned-verdict application boundary
health/probe/l4.rs                TCP/Unix reachability only
health/event.rs                   fleet-carried health and recovery handoff signals
health/fleet.rs                   origin/source matching and current-slot projection
health/fleet/bridge.rs            Automatic subscriptions/startup/reload/lag replay
state/access.rs                   scoped semantic health, validator and pressure access
state/assignment.rs               local canonical reusable health and native joined-owner fences

provider/registry.rs              current services, generation adoption and retirement
provider/finalize.rs              held-lease release only; no profile/Runnable/SHM stop
binding.rs                        shared trusted context and request-policy projection
metrics.rs, metrics/*             feature-gated recording, export and fleet scalar projection
```

The file configuration layer owns no compiled profile/app representation.
`CompiledUpstreams` and `Validation` live in `compile`,
`Capacity` and `Profile` in `compile/profile`
and `CompiledUpstreamApp` in `app`. There is one `compile(&Validation)` transition for both
production and regression tests. `Overridable` derives sparse profile-local
policy shapes which apply directly over each resolved type's built-in value.
Missing retains that built-in value; explicit null disables an optional
duration; required durations reject null.
`Capacity::H1` and `Capacity::H2` distinguish connections from stream permits.
H1 capacity validates `min_idle <= max_idle <= max_live` without silently
clamping limits. `min_idle` defaults to zero and is a per-serving-worker
floor on live connections, including leases. `capacity.warmup` defaults to
false for both protocols. H1 starts floor maintenance at first acquisition;
opt-in warmup starts it at lifecycle activation. H2 grows an origin's pool
when every existing connection has exhausted its effective local/peer stream
capacity. `max_live` bounds physical connections, including connecting and
still-draining ones; `max_idle` bounds empty retention. H2 rejects `min_idle`.
Optional `max_streams` limits each connection; omission uses peer SETTINGS and
never falls back to max_live. Physical-capacity metrics count max_live per
origin. `first` uses total pool stream load/capacity and considers an H2 pool
full only when it has neither an available stream nor room for another
connection. Explicit per-origin request limits remain independent.
Enabled warm maintenance owns its retry policy in `capacity.replenish`:
`initial_backoff` (100ms default) and `max_backoff` (5s default), validated
positive and ordered; disabled replenishment does not repair unused values.
These maintenance retries are separate from request and probe deadlines.

H2 uses the role name `H2Connection` and `conn_id` for backend connection
identity. Inbound `SessionSpawner` remains the native tunnel owner; its session
terminology and lifetime contract are unchanged.

`health/probe.rs` is a decision primitive, not an observability adapter.
It owns the transport-level reachability probe and classifies
origin-side, pre-byte failures that are allowed to affect `OriginHealth`.
Client cancellation, stale pooled leases, and post-byte body/framing
failures must stay outside that health transition path.
ProbeKind/ProbeOutcome own fixed labels and metric ordinals. Their shared
apply_outcome boundary renews the exact held tenure before counting/applying a
verdict, including active/drop duties. Lost/finalized ownership discards it.
Drop success records reachability confirmation but cannot claim recovery;
active/recovery success records health success. Recovery controls backoff and
reservation lifetime around this boundary.

probe/failure.rs retains typed Cause/RequestFailure, IO kind and optional raw OS code,
never a rendered source/reason String. Transport/handshake owners log original
errors cold with prepared source identity. Display renders facts at log time.
probe/message.rs recognizes the legacy early-EOF diagnostic through streaming
ASCII matching, preserving arbitrary length/chunk boundaries without lowercased
String copies or a truncation limit. A HalfOpen failure always returns Down,
even when peer history has not reached this worker's passive failure threshold.


`health/lock.rs` is always compiled as part of upstream coordination. It uses
`orbit` to join an existing fleet through `AppState`, then
uses `orbitive::lock` primitives to avoid duplicate background probes.
It does not make HTTP upstream own Orbit boot or create per-origin SHM
paths. Keys are typed and profile/origin scoped. Suspicious down confirmation,
active probing, and recovery probing use different lock namespaces, so they
do not block each other. An acquired key carries a short RAII `FlagLock`; an
occupied key means another worker owns that probe attempt.

`ProbeDuty<K>` uses the origin's boot-prepared key for the selected namespace;
keys use the same escaped profile/name/address identity as the state table.
An occupied tenure yields even when its owner string matches this worker.
Fleet acquisition errors yield rather than granting a local probe; an unavailable
fleet lock capability fails provider boot. Fleet-less callers use local duties.
Provider boot retains one lock capability across reloads, so weak guard tracking
covers both current and retiring profile generations. Each held duty has one
mutex-protected optional exact Lock; guard Drop and UpstreamProvider finalization
share its take/release point. All held tenures are attempted on finalization,
even if one backing operation fails. Successful recovery release publishes one
vacancy wake-up only while the slot remains nonhealthy and a next worker exists;
stale ownership and repeated release never advertise vacancy. No force-release,
health reset, profile/Runnable stop, process signal or SHM cleanup occurs here.
The state provider separately finalizes its resource with Preserve.

Recovery renews the exact tenure during both backoff and probing, at intervals
no longer than `min(probe_timeout, max_ttl / 2)`, and once before accepting the
verdict. Losing ownership cancels the pending operation and discards its verdict;
RAII cannot release a successor tenure. The half-TTL bound is algorithmic safety,
not an additional operational setting. Short configured TTLs stay short.

Enabled probe timers are validated at compilation: zero is invalid, recovery
initial backoff cannot exceed its maximum, and recovery TTL must reach Orbit's
1ms lease resolution. No hidden 1s TTL floor, 10ms backoff floor, 300ms recovery
release delay or 500ms driver-error settle delay changes configured policy.


### Why active probe uses a lock

Forty workers asking "is the upstream alive?" every interval is not a
health check. It is a denial-of-service against the upstream. A typed lock
assigns the duty to one worker per round and propagates the verdict to
the fleet via events. We check; we do not raid.

`health/event.rs` publishes coarse origin wake-ups, not connection state.
Reachability and serving live in the current kind-222 slot. Peers re-read that
slot when an event arrives and when selecting candidates, so an event missed
before attachment is not a missing health verdict. A delayed Healthy/Down
payload cannot overwrite newer table state. Worker-local observed-health/serving
atomics remember only the last projected verdict for metrics/pool cleanup;
it never controls selection. Remote Down purges this worker's idle H1 and
submits H2 connection cleanup to the epoch maintenance tracker. Configured
origins stay installed. Targeted recovery vacancy events wake one next worker
without forcing the slot Down; the lock TTL remains the fallback for a dead
owner. OriginHealthEvent carries a typed OriginId plus source/name. The id is used
only when identity still matches; reordered generations fall back to source/name.
Health has one enum encoding; the former event enum is only an alias.
The required id field uses health topic v2 rather than decoding v1 as its schema.
OriginServingEvent has its own serving.v1 topic and projects the current serving
slot/gauge without changing reachability. Both event payloads remain wake-ups.

UpstreamHealthBridge is an Automatic Runnable after UpstreamProvider. Each
RunContext generation subscribes once, replays current slots before waiting,
and replays again on registry changes or receiver lag. Startup wakes recovery
once; Down replay never repeatedly wakes itself. Cancelling the bridge only
ends its subscriptions. Local Down cleanup remains owned by the captured profile
epoch: H1 purges synchronously; H2 clear runs in its maintenance tracker and
profile stop joins it. This cleanup is not a separate probe loop. Selection
also projects missed health/serving transitions; a known Down candidate does
not repeatedly broadcast a new registry round on every request.

Active and recovery schedules run under their independent Runnable RunContext
and captured profile epoch. Services built from `AppState` inherit that state's shutdown token; active probes,
recovery validators, and driver-error drop probes stop when shutdown
starts and do not record new health failures or transitions during
drain. Request graceful shutdown is handled by the outer transport
runtime; upstream's shutdown concern is to stop its own maintenance work
without turning process shutdown into an origin outage signal. Recovery
validators may exist in multiple workers after a fleet-wide Down event,
but each recovery probe attempt is guarded by the Orbit lock so only
one worker probes a given profile/origin at a time. A killed owner is
recovered by the lock TTL expiring and another worker acquiring a
later attempt.

This split is runtime-first. H1/H2 and test files can bloom into folders
when those concepts develop their own local substructure, but the first
boundary is the one that keeps lifecycle, selection, and protocol
behavior from collapsing back into one service file.

---

## 4. Config

`UpstreamProvider` is a Congif `Configurable`: it declares
`config/upstream.yaml` as its source, is bound to the application's frozen
configuration environment at registration, and never opens the file itself.
The reader prepares and deserializes `UpstreamYaml` before boot and as part of
the central reload batch; boot and reload take that prepared snapshot and
compile it against the live fleet. An absent file is the empty document — no
profiles, no apps — and the provider treats both the same way: everything
retired, the proxy disabled. A source change prepared by Congif is applied
through `ConfigObserver` exactly as a manual reload is.

`config/upstream.yaml` contains named upstream profiles and optional upstream
apps. Profiles own outbound connection lifecycle and policy directly; there is
no top-level inheritance layer. Apps are a small boot-compiled matcher that
chooses one of those profiles for a request path.

Inside `apps.<name>.routes`, a route with only `profile` and no selector
is a root fallback. This keeps one-profile apps concise while preserving
explicit `exact`, `path`, `paths`, and `regex` selectors for
mixed-profile apps.

```yaml
metrics:
  interval: 1s
  max_age: 5s

upstreams:
  panel_api:
    protocol: h1
    authority: panel.internal
    origins:
      - tcp://127.0.0.1:3000
      - tcp://127.0.0.1:3001
      - name: local_uds
        target: unix:/tmp/panel.sock
        authority: panel.internal
    capacity:
      max_live: 64
    timeouts:
      acquire: 500ms
      connect: 1s
    health:
      passive:
        failure_threshold: 1
      recovery_probe:
        initial_backoff: 100ms
        max_backoff: 60s
  panel_api_h2:
    protocol: h2
    authority: panel.internal
    origins:
      - tcp://127.0.0.1:3040
    capacity:
      max_live: 4
      max_idle: 2
      max_streams: 32

apps:
  panel:
    routes:
      - profile: panel_api_h2
        path: /h2/
      - profile: panel_api
        path: /api/
      - profile: panel_api
        path: /
        excludes:
          - /php/*
          - '*.php'
```

The profile-local example is in `CONFIG.md`; no timeout
block is required because each policy type has safe built-in values. The old
top-level `defaults:` block is rejected rather than silently ignored. See
`CONFIG.md` for the five timer boundaries and profile-local example.
Phase 2 declares TLS, balance, forwarding, retry, body, outlier and L7 policy
as strict data; profile/capacity groups reject unknown keys. Phase 4 consumes TLS, forwarding, retry, request buffering, outlier and L7 policy; Phase 3C consumes balancing.
TLS targets require the `tls` feature and never fall back to plaintext.
compile/tls builds one shared ClientConfig per profile using TLS policy
provider/versions, native roots or the configured cwd-relative PEM bundle.
Optional `tls.client: {cert, key}` is one strict identity capability. Each TLS
origin prepares its ServerName from server_name or HTTP authority; IP names
send no SNI. HTTP/2 TLS URIs use https independently of the ingress scheme.
Full trust/client certificate material participates in desired equality, so
unchanged reload retains pools while same-path CA/client rotation replaces them.
No trust/certificate IO occurs during acquisition. Rustls session resumption
remains process/profile local; the fleet does not store TLS client sessions.

proxy/connect/tls performs TCP dial and separately bounded TLS handshake, then checks
negotiated ALPN against the profile HTTP protocol. An explicit ALPN offer is
allowed, but mismatch is always a pre-byte failure; certificate, TLS-handshake,
ALPN and handshake-timeout causes stay typed. Certificate failures log at error.
L4 reachability stays TCP-only even for TLS targets. Explicit insecure verification
bypasses certificate/name checks but verifies handshake signatures; provider
warns once on initial/changed profile adoption, not repeated unchanged reload.
The facade weakly enables this connector with tls+upstream, without webpki-roots.

Precedence:

```text
built-in safe defaults
  < named upstream profile (per-field overrides)
  < later per-origin non-protocol overrides
```

Each profile's `timeouts`, `connection` and `health` blocks are local to that
profile. Missing fields retain the typed built-in policy; no value is inherited
from another YAML section or profile. This keeps the effective policy visible
at its owner and prevents a shared edit from changing unrelated upstreams.

`authority` is the default outbound `Host` value for every origin in
the profile. A detailed origin may override it. UDS origins require an
explicit authority because the socket path is not an HTTP authority.

`capacity.max_live` is profile-owned and required. No global setting silently
allocates origin capacity to every upstream.

`protocol` is profile-owned. A single app may route different paths to
different profiles, so H1 and H2 can run beside each other under one
vhost. A single profile does not mix H1 and H2 origins. `tcp://` and
`unix:` are transport targets. For H2C, `protocol: h2` plus a `tcp://`
or `unix:` target means cleartext HTTP/2 over that transport.

Host config remains a capability pointer:

```yaml
upstream: panel
```

The value first resolves as an app name from `apps:`. If no app exists,
it resolves as a profile root fallback. This keeps the single-upstream
host shape small while allowing one host to map multiple upstream areas
through an app matcher.

App route selectors are intentionally small:

```text
exact path
segment-aware path prefix
regex
root fallback (`path: /`)
route-local wildcard excludes
```

Configured host dispatch runs Rust applications, Static and TryFiles before
the upstream backend. The prepared owner selects explicit exact/path/regex
routes before root fallback and retains final 404 on a miss. It performs no
local file probe. Without an upstream declaration, no service handle or backend
action is prepared. Response completion uses the common Backend contract;
profile replacement and accepted lease/stream ownership remain service-owned.

Reverse caching is intentionally not specified here yet. The pipeline
finding in `../edge` captures the current
boundary: if reverse caching becomes real, it should start as an
upstream-first, opt-in, stream-safe policy rather than a generic
dynamic-response wrapper.

---

### DNS preparation

Named origins require an application-bound `ResolverFactory`; DNS queries run outside request handling during cold membership preparation.
Materialization produces concrete origin members for revision-checked publication, preserving the current pools when preparation or adoption fails.

### Managed desired boot and serving adoption

With `dns`, file loading uses DesiredUpstreams rather than the literal-only
compiler. Without it, named origins return ConfigError::ResolverUnavailable.
The provider serializes rebuilds, pauses local profile futures and awaits their
completion guards before preparing a changed plan. Cancellation drops queries
and exact tenure first; it never acknowledges native IO or revokes peer tenure.
A failed/canceled candidate drops the pause guard and restores existing duties.
Identical desired config retains current services, native pools and resolver
bindings, including intentionally empty live membership, without requiring boot
again. Removing config publishes empty duties and seals removed services.

PreparedService registers only unique inert owners before publication. Registry
write -> retiring lifecycle admissions -> table mutex/flock is the boot/reload
lock order. Under one exclusion, all changed materialized profiles are checked
against their complete authoritative revisions, then committed, old services
sealed/queued and captured app bindings advanced. A rejected candidate explicitly
aborts only its unique empty closed transport owners; existing pools stay live.
A registry-controlled retirement driver joins removed IO. The cold resolver
then activates pending addresses only after the physical proof is ready.

UpstreamResolver subscribes before reading, polls per-profile owner/consumer
futures in FuturesUnordered and spawns no child resolution tasks. Serving
workers/standalone elect query ownership; master consumes publications. Consumers
freeze a MembershipTarget before materialization, publish a complete checked
view and preserve unchanged pools. Events and registry notifications are hints;
configured retry_interval supplies the fallback for missed hints and unavailable
preparation. Runnable cancellation releases queries/leases without stopping
serving pools. A fleet requires the managed ResolutionProvider; only an embedder
without Fleet uses the private memory fallback.

Live one-TTL acceptance is recorded in the refactor plan and downstream
claviron-full fixture evidence; it also checks verified TLS/H2 serving. Literal
request behavior and kind-222 ABI remain unchanged. Tests use controlled
queries/clocks without DNS sockets; native serving tests use loopback HTTP, and
publication lifecycle tests use only isolated test-owned SHM names.

---

## 5. Origin Rules

Runtime provider validation accepts the multi-origin, multi-protocol
origin surface:

```text
origins.len() >= 1
tcp:// origins must use IP literal hosts
connect target may be TCP (tcp://), Unix (unix:) or TLS over TCP (tls://)
UDS origins require explicit HTTP authority
protocol is profile-owned; tcp://, unix: and tls:// are transport-only schemes
no DNS resolution
no internal IP cache
certificate material is the process certificate manager's, never this crate's
```

Production and tests use the same compiler. It prepares each OriginId, HTTP
authority, Host value, label, connect target and typed probe keys once.

Future DNS belongs in a separate resolver/provider that publishes
versioned resolved address sets to the pool. The pool core never does
DNS in the acquire hot path.

Pools belong to a compiled profile and configured OriginId, not SocketAddr
alone. Connections carry the OriginId and pool generation; logs read the
shared profile rather than copying identity into each connection. Driver-end
contexts weakly reference the service and H2 pool, avoiding ownership cycles.
Profiles pointing at the same IP remain separate capacity domains.

---

## 6. H1 Service Contract

The current live service is a pool-backed H1 balancer:

```text
select selectable origins by the profile's balancer (round_robin by default)
  -> acquire origin TargetPool
  -> reuse idle H1 connection after preflight or connect TCP/UDS + Hyper H1 handshake
  -> prepare request once through proxy/service/request for the selected origin
  -> send request once
  -> response body wrapper owns lease until clean EOF or dirty close
```

Before origin selection, the service rejects request-smuggling shapes
through `claviron-http::forwarding` (`Content-Length` together with
`Transfer-Encoding`, invalid/conflicting `Content-Length`). The selected
origin request then uses the same shared helper to strip hop-by-hop
headers, set `Connection: keep-alive` when H1 idle reuse is enabled
(`capacity.max_idle > 0`) or `Connection: close` when reuse is disabled,
rewrite `Host` to the origin
authority, and emit Claviron-controlled `X-Forwarded-Host`,
`X-Forwarded-Proto`, `X-Forwarded-For`, and `Via` headers. The native
binding builds that forwarded context from `RealIpExt` first and
falls back to `ConnCtx` peer IP, so trusted proxy resolution wins while
direct clients still get a stable `X-Forwarded-For`.

Configured and explicit Other dispatch use the same `ForwardedRequestContext` projection. H1, H1
upgrade and H2 forward the accepted HTTP(S) scheme, not a hard-coded HTTPS
value. Direct callers may supply typed context or an absolute HTTP(S) URI
when no native context exists; otherwise unknown scheme is omitted. Client
forwarding headers are replaced. The backend H2C URI remains `http://` even
for HTTPS ingress: backend transport and original request scheme are distinct.

An idle H1 connection is preflighted with Hyper's synchronous readiness
hints before the request is sent. If the sender is closed or not ready,
the lease is discarded as `stale_idle_preflight` and the service may
open a fresh connection for the same origin. This is dead-lease discard,
not request replay: no upstream request bytes have been written yet.
The stale lease itself is not an origin health signal. Only the fresh
dial that follows may mark the origin down.

If a new connect, H1/H2 handshake, or H2 protocol-readiness check fails
before any request byte is written, the service records an origin
reachability failure and may try the next healthy origin. Once a pooled
connection is leased and `send_request` starts, the request is never
retried. This is origin failover, not request replay.

### Four mechanisms, four names

Health is broken into four orthogonal mechanisms. Each owns a distinct
trigger and lives under its own config key:

| Mechanism | Trigger | Config key | Default |
|---|---|---|---|
| **Passive observation** | Per-request pre-byte failures | `health.passive` | always on |
| **Drop probe** | Connection driver task ends with an error | `health.drop_probe` | H2: on, H1: off |
| **Recovery probe** | Origin transitions to Down | `health.recovery_probe` | on (was `validator`) |
| **Active probe** | Periodic timer, regardless of state | `health.active` | off (opt-in) |

The four are deliberately separable: passive observation alone gives a
small, well-shaped circuit breaker; layering drop + recovery on top
catches origin death from session-end events; turning on active closes
the idle-origin gap where neither traffic nor connection drops would
otherwise notice.

Availability is read from the fleet state table and can reduce the candidate
set to zero. If every origin visible to a worker is Down, the request path
returns `NoHealthyOrigin`/503 instead of doing extra TCP connects as
implicit probes. The worker that observes the Down transition starts the
recovery validator; recovery/active probes publish the next UP signal.
Workers with the same key share current health even after a missed event or
restart; profile generations and physical connection pools remain local.

```text
Healthy
  -> K consecutive pre-byte failures (passive)
     OR active probe failures cross threshold
     OR (H2 default / H1 opt-in) drop probe failure after driver Err
  -> Down
  -> zero request candidates / 503 while all visible origins are Down
  -> background recovery probe with exponential backoff capped by
     health.recovery_probe.max_backoff and fleet lock TTL capped by
     health.recovery_probe.max_ttl
  -> success: Healthy
  -> failure: Down
```

Healthy origins always win over down origins. Down origins do not enter
the request candidate set and are not used as implicit hot-path probes.
The recovery probe fills the other side of that rule: even while
another origin stays healthy, a Down origin is probed in the background
until it recovers. Recovery backoff grows exponentially from
`health.recovery_probe.initial_backoff` (built-in default `2s`, often set to
something shorter like `1s` in a profile) and is capped at
`health.recovery_probe.max_backoff` (default `60s`; `null` removes the
ceiling and lets the delay keep doubling for a long-dead origin). Under Orbit,
each recovery probe attempt also holds a profile/origin lock. A
successful probe releases its exact lease immediately; no hidden settle
window changes the configured schedule. A failed or timed-out probe retains the lock across
backoff, subject to `health.recovery_probe.max_ttl`
(default `10m`, often shorter for smoke profiles such as H2C) so peers
do not stampede the same down origin.

Policy: futures are Claviron's leisure work: when the hot path is quiet,
small async probes turn idle time into fresh reachability.

_Who hurts, who probes._

Why the first Down catcher: occurrence is first-class evidence. The
worker that first observes a connection-driver failure owns the useful
local facts: exact origin, protocol pool, leased/idle state, and driver
termination reason. It may acquire an Orbit lock to avoid duplicate fleet
probes, but the competition is still among workers that observed the
occurrence; it is not delegated to a quieter peer. Concrete local pain
beats remote hearsay: the catcher can validate reachability and clean
the pool state that only it can see, while later contenders yield to
the active holder.

### Why drop_probe defaults differ per protocol

`drop_probe` remains enabled by default for H2 and disabled for H1. It is a
one-shot transport reachability confirmation after a driver error, not a
verdict that one closed pooled connection proves the entire origin is down.
The existing probe duty/lock and outcome mechanisms own confirmation and
publication. Operators can change the policy per profile.

### Probe is availability, not application health

`probe_origin_reachability` is a pure transport-layer reachability check
used by the drop, recovery, and active mechanisms identically: TCP
`connect()` for `tcp://` origins, `connect()` for `unix:` origins. No
HTTP, no H1/H2 handshake, no SETTINGS exchange. The probe answers
exactly one question — _can this origin accept a new connection right
now?_ — and stays out of application semantics.

An earlier shape ran the full H1/H2 handshake before declaring success.
Hyper's `http2::Builder::handshake()` returns `Ok` after writing the
client preface and the local SETTINGS frame, before the server SETTINGS
round-trips. On a freshly closed listener that pattern reported
"succeeded" against a peer that no longer existed. TCP `connect()` does
not have that ambiguity: `ECONNREFUSED` is delivered during the SYN
handshake, and a successful connect proves the kernel on the other side
accepted the socket. Application-level health (responsiveness, error
rates) is the request path's job, not the probe's.

### Active probe closes the idle-origin gap

`health.active` is the only mechanism that fires independently of
request flow or connection lifecycle. A ticker per origin runs
`probe_origin_reachability` every `interval` (default 30s) and feeds
failures into the standard `record_origin_reachability_failure` path,
so threshold + recovery loop kick in exactly as they would for a
request-path failure. Active is off by default because it adds periodic
outbound traffic to every origin; turning it on is an explicit operator
choice for workloads where idle periods are long enough that a
passive-only setup would let an origin go dark without notice. Within a
fleet, the typed active-probe lock selects one worker for each origin and
interval.

Origin `Down` also cleans this worker's local pool state for that
origin: H1 idle connections are purged and H2 sessions are cleared.
Fleet-wide cleanup and recovery propagation are not in this layer yet.
The Orbit lock binding coordinates background probes;
there is still no DNS refresh, generic proxy mode, or H3 upstream in
this layer.

---

## 7. H2C Service Contract

The H2 backend service is an h2c session-backed balancer:

```text
select selectable origins by the profile's balancer (round_robin by default)
  -> reserve a stream on a reusable connection in that origin's H2TargetPool
  -> if all are full, open TCP/UDS + Hyper H2 within max_live; otherwise queue
  -> poll H2 sender readiness
  -> rewrite request as absolute http://authority/path
  -> send request once
  -> response and any request-body writer retain the stream until both finish
```

H2 request cleanup uses the same forwarding helper as H1, but does not
emit `Connection: keep-alive` or a `Host` header. The outbound H2
`:authority` comes from the absolute URI built with the origin authority.
If `connection.keep_alive_interval` is set, the H2 Hyper builder sends
HTTP/2 PING keepalive frames using `connection.keep_alive_timeout` and
`connection.keep_alive_while_idle`. H1 currently ignores those keepalive
fields; H1 reuse is controlled by `capacity.max_idle` and `idle_timeout`.

`proxy/h2/capacity` owns each connection's effective stream limit, active
reservation count and idle deadline. The driver updates the peer limit after
polling Hyper. Admission waits for that protocol capacity before growing a
newly opened connection's pool again. `proxy/h2/pool` reads an ArcSwap snapshot
of connections, reserves available streams atomically, and serializes cold
connection establishment. The physical semaphore counts both dialing and
retained connections. Cancelled dials refund it before waking waiters; a full
pool parks on notifications under the declared queue budget.

max_idle trimming and configured idle expiry atomically seal only empty
connections. They cannot evict a racing accepted stream. Physical capacity is
released when the connection drops; removed but draining connections still
consume it. The per-connection driver owns the event-driven idle deadline;
there is no sweep or polling cadence. Explicit invalidation clears only the
matching conn_id, so late driver completion cannot remove a replacement.

A nonempty request shares one stream lease allocation between input and
response; empty requests keep the lease inline. This retains upload IO after
an early response and lets idle cleanup wait for both halves. Neither path
copies body frames. GOAWAY-specific telemetry remains follow-up work.

If the H2 sender is not ready before request bytes are sent, the session
is cleared and the service may try the next healthy origin. Once
`send_request` starts, the request is never retried.

SSE follows this same body lifecycle. It has no special route through
the upstream service: H1 SSE bodies dropped before EOF close the
underlying connection instead of returning it to idle, and H2 SSE body
drop releases stream capacity so later streams are not stranded.

H2 extended CONNECT is request-policy gated by
`UpstreamRequestPolicy::allow_connect_protocol`, which the bindings
derive from `HostConfig.transport.h2.connect_protocol`. Generic CONNECT
without `hyper::ext::Protocol` is rejected. A successful extended
CONNECT keeps the `H2Permit` inside the tunnel task until
`copy_bidirectional` ends, so stream capacity is not released while the
tunnel is still open.

That tunnel is submitted through the inbound request's `SessionSpawner`
before returning 200. The task owns both the outbound `H2Permit` and the
original inbound connection slot; these are different capacity domains.
Listener drain joins the tunnel and explicit force releases both upgraded
streams and the permit. It does not close the pool's shared H2 session:
other streams and subsequent requests can continue to use that backend.

---

### Origin selection

`Balancer::order` returns typed OriginId candidates and never writes health.
All four request doors supply a RequestKeyView before URI/header rewriting;
it borrows headers/path and uses the trusted forwarded IP projection, retaining
no body. Round-robin uses an atomic worker-local cursor. Consistent hashing
builds virtual nodes from unique origin labels once during config compilation,
using fixed FNV-1a plus avalanche mixing and little-endian replica ordinals so peer rings agree
across processes and machines. Header names are validated/prepared at compile.
Cookies are read from borrowed Cookie fields; path excludes the query; remote_ip
hashes IP octets, never a formatted String. Missing header/cookie/IP hashes empty
bytes, giving deterministic affinity rather than a new random origin.

The ring stores sorted points per origin. Its iterator ranks each origin by its
nearest clockwise virtual node, yielding each origin once while skipping Down
or serving-No slots. No request Vec, hash ring rebuild or boxed iterator is used.
Failover remains acquisition-only and before request bytes. Invalid keys, zero
replicas, duplicate hash labels and unsupported algorithm/scope combinations
fail compilation before registry adoption. Fleet scope is supported only for
least-connections. Peak-EWMA requires a positive decay; enabled slow start
requires initial_percent within 1..=100.

Smooth weighted RR uses one worker-local schedule bank; equal weights with slow
start disabled retain the atomic RR fast path. It chooses the preferred origin
with accumulated effective weights, then walks remaining configured origins for
pre-byte failover. Least-connections compares exact integer load/weight ratios.
Slow start scales configured weights linearly from initial_percent to full
weight using the bound slot's healthy_since; regular request successes do not
restart it. Millisecond fleet timestamps define its resolution; future/skewed
Healthy clocks start at the initial weight rather than underflow.

Peak-EWMA has one worker-local latency window per origin. A higher response-header
sample immediately raises the peak; smaller samples blend with exp(-elapsed /
decay), and idle estimates decay with a positive microsecond signal floor.
Preference cost is estimate * (held permits + 1). attempt::run starts each
origin's successful acquisition timer independently, so a prior failed origin's
latency is not charged to its successor. All four doors call attempt::observe
when backend response headers arrive, before body reading or tunnel handoff.
Body time, reachability probes and warm maintenance dials never update this
window. Restart resets worker schedules/windows; authoritative health persists.

Least-connections reads H1 leased + request connecting and H2 held capacity permits
from the request's captured epoch, including admission during dial/readiness.
Idle sockets are not traffic. Fleet scope reads each bound OriginSlot.in_flight;
one Traffic RAII reservation follows acquisition through body EOF, dirty drop,
upgrade handoff or stream/tunnel end. Cancelled/failed acquisitions release it.
Old accepted epochs continue to contribute while a replacement epoch admits new
requests. The u32 counter refuses overflow with CapacityFull/503 and leaves
health unchanged. Algorithms never overwrite that counter or infer peer death.

Ranked iterators re-read current pressure before each failover choice. Cursor
rotation breaks ties, and an inline bitmap prevents an origin already attempted
from being yielded again when concurrent pressure changes. It covers the entire
u16 OriginId domain (8 KiB storage, no candidate heap allocation or operator
capacity setting); RR/hash do not initialize the bitmap. The future's inline
storage tradeoff is separate from allocation-count targets.

## 8. H1 Pool Contract

H1 is exclusive connection leasing.

```text
Idle
  -> LeasedDirty
  -> BodyOwnedByResponseWrapper
  -> ReusableAfterEof
  -> Idle

LeasedDirty / BodyOwnedByResponseWrapper
  -> Closing on drop, cancel, framing error, write error, or incomplete body
```

The central invariant:

```text
the H1 lease lives inside the upstream response body wrapper
```

The connection must not return to the pool after response headers. It
returns only after the response body wrapper observes clean completion.
Unknown state is dirty. Dirty means close, not recycle. `H1Lease` is the
single lease state machine; a separate response `BodyState` is unnecessary.
Acquire chooses idle/connect/wait with one lock. RAII accounting releases
connecting/leased reservations even when their futures are cancelled. Waiters
use notifications, not a private polling interval. Pools read limits from
the shared `Arc<Profile>` and reject late returns to a closed generation.

H1 `pool/warm` maintains the configured per-serving-worker floor on live,
including leased and connecting sockets. Wiring weak service/epoch callbacks
is inert in constructors. Provider boot/reload prepares and publishes services;
the provider's automatic runtime schedules worker/standalone warm work after
boot and registry changes, without awaiting readiness. Pools with warmup off
remain inactive until their first real acquisition. Master roles remain ineligible even after health or serving transitions.
Acquire/release and recovery/resumed serving recheck deficits. One AtomicBool
compare-exchange admits a replenisher; its cancellation-safe guard releases the
flag, and normal completion rechecks after releasing it to close deficit races.
There is no maintenance tick. Failure sleeps use capacity.replenish backoff,
double to its maximum and reset after success, without changing health.

Warm reservations count towards live/max_live but have no Traffic marker;
worker in-flight subtracts warming reservations too. Direct maintenance dials
never pass through attempt::observe. Completion puts actual handshaken sockets
into idle only for the current generation and a still selectable origin;
Down/non-serving origins are skipped. Idle expiry discards only surplus above
the live floor; stale sender preflight still discards dead sockets. Returned
burst sockets shrink to max_idle without creating a floor deficit. The captured
epoch tracks the task, cancellation wraps both dialing and backoff, and profile
stop cancels/joins it before draining IO; restart wires fresh pools.

---

## 9. H1 Transparent Upgrade

HTTP/1.1 upgrade is a tunnel handoff, not a pooled response. The service
only enters this path when the caller attaches
`UpstreamRequestPolicy { allow_h1_upgrade: true }`, which native
bindings derive from `HostConfig.transport.h1.upgrade`.

```text
upgrade request
  -> acquire H1 connection
  -> preserve Connection/Upgrade headers
  -> send request once
  -> if upstream returns 101:
       remove the lease from pool accounting
       join the completed backend Hyper upgrade driver (retain abort-on-drop while waiting)
       submit upgraded client/upstream IO copying to the inbound session owner
  -> if upstream returns a normal response:
       use the normal pooled body wrapper
```

Claviron does not parse WebSocket frames in this path. Hosts that need
message-level behavior use `pusher` or a custom Rust adapter.
SSE is not an upgrade and remains on the normal response-body path.

Both H1 upgrade and H2 extended CONNECT require the accepting native
transport's `SessionSpawner`, including for direct service callers. Missing
owner is a 500 integration error before backend IO. If stop seals admission
during the backend handshake, rejected submission drops the pending upgraded
IO/stream permit and returns 503, never an unowned success response. This
inbound lifecycle error does not enter `RequestFailure`, mark an origin Down or
start recovery probes.

These byte tunnels do not invent protocol close frames. A tunnel has no
request to finish, so a drain does not wait on it: when the listener enters
graceful drain (worker shutdown) or the profile enters Stopping, the tunnel
shuts down both halves in order, each side sending TLS `close_notify` and the
TCP FIN so the peer reads a clean end of stream. That close is bounded by
the profile's `lifecycle.drain_timeout`; the listener's `None` grace still
acquires no hidden timeout of its own. Explicit listener force aborts the
owned tunnel and joins resource release before the generation stops. The outbound H1 driver has already joined before
returning 101, and the upgraded connection never returns to H1 idle reuse.
Pool-maintenance/ordinary response drivers remain pool-owned; this change does
not turn their lifecycle into an inbound listener session or a global shutdown.

---

## 10. Hyper Binding

The H1 path uses `hyper::client::conn::http1::handshake()`. The H2C
path uses `hyper::client::conn::http2::Builder::handshake()` with a
cleartext TCP/UDS stream; Claviron applies configured HTTP/2 PING
keepalive options on that builder.

Do not use `hyper::Client`; it brings its own pooling contract.
Hyper owns protocol state for one connection/session. Claviron owns
admission, lifecycle, reuse, generation, and availability accounting.

---

## 11. Availability Metrics

The current runtime exposes upstream availability through service metrics,
not through a separate observer/snapshot adapter. The old pool-observer
surface was removed until a concrete reader exists; if future work needs a
live pool-state API, its types should live next to the state they read
rather than as an unused adapter shell.

The current runtime emits debug-build-only lifecycle logs for origin
selection, H1 connection open/reuse, clean idle return, dirty close,
and H1/H2 connection driver termination. With the `metrics` feature it
also updates process-local `UPSTREAM_METRICS` for request status class,
probe source/result, state transitions, configured live capacity, and
origin health. Those metrics
are Claviron-side upstream service metrics, not remote application
metrics; Claviron does not scrape or infer the upstream application's
own internals.

Service construction binds each OriginId to a dense `ProfileMetrics` bank
(`Box<[OriginSlot]>`). Request/probe recording touches its fixed atomics directly;
it constructs no labels or map keys and clones no bank handle. Labels and
capacity are immutable export metadata copied from the compiled profile once;
the bank only weakly references that profile, avoiding an ownership cycle.
Health gauges are written only on an encoded state change. Serving starts at Yes; L7 and outlier results project independently, while
retry counters record explicit configured attempts by typed reason. Nonstandard HTTP status classes use `other`.

Reload installs a new bank. Accepted requests can finish against the old bank,
and their counters remain in cumulative exports. Gauges and capacity come only
from the current bank. Unreferenced retired banks with matching ordered labels
are folded during registration; no producer can still write a folded bank.

Standalone `/metrics` materializes full labeled upstream rows at export time.
Supervisor aggregation exposes scalar totals for requests, request errors,
probes, probe failures, state transitions, configured live capacity, retries and
ejections. `Scalars` reads atomics directly without building labeled rows.
Each worker publishes at `metrics.interval`; the scraping worker accepts
samples no older than `metrics.max_age`. Kind `213` is managed as `Reuse` /
`Preserve`, so retained bytes never require a master reset; freshness excludes
an older run. The upstream crate owns the store, snapshot, 74-byte codec,
publisher, collector and `MetricsFamily`. Neither `supervisor` nor the
HTTP endpoint knows the upstream metric type. Kind `212`'s incompatible older
58-byte record is retired, with history retained in the SHM Kind ID Index.
Old and new segments may coexist during deployment; retiring source does not
unlink an existing segment. Cleanup of 212 requires a stopped fleet.
Carrying bounded label rows across workers requires a dedicated upstream
row/chunk Orbit family.

Worker growth/removal does not restart retained upstream pools. A vacated
recovery probe selects its next worker from current `FleetMembership`, including
non-contiguous ids; missing membership emits no targeted handoff. Pool config
validation uses the bootstrap worker count during boot (before a supervisor
snapshot exists), and current membership at explicit reload. That validation
hint is not runtime capacity division or an assertion that all members are
healthy; resize does not silently recompile an existing pool's policy.

Observability is broader than metrics. Metrics are one projection of
this surface; the source model is event + snapshot oriented so it can
feed Prometheus gauges/counters, Orbit status rings, debug endpoints, or
future supervisor views without making any of those sinks the pool's
core API.

The minimum future operator-visible state includes configured/effective
capacity, live/idle/leased/connecting counts, waiters, generation,
origin health/reachability state, dirty reasons, recent error class, and
replenisher state.

---

## 12. Current Origin State Table

`state/layout.rs` owns the kind-222 ABI: a 64-byte header and fixed 160-byte
slots. Key/hash fields are atomic as well as runtime fields, so structural
installation needs no mutable reference over concurrent readers. The listed
fields occupy 136 bytes; explicit reserved bytes complete the promised stride.
Geometry is pinned at build time by `ORBIT_UPSTREAM_STATE_CAPACITY` (default
1024, power of two), matching the other fleet table geometries.

`UpstreamStateProvider` is registered through the managed SHM lifecycle and
orders after `FleetProvider`; `UpstreamProvider` orders after the state provider.
Every fleet user can access this state, so activity evidence comes from the
existing process-family authority, not an upstream-specific heartbeat. It owns
any full-interval observation. Unknown evidence disables destructive boot and
preserves the table. Finalize always preserves. Physical validation maps only
the existing header read-only and never uses an open-or-create API. Locked
creation publishes initialized header/slots; compatible attachments reuse
atomics, and reset is confined to the physical adapter after quiescence proof.
The canonical lifecycle contract remains in claviron-orbit/ARCHITECTURE.md.

The immutable header is published by coordinator boot before worker consumers;
retained headers never change during reuse or reset. No-fleet runtimes lazily
use the same slot layout in memory. A fleet runtime requires the booted managed
capability and cannot silently switch to memory.

Compilation prepares each origin's escaped identity key once. The default
label repeats the address, so `profile|u|address` encodes that case without
storing the address twice. Other labels use `profile|n:label|address`; separators
and backslashes are escaped. The full key is checked against the frozen 96-byte
region rather than truncated. Installation compares complete keys to resolve
hash collisions; its structural lock covers installation and fleet outlier publication/expiry. A returned SlotRef
retains its backing mapping at an immutable slot index. Serving code cannot dereference
SlotRef into retained raw atomics. `state/access.rs` owns semantic health,
serving, validator reservation, slow-start clock and traffic pressure access.
Compound updates and pressure CAS/release execute inside SlotRef's scoped
read/write operation; closures cannot return borrowed physical slot references.
Invalid-assignment fallbacks at this boundary are fail-closed (Down/No, refused validator,
maximum pressure, Closed traffic admission and ignored stale release). Resident
literal slots always admit those scopes. Health transitions capture before/after
inside the same scoped mutation; projections read optional real verdicts, so a
retirement fallback cannot manufacture a Down transition or purge an accepted H2
session. Recovery scheduling and active rounds reject unavailable assignments. An explicit raw helper exists only in
unit builds to seed/inspect physical ABI fixtures. It is absent in serving builds.
Reusable assignments validate their stamp after admission CAS and retain a
counted scope through the entire mutation, including validator cancellation.
The local capability and unified fleet capability below use separate native
ownership evidence; literal kind 222 retains its original contract.


With `dns`, `state/assignment.rs` adds an explicit **local** reusable health
capability. `UpstreamService::local_membership_state` reserves a fixed canonical
range for its fleet-less memory runtime; a capability cannot bind another
runtime's table and rejects a managed/fleet context. Its cells are separate from
kind 222 and hold an assignment stamp, a closed/count scope gate, native owner
count and cold identity mutex. `bind` checks the concrete socket, nonzero/newer
stamp and full assignment identity. Metadata changes retain health and native
owners. `retire` closes new scopes without writing Down or emitting health
signals; `drained` requires the exact assignment, closed zero-count gate and no
native owners. A newer bind resets atomics only after that proof. All raw fields
remain atomic; reset creates no mutable aliases. The scope count stays reserved
through the entire callback or admitted dial/probe, and the stamp is checked
**after** its CAS, so a cached open gate cannot admit an old handle into a
reopened generation. Late traffic Drop and validator cancellation are refused.

Each concrete OriginTransport registers one owner before serving IO; unchanged
membership views share that owner. Losing registration closes the inert
transport. Admission, native dial and HTTP handoff check assignment scope.
Close/force alone is not ACK: only force plus tracker join releases ownership,
exactly once, including forced stop with a retained unpolled response. A refused
membership candidate consumes only its unique inert new owners before sealing
or registering any in the serving lifetime. Unique transport/pool ownership,
zero admission, absent projections and closed empty native tracker prove that
abort has no IO to join; unchanged serving owners are never aborted. A refused
registration returns StateBinding and preserves the live view and binding.
Dropping
the native owner handle does not acknowledge anything; it leaves the cell
conservatively unavailable. Membership identity includes the assignment binding,
so the same socket with a fresh stamp cannot inherit a retired pool. Assignment
binding remains excluded from desired-file config equality. This capability
adds no timer, task, SHM shape or named-config activation. It is usable by the
local cold membership API, not a substitute for cross-process participant
registration, authoritative snapshot commit or fleet joined retirement. Those
boundaries still block named serving config and UpstreamResolver integration.

The foundation installs keys before registry adoption and fails boot/reload
with StateFull naming the key/capacity. Failed reload retains live services.
Removed keys remain until a quiescent reset; there is no active deletion.
Provider boot binds the installed SlotRef onto each prepared Origin before
registry adoption. Detached construction binds its private config copy to its
own memory backing; it cannot mutate a caller's shared Profile or steal a
managed mapping. Binding identity is excluded from desired-config equality,
so unchanged reloads still retain their service/pools. Public detached
construction accommodates the compiled origin count; managed geometry stays
fixed and full tables still fail before adoption.

`origin/health.rs` decodes the frozen u8 Healthy/Down/HalfOpen and Yes/No axes.
Passive failure accounting saturates atomically, recovery reserves Down with
CAS, and success clears failures without clearing a serving ejection. Recovery
publishes its healthy timestamp before making the origin selectable; ordinary
successful requests keep the clock. No ServiceState mutex or local OriginState
copy remains. The round-robin cursor is worker-local and atomic.

UpstreamRecoveryProbe is an Automatic Runnable ordered after UpstreamProvider.
Its generation owns one cold future per nonhealthy origin, identified by profile
PoolGeneration and OriginId. The shared round scheduler polls these futures
inside the generation without spawning child tasks. Request/event paths notify
registry observers; they never create validators. There is no recovery_tasks
vector, validator_active query or local recovery-running health bit. Cancelling
recovery drops its futures/leases without cancelling active or the profile.
A cancelled probe returns
only its owned HalfOpen reservation to Down without recording failure. An
exact-lease renewal fences a lost tenure from abandoning a successor's
reservation or publishing a probe result; an acquired successor can reclaim
crash-left HalfOpen. Typed duties renew recovery across the whole delay/probe.
Active, recovery and bridge are independently runnable; their outcome boundary
is shared. The memory fixture includes locks, events and the state table.
Live application verification covers master/eight workers, kind-222 ABI and
page-rounded size, H1/H2C forwarding and graceful Preserve shutdown.


---

### Unified named health and native receipts

With `dns`, `resolve/table/membership.rs` binds a concrete prepared origin to an
exact active assignment/publication through `UpstreamService::resolved_origin`.
The parent runtime/authority context is checked cold. This explicit capability
does not yet accept named file configuration. Cold member transfer retains the
existing local health projection when reusing an equal physical assignment and
its native owner; a newly prepared equal projection has no registration itself.

Each physical canonical cell has a bounded set of 64-byte native records. A
record holds a monotonically increasing native generation, kernel PID/start
identity, one generation-tagged closed/count scope gate, pressure and receipt phase. The same 64-bit scope CAS carries a 32-bit native generation and 31-bit
scope count plus closure bit. Receipt phases carry the same generation tag,
so a paused old finalizer cannot ACK a successor with outstanding scopes. A cached old CAS cannot enter a reopened native
record: a post-CAS refund would race a foreign process's death cleanup. Native
generation never wraps; an exhausted record stays unavailable. Only that
process may increment or drop scopes in its record; copied/forked handles cannot use it. No global
anonymous scope counter can survive a killed owner without attributable proof.
A native tracker join (or consumed unique inert construction) closes its gate
and marks joined. The last already admitted scope clears pressure and publishes
Free. Close alone, response EOF alone, dropped Arc, heartbeat absence, lease TTL
and master restart are not ACK. There is no public ACK method or Drop receipt.

Cold registration/retirement/publication reconcile an exact process only when a
successful kernel start identity differs or signal-zero returns ESRCH. Failed
identity lookup with a live/permission-denied/unknown PID preserves ownership.
No polling timer, implicit timeout or process signal is added. A physical cell
can reset only when every native record is Free with a closed zero-count gate.
`ResolutionEntry::retirement_ready` also requires the exact currently published
retirement. Publication revalidates the proof under its flock before reuse.

Fleet request pressure lives in the native records, summed for least-connections.
Every Traffic reservation freezes its original native generation and assignment;
force plus native join clears that record even when a logical response remains
retained. Late Drop cannot decrement a reused record, including operator restart
of the same assignment/socket. Health/validator operations require active bank
eligibility. Accepted pressure releases may finish during retirement until the
native join closes their record. Neither pressure cleanup nor retirement records
a reachability failure or starts recovery. Steady request operations use atomics and owned Arc loads, without a process
flock, JSON decode or DNS query; historical warm allocation measurements covered this path.

Production resolver Runnable and provider desired boot/adoption are connected.
Typed desired materialization and joined local handoff are documented above.
The registry's revision-checked adoption capability is documented above. The physical capability
was exercised with real H1/H2 retained responses and exec'd SHM peers, including
one cooperative native join and one killed process holding native IO and a scope.

---

## 13. What This Crate Must Not Own

- Transport accept loops.
- Vhost resolution.
- Axum routing.
- Static file policy.
- Response header rewrite policy (HostConfig owns it).
- Response cache policy.
- Open forward proxy / generic CONNECT.
- H3 upstream.
- Undeclared request retry.

Those concerns either already belong to other crates or are explicit
future features.


### Worker/profile breaker admission

`breaker` has four optional limits (`max_connections`, `max_pending`,
`max_requests`, `max_retries`), each unset by default. These are worker-local
limits across all origins of one profile, separate from per-origin capacity.
Zero is an explicit closed boundary. An inert, epoch-owned breaker state is
created only when a limit is configured; the default request path allocates no
breaker guard and takes no breaker lock.

Connection reservations include dialing, warm and idle H1 connections and
single-flight H2 sessions. They are owned by the concrete connection, or by
an H1 native tunnel after handoff. A failed/cancelled dial releases its
reservation. Warm maintenance waits for a connection release when the breaker
is full, observes its existing cancellation owner, and does not spin or add a
new task. Compilation rejects a connection limit below the sum of H1 warm
floors. Dedicated health probes are outside serving pool admission.

Admission reserves pending capacity and request capacity synchronously before
pool acquisition; readiness transfers pending to active. The guard survives
both request and response IO, including an early H2 response, and native tunnel
handoff. Rejection is 503 `BreakerOpen`, never a reachability verdict or queue.
The labeled `rejections_total{cause="breaker_open"}` counter is owner-local;
existing fleet scalar request/error totals include it without changing kind
213's wire geometry. Explicit retry admission uses the same state and releases
the failed attempt before admitting another.


### Dedicated L7 health

Enabled `health.l7` compiles the method, profile/origin-form URI and streaming
substring prefix plan at boot/reload. Invalid enabled timer, method, path,
status range or threshold fails compilation. Dedicated H1/H2 connections use
the same TCP/Unix/TLS connector and profile authority, with an outer L7 timeout
covering connect, handshake, response headers and any substring scan. These
connections neither enter serving pools nor consume breaker admission.

The active Runnable polls L4 and L7 schedules independently without per-origin
spawns. The typed `claviron.upstream.active.l7` duty has its own cold key so
L4 and L7 cannot change each other's interval or tenure. One worker retains
and renews this duty across the schedule; successor ownership starts a fresh
consecutive-outcome streak while retaining the slot's current serving verdict.
Cancellation, lost tenure or profile stop closes the probe connection and
releases exact ownership. The owning generation joins through the existing
maintenance tracker. No recovery loop is started by L7.

L7 outcomes have their own serving application path and metric recorder; the
reachability-only ProbeKind/apply_outcome API cannot take an L7 verdict. Failure
and success thresholds write only Serving, project the local gauge and publish
OriginServingEvent on change. Reachability/failure history, traffic and response
latency remain untouched. Body substring matching preserves chunk boundaries
without collecting or buffering a response; the prepared prefix table provides
bounded memory independently of peer body length.

Disabling a previously enabled L7 policy restores its eligibility only after
old local probe writers have joined during reload. The bit-zero update retains
independent serving deny bits; an L7 success/rollback cannot undo another cause.
Family reload completion therefore provides the rollback observation boundary.


### Serving outlier ejection

Configured response-class detectors retain consecutive 5xx/gateway streaks
and a configured interval window in the captured worker/profile epoch. Disabled
policy constructs no detector state. Real response headers, including refused
upgrade/CONNECT responses, enter detection; ingress errors and probe outcomes
do not. Retired/cancelled epochs cannot publish new bans. Worker scope affects
only that worker's eligibility, not reachability or the shared L7 verdict.
Fleet-less fleet scope has the same local semantics because there are no peers.

Fleet scope publishes the separate outlier deny bit in the existing serving
byte. Raw 1 remains the sole admitted encoding for older readers; L7 updates
preserve the deny bit. The kind-222 160-byte ABI and reserved bytes are unchanged.
A cold `claviron.upstream.outlier` key uses the existing Orbit lock capability
as a bounded durable ban: its configured TTL is published before the deny bit.
This is a policy record, not a probe execution tenure, and deliberately has no
RAII release at profile/process stop. A lost publisher cannot leave a permanent
ban; any peer re-reads expiry authority before clearing only that deny cause.
Unreadable authority retains the ban. Events remain wake-ups, never table data.

Expiry and new fleet publications serialize with the state table's structural
process lock; profile ejection caps therefore include all workers. The cap is
floor(origin_count * max_ejection_percent / 100), bounded by origin_count - 1.
Outlier policy never removes every configured origin; independent reachability
or L7 failures can still leave an empty set, which returns 503. Each successful
ejection uses base_ejection_time times this detector epoch's ejection count;
response streaks/counts are worker-owned, while fleet bans/expiry are shared.
An in-flight failure cannot extend an existing ban. The active Runnable polls
configured expiry intervals without child spawns, and selection checks expiry
lazily so eligibility never depends on receiving an event or keeping its writer
alive. Disabled/reloaded fleet policy leaves an existing bounded ban intact until
expiry; new generations do not silently resurrect an ejected origin.


### Prepared request forwarding

Static `forward.set` values and `forward.unset` names parse at compilation;
invalid names/values reject the candidate before registry adoption. The one
request preparation boundary strips hop headers, replaces untrusted forwarding
facts, applies static unsets/sets, then serializes protocol-owned Host/Connection.
`host: preserve` retains ingress Host for H1/upgrades and H2 URI authority;
missing Host falls back to the compiled origin. H2 rejects an invalid preserved
authority. Backend scheme remains transport-owned. RFC 7239 uses the shared
HTTP emitter with trusted request facts, quoted IPv6/escaped Host and one
Forwarded element; `none` cleans the forwarding families without emitting facts.
`via: false` suppresses this hop's append, preserving existing end-to-end Via.
Response header ownership remains with HostConfig.


### Explicit replay attempts

Enabled retry compiles its method list and validates its window/percentage.
The epoch owns a worker/profile budget over all admitted profile requests:
floor(requests * budget_percent / 100) retries per interval, without a minimum
allowance. Pending reservations refund on local rejection; started attempts stay
counted even after cancellation. Breaker retry admission covers the attempt and
response lifetime; a failed attempt releases its admission before another starts.
Retry counters/logs carry the configured typed cause and next selected origin.

Only eligible ordinary requests whose body is empty or fully collected under
`body.request_buffer` enter replay. Collection retains a single chunk without
copying; multiple chunks coalesce within the byte bound, with trailers retained.
Over-limit collection returns the bounded prefix, crossing frame and remaining
stream to the ordinary send-once path; it does not reject a listener-owned body.
Request idle deadlines still apply. Breaker pending reservation precedes body
collection, with pre-selection profile rejection attributed to the first origin;
cancellation releases it. Fully consumed incoming IO releases its input permit,
while the accepted response lease and epoch-owned native writer retain IO life.
Responses are never collected by this policy.

A private NUL-delimited replay-head buffer captures original typed URI/header
bytes in one allocation, leaving first-attempt HeaderMap ownership unchanged.
URI/header types prohibit NUL. Only a real retry reconstructs the original head,
so forwarding/static rewrites run fresh rather than appending duplicate facts.
Dispatch/trusted context remains in RequestScope. Replay serializes valid H1
trailers with new chunked framing/announcement and preserves native H2 trailers.
The original balancer order continues across attempts and rewinds at exhaustion;
selection re-checks both axes, never resurrecting a Down/ejected origin.

H1 serving connections created for enabled retry observe decrypted peer HTTP
bytes. Failure closes and joins the old reader before accepting zero-byte
proof, preventing an abort/read race. Evidence never resets: a reused or warm
connection without fresh proof conservatively vetoes post-send replay. User/
parse errors and any observed response byte cannot replay; successful headers
return their streaming response once, including later body failures. TLS trust/
ALPN policy failures cannot become repeated same-target attempts.
Hyper's multiplexed H2 boundary exposes no stream-scoped first-response-byte
fence. H2 therefore supports explicit pre-send connection retries and conservatively
vetoes post-send replay, including stream resets/header timeouts. Connection
control-frame counters would not prove a stream safe. No unsafe pinning or new
H2 timed-wrapper allocation is introduced. The default retry-disabled handler
remains the ordinary concrete streaming path.

### Resolution observation and metrics

With `dns`, UpstreamProvider::resolution_status returns a typed cold observation:
per-name authoritative revision/generation, active/pending/retiring addresses and
stale/truncated flags, plus this process's separately sampled serving sockets.
It performs no query/election, and is not an atomic adoption acknowledgement.
With `metrics`, the same observation carries serializable QueryCounts and owns
the four planned labeled resolution Prometheus families in every serving mode.

Only elected boot/refresh owners count completed logical lookups, including
required-boot errors; canceled/lost queries, peer reads, hints and retirement
receipts do not count. Results are ok/nxdomain/empty/timeout/error. Gauges read
canonical publications without summing per-worker copies. Fleet counters use
existing managed CounterProvider kind 230, with full BLAKE3-derived profile/name
keys; no new ABI or metrics ring is introduced. Attachment increments zero to
install without resetting peer totals. Counter capacity failure rejects cold
preparation explicitly; removed keys remain resident until the counter owner's
authorized quiescent reset. Embedders without Fleet retain private atomics across
reloads. Counter overflow is a metric error, never a DNS health verdict.

## Managed warm generations

`proxy/service/warm` owns the optional provider-generation boundary. The provider
opens it from its RunContext token, primes current profiles, and closes/joins
warm work before returning on cancellation or process shutdown. Attachment and
closure share a synchronous gate; cancelled generations cannot start cold jobs
after return. The run guard also seals attachment if its future is abandoned,
without claiming an awaited drain. A new normal run receives a fresh tracker.

`proxy/service/warm` dispatches to H1 floor maintenance or H2 preconnect.
`proxy/h2/warm` prepares one initial connection through the same
serialized dial as request acquisition. It reserves connection capacity,
but no stream or traffic permit. Concurrent warm passes share a cancellable
job flag; configured replenish backoff handles failed preparation. Both h2c
and TLS use the normal connector and ALPN validation. No boot readiness barrier
is introduced.

Warm jobs remain tracked by their captured profile epoch as well. The provider
boundary wraps dialing and configured backoff through cancellation, while epoch
stop and stale-generation checks keep their existing authority. It never
cancels the epoch background/force token, request gate, native response/tunnel IO
or independently managed health work. Registry replacement passes the same
provider control to fresh profiles. Direct service runtimes have no provider
control and keep epoch-only ownership. Provider tracking adds no polling
cadence or request hot-path lookup; each H2 driver owns its configured idle
deadline.

## Coherent reachability verdict

The existing eight-byte kind-222 region at offset 112 is a single aligned
AtomicU64: native-endian bytes remain health, serving, lifecycle, padding, and
u32 failures. Slot size, header version, all offsets and counter range are
unchanged, including inline OriginSlot values in named membership. One CAS
publishes each success/failure/validator transition. Serving deny-bit updates
use that same word and preserve reachability. There is no lock-holder state,
process-local mutex, timer or helper to recover after a writer dies.

A success clears the counter in the same transition that publishes Healthy;
a failure saturates u32 and publishes Down at its threshold or after failed
HalfOpen recovery. Scoped assignment/native fencing still surrounds the full
CAS operation. Transition event pairs come from the successful CAS rather than
independent before/after reads. Recovery time is prepared before Healthy is
published; ordinary Healthy success does not reset slow start.

Byte-layout compatibility does not authorize mixed-width concurrent access:
all processes accessing a shared table must use the single-word implementation.
Older binaries performing byte/u32 updates must not run against it concurrently.
This is not an assertion that older code has gained atomic compound transitions.
