# HTTP layer — Architecture

`config::spa::SpaConfig` declares a shell entry path, `reject_ajax` and optional
existing `CachingConfig`. It accepts the old scalar entry or a strict options
map; file serving owns execution, and application bootstrap owns personalized
session/CSRF content.

A protocol-agnostic HTTP primitive crate. The crate operates no listener,
opens no socket, terminates no TLS, and owns no edge policy or configuration.
It defines the canonical request/response types every transport produces and
every handler consumes, exposes request-extension types that travel from
accept time to handler time, composes application-owned Axum route
capabilities, and surfaces protocol-level resolvers
(cookie joining for HTTP/2/3, real-IP extraction behind trusted
proxies), and provide the metrics endpoints / HOL-blocking watchdog
that listeners and middleware feed into. Host ACL, path deny, CORS,
response-header policy belong to `edge-config` and `edge`, not here.
Application-specific middleware and its response policy live in the consuming
framework, not in this primitive crate.

The role is *hub*: every transport listener in the workspace and
every middleware-bearing crate downstream pulls types from here.
The pure CORS/header declarations in `config` have no Axum requirement;
Edge enforces explicitly configured policies in its shared request/response
exchange independently of configured, Merged, bypass or Standalone ownership.
Without a declaration, no optional exchange policy is compiled.
Application routing and these edge policies are separate boundaries.
The
crate stays deliberately small per surface area — most files are well
under 200 lines — but it sits across many other crates' dependency
graphs, so its API shape carries a lot of weight.

---

## Optional HTTP/1 upgrades

The `websocket` feature owns `upgrade`, migrated from the former
`claviron-websocket` package. It selects the existing `server` and `axum`
contracts plus Hyper and the RFC 6455 SHA-1 handshake digest. None of those
upgrade-specific dependencies are enabled by the default HTTP surface.
Acceptance, opaque state handoff, capacity retention and graceful/forced drain
are unchanged; [the detailed contract](docs/upgrade/ARCHITECTURE.md) remains
listener-owned. HTTP does not depend on Edge or a WebSocket application.
H2 CONNECT and H3 WebTransport are not converted into HTTP/1 upgrades.
Header-upgrade detection accepts any request body type; it only reads the
HTTP version and headers. Edge's Link adapter performs native H2/H3 acceptance
separately from this H1 registry.

## 1. Position in the layering

`config::routes::OrbitRouteConfig` requires a typed destination `fleet_name`
and a logical `application` name. Its optional `path` is an HTTP route prefix,
not an IPC path; omitting it claims the whole host. No raw SHM path or user
selector is exposed. Edge and runner own joining and execution.

```
                           transport listener crates
                  (HTTP/1.1 edge, HTTPS H1/H2, HTTP/3 QUIC)
                                      │
                                      │  consume HTTP primitives
                                      │  build a `RouteResponse`
                                      ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │  this crate                                                     │
   │                                                                 │
   │   ctx           RouteResponse, RouteBody, RouteFuture,          │
   │                 HttpExchange, ServiceResult, body helpers       │
   │   error         HTTP Error / Result, public response metadata,  │
   │                 private diagnostic sources                      │
   │                                                                 │
   │   routing       Routable, RoutableRegistry                     │
   │                                                                 │
   │   ext           PathPrefix, Env, RealIpExt,                     │
   │                 ClavironApplication                             │
   │   ext (server)  ConnCtx, HostName, VhostKey                     │
   │   ext (cache)   Cache                                           │
   │                                                                 │
   │   forwarding    hop-by-hop cleanup, request-smuggling guards,   │
   │                 forwarded-header helpers for proxy adapters     │
   │                                                                 │
   │   resolvers     join_cookie_header,                             │
   │                 resolve_real_ip_from_headers                    │
   │                                                                 │
   │   metrics       HttpWatchdogProvider, metrics_middleware,       │
   │                 MetricsProvider-backed /metrics route           │
   └─────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                            runtime + helpers
                            types (base)
                            tls + rustls (server only)
                            tokio + tower +
                            http + http-body + axum (gated)
```

`edge-config` owns the typed `http.yaml` declaration and resolved
policy shapes. `edge` owns `HttpConfigProvider` and publishes those
snapshots. `edge-config` also owns ACL/CORS/rate-limit
declarations and dynamic-file classification; `edge` owns their
runtime enforcement and middleware. Crates above this layer (edge routing,
file serving, the PHP
runners, redirect / ACME edge) consume the request-extension types
and the `ServiceResult` / `RouteResponse` shape. Listener crates
consume the per-transport configuration slices, install the tower
layers, and write into the metrics counters via the middleware
provided here. The crate has no application-domain knowledge — no
PHP, no database, no DNS, no certificate pipeline. Anything that
crosses into a domain belongs in another crate.

### 1.1 What this crate is not

- **Not a server.** No socket binds, no accept loops, no TLS
  handshakes, no QUIC negotiation. The transport crates own those.
- **Not a vhost router.** `RoutableRegistry` composes application-owned Axum
  routes, but it has no host selection, listener, TLS, or per-host
  configuration semantics. Edge/vhost code decides where that router is
  mounted.
- **Not a file handler.** Static asset serving, directory indexes,
  and content negotiation belong in a separate crate.
- **Not a cross-domain metrics bag.** This crate owns only its HTTP request and
  transport counter/snapshot types, wires request accounting into the pipeline,
  and exposes the composition-backed Prometheus endpoint. Other subsystems own
  and register their own metric families.

### 1.2 Axum request extractors

Axum-specific request extractors live in this crate behind the `axum`
feature. They are HTTP boundary conveniences over Claviron runtime
state; they must not move into `continuo` or `runtime`.

`Inject<dyn T>` resolves a dyn capability from `AppState.registry_ref()`
on each request. Because the lookup happens at request time, providers
may change the active dyn binding during reload without rebuilding the
router. Long-lived app services may still be injected with Axum
`Extension<Arc<Service>>`; `Inject` is for the direct runtime-DI
boundary.

With the independent `cache` feature, `Cache` is the corresponding typed extractor for
the configured `CacheProvider`. It dereferences to the provider's cache
facade, so default-store calls use `cache.get(...)` / `cache.put(...)` and a
named store uses `cache.store(name)?.get(...)`. `AppState` itself remains
cache-agnostic; the dependency exists only at this Axum boundary.

### 1.3 Application route capabilities

The opt-in `routing` feature enables Axum and exposes `routing::Routable`
and `RoutableRegistry`. Raw HTTP and Axum-only consumers do not compile
the Axum route-composition registry. The pure
dispatch::RuleContext, RouteCondition, ConfiguredRoute<H> and RouteDispatch<H>
types require no feature and do not depend on Axum.
Application composition registers provider-owned Rust routes once; a
standalone runner may merge the complete registry into its local router.
Edge-bound composition uses the independent, same-named
`claviron_edge::Routable` contract because selecting and configuring a host
is a separate capability. This `Routable` therefore carries no host, listener,
SSL/TLS, or process-topology assumptions.
`Routable::routes` returns `None` when a registered capability has no ordinary
HTTP routes in the current snapshot; upgrade-only and temporarily disabled
providers therefore do not manufacture an empty edge binding.

`routes(self: Arc<Self>, state)` receives the same shared provider instance
registered for lifecycle handling. Route construction can capture that Arc
in handlers without cloning the provider's data, resolving it again, or
installing an Axum state/extension solely to reach it. The registry consumes
its cloned registration snapshot at composition time; registration and the
`Option<Router<AppState>>` result are unchanged. Handler invocations clone the
captured Arc only when their async work needs ownership.

`config::routes::rules::RouteRule` retains the old rule-only declaration as
migration data. It does not implement AJAX behavior. Current route conditions
are compiled by consumer-owned extensions; AJAX admission is not a builtin
routing field or a bundled Edge directive.

`config::routes::RouteTable<T>` owns a prefix-keyed map and its immutable,
longest-segment-prefix lookup. The generic value may be a declaration or a
prepared handler; HTTP does not resolve application engines. Map parsing
rejects duplicate normalized prefixes. No regex, sorting or deserialization
runs during lookup. Edge re-exports this type through its routing module.

Host directive codecs and their effects belong to Edge's `pipeline::ext`.
HTTP supplies the framework-neutral `RouteCondition` and `RuleContext` only.

dispatch.rs owns the framework-neutral prefix/entry decision. RouteDispatch<H>
retains the existing Arc<RouteTable<ConfiguredRoute<H>>> geometry and borrows
one H after a single longest-segment-prefix selection. Conditions see method,
URI and headers only; a rejection selects the explicit rejected H, never a
shorter prefix or the missing-prefix fallback. No handle clone, body adaptation,
future allocation, registry lookup or configuration parsing occurs in selection.
H need not be Clone to select it. A returned reference borrows the table and
does not retain the head, allowing the caller to transfer the original request.

The caller invokes the selected owner and retains request/body ownership.
HTTP does not supply an Axum dispatcher or downstream delegation handle.
Host registration and engine preparation remain Edge-owned; concrete directives
belong to consumers. The independent `routing` capability only composes
application-owned Axum routes through `RoutableRegistry`.

The application declarations under `config::routes` are gated by the HTTP
crate's dependency-free `js`, `lua` and `orbit` features. The shared WebSocket
endpoint shape is available with either `js` or `lua`; generic `rules` remain
unconditional. These gates expose data only, never execution engines. The
facade and edge forward their corresponding capability selections.

`LuaRouteConfig.origin` is a pure declaration shared by the route's WS and WT
endpoints: `any` (default) imposes no Origin restriction; `same-origin` requests
strict browser-origin admission. Edge's native acceptance handlers enforce it,
not Lua callbacks or HTTP CORS. HTTP owns no socket admission execution.

`config::statics::StaticConfig` separates the validated URL mount
(`PathPrefix`) from the filesystem directory (`PathBuf`). Filesystem paths do
not use URL-prefix validation. Edge compiles the prefix from the route key;
the prepared mount retains it for direct and axum-free file consumers.

`config::files::FilesConfig` groups the pure file-serving declarations: root,
ordered indexes, try_files, SPA and explicit static mounts. It has no host,
Edge or runtime dependency. Edge uses it as a prepared route/file view: its
external schema permits `root` only on the host and static directories only
under route keys. File-service adapters can receive this type directly. Preparation may
anchor its root once to the application's cwd; HTTP itself performs no IO.

Prefer `Extension<Arc<Service>>` for hot-path application services,
especially when the service owns caches, snapshots, or multiple
strategies. In that shape the service is injected once as ordinary Axum
state, and its `Reloadable` implementation swaps the active strategy or
snapshot internally. `Inject<dyn T>` is the lighter capability boundary:
use it when the handler genuinely wants the currently bound runtime dyn
contract, not when an application service would be the clearer owner.

---

## 2. Edge configuration boundary

`config` owns the pure `HttpYaml`, listener declaration, resolved listener
policy and override types. There is no reader of its own: every consumer is a
Congif `Configurable` that declares `http.yaml` as its source, and
`HttpSnapshot` deserializes through `HttpYaml::resolve`, so inheritance and
listener validation finish inside preparation. Variables come from the bound
application environment; nothing here reads process environment. Congif shares
source bytes and parsed syntax within a preparation batch, and each transport
narrows the shared snapshot to what it owns through a `TryFrom<HttpSnapshot>`
newtype (`HttpServerConfig`, `HttpsServerConfig`, `H3ServerConfig`) that also
checks the capabilities it was built with. The named-listener schema separates
request defaults from connection/transport policy:

```yaml
defaults:
  request:
    max_concurrent_requests: 1000
    deny: ['^/private']
  listener:
    max_connections: 500
    graceful_timeout: 30s
    tcp: {nodelay: true, keepalive: 60s}
    h1: {header_read_timeout: 10s}
listeners:
  local:
    transport: tcp
    listen: '127.0.0.1:8080'
    tls: false
    protocols: h1
  public:
    transport: tcp
    listen: ['0.0.0.0:443', '[::]:443']
    tls: true
    protocols: [h2, h1]
  public-h3:
    transport: quic
    listen: '0.0.0.0:443'
    protocols: h3
    quic: {idle_timeout: 60s}
    requests: {max_body_size: 67108864, body_read_timeout: 30s}
```

`TcpConfig.backlog` is a positive `NonZeroU32` count, defaulting to 1024 through
the named configuration default. `defaults.listener.tcp.backlog` inherits by
field and `listeners.<name>.tcp.backlog` overrides it for each TCP socket.
Zero, negative/null declarations and values above the signed socket API's
2147483647 maximum fail preparation. The OS may cap the requested accept queue;
it is independent of active connection admission and request concurrency.
`ListenerConfig::same_binding` treats a backlog change as a socket-binding
change, so a live policy reload cannot claim to apply it. Stop/start of the
owning listener is required. Accepted connections retain their own generation.

### 2.1 Desired data versus resolved snapshots

In `edge-config`, `HttpYaml` holds `HttpDefaults` and a `BTreeMap<ListenerId,
ListenerDefinition>`. Names are validated once; a custom map visitor rejects
duplicate names rather than allowing the last declaration to overwrite one.
There is no fallback translation from `services.http/https/http3`: the old
envelope and unknown keys are errors.
`HttpYaml` enforces this during deserialization: a misspelled or retired
section cannot publish an empty replacement snapshot over active listeners.

The known top-level `metrics` and `watchdog` sections remain accepted when the
`metrics` feature is disabled. That build consumes their contents without
validating metrics policy or creating metrics/watchdog runtime services, so
operators can share the same HTTP document across build compositions. With
`metrics` enabled, these sections use their existing typed configuration.

`HttpYaml::resolve()` produces `HttpSnapshot` with one shared `RouterConfig`
and named `Arc<ListenerConfig>` entries. Each entry holds its identity, bind
addresses, connection limits, proxy policy and `ListenerSettings` enum.
The enum separates TCP settings from QUIC settings. TCP's `TcpSecurity`
further separates cleartext redirect/HTTP-01 settings from TLS handshake
settings. QUIC has no insecure runtime variant. Transport sockets are not
opened here.

The edge provider's `snapshot()` returns one coherent Arc for consumer generation
construction. `router()` returns the request defaults only. The old singular
`http()`, `https()`, and `http3()` accessors are removed: selecting an
arbitrary group would lose listener identity and independent gate limits.
An absent `http.yaml` is answered by each owner's `absent_source`: no file,
no listeners. `HttpSnapshot` implements Deserialize through `HttpYaml::resolve`:
inheritance and listener validation finish before Congif compares or publishes
candidates.

Hyper/H3 consume these named groups. `ListenerSnapshots<S>` coordinates each
transport's policy publication with its active generation. It holds no sockets
or tasks: an `ActiveListeners` guard spans the transport-owned bind/accept/drain
lifetime. Accepted connections retain their selected snapshot.

While a generation runs, changing group membership, transport/security mode,
bind addresses or gate limits fails reload with a restart-required error and
does not publish the candidate to that transport. Parser/TLS/proxy policy can
reload for new connections. Start always rebuilds from the provider's latest
desired configuration, including after a rejected topology reload. Desired
`HttpSnapshot` publication and running transport publication are distinct;
there is no cross-provider atomic reload transaction.

The facade registers the edge-owned reader with immutable `ListenerCapabilities`.
Unavailable TLS/H3 transports reject the candidate before publication, at
boot and reload on every process role. Direct data-only readers default to
all capabilities; configuration parsing does not depend on driver crates.
Core integration and downstream configuration remain later steps of
`../../docs/HTTP-REFORM.md`.

### 2.2 Inheritance and validation

There are three states: missing inherits, explicit null disables an optional
setting, and a value replaces it. Nested blocks merge field by field. Lists
replace rather than append; an explicit empty list clears. Null is rejected
for non-nullable scalars and protocol option blocks. Defaults are validated
even when the corresponding transport is not currently used.

Listener names, transport, bind addresses and protocol choices do not inherit.
Both `listen` and `protocols` accept OneOrMany, but neither may be empty or
contain duplicates. Exact nonzero-port binds cannot be declared by two groups
of the same transport. TCP and QUIC can share an IP/port because their sockets
use different transport protocols. Port zero is allowed for ephemeral binds;
cross-group port-zero entries are not considered collisions.

TCP accepts H1/H2 only. Cleartext currently accepts H1 only (prior-knowledge
H2 is not implemented). QUIC accepts H3 only and requires TLS. Explicit
options for another transport or a disabled protocol are rejected, never
silently ignored. Per-listener handshake options require an encrypted
transport. HTTP-01 and redirect port belong to cleartext TCP.

`max_connections` uses `Option<NonZeroUsize>`; absent throughout inheritance
or explicitly null is unlimited. Configuration resolution never derives a cap
from file-descriptor limits. Unlimited TCP admission uses the immediate gate
entry path for drain ownership, without capacity waiting or acquisition timing.
Explicit counts retain the configured capacity and acquisition policy. Grace
timeout null is indefinite. Acquisition timeout is required when specified
and may be zero (immediate admission decision). Handshake and heartbeat
timeouts must be positive; invalid stream/parser limits fail before publish.
Hyper's H1 buffer minimum (8192 bytes) and H2 send-buffer maximum (`u32::MAX`)
are validated instead of letting its builders panic. H3's request body cap
may be zero to reject nonempty bodies.

H1's header deadline uses `HeaderReadTimeout` in resolved `H1Config`:
`DriverDefault`, `Disabled` or `After(Duration)`. Omitting the setting everywhere
keeps the driver's default; the transport must leave its timeout setter alone.
An explicit YAML null becomes `Disabled`; a duration becomes `After`. Desired
`H1Override` still uses `Override<Option<Duration>>`, so an omitted listener
field inherits its parent's resolved choice, including explicit null. Empty
Rust/YAML configuration agrees. Zero, malformed and unrepresentable monotonic
deadlines are rejected before publication. This config crate neither imports
Hyper nor copies its default timeout duration.

Public listener deployments should keep a finite header deadline and choose a
positive connection budget. Explicit null is for deliberately trusted networks
or an ingress that enforces its own deadlines; a request/path ACL cannot protect
the earlier header-read phase. This is separate from indefinite graceful drain.

### 2.3 Policy defaults

| Setting | Default |
| --- | --- |
| Connection cap / grace deadline | None (unbounded / indefinite) |
| Admission wait | 30s |
| TLS TCP / QUIC handshake | 10s / 5s |
| TCP nodelay / keepalive | true / 60s |
| TCP accept retry initial / maximum | 50ms / 1s |
| H1 header deadline / max headers | Driver default / 100 |
| H1 max parser buffer (request head limit) | 16384 bytes |
| H1 keep-alive / half-close / pipeline flush | true / true / false |
| H2 heartbeat interval / timeout | None / 20s |
| H2 stream cap / send buffer | 200 / 409600 bytes |
| H2 max header list | 16384 bytes |
| QUIC bidi/uni stream caps | 100 / 100 |
| QUIC idle / keepalive | 60s / None |
| QUIC early data | false |
| H3 max field section | 16384 bytes |
| H3 request-body cap | 64 MiB |
| Redirect target port | 443 |

Protocol option types use the same named defaults for Rust construction and
empty YAML blocks. TLS certificate/session settings remain in `tls.yaml`.

`tcp.accept_retry: {initial: 50ms, max: 1s}` controls per-socket recovery from
OS resource pressure, not connection admission or application retries. Both
durations are required-positive when specified, nested fields inherit normally,
and `initial` cannot exceed `max`. Null and unrepresentable deadlines are rejected.
Hyper captures this policy for the whole accepting generation; a reload retains
the current accept policy until stop/start while new connection policies may
change independently. There is no retry job queue or per-peer retained state.

### 2.4 Request and proxy ownership

`defaults.request` holds `max_concurrent_requests` and deny patterns, not
proxy trust. A host writes its own `max_concurrent_requests` — a count, or
`off` — which replaces the default rather than narrowing it: it is that
host's capacity. The time to a reply's head is request policy
(`requests.head_timeout`, listener then host), not a host default. The
former `tuning:` block is refused, naming where each field went.

Private unit fields in `RouterConfig` and `H3Policy` are Serde rejection hooks
for removed keys (`tuning` and `streaming`), not non-exhaustive markers. Keep
their migration diagnostics; `#[non_exhaustive]` cannot replace those hooks.

`defaults.listener.proxy` supplies trusted CIDRs and ordered forwarding
headers. Each listener may override either list, clear a list with `[]`,
or disable proxy handling with `proxy: null`. Headers are validated
`http::HeaderName` values, not runtime-parsed strings. Trust is checked
against the accepted peer, never selected using the request's Host header.

HTTP-01 is opt-in via `acme: {challenges_dir: ...}` on a cleartext group and
requires the `acme` feature. Relative paths stay process-cwd-relative.
Response-header mutation and Alt-Svc remain per host, not per listener.
H3 advertisement does not depend on enabling 0-RTT.

### 2.5 Publication and lifecycle

Loading expands environment from the frozen application snapshot, parses,
resolves and validates the whole candidate, then atomically replaces the
active Arc. A parse or validation failure leaves the active Arc untouched.
Existing consumers retain the prior snapshot for their accepted work.
The provider owns desired configuration, not socket reconciliation: the next
transport stage must distinguish live policy changes from bind/gate changes
that require a service restart.

Congif now performs source IO, expansion and preparation through the Edge
provider's `Configurable` implementation. `HttpSnapshot` equality compares
resolved values, including regex patterns rather than compiled matcher
identity. Formatting-only source changes therefore do not reload runtime
owners. Cleartext and TLS TCP transports declare their own projections of the shared source;
batch source IO is deduplicated and successful application is acknowledged
separately from preparation.

### 2.6 Native session ownership (`session`, feature `server`)

This server-only, framework-independent module composes Continuo's
existing `Gate`/`Permit` with a Tokio task set. It introduces neither another
runtime nor another timeout policy. `SessionOwner` belongs to one accepting
listener generation. Each accepted `ConnectionScope` retains the original
gate permit; `SessionSpawner` is a weak capability attached by the transport
to upgrade/CONNECT requests after head admission.

Submitting a session before its success response retains that same permit:
one connection still consumes one slot, even after its HTTP task has handed
off IO. Unused capabilities retain neither capacity nor a generation.
Completed join records are reaped on submission and drain; normal requests
do not acquire the session task-set lock. Submission and `close()` are
serialized so every accepted hand-off is included in the sealed drain.

The transport closes submissions, signals its gate, honors `None` as
indefinite and calls `abort_all()` only when explicitly forced. It must await
`SessionDrain::join()` before reporting stopped, including after force.
`wait_graceful_shutdown()` is cooperative notification, not automatic task
cancellation. Protocol close handshakes remain handler-owned. Dropping an
owner/drain aborts as an emergency fallback but is not proof of joined cleanup.
Old weak capabilities cannot submit into a newly started generation.

`ConnectionScope::with_graceful_signal` optionally carries an owned,
connection-specific retirement future through `SessionSpawner` hand-off.
`wait_graceful_shutdown` observes that signal as well as the original gate's
drain. This shared future owns no task, deadline or additional permit; closing
one host's sessions does not drain its listener or other hosts. The handler
still owns the protocol's cooperative close and completion.
The shared-future dependency is enabled by `server`, not by config-only builds.

TCP uses this owner; H3 retains its separate native stream/session driver.
Arbitrary detached tasks are outside the ownership contract. This primitive
does not implement a WebSocket protocol, a tunnel, or a global session manager.

## 3. The canonical request/response types

The crate's most consumed surface area is a small set of type
aliases that every transport and every handler agrees on.

```rust
pub type RouteBodyError = Box<dyn StdError + Send + Sync>;
pub type RouteBody      = UnsyncBoxBody<Bytes, RouteBodyError>;
pub type RouteResponse  = Response<RouteBody>;
pub type RouteFuture    = Pin<Box<dyn Future<Output = Result<RouteResponse, Infallible>> + Send>>;
```

Three deliberate choices:

- **`Bytes` data type.** Every body chunk is a `bytes::Bytes`. The
  transports decode their wire form into `Bytes` once at the edge;
  every layer between accept and handler manipulates `Bytes`
  without re-allocating.
- **`UnsyncBoxBody`.** `Sync` is not required — the request body
  travels on a single task at a time. Dropping `Sync` widens the
  set of body types that can be returned without wrapping (notably
  any `!Sync` streaming body from a downstream service).
- **`Result<RouteResponse, Infallible>`.** Every route's future
  produces a response, never an error. Error conditions are
  represented inside the response itself (status codes, custom
  bodies, error headers). The `Infallible` error type means callers
  can `unwrap()` the future's result without runtime cost when they
  need the response value.

Body construction goes through three helpers:

```rust
pub fn boxed_body<B>(body: B) -> RouteBody
    where B: Body<Data = Bytes> + Send + 'static, B::Error: StdError + Send + Sync + 'static;
pub fn empty_body() -> RouteBody;
pub fn bytes_body(bytes: Bytes) -> RouteBody;
```

`boxed_body` adapts any `http_body::Body` whose data is `Bytes` into
the canonical `RouteBody`. `empty_body` and `bytes_body` are the
common constructors for synchronous responses (no body, fixed body).

`into_axum_response` is the one-line adapter that converts a
`RouteResponse` into the `axum::response::Response` shape, gated on
the `axum` feature so the base crate stays axum-free.

`Error` and `Result<T>` are the handler-facing HTTP failure surface.
`Error` owns protocol-level response metadata (`http::StatusCode`, a
stable code, and a public message) plus an optional diagnostic source.
The type itself is framework-neutral. Framework bindings decide how to
render its public metadata; the Axum `IntoResponse` implementation lives
in `bindings.rs` and exists only with the `axum` feature. Internal
sources are available through `std::error::Error::source` for logging
and must never be serialized to clients.

Domain and lifecycle errors do not convert implicitly into particular
4xx statuses. Callers select contextual errors such as `not_found` or
`forbidden`; `Error::internal(source)` is the conservative conversion
when no safe public classification exists.

The data carried by request-extension types in `ext` also remains
available without Axum. Their `FromRequestParts`, typed-header, and
rejection-response implementations are compiled only with the `axum`
feature, matching the same adapter boundary.

Forwarding helpers compare repeated Content-Length values numerically after
validating decimal digits, reject length/transfer-encoding conflicts, and strip
hop headers named by every Connection value. They format trusted IP addresses
on the stack and use static common proto/Via values. This avoids temporary
collections while retaining the shared forwarding boundary.

### 3.1 ServiceResult — claim or pass

```rust
pub enum ServiceResult<T, U = ()> {
    Handled(T),
    Unhandled(U),
}
```

A two-arm result for "did this layer claim the request?". `Handled`
carries the response (or whatever the layer produces); `Unhandled`
carries the (possibly transformed) request back so the next layer
can try.

The pattern keeps middleware composition explicit: a layer that
might or might not handle a request returns `ServiceResult` rather
than a magic sentinel response, and the caller branches without
inspecting status codes or headers.

### 3.2 HttpExchange — request/response capture

```rust
pub struct HttpExchange {
    pub request_body:    Bytes,
    pub response_body:   Bytes,
    pub response_status: StatusCode,
}
```

A small struct shipped through an optional `mpsc::Sender<HttpExchange>`
when the embedder wants per-request audit, replay, or analysis. The
struct is intentionally minimal: the request body, the response body,
the status. Headers, trailers, and per-frame timing are not part of
the canonical capture — the consumer that wants them defines its own
type.

Handlers that opt into capture take an `Option<mpsc::Sender<HttpExchange>>`,
clone whatever bodies they have lying around, and `try_send` the
struct on the way out. The audit consumer drains the channel on its
own task, with whatever durability it chooses.

### 3.3 RequestContext — borrowed view

```rust
pub struct RequestContext<'a> {
    pub uri:     &'a Uri,
    pub headers: &'a HeaderMap,
    pub real_ip: Option<IpAddr>,
}
```

A read-only borrowed view used by helpers that want to inspect a
request without taking ownership of the parts. Lightweight, used in
a few resolution paths where threading three borrows through a long
function signature would be noisier than a single struct.

---

## 4. Request extensions

Information that travels from accept time to handler time goes
through the `http::Request` extensions map. The crate defines the
extension types; the listener crates and middleware install them.

### 4.1 ConnCtx — connection-level identity

```rust
pub struct ConnCtx {
    pub state:        ConnectionState,
    pub listener:     ListenerId,
    pub proto:        Option<Proto>,
    pub remote:       SocketAddr,
    pub local_ip:     IpAddr,
    pub local_port:   u16,
    pub real_ip:      Option<IpAddr>,
    pub country_code: Option<String>,
    pub tls:          Option<TlsInfo>,
}
```

Captured as `Arc<ConnCtx>` at connection establishment. Request adapters
clone it to attach that request's resolved client IP, without mutating the
connection baseline. The fields cover:

- The mandatory typed listener-group identity (`listener`), independent of
  which address in that group accepted the connection. It is also injected
  directly as a `ListenerId` request extension.
- The peer socket address (`remote`) and the listener-side address
  (`local_ip` / `local_port`). The latter is what binds the
  connection to a particular listening socket — a deployment with
  multiple HTTPS listeners can distinguish them from this.
- The resolved real client IP (`real_ip`). Without listener proxy policy it
  is the peer IP. Behind a trusted proxy it is resolved for **each request**,
  never retained from an earlier request on a multiplexed connection. H3
  WebTransport authentication uses the same request-scoped resolution.
- The optional ISO country code from a GeoIP lookup. `None` when
  GeoIP is disabled or the IP is non-routable.
- Optional TLS metadata captured by the terminating transport
  (`TlsInfo`): negotiated protocol, cipher, and verified client-cert
  subject/issuer when mTLS was completed. PHP runners expose this
  through standard CGI/mod_ssl-style variables such as
  `SSL_PROTOCOL`, `SSL_CIPHER`, and `SSL_CLIENT_S_DN`.

The structure is `Clone`; connection facts remain stable while request-local
client identity is carried by a separate Arc.

`ConnectionState` retains typed consumer-owned state for that accepted connection.
Its clones share one store, including request copies made for forwarded IP
resolution. Consumers initialize a type once with `get_or_insert_with`; the
short synchronous lock never spans async work. A new accepted connection gets
an empty state. HTTP owns no application actor or authentication policy.

`ConnCtx::scheme()` returns the typed `http::uri::Scheme` from physical
transport facts: TLS metadata present means HTTPS, absent means HTTP. Port
numbers, absolute-form request URIs and forwarding headers do not override
that fact. The common Hyper vhost service also injects the scheme directly
as a request extension. Trusted forwarded scheme policy, if introduced, must
remain distinct from whether this connection actually completed TLS.
`ext::request_scheme` reads `Arc<ConnCtx>` first, then a typed HTTP(S) `Scheme`
extension supplied by a standalone adapter caller. It never assumes HTTPS
when context is absent. PHP/JS/upstream use this contract. Direct JS/upstream
callers without transport context may instead provide an absolute HTTP(S)
URI; such a URI never overrides an existing transport context.

### 4.2 Vhost identity types

```rust
pub struct HostName(pub Arc<Host>);
pub struct VhostKey(pub Arc<Host>);
pub struct VhostKeyRef(pub Arc<str>);
```

Three shapes around the same concept (the SNI-resolved vhost
identity), each shaped for a different consumer:

- `HostName` is the request-extension form. Inserted by whichever
  middleware does the SNI-to-host-config resolution.
- `VhostKey` is the equality/hash key form. `Eq + Hash + Clone`,
  suitable for use as a map key in per-host caches.
- `VhostKeyRef` is the `axum::FromRequestParts` form. Extracts the
  `VhostKey` from the request extensions, normalises to a string,
  and presents itself as `Arc<str>` to handlers that just want the
  textual host. Implements `Display` and `Deref<Target = str>`.

The actual `Host` type is defined in the TLS/SSL crate (because
SNI is a TLS concept). The extension types here are workspace-shared
so any layer can reach the resolved host without importing the
TLS crate directly.

### 4.3 RealIp / RealIpExt

```rust
pub struct RealIpExt(pub IpAddr);
pub struct RealIp(pub IpAddr);
```

`RealIpExt` is the request-extension form (inserted by IP-resolution
middleware); `RealIp` is the `axum::FromRequestParts` form (extracts
from the extension and presents as `Deref<Target = IpAddr>` for
handler ergonomics). The split mirrors the vhost types: one is the
storage key, the other is the extractor.

### 4.4 PathPrefix — invariant-checked path prefix

```rust
pub struct PathPrefix(String);

impl PathPrefix {
    pub fn root() -> Self;
    pub fn as_str(&self) -> &str;
    pub fn has_path_segment(&self, path: &str) -> bool;
}
```

A wrapped `String` whose deserializer enforces three invariants:
must start with `/`, must not contain whitespace, normalises to
no trailing `/` (except the root `/`).

The matcher (`has_path_segment`) is segment-safe: `/api` matches
`/api` and `/api/x` but does not match `/apix`. This is the
behaviour every routed-prefix consumer needs. Implementing it
inline at each call site invites the `starts_with` bug; centralising
the type closes that off.

`Serialize` and `Display` round-trip the canonical form.

Normalization removes all trailing slashes, not just one; slash-only values
normalize to `/`. Prefix ACL config uses this same type and rejects duplicate
normalized keys. URI decoding/security checks for ACL belong to its owning
vhost admission policy, not this generic prefix matcher.

### 4.5 Env extractor

```rust
pub struct Env(pub EnvSnapshot);
```

A thin axum extractor over the foundation runtime's `EnvSnapshot`.
Handlers that need to resolve a deploy-time environment variable do
so through the snapshot rather than `std::env::var` (which returns
`None` for application keys after the worker scrub). The extractor
clones the snapshot from the application state on each request;
the snapshot's inner `Arc<HashMap>` makes the clone cheap.

### 4.6 ClavironApplication typed header

A `headers::Header` implementation for a custom request header that
carries an opaque application identifier. Decode validates that
the value is alphanumeric plus dash and underscore; encode is the
trivial inverse. Useful for downstream layers that route on
application identity rather than host name.

### 4.7 Forwarding helpers

`forwarding.rs` is the shared policy surface for crates that proxy a
request to another HTTP-like runtime. It owns:

- hop-by-hop request/response header stripping, including names listed
  by the `Connection:` header;
- request-smuggling guards for `Content-Length` / `Transfer-Encoding`
  conflicts and conflicting `Content-Length` values;
- `X-Forwarded-*`, or a single RFC 7239 `Forwarded` element, and `Via`
  emission from Claviron-controlled context. RFC 7239 emission removes the
  full inbound X-Forwarded family, quotes IPv6/Host and escapes quoted strings;
  unknown context is omitted. Policy selection remains with the caller.

These helpers are deliberately small functions, not tower layers. Both
the normal Axum pipeline and axum-free Other adapters need the same
rules, and Other adapters cannot depend on router middleware to clean
their boundaries.

`ForwardedRequestContext` carries a typed `Option<Scheme>` alongside the
trusted client IP. `from_extensions` shares identity/scheme projection across
Axum and bypass bindings. `from_request` also supports explicit direct-caller
context/URI, but native scheme takes precedence. Unknown scheme stays absent;
incoming forwarding headers cannot supply it.

---

## 5. Edge policy boundary

Host selection and Edge exchange policy do not belong in this primitive crate.
Reusable ACL/CORS data lives under HTTP config; Edge owns host declarations,
deny/ACL, response-header and CORS enforcement. HTTP's optional RateLimiter
adapts a prepared Orbit store; Edge config/register_quota binds it to a host
before routing or bypass. Application response middleware remains consumer-owned.

### Edge Tower middleware layers

Tower layers compile without `axum`; router adapters require that feature.
CORS policy/configuration and allowlist evaluation are always available under
`layers::cors`, while its middleware and response helpers require `axum`.

### 5.1 Always-on layers

#### Served-By — Server header

```rust
pub fn build_served_by(served_by: &str) -> SetResponseHeaderLayer<HeaderValue>
```

A thin wrapper around `tower-http`'s `SetResponseHeaderLayer` for callers
building their own Tower stacks. Edge itself stores a prepared baseline in
each `HostApp` rather than installing this layer. `HttpYaml.defaults.headers` and
`HttpSnapshot.headers` reuse `HostHeadersConfig` for server-wide response
defaults. `http.yaml`'s `defaults.headers.server` replaces the ordinary `"claviron"`
baseline; explicit vhost/route header policy wins. Listener configuration has
no separate header field. Standalone applications retain their response identity
unless a global or vhost override is explicitly declared. Validation and merging
happen before publication, not on the request path.

#### MoreHeaders — per-vhost set / unset

```rust
pub fn build_more_headers(
    set: Vec<(HeaderName, HeaderValue)>,
    unset: Vec<HeaderName>,
) -> MoreHeadersLayer
```

Per-vhost response-header overrides — nginx `headers-more`
parity (`more_set_headers` / `more_clear_headers`). The vhost
router builder reads `HostConfig.headers.{set,unset}` and
hands the precompiled lists to this layer.

Application order: **unset first, then set**. Each name in
`unset` is removed from the outgoing response (looped to
clear multi-valued headers fully), then each pair in `set` is
inserted (override semantics — replaces any value the inner
pipeline produced). An operator who lists a name under both
ends with the explicit `set:` value on a clean slate.

This layer remains available for custom Tower stacks. Edge uses its common
compiled exchange policy instead: global rewrites are merged with the selected
vhost/route policy at snapshot construction, and local actions win by header
name. The same response shaping applies to Axum, bypass and Standalone owners.

#### Admission — concurrency and the time to a reply's head

`claviron_http::admission` holds what a server's requests are admitted
under before an application sees them: `Admission` (a semaphore per host,
entered for each request and waited on up to its `requests.queue_timeout`,
503 past it) and `head_within` (the reply's head within
`requests.head_timeout`, 504 past it). They are a server's health, not a
pipeline's, so they hold on every path to a host — router, bypass, relay —
and live here for any server to apply at its own door. The edge applies
them in `HostService::dispatch`; a backend that times its own head against
the same narrowed value (the upstream) is not timed twice. They replaced a
Tower layer on the Axum router, which the bypass paths never passed.

`head_within` stores the answering future once and projects it in place. It is
lazy: construction needs no timer context, and a configured deadline starts
on first poll. With no limit, it polls only the answer without reading a clock
or creating a timer. Timed polling preserves Tokio's reply-before-deadline and
cooperative-budget handling, including an answer that exhausts its task budget.
No extra allocation, task or body wrapper is introduced. Dropping the head wait
drops its answering future and timer, retaining the same cancellation boundary.
Completion or expiration drops that inner future in place before returning
`Ready`, even when the caller keeps the completed head-wait object alive.

### Typed listener configuration foundation

Behind `server`, `listener::ListenerId` is the shared, validated listener identity for the
named-listener migration. It is case-sensitive and independent of socket
addresses; YAML keys and selectors deserialize through the same constructor.
It has no unchecked public constructor. Names begin with an ASCII letter or
digit and otherwise contain ASCII letters, digits, dots, underscores or
hyphens. `ListenerTransport` describes TCP versus QUIC. `HttpProtocol` reuses
the existing `claviron_tls::Proto` enum rather than duplicating H1/H2/H3;
protocol ownership can be revisited with the transport-context domain split.

`claviron_helpers::overrides::Override<T>` represents absent versus explicit values. Nullable
settings use `Override<Option<T>>`; non-nullable scalars reject null. Lists
replace rather than append; an explicit empty list clears inherited values.
The named listener parser and provider snapshot are implemented in section 2.
Hyper/H3 preserve the typed group identity through transport snapshots and
connection/request context. The old `services` envelope is rejected rather
than translated into an arbitrary group.

`H1Config` and `H2Config` use manual `Default` implementations matching their
serde field defaults. An absent protocol block and an empty `{}` block now
produce the same parser and flow-control settings.

### Edge configuration ACL primitive

```rust
pub enum AccessPolicy { Allow, Deny }
pub struct AccessLimiter {
    pub policy:  AccessPolicy,
    pub allowed: Vec<IpNet>,
    pub denied:  Vec<IpNet>,
}
```

`is_allowed(ip)` walks the appropriate list:

- Under `Deny` policy (whitelist semantics): returns `true` on first
  matching CIDR in `allowed`, otherwise `false`.
- Under `Allow` policy (blacklist semantics): returns `false` on
  first matching CIDR in `denied`, otherwise `true`.

Only `allow`/`deny` and `allowed`/`denied` are accepted; legacy aliases and
unknown fields are rejected during deserialization. `AccessLimiter` remains
the reusable IP rule primitive; host-specific prefix maps are declared by
`claviron-edge-config::acl::AclConfig` and compiled/enforced by vhosts outside
Axum. `acl.rs` is available without any feature. The old `AclLayer`,
`AclMiddleware` and `RouterAclExt` are removed: host admission is the single
enforcement boundary, before routing and bypass dispatch.

### Inheritable route extension layers

Edge's `extension::RouteExtension::inherit_from_host` defaults to false. Host-aware
composers may opt in a codec whose effect is always `RouteEffect::Layer`:
the host declaration supplies route defaults, explicit routes override them,
and fallback pipelines retain the default policy. Handlers and conditions cannot
be inherited through this hook. Edge compiles these layers into HostPipeline;
HTTP's Axum routing adapter does not own them.

### 5.3 Framework-independent path deny

`claviron_edge::deny::DenyService` retains the compiled `RouterConfig` RegexSet and exposes
`is_blocked(path)`. Empty sets short-circuit. It contains no Axum/Tower code
and is available without any feature; the former `layers::deny` path is gone.
`HostService` checks it before routing, bypass and native session admission.
Native drivers retain wire policy (Hyper 403, H3 404), not this matcher.
There is no `deny_middleware` or second Axum check. Cleartext retains its
early deny-drop guard before authority and non-SSL selection. `check_request`
returns a `DenyCheckedRequest` borrowing the immutable matcher and owning the
unchanged request. Only read access is exposed. `accept_checked` skips regex
evaluation for that exact matcher; a different matcher evaluates its rules.
The proof needs no allocation/refcount increment and cannot exempt a mutated
URI or different policy. HTTP-01 continues to short-circuit before deny.

### 5.4 Framework-independent request authority

`authority::RequestAuthority` validates the native request head without polling
the body or changing its URI/headers. It retains the selected wire `Authority`
and the normalized `claviron_types::Host` separately. `from_request` and
`from_parts` share one parser over borrowed version/URI/header fields; an
already split request head needs no reconstruction or clone. Host fields are singular
and validated even when the target supplies its own authority. HTTP/1.1
requires Host; HTTP/1 absolute/authority-form targets take precedence over a
valid Host value. HTTP/1.0 may have no authority, leaving any connection-bound
default to the caller; cleartext selection still requires an explicit authority.
HTTP/2 and HTTP/3 accept URI authority or Host fallback and reject disagreements
after host and scheme-default-port normalization. Listener bind ports never
substitute for the public request port.

Native drivers remain responsible for wire framing and pseudo-header rules,
including classic versus extended CONNECT. This primitive neither selects a
vhost nor compares authority with TLS/SNI: those are separate host-admission
policies. It is available without Axum and has no Hyper/H3 dependency.

### Edge Axum-bound layers

#### CORS — three-policy with regex allowlist

```rust
pub enum CorsPolicy { Disabled, Permissive, Allowlist }
pub struct CorsConfig {
    pub policy:      CorsPolicy,
    pub credentials: bool,
    pub origins:     Vec<String>,
    compiled:        OnceCell<RegexSet>,
}
```

Three policies cover the common cases:

- `Disabled` — no CORS headers added; cross-origin requests fail at
  the browser. The middleware short-circuits with `403` if a request
  with `Origin:` arrives.
- `Permissive` — `*` (or echo of `Origin:` when credentials are on,
  per spec) for any origin.
- `Allowlist` — exact-string match first (cheap), then a `RegexSet`
  match for patterns. The regex set is compiled lazily on first
  use via `OnceCell` and cached for the configuration's lifetime.

The middleware handles preflight `OPTIONS` requests itself,
mirroring `Access-Control-Request-Method` and
`Access-Control-Request-Headers` from the request, setting
`Access-Control-Max-Age: 600`, and returning `204 No Content`. For
non-preflight, it runs the inner service and decorates the
response.

`Vary:` is computed contextually: always includes
`access-control-request-method` and `access-control-request-headers`;
adds `origin` when the response value depends on the request origin
(any allowlist policy, or permissive with credentials).

The deserializer accepts both a bare string ("permissive") and a
full map (`{policy: ..., credentials: ..., origins: [...]}`), so
the simple case stays terse in configuration files.

#### Bounded rate admission

The optional `rate-limit` surface is independent of Axum/server/Edge/Runner.
`RateLimiter<K>` owns a prepared `claviron_orbit::quota::Quota`, a bounded key
namespace, request cost and one KeyProvider instance. The generic provider has
an instance-fixed maximum key length and an owning byte-shaped output. There
is no process-wide provider, user/API-key enum or authentication lookup. Custom
providers can inspect already parsed head/extensions and must keep their own
work bounded. IpKey uses trusted RealIpExt with allocation-free binary encoding;
IPv4-mapped IPv6 shares the IPv4 identity. Missing identity never falls back
to untrusted headers.

Quota identity, arithmetic, retention and commit remain in Orbitive; the Orbit
binding owns store selection/lifetime. `check` returns authoritative decisions
and preserves typed errors. `Rejection::from_result` is a framework-neutral,
empty HTTP reply adapter: Limited is 429 with ceiling-seconds Retry-After,
Busy/CapacityFull is 503 without a guessed retry delay, invalid custom identity
is 400, and setup/infrastructure failures are 500. There is no wait, retry,
refund, remaining-count approximation or per-attempt logging.

Reporting is disabled per limiter by default. `with_headers(true)` makes
`admit` request the primitive's committed Snapshot, without another identity
lookup, clock/storage read or CAS. `check` remains decision-only. `apply_headers`
adds X-RateLimit-Limit (burst units) and X-RateLimit-Remaining (whole units after
this charge) on 2xx/101, and private/no-store cache control. It replaces the
pair together after application response policy; no cached template is mutated.
Refusals retain only Retry-After. Snapshots can be stale/reordered at delivery;
they are neither reservations nor a fixed-window remaining-request contract.

The legacy fixed-window `config::rate_limit::RateLimitConfig` remains available
to consumers of the earlier application middleware. It is not the new Edge
GCRA declaration and is not silently converted into rate/burst semantics.

---

## 6. Resolvers

Two small protocol-edge helpers that do not warrant a full layer.

### 6.1 Cookie joining

```rust
pub fn join_cookie_header(headers: &HeaderMap) -> Option<String>;
```

HTTP/2 (RFC 7540 §8.1.2.5) and HTTP/3 explicitly allow clients to
split the `Cookie` field into multiple header occurrences for
HPACK / QPACK compression efficiency. Major browsers routinely do
this. A naive `headers.get(COOKIE)` returns only the first
occurrence; cookies in subsequent occurrences silently disappear.
The visible failure mode is "session cookie vanishes after a few
requests while smaller cookies survive", because the small ones
happened to land in the first occurrence.

The helper walks `headers.get_all(COOKIE)`, joins non-empty values
with `"; "`, and returns `None` when no `Cookie:` is present
(distinguishes "no cookies sent" from "empty cookie value"). The
output is the HTTP/1.x-shaped single-line form that downstream
parsers expect.

A small test suite in the same file pins the edge cases (single
cookie unchanged, split joined with separator, empty values
skipped, absent header returns `None`).

### 6.2 Real-IP resolution

```rust
pub fn resolve_real_ip_from_headers(
    headers:        &HeaderMap,
    remote_ip:      IpAddr,
    trusted_proxies: &[IpNet],
    header_order:   &[HeaderName],
) -> IpAddr;
```

The trust gate: only honour proxy headers when the immediate peer
is inside one of the trusted-proxy CIDRs. An untrusted peer
contributing `X-Forwarded-For` headers cannot poison the resolved
IP — the helper returns the peer address unchanged.

When the peer is trusted, the helper walks `header_order` and,
for each header, parses comma-separated values left-to-right,
returning the first parseable `IpAddr`. The leftmost-parseable rule
matches the de-facto convention for `X-Forwarded-For` (`client,
proxy1, proxy2`) where the original client lands at the start.

If none of the configured headers contain a parseable IP, the peer
address is the fallback.

---

## 7. Metrics & watchdog

Feature-gated on `metrics`. The crate provides three pieces:

### 7.1 Request middleware

```rust
pub async fn metrics_middleware(req: Request, next: Next) -> Response;
```

Two responsibilities, both around the global `HTTP_METRICS` counter:
bump `request_start` on entry, bump `request_end` on exit. The exit
bump is wired through an RAII guard, so cancellation, panic, and
client disconnect all decrement the counter — the in-flight count
stays accurate even when futures are dropped mid-flight.

The same HTTP family owns static response-cache observations. The file cache
records `HIT+` after `fast_cache_hit`, `HIT` after
`try_serve_from_cache`, and `MISS` after `maybe_read_and_cache` or the normal
`stream_from_disk` fallback. `claviron_http_memory_cache_entries` is refreshed
from `MEMORY_CACHE.len()`, while `claviron_http_memory_cache_bytes` mirrors the
cache's authoritative `TOTAL_CACHE_BYTES` admission counter. These are fields
of the existing HTTP snapshot, not a separate file-metrics provider or
background service.

### 7.2 Family, Orbit transport, and endpoint

The crate owns two distinct metric families. `HttpMetricsFamily`
(`claviron.http`) owns request and cache values. `ServerMetricsFamily`
(`claviron.server`) owns TCP/UDP accepts, connection state, TLS failures, and
vhost rejects. They are not combined merely because the old endpoint once
formatted both stores. `local` is not part of either type or family name:
locality is a data-source detail.

- **Standalone** — each family renders its own process-local store directly.
- **Worker fleet** — every worker publishes HTTP kind `224` and server kind
  `225`. Each family collecting a `/metrics` request takes the newest fresh
  sample per worker from its own ring, merges its own snapshot, and renders
  the same canonical Prometheus names as standalone mode.
- **Master** — managed SHM companions validate/create/reuse kinds `224` and
  `225`; the master neither publishes worker counters nor serves application
  traffic. All processes preserve the rings, while `max_age` excludes retained
  samples from an older run.

The fixed-width kind `224` record is exactly 66 bytes (fleet node id, capture
timestamp, seven request/cache values). Kind `225` is exactly 58 bytes
(fleet node id, capture timestamp, six server/transport values). Each ring uses
the standard 1,024-slot metric snapshot window. Both publication paths share
the policy in the single-reader `http.yaml` surface:

```yaml
metrics:
  enabled: true
  publish_interval: 1s
  max_age: 5s
```

The private callback beside `metrics::routes()` only resolves
`claviron_metrics::MetricsProvider` and renders its registered families. It
does not decide whether an owner reads local state or Orbit. The route does
not install `metrics_middleware`: the owning vhost router installs that layer
once around the completed host router, preventing `/metrics` requests from
being counted twice. HTTP has no supervisor or PHP dependency.

### 7.3 HOL-blocking watchdog

```rust
struct HttpWatchdogProvider { /* watchdog state + shared HttpConfigProvider */ }
pub struct WatchdogConfig { /* check_interval, warn_threshold, panic_threshold,
                                stall_timeout, panic_on_hol */ }
```

A `Provider + Runnable` that polls `HTTP_METRICS` on a configured
interval looking for evidence of head-of-line blocking — requests
piling up in flight without the total counter advancing.

The provider is registered only in traffic-serving processes: workers and
standalone. The master has no watchdog registration or service membership,
because it does not handle application traffic. `claviron-http::register` owns watchdog registration; the root facade
bootstrap does not know the concrete provider. The runnable reads configuration
on every tick from its shared `HttpConfigProvider` so a configuration
reload that changes the interval or thresholds takes effect on the
next tick without a process restart.

`start_policy` reads the current watchdog config: `Some` is `Automatic`,
`None` is `Unavailable`. A disabled watchdog remains visible on eligible
processes, creates no initial generation and rejects explicit starts.
Policy reevaluation alone does not start or stop a generation: enabling a
previously stopped watchdog requires an explicit service start. An already
running generation idles while config is disabled and resumes checking when
enabled again; explicit stop still cancels its ticker normally.

Per tick, the watchdog compares the current `(in_flight, total)`
pair against the previous tick's snapshot. Progress is defined as
"total increased OR in-flight decreased" — either requests finished
or backpressure is releasing. Without progress, the stall duration
extends; once it crosses `stall_timeout` the watchdog logs at
`error` level. Once in-flight crosses `warn_threshold` the watchdog
logs at `warn` level and dumps tokio runtime metrics.

The `panic_on_hol` flag is the optional escalation: when set, an
in-flight count past `panic_threshold` calls `panic!` after dumping
diagnostics. The intent is to get a clean process exit and a
supervisor-driven respawn rather than letting an unrecoverable
freeze persist.

---

## 8. Feature flags

| feature      | what it pulls in |
|--------------|------------------|
| `axum`       | Framework adapters and runtime extractors. No application middleware or automatic policy installation. |
| `routing`    | Implies `axum`; application-owned route composition. Host directive codecs remain Edge-owned. |
| `server`     | Accepted-connection context, listener identity and native session ownership shared by server transports. Edge configuration/provider/middleware remain in their edge crates. |
| `cache`      | Axum `Cache` extractor and `cache`; implies only `axum`. |
| `metrics`    | Adds the HTTP metrics composition provider, HTTP request/transport stores, Axum endpoint, request middleware, HTTP and server metric families, and watchdog. It implies `axum`, not `server`, so a standalone mini HTTP runner may use it without edge listener primitives. |

The base and `axum`-only surfaces remain application-runner safe.
`types` supplies the base authority identity, but
`claviron-http/axum` does not pull `tls`, rustls, listener lifecycle,
or native session ownership. Those arrive only through `server`.

---

## 9. Components

| File | Approx LoC | Role |
|------|----------:|------|
| `lib.rs`                 |     | Primitive module declarations and public request/response re-exports. |
| `listener.rs`            | — | Validated listener identity and transport/protocol types. |
| `ctx.rs`                 |  61 | `RouteResponse` / `RouteBody` / `RouteFuture` aliases, `HttpExchange`, `ServiceResult`, `RequestContext`, body helpers, `into_axum_response`. |
| `forwarding.rs`          | 150 | Hop-by-hop cleanup, smuggling guards, and forwarded-header helpers for proxy adapters. |
| `ext/conn.rs`            |  47 | `ConnCtx` — connection-level identity (peer + listener + real IP + country code). |
| `ext/host.rs`            |  67 | `HostName`, `HostKey` — server-side vhost identity. |
| `ext/scheme.rs`          |     | Trusted typed request scheme; consults `ConnCtx` only with `server`. |
| `ext/ip.rs`              |  46 | `RealIp`, `RealIpExt` — resolved real IP + axum extractor. |
| `ext/prefix.rs`          | 100 | `PathPrefix` — invariant-checked, segment-safe path prefix with serde. |
| `ext/env.rs`             |  48 | `Env` — axum extractor over the runtime `EnvSnapshot`. |
| `ext/xapp.rs`            |  48 | `ClavironApplication` typed header. |
| `resolvers/cookies.rs`   |  77 | `join_cookie_header` + tests. |
| `resolvers/ip.rs`        |  46 | `resolve_real_ip_from_headers` + trust gate. |
| `metrics/config.rs`      |     | HTTP metric enablement, publish cadence, and freshness policy. |
| `metrics/http.rs`        |     | Request/cache store, snapshot, merge rule, and canonical formatter. |
| `metrics/server.rs`      |     | Server/transport store, snapshot, merge rule, and canonical formatter. |
| `metrics/orbit.rs`       |     | Exact kind `224` HTTP and kind `225` server worker snapshot codecs and ring specs. |
| `metrics/provider.rs`    |     | Separate HTTP/server families, worker publishers, and managed reusable-ring registration. |
| `metrics/router.rs`      |     | `/metrics` route and its small composition-only callback. |
| `metrics/middleware.rs`  |  38 | Request `start`/`end` counter middleware with RAII guard. |
| `metrics/watchdog.rs`    |     | `WatchdogConfig`, progress state, structured diagnostics and the private worker/standalone runnable. |

---

## 10. Status & remaining work

The crate is stable. Scope-bounded work that may land in subsequent
passes:

- **Streaming body ceilings.** `HttpPolicy` makes every listener declare
  a `max_body_size` and a `body_read_timeout`, each a value or `off`. A total-duration ceiling
  for a body that keeps arriving slowly but never stalls is still
  missing; capping the total is not obviously right, since a large
  upload on a slow link is legitimate.
- **Watchdog without `metrics`.** The watchdog is feature-gated
  because it reads metrics-crate counters. A no-op stub on
  the disabled-feature path could simplify downstream conditional
  registration; today consumers `cfg`-gate the registration call
  themselves.

None of these block consumer work; the crate's API has been stable
for several releases of every consuming crate.

---

## 11. Out of scope

The crate intentionally does not cover:

- **Listener / server implementations.** No socket binds, no accept
  loops, no TLS handshakes, no QUIC negotiation. Those live in
  per-transport crates.
- **Routing / dispatch.** The vhost-resolution and per-host route
  composition live in a separate crate that consumes the host-name
  extension types defined here.
- **TLS configuration.** Certificate resolution, SNI matching,
  OCSP, ALPN — all in the TLS/SSL crate.
- **Body transformations.** Compression, decompression, content
  negotiation — handled either by `tower-http` directly or by
  domain-specific crates that wrap responses.
- **WebSocket / long-poll upgrade handling.** Belongs in the
  consumer that owns the streaming protocol.
- **Application logic.** Anything that interprets the request
  beyond shape — vhost selection, file existence, PHP routing,
  database calls — is somebody else's problem.

The dependency direction is one-way: anything in this crate can be
used by transport listeners, middleware-bearing crates, and
handler crates without those callers' concerns leaking back. If a
new feature would need to know about a downstream consumer's
specifics, it belongs in that consumer.
