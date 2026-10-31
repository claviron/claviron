# claviron-edge

HTTP host admission, routing, application dispatch, and optional native transports.

The edge decides which virtual host owns a request, applies that host's admission
policy, and selects its application. Its opt-in [TCP](docs/tcp/README.md) and
[QUIC](docs/quic/README.md) modules own native wire protocols and connection
lifecycle separately from the shared hosting pipeline.

Applications normally use `claviron::edge` through the root crate's `edge`
feature. Enable TLS, HTTP/3, and application adapters
explicitly as needed. The edge is neither an interpreter nor a language-specific
application framework.

Direct crate consumers select `tcp` for HTTP/1.1 and HTTP/2, add `tls` for
the HTTPS driver, or select `quic` for HTTP/3 and its native WebTransport
bindings. TCP and native host compilation do not require the `axum` feature;
select it when supplying Axum Rust applications. The existing `http3` feature exposes shared H3 declarations;
`quic` additionally selects the driver. Selecting a feature does not register
or start a listener. Runner retains its own ingress and does not depend on Edge.

## Two HTTP server compositions

Claviron offers two main HTTP server compositions. Choose the edge when building
a comprehensive application server directly on Claviron: multiple virtual hosts,
native services, script routes, embedded applications, static files, and upstreams
can coexist under its host-aware routing and admission policy. It can also serve
a single host with a rich mix of those capabilities.

For one framework or application that does not need a virtual-host table, use
[claviron-runner](../runner/README.md). A runner surrounds one application owner
with shared routing and ingress. It can serve HTTP directly, act as an HTTP
upstream behind an edge or another proxy, or receive same-host requests over
Orbit Link. The edge can therefore host applications directly or route to
dedicated runners without making their execution engines part of the edge.

## Configuration declarations

`claviron::edge_config` is a facade alias for `claviron::edge::config`, available
with the root `edge` feature. In the current tree it is a module of this crate,
not a separate `claviron-edge-config` package.

`HostConfig` combines host-level declarations with normalized route entries.
`RouteConfig` holds application and file declarations; reusable listener,
transport, request, response, and file-policy vocabulary lives in
[claviron-http](../http/README.md). Parsing a declaration does not start its
listener or application. Runtime composition and publication remain separate.

Host policy distinguishes several boundaries:

- `listeners` selects named listeners. Omission permits all; an explicit empty
  list permits none. Explicit names are checked before host publication.
- `transport` narrows listener protocol capabilities; a host cannot enable a
  protocol the listener does not support.
- `tls` describes handshake policy. Request and response policy are separate
  declarations, not alternative ways to configure the handshake.

Prefix-keyed `routes` select response owners and route directives. Normalization
resolves `/` before child entries, regardless of map order, and rejects conflicting
flat/root declarations or multiple application owners on one entry. The filesystem
`root` belongs only to the host. Each route pairs its URL prefix with one disk
directory: without `path`, URL components name directories below the host root;
an explicit `path` selects another directory relative to that root, or an
absolute directory. Children below a mapped prefix retain that mapping and append
their remaining URL components. Static, `try_files` and PHP disk lookup use
the same prepared directory/prefix pair without rewriting the HTTP URI.
Host-level `statics`, route `root` overrides and `static.dir` are rejected.
Within a route, `statics` lists relative directory names with their file policies.
Each `dir` supplies both the directory below the route's disk mapping and the
URL suffix below its prefix; entries cannot declare another `path` or `root`.
PHP, Orbit and other application owners are not implicitly
inherited. Matching and fallback are
prepared routing behavior, not an arbitrary per-request configuration merge.

```yaml
example.test:
  root: public
  routes:
    /frontend:
      try_files: true
      statics:
        - dir: assets
        - dir: icons
```

This serves `/frontend/assets/app.js` from `public/frontend/assets/app.js`.
To place the frontend elsewhere, set `path` on `/frontend`; its directories
and `try_files` lookup use that same mapping. Each directory's rules see file
names below its own prefix, without the containing URL path. These directory
policies do not create additional routes or quota stores.
Native Rust intercepts receive a `config::SelectedRoute` extension, including
its prepared file policy. Canonical host metadata remains separate. Axum-free
file consumers receive a compiled
mount view from the same route declarations; they need no separate YAML format.

The outer host schema composes feature-dependent sections and route-extension
directives; it is not uniformly a closed, strict record. Fixed nested policy
records apply their own parsing rules. Unknown or unavailable extension directives
are rejected during preparation. Accepting configuration data does not install a
disabled engine or grant a route an undeclared runtime capability.

## From host declaration to request

A host declares its identity, aliases, root, routes, and policies through
`HostConfig`, configuration documents, or a registered host source. During
publication, Claviron prepares the application and transport policy for each
changed host and publishes the resulting table together.

Requests select a prepared application; they do not call its factory. An
unchanged host reuses its built application. A rejected candidate leaves the
previously published table in service instead of partially publishing a router
and its policy.

Host selection and admission include authority, listener/protocol policy,
TLS binding where applicable, access controls, and configured host responses.
Application dispatch follows admission; choosing an Axum-free handler does not
skip those checks. Forwarded origin information is distinct from physical peer
identity and follows the configured trust policy.

## Rate admission

Select the facade's `rate-limit` feature alongside `edge`. A host's
`rate_limit` is compiled into a prepared IP limiter, not an Axum extension:

```yaml
example.test:
  root: public
  rate_limit:
    namespace: public
    cost: 1
    quota:
      units: 20
      period: 1s
      burst: 40
      table: {slots: 1024, shards: 16, key_bytes: 64, probes: 8}
      cas_attempts: 64
      store: {mode: local_guard}
```

These values are example declarations, not implicit defaults. `local_guard`
has an independent budget per process. To share one host-wide budget, select
`fleet_guard` or `fleet_quota` and declare the shared resource:

```yaml
store:
  mode: fleet_guard
  resource: {fleet: example, kind: 0}
  owners: 32
  recovery_attempts: 0
```

Each limit definition needs its own SHM resource in a dedicated quota namespace,
not the runtime fleet. Peers must use identical policy/geometry; incompatible
state is rejected, never cleared. Guards can discard cold key debt; FleetQuota
retains live debt. Neither mode divides the budget by worker count.

An explicit route may declare its own `rate_limit`. The longest matching
declaration selects one store; otherwise the host declaration applies. Aliases
share their canonical host's prepared limiter. No declaration means no quota
processing. Limits run after trusted IP/ACL admission and host replies, before
body adaptation, native session acceptance, static files and the Axum/bypass fork.
This limits HTTP requests and socket handshakes, not messages inside an accepted
WS/WT session or the earlier TLS/QUIC handshake.

Reporting is off by default: only rate refusals carry `Retry-After`. Opt in per
definition with `rate_limit.headers: true`, or per Rust limiter with
`limiter.with_headers(true)`. Successful HTTP responses and H1/H2 WebSocket
acceptance then carry `X-RateLimit-Limit` (burst capacity) and
`X-RateLimit-Remaining` (whole quota units after this charge). At 20 units/s,
burst 40 and cost 1, the first admission reports 40 and 39. Weighted costs are
not request counts. 429 continues to carry only `Retry-After`.

All three store choices report from the successful admission, with no extra
store/clock read or CAS. Reports can be stale when delivered: LocalGuard is
process-local, FleetGuard permits cold-key replacement, and concurrent fleet
charges change the balance. Reported replies use `Cache-Control: private,
no-store` so shared response caches cannot replay a caller's snapshot; internal
resource caches are unchanged. The opt-in pair replaces application values
with the selected Edge limiter's values. No fixed-window reset is advertised.
Native H3 WebTransport acceptance currently has no custom-header API in the H3
dependency; its handshake does not carry these optional headers. Ordinary H3
HTTP responses do.

Code can instead attach its own prepared limiter at exactly that boundary:

```rust,ignore
edge::register_quota(state, host, PathPrefix::root(), limiter)?;
```

`limiter` is an HTTP `RateLimiter<K>` retaining its own store and `KeyProvider`.
Register before the first host preparation. A duplicate host/prefix or a
config/Rust collision is an error, not an override. Config currently uses
`IpKey` only; auth-provider config and embed APIs are separate work.

Unchanged host preparation reuses the budget. Changing a live limit requires a
process restart; quota reload is not supported. Configuration checks validate
structure without opening quota SHM. Runtime opening also verifies native
atomics, clock and persisted state compatibility. No publication or runtime
validation is implied by this source API.

## Lua browser socket admission

Each `lua:` application route selects `origin: any` (the default, no Origin
restriction) or `origin: same-origin`. The same choice applies to H1 WebSocket
Upgrade, H2 WebSocket CONNECT and H3 WebTransport CONNECT. With `same-origin`,
before acceptance, one HTTP(S) `Origin` must match the requested host, effective port
and accepted transport scheme. Missing, opaque (`null`), malformed or repeated
origins receive 403. HTTP CORS settings, including permissive CORS, do not widen
this socket boundary. Aliases compare against the requested alias, not its
canonical vhost key. This blocks other websites' browser connections; it is
not user authentication, and native clients can forge Origin. Other registered
handlers retain their own admission contracts.

```yaml
lua:
  application: demo
  origin: same-origin
  websocket: { path: /socket }
```

## Adding an application

In a server registration callback, declare a host and attach a Rust Axum
application before its native downstream:

```rust
use claviron::{
    axum::{Router, routing::get},
    edge::{self, Host, HostAppRegistry, HostConfig},
    runtime::{AppState, registry},
};

fn register_host(state: &AppState) -> registry::Result<()> {
    edge::register_host(
        state,
        "example.test".parse::<Host>()
            .map_err(|error| registry::Error::msg(error.to_string()))?,
        HostConfig::new("public"),
    )?;

    let apps = state.registry_ref().resolve::<HostAppRegistry>()
        .ok_or_else(|| registry::Error::msg("edge registration is required"))?;

    apps.register_app("example.test", |_state| {
        Router::new().route("/health", get(|| async { "ok" }))
    });
    Ok(())
}
```

This uses the root `edge` feature and assumes the standard edge infrastructure
has already been registered. It does not configure or start a listener. The key
passed to `register_app` selects the canonical vhost, not an arbitrary alias.

`register_hosts` lets an integration supply multiple declarations from its
prepared configuration. Host-source callbacks run while rebuilding the host
table, never for individual requests; they should consume prepared snapshots
rather than perform their own file I/O.

## Application shapes

`HostAppRegistry` supports distinct ownership arrangements:

- `register_app` adds an Axum Rust application before the selected native
  file/backend chain.
- `register_router` installs a complete Axum router whose application middleware
  is owned by the caller. Host admission still applies.
- `register_bypass` installs a native handler without requiring an Axum router.
  Here “bypass” refers to the router shape, not to host access controls.

Services can implement the host-aware `Routable` contract and register the same
shared service instance with `register_routable`. This connects component
lifecycle to per-host route construction. It is distinct from the host-free
`claviron::http::routing::Routable` contract used by single-application routing.

`Routable::application` prepares a native `ApplicationInterceptor` without Axum.
It receives an `InterceptRequest` whose head can be inspected or changed while
the body remains private. Return `Unhandled` with that capability to continue;
consume it with `into_request` when handling the body. A handled error response,
including 404, does not fall through. Only registered contributions execute,
in registration order, before the configured owner; absent contributions leave
the direct owner unchanged. Axum routes remain an optional Rust application
adapter. `layer_routes` returns `Some(router)` when active and `None` otherwise;
an inactive hook installs no Axum invocation.

Configured dispatch prepares the order **WebSocket → Routable → Static →
TryFiles → Embed/backend**. The optional WebSocket step requires a registered
exact endpoint, permission for H1 in `transport.protocols`, and
`transport.h1.upgrade: true`. It is absent when any requirement is missing.
Its captured endpoint map requires no request-time registry lock; ordinary
requests pass their original body onward. Host security, admission and CORS
precede dispatch; response headers still apply before the handshake is sent.

`StaticStage` and `TryFilesStage` are separate native steps
using the shared file service. Only declared steps are installed. A static miss
continues only when its mount permits it; a TryFiles miss passes the same unread
body to the backend. Directory-index and SPA fallback remain behind backend
owners. Mount prefixes and disk directories are prepared once, without URI
rewriting or a second Static/TryFiles probe in the backend fallback.

Embedded runtime backends live under `pipeline::embed`. Lua uses
`embed::lua::LuaBackend` to select its prepared mount and call the native Lua
application adapter. A matching unavailable application returns 503; it never
falls through to another backend. Nonmatching mounts retain the original body.
Lua dispatch requires no Axum middleware, extractor or `Next`.

`embed::php::PhpBackend` prepares distinct Script, Application and FCGI owners
from resolved runtimes. Runner selection happens only at boot/reload. A
persistent application calls its prepared application handle without script
resolution; a matching unavailable application returns 503. Script/FCGI
resolution may yield the original unread request. PHP engines and shared
contracts no longer supply Axum bindings.

`pipeline::upstream::UpstreamStage` selects an upstream profile and calls its
native request boundary. It retains live profile replacement and the service's
existing streaming, lease and cancellation behavior. Static/TryFiles belong to
the preceding host steps; the explicit `UpstreamBypass` file composition remains
available separately.

`pipeline::link::LinkBackend` prepares segment-safe application mounts and their
destination handles. A miss returns the original request; a matching unavailable
destination owns its 503. Upload and response streaming remain concurrent. An
upload's task owner moves from the pending head to the response body and is
cancelled when that exchange is dropped or ends. Empty requests create no upload
task. WebSocket and WebTransport keep their native session owners.

Host preparation compiles selected scopes directly into native owners. The
longest prefix and its conditions select one chain; a rejected entry goes to
the declared fallback and skips that entry's applications and WebSocket. A sole
unconditional root is installed directly. Each scope carries its own
`SelectedRoute` metadata and file/backend ownership.

Config-only hosts require no Axum router. Axum is entered for explicitly supplied
Rust routes or active application layers. Configured Pusher REST endpoints use an optional Rust application adapter
before files/backends; user native contributions keep first claim. Embedded
engines, files, Upstream and Link do not use an Axum adapter. Accepted connections retain the prepared publication across reload.

Native backends implement `DispatchStep` with `Request<RouteBody>`, independently
of Rust application interception. `Unhandled` transfers that same request to
the next owner; it does not decode or replay the body. Synchronous decisions
stay inline in `DispatchFuture`; asynchronous steps erase only their own work.

Static files, native routes, embedded applications, FastCGI, HTTP upstreams, and
Orbit application links are composable capabilities, not interchangeable
execution engines. Enabling one does not select or configure all the others.

The [host pipeline](../../docs/design/HOST_PIPELINE.md) covers connection
binding and native application composition. [Early-data policy](../../docs/design/EARLY_DATA_POLICY.md)
defines which replayable requests may reach that composition.

## Route extensions

Extensions are resolved and compiled while building a host's routes. Unknown or
unavailable directives fail preparation rather than being silently ignored.
Request handling uses the prepared route layers, not a per-request extension
registry lookup.

`claviron::edge::extension` exposes the generic directive, condition and layer
contracts with the ordinary Axum routing surface; no separate extension feature
is required. Applications and frameworks own concrete directives such as AJAX
admission and CSRF, including their errors and response policy. Built-in rate
admission uses the separate host quota boundary described above.
Register them through `Server::with_edge_extension`, `HostAppRegistry` or a
matched `Routable` provider. Claviron installs no application policy implicitly.

A framework built on Claviron can add its own typed settings to a vhost's
routing configuration without changing Claviron's schema. For example, after
registering framework-owned `locale` and `csrf` directives, a route could declare:

```yaml
routes:
  /:
    locale: tr
    csrf: true
```

These are illustrative framework settings, not built-in Claviron directives.
The framework owns their types, validation, request behavior and failure
responses. Claviron supplies the configuration extension points and prepares
their effects with the host's routes. Host-level layer inheritance is explicit
through `RouteExtension::inherit_from_host`, not automatic for every custom key.

## Reload and connection lifetime

Host publication, application preparation, and listener ownership are separate.
A host change does not rebuild unrelated hosts. Changing a transport's bound
listener settings may require a restart according to that transport's contract.

Established TLS connections retain a validated binding. Replacement or removal
of that binding follows connection retirement and graceful-drain rules; it is
not equivalent to reinterpreting an existing connection as a different TLS
host. Policies designed for request-time lookup can change without rebuilding
the application. Do not assume every reload updates every established
connection in the same way.

## HTTP/3 and WebTransport ownership

Edge uses the crates.io H3 fork family `0.0.2`. One native connection driver
continues accepting HTTP requests after a WebTransport CONNECT succeeds.
Pending HEADERS resolution, session authorization and CONNECT response I/O
progress independently; none hands the connection to an application session.

`H3WebTransportSession::accept_bi().await` returns an
`Option<H3WebTransportBiStream>`, already routed to that session ID rather than
a native resolver. The input queue is bounded by the accepting listener's
`quic.max_concurrent_bidi_streams`; an unavailable or full receiver refuses
that stream without waiting in the shared HTTP acceptance loop. Ordinary HTTP
requests never enter this receiver. Native outgoing stream/datagram methods
remain available through the session's underlying H3 transport.

Host admission, Origin checks, 0-RTT rejection and the original connection
permit remain in place. On graceful shutdown or host retirement, new HTTP
admission stops while the driver remains alive for accepted work. The native
owner joins request/session tasks before releasing capacity, including after
an explicitly forced drain. Built-in Lua and Broadcast handlers retain their
existing single-bidi message loops.

## Related crates

- [claviron-runtime](https://github.com/claviron/claviron/tree/main/crates/runtime): composition, events, and drain.
- [claviron-runner](https://github.com/claviron/claviron/tree/main/crates/runner): a host-free shell around one application owner.
- [claviron-app](https://github.com/claviron/claviron/tree/main/crates/app): hosted application identity and control.
- [claviron-supervisor](https://github.com/claviron/claviron/tree/main/crates/supervisor): process-family lifecycle.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
