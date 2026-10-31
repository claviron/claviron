# claviron-edge

## `src/`

### [`lib.rs`](src/lib.rs)

Public module exports and registration APIs.

- `register` installs the host resolver, application registry and native session
  registries. Serving processes also register `HostAppResolver`; the master
  retains host declarations and, with `orbit`, Link shutdown notification.
  Link target tracking and shutdown include flat and prefix-scoped destinations.
- `register_host` / `register_hosts` retain host declarations or their source.
- `register_routable` / `register_host_binding_provider` register provider
  lifecycle and host contributions or exclusive owners.
- `register_quota` attaches a code-owned limiter to a host/prefix.

`config`, `Host` and `HostConfig` are available without the serving composition.
`server` enables provider/pipeline/transport registration; `tcp` and `quic`
expose the native drivers. `access-log`, `db`, `rate-limit`, `websocket` and
`webtransport` gate their corresponding modules or registration surfaces.

### [`config.rs`](src/config.rs)

`HostConfig` combines canonical host policy with normalized route scopes.
`HostFields` holds host-wide declarations; `RouteConfig` supplies the file and
application view. Native preparation consumes these declarations separately.

- Read-only accessors expose PHP/Lua/Link declarations and host policy.
- `FileSite` exposes the selected file mapping without a host-aware file service.
- `SelectedRoute` and `HostConfigExt` carry shared configuration metadata.
- Axum extraction is implemented by the optional application adapter.

### `config/`

- [`routing.rs`](src/config/routing.rs) — `RouteConfig` and `HostConfig`
  deserialization: normalize root/prefix declarations, validate enabled
  capabilities and owner conflicts, inherit policy and resolve file mappings.
- [`routing/statics.rs`](src/config/routing/statics.rs) — Compile relative
  directory declarations into shared static mounts with derived URL prefixes.
- [`quota.rs`](src/config/quota.rs) — `rate-limit`: IP admission declarations.
  Validate namespace/key capacity and cost against the quota definition;
  delegate store geometry and policy validation to Orbit without opening SHM.

Normalization runs while preparing configuration. Root and prefix declarations
share feature validation; provider directives remain data for host compilation.
Directory mappings inherit the closest declared `path`. URL-derived suffixes
reject dot segments; explicit disk paths retain their configured meaning.
Policy-only children inherit overlapping static mounts from the closest
declaring scope, retaining mount-relative cache rules unless they remap disk
directories. Application owners and TryFiles are not inherited.

### [`provider.rs`](src/provider.rs)

`HttpConfigProvider` owns `http.yaml` through Congif. Boot, reload and observed
changes publish prepared `HttpSnapshot` values atomically; listener capability
validation precedes publication. An absent file declares no listeners.
Registration also connects watchdog and optional metrics to snapshot accessors.

### [`loader.rs`](src/loader.rs) and `loader/`

Host declaration loading and immutable publication; no request-body execution.

- [`config.rs`](src/loader/config.rs) — `VhostDocument`, recursive includes,
  partial reads and declaration identity reuse through Congif.
- [`error.rs`](src/loader/error.rs) — Declaration/load errors.
- [`table.rs`](src/loader/table.rs) — Canonical names, aliases, listener/log
  selectors and prepared application handles in one publication.
- [`snapshot.rs`](src/loader/snapshot.rs) — Candidate host/alias edits and
  optional database alias hydration.
- [`resolver.rs`](src/loader/resolver.rs) — Retained host sources, dependent
  preparation, atomic table publication and connection retirement notification.
  File changes preserve unrelated hosts and live aliases; failed preparation
  retains the published table. `db` adds hosting dependencies and alias reads.

### `pipeline/`

- [`apps.rs`](src/pipeline/apps.rs) — Retained Rust factories, Routable and
  directive registration; per-host application preparation and snapshot reuse.
  Duplicate snapshot bindings fail without replacing the existing owner.

#### [`app.rs`](src/pipeline/app.rs)

Optional Axum adapter (`axum` feature): `Router` implements `ApplicationHandler`;
`application` adapts it to a selective `HostHandler` contribution. The router's
response is terminal, including 404. This module also implements Axum extraction
and rejection for `config::HostConfigExt`.

#### `host/`

- [`identity.rs`](src/pipeline/host/identity.rs) — Prepare shared direct peer
  identity or request-local identity from a trusted proxy's forwarding headers.
- [`identity/extensions.rs`](src/pipeline/host/identity/extensions.rs) — Prepare
  typed metadata and populate each request's independently owned extension map.

#### `embed/`

- [`lua.rs`](src/pipeline/embed/lua.rs) — Prepared Lua mount selection.
  Nonmatching requests return synchronously with the unread body; only a
  selected adapter creates an asynchronous call. Engine execution belongs to Lua.
- [`lua/websocket.rs`](src/pipeline/embed/lua/websocket.rs) — Lua H1 upgrades
  and H2 CONNECT handoff from flat and prefix-scoped declarations. H2 and H3
  revalidate endpoint ownership and origin against their admitted host config.
- [`lua/webtransport.rs`](src/pipeline/embed/lua/webtransport.rs) — Lua H3
  session admission and native stream handoff, rechecking the selected route.
- [`lua/origin.rs`](src/pipeline/embed/lua/origin.rs) — Shared browser-origin
  admission using the requested authority and accepted transport scheme.

### [`quic/mod.rs`](src/quic/mod.rs) and `quic/`

- [`types.rs`](src/quic/types.rs) — H3 provider registration, Congif environment
  binding, listener snapshots and per-host TLS policy preparation.
- [`error.rs`](src/quic/error.rs) — Normal closure and interrupted-stream
  classification, separate from protocol failures.
- [`listener/drain.rs`](src/quic/listener/drain.rs) — Join connection-owned
  stream tasks after completion, explicit force or driver panic.
- [`wt.rs`](src/quic/wt.rs) — Authorize and hand native H3 sessions to handlers.
- [`wt/streams.rs`](src/quic/wt/streams.rs) — Bounded per-session stream input;
  saturated or ended sessions never block the connection driver.

## `tests/`

### [`provider.rs`](tests/provider.rs)

Configuration preparation, value comparison, unavailable transport rejection
and retention of the active snapshot after invalid input. No server boots.

- [`fixtures/http/base.yaml`](tests/fixtures/http/base.yaml) — Valid candidate.
- [`fixtures/http/changed.yaml`](tests/fixtures/http/changed.yaml) — Changed
  listener policy.
- [`fixtures/http/invalid.yaml`](tests/fixtures/http/invalid.yaml) — Invalid
  listener address.

### `config/`

- [`routing.rs`](tests/config/routing.rs) — Route/file normalization, policy
  inheritance, invalid declarations and unavailable backend rejection.

### `pipeline/host/`

- [`identity.rs`](tests/pipeline/host/identity.rs) — Fixed identity reuse, proxy
  trust and per-request clients, typed metadata and unchanged body ownership.

### `loader/`

- [`table.rs`](tests/loader/table.rs) — Canonical/alias lookup, shared policy
  and named listener validation.
- [`snapshot.rs`](tests/loader/snapshot.rs) — Database alias input validation.
- [`resolver.rs`](tests/loader/resolver.rs) — Hosting/config boot dependencies.
- [`resolver/selection.rs`](tests/loader/resolver/selection.rs) — Coherent
  application selection, publication failures and generation ownership.
- [`binding.rs`](tests/loader/binding.rs) — Event-driven retirement on host or
  alias replacement; unrelated publications retain the binding.
