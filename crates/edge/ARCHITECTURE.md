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

### `pipeline/`

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
