# claviron-edge

## `src/`

### [`lib.rs`](src/lib.rs)

Public module exports and registration APIs.

- `register` installs the host resolver, application registry and native session
  registries. Serving processes also register `HostAppResolver`; the master
  retains host declarations and, with `orbit`, Link shutdown notification.
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

Normalization runs while preparing configuration. Root and prefix declarations
share feature validation; provider directives remain data for host compilation.
Directory mappings inherit the closest declared `path`. URL-derived suffixes
reject dot segments; explicit disk paths retain their configured meaning.

### `pipeline/`

#### [`app.rs`](src/pipeline/app.rs)

Optional Axum adapter (`axum` feature): `Router` implements `ApplicationHandler`;
`application` adapts it to a selective `HostHandler` contribution. The router's
response is terminal, including 404. This module also implements Axum extraction
and rejection for `config::HostConfigExt`.

## `tests/`

### `config/`

- [`routing.rs`](tests/config/routing.rs) — Route/file normalization, policy
  inheritance, invalid declarations and unavailable backend rejection.
