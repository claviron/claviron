# claviron-cache

Named cache stores and a shared value contract for Claviron applications.

This crate separates cache selection, value encoding, and storage. Applications
use one `CacheProvider`, resolve a named `CacheRepository` when needed, and can
change backend configuration without teaching each caller about Redis, files,
or fleet coordination.

Applications normally use `claviron::cache` through the root crate's `cache`
feature. The facade's `redis` feature adds the optional Redis backend; selecting
it alone does not enable the cache subsystem.

## Stores and ownership

| Surface | Responsibility |
| --- | --- |
| `CacheProvider` | Default-store operations and the active named-store catalog. |
| `CacheRepository` | One store's byte operations and common typed value codec. |
| `CacheStore` | Driver-neutral storage of immutable owning byte buffers. |
| `CacheDriver` | Construction and validation of stores from their options. |
| `CacheRegistry` | Driver registration and complete catalog composition. |

The built-in drivers are:

- `orbit`: bounded fleet-shared cache state with a per-process L1 index and
  optional named L2 backing store.
- `file`: content-addressed disk storage through `cacache`; enabled by this
  implementation crate's default feature.
- `redis`: named Redis connections and a configurable key prefix; opt-in.

The provider has no cache-maintenance loop of its own. Orbit's shared-memory
coordination and inbound lifecycle belong to [claviron-orbit](../orbit/README.md).
This is a cache API, not a lock service or an atomic-counter API.

## Configuration and composition

The file-backed registration reads `cache.yaml` from Claviron's configuration
directory. This minimal example selects an Orbit store without L2:

```yaml
default: shared
stores:
  shared:
    driver: orbit
    l1_capacity: 10000
```

The capacity is an example entry limit per process for this logical store, not
a global byte budget. Shared arena and notification-ring geometry are separate
Orbit settings. Ring capacity limits consumer lag, not the number of resident
keys.

`default` names a declared store; the driver name is not implicitly the default
store. Redis stores refer to a connection declared in `redis.yaml`. File stores
declare `path` and an optional `prefix`. Relative paths are resolved against the
application process's working directory, not the YAML file.

An absent `cache.yaml` composes no stores; it does not silently select disk or
Redis. Registration still prepares the registry and Orbit service so later
configuration can be composed through their lifecycle. An enclosing runner may
instead call `register_with_config(state, CacheConfig)` with immutable boot
options, without reading or watching `cache.yaml`.

Custom drivers or ready store instances can be registered with `CacheRegistry`
before configuration seals it. Driver factories remain available for reload
candidate construction; runtime requests do not construct drivers.

## Values and reads

`get` and `put` operate on bytes. `get_value` and `put_value` use `CacheValue`:
null, booleans, signed/unsigned integers, floats, strings, bytes, and structured
`CacheJson` values. Typed frontends can therefore share an encoding without
imposing an application model on the storage driver.

For example, a caller with an already configured provider can read one named
store without knowing its backend:

```rust
use claviron::cache::{CacheProvider, CacheValue, Result};

async fn read_label(cache: &CacheProvider, key: &str) -> Result<Option<String>> {
    let store = cache.store("shared")?;
    match store.get_value(key).await? {
        Some(CacheValue::String(label)) => Ok(Some(label)),
        _ => Ok(None),
    }
}
```

Raw reads return `Bytes` that owns its lifetime independently of store locks or
eviction. `CacheValue::view(&bytes)` validates a borrowed view; keep the byte
owner alive while using it. Current composite archives can be inspected without
first building an owned JSON tree. `get_value` deliberately returns owned data.

The value envelope is versioned. Do not assume arbitrary old and new binaries
can share cache namespaces across codec changes without a compatibility plan.

## TTL, backing stores, and recovery

TTL is a store capability, not a promise every driver makes. Orbit and Redis
support expiry; the file driver does not. An expiring Orbit write with a
non-expiring L2 is rejected before either layer is mutated.

An Orbit store's `l2` names another, non-Orbit store. Writes validate the Orbit
operation, update L2, then publish the shared mutation. Deletes and clears also
apply to L2 before shared publication. A read satisfied by L2 is returned
directly, without promotion: the raw store contract supplies neither a revision nor a
remaining TTL that would make promotion safe.

Consumers that fall behind discard unsafe L1 state and recover through L2 when
available; without a usable backing path they report the recovery failure
rather than knowingly serving a stale index. A fresh process starts with an
empty L1. Retained SHM is not a durable snapshot that reconstructs every key.

`clear` is namespace-scoped. File clearing is limited to the store's path or
prefix; Redis clearing requires a non-empty prefix and does not flush the
whole server.

## Reload and optional capabilities

File-backed configuration builds and validates a complete candidate catalog
before publishing it atomically. Existing repository handles retain their old
generation; resolve again through the provider when a long-lived caller needs
the new one. Removing a store does not delete its persistent data.

Orbit topology is not freely mutable in place: changing an existing store's
L1 capacity or L2 relationship can require restart. Reload must respect the
already-booted Orbit service and reject incompatible changes.

The implementation features are `file`, `redis`, and `axum`; the last adds HTTP
error conversion, not cache routes. Redis TLS integration is currently parked
behind an internal compatibility configuration and is not an advertised
capability of the normal `redis` feature.

## Related crates

- [Runtime](../runtime/README.md): registry, prepared configuration, and lifecycle.
- [Orbit](../orbit/README.md): bounded same-host shared state and cache transport.
- [Laravel](https://github.com/claviron/lumenize): framework cache integration above this API.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
