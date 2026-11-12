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

- `memory`: process-local Moka storage with a configured entry limit and
  per-entry TTL.
- `orbit`: bounded fleet-shared cache state with worker-local L1 values and
  optional named L2 backing store. Requires the `orbit` feature.
- `file`: content-addressed disk storage through `cacache`.
- `redis`: named Redis connections and a configurable key prefix; opt-in.

The provider has no cache-maintenance loop of its own. Orbit's shared-memory
coordination and inbound lifecycle belong to [claviron-orbit](../orbit/README.md).
This is a cache API, not a lock service or an atomic-counter API.

## Configuration and composition

The file-backed registration reads `cache.yaml` from Claviron's configuration
directory. This example configures a local memory store and a fleet store.

```yaml
default: shared
stores:
  local:
    driver: memory
    capacity: 10000
  shared:
    driver: orbit
    l1_capacity: 10000
```

Both capacity settings are entry limits per process for their logical store.
An Orbit cache connection also shares a resident payload
byte budget across its stores. Transport arena and notification-ring geometry
are separate Orbit settings. Ring capacity limits consumer lag, not the number of resident
keys.

`default` selects a named store. An absent `cache.yaml` or omitted `default`
selects the built-in `memory` store. An omitted store `driver` also selects
`memory`. Each memory store defaults to 10,000 entries per process.
File stores declare `driver: file`, `path`, and an optional `prefix`.
Relative paths are resolved against the application's working directory.
Redis stores refer to a connection declared in `redis.yaml`.

Each memory store starts empty in every process. A zero capacity disables
retention. Rust callers can also build a typed, synchronous Moka cache with
`claviron::cache::memory::new` for fixed-TTL workloads.

An enclosing runner can call `register_with_config(state, CacheConfig)` with
immutable boot options. The same store defaults apply.

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

Memory, Orbit, and Redis support expiry. The file driver only accepts entries
without expiry. An expiring Orbit write with a
non-expiring L2 is rejected before either layer is mutated.

An Orbit store's `l2` names another store shared by its workers. Writes validate
the Orbit operation, update L2, then publish the shared mutation. Deletes and clears also
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

The optional features are `orbit` for fleet stores, `redis` for Redis stores,
and `axum` for HTTP error conversion. The Claviron facade enables fleet stores
when both `cache` and `orbit` are selected. Redis TLS integration is currently parked
behind an internal compatibility configuration and is not an advertised
capability of the normal `redis` feature.

## Related crates

- [Runtime](../runtime/README.md): registry, prepared configuration, and lifecycle.
- [Orbit](../orbit/README.md): bounded same-host shared state and cache transport.
- [Laravel](https://github.com/claviron/lumenize): framework cache integration above this API.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
