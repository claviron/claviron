# claviron-cache - Architecture

`cache` is Claviron's configured cache facade. It separates backend
composition from request-time use: drivers build byte stores, repositories add
common cache behavior, and the final provider resolves named repositories or
proxies to the default one.

The crate is above file, Redis, and Orbit storage primitives. It is below HTTP
handlers, language bindings, and application services.

## Role

The cache layer owns:

- owning `config/cache.yaml` as a Congif `Configurable` (`CacheProvider`),
- owning `config/redis.yaml` the same way (`RedisProvider`) when the Redis
  feature is enabled, independently of the cache composition,
- a registration-time extension point for built-in and third-party drivers,
- building and sealing the initial named store catalog before request traffic,
- rebuilding `cache.yaml` into an atomically published catalog generation on
  reload,
- selecting the default store,
- resolving the optional named Orbit L2 and adapting it to `CacheBacking`,
- the byte-oriented `CacheStore` contract,
- the driver-neutral `CacheRepository` behavior,
- the runtime `CacheProvider` facade,
- registering the Orbit cache service when at least one Orbit store exists.

It does not implement SHM rings, mutation ordering, arena pin ownership, local LRU
eviction, L1/L2 ordering, resynchronization, or platform notification. Raw
mechanics are consumed through `orbitive::cache`; the Claviron cache algorithm
lives in `claviron-orbit::cache`.

## Public Shape

- `CacheRegistry`: registration/build-time registry. Downstream code may add an
  `Arc<dyn CacheDriver>` or a ready `Arc<dyn CacheStore>` during application
  registration. `CacheProvider::configure` completes cross-store links,
  installs the initial catalog, then seals further registration. Once sealed, it retains driver
  factories so `CacheProvider` can ask it to build a complete reload candidate.
- `CacheProvider`: reloadable runtime facade. Rust and language adapters call
  `get`/`put`/`get_value`/`put_value`/`forget`/`clear` for the default store or
  `store(name)` for a named `CacheRepository`. It owns the config path and the
  active catalog generation, but driver knowledge remains in `CacheRegistry`.
  It owns no background task.
- `CacheRepository`: common operations for one named store. It owns
  `CacheValue` encoding/decoding and delegates raw storage to `CacheStore`.
- `CacheStore`: async byte `get`, `put`, `forget`, namespace `clear`, and TTL
  capability contract. `get` returns immutable owning `bytes::Bytes`, not a
  freshly copied vector. Storage drivers do not own serialization policy.
- `CacheDriver`: named-store factory used at boot and reload. A driver receives
  the common store entry and may parse its options, ignore them, or use
  configuration from its own provider. Its optional finalization receives a
  read-only resolver for the complete candidate graph, not the active catalog.
- `CacheValue`: driver-neutral typed envelope owned by this crate. Composite
  values use the crate-owned `CacheJson` tree rather than exposing a serializer
  type to language adapters.
- `CacheValueRef`: validated borrowed envelope view. Composite values expose
  the crate-owned `ArchivedCacheJson`; adapters can traverse it without first
  allocating an owned `CacheJson` tree.
- `FileCacheStore`, optional `RedisCacheStore`, optional `OrbitCacheStore`.

Framework-neutral cache primitives are not re-exported from this crate. Rust
callers that need them depend on the Orbitive facade and use
`orbitive::cache` directly.

`increment` and `decrement` are deliberately absent. Atomic numbers are shared
state, not cache mutations, and will use an Orbit state primitive instead of a
side log hidden inside this crate.

The embedded PHP adapter exposes driver-neutral `claviron_cache_*` primitives.
`claviron_cache_store()` resolves a named or default `CacheRepository` once and
returns an opaque native handle; get/put/forget/clear operate on that handle.
`CacheProvider` remains an internal catalog/lifecycle dependency and is not
exposed as a PHP value.

## Configuration

```yaml
default: fleet
stores:
  disk:
    driver: file
    path: storage/cache/default
  redis:
    driver: redis
    connection: default
    prefix: "claviron:"
  fleet:
    driver: orbit
    l1_capacity: 10000
    l2: redis
```

Redis connection ownership is separate from the cache store graph:

```yaml
# redis.yaml
connections:
  default:
    url: redis://127.0.0.1:6379/0
    min_delay: 2s
    max_delay: 5m
```

`l1_capacity` is the number of metadata entries retained by each process. It is
independent of mutation history and the existing shared arena's byte/index budget. `l2` optionally names
one non-Orbit store in the same configuration. Multiple Orbit stores may be
configured: they have independent local L1/L2 state but share one physical
mutation ring and existing fleet arena and one inbound listener per process.

Relative paths remain process-cwd relative. They are not resolved relative to
the YAML file.

`cache.yaml` describes cache composition; it does not own Redis connection
configuration. `RedisProvider` owns `redis.yaml` and exists whether or not a
cache uses it, so non-cache services share the same provider. Both files are
prepared by Congif — variables from the application's frozen configuration
environment — before boot and in the central reload batch; neither provider
opens its file. An absent `redis.yaml` declares no connections; a changed one
drops the cached clients so the next use connects as declared. An absent
`cache.yaml` composes no cache until a reload or a prepared source change
declares one.

`cache.yaml` is not a requirement imposed on driver implementations. A
downstream driver can use its flattened store options:

```rust
let caches = state
    .registry_ref()
    .resolve::<CacheRegistry>()
    .expect("CacheRegistry not registered");
caches.register_driver(my_driver)?;
```

It may instead read its own config and call `register_store` with a ready
`Arc<dyn CacheStore>`. In both cases registration must finish before the
owners are configured, which every registration does. Unknown configured
drivers fail composition instead of being silently skipped.

## Reload Generations

Reload takes the prepared `cache.yaml`, constructs every configured non-Orbit store,
resolves cross-store links against that candidate graph, and validates its
default before publishing anything. A failure leaves the current catalog
untouched. Publication is one `ArcSwap` operation, so new resolutions see the
complete new generation and never a partially rebuilt graph.

An already-resolved `Arc<CacheRepository>` remains valid and finishes against
its old store generation. This is intentional for in-flight PHP requests and
Rust operations. Callers that hold a repository beyond one operation must
resolve it again when they want reload changes. Removing a store from config
only removes it from new resolution; reload never clears its file data, Redis
keys, or other backing state. Ready stores registered directly by the
application are reused across generations because their construction belongs
to their owning provider.

Orbit's listener, physical rings, logical L1, and optional backing binding are
long-lived process state. Reload therefore reuses existing Orbit stores. A new
Orbit store may be enabled when `OrbitCacheService` was already active at boot;
it is opened on that service without registering another listener. Disabling a
store removes it from the new facade catalog but deliberately retains its
process-local runtime, allowing old handles to finish and a later enable to
reuse the same L1. Enabling the first Orbit store still requires restart
because reload cannot add the missing runnable provider.

An active Orbit store's L1 capacity or L2 topology cannot be changed in place,
and a store used as its L2 cannot be reconfigured while that Orbit store stays
active. Those candidate builds fail without replacing the current catalog.
Supporting such topology changes later requires an explicit generation
contract in the Orbit cache service rather than silently replacing live L1
state.

## Orbit L1

Each `OrbitCacheStore` owns one logical
`claviron_orbit::cache::CacheRuntime` handle per process. Clones share that
store's metadata L1 and backing binding. All logical runtimes created by the
driver share the `claviron_orbit::cache::CacheService` mutation cursor and
mutation ring and byte arena.

```text
models L1       responses L1
    ^                ^
    +---- (store, key) mutation dispatch ----+
                         |
       one mutation ring -> one existing fleet arena
```

The mutation ring is transport, not cache storage. Every mutation carries a
separate store name and key. `Put` carries an immutable arena address; only
revision/expiry/address metadata enters L1. Reads pin the exact arena generation
and return `Bytes` over SHM, without a SHM-to-L1 copy. `Delete` and `Reset`
update only that store; they never reset the arena's other consumers. A process starts
with empty L1s and follows future mutations; it does not reconstruct a snapshot
by scanning retained history.

Linux and FreeBSD wait on the cache mutation ring's native readiness fd. Other
targets poll every 10 ms. Those mechanics and the long-lived task live in
`claviron-orbit::cache::CacheService`. `CacheProvider` and
`OrbitCacheDriver` do not implement the inbound runnable.

The mutation transport and existing arena are registered through Claviron's managed
SHM lifecycle with `Reuse` at boot and `Preserve` at finalize. Every process
starts its process-local L1 empty and opens the mutation cursor at the current
head, so retaining the physical resources neither replays old frames nor
claims persistent cache values. Existing peers keep their own cursors and are
not split onto a replacement ring during master adoption.

## Optional L2

Each Orbit store may name any registered non-Orbit store as L2. File and Redis
are built-in examples; stores supplied by third-party drivers work through the
same `CacheStore` contract. `cache` resolves that name and wraps the
selected store as a `CacheBacking`. L2 remains part of that logical store's
Orbit algorithm rather than a second public cache selected by callers; the
algorithm itself runs in `orbit`:

```text
get:    L1 hit -> return
        L1 miss -> read named L2

put:    validate Orbit bounds -> write L2 -> publish Orbit Put
forget: validate Orbit key    -> delete L2 -> publish Orbit Delete
clear:  clear L2 namespace    -> publish Orbit Reset
```

Writing L2 before publishing prevents a peer that receives the mutation from
falling back to an older backing value. Orbit validation also runs before the
L2 write, so an oversized key/value cannot partially update only L2.

An L2 result is returned directly and is not silently promoted into L1. The
generic `CacheStore::get` contract does not expose the backing entry's revision
or remaining TTL; installing such a value as an unbounded local entry could
resurrect an older value or outlive its backing TTL. Live puts and inbound
mutations populate L1 normally. A future promotable L2 contract must carry
version and expiry explicitly.

An expiring Orbit write requires an L2 whose `supports_ttl` capability is
true. Redis preserves the millisecond TTL and is accepted. The current file
driver does not support TTL and rejects such writes; consequently, an Orbit
store backed by file accepts non-expiring values but rejects TTL writes before
changing either layer. This prevents an expired L1 entry from reappearing
indefinitely through a stale L2 value.

If the mutation cursor loses history, `orbitive::cache` disables local hits. With
L2 configured, `CacheRuntime` discards uncertain state, advances to the current
ring heads, records the shared revision floor, and resumes as an empty
L2-backed L1. Without L2, mutation loss is an error; an incomplete local view
must not be presented as coherent.

Runtime `reset` clears the complete configured L2 namespace before publishing
the Orbit `Reset` mutation. File stores clear their dedicated cache path or,
when prefixed, only matching logical keys. Redis stores require a non-empty
prefix and scan/unlink only that namespace; an unprefixed Redis store rejects
clear rather than risking deletion of unrelated database keys.

Synchronous Orbit cache access uses the Tokio handle captured during
`CacheService` boot. Rust async callers use the normal `CacheRepository`
methods, which delegate raw operations to `CacheStore`. Rust code that wants
only the framework-neutral primitive uses `orbitive::cache` directly.

## CacheValue Envelope

`CacheValue` supports null, bool, signed/unsigned integers, floats, strings,
bytes, and JSON-shaped array/object trees. Scalar values retain the compact
`CLVCV1` envelope. New composite values use a validated, unaligned rkyv archive
under the `CLVCV2` envelope, so file, Redis, Orbit, and third-party byte stores
share one codec without rendering the tree as JSON text. `CacheJson` preserves
separate signed, unsigned, and floating-point number variants and preserves
object entry order for language adapters such as PHP.

Raw `get` retains the immutable buffer owner. An Orbit hit acquires a short-lived
arena pin and constructs `Bytes::from_owner` without copying the payload; file
and Redis readers transfer their received allocation into `Bytes`. Eviction,
overwrite, reset and catalog replacement cannot invalidate an already returned
buffer. L1 retains no value bytes or pins. Arena ownership rows use kernel holds
for death-proven SIGKILL recovery; live pins are never reclaimed by timeout.

`CacheValue::view(&bytes)` validates the envelope and borrows strings, binary
values, and `ArchivedCacheJson` directly from that buffer. The owner stays with
the caller, so ordinary Rust lifetimes enforce safety without a self-referential
wrapper or unchecked archive access; `Bytes` retains the arena pin underneath:

```rust,ignore
if let Some(bytes) = repository.get("entry").await? {
    match CacheValue::view(&bytes)? {
        CacheValueRef::Json(ArchivedCacheJson::Array(values)) => {
            // Traverse archived elements while `bytes` owns the arena pin or driver allocation.
            println!("{} elements", values.len());
        }
        _ => {}
    }
}
```

`get_value` and `CacheValue::decode` deliberately return independent owned
values. They use the same validated view and explicitly deserialize through
`CacheValueRef::into_owned`. Borrowed reads avoid deserialization, not validation;
checking a composite archive still traverses its structure.

Readers accept both formats: the former `CLVCV1` type-7 JSON payload is decoded
for existing file/Redis cache entries and converted into `CacheJson`; new
composite writes emit only `CLVCV2`. Unheadered bytes still decode as
`CacheValue::Bytes`.

`CLVCV2` is an archive schema boundary. Changing the `CacheJson` archived shape,
rkyv format-control features, or an incompatible rkyv version requires a new
envelope version and a decoder for the old one. Cache bytes are validated before
borrowed access or deserialization. The workspace selects rkyv's unaligned
representation because
the byte-oriented `CacheStore` contract does not promise aligned allocations and
an envelope prefix may place the archive at an arbitrary address. Little-endian
encoding and 32-bit archive offsets are selected explicitly rather than relying
on rkyv defaults; those format controls are part of `CLVCV2`.

The new reader is backward-compatible, but an already-running pre-`CLVCV2`
binary cannot understand new composite writes. A deployment that overlaps old
and new workers must isolate the new generation with a cache namespace/path
bump, or quiesce the old workers before allowing new composite writes. Merely
clearing a shared cache is insufficient while mixed writers remain active.

The canonical type lives in `cache` because it is shared repository
behavior, not an Orbit primitive. File, Redis, Orbit, and third-party stores
all remain serializer-neutral and store only bytes.

## Provider Ordering

`register_with_config(state, CacheConfig)` accepts the same schema from an
enclosing launch document. It registers the same drivers and facade, without
reading or watching `cache.yaml`; ordinary default registration retains it.
All application driver registrations finish before this facade's boot builds
and seals its catalog, before ordered cache consumers boot. The supplied intent
is immutable for that process: it exposes neither Configurable nor Reloadable.
Changing this enclosing launch requires restart. An empty declaration builds
an empty catalog and creates no cache stores.

`CacheProvider` implements Continuo's `Register<AppState>` via the runtime
facade, delegating to the same crate-level composition entry, and is the
Congif `Configurable` for `cache.yaml`.

`claviron-cache::register` composes without reading a file: it registers
`RedisProvider`, creates `CacheRegistry` and `CacheProvider`, and registers
the built-in drivers whose dependencies are available. Registering a driver
only records it; `CacheProvider::configure` hands the registry the prepared
`cache.yaml` and seals it in the same step: every configured store is built
from the registered drivers, each is verified to have one, links such as
Orbit L2 are finished, the final provider catalog is installed and
registration is sealed. `CacheRegistry` has no boot of its own. The
downstream application's registration callback can still extend
`CacheRegistry` before that, because every registration precedes `configure`.
`CacheProvider` is explicitly ordered after `CacheRegistry`; cache-dependent
providers can order after `CacheProvider` and see only the finished facade.
Config-only validation uses the same path, since it configures every owner.
After sealing, a prepared source change is a new catalog generation, exactly
as an explicit reload is.

The Orbit cache's rings and the shared `claviron_orbit::cache::CacheService`
are registered with the driver, not with the first Orbit store: whether a
store uses them is the configuration's to say, at boot or on a later reload.
The service is ordered after `FleetProvider`; its `Runnable::run` owns the one
notification/poll loop until shutdown, and with no fleet and no store it stays
idle rather than failing boot.

## Invariants

- Driver names and store names are unique; duplicate registration is an error.
- `CacheRegistry` accepts registrations only before it is sealed.
- A usable `CacheProvider` always contains a configured, registered default
  repository. If `cache.yaml` is absent, neither `CacheRegistry` nor
  `CacheProvider` is registered; an independently configured `RedisProvider`
  remains available.
- Every store declared in `cache.yaml` must be supplied by a registered driver
  or registered directly; unsupported drivers are never silently ignored.
- An Orbit store may reference only an existing non-Orbit L2 store.
- Any number of named Orbit stores may share one physical cache connection.
- There is exactly one cache mutation listener per process when Orbit stores
  are configured.
- `(store, key)` is the cache identity; the same key may exist independently
  in multiple stores, and reset is store-scoped.
- Ring capacity bounds consumer lag, not resident key count.
- Every logical store's L1 is independently bounded by its `l1_capacity` and
  owned through `claviron-orbit::cache::CacheRuntime`.
- A backing value without revision/expiry metadata is never promoted to L1.
- An Orbit TTL write never passes through an L2 that cannot preserve TTL.
- L2 writes/deletes complete before the corresponding Orbit mutation publishes.
- L2 namespace clear completes before the corresponding Orbit reset publishes.
- Mutation loss requires L2 recovery or an explicit error.
- Cache boot/finalize never resets or unlinks the shared mutation transport or arena; every process opens its cursor at the current head.
- Typed value semantics belong in `CacheRepository` and `CacheValue`, not in
  storage drivers.
- Counters and locks are not cache mutations; lock primitives belong to
  `orbitive::lock` and
  `claviron-orbit::lock`.
