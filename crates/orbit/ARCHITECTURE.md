# claviron-orbit - Architecture

`orbit` binds the framework-neutral `orbitive` facade into the
Claviron runtime lifecycle. It
owns fleet provider registration, boot order, the `AppState` fleet accessor,
the typed event bridge, the Claviron-facing Orbit cache runtime, and Claviron's
keyed counter state. It also owns Claviron's typed, domain-facing lifecycle
policy boundary for shared-memory resources; Orbitive remains responsible for
the underlying POSIX mechanics.

For application dispatch, Orbitive's `orbit-link` owns the generic named
inbox, admission, reusable duplex slots, and exact incarnation recovery.
Claviron supplies kinds and geometry, then adds application naming, HTTP
exchange frames, routing, status mapping, and runtime lifecycle integration.

HTTP routes prepare a `TransportTarget` from their destination client while
the route is built. Its shared immutable lane snapshot is tagged with the Link
directory generation. The ordinary send reads that snapshot without a client
map lookup or directory-cache mutex. A generation change performs one bounded
lane scan and one compare-and-swap publication attempt; a delayed refresher
cannot replace a snapshot published since it began. The generation is captured
before scanning, so a concurrent change invalidates the result on the next send.
Depth and admission are still read per exchange. The name-based client API
retains its existing cache for callers that do not prepare a target. Stream
loans, reply correlations, crash recovery and wake publication are unchanged.

A lent response reader signals reader departure as soon as it is dropped,
even if the request upload still holds its half. Returning both halves to the
pool remains separate: rearming waits for the remote endpoint to release its
generation. A cancelled reader therefore cannot retain pre-head runner work
until an unrelated upload finishes.

## Role

### Explicit quota stores

`QuotaStore::try_consume_with_snapshot` is forwarded unchanged to the selected
store, as is decision-only admission. The binding does not calculate remaining
capacity, reread state or add synchronization; optional snapshots come from
Orbitive's successful transition. HTTP reporting remains in the HTTP adapter.

The optional `quota` surface assembles Orbitive's LocalGuard, FleetGuard and
FleetQuota behind `Quota`. It owns typed immutable definitions and handle
lifetime, not key selection, HTTP status or authentication. It consumes the
`orbitive::quota` facade, never an internal package path.

Each local construction creates independent authority; clones share its fixed
table. Fleet definitions explicitly identify a quota-owned namespace/kind and
owner capacity. These are explicit-handle resources, opened by their owning
component during preparation, not per-store Provider/Runnable registrations.
They perform no reset/unlink and have no sweeper or publication loop. Matching
opens retain committed debt; incompatible state is an error. No fixed core
SHM kind is allocated. Edge rejects resources in the runtime fleet and duplicate
resource bindings across limit definitions. Operators own dedicated quota
namespaces; they must not reuse another subsystem's namespace/kind.

The managed provider lifecycle remains mandatory for registered providers;
this explicit store API supplies neither a Provider implementation nor implicit
destructive authority. Configuration checks do structural validation only and
do not open these stores or claim owner rows. Physical/native compatibility is
verified when the serving owner constructs a store. No reload or language
binding is provided in this phase.

This crate answers two related questions:

> How does a Claviron process join an Orbit fleet at the right lifecycle point
> and expose that fleet to other providers?

> How does that process drive Orbit events and cache mutations without leaking
> ring, cursor, or platform-notification mechanics into higher-level crates?

Fleet-coherent ring and L1 mechanics remain in `orbitive::cache`. This crate
owns the Claviron runtime binding and abstract L2 policy.
Concrete File/Redis adapters and operator configuration remain in
`cache`. Metrics aggregation, language host functions, and concrete
domain event definitions do not belong here. The language-neutral
`ApplicationEvent` envelope belongs to `claviron-app`; this crate only provides
the generic typed fleet codec and bridge it uses.

## Public Shape

- `placement::{Scope, Dispatch, Scoped, Dispatched}`: shared placement
  vocabulary owned by Continuo's optional `fleet` feature, re-exported here
  and at the crate root. Continuo has no Orbitive dependency. Scope admits master, worker,
  or any role; standalone satisfies both roles. `Fleet` is a compatibility
  alias for `Any`, not a fourth scope. Dispatch declares each/one/pool selection
  within the eligible set. Continuo filters local runnable entries when the
  provider exposes `as_scoped`, using AppState's process role. Provider lifecycle
  registration remains intact. Scheduling remains a TaskService concern. The
  task crate now re-exports these enums as `TaskScope` and `TaskDispatch`,
  retaining legacy names while keeping timing and execution in TaskService.
- `placement::{OrbitDispatcher, DispatchPolicy, register_dispatcher}`:
  implements Continuo's `Dispatcher<AppState>` contract for runnable `One` and
  `Pool` generations. Registration is explicit and supplies managed lock SHM
  dependencies; server bootstrap installs the same adapter on its Runtime.
- `FleetProvider`: joins the SHM fleet at `priority::EARLY` and exposes its
  handle. It uses the explicit `AppState::fleet_name()` sharing domain,
  independently of the logical process-family instance name. It is a capability
  provider, not a runnable service.
- `OrbitStateExt`: opt-in `state.fleet()` accessor.
- `register_fleet(state)`: inserts the fleet capability and its common managed
  typed-cell tables. Applications do not register cell infrastructure.
- `register(state)`: also inserts the Orbit event registry in supervised mode.
- `FleetEventRegistry`: typed bridge between raw Orbit events and the local
  process event bus.
- `cache::CacheService`: lifecycle owner for one physical Orbit cache
  connection, one mutation cursor, and one inbound driver per process.
- `cache::CacheRuntime`: one named logical store's L1 and optional L2 policy.
- `cache::CacheBacking`: storage-neutral async port implemented by concrete
  adapters in `cache`.
- `counter::Counter`: key-based signed 64-bit fleet counter table.
- `counter::CounterProvider`: optional lifecycle owner for that table.
- `invoke::InvocationBus`: bounded invocation transport over a dedicated ring,
  with runtime-neutral operation names and opaque payload bytes.
- `invoke::InvocationProvider`: optional lifecycle owner that opens the
  already-managed invocation ring without owning executor or handler policy.
- `link::Connection`: one protocol-neutral Link membership and exclusive reader,
  owned by the calling service. Explicit namespace, geometry, name, role and
  incarnation; no HTTP bodies, implicit SHM cleanup or detached task.
- `link::EventConnection`: optional topic/payload forwarding on that connection
  using a supplied event bus, connection-local interest and owner cancellation.
  Plain topics use `Routing::Topic`; envelope owners may select `Routing::Key`.
- `link::transport::{TransportSegment, TransportBodies}`: Claviron adapters over
  `orbitive::link`, fixing the application-link geometry while retaining HTTP
  frames, target policy, and lifecycle here. `transport` remains a public
  re-export of this module to preserve existing consumers and type identity.
- `lock::OrbitLock`: framework-neutral fleet lock handle.
- `lock::Lock`: exact-lease RAII binding for Rust services.
- `lock::{LOCK_STATE_KIND, LOCK_EVENT_RING_KIND}`: the physical kind ids
  reserved by the default Orbit lock layout, exposed so sibling subsystems can
  enforce non-overlap.
- `register_lock(state)`: idempotently registers the default lock state table
  and transition ring as managed `Reuse` / `Preserve` resources before a PHP,
  JavaScript, task, or upstream consumer opens them.
- `service::FleetServiceManager`: fleet command and observation adapter over
  Continuo's process-local `ServiceManager`.
- `service::register(state)`: installs the process-local service command
  listener and, in supervised mode, its typed Orbit bridge plus managed
  per-process snapshot publisher. It delegates to
  `service::ServiceSnapshotPublisher`'s `Register<AppState>` implementation;
  the local manager is a boot dependency, not a registration prerequisite.
- `shm::ManagedShmResource`: a provider-owned physical adapter plus declared
  boot/finalize policy for one SHM resource.
- `shm::ManagedShmResources`: a domain resource set that deliberately cannot
  register as a runtime provider by itself.
- `shm::ManagedShmProvider<P>`: the mandatory registry wrapper that owns boot,
  finalize, and runtime decision recording.
- `shm::OrbitRingResource<T>`: typed physical adapter deriving the conventional
  segment name and exact fleet-aware size from `OrbitTyped` geometry.
- `shm::ReusableOrbitRing<T>` and `register_reusable_ring`: the common managed
  `Reuse` / `Preserve` registration for transient transports and timestamped
  read-model rings.
- `shm::ShmActivitySource`: a bound domain authority for activity evidence;
  supervised Claviron supplies exact presence plus a one-heartbeat-interval
  lane-motion fallback.
- `shm::ShmLifecycleInventory`: registry-visible static descriptors and latest
  successful boot/finalize decisions.
- `shm::register(state, resources)`: the only constructor/registration path
  for the managed wrapper; it registers the inventory at the same boundary.
- `shm::{boot, finalize}`: common policy executors that distinguish physical
  state from domain activity, emit structured decisions, and fail closed
  before destructive operations.
- Selected core primitives re-exported from `orbitive`: `Fleet`, `NodeId`,
  `NetId64`, `OrbitEpoch`, and typed ring identity.
- Selected event types re-exported from `orbitive::events` for the Claviron
  lifecycle bridge.

Configured callers normally use the `cache` facade. Rust callers that
want only framework-neutral primitives use the appropriate feature module on
the `orbitive` facade.

## SHM Kind ID Index

`SHM_KIND_INDEX: UPDATE_WITH_EVERY_RESOURCE_CHANGE`

This is the canonical allocation index for physical SHM kind IDs used by
`claviron`, `claviron-extra`, and `claviron-full`, including the default
Orbitive layouts they consume. Keep the complete index here rather than
allocating from a crate-local list: independently optional crates and downstream
application providers can run in the same fleet and therefore share one
physical namespace.

For owner-only resources, the ID is the `u8` in `/orbit-{fleet}-{kind}-{uid}`.
Explicit group Links retain kinds `195`/`211` but use Orbitive's separate
access-derived namespace; this never widens the host fleet's other resources.
Claviron's `{fleet}` is the
explicit sharing domain reported by `AppState::fleet_name()` and defaults to
`clv`; it is not derived from the application/process instance name. A state
table and a ring with the same kind collide just as two rings do.
`OrbitTyped::KIND`, layout kinds, and custom table kinds all belong in this
index. Frame kinds/opcodes, event topics, node IDs, run IDs, and cell slot IDs
are different namespaces and do not reserve SHM kinds.

The table below records current and retired allocations, checked against the three
repositories and the current Orbitive defaults. Definition links identify
the owner; constants and typed layouts in source remain the executable ABI
authority. Orbitive-owned constants are consumed through its public facade,
not duplicated as new Claviron constants.

| Kind ID | Physical resource | Shape | Owning definition / Claviron binding |
|---|---|---|---|
| `186` | Recoverable text cells v5 with two committed/staging banks, full-width revision and exact owner incarnations | Table + owner identities + per-owner slot pin rows | Orbitive `CELL_TEXT_KIND`; replaces `188`; owner-exit recovery completes pending wakes under exact kernel holds, without touching the last committed text |
| `187` | Handle-pinned scalar cells v4 with a separate parking generation | Table + per-owner slot pin rows | Orbitive `CELL_STATE_KIND`; replaces `189`; logical release precedes physical reuse, cancellation changes the native park word without changing the value version |
| `188` | Retired handle-pinned text cells v4 with a separate parking generation | Table + per-owner slot pin rows | Replaced by `186`; anonymous writer/seqlock publication has no owner-death recovery; retained objects are not migrated, reset or unlinked automatically |
| `189` | Retired handle-pinned scalar cells v3 | Table + per-owner slot pin rows | Replaced by `187`; old peers parked on the value version; retained objects are not migrated, reset or unlinked automatically |
| `190` | Retired handle-pinned text cells v3 | Table + per-owner slot pin rows | Replaced by `188`; old peers parked on the seqlock version; retained objects are not migrated, reset or unlinked automatically |
| `191` | Runnable service snapshots with controller lifecycle, dispatch and actual execution census | Ring, 512-byte payloads | [SERVICE_SNAPSHOT_RING_KIND](src/service/snapshot.rs); replaces `232`; `Reuse` / `Preserve` |
| `192` | Application snapshots with execution descriptions, row version 2 | Ring, 512-byte payloads | `claviron-app` [APPLICATION_SNAPSHOT_RING_KIND](../app/src/fleet/snapshot.rs); replaces `235`; `Reuse` / `Preserve` |
| `193` | Shared byte arena v2 with per-process reader pins, reclaimed only after taking the dead owner's kernel hold | Index + owner rows + byte ring | Orbitive `ARENA_KIND`; [ArenaProvider binding](src/arena.rs); replaces `242` |
| `194` | Arena-addressed cache mutation transport (protocol v3); values stay in kind `193` | Ring | Orbitive `CACHE_MUTATION_RING_KIND`; [cache binding](src/cache/runtime.rs); replaces `200`/`201` |
| `195` | Application-link inbox: one lane per transport participant, fixed-size frames carrying an exchange header and its stream ticket. Many writers, the lane's owner reads; a full lane refuses rather than overwrites, and the next process to join takes back lanes whose kernel hold it can take (see Cleanup). Blocking consumers wait on the lane word; Tokio consumers arm the lane's named descriptor bell and await the reactor while frames remain in SHM. One directory-watcher slot per lane follows the lanes: a process that holds no lane (a master) is woken by the same claims and releases through its slot, held like a lane by a kernel lock (`orbitive::shm::try_hold_watcher`), and never appears in the directory. An interest entry per lane and per watcher follows: the routing keys (application-event domain topics) that slot's process subscribes to, read by `orbitive::link::LinkEvents` so an application event crosses the link only to a side that wants it. Lives in the selected destination fleet (`/orbit-<fleet_name>-195-<uid>`), with application names advertised inside it | Ring, watcher slots after the lanes (orbit-link layout 6) | Orbitive `orbit-link` layout with Claviron `TRANSPORT_LINK_SPEC`; `ORBIT_TRANSPORT_INBOX_CAPACITY` / `ORBIT_TRANSPORT_INBOX_PAYLOAD_CAPACITY` / `ORBIT_TRANSPORT_FLEET_CAPACITY`; [HTTP transport](TRANSPORT.md) |
| `196` | Retired application-link inbox without interest entries (orbit-link layout 5) | Ring | Replaced by `195` when the interest table grew the segment; retained segments are not resized, reset or unlinked automatically |
| `197` | Current worker lifecycle/operation snapshots with master runtime start, monotonic uptime and reload revision | Ring, 171-byte rows on 192-byte payloads | `supervisor` [WORKER_SNAPSHOT_RING_KIND](../supervisor/src/observation/snapshot.rs); replaces kind `240`; `Reuse` / `Preserve` |
| `198` | Auth live sessions and consumed replay IDs; typed BLAKE3, encrypted or explicitly unprotected records; Principals stay process-local | Table, 320-byte slots, two 17-word banks; OAUTH004 with policy ID | Orbitive `orbit-auth` `AUTH_STATE_KIND`; supplied-Fleet backing, preserve across restart, no automatic migration; no Claviron provider binding yet |
| `199` | Retired auth HMAC-only fixed metadata layout | Table, 320-byte slots, two 16-word banks; OAUTH003 | Replaced by typed kind `198`; retained segments are not resized, migrated, reset or unlinked automatically |
| `200` | Retired ring-payload cache mutation transport (protocol v2) | Ring | Replaced by `194`; retained segments are not reset or unlinked automatically |
| `201` | Retired cache payload transport | Ring | Values now live in arena `193`; retained segments are not reset or unlinked automatically |
| `202` | Retired application-link inbox without directory-watcher slots (orbit-link layout 4) | Ring | Replaced by `196` when the watcher table grew the segment; retained segments are not resized, reset or unlinked automatically |
| `203` | Benchmark RSS requests | Ring | `claviron-full` [RSS_REQUEST_RING_KIND](../../../claviron-full/src/apps/benchmark/orbit/rss.rs) |
| `204` | Benchmark RSS responses | Ring | `claviron-full` [RSS_RESPONSE_RING_KIND](../../../claviron-full/src/apps/benchmark/orbit/rss.rs) |
| `205` | Retired upstream origin connection pool v1: fleet live/creating census without a fleet-wide idle counter | Pool table | Replaced by `252`; retained deployed segments are not reset or unlinked automatically |
| `206` | Retired FastCGI fleet connection pool table | Pool table | The `orbit-pool` FCGI integration was removed; retained deployed segments are not reset or unlinked automatically |
| `207` | Retired FastCGI client pool event metrics | Ring | Fleet event aggregation was removed; retained deployed segments are not reset or unlinked automatically |
| `208` | Retired FastCGI fleet exchange control | Stream table | The fleet FCGI exchange was removed; retained deployed segments are not reset or unlinked automatically |
| `209` | Retired FastCGI fleet exchange request payload | Payload arena | The fleet FCGI exchange was removed; retained deployed segments are not reset or unlinked automatically |
| `210` | Retired FastCGI fleet exchange response payload | Payload arena | The fleet FCGI exchange was removed; retained deployed segments are not reset or unlinked automatically |
| `211` | Application-link bodies: per-node lanes of stream slots, two bounded byte rings per slot. One duplex slot per exchange — request body one way, response body the other. Separate from kind `246` because worker coordination wants many narrow lanes and this wants fewer and wider. In the selected destination fleet beside kind `195`, addressed by the same lane index | Header + doorbells + bitmaps + slots + rings in one segment | Orbitive `orbit-link` / `orbit-stream` with Claviron `TRANSPORT_LINK_SPEC`; `ORBIT_TRANSPORT_LANE_CAPACITY` / `ORBIT_TRANSPORT_BUFFER_BYTES`; [HTTP transport](TRANSPORT.md) |
| `212` | Retired upstream worker metrics v1 (58-byte payload) | Ring | Replaced by `213`; retained deployed segments are not reset or unlinked automatically |
| `213` | Upstream worker metrics v2 (74-byte payload) | Ring | `upstream` [UpstreamOrbitMetrics::KIND](../upstream/src/metrics/orbit.rs); `Reuse` / `Preserve` |
| `214` | Embedded PHP worker metrics | Ring | `php-embed` [PhpEmbedOrbitMetrics::KIND](../php-embed/src/metrics/orbit.rs) |
| `215` | PHP FastCGI worker metrics | Ring | `php-fcgi` [FcgiOrbitMetrics::KIND](../php-fcgi/src/metrics/fcgi/orbit.rs) |
| `216` | FPM pool metrics | Ring | `php-fcgi` [FpmPoolOrbitMetrics::KIND](../php-fcgi/src/metrics/fpm/store.rs) |
| `217` | Task service metrics | Ring | `tasks` [TaskServiceOrbitMetrics::KIND](../tasks/src/metrics/store.rs) |
| `218` | Per-task metrics | Ring | `tasks` [TaskOrbitMetrics::KIND](../tasks/src/metrics/store.rs) |
| `219` | Task/role pair metrics | Ring | `tasks` [TaskPairOrbitMetrics::KIND](../tasks/src/metrics/store.rs) |
| `220` | Fleet events, including bridged domain commands/events | Ring | Orbitive `EVENT_RING_KIND`; [fleet event binding](src/event.rs) |
| `221` | Worker runtime heartbeat | Ring | `supervisor` [HEARTBEAT_RING_KIND](../supervisor/src/observation/heartbeat.rs) |
| `222` | Upstream current origin state | Table | `upstream` [UPSTREAM_STATE_KIND](../upstream/src/state/layout.rs); 160-byte slots, managed quiescent reset / preserve |
| `223` | Supervisor process metrics | Ring | `supervisor` [PROCESS_METRICS_RING_KIND](../supervisor/src/observation/metrics.rs) |
| `224` | HTTP worker metrics | Ring | `http` [HttpMetricsRecord::KIND](../http/src/metrics/orbit.rs) |
| `225` | Server worker metrics | Ring | `http` [ServerMetricsRecord::KIND](../http/src/metrics/orbit.rs) |
| `226` | Task inventory/lifecycle snapshots | Ring | `tasks` [TASK_SNAPSHOT_RING_KIND](../tasks/src/snapshot.rs) |
| `227` | Retired text cells v2 without handle pins | Table | Replaced by `190`; retained objects are not migrated, reset or unlinked automatically; see earlier reuse warning below |
| `228` | Authoritative lock state | Table | Orbitive `LOCK_STATE_KIND`; [lock binding](src/lock.rs) |
| `229` | Lock transitions | Ring | Orbitive `LOCK_EVENT_RING_KIND`; [lock binding](src/lock.rs) |
| `230` | Keyed signed counters | Table | Orbitive `COUNTER_STATE_KIND`; [CounterProvider binding](src/counter.rs) |
| `231` | Fleet TLS server sessions | Table | `tls` [TLS_SESSION_STATE_KIND](../tls/src/orbit.rs) |
| `232` | Retired controller-only runnable service snapshots | Ring, 512-byte payloads | Replaced by `191`; retained segments are not resized, reset or unlinked automatically |
| `233` | Retired scalar cells v2 without handle pins | Table | Replaced by `189`; retained objects are not migrated, reset or unlinked automatically |
| `234` | Application events | Ring | [APPLICATION_EVENT_RING_KIND](src/event.rs) / `ApplicationEventLayout` |
| `235` | Retired application snapshots without execution descriptions | Ring, 512-byte payloads | Replaced by `192`; retained segments are not resized, reset or unlinked automatically |
| `236` | Broadcast publication metadata | Ring | `broadcast` [PUBLICATION_RING_KIND](../broadcast/src/fleet.rs) |
| `237` | Broadcast publication payload chunks | Ring | `broadcast` [PAYLOAD_RING_KIND](../broadcast/src/fleet.rs) |
| `238` | Master generation identity | Ring | `supervisor` [MASTER_GENERATION_RING_KIND](../supervisor/src/process/identity.rs) |
| `239` | Worker-owned process presence | Ring | `supervisor` [PRESENCE_RING_KIND](../supervisor/src/observation/presence.rs) |
| `240` | Retired worker lifecycle/operation snapshots without master runtime clocks | Ring, 147-byte rows on 160-byte payloads | Replaced by `197`; retained deployed segments are not resized, reset or unlinked automatically |
| `241` | Generic invocation requests | Ring | Orbitive `INVOCATION_RING_KIND`; [InvocationProvider binding](src/invoke.rs) |
| `242` | Retired byte arena v1 with anonymous pins | Index + byte ring | Replaced by `193`; retained segments are not resized, reset or unlinked automatically |
| `243` | Embedded PHP SAPI pool/thread statistics | Table | `php-embed` [SAPI_STATS_KIND](../php-embed/src/functions/sapi_stats.rs) |
| `244` | Retired private PHP invalidation transport | Ring | Replaced by the inbound-only kind-220 event bridge; [retirement contract](../php-embed/ARCHITECTURE.md). Not available for incompatible reuse; retained segments require stopped-fleet cleanup |
| `245` | Retired upstream DNS snapshot-only v1 | Table | `upstream` [RETIRED_RESOLUTION_KIND](../upstream/src/resolve/table/layout.rs); replaced by unified kind `248`; retained deployed bytes are neither migrated nor reset/unlinked automatically |
| `246` | Fleet byte streams: per-node lanes of stream slots, two bounded byte rings per slot, per-node doorbells and offer bitmaps | Header + doorbells + bitmaps + slots + rings in one segment | Orbitive `orbit-stream` `STREAM_KIND`; `ORBIT_STREAM_LANE_CAPACITY` / `ORBIT_STREAM_BUFFER_BYTES`; [StreamsProvider binding](src/stream.rs), reset when quiescent at boot, and a worker ends what its node's earlier lives held when it opens (`Streams::supersede`); first user: Lua's `claviron.stream` |
| `247` | Retired fleet resource pool: key table (creation budget, members), per-node resource lanes (capacity, reserved/active units, fence), per-node doorbells, interest and claim tables | Header + doorbells + bitmaps + claims + keys + resources in one segment | Orbitive `orbit-pool` `POOL_KIND`. Claviron's opt-in binding (`pool::DefaultPoolTable`, `register_pool::<T>`) was removed 2026-09-24; nothing had ever registered it and this host's fleet holds no segment for it. Listed so the number is not reused blindly |
| `248` | Authoritative upstream DNS membership, reusable named health and native IO ownership | Table | `upstream` [RESOLUTION_KIND](../upstream/src/resolve/table/layout.rs); one revision/bank commit for snapshot and eligibility, fixed canonical cells and per-native process/generation scope gates; managed `Reuse` / `Preserve`; kinds 222 and 245 unchanged |
| `249` | Retired upstream H1 fleet exchange control | Stream table | Belonged to `upstream`'s fleet H1 exchange (`proxy/fleet.rs`), removed with that module; the `orbit::exchange` layer under it followed on 2026-09-24. This host's fleet holds no segment for it. Listed so the number is not reused blindly |
| `250` | Retired upstream H1 fleet exchange request payload | Payload arena | Request payload arena of the removed `upstream` fleet H1 exchange; see `249` |
| `251` | Retired upstream H1 fleet exchange response payload | Payload arena | Response payload arena of the removed `upstream` fleet H1 exchange; see `249` |
| `252` | Retired upstream origin connection pool v2: fleet live/creating budget plus atomically tracked fleet-wide idle capacity | Pool table | Replaced `205`, then went with `upstream`'s `proxy/fleet.rs`; the `orbit::pool` layer under it followed on 2026-09-24. This host's fleet holds no segment for it. Listed so the number is not reused blindly |
| `253` | Log pipeline records: per-node lanes, one producer per lane, one writer per fleet draining every lane. Full ring overwrites; the writer counts the loss from its cursor. Reuse + Preserve | Ring | `logging` [pipeline design](../logging/ARCHITECTURE.md) |
| `254` | Fleet TLS client sessions | Table | Orbitive `orbit-rustls` client session table (`rustls_0_24`); reserved even when the optional integration is disabled |
| `255` | Retired auth prototype: encrypted OAUTH001, then unencrypted fixed-metadata OAUTH002 | Table; OAUTH002 uses 256-byte slots | Replaced first by MAC-authenticated kind `199`, now typed kind `198`; retained segments are not resized, migrated, reset or unlinked automatically; retire outstanding one-use credentials before switching state stores |

`claviron-extra` currently declares no independent physical SHM kind. Its typed
fleet codecs, such as [chat events](../../../claviron-extra/crates/chat/src/event.rs),
reuse the fleet event transport; a new topic is not a new segment allocation.
`claviron-full` adds kinds `203` and `204`. Its
[benchmark status readers](../../../claviron-full/src/apps/benchmark/orbit/status.rs)
also attach to existing cache, event, and heartbeat kinds; these are readers of
the same ABI, not additional allocations.

The runtime `ShmLifecycleInventory` complements this static index: it reports
registered descriptors and decisions for the enabled process, not every kind
reserved across repositories. In particular, `CellsProvider` currently has one
descriptor under scalar kind `187` while also opening text kind `186`; both
physical allocations must remain listed here.

### Existing kind reuse warning: `227`

Before worker snapshots moved to `240`, the supervisor used kind `227` for its
older 64-byte-payload snapshot ring. Orbitive now uses `227` for the text-cell
table. An old object at the same fleet/UID name is therefore not automatically
a text-cell object or disposable stale state. Validate the actual header and
geometry and apply the owning lifecycle/activity policy; never clear it merely
because an older document calls `227` orphaned. This index records the existing
reuse, not approval to reuse retired kinds for new incompatible resources.

### Required workflow for new SHM resources

1. Consult this index **before choosing an ID**, across all three repositories
   and the Orbitive defaults in the dependency version being adopted. Search
   source for `OrbitTyped`, layout kind constants, and custom table segment
   names as well. A gap in the table is not proof that an ID is safe: check
   historical deployments and retired allocations before using it.
2. Reserve one distinct kind per incompatible physical resource. Sharing a kind
   is valid only for intentional adapters/readers of the exact same ABI. A
   different payload capacity, topology, header, or table layout is not a new
   topic or frame kind; document compatibility/migration and use a new SHM kind
   when the layouts cannot coexist safely.
3. **Update this index whenever adding, changing, or retiring a SHM ring/table**,
   and its row under *Cleanup* below,
   including additions in `claviron-extra` or `claviron-full`. Record the numeric
   ID, resource, physical shape, owning definition, and any migration/history
   note. Coordinate the core documentation change with a downstream change;
   do not leave a new downstream allocation unindexed. Keep retirement history
   visible rather than silently making the ID available again.
4. Define typed geometry and the resource-owned lifecycle policy in its owning
   crate. Claviron providers must register through `ManagedShmResources` /
   `ManagedShmProvider`; indexing a resource does not grant reset/unlink authority
   or replace physical validation, activity evidence, or provider ordering.
5. Keep collision/compatibility checks and relevant regression coverage with the
   implementation. Standalone test-only kinds in isolated fleets do not become
   production reservations, but tests must not collide with a live deployment.

## Shared-memory Lifecycle Boundary

`orbitive::shm::ShmRegion::validate_existing` performs the mutation-free
physical check: missing is a normal state, while a present object must be large
enough for the requested mapping. The owning adapter then validates its own
magic, version, geometry, and other semantic header fields.

Claviron policy is explicit and typed:

- `ExistingBootPolicy` decides whether valid state is reused, rejected, reset,
  or recreated;
- `FinalizePolicy` decides whether state is preserved or conditionally cleaned;
- destructive policies require provider-owned `ResourceActivity` evidence;
- `Unknown` activity never authorizes reset, recreation, or unlink;
- the active master role by itself grants no destructive authority.

The lifecycle executor supports multiple resources per provider and records a
decision for each one. The resource-set type does not implement `Provider`;
only `ManagedShmProvider<P>` does, and its construction is private to the
registration function. This prevents a managed resource from being inserted
while silently bypassing the executor or inventory. Domain adapters own any
heartbeat or one-full-interval observation needed to prove quiescence.
Physical invalidity is returned with its original detail and never translated
into an automatic unlink.

Creation coordination is separate from destructive authority. When supervised
boot guarantees that the master finishes provider boot before workers start, a
resource may assign first creation and header initialization to the master;
workers then validate and reuse that initialized object. This prevents
concurrent consumers from observing the interval between `shm_open`, sizing,
and header publication. It does not authorize the master to reset, recreate, or
unlink a compatible existing object: those actions still require the declared
policy and provider-owned activity evidence. Standalone mode performs both the
coordinator and consumer roles in one process.

### Reply publication failure

An admitted HTTP exchange owns its response stream before publishing its reply
head. If the runner cannot publish that head, it resets the response direction,
including when a small buffered body already published FIN. `ReplyWait` checks
its oneshot first and otherwise observes the existing stream's reset-only
signal; data/FIN do not complete a header wait. It removes the correlation on
failure or cancellation. Successful delivery already removes it and needs no
second map lookup. No deadline, polling driver or extra SHM field is introduced.
This preserves terminal failure even when bounded inbox admission refuses a
reply; a refused request remains a pre-execution 503.

## Cleanup: What Is Cleared, By Whom, and What Stays

`SHM_CLEANUP: UPDATE_WITH_EVERY_RESOURCE_CHANGE`

Segments outlive processes, so every resource answers four questions: what a
boot does with an existing segment, what a finalize does, whether the next
process on a node clears what the earlier one left, and what stays behind.
This section is where those answers live; a new resource adds its row with
its index row.

### The mechanisms

- **Quiescent boot reset** (`ExistingBootPolicy::ResetWhenQuiescent`). Only a
  master or standalone process may do it, and only on proof that nobody is
  attached: the supervisor's exact PID and start-stamp presence (kind `239`)
  proves every named process dead; only a lane that has beaten on kind `221`
  without any presence record is then watched for one heartbeat interval. A
  clean restart, where every lane names its dead process, is decided without
  waiting. Workers always
  answer `Active`; `Unknown` fails the boot, or turns the destructive policy
  off where the provider says so. With live peers the segment is reused.
- **Quiescent finalize** (`FinalizePolicy::ResetWhenQuiescent` /
  `UnlinkWhenQuiescent`): the same proof, on the way out.
- **Reuse + Preserve** — `register_reusable_ring` and most tables. Nothing is
  cleared. A ring is safe because its readers start at the head or filter by
  freshness (`max_age`); a table is safe because its slots carry generation
  stamps, so a released id never reaches a reinstalled slot.
- **Successor takeover.** A node id is a role a replacement inherits; its
  incarnation is not. A process that opens a per-node resource ends what
  every other incarnation of its node left there, by the stamp alone and with
  no report. Sound only because the supervisor spawns a replacement after the
  slot's process has exited (restart and rotation wait in `await_slot_exit`;
  recovery runs after the exit). The master's incarnation does not count
  lives, so it never takes over; a quiescent boot covers it.
- **Kernel-held lock**: an owner holds a lock the kernel releases when the
  process dies, however it dies; whoever can take it knows the owner is gone.
  Transport lanes (`195`) use one per lane (`orbitive::shm::try_hold_lane`),
  directory watchers one per watcher slot (`orbitive::shm::try_hold_watcher`),
  and the joiner that takes a lane back ends its streams (`211`) with
  `node_died`, naming the dead incarnation.
- **Death report** (`node_dead`) from a supervisor: defined for the fleet's
  streams (`246`); application-link body streams (`211`) now receive this
  through the supervisor's exact kernel exit observation. Configured target
  fleets are recorded at vhost publication in the master too; recording does
  not join a lane. Recovery maps only existing tables without installing a
  driver or claiming an endpoint. Refused-write reclamation likewise ends
  the dead incarnation's streams while the lane's kernel hold still fences
  replacement. Ordinary routing withdrawal preserves the last ownership
  stamp so a process dying during drain is recoverable.
- **Operator**: `orbit clear` with the server stopped (Rule 5b of the workspace
  instructions).

### Per kind

| kind | boot | finalize | successor | what stays, and why it is harmless |
|---|---|---|---|---|
| `194` cache mutation transport | Reuse | Preserve | — | old frames; cursors start at the head |
| `195` application-link inbox (selected transport fleet, not managed) | `orbitive::link::LinkSegment` creates once and otherwise attaches; a lane is won under its hold by CAS, then cleared and stamped | Claviron releases the lane, then its hold | **the next joiner takes back every lane whose hold it can take**: the kernel let go of it, so its owner is gone | a dead owner's lane and frames until the next process joins the fleet; the lock files beside the segment (removed with it) |
| `211` application-link bodies (selected transport fleet, not managed) | `orbitive::link::LinkBodies` attaches without reset | nothing | join recovery, refused-write recovery, and exact supervisor exit reset only the dead lane/incarnation's sides | unsupervised peers still need reclaim on a refused write or a join |
| `213`–`219`, `223`–`225` metrics | Reuse | Preserve | overwrite | old samples; readers filter by `max_age` |
| `220`/`234` events | Reuse | Preserve | — | old events; the inbound cursor starts at the head |
| `221` heartbeat | Reuse | Preserve | overwrite | a stale head; activity reads lane *motion* |
| `222` upstream state | ResetWhenQuiescent{Reuse}; `Unknown` reuses | Preserve | — | only a live fleet's adopted state |
| `191`, `192`, `226` snapshots | Reuse | Preserve | overwrite | `max_age` filter |
| `235` retired application snapshots | no longer opened | no automatic cleanup | — | old rows retained for older peers; operator cleanup only after the fleet stops |
| `187`+`186` cells | Reuse | Preserve | exact kernel holds fence dead-owner pin reclamation; text owner-exit recovery completes abandoned notification before clearing ownership | values survive restart; released generations remain pinned until old handles drop; text owner row reuse first recovers its old descriptors |
| `188` retired text cells v4 | no longer opened by the recoverable-text protocol | no automatic cleanup | — | old tables remain for older binaries; operator cleanup only after their users stop |
| `189`+`190` retired cells v3 | no longer opened by the separate-parking protocol | no automatic cleanup | — | old tables remain for old binaries; operator cleanup only after their users stop |
| `227`+`233` retired cells | no longer opened by the handle-pinned protocol | no automatic cleanup | — | old tables remain for old binaries; operator cleanup only after their users stop |
| `228` lock state | Reuse | Preserve | — | a dead holder's lease, until its TTL |
| `229` lock transitions | Reuse | Preserve | — | cursor at the head |
| `230` counters | Reuse | Preserve | — | values, on purpose, as for cells |
| `231` TLS sessions | ResetWhenQuiescent{Reuse}, when `cleanup_on_exit` | UnlinkWhenQuiescent | — | nothing after a quiescent exit; otherwise sessions bounded by their TTL |
| `236`/`237` broadcast | Reuse | Preserve | — | cursor at the head |
| `238` master generation | Reuse | Preserve | a new master publishes a higher generation | the head orders masters |
| `239` presence | Reuse | Preserve | overwrite | a stale PID; liveness checks PID and start stamp |
| `197` worker snapshots | Reuse | Preserve | overwrite; the master publishes the exit | `max_age` filter |
| `240` retired worker snapshots | no longer opened | no automatic cleanup | — | old geometry retained for older peers; operator cleanup only after the fleet stops |
| `241` invocations | Reuse | Preserve | — | subscriptions start at the head |
| `193` arena | Reuse; ResetWhenQuiescent{Reuse} with the files `arena boot: reset` | Preserve | — | records, on purpose; a crashed writer leaves at most one unpublished record |
| `243` PHP SAPI stats | Reuse | Preserve | a worker overwrites its slot | a dead worker's slot, reported stale after 2 s |
| `246` fleet streams | ResetWhenQuiescent{Reuse} (`reset_all` + new epoch) | Preserve | **a worker calls `Streams::supersede` when it opens the table** | a slot never respawned (scale-down) keeps its sides until a quiescent boot; the master's node-0 sides survive a restart that adopts live workers |
| `248` upstream DNS | Reuse | Preserve | — | dead native owners are reclaimed at publication (PID + start stamp) |
| `253` log records | Reuse | Preserve | overwrite | cursor at the head; frames written while no writer runs are dropped |
| `203`/`204` benchmark RSS (`claviron-full`) | opened directly, not managed: never cleared | nothing | — | cursors start at the head |

### Files outside shared memory

On macOS and FreeBSD a node whose stream table (`246`, `211`) answers its
doorbell on Tokio binds a socket file
`/tmp/orbit-bell-<uid>/<segment>.<node>`. It is removed when the process's last
table handle drops; a killed process leaves it. A successor on the node removes
it before binding, and a ring to a dead socket fails silently, so a left file
costs nothing. The directory is never removed. On Linux the name is abstract
and nothing is left.

Lock files sit in the per-uid lock directory (`$XDG_RUNTIME_DIR/orbit-<uid>`
or `/tmp/orbit-<uid>`): one beside a locked region, and one per transport lane
ever held (`<segment>.lane<n>.lock`). An unlocked one is harmless and reused.
Unlinking the segment, from code or with `orbit clear`, removes them with it.

## Boot Contract

Every process that registers the provider joins the same fleet during boot:

```text
registry insert FleetProvider
  -> boot priority EARLY
  -> Fleet::join_shm_as("clv", fleet_capacity, node_id)
  -> store Arc<Fleet>

common typed-cell infrastructure
  -> CellsProvider at EARLY, after FleetProvider
  -> validate/open/reuse the fleet's scalar and text tables
  -> bind application Orbital declarations before normal-priority provider boot
  -> any setup/allocation failure rejects startup before runnables start

master/worker
  -> managed event SHM companion after FleetProvider
  -> validate/create/reuse and record Reuse + Preserve policy
  -> register FleetEventRegistry after the managed companion
  -> capture a new inbound cursor at the current head

standalone
  -> expose Fleet only; local events remain on state.events()

optional counter registration
  -> CounterProvider after FleetProvider
  -> boot reuses the table: values are kept across a restart
  -> finalize preserves the table

optional arena registration
  -> ArenaProvider after FleetProvider, managed kind 193
  -> quiescent master/standalone boot empties the index; a live fleet is reused
  -> finalize preserves the segment

optional invocation registration
  -> managed kind 241 companion after FleetProvider
  -> validate/create/reuse with Reuse + Preserve policy
  -> InvocationProvider opens after the companion
  -> runtimes own payload codecs, handler placement and execution

fleet service management
  -> insert Continuo ServiceManager
  -> register typed Orbit command bridge
  -> managed snapshot SHM companion validates/creates/reuses the ring
  -> insert ServiceSnapshotPublisher
  -> retained batches are accepted only through current membership/freshness
```

The short fleet name is required by macOS POSIX SHM name limits. Orbit builds
segments as `/orbit-{fleet}-{kind}-{uid}`.

Every peer must agree on one immutable `fleet_capacity`. This is physical SHM
lane geometry, not a claim that every reserved lane currently has a live
writer. It is built in: `CLAVIRON_FLEET_CAPACITY` in the deployment's
`.cargo/config.toml`, beside the ring geometry it must agree with, and 17
seats when the build pins nothing. There is no runtime option for it.
Standalone uses the same compiled geometry; its one serving process does not
reduce the physical lane count to one.

The current single-process-family adapter assigns node zero to
standalone/master and initially maps each worker id to the equal numeric fleet
node id. `FleetNodeId` and `WorkerId` remain different runtime types so a future
fleet directory can allocate unique lanes to independently launched Claviron
process families. Read-only attachments need no writer identity; writable
external members will require that allocator and are not admitted by the
current adapter.

Every `OrbitTyped::KIND` still declares its own per-lane `RingSpec`. Shared
topology has one lane; per-node topology reserves one writer lane per fleet
capacity slot in the same SHM segment. A ring's memory is allocated only when
that kind is opened, and every peer must agree on the kind, spec, and immutable
fleet capacity. Liveness/status readers scan the physical capacity and then
apply their domain freshness rules; capacity itself is never membership
evidence.

Orbit locks use a fixed authoritative current-state table plus a notified
transition ring. Table capacity bounds simultaneous resident lock keys, not
retained changes. Locks are independent from cache values and cache clearing.

## Invocation Transport

### Targeted calls

`invoke::Client` is a boot-registered map of logical targets to `Route`
implementations. The caller supplies a target, operation and opaque bytes;
placement and transport belong to registration. `CallError`/`CallResult` remain
separate from the existing ring's `Error`/`Result` exports. No implicit local,
Link or ring fallback is selected.

`link::invoke` is the direct same-host Link adapter. Each endpoint consumes
one `Connection` reader, optionally retaining the existing event bridge through
`EventConnection::invocations`. It uses the owner's caller-supplied `LinkSpec`,
not HTTP transport constants, and opens no invocation ring or HTTP body table.
The owner/group permission contract remains unchanged. A logical Link service
must resolve to exactly one live lane; multiple matches are an explicit error,
not an implicit load-balancing or failover policy.

The inbox protocol uses downstream tag `0x49`, version 1, distinct from HTTP
tags 1..5 and Link-reserved tags F0..FF. A request carries both endpoint
incarnations, a never-reused local call sequence, a result limit and a Link
stream ticket. The caller reserves side A before publication. The target
validates its destination and the ticket's origin/kind/side and claims side B
before invoking domain code. Duplicate ticket claims cannot execute twice.
The single bounded result record is written to that reserved stream, not to
the caller's inbox; events cannot consume response capacity. The record retains
both endpoint stamps and the call sequence. This is not an HTTP exchange.

Pending calls, accepted callback futures and request/result sizes are explicit
per-endpoint limits. Stream slots and per-direction byte buffers remain part of
the owner-chosen Link geometry. Results use native stream backpressure; cancelled
readers affect only their call. A malformed frame never enters a handler.
When callback capacity is full, the reader holds its current observation and
stops consuming until a slot opens; the bounded inbox provides backpressure.
Callbacks must yield or use a bounded blocking executor for synchronous work.

Shutdown stops new local calls but retains the reader and lane while accepted
work and pending calls drain. New inbound calls receive Closing. Only after
drain does the driver withdraw the lane. There is no timeout, timer, retry loop
or event rebroadcast of calls. Directory changes can report a missing peer;
abrupt death is not inferred from elapsed time. Local cancellation is neither
remote cancellation nor rollback. Missing results are outcome-unknown, not safe
replay. Domain commit IDs, revisions, authorization and outcome queries remain
the handler's responsibility. Group SHM writer access is not caller identity.

### Append-only invocation ring

`invoke::InvocationBus` is a bounded, append-only instruction transport. Its
dedicated kind and compile-time `ORBIT_INVOKE_RING_CAPACITY` /
`ORBIT_INVOKE_RING_PAYLOAD_CAPACITY` geometry isolate invocation retention and
readiness from broadcast application events. A committed frame receives an
`InvocationId` backed by its ring `NetId64`; the embedded node identifies the
submitter. The envelope carries only a validated operation string and opaque
payload bytes; the frame version carries submission time as an `OrbitEpoch`.

Publication proves only that a frame was committed. The ring is not a durable
queue and provides no acknowledgement, claim, retry, response or exactly-once
guarantee. Cursors report overwritten or invalid frames as lag. Runtime
adapters own codecs and executor selection; PHP, JavaScript, application
containers and Tokio task policy do not enter this module. A future claim or
result protocol must add its own authoritative state/ring instead of changing
the meaning of this request ring.

Kind `241` is registered through a managed `OrbitRingResource<T>` companion
with `Reuse` at boot and `Preserve` at finalize. A new subscription starts at
the current head, while an adopted consumer retains its existing mapping and
cursor. Resetting the ring could neither acknowledge nor cancel outstanding
work, so provider boot has no reset authority.

Completion remains an application concern. A consumer that needs to announce
an outcome may publish a normal application event such as `TaskCompleted` and
carry the `InvocationId` in its payload. That correlation does not turn the
invocation transport into a response channel or make completion mandatory.

## Fleet Heartbeat

`FleetProvider` does not publish heartbeats and does not implement `Runnable`.
`orbitive::Fleet` exposes only generic typed operations. Heartbeat records,
cadence, freshness, and process policy belong to `supervisor`.

The supervisor correlates each worker node with its tracked child PID, uses the
Orbit heartbeat as runtime liveness, and owns TERM/KILL escalation and restart
policy. A heartbeat is not HTTP application readiness.

Worker boot, reload, and shutdown completion use distinct supervisor-owned
typed events over this crate's ordinary Orbit event transport. The supervisor
has no UDS transport. Its internal heartbeat observer reads the dedicated
heartbeat ring; process ownership and control policy stay with the supervisor.
The local `ProcessStarted` milestone is
not bridged: the supervisor worker adapter translates it into an identity-bearing
`WorkerStarted` envelope, which is never decoded back into a local startup.

## Downstream Ordering

Providers that require Fleet declare their dependency normally:

```rust
fn order(&self) -> ProviderOrder {
    ProviderOrder::new().after::<FleetProvider>()
}
```

They resolve the handle during `boot()` or `Runnable::run()`. `FleetProvider`
is the canonical per-process owner. Consumers may clone its `Arc<Fleet>` but
must not create another Orbit registry or cache an optional Fleet merely to
wait for boot.

### Managed typed-cell constructor

`claviron::orbit::Orbital::<T>::new(initial)` declares a value;
`Orbital::<u64>::default()` declares zero without caller-owned setup.
`register_fleet` automatically installs `CellsProvider` at `EARLY`, ordered
after `FleetProvider`. During its managed startup it opens the common tables
and binds pending declarations. Applications need no cell-specific `boot`,
`order`, `OnceLock` or `finalize` implementation. Infrastructure and pending
allocation errors reject startup before application execution.

The process-default constructor binds to one managed owner. Its weak binding
does not retain that owner's runtime, and another live owner cannot silently
replace it. Provider teardown drops the binding's availability when the owner
is dropped; preserving the physical SHM remains the existing lifecycle policy.
Declarations and pending entries do not retain the runtime or its services.
Each value starts with its typed initial value; before binding, `load` returns
that value, while mutations fail with `CellError::NotPrepared` rather than
pretending an unshared update succeeded. `None` is the infrastructure handle,
not the number. After binding, a value operation reads its local ArcSwap handle
and calls Orbitive directly: no registry lookup, process-default mutex, fleet
join, table allocation or background loop occurs on that path.

Construction in an already-running runtime binds immediately. `try_new(initial)`
reports an unavailable runtime or exhausted table immediately; infallible
`new`/`Default` retain an allocation error for subsequent operations. `CellError`
distinguishes unavailable infrastructure, allocation failure and raw value
errors (including stale generations and checked arithmetic overflow). Errors
never become zero after a prepared handle has gone stale.

Clone shares one ownership object and id. The last managed clone retires its
slot automatically; `release` explicitly retires it earlier. Low-level holders
opened from its id borrow this lifetime and receive Stale when it ends. No
per-value runtime service or registration/finalization callback is required.

The low-level `claviron::orbit::Cells` and `claviron::orbit::cell::Orbital<T>`
remain available for explicit-handle composition. PHP, Lua and JavaScript
adapters retain those explicit handles and do not change allocation, id,
serialization or explicit release semantics. No SHM kind, geometry or wait
policy changes; values are slots in the fleet's common tables, not new rings.

## Orbit Event Bridge

### Protocol-neutral Link ownership

`link` is the Claviron lifetime binding over Orbitive's native Link, not a new
transport or event bus. `Connection` owns one `Joined` kernel hold and one
`InboxReceiver`. A failed reader bind withdraws the claimed lane. Withdrawal
stops new admission and removes the local reader before the ownership hold is
released. Drop never resets or unlinks SHM. A raw protocol owner drains accepted
work before dropping its connection; withdrawal itself does not prove completion.

`Connection::events` binds the supplied SHM-backed `FleetEventBus` through
Orbitive's existing `LinkEvents`. It allocates no event ring or HTTP body table.
The event bus's publisher node remains the host's, not necessarily the Link lane
index. `EventConnection::run_until` drives the bridge and exclusive reader in
the caller's future; it spawns no task and installs no interval or signal handler.
The event bus must support native ring readiness; an in-memory bus or unsupported
native platform fails the driver rather than activating a polling fallback.
Subscription guards update connection-local interest. Accepted handlers are
awaited on cancellation or driver failure, including when withdrawal reports an
error. The lane remains held through that drain. Ordinary queued frames are
not an accepted-work guarantee and remain subject to protocol resynchronization.

Received events are republished into the supplied ring once by `LinkEvents` and
handed to this process's handler once. A caller must not also deliver that own
publication as a second local event. Topics/payloads remain opaque; exact topic
checks, authorization, codecs, revision and acknowledgement belong to the
protocol owner. No universal ApplicationEvent envelope is introduced.

The existing application/HTTP transport is nested under `link::transport`,
including its existing ApplicationEvent bridge and routing. Its physical layout,
frame bytes and public `transport` path are unchanged. Existing HTTP readers
still own their lanes; the new generic reader must never be attached as a second
reader of a running application lane. Common interest ownership lives directly
under `link` and is reused by the application adapter.

No SHM kind is allocated by the generic API. The caller supplies a compatible
deployment-owned `LinkSpec`. `Access::OwnerOnly` remains the default of every
existing constructor. `Connection::open_with_access` and transport's explicit
access constructors forward `Access::Group(GroupAccess)` to Orbitive's dedicated
group namespace. Only that Link's inbox, streams, lane/watcher locks and wake
paths are shared. No Fleet permissions or ring constructors are changed.
The owner provisions first and peers attach; no private fallback is permitted.
Group writers share trust in Link state, not a cryptographically authenticated
source identity. Scoping does not grant capabilities or file access.

`TransportBodies::for_segment` derives geometry and access from the already
selected inbox. TransportClient uses it for all new joins. Reclamation and
existing-only management inspection retain the same access. TransportService
accepts immutable per-destination access registration before its first client
opens; registration maps nothing. Unregistered destinations stay private.
Hosts supply the same policy in every participating process. This phase adds
no YAML/global policy, credential switching, unlink/reset or reload mechanism.

### `NO_IMPLICIT_TERMINATION: EVENT_SHM_SURVIVES_MASTER_REPLACEMENT`

The event segment is a fleet resource, not one master's disposable boot
scratch space. A replacement master must join the existing segment because a
live adopted worker may still publish accepted-work progress or completion.
Master boot therefore creates neither reset nor unlink authority.

Each fleet/application event ring is registered first as a typed
`OrbitRingResource<T>` companion with `Reuse` at boot and `Preserve` at
finalize. `EventRegistry<L>` is explicitly ordered after that managed wrapper,
so its runnable transport cannot bypass physical validation or the runtime
inventory. The companion uses an internal marker with the exact same kind and
geometry as Orbitive's event record; Orbitive's ring header remains the
semantic ABI authority.

`orbitive::events::FleetEventBus` carries raw topic strings and byte payloads.
`FleetEventRegistry` adds the Claviron codec boundary:

```rust
trait FleetEventCodec: Event {
    const TOPIC: &'static str;
    fn encode_orbit(&self) -> Vec<u8>;
    fn decode_orbit(bytes: &[u8]) -> Result<Self, String>;
}
```

`register_bridge::<E>` links local emits to Orbit publication and inbound Orbit
frames to local typed emits. All event types share one event segment with one
writer lane per fleet node. There is no per-event SHM path and no total order
claimed across node lanes.

`register_inbound::<E>` installs only peer-frame decoding into local typed
events, without an outbound callback or payload echo-suppression queue. Use it
with synchronous `publish_raw` producers whose local operation has already
completed, such as PHP cache invalidation intercepts. Registration is idempotent
for the same type and mode; conflicting topics or forwarding modes are rejected.

Bridge registration subscribes synchronously but starts no task. It installs
one callback per selected type in AppState's local `EventDispatcher`; all
outbound callbacks share that dispatcher's managed runnable. Registration must
precede runnable submission and returns an error when a new listener is too
late or another type already owns the topic. Re-registering a type is a no-op.
Local and decoded Orbit events reach the same local subscribers only for
explicitly bridged types; implementing a codec alone enables no forwarding.

Local emits are forwarded asynchronously; returning from `emit` is not
proof of an Orbit write. `publish_raw` performs the ring write synchronously,
without waiting for peer consumption. Terminal worker notifications use this
direct path after managed teardown, even though the worker's inbound poller
has stopped on cancellation. The bus/Fleet handles remain available through
provider finalization. No local `ProcessExit` event or arbitrary sleep is needed.

`claviron-app::ApplicationEvent` is a deliberately generic bridged event. Its static
outer Orbit topic identifies the codec; its validated inner topic is the
application-level routing key. It carries no audience field. Subscriptions are
the audience: every PHP, JavaScript, or Rust application execution chooses the
inner topics it accepts. The envelope remains broadcast, not a queue or
consumer group. Framework adapters map their own typed events (for example a
Laravel `UserLoggedIn`) to stable inner topic names and back.

The bridge suppresses the immediate echo of an event it decoded before
emitting it locally. This prevents bidirectional registrations from
republishing the same payload forever.

Link connectivity itself is observed locally from the opposite-side lane
directory (`EdgeConnected` / `ApplicationConnected`), not discovered with
fleet-broadcast ApplicationEvent questions and replies. The application
adapter exposes those direct observations to guests without ring publication;
one link's introductions must not reach other workers' or other links' guests.

Linux, FreeBSD, and macOS 14.4 or later wait through the ring's native shared
generation readiness bridge and a process-local fd. Readiness only says the
ring changed; the registry drains the fd and bulk-polls with its own cursor.
Older macOS releases and other targets use a 10 ms fallback interval.

Boot only prepares the transport: native targets prepare readiness, then every
process captures its inbound cursor at the current ring head. Neither master
nor worker resets or unlinks the event ring. Master boot is not proof of fleet
quiescence because an adopted worker may still be publishing. Starting the new
cursor at the head prevents stale history from being replayed while preserving
the same transport for existing peers. The cursor is captured before worker
spawning, not on first poll, so events published between boot and run are not
skipped.

`EventRegistry<L>` explicitly exposes `Scope::Any` through `as_scoped`: each
registered fleet or application ring needs its process-local inbound driver in
master as well as worker. Provider boot prepares transport, but cannot process
worker startup acknowledgements without runnable admission. This does not
change the default worker scope of application services or register a ring in
a process that does not use it.

`FleetEventRegistry` exposes one inbound `Runnable`. It begins polling only
when Runtime submits runnables after provider boot, and stops on its current
service-generation cancellation. The cursor is returned to the registry when
that generation ends, so a targeted service restart resumes without skipping
events retained while the poller was stopped. Runtime joins/aborts that loop;
native readiness failures return
as runnable errors instead of silently ending a detached task. It stays active
through master fleet draining, which precedes global cancellation. Outbound
`publish_raw` remains available after the inbound runnable ends. Standalone
does not open the event ring because there is no peer process to bridge.

## Fleet Runnable Service Management

Continuo owns every process-local runnable generation, cancellation token, and
`ServiceSnapshot`. `service::FleetServiceManager` does not copy that state. It
publishes typed start, stop, reload, and restart commands through the ordinary
Orbit event bridge; each process applies a command to its own registered
Continuo manager. A service missing from one process is simply not placed
there. Provider registration is therefore the sole placement source of truth;
there is no services YAML table or second enable flag.

Each process publishes complete service batches to a per-node ring under Orbit
kind `191`. A batch carries controller lifecycle, dispatch, active execution
count, provider-owned start policy, generation, reload capability, completed
reload revision, and bounded last run/reload
errors. Readers accept only fresh, complete batches and join them by service
name. `list()` returns that complete observation; `status(name)` narrows the
same snapshot to one service without creating another state source.
`Running` is the controller lifecycle, not evidence that this worker holds a
dispatch slot. Continuo counts each actual runnable future separately, including
draining futures until they return. A running `One`/`Pool` controller with zero
executions is waiting; a positive count identifies executing members. This is
execution evidence, not a universal protocol-readiness check. Kind `232` remains
retired so older processes cannot misread the expanded row shape.

The default publisher cadence is one second and the default maximum observation age is five
seconds. `ServiceSnapshotPublisher` owns `config::OrbitConfig` through Congif's
`Configurable` contract and reads `orbit.yaml` from `AppState::config_dir()`:

```yaml
service_snapshots:
  publish_interval: 1s
  max_age: 5s
```

Both durations must be nonzero and the publication interval must fit the runtime
clock. Unknown keys fail preparation. Missing fields use the documented defaults;
a missing file retains the policy supplied to `register_with_policy` (or those
defaults for ordinary registration). A present file replaces that registration
policy. Registration is idempotent: the first registration owns the fallback.
The config reader uses the process's frozen environment, as other Claviron
providers do. Physical SHM geometry and domain-owned cache/supervisor policies
are not part of this document.

Congif prepares and validates before boot. Manual reload applies the prepared
snapshot and notifies the existing publisher through a watch channel; it does
not add a polling loop or restart the service. A cadence change schedules the
next publication one newly declared interval from its observation. Changing
only freshness leaves the current cadence intact, and reapplying an unchanged
policy sends no wake. Snapshot collection reads the same active policy. There
is no automatic file observer. Standalone has no snapshot publisher/config
owner because it observes its local service manager directly; an explicitly
registered dispatcher may still own dispatch configuration there.

`FleetMembership` is a dynamically bound process-owner capability shared by
service and user-application snapshot collectors, not a
service registry. The supervisor binds its fresh authoritative worker-table
reader: master plus nonterminal workers are expected; terminal, reaped slots
are excluded even while their last service batch is still fresh. Collection
reads exactly these node identities and counts only matching fresh reports.
It never equates desired worker count or allocated capacity with live membership.
Missing/stale ownership data fails closed; missing service data from an expected
member remains an incomplete snapshot. Without a bound owner the conservative
configured membership is retained, never reduced merely because a publisher is
silent. Standalone continues to use its single local observation. This port
keeps supervisor process lifecycle out of Orbit and needs no gRPC/CLI exception.

`service::current_members` is the strict reusable read boundary for resize-aware
consumers (task inventory/budget, upstream probe handoff, SAPI statistics).
It resolves the bound owner on each read and validates roles, capacity, unique
nodes, master and current-process presence. Unlike the legacy service-manager
fallback above, it requires a process owner in supervised mode and never uses
a retained worker's bootstrap count when observation is unavailable. Standalone
returns its one local member. Membership is not a healthy/ready-worker count.

Fleet commands acknowledge validation and publication, not remote completion.
Management clients prove convergence from snapshots: start reaches running,
stop reaches a non-running terminal state, restart advances generation, and
reload advances reload revision with no latest reload error. Registration runs
before configuration and boot; the Orbit event bridge must already be registered
in supervised mode. `runtime.manager()` may be inserted afterwards, before the
provider lifecycle runs. Both local control and snapshot publication declare
that manager dependency. Standalone mode feeds the same command into its process-local event
listener and opens no Orbit service command or snapshot transport, preserving
the accepted-then-observed contract even when the target is the HTTPS server
carrying the RPC.

`FLEET_CONTINUITY` protects the command listener, inbound Orbit bridge,
and snapshot publisher from fleet-wide operations that would remove their
own recovery channel. The remote ingress adapter protects the actual transport
carrying its request; Orbit does not assume that management rides `HttpsServer`.
An independent management listener can therefore stop/restart web transport.
`WorkerSnapshotPublisher` cannot be stopped through
fleet service control because its process-membership reports are now required
to target and recover services; its managed restart remains available.
Use another ingress or rolling worker restart to replace the listener carrying
a management request. Restart remains available for unrelated transports.

## Orbit Cache Binding

`cache::CacheService` wraps one `orbitive::cache::Cache` connection per process.
It owns the native Linux/FreeBSD/macOS 14.4+ readiness loop, one mutation
cursor, and the logical-store registry. Older macOS releases and targets
without native readiness use a 10 ms fallback loop. Readiness is installed
before the driver's first poll so a mutation committed at startup cannot be
stranded between that poll and the wait.

The mutation ring is a managed `OrbitRingResource<T>` using `Reuse` at boot
and `Preserve` at finalize. The existing `ArenaProvider` supplies the shared
byte store, also preserved by default. `CacheService` starts after physical
validation and creates metadata-only L1s with a current-head mutation cursor.
The ring carries arena addresses, revisions and TTL, never cache value bodies.
It is transient coherence traffic, not a durable keyed value snapshot.

Each registered `cache::CacheRuntime` wraps one named `orbitive::cache::Store`.
Clones share only that store's bounded L1 and optional backing. Multiple
logical stores share the service's arena, mutation ring and inbound loop. Every mutation is
addressed by `(store, key)`, so equal keys in different stores remain
independent and `Reset` is store-scoped.

`CacheService::open_store` may reopen an already registered logical name when
its L1 capacity is unchanged. This lets an application-layer cache catalog
disable and later re-enable a store without constructing another service or
listener. The service retains the logical runtime while it is hidden from that
catalog; changing its capacity or backing identity remains a restart-level
topology change.

An optional `CacheBacking` supplies L2 without teaching this crate about File,
Redis, connection pools, or config files:

```text
get:    L1 hit -> return
        L1 miss -> backing get

put:    validate Orbit bounds -> backing put -> publish Orbit Put
forget: validate Orbit key    -> backing forget -> publish Orbit Delete
clear:  backing clear         -> publish Orbit Reset
```

Validation happens before an L2 write. L2 mutation completes before Orbit
publication, so a peer that receives the mutation cannot fall through to an
older backing value. A backing store used for expiring values must preserve
TTL.

If mutation history is lost, the missing mutation's store is unknowable because
the connection cursor is shared. The service therefore disables every
registered L1. It advances to the current ring heads only when every logical
store has a backing, then resumes with empty coherent L1s whose ordinary misses
fall through to their respective L2s.

Typed cache value encoding is owned by `claviron-cache::CacheRepository`.
This crate and `orbitive::cache` remain byte-oriented and serializer-neutral.
`CacheRuntime::get`, `get_sync`, and `CacheBacking::get` return immutable owning
`bytes::Bytes`. An L1 hit takes an owner-aware pin on the exact immutable
arena generation; the returned Bytes owns that pin and the mapping. No value
body is copied into L1. TTL is checked before a new pin; Delete and Reset
invalidate metadata, while already returned snapshots remain valid.
Overwritten or evicted arena addresses are honest misses. L1 eviction drops
only metadata, not a long-lived arena pin. Encoding and checked archive views
remain the responsibility of `claviron-cache`.

Lock mechanics live in the independent `orbitive::lock` primitive. Cache stores may
adapt that primitive for framework lock-provider contracts, but cache value
mutation and lock state never share keys, reset behavior, or storage.

## Orbit Arena Binding

`arena::ArenaProvider` opens `orbitive::arena::Arena`, the fleet's one byte
store: keyed and addressed records over a ring of bytes in a single kind-193
segment. A record is an identity body, an optional encoded body with an
opaque tag, and two validators the caller reads back. The arena stores bytes
and knows nothing else; the static file service is its first user and keeps
file paths, MIME types and HTTP on its own side of the boundary.

The arena is shared state, not coherence traffic: `orbit-cache` keeps
metadata-only L1s and carries addressed mutations between them, while the arena
holds each immutable value body once and every process reads it in place. A hit borrows
the record, pinning its slot until the borrow is dropped; a `bytes::Bytes`
built over that borrow serves a response without a copy.

There is no expiry. A put writes at the ring's cursor and evicts whatever
the bytes land on, except a record a reader is pinning: the cursor moves
past it. Readers take no lock. A writer holds the region's process lock
(`flock`, released by the kernel on process death) for the put, so a crashed
writer leaves at most one unpublished record. Lookups are open addressing;
retired slots preserve probe chains. An addressed record includes its slot
generation, so index reuse never resolves an old address to a new value.

Pins belong to per-process reader rows; the row count follows fleet capacity.
A process holds a kernel lane lock for the lifetime of its mapping. Writers
recover abandoned pins under allocation pressure only if they can take that
lock; no heartbeat, PID reuse heuristic, polling interval or timeout decides
whether a reader has died. Healthy readers keep their pins. Every local
consumer of the same arena shares its mapping and owner row.

`reset` empties the index and bumps a generation. Compatible state is preserved
across boot by default. The owning domain may instead request a reset after
quiescence proof; a process joining a live fleet always reuses what the others
filled. Finalize preserves the segment.

Geometry is compile-time like the other tables: `ORBIT_ARENA_SLOTS` and
`ORBIT_ARENA_BYTES` in the application's `.cargo/config.toml`, checked in
the segment header by every peer. A record is refused above a quarter of the
ring, and the file service asserts at build time that its small-body
threshold fits twice over. What no build can check is the pins: every
response in flight holds its record's bytes, on every node at once, so the
ring has to hold the working set plus `nodes × responses in flight × record`,
where the fleet capacity bounds the nodes and the live worker count is the
usual case, or a put finds no unpinned room. That case is `Error::Pinned`, logged and
served from the bytes just read; sizing is the deployment's to state, not
the library's to know.

## Keyed Counters

`counter::Counter` is deliberately a Claviron-owned primitive for now. It uses
one fixed-capacity authoritative SHM table under kind `230`; it does not write
cache mutations, generic events, or a counter history ring.

The table has 1,024 slots and accepts keys up to 240 bytes. A key is installed
once per runtime generation under a short process-recoverable structural lock.
After publication its key bytes and slot address remain immutable, so `get`,
`increment`, `decrement`, and per-key `reset` operate directly on the slot's
shared `AtomicI64`. Per-key reset installs a missing key or sets an existing
value to zero and retains the slot; there is intentionally no runtime key
deletion/reuse race.

Missing keys begin at zero when incremented or decremented. Amounts are
non-negative signed integers, results may be negative, and checked updates
reject `i64` overflow without changing the previous value. Decimal scale is a
caller contract: the counter stores integer units only.

`register_counter(state)` is opt-in and also ensures `FleetProvider` is
registered. `CounterProvider` is the mandatory managed wrapper for the table.
Every process attaches and every boot reuses the table, so counter values are
kept across a restart, as cell values are; generation stamps keep a released
id from reaching a reinstalled slot. Finalization preserves the mapping and
values, so an adopted worker is never mutated underneath itself.

## Orbit Lock Binding

`lock::OrbitLock` performs one atomic operation per method call. It has no
background task, retry loop, or blocking policy. Consumers that want to wait
use the notified transition ring or their own bounded retry policy.

The authoritative state table and transition ring are both lifecycle-visible
and retained across master replacement. Lease expiry plus fencing owns stale
lock state; clearing the table at boot would invalidate live tenures. New
transition consumers begin at the current head, so retained notification bytes
do not replay as new work.

`lock::Lock` is the Claviron Rust RAII binding. It stores an exact lease and
releases that lease on drop, so an expired holder can never delete a successor.
PHP/Laravel integrations use `OrbitLock` directly: Laravel owns owner-token
generation, blocked-attempt sleeping, callbacks, and `finally` release.

## Runnable Dispatch Ownership

[INCARNATION_MASKING](../../docs/design/INCARNATION_MASKING.md) applies at this
boundary too: service-slot keys remain stable while exact lease owners change.
Applications declare placement intent, not the PID that must satisfy it.

Continuo owns the managed controller generation, cancellation, status and local
service commands. `placement::OrbitDispatcher` is a registered capability,
not another runnable or service manager. It receives `ServiceExecution` from
Continuo and owns exact per-slot leases in the existing lock table/ring
(kinds `228`/`229`); no additional SHM kind is allocated. TaskService's existing
task controllers are unchanged: service dispatch does not introduce task
timing, retry or schedule semantics into providers.

Claviron's server bootstrap registers `OrbitDispatcher` as shared infrastructure
and installs the same registry instance on Runtime before configuration and
provider boot. Applications and extensions using that bootstrap do not register
their own dispatcher. Services still declare their own dispatch policies.

`OrbitDispatcher::register` is idempotent: an existing registry instance is reused.
Registration installs the managed lock dependencies in supervised mode; it does
not read files, open SHM or acquire leases. Direct Continuo hosts that bypass
Claviron's server bootstrap explicitly register the adapter and pass it to
`Runtime::with_dispatcher`. Code-owned composition can use
`OrbitDispatcher::new()` plus `register_dispatcher`, which rejects a duplicate
registration.

The component implements `Scoped` and `Dispatched`, exposes `as_dispatched`,
and supplies its policy through `Dispatched::dispatch_policy()`. The existing
`DispatchPolicy` data type is owned by Continuo and re-exported here; it contains
no Orbit handle, SHM geometry or lock behavior. An owning service may use it in
its existing config, for example under a listener:

```yaml
listener:
  scope: workers
  dispatch: pool
  dispatch_policy:
    capacity: 3
    lease_ttl: 20s
    renewal_interval: 5s
```

These are example declarations, not compiled defaults. Each needs no policy;
One/Pool without one fail dispatch validation rather than falling back to Each.
Capacity and both durations are explicit, renewal is nonzero and shorter than
the lease, and Orbit additionally validates its millisecond lease precision.
`Dispatched` defaults the optional policy method to `None`, so existing trait
implementations still compile; lease-based consumers must supply their policy.

Continuo captures this value in `DispatchRequest` for the managed generation.
The adapter retains it throughout execution and drain. A service's configuration
owner controls preparation/reload and when a later generation adopts changes;
active leases are never mutated by a config read. Fleet peers must agree on
the service's policy. There is no name-keyed policy map or config companion.
The retired `orbit.yaml::dispatch` section is rejected by the strict schema;
`orbit.yaml` continues to own service-snapshot cadence/freshness only.

Each coordinated service has an explicit immutable `DispatchPolicy` containing
pool capacity, `lease_ttl` and `renewal_interval`. There are no adapter duration
defaults. `One` has exactly one shared key; `Pool` has exactly `capacity` shared
keys in the `claviron.services.dispatch` lock namespace. Keys are stable across
process/generation replacement; the owner identity includes process role, PID,
service name and local generation. Membership is read from the process-owner
capability, never a bootstrap worker count. Unavailable membership defers new
acquisitions, while already-owned slots continue to be renewed.

Capacity is not divided by the membership count: a member need not register
this provider. A contender may own multiple available slots; this adapter does
not promise balanced placement. A pooled runnable must support concurrent
executions of its shared provider instance. Normal runnable completion ends
the local controller generation; errors/panics fail it after other local slots
drain. There is no implicit runnable retry policy. A slot retired by ownership
loss can be reacquired after its old local execution has returned.

On Linux, FreeBSD and supported macOS, lock transitions wake acquisition via
native ring readiness, armed before the initial acquisition. Renewed records
do not trigger additional acquisition scans. Lease renewal and expired-owner
reconciliation use the explicitly declared renewal cadence; missed ticks are
skipped, not replayed. Unsupported native readiness is an explicit error, not
a hidden polling fallback. Standalone owns its configured slots locally,
opens no dispatch lock table and performs no renewal polling.

Stop prevents acquisition, cancels owned children, and awaits them without an
adapter-imposed graceful deadline. Exact leases continue to renew through
intentional drain and release only after each execution completes. Lease loss
cancels the affected child; the adapter never renews or releases a successor's
tenure. An expired/lost lease cannot exclude a successor while the old child
drains: external side effects still need their own fencing/idempotency. This
is placement ownership, not an exactly-once execution guarantee.

## What Belongs Here

- Joining Orbit during Claviron boot.
- Keeping the process `Arc<Fleet>`.
- The `AppState` extension trait.
- The typed event bridge.
- The language-neutral application-event envelope and native value codec.
- Binding `orbitive::cache` to Claviron lifecycle and platform readiness.
- Supplying application-link kinds and geometry, HTTP frames, target selection,
  and Claviron runtime adapters over `orbitive::link`.
- Registering named logical cache stores on one shared physical connection.
- Abstract L2 ordering and recovery policy for Orbit cache.
- Claviron's keyed signed counter state and lifecycle.
- Claviron's exact-lease RAII lock binding.
- Typed SHM lifecycle policy, provider inventory, and decision reporting.
- Re-exporting framework-neutral Orbit primitives.

## What Does Not Belong Here

- Cache ring algorithms or L1 storage internals.
- Named inbox layout, admission mechanics, reusable duplex-slot mechanics, or
  generic link death recovery; those belong to `orbitive::link`.
- Concrete File/Redis stores or cache configuration.
- Generic cache value encoding or repositories.
- PHP host functions.
- Metrics capture or aggregation.
- Supervisor heartbeat and command policy.
- Domain-specific activity inference; each resource-owning provider supplies
  that evidence to the SHM lifecycle executor.
- Concrete domain event definitions, dispatchers, or framework providers.

## Invariants

- `FleetProvider` boots before every Fleet consumer.
- Master role is not reset or unlink authority.
- Missing SHM is a normal boot state; valid SHM is not reset by default.
- Physical validation and live-user activity are separate observations.
- Unknown activity fails closed for every destructive SHM action.
- Invalid SHM is reported with its raw mismatch and is never implicitly
  unlinked.
- `FleetProvider` remains a capability provider, not a heartbeat runnable.
- `FleetEventRegistry` boots after `FleetProvider`.
- Event boot prepares the cursor but does not spawn a polling task. The inbound
  loop belongs to Runtime; typed outbound bridges belong to the local dispatcher.
- Supervised node ids have one live process incarnation; the supervisor reaps a
  worker before reusing its lane.
- Typed event registration never creates per-type SHM segments.
- Lock state is not reconstructed by scanning a ring.
- Linux/FreeBSD/macOS listeners own process-local readiness fds; an fd is never
  the fleet-wide broadcast object.
- `FleetEventRegistry` never resets or unlinks event transport; the shared ring
  survives master replacement so adopted peers remain on the same bus.
- One `CacheService` clone family shares one physical connection, mutation
  cursor, and inbound driver.
- Each `CacheRuntime` clone family shares one named store's L1 and backing.
- Cache mutations use `(store, key)` identity and resets are store-scoped.
- Orbit cache validation precedes L2 writes; L2 mutations precede Orbit
  publication.
- Mutation loss requires an attached backing store before L1 is made coherent
  again.
- Lease locks remain independent from cache value mutations and L2.
- Orbit lock calls never hide a retry or polling loop.
- Counter keys are immutable after publication; hot-path updates use checked
  signed atomics and publish no ring event.
- The counter table is never reset by a boot; it is cleared only by the operator.
- An arena record a reader holds is never overwritten; the cursor skips it.
- A compatible arena is preserved across boot by default. Its owning domain
  may explicitly request a reset after quiescence proof; an explicit runtime
  reset remains available to that owner.
- The bind layer must remain thin enough that non-Claviron users can use
  Orbitive directly.

## Application WebSocket messages

The application transport's existing duplex slot also carries complete WebSocket
messages after upgrade acceptance, and the same records for reliable
WebTransport bidi streams. H1 Upgrade, H2 extended CONNECT and H3 WebTransport
share the application acceptance marker; Edge owns the native 101/200 response.
This uses the existing Exchange flags and stream geometry. It adds no SHM kind
or layout change. Each WebTransport bidi stream is an independent slot/session;
datagrams and uni streams do not become message records implicitly.
`transport::MessageReader` is cancellation
safe and optionally bounds declared payload sizes before allocation. The bound
is a consumer policy; default construction preserves the existing unbounded
reader. Broadcast and Gufi share the record format and existing stream wakeups. See
[the transport contract](TRANSPORT.md#websocket-records).
