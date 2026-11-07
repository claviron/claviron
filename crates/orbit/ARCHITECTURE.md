# claviron-orbit Architecture

## Boundary

This crate binds the framework-neutral `orbitive` facade into Claviron's runtime
lifecycle: when a process joins its fleet, who owns each shared resource, what may
clear it, and how events and cache mutations reach the rest of the process.

Orbitive owns the primitives: POSIX mechanics, ring and table layouts, cache and L1
algorithms, the Link inbox, its duplex slots and exact incarnation recovery. Claviron
supplies kinds and geometry here, then adds application naming, HTTP exchange frames,
routing, status mapping and lifecycle integration. It stays thin enough that a
non-Claviron user can use Orbitive directly.

Not here: concrete File and Redis stores and their configuration (`cache`), typed cache
value encoding (`claviron-cache`), metrics aggregation, language host functions, concrete
domain events, the `ApplicationEvent` envelope (`claviron-app`), heartbeat records and
process policy (`supervisor`), and scheduling (`tasks`). Each resource's activity evidence
comes from the provider that owns the resource.

## Sources

- [`lib.rs`](src/lib.rs), [`registry.rs`](src/registry.rs) — The facade and the `register*` functions. Registration is idempotent.
- [`fleet.rs`](src/fleet.rs) — `FleetProvider`: the process's one `Arc<Fleet>`.
- [`shm.rs`](src/shm.rs) — Boot and finalize policy, the managed provider wrapper and the lifecycle inventory.
- [`event.rs`](src/event.rs) — The typed bridge between Orbit frames and the local event bus.
- [`cell.rs`](src/cell.rs), [`cell/orbital.rs`](src/cell/orbital.rs) — The typed cell tables and `Orbital<T>` declarations.
- [`cache.rs`](src/cache.rs), [`cache/runtime.rs`](src/cache/runtime.rs) — One cache connection per process, its logical stores and L2 ordering.
- [`arena.rs`](src/arena.rs), [`counter.rs`](src/counter.rs), [`lock.rs`](src/lock.rs), [`stream.rs`](src/stream.rs), [`quota.rs`](src/quota.rs) — Lifecycle bindings of those primitives.
- [`invoke.rs`](src/invoke.rs), [`invoke/call.rs`](src/invoke/call.rs) — The invocation ring and transport-independent targeted calls.
- [`service.rs`](src/service.rs), [`service/`](src/service/) — Fleet commands and snapshots over Continuo's process-local service manager, and live membership.
- [`placement.rs`](src/placement.rs), [`placement/controller.rs`](src/placement/controller.rs) — `OrbitDispatcher`: `One` and `Pool` leases.
- [`config.rs`](src/config.rs) — `orbit.yaml`.

### `link/`

- [`connection.rs`](src/link/connection.rs), [`access.rs`](src/link/access.rs) — One protocol-neutral membership and exclusive reader, owned by its calling service.
- [`events.rs`](src/link/events.rs), [`interest.rs`](src/link/interest.rs) — Topic forwarding on a connection, and connection-local interest.
- [`invoke/`](src/link/invoke/) — Bounded command and result calls over a service-owned geometry.

### `link/application/`

- [`segment.rs`](src/link/application/segment.rs), [`body.rs`](src/link/application/body.rs), [`geometry.rs`](src/link/application/geometry.rs) — The application link's namespace, naming, body table and compiled geometry.
- [`frame.rs`](src/link/application/frame.rs) — What crosses the inbox: one exchange's control, never its body.
- [`client.rs`](src/link/application/client.rs), [`service.rs`](src/link/application/service.rs), [`target.rs`](src/link/application/target.rs) — The calling side: one client per destination fleet, and a route's prepared target.
- [`message.rs`](src/link/application/message.rs) — Message records on a stream slot.
- [`app.rs`](src/link/application/app.rs), [`peer.rs`](src/link/application/peer.rs) — Application events across a link, and who is on its other side.

## Boot

```text
FleetProvider              priority EARLY: Fleet::join_shm_as(fleet_name, capacity, node)
CellsProvider              EARLY, after it: open the tables, bind Orbital declarations
master and worker          event ring companion, then FleetEventRegistry with its cursor at the head
standalone                 Fleet only; local events stay on state.events()
optional providers         counter, arena, invocations, locks, streams: after FleetProvider
fleet service management   Continuo ServiceManager, command bridge, snapshot companion, publisher
```

A consumer declares `ProviderOrder::new().after::<FleetProvider>()` and resolves the handle
during `boot()` or `run()`. It may clone the `Arc<Fleet>`; it does not create another
Orbit registry or cache an optional Fleet to wait for boot. `FleetProvider` is a capability,
not a runnable: it publishes no heartbeat.

`AppState::fleet_name()` is the explicit sharing domain, independent of the instance name, and
defaults to `clv`. Segments are `/orbit-{fleet}-{kind}-{uid}`; the name is short because of
macOS POSIX SHM limits.

`fleet_capacity` is immutable physical lane geometry, built in through
`CLAVIRON_FLEET_CAPACITY` beside the ring geometry it must agree with. Every peer must agree on
it, and standalone uses the same value. It never proves membership: readers scan the capacity
and apply their own freshness rules. Node zero is standalone or master, and each worker id maps
to the equal node id. `FleetNodeId` and `WorkerId` stay different types so a directory could
allocate lanes to independently launched process families; writable external members are not
admitted today.

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
| `185` | Fleet TLS server sessions v2: a claim on each set instead of a table lock, owner registrations held by kernel locks | Header + owner generations + set claims + slots | Orbitive `orbit-rustls` `SERVER_SESSION_STATE_KIND` since 0.5.6; `tls` [TLS_SESSION_STATE_KIND](../tls/src/orbit.rs) follows that constant |
| `186` | Recoverable text cells v5 with two committed/staging banks, full-width revision and exact owner incarnations | Table + owner identities + per-owner slot pin rows | Orbitive `CELL_TEXT_KIND`; replaces `188`; owner-exit recovery completes pending wakes under exact kernel holds, without touching the last committed text |
| `187` | Handle-pinned scalar cells v4 with a separate parking generation | Table + per-owner slot pin rows | Orbitive `CELL_STATE_KIND`; replaces `189`; logical release precedes physical reuse, cancellation changes the native park word without changing the value version |
| `188` | Retired handle-pinned text cells v4 with a separate parking generation | Table + per-owner slot pin rows | Replaced by `186`; anonymous writer/seqlock publication has no owner-death recovery; retained objects are not migrated, reset or unlinked automatically |
| `189` | Retired handle-pinned scalar cells v3 | Table + per-owner slot pin rows | Replaced by `187`; old peers parked on the value version; retained objects are not migrated, reset or unlinked automatically |
| `190` | Retired handle-pinned text cells v3 | Table + per-owner slot pin rows | Replaced by `188`; old peers parked on the seqlock version; retained objects are not migrated, reset or unlinked automatically |
| `191` | Runnable service snapshots with controller lifecycle, dispatch and actual execution census | Ring, 512-byte payloads | [SERVICE_SNAPSHOT_RING_KIND](src/service/snapshot.rs); replaces `232`; `Reuse` / `Preserve` |
| `192` | Application snapshots with execution descriptions, row version 2 | Ring, 512-byte payloads | `claviron-app` [APPLICATION_SNAPSHOT_RING_KIND](../app/src/fleet/snapshot.rs); replaces `235`; `Reuse` / `Preserve` |
| `193` | Shared byte arena v2 with per-process reader pins, reclaimed only after taking the dead owner's kernel hold | Index + owner rows + byte ring | Orbitive `ARENA_KIND`; [ArenaProvider binding](src/arena.rs); replaces `242` |
| `194` | Arena-addressed cache mutation transport (protocol v3); values stay in kind `193` | Ring | Orbitive `CACHE_MUTATION_RING_KIND`; [cache binding](src/cache/runtime.rs); replaces `200`/`201` |
| `195` | Application-link inbox: one lane per link participant, fixed-size frames carrying an exchange header and its stream ticket. Many writers, the lane's owner reads; a full lane refuses rather than overwrites, and the next process to join takes back lanes whose kernel hold it can take (see Cleanup). Blocking consumers wait on the lane word; Tokio consumers arm the lane's named descriptor bell and await the reactor while frames remain in SHM. One directory-watcher slot per lane follows the lanes: a process that holds no lane (a master) is woken by the same claims and releases through its slot, held like a lane by a kernel lock (`orbitive::shm::try_hold_watcher`), and never appears in the directory. An interest entry per lane and per watcher follows: the routing keys (application-event domain topics) that slot's process subscribes to, read by `orbitive::link::LinkEvents` so an application event crosses the link only to a side that wants it. Lives in the selected destination fleet (`/orbit-<fleet_name>-195-<uid>`), with application names advertised inside it | Ring, watcher slots after the lanes (orbit-link layout 6) | Orbitive `orbit-link` layout with Claviron `LINK_SPEC`; `ORBIT_LINK_INBOX_CAPACITY` / `ORBIT_LINK_INBOX_PAYLOAD_CAPACITY` / `ORBIT_LINK_FLEET_CAPACITY`; [application link](../../docs/design/ORBIT_LINK.md) |
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
| `211` | Application-link bodies: per-node lanes of stream slots, two bounded byte rings per slot. One duplex slot per exchange — request body one way, response body the other. Separate from kind `246` because worker coordination wants many narrow lanes and this wants fewer and wider. In the selected destination fleet beside kind `195`, addressed by the same lane index | Header + doorbells + bitmaps + slots + rings in one segment | Orbitive `orbit-link` / `orbit-stream` with Claviron `LINK_SPEC`; `ORBIT_LINK_LANE_CAPACITY` / `ORBIT_LINK_BUFFER_BYTES`; [application link](../../docs/design/ORBIT_LINK.md) |
| `212` | Retired upstream worker metrics v1 (58-byte payload) | Ring | Replaced by `213`; retained deployed segments are not reset or unlinked automatically |
| `213` | Upstream worker metrics v2 (74-byte payload) | Ring | `upstream` [UpstreamOrbitMetrics::KIND](../upstream/src/metrics/orbit.rs); `Reuse` / `Preserve` |
| `214` | Embedded PHP worker metrics | Ring | `php-embed` PhpEmbedOrbitMetrics::KIND |
| `215` | PHP FastCGI worker metrics | Ring | `php-fcgi` FcgiOrbitMetrics::KIND |
| `216` | FPM pool metrics | Ring | `php-fcgi` FpmPoolOrbitMetrics::KIND |
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
| `231` | Retired fleet TLS server sessions v1 with one table-wide lock | Table | Orbitive `orbit-rustls` through 0.5.5; replaced by `185`; retained segments are not reset or unlinked automatically |
| `232` | Retired controller-only runnable service snapshots | Ring, 512-byte payloads | Replaced by `191`; retained segments are not resized, reset or unlinked automatically |
| `233` | Retired scalar cells v2 without handle pins | Table | Replaced by `189`; retained objects are not migrated, reset or unlinked automatically |
| `234` | Application events | Ring | [APPLICATION_EVENT_RING_KIND](src/event.rs) / `ApplicationEventLayout` |
| `235` | Retired application snapshots without execution descriptions | Ring, 512-byte payloads | Replaced by `192`; retained segments are not resized, reset or unlinked automatically |
| `236` | Broadcast publication metadata | Ring | `broadcast` PUBLICATION_RING_KIND |
| `237` | Broadcast publication payload chunks | Ring | `broadcast` PAYLOAD_RING_KIND |
| `238` | Master generation identity | Ring | `supervisor` [MASTER_GENERATION_RING_KIND](../supervisor/src/process/identity.rs) |
| `239` | Worker-owned process presence | Ring | `supervisor` [PRESENCE_RING_KIND](../supervisor/src/observation/presence.rs) |
| `240` | Retired worker lifecycle/operation snapshots without master runtime clocks | Ring, 147-byte rows on 160-byte payloads | Replaced by `197`; retained deployed segments are not resized, reset or unlinked automatically |
| `241` | Generic invocation requests | Ring | Orbitive `INVOCATION_RING_KIND`; [InvocationProvider binding](src/invoke.rs) |
| `242` | Retired byte arena v1 with anonymous pins | Index + byte ring | Replaced by `193`; retained segments are not resized, reset or unlinked automatically |
| `243` | Embedded PHP SAPI pool/thread statistics | Table | `php-embed` SAPI_STATS_KIND |
| `244` | Retired private PHP invalidation transport | Ring | Replaced by the inbound-only kind-220 event bridge; retirement contract. Not available for incompatible reuse; retained segments require stopped-fleet cleanup |
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

## Shared-memory lifecycle

`orbitive::shm::ShmRegion::validate_existing` is the mutation-free physical check: missing is a
normal state, and a present object must be large enough. The owning adapter then validates its
magic, version and geometry. Physical invalidity is returned with its detail and never turned
into an automatic unlink.

Policy is typed and separate from that check:

- `ExistingBootPolicy` decides whether valid state is reused, rejected, reset or recreated.
- `FinalizePolicy` decides whether state is preserved or conditionally cleaned.
- Destructive policies require provider-owned `ResourceActivity` evidence. `Unknown` never
  authorizes reset, recreation or unlink, and the master role alone grants nothing.

`ManagedShmResources` deliberately does not implement `Provider`. Only `ManagedShmProvider<P>`
does, and only `shm::register` constructs it, so a managed resource cannot be inserted around the
executor or the inventory. `ShmLifecycleInventory` reports what the running process registered and
decided; the index above covers everything reserved.

Creation coordination is not destructive authority. Where the master finishes provider boot before
workers start, it may create and initialize a resource so no consumer sees the gap between
`shm_open`, sizing and header publication. Resetting a compatible object still needs its policy
and its evidence.

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
  Link lanes (`195`) use one per lane (`orbitive::shm::try_hold_lane`),
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
| `195` application-link inbox (selected link fleet, not managed) | `orbitive::link::LinkSegment` creates once and otherwise attaches; a lane is won under its hold by CAS, then cleared and stamped | Claviron releases the lane, then its hold | **the next joiner takes back every lane whose hold it can take**: the kernel let go of it, so its owner is gone | a dead owner's lane and frames until the next process joins the fleet; the lock files beside the segment (removed with it) |
| `211` application-link bodies (selected link fleet, not managed) | `orbitive::link::LinkBodies` attaches without reset | nothing | join recovery, refused-write recovery, and exact supervisor exit reset only the dead lane/incarnation's sides | unsupervised peers still need reclaim on a refused write or a join |
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
| `185` TLS sessions | ResetWhenQuiescent{Reuse}, when `cleanup_on_exit` | UnlinkWhenQuiescent | — | nothing after a quiescent exit; otherwise sessions bounded by their TTL |
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
or `/tmp/orbit-<uid>`): one beside a locked region, and one per link lane
ever held (`<segment>.lane<n>.lock`). An unlocked one is harmless and reused.
Unlinking the segment, from code or with `orbit clear`, removes them with it.

## Events

`FleetEventRegistry` adds a codec to `orbitive::events::FleetEventBus`, which carries raw topics and
bytes. All event types share one segment with one writer lane per node: no per-type SHM, and no
total order across lanes. Implementing `FleetEventCodec` alone forwards nothing.

- `register_bridge::<E>` links local emits to Orbit and inbound frames to local emits. An event it
  decoded is not echoed back. Local emits are forwarded asynchronously, so returning from `emit`
  does not prove an Orbit write.
- `register_inbound::<E>` only decodes peer frames, for a producer that calls `publish_raw` after its
  local operation has completed. `publish_raw` writes synchronously and stays available after the
  inbound runnable ends, which is how a worker's terminal notification goes out after teardown.

Registration subscribes synchronously and starts no task. Outbound callbacks share the local
`EventDispatcher`'s runnable, so registration must precede runnable submission. A conflicting topic
or a late listener is an error; registering the same type again is a no-op.

`NO_IMPLICIT_TERMINATION: EVENT_SHM_SURVIVES_MASTER_REPLACEMENT`. The event segment is a fleet
resource: a replacement master joins it because an adopted worker may still be publishing
accepted-work progress. Each ring is a `OrbitRingResource<T>` companion with `Reuse` and `Preserve`,
and `EventRegistry<L>` is ordered after it. Master boot is not proof of quiescence.

Boot captures the inbound cursor at the current head, before workers spawn, so stale history is not
replayed and events published between boot and run are not skipped. The inbound loop is a `Runnable`
with `Scope::Any`, because a master must hear its workers' startup acknowledgements. Its cursor
returns to the registry when a generation ends, so a targeted restart resumes without a gap. It keeps
running through master fleet draining. Standalone opens no event ring.

Linux, FreeBSD and macOS 14.4 or later wait through native ring readiness and a process-local fd; an fd
is never the fleet-wide broadcast object. Readiness only says the ring changed, and the registry then
drains with its own cursor. Older macOS and other targets use a 10 ms interval.

`ApplicationEvent` is a bridged envelope: its outer topic identifies the codec, and its inner topic is
the routing key. It carries no audience, because subscriptions are the audience, and it is broadcast,
not a queue. Link connectivity is observed from the opposite side's lane directory (`EdgeConnected`,
`ApplicationConnected`), not discovered by broadcast questions, so one link's introductions do not
reach other links' guests.

## Link

`link::Connection` is the lifetime binding over Orbitive's Link: one kernel hold and one `InboxReceiver`.
A failed reader bind withdraws the lane. Withdrawal stops admission and removes the reader before the
hold goes, and it does not prove completion: a protocol owner drains its accepted work first. Drop
never resets or unlinks SHM. The caller supplies a `LinkSpec`, so the API allocates no kind.

`Connection::events` binds a supplied SHM-backed event bus through `LinkEvents`. `run_until` drives the
bridge and exclusive reader in the caller's future, with no task, interval or signal handler. A bus
without native readiness fails the driver instead of polling. Accepted handlers are awaited on
cancellation or failure while the lane is held. A received event is republished once and handed to the
handler once, so a caller must not deliver it locally a second time. Topics stay opaque: authorization,
codecs and acknowledgement belong to the protocol owner.

`Access::OwnerOnly` is the default. `Access::Group` forwards to Orbitive's group namespace and shares only
that Link's inbox, streams, locks and wake paths. The owner provisions first and peers attach, with no
private fallback. Group writers share trust in Link state; this is not an authenticated source
identity. Every participating process supplies the same policy.

### Application link

Design record: [ORBIT_LINK](../../docs/design/ORBIT_LINK.md).

A route prepares an `ApplicationTarget` while it is built. Its immutable lane snapshot is tagged with the
directory generation, so an ordinary send takes no client map lookup or mutex. A generation change does
one bounded lane scan and one compare-and-swap publication; a delayed refresher cannot replace a newer
snapshot. The generation is captured before scanning, so a concurrent change invalidates the result on
the next send. Depth and admission are still read per exchange.

A lent response reader signals departure as soon as it is dropped, even while the upload holds its half.
Returning both halves to the pool waits for the remote side to release its generation. So a cancelled
reader does not keep pre-head runner work alive until an unrelated upload ends.

An admitted exchange owns its response stream before publishing its reply head. If the head cannot be
published, the application resets the response direction, including after a small buffered body already
published FIN. `ReplyWait` checks its oneshot, then the stream's reset-only signal; data and FIN do not
complete a header wait. It adds no deadline or poll. A refused request remains a pre-execution 503.

After an accepted upgrade the same slot carries whole message records: for a WebSocket, and for each
reliable WebTransport bidi stream, which is its own slot and session. Datagrams and uni streams are not
carried. The door that offers such a session to an owner is `claviron-transport`'s; the message type is
`claviron-types`', and `MessageReader` here bounds a declared length before allocating. A close stays
readable under any limit, because it is how the session ends.

`ApplicationBodies::for_segment` takes its geometry and access from the selected inbox.
`ApplicationService` accepts per-destination access before its first client opens; an unregistered
destination stays private. The generic reader must never attach to a running application lane as a second
reader.

## Invocations

`invoke::Client` maps logical targets to `Route`s fixed at boot. A caller supplies a target, operation and
opaque bytes; no local, Link or ring fallback is chosen for it.

`link::invoke` is the same-host adapter over one `Connection`, using the owner's `LinkSpec`. A logical service
must resolve to exactly one live lane; more than one is an error, not load balancing. A request carries both
endpoint incarnations, a never-reused call sequence and a stream ticket. The caller reserves side A before
publication, and the target claims side B before running domain code, so a duplicate claim cannot execute
twice. The one bounded result goes on that stream, so events cannot use up response capacity.

When callback capacity is full, the reader stops consuming and the bounded inbox gives backpressure. Shutdown
stops new calls and keeps the reader and lane while accepted work and pending calls drain; new inbound calls
get `Closing`. There is no timeout or retry, and abrupt death is not inferred from elapsed time. Local
cancellation is not remote cancellation, and a missing result is outcome-unknown, not safe replay. Commit ids
and authorization are the handler's. Group writer access is not caller identity.

`invoke::InvocationBus` is a separate bounded append-only ring (kind `241`), isolated from application events.
Publication proves only that a frame was committed: no acknowledgement, claim, retry or exactly-once. Cursors
report overwritten frames as lag. It is never reset at boot, because a reset could neither acknowledge nor
cancel outstanding work. A consumer that wants to announce completion publishes an application event carrying
the `InvocationId`.

## Typed cells

`register_fleet` installs `CellsProvider`, so an application needs no cell setup. `Orbital::<T>::new(initial)`
declares a value, and allocation errors reject startup before the application runs.

The default constructor binds weakly to one managed owner; another live owner cannot replace it. Before binding,
`load` returns the initial value and a mutation fails with `CellError::NotPrepared` rather than pretend an unshared
update succeeded. After binding, an operation reads a local `ArcSwap` handle and calls Orbitive, with no lookup or
lock. `try_new` reports a failure immediately; `new` keeps it for later operations. An error never becomes zero.

Clones share one id. The last clone retires the slot, and `release` retires it earlier. Handles opened from an id
borrow that lifetime and read `Stale` afterwards. `Cells` and `cell::Orbital<T>` remain for explicit-handle
composition, which language adapters use.

## Cache

`CacheService` wraps one `orbitive::cache::Cache` connection per process: its readiness loop, one mutation cursor
and the logical stores. Each `CacheRuntime` wraps one named store's L1 and optional L2. Mutations are addressed by
`(store, key)`, and `Reset` is store-scoped. Readiness is installed before the driver's first poll, so a mutation
committed at startup is not stranded.

The ring carries arena addresses, revisions and TTL, never bodies. It is coherence traffic, not a snapshot.

```text
get:    L1 hit -> return
        L1 miss -> backing get

put:    validate Orbit bounds -> backing put -> publish Orbit Put
forget: validate Orbit key    -> backing forget -> publish Orbit Delete
clear:  backing clear         -> publish Orbit Reset
```

L2 completes before publication, so a peer that receives a mutation cannot fall through to an older value.

If mutation history is lost, the missing mutation's store is unknowable because the cursor is shared, so every L1 is
disabled. It resumes at the current heads only when every store has a backing, and then with empty L1s.

An L1 hit takes an owner-aware pin on the exact arena generation, and the returned `Bytes` owns that pin and the
mapping. TTL is checked before the pin. Delete and Reset invalidate metadata, while snapshots already returned stay
valid. An overwritten address is an honest miss.

`open_store` may reopen a registered name with the same L1 capacity, so a catalog can hide a store and bring it back;
changing capacity or backing identity is a restart-level change. Lock state never shares keys, reset behavior or
storage with cache values.

## Arena

`ArenaProvider` opens the fleet's one byte store, kind `193`, whose first user is the file service. The arena stores
bytes and knows nothing else. There is no expiry: a put evicts what its bytes land on, except a record a reader pins,
which the cursor moves past.

Readers take no lock. A writer holds the region's `flock` for the put, so a crash leaves at most one unpublished
record. An address includes its slot generation, so index reuse never resolves to a new value. Pins belong to
per-process rows. A process holds a kernel lane lock for its mapping's lifetime, and a writer recovers abandoned pins
only if it can take that lock: no heartbeat, PID heuristic or timeout decides that a reader died.

Geometry is compile-time (`ORBIT_ARENA_SLOTS`, `ORBIT_ARENA_BYTES`) and checked in the header by every peer. A record
above a quarter of the ring is refused. No build can check the pins: the ring must hold the working set plus
`nodes × responses in flight × record`, or a put gets `Error::Pinned`, logged and served from the bytes just read.
Sizing is the deployment's to state.

## Counters and locks

`Counter` is one authoritative table, kind `230`, with no history ring. A key is installed once under a short
recoverable lock; after that its bytes and slot are immutable, so `get`, `increment`, `decrement` and `reset` work on
the slot's atomic. Runtime key deletion does not exist, which removes a reuse race. Updates are checked and reject
overflow unchanged. Every boot reuses the table, so values survive a restart, and an adopted worker is never mutated
underneath itself.

`OrbitLock` is one atomic operation per call, with no task, retry or blocking. A consumer that waits uses the transition
ring or its own bounded retry. State and ring survive master replacement, because lease expiry plus fencing owns stale
state and clearing would invalidate live tenures. State is never rebuilt by scanning the ring. `Lock` is the Rust RAII
binding: it holds an exact lease, so an expired holder cannot delete a successor. Laravel uses `OrbitLock` directly and
owns its own tokens, sleeping and release.

## Service management

Continuo owns each process's runnable generations, cancellation and `ServiceSnapshot`. `FleetServiceManager` publishes
typed start, stop, reload and restart commands through the event bridge, and each process applies them to its own
manager. Provider registration is the only placement source: a service missing from a process is not placed there.

Commands acknowledge validation and publication, not completion. Clients prove convergence from snapshots: start reaches
running, stop a non-running terminal state, restart a new generation, reload a new revision with no error. Standalone
feeds the same command to its local listener and opens no Orbit transport, keeping the accepted-then-observed contract
even when the target is the server carrying the RPC.

Each process publishes complete batches to a per-node ring, kind `191`, and readers accept only fresh complete batches.
`Running` is the controller's lifecycle, not a dispatch slot: Continuo counts each actual runnable future, so a running
`One` or `Pool` with zero executions is waiting. That is execution evidence, not protocol readiness.

`ServiceSnapshotPublisher` owns `orbit.yaml`, through Congif:

```yaml
service_snapshots:
  publish_interval: 1s
  max_age: 5s
```

Those are the defaults. Both must be nonzero, and unknown keys fail preparation. A missing file keeps the policy given to
`register_with_policy`. Reload notifies the publisher through a watch channel, with no polling or restart; a changed cadence
schedules the next publication one new interval from the last observation. Standalone has no publisher.

`FleetMembership` is a process-owner capability the supervisor binds: master plus nonterminal workers are expected, and
terminal slots are excluded even while their last batch is fresh. Missing ownership data fails closed, and missing data
from an expected member is an incomplete snapshot. `service::current_members` is the strict read for resize-aware consumers:
it requires an owner in supervised mode and never falls back to a bootstrap count. Membership is not a healthy-worker count.

`FLEET_CONTINUITY` protects the command listener, inbound bridge and snapshot publisher from fleet-wide operations that would
remove their own recovery channel. The ingress adapter protects the transport carrying its own request; Orbit does not
assume management rides `HttpsServer`. `WorkerSnapshotPublisher` cannot be stopped through fleet control, because membership
reports are needed to target and recover services; its managed restart stays available.

## Runnable dispatch

[INCARNATION_MASKING](../../docs/design/INCARNATION_MASKING.md) applies here: slot keys are stable while exact lease owners
change. Applications declare placement intent, not a PID.

`Scope` and `Dispatch` are Continuo's vocabulary, re-exported here. `OrbitDispatcher` is a registered capability, not a runnable
or a manager. It receives `ServiceExecution` from Continuo and owns exact per-slot leases in the lock table and ring, with no
SHM of its own. Server bootstrap installs it on the Runtime, so applications using that bootstrap do not register one.

Each coordinated service declares an immutable `DispatchPolicy` in its own configuration:

```yaml
listener:
  scope: workers
  dispatch: pool
  dispatch_policy:
    capacity: 3
    lease_ttl: 20s
    renewal_interval: 5s
```

These are example declarations, not defaults. There are none: `One` or `Pool` without a policy fails validation rather than
falling back to `Each`. `One` has one shared key and `Pool` has `capacity`, in the `claviron.services.dispatch` namespace. The owner
identity includes role, PID, service and generation. Unavailable membership defers new acquisitions, while owned slots keep
renewing.

Capacity is not divided by membership, because a member need not register the provider, and placement is not promised to be
balanced. Lock transitions wake acquisition through native readiness, and unsupported readiness is an error, not a hidden poll.
Renewal and expired-owner reconciliation use the declared cadence, and missed ticks are skipped.

Stop cancels owned children and awaits them with no adapter deadline, renewing leases until each execution completes. Lease loss
cancels the affected child, and the adapter never touches a successor's tenure. A lost lease cannot hold back a successor while the
old child drains, so side effects need their own fencing: this is placement, not exactly-once. Standalone owns its slots locally and
opens no table.

## Quota

The `quota` feature assembles Orbitive's LocalGuard, FleetGuard and FleetQuota behind `Quota`. It owns typed definitions and handle
lifetime, not key selection, HTTP status or authentication. `try_consume_with_snapshot` forwards unchanged, and snapshots come from
Orbitive's own successful transition.

These are explicit-handle resources, opened by their owner during preparation, not providers. They never reset or unlink. Matching
opens keep committed debt, and incompatible state is an error. No core SHM kind is allocated: operators own dedicated namespaces and
must not reuse another subsystem's. Configuration checks are structural and open nothing.
