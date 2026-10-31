# Broadcast Service Boundary

`claviron-broadcast` owns Claviron's realtime publication and session service.
Applications select the `broadcast` facade feature and resolve `BroadcastService`.
The application catalog is `broadcast.yaml`, prepared by Congif: the service is
a `Configurable` bound to the application's frozen configuration environment,
validated (unique ids and keys) before any provider boots, and handed the
catalog in `configure`. The file is required where the service is registered.
Native publishers call Rust directly; process-to-process messages use Orbit SHM
with native wakeups.

Compositions with their own launch document can instead call
`register_with_config(state, BroadcastConfig)`. This validates the same catalog
and installs the same lifecycle and Orbit resources, without a file-backed
`Configurable`. The default `register` retains an already registered service
rather than opening `broadcast.yaml` again. The in-hand catalog is boot intent;
changing it requires a process restart, not an implied file observer.

Pusher names identify wire compatibility, exposed through
`claviron::broadcast::protocol::pusher`. The protocol crate owns signatures and
message shapes; it does not own the service. This crate's optional `pusher`
feature adds compatibility with an external Pusher HTTP upstream. The facade
exposes only `broadcast`; an HTTP edge binding selects `pusher` internally. Ordinary native publication and Orbit
sessions need neither an outbound HTTP client nor an additional listener.

## Ownership

One `BroadcastService` is registered in every serving worker. Its configuration
contains the application catalog:

```yaml
applications:
  laravel:
    id: "2010777"
    key: bthd45gf2wd
    secret: ${PUSHER_APP_SECRET}
    client_events: true
```

The map key is Claviron's stable application name. The ID, key, and optional
secret are Pusher wire identities. Duplicate IDs or keys fail boot.

The serving component receives an optional `FleetProvider` through Continuo
`Deps`: direct local hosts retain fleetless session delivery. The master-side
occupancy component requires that provider because its state machine consumes
fleet subscription records. Normal registration installs Orbit for both roles;
each runnable receives its prepared dependency when its generation starts.

Native callers that already have a Pusher key use
`BroadcastService::publish_by_key`. The service resolves it through this same
catalog and delegates to the existing `publish` path; no additional application
map, default selection, transport or cache is created. Unknown keys fail without
exposing their values. This is selection for trusted native callers, not external
REST credential verification.

The service owns a replaceable generation. Each accepted WebSocket holds a
generation permit. Stop seals admission, signals every accepted socket, waits
for all permits, and only then reports the service stopped. A later start
creates a fresh generation on the same service object.

The listener remains the transport owner. The Broadcast generation is a second,
application-level lifecycle boundary; neither detaches the socket from the
other.

## Session transports

`BroadcastService::accept_session` returns an admitted `BroadcastSession` retaining its
generation permit. `run` consumes it under the ingress owner's task and executes
one protocol loop over `MessageStream`. `run_with_shutdown` additionally accepts
the ingress owner's graceful signal; interrupted writes still pass through
the same subscription/presence cleanup. No detached task or timeout is added.
The loop owns authorization, channel
membership, publication and service-stop close independently of framing.

`FastWebSocketStream` adapts an upgraded HTTP connection; `OrbitMessageStream`
adapts an accepted duplex Orbit slot using the existing Text/Binary/Close record
format. `WebTransportMessageStream` adapts a reliable bidirectional H3 stream
using that same record codec and cancel-safe bounded reader, not a second
channel protocol. Records are `u8 kind | u32 length (little endian) | payload`:
Text = 1 (UTF-8 Pusher JSON), Binary = 2, Close = 8 (little-endian u16 code,
then UTF-8 reason). A browser client must use a WebTransport socket adapter;
the ordinary Pusher WebSocket constructor cannot open this stream.
The edge owns WebSocket masking, fragmentation and control frames on the
Orbit path. The Orbit reader retains partial records across cancelled reads and
enforces the native service's existing message-size bound before allocating the
declared payload. Closing shuts down the outgoing direction; dropping stream
halves releases this session's ownership and wakes its peer. No adapter
opens a listener or spawns an unowned session task.

## Current protocol surface

- HTTP/1 WebSocket upgrade through `upgrade`, HTTP/2 WebSocket extended CONNECT,
  HTTP/3 WebTransport, or edge-accepted Orbit sessions;
- `pusher:connection_established`;
- Pusher ping/pong;
- public-channel subscription acknowledgement;
- private-channel authorization using the Laravel-generated Pusher signature;
- fleet-wide presence membership, subscription snapshots, and member events;
- fleet-global `channel_occupied` / `channel_vacated` application events;
- opt-in `client-*` events for authorized private and presence channels;
- Pusher REST request signing and authenticated `/apps/{id}/events` publication;
- worker-local channel and socket registries with bounded outbound queues;
- `socket_id` exclusion;
- Orbit-backed publication fanout between serving workers;
- cooperative service-stop close.

Edge prepares the same exact `/app/{key}` bindings for H1/H2 and, when its
`webtransport` capability is selected, H3. Host protocol and H2 CONNECT policy
still control advertisement. Native entrances retain the same catalog and
`BroadcastSession`; private/presence channel signatures, public subscriptions,
client-event admission, socket exclusions and fleet publication do not change.
H2 uses the existing listener `SessionSpawner`; H3 retains the native connection
permit and session. Service stop while waiting for the client's first stream
releases admission too. Configured CORS is checked before native acceptance,
matching the host's ordinary WebSocket origin policy.

An application may register `BroadcastService::require_session` with a
`SessionAuthenticator` before serving. This callback verifies the application's
existing session authority; Broadcast creates no login or guest session and
knows no user schema. The policy is scoped to the existing catalog application,
so its native entrances share the requirement while unconfigured applications
retain Pusher-compatible anonymous admission. H1/H2 call `authenticate_headers`
before upgrading. The H1 handler uses Request<RouteBody> through HTTP's native
handshake contract; it does not require an Axum request body.
The verifier also receives the canonical ingress vhost key captured by the
transport. One catalog application may therefore select distinct existing
guards per entrance without trusting a Host header, trying other guards or
creating another service/application. The same key reaches token verification
for WebTransport. Browser WebTransport cannot carry cookies/custom auth headers;
`authenticate_stream` consumes the first bounded `broadcast.authenticate`
record containing the existing session token before any Pusher message,
subscription or publication. Invalid input is not logged; it closes with 1008
(1011 for verification failures). Verification observes listener/service shutdown
without adding a task, poll or timeout. A protected `BroadcastSession` refuses
to run without successful verification, including direct/Orbit callers.
Private/presence channel signatures remain an additional authorization boundary.

Fleet publication uses two dedicated per-node Orbit rings. Event bytes are
written as one contiguous batch of 4 KiB payload chunks; only after that batch
commits is a compact descriptor published and peers notified. Readers verify
the exact payload frame IDs before decoding. This keeps the SHM allocation
small while preserving Pusher event payloads larger than Orbit's general event
slot. Ring loss is observable and never reconstructed from replacement slots.
Linux, FreeBSD, and macOS 14.4 or later wait on the descriptor ring's native
readiness fd; older macOS releases and other targets retain a 10 ms polling
fallback. The payload ring is addressed storage and never emits a second wake.
Both rings are registered through the managed SHM inventory as `Reuse` /
`Preserve`. A new descriptor cursor starts at the current head, while retained
payload bytes are reachable only through validated frame ids from a current
descriptor; boot reset is therefore unnecessary and unsafe for adopted peers.

Presence join/leave records use the same ordered fleet transport. A worker
generation announces a reset when it starts; peers purge that node's stale
members and replay their local memberships. This prevents restarted workers
from leaving permanent ghost members without introducing Redis as a second
source of truth.

Channel occupancy is decided once by the master-side
`BroadcastOccupancyService`, never independently by serving workers. Workers
publish subscription join/leave records keyed by Pusher `app_id` and channel;
the owner emits `ApplicationEvent` only for the fleet-wide `0 -> 1` and
`1 -> 0` transitions. The owner assigns one unique `event_id` to the decided
transition before publishing it; every application subscriber receives that
same identity rather than a newly generated event. Both event payloads also
contain `app_id`, `channel`, `time`, `user_id`, and `user_info`. `time` is the
owner's ISO-8601 timestamp with six fractional-second digits for the decided
transition. The last two are populated for a presence member and are `null`
for public/private channels. Generation reset/resync records
remove a restarted node's ghost subscriptions and replay live local state.
Standalone mode runs the same owner locally and observes its own records.

## Local hooks

`hooks/occupancy.rs` owns the typed callback contract and registration handles;
`hooks/occupancy/owner.rs` owns the fleet occupancy state machine.
`hooks/occupancy/event.rs` adapts the change to the existing ApplicationEvent
publication; the typed change itself has no Event dependency. Consumers
register through `BroadcastService::hooks().on_occupancy(callback)` and retain
the returned `Subscription` in their own lifecycle. No HTTP endpoint, Rust
`Event` implementation or ApplicationEvent topic decoder is needed to receive
an `OccupancyChange`. This is a channel-liveness transition, not proof that a
user is interacting with the application.

Registrations are process-local. Since occupancy is decided in the master
(or standalone process), callbacks must be registered there before the owner
runs; a serving worker's registration is not forwarded to the master. The
service and owner share the same local hook registry. Registrations survive
service generation replacement while their handles remain alive.

For each decided transition the existing ApplicationEvent is published first,
then local callbacks are awaited in registration order in the owner's existing
future. No additional task, queue, polling cadence, timeout or retry is introduced.
Errors are logged and the remaining callbacks continue; the application event
is not suppressed by a callback failure. A callback must cooperate with shutdown
and must not wait for a later occupancy transition; slow work delays the owner.
Database callbacks capture the application's existing pool. The broadcast
service knows neither the schema nor a connection/transaction policy.

The registry holds weak callback references. Dropping a Subscription removes
it from subsequent dispatch admission and releases captures once already-admitted
work finishes. No lock is held while invoking or awaiting user code. Dropping
a subscription does not cancel an in-flight DB operation. This is best-effort
local delivery, not a durable job or an acknowledged cross-host transport.

Keep the registration in application-owned state, for example:

```rust,ignore
let subscription = broadcast.hooks().on_occupancy(move |change| {
    let existing_pool = existing_pool.clone();
    async move { update_presence(&existing_pool, change).await }
});
// Retain `subscription` until this application's consumer shuts down.
```

`update_presence` is application logic, not a function provided by broadcast.

`laravel` is the native publisher boundary. Its compiled-in PHP
function calls `BroadcastService::publish` directly, which writes this crate's
dedicated multi-chunk Orbit publication ring. Laravel broadcasting therefore
does not open a loopback HTTP connection. The authenticated REST endpoint is
retained only for Pusher-compatible external clients.

## Channel departure

`pusher:unsubscribe` accepts object or JSON-string data. The session removes its
local subscription before invoking the same presence, channel and fleet
occupancy departure path used at disconnect. Unknown/repeated leaves do not
emit another departure. Final cleanup processes only remaining memberships;
it does not terminate another subscription or the socket.
