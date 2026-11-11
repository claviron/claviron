# claviron-transport

Accept connections, drive HTTP and drain accepted work. This crate depends on
HTTP and runtime contracts, never on Edge. Edge and runner supply adapters to
the same named `Listener` Runnable.

## Listener lifecycle

`listener.rs` owns `Listener<A>` and `Adapter`. The adapter supplies prepared
configuration, dependencies, optional reload and serving composition. The
Runnable owns execution selection and worker/standalone start policy. Every
instance is managed as `Listener:<id>`; the master accepts no connections.
`instances.rs` preserves declared and draining instance identities.

Configuration is `claviron_http::config::ListenerDefinition`, resolved once to
`ListenerConfig`. TCP, Unix and QUIC use the same named declaration and
`inherit`/`dedicated` execution choice. Dedicated uses Continuo's
`Dedicated(CurrentThread)` for this runnable instance. `single.rs` composes the socket
drivers for one request owner. `owner.rs` defines that owner's body and request
contract. Virtual-host selection and host pipelines belong to Edge's adapters.

## Socket drivers

- `tcp/listener.rs`, `tcp/listener/accept.rs`: bind groups, admission, accept recovery and drain.
- `tcp/plain.rs`, `tcp/prior.rs`: cleartext H1 and H2 prior knowledge.
- `tcp/secure.rs`, `tcp/tls.rs`: TLS selection, ALPN and certificate input for one owner.
- `tcp/mod.rs`, `tcp/body.rs`: connection-local requests, session offers and native body adaptation.
- `tcp/io.rs`, `tcp/protocol.rs`: TCP readiness, socket settings and Hyper builders.
- `unix.rs`: Unix socket acceptance using the same cleartext snapshots and protocol drivers.
- `unix/shared.rs`: master-owned bind and inherited worker descriptors.
- `geo.rs`: optional connection-time peer country lookup.

Unix listeners use `transport: unix` and `listen: unix:<path>`. TLS is not
available on this transport. Their context identifies `ListenerTransport::Unix`
and uses loopback as local ingress identity; forwarded client identity is
request-specific. Socket permissions remain the Unix ingress trust boundary.

In supervised mode, configuration registers a lazy descriptor owner. Immediately
before spawning the first worker, the master binds the socket once. Every worker
inherits a duplicate across exec and duplicates its retained FD for each listener
generation. A worker neither rebinds nor unlinks the shared path. Standalone
listeners remove only the socket file they created, after drain.

A replacement master cannot recover an FD from surviving workers yet. It refuses
a live path rather than unlinking or replacing it. Worker restart under the
original master reuses its retained socket. Shared paths may remain after process
exit; the next bind removes one only after a refused connection proves it stale.

## QUIC

- `quic/listener.rs`: endpoint groups, connection admission, stream and session driving.
- `quic/single.rs`: one-owner composition.
- `quic/config.rs`: declared transport settings.
- `quic/request.rs`: H3 framing, body adaptation and response I/O.
- `quic/wt.rs`, `quic/wt/streams.rs`: WebTransport session authorization and bounded stream delivery.
- `quic/error.rs`, `quic/drain.rs`: closure classification and owned child drain.
- `quic/mod.rs`: public driver contracts.

## Orbit and sessions

`orbit.rs` drives an application's Orbit Link lane. `websocket.rs` adapts an
owner's whole-message sessions to socket WebSockets. These use the same owner
contract; they introduce no virtual-host configuration. Orbit joins with its
optional `max_in_flight` bound and its `max_queued`, publishing their sum as
the admission window and running `max_in_flight` at once through the
shared `WorkQueue`; the rest wait in arrival order and start, without a wake,
when a running exchange ends. It uses the shared `ApplicationReceiver` and
`accept_frame` contract; accepted work retains its slot through body/session
completion; queued work is still run when the receiver drains at shutdown. Intake continues for
events/control even at capacity, and withdrawal stops routing before drain.

## Tests

Tests mirror source modules under `tests/`. Parsing, publication identity and
isolated state transitions belong here. Real listener and process inheritance
acceptance runs belong to an independent executable harness.

Shared descriptor declarations are sealed at the first worker spawn. Adding a
Unix bind or changing its socket settings requires a process-family restart;
policy reload on an existing bind reuses its descriptor. Retiring a listener
releases retained copies without closing active generation copies or unlinking
another process's path.

The Orbit door shares `claviron_orbit::config::LinkCapacity` with its configuration
owner. The controller retains weak lane references, so it cannot prolong a
receiver's lifetime. Joining and changing the bound are serialized only on this
cold control path; request dispatch takes no controller mutex. Inbox, body table,
master observation and management publication use the same frozen Link geometry.

Orbit ingress keeps count management separate from dispatch. A master's normal
directory watcher consumes no application lane. Explicit idle retirement adds
one management lane for existing capacity-interest wakes; disabled retirement
keeps the watcher-only topology. The process owner supplies exact removable
PID candidates and receives a process-local idle event. Transport never spawns,
selects or kills a process; the runner or command controller owns that action.
