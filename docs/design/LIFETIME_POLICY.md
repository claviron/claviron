# Do the work at the right lifetime

Design identifier: `LIFETIME_POLICY`.

Use the time before the request. A choice known at compilation should not need
runtime discovery. A configuration that can be resolved at boot should not be
interpreted on every request. A prepared application should be served, not
rebuilt, when traffic arrives.

That is what lifetime means here: finding the right owner and moment for work
so the hot path stays small **without becoming less safe**. Shutdown authority
has its own contract in [Graceful ownership](GRACEFUL_OWNERSHIP.md).

## Spend preparation once

| Stage | What belongs there |
| --- | --- |
| Compile time | Select optional capabilities and types; establish layouts that communicating binaries must agree on. |
| Boot | Resolve configuration and provider dependencies, validate declarations, prepare application bindings and resource owners. |
| Reload / publication | Prepare changed configuration and application generations before publishing them to readers. |
| Connection acceptance | Select connection-owned TLS, host and session capabilities. |
| Request / operation | Check facts that belong to this request and execute it against prepared state. |

These are places to put work, not a demand to make every value static. A
configuration-dependent decision cannot be moved to compilation merely to
remove a branch. Reloadable state needs a publication boundary rather than
being frozen forever at first use.

## A concrete example: host applications

The host application registry prepares routing outside request dispatch and
publishes an application snapshot with the host table. Factories do not run
because a request arrived. An unchanged host can reuse its prepared application
when another host changes.

Readers retain the publication they accepted; replacing the published table
does not mutate an application under an in-flight request. This shifts setup
away from traffic while keeping the old generation alive for its users.

The same direction appears in [hot client pools](HOT_CLIENT_POOL.md): retain
and maintain usable resources instead of treating every operation as a fresh
setup. It also appears in [managed cells](ORBIT_CELL.md), where declaration and
binding are separate from value operations.

## A concrete example: connection state

Some of what a request needs is settled when the handshake ends: who the peer
is, which host was selected, and whatever follows from the two. Deriving it
again for every request, or looking it up in a shared table by connection,
spends request time on a connection-time answer.

An accept layer does that work once. It is registered before the first
publication and selected per host at boot and reload. When a connection of a
selected host is accepted and its facts are complete, the layer receives them
and puts its typed result into the connection's `ConnState`. Every request of
that connection borrows the result through `HostContext::conn_state`: no
lock, no lookup, no allocation, nothing written into the request.

```rust
registry.register_accept_layer(
    |host, _config, _protocol| host == &management,
    |accepted: &Accepted<'_>, state: &mut ConnStateBuilder| {
        if let Some(peer) = verified_peer(accepted.connection) {
            state.insert(Session::new(peer));
        }
    },
);

// Later, for each request of that connection:
let session = context.conn_state().get::<Session>();
```

The cost sits where the use is. A build that registers no layer pays one
empty registration list at boot. A host no layer selected carries none, so
accepting its connections runs nothing and allocates nothing. A selected
host's connection pays its layers once, however many requests follow.

What the edge provides is the moment and the place. What a layer derives and
keeps is its own business; the state is keyed by the layer's own types and
read only by code that names them.

Two limits follow from where the evidence is. The state is written at
acceptance and never afterwards, so it holds what is true of the whole
connection; what changes per request stays with the request, and what belongs
to one WebTransport, CONNECT or WebSocket session stays with that session's
owner. And a layer runs when the host is known, which is after a TLS or QUIC
handshake. A cleartext connection names its host per request and may name
another next time, so nothing host-bound is prepared for it.

## Keep checks at the lifetime of their evidence

Preparation is not permission to skip request authority, live access policy,
capacity admission or stale-handle checks. The handshake can establish a host;
each request must still match it. A pool can retain a connection; the protocol
must still decide whether that connection is reusable.

Do not make a fast stale answer out of a check whose input changes. Conversely,
do not repeat configuration parsing, application construction or global
registry discovery when an already-owned typed handle answers the question.

## The cost remains visible

Prepared state consumes memory and may keep an old generation alive during
drain. A warm pool consumes real resources. These costs are deliberate and
bounded by their owners, not hidden background work with invented intervals.
There is no blanket promise that a hot path allocates nothing or takes no lock.

When reviewing a new operation, ask: **what is the earliest lifetime at which
its inputs are known, and how is a later change published safely?**

## Follow the implementation

- [Host snapshots, preparation and unchanged-host reuse](../../crates/edge/src/host/apps.rs).
- [TLS policy preparation](../../crates/edge/src/tcp/secure/tls.rs).
- [Accept layers and connection state](../../crates/edge/src/host/accept.rs).
- [Request-time admission](../../crates/edge/src/service/host.rs).
- [Managed cell binding and operations](../../crates/orbit/src/cell/orbital.rs).

Use `LIFETIME_POLICY` when moving work between these stages, not as shorthand
for removing a safety check.
