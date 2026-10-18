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

- [Host snapshots, preparation and unchanged-host reuse](../../crates/edge/src/pipeline/apps.rs).
- [TLS policy preparation](../../crates/edge/src/tcp/secure/tls.rs).
- [Request-time admission](../../crates/edge/src/service/host.rs).
- [Managed cell binding and operations](../../crates/orbit/src/cell/orbital.rs).

Use `LIFETIME_POLICY` when moving work between these stages, not as shorthand
for removing a safety check.
