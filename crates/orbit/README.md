# claviron-orbit

Claviron lifecycle integration for Orbitive's same-host shared runtime state.

[Orbitive](https://crates.io/crates/orbitive) supplies framework-independent
primitives. This crate joins a Claviron process to its fleet at boot, binds shared
resources to their owners, bridges selected events, and connects application
transport and service placement to the runtime.

Use `claviron::orbit` with the root crate's `orbit` feature when working inside
Claviron. Use Orbitive directly when you need its primitives without Claviron's
service lifecycle. Orbit is not an Internet transport or a durable database.

## What is shared

Different data has different storage and delivery contracts:

- Cells and `Orbital<T>` hold typed current values; counters and locks provide
  their dedicated coordination operations.
- Arenas and cache primitives hold keyed state. The configured cache stores and
  backing-store policy are assembled by `claviron-cache`.
- Event rings retain bounded append-only history. Readers own cursors and must
  handle lag; a ring is not an ever-growing queue of authoritative records.
- Streams and Orbit Link carry application traffic between same-host peers.
- Invocation transport carries an operation and payload; its application owner
  defines the meaning and outcome.

These capabilities do not make all application data shared automatically. Each
resource has a declared owner, layout, capacity, and lifecycle.

## Lifecycle and registration

The standard server composition registers the common Orbit infrastructure.
Integrations can opt into resource providers with `register_cells`,
`register_counter`, `register_lock`, `register_arena`, `register_streams`, and
`register_invoke`. Register providers before boot, not from a request handler.
Arena boot policy can be selected explicitly with `register_arena_with_policy`.

`OrbitStateExt` adds a fleet accessor without placing that API directly on
`AppState`:

```rust
use claviron::{orbit::OrbitStateExt, runtime::AppState};

fn fleet_is_prepared(state: &AppState) -> bool {
    state.fleet().is_some()
}
```

The accessor returns `None` before the fleet provider has booted. Obtaining a
fleet handle is not a readiness check for every service in that fleet.

`Orbital<T>` is a managed shared value. It may be declared before boot with an
initial value; loading before binding returns that initial value, but mutation
requires a prepared binding. Clones refer to the same managed allocation.
Allocation and stale-handle errors remain errors rather than silently becoming
zero-valued state.

The [cell design note](../../docs/design/ORBIT_CELL.md) explains declaration,
binding and managed allocation lifetime. [Fleet awareness](../../docs/design/FLEETAWARE.md)
defines the same-OS scope; [Orbit Link](../../docs/design/ORBIT_LINK.md) describes
how independently running applications communicate within it.

## Quota stores

The opt-in `quota` capability prepares a `quota::Quota` from an explicit
`QuotaConfig`. It selects `LocalGuard`, `FleetGuard`, or `FleetQuota` from
Orbitive without defining HTTP identity or authentication. Local constructors
create independent budgets; clones share one budget. Matching SHM resource
identities attach to preserved fleet debt.

Every definition declares rate, burst, table geometry and CAS work budget.
Fleet modes additionally declare a quota-owned namespace/kind, owner capacity
and recovery budget. Keep that namespace separate from the runtime fleet and
other resources. Opening/dropping a store never resets or unlinks committed
debt. A store is an owned handle, not a runnable task or a background poller.

Guards may replace cold keys under table pressure; strict FleetQuota retains
live debt and may refuse new keys. Busy, capacity and clock failures never
grant permission. HTTP key selection and status mapping live in
[`claviron-http`](../http/README.md); host/config binding lives in Edge.

## Events and invocations

The process-local event bus and fleet event transport are distinct. Cross-process
events require a registered layout and codec; arbitrary Rust values are not
implicitly serialized or broadcast. Event payloads and retained history are
bounded.

Publishing an invocation submits work to the transport. It does not prove that
an application executed it successfully, and supplies no automatic replay,
acknowledgement, or exactly-once guarantee. Applications requiring durable
completion must model that separately.

## Link connections without HTTP

`claviron::orbit::link::Connection` owns one Link membership and its single
inbox reader. A Rust service can use it without a route, an HTTP exchange or
an application runner. The service supplies its Link namespace, layout, name,
role and execution incarnation; these are not inferred from an Edge process.
Opening a connection creates no HTTP body table and installs no background task.

`Connection::events` binds a caller-owned `FleetEventBus` to the connection.
The bus must support native SHM ring readiness; it may be the host's existing
fleet bus or an explicitly prepared standalone service bus. A plain in-memory
bus is rejected by the driver instead of introducing a polling fallback. Topics and bytes
keep their protocol owner's wire format: `ApplicationEvent` is not required.
Keep subscription guards alive for the keys the receiver understands. All
subscriptions use the same connection; they do not register named services.

```rust,no_run
use std::sync::Arc;
use claviron::{
    orbit::{FleetEventBus, link::{Connection, Incoming, LinkSpec, Routing}},
    runtime::RunContext,
};

async fn consume(
    spec: LinkSpec,
    incarnation: u64,
    bus: Arc<FleetEventBus>,
    context: RunContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::open(
        "example", spec, "example-service", "consumer", incarnation,
    )?;
    let events = connection.events(bus, |role| role == "source", Routing::Topic)?;
    let _subscription = events.subscribe(["example.policy.v1"]);
    // Other callbacks can publish through events.bus() while this reader runs.
    events.run_until(context.cancelled(), |incoming| async move {
        if let Incoming::Event { topic, payload } = incoming {
            // Validate and decode this protocol's payload in the owning service.
            let _ = (topic, payload);
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }).await
}
```

`run_until` uses native readiness, not a polling interval. Cancellation
withdraws admission and awaits the accepted handler before releasing the lane.
It does not reset/unlink shared resources or install a drain deadline. Frames
still queued when admission stops are not promised execution; protocols own
acknowledgement, revision and resynchronization. Publication commits to the
local ring, not to the remote service's durable state. Inspect `Incoming::Frame`
and `Incoming::Directory` when the protocol requires them. Interest is only a
routing hint, never an authorization boundary.

The existing application/HTTP protocol lives under `link::transport`; the
`orbit::transport` path continues to expose the same API. Its `ApplicationEvent`
codec, routing, kinds and geometry are unchanged. Application transport may
carry both exchanges and events, while an event-only connection opens no body
streams. Its peer advertisement describes reachability, not application health.

Current Link connections remain same-UID. A shared OS group alone does not
provide cross-user joining: resource names, ownership locks and wake addresses
are UID-scoped. Deployment permissions do not change that addressing contract.
External TLS/mTLS transport and cross-host topology are not supplied simply by
enabling this crate.

## Service placement

The optional-at-composition `placement` surface integrates runtime services with
fleet placement through `OrbitDispatcher`, `Dispatch`, and `DispatchPolicy`.
Local execution and fleet-wide placement are different choices. Lease-backed
placement declares capacity, lease lifetime, and renewal timing explicitly.

Losing a lease requests cooperative cancellation and drain. That alone cannot
undo external side effects or prevent a replacement from encountering earlier
work; application operations still need suitable fencing or idempotency.
Fleet service commands are accepted separately from the fresh execution
snapshots used to observe their results.

## Retained state and compatibility

Shared-memory segments can outlive every process that created them. Restarting
the server does not mean shared state has been cleared. Recovery and cleanup
must follow resource ownership, not assumptions about process exit.

Processes joining the same fleet must agree on its physical layouts and
capacity. An incompatible segment is not silently resized or unlinked. Changing
a record shape requires an explicit compatibility and retirement plan.
The logical server name, Orbit fleet name, and process incarnation are distinct.
The current lane allocation model belongs to one supervised process family,
not independently launched masters sharing a namespace.

There is no generic promise of database durability, unlimited retention, or
automatic schema migration. Choose persistent backing and recovery policies for
the data that needs them.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
