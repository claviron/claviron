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

## Events and invocations

The process-local event bus and fleet event transport are distinct. Cross-process
events require a registered layout and codec; arbitrary Rust values are not
implicitly serialized or broadcast. Event payloads and retained history are
bounded.

Publishing an invocation submits work to the transport. It does not prove that
an application executed it successfully, and supplies no automatic replay,
acknowledgement, or exactly-once guarantee. Applications requiring durable
completion must model that separately.

Orbit Link adds named same-host application routing and request/body transport.
Its peer advertisement describes reachability, not the lifecycle or health of
the hosted application. External TLS/mTLS transport and cross-host topology are
not supplied simply by enabling this crate.

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
