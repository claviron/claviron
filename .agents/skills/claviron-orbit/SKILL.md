---
name: claviron-orbit
description: Add or change Claviron's same-host Orbit integration, shared state, event delivery, service placement, or Orbit Link. Use when process boundaries and SHM ownership matter; not for distributed networking or ordinary local state.
---

# Claviron Orbit integration

Separate the primitive from its runtime adapter. Orbitive owns bounded memory
primitives; Claviron binds them to service lifecycle and application routing.
Use `claviron::orbit` inside Claviron consumers and the public Orbitive facade
when working directly on its independent primitives.

## Choose the contract

- Use cells for typed current values, counters for atomic counts, and locks for
  declared mutual exclusion. A process-local `Arc` is not shared across workers.
- Use cache/arena state for keyed values. Do not store authoritative current
  state only in a bounded append-only event ring.
- Events have caller-owned cursors, bounded retention, and possible lag. Publishing
  an event or invocation does not prove execution or durable completion.
- Orbit Link addresses named same-host application peers. Stream bodies have
  bounded duplex flow and backpressure; Link presence is reachability evidence,
  not proof that the hosted application is ready.

Keep process-local callbacks distinct from fleet transport. Cross-process events
need the owning registered codec and layout; arbitrary Rust values are not
automatically broadcast. Use an existing envelope/bridge where one exists.

## Preserve ownership and memory compatibility

Register resource providers before boot. Distinguish logical server name, fleet
namespace, process incarnation, worker count, and immutable allocation capacity.
Independent masters must not assume they can claim the same lanes.

Segments survive process exit. Never unlink or reset a segment merely because a
server restarted, and never clear resources another live process owns. Check the
canonical owner and layout before maintenance; cleanup needs an explicit target
and authorization, with writers quiescent.

Do not invent a SHM kind ID or grow a live kind's record shape in place. A layout
change needs an explicit allocation/compatibility/retirement decision. Consult the
canonical kind index when available; if ownership cannot be established, resolve
that before changing the layout rather than guessing from a retired label.

Use the native wake/readiness path. A wake is coalesced readiness: drain it, then
read through the caller-owned cursor. Cancellation must set its observed state
and wake the wait; do not add a sleep loop or arbitrary timeout to escape it.

Lease loss requests cancellation; it does not reverse financial or external
effects. Keep fencing/idempotency in the owning operation where required.

## Source map

Start with `crates/orbit/README.md`, the owning public types, and the affected
transport/resource implementation. If present, `crates/orbit/ARCHITECTURE.md`
contains the canonical kind index. For direct primitive changes use Orbitive's
own README and protocol contract. Describe benchmark boundaries; do not turn a
transport microbenchmark into a blanket production speed or zero-copy claim.
