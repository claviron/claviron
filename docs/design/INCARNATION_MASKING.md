# The role continues; its process can change

Design identifier: `INCARNATION_MASKING`.

A worker is a member of a fleet, not a particular execution of a binary.
`worker #3` names a logical slot: replacing its process does not create
`worker #4`. A surviving worker adopted by a new master remains `worker #3`.
We preserve the role and account for the changing owner behind it.

This names an existing design choice, not a new dispatch mechanism.

## Applications describe work, not the process that must host it

An application should not need to follow worker births, PIDs or master
generations to keep using a service. With `dispatch: one`, it asks for one
execution within the eligible scope, not execution on a particular worker.
The dispatcher coordinates ownership; the service retains its name when the
executing member changes. Named applications reached through
[Orbit Link](ORBIT_LINK.md) likewise do not require the caller to track PIDs.

This is the sense in which workers are private to the runtime. It is not
anonymity: diagnostics and management can expose worker numbers and exact
identities. Application correctness should not depend on one incarnation
continuing to occupy a role. Scope and dispatch are declarations of intent;
process placement is the runtime's responsibility.

## Mask outside; distinguish inside

The stable name is useful only if the runtime never confuses its successive
owners. These identities answer different questions:

| Identity | What it identifies |
| --- | --- |
| Fleet name | The same-OS sharing domain. |
| Worker number | A logical slot in its supervised process family. |
| Process identity and incarnation | The exact process currently occupying that slot. |
| Master identity and generation | The supervisor currently authorized to manage it. |

The supervisor qualifies a worker number with its PID, kernel start stamp,
incarnation and master identity. The start stamp defeats PID reuse. An old
heartbeat, drain completion or ownership acknowledgement must not act on a
replacement merely because it occupies the same numbered slot.

Transport and lease owners also retain the exact identities they need for
fencing and cleanup. Masking incarnation at the application boundary is not
permission to erase it at those ownership boundaries.

## Replacement and adoption are different transitions

On replacement, the supervisor proves the old process exited before reusing
its slot and node-owned lane. The replacement keeps the worker number and
receives a new incarnation. It must provide its own boot acknowledgement;
the predecessor's observation cannot make it running.

On adoption, the process did not die. Its worker number, process identity
and incarnation remain; an explicit, validated handshake changes its master
ownership. Adoption is not a reason to recreate its runtime or unlink shared
state it still uses. [WORKER_ADOPTION](WORKER_ADOPTION.md) defines the transfer.

The same separation applies to runnable dispatch. Shared service-slot keys
remain stable across process and service-generation replacement; lease owners
do not. A previous holder cannot renew or release its successor's lease.
Placement alone does not guarantee exactly-once external effects.

## The fleet is not one master's lifetime

We build as though the fleet will keep serving while its processes come and
go. The master manages that continuity; its own PID does not define it.
A successor joins compatible retained state and reconciles living members
instead of treating its boot as the birth of an empty system.

This keeps compatible SHM resources useful across runtime and master
replacement where their owners permit reuse. The OS sharing domain, rather
than a convenient process restart, is the outer operating boundary. Each
resource still defines its own shorter lifetime, reset and finalization rules;
[GRACEFUL_OWNERSHIP](GRACEFUL_OWNERSHIP.md) governs accepted work.

`NetId64` provides addresses within that resource model, not an immortal
identity for a process. For example, a stream address carries its creator's
node and the slot's generation. Exact ownership and generation checks keep
reuse from redirecting an old address to new work. We extend resource reuse
where it is safe; we do not extend a stale handle's authority.

Stable worker numbers do not migrate process-local memory, sockets or
in-flight streams into a replacement. A surviving adopted process can retain
what it still owns; an exited process's streams are recovered by its exact
ownership stamp. SHM persistence is not disk durability or a host-reboot
guarantee. These are [same-OS fleet](FLEETAWARE.md) contracts.

## Follow the implementation

- [Exact worker identity](../../crates/supervisor/src/process/identity.rs).
- [Rotation within the same worker slot](../../crates/supervisor/src/lifecycle/rotation.rs).
- [Identity-checked adoption](../../crates/supervisor/src/lifecycle/reconcile.rs).
- [Exit-owned Link cleanup](../../crates/supervisor/src/process/owned.rs).
- [Stable service slots and exact dispatch leases](../../crates/orbit/src/placement/controller.rs).
- [Exact-incarnation stream recovery](../../crates/orbit/src/transport/segment.rs).

Use `INCARNATION_MASKING` as the search key for this separation between a
continuing role and its replaceable process owner.
