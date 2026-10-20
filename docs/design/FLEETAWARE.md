# A fleet lives on one OS

Design identifier: `FLEETAWARE`.

When we say fleet, we mean cooperating sibling processes on the same operating
system, not a cloud cluster. They can run in different processes and still
share bounded runtime state through Orbit. An application's useful boundary
does not have to be the boundary of one monolithic process.

## A sharing domain, not an address on the Internet

A fleet has an explicit name and compatible shared-memory layouts. Members
have node identities; worker and master identities additionally distinguish
process generations and incarnations where their owners require them.
The instance name, fleet name and hosted application name answer different
questions and must not be substituted for one another.
Under [INCARNATION_MASKING](INCARNATION_MASKING.md), applications use logical
roles and names while the runtime tracks their exact process owners.

The fleet provider joins the selected shared-memory domain during boot.
Individual resource providers own the tables, rings or streams used there.
Having a fleet handle does not mean every service has booted or every resource
is ready.

An edge and an independently running application can communicate through a
named [Orbit Link](ORBIT_LINK.md). The Link destination namespace is explicit;
it need not be the caller's own process-family namespace.

## What fleet awareness changes

A fleet-aware component knows which state is local, which admission or
coordination is shared, and which process owns the actual work. For example:

- Events cross processes through declared layouts/codecs, not by sharing an
  arbitrary Rust object or assuming a local event callback runs everywhere.
- A fleet-composed client pool can share resource admission while the physical
  client remains in one process. A peer reaches that owner through an exchange.
- A supervisor can reconcile a surviving worker by exact identity and an
  ownership handshake; a recently written heartbeat alone is not ownership.

Not every local service becomes a singleton by joining a fleet. Placement,
election, leases and side-effect fencing require their own explicit mechanisms.
Sharing a table also does not turn separate reads and writes into a transaction.

## Local has real boundaries

Members must be able to access the same OS shared-memory resources and agree
on their physical shape. Two machines with the same fleet name do not share
state. Orbit's local primitives do not imply cross-host routing, consensus or
database durability.

Segments may outlive their creators. Restarting a process does not clear its
fleet; removing a name while another process still maps it is not a coordinated
reset. Resource owners define reuse, recovery and retirement.

## Follow the implementation

- [Fleet provider and explicit namespace](../../crates/orbit/src/fleet.rs).
- [Orbit integration and resource boundaries](../../crates/orbit/README.md).
- [Link destination ownership](../../crates/orbit/src/transport/service.rs).
- [Worker adoption](WORKER_ADOPTION.md).
- [Hot client pools](HOT_CLIENT_POOL.md).

Use `FLEETAWARE` for this same-OS cooperation, not as a synonym for distributed
cloud execution.
