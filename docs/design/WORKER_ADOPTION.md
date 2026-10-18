# A new master does not imply new workers

Design identifier: `WORKER_ADOPTION`.

If a master exits while its workers remain useful, restarting it should not
automatically create a second set on top of the survivors. Nor should a fresh
master claim every PID it finds. Claviron separates evidence that a process
exists from permission to manage it.

Adoption keeps a surviving worker in its logical slot through an explicit
ownership transfer. It is not a scan for processes with familiar names.

## Presence is evidence, not ownership

A worker publishes typed presence including its exact process identity,
incarnation, master identity, phase and active operation. The kernel start
stamp distinguishes a process from a later process that reused its PID.
A heartbeat's age alone cannot prove that the old owner is dead.

Startup can reserve live slots above the configured worker floor so a survivor
is not overlapped or hidden. Reserving the slot still does not adopt the worker.

Reconciliation checks the exact process and the declared freshness policy.
For a transfer from an earlier master, it also requires running, non-busy
presence, a known original master, proof that master is dead, and an older
master generation. Refused reconciliation remains an explicit state; it is
not repaired by spawning into the occupied slot.

## The worker accepts the transfer

The successor names the exact worker identity in an ownership claim. The
worker validates the claim, updates its own master identity and acknowledges
the transfer. Only matching acceptance—or subsequent retained presence already
carrying that accepted identity—allows installation of the adopted process
handle in the master's authoritative worker table.

An acknowledgement for another slot, process, incarnation or ownership
transition cannot complete the pending claim. The slot is checked again when
the handle is installed so evidence cannot silently win a race against a
replacement.

Newly spawned workers likewise need their matching boot acknowledgement;
fresh metrics or a recently touched lane are not substitutes for readiness.

## Keep the exit proof

The supervisor retains an exact process handle and observes its exit before
reclaiming the slot. Cleanup of Link streams uses that process/incarnation,
not “everything with this worker number.” Adoption does not turn a non-child
process into an OS child of the new master.

Origin also matters during shutdown. In the current unbounded fleet-shutdown
policy, adopted processes are outside the successor's child exit barrier and
may remain alive; spawned children must be joined. Explicitly bounded shutdown
has its own escalation authority. The full policy belongs to
[Graceful ownership](GRACEFUL_OWNERSHIP.md), not a second rule hidden here.

## Process continuity is not every service's continuity

Adoption does not load new code into a surviving process. It also does not
prove that every cache, event cursor or service generation survived master
replacement coherently. Those resources need compatible layouts and their
own reuse/finalization contracts. Keeping a PID alive is not permission to
unlink its shared state and recreate something else under the same name.

The benefit is controlled continuity where the owners support it, not a promise
that any arbitrary process or binary version can be adopted safely.

## Follow the implementation

- [Ownership claims, acceptance and presence decisions](../../crates/supervisor/src/events/ownership.rs).
- [Topology recovery and authoritative slot installation](../../crates/supervisor/src/lifecycle/reconcile.rs).
- [Exact process handles](../../crates/supervisor/src/process/owned.rs).
- [Shutdown exit barriers](../../crates/supervisor/src/lifecycle/drain.rs).
- [Supervisor guide](../../crates/supervisor/README.md).
