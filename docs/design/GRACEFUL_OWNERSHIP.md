# Graceful ownership

Contract identifier: `GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION`.

This is Claviron's canonical design contract for graceful lifetime ownership.
It is a runtime policy, not a protocol standard. Crate documentation describes
its local application and links here rather than defining another version.

## The design choice

The component that accepted work owns its drain. Requesting shutdown does not
give another component unrestricted authority to abandon that work, interrupt
an unrelated execution, detach a process, or invent a completion deadline.

An operator may choose bounded shutdown for availability or indefinite drain
for work that must complete. The runtime preserves that choice instead of
silently deciding which risk is acceptable.

## Canonical statement

> **No deadline, no kill.**
>
> The protected choice is an indefinite drain: `graceful_timeout: None` for a
> service and `shutdown.timeout: None` for a worker process. It must survive
> every layer between configuration and the drain. The contract applies at
> both service and process level, not only to worker lifetime.
>
> Rust execution retains ownership of accepted work until its drain completes
> or an explicit policy authorizes escalation. Hosting foreign execution does
> not remove the host's Rust drain obligation; it adds a guest-execution
> boundary whose interruption and completion need their own explicit policy.

Once configuration has produced an `Option<Duration>`, runtime code preserves
it. `None` is a complete policy value, not a missing setting to repair with a
compiled constant, `unwrap_or`, a provider-local default or a hidden fallback.

| Selected grace | Meaning |
| --- | --- |
| `None` | Wait indefinitely; do not infer escalation authority from elapsed time. |
| `Some(duration)` | After that bound, the owner may abandon or force its remaining work using its declared mechanism. |

Not every runnable needs a grace setting. A listener or ticker with no retained
work may observe cancellation and return immediately. An owner of in-flight
requests, task executions or processes exposes a grace policy where bounded
versus unbounded drain is meaningful.

## Different clocks, different authority

Graceful drain, execution deadlines, explicit hard-kill actions and intentional
process detachment are separate contracts. None silently borrows the authority
of another.

- A drain bound starts from lifecycle cancellation and concerns the work that
  owner already accepted.
- An execution budget such as `execution_timeout` bounds one execution, whether
  or not shutdown has started. It is armed and disarmed around that execution;
  it is never derived from a drain deadline.
- An explicit kill or keep-running/detach action retains its own declared scope
  and policy; it is not an implicit interpretation of an absent grace value.

An expired grace authorizes action only against the expiring owner's work.
One service's drain must not trigger a process-wide interrupt that also ends
another service's executions.

## Rust execution and foreign execution

A Rust runnable represents accepted work with awaited futures, process handles
or RAII-backed in-flight ownership. Its `run()` future does not report graceful
completion while that work is still draining. Blocking or CPU-bound work needs
an appropriate execution owner; hiding it inside an async future does not make
it cooperatively cancellable.

The reason to preserve indefinite drain is not merely convenience. Accepted
work can contain a domain transition, transaction boundary, idempotency key or
outbox operation. Whether to abandon it is an explicit operator decision.
Application correctness must nevertheless not depend on a PHP request, Rust
future or worker process surviving termination.

An embedded guest does not promise prompt cooperative cancellation. PHP can
observe a VM interrupt between opcodes, including in a PHP loop, but an OS/ZTS
thread blocked in a native call may not return to an interrupt point. The host
must not claim otherwise or treat a timeout report as proof of thread exit.

For embedded PHP, an explicitly bounded drain may request an interrupt scoped
to the owning execution or service generation. This is distinct from its
separately configured execution budget. An unbounded drain never acquires that
interrupt authority just because time has passed. Thread handles remain owned
until teardown completes; a bound does not make arbitrary foreign work safely
preemptible or authorize detaching the thread.

## Worker processes

The supervisor may send SIGTERM to begin a worker's graceful drain. In the
worker-drain path, authority to send SIGKILL comes from an explicit
`shutdown.timeout: Some(duration)` after that duration has elapsed. `None`
does not synthesize such authority.

This policy applies to initial-boot rollback, heartbeat recovery, rotation,
manual stop, scale-down and full shutdown. Dropping a supervisor is not an
alternative signal path; process teardown is an explicit asynchronous lifecycle
operation. Whether a spawned or adopted process belongs in a master's shutdown
barrier remains a separate process-ownership decision.

An indefinite drain may block rollback, recovery, reload or shutdown forever.
That availability cost belongs to the operator who selected it. The operating
system and its service manager remain the final external liveness boundary;
their policy must also permit indefinite drain if that is the desired outcome.

`FLEET_CONTINUITY` concerns the fleet, not an immortal PID. Individual workers
and services may drain, restart or rotate while the management plane retains
live capacity. Graceful ownership does not prohibit those lifecycle operations.

## Adoption is not service-state adoption

Keeping a process alive does not guarantee that every service resource, cache,
event stream or communication channel survives master replacement unchanged.
Those continuity guarantees require the resource owner's explicit support.

A shutting-down master may run its providers' `Finalizable` hooks once the
applicable shutdown policy permits it, including when a separate ownership
policy leaves adopted processes alive. A successor may finalize its own
providers when it later shuts down. Before forking new workers it may perform
resource-owner-defined initialization or cleanup. None of this authorizes
finalization during ordinary operation, worker rotation or a targeted service
stop, nor may it bypass an outstanding required drain.

Shared memory illustrates the distinction: unlinking and recreating a named
segment may leave surviving peers on an old mapping while new peers use the
new one. Memory safety, event delivery and cache coherence have their own
contracts; process survival alone proves none of them.

## Implementation map

- [Runtime](../../crates/runtime/README.md):
  submission, awaited drain and observable Gate outcomes.
- [Supervisor](../../crates/supervisor/README.md):
  exact process ownership, worker drain and escalation.
- [Tasks](../../crates/tasks/README.md):
  cancellation scopes, per-task grace and external process actions.
- [PHP Embed](../../crates/php-embed/README.md):
  execution ownership, VM interruption and joined OS-thread teardown.
- [Logging](../../crates/logging/README.md): producer-before-writer drain.
- [Upstream](../../crates/upstream/README.md): outbound profile drain
  remains distinct from an inbound listener's lifetime.

Use `GRACEFUL_OWNERSHIP` as the stable search key in code and documentation;
use `NO_IMPLICIT_TERMINATION` for this contract's no-invented-escalation rule.
