# Fleet Client Admission

Status: proposal, not implemented.

A client pool in each process bounds what that process sends. A fleet of
forty workers then sends forty times the bound, which is wrong wherever the
limit belongs to the remote side: a certificate authority limits an account,
not a worker. This note proposes sharing one request budget across the fleet
while every physical client stays a cheap, process-local object.

## What is shared and what is not

| | Where it lives |
| --- | --- |
| Physical clients (an HTTP client, its connections) | Each process, in its own `ClientPool` |
| The request budget for a key (`max_concurrency`) | The fleet, in one Orbit pool table |
| Waiting callers (`max_waiting`, `acquire_timeout`) | Each process, as the local pool already does |

A request first takes a fleet admission permit for its key, then a lease from
its own process's pool, and returns both when the request ends. No bytes cross
processes and no client moves: unlike the exchange composition in
[Hot client pool](HOT_CLIENT_POOL.md), no process uses another's client, so
no exchange segment is opened. That composition stays for protocols whose
physical client is scarce; for HTTP every process can build its own.

The permit spans one request, not one operation. An ACME order is many
requests; two orders interleave through the budget, and neither waits for the
other to finish. The pool is the only coordination; no lock is added.

## Orbit pieces

- An Orbit pool table opened with `PoolSpec::new(kind, key_capacity,
  lane_capacity).with_fleet_concurrency()`. `Pool::admit(key,
  max_concurrency)` admits one request fleet-wide and returns an
  `AdmissionPermit`; dropping it returns the admission and wakes one waiter.
- A `Key` per budget: a 128-bit digest the consumer names, such as its name and
  the remote authority (`acme` with the CA's directory URL). Several consumers
  share the table, each under its own keys.
- A confirmed `node_dead(node, incarnation)` returns the permits a dead process
  held. The pool checks no liveness; Claviron's supervisor reports deaths it has
  confirmed. Without that report a crashed worker's permits stay taken.

## Claviron pieces

1. **The table.** A managed SHM resource like the counter table: one new
   kind, its geometry validated on reuse, kept across boots, joined by every
   process of the fleet. The kind takes a row in the SHM Kind ID Index and its
   cleanup row.
2. **Deaths.** The master reports each confirmed worker exit to the table with
   that worker's node and incarnation, from the exit cleanup that already
   recovers Link streams (`OwnedProcess::recover_orbit_on_exit`),
   which spawned and adopted workers share.
3. **The API.** Beside `ClientPool`, a fleet-admitted pool: a key, the local
   pool's options and the fleet `max_concurrency`, from `HttpClientConfig`.
   `acquire` returns one lease holding both the permit and the local client.
4. **Scope in the declaration.** `PoolConfig` names its scope, `process` or
   `fleet`, and the scope is required: it changes what the limits mean.
   `process` is the local pool alone, its bounds per process. `fleet` adds the
   fleet admission and is refused, not narrowed, where no fleet exists. The
   same configuration then fits a single process, a fleet, or a later
   standalone service.
5. **Consumers.** ACME first: its client goes behind the fleet-admitted pool,
   a permit per request, and its renewal may then run in any process. Domains
   follows with its registrars.

## Decisions

| Question | Proposal |
| --- | --- |
| Kind | 175, the next free one going down; checked against the index and retained segments before use. |
| Geometry | `lane_capacity` from the fleet's lane count; `key_capacity` a declared fleet setting, since the number of budgets is the deployment's. |
| Same limits everywhere | Every process must use one `max_concurrency` per key. Configuration is fleet-wide and reloads together; a rolling restart briefly runs mixed values, which the table tolerates, as it enforces each atomic claim. |
| Rate-limit pause | Stays process-local, as Orbitive documents for fleet pools. The fleet budget bounds total load; a fleet-wide pause needs a shared flag of its own and is not proposed here. |
| Warm clients | None shared: `min_idle` stays a local floor. HTTP clients are created on demand. |

## Not in scope

Forwarding a request to the process that owns a client, a fleet-wide idle
policy (`with_fleet_availability`), and canonical limits stored in the table
rather than in each process's configuration.
