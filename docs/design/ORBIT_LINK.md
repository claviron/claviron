# Separate processes without another local HTTP hop

Design identifier: `ORBIT_LINK`.

An application does not have to be embedded to participate in Claviron. You can
keep it in another process, select a named fleet and application, and let the
edge reach it over shared memory. That keeps process and runtime choices
separate from the public HTTP connection.

[Orbitive](https://crates.io/crates/orbitive) supplies Link and asynchronous SHM
stream primitives. Claviron adds application naming, HTTP exchange frames and
lifecycle integration. It does not connect two local HTTP servers through a
socket and parse the same HTTP wire protocol a second time.

## Names find peers; streams carry bodies

An application advertises its name and runtime in a Link lane. An edge worker
reuses an application client for applications in the same destination fleet;
different destination fleets have independent clients. Routing identity is
separate from a management command's runtime-qualified target identity.

A request head travels in a bounded inbox frame, with correlation information
and a stream ticket. Request and response bodies use the paired directions of
a duplex SHM stream. A stream's window provides backpressure, not an assertion
that the complete body must fit in that window.

Full inboxes are refused, not overwritten. There is no successful submission
whose meaning is “an earlier request disappeared to make room.” An inbox write
proves acceptance into the link, not successful application execution.

The geometry and frame formats are shared declarations. Peers that map the
same namespace must agree on them. A change in physical shape is a compatibility
change, not something one side can deploy by silently resizing a live segment.

## Selecting application workers

Each Orbit destination declares its receiver selection, `balance`,
independently of the application's worker policy:

```yaml
orbit:
  - application: example-app
    fleet: apps
    balance: round_robin
```

- `first` is the default and preserves existing selection: prefer this
  caller's home receiver while it can start work immediately. When all bounded
  receivers are busy, report growth pressure and try the less loaded of two
  receivers before falling back to the remaining admission windows. Unbounded
  receivers use two-choice load selection.
- `round_robin` rotates through receivers for each selection, skipping gone or
  saturated receivers. It first looks for a receiver that can start immediately;
  when none can, it reports growth pressure and tries queue capacity in cyclic
  order. A full fleet is still refused immediately. The cursor belongs to the
  prepared target in each caller process, is shared by its clones, and is not
  a fleet-wide request counter or a guarantee of equal CPU use.

New and departed receivers are reflected by directory notifications. A
`balance` change takes effect when the host/site is prepared again;
already-published exchanges stay with their original receiver. No queue budget,
retry, admission limit, stream ownership or cancellation rule changes with it.

In Rust, use `ApplicationClient::target_with_balance(application, LinkBalance::RoundRobin)`.
`target(application)` and direct `send` retain `first`. The enum is shared
pure data, re-exported from `claviron::orbit::link::application` and the HTTP route
configuration; schema-only builds do not acquire a Link runtime dependency.

`workers.policy: dynamic` has a different job: the process owner starts more
workers after unmet demand is reported. Either `balance` can use that same
growth policy, or run against a static worker set.

## Receiver-owned admission

`max_in_flight` limits how many exchanges one application receiver runs at
once, across every caller. `None` remains unbounded. A bounded receiver also
declares `max_waiting`: how many it admits in all, running ones included (PHP's
`max_waiting` counts the same way); those beyond `max_in_flight` wait in the
receiver, in arrival order, and start as running ones end. `max_waiting` is
the admission *window*: `max_in_flight <= max_waiting`, and it stays below the
inbox capacity, which also carries control frames. `max_waiting` equal to
`max_in_flight` refuses as soon as every running place is taken. This is distinct from inbox depth,
stream byte windows and the number of body slots a caller can originate.

A caller reserves a window slot before obtaining a body slot. It prefers its
home receiver while that receiver will start the exchange at once; otherwise
the exchange will wait in some receiver's queue, which is reported as unmet
demand (a growth policy may add a worker), and the less loaded of two receivers
is tried. A caller never waits for receiver capacity: when every window is
full the exchange is refused at once and answered 503. Waiting happens in the
receiver's queue, where the end of one exchange starts the next without a
wake. The route's `requests.queue_timeout` is one budget for the whole wait
before the application starts: the caller's own body slot, then the receiver's
queue. When it ends with the exchange still queued, the caller withdraws it
atomically; if that wins, nothing ran and the answer is 503, otherwise the
receiver started it first and its reply is still owed.

The receiver accepts admission when it starts an exchange, not when the frame
arrives, and holds it through the response body or accepted message session. Its reader keeps processing events
and control at full capacity. Unpublished reservations roll back on cancellation;
accepted work is released by its receiver. Recovery uses confirmed process exit
and exact lane ownership, not age or a heartbeat. Native bindings use the same
frame admission contract; their declared retained-exchange budgets also retain
credit until the foreign handle is released.

Admission changes the inbox layout and its body-table membership domain. Edge,
runners and native bindings must be rebuilt with matching `LINK_SPEC` geometry.
Retained mappings from the preceding domain are not resized or cleared.

## A connection signal is not application health

The directory describes reachability. A connected lane alone does not prove
that its hosted application is healthy, ready for every operation, or owned by
the caller's supervisor. Conversely, a configured route can exist while its
application has no connected lane.

Application-event forwarding is explicit: a registered envelope/codec, routing
and live subscription interests determine what crosses. A readiness wake is
not the event payload or an acknowledgement of its business effects. Bounded
rings and inboxes do not become durable queues or exactly-once delivery.

## Recover the owner that actually left

Stream cleanup is tied to the exited process and its incarnation. A request
timeout, a missed pulse or a lane being withdrawn is not permission to reclaim
another live owner's resources. Supervisor-confirmed exit can recover that
exact owner's streams without closing a sibling's or a newer incarnation's.
This is the link side of [INCARNATION_MASKING](INCARNATION_MASKING.md):
stable names do not make successive process owners interchangeable.

Shared-memory mappings can survive processes. Reusing or retiring them must
follow the resource owner's policy; unlinking a name while peers retain the
old mapping is not a coordinated fleet restart.

This is a [same-OS transport](FLEETAWARE.md), not an Internet protocol or a
cross-host trust boundary. The edge still owns public TLS/HTTP admission; the
application still owns its behavior and completion semantics.

## Separate OS users without sharing the whole fleet

Owner-only is the default. A dedicated Link can instead declare an explicit
resource owner, OS group and IPC directory through `link::Access::Group`.
Only its inbox, duplex streams and ownership/wake resources are shared; the
processes keep their own private event rings, auth/cache state and files.
Kernel privileges stay with the account that performs the privileged work.

The owner provisions first. Other users attach existing resources, and every
link/body/recovery path retains that same scope. Group access is never a
fallback for an owner-only failure. The managed caller registers access per
destination before opening its first client; runtime policy replacement is
not supported. Group writers are trusted with Link state: an advertised name
or role is not authenticated source identity. Domain authorization remains
with the consumer.

## Follow the implementation

- [Connection access policy](../../crates/orbit/src/link/access.rs).
- [Layout and addressing](../../crates/orbit/src/link/application/mod.rs).
- [Destination clients and lifecycle](../../crates/orbit/src/link/application/service.rs).
- [Inbox ownership and exact-process recovery](../../crates/orbit/src/link/application/segment.rs).
- [Duplex body streams](../../crates/orbit/src/link/application/body.rs).
- [Explicit application-event bridge](../../crates/orbit/src/link/application/app.rs).
- [Orbit integration guide](../../crates/orbit/README.md).

## Process counts and receiver selection

A caller route's `balance` selects `first` or `round_robin`; the application
process owner selects its count. A runner (`orbit.link.workers`) or a Link
task (`tasks.<name>.link`) can declare
`workers: { policy: static, count: 4 }`, or
`workers: { policy: dynamic, min: 4, max: 16, step: 1, retire_idle: off }`
(`on_pressure` is the earlier spelling of `dynamic`, still accepted).
Static and growing applications use the same independent `balance`.
`dynamic` grows only over receivers that declare `max_in_flight`: an unbounded
receiver never reports pressure.

A Link task is worker policy for applications without a supervisor of their
own, such as a binding's process: `kind: worker` grows the fleet's processes,
`kind: runner` starts one runner that owns its workers. A fixed count needs no
Link task; a plain command may be launched by any ordinary process owner and
serve as many instances as that owner starts.

Idle retirement is opt-in with a declared `idle_timeout`, requires tracked
admission and uses existing capacity wakes. It is the owner's graceful removal
request, not a new application or language ABI operation. See the Orbit, runner
and Task architecture contracts for ownership and lane accounting.
