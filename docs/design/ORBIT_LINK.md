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

## Receiver-owned admission

`max_in_flight` limits one application receiver's reserved and accepted exchanges
across every caller. `None` remains unbounded; a declared positive limit must fit
`LINK_INBOX_CAPACITY`. This is distinct from inbox depth, stream byte windows and
the number of body slots a caller can originate.

A caller reserves target capacity before obtaining a body slot. When all targets
are busy, pipeline dispatch waits on a capacity notification, subject to the
existing `requests.queue_timeout` if set. Direct `send` stays fail-fast;
`ApplicationTarget::send_wait` provides notified waiting without a timer or local
overflow queue. Dropping a wait has not submitted work.

The receiver validates admission before execution and holds it through the
response body or accepted message session. Its reader keeps processing events
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
