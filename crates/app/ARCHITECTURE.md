# claviron-full Architecture

`claviron-full` is the user-level application control boundary. An application
is code hosted by Claviron, not one of the infrastructure services that makes
Claviron run. The downstream full reference server uses a distinct Cargo
package identity, so this concise domain name remains unambiguous.

## Boundary

The crate owns:

- typed application runtime/name identity;
- the language-neutral request admission and execution budget vocabulary;
- `ApplicationEvent`, its optional runtime-qualified application target,
  validated dynamic topic, fleet envelope ID, and lossless language-neutral
  value payload;
- the common application lifecycle and pool status vocabulary;
- `ApplicationController`, implemented once by each real execution runtime;
- `ApplicationManager`, which indexes runtime controllers and controls their
  applications without copying live state.

It does not own HTTP routing, PHP or JavaScript engines, daemon supervision,
Claviron server/worker lifecycle, or Orbit bootstrapping.

## State ownership

`ApplicationDescription` is the common execution introduction: runtime,
framework list, runner, request mode and optional symbolic icon code. These are execution-owner facts,
not the runtime-qualified control identity or a connection method. PHP reports
`application`, Gufi reports `ASGI`, and Web-style handlers report `fetch`.
WSGI/ASGI describe call interfaces, not whether a framework is booted once.
Explicit framework lists win, including an empty list that disables discovery.
Each language adapter discovers its own list from the application's package manifest during preparation,
never guess it from the application name. Descriptions are prepared with
definitions and included in management observations only;
HTTP dispatch performs no discovery, serialization or extra lookup.

Framework names are free-form informational tags, not executable capabilities;
multiple names remain distinct rather than collapsing to `Mixed`. The icon is
a client-owned code, never a filename, URL or runtime instruction. Clients map
known codes to their bundled assets or system icons and fall back to the runtime
icon for absent/unknown codes. These fields use the existing bounded description
budget; transport geometry and cache/HTTP hot paths are unchanged.

Each placement retains its own description, so a mixed deployment does not
silently take one worker's metadata as the whole application's truth.
Embedded and linked applications use this same snapshot and controller
vocabulary. Linked is a remote control relationship; Orbit is the currently
implemented connection method. Ordinary HTTP/FastCGI upstreams remain a
separate domain and acquire no start/stop authority merely by being reachable.

The manager contains one controller handle per runtime, such as `php` or `js`.
A controller returns every application in the common snapshot format, reads
status from its authoritative engine service, and sends commands there. New
application definitions therefore do not mutate the manager registry. The
manager must not cache a second enabled flag, generation, pool, or error state.

Infrastructure `Runnable` services are not applications automatically. A Rust
`Runnable` becomes visible as an application only when the downstream server
explicitly registers an application controller for it.

## Orbit

Claviron owns Orbit initialization, node identity, SHM/ring lifecycle, and
master/worker membership. This crate defines the user-level application event
codec and may consume the already-registered Claviron Orbit providers to carry
application events, commands, and snapshots. It must never initialize, reset,
or own Orbit itself. PHP, JavaScript, and Rust adapters translate their native
values to this contract rather than defining competing fleet envelopes.

One event codec and one fleet ring carry both application-private and explicit
shared events. A targeted frame names the full runtime/application identity;
adapters discard a non-matching target before user code sees it. A targetless
frame is deliberately shared and is accepted only by topics the receiving
adapter registered. Targeting is routing metadata in the envelope, not a field
forced into the framework event payload.

Fleet application commands carry the full runtime-qualified application id,
so identically named PHP and JavaScript applications remain distinct. The
process event listener applies a command only where that runtime and
application are registered; the master and intentionally absent placements
ignore it. Actual controller failures remain errors. Supervised registration
requires Claviron's existing `FleetEventRegistry`, while standalone mode keeps
the same local command path without creating an Orbit transport.

`link` translates the existing transport directory's `EdgeConnected` and
`ApplicationConnected` into process-local guest observations on
`claviron.edge.connected.v1` and `claviron.application.connected.v1`. It uses
the same inbound-only delivery path as departures. These notices are never
published into a fleet ring or forwarded across another Link. A process sees
only its direct opposite-side lanes, including peers already present at its
first directory observation; incarnation changes remain transport-owned.
The payload's fleet is the Link segment's fleet, role is the transport role,
and PID is nullable when the directory has no owner. No remote process role or
source fleet is inferred. The former transponder query/reply topics and
automatic responder are removed: an edge with N workers and an application
with M workers needs N×M local connection observations on either side, not
queries and answers broadcast to every other worker and linked application.
Application-private and explicit shared domain events keep their normal
fleet/Link transport. No SHM layout, capacity or duration changes.

Every serving process publishes complete application batches through one
common Orbit snapshot ring. Empty batches are first-class observations, so an
empty worker is distinct from a missing or stale worker. Collection uses the
supervisor-owned `FleetMembership` capability and expects only live worker
placements, never the master. Pool fields retain `None` when an engine does not
measure them, and oversized errors are UTF-8 safely truncated with an explicit
marker. `FleetApplicationManager` requires a complete fresh membership
snapshot before accepting a fleet command; acceptance remains separate from
the later lifecycle events emitted by each engine.

`ApplicationSnapshotPublisher` remains registered on master and workers. A
managed SHM companion validates, creates, or reuses the application snapshot
ring with `Reuse` / `Preserve`; no master boot reset occurs. Complete batch
identity and freshness make retained observations harmless. The fleet manager
resolves the publisher for snapshot policy. At registration it
captures whether this process is a worker: only workers expose `as_runnable()`
and use the default automatic start policy. The master exposes no runnable,
so it contributes no stopped/unavailable publisher placement to service
snapshots or fleet commands. Standalone does not register this fleet provider.
Provider boot responsibility and runnable service membership are distinct;
this needs neither another provider nor a new snapshot field.

The same bounded application snapshot row codec is used when a language runner
reports its hosted applications across Orbit Link. A complete batch from one
live Link lane supplies that runner worker's runtime-qualified application
identities, lifecycle, generation, pool counters, and errors. Link reachability
is separate from those application placements; a stopped application may keep
its runner lane connected. Edge accepts only complete reports for the current
lane incarnation and exposes partial reporting counts when some lanes have not
reported. Supervised workers publish at their existing application snapshot
cadence; a runner also reports when its Link lane joins, on an Edge status
request, and after an application lifecycle event. Application state is never
inferred from a connected Link lane.

Description-bearing rows use wire version 2 and SHM kind `192`, replacing
retired kind `235`. The 512-byte payload capacity is unchanged; description
bytes share the existing bounded row budget and are never silently truncated.
Old and new participants must be rebuilt together for complete application
reports. No existing SHM is resized, reset, migrated or unlinked. The old kind
may be cleaned by the operator only after its old fleet is stopped.

## Invocations served in Rust

`invoke::InvocationRouter` is the Rust side of the invocation ring: an
operation registered on it is a capability the runtime offers to whatever
runs on it, without a native function per capability. A handler implements
`InvocationHandler { OPERATION, Request, Response, handle }`; the request is
the submitter's payload read as JSON, the response goes back as JSON in the
outcome's `output`, and a `String` error becomes an `InvocationFailed` with
that reason. Outcomes are `ApplicationEvent`s on the topics the Laravel
bridge already types (`InvocationCompleted` / `InvocationFailed`), carrying
the invocation id, so a PHP caller `await`s a Rust answer exactly as it
awaits a PHP one.

The same registered handler table also has a direct in-process invocation
path. A hosted language engine calls it when the caller and Rust capability
share a process, preserving the operation name, typed request/response and JSON
boundary without routing a local call through Orbit. Remote callers still use
the invocation ring and receive the event outcome described above; the
transport does not define the capability.

The ring is a broadcast with a cursor per subscriber, so one `invoke-router`
task (`dispatch: one`, registered by the server bootstrap where the task
service and this crate meet) serves the fleet; handlers registered in every
process only run where that task lands. Concurrency inside is a `Gate`, and
cancellation drains what is in flight before reporting what did not finish.
`claviron.ping.v1` is registered here as the smallest proof of the path.
Output is cut at the frame budget and marked `truncated`; anything larger
belongs on a result ring, not in an event.

## Lifecycle operations

The common control surface is intentionally limited to start, stop, and
restart. `reload` is not a universal application operation: module reload,
configuration reload, opcache invalidation, rolling instance replacement, and
WebSocket draining have different contracts. A future rolling operation must
be modeled explicitly instead of silently changing restart semantics.

Lifecycle observations such as `ApplicationStarted`, `ApplicationStopped`, and
`ApplicationFailed` belong to the application management plane. They are not
`ApplicationEvent` payloads published by user code. Management events describe
real engine state transitions; accepting a control command does not emit a
successful lifecycle event. Application events remain the user-level
cross-runtime messaging contract. Restart is observed as the old generation
stopping and the new generation starting rather than as an ambiguous reload
event.

PHP background daemons may publish and consume application events, but they
are not request-serving applications. Their dispatch, lease, retry, and
finished-state lifecycle remains in the PHP daemon subsystem.

## Execution environments

`environment` shares execution-environment preparation, not execution control.
`EnvironmentContext` keeps the Dotenw store and a detached recipe based on
the runtime's config-environment handle; it does not retain `AppState` or its
service registry. Base values can resolve placeholders but are not implicitly
exported to guest programs. Explicit env files are cwd-relative; their selected
deployment variants follow each file, and inline values override them last.

`ExecutionEnvironment` owns a Dotenw notification registration. One existing
owner loop awaits its changes, optionally multiplexed by `ApplicationEnvironments`;
there is no detached async watcher per application. Dotenw shares native file
watching, compares effective values, and retains the last valid snapshot on
errors. Dropping the last environment owner removes that subscription.

Persistent engines decide how to renew a named execution. SAPI request owners
instead take `request_snapshot()`: a cheap immutable snapshot for that whole
request, with pending errors reported without discarding valid values. This
does not add a universal application reload operation or control counters.

## Request policy

`ApplicationRequestPolicy` carries the admission bound and two budgets shared
by request-serving application engines: how many callers may enter dispatch,
how long one may wait for an execution slot, and how long accepted guest
execution may run. The application owns these values,
not a vhost that happens to route to it. Each engine still owns its dispatch
algorithm and its real interruption mechanism; the common type does not turn
PHP and JavaScript pools into the same implementation. Retry, replacement,
elastic capacity and guest-specific lifecycle remain runtime-local policy.
