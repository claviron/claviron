# An application-shaped pipeline

Design identifier: `BYPASS_PIPELINE`.

This is the serving-pipeline application of
[APPLICATION_FIRST](APPLICATION_FIRST.md).

If your repository has both `app/*.php` and `src/main.rs`, routing should help
those parts work together. It should not force you to put another server between
them, or rewrite the application before Rust can handle its first request.

Claviron is an edge, but its pipeline is built around the application it serves.
TLS, host admission and HTTP transport belong to the edge. Which application
owns a path, and which parts you choose to implement in Rust, need not be fixed
for the whole host or for the lifetime of the project.

## Start with what already runs

You can keep an existing application as the main request owner and add native
Rust routes where they are useful. `Routable` describes application routing;
interceptors let selected paths be handled before the configured fallback.
Unmigrated paths can continue to reach the existing application.

This makes a gradual Rust migration possible, but migration is not required.
Keeping PHP for presentation and Rust for a particular service can be the
finished design. An application may also run in a separate process reached
through [Orbit Link](ORBIT_LINK.md) rather than being embedded.

These are explicit bindings, not automatic discovery of every file in your
repository. The application and its host configuration declare what is served.

## Bypass is a choice of dispatch, not an escape from admission

Not every host needs the merged router. The application registry supports
different composition shapes:

| Shape | What you choose |
| --- | --- |
| Configured / merged routing | Compose configured owners, native routes and the applicable router layers. |
| An application-owned router | Supply the router yourself without the merged Claviron router stack. |
| Bypass | Dispatch directly to a native `BypassHandler` or a built-in backend without an Axum router. |

The choice is per host, not a server-wide mode. A native resource service and a
mixed application can live behind the same edge without taking the same route
through it.

Bypass remains behind shared host admission: accepted authority and transport,
deny/access rules and the narrowed HTTP policy still apply. Request admission,
head timing and response-body guards are not Axum-only facilities. Skipping the
router must not make an application invisible to exchange accounting either.

The immutable host binding prepares the listener/host request and response
limits once. Cleartext requests reuse them while that binding matches the
current host configuration; replacement prepares new limits without changing
already admitted requests. ACL and redirect rules remain live per request.

Typed request extensions remain request-owned. Context replacement reuses an
existing value's box and releases the old value; unrelated entries remain
untouched. A fresh request still needs independent typed values and the ordinary
`http::Extensions` allocations. The prepared host map does not share mutable
extension storage between requests.

## Prepared host actions

`HostPipeline` prepares active `HostLayer` actions with the host binding.
Requests execute the prepared head actions in order with their own typed state;
an unhandled request keeps its original body until a terminal handler claims it.
Response actions run in reverse order, within the scope reached by the request.

`IngressPolicy` is the first migrated layer: authority/binding, early-data,
H1 upgrade and deny checks retain their existing order and typed rejections.
Inactive upgrade/deny gates add no action. Accepted connections keep their plan;
replacing a cleartext host binding prepares a new one.

`IdentityPolicy` runs after ingress and prepares typed connection/host
extensions without reading the body. The binding selects direct, peer-only or
trusted-proxy handling once. Forwarding headers are resolved per request only
on the trusted-proxy path; accepted TLS, protocol and listener facts stay fixed.
HTTP extensions and native authentication retain the same `Arc<ConnCtx>`.
`AccessPolicy` follows identity and applies live canonical-host ACL and redirect
rules before application routing. Each admission selects the current rule
snapshot; an open host traverses no rules and clones no rule Arc. The canonical
lookup remains necessary so removal or newly enabled rules also reach existing
connections. Host ACL, route ACL and redirects contain only active checks,
compiled in that order. An admitted request retains its selected generation
through dispatch or cancellation.

With `rate-limit`, `QuotaPolicy` is the next prepared head step. Only configured
limits or explicit Rust registrations create a quota plan. A uniform root limit
selects its direct path during preparation; scoped limits retain canonical path
checks and longest-prefix selection. Key providers borrow the parsed head and
typed extensions, and a successful admission retains its committed snapshot.
Cancellation does not refund a charge or perform another store read.

Quota plans stay with the selected `HostApp`, which owns the stores, rather
than the cleartext metadata cache. `HostService` continues the prepared head
through that plan after access and before reassembling the original body for
dispatch. It does not clone the plan or quota handle per admission. Unconfigured
hosts skip quota execution.

`ExchangeLayer` prepares only the declared CORS and response-header actions for
an effective host/prefix scope. CORS runs once against the request head; its
accepted origin stays in `ExchangeState`. Response processing applies the final
header unsets/sets, then CORS headers, including on preflight, rejected-origin
and admission/timeout replies. An empty scope contributes no actions.

`prepare_head` borrows request parts and returns any early decision together
with a `PreparedResponse` scope. The caller retains its request body while
waiting for admission or a reply. Finishing the scope applies only the armed
response actions; dropping it releases state without running those callbacks.
A host without exchange policy does not enter this preparation or split its
request for it. Prefix selection and the plan remain with the pinned
`HostApp`; the native execution stage borrows them during dispatch.

`ReplyPolicy` selects response-body guarding when the effective listener/host
policy is prepared. Without a size or idle limit there is no guard policy.
Native dispatch uses that typed policy directly after exchange response actions
and before access accounting. A composed HostPipeline can register the same
policy as a body action; it contributes no request or response-head callback.
The guard forwards data and trailers without collecting or decoding them,
and dropping the body releases its original source. Idle timers still start
only when HTTP polls a guarded body. Transport write limits, buffering and
queue/head deadlines retain their existing owners.

`AdmissionPolicy` prepares queue/head timing from the same effective request
policy. It borrows the selected application's `Admission` capacity instead of
keeping a semaphore in cached connection metadata. `enter` returns HTTP's
existing future directly; its permit still lives until the reply head or
cancellation. Backend timing ownership is queried only for a declared head
limit, so a backend enforcing that limit is not timed twice. The existing
HTTP head future retains first-poll deadline and cancellation behavior.
This asynchronous execution stage stays outside synchronous `HostLayer` head
actions; it adds no erased layer future, task or implicit duration.

With `access-log`, `AccessLogLayer` prepares the producer capability and
canonical host declaration when a ready process binds a host. `off` or an
absent producer contributes no layer. An existing producer retains a live
channel selector, including when its current default is empty: later channel
configuration changes must still reach existing connections. No record or
body wrapper is created when that lookup has no destination.

`AccessLogState` owns one record through admission and the response body.
Native dispatch uses the prepared layer directly without splitting its request;
composed pipelines install it after identity and outside response shaping/body
guards. The execution owner marks actual admission. An early 503/504 completes
the empty reply record, while a normal response transfers it to the stream.
HTTP/gRPC status, trailers, byte counts and drop/completion behavior retain the
same recorder. Runtime activity accounting remains independent of access logs.

`ApplicationHandler<B>` is the typed terminal call after host admission. It
consumes one request and returns that owner's concrete response future.
`PreparedRequest::dispatch_to` defers the call until first poll and finishes
the armed response actions. An unhandled request still owns its original body
before this handoff; no status code, including 404, implicitly returns ownership.
The existing closure-based `dispatch` remains available.

Native Bypass and Axum dispatch use this same contract without another future
box or service clone. Bypass keeps its existing handler future; Axum uses its
native oneshot. Application selection happens before those futures are built,
and the host execution boundary still owns capacity, head timing and accounting.
This step does not replace host registration, merged routing or engine adapters.

`Routable::application` now prepares the native Rust contribution through the
existing host registry. It returns an `ApplicationInterceptor`; no parallel
exclusive route registry is introduced. The contribution is compiled once per
changed host alongside the existing route/upgrade capabilities, and remains
pinned with that host's application publication.

`ApplicationStage<H>` visits only those contributions before its configured
owner. An `InterceptRequest` exposes the parsed head but keeps its original body
private. `Unhandled` carries that same capability onward. Claiming the body
consumes it through `into_request`, after which it cannot be reconstructed for
fallthrough. A handled 404 or other error remains a handled response. No body
buffer, replay, decode, lock, timer or task is introduced by this handoff.

Absent contributions keep the original direct owner at preparation. Active
heterogeneous contributions have an explicit erased-future boundary; the outer
pipeline future stays concrete until its existing application adapter boundary.
The combined Rust/backend call keeps Edge's declared whole-head deadline;
backend-specific narrowed deadlines remain with their execution owner.

The current compatibility compiler installs this stage inside the selected route's policy
scope. Existing Rust Axum routes retain precedence; their delegation handle
reaches the native stage and then the configured owner without re-entering those
Rust routes. A rejected entry uses the original backend fallback and never runs
the rejected native contribution. The compatibility bridge only adapts body
types and is absent when neither native contributions nor file steps exist.
The remaining host compiler still supplies an Axum application adapter.
Its backend steps are native; they no longer have separate Axum bindings.
Removing that outer compiler is subsequent work. Axum's target
boundary is the Rust Application itself. Once it yields, Static, TryFiles and
the Embed, upstream or Link backend are native pipeline owners, not Axum
middleware or routers. The pipeline calls the SHM application transport Link;
existing Orbit configuration and transport APIs retain their names.

Configured file dispatch prepares **Routable → Static → TryFiles → backend**.
Static and TryFiles are separate asynchronous steps using `claviron-files`,
never implementations of cache, traversal checks or file delivery in Edge.
Each static mount prepares its URL/disk mapping once. A selected route supplies
only its own steps; disabled declarations produce no step. Missing files obey
`none_found`, and dynamic extensions yield without disk work. Each miss returns
the original `Request<RouteBody>`; no request body is polled or collected.

The residual backend file view omits both Static and TryFiles, retaining only
directory-index/SPA fallback. The automatic upstream binding shares a prepared
scope table with that fallback and preserves empty child scopes. Direct public
`UpstreamBypass` callers keep their explicit file composition. The migration
adapter invokes native backend steps through one shared application bridge;
engines and transport backends do not supply Axum builders.

The local runtime category is named Embed. `pipeline/embed` owns its native
backend adapters; it is not an Axum application category. Lua is the first
migrated backend. Its prepared mounts preserve segment boundaries, specificity
and the former equal-prefix precedence. Only a nonmatching mount yields the
unchanged request; a matching unavailable application owns its 503 response.
`LuaApplicationAdapter` retains the existing live generation, policy and execution
path. Lua no longer provides an Axum feature or router binding. HTTP/1 WebSocket
handshake/admission belongs to Edge; the engine receives the upgraded transport.
The existing compiler still bridges
the native Lua stage at its former position so other backend precedence is
unchanged while their migration proceeds. It no longer calls Lua's Axum builder.

PHP is the next native Embed backend. pipeline/embed/php.rs prepares mounts
from resolved PhpRuntime values; its only runner match is at preparation.
Script, Application and FCGI each own a separate request future. Application
execution therefore carries no script resolver, filesystem probe or Script
future state. It retains PhpApplicationHandle's live generation semantics and
always owns a matched response, including unavailability. Script and FCGI use
consuming native try_handle calls whose Unhandled contract returns the original
unread request. Native DispatchStep owners pass Request<RouteBody> directly;
only Rust application interception uses InterceptRequest. A backend does not
construct or restore an interception capability. Dropping the call retains the engine's existing cancellation path.

The PHP contract, embed and FCGI crates no longer contain an Axum feature,
dependency or binding. The current host compiler temporarily bridges the native
PHP owner at its former position, as for Lua. Upstream and Link also use native
DispatchStep owners through the same bridge. Existing script/application engine capacity, INI, environment and
lifecycle contracts are unchanged.

UpstreamStage selects the existing live UpstreamApp profile, then transfers
Request<RouteBody> directly to call_upstream_service. Explicit/root selection,
request forwarding, response lease ownership and profile retirement remain with
Upstream. Static and TryFiles are not repeated inside this stage. A selected
upstream with no matching service retains its final 404 rather than entering
another backend. The separately exposed UpstreamBypass retains its file-first
composition contract.

LinkBackend prepares destination targets and segment-safe mounts at boot/reload.
It retains specificity and equal-prefix precedence. An unmatched mount returns
the untouched request synchronously; an unavailable matched target owns its 503.
Exchange metadata retains the existing Link wire encoding; body chunks are not
JSON-decoded or routed through an Axum Body. The response is a native StreamBody
bounded by the existing Link window. Body delivery policy can still request
buffering explicitly.

A nonempty upload keeps its existing independently driven task and backpressure.
The pending head owns its abort-on-drop handle, then transfers it into the
response body. Cancellation, head failure, body drop, EOF or response error
releases that owner; an unfinished upload resets its own input direction.
Empty requests create neither an upload task nor its response wrapper. Native
WebSocket session handoff and WebTransport streams retain their existing owners
and shutdown signals. No retry, deadline, polling or SHM ABI change is added.

When declared, WebSocket is the first dispatch step before Rust applications.
Its preparation requires a registered exact path, permission for HTTP/1 and an
enabled H1 handshake in the host transport policy. Otherwise no WebSocket step
is installed. Preparation captures an immutable host endpoint map, so ordinary
requests require neither a registry lock nor an allocated future for a miss.
The protocol boundary owns acceptance and listener session hand-off independently
of Axum. Required security/admission/CORS precede it; response actions still
finish the handshake response before it is sent.

Native built-ins use `DispatchStep`, distinct from `ApplicationInterceptor`.
`DispatchFuture` keeps synchronous decisions inline and erases only asynchronous
work. `Unhandled` transfers the same Request<RouteBody>, without body polling,
collection or decoding. Cancellation drops the currently owned request/future.

Native execution now lives in `pipeline::host`: `HostPolicy` prepares the
immutable binding and `HostExecution` owns one ordinary exchange. `HostService`
still selects/adapts the admitted application for the transport. It returns the
same concrete execution future directly, without an additional async wrapper.

Execution retains the existing order: activity/access record, configured
exchange policy, request metadata, capacity, head timing, application response,
response policy and stream accounting. Capacity and quota stores remain with
the selected `HostApp`; the cleartext metadata cache retains neither. Queue/head
cancellation releases that request's body and guards. A streamed response keeps
only the body resources transferred to it. These are internal native owners;
public composition continues through `HostPipeline` and `ApplicationHandler`.

The shared HTTP `dispatch::RouteDispatch<H>` selects a prepared owner from a
borrowed method, URI and header view. It has no Axum dependency or execution
engine knowledge. It selects the longest segment prefix and checks only that
entry's conditions; rejection selects the explicit rejected owner, never a
shorter prefix. The returned reference belongs to the prepared table, so the
caller can transfer the original request directly to its owner.

The existing Axum route adapter now delegates that decision to this selector.
Its `routing::ConfiguredRoute`, `routing::RouteDispatch`, `RouteCondition` and
`RuleContext` paths remain available. Native configured-backend compilation
will use the same selection contract; it is not connected by this extraction.

## Keep the boundary useful

Native transports adapt their request bodies to `RouteBody`; bypass handlers
return `RouteResponse`. The bypass dispatch chain does not require Axum types.
That lets a backend participate without pretending to be a router middleware.

On native TCP, the host-dispatch future stays inline for bypass requests. The
handler returns its erased `RouteFuture`; the transport does not add a second
future box. Axum erases its own selected dispatch future separately, so its
concrete state cannot enlarge bypass dispatch. Admission and head timing retain
the same handler future until completion or cancellation, while response bodies
keep their independent streaming lifetime.

Conversely, middleware attached only to a router does not run merely because a
bypass handler serves the same host. Framework-specific validation, CSRF and
response formatting belong to the application/framework composition, not an
implicit promise that every dispatch shape executes every layer.

The goal is a pipeline suited to your application, not a claim that bypass is
always faster or should replace routing everywhere.

## Follow the implementation

- [Edge routing and composition guide](../../crates/edge/README.md).
- [Routable bindings](../../crates/edge/src/pipeline/routable.rs).
- [Host application registry](../../crates/edge/src/pipeline/apps.rs).
- [Native bypass dispatch](../../crates/edge/src/pipeline/bypass.rs).
- [Prepared host pipeline](../../crates/edge/src/pipeline/host.rs).
- [Request identity and proxy policy](../../crates/edge/src/pipeline/host/identity.rs).
- [Live host access policy](../../crates/edge/src/pipeline/host/access.rs).
- [Prepared quota policy](../../crates/edge/src/pipeline/host/quota.rs).
- [CORS and response-header layer](../../crates/edge/src/pipeline/host/exchange.rs).
- [Response-body policy](../../crates/edge/src/pipeline/host/reply.rs).
- [Prepared admission timing](../../crates/edge/src/pipeline/host/admission.rs).
- [Access-record layer](../../crates/edge/src/pipeline/host/access_log.rs).
- [Typed application call](../../crates/edge/src/pipeline/host/application.rs).
- [Native application interception](../../crates/edge/src/pipeline/host/intercept.rs).
- [Native host execution](../../crates/edge/src/pipeline/host/execution.rs).
- [Shared host admission and dispatch](../../crates/edge/src/service/host.rs).

Use `BYPASS_PIPELINE` as the search key for this application-first composition.
