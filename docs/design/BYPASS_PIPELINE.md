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
`HostApp`; application dispatch remains in `HostService`.

`ReplyPolicy` selects response-body guarding when the effective listener/host
policy is prepared. Without a size or idle limit there is no guard policy.
Native dispatch uses that typed policy directly after exchange response actions
and before access accounting. A composed HostPipeline can register the same
policy as a body action; it contributes no request or response-head callback.
The guard forwards data and trailers without collecting or decoding them,
and dropping the body releases its original source. Idle timers still start
only when HTTP polls a guarded body. Transport write limits, buffering and
queue/head deadlines retain their existing owners.

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
- [Shared host admission and dispatch](../../crates/edge/src/service/host.rs).

Use `BYPASS_PIPELINE` as the search key for this application-first composition.
