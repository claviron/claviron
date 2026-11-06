# Edge backends

Design contract: `EDGE_BACKENDS`.

Edge is an application-server factory: boot/reload prepares host and path plans
with their selected request owners. Configuration can select an embedded
application or relay; Rust registrations can supply routers and handlers.
Reusable services are resolved during preparation. Runtime startup, instance
management and shutdown belong to their lifecycle owner.

A **Backend** is the owner that receives an admitted HTTP request for application
execution or forwarding. Rust routers/handlers, embedded runtimes, Upstream and
Link share this boundary. WebSocket and broadcast application calls use it too.
Static, TryFiles and file fallback are native pipeline steps; they need no
application metadata export.

## HostPipeline registration

`BackendFactory` compiles a backend directive into execution owners. Its
`BackendContext` contains the resolved file view, listener protocol and final
HTTP policy. Register factories on `HostAppRegistry` before host preparation.
Every factory declares `BackendPlacement::BeforeFiles` or `AfterFiles`.
Both slots follow Rust contributions and the terminal router; file fallback
follows both. Factories run in registration order within their selected slot.
Dispatch never calls the factory or decodes config. The facade registers Lua, PHP and Upstream
in that order in AfterFiles. Broadcast/Pusher REST selects BeforeFiles.
Link remains Edge-native and precedes the registered AfterFiles owners.

Edge names no backend. Its configuration keeps every directive it does not
execute itself as written, and a directive becomes an application owner by a
factory being registered under its name. A scope declares at most one owner,
Link included; because only the registry knows the names, that rule is held when
the scope is prepared and the error names the owners it found. A directive no
factory or route extension claims fails the same preparation.

`RouteExtension` is separate: conditions, request preparation and response
hooks precede execution. Interception uses that route/layer boundary, rather
than becoming an Axum application. A directive has one registered owner.

- Select host, path and protocol eligibility before preparing the corresponding
  backend handle. Install only effective actions; an absent or empty capability
  leaves no pipeline entry, request state or enabled check.
- Resolve listener → host → prefix policy at boot/reload. `Backend<B>` carries
  the selected owner and final `HttpPolicy` metadata contract.
- `Unhandled` returns the original request with its body unread. Once a backend
  consumes input, a response status cannot select another backend.

The request order is maintained in
[HostPipeline order](../../crates/edge/ARCHITECTURE.md#hostpipeline-order).
Backend registration participates in that plan; it provides no policy bypass.

## Request policy

HostPipeline owns HTTP request-size and read-idle guards. Backends consume that
prepared body without repeating its declared-length cap, cumulative byte
counter, idle timer or guard wrapper. Collection still reads through the guard;
its errors must propagate to the execution boundary.

Policy metadata does not enforce itself. A direct caller outside HostPipeline
must supply the final policy and prepare the same ingress bounds. A backend
requiring `HttpPolicy` rejects its absence instead of inventing a fallback.
`None` in an effective bound means no Claviron limit; a nearer `off` cannot
remove an inherited ceiling.

Runtime constraints remain distinct. PHP's POST/upload parsing rules, an
execution queue deadline and a producer's bounded channel do different work
from an HTTP body guard. Any producer-side bound must identify the allocation
or queue it protects; it must not introduce another ingress limit.

## Response policy

HostPipeline's reply layer owns outgoing HTTP body-size and inter-frame idle
bounds. It wraps the original response body without collecting it. Only reached
response hooks apply, in reverse order.

An execution service may bound its own output allocation or channel before the
HTTP body exists. Document that resource and reuse the supplied policy where
applicable; do not add another identical HTTP guard. A blocked write to the
client, waiting for a response head and waiting for the next backend body frame
are different events with separate declared timeout owners.

Before a response starts, an input failure can become an HTTP rejection.
After headers are committed, a body failure ends the affected stream/exchange;
it cannot replace the response status or trigger backend fallthrough.

## Buffering

The backend declares its actual input requirement through `BodyNeed`:

| Requirement | Delivery consequence |
| --- | --- |
| `Stream` | Accept pieces without requiring a known length. |
| `Length` | Collect unknown-length input when execution requires its final length, as with FastCGI `CONTENT_LENGTH`. |
| `Whole` | Collect once before execution, as with PHP application messages or replayable input. |

Use `RequestPolicy::delivery` for that choice. `buffer_size` is a delivery
threshold, not another body-size limit. Collection still reads through the
existing ingress guard. Keep `Bytes` owners until the execution interface
requires a copy or text decoding; avoid serialization merely to cross Rust APIs.

When response delivery can be selected, use `ResponsePolicy::delivery` after the
response head is known. Its streaming signals and the backend's execution mode
must agree. Buffering the request does not require buffering its response.

## Streaming and cancellation

Preserve lazy polling, backpressure and data/trailer ownership through the
pipeline. Unknown length does not mean empty input. Translate unsupported
runtime input at the selected backend boundary, after the request is claimed.
Header hooks and body guards must not collect a stream.

`BackendContract` also declares reply lifetime. `BackendReply::Owner` leaves
completion with the execution owner. `ExternalEventStream` supplies that
owner's shutdown signal; the common dispatch boundary applies
`EVENT_STREAM_ENDS_AT_SHUTDOWN` only to its actual SSE responses. Normal
responses create no shutdown future or additional body wrapper. A backend's
body retains its own lease/exchange cleanup, which runs when the body is dropped.

Propagate request cancellation to the execution owner and release abandoned
input/output resources. Work already accepted by a runtime remains owned until
completion or its declared cancellation path; cancellation of one request
does not stop the shared runtime or unrelated work. Follow
[GRACEFUL_OWNERSHIP](GRACEFUL_OWNERSHIP.md) for drain and shutdown.

## Request identity

Every backend call is lent a `HostContext`: the accepted `Arc<ConnCtx>`, the
selected `HostName` and the effective policy of the scope that selected the
backend. Nothing is written for it. The work a backend leaves pending outlives
the call, so it copies what that work needs before it returns. Link, Lua and
Upstream take their policy and connection this way.

Identity written into the request is the other, dearer form: it costs an
extension map for every request, so it is exported only to a backend that
reads it there. A backend states that once, when the host plan is prepared,
through the required `BackendHandler::contract` declaration.
`BackendIdentity::None` preserves the request; `Request` exports metadata. PHP,
WebSocket and broadcast still read it from the request, where their own
execution carries it further. A Rust handler (`OtherHandler`, `HostHandler`)
receives the request as it arrived; a Rust owner that needs the accepted facts
is a route extension and borrows them from `HostContext`. For `OtherHandler`
that is fixed (`OTHER_HANDLER: NO_EDGE_INPUT`): it is never given anything of
the edge's, so it can be served from the HTTP layer without one. An Axum or Warp
router reads through extractors, has no declaration of its own and always
receives identity.

The builder places Identity immediately before the first Backend that reads
it. It exports the existing `Arc<ConnCtx>`, selected `HostName` and effective
policy. Later Backend fallthrough preserves those values without another
insertion. A plan whose owners read none exports none, and the request reaches
its owner as it arrived.

Transport facts remain in `ConnCtx`. Trusted forwarding is request-local
`RealIpExt`; consume that projection rather than reparsing proxy headers or
reconstructing scheme, TLS and peer identity. Native file-only paths need no
backend identity map.

## Protocol compatibility

H1, H2 and ordinary H3 requests use the same HostPipeline body contract.
Select protocol-dependent actions during preparation. Transport selection and
HTTP version are separate: an HTTPS listener can carry H1 or H2.

### HTTP/1 over plain TCP or TLS

Hyper owns native HTTP/1 framing; Edge adapts its body before pipeline dispatch.
Backend code must not reinterpret transfer framing. TLS facts belong to the
accepted connection. An HTTP upgrade transfers connection ownership through
the registered native handoff, rather than a whole-message collector.

### HTTP/2

Requests are multiplexed streams. Preserve stream-local cancellation and flow
control; a failed application exchange must not cancel sibling streams.
Protocol stream/window settings and application body budgets govern different
resources. Extended CONNECT uses its native session handoff.

### HTTP/3

H3 has its own native adapter, outside Hyper. It maps received data and trailers
to lazy body frames, writes response frames and completes the native send
stream. It adds no second HTTP size budget and does not collect input because
the transport is QUIC.

Preserve H3 framing checks, receive/send ownership and stream-ending behavior.
Early rejection or body failure must finish or cancel the affected stream
without treating other requests on the connection as the same exchange.
WebTransport retains its native session handoff. Transport early-data acceptance
and permission to execute remain separate
([EARLY_DATA_POLICY](EARLY_DATA_POLICY.md)).

## Implementation

- [HostPipeline order and module index](../../crates/edge/ARCHITECTURE.md#hostpipeline-order)
- [Backend handoff](../../crates/pipeline/src/factory.rs)
- [Scope compiler](../../crates/edge/src/host/compile.rs)
- [Body delivery policy](../../crates/http/src/policy.rs)
- [H3 request/response adapter](../../crates/edge/src/quic/request.rs)
- PHP input contract

Verify new integrations with isolated ownership and transformation tests:
unread fallthrough, guard-error propagation, byte ownership and cancellation.
Protocol-specific changes also need frame/trailer and stream-ending checks.
Keep runtime boot and end-to-end validation in executable application harnesses.
