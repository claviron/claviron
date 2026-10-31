# claviron_http::upgrade

HTTP/1 upgrade primitives shared by host routing and application runtimes.
This crate depends on HTTP/runtime/host contracts, not on vhosts or any
WebSocket application. H2 CONNECT and H3 WebTransport retain their own native
handlers; they are not represented as H1 upgrades.

## Acceptance and hand-off

`H1UpgradeRegistry` selects a typed canonical host plus exact path. Edge captures
one host's bindings during preparation, without registry locks during requests.
`try_h1_upgrade` consumes `Request<RouteBody>` and returns an ordinary request
unchanged when the selected handler declines. It has no Axum dependency. An
optional Axum middleware adapter delegates to this same boundary.
An upgrade handler validates/authenticates
the head through `accept()` before the response switches protocols. Its opaque
`H1UpgradeState` is carried to `handle_upgraded()` without inspection.

`H1UpgradeContext.extensions` preserves request-local policies and identity
prepared by the host pipeline for native admission. The protocol handler remains
responsible for its own credential carrier and authentication before returning
101; the transport does not select an auth policy or reinterpret subprotocols.

The native transport must have already applied host/listener policy and
attached `Arc<ConnCtx>` plus `SessionSpawner`. Missing context or session owner
is an integration error (500), not an unowned fallback spawn. The native boundary
submits the complete future (await Hyper upgrade, then await handler) before
returning 101. A sealed/draining owner rejects hand-off with 503 and drops the
accepted state; it never reports a successful upgrade without owned work.

## Lifecycle contract

The accepting listener owns the submitted task and retains the original
connection gate permit after Hyper releases the HTTP connection driver.
One upgraded connection remains one capacity slot. The listener seals new
submissions before graceful drain; `None` grace remains indefinite. Explicit
force aborts the owned task and joins cleanup before reporting stopped.

`H1UpgradeContext.sessions.wait_graceful_shutdown()` lets a protocol handler
initiate its own close handshake. It is not an execution deadline or permission
to silently discard work. Keep work in the awaited handler future, or submit
children through that capability. Arbitrary detached spawns cannot be joined
by the listener. Old capabilities cannot target a restarted generation.

This boundary implements no WebSocket message loop, retry policy, idle
timeout, proxy pool or application manager. Upstream/Pusher tunnel ownership
and H3's native stream lifecycle are separate integration paths.
