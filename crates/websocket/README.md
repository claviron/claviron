# claviron-websocket

HTTP/1 WebSocket upgrade admission and listener-owned session handoff.

This crate connects an application's upgrade handler to Claviron's HTTP
pipeline. It selects an exact host/path binding, lets the application accept or
reject the request before switching protocols, and hands Hyper's upgraded IO
to work owned by the accepting listener.

Applications normally use `claviron::websocket` through the root crate's
`websocket` feature. This is an upgrade integration layer, not a general-purpose
WebSocket client, frame codec, or realtime application server.

## Accept first, then hand off

The main contracts are:

- `H1UpgradeRegistry`: typed canonical-host and exact-path dispatch.
- `H1UpgradeHandler`: upgrade detection, asynchronous acceptance, and an
  awaited upgraded-session body.
- `H1UpgradeContext`: captured URI, headers, request extensions, trusted
  connection context, and the listener's session capability.
- `H1UpgradeAccept` / `H1UpgradeReject`: the pre-upgrade decision.
- `H1UpgradeState`: opaque application-owned state carried from acceptance
  into the session without transport inspection.

The middleware passes ordinary requests to the application. A matching upgrade
calls `accept` before constructing a successful protocol switch. The handler
can authenticate using request-local policies and identity already prepared by
HTTP middleware; the transport does not invent a guard or reinterpret a
WebSocket subprotocol as a credential.

`is_h1_header_upgrade` recognizes the HTTP/1.1 upgrade headers.
`websocket_response_headers` prepares the WebSocket accept header and an
optional selected subprotocol. Application handlers remain responsible for
their complete handshake, origin, credential, and subprotocol policy.

## Register a native handler

A composition with the facade's `runtime` and `websocket` features can register
an already constructed handler:

```rust
use std::sync::Arc;
use claviron::runtime::registry::Result;
use claviron::websocket::{H1UpgradeHandler, H1UpgradeRegistry};

fn register_socket(
    registry: &H1UpgradeRegistry,
    handler: Arc<dyn H1UpgradeHandler>,
) -> Result<()> {
    registry.register_handler_for_host("app.example.test", "/socket", handler)
}
```

Registration does not open a listener or publish a virtual host. The enclosing
HTTP composition installs the registry and middleware. Duplicate host/path
bindings are rejected; the request path does not scan application catalogs or
use a regex router to find an upgrade handler.

## Transport ownership survives the upgrade

The native ingress must attach `Arc<ConnCtx>` and `SessionSpawner` before the
middleware runs. Missing context is an integration error, not permission to
spawn an unowned task. A sealed or draining session owner rejects handoff with
503 rather than reporting a successful upgrade it cannot own.

The submitted future includes both waiting for Hyper's upgrade and awaiting
`handle_upgraded`. The listener retains the original connection's capacity
permit after the HTTP driver releases it: an upgraded connection still occupies
one connection slot.

Handlers can observe `ctx.sessions.wait_graceful_shutdown()` to initiate their
close handshake. Keep session work in the awaited future, or submit children
through the supplied session capability. Detached tasks cannot participate in
the listener's join guarantee. Old capabilities cannot submit work into a
restarted listener generation.

Graceful shutdown seals admission and drains accepted work. An indefinite
grace remains indefinite; an explicit force operation aborts and joins owned
work. This crate supplies no idle timeout, reconnect policy, message loop, or
application manager.

## Other transports and integrations

HTTP/2 extended CONNECT and HTTP/3 WebTransport have native stream/session
contracts. They are not represented as HTTP/1 upgrades in this registry.
Likewise, an upstream proxy tunnel has its own outbound connection ownership.

- [HTTP](../http/README.md): trusted connection context and listener sessions.
- [Broadcast](../broadcast/README.md): Pusher-compatible channels and sessions.
- [Edge](../edge/README.md): host-aware routing and publication of upgrade bindings.
- [Runner](../runner/README.md): single-application listener composition.
- [Upstream](../upstream/README.md): transparent proxy upgrade ownership.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
