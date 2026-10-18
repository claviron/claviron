# claviron-pusher

Pusher-compatible protocol messages, signing, and target configuration.

This crate owns wire contracts, not the live realtime service. Its default
feature set is transport-free: typed application identifiers and redacted
secrets, REST authentication, channel authorization, publication validation,
and client/server message shapes.

Applications using the Claviron facade reach these contracts through
`claviron::broadcast::protocol::pusher` with the `broadcast` feature. The live
service is [claviron-broadcast](../broadcast/README.md); there is no separate
facade feature that turns this protocol model into a second service.

## Protocol and identity types

`PusherAppId` and `PusherAppKey` are validated path-segment identities.
`PusherAppSecret` redacts its value in `Debug`; deliberate signing code can
access it through `expose`. These are protocol application identities, not
Claviron vhost names or end-user identities.

The principal message types are:

- `ClientMessage` and `SubscriptionRequest` for incoming client messages.
- `ServerMessage` for connection, subscription, presence, ping/pong, and event
  responses.
- `TriggerEvent` for an incoming publication and `PublishedEvent` for its
  validated form.
- `PresenceMember` and `PresenceSnapshot` for presence-channel projections.

`TriggerEvent::validate` resolves `channel` versus `channels`, rejects empty or
ambiguous targets, and limits an event to 100 channels and 10 KiB of event
data. Unsupported `info` queries are rejected. String data remains the wire
payload; structured JSON values are serialized into it. This is the currently
implemented protocol surface, not a claim to implement every hosted Pusher API.

## Signing is separate from user authorization

`authentication_query` signs a REST method, path, query, and body digest;
`authenticate` verifies the request, including its timestamp window and
signature. The `_at` variants accept the caller's timestamp explicitly.

`authorize_channel` and `verify_channel_authorization` implement socket/channel
HMAC authorization. Presence signatures include `channel_data`; private-channel
signatures must not contain it. An application decides who may join a channel
before issuing that signature. The protocol primitive does not resolve a user,
read a framework session, or select application roles.

For example, an application can issue a private-channel signature after its own
authorization decision:

```rust
use claviron::broadcast::protocol::pusher::{
    PusherAppKey, PusherAppSecret, authorize_channel,
};

fn private_channel_signature(
    socket_id: &str,
    channel: &str,
    key: &PusherAppKey,
    secret: &PusherAppSecret,
) -> String {
    authorize_channel(socket_id, channel, None, key, secret)
}
```

Do not expose the application secret to a browser or mistake an authorized
channel signature for a reusable application login credential.

## Select local service or external HTTP

Host-facing `PusherConfig` selects where the Pusher surface is handled. A local
target is expressed as:

```yaml
broadcast:
  target: fleet
```

`fleet` has no socket address or duplicate credentials. It uses the registered
Broadcast service and that service's `broadcast.yaml` application catalog.
Edge prepares exact `/app/{key}` bindings from the catalog before requests
arrive.

An external target instead uses `target: { http: ... }`, with an `address`
and separate `connect_timeout`, `request_timeout`, and `idle_timeout` settings.
This selects an HTTP/1.1-compatible upstream, not an in-process Broadcast
generation. Configuration rejects unknown fields.

## Optional HTTP adapter

The implementation's `http` feature adds `PusherAdapter`, an axum-free
request/response adapter for external Pusher servers. `axum` adds its router
binding. Neither is required merely to use wire types and signatures.

The adapter forwards `/app/{key}` upgrades and `/apps/{id}` REST paths,
preserving the public path and query. It replaces untrusted forwarding facts
through the shared HTTP boundary, strips hop-by-hop response headers, and keeps
accepted tunnels under listener session ownership.

It owns neither vhost resolution nor a local socket/channel registry. In
particular, a local `fleet` target must be resolved by the composition layer;
the external HTTP adapter does not implement the Broadcast service.

## Related crates

- [Broadcast](../broadcast/README.md): live sessions, publication, and fleet fanout.
- [WebSocket](../http/docs/upgrade/README.md): native HTTP/1 upgrade handoff.
- [HTTP](../http/README.md): trusted headers, bodies, and session ownership.
- [Edge](../edge/README.md): host target selection and prepared routes.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
