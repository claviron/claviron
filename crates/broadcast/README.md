# claviron-broadcast

Realtime sessions, channel publication, and same-host fleet fanout for Claviron.

`BroadcastService` owns live sessions and subscriptions. Native Rust callers
publish directly; serving workers exchange publications through Orbit rather
than making loopback HTTP requests or using Redis as a second membership
authority. Pusher names describe wire compatibility, not a separate service.

Applications normally use `claviron::broadcast` through the root crate's
`broadcast` feature. Wire types are available at
`claviron::broadcast::protocol::pusher`.

## One application catalog

File-backed registration uses `broadcast.yaml` from Claviron's configuration
directory. For example:

```yaml
applications:
  dashboard:
    id: "1"
    key: example-key
    secret: ${BROADCAST_APP_SECRET}
    client_events: false
```

The map key is Claviron's application name. ID, key, and optional secret are
Pusher wire identities. Empty names and duplicate IDs or keys fail validation.
The registered file is required; the prepared configuration environment
supplies substitutions before the catalog is used.

An enclosing runner can instead call
`register_with_config(state, BroadcastConfig)` with the same validated schema.
This retains the normal service/fleet lifecycle without reading the file;
supplied boot options change on restart. Ordinary registration preserves an
already registered service. Neither form opens an additional listener.

The catalog is distinct from host routing. One catalog application can be
published on several ingress hosts without copying its credentials or creating
a second service instance for each host.

## Publish from native code

`publish` addresses a catalog application by name. `publish_by_key` resolves its
configured Pusher key through the same catalog; this is selection for trusted
native callers, not external REST authentication. `publish_local` deliberately
omits fleet fanout.

```rust
use claviron::broadcast::{BroadcastService, Result};
use claviron::broadcast::protocol::pusher::TriggerEvent;

fn publish_notice(
    service: &BroadcastService,
    channel: &str,
    payload: String,
) -> Result<usize> {
    let event = TriggerEvent {
        name: "notice".into(),
        data: payload.into(),
        channels: vec![channel.into()],
        channel: None,
        socket_id: None,
        info: None,
    }.validate()?;
    service.publish("dashboard", &event)
}
```

The returned count concerns local delivery, not acknowledgements from every
remote worker or proof that a user read the message. This bounded, in-memory
transport is not a durable notification queue; applications needing history
or replay own that persistence.

Pusher-compatible external publishers can use authenticated
`POST /apps/{id}/events`. The service verifies signatures before decoding and
delivering the event. An application without a secret cannot accept signed REST
publication. Laravel's native publisher calls the service directly instead.

## Sessions and channel policy

The current protocol includes connection establishment, ping/pong, public
subscriptions, signed private channels, fleet-wide presence, socket exclusion,
and opt-in `client-*` events on authorized private/presence channels. Outbound
session queues are bounded.

`require_session(application, authenticator)` installs an optional
`SessionAuthenticator` before serving. It verifies an existing application
session; Broadcast creates neither a login nor a guest session and knows no
user schema. Unconfigured applications retain Pusher-compatible anonymous
connection admission. Private/presence channel signatures remain a separate
authorization requirement even when connection admission requires a session.

The authenticator receives the canonical ingress vhost key, not an untrusted
Host header. It can choose the appropriate existing guard for that entrance
without trying unrelated guards or duplicating the service.

## Transport-independent protocol loop

`accept_session` admits a `BroadcastSession`; the ingress owner runs it over a
`MessageStream`. Adapters currently cover HTTP/1 WebSocket upgrade, HTTP/2
WebSocket extended CONNECT, reliable HTTP/3 WebTransport streams, and accepted
Orbit duplex slots. Host transport and origin policy still apply.

WebTransport uses a bounded Text/Binary/Close record codec, not raw WebSocket
frames. A browser needs a compatible stream adapter; an ordinary Pusher
WebSocket constructor cannot open it. Protected WebTransport entrances consume
an initial `broadcast.authenticate` record carrying the existing token before
any Pusher traffic. HTTP/1 and HTTP/2 verify headers before handoff.

The accepting listener owns transport work. Each session also retains a
Broadcast generation permit. Service stop seals admission, signals sessions,
and drains their cleanup before reporting stopped; a later start creates a
fresh generation. Neither owner detaches the session from the other.

## Presence, occupancy, and local hooks

Workers own local channel/socket registries. Orbit fanout carries publications
and membership changes across the same-host fleet. Restart-generation resets
and membership replay remove stale presence rather than leaving permanent ghost
members. Ring loss is observable, not repaired by decoding overwritten slots.

`channel_occupied` and `channel_vacated` describe fleet-wide zero-to-one and
one-to-zero subscription transitions. The master-side occupancy owner decides
them once; standalone mode owns them locally.

`service.hooks().on_occupancy(callback)` also exposes typed local callbacks.
Register them in the master/standalone process and retain the returned
subscription handle. They run in the existing owner's future, after its
application event is published. Slow work delays that owner; dropping a handle
prevents later admission but does not cancel already admitted work. Hooks are
best-effort observations, not proof of user interaction or durable jobs.

## Addressed groups and optional integrations

`service.groups()` provides byte mailboxes and group fanout for channel-layer
adapters. Only process-specific channel names are supported. Callers supply
mailbox capacity/expiry; full direct sends fail, group sends skip full boxes,
and expired messages are removed when accessed. This is not a general shared
worker queue.

The implementation's optional `pusher` feature adds an external HTTP target
adapter through [claviron-pusher](../pusher/README.md). It is not needed for
native publication or local sessions. Edge selects it for its HTTP bindings.

- [WebSocket](../http/docs/upgrade/README.md): HTTP/1 upgrade ownership.
- [Orbit](../orbit/README.md): bounded fleet state and transport.
- [Laravel](https://github.com/claviron/lumenize): native framework publication.
- [Edge](../edge/README.md) and [Runner](../runner/README.md): HTTP composition.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
