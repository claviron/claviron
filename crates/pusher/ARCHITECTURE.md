# Pusher Protocol Boundary

`pusher` owns the Pusher-compatible wire model and the host-facing
target configuration. It does not own WebSocket sessions, fleet membership,
vhosts, certificates, or service lifecycle.

The default feature set is transport-free. It provides typed app IDs, keys,
and redacted secrets; REST authentication/signing; client/server WebSocket and
publication messages; and `PusherConfig`, which selects a local fleet service
or an external HTTP target.

The optional `http` feature provides the external upstream adapter.
A consumer that only runs the in-process service does not pay for an outbound
HTTP stack.

## Configuration

A host chooses where its Pusher surface is handled:

```yaml
broadcast:
  target: fleet
```

or:

```yaml
broadcast:
  target:
    http:
      address: 127.0.0.1:6001
      connect_timeout: 5s
      request_timeout: 600ms
      idle_timeout: 120s
```

`fleet` contains no socket address or application credentials. It means that
the host publishes the configured applications of the worker-local
`BroadcastService`. The service configuration is the single source of truth for
Pusher app IDs, keys, and secrets.

Unknown configuration fields fail parsing. App IDs and keys are typed path
segments and reject reserved URI characters. Secrets never reveal their value
through `Debug`.

## Runtime ownership

```text
HostConfig -> PusherConfig -> fleet | external HTTP
                                 |
                                 v
                         claviron-broadcast
                    service + live sessions
```

Broadcast's opt-in `edge` module resolves the target during host preparation.
For a fleet target it obtains the registered `BroadcastService` and creates exact
`/app/{key}` upgrade routes. Exact routes preserve the constant-time lookup of
the shared H1 upgrade registry; the hot path does not scan the app catalog.

`broadcast` depends on this crate for protocol, authentication, and config
types. The reverse dependency is forbidden. Internal REST publication is
implemented by Broadcast against contracts owned here; this crate never depends
on the live service.

## External HTTP adapter

With `http`, the adapter proxies the original public Pusher paths to an
HTTP/1.1 upstream:

- `/app/{key}` for WebSocket upgrades;
- `/apps/{id}` and `/apps/{id}/...` for REST requests.

It preserves the request path and query, replaces forwarding headers through
the shared Claviron boundary, strips hop-by-hop response headers, and keeps an
upgraded tunnel under listener session ownership. Connect, request, and idle
timeouts have separate meanings.

`PusherAdapter::accepts_path` selects these namespaces without reading a body
or depending on host routing. Runner and the optional Broadcast Edge adapter
can use it before transferring request ownership.

The axum-free adapter is the one entry to that implementation. Errors become HTTP responses rather than terminating the host
service.

## Invariants

- Host selection and application credentials are separate configuration
  domains.
- A local fleet target never creates an upstream TCP connection.
- The protocol crate owns no long-lived task or mutable session registry.
- REST signatures are verified before JSON decoding or local/fleet delivery.
- Upgrade admission occurs before `101 Switching Protocols`.
- Accepted sockets remain owned by both listener lifecycle and the Broadcast
  service generation.
- Authentication/session integration is a later boundary; it does not belong
  in the initial protocol crate.

The Claviron facade exposes these wire contracts at
`claviron::broadcast::protocol::pusher`. Select `broadcast` for the live service
on the facade. The broadcast crate's own `pusher` feature enables an external
HTTP upstream adapter. Broadcast's optional Edge integration selects it for
host-based exposure; Runner and direct HTTP consumers need no Edge feature.
Neither this crate nor HTTP depends on Edge or the Broadcast service.
