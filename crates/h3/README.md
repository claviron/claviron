# claviron-h3

Native QUIC, HTTP/3, and WebTransport ingress for the Claviron edge.

This crate accepts UDP connections with Quinn, negotiates TLS, and dispatches
HTTP/3 streams directly into Claviron's shared host/application boundary. It is
not an HTTP/3 sidecar proxy. Application selection stays with
[Edge](../edge/README.md), while this driver owns native QUIC/H3 streams,
response framing, and connection-local session drain.

Applications normally select the root `claviron` crate's `edge` and `http3`
features. `http3` includes TLS support. Registering `H3Server` does not by itself
declare a UDP listener, authorize a host, or advertise HTTP/3 to clients.

## Configure and expose HTTP/3

The transport consumes named `transport: quic` groups from the prepared
`http.yaml` catalog. Each group declares its UDP `listen` addresses and
`protocols: [h3]`. QUIC requires TLS; it cannot be configured as a cleartext
listener. TCP and UDP may use the same numeric port because their sockets are
different transports.

Group connection admission and handshake/drain settings are separate from
`quic` stream, idle, keep-alive, and early-data settings. Request and response
limits belong to the shared `requests` and `responses` policy. See the
[HTTP policy contract](../http/README.md#named-listeners-and-policy) for the
required body declarations. Certificates come from `tls.yaml` and the shared
[TLS resolver](../tls/README.md).

The host must separately permit H3 and the accepting listener. Operators can
advertise it selectively with an `Alt-Svc` response header. Advertisement is
not tied to enabling 0-RTT. This driver does not internally fall back to H2/H1;
clients choose an appropriate new connection when another protocol is needed.

QUIC runs over UDP. Idle and keep-alive policy matter for detecting a peer that
disappears without closing a long-lived stream. Configure that behavior for the
deployment rather than assuming TCP-style disconnect detection.

## Identity, admission, and early data

ClientHello selects the captured host and its authentication policy before
handshake completion, including host-specific mandatory client certificates.
Ordinary request authority is checked against that accepted binding before
body adaptation. A wrong-host request receives `421 Misdirected Request` rather
than being routed to another application on the same connection.

Early data is a separate transport capability and per-host replay policy.
Requests rejected by replay policy receive `425 Too Early`. Permitting selected
methods is not an application-level exactly-once guarantee. Client-authenticated
QUIC configurations currently disable early data, and WebTransport does not
accept early-data sessions.

Shared admission also applies current ACL/redirect rules for the captured host.
Refreshing those rules does not reselect the connection's TLS identity or
application and does not retroactively revoke an accepted session.

## Native bodies and responses

Bodyless requests take the empty/buffered path. Requests that may carry a body
bridge their native receive stream into `RouteBody` with backpressure and
trailers rather than collecting the stream first. Declared and observed body
sizes are checked against the accepted listener policy.

The prepared application determines Axum versus bypass dispatch; this transport
does not maintain two application-routing implementations. Responses are written
as native H3 headers, data, trailers, and stream completion. Body or transport
errors are surfaced instead of being turned into a fake successful response.

Resolving one request's headers does not block acceptance of unrelated streams
on that connection. Native stream resolution and session ownership remain in
the H3 driver, not in an application middleware task.

## WebTransport

Select the facade's `webtransport` capability, which also selects `edge` and
`http3` (including TLS), and register a code-owned binding for the canonical host
and exact path. Listener configuration remains explicit. WebTransport is a native
session lane, not an ordinary Axum route or a boolean host option. The connection advertises
support only when its host has a registered binding.

The handler receives the shared admission/authentication context before session
acceptance. Missing bindings and rejected authentication do not open a session.
An accepted session retains the original connection admission ownership and
can observe graceful shutdown. Protocol behavior and cooperative close remain
handler-owned.

Ordinary HTTP request middleware does not automatically account for native
WebTransport sessions; their metrics must reflect the native session boundary.

## Reload and shutdown

The provider prepares configuration through Congif and resolves runtime
dependencies before binding. Each named group owns its gate and connection
tasks; each accepted connection owns its stream/session tasks. The master
serves no traffic, while standalone and worker binds follow their respective
socket-sharing policies. Group connection budgets apply per serving process,
not as one fleet-global gate.

Policy reload applies to new connections. Active group membership, bind, or
gate changes require restart. Accepted connections retain their captured
snapshot and application. Host-binding retirement sends GOAWAY and stops new
streams while already accepted streams and sessions drain; unrelated hosts do
not need their listeners rebuilt.

Shutdown stops new handshakes, drains owned work, and releases QUIC resources.
No grace deadline means indefinite drain. With an explicit finite grace policy,
forced teardown still joins connection-local children before releasing capacity.
Application-created detached tasks are outside this transport ownership.

## Features and related crates

The implementation has no default features; its optional `metrics` and `geoip`
features add their corresponding observations. Facade HTTP/3 and WebTransport
selections compose the transport and Edge capabilities for applications.

- [HTTP](../http/README.md): common request types, body limits, and listener data.
- [Hyper](../hyper/README.md): the separate HTTP/1.1 and HTTP/2 TCP drivers.
- [TLS](../tls/README.md): shared certificates and handshake policy.
- [Edge](../edge/README.md): host publication and native session bindings.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
