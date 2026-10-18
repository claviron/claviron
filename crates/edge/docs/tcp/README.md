# claviron_edge::tcp

Native TCP transport for Claviron's HTTP/1.1 and HTTP/2 edge.

This Edge module integrates Hyper's protocol drivers with Claviron's listener policy,
host admission, prepared applications, and owned connection lifecycle. It is
not a replacement for the upstream `hyper` library and is not an outbound HTTP
client. QUIC and HTTP/3 belong to the [QUIC module](../quic/README.md).

Applications normally select the root `claviron` crate's `edge` feature, adding
`tls` for HTTPS. Direct Edge users select `tcp` and optionally `tls`;
the services remain explicitly registered.
For a host-free, single-framework server, use [Runner](../../../runner/README.md)
instead of carrying the Edge transport composition.

## Drivers and host selection

`HttpServer` serves cleartext HTTP/1.1. A host may explicitly allow its prepared
application with `transport.non_ssl: serve`; otherwise the configured non-TLS
policy applies, with redirect as the default. HTTP-01 challenges are a separate
opt-in capability. Prior-knowledge cleartext HTTP/2 is not implemented.

`HttpsServer` serves HTTP/1.1 and HTTP/2 after TLS negotiation. ClientHello SNI
selects a permitted host, its client-authentication policy, and the ordered
intersection of listener/host protocols before the handshake proceeds.
Certificates come from the shared [TLS resolver](../../../tls/README.md).

For cleartext keep-alive, each request selects its host independently. For TLS,
the accepted host/application binding is retained; a later Host or authority
cannot silently switch it. Incompatible authority receives `421 Misdirected
Request`. Request-time ACL/redirect policy can refresh without replacing that
connection's TLS identity or application.

## Configure named TCP listeners

The drivers consume their own projections of the prepared `http.yaml` catalog.
Each group declares `transport: tcp`, its `listen` address or addresses,
`protocols`, and whether `tls` is enabled. A missing HTTP document declares no
listeners. TLS certificates and session settings belong to `tls.yaml`.

The principal policy groups are:

- Connection admission: `max_connections`, `acquire_timeout`, and
  `graceful_timeout`.
- TCP settings, including `tcp.accept_retry` for socket resource-pressure
  recovery.
- H1 parser/header policy and H2 stream/flow-control policy.
- `requests` and `responses` for application-facing HTTP limits.
- `proxy` for trusted peer networks and forwarding-header selection.

The [shared HTTP contract](../../../http/README.md#named-listeners-and-policy) explains
required body limits and inheritance. Header deadlines protect an earlier phase
than request routing; an application ACL does not replace them. If no TCP
connection budget is declared, configuration may derive one from the process's
file-descriptor limit. Explicit `max_connections: null` remains unlimited.

Listener capability declarations and host permissions are independent: opening
an HTTPS group does not authorize every host on it, and declaring protocol
options does not enable a protocol excluded by its host's protocol list.

## Dispatch and native hand-offs

Accepted connections carry actual peer/listener/TLS context into the common
HTTP boundary. Shared host admission runs before application dispatch and native
CONNECT handling. Ordinary requests become `RouteBody`/`RouteResponse` traffic;
only the application boundary chooses Axum versus an Axum-free handler. Router
factories are not called per request.

H1 upgrades and H2 extended CONNECT require host permission and a registered
handler. The driver supplies `SessionSpawner` after admission so a handler can
submit its hand-off before returning success. This retains the original
connection capacity until the session finishes. Enabling native protocol
mechanics alone neither authenticates a session nor registers a WebSocket
application.

## Reload, recovery, and drain

All addresses for a generation are bound before acceptance starts. Addresses in
one group share its admission gate; different groups have independent gates.
These gates belong to each serving process, not a fleet-global connection budget.
Workers use the process-role-specific socket-sharing policy; standalone binds
exclusively, and the supervised master serves no HTTP traffic.

Reload can prepare policy for future connections. Changes to group membership,
security mode, bind addresses, or gate limits require restart while listeners
are active. A rejected change is not partially published to that driver. An
existing connection retains its accepted snapshot; retirement of its host
binding initiates graceful closure without rebinding unrelated listeners.

OS descriptor/buffer/memory pressure is handled per socket under the configured
accept-retry policy, without stopping other sockets or existing connections.
Unrecoverable accept failure stops admission for the owning generation and
drains its work before reporting failure; it does not cancel the whole process.

Shutdown closes listeners and native-session admission, then drains and joins
owned connections and sessions. An absent grace deadline means indefinite
drain. Only an explicitly bounded policy authorizes forced teardown; even then,
owned task completion is joined before the generation reports stopped.

## Features and related crates

The implementation enables `tls` by default. The root facade disables that
implementation default and opts in through its own `tls` feature; `edge` alone
does not silently enable HTTPS. Other implementation capabilities are `acme`,
`metrics`, and `geoip`. HTTP-01 challenge fulfillment requires a configured
challenge provider; it is not an ACME issuance client in this crate.

- [HTTP](../../../http/README.md): shared types, configuration, and policy.
- [Edge](../../README.md): host publication, admission, and application selection.
- [TLS](../../../tls/README.md): certificates, client authentication, and ALPN helpers.
- [H3](../quic/README.md): the separate QUIC transport using the same host boundary.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
