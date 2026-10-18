# The host is part of the connection

Design identifier: `HOST_BINDING`.

Claviron does not wait until application routing to decide what a TLS
connection belongs to. The ClientHello selects a hosted name, its TLS policy
and its application binding together. Configure the client or proxy to connect
to that host; do not treat the accepted connection as a reusable entrance to
any site that happens to be served by the same binary.

This starts from an edge-facing deployment. Running behind another proxy is
possible, but the proxy must preserve this connection-level host choice rather
than expecting a later `Host` header to replace it.

## Select once, check each request

For TLS over TCP and QUIC, the accepted binding captures the requested hostname,
the canonical vhost, its application and the relevant handshake policy.
Listener capabilities and host declarations jointly determine the protocols
that connection may use. Required client authentication is selected before the
handshake finishes, not added after an HTTP request arrives.

Every ordinary HTTP request still has its authority parsed and validated.
A different hostname receives **421 Misdirected Request** instead of being
rerouted to another application on that connection. Ports do not change this
host-identity comparison. Invalid authority is a separate malformed-request
failure; the legal HTTP/1.0 missing-authority case can use the established TLS
binding.

A certificate covering several names is not routing permission. Neither an
overlapping SAN nor an alias pointing to the same application permits a client
to switch the accepted hostname mid-connection.

Unknown or absent SNI cannot select a hosted binding. Refusing that handshake
is a consequence of the model, not its whole purpose: a default certificate
must not silently choose an unrelated application.

## What a reload changes

A new host publication can affect subsequent connection acceptance. Existing
connections retain their accepted TLS/authentication and application binding.
Policies explicitly read live, such as host access rules and early replies,
may change without turning the connection into a different host.

This is not retroactive revocation of an established TLS session. If a change
requires existing sessions to end, that needs its own lifecycle operation.

## Cleartext is different

There is no SNI binding on a cleartext HTTP/1 connection. Each request resolves
its validated authority against the current host table, including on keepalive.
The host's non-SSL policy then selects reject, redirect or explicit serving.
Cleartext cannot satisfy a required TLS client-certificate policy.

Likewise, trusted forwarded headers can help resolve a request's client IP;
they do not select a different TLS host. A proxy routing multiple public names
must use connections with the appropriate upstream SNI instead of relying on
cross-host reuse of one accepted connection.

## Follow the implementation

- [TCP TLS acceptance](../../crates/edge/src/tcp/secure/tls.rs).
- [QUIC host and TLS policy](../../crates/edge/src/quic/tls.rs).
- [Host identity and authority comparison](../../crates/edge/src/service/extensions.rs).
- [Shared request admission](../../crates/edge/src/service/host.rs).
- [Cleartext host selection](../../crates/edge/src/tcp/plain/handler.rs).
- [TLS capabilities](../../crates/tls/README.md).

Use `HOST_BINDING` when a change concerns connection identity rather than
ordinary path routing.
