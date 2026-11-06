# claviron-transport Architecture

## Boundary

This crate accepts connections and drives HTTP over them. It does not select a
host, read host configuration, compile routes or know a backend. Its inputs are
listener settings from `claviron-http` and a service to hand requests to.

The dependency points one way: `claviron-edge` depends on this crate. Nothing
here may name an Edge type. What only Edge needs is expressed as a contract
declared here and implemented there.

`claviron-http` remains the domain: listener and protocol configuration,
connection facts (`ConnCtx`), request policy and the body types.

## Sources

- [`lib.rs`](src/lib.rs) — Module roots, each behind its feature.
- [`instances.rs`](src/instances.rs) — Cold listener declarations, retained handles and weak references to draining instances.
- [`geo.rs`](src/geo.rs) — Optional peer country lookup for a connection's facts.

### `tcp/`

- [`listener.rs`](src/tcp/listener.rs) — Bind groups, shared capacity, accepted tasks/session ownership and drain.
- [`listener/accept.rs`](src/tcp/listener/accept.rs) — Socket-local accept error classification and declared recovery backoff.
- [`io.rs`](src/tcp/io.rs) — Tokio's native TCP read readiness and write shutdown
  owned by the protocol; a separate error observer on the same socket, awaited
  only by an H1 connection with half-close on.
- [`protocol.rs`](src/tcp/protocol.rs) — Hyper H1 builder and TCP socket options from resolved config.
- [`body.rs`](src/tcp/body.rs) — Lazy body erasure and proven-empty-body handling; preserve trailers.
- [`mod.rs`](src/tcp/mod.rs) — `Requests`, the owner of one connection's requests, and `HandlerRequests`, which gives them all to one `OtherHandler`.
- [`plain.rs`](src/tcp/plain.rs) — Drive one cleartext HTTP/1.1 connection: socket setup, connection facts, Hyper and graceful/forced drain. `Cleartext` is what a listener publishes; `Single` is its host-free implementation.
- [`secure.rs`](src/tcp/secure.rs) — TLS handshake deadline, ALPN and protocol choice, connection facts, H1/H2 driving and graceful/forced drain. `Secure` selects a TLS configuration and a `SecureHost` from the ClientHello; `Single` is its host-free implementation.

### `quic/`

- [`error.rs`](src/quic/error.rs) — Normal closure and interrupted-stream classification.
- [`drain.rs`](src/quic/drain.rs) — Join connection-owned children on completion, explicit force or panic.
- [`wt/streams.rs`](src/quic/wt/streams.rs) — Bounded per-session input; a saturated session never blocks the shared driver.
- [`wt/session.rs`](src/quic/wt/session.rs) — One application-facing WebTransport session and its routed bidi input; classification of a stream whose session has ended.

## The contract with a host selector

A listener's owner publishes one snapshot. For each accepted connection the
driver completes the connection's facts and asks for that connection's request
owner, which is then called on the connection's task. Selecting a host, caching
the selection and answering a request for an unknown name all happen behind
that call.

A cleartext connection has nothing to select by until a request arrives, so
`Cleartext::connection` takes only the facts. A TLS connection is selected by
its ClientHello: `Secure::select` returns the TLS configuration and a
`SecureHost` together, so the host cannot change underneath a connection. The
host then answers what the driver must know before it can serve: whether this
listener serves it, which protocols it accepts and whether ALPN is required,
when the connection should retire, and finally its request owner.

A server name is used twice and only one use is host selection. Choosing a
certificate by server name happens inside the TLS configuration's resolver and
needs no host. Choosing which configuration applies — client authentication,
the ALPN list, session storage — and who owns the connection is what `select`
decides. A listener with many certificates and one owner is therefore still a
`Single`.

Edge implements both with its virtual-host table. `Single` implements both
with one handler: it selects nothing, applies no request policy, attaches
nothing to the request and never retires a connection.

## Still in `claviron-edge`

The TCP drivers are here; their listener owners (`HttpServer`, `HttpsServer`)
are not, because they publish against Edge's host table and prepare each
host's TLS configuration. The HTTP/3 owner, listener and request adaptation
also still live in Edge, together with the registries that decide who may
accept a WebTransport or H2 CONNECT session for a host and path. Who may accept
a WebTransport or H2 CONNECT session for a host and path stays an Edge registry.

## Tests

Test files mirror their owning modules under `tests/` and are included from
them. They run without a provider, a listener or a host.
