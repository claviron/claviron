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

### `quic/`

- [`error.rs`](src/quic/error.rs) — Normal closure and interrupted-stream classification.
- [`drain.rs`](src/quic/drain.rs) — Join connection-owned children on completion, explicit force or panic.
- [`wt/streams.rs`](src/quic/wt/streams.rs) — Bounded per-session input; a saturated session never blocks the shared driver.
- [`wt/session.rs`](src/quic/wt/session.rs) — One application-facing WebTransport session and its routed bidi input; classification of a stream whose session has ended.

## Still in `claviron-edge`

The listener owners (`HttpServer`, `HttpsServer`, `H3Server`), the per-connection
handlers and the TLS selection still live in Edge, because they resolve a host
before they can serve. They move here once that resolution is a contract: given
a connection's facts, return the service for it or a rejection. Who may accept
a WebTransport or H2 CONNECT session for a host and path stays an Edge registry.

## Tests

Test files mirror their owning modules under `tests/` and are included from
them. They run without a provider, a listener or a host.
