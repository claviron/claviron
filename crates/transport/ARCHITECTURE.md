# claviron-transport Architecture

## Boundary

This crate accepts connections and drives HTTP over them. Connection first:
what a listener serves is decided here, by how it is configured and composed,
not by which crate an application started from.

Without the `edge` feature it selects no host, reads no host configuration,
compiles no route and knows no backend. Its inputs are listener settings from
`claviron-http` and a service to hand requests to.

With the `edge` feature it borrows `claviron-edge`. Edge owns its hosts,
pipelines and registries and opens no socket; the [`edge`](src/edge/mod.rs)
module prepares a listener's snapshot from the host table Edge publishes and
gives each connection to the host Edge selects for it. That module is the only
place an Edge type is named. The dependency points one way: Edge never depends
on this crate.

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
- [`prior.rs`](src/tcp/prior.rs) — Cleartext HTTP/2 by prior knowledge, alone or beside HTTP/1.1 on one socket. The YAML listener declaration still refuses it, so Edge's cleartext listeners stay HTTP/1.1; `Single::prior_knowledge` opts in.
- [`secure.rs`](src/tcp/secure.rs) — TLS handshake deadline, ALPN and protocol choice, connection facts, H1/H2 driving and graceful/forced drain. `Secure` selects a TLS configuration and a `SecureHost` from the ClientHello; `Single` is its host-free implementation.

### `unix.rs`

HTTP over a Unix socket with the same drain and HTTP settings as TCP. `Unix` is what such a listener publishes; `tcp::plain::Single` implements it. It removes the socket file it created and refuses a path a live process answers on.

### `quic/`

- [`mod.rs`](src/quic/mod.rs) — HTTP/3 module exports.
- [`listener.rs`](src/quic/listener.rs) — Endpoint groups, connection capacity, handshake deadline and concurrent request/session stream driving. `Quic` is what a listener publishes, `QuicHost` the owner a connection was accepted for and `QuicRequests` its request owner.
- [`single.rs`](src/quic/single.rs) — `Single`: the host-free implementation that gives every request to one `Owner`.
- [`request.rs`](src/quic/request.rs) — H3 replay/framing checks, lazy frame/trailer adaptation and response IO.
- [`config.rs`](src/quic/config.rs) — QUIC transport parameters from a listener's settings.
- [`wt.rs`](src/quic/wt.rs) — Recognize a WebTransport CONNECT, authorize it beside the connection's other work and hand the session to its `QuicSession` owner.
- [`wt/streams.rs`](src/quic/wt/streams.rs) — Bounded per-session input; a saturated session never blocks the shared driver.
- [`error.rs`](src/quic/error.rs) — Normal closure and interrupted-stream classification.
- [`drain.rs`](src/quic/drain.rs) — Join connection-owned children on completion, explicit force or panic.

### `edge/` (feature `edge`)

- [`mod.rs`](src/edge/mod.rs) — The one module that names Edge types.
- [`tls.rs`](src/edge/tls.rs) — A host's TLS configuration: client authentication from its declared authority, and whether a retained configuration still matches.
- [`service.rs`](src/edge/service.rs) — An accepted connection bound to the host Edge selected; no file loading or backend execution engine.
- [`service/authority.rs`](src/edge/service/authority.rs) — Validated cleartext authority and connection-local name identity reuse; borrowed access to the admitted request.
- [`service/host.rs`](src/edge/service/host.rs) — `HostService` and native request handoff; no fixed Ingress/Identity/Access chain.
- [`service/host/policy.rs`](src/edge/service/host/policy.rs) — Accepted connection facts and the narrowed policy a host context lends.
- [`service/host/cache.rs`](src/edge/service/host/cache.rs) — Cleartext metadata cache, owner selection against the supplied publication, and reuse of the last admitted owner for a repeated authority on that same publication.
- [`service/host/dispatch.rs`](src/edge/service/host/dispatch.rs) — Execute the prepared host owner; retain the direct-call optimization only for a plan without preceding actions.
- [`tcp/mod.rs`](src/edge/tcp/mod.rs) — TCP listener owner exports.
- [`tcp/service.rs`](src/edge/tcp/service.rs) — One TLS connection's requests: authority admission against the handshake, then the bound host.
- [`tcp/plain/server.rs`](src/edge/tcp/plain/server.rs) — `HttpServer`: configuration, snapshots against the published host table and named listener lifetimes.
- [`tcp/plain/handler.rs`](src/edge/tcp/plain/handler.rs) — Cleartext authority selection and host handoff, as this crate's `Requests`.
- [`tcp/plain/conn.rs`](src/edge/tcp/plain/conn.rs) — Public `ConnectionHandler` for caller-accepted streams.
- [`tcp/plain/error.rs`](src/edge/tcp/plain/error.rs) — Cleartext driver errors.
- [`tcp/secure/server.rs`](src/edge/tcp/secure/server.rs) — `HttpsServer`: configuration owner, named listener lifetimes and TLS policy preparation.
- [`tcp/secure/tls.rs`](src/edge/tcp/secure/tls.rs) — Per-host TLS configuration, ALPN intersection and TLS policy epochs; selects configuration and host from a server name.
- [`tcp/secure/conn.rs`](src/edge/tcp/secure/conn.rs) — The SNI host as `SecureHost`: listener admission, protocol policy, retirement and the application binding; public `ConnectionHandler`.
- [`tcp/secure/error.rs`](src/edge/tcp/secure/error.rs) — Why a selected host refuses an accepted connection.
- [`quic/server.rs`](src/edge/quic/server.rs) — `H3Server`: configuration owner, snapshots and named listener lifetimes.
- [`quic/tls.rs`](src/edge/quic/tls.rs) — Deferred SNI authentication and coherent host/TLS selector views.
- [`quic/conn.rs`](src/edge/quic/conn.rs) — The SNI host as `QuicHost`: early-data and WebTransport policy, retirement, the handoff to its execution, and sessions for the handlers Edge's registry binds.

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

A connection's request owner also receives the connection's session owner.
`tcp::attach_sessions` hands it only to a request that can take over its
connection, a CONNECT or an `Upgrade`, so an ordinary request pays nothing for
it. Whatever an upgrade starts is joined with the listener's drain through it.

The `edge` module implements both with Edge's virtual-host table. `Single` implements both
with one `Owner`: it selects nothing and never retires a connection.

## Owners without a host

Every driver runs without Edge: `tcp::plain::Single`, `tcp::secure::Single` and
`quic::Single` serve a listener with one [`Owner`](src/owner.rs). They are
composed from the driver's own serve functions; nothing here reads a
configuration file for them.

An owner is one of two things. `owner::Handler` wraps an `OtherHandler`, which
receives the request as the connection produced it, with no policy applied and
nothing attached. A `HostExecution` from `claviron_pipeline::compile::compile_site`
is a whole site: its policy, files, directives and backends, with the connection's
facts lent to them and no host name.

The listener owners that do read one (`HttpServer`, `HttpsServer`, `H3Server`)
live under `edge/`, because they publish against Edge's host table.

The listeners are pinned over real connections, cleartext, TLS and QUIC, with
and without Edge. A WebTransport session is not exercised that way; no H3
WebTransport client is in the dependency tree.

## Tests

Test files mirror their owning modules under `tests/` and are included from
them. They run without a provider, a listener or a host.
