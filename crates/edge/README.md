# claviron-edge

HTTP host admission, routing, application dispatch, and optional native transports.

The edge decides which virtual host owns a request, applies that host's admission
policy, and selects its application. Its opt-in [TCP](docs/tcp/README.md) and
[QUIC](docs/quic/README.md) modules own native wire protocols and connection
lifecycle separately from the shared hosting pipeline.

Applications normally use `claviron::edge` through the root crate's `edge`
feature. Enable TLS, HTTP/3, and application adapters
explicitly as needed. The edge is neither an interpreter nor a language-specific
application framework.

Direct crate consumers select `tcp` for HTTP/1.1 and HTTP/2, add `tls` for
the HTTPS driver, or select `quic` for HTTP/3 and its native WebTransport
bindings. The existing `http3` feature exposes shared H3 declarations;
`quic` additionally selects the driver. Selecting a feature does not register
or start a listener. Runner retains its own ingress and does not depend on Edge.

## Two HTTP server compositions

Claviron offers two main HTTP server compositions. Choose the edge when building
a comprehensive application server directly on Claviron: multiple virtual hosts,
native services, script routes, embedded applications, static files, and upstreams
can coexist under its host-aware routing and admission policy. It can also serve
a single host with a rich mix of those capabilities.

For one framework or application that does not need a virtual-host table, use
[claviron-runner](../runner/README.md). A runner surrounds one application owner
with shared routing and ingress. It can serve HTTP directly, act as an HTTP
upstream behind an edge or another proxy, or receive same-host requests over
Orbit Link. The edge can therefore host applications directly or route to
dedicated runners without making their execution engines part of the edge.

## Configuration declarations

`claviron::edge_config` is a facade alias for `claviron::edge::config`, available
with the root `edge` feature. In the current tree it is a module of this crate,
not a separate `claviron-edge-config` package.

`HostConfig` combines host-level declarations with normalized route entries.
`RouteConfig` holds application and file declarations; reusable listener,
transport, request, response, and file-policy vocabulary lives in
[claviron-http](../http/README.md). Parsing a declaration does not start its
listener or application. Runtime composition and publication remain separate.

Host policy distinguishes several boundaries:

- `listeners` selects named listeners. Omission permits all; an explicit empty
  list permits none. Explicit names are checked before host publication.
- `transport` narrows listener protocol capabilities; a host cannot enable a
  protocol the listener does not support.
- `tls` describes handshake policy. Request and response policy are separate
  declarations, not alternative ways to configure the handshake.

Prefix-keyed `routes` select response owners and route directives. Normalization
resolves `/` before child entries, regardless of map order, and rejects conflicting
flat/root declarations or multiple application owners on one entry. Filesystem
`root` and `index` defaults can be inherited; a child does not implicitly inherit
the root's PHP, Orbit, or other application owner. Matching and fallback are
prepared routing behavior, not an arbitrary per-request configuration merge.

The outer host schema composes feature-dependent sections and route-extension
directives; it is not uniformly a closed, strict record. Fixed nested policy
records apply their own parsing rules. Unknown or unavailable extension directives
are rejected during preparation. Accepting configuration data does not install a
disabled engine or grant a route an undeclared runtime capability.

## From host declaration to request

A host declares its identity, aliases, root, routes, and policies through
`HostConfig`, configuration documents, or a registered host source. During
publication, Claviron prepares the application and transport policy for each
changed host and publishes the resulting table together.

Requests select a prepared application; they do not call its factory. An
unchanged host reuses its built application. A rejected candidate leaves the
previously published table in service instead of partially publishing a router
and its policy.

Host selection and admission include authority, listener/protocol policy,
TLS binding where applicable, access controls, and configured host responses.
Application dispatch follows admission; choosing an Axum-free handler does not
skip those checks. Forwarded origin information is distinct from physical peer
identity and follows the configured trust policy.

## Adding an application

In a server registration callback, declare a host and attach a merged Axum
application:

```rust
use claviron::{
    axum::{Router, routing::get},
    edge::{self, Host, HostAppRegistry, HostConfig},
    runtime::{AppState, registry},
};

fn register_host(state: &AppState) -> registry::Result<()> {
    edge::register_host(
        state,
        "example.test".parse::<Host>()
            .map_err(|error| registry::Error::msg(error.to_string()))?,
        HostConfig::new("public"),
    )?;

    let apps = state.registry_ref().resolve::<HostAppRegistry>()
        .ok_or_else(|| registry::Error::msg("edge registration is required"))?;

    apps.register_app("example.test", |_state| {
        Router::new().route("/health", get(|| async { "ok" }))
    });
    Ok(())
}
```

This uses the root `edge` feature and assumes the standard edge infrastructure
has already been registered. It does not configure or start a listener. The key
passed to `register_app` selects the canonical vhost, not an arbitrary alias.

`register_hosts` lets an integration supply multiple declarations from its
prepared configuration. Host-source callbacks run while rebuilding the host
table, never for individual requests; they should consume prepared snapshots
rather than perform their own file I/O.

## Application shapes

`HostAppRegistry` supports distinct ownership arrangements:

- `register_app` merges Rust routes into Claviron's configured pipeline, retaining
  its configured static-file and application layers.
- `register_router` installs a complete Axum router whose application middleware
  is owned by the caller. Host admission still applies.
- `register_bypass` installs a native handler without requiring an Axum router.
  Here “bypass” refers to the router shape, not to host access controls.

Services can implement the host-aware `Routable` contract and register the same
shared service instance with `register_routable`. This connects component
lifecycle to per-host route construction. It is distinct from the host-free
`claviron::http::routing::Routable` contract used by single-application routing.

Static files, native routes, embedded applications, FastCGI, HTTP upstreams, and
Orbit application links are composable capabilities, not interchangeable
execution engines. Enabling one does not select or configure all the others.

The [application-shaped pipeline](../../docs/design/BYPASS_PIPELINE.md) explains
this composition choice. [Host binding](../../docs/design/HOST_BINDING.md) and
[early-data policy](../../docs/design/EARLY_DATA_POLICY.md) describe the
transport boundaries that remain in front of it.

## Route extensions

Extensions are resolved and compiled while building a host's routes. Unknown or
unavailable directives fail preparation rather than being silently ignored.
Request handling uses the prepared route layers, not a per-request extension
registry lookup.

`claviron::edge::extension` exposes the generic directive, condition and layer
contracts with the ordinary Axum routing surface; no separate extension feature
is required. Applications and frameworks own concrete directives such as AJAX
admission, CSRF and rate limits, including their errors and response policy.
Register them through `Server::with_edge_extension`, `HostAppRegistry` or a
matched `Routable` provider. Claviron installs no application policy implicitly.

A framework built on Claviron can add its own typed settings to a vhost's
routing configuration without changing Claviron's schema. For example, after
registering framework-owned `locale` and `csrf` directives, a route could declare:

```yaml
routes:
  /:
    locale: tr
    csrf: true
```

These are illustrative framework settings, not built-in Claviron directives.
The framework owns their types, validation, request behavior and failure
responses. Claviron supplies the configuration extension points and prepares
their effects with the host's routes. Host-level layer inheritance is explicit
through `RouteExtension::inherit_from_host`, not automatic for every custom key.

## Reload and connection lifetime

Host publication, application preparation, and listener ownership are separate.
A host change does not rebuild unrelated hosts. Changing a transport's bound
listener settings may require a restart according to that transport's contract.

Established TLS connections retain a validated binding. Replacement or removal
of that binding follows connection retirement and graceful-drain rules; it is
not equivalent to reinterpreting an existing connection as a different TLS
host. Policies designed for request-time lookup can change without rebuilding
the application. Do not assume every reload updates every established
connection in the same way.

## HTTP/3 and WebTransport ownership

Edge uses the crates.io H3 fork family `0.0.2`. One native connection driver
continues accepting HTTP requests after a WebTransport CONNECT succeeds.
Pending HEADERS resolution, session authorization and CONNECT response I/O
progress independently; none hands the connection to an application session.

`H3WebTransportSession::accept_bi().await` returns an
`Option<H3WebTransportBiStream>`, already routed to that session ID rather than
a native resolver. The input queue is bounded by the accepting listener's
`quic.max_concurrent_bidi_streams`; an unavailable or full receiver refuses
that stream without waiting in the shared HTTP acceptance loop. Ordinary HTTP
requests never enter this receiver. Native outgoing stream/datagram methods
remain available through the session's underlying H3 transport.

Host admission, Origin checks, 0-RTT rejection and the original connection
permit remain in place. On graceful shutdown or host retirement, new HTTP
admission stops while the driver remains alive for accepted work. The native
owner joins request/session tasks before releasing capacity, including after
an explicitly forced drain. Built-in Lua and Broadcast handlers retain their
existing single-bidi message loops.

## Related crates

- [claviron-runtime](https://github.com/claviron/claviron/tree/main/crates/runtime): composition, events, and drain.
- [claviron-runner](https://github.com/claviron/claviron/tree/main/crates/runner): a host-free shell around one application owner.
- [claviron-app](https://github.com/claviron/claviron/tree/main/crates/app): hosted application identity and control.
- [claviron-supervisor](https://github.com/claviron/claviron/tree/main/crates/supervisor): process-family lifecycle.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
