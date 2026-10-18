# claviron-edge

Protocol-independent HTTP host admission, routing, and application dispatch.

The edge decides which virtual host owns a request, applies that host's admission
policy, and selects its application. Native HTTP/1 and HTTP/2 drivers and the
HTTP/3 integration own their wire protocols; this crate owns the common hosting
contract above them.

Applications normally use `claviron::edge` through the root crate's `edge`
feature. Enable TLS, HTTP/3, application adapters, and ready-made route extensions
explicitly as needed. The edge is neither an interpreter nor a language-specific
application framework.

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

## Route extensions

Extensions are resolved and compiled while building a host's routes. Unknown or
unavailable directives fail preparation rather than being silently ignored.
Request handling uses the prepared route layers, not a per-request extension
registry lookup.

The implementation's `extensions` feature supplies ready-made directives. The
root facade exposes it as `edge-extensions`, including the typed
`claviron::edge::extension` surface. Enabling that feature makes directives
available; it does not automatically impose rate limits, authentication, or
CSRF policy on every route.

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

## Related crates

- [claviron-runtime](https://github.com/claviron/claviron/tree/main/crates/runtime): composition, events, and drain.
- [claviron-runner](https://github.com/claviron/claviron/tree/main/crates/runner): a host-free shell around one application owner.
- [claviron-app](https://github.com/claviron/claviron/tree/main/crates/app): hosted application identity and control.
- [claviron-supervisor](https://github.com/claviron/claviron/tree/main/crates/supervisor): process-family lifecycle.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
