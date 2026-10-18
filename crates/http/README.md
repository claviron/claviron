# claviron-http

Shared HTTP types, policy, configuration data, and routing contracts for Claviron.

This crate is the common HTTP vocabulary used by transports, application
adapters, and handlers. It does not bind sockets, perform TLS handshakes, select
virtual hosts, or execute an application engine. Those responsibilities remain
with the transport, Edge, and application owners.

Applications normally use `claviron::http` through the root crate's `http`
feature. Add `axum` for framework bindings. Both
[Edge](../edge/README.md) and [Runner](../runner/README.md) consume these
contracts: using HTTP primitives does not require a multi-vhost server.

## Where it fits

- `claviron-http` defines the common request/response boundary and HTTP policy.
- Edge's [TCP module](../edge/docs/tcp/README.md) drives HTTP/1.1 and HTTP/2.
- Edge's [QUIC module](../edge/docs/quic/README.md) drives HTTP/3.
- [claviron-tls](../tls/README.md) supplies certificate and TLS policy machinery.
- Edge compiles host routing and admission; Runner composes a single application.

Transport drivers retain native framing and connection ownership. Application
adapters receive the common HTTP shape instead of importing every driver's
stream and body types.

The opt-in `websocket` feature exposes [HTTP/1 upgrade integration](docs/upgrade/README.md)
as `claviron_http::upgrade`. It retains the existing `server` and Axum
requirements; HTTP-only consumers do not select its Hyper or SHA-1 dependencies.
H2 CONNECT and H3 WebTransport remain separate native session paths.

## Bodies, responses, and failures

`RouteBody` is a type-erased, `Send` HTTP body carrying byte frames and a shared
error type. `RouteResponse` is its response counterpart. `boxed_body` adapts a
native body at the boundary; `empty_body` and `bytes_body` construct simple
responses. Erasing the body type does not require collecting a stream.

```rust
use claviron::http::{RouteResponse, bytes_body};

fn reply() -> RouteResponse {
    RouteResponse::new(bytes_body("ok".into()))
}
```

This needs only the facade's `http` feature. An Axum application uses its usual
body type inside the router; `into_axum_response` and `from_axum_response`
convert at the framework boundary. The bypass path remains Axum-free.

`ServiceResult::Handled` claims a request; `Unhandled` passes its request or
context onward. A routing miss is not encoded as a magic response status.
`RequestContext` provides a borrowed view for inspection without body ownership.

The framework-neutral `Error` carries a public status, code, and message, with
an optional private diagnostic source. Its Axum response binding exposes only
the public fields. Domain errors do not acquire an arbitrary HTTP status
implicitly; the handler chooses the appropriate public classification.

## Request context and forwarding

Typed extensions carry resolved client IP, requested/canonical host identity,
path prefixes, and environment access. With server support, `ConnCtx` records
the actual listener, peer, transport, and TLS facts. Forwarded origin information
is separate from those connection facts and is honored only under the caller's
configured trust policy.

Shared helpers join split Cookie headers, resolve client IP through trusted
proxies, strip hop-by-hop headers, and validate proxy framing boundaries. The
authority parser checks request-head consistency without polling the body;
matching that authority to an accepted TLS host remains Edge admission policy.

## Named listeners and policy

The configuration types describe named TCP and QUIC groups in `http.yaml`.
Parsing resolves inheritance and validates a candidate; it opens no listeners.
[Congif](https://crates.io/crates/congif) prepares configuration for the runtime
owners that consume it. Transport capability validation rejects unavailable
drivers instead of silently discarding their declarations.

Each listener must declare `requests.max_body_size` and
`requests.body_read_timeout`, directly or through listener defaults. A limit may
explicitly be `off`; absence is not permission to invent one. Body-read timeout
bounds a pause between body chunks, not the total duration of an upload.

`HttpPolicy` combines request and response policy. Host/application body-size,
body-read, and buffer-size limits can narrow the listener's bounds. Buffering
preference, queue timeout, and response-head timeout have their own replacement
rules. Response-head time is distinct from response streaming and from an
execution engine's guest timeout. Body delivery also accounts for the backend's
declared needs; a policy field alone does not promise identical buffering in
every adapter.

Omitted inheritable settings, explicit null, and explicit values remain distinct;
null is accepted only for nullable settings. Lists replace rather than append.
An H1 header-read timeout can preserve the native driver's default, be disabled,
or use a declared duration. Header parsing limits are separate from body limits.

## Application routes and native sessions

The implementation's `routing` capability adds the host-free `Routable`
contract, provider-owned route composition, typed extension codecs, and immutable
prefix dispatch. Edge has a separate host-aware `Routable` contract. Extensions
are compiled during composition rather than looked up dynamically per request.

The server-only `session` module owns native upgrade/CONNECT hand-offs for a
listener generation. These are transport sessions, not login sessions or cookie
storage. Submitted work retains the original connection permit and participates
in drain. Protocol handlers still own their close handshake; detached tasks do
not become listener-owned automatically.

## Optional capabilities

The implementation has no default features. `axum` enables framework adapters
and runtime-DI extractors; `routing` adds route composition; `server` adds
listener/connection context and native session ownership. `cache` adds the
configured cache extractor. `metrics` adds HTTP/server observations and metrics
integration; watchdog configuration remains an explicit runtime policy.

The `js`, `lua`, `orbit`, `acme`, and `http3` implementation features expose
related declaration shapes, not execution engines or listening sockets. Facade
features compose and forward these capabilities; their names and scope need
not match the implementation features one for one.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
