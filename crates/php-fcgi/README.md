# claviron-php-fcgi

PHP-FPM gateway with FastCGI connection pooling and streaming responses.

Claviron owns the inbound HTTP boundary and the FastCGI client connections;
PHP-FPM owns its PHP processes, INI, extensions, and request execution. This
crate does not embed libphp, start an FPM master, or install Claviron's native
PHP host functions inside external FPM workers.

Applications normally select the root `claviron` crate's `php_fcgi` feature with
their [Edge](../edge/README.md) composition and access the adapter through
`claviron::fcgi`. Choose [embedded PHP](../php-embed/README.md) instead when PHP
must execute inside the Rust process and use its native service bridges. A
server can explicitly mix both backends on different routes.

## Catalog and routes

`PhpFcgiService` consumes `fpm.yaml` through
[Congif](https://crates.io/crates/congif). The document defines named `fpm`
entries, housekeeping policy, and optional metrics policy. A missing file means
an empty catalog, not an implicit FPM endpoint.

An entry selects a Unix socket or TCP address and declares its connection and
response-head policy. A `pool` block opts into reusable FastCGI connections;
without it, each request opens a one-shot connection. `connect_timeout` and
`head_timeout` are entry-level settings, not pool-sizing fields.

The [shared PHP route](../php/README.md) selects `runner: fcgi`, the allowed
`fpm` entry names, and the document root/front-controller policy. This crate
receives the prepared `PhpRuntime`; it does not reread vhost configuration.
The supplied request context must include trusted connection/host/client facts
and an effective `HttpPolicy`. A missing policy is an execution error, not
unlimited admission.

`PhpFpmAdapter` serves the native bypass path, and `build_fcgi_router` supplies
the optional Axum binding. Both use the same forwarding implementation.
Bypassing Axum does not bypass the caller's HTTP admission or host policy.

## Pool ownership and balancing

Pools are instantiated lazily when named entries are used. Unused catalog
entries do not create warm FastCGI connections. Each serving process owns its
own client pools; their limits must be considered together against FPM's actual
child capacity, not mistaken for one fleet-global limit.

The three sizing fields have separate roles:

- `min_live`: the replenisher's minimum live-connection target.
- `max_idle`: the number of idle reusable connections retained after use.
- `max_live`: the hard live-connection ceiling, with a configured acquisition
  wait when saturated.

FPM's own `pm.*` settings are not changed by these client-side values. Lifetime,
request-count, and idle retirement also belong to the client-pool policy.

Background-created idle connections are validated through FPM's configured ping
path; acquire-time dials prove themselves through their real request. A socket
connect alone is not an application-health guarantee. A failed connection is
retired rather than reused. Suspicion of a stale idle queue is tracked separately
from failure of a freshly dialed socket.

A route naming multiple entries uses the current capacity-based balancer, with
round-robin tie-breaking. One-shot entries can provide overflow when pooled
capacity is unavailable. This is not an ordered-primary failover guarantee or
a distributed load balancer.

## Request and response streaming

Effective HTTP policy and the backend's length requirement determine request
delivery. FPM expects `CONTENT_LENGTH`: an unknown-length request is collected
and counted before forwarding; a known-length body may be forwarded as it
arrives. Both paths enforce the effective body-size cap. PHP-FPM's own INI
limits remain separate.

Responses are forwarded through the native streaming path. The response-head
deadline bounds the wait before FPM starts its reply, not the total duration
of an SSE stream or download. A streamed response retains its FastCGI lease
until completion or abandonment; disconnected-client detection therefore
depends on the inbound transport as well as the backend connection.

CGI scheme, peer, and TLS variables come from Claviron's trusted request context.
Client forwarding headers cannot replace that projection. Declared CGI
environment values are distinct from FPM's own process environment; the
embedded runtime's native function and environment machinery is not injected
through this protocol.

## Failure and replay boundary

A pooled request whose complete body was retained may be sent once more on a
fresh lease after an eligible unanswered connection failure. It is not replayed
after observing response bytes, and a body already consumed as a live stream
cannot be replayed. One-shot requests do not use that pooled replay path.

No response bytes is not proof that PHP performed no side effects: a script
can commit work and fail before emitting output. This transport supplies no
exactly-once guarantee; mutation endpoints still need application-level
idempotency and transaction policy. Pool recovery is not financial recovery.

## Reload and observations

Configuration reload applies the service's prepared catalog and reconciles
materialized entries. It does not edit FPM configuration or restart external
PHP processes. Housekeeping uses the declared `housekeep` policy and the optional
TaskService integration rather than owning application scheduling.

The `metrics` implementation feature exposes client-side FastCGI observations
and optional FPM status collection. Those are distinct families: a warm client
connection and an available FPM child are different facts. The other optional
implementation capabilities are `axum` and `tasks`; facade features compose and
forward the selected integrations.

## Related crates

- [PHP](../php/README.md): backend-neutral route and header contracts.
- [Embedded PHP](../php-embed/README.md): in-process execution, not FastCGI.
- [HTTP](../http/README.md): native bodies, trusted context, and policy.
- [Edge](../edge/README.md): host admission and route ownership.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
