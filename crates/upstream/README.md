# claviron-upstream

HTTP reverse proxying with explicit origin capacity and lifecycle ownership.

This crate keeps an existing HTTP application behind Claviron while Claviron
owns ingress transport and policy. It prepares backend profiles, selects origins,
manages outbound connections and streaming bodies, and coordinates availability.
It does not open inbound listeners, resolve virtual hosts, serve static files,
or implement an open forward proxy.

Applications normally use `claviron::upstream` through the root crate's
`upstream` feature. [Edge](../edge/README.md) owns the host router/bypass binding;
the proxy core accepts the shared HTTP request/response contracts. This is also
an incremental migration boundary: moving one route to native code need not
replace the whole upstream application.

## Profiles, applications, and protocols

`UpstreamProvider` owns prepared profiles and named `UpstreamApp` matchers.
A profile declares transport, origins, capacity, and policies. An application
maps exact, prefix, or regex route selectors to those profiles, with optional
exclusions. Preparing these matchers is composition work, not per-request
configuration parsing.

Inbound and outbound protocols are independent:

| Boundary | Current transports |
| --- | --- |
| Client to Claviron | The ingress's HTTP/1, HTTP/2, or HTTP/3 capabilities. |
| Claviron to origin | HTTP/1 or HTTP/2 over TCP/Unix sockets, with TLS for `tls://` targets. |

`protocol: h2` with `tcp://` means cleartext H2, not TLS. TLS is explicit in
the origin scheme; `https://` is not an implicit protocol selector. HTTP/3
upstream and generic CONNECT forwarding are not implemented by this crate.

## Declare an upstream application

The provider reads `upstream.yaml` from Claviron's configuration directory.
For example, this profile and root matcher forward to a literal local address:

```yaml
upstreams:
  application:
    protocol: h1
    origins: [tcp://127.0.0.1:8080]
    capacity:
      max_live: 32
    connection:
      connect_timeout: off
apps:
  backend:
    routes:
      - profile: application
        path: /
```

The capacity is illustrative. Here `off` deliberately delegates connection
timeout to the operating system; deployments can instead declare a duration.
Profiles using DNS must supply a finite connect budget because it also bounds
the complete lookup. TLS handshakes have a separate bound.

The enclosing host selects the named application; this document does not
publish a vhost by itself. An absent file declares no upstream profiles/apps.
The runtime's [Congif](https://crates.io/crates/congif) catalog prepares file
contents and environment substitutions. Relative paths follow the application
process's working directory, not the YAML file.

## Capacity follows the protocol

Pools are process-local. Workers may share availability and membership facts
through Orbit, but never borrow each other's live TCP connections.

For H1, `max_live` bounds live, leased, idle, and connecting capacity per origin.
`max_idle` caps retained idle connections; `min_idle` and replenishment policy
can declare a warm floor. A response retains its lease until EOF. Dropping a
partial body closes that connection rather than returning unread bytes to the
pool.

[Hot client pools](../../docs/design/HOT_CLIENT_POOL.md) explains the preparation
and reuse policy, including why a warm floor is not an unconditional promise
that a client is immediately available.

H2 uses a process-local connection per origin with stream-based admission.
`max_streams` sets the stream limit, falling back to `max_live` when omitted.
Finishing or cancelling a response releases its stream permit, not unrelated
streams. SSE remains a streaming HTTP body, not an upgrade special case.

Transparent H1 upgrades and H2 WebSocket extended CONNECT require the ingress's
explicit permission. Accepted tunnels retain outbound origin/profile ownership
until closure; an upgraded connection is not returned to the ordinary H1 pool.

Optional profile-wide breaker limits bound connections, pending requests,
active requests, and retries separately from per-origin capacity. Rejection
is an admission result, not evidence that the origin is unreachable.

## Forwarding, deadlines, and replay

The proxy replaces untrusted forwarding facts using Claviron's accepted
request context, strips hop-by-hop headers, and applies prepared `forward.set`
and `forward.unset` rules. Host authority can follow the origin or preserve the
ingress authority. Response-header policy remains with the host composition.

Profile request/response overrides narrow the effective ingress `HttpPolicy`.
Queue wait, incoming body gaps, response-head wait, and response-body gaps are
separate bounds, not one overall execution deadline. Connection, TLS handshake,
health-probe, and profile-drain settings have separate ownership too.

Retry is disabled by default. Explicit retry policy selects methods, failure
causes, attempt limits, and a worker/profile budget. Replay requires an empty
or bounded, fully retained request body. Bodies exceeding that collection bound
continue on the send-once streaming path; responses are not buffered for replay.

Post-send H1 replay requires proof that no response bytes were observed.
H2 supports eligible pre-send connection retries but conservatively refuses
post-send replay without stream-scoped evidence. Accepted response headers end
replay eligibility; later body failures are not transparently retried. These
rules do not make non-idempotent application actions safe to repeat.

## TLS and DNS are prepared capabilities

Outbound TLS supports native trust, Claviron's configured authority, or an
explicit PEM CA, with optional client identity. It uses the process certificate
manager rather than a second private certificate catalog. Named client
identities are resolved at handshake so renewals reach new connections;
file-based material participates in profile replacement. Missing required trust
or identities fail preparation rather than silently falling back to plaintext.

DNS address and SRV membership use a bound `dyn ResolverFactory`; the
application chooses the resolver implementation. A named profile without a
resolver fails boot. Literal addresses need no DNS driver. Resolution policy
declares address capacity, TTL/refresh behavior, failure handling, and whether
boot requires an initial usable answer or permits an empty one.

Lookups and membership publication happen outside request dispatch. Revision-
checked adoption retains unchanged origin owners, seals removed origins, and
rejects stale candidates. Old IO drains under its original ownership; updating
DNS is not permission to terminate another origin's accepted requests.

## Availability is not one boolean

Reachability and serving eligibility are separate. The runtime supports:

- Passive request failures, driver-drop probes, recovery probes, and optional
  active L4 checks for reachability.
- Dedicated L7 checks with configured method/path/status/body conditions.
- Response-based outlier ejection for serving eligibility.
- Round-robin, least-connections, peak-EWMA, consistent-hash, and fill-first
  selection; fleet-scoped selection is limited to least-connections.

Probe outcomes do not become user traffic or request-latency samples. Application
health failure and a breaker refusal must not masquerade as a dead socket.
Probe schedules, thresholds, recovery policy, and ejection windows are explicit
configuration surfaces. No enabled probe is a substitute for an application's
own consistency checks.

## Reload and drain

The provider compiles candidates before registry adoption. Unchanged profiles
retain their live services; changes replace only affected ownership. Provider-
bound application handles follow current profile publication instead of keeping
an obsolete membership view forever.

Profile stop rejects new admission, drains accepted IO under
`lifecycle.drain_timeout` (schema default: 30 seconds), then closes and joins
remaining outbound work. This explicit profile deadline is distinct from an
inbound listener's potentially indefinite graceful drain. Restart creates fresh
pools. Removed-origin and full-profile shutdown remain separate operations.

Public entry points are organized by responsibility: `UpstreamProvider`,
`binding::UpstreamApp`, `binding::call_upstream_service`,
`proxy::service::UpstreamService`, and the `origin`, `resolve`, `config`, and
`metrics` modules. Host router construction belongs to Edge. The implementation
has no default features; optional `metrics` adds instrumentation, while `axum`
is an integration dependency selection, not another listener or proxy engine.

## Related crates

- [HTTP](../http/README.md): normalized bodies, trusted context, and policy.
- [TLS](../tls/README.md): process certificate management.
- [Orbit](../orbit/README.md): same-host membership and availability coordination.
- [Edge](../edge/README.md): host selection, static fallthrough, and proxy binding.
- [WebSocket](../http/docs/upgrade/README.md): native application upgrades, distinct from
  transparent upstream tunnels.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
