# claviron-tls

Shared certificate resolution, TLS policy, and mutual TLS configuration for
Claviron services.

This crate prepares the certificate and policy machinery used by TLS endpoints.
It does not open listeners or select HTTP routes. The
[Hyper](../edge/docs/tcp/README.md) and [H3](../edge/docs/quic/README.md) transports own their
handshakes and connections; [Edge](../edge/README.md) owns host admission and
application selection after those connections establish their identity.

Applications normally use `claviron::tls` through the root crate's `tls` feature.
That capability does not open an HTTPS listener by itself. Runner and Edge
compositions decide where TLS is used.

## One certificate source

`CertResolverProvider` prepares `tls.yaml` through
[Congif](https://crates.io/crates/congif) and publishes the shared `CertResolver`.
The resolver maps known SNI names to certificate/key material. Handshake lookup
does not read PEM files or query a database.

Certificate reload builds a replacement snapshot before publishing it. Consumers
retain the same resolver handle; subsequent lookups see the replacement without
every service maintaining a separate certificate store. Missing or unknown SNI
is rejected by the resolver rather than silently receiving its stored default
certificate. Certificate availability alone does not authorize an HTTP host.

`auth::derive_resolver_for_hosts` makes a scoped, point-in-time projection using
shared certificate allocations. It is not a live view: an owner that needs
parent reloads must refresh that projection under its own lifecycle.
`auth::ClientIdentity` is the outbound counterpart: it resolves the configured
identity by name for subsequent client-authenticated handshakes.

## Configuration and material

`tls.yaml` declares:

- `certstore`: a store document path or inline store declaration.
- `default_cert`: the default identity source; omission or null selects the
  conventional `default` identity rather than disabling certificate loading.
- `certs` / `sni_entries`: additional certificate entries, including named store
  entries or explicit PEM pairs. Loaded certificate names populate the resolver.
- `sessions`: TLS resumption retention and lifecycle policy.
- `policy`: process-wide TLS version, cipher-suite, and key-exchange selection.

Certificate selectors distinguish named lookup, explicit file paths, and
database-only identities. Missing files can permit the configured named fallback;
corrupt or inaccessible material is not hidden by trying another source.
Required material must resolve successfully before serving traffic.

Explicit PEM certificate/key pairs use process-cwd-relative paths. Paths within
a certificate store follow that store's directory rules. This is not a promise
that every path is relative to `tls.yaml`.

The cryptographic provider boundary uses rustls with aws-lc-rs. Operator-provided
trust and client-authentication policy remain explicit; enabling TLS does not
silently select a public-root trust bundle for inbound mTLS.

## Building endpoint configurations

Given a prepared `Arc<CertResolver>` named `resolver`, a custom TCP endpoint can
build its rustls configuration with:

```rust
use std::sync::Arc;
use claviron::tls::{Proto, create_tls_config};

let config = create_tls_config(&[Proto::H2, Proto::H1], Arc::clone(&resolver))?;
```

This is an integration fragment, not a complete listener. `Proto` supplies the
ALPN bytes; built-in transports additionally apply their listener/host protocol
intersection and authentication policy. The public builder uses no client
authentication. Use `auth::create_auth_config` or
`auth::create_tcp_auth_config` when client certificates are required.

Client-authentication builders validate the certificate chain against the
declared CA and can add organization/organizational-unit subject policy.
`ClientAuthPolicy` expresses host authentication requirements; the accepting
transport selects the appropriate verifier before completing the handshake.
TLS certificate authentication is distinct from application login, session
cookies, and route authorization.

## Reload boundaries

The shared certificate map can be replaced without replacing the resolver's
identity. Parse, material-loading, or snapshot failures prevent its replacement.
Accepted connections retain their established identity; a renewal affects new
handshakes rather than renegotiating already accepted application requests.

Negotiation policy is a separate, startup-bound process policy. The first
validated policy is retained; changing certificate configuration does not replace
it. Restart to change negotiated version/cipher/key-exchange policy. Unknown
suite/group names and an invalid version range are configuration errors.

`CertAuthority` exposes configured trust and, where the hosting integration
provides it, controlled issuance capability. Trust changes require their own
restart boundary; certificate-map reload is not authority replacement.

## TLS resumption and hosting

The built-in public TCP and QUIC transports use explicit, separate domains for
fleet-shared TLS session resumption. Client-authenticated configurations retain
isolated local storage. Calling `create_tls_config` alone does not install the
fleet session adapter.

These are TLS resumption records, not HTTP authentication sessions. Retention,
resource layout, and cleanup follow the managed shared-state contract; restarting
a process does not imply that all shared material has disappeared.

Optional database/hosting support adds certificate loading and update helpers.
`CertificateManager` applies local certificate changes; `CertificateListener`
consumes the configured PostgreSQL notification path. Application composition
must explicitly supply hosting and database capabilities. A database pool alone
does not opt an application into certificate management.

## Features and scope

The implementation has no default features. `db` adds hosting/database-backed
certificate integration; `postgres` enables its PostgreSQL support. The facade
composes hosting through its `hosting` and database-backend selections
for applications that need database-managed certificates.

Certificate parsing, generation, and import primitives belong to the shared SSL
types surface; ACME issuance workflows belong to their service integration. This
crate does not provide an ACME scheduler, a cross-host certificate distribution
service, or an HTTP server of its own.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
