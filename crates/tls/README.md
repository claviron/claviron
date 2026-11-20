# claviron-tls

Shared certificate resolution, TLS policy, and mutual TLS configuration for
Claviron services.

This crate prepares the certificate and policy machinery used by TLS endpoints.
It does not open listeners or select HTTP routes. The
[Hyper](../edge/src/tcp/mod.rs) and [H3](../edge/src/quic/mod.rs) transports own their
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

A host whose launch document already holds this declaration supplies it with
`register_with_config(state, TlsConfigSource)` instead; no `tls.yaml` is then
read, watched or reloaded, and a change takes a restart. `declaration(state)`
returns the declaration the process has, from either source.

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
authentication. When client certificates are required, declare an
`auth::ClientAuth` and build its verifier with `auth::client_verifier`, or a
complete configuration with `auth::create_client_auth_config`.

`ClientAuth` is present or absent: present requires a client certificate and
names exactly one verifier, a certificate-store authority or a CA bundle,
with an optional organization/organizational-unit subject policy:

```yaml
client_auth:
  authority: {local: example}   # or: ca_bundle: path/to/ca.pem
  organization: Example         # optional, together with unit
  unit: Operations
```

The verifier validates the certificate chain against that CA and then the
subject policy, before the handshake completes.

A listener states its policy in its own declaration (`tls: on` for TLS
without one, `tls: {client_auth: ...}` with one), and an Edge host may state
it in its `tls` block; under one listener there is one policy. The states and
the rule are described in
[ARCHITECTURE §5.0b](ARCHITECTURE.md#50b-a-listeners-tls-and-the-one-policy-rule).
TLS certificate authentication is distinct from application login, session
cookies, and route authorization.

`CertSigner::issue_client_certificate` can sign an optional `ssl::SubjectUri`
into the client certificate. The CN remains a label. Applications interpret the
URI namespace and authorize the resulting subject; issuance assigns no rights.

## Reload boundaries

The shared certificate map can be replaced without replacing the resolver's
identity. Parse, material-loading, or snapshot failures prevent its replacement.
Accepted connections retain their established identity; a renewal affects new
handshakes rather than renegotiating already accepted application requests.

Negotiation policy is a separate, startup-bound process policy. The first
validated policy is retained; changing certificate configuration does not replace
it. Restart to change negotiated version/cipher/key-exchange policy. Unknown
suite/group names and an invalid version range are configuration errors.

`CertificateStore` resolves identities and public CA chains without an SNI map
or virtual hosts. `CertAuthority` contains public trust only; local issuance
explicitly obtains a separate `CertSigner`. Trust changes require their own
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

Resolver reads use one immutable snapshot without taking a writer lock.
Incremental updates and full reload publication share a cold writer lock, so
successful updates to different hostnames cannot overwrite one another. Full
reload is authoritative at publication; it replaces, rather than merges, the
previous snapshot. File and database IO complete before taking that lock.

A managed certificate rotation publishes its CN and complete current alias set
in one snapshot. Retired aliases disappear; aliases reassigned to another
certificate remain with their new owner. Disabling or deleting the certificate
removes only bindings it still owns. Invalid replacement material leaves the
previous publication intact. Ownership loaded from the database survives full
reload, while file-derived bindings retain their existing first-wins precedence.
These changes affect subsequent handshakes, not established TLS connections.

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
