# claviron-types

Shared host identities and optional certificate primitives for Claviron.

Routing, TLS configuration, certificate tools, and hosting integrations need to
agree on what a host name means. This crate supplies those common values without
owning listeners, application dispatch, or service lifecycle. Its default build
contains the identity types; certificate tooling is opt-in through `ssl`.

Applications normally consume these values through the root facade:
`claviron::edge::Host` for edge declarations, or `claviron::ssl` with the `ssl`
feature for host and certificate primitives. There is currently no general
`claviron::types` re-export.

## Host identity

`Host` distinguishes three forms rather than treating every identity as a DNS
name:

| Variant | Meaning |
| --- | --- |
| `Dns(Sni)` | An ASCII DNS-shaped name, optionally with a leading wildcard. |
| `Ip(IpAddr)` | A parsed IPv4 or IPv6 address. |
| `Single(Arc<str>)` | A validated single-label name. |

Use `FromStr` or Serde deserialization at input boundaries. Textual names are
normalized to lowercase without trailing dots. `Sni` validates label lengths,
ASCII characters, and hyphen placement; it requires a dotted name. This is not
DNS resolution, Unicode-to-IDNA conversion, or a wildcard matching policy.
Constructing a public tuple value directly is not a substitute for parsing.

`KeyString` provides the canonical key representation. `key_str()` can borrow
textual identities; formatting an IP address requires an owned string.
`key_string()` provides an owned key when a registry or storage API needs one.

```rust
use claviron::ssl::{Host, KeyString};

fn host_key(input: &str) -> Result<String, String> {
    let host: Host = input.parse()?;
    Ok(host.key_string())
}
```

This example uses the root facade's `ssl` feature. Host parsing itself does not
require the optional cryptographic implementation.

`Aliases` wraps a list of hosts, accepts a single host or a list in configuration,
and exposes explicit `push_unique` / `extend_unique` operations. Its `domains()`
iterator selects only DNS identities. Direct construction and deserialization
do not automatically deduplicate a supplied list.

`CertificateKind` identifies server, client, and CA roles independently of the
optional SSL implementation.

## Optional certificate toolkit

The `ssl` module provides shared building blocks used by runtime integrations
and certificate-management tools:

- `Algorithm`, `CommonName`, `Organization`, and `Issuer` describe certificate
  identities and signing choices. A CA common name is not a routing hostname.
- Parsers inspect PEM, DER, certificates, and certificate requests.
- Generators create keys, certificate requests, CAs, intermediate CAs, and
  server/client certificates.
- Formatters produce readable certificate information.
- `CertStore` supplies the shared on-disk layout and synchronous file helpers.

This feature adds cryptographic and filesystem capabilities; it is not merely
an additional set of serializable strings. The base identity surface does not
need those dependencies.

## Signing, import, and renewal boundaries

Signing checks include issuer CA role, key usage, matching issuer key, validity,
and path-length constraints where applicable. Invalid input is an error rather
than permission to silently widen the issuer's authority.

`ssl::transfer::plan_import` accepts certificate/key material and caller-supplied
issuer candidates. It identifies the certificate matching the private key,
checks chain relationships and signatures, and prepares ordered certificate
DER and PKCS#8 key material. Planning does not query a database or persist the
result. These checks are not a complete trust-store, revocation, or general
RFC 5280 validation service.

`ssl::renewal::renew_certificate` renews certificate material while preserving
its subject, issuer, public key, and extension encodings. It checks the signing
relationship and explicit validity bounds; it does not replace a deployed file,
publish runtime state, or schedule its own renewal task.

TLS handshakes, client admission, trust policy, ACME orchestration, and live
certificate publication belong to their owning runtime integrations, not to
these primitives.

## Related crates

- [TLS](../tls/README.md): handshake policy and runtime certificate handling.
- [Edge](../edge/README.md): host declarations and request admission.
- [Hosting](../hosting/README.md): persistent hosting identities and records.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
