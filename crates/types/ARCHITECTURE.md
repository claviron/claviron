# Shared types and optional SSL primitives — Architecture

A pure-Rust, foundation-level crate that defines shared identity value
types. Its default build contains only `Host`, `Sni`, `KeyString`, `Aliases`, and the pure-data
`CertificateKind`; it has no certificate, TLS runtime, async, or application
dependency. The optional `ssl` feature adds X.509 value types, the
on-disk certificate-store layout, parsers, generators, and formatters.

The crate is deliberately reusable on **both server and CLI sides**.
A long-running listener uses these primitives to resolve SNI to a
host record at handshake time. A command-line tool uses the same
primitives to generate a CA, sign a server certificate, mint a
client certificate, parse a deployed certificate, or print a
human-readable summary. The shapes are the same; only the calling
context differs.

> **Naming clarification — SSL vs TLS.** `ssl` is a feature and module,
> not the identity of this crate, and it is **not** a TLS server. A TLS
> server is the live entity that performs handshakes, terminates
> connections, multiplexes streams, and serves traffic. That work
> lives in the per-transport listener crates. This crate sits one
> layer below: it produces and consumes the *content* (certificates,
> keys, hostnames, identities) that a TLS server then uses to do its
> job. Nothing in this crate opens a socket, performs a handshake,
> or speaks a wire protocol.

---

## 1. Position in the layering

```
                              CLI tooling                 TLS / HTTPS / HTTP/3
                          (cert mint, cert dump,           listener crates
                           CA bootstrap, alias mgmt)        (handshake, ALPN,
                              │                              cert resolution)
                              │                                  │
                              ▼                                  ▼
   ┌────────────────────────────────────────────────────────────────────┐
   │  claviron-types                                                    │
   │                                                                    │
   │   domain types        Host, Sni, KeyString, Aliases                │
   │   ssl feature         Issuer, Organization, Algorithm, CertStore    │
   │                       parsers, generators, formatters, error        │
   └────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
            rustls-pki-types + rcgen + x509-parser + time + serde
            (no openssl, no async runtime, no application state)
```

The crate sits at the bottom of the certificate / identity
dependency graph. **Both** the runtime listener crates and the
command-line tooling consume it. There is no separate "CLI primitives"
crate and no separate "server primitives" crate — the types and
primitives are the same on both sides, which means an operator can
generate a CA on the CLI and the runtime listener will load it
through the same `Host` / `Aliases` / `CertStore` shapes.

### 1.1 What this crate is not

- **Not a TLS server.** No socket, no handshake, no `Acceptor`, no
  ALPN negotiation. Listener crates own those.
- **Not a certificate resolver in the rustls sense.** The crate
  produces the inputs a resolver needs (parsed X.509 → host
  identity → canonical key) but the resolver wiring (`ResolvesServerCert`
  trait implementations, SNI dispatch tables) lives in the
  TLS-listener crates.
- **Not an ACME client.** ACME order workflows, HTTP-01 / DNS-01
  challenge handling, and renewal scheduling live in a separate
  ACME-domain crate that consumes these primitives but adds the
  protocol around them.
- **Not a certificate database.** `Algorithm` has stable string parsing,
  display, and serde forms so a storage crate can persist it without this
  primitive crate depending on a query library.

---

## 2. The domain type kernel

Modern HTTP-stack architecture (HTTP/2 / HTTP/3, TLS 1.3 baseline,
SNI mandatory) puts the **host identity at the centre of every
routing decision**. SNI is read at the TLS handshake before any
HTTP framing is parsed; the handshake's hostname is the *first*
piece of information available to the server about which logical
host the client wants. Every routing, certificate-resolution, and
configuration-lookup step downstream depends on agreeing on what
"host identity" means — across yaml configuration, in-memory
hash tables, certificate SAN extensions, log fields, and database
rows.

The crate establishes that agreement at the **type** level. Once
the type is shared, the rest of the codebase composes safely.

### 2.1 `Host` — the canonical host identity

```rust
pub enum Host {
    Dns(Sni),       // strict DNS-shaped name, with dots
    Ip(IpAddr),     // v4 or v6
    Single(String)  // bareword (e.g. "localhost", "bison")
}
```

Three flavours, because real deployments produce all three:

- `Dns(Sni)` — a fully qualified domain name (FQDN). The form
  every public-facing host takes; carries an `Sni` inside (see §2.2)
  so the validation work runs once at construction.
- `Ip(IpAddr)` — an IPv4 or IPv6 literal. Useful for testing,
  internal services without a DNS name, and for X.509 SAN
  extensions that legitimately encode IP addresses (the
  `iPAddress` general name).
- `Single(String)` — a bareword (no dots, lowercase letters /
  digits / hyphens, ≤ 63 chars). Lab / development hosts, mDNS-style
  names, supervisord identifiers.

`FromStr` and `TryFrom<&[u8]>` cover both string-shaped and
raw-byte-shaped inputs. The byte-slice path has a fast path for
X.509 SAN-encoded IPs: a 4-byte slice becomes an IPv4, a 16-byte
slice becomes an IPv6, anything else falls back to UTF-8 textual
parse. This is the path taken when extracting SAN entries from a
DER-encoded certificate.

Serde implementation round-trips through the canonical string form,
so a vhost configuration that lists hosts as bare strings parses
into typed `Host` values directly without an intermediate
deserialization stage.

### 2.2 `Sni` — strict, validated FQDN

```rust
pub struct Sni(pub Arc<str>);
```

A newtype wrapping `Arc<str>` so cloning is a pointer bump and
hashing / comparison work on the inner bytes directly. The
constructor (`FromStr`, `Deserialize`) runs `Sni::validate()`,
which enforces:

- Optional leading wildcard (`*.example.com`) is allowed.
- Must contain at least one dot (it is FQDN-like by definition).
- Total length ≤ 253 bytes (per the DNS spec).
- Must contain at least one ASCII letter (rejects purely numeric
  FQDNs that would alias IP literals).
- Each label is 1–63 bytes, lowercase letters / digits / hyphens
  only, no leading or trailing hyphen.

The `norm()` helper canonicalises an input string before validation:
ASCII-lowercases and trims a trailing dot. This means the same
FQDN entered as `Example.COM` or `example.com.` produces an equal
`Sni` value, so HashMap lookups and equality comparisons across
configuration sources never miss because of cosmetic differences.

`AsRef<str>` and `Borrow<str>` are implemented so an `Sni` can be
used as a HashMap key and looked up with a plain `&str` query
without constructing a new instance.

### 2.3 `KeyString` — the canonical-string trait

```rust
pub trait KeyString {
    fn key_str(&self) -> Cow<'_, str>;
    fn key_string(&self) -> String { self.key_str().into_owned() }
}
```

The single trait that gives every host-like type a uniform
"borrow-or-allocate" string view. The point is **uniformity across
the codebase**:

- A vhost configuration file uses strings to identify hosts.
- The routing layer keys a HashMap on `Host` directly.
- The TLS layer reads SNI as a string from the handshake.
- A log line wants to print the host identity.
- A database row stores the host as a column.

Without `KeyString`, the codebase needs five `host_to_string`
conversion functions and they all eventually drift. With
`KeyString`, every type that means "a host" answers `key_str()` the
same way and downstream code only learns one method.

`Sni` returns a borrowed string (zero allocation; the inner
`Arc<str>` lives there already). `Host::Dns` and `Host::Single`
also return borrowed; `Host::Ip` allocates via `IpAddr::to_string`
because the textual form is not stored. The `Cow` return type lets
callers stay zero-allocation in the common case while still working
when the underlying value has to be materialised.

### 2.4 `Aliases` — a deduplicating list of hosts

```rust
pub struct Aliases(pub Vec<Host>);
```

A newtype around `Vec<Host>` that adds:

- `push_unique(host)` — only inserts if the host is not already in
  the list. Returns `bool` so the caller can detect the dedup.
- `extend_unique(iter)` — bulk equivalent.
- `domains()` — borrowing iterator over only the `Host::Dns`
  entries. Useful when the caller cares about FQDN-shaped aliases
  specifically (e.g. for ACME order construction).

Serde deserializes from either a single host or a list of hosts
(the `OneOrMany` pattern), so a configuration entry can write
`alias: example.com` or `alias: [a.example.com, b.example.com]`
interchangeably.

`Aliases` is also the type produced when extracting SAN entries
from an X.509 certificate (see §4.1); the same value fed into the
generator path becomes the SAN list of a freshly-signed
certificate. The round-trip is total: parse a cert into `Aliases`,
mint a new cert from those aliases, the SAN list survives.

### 2.5 Why these types matter at the routing layer

The routing layer in a downstream crate keys its main lookup table
directly on `Host`. The shape (paraphrased):

```rust
struct VhostTable {
    inner: Arc<HashMap<Host, Arc<VhostRecord>>>,
}

struct VhostRecord {
    canonical: Host,
    config:    Arc<HostConfig>,
}
```

The TLS handshake produces an SNI string; the listener wraps it as
`Host::Dns(Sni::from_str(sni)?)`; the table looks up the record;
the canonical host (which may differ from the alias the client sent
— a vhost can have multiple aliases all pointing at the same
canonical) drives every downstream decision (route stack, certificate
choice, log labelling).

The whole chain works because **the type for "host identity" is
the same at every step**. SNI parsing produces `Host`; configuration
deserialization produces `Host`; X.509 SAN extraction produces
`Host`; database alias rows resolve through `Host::from_str`. The
crate's job is to define that type rigorously enough that every
producer agrees with every consumer.

This is the practical payoff of putting type discipline at the
foundation: no string-keyed table needs case normalisation at the
call site, no SNI lookup needs a separate "but did you remember to
strip the trailing dot" branch, and no SAN extraction needs a
`format!("{}.example.com", base)` kludge to match what the
configuration file said.

---

## 3. Identity-bearing types

Three small data types support the certificate generation and
parsing pipelines. They are deliberately plain — fields are
public, no async, no interior mutability.

### 3.1 `Algorithm`

```rust
pub enum Algorithm { RS256, ES256, ES384, EdDSA }
```

The set of signature algorithms the crate's generators and
downstream ACME pipeline both understand. Parses from string
case-insensitively (`"RS256"`, `"rs256"`, `"EdDSA"`, `"eddsa"`),
serializes as the canonical uppercase form.

`Algorithm` is always a plain `Copy` enum. Storage adapters persist its
canonical `Display` value and restore it through `FromStr`; no ORM derive
is part of the public type.

### 3.2 `Organization`

```rust
pub struct Organization {
    pub name:    String,
    pub email:   String,
    pub country: String,
    pub state:   String,
    pub city:    String,
    pub unit:    String,
}
```

The subject DN fields used when generating a certificate. All
fields are optional (empty strings are skipped in serialization
and skipped in DN composition), so the same struct works for a
minimal "just the organisation name" CA and for a fully-attested
end-entity certificate.

The `from_name(name)` constructor is the common ergonomic shortcut
for the minimal case.

### 3.3 `Issuer`

```rust
pub struct Issuer {
    pub cn:           String,
    pub organization: String,
    pub country:      String,
    pub unit:         String,
}
```

The reverse direction: the issuer-shaped fields extracted from a
parsed X.509 certificate's issuer DN. Used by the parsers module
(`TryFrom<&X509Certificate>`) so a deployed certificate's
provenance can be inspected without re-parsing the raw DN bytes.

The structural difference from `Organization`: an issuer has a CN
(the CA's common name); an organisation does not (CN is the
end-entity's host).

---

## 4. On-disk layout — `CertStore`

```rust
pub struct CertStore {
    dir:          PathBuf,
    authority:    Option<AuthorityEntry>,
    organization: Organization,
    crl_path:     Option<PathBuf>,
}
```

A small helper that captures the on-disk certificate store layout
the runtime and the CLI both agree on. Loaded from a YAML
configuration file via `from_path()`; the loader normalises the
store's root path to the file's parent so a configuration that
lives at `…/store/config.toml` produces a store rooted at `…/store/`.

The store layout is conventional:

```
<store>/ca/<ca-name>/ca.crt
<store>/ca/<ca-name>/ca.key
<store>/servers/<server-name>/...
<store>/clients/<client-name>/...
```

Client generation and import share `CLIENT_CERT_FILENAME` (`client.crt`) and
`CLIENT_KEY_FILENAME` (`client.key`) under `CLIENT_CERTS_DIRNAME` (`clients`).

`crl_path` optionally overrides the CRL file. `crl_path()` defaults to
`dir/CRL_FILENAME` (`revocations.crl`); explicit relative paths are cwd-relative.
This type performs no revocation I/O or admission checks. `RevocationId` owns
the issuer X.509 Name DER and normalized unsigned serial bytes. It can be read
from the first certificate of a DER chain without a database or private key.
CRL entries and certificate records use exactly the same identity; BLAKE3 is
not part of the storage or wire contract. Certificate issuance remains unchanged.
`ssl::crl::revoke` creates a full signed CRL update from a local certificate,
issuer, key and optional previous DER/PEM bundle. It verifies the target's issuer
and key, preserves prior serials and revocation times, increments the CRL number
and retains other issuers' lists. Scoped, delta and unsupported extended lists
are refused rather than silently rewritten. `nextUpdate` is the issuer's expiry;
revocation has no undo. This primitive owns no I/O, locking, database or runtime
refresh. Local tools serialize and atomically publish file updates. CA-chain
revocation and temporary certificate holds are outside this leaf operation.
The optional full chain uses the shared `FULLCHAIN_CERT_FILENAME`.
`client_cert_path()` selects that full chain for TLS consumers; import can
use the leaf and reconstruct its chain from registered issuer certificates.

Path accessors:

- `ca_cert_path()` — the active CA's certificate.
- `ca_key_path()` — the active CA's private key.
- `client_dir(cname)` — the client subtree for one named client.
- `server_dir(cname)` — the server subtree for one named server.
- `issuer_dir(name)` — the directory for any named issuer (root
  CA, intermediate, or other).

The store's `organization()` accessor returns the default
`Organization` to use when generating new certificates inside the
store, so a CLI command "mint a new server cert" inherits the
store's institutional identity by default.

The deliberate choice to put the store layout into a typed helper
(rather than ad-hoc string concatenation at each call site) is the
same discipline as `KeyString` for hosts: one place defines the
convention, every consumer agrees, no per-call-site drift.

---

## 5. Parsers — X.509 / CSR / PEM → domain types

The parsers module contains a small toolkit for turning raw
certificate bytes into the domain types defined above. The bridge
crate is `x509-parser`; the conversion shapes are `TryFrom`
implementations:

- `TryFrom<&X509Certificate> for Organization` — extracts O / CN /
  email / C / L / OU from the subject DN.
- `TryFrom<&X509Certificate> for Issuer` — extracts CN / O / C /
  OU from the issuer DN.
- `TryFrom<&X509Certificate> for Host` — extracts the subject CN
  and parses it into a `Host`.
- `TryFrom<&X509Certificate> for Aliases` — extracts every
  `subjectAltName` (DNS or IP) into an `Aliases` list.
- `TryFrom<&X509CertificationRequest> for Aliases` — same shape
  for a CSR, including the requested-extensions block plus the
  CSR's subject CN.

Convenience helpers chain the parse-and-convert steps so callers
do not have to handle the `parse_x509_pem` / `parse_x509_certificate`
sequencing themselves:

- `Aliases::from_cert_bytes(content)` — accepts both PEM and DER;
  PEM is parsed if present, falls back to DER.
- `Aliases::from_csr_der(data)` — DER-encoded CSR.
- `Aliases::from_csr_content(content)` — accepts both PEM and DER
  for a CSR.

`sanitize_pem_content_to_der(content)` is the small helper that
strips PEM armour (the BEGIN/END headers and the base64 wrap) and
returns raw DER, falling back to the input bytes unchanged if PEM
parsing fails.

`take_first_cert_from_der(der)` extracts the first certificate
from a possibly-multi-cert DER blob — useful when the input is a
chain file but the caller only needs the leaf.

`concat_chain_bytes_from_pem(pem)` walks every certificate in a
PEM stream and concatenates the DER bodies. Used when the
downstream consumer (typically a TLS resolver) wants the chain in
DER form ready for handshake.

---

## 6. Generators — minting CAs and end-entity certs

Built on top of `rcgen` (pure-Rust X.509 generator). The crate
defaults to ECDSA P-256 / SHA-256 for both CA and end-entity keys —
the modern profile, fast handshake, small certificate size.

### 6.1 Key generation

```rust
pub fn generate_ca_key()      -> Result<KeyPair, Error>
pub fn generate_private_key() -> Result<KeyPair, Error>
pub fn read_private_key_from_pem(content: &[u8]) -> Result<KeyPair, Error>
pub fn read_private_key_from_der(content: &[u8]) -> Result<KeyPair, Error>
```

Both `generate_ca_key` and `generate_private_key` produce a fresh
ECDSA P-256 keypair. The split exists so that a future profile
change (RSA-4096 for CAs, separate from ES256 for end-entities,
say) can be made in one place without touching call sites.
The two `read_private_key_*` helpers restore persisted material for flows such
as certificate renewal, where retaining the existing identity is required.

### 6.2 Server certificate flow

```rust
pub fn generate_server_csr(
    private_key: &KeyPair,
    cname:       &str,
    aliases:     impl Into<Vec<String>>,
    org:         &Organization,
) -> Result<CertificateSigningRequest, Error>

pub fn generate_server_cert(
    csr:             &CertificateSigningRequest,
    subject_key_pem: &[u8],
    ca_cert_pem:     &[u8],
    ca_key_pem:      &[u8],
    validity_days:   u32,
) -> Result<Vec<u8>, Error>
```

Two-stage. The CSR generation produces a certificate-signing
request with the requested CN, the SAN list (rcgen auto-detects
DNS / IP / URI / email by content), and the organisation DN
fields. The signing step takes the CSR plus the CA's PEM bundle
and emits a PEM-encoded server certificate.

The end-entity profile sets:
- Extended Key Usage: `clientAuth` + `serverAuth`. Both are set
  intentionally so the same cert can be used for mTLS in either
  direction (server-side TLS + client-side mTLS to an upstream).
- Key Usage: `digitalSignature`.
- Validity: now to now + `validity_days`.

### 6.3 Client certificate flow

```rust
pub fn generate_client_cert(
    client_name:    &str,
    client_key_pem: &[u8],
    ca_cert_pem:    &[u8],
    ca_key_pem:     &[u8],
    validity_days:  u32,
    extra_sans:     &[impl Into<String> + Clone],
) -> Result<Vec<u8>, Error>
```

The mTLS workhorse. Mints a client certificate signed by the
provided CA, with:
- Extended Key Usage: `clientAuth` only.
- Key Usage: `digitalSignature`.
- Subject CN: the client name.
- SAN: the CN plus any extra SANs the caller supplies (DNS, IP,
  URI, email — rcgen auto-detects). Duplicates are deduplicated
  while preserving insertion order.
- `is_ca = NoCa` (leaf cert, cannot sign anything).

The returned PEM is what a client (an HTTP client library, a curl
command, a database client, anything that speaks TLS) presents to
a server during the handshake. See §8 for the architectural
implications.

### 6.4 CA bootstrap flow

```rust
pub fn generate_ca_cert(
    ca_name:       &str,
    ca_key_pem:    &[u8],
    validity_days: u32,
    org_name:      Option<&str>,
    country:       Option<&str>,
) -> Result<Vec<u8>, Error>

pub fn generate_intermediate_ca_cert(
    int_key_pem:        &[u8],
    issuer_ca_cert_pem: &[u8],
    issuer_ca_key_pem:  &[u8],
    common_name:        &str,
    validity_days:      u32,
    org:                &Organization,
    path_len:           Option<u8>,
) -> Result<Vec<u8>, Error>
```

A self-signed root CA and an intermediate CA generator. The root
sets `IsCa = Ca(Unconstrained)` (no path-length limit on what it
can issue); the intermediate either inherits the same lack of
constraint or sets a `Constrained(n)` path limit so a deployment
that wants three-tier signing (root → intermediate → leaf, no
further) can enforce that structurally.

CA profile sets `KeyCertSign` + `CrlSign` key usages and **no**
extended key usage (CA certs do not need EKU). SAN is empty (CA
certs do not carry SANs).

### 6.5 The CLI dual-use

The split between `generate_*_csr` and `generate_*_cert` exists
because real workflows separate them:

- A CLI tool generates the CSR and ships it to a remote signer.
  The CA never sees the private key.
- A CI pipeline generates the end-entity key on the target host,
  produces a CSR, sends the CSR to the CA service, receives the
  signed certificate back, installs it.
- A self-contained "mint everything locally" tool calls both in
  sequence.

All three flows use the same primitives. The crate makes no
opinion about which flow is right; it provides the building
blocks and lets the operator's tool compose them.

---

## 7. Formatters — human-readable certificate text

`parse_text_from_pem(cert_pem)` produces an `openssl x509 -text -noout`–style
text dump of a certificate: version, serial number, signature
algorithm, issuer DN, validity window, subject DN, public key info
(algorithm + key size), and extensions (subjectAltName with each
general name, KeyUsage, ExtendedKeyUsage). The signature itself is
truncated to the first 16 bytes for readability.

`certificate_chain_to_pem(chain)` converts the hosting store's concatenated
DER representation back into a public PEM fullchain. This is the safe
distribution shape for external certificate consumers; private keys are not
part of the formatter contract.

The intent is operational visibility. An admin running a CLI
command to inspect a deployed certificate gets the same shape they
would get from `openssl` without having to install openssl, and
without losing information that matters for verifying a
deployment.

---

## 8. mTLS as architecture

This is the position the crate is shaped for. It is worth stating
plainly because the **set of primitives only makes sense if the
deployment story is mTLS**.

### 8.1 The problem mTLS solves

Modern HTTP stacks (HTTP/2, HTTP/3, QUIC) require TLS — there is
no plaintext HTTP/2 over the open internet, there is no plaintext
QUIC. Every connection goes through a TLS handshake. The handshake
ordinarily authenticates **the server to the client** (the client
verifies the server's certificate against its trust store).

For services that are intended for the open internet (a public
website, a content-delivery endpoint), one-way TLS plus
application-level authentication (sessions, JWTs, API keys) is the
standard model. This works because the server is supposed to
accept anyone.

For services that are intended **only for a small, known set of
clients** — internal admin panels, machine-to-machine APIs,
backend control planes, partner-only integrations — the
conventional patterns each have problems:

- **VPN.** Adds a separate authentication layer, requires client
  software, fails badly under partial network outages, hides
  traffic from inspection tools, requires a VPN appliance that
  becomes its own attack surface.
- **IP allowlist.** Brittle when clients live on dynamic IPs
  (mobile networks, residential connections, cloud auto-scaling);
  no per-client identity (every request from an allowed IP is
  trusted equally).
- **Application-layer auth alone.** The service must be reachable
  to be authenticated against. Anonymous traffic still hits the
  application, consumes resources, and probes for auth bypass
  bugs. The service is exposed even when the auth is sound.

mTLS removes those trade-offs. The handshake itself **also
authenticates the client to the server**: the server only completes
the TLS negotiation if the client presents a certificate signed by
a CA the server trusts. No certificate ⇒ no handshake ⇒ no
connection ⇒ no requests, no probes, no log noise. The
authentication happens before HTTP framing is parsed, before any
application code runs, before any session is created.

### 8.2 What changes once mTLS is the gate

A private service can be **safely exposed on the open internet**.
The IP is reachable to anyone, but only clients holding a
CA-signed cert can complete a handshake. Concretely:

- No VPN required. The client runs anywhere, on any network. The
  certificate is the credential.
- No IP allowlist required. Dynamic-IP clients work without
  changes.
- No "is this request authenticated" branch in application code
  for the gated routes. The TLS layer is the gate.
- Standard tools work end-to-end. `curl --cert ... --key ...`,
  `openssl s_client`, every HTTP library — they all speak TLS
  natively.
- Log volume drops. There is nothing to log: anonymous traffic
  cannot reach the application.

### 8.3 What this crate provides for the mTLS deployment

The full toolkit:

- **CA bootstrap** — `generate_ca_cert` produces the root CA the
  service trusts. The CA private key never has to leave the
  operator's controlled environment.
- **Server certificate minting** — `generate_server_csr` +
  `generate_server_cert` produce the server-side cert. The EKU
  set includes both `serverAuth` and `clientAuth` so the same
  identity can be used for inbound mTLS *and* outbound mTLS to
  another service.
- **Client certificate minting** — `generate_client_cert`
  produces a per-client certificate, with arbitrary extra SANs
  for service-mesh-style routing decisions or per-environment
  isolation. The client name becomes the CN and the first SAN.
- **On-disk store layout** — `CertStore` keeps the CA, server,
  and client subtrees in a discoverable, predictable layout.
- **Inspection** — `parse_text_from_pem` and the `TryFrom<X509Certificate>`
  conversions let an operator inspect any deployed certificate
  for provenance, validity, SAN list, and identity fields.

All certificate and mTLS-supporting primitives in this crate are gated by
the `ssl` feature. Deployment policy such as "mTLS required" belongs to the
consumer and is not a second feature of this type crate.

The architectural recommendation: any service that is **not**
designed for the public internet should be exposed only over
mTLS. The cost is one CA bootstrap per service identity domain
plus one client cert per consumer, both fully automatable. The
benefit is the elimination of an entire category of unauthenticated-
reachability bugs.

---

## 9. Crypto stack choice

Three deliberate choices in the dependency set:

- **`rustls`** for any TLS-protocol type the crate touches (the
  PEM iterator from `rustls-pki-types`). No `openssl`. No native
  TLS bindings. The whole certificate path is pure Rust and
  builds reproducibly without a system OpenSSL.
- **`rcgen`** for certificate generation. Pure Rust, builds on
  top of `ring` and `rustls`'s primitives, supports the modern
  ECDSA / Ed25519 algorithms the crate defaults to.
- **`x509-parser`** for parsing. Pure Rust, walks DER directly
  without round-tripping through openssl-style ASN.1 macros.

The cumulative effect is that the crate (and every consumer that
depends on it) builds without a system OpenSSL, without a
compile-time C linker dependency, and without the recurring
auditability problems that come from FFI into a large C
codebase. The decision is operationally significant for build
pipelines and supply-chain review; it also opens up cross-compile
paths (e.g. to musl-static targets) that openssl-bound crates
struggle with.

The choice is paired throughout: the same cryptographic library
is used for parsing, generating, and verifying. There is no
opportunity for the parser and the generator to disagree on
encoding edge cases.

---

## 10. Feature flags

| feature | what it pulls in |
|---------|------------------|
| default | `Host`, `Sni`, `Aliases`, and `KeyString`; only serde is required. |
| `ssl`   | `claviron_types::ssl`: certificate types, YAML store, X.509/PEM parsers, certificate generators, and formatters. |

The shared identity kernel is deliberately unconditional. Certificate
dependencies are deliberately not: HTTP routing, host configuration, and
file serving must not compile rcgen, rustls, or x509-parser merely to name a
host.

---

## 11. Components

| File | Approx LoC | Role |
|------|----------:|------|
| `lib.rs`         | small | Core identity module declarations and public re-exports; exposes `ssl` only when the feature is enabled. |
| `host.rs`        | 198 | `Host` enum (`Dns` / `Ip` / `Single`), `FromStr`, `TryFrom<&[u8]>`, `Display`, serde, `KeyString`, conversions from `Sni`. |
| `sni.rs`         | 126 | `Sni` newtype, validation, normalisation, `KeyString`, `Borrow<str>` / `AsRef<str>` for HashMap interop, `one_or_many_sni` deserialiser. |
| `key.rs`         |  12 | The `KeyString` trait — the canonical-string contract. |
| `aliases.rs`     | 148 | `Aliases` (newtype `Vec<Host>`), unique-push semantics, one-or-many serde, `domains()` borrowing iterator, conversions from `Sni` lists. |
| `ssl.rs`         | small | Feature-gated SSL namespace and public re-exports. |
| `ssl/algorithm.rs`   | 42 | Storage-neutral `Algorithm` enum, `FromStr`, `Display`, and serde. |
| `ssl/organization.rs`| 49 | `Organization` certificate subject fields. |
| `ssl/issuer.rs`      | 18 | `Issuer` certificate issuer fields. |
| `ssl/certstore.rs`   | 200 | `CertStore` on-disk layout helper and YAML loader. |
| `ssl/parsers.rs`     | 192 | X.509 / CSR / PEM parsing and conversions into shared identity types. |
| `ssl/generators.rs`  | 301 | Key, CSR, CA, server, client, and intermediate certificate generators. |
| `ssl/formatters.rs`  | 131 | `parse_text_from_pem` human-readable certificate dump. |
| `ssl/error.rs`       | 58 | Errors for certificate I/O, parsing, generation, and encoding. |

---

## 12. Status & remaining work

The crate is stable in production use. Touch-ups in the works
rather than design-level changes:

- **`fastrand` for the alphanumeric helper.** `random_alphanum`
  uses `fastrand` for non-cryptographic randomness (file naming,
  scratch IDs). Cryptographic key material goes through `rcgen`'s
  `ring`-backed primitives; the two random sources never cross.
  Worth flagging in case a future contributor confuses them.
- **Algorithm coverage.** Today the supported set is
  RS256 / ES256 / ES384 / EdDSA. `EdDSA` is in the enum but the
  generator path defaults all keys to ECDSA P-256; an EdDSA-default
  alternative generator is straightforward but not yet wired.
- **Cert chain handling.** `concat_chain_bytes_from_pem` returns
  concatenated DER. A consumer that wants a typed
  `Vec<CertificateDer<'static>>` pulls the chain with
  `CertificateDer::pem_slice_iter` directly. A small typed wrapper
  in this crate would centralise that pattern.

None of these block consumer work; they are sharpening passes.

---

## 13. Out of scope

The crate intentionally does not cover:

- **TLS server / handshake.** The actual TLS termination,
  handshake negotiation, ALPN selection, and session resumption
  live in the per-transport listener crates.
- **Certificate resolution at runtime.** A `ResolvesServerCert`
  implementation that maps an SNI name to a (cert, key) pair lives
  in the TLS-listener crate; it consumes the types defined here
  but adds the resolver wiring on top.
- **ACME.** Order construction, HTTP-01 / DNS-01 challenge
  fulfillment, renewal scheduling — all in a separate ACME-domain
  crate that consumes these primitives.
- **OCSP / CRL.** Revocation checking is not implemented here.
- **Database schema.** Storage crates map the canonical `Algorithm`
  string at their own boundary; this crate ships no migrations or models.
- **Async I/O.** The crate is fully synchronous; certificate
  parsing and generation are CPU work and do not benefit from
  async.

The crate is the **type and primitive kernel**, used identically
on both server and CLI sides. Anything that adds protocol,
runtime, or storage policy belongs in a separate crate that
depends on this one.

## Certificate roles and signing validation

`CertificateKind` (`server`, `client`, `ca`) is available without SSL or database
dependencies. Hosting and TLS share this role type; issuance method belongs to
hosting's issuer relationship. Leaf and intermediate signing validate the CA
flag, signing key usage, matching key, and requested validity before signing.
Validity must be positive and representable; it is never silently clamped to
the CA's expiry. Intermediate path length must fit the parent constraint.
These primitives return certificate bytes and never persist files or DB rows.

## Certificate import planning

`ssl::transfer::plan_import` consumes PEM certificate/key bytes and enabled local
issuer public chains supplied by the caller. It selects exactly one certificate
by matching the private key's SPKI, derives CN/SAN, validity and server/client/CA
role, and verifies issuer signatures with x509-parser's aws-lc verification
backend. It rejects ambiguous issuer matches, unrelated supplied certificates,
cycles and CA path-length violations. It reorders and deduplicates the chain,
returning target-first concatenated DER plus normalized PKCS#8 key bytes.
An explicitly registered intermediate can terminate the available chain.
This is import relationship validation, not a general RFC 5280 trust verifier:
revocation and application trust policy remain outside this primitive.

CA common names are authority identities: import preserves their subject CN,
including spaces and case, and emits no routing aliases. Only leaf identities
pass through the existing Host/SAN normalization. A CA's CN must be non-empty;
it is never interpreted as a DNS name or installed as an SNI alias.
`CertStore.authority` and TLS `default` share `CertificateSource`, using
validated `CommonName`, not Host. A bare single-component name tries the
file store first, then hosting only when the files are absent. `{cn: "Roma CA"}`
selects DB only; `{path: ca/roma/ca.crt}` selects a PEM file only. Paths resolve
against the store directory; explicit `cert_path`/`key_path` pairs retain
cwd-relative semantics. Mixed, empty, unknown and traversal selectors fail.
The default identity is `default`; it is not a routing hostname. SNI entries
still use Host. A damaged or inaccessible file is never hidden by DB fallback.

`ssl::renewal::renew_certificate` renews local X.509 material without rotating
the key. It preserves the original DER subject, issuer, SPKI and all extension
encodings (including unknown critical extensions). Only serial, validity and
signature change. The original issuer relationship and key match are checked;
unsupported signature algorithms fail rather than lossy reconstruction.
Validity is explicitly caller-declared and cannot outlive the issuer.

The planner performs no database lookup or mutation. Self-signed CA imports have
no parent issuer; other automatic imports must match one supplied local issuer.
The primitive supports root bootstrap; callers enforce management admission
and explicitly activate any new authority in configuration. ACME account binding
and import are outside this primitive. `private_key_to_pem` provides separate
padded PKCS#8 PEM export.

## Peer identity facts

PeerIdentity is a pure shared value with exact public leaf DER, subject/issuer
DNs, structured CNs and URI SAN candidates. SSL peer extraction parses these
facts without selecting an identity namespace or performing authentication.
Transports retain their TLS verification flag and attach the parsed value.
The host owns trust-domain selection and user binding; issuer DN is not a
trust-anchor identifier. Names and URI contents grant no service permissions.

## Certificate subject URIs

`ssl::SubjectUri` carries an application-owned absolute ASCII URI in the X.509
Subject Alternative Name extension. Leaf generation encodes URI values as URI
GeneralNames, separately from DNS/IP aliases and the subject CN. The primitive
validates URI text; the application owns namespace semantics and authorization.
`ssl::peer::peer_identity` exposes these signed identities through
`PeerIdentity.subject_uris`. A CN label does not establish a stable application
subject by itself.

## Role vocabulary

`role` contains the database-independent `Role` value and `RoleDefinition`
trait. Role names follow the same case-sensitive ASCII grammar as permission
names. `Role::from(value)` and `value.into()` validate a definition's static
name at compile time; `new` and `FromStr` validate stored or external names at
runtime. Equality, ordering and hashing use the exact name, so independently
defined marker types and stored values match without a central enum.
Auth re-exports the module as `auth::role`. Role assignment and permission
resolution remain application policy.

## Permission vocabulary and signed declarations

`permission` contains pure, database-independent permission value types.
`Permission` owns a validated `Arc<str>` and compares exact case-sensitive names.
`PermissionDefinition` gives a subsystem's marker type its stable string name.
Any definition value converts through `Permission::from(value)` or `value.into()`;
the static name is validated at compile time. Dynamic names use `new` or
`FromStr` and report validation errors at runtime. Both paths produce the same
value: equality, ordering and hashing depend only on the exact name. Separate
modules can define the same permission without sharing a concrete marker type
or depending on a central enum. Consumers may accept `Into<Permission>` when
they need to work with either definitions or existing permission values.
`PermissionCatalog` is explicitly constructed, validates typed or dynamic
registration, and resolves complete issuance selections without global state.
`Permissions` is a deduplicated sorted selection; membership does not establish
an authenticated grant. Auth re-exports this module rather than defining a second
identity type or making SSL depend on application/auth services.

The optional SSL module `ssl::permission` encodes one versioned application URI
SAN: `urn:claviron:permissions:v1:` followed by comma-separated permission names.
The name grammar excludes delimiters and URI escapes. A present empty payload
means an explicit empty scope; a missing URI means no scope declaration.
Parsing rejects malformed names, multiple declarations and unknown versions.
This is an application wire format, not a standard X.509 permissions extension.
Consumers check transport trust before using it and retain host-owned policy;
certificate renewal preserves the signed declaration with other extensions.
