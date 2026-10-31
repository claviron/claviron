# TLS layer — Architecture

The TLS layer is the **heart of every traffic-serving deployment**
this workspace produces. Modern HTTP — HTTP/2 over TCP, HTTP/3 over
QUIC — has no plaintext baseline; every connection begins with a
TLS handshake, every handshake produces an established identity for
the client's intended host, and every routing decision downstream of
that handshake is downstream of what TLS established. This crate is
where that establishment happens.

The crate's job is small in code and large in consequence. It owns
the **certificate resolver** — the single, hot-reloadable, shared
data structure that every TLS endpoint in the entire process
consults during the ClientHello phase to decide which certificate
to present. It owns the **server-config builder** that translates a
shared resolver plus an ALPN list into a `rustls::ServerConfig`
ready to hand to a listener. It owns the **mTLS verifier** machinery
that lets services demand client certificates with policy
enforcement on the subject DN. And it owns the **certificate
manager** — a borrow-lifetime helper used inside request handlers
(gRPC ACME callbacks, pubsub cert-rotation messages) to mutate the
resolver atomically when a new certificate arrives.

Everything written elsewhere in the workspace about routing,
virtual hosts, and request identity ultimately starts from the
identity TLS established. That is why this crate is shaped the
way it is.

---

## 1. The architectural claim — TLS is the source of truth

Three statements that this crate's design rests on. All three are
load-bearing for the rest of the workspace and are worth being
explicit about up front.

### 1.1 SNI is the routing source of truth, not the Host header

In the era of HTTPS-everywhere, the **first** signal the server
receives about which logical host the client wants is the SNI
extension in the TLS ClientHello — sent before any HTTP framing
exists, before the application can read a single byte of plaintext
from the wire. Routing on SNI is therefore not a convenience; it is
the only signal available at the moment the server has to decide
which certificate to serve and (after the handshake) which vhost's
configuration applies.

This crate's `CertResolver` reads SNI from the ClientHello and
resolves it to a `CertifiedKey`. That resolution **is** the routing
decision at the TLS layer. The handshake completes (or is rejected)
based on whether the resolver had a certificate for the requested
SNI. No SNI, no certificate, no handshake — handled at this layer,
no application code involved.

### 1.2 The Host header is verified against SNI, not trusted as identity

After the handshake, the request body arrives. HTTP/1.1 carries a
`Host:` header. HTTP/2 and HTTP/3 carry an `:authority`
pseudo-header. **None of these are the source of truth for vhost
identity in this workspace.** They are *verified* against the SNI
the TLS layer already established; if they disagree, the HTTP-level
machinery returns `421 Misdirected Request` (handled in the routing
crate, not here).

The reason for the asymmetry: SNI is fixed for the lifetime of the
TLS connection. The `Host:` / `:authority` value can vary per
request on a multiplexed connection (HTTP/2 / HTTP/3 connection
coalescing — a single TCP/QUIC connection serving multiple
hostnames that share a certificate). A request whose `:authority`
points at a hostname not covered by the connection's negotiated
certificate is potentially a misdirection (legitimate or
malicious); the only safe response is to refuse it.

The vhost crate documents the 421 enforcement in detail. What
matters here is the **direction of trust**: TLS establishes; HTTP
verifies. SNI is what we route on.

### 1.3 The CertResolver is the system's single source of TLS truth

Name-based TLS endpoints — HTTPS, HTTP/3 and gRPC — consume the same
`CertResolver`, directly or through a scoped projection. The resolver is an
SNI consumer, not the certificate source. Non-HTTP services resolve identities
and public trust through `CertificateStore` without constructing a host table
or default SNI certificate.

When a service has special needs — a per-service certificate
subset, an additional client-cert verifier, a custom ALPN list —
those needs are expressed as **derivations** from the shared
resolver, not as parallel resolvers. The auth module ships
`derive_resolver_for_hosts(&["api.example.com"], &shared)` as the
canonical pattern: pull the certs you need out of the shared
resolver, build a new `CertResolver` over that subset, hand it to
your `ServerConfig`. Both resolvers see the same underlying `Arc`
allocations at derivation time. The child is a point-in-time projection;
rebuild it explicitly to consume a later shared publication.

This single-source discipline is what makes operations like "an
ACME order just renewed the cert for `api.example.com`" propagate
to every TLS endpoint atomically: `CertificateManager::upsert_cn`
mutates the shared resolver's `ArcSwap`-backed map, and every
endpoint that reads through the same `CertResolver` (directly or
via a newly rebuilt projection) sees the new certificate on the next
handshake.

---

## 2. Position in the layering

```
     HTTPS listener     HTTP/3 listener     gRPC service     custom mTLS service
            │                  │                  │                   │
            │   each builds its own ServerConfig with this crate     │
            │                  │                  │                   │
            ▼                  ▼                  ▼                   ▼
   ┌─────────────────────────────────────────────────────────────────────────┐
   │  this crate                                                             │
   │                                                                         │
   │   CertResolver           Shared, hot-reloadable, ResolvesServerCert.   │
   │                          The system's TLS source of truth.              │
   │                                                                         │
   │   CertResolverProvider   Boots at priority::EARLY, builds initial      │
   │                          snapshot from files + DB, registers resolver. │
   │                                                                         │
   │   Snapshot               Build/reload scratch — merges file-backed     │
   │                          + DB-backed certs, dedupes by SNI.            │
   │                                                                         │
   │   create_tls_config      Resolver + ALPN → rustls::ServerConfig.       │
   │                                                                         │
   │   auth/                  mTLS — WebPkiClientVerifier wrapped in        │
   │                          O/OU PolicyVerifier; derive_resolver_for_hosts │
   │                          for per-service cert subsets.                  │
   │                                                                         │
   │   CertificateManager<'a> Borrow-lifetime helper for request handlers   │
   │                          to mutate the resolver from DB cert rows.     │
   │                                                                         │
   │   pem / proto            Helpers — PEM ⇄ CertifiedKey, ALPN bytes.     │
   └─────────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
                       rustls + aws-lc-rs + x509-parser
                       SSL primitives crate (Host, Sni, Aliases, KeyString)
                       optional: HostingStore + DbService (db feature)
```

The crate sits on top of the SSL primitives crate (the X.509 type
kernel), uses `rustls` + `aws-lc-rs` for the cryptographic
implementation, and depends on the foundation runtime for provider
registration. The optional `db` feature pulls in the database service
and stable hosting store so certificates can be loaded from a `certificates`
table (joined with `cert_aliases` for SAN extraction). The root facade's `hosting` capability selects DB-managed certificates without
selecting Edge. SNI consumers use the existing certificate listener; a source-only
application has no resolver listener to run.

The facade forwards a selected SQLite or PostgreSQL backend to TLS only when
TLS is enabled. TLS forwards that backend to its existing database and hosting
storage dependencies. A consumer selecting `tls + sqlite` therefore does not
need the facade's `hosting` feature, and SQLite alone does not enable TLS.
Validate this composition from an independent consumer: workspace test feature
unification can otherwise supply SQLite through TLS's dev-dependencies and hide
a missing normal dependency feature.

---

## 3. The CertResolver — anatomy of the source of truth

```rust
pub struct CertResolver {
    published: ArcSwap<Published>,
    writers: Mutex<()>,
}

impl ResolvesServerCert for CertResolver {
    fn resolve(&self, client_hello: ClientHello) -> Option<Arc<CertifiedKey>>;
}
```

One immutable publication contains the lowercased SNI map, default key and
managed hostname-to-canonical-CN ownership. The default remains independent
of SNI admission: missing or unknown SNI is rejected (§3.4).

### 3.1 Lock-free at the handshake hot path

The handshake is latency-critical. Every TLS connection pays its
cost; busy servers pay it tens of thousands of times per second.
The resolver's read path is therefore lock-free:

```rust
fn resolve(&self, client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
    let server_name = client_hello.server_name()?;
    let sni = server_name.to_ascii_lowercase();
    let snapshot = self.published.load();
    snapshot.certs.get(&sni).cloned()             // HashMap lookup + Arc clone
}
```

`ArcSwap::load()` is wait-free; the returned `Arc` keeps the
snapshot alive for the duration of the lookup. There is no mutex,
no `RwLock`, no contention with concurrent reloads. A reload that
calls `replace_all` mid-handshake just leaves the old snapshot in
memory until its last reader releases it — readers see no
inconsistency. Only writers take the cold publication mutex.

### 3.2 Hot-reload — the publish path

```rust
pub fn replace_all(&self, new_map: HashMap<String, Arc<CertifiedKey>>, new_default: Arc<CertifiedKey>);
pub fn upsert_host(&self, host: impl AsRef<str>, ck: Arc<CertifiedKey>);
pub fn remove_host(&self, host: &str);
pub fn set_default(&self, ck: Arc<CertifiedKey>);
```

Two granularities:

- **`replace_all`** — full snapshot swap. Used by the
  `Reloadable::reload` path when the operator triggers reload via
  signal or IPC: rebuild the snapshot from files + DB, swap.
  Single atomic store; every subsequent handshake sees the new
  map.
- **`upsert_host` / `remove_host`** — unowned, single-host mutations.
- **Managed identity replacement** — `CertificateManager` parses first, then
  replaces the CN and complete alias set in one publication. Previous bindings
  still owned by that CN are retired; bindings now owned by another CN survive.
  Boot and full reload retain DB ownership, stamping only successful first-wins
  insertions. File entries do not acquire DB ownership by sharing a key.

All mutations, default changes and authoritative full replacement share one
writer mutex. Incremental writes clone the latest publication while holding
that mutex, so disjoint successful writes are preserved. Full replacement is
ordered at publication and deliberately supersedes earlier writes. Snapshot
preparation, IO and parsing happen outside the mutex; no lock crosses an await.
Readers never acquire it. Updates remain O(n) copy-on-write on the cold path.

The choice matters: a `RwLock<HashMap>` would let upserts mutate
in place without the clone, but every read would take the read
lock and every write would block reads. With `ArcSwap`, reads are
free at all times and writes pay the clone cost on the (rare)
mutation path. For the workload — many reads, occasional writes —
`ArcSwap` is the obvious answer.

### 3.3 Read-side query helpers

Beyond the `ResolvesServerCert::resolve` impl, the resolver
exposes a small query surface for code that needs to inspect or
project the cert map:

- `get_cert(hostname)` — typed lookup, returns `Option<Arc<CertifiedKey>>`.
- `contains(hostname)` — boolean predicate.
- `hostnames()` — all loaded hostnames (diagnostic / admin use).
- `count()` — number of loaded certs.
- `get_any(hosts)` — for a list of hostnames, return the subset
  that exist in the resolver, as a `HashMap`. This is the
  primitive `derive_resolver_for_hosts` builds on (see §5.2).
- `get_default()` — the current default cert.

### 3.4 SNI miss policy — reject, not fall through

The current `ResolvesServerCert::resolve` impl returns `None` on
SNI miss, which causes `rustls` to abort the handshake with an
"unknown SNI" alert. The default cert is held but **not** served
on miss; it exists for future use (e.g. an OCSP-stapling test
endpoint, a metrics scrape over a fixed local cert) and as a
safety net.

The policy is intentional: a server that serves a default cert on
unknown SNI is helping a misconfigured client succeed where it
should fail. Rejecting the handshake surfaces the misconfiguration
immediately. If a deployment ever needs the fall-through behaviour,
flipping the resolver's `None` branch to `Some(self.default_cert.load_full())`
is a one-line change — the cert is already there waiting.

---

## 4. Sources and SNI consumption

`CertificateProvider` owns `tls.yaml` through Congif and boots after the optional
`HostingService`. It resolves the certstore declaration off the async executor,
validates the process TLS policy and publishes `CertificateStore` plus the
configured, public-only `CertAuthority`. It does not load a default identity,
create an SNI map, register virtual hosts or bind a listener.

`CertificateStore` exposes separate operations:

- `trust(selection)` / `authority()`: public CA chains only;
- `identity(selection, kind)`: a server/client certificate and its own key;
- `signer(authority)`: explicitly obtain a DB local-CA signing capability.

Bare names retain file-first lookup. Explicit CNs select hosting; explicit paths
select files. Partial, invalid or inaccessible file material fails rather than
falling through to a different source. A selected DB server identity must be
enabled, have the requested kind and belong to the current server assignment.
Client identities do not need an inbound server assignment. The identity API
rejects CA keys. Blocking file/key parsing runs through `spawn_blocking`; hosting
queries remain async and use the service's current DB profile.

The root facade registers the source for `tls`. It additionally registers
`CertResolverProvider` for `tls + edge` or `tls + upstream`, preserving the current
HTTP/gRPC and outbound named-identity compositions. A custom SNI consumer can
explicitly call `resolver::register`; this also registers the source idempotently.
No additional combination features are required.

### 4.1 Resolver build and reload

`CertResolverProvider` depends on `CertificateProvider`, so the optional hosting
identity and DB service boot first. It loads the existing default/SNI file
snapshot, resolves a named DB default if needed, merges enabled DB server
certificates and publishes the shared `CertResolver`. Existing file-first and
server-assignment rules remain unchanged.

The source owns configuration observation. On reload it selects the new source
declaration, refreshes revocation and, when an SNI consumer exists, rebuilds its
snapshot. Revocation preparation failure blocks new mTLS handshakes until a
successful refresh; it does not retain a potentially stale blocklist. A failed
SNI preparation keeps the serving certificate snapshot intact. Resolver replacement
retains its Arc identity; accepted connections keep their negotiated identity.
No source-only consumer needs a default certificate or SNI entry.

## 4b. CertAuthority — lending the CA we issue under

`CertAuthority` contains only public chains and optional issuer identity. A
named `certstore.authority` selects an enabled `local_ca` record through
`HostingStore::public_certificate_by_id`. That SQL projection never selects or
decodes `key_der`. File trust similarly opens only the public certificate file.
Hosting persists records and relationships; the consuming application's explicit
selection establishes trust. Reading a certificate does not grant signing.

`CertSigner::from_authority` explicitly acquires the selected CA key, checking
its issuer relation, enabled state and chain against the pinned public snapshot.
`from_issuer` supports an explicit named-issuer override. Issuance revalidation
rejects disabled or changed authorities; installing changed trust needs restart.
The source provider never registers a signer or obtains a CA key at boot.
Application admission and DB access permissions remain the authorization boundary;
these typed capabilities do not turn a broadly privileged DB account into a
restricted one.

Signing runs on a blocking thread and returns material for persistence, without
writing files or mutating an SNI resolver. Client certificates retain clientAuth;
server identities retain the existing serverAuth + clientAuth profile.
Intermediate CAs require explicit path length and remain bounded by their parent.
The gRPC mTLS manager acquires this signer only for an authorized issuance call;
its RPC contract and persisted certificate model are unchanged.

The [hosting model](../hosting/ARCHITECTURE.md#certificate-identity-and-issuers)
owns persistence; ACME issuance remains in its separate service integration.

## 5. mTLS — the auth module

The `auth` module provides the building blocks for services that
require client certificates at the TLS handshake (mTLS):

```rust
pub fn create_auth_config(
    ca_path:  impl AsRef<Path>,
    resolver: Arc<CertResolver>,
    alpns:    Vec<Vec<u8>>,
    cfg:      Option<&PolicyConfig>,
) -> Result<ServerConfig, Error>;

pub fn create_tcp_auth_config(
    ca_path:  impl AsRef<Path>,
    resolver: Arc<CertResolver>,
    alpns:    Vec<Vec<u8>>,
    cfg:      Option<&PolicyConfig>,
) -> Result<ServerConfig, Error>;

pub fn create_client_cert_verifier(
    ca_path: impl AsRef<Path>,
    cfg:     Option<&PolicyConfig>,
) -> Result<Arc<dyn ClientCertVerifier>, Error>;

pub fn derive_resolver_for_hosts(
    cert_names:      &[String],
    shared_resolver: &Arc<CertResolver>,
) -> Result<Arc<CertResolver>, Error>;
```

### 5.1 The verifier stack

`create_client_cert_verifier` builds a rustls client-cert verifier.
`create_auth_config` wraps that verifier into a full `rustls::ServerConfig`.
The verifier is either:

- A bare `WebPkiClientVerifier` — chain validation against the
  CA bundle, EKU enforcement, expiry check.
- A `PolicyVerifier` wrapping the WebPKI verifier — adds an O/OU
  match on the client cert's subject DN. The wrapper is
  configured with `PolicyConfig { organization, unit }`; the
  verify callback runs WebPKI first, then re-parses the leaf to
  check that the subject's `O` and `OU` attributes match the
  required values exactly.

The two-layer pattern is the right shape for mTLS-with-policy:
WebPKI handles the cryptographic validation (signature chain,
time, EKU); the policy wrapper handles the deployment-specific
identity check ("this client is from the *Operations* OU of
*ExampleCorp*"). The wrapper composes — it could itself be
wrapped by another verifier for additional checks.

`client_auth_mandatory()` returns true: a client connecting
without a certificate is rejected at the handshake, same way a
client with a wrong-CA certificate is rejected. The handshake is
the gate.

### Leaf revocation

`CertificateProvider` prepares one shared `revocation::Revocations` handle.
Edge TCP and QUIC mTLS use `create_auth_config_with_revocations`: normal
chain/time/subject verification is followed by an issuer/serial identity lookup.
A leaf absent from the revoked set remains eligible regardless of
where it was issued. Application `AccessPolicy` remains a separate admission
decision; no DB query or file read occurs in the handshake.

The certstore declaration can override the default CRL path:

```yaml
crl_path: etc/ssl/revocations.crl
```

Without an override, `CertStore::crl_path()` selects `dir/revocations.crl` through
the public `CRL_FILENAME` constant. Explicit relative paths remain cwd-relative.
The file accepts standard DER or PEM (`X509 CRL`, including a PEM bundle).
Missing files contribute no entries; empty files warn and contribute no entries.
Nonempty files require `certstore.authority`, whose public CA chain verifies CRL
signatures and signer key usage. No CA private key is loaded. Unsupported
versions, delta/indirect CRLs and critical extensions are rejected using rustls'
CRL parser. Only current full CRLs with `nextUpdate` are accepted.

The RAM snapshot retains only normalized `RevocationId` values and the earliest
CRL `nextUpdate`; DER, PEM, parsed CRLs and certificate chains are discarded.
Even an empty signed CRL retains its freshness boundary. New handshakes fail
closed after that boundary until a successful reload; no timer or polling task
is added. Invalid/unreadable sources fail preparation, and refresh failure
makes existing handles unavailable for new handshakes.

With DB support and a registered `HostingService`, unexpired certificates with
`kind=client` and `revoked_at IS NOT NULL` are merged into the client-auth set. The sources are
additive, deduplicated by issuer Name DER and normalized serial, without a
storage-selector enum. `enabled` and server assignments do not participate.
CRL entries contain neither certificate expiry nor usage kind. DB expiry/kind
filtering therefore does not apply to CRL entries; the verifier checks only the
presented client leaf against this issuer/serial index. PostgreSQL certificate notifications and SQLite
`HostingChanged::Certificates` refresh before SNI filtering. Refreshes serialize
off the handshake path and atomically publish complete snapshots. Explicit
resolver reload rereads files even if tls.yaml is unchanged. Source-only
consumers may load a snapshot through `CertificateStore::revocations()`.

The verifier parses issuer/serial from the peer's leaf after normal validation;
the rustls verifier's parsed certificate is not exposed for reuse. An empty
identity set skips this extra parse, while still enforcing CRL freshness.

Edge mTLS disables session resumption, tickets and early data so every new
connection checks the current snapshot. Public TLS resumption is unchanged.
Existing connections are not terminated. The older standalone TLS builders
retain their existing behavior; dedicated consumers must opt into the snapshot
builder. This local admission list is independent of public-CA ACME revocation.

### 5.2 Derived resolvers — single source, scoped projections

`derive_resolver_for_hosts(&["api.example.com", "ops.example.com"], &shared)`
returns a new `Arc<CertResolver>` containing only the certs for
those two hostnames, plus the same default cert as the shared
resolver. The extracted certs are **`Arc::clone`s of the same
underlying `CertifiedKey`s** in the shared resolver — no
reloading, no re-parsing, no duplication.

The pattern lets a service that wants its own narrow TLS config
(say, an mTLS-only admin endpoint that only accepts handshakes
for `admin.example.com`) build its `ServerConfig` over a derived
resolver:

```rust
let admin_resolver = derive_resolver_for_hosts(&["admin.example.com"], &shared)?;
let server_cfg     = create_auth_config(&ca_path, admin_resolver, alpns, Some(&policy))?;
```

**Important property:** the derived resolver's underlying cert
`Arc`s are shared with the parent. When the parent reloads, the
parent's `replace_all` swaps its snapshot — but the derived
resolver still holds the old `Arc`s in *its* snapshot. Derived
resolvers are point-in-time projections, not live views of the
parent.

For deployments where the derived resolver also needs to track
parent reloads, the right pattern is to re-derive on every reload
(register the derived resolver as `Reloadable` and re-call
`derive_resolver_for_hosts` from its `reload` impl). The bare
derivation is the cheap default; the live-tracking version costs
one additional `Reloadable` registration but is the correct
shape for production mTLS endpoints.

This is the architectural payoff of the single-source discipline
described in §1.3: **custom TLS-auth services do not maintain
their own cert store. They derive a scoped view from the shared
resolver and add whichever verifier policy they need on top.**
The code that says "this service speaks mTLS" is one
`derive_resolver_for_hosts` plus one `create_auth_config`; the
rest is a one-time configuration concern, not an ongoing
maintenance burden.

---

### 5.3 ClientIdentity — the outbound twin

`derive_resolver_for_hosts` narrows the resolver for a server; `ClientIdentity`
is the same idea pointing outward. A client that authenticates itself owns a
*name*, not certificate material: the resolver answers that name at each
handshake, so a renewal reaches the next connection without the consumer
rebuilding its TLS configuration, and an operator moving a file changes
nothing in the consumer. The server's acceptable-CA hints are ignored on
purpose — the operator named one identity for that leg, and offering another
would present a different principal.

`upstream` uses it for outbound mutual TLS to our own backends, and
`CertAuthority` for the trust that verifies them.

## 6. CertificateManager — the borrow-lifetime helper

```rust
pub struct CertificateManager<'a> {
    resolver: &'a CertResolver,
    store:    &'a HostingStore,
}

impl<'a> CertificateManager<'a> {
    pub fn new(store: &'a HostingStore, resolver: &'a CertResolver) -> Self;
    pub async fn upsert_cn(&self, cn: &str) -> Result<(Certificate, Vec<String>)>;
    pub fn upsert(&self, cert: HostedCertificate) -> Result<(Certificate, Vec<String>)>;
}
```

A small struct with a borrowed lifetime that exists for the
duration of one operation — typically a request handler. Two
fields, both borrowed, no internal state. This shape is
deliberate.

### 6.1 The pattern

The manager is constructed inside a handler that already has the
resolver and the hosting store on hand:

```rust
async fn on_acme_certificate_renewed(state: AppState, hosted: HostedCertificate) -> ... {
    let resolver = state.registry_ref().resolve::<CertResolver>().expect("resolver");
    let store    = HostingStore::new(resolve_pool(&state)?);
    let manager  = CertificateManager::new(&store, &resolver);
    let (cert, aliases) = manager.upsert(hosted)?;
    // … handler returns; manager drops; borrows released.
}
```

The handler holds the manager for exactly as long as it needs
to. There is no "mount the manager somewhere", no lifecycle, no
registration. The struct's only job is to bundle two borrows
together so the operation (`upsert_cn`, `upsert`)
can read from one and write to the other transactionally.

### 6.2 What the operations do

Both operations have the same shape:

1. Parse the DB cert row (`certs_der` chain + `key_der` PKCS#8)
   into a `rustls::sign::CertifiedKey` via `pem::db_cert_to_certified_key`.
2. Wrap in `Arc`.
3. Collect the complete current alias set from `HostedCertificate`.
4. Retire bindings still owned by this canonical CN and install its CN and
   aliases in one serialized, atomic publication.
5. Return the certificate model and alias list.

Disabled records remove the owned group without parsing a private key. A failed
parse publishes nothing. Ownership is independent of `CertifiedKey` allocation;
rotation changes key allocation without orphaning older aliases.

`upsert_cn(cn)` does an extra step at the start — looking up the
hosted certificate by CN. `upsert(hosted)` skips the lookup and uses
the plain hosting value directly. Both produce the same end-state.

### 6.3 Where it is called from

The pattern is used by handler-style code that processes a
single event and needs to reflect a cert change in the resolver:

- **The pubsub message handler** that receives a "certificate
  renewed" notification from another node in the cluster (one
  node ran the ACME order; the others need to update their own
  resolvers to match).
- **The gRPC ACME callback** that receives a freshly-issued
  certificate and installs it into the resolver as part of
  completing the order.

In both cases the handler runs briefly, holds the manager for the
duration of one `upsert`, and returns. No part of the system holds
a `CertificateManager` long-term. The lifetime annotation
(`<'a>`) makes that constraint explicit — a `CertificateManager`
that outlived its DB connection or its resolver reference would
not compile.

This is the **request-handler-lifetime helper** pattern: bundle
the borrows you need, do the operation, drop. It is a useful
shape whenever an operation needs to span two long-lived
resources without owning either one.

---

## 7. CertificateListener — live updates via NOTIFY/LISTEN

The `db` feature enables hosting-backed certificate updates on the selected
backend. PostgreSQL pool construction and LISTEN/NOTIFY handling additionally
require `postgres`; without it, the listener receives committed hosting changes
over fleet events and never references PostgreSQL APIs.

`CertificateManager` is the **explicit-call** mutation path —
something inside the process knows a cert changed (because *it*
ran the ACME order, or it received a pubsub message from another
node) and synchronously installs the result.

`CertificateListener` is the **out-of-band** mutation path: it
subscribes to a postgres `NOTIFY` channel and reacts to changes
made by **other** processes (a peer claviron node, a control-plane
operator running raw SQL, a sibling cluster member that just
finished an ACME order). It mirrors the
`claviron-edge::listener::VhostAliasListener` pattern but for
TLS state.

```rust
pub struct CertificateListener { /* signal-driven state only */ }

impl CertificateListener {
    pub async fn start(self: Arc<Self>, state: AppState)
        -> claviron_runtime::registry::Result<()>;
}
```

Backed by `Provider` + `Reloadable` + `Runnable` impls — same
trifecta used by every long-running registry-backed task in the
workspace.

### 7.1 The channel and the payload

A single channel — `hosting_certificate_changed` — carries events
from two source tables, fan-out by the payload's `table` field:

| Source table | What it represents | Listener action |
|---|---|---|
| `hosting.certificates` | A certificate row was inserted, updated (renewal — DER bytes change), or deleted. | INSERT/UPDATE → `CertificateManager::upsert_cn(cn)` (disabled server records remove their existing resolver entries). DELETE → owner-only certificate removal. |
| `hosting.cert_aliases` | A SAN was added, renamed, or removed. | INSERT/UPDATE → `upsert_cn(parent_cn)` re-stamps every alias from DB. DELETE → owner-conditional alias removal. |

Payload is intentionally minimal — `cn` for `certificates`,
`(hostname, cn)` for `cert_aliases`. DER bytes / metadata are
loaded by the listener via `CertificateManager::upsert_cn`. See
`migrations/2026050501_certificate_notify_triggers.sql` for the
full SQL contract.

The minimal payload is deliberate: PostgreSQL `NOTIFY` has an
8KB asynchronous queue limit, and embedding cert bytes (multi-KB
each) on the channel would risk overflow on busy clusters.

### 7.2 Why every process runs the listener

The vhost listener (in `edge`) is **workers only** —
the master process orchestrates but does not serve HTTP, so it
has no `HostAppResolver` to keep in sync. The cert listener has
no such carve-out: it runs in **every** claviron process —
master, workers, standalone — because `CertResolver` is the
system's single source of TLS truth (see §1.3) and is registered
unconditionally by `bootstrap::register`. Even SSL-less roles
(redirect-only, control-plane-only) still hold the resolver and
must keep it in lock-step with the cluster — for example, to
resolve sibling-cert state when emitting `Alt-Svc` headers
pointing at the HTTPS endpoint.

### 7.3 Server-id filtering

In a multi-server deployment, each `hosting.certificates` row is
linked to one or more servers via the `hosting.certificate_server`
pivot. A listener only acts on events whose cert is assigned to
**its** `server_id` (read once from `HostingService` at start).

The check is one indexed `EXISTS` query per event:

```sql
SELECT 1
  FROM hosting.certificate_server cs
  JOIN hosting.certificates c ON c.id = cs.certificate_id
 WHERE c.cn = $1 AND cs.server_id = $2
 LIMIT 1
```

Embedding `server_ids` directly in the trigger payload was
considered and rejected — on `certificates` INSERT the pivot rows
likely don't exist yet (FK depends on the new id), so the array
would be empty exactly when we'd need it populated. A per-event
DB lookup is sub-millisecond on the indexed pivot and avoids the
race entirely. See the migration's header comment for the full
rationale.

DELETE events skip the assignment check on purpose: the parent
row may already be gone (cascade), so the join would return false
unconditionally. Managed certificate and owner-conditional alias removal are idempotent —
a `0` removal is a no-op — so it's safe to broadcast DELETE to
every listener.

### 7.4 Operation-level semantics

| Op (table)             | What the listener does |
|---|---|
| `INSERT certificates`  | Filter by server_id; if assigned, `upsert_cn(cn)` (loads DER bytes + every alias). |
| `UPDATE certificates`  | Same as INSERT — typically a renewal where DER bytes change but `cn` is stable. |
| `DELETE certificates`  | Owner-only removal retires every binding still owned by this canonical CN, preserving reassigned names. |
| `INSERT cert_aliases`  | Filter by server_id; if assigned, `upsert_cn(parent_cn)` to re-stamp the cn + alias map. |
| `UPDATE cert_aliases`  | Same-parent rename publishes the complete group through `upsert_cn`; a cross-parent move conditionally retires the old owner's binding before refreshing the new group. |
| `DELETE cert_aliases`  | Remove only the named alias still owned by the supplied CN. Without a CN, leave cleanup to the parent certificate event. |

### 7.5 Failure modes

The listener is intentionally tolerant: every dependency it can't
resolve surfaces as `Error::Recoverable`, so the worker keeps
serving traffic and the listener simply doesn't carry live cert
events until the dependency comes back:

- `claviron_url()` missing → `db.yaml::postgres.default.connect`
  is unset; recoverable.
- `PgListener::connect` failing → log warning, sleep 2s, retry.
  The loop observes its `RunContext` everywhere, so targeted listener
  stop/restart and whole-process shutdown use the same reconnect teardown.
- `claviron_pool()` returns `None` → idle, sleep 2s, restart the
  outer connect loop.
- Notification recv error → reconnect after a 2s backoff.
- Payload parse error → log, continue (one bad event doesn't take
  the listener down).

### 7.6 Reload behaviour

`Reloadable::reload` bumps an internal generation counter on a
`tokio::sync::watch` channel. The recv loop watches it via
`select!`; on bump, the loop drops the active `PgListener` and
restarts the connect cycle. This handles env-driven URL changes
(`DATABASE_URL` rotation, schema reroute) without needing a
process restart.

### 7.7 Interaction with CertificateManager

The two paths converge: `CertificateListener` invokes
`CertificateManager::upsert_cn` for every successful filter hit.
The same `CertificateManager` is also used by gRPC ACME callbacks
and pubsub message handlers (§6.3). Both paths produce the
identical end-state in the resolver: an immutable key group and explicit
canonical-CN ownership published together. Owner-only removal does not need to
query alias rows after a cascade, and cannot erase another certificate's current
binding. Raw map APIs retain pointer-group removal for unowned entries only.

---

## 8. Configuration

`certstore.authority` and `default` use the same `CertificateSource`. A bare
name uses files first (CA: `ca/<name>/ca.crt`; default: `servers/<name>/fullchain.pem`
and `privkey.pem`), then hosting when required files are absent.
Default omission or YAML null (`default:` / `default: null`) selects the name
`default`; it does not disable the default certificate or bypass load errors.
`{cn: "Roma CA"}` explicitly
selects hosting only; `{path: ca.crt}` selects file trust only. For a default
certificate, `path` points to its full chain and the sibling `privkey.pem`.
Relative `path` resolves against `certstore.dir`; absolute paths stay absolute.
Existing explicit `cert_path`/`key_path` pairs remain cwd-relative.
Empty, mixed and traversal selectors fail; corrupt, partial or inaccessible
file material never falls through to DB. DB default lookup requires an enabled
server certificate assigned to this HostingService's server identity. Missing
DB support/service is an explicit boot error when file fallback needs DB.
Server certificate entries still use `Host` for
DNS/SNI identities. CA names such as `Roma CA` are not routing hostnames.

`certstore` (alias `cert_store`) accepts a YAML path or an inline CertStore.
Omission and explicit null both load `claviron_types::ssl::certstore::default_certstore_path()`:
the shared type crate selects an existing platform config file, then ~/.config/claviron,
then cwd-relative ./certstore.yaml. Failure to read/parse that file is a config
error, just like an invalid explicitly selected file. This does not construct
an empty/default in-memory store or hide an explicit-path failure. Named cert
entries resolve through the loaded store; inline store directories remain
cwd-relative, while a YAML store's relative directory resolves beside its file.
The SNI list accepts `sni_certs` and its aliases `certs`, `hosts`, and `servers`;
each accepts both store names and explicit PEM pairs.

```yaml
# tls.yaml
certstore: etc/ssl/certstore.yaml  # Omitted/null loads the shared default path.
default_cert:
  cert_path: /etc/<service>/certs/default.crt
  key_path:  /etc/<service>/certs/default.key

certs:                                        # aliases: sni_certs, hosts, servers
  - cert_path: /etc/<service>/certs/api.crt
    key_path:  /etc/<service>/certs/api.key
  - cert_path: /etc/<service>/certs/web.crt
    key_path:  /etc/<service>/certs/web.key
```

The configuration is intentionally minimal: a default cert pair
and a list of additional cert pairs. Each entry is a PEM file
pair on disk (cert + private key). The provider extracts SAN
entries from each loaded cert and registers one resolver entry
per SAN, so a single multi-SAN certificate covers all its
hostnames automatically — the configuration does not need to
list hostnames, only file pairs.

The optional `ca_path` field on each entry is reserved for future
use (per-cert CA bundles for verification chains; today the
verification CA is global, configured at the consumer's
`create_auth_config` call site).

### 8.1 Database-backed certificates

When the `db` feature is enabled and the database service is
booted, certificates are also loaded from the `certificates`
table (joined with `cert_aliases` for SAN entries) for the
running server's `server_id`. This is the path operationalised
deployments use:

- An ACME provider issues a certificate. The order completion
  writes the `(cn, certs_der, key_der)` triple into the
  `certificates` row plus one `cert_aliases` row per SAN.
- The local `CertificateManager` upserts the cert into the
  resolver synchronously (the order completion handler does
  this).
- Other nodes in the cluster receive a pubsub notification, run
  their own `CertificateManager::upsert_cn`, and converge.
- A fresh process boot (or a `CertResolverProvider::reload`)
  picks up the same certificate via `merge_db_certs` from the
  database table, so cluster state survives restarts without
  re-running ACME orders.

The first-wins deduplication in `Snapshot::insert_ck` means
file-backed certs and DB-backed certs can coexist on the same
SNI: whichever loads first wins. The default load order is
file-first, so a deployment in transition from file-managed to
DB-managed certs can keep file fallbacks for cases where the DB
hasn't been populated yet.

---

## 9. Building a `ServerConfig`

`create_tls_config(protos, resolver)` is the standard server-side
builder used by the HTTPS / HTTP/3 listener crates:

```rust
let cfg = create_tls_config(&[Proto::H2, Proto::H1], resolver.clone())?;
```

The function:

1. Builds a `rustls::ServerConfig::builder()` with no client
   auth, attached to the supplied resolver.
2. Selects the aws-lc-rs crypto provider and rustls' safe default protocol
   versions. The explicit selector remains the boundary where a future
   transport-specific provider constraint can be applied.
3. Sets ALPN protocols by mapping each `Proto` enum value through
   `Proto::alpn_versions()` (e.g. `H2 → [b"h2"]`, `H3 → [b"h3", b"h3-29"]`).
4. Replaces rustls' default server-session storage with a 2048-session
   in-memory cache when H1 or H2 is present. H3-only configs retain rustls'
   default stateful storage; Quinn uses the supplied rustls `ServerConfig` for
   QUIC session resumption as well.

### 9.1 Orbit session-storage incubator

With the `orbit` feature, `orbit::FleetServerSessions` exposes a fleet-shared
implementation of rustls' `StoresServerSessions`. It is not installed by
`create_tls_config` itself. `FleetServerSessionsProvider` is the mandatory
managed-SHM wrapper: after Orbit's `FleetProvider` it validates, opens, and
publishes the single shared table handle used by TLS transports.
`sessions.cleanup_on_exit` remains a lifecycle-only setting and defaults to
`true`, but cleanup now requires supervisor-owned activity evidence. With live
or adopted workers, boot reuses the table and finalization preserves it. Only
proven quiescence authorizes boot reset and final unlink. When the setting is
`false`, the configured policy is always `Reuse` / `Preserve`, so unexpired
sessions remain available across an externally managed restart. Exact process
presence is preferred; the supervisor falls back to one configured heartbeat
interval of lane motion. Reload updates the session TTL but deliberately leaves
the boot/finalize policy unchanged for the running process family. The provider
is not a `Runnable` because the table has no background work. HTTPS and HTTP/3
assign explicit domain views to their public `ServerConfig` values. Each
transport's `register()` idempotently registers this capability, which in turn
guarantees that the underlying fleet provider is present; direct crate
consumers do not depend on root-facade registration order.

The reusable rustls integration is consumed through `orbitive::rustls`. Its
underlying `orbit-rustls` package owns both the `StoresServerSessions`
adapter/domain boundary and the fixed-capacity Orbit SHM table beneath it.
This crate's `orbit.rs` contains only Claviron's config, provider ordering,
reload, and finalization adapter.

The table is authoritative current state, not an event ring. Every mutation is
serialized with a process-local mutex and Orbit's process-recoverable SHM
`flock`. `take()` copies and zeroes a session under that same lock, ensuring
that only one fleet process can consume a stateful TLS ticket. TCP, QUIC,
public TLS use explicit, separate `SessionDomain` values. Edge mTLS does not
resume sessions because admission checks the current revocation set on each
new connection. Runtime storage failures fail closed as cache misses. Temporary trace
instrumentation under `orbit_rustls::session_cache` logs operation, domain,
lengths, TTL, and hit/cached result, but never ticket-key or session-value
bytes.

`sessions.ttl` in `tls.yaml` controls retention, defaults to `24h`, and may be
shortened but not extended beyond rustls 0.23's 24-hour stateful lifetime. The
provider updates one shared atomic TTL on reload, so already-created TCP and
QUIC storage views observe the new value. The table's capacity and per-value
ceiling remain layout constants while the incubator ABI is being measured;
changing either requires a layout-version change shared by every fleet
process.

### 9.2 The Proto enum — single source for ALPN bytes

```rust
pub enum Proto { H3, H2, H1 }

impl Proto {
    pub fn alpn_versions(&self) -> &'static [&'static [u8]] {
        match self {
            H3 => &[b"h3", b"h3-29"],
            H2 => &[b"h2"],
            H1 => &[b"http/1.1"],
        }
    }
}
```

The reason this enum exists in this crate (rather than each
listener defining its own ALPN constants): there are exactly
three values that mean anything for HTTP-stack TLS, and getting
the bytes right matters (a typo on `b"h2"` aborts every HTTP/2
client's negotiation). One enum, one set of byte literals, every
listener picks from it.

The H3 entry includes both `h3` (RFC final) and `h3-29` (one
common draft) so handshakes from older clients still negotiate
successfully.

---

## 10. Components

| File | Approx LoC | Role |
|------|----------:|------|
| `lib.rs`         |  18 | Public surface. |
| `resolver.rs`    | 232 | `CertResolver` (the source of truth), `CertResolverProvider` (SNI consumption), `ResolvesServerCert` impl, `install_aws_crypto_provider`. |
| `server`    | 122 | `build_resolver`, `reload_resolver_certs`, `merge_db_certs` (db-feature), `create_tls_config`, `tls_config_path`. |
| `snapshot.rs`    | 218 | `Snapshot` — build/reload scratch; loads default + SNI entries from PEM, optionally merges DB; first-wins dedup. |
| `pem.rs`         |  93 | PEM/DER helpers; `certified_key_from_path_pair`, `db_cert_to_certified_key` (db-feature), `parse_cert_chain_from_bytes`. |
| `source.rs` | — | `CertificateProvider`, `CertificateStore`: source/trust/identity access without SNI. |
| `authority.rs` | — | Public-only `CertAuthority`. |
| `signer.rs` | — | Explicit `CertSigner` and issued material. |
| `manager.rs`     |  68 | `CertificateManager<'a>` — borrow-lifetime helper for handlers (db-feature). |
| `listener.rs`    | 305 | `CertificateListener` — postgres NOTIFY/LISTEN reactor, server_id-filtered, runs in every process (db-feature). |
| `auth/mod.rs`    |  61 | `create_auth_config` (mTLS server config builder), `derive_resolver_for_hosts`. |
| `auth/verifier.rs` | 152 | `PolicyConfig` + `PolicyVerifier` (O/OU subject DN policy wrapper around `WebPkiClientVerifier`). |
| `config.rs`      |  52 | `TlsConfig` + `CertPaths` — `tls.yaml` deserialisation. |
| `proto.rs`       |  30 | `Proto` enum (H1/H2/H3) + ALPN byte mapping. |
| `error.rs`       |  93 | Unified error type covering PEM I/O, X.509 parse, signing-key construction, DB cert errors. |

---

## 11. Status & remaining work

The crate is stable in production use. Smaller items in the
backlog:

- **OCSP stapling.** The `default_cert_paths` field was originally
  reserved for an OCSP stapling test path; the path has not
  been implemented. The cert is currently held but not actively
  used; the resolver rejects unknown SNI rather than falling
  through to it.
- **Cert chain ordering on DB load.** `parse_cert_chain_from_bytes`
  parses concatenated DER without reordering; deployments that
  store the chain in non-leaf-first order would need a sort step
  via SKI/AKI before building the `CertifiedKey`. Not currently
  observed in practice but worth a future hardening pass.
- **Per-cert CA bundles.** `TlsEntry::ca_path` is a config field
  that the loaders do not consume yet. The intent is that
  per-cert CA bundles could pin verification roots more
  narrowly than the global CA used at the auth layer. The wiring
  is straightforward; it lands when a deployment needs it.
- **Live-tracking derived resolvers.** Today
  `derive_resolver_for_hosts` produces a point-in-time projection;
  consumers that want their derived resolver to track parent
  reloads register their own `Reloadable` impl. A `derive_live_resolver`
  helper that registers the boilerplate would remove a small
  amount of repetition.
- **`upsert_host` mutation cost.** The current copy-on-write map
  clone is `O(n)` per upsert; this is fine at typical fleet size
  (tens of hosts, occasional renewals) but a deployment with
  thousands of hosts and frequent renewals would benefit from a
  per-host `ArcSwap<HashMap>` partitioning scheme. Not currently
  needed.

---

## 12. Out of scope

- **TLS handshake / connection management.** This crate provides
  the resolver and the server config; the actual `accept()` loop,
  TCP/TLS state machine, and connection-level lifecycle live in
  the per-transport listener crates.
- **Routing / vhost dispatch.** The resolver decides which
  certificate to serve at handshake time; the routing crate
  decides which vhost configuration applies after handshake. The
  421-Misdirected-Request enforcement (verify Host header against
  SNI) lives in the routing crate.
- **ACME protocol.** Order construction, challenge fulfillment,
  and renewal scheduling live in the ACME-domain crate. This
  crate consumes the resulting certificate (via the `db` feature
  + `CertificateManager`) but knows nothing about the ACME
  state machine.
- **Cluster propagation.** When a cert is renewed on one node and
  needs to reach others, the pubsub-domain crate carries the
  notification; this crate provides the `CertificateManager`
  that the pubsub handler invokes locally.
- **Certificate generation / minting.** The `rcgen`-backed
  generators (CA bootstrap, server CSR + signing, client cert
  for mTLS) live in the SSL primitives crate. `CertSigner` delegates
  to those primitives; persistence and issuance admission belong to its caller.

The TLS layer is the establishment point. Everything that
*flows from* what TLS established is somebody else's job; the job
of this crate is to make sure the establishment is correct,
fast, and consistent across every endpoint in the process.

## Explicit client subject identity

`CertSigner::issue_client_certificate` accepts an optional `ssl::SubjectUri`.
It signs that identity into a URI SAN while retaining the CN and clientAuth
profile. Plain issuance declares no application permissions and does not resolve
a user. `issue_client_certificate_with_permissions` requires an explicit
`PermissionCatalog`, rejects unknown names before signing, and signs the validated
selection as a versioned URI SAN. An explicit empty selection stays distinct from
an absent scope. Neither form grants access automatically. The caller authorizes
delegation and the accepting service evaluates effective authority. The ordinary
signer rejects attempts to pass permission URIs through aliases or the identity
field, which would bypass catalog validation. Low-level X.509 generators remain
cryptographic primitives, not enrollment policy. Public authority loading still
excludes CA private keys. Existing issuance without an application URI retains
its behavior.
