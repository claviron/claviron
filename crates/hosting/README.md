# claviron-hosting

Hosting persistence for server identities, vhost aliases, and certificates.

`HostingStore` exposes domain operations over PostgreSQL or SQLite, returning
plain hosting values and storage-neutral errors. The optional `HostingService`
connects that store to Claviron's runtime identity and committed-change events.
Neither is a web server, certificate issuer, ORM, or public management API.

Applications normally use `claviron::hosting` through the root crate's
`hosting` feature. This selects database-managed aliases and TLS certificate
integration, not merely a database connection. Use [db](../db/README.md) for
the lower-level pool capability.

## Domain operations, not table builders

The store owns persistence for:

- Named servers and their hosting identity.
- Vhosts, aliases, and server assignments.
- Certificate material, aliases, issuers, and server assignments.
- Certificate enable/disable, material replacement, and guarded renewal.
- Importing configured vhost names and aliases into persistence.

Public operations return `Server`, `Vhost`, `HostedVhost`, `Certificate`,
`HostedCertificate`, and related values, or `HostingError`. SQLx appears at
pool construction, not as query rows or transaction types in the domain API.
Errors distinguish invalid input, missing records, conflicts, unavailable
initialization, and storage failure without exposing SQLx error variants.

For example, application code can consume the store without selecting SQL:

```rust
use claviron::hosting::{HostingStore, Result, Vhost};

async fn assigned_vhosts(
    store: &HostingStore,
    server_id: i32,
) -> Result<Vec<Vhost>> {
    store.vhosts_for_server(server_id).await
}
```

This boundary does not grant management authority. The caller must authorize
changes and protect returned data. Certificate records include private key
material; do not expose their generic serialization or debug representation
through an untrusted response or diagnostic log.

`public_certificates_by_issuer` returns an issuer's public certificate records
without reading private keys or requiring a server assignment. Usage and enabled
state remain visible; consumers choose their own trust and admission policy.

## Backends and runtime identity

The implementation has no default features. `postgres` and `sqlite` compile
their respective backends; `server` adds runtime integration. On the facade,
combine `hosting` with the required backend feature.

`HostingStore::new` accepts a PostgreSQL pool; `HostingStore::sqlite` accepts
an SQLite pool. With runtime integration, `HostingStore::open` uses the profile
selected by `db.yaml`'s `hosting` field. For an already declared SQLite profile,
that selection is:

```yaml
hosting: sqlite.hosting
```

PostgreSQL stores use the `hosting` schema; SQLite uses corresponding
`hosting_<table>` names. Provision the matching schema before using the store.
This crate does not apply migrations automatically or turn arbitrary SQL into
a portable application query language.

`HostingService` boots after `DbService`, resolves the server row by Claviron's
instance name, and owns its ID in one process-local cell. Boot reads an existing
declaration; it does not register a new server row implicitly. Server setup is
a separate management step. A missing declaration fails boot.

`HostingService::store()` obtains the current DB profile when called, so new
store handles follow a pool reload. A previously retained store still owns its
original pool handle.

## Persistence and live publication are separate

Canonical runtime hosts belong to Edge configuration and its vhost resolver.
A persisted vhost supplies alias/assignment identity; its absence does not
invalidate an otherwise configured host. Persisting a record does not itself
open a listener or define a runtime route.

Alias operations such as `add_alias_for_server` and `replace_alias_for_server`
accept a canonical host already validated by the caller. They atomically
establish persistence identity, server linkage, and alias changes. Adding an
alias is idempotent for the same target and rejects another owner; replacement
requires an existing alias. Failed operations roll back their related writes.

PostgreSQL change observation uses database notifications in the consuming
TLS/Edge components. SQLite has no NOTIFY: stores obtained through
`HostingService::store()` announce committed `HostingChanged` facts through
the process/fleet event bridge. A directly constructed store has no such
runtime announcement. Use the service-owned path for live SQLite writes.

Listeners own resolver updates and resynchronization after missed events.
This crate does not replace the active vhost table, reconnect a listener, or
rotate TLS state from inside a storage transaction.

## Certificate records and renewal

`CertificateKind` describes usage (`server`, `client`, or `ca`);
`IssuerKind` describes issuance (`acme` or `local_ca`). These are separate
relationships. Issuer records reference the appropriate account or CA;
certificate rows do not embed an ACME-specific account field.

Server assignment operations accept server certificates. Client/CA identities
can be persisted without an inbound server assignment. Stored aliases exclude
a duplicate of the certificate's common name, without rewriting the encoded
certificate's SAN set.

`renew_certificate_material` compares the original identity/material before
updating certificate bytes and issuance/expiry metadata. Concurrent replacement
or disable is a conflict, not an implicit retry or overwrite. Same-key renewal
preserves the key, issuer, aliases, assignments, and record identity.

Certificate parsing and signature verification remain TLS/SSL primitives;
issuance/import orchestration and ACME accounts remain with their owners.
The store supplies persistence, not an encryption-at-rest or key-custody service.

## Related crates

- [DB](../db/README.md): connection profiles and SQLx pool lifecycle.
- [TLS](../tls/README.md): active certificate resolution and trust.
- [Edge](../edge/README.md): canonical hosts and live aliases.
- [Runtime](../runtime/README.md): ordered boot and typed events.
- [Orbit](../orbit/README.md): same-host change propagation.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
