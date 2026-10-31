# Hosting store architecture

`hosting` is the implementation crate behind the public
`claviron::hosting` facade. Its public contract is a hosting domain API, not
an ORM model collection.

## Role

The crate owns Claviron's operational `hosting.*` persistence boundary:

- server identity;
- vhost and alias assignment;
- certificate and certificate-alias assignment;
- transactional certificate issuance/renewal material writes;
- management listings and enable/disable/delete operations;
- config-to-database vhost import inputs.

The `hosting` facade feature means that vhost aliases and TLS certificates are
managed from the hosting database. It does not merely mean that a database
connection exists. The lower-level `db` feature remains the connection/pool
capability. HostingService does not require Edge or a VhostResolver. Host and
certificate DTOs are organized under `types/` and retain their root re-exports.
Their presence does not register a vhost runtime.

ACME accounts and audit logs, registrar/domain records, application users,
auth, and JWT data are app-layer concerns. They use their owning store APIs in
downstream workspaces and do not belong in this core API.

## Boundary

`HostingStore` is constructed from a `sqlx::PgPool` (`new`), an
`sqlx::SqlitePool` (`sqlite`, behind the `sqlite` feature), or from
`DbService` (`open`), which opens the connection `db.yaml`'s `hosting:` names.
`HostingService::store()` is the same, for callers that hold the service.
SQLx is private after construction. Public methods return plain structs and
`HostingError`; they do not expose SQLx query rows, transactions, executor
traits, or `sqlx::Error`. This is the compatibility seam for future SQLx
upgrades, and the one that lets a backend be swapped.

## Backends

Which one is configuration, not a build choice; the `sqlite` feature only
compiles SQLite in:

```yaml
# db.yaml
hosting: sqlite.hosting        # <backend>.<name>; postgres.default when unwritten
```

- **PostgreSQL** (default): roma's `hosting` schema. Changes arrive over
  `NOTIFY`, which the certificate listener (`tls`) and the alias listener
  (`edge`) receive on the same connection.
- **SQLite**: the same tables in one file, named `hosting_<table>` since
  SQLite has no schemas. The schema is roma's too (`roma/sqlite/`), applied by
  the developer as the PostgreSQL one is. There is no `NOTIFY`, and nothing
  outside this host's fleet writes the file, so the store says it: a store
  from `HostingService::store()` emits `HostingChanged` (`Vhosts` or
  `Certificates`, topic `claviron.hosting.changed.v1`) after each committed
  write, bridged over the fleet event ring to every process, the writer's
  included. Both listeners read the profile, subscribe, and bring their side
  up to date on each event, or on a lagged receiver, since a missed event may
  have been theirs: certificates by reloading the resolver, aliases by
  `VhostResolver::apply_stored_aliases`, alias by alias. Never by
  `replace_all`: replacing the vhost table fails every listener closed (every
  host, mTLS included) until a full reload rebuilds transport policy, which is
  what took all of portico's sites down on 2026-09-28 until it was reverted. A store from `HostingStore::open` directly announces nothing:
  writers go through the service.

Every statement is written once, for PostgreSQL, and run on SQLite through a
mechanical rewrite (`hosting.` → `hosting_`, `NOW()` → UTC text, parameter
casts dropped). The three whose shape differs — `UPDATE … FROM … RETURNING`,
`DELETE … USING`, `= ANY($n)` — are written twice. Timestamps are bound as
UTC whole seconds on both backends, because SQLite compares them as text and
text compares right only at one width.

`DbService` remains the lower-level transport owner. It provides pools,
connect options, dedicated connections, and listeners. It does not know the
hosting schema and never provides an ORM connection.

## Identity

`HostingService` owns the process's resolved `hosting.servers.id` in one
`OnceLock`. Its boot looks the server row up by the instance name, ordered
after `DbService` has built the pool; it never creates the row. The hosting
database is a configuration source here — the operator's declarations kept in
a store rather than a file — so boot reads it and nothing more. Registering a
host is a setup step: `claviron-hosting --server-name <name> register-server`
(claviron-tools), the one place `find_or_create_server` is still used; a name
hosting does not know fails boot with that instruction. The id is not duplicated in `AppState`, a
global static, or `DbService`. Hosting-aware providers resolve
`HostingService` explicitly.

After the server lookup and identity initialization succeed, master and standalone
processes log the selected hosting profile, server id and name at INFO. Workers
resolve their own identity through the same boot path and log it only at DEBUG.
The profile name is logged, never the connection URL or credentials.

## Alias persistence for configured hosts

Canonical host definitions belong to edge configuration and `VhostResolver`.
The hosting vhost row supplies persistence identity for aliases and server
relationships; its absence does not make a configured host unavailable.
`add_alias_for_server` and `replace_alias_for_server` accept a canonical name
already validated by the caller. Each transaction finds or inserts the vhost,
ensures the current server link, and writes the alias before announcing the
committed change. Add is idempotent for the same target and rejects another
owner; replace requires a persisted alias. Failure rolls back the new identity
and link as well as the alias write. These operations never define a runtime host.

## Invariants

- SQL is PostgreSQL's; SQLite reads it through the rewrite above, and a
  statement the rewrite cannot carry is written for both backends side by
  side, never approximated.
- Consumers perform domain operations through `HostingStore`; raw SQL belongs
  here unless it is protocol-specific LISTEN/NOTIFY handling.
- DTOs describe domain records, not table update builders. Do not reintroduce
  generic CRUD or ActiveModel-style public APIs.
- Runtime side effects such as resolver swaps and listener reconnects stay in
  TLS/vhost crates. This crate only loads and mutates persisted records.
- `HostingError` stays storage-neutral. Add domain error variants when callers
  need to distinguish behavior; do not leak SQLx error variants.

## Certificate identity and issuers

`certificates.revoked_at` is a nullable revocation timestamp, independent of the
reversible `enabled` flag. `revoke_certificate` records the first timestamp and
sets `enabled=false` in one update, announcing the committed certificate change.
Repeating the operation on a re-enabled row disables it again while preserving
the original timestamp; enabling never clears revocation.
`revoked_certificate_chains(as_of, kind)` selects only public chains of the
requested usage kind whose revocation is recorded and whose `expires_at` is
later than snapshot creation. TLS client authentication selects `Client`. It filters
neither enabled state nor server assignments, reads no private keys and stores
no additional fingerprint or ID. TLS derives issuer/serial IDs from these chains.
Certificate and public-certificate DTOs expose the nullable revocation date,
including inventory queries. The revocation index query and mutation remain
separate operations; consumers do not derive revocation from enabled state. Local DB revocation
requires a local certificate row; external identities can also arrive via CRL.
Material replacement and renewal refuse revoked rows. Unexpired revoked rows
cannot be deleted through HostingStore, so their issuer/serial identity cannot
disappear from later snapshots. Deletion is permitted after certificate expiry.

`CertificateKind` is the usage role: `server`, `client`, or `ca`.
`certificates.issuer_id` points to `certificate_issuers`; only a self-signed CA
has no parent issuer. The issuer owns the issuance method (`acme` or `local_ca`)
and exactly one reference: `acme_account_id` or `ca_certificate_id`. The
schema's CHECK and foreign keys enforce the issuer target; certificate rows
carry no ACME-specific field. Roma owns migration `2026100301_certificate_kinds`.
Existing rows become server certificates linked through one issuer per ACME
account; material, certificate IDs, aliases and server assignments survive.

`ensure_acme_issuer` also handles newly registered accounts. `acme_certificates`
filters by the issuer relation for renewal; local CA certificates never enter
ACME renewal. `insert_unassigned_certificate` persists client/CA identities
without an inbound server assignment. Saving a CA creates its `local_ca` issuer
in the same transaction, named by its CN, so configuration can select it.
`insert_certificate` and subsequent server-link operations accept server kinds
only. TLS loading filters by server kind even if the DB was edited externally.
The CN is stored on the certificate and installed independently by the TLS
resolver. Certificate persistence skips a supplied SAN equal to the CN;
`cert_aliases` holds only additional names, for both imports and ACME issuance.
The certificate's encoded SAN set is unchanged. ACME renewal includes the CN
independently of stored aliases.

The SQL/Rust schema change is a coordinated upgrade: apply the Roma migration
before starting consumers of the new model. Old consumers of `account_id` need
to be stopped for the switch. Issuer trust changes are loaded by TLS on restart.

`local_issuer_certificates` exposes only issuer IDs and public certificate chains
for enabled local CAs. Import planning uses this query to verify relationships
without loading issuer private keys. Certificate parsing and signature checking
remain SSL primitives; import/issuance orchestration remains in the caller.

`PublicCertificate` and the `public_certificate_by_id`/`public_certificate_by_cn`
queries expose metadata and chains without selecting `key_der`. They neither
select trusted roots nor grant signing authority. Private identity lookup can
filter the requested usage kind in SQL before loading material; the TLS source
keeps CA keys on its separate explicit signing path. Existing full-material
queries remain available to authorized management callers. Database roles and
application admission, not DTO shape, enforce access permissions.

`public_certificates_by_issuer` is the same public projection for an issuer's
complete certificate set. It does not filter by usage, enabled state or server
assignment: these are independent consumer policies, not implicit trust grants.

Local renewal uses `renew_certificate_material`: optimistic comparison of the
original certificate and key guards against concurrent renewal or disable.
Only the DER certificate and issued/expiry metadata change; the key, record ID,
issuer, aliases and server assignments remain intact. A conflict is not retried
or overwritten. The normal certificate-change notification follows persistence.
