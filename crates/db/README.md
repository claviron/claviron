# claviron-db

Typed database configuration and SQLx pool ownership for Claviron.

This crate translates named connection declarations into backend-specific
SQLx options and pools. Its runtime provider publishes process-local pools and
replaces their catalog on explicit configuration reload. It is not an ORM,
query builder, repository layer, or migration runner.

Applications normally use `claviron::db` through the root crate's `db` feature,
selecting `postgres`, `mysql`, or `sqlite` for the required backends. The
facade's `server` composition enables the runtime provider when DB is selected;
backend configuration and pool builders can also be used without that provider.

## Named connections

`DbFileConfig` groups named entries by backend. A profile such as
`postgres.default`, `mysql.analytics`, or `sqlite.local` identifies both the
backend and the connection name. `DbService::profile` returns the matching
`Pool` variant or explains which backend or entry is unavailable.

Backend-specific accessors such as `postgres_named`, `mysql_named`, and
`sqlite_named` return ordinary SQLx pools. Callers keep SQLx's transaction,
binding, and query APIs; this crate does not reinterpret application SQL.

`hosting` optionally selects the profile used by Claviron's hosting subsystem.
Application repositories remain responsible for choosing their own profiles
and owning their schemas.

## Configuration inputs

File-backed registration owns `db.yaml` under Claviron's configuration
directory. [Congif](https://crates.io/crates/congif) prepares the document and
expands `${ENV_KEY}` values from the runtime's captured configuration
environment before boot and explicit reload. `db.yaml` is required on this
registration path; it is not automatically reloaded on every file save.

PostgreSQL and MySQL entries select one of:

- `url`: a typed connection URL parsed by SQLx's own backend options.
- `connect`: structured `host`, `port`, `database`, `username`, `password`,
  and optional Unix `socket` fields.

Use one transport form per entry. Structured fields go directly to the
backend's builder rather than being converted into a synthetic URL. Unix
sockets retain backend-specific semantics.

Every entry declares its pool policy: `pool_size`, `idle_size`,
`acquire_timeout`, `idle_timeout`, and `max_lifetime`. These are explicit
deployment choices, not hidden pool defaults selected by a request handler.
Optional `statement_timeout` is a PostgreSQL connection-level setting; absence
preserves the database server's setting. SQLite rejects it rather than silently
ignoring an unsupported bound.

An enclosing runner can supply the same schema through
`register_with_config(state, config)` or `DbService::from_config(config)`.
Pools are still constructed during normal asynchronous boot. Supplied options
do not read/watch `db.yaml` or expose file reload; changing them requires
restart. The normal file registration preserves an explicitly supplied service.

## Pool lifetime and reload

`DbService` boots early so dependent providers can use its pools. Pools belong
to the current process; SQLx pool handles are not shared across forked workers
through SHM.

File-backed reload constructs a complete `DbState` and atomically publishes the
replacement. Subsequent accessor calls see the new pools. A previously cloned
SQLx handle still refers to the old pool; replacing the catalog does not rewrite
handles retained by application services.

Connect-option and dedicated-connection accessors support consumers that need
a connection outside ordinary query checkout. Such consumers own that
connection's lifecycle and must account for pool/configuration replacement.

## SQLite files and memory databases

SQLite is enabled by default in this implementation crate. File entries use
`mode: file` (the default), a `path`, a required `busy_timeout`, and a pool
policy. `create` defaults to false. Relative file paths are process-working-
directory relative, not YAML-directory relative.

File databases use WAL so readers can continue while a writer holds the write
lock. Writers still serialize; `busy_timeout` is the declared wait for that
lock, not a way to obtain concurrent writers. Optional `mmap_size` sets a
per-connection read-mapping ceiling, not a RAM reservation or an in-memory DB.
It may be capped by SQLite or unavailable on a platform; it is not a guaranteed
performance improvement. Never replace or truncate a live mapped database file.

`mode: memory` creates a database private to one pool in one process. Its query
connections share that database, while separate pools do not. A private anchor
connection keeps it alive while query connections recycle. File-only `path`,
`create`, and `mmap_size` options are rejected. Reload creates a new empty memory
database; this mode provides neither persistence nor fleet-shared state.

Configuration checks open file databases read-only and immutable rather than
modifying the live file. They use isolated databases for memory-mode checks.

## PostgreSQL change notifications

With `server` and `postgres`, `ChannelListener` wraps SQLx's LISTEN support for
a runtime component. It holds a connection from the selected pool, batches
available notifications as `Notice::Changed`, and reports a connection gap as
`Notice::Resync`.

Notifications are not durable delivery. On resync, the consumer reads its
authoritative tables again; `missed()` records that applying a notice failed.
The listener uses process-local `PostgresConnected` events to resume after a
database failure instead of inventing a retry timer. The owner selects on its
own cancellation signal to end a pending wait.

## Features and related crates

The implementation features are `postgres`, `mysql`, `sqlite`, and `server`.
The first three select SQLx backends; `server` adds `DbService`, runtime
registration, and reload integration. Backend availability is a compile-time
choice; selecting a profile cannot enable an uncompiled driver.

At the root facade, each of `postgres`, `mysql`, and `sqlite` also selects `db`.
This exposes `claviron::db` and, in a standard server composition, registers its
database service. Selecting a backend does not open a connection before boot or
supply connection settings.

- [Runtime](../runtime/README.md): ordered boot and prepared configuration.
- [Laravel](https://github.com/claviron/lumenize): framework database adapters above native pools.
- [Runner](../runner/README.md): enclosing application composition and boot options.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
