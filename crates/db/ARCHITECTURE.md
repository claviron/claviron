# Database config and pool provider — Architecture

`db` is the workspace's narrow database boundary. It owns
three things only:

1. The pure-data shape of `db.yaml`.
2. Translation from that config into backend-specific `sqlx`
   connect/pool options.
3. The live `DbService` provider that publishes swappable pools and
   the original connect metadata to the runtime registry.

It is intentionally not a query layer, migration layer, ORM policy
layer, or application repository crate.

## Facade feature composition

The root facade's `postgres`, `mysql`, and `sqlite` features each select `db`
alongside their backend forwarding. This keeps `claviron::db` and the standard
server's `DbService` registration available when a caller selects a backend.
The implementation crate's backend features remain independent of its `server`
feature; connection settings, boot, and pool ownership are unchanged.

## 1. Layering

`db` sits between config consumers and `sqlx`:

- Upstream callers read `DbService` from the runtime registry and ask
  for pools, connect options, or the original parsed `connect:`
  block.
- `DbService` is a Congif `Configurable` for `db.yaml`: the reader prepares
  the file — `${ENV_KEY}` expanded from the application's frozen
  configuration environment — before boot and in the central reload batch,
  and this crate constructs backend-specific pools from that prepared
  snapshot. The file is required; it has no absent form, and no source watch:
  pools are rebuilt on an explicit reload.
- `sqlx` remains the source of truth for connection-string parsing and
  backend semantics.

An enclosing runner can instead call `register_with_config(state, DbFileConfig)`
or register `DbService::from_config(config)`. This supplies the same schema as
immutable boot options: no `db.yaml` is read or watched and no config/reload
capability is exposed. Normal async boot constructs the same process-local
pools. Built-in file registration preserves an already supplied service;
duplicate explicit registrations still fail. Options changes require restart.

That last point is load-bearing: this crate should not reimplement SQL
URL parsing rules that `sqlx` already owns.

## 2. Config contract

A `ConnectionEntry` carries two top-level transport fields plus the
pool block:

- `url:` — `Option<url::Url>`. When present, parsed by serde at
  deserialize time and handed straight to
  `sqlx::ConnectOptions::from_url(...)`.
- `connect:` — `Option<ConnectInfo>` (nested map: `host`, `port`,
  `database`, `username`, `password`, `socket`). Each backend
  (`postgres.rs`, `mysql.rs`) applies the structured fields directly
  to its own `*ConnectOptions` builder.

These are deliberately **two separate top-level fields**, not a
flattened or untagged-enum single field. serde-yaml silently drops
flattened fields under the inner-default-everywhere shape we want
here; an untagged enum routes a string under `connect:` to the wrong
branch when `${ENV_VAR}` expands to a non-URL string. Two named fields
keep parsing predictable.

The structured branch is **not** routed through URL synthesis: each
backend's structured semantics differ (postgres' socket-as-host
convention, ssl/options/application_name flags, vs. mysql's first-class
socket option) and folding them into a URL only to re-parse it loses
fidelity for no win.

Boundary contract:

- Use exactly one of `url:` or `connect:` per entry.
- `url:` must stay typed (`url::Url`). Do not downgrade it to `String`
  or wrap it in a custom type; that re-opens the dual-mode mess.
- `socket:` is the first-class Unix-domain-socket field on
  `ConnectInfo`. Do not tunnel socket paths through `host:`.

### `pool.statement_timeout`

Optional; absent leaves the server's own setting alone. It is appended
to the PostgreSQL **connection options** rather than issued as a `SET`
once a connection is checked out, for two reasons: it then costs no
round trip, and it becomes the session default, so it survives the
`RESET ALL` that a pooled connection handed back to a request-scoped
caller is given. A `SET` would be undone by that reset, silently, and
the pool would go on serving unbounded statements.

`DbService::postgres_statement_timeout()` reports the configured value.
PostgreSQL enforces the bound on its own; the accessor exists for
callers that park an OS thread on a query — `php-embed`'s
request session does — and need to know how long that is allowed to
take before treating the connection as unresponsive.

## 3. Backend-specific invariants

The config shape is shared, but backend interpretation is not:

- Postgres can represent socket transports through its connect options
  directly (slash-prefixed `host` is the postgres-native socket form).
- MySQL has a dedicated `socket` option on `MySqlConnectOptions`;
  structured MySQL config must map `socket:` to
  `MySqlConnectOptions::socket(...)`, never to `host(...)`.

## 3b. ⚠️ IMPORTANT — MySQL auth plugin gotcha (read this BEFORE debugging auth)

> **sqlx-mysql + unix socket cannot complete `caching_sha2_password`
> auth — the MySQL-family default (MySQL 8, Percona Server 8, …).
> Symptom: `Access denied (using password: YES)` even with the correct
> password.**
>
> Two **alternative** plugins escape this — pick one, not both:
>
> - **`auth_socket`** (this repo's choice). Postgres-`peer`-style:
>   MySQL trusts `SO_PEERCRED`'s OS uid. No password needed for any
>   user — app pool or shell. Setup in §3b ("auth_socket" subsection).
> - **`mysql_native_password`** (alternative). Keep a password,
>   negotiate the legacy SHA1 challenge. Listed here as the documented
>   fallback in case auth_socket ever stops working.
>
> This took multiple hours to track down. Record kept here so the
> next person does not repeat it.

`my.cnf` (in this repo: [`etc/percona.cnf`](../../etc/percona.cnf))
must include:

```ini
[mysqld]
# socket transport is what we use; matches MYSQL_DATABASE_URL `?socket=...`
socket = /var/run/mysqld/mysqld.sock

# REQUIRED — sqlx-mysql cannot complete `caching_sha2_password` auth
# over a unix socket. Toggle the legacy plugin so users CREATEd /
# ALTERed with `IDENTIFIED WITH mysql_native_password` actually work.
mysql_native_password = ON

# Dev-only — without this MySQL 8 rejects non-TLS connections on a
# fresh install. Unix socket counts as non-TLS for this check. In
# prod, terminate TLS instead and remove this line.
require_secure_transport = OFF
```

Then for every user claviron will reach via socket:

```sql
ALTER USER 'claviron'@'localhost'
  IDENTIFIED WITH mysql_native_password BY '<password>';
FLUSH PRIVILEGES;
```

What happens with the default plugin (`caching_sha2_password`) over a
unix socket:

1. sqlx sends a fast-auth SHA256 scramble.
2. Server replies AUTH_CONTINUE (cache miss is the common path on a
   fresh user / first connection).
3. sqlx's full-auth fallback ([`auth.rs::encrypt_rsa`][1]) checks
   `stream.is_tls`. **Unix socket has `is_tls=false`**, even though
   the transport is filesystem-permission-secured.
4. sqlx tries the RSA-OAEP exchange path (request public key, encrypt
   password). If the server's RSA setup is incomplete, or the
   exchange races with the cache_clean state, the result surfaces
   as a flat `Access denied (using password: YES)` — indistinguishable
   from a wrong password. There is no useful retry path.

[1]: ../../.cargo registry path: `sqlx-mysql-0.8.6/src/connection/auth.rs`

Compare with postgres: `libpq`-style auth handles socket as secure by
construction; password / scram-sha-256 / md5 all "just work" over the
socket. The two backends are not symmetric.

MySQL 8.4+ ships with the `mysql_native_password` plugin **off** by
default — that's why the `mysql_native_password = ON` line in `my.cnf`
above is required, not optional. MySQL 8.0 / MariaDB ship with it on.

TCP-only setups can use `caching_sha2_password` over TLS without
issue — the gotcha is socket-specific.

### auth_socket — the chosen plugin (postgres `peer` analogue)

Both the claviron app pool **and** every human developer authenticate
through `auth_socket` over the unix socket. MySQL reads
`SO_PEERCRED` from the connection, matches the OS uid against the
user's `auth_socket` mapping, and lets you in without a password.
Same idea as postgres `peer` — symmetric across app and shell, no
secrets to rotate, no `~/.my.cnf`, no `${MYSQL_PASSWORD}` env var to
leak.

The `auth_socket.so` plugin ships with the server but is not loaded
by default. Two ways to load it:

```ini
# my.cnf (in this repo: etc/percona.cnf) — survives restarts
[mysqld]
plugin-load-add = auth_socket.so
```

```sql
-- runtime install — does not survive restart by itself, but
-- INSTALL writes to mysql.plugin so it auto-loads on next start.
INSTALL PLUGIN auth_socket SONAME 'auth_socket.so';
```

Verify:

```sql
SELECT plugin_name, plugin_status FROM information_schema.plugins
 WHERE plugin_name = 'auth_socket';
-- expected: auth_socket | ACTIVE
```

Then create one user per OS identity that needs to talk to MySQL —
the app's runtime user (`claviron`) and each developer's shell user
(`developer`, …):

```sql
CREATE USER 'claviron'@'localhost'  IDENTIFIED WITH auth_socket;
CREATE USER 'developer'@'localhost' IDENTIFIED WITH auth_socket;

GRANT ALL PRIVILEGES ON test_db.* TO 'claviron'@'localhost';
GRANT ALL PRIVILEGES ON *.*       TO 'developer'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

After this, plain `mysql` Just Works for the OS user, and claviron's
sqlx pool authenticates without a password — both via the same
`auth_socket` mapping. `db.yaml` uses an empty password (or omits it
entirely):

```yaml
mysql:
  default:
    connect:
      socket: /run/mysqld/mysqld.sock
      database: test_db
      username: claviron
      password: ""        # auth_socket — server ignores the password
    pool: { ... }
```

`auth_socket` over sqlx — empirical note: sqlx 0.8.x does **not**
list `auth_socket` in its `AuthPlugin` enum
(`sqlx-mysql-0.8.6/src/protocol/auth.rs`), yet socket-auth users
still authenticate successfully in practice. The reason is the
server side: when the `auth_socket` plugin matches `SO_PEERCRED`'s OS
uid against the user's identifier, MySQL replies AUTH_OK during the
initial handshake without issuing an `AUTH_SWITCH_REQUEST`. sqlx
sends a discardable scrambled password, the server ignores it, the
connection is accepted.

Caveat: this depends on MySQL not falling back to `AUTH_SWITCH` for
auth_socket users. If a future server build / config sends an
explicit switch to `auth_socket`, sqlx 0.8.x will error on the
unknown plugin name. If that ever happens, the alternative is the
`mysql_native_password` plugin (documented above as the fallback —
keep a real password, sqlx scrambles it via SHA1 challenge, no
RSA exchange needed). Both routes coexist; switching is a per-user
`ALTER USER ... IDENTIFIED WITH ...` away.

Same gotcha does NOT apply to:

- TCP loopback (`mysql://user:pw@127.0.0.1/db`) — TLS-or-not,
  `caching_sha2_password` works because RSA exchange has time to
  complete normally.
- `sqlx-postgres` — irrelevant, postgres uses different auth.

If you hit `Access denied (using password: YES)` over a unix socket,
the auth plugin is the first thing to check, not the password.

## 3c. Process identity and `'user'@'localhost'`

Unix socket connections always present as `'<user>'@'localhost'` to
MySQL — even with `skip-name-resolve=ON`, even when the client is on
the same box. MySQL **does not** fall back from `@'%'` to `@'localhost'`
(or vice versa) for socket origin. A user that exists only as
`'claviron'@'%'` will get `Access denied` for socket connections.

Always create the `@'localhost'` row explicitly when you intend to
serve socket clients, even if a `@'%'` row already exists.

## 3c'. Profiles: `<backend>.<name>`

A connection is named by its backend and its key: `postgres.default`,
`mysql.revolution`, `sqlite.hosting`. `DbService::profile(name)` returns that
connection's pool as `Pool`, or says what is missing — a backend this build
lacks, or a name the file lacks. `db.yaml`'s top-level `hosting:` is such a
profile: where Claviron's hosting data is, `postgres.default` when unwritten.

## 3d. SQLite (`sqlite` feature)

`sqlite:` names SQLite files, each opened by every process of the fleet that
uses it:

```yaml
sqlite:
  hosting:
    path: /var/lib/portico/hosting.db
    busy_timeout: 5s     # required — no default is invented for it
    create: true         # default false: a missing file is an error
    pool: { pool_size: 4, idle_size: 0, acquire_timeout: 10s, idle_timeout: 5m, max_lifetime: 30m }
```

- The journal is always WAL. Several processes read one file; the rollback
  journal would stop every reader for each write, WAL lets them go on while
  one process writes. This is structural, not tuning, so it is not a key.
- Writers in all processes take turns on one lock. `busy_timeout` is how
  long a statement waits for its turn before `SQLITE_BUSY`; it is the
  operator's decision and the entry does not parse without it.
- `pool.statement_timeout` is refused on an SQLite entry: SQLite has no
  statement timeout, and a written bound must not silently do nothing.
- Optional profile-level `mmap_size` sets the maximum bytes of the persistent
  database file mapped for reads on every pool connection, including newly
  opened/replacement connections. It accepts a byte count or a human-readable
  size (`16 MiB`); omission/null preserves SQLite's default, zero disables it.
  Values beyond SQLite's signed 64-bit range are rejected. SQLite may cap the
  requested ceiling or disable mapping on unsupported platforms. This is not
  an in-memory database or a RAM reservation: file persistence, WAL, and the
  pool/query API remain unchanged. Mapping mainly benefits reads and is not
  guaranteed to improve performance; mapped-file I/O faults can terminate the
  process. Do not replace or truncate a live database file.
- `synchronous` and other unconfigured SQLite settings stay at SQLite's defaults.
- `mode: memory` creates a fresh process-local database per pool, with the same
  `SqlitePool`/query API. Omit `path`, `create` and `mmap_size`; these file-only
  options are rejected, not ignored. `mode: file` is the default and requires
  `path`. Memory mode uses SQLite's MEMORY journal and SQLx's uniquely named
  shared-memory URI so query connections share data but separate pools do not.
  A lazily opened private anchor connection retains data while query connections
  recycle; it is outside `pool_size`, runs no queries, and closes on pool close
  or last-handle drop. Query pool durations/capacity remain operator-owned.
  Reload creates a new empty database; memory mode is neither persistence nor
  fleet-shared state. A configuration check creates only an isolated memory DB.
- A configuration check (`ConfigCheck`) opens every file read-only and
  immutable: it writes nothing the running server shares, and needs no
  `-shm` and no write access to the directory. WAL otherwise needs both,
  even to read.
- SQLite has no LISTEN/NOTIFY. A consumer that needs to hear about changes
  hears them from whoever wrote, over the fleet's own events; nothing here
  polls the file.

## 4. Provider lifecycle

`DbService` is a runtime component with a `Bootable` implementation at
`priority::FIRST`.

Why:

- Other providers may need working DB pools during their own boot.
- The registry constructs pools during ordered boot. File-backed reloads
  rebuild the full `DbState` and atomically swap it through `ArcSwap`.
- Supplied options use the same pool construction path but expose neither a
  file configuration owner nor a reload hook. The runner owns their launch.

Boundary contract:

- Reload replaces the whole backend state snapshot; it does not mutate
  individual pools in place.
- `connect_info` mirrors the parsed config for callers that need a
  dedicated connection outside the pool model.

## 5. What does not belong here

Do not add:

- query repositories
- migration orchestration
- app-specific connection selection policy
- ad hoc string munging for backend URLs when `sqlx` already exposes
  the needed parser/builder

Those belong in callers or in a higher-level crate that owns the
application behavior.
