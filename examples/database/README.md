# Shared SQLite demonstration database

`demo.sqlite` is the one database used by the Examples examples on a single OS.
It is prepared with Octane's Laravel migrations and synthetic seed data. No
PostgreSQL server or database credentials are needed. Open it with any SQLite
browser; the demo user is `demo@example.test` with password `password`.

Octane owns the migrations under `../octane-app/database/migrations`. Its
workers each use their own PDO connection. Orbit's JavaScript process and
FastAPI's Python process open their own connections to this same file; Orbit
Link carries application traffic, not database handles or transactions. WAL
allows concurrent readers while SQLite serializes writers. Edge does not need
to open this database merely to route requests.

To apply new migrations and seed missing example records, from `octane-app`:

```sh
"$PHP" artisan migrate --database=sqlite --force
"$PHP" artisan db:seed --database=sqlite --force
```

These are explicit maintenance commands, never startup hooks. Only use them
against this demonstration database. They do not drop existing tables. Stop all
example readers/writers before replacing or copying the database; checkpoint its
WAL before sharing a snapshot. `-wal` and `-shm` files stay untracked. The checked-in
file contains public synthetic data only; do not store personal or production
data in it. Local use changes the file, so review it before committing again.

Do not run macOS and VM workers against the same live file on a shared mount.
Use one OS at a time, or an explicitly configured host-local copy. If an
application overrides `EXAMPLES_DATABASE` or `DB_DATABASE`, it is no longer using
this common demonstration file.

Lua's native SQL adapter currently exposes PostgreSQL/MySQL, not SQLite. The Lua
DB diagnostic returns an explicit unavailable response until that adapter exists;
it must not use a remote PostgreSQL database as an implicit fallback.
