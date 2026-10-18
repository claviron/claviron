# One request, one PostgreSQL session

Design identifier: `CONNECTION_HIJACK`.

Your PHP code already has a database session. If it calls Rust to do part of
the work, why acquire a second connection? The important reason to reuse the
first is not just avoiding a dial or a pool checkout: both halves must see the
same transaction, locks, cursors and session state.

We use `CONNECTION_HIJACK` as a searchable name for this borrowing arrangement.
It is not HTTP connection takeover. The current implementation is the embedded
PHP PostgreSQL bridge, not a general ability to steal an arbitrary PDO handle.

## Borrow the lease that is already there

The bridge acquires a pooled PostgreSQL connection lazily, on first use, and
holds it for the request. A request that never uses the database holds none.
Each named pool has its own lease.

A native host function called on the PHP thread can use
`with_request_connection(pool, callback)` to borrow that same `PgConnection`.
The callback receives the connection and the runtime handle. It returns before
the borrow ends; it must not hand the connection to detached work.

This lets Rust operations participate in a transaction opened by PHP, and PHP
operations participate in one opened by Rust. Merely acquiring from the same
pool would not do that: a different connection is a different database session.
Commit and rollback still belong to the operation's explicit transaction flow;
calling Rust does not commit anything by itself.

## Keep ownership with the runtime

Borrowing is not a transfer of the connection out of the pool or the request.
The synchronous bridge must run on its supported execution thread; a nested
borrow already using the session is rejected rather than creating a second
session to hide the conflict. Borrow errors are separate from whatever error
the callback's database operation returns.

The runtime closes the request's session scope. It rolls back an outstanding
tracked transaction, resets session state before reuse, and discards a
connection if cleanup fails. A Rust borrow is conservatively marked dirty:
the bridge cannot inspect arbitrary Rust statements to discover their effects.
Cleanup preserves reusable prepared statements rather than blindly issuing
`DISCARD ALL` and destroying the statement cache.

Outside a request, such as a PHP task, the lease is not held for an imaginary
HTTP lifetime: it is retained while a transaction needs it and otherwise
released after the operation.

## Use it for a shared unit of work

This path is for native work that needs the PHP caller's session. Independent
background operations can use their own pooled connection or invocation.
An invocation in another execution context does not inherit the caller's
transaction. Nor does this bridge create a transaction spanning two pool names
or two databases.

The boundary lets an application move critical logic to Rust without splitting
one database unit of work into two unrelated ones.

## Follow the implementation

- [Public PostgreSQL borrowing API and rationale](../../crates/php-embed/src/postgres.rs).
- [Request leases, borrowing and cleanup](../../crates/php-embed/src/functions/postgres/session.rs).
- [Request-owned cursors](../../crates/php-embed/src/functions/postgres/cursor.rs).
- [PHP Embed integration](../../crates/php-embed/README.md).

Keep `CONNECTION_HIJACK` as the search key if the public name evolves later.
