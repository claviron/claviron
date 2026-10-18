//! The request's PostgreSQL session.
//!
//! A pooled connection is checked out on first use and kept for the rest of the
//! request, so everything PHP does lands on one session: `SET`, a temp table,
//! an advisory lock and a transaction all behave the way they would behind a
//! PDO handle. Acquiring per statement instead — which is what this bridge did
//! before — made session state depend on which connection each statement
//! happened to draw, and left whatever it set behind for the next request that
//! drew the same one.
//!
//! The lease is deliberately lazy: a request that never touches the database
//! holds nothing, so the pool is sized by the PHP instances that are actually
//! querying rather than by the requests in flight.
//!
//! Two guarantees come out of owning the boundary here rather than in PHP:
//!
//! * A transaction cannot outlive the request that opened it. The runtime rolls
//!   back whatever is still open, and throws the connection away outright if
//!   even the rollback fails, so no later request can inherit it.
//! * Nothing a request leaves on a connection reaches the next one, because a
//!   connection that ran anything stateful is reset on its way back.
//!
//! Contexts with no request boundary — a PHP task, an embedded queue worker —
//! get the older, narrower promise: a connection is held only for as long as an
//! open transaction needs it. The rule is one sentence: *inside a request, for
//! the request; outside one, for the transaction.*

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::time::Duration;

use claviron_db::DbService;
use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, Executor, PgConnection, Postgres};
use tokio::runtime::Handle;
use tokio::time::timeout;
use tracing::warn;

use super::{Parameter, QueryResult};

/// Everything the next request must not inherit, and nothing that would throw
/// away the per-connection statement cache.
///
/// `DISCARD ALL` would be the obvious call and is the wrong one: it implies
/// `DEALLOCATE ALL`, which drops the prepared statements the type layer relies
/// on to describe parameters, so every request after a reset would pay the
/// describe cost again.
const RESET_SESSION: &str = "CLOSE ALL; \
     RESET ALL; \
     UNLISTEN *; \
     SELECT pg_advisory_unlock_all(); \
     DISCARD TEMP; \
     DISCARD SEQUENCES";

/// Why the request's connection could not be borrowed.
///
/// Only the borrow fails this way. What the borrower then does with the
/// connection is its own business and its own error type.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SessionError {
    #[error("Claviron PostgreSQL is not configured")]
    Unconfigured,
    #[error("PostgreSQL pool is not registered: {pool}")]
    UnknownPool { pool: String },
    #[error("PostgreSQL connection is unavailable: {source}")]
    Unavailable {
        pool: String,
        #[source]
        source: sqlx::Error,
    },
    #[error(
        "the request's PostgreSQL session is already in use on this thread"
    )]
    Busy,
    #[error(
        "synchronous PostgreSQL access must run outside a Tokio runtime thread"
    )]
    RuntimeThread,
}

/// How far behind PostgreSQL's own `statement_timeout` the client-side bound
/// sits.
///
/// The server's timeout is the real one and gives the better error, so this is
/// not a second policy: it is the backstop for a server that never answers at
/// all — a partitioned network, a host that went away — where the PHP thread
/// would otherwise block until TCP gives up, which is exactly the runaway this
/// is meant to prevent. A margin rather than a knob, so an operator has one
/// number to reason about.
const UNRESPONSIVE_GRACE: Duration = Duration::from_secs(5);

/// One pooled connection, checked out on this thread.
struct Lease {
    /// `None` only while the connection is being handed back, which `Drop` and
    /// `discard` are the only things that do.
    connection: Option<PoolConnection<Postgres>>,
    /// Returning a connection to the pool is asynchronous work, so the drop
    /// needs a runtime context wherever it happens — including when the PHP
    /// thread itself is going away, which is why the handle lives here rather
    /// than being passed in.
    runtime: Handle,
    /// A top-level transaction is open. Savepoints are the caller's business:
    /// Laravel writes them as ordinary statements, so only the outermost
    /// transaction is tracked here.
    in_transaction: bool,
    /// Open server-side cursors. A cursor lives on the connection that declared
    /// it, so it pins the lease the way a transaction does — otherwise the
    /// first `FETCH` outside a request would land on a different connection and
    /// find nothing.
    cursors: usize,
    /// Something ran that can leave state on the connection, so it is reset
    /// before it goes back to the pool.
    dirty: bool,
}

impl Lease {
    fn connection(&mut self) -> &mut PgConnection {
        self.connection
            .as_mut()
            .expect("a lease holds its connection until it is handed back")
    }

    /// Throws the connection away instead of returning it, for when its state
    /// could not be cleaned. PostgreSQL ends whatever it was still holding when
    /// the socket closes, which is the only guarantee left at that point.
    fn discard(&mut self) {
        if let Some(connection) = self.connection.take() {
            let _guard = self.runtime.enter();
            drop(connection.detach());
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };

        // sqlx hands the connection back from its own `Drop`, and that panics
        // without a runtime context. Entering here covers every path a lease
        // can end on, including thread teardown.
        let _guard = self.runtime.enter();
        drop(connection);
    }
}

thread_local! {
    /// Connections this thread has checked out, keyed by pool name. A request
    /// that talks to two pools holds one connection from each.
    static LEASES: RefCell<HashMap<String, Lease>> = RefCell::new(HashMap::new());

    /// Whether the runtime has a unit of work open on this thread.
    static IN_REQUEST: Cell<bool> = const { Cell::new(false) };
}

/// Opens the request scope. Anything still checked out belongs to a request
/// that never reached its end, so it is cleaned up first.
pub(crate) fn begin_request(runtime: &Handle) {
    release_all(runtime, "a new request started");
    IN_REQUEST.set(true);
}

/// Closes the request scope: any open transaction is rolled back, a connection
/// that may carry session state is reset, and both go back to the pool.
///
/// Idempotent, because it runs from both ends — after the response, and again
/// when the next request arrives, since a request that fails before responding
/// never reaches the first.
pub(crate) fn end_request(runtime: &Handle) {
    IN_REQUEST.set(false);
    release_all(runtime, "the request ended");
}

/// Borrows the connection the current request is already using.
///
/// This is what makes Rust and PHP one unit of work rather than two. Critical
/// logic belongs in Rust and is called from PHP; because the host function runs
/// on the PHP thread, it lands on the same lease, so a transaction opened in
/// PHP encloses the Rust writes and a transaction opened here encloses PHP's.
/// Without it the Rust half would draw its own pooled connection and commit
/// outside whatever PHP had open.
///
/// `run` is handed the connection and the runtime handle and does its own
/// `block_on`, so the borrow never crosses an await. Its return value comes
/// back untouched — the error in the signature is only about the borrow.
///
/// Outside a request the connection is released as soon as `run` returns,
/// unless `run` left a transaction open on it.
///
/// PHP reaches this through a host function: one registered with
/// `PhpEmbedExtension::append_functions`, which is what makes it run on PHP's
/// own thread and therefore on the request's lease. That does not mean a host
/// function per job. The shape is one entry point that dispatches by
/// operation to handlers registered in Rust, the way the invocation router
/// does, so a new job is a handler and nothing on PHP's side changes. Work
/// that has no need of PHP's transaction does not belong here at all: it goes
/// out as an invocation and runs on a pooled connection of its own.
///
/// ```no_run
/// # use claviron_php_embed::postgres::with_request_connection;
/// let total: i64 = with_request_connection("octane", |connection, runtime| {
///     runtime.block_on(async {
///         sqlx::query_scalar("select count(*) from ledger_entries")
///             .fetch_one(connection)
///             .await
///     })
/// })??;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn with_request_connection<T>(
    pool: &str,
    run: impl FnOnce(&mut PgConnection, &Handle) -> T,
) -> Result<T, SessionError> {
    let bridge = super::bridge().map_err(|_| SessionError::Unconfigured)?;

    with_lease(&bridge.service, &bridge.runtime, pool, |lease| {
        // The runtime reads PHP's statements to decide whether a connection
        // needs resetting; it cannot read arbitrary Rust. So a borrow is
        // assumed to leave session state. One round trip at the end of the
        // request is the cheap side of that trade — the expensive side is a
        // `SET` or a temp table reaching the next one.
        lease.dirty = true;

        run(lease.connection(), &bridge.runtime)
    })
}

/// Runs one prepared statement on the request's connection, bounded by the
/// pool's statement budget.
pub(super) fn execute(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    sql: &str,
    parameters: Vec<Parameter>,
) -> Result<QueryResult, String> {
    bounded(service, runtime, pool, sql, async |connection| {
        super::execute_on(connection, sql, parameters).await
    })
}

/// Runs raw SQL on the request's connection through the simple query
/// protocol, the way `PDO::exec()` does: several statements in one string are
/// allowed, nothing is bound, and the answer is the number of rows affected.
pub(super) fn execute_unprepared(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    sql: &str,
) -> Result<u64, String> {
    bounded(service, runtime, pool, sql, async |connection| {
        super::execute_unprepared_on(connection, sql).await
    })
}

/// Runs `run` on the request's connection, bounded by the pool's statement
/// budget.
///
/// PostgreSQL cancels a statement that outruns its own `statement_timeout`, and
/// the connection stays usable. The bound here is for the case where no answer
/// arrives at all: the thread is released, and the connection is thrown away
/// rather than pooled, because a query may still be running on it and nothing
/// about its state can be trusted.
fn bounded<T>(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    sql: &str,
    run: impl AsyncFnOnce(&mut PgConnection) -> Result<T, sqlx::Error>,
) -> Result<T, String> {
    let budget = service
        .postgres_statement_timeout(pool)
        .map(|bound| bound + UNRESPONSIVE_GRACE);
    let unresponsive = Cell::new(false);

    let dirties = leaves_session_state(sql);

    let result = with_lease(service, runtime, pool, |lease| {
        lease.dirty |= dirties;

        runtime.block_on(async {
            let query = run(lease.connection());

            let Some(budget) = budget else {
                return query.await.map_err(|error| super::describe(&error));
            };

            match timeout(budget, query).await {
                Ok(result) => result.map_err(|error| super::describe(&error)),
                Err(_) => {
                    unresponsive.set(true);

                    Err(format!(
                        "PostgreSQL did not answer within {budget:?}; the connection was dropped"
                    ))
                }
            }
        })
    })
    .map_err(|error| error.to_string());

    if unresponsive.get() {
        warn!(pool = %pool, "PostgreSQL statement went unanswered; discarding the connection");
        discard(pool);
    }

    result?
}

/// Opens a top-level transaction on the request's connection and pins it there
/// until it is committed or rolled back.
pub(super) fn begin(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    statement: &str,
) -> Result<(), String> {
    with_lease(service, runtime, pool, |lease| {
        if lease.in_transaction {
            return Err(
                "a PostgreSQL transaction is already open on this connection"
                    .to_owned(),
            );
        }

        runtime
            .block_on(
                lease.connection().execute(AssertSqlSafe(statement.to_owned())),
            )
            .map_err(|error| super::describe(&error))?;
        lease.in_transaction = true;

        Ok(())
    })
    .map_err(|error| error.to_string())?
}

/// Ends the open transaction. Outside a request the connection goes back to the
/// pool with it, because a transaction was the only reason to hold it.
pub(super) fn finish(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    commit: bool,
) -> Result<(), String> {
    let result = with_lease(service, runtime, pool, |lease| {
        if !lease.in_transaction {
            return Err("no PostgreSQL transaction is open on this connection"
                .to_owned());
        }

        // The transaction is over either way: a failed COMMIT leaves nothing to
        // commit later, and keeping the flag would only block the next BEGIN.
        lease.in_transaction = false;

        runtime
            .block_on(lease.connection().execute(AssertSqlSafe(
                if commit { "COMMIT" } else { "ROLLBACK" }.to_owned(),
            )))
            .map(|_| ())
            .map_err(|error| super::describe(&error))
    })
    .map_err(|error| error.to_string());

    if !IN_REQUEST.get() && !pinned(pool) {
        release(runtime, pool);
    }

    result?
}

/// Whether the request still holds an open transaction on `pool`.
pub(super) fn in_transaction(pool: &str) -> bool {
    LEASES.with_borrow(|leases| {
        leases.get(pool).is_some_and(|lease| lease.in_transaction)
    })
}

/// Releases only this thread's lease, without acquiring another connection.
/// The request boundary remains open; its next query may obtain a fresh lease.
pub(super) fn disconnect(
    runtime: &Handle,
    pool: &str,
) -> Result<(), SessionError> {
    blocking_thread()?;
    let lease = LEASES.with(|leases| {
        leases
            .try_borrow_mut()
            .map(|mut leases| leases.remove(pool))
            .map_err(|_| SessionError::Busy)
    })?;
    if let Some(lease) = lease {
        // Cleanup rolls back and resets, or discards the connection on error.
        // No state from a failed cleanup is returned to the pool.
        clean(runtime, pool, lease);
    }

    Ok(())
}

/// Whether anything still needs this connection to be the same one next time.
fn pinned(pool: &str) -> bool {
    LEASES.with_borrow(|leases| {
        leases
            .get(pool)
            .is_some_and(|lease| lease.in_transaction || lease.cursors > 0)
    })
}

/// Declares a server-side cursor and pins the connection to it.
///
/// The pin is taken inside the same borrow that declares it: a cursor declared
/// and then released would be a cursor on a connection nobody can reach.
pub(super) fn declare_cursor(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    sql: &str,
    parameters: Vec<Parameter>,
) -> Result<(), String> {
    with_lease(service, runtime, pool, |lease| {
        lease.dirty = true;

        runtime
            .block_on(super::execute_on(lease.connection(), sql, parameters))
            .map_err(|error| super::describe(&error))?;
        lease.cursors += 1;

        Ok(())
    })
    .map_err(|error| error.to_string())?
}

/// Closes a cursor and gives up its share of the pin.
pub(super) fn close_cursor(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    sql: &str,
) -> Result<(), String> {
    let result = with_lease(service, runtime, pool, |lease| {
        // Released either way: a cursor that could not be closed is not a
        // reason to hold the connection for ever, and the reset that follows
        // closes every cursor on it.
        lease.cursors = lease.cursors.saturating_sub(1);

        runtime
            .block_on(lease.connection().execute(AssertSqlSafe(sql.to_owned())))
            .map(|_| ())
            .map_err(|error| super::describe(&error))
    })
    .map_err(|error| error.to_string());

    if !IN_REQUEST.get() && !pinned(pool) {
        release(runtime, pool);
    }

    result?
}

/// Borrows the lease for `pool`, checking a connection out if there is none.
///
/// Outside a request the connection is released again as soon as `run` is done,
/// unless `run` left a transaction open on it.
fn with_lease<T, F>(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    run: F,
) -> Result<T, SessionError>
where
    F: FnOnce(&mut Lease) -> T,
{
    blocking_thread()?;

    let handle = service
        .postgres_named(pool)
        .ok_or_else(|| SessionError::UnknownPool { pool: pool.to_owned() })?;

    let result = LEASES.with(|leases| {
        // `try_borrow_mut` rather than `borrow_mut`: a re-entrant call is a bug
        // worth naming, not a panic in the middle of a request.
        let mut leases =
            leases.try_borrow_mut().map_err(|_| SessionError::Busy)?;

        if !leases.contains_key(pool) {
            let connection =
                runtime.block_on(handle.acquire()).map_err(|source| {
                    SessionError::Unavailable { pool: pool.to_owned(), source }
                })?;
            leases.insert(
                pool.to_owned(),
                Lease {
                    connection: Some(connection),
                    runtime: runtime.clone(),
                    in_transaction: false,
                    cursors: 0,
                    dirty: false,
                },
            );
        }

        let lease = leases.get_mut(pool).expect("the lease was just inserted");

        Ok(run(lease))
    });

    if !IN_REQUEST.get() && !pinned(pool) {
        release(runtime, pool);
    }

    result
}

/// Throws the request's connection for `pool` away without trying to clean it.
/// Used when a statement went unanswered: a rollback or reset would only be
/// another wait on a connection that has already proven it may not answer.
fn discard(pool: &str) {
    if let Some(mut lease) =
        LEASES.with_borrow_mut(|leases| leases.remove(pool))
    {
        lease.discard();
    }
}

/// Hands one connection back, rolling back and resetting first when it needs it.
fn release(
    runtime: &Handle,
    pool: &str,
) {
    let Some(lease) = LEASES.with_borrow_mut(|leases| leases.remove(pool))
    else {
        return;
    };

    clean(runtime, pool, lease);
}

fn release_all(
    runtime: &Handle,
    reason: &str,
) {
    if blocking_thread().is_err() {
        warn!(%reason, "PostgreSQL session cannot be released from a runtime thread");

        return;
    }

    for (pool, lease) in LEASES.with_borrow_mut(std::mem::take) {
        if lease.in_transaction {
            warn!(
                pool = %pool,
                %reason,
                "PostgreSQL transaction was still open; rolling it back"
            );
        }

        clean(runtime, &pool, lease);
    }
}

/// Rolls back and resets as needed, then lets the lease go — back to the pool
/// if it is clean, into the bin if it is not.
fn clean(
    runtime: &Handle,
    pool: &str,
    mut lease: Lease,
) {
    if lease.in_transaction
        && let Err(error) = runtime.block_on(
            lease.connection().execute(AssertSqlSafe("ROLLBACK".to_owned())),
        )
    {
        warn!(pool = %pool, %error, "PostgreSQL rollback failed; discarding the connection");
        lease.discard();

        return;
    }

    if lease.dirty
        && let Err(error) = runtime.block_on(
            lease.connection().execute(AssertSqlSafe(RESET_SESSION.to_owned())),
        )
    {
        warn!(pool = %pool, %error, "PostgreSQL session reset failed; discarding the connection");
        lease.discard();

        return;
    }

    tracing::trace!(pool = %pool, reset = lease.dirty, "PostgreSQL connection returned to the pool");
}

/// Every database round trip parks this thread on the runtime, because PHP
/// calls are synchronous. Parking a runtime worker thread on its own runtime
/// deadlocks it, so the check stays even though a PHP thread is never one.
fn blocking_thread() -> Result<(), SessionError> {
    match Handle::try_current() {
        Ok(_) => Err(SessionError::RuntimeThread),
        Err(_) => Ok(()),
    }
}

/// Whether a statement can leave something behind on the connection.
///
/// The safe list is the shape of ordinary application traffic; everything else
/// — `SET`, `LISTEN`, `PREPARE`, `CREATE TEMP TABLE` — is treated as session
/// state. Conservative on purpose: a false positive costs one round trip at the
/// end of the request, a false negative leaks into the next one.
fn leaves_session_state(sql: &str) -> bool {
    let keyword = sql
        .trim_start()
        .split(|character: char| character.is_whitespace() || character == '(')
        .next()
        .unwrap_or_default();

    let ordinary =
        ["SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "VALUES", "TABLE"]
            .iter()
            .any(|candidate| keyword.eq_ignore_ascii_case(candidate));

    if !ordinary {
        return true;
    }

    // An advisory lock hides inside an ordinary select and outlives it.
    contains_ignore_ascii_case(sql, "advisory")
}

fn contains_ignore_ascii_case(
    haystack: &str,
    needle: &str,
) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}
