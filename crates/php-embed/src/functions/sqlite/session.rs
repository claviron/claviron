//! Request-local SQLite leases. The engine's autocommit flag is authoritative,
//! including SQLite's automatic rollback after an error or explicit raw SQL.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::ffi::{c_int, c_void};

use claviron_db::DbService;
use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, Connection, Executor, Sqlite};
use tokio::runtime::Handle;

use super::{Parameter, QueryResult};

// Stable SQLite C API, linked by SQLx. Holding SQLx's locked handle prevents its
// worker from using the connection while its autocommit flag is inspected.
unsafe extern "C" {
    fn sqlite3_get_autocommit(connection: *mut c_void) -> c_int;
}

struct Lease {
    connection: Option<PoolConnection<Sqlite>>,
    runtime: Handle,
    transaction: bool,
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            // Close instead of returning a session whose PRAGMAs, temporary
            // tables or unfinished transaction belong to a previous request.
            // Await close so the next work unit cannot inherit its write lock.
            let connection = connection.detach();
            if let Err(error) = self.runtime.block_on(connection.close()) {
                tracing::warn!(%error, "SQLite request connection close failed");
            }
        }
    }
}

thread_local! {
    static LEASES: RefCell<HashMap<String, Lease>> = RefCell::new(HashMap::new());
    static IN_REQUEST: Cell<bool> = const { Cell::new(false) };
    // Keep an aborted transaction from silently continuing in autocommit.
    static FAILED: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

pub(crate) fn begin_request(runtime: &Handle) {
    end_request(runtime);
    IN_REQUEST.set(true);
}

pub(crate) fn end_request(_runtime: &Handle) {
    IN_REQUEST.set(false);
    FAILED.with_borrow_mut(HashSet::clear);
    // sqlite3_close rolls back any open transaction; no implicit commit.
    drop(LEASES.with_borrow_mut(std::mem::take));
}

pub(super) fn disconnect(pool: &str) {
    FAILED.with_borrow_mut(|failed| failed.remove(pool));
    LEASES.with_borrow_mut(|leases| leases.remove(pool));
}

fn transaction_open(lease: &mut Lease) -> Result<bool, sqlx::Error> {
    lease.runtime.block_on(async {
        let mut handle = lease
            .connection
            .as_mut()
            .expect("live lease")
            .lock_handle()
            .await?;
        // SAFETY: the handle is live and exclusively locked until this call
        // completes. SQLite's stable C API returns zero inside a transaction.
        Ok(unsafe {
            sqlite3_get_autocommit(handle.as_raw_handle().as_ptr().cast()) == 0
        })
    })
}

fn with_lease<T>(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    run: impl FnOnce(&mut Lease) -> Result<T, sqlx::Error>,
) -> Result<T, String> {
    if Handle::try_current().is_ok() {
        return Err(
            "synchronous SQLite access must run outside a Tokio runtime thread"
                .into(),
        );
    }
    if FAILED.with_borrow(|failed| failed.contains(pool)) {
        return Err("SQLite session failed; explicitly roll back or begin a new transaction".into());
    }
    let result = LEASES.with(|leases| {
        let mut leases = leases.try_borrow_mut().map_err(|_| {
            "the request's SQLite session is already in use".to_owned()
        })?;
        if !leases.contains_key(pool) {
            let handle = service.sqlite_named(pool).ok_or_else(|| {
                format!("SQLite pool is not registered: {pool}")
            })?;
            let connection = runtime
                .block_on(handle.acquire())
                .map_err(|e| super::describe(&e))?;
            leases.insert(
                pool.to_owned(),
                Lease {
                    connection: Some(connection),
                    runtime: runtime.clone(),
                    transaction: false,
                },
            );
        }
        let lease = leases.get_mut(pool).expect("inserted lease");
        let was_transaction = lease.transaction;
        let result = run(lease);
        let state = transaction_open(lease);
        // Constraint errors usually leave a transaction open; ON CONFLICT
        // ROLLBACK and engine failures may end it. Inspect rather than guess.
        let aborted = state.is_err()
            || (result.is_err()
                && was_transaction
                && matches!(&state, Ok(false)));
        if let Ok(open) = &state {
            lease.transaction = *open;
        }
        if aborted {
            leases.remove(pool);
            FAILED.with_borrow_mut(|failed| failed.insert(pool.to_owned()));
        }
        match result {
            Err(error) => Err(super::describe(&error)),
            Ok(value) => state.map(|_| value).map_err(|e| super::describe(&e)),
        }
    });
    if !IN_REQUEST.get() && !in_transaction(pool) {
        LEASES.with_borrow_mut(|leases| leases.remove(pool));
    }
    result
}

pub(super) fn execute(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    sql: &str,
    parameters: Vec<Parameter>,
) -> Result<QueryResult, String> {
    with_lease(service, runtime, pool, |lease| {
        runtime.block_on(super::execute_on(
            lease.connection.as_mut().expect("live lease"),
            sql,
            parameters,
        ))
    })
}

pub(super) fn execute_unprepared(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    sql: &str,
) -> Result<u64, String> {
    with_lease(service, runtime, pool, |lease| {
        runtime.block_on(super::execute_unprepared_on(
            lease.connection.as_mut().expect("live lease"),
            sql,
        ))
    })
}

pub(super) fn begin(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    statement: &str,
) -> Result<(), String> {
    if FAILED.with_borrow_mut(|failed| failed.remove(pool)) {
        LEASES.with_borrow_mut(|leases| leases.remove(pool));
    }
    if in_transaction(pool) {
        return Err(
            "a SQLite transaction is already open on this connection".into()
        );
    }
    with_lease(service, runtime, pool, |lease| {
        runtime.block_on(
            lease
                .connection
                .as_mut()
                .expect("live lease")
                .execute(AssertSqlSafe(statement)),
        )?;
        Ok(())
    })
}

pub(super) fn finish(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    commit: bool,
) -> Result<(), String> {
    if !commit && FAILED.with_borrow_mut(|failed| failed.remove(pool)) {
        return Ok(());
    }
    if !in_transaction(pool) {
        return Err("no SQLite transaction is open on this connection".into());
    }
    with_lease(service, runtime, pool, |lease| {
        runtime.block_on(
            lease.connection.as_mut().expect("live lease").execute(
                AssertSqlSafe(if commit { "COMMIT" } else { "ROLLBACK" }),
            ),
        )?;
        Ok(())
    })
}

pub(super) fn in_transaction(pool: &str) -> bool {
    FAILED.with_borrow(|failed| failed.contains(pool))
        || LEASES.with_borrow(|leases| {
            leases.get(pool).is_some_and(|lease| lease.transaction)
        })
}
