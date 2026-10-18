//! One MySQL lease per pool and PHP work unit. No connection can carry an
//! abandoned transaction or session state into another work unit.
//!
//! SQLx does not expose COM_RESET_CONNECTION. Until it does, these leases are
//! detached and closed, not returned as reusable sessions. Outside a request,
//! only an explicit transaction pins a lease across primitive calls.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

use claviron_db::DbService;
use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, Executor, MySql};
use tokio::runtime::Handle;

use super::{Parameter, QueryResult};

struct Lease {
    connection: Option<PoolConnection<MySql>>,
    runtime: Handle,
    transaction: bool,
}

impl Drop for Lease {
    fn drop(&mut self) {
        // Closing the socket also rolls back on thread teardown or a Rust
        // unwind. Never let PoolConnection::drop return an unclean session.
        if let Some(connection) = self.connection.take() {
            let _entered = self.runtime.enter();
            drop(connection.detach());
        }
    }
}

thread_local! {
    static LEASES: RefCell<HashMap<String, Lease>> = RefCell::new(HashMap::new());
    static IN_REQUEST: Cell<bool> = const { Cell::new(false) };
    // A lost/aborted transaction must never silently become an autocommit
    // connection. Only explicit rollback/begin or the next work unit clears it.
    static FAILED: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

pub(crate) fn begin_request(runtime: &Handle) {
    end_request(runtime);
    IN_REQUEST.set(true);
}

pub(super) fn disconnect(pool: &str) {
    FAILED.with_borrow_mut(|failed| failed.remove(pool));
    // Lease::drop closes the socket; MySQL rolls back the disconnected session.
    LEASES.with_borrow_mut(|leases| leases.remove(pool));
}

pub(crate) fn end_request(runtime: &Handle) {
    IN_REQUEST.set(false);
    FAILED.with_borrow_mut(HashSet::clear);
    let leases = LEASES.with_borrow_mut(std::mem::take);
    for (pool, mut lease) in leases {
        if lease.transaction {
            let connection = lease.connection.as_mut().expect("live lease");
            if let Err(error) =
                runtime.block_on(connection.execute(AssertSqlSafe("ROLLBACK")))
            {
                tracing::warn!(%pool, %error, "MySQL request rollback failed; closing session");
            }
        }
        // Drop closes even when rollback failed. No implicit commit.
    }
}

fn with_lease<T>(
    service: &DbService,
    runtime: &Handle,
    pool: &str,
    run: impl FnOnce(&mut Lease) -> Result<T, sqlx::Error>,
) -> Result<T, String> {
    if Handle::try_current().is_ok() {
        return Err(
            "synchronous MySQL access must run outside a Tokio runtime thread"
                .into(),
        );
    }
    if FAILED.with_borrow(|failed| failed.contains(pool)) {
        return Err("MySQL session failed; explicitly roll back or begin a new transaction".into());
    }
    let result = LEASES.with(|leases| {
        let mut leases = leases.try_borrow_mut().map_err(|_| {
            "the request's MySQL session is already in use".to_owned()
        })?;
        if !leases.contains_key(pool) {
            let handle = service.mysql_named(pool).ok_or_else(|| {
                format!("MySQL pool is not registered: {pool}")
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
        match run(lease) {
            Ok(value) => Ok(value),
            Err(error) => {
                // Ordinary SQL errors (for example a duplicate key) do not
                // abort a MySQL transaction. A deadlock does; I/O failures
                // leave the outcome unknown. Neither may resume autocommit.
                let aborted = match &error {
                    sqlx::Error::Database(db) => db
                        .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                        .is_some_and(|mysql| mysql.number() == 1213),
                    sqlx::Error::Encode(_)
                    | sqlx::Error::Decode(_)
                    | sqlx::Error::ColumnDecode { .. } => false,
                    _ => true,
                };
                if aborted {
                    leases.remove(pool);
                    FAILED.with_borrow_mut(|failed| {
                        failed.insert(pool.to_owned())
                    });
                }
                Err(super::describe(&error))
            }
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
    statements: &[String],
) -> Result<(), String> {
    FAILED.with_borrow_mut(|failed| failed.remove(pool));
    if in_transaction(pool) {
        return Err(
            "a MySQL transaction is already open on this connection".into()
        );
    }
    with_lease(service, runtime, pool, |lease| {
        runtime.block_on(async {
            for statement in statements {
                lease
                    .connection
                    .as_mut()
                    .expect("live lease")
                    .execute(AssertSqlSafe(statement.as_str()))
                    .await?;
            }
            Ok::<(), sqlx::Error>(())
        })?;
        lease.transaction = true;
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
        return Err("no MySQL transaction is open on this connection".into());
    }
    with_lease(service, runtime, pool, |lease| {
        runtime.block_on(
            lease.connection.as_mut().expect("live lease").execute(
                AssertSqlSafe(if commit { "COMMIT" } else { "ROLLBACK" }),
            ),
        )?;
        lease.transaction = false;
        Ok(())
    })
}

pub(super) fn in_transaction(pool: &str) -> bool {
    FAILED.with_borrow(|failed| failed.contains(pool))
        || LEASES.with_borrow(|leases| {
            leases.get(pool).is_some_and(|lease| lease.transaction)
        })
}
