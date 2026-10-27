use std::sync::Arc;

use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions,
};
use sqlx::{ConnectOptions, Connection, SqlitePool};

use crate::config::{SqliteEntry, SqliteMode};
use crate::error::{Error, Result};

/// `checking`: a configuration check (`ConfigCheck`) opens the file read-only
/// with SQLite's ordinary locking and WAL visibility. SQLite may manage WAL
/// index coordination; the check does not write application data or force a
/// checkpoint. A live source must not be declared immutable.
pub fn build_pool(
    name: &str,
    entry: &SqliteEntry,
    checking: bool,
) -> Result<SqlitePool> {
    // Refused rather than ignored: SQLite has no statement timeout, and a
    // bound the operator wrote must not silently mean nothing.
    if entry.pool.statement_timeout.is_some() {
        return Err(Error::Invalid(format!(
            "sqlite `{name}`: statement_timeout is PostgreSQL's; SQLite has none"
        )));
    }
    let memory = entry.mode == SqliteMode::Memory;
    let options = if memory {
        if !entry.path.as_os_str().is_empty() || entry.create || entry.mmap_size.is_some() {
            return Err(Error::Invalid(format!(
                "sqlite `{name}`: memory mode does not accept path, create or mmap_size"
            )));
        }
        // SQLx allocates a unique shared-memory URI for each options instance.
        "sqlite::memory:".parse::<SqliteConnectOptions>()?
    } else {
        if entry.path.as_os_str().is_empty() {
            return Err(Error::Invalid(format!("sqlite `{name}`: file mode requires path")));
        }
        SqliteConnectOptions::new().filename(&entry.path)
    }
        .busy_timeout(entry.busy_timeout)
        .foreign_keys(true);
    let options = if memory {
        options.journal_mode(SqliteJournalMode::Memory)
    } else if checking {
        options.read_only(true)
    } else {
        options
            .create_if_missing(entry.create)
            .journal_mode(SqliteJournalMode::Wal)
    };
    let options = if let Some(bytes) = entry.mmap_size {
        let bytes = i64::try_from(bytes).map_err(|_| {
            Error::Invalid(format!(
                "sqlite `{name}`: mmap_size exceeds SQLite's signed 64-bit byte limit"
            ))
        })?;
        options.pragma("mmap_size", bytes.to_string())
    } else {
        options
    };
    let max = entry.pool.pool_size.max(1);
    let min = entry.pool.idle_size.min(max);
    let mut pool_options = SqlitePoolOptions::new()
        .min_connections(min)
        .max_connections(max)
        .acquire_timeout(entry.pool.acquire_timeout)
        .idle_timeout(entry.pool.idle_timeout)
        .max_lifetime(entry.pool.max_lifetime);
    // A private anchor keeps the database alive across idle/lifetime recycling,
    // without changing the configured query connection capacity or durations.
    let anchor = Arc::new(tokio::sync::Mutex::new(None));
    if memory {
        let anchor = Arc::clone(&anchor);
        let options = options.clone();
        pool_options = pool_options.after_connect(move |_, _| {
            let anchor = Arc::clone(&anchor);
            let options = options.clone();
            Box::pin(async move {
                let mut keeper = anchor.lock().await;
                if keeper.is_none() {
                    *keeper = Some(options.connect().await?);
                }
                Ok(())
            })
        });
    }
    let pool = pool_options.connect_lazy_with(options);
    if memory {
        let closed = pool.close_event();
        tokio::spawn(async move {
            closed.await;
            if let Some(connection) = anchor.lock().await.take() {
                let _ = connection.close().await;
            }
        });
    }
    Ok(pool)
}

#[cfg(test)]
#[path = "../tests/sqlite.rs"]
mod tests;
