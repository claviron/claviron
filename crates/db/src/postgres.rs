use std::sync::Arc;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool, Pool, Postgres};

use crate::config::{ConnectInfo, ConnectionEntry, PoolConfig};
use crate::error::{Error, Result};

/// Called each time the pool opens a connection to the server — at start,
/// and again after the server comes back. What it says is the pool's own
/// observation that the database answers.
pub type ConnectHook = Arc<dyn Fn() + Send + Sync>;

pub fn build_pool(
    name: &str,
    entry: &ConnectionEntry,
) -> Result<PgPool> {
    build_pool_notifying(name, entry, None)
}

/// [`build_pool`], calling `on_connect` for every connection the pool opens.
pub fn build_pool_notifying(
    name: &str,
    entry: &ConnectionEntry,
    on_connect: Option<ConnectHook>,
) -> Result<PgPool> {
    let opts = if let Some(url) = entry.url.as_ref() {
        PgConnectOptions::from_url(url).map_err(|e| {
            Error::Invalid(format!(
                "postgres `{name}` invalid `url:` in db.yaml: {e}"
            ))
        })?
    } else if let Some(info) = entry.connect.as_ref() {
        structured_options(info)
    } else {
        return Err(Error::Invalid(format!(
            "postgres `{name}` needs `url:` or `connect:` block in db.yaml"
        )));
    };
    Ok(apply_pool_options(
        apply_statement_timeout(opts, &entry.pool),
        &entry.pool,
        on_connect,
    ))
}

/// Appends `statement_timeout` to the connection options, so every connection
/// this pool opens starts with it and keeps it across `RESET ALL`.
fn apply_statement_timeout(
    opts: PgConnectOptions,
    pool: &PoolConfig,
) -> PgConnectOptions {
    match pool.statement_timeout {
        Some(timeout) => opts
            .options([("statement_timeout", timeout.as_millis().to_string())]),
        None => opts,
    }
}

fn structured_options(info: &ConnectInfo) -> PgConnectOptions {
    let mut opts = PgConnectOptions::new();
    if let Some(socket) = info.socket.as_ref() {
        opts = opts.host(&socket.to_string_lossy());
    } else if !info.host.is_empty() {
        opts = opts.host(&info.host);
    }
    if info.port != 0 {
        opts = opts.port(info.port);
    }
    if !info.database.is_empty() {
        opts = opts.database(&info.database);
    }
    if !info.username.is_empty() {
        opts = opts.username(&info.username);
    }
    if !info.password.is_empty() {
        opts = opts.password(&info.password);
    }
    opts
}

fn apply_pool_options(
    opts: PgConnectOptions,
    pool: &PoolConfig,
    on_connect: Option<ConnectHook>,
) -> Pool<Postgres> {
    let max = pool.pool_size.max(1);
    let min = pool.idle_size.min(max);
    let mut options = PgPoolOptions::new()
        .min_connections(min)
        .max_connections(max)
        .acquire_timeout(pool.acquire_timeout)
        .idle_timeout(pool.idle_timeout)
        .max_lifetime(pool.max_lifetime);
    if let Some(on_connect) = on_connect {
        options = options.after_connect(move |_, _| {
            on_connect();
            Box::pin(async { Ok(()) })
        });
    }
    options.connect_lazy_with(opts)
}
