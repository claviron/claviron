// IMPORTANT — sqlx-mysql + unix socket requires `mysql_native_password`
// on the server side. The default MySQL 8 plugin (`caching_sha2_password`)
// will surface as `Access denied (using password: YES)` with no useful
// hint, even with a correct password. See `ARCHITECTURE.md` §3b before
// debugging any "wrong password" report.

use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{ConnectOptions, MySqlPool};

use crate::config::{ConnectInfo, ConnectionEntry};
use crate::error::{Error, Result};

pub fn build_pool(
    name: &str,
    entry: &ConnectionEntry,
) -> Result<MySqlPool> {
    let opts = if let Some(url) = entry.url.as_ref() {
        MySqlConnectOptions::from_url(url).map_err(|e| {
            Error::Invalid(format!(
                "mysql `{name}` invalid `url:` in db.yaml: {e}"
            ))
        })?
    } else if let Some(info) = entry.connect.as_ref() {
        structured_options(info)
    } else {
        return Err(Error::Invalid(format!(
            "mysql `{name}` needs `url:` or `connect:` block in db.yaml"
        )));
    };

    //
    // server-public-key-inference=true
    let max = entry.pool.pool_size.max(1);
    let min = entry.pool.idle_size.min(max);
    Ok(MySqlPoolOptions::new()
        .min_connections(min)
        .max_connections(max)
        .acquire_timeout(entry.pool.acquire_timeout)
        .idle_timeout(entry.pool.idle_timeout)
        .max_lifetime(entry.pool.max_lifetime)
        .connect_lazy_with(opts))
}

fn structured_options(info: &ConnectInfo) -> MySqlConnectOptions {
    let mut opts = MySqlConnectOptions::new();
    if let Some(socket) = info.socket.as_ref() {
        opts = opts.socket(socket);
    } else {
        if !info.host.is_empty() {
            opts = opts.host(&info.host);
        }
        if info.port != 0 {
            opts = opts.port(info.port);
        }
    }
    if !info.database.is_empty() {
        opts = opts.database(&info.database);
    }
    if !info.username.is_empty() {
        opts = opts.username(&info.username);
    }
    opts = opts.password(&info.password);
    opts
}
