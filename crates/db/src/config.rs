use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use claviron_helpers::de::{
    deserialize_duration, deserialize_optional_duration,
};
use serde::Deserialize;

/// Database composition from `db.yaml` or an enclosing runner's launch options.
///
/// Every section is optional here. What needs a connection says so where it
/// looks for one: hosting asks for the connection `hosting:` names
/// (`postgres.default` when unwritten) and refuses to boot without it.
#[derive(Debug, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct DbFileConfig {
    /// Which connection holds Claviron's hosting data (vhosts, aliases,
    /// certificates), as `<backend>.<name>`: `postgres.default` when absent,
    /// or e.g. `sqlite.hosting`.
    pub hosting: Option<String>,
    #[cfg(feature = "postgres")]
    pub postgres: BackendConnections,
    #[cfg(feature = "mysql")]
    pub mysql: BackendConnections,
    #[cfg(feature = "sqlite")]
    pub sqlite: std::collections::HashMap<String, SqliteEntry>,
}

/// Map of named connections under a single backend (postgres or mysql).
///
/// The literal key `"default"` is conventionally the system default — for
/// postgres it is required at boot, for mysql it is optional.
#[derive(Debug, Deserialize, Default, PartialEq)]
#[serde(transparent)]
pub struct BackendConnections {
    pub entries: HashMap<String, ConnectionEntry>,
}

impl BackendConnections {
    pub fn default_entry(&self) -> Option<&ConnectionEntry> {
        self.entries.get("default")
    }

    pub fn get(
        &self,
        name: &str,
    ) -> Option<&ConnectionEntry> {
        self.entries.get(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// One named connection. Two ways to specify the transport:
///
/// - `url:` — a full connection URL (`postgres://…`, `mysql://…`).
///   When present, structured fields are ignored. Handed straight to
///   `sqlx::ConnectOptions::from_url(...)`.
/// - `connect:` — nested map with structured fields (`host`, `port`,
///   `database`, `username`, `password`, `socket`).
///
/// Two top-level fields rather than `#[serde(flatten)]` because
/// serde-yaml silently drops flattened fields under certain shapes
/// (the rest-map round-trip eats fields like `url:` when the inner
/// struct has `#[serde(default)]`). Keep them explicit.
///
/// `pool:` is always required. claviron does not read process env at
/// runtime; if a value must come from the deploy environment, use
/// `${ENV_KEY}` substitution at the yaml layer: Congif expands it from the
/// application's frozen configuration environment while preparing the file.
#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct ConnectionEntry {
    #[serde(default)]
    pub url: Option<url::Url>,
    #[serde(default)]
    pub connect: Option<ConnectInfo>,
    pub pool: PoolConfig,
}

/// One named SQLite database: a file on this host, opened by every process
/// of the fleet that uses it.
///
/// ```yaml
/// sqlite:
///   hosting:
///     path: /var/lib/portico/hosting.db   # relative paths are cwd-relative
///     busy_timeout: 5s                    # required: how long a writer waits for the lock
///     create: true                        # create the file when missing; default false
///     mmap_size: 16 MiB                   # optional read mapping ceiling; 0 disables it
///     pool: { pool_size: 4, idle_size: 0, acquire_timeout: 10s, idle_timeout: 5m, max_lifetime: 30m }
/// ```
///
/// The journal is always WAL: readers in every process go on while one
/// writes, which is what several workers on one file need; the default
/// rollback journal would block them for every write. SQLite's other
/// settings, `synchronous` among them, are left at SQLite's own defaults
/// unless an explicit connection option overrides them.
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SqliteEntry {
    #[serde(default)]
    pub mode: SqliteMode,
    #[serde(default)]
    pub path: PathBuf,
    /// How long a statement waits for another process's write lock before
    /// it fails with `SQLITE_BUSY`. Writers across processes take turns on
    /// one lock; this is the operator's answer to how long a turn may take.
    #[serde(deserialize_with = "deserialize_duration")]
    pub busy_timeout: Duration,
    /// Create the file when it does not exist. Off, a missing file is an
    /// error at the first connection rather than a new empty database.
    #[serde(default)]
    pub create: bool,
    /// Maximum bytes of the persistent database file to memory-map for reads.
    /// Omission/null preserves SQLite's default; zero disables mapping.
    /// Accepts a byte count or a size such as `16 MiB`. SQLite may cap this
    /// value or disable mapping on platforms that do not support it.
    #[serde(default, deserialize_with = "deserialize_optional_byte_size")]
    pub mmap_size: Option<u64>,
    pub pool: PoolConfig,
}

/// File databases persist across processes; memory databases belong to one pool.
#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SqliteMode {
    #[default]
    File,
    Memory,
}

fn deserialize_optional_byte_size<'de, D>(
    deserializer: D
) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Size {
        Bytes(u64),
        Human(
            #[serde(
                deserialize_with = "claviron_helpers::de::deserialize_byte_size"
            )]
            u64,
        ),
    }

    Ok(Option::<Size>::deserialize(deserializer)?.map(|size| match size {
        Size::Bytes(bytes) | Size::Human(bytes) => bytes,
    }))
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(default)]
pub struct ConnectInfo {
    pub socket: Option<PathBuf>,
    pub host: String,     // default localhost
    pub port: u16,        // default 3306 (mysql) / 5432 (postgres)
    pub database: String, // no default
    pub username: String, // no default
    pub password: String, // default empty
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct PoolConfig {
    pub pool_size: u32,
    pub idle_size: u32,
    #[serde(deserialize_with = "deserialize_duration")]
    pub acquire_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub idle_timeout: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub max_lifetime: Duration,
    /// How long a single statement may run before PostgreSQL cancels it.
    ///
    /// Carried as a connection option rather than issued as a `SET`, which
    /// makes it the session default: it costs no round trip, and it survives
    /// the `RESET ALL` a pooled connection is given when it is handed back.
    ///
    /// Absent leaves the server's own setting alone. Set it wherever a pool
    /// serves something that must not be held up by one query — a request hot
    /// path above all, where a runaway statement otherwise occupies a worker
    /// until it finishes.
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub statement_timeout: Option<Duration>,
}
