use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "postgres")]
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron_runtime::AppState;
use claviron_runtime::Register;
use claviron_runtime::congif::{self, ConfigFile, Configurable};
use claviron_runtime::registry::{Bootable, Component, Reloadable, priority};
#[cfg(any(feature = "postgres", feature = "mysql"))]
use sqlx::Connection;
#[cfg(feature = "mysql")]
use sqlx::mysql::MySqlConnectOptions;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgConnectOptions;
#[cfg(feature = "mysql")]
use sqlx::{MySqlConnection, MySqlPool};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, PgPool};
use tracing::info;

use crate::config::DbFileConfig;
use crate::error::Result;

/// Live database state — atomically swappable on reload.
///
/// Each backend keeps a map of named connections (`default`, `analytics`, ...).
/// `postgres.default` is the system default; downstream callers reach it via
/// `DbService::postgres()`.
#[derive(Default)]
pub struct DbState {
    #[cfg(feature = "postgres")]
    pub postgres: HashMap<String, PgPool>,
    /// The configured statement budget per postgres pool. PostgreSQL enforces
    /// it on its own; this is here for callers that block a thread on a query
    /// and need to know how long that is allowed to take.
    #[cfg(feature = "postgres")]
    pub postgres_statement_timeout: HashMap<String, Duration>,
    #[cfg(feature = "mysql")]
    pub mysql: HashMap<String, MySqlPool>,
    #[cfg(feature = "sqlite")]
    pub sqlite: HashMap<String, sqlx::SqlitePool>,
    /// `db.yaml`'s `hosting:` profile, `postgres.default` when unwritten.
    pub hosting: String,
}

/// One named connection's pool, whichever backend it is on.
#[derive(Clone)]
pub enum Pool {
    #[cfg(feature = "postgres")]
    Postgres(PgPool),
    #[cfg(feature = "mysql")]
    MySql(MySqlPool),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
}

/// Multi-backend database provider.
///
/// `register()` is sync and only stores the config path; Congif prepares
/// `db.yaml` before boot, and pool construction happens in async `boot()`
/// from that prepared snapshot. [`Self::from_config`] instead accepts the same
/// schema as immutable boot options, without a file or configuration watcher.
/// File-backed reload takes the centrally prepared snapshot and hot-swaps the
/// entire `DbState` via `ArcSwap`.
pub struct DbService {
    config_path: PathBuf,
    initial_config: Option<DbFileConfig>,
    state: ArcSwap<DbState>,
}

impl DbService {
    pub fn empty(config_path: PathBuf) -> Self {
        Self {
            config_path,
            initial_config: None,
            state: ArcSwap::from_pointee(DbState::default()),
        }
    }

    /// Supply typed boot options from code or an enclosing launch document.
    /// Pools are constructed during normal boot, not in this constructor.
    /// Changing these options requires a process restart.
    pub fn from_config(config: DbFileConfig) -> Self {
        Self { initial_config: Some(config), ..Self::empty(PathBuf::new()) }
    }

    /// Default postgres pool (`postgres.default`). `None` until `boot()` runs.
    #[cfg(feature = "postgres")]
    pub fn postgres(&self) -> Option<PgPool> {
        self.postgres_named("default")
    }

    /// Named postgres pool.
    #[cfg(feature = "postgres")]
    pub fn postgres_named(
        &self,
        name: &str,
    ) -> Option<PgPool> {
        self.state.load().postgres.get(name).cloned()
    }

    /// How long a statement on this pool may run, when the pool sets a bound.
    #[cfg(feature = "postgres")]
    pub fn postgres_statement_timeout(
        &self,
        name: &str,
    ) -> Option<Duration> {
        self.state.load().postgres_statement_timeout.get(name).copied()
    }

    /// Default postgres connect options (`postgres.default`). Internal-use
    /// accessor — meant for callers that need to open their own dedicated
    /// connection (LISTEN/NOTIFY listeners, long-running cursors, etc.)
    /// without burning a pool slot.
    #[cfg(feature = "postgres")]
    pub fn postgres_options(&self) -> Option<Arc<PgConnectOptions>> {
        self.postgres_named_options("default")
    }

    /// Named postgres connect options. Same internal-use semantics as
    /// `postgres_options()`.
    #[cfg(feature = "postgres")]
    pub fn postgres_named_options(
        &self,
        name: &str,
    ) -> Option<Arc<PgConnectOptions>> {
        self.state.load().postgres.get(name).map(|p| p.connect_options())
    }

    /// Open a fresh dedicated postgres connection (`postgres.default`) using
    /// the pool's connect options. Internal-use accessor — for raw
    /// connections that live outside the pool. Returns `None` when the named
    /// pool isn't registered, `Some(Err(_))` when the dial itself fails.
    #[cfg(feature = "postgres")]
    pub async fn postgres_connection(
        &self
    ) -> Option<sqlx::Result<PgConnection>> {
        self.postgres_named_connection("default").await
    }

    /// Open a fresh dedicated connection for a named postgres pool.
    #[cfg(feature = "postgres")]
    pub async fn postgres_named_connection(
        &self,
        name: &str,
    ) -> Option<sqlx::Result<PgConnection>> {
        let opts = self.postgres_named_options(name)?;
        Some(PgConnection::connect_with(&opts).await)
    }

    // -----------------------------------------------------------------
    // Claviron-system shortcuts.
    //
    // Semantic alias of `postgres.default` for claviron-internal callers
    // (vhost / certificate / server lookups, alias listener, hosting
    // service). Reads as system-level intent at call sites instead of
    // "just the default app pool". If/when we ever want a real separation
    // between claviron's pool and the app's default pool, only these
    // accessors need to switch — call sites stay unchanged.
    // -----------------------------------------------------------------

    /// Pool used by claviron's own internal queries (alias of
    /// `postgres.default`).
    #[cfg(feature = "postgres")]
    pub fn claviron_pool(&self) -> Option<PgPool> {
        self.postgres()
    }

    /// Connect options for claviron's own connection (alias of
    /// `postgres_options()`).
    #[cfg(feature = "postgres")]
    pub fn claviron_options(&self) -> Option<Arc<PgConnectOptions>> {
        self.postgres_options()
    }

    /// Open a fresh dedicated connection for claviron's internal use
    /// (alias of `postgres_connection()`).
    #[cfg(feature = "postgres")]
    pub async fn claviron_connection(
        &self
    ) -> Option<sqlx::Result<PgConnection>> {
        self.postgres_connection().await
    }

    #[cfg(feature = "mysql")]
    pub fn mysql(&self) -> Option<MySqlPool> {
        self.mysql_named("default")
    }

    #[cfg(feature = "mysql")]
    pub fn mysql_named(
        &self,
        name: &str,
    ) -> Option<MySqlPool> {
        self.state.load().mysql.get(name).cloned()
    }

    /// The pool a `<backend>.<name>` profile names, as `db.yaml` writes one
    /// (`postgres.default`, `sqlite.hosting`). An error says which part is
    /// missing: a backend this build lacks, or a name the file lacks.
    pub fn profile(
        &self,
        profile: &str,
    ) -> std::result::Result<Pool, String> {
        let (backend, name) = profile.split_once('.').ok_or_else(|| {
            format!("`{profile}` is not <backend>.<name>, as postgres.default")
        })?;
        let missing = || format!("db.yaml declares no `{profile}`");
        match backend {
            #[cfg(feature = "postgres")]
            "postgres" => self
                .postgres_named(name)
                .map(Pool::Postgres)
                .ok_or_else(missing),
            #[cfg(feature = "mysql")]
            "mysql" => {
                self.mysql_named(name).map(Pool::MySql).ok_or_else(missing)
            }
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                self.sqlite_named(name).map(Pool::Sqlite).ok_or_else(missing)
            }
            other => {
                Err(format!("`{profile}`: this build has no {other} backend"))
            }
        }
    }

    /// Where Claviron's hosting data is: `db.yaml`'s `hosting:`, or
    /// `postgres.default` when it is not written.
    pub fn hosting_profile(&self) -> String {
        self.state.load().hosting.clone()
    }

    /// Named SQLite pool.
    #[cfg(feature = "sqlite")]
    pub fn sqlite_named(
        &self,
        name: &str,
    ) -> Option<sqlx::SqlitePool> {
        self.state.load().sqlite.get(name).cloned()
    }

    #[cfg(feature = "mysql")]
    pub fn mysql_options(&self) -> Option<Arc<MySqlConnectOptions>> {
        self.mysql_named_options("default")
    }

    #[cfg(feature = "mysql")]
    pub fn mysql_named_options(
        &self,
        name: &str,
    ) -> Option<Arc<MySqlConnectOptions>> {
        self.state.load().mysql.get(name).map(|p| p.connect_options())
    }

    #[cfg(feature = "mysql")]
    pub async fn mysql_connection(
        &self
    ) -> Option<sqlx::Result<MySqlConnection>> {
        self.mysql_named_connection("default").await
    }

    #[cfg(feature = "mysql")]
    pub async fn mysql_named_connection(
        &self,
        name: &str,
    ) -> Option<sqlx::Result<MySqlConnection>> {
        let opts = self.mysql_named_options(name)?;
        Some(MySqlConnection::connect_with(&opts).await)
    }

    /// Build from supplied options or the prepared file snapshot. Nothing
    /// here opens a configuration file.
    async fn rebuild(
        &self,
        state: &AppState,
    ) -> Result<()> {
        let new_state = if let Some(config) = &self.initial_config {
            build_state(config, state)?
        } else {
            let config = state.config_reader().snapshot::<Self>()?;
            build_state(&config, state)?
        };
        self.state.store(Arc::new(new_state));
        Ok(())
    }

    /// Publishes the pools. Called from [`Bootable::boot`]; kept as its own
    /// method so the work has a name at the call site.
    async fn boot_pools(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.rebuild(state).await.map_err(|e| {
            claviron_runtime::registry::Error::msg(format!(
                "DbService boot: {e}"
            ))
        })?;
        Ok(())
    }
}

impl Component<AppState> for DbService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "DbService"
    }

    /// The same reservation `Reloadable::priority` makes below, for the same
    /// reason: many providers touch the database while they boot, so the pool
    /// has to exist before any of them run.
    ///
    /// This used to be a `pre_boot` hand-called from the server bootstrap
    /// ahead of `boot_all`, with a comment saying priority could not express
    /// the chain. It can — `boot_all` orders on `boot_priority` and honours
    /// `ComponentOrder`, which is how `HostingService` now says it comes after
    /// this one.
    fn boot_priority(&self) -> Option<u8> {
        Some(priority::FIRST)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        self.initial_config.is_none().then_some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        self.initial_config.is_none().then_some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState> for DbService {
    async fn boot(
        &self,
        _deps: (),
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.boot_pools(state).await
    }
}

/// `db.yaml` is required where the `db` feature is on: there is no absent
/// form, and no source watch — pools are rebuilt on an explicit reload, not on
/// every save of the file.
#[async_trait]
impl Configurable for DbService {
    type Config = DbFileConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    async fn configure(
        &self,
        _config: &DbFileConfig,
    ) -> congif::Result<()> {
        // Pools open in boot, in the order boot_priority reserves for them.
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for DbService {
    /// `priority::FIRST` is reserved for DbService — every other
    /// `Reloadable` provider must be at `EARLY` (50) or later.
    /// Reasoning: many providers (VhostResolver, AuthService, …)
    /// touch DB at reload-build time; the pool MUST be valid before
    /// they run.
    fn priority(&self) -> Option<u8> {
        Some(priority::FIRST)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.rebuild(state).await.map_err(|e| {
            claviron_runtime::registry::Error::msg(format!(
                "DbService reload: {e}"
            ))
        })?;
        Ok(())
    }
}

/// A Postgres pool opened a connection to its server: at start, and each time
/// the server comes back after being unreachable. A process-local event;
/// several arrive when a pool opens several connections, so a consumer acts
/// on it only when it has something to catch up on.
#[derive(Clone, Debug)]
#[cfg(feature = "postgres")]
pub struct PostgresConnected {
    /// The `db.yaml` entry, e.g. `default`.
    pub entry: String,
}

#[cfg(feature = "postgres")]
impl claviron_runtime::events::Event for PostgresConnected {}

/// Locate `db.yaml` inside the runtime config directory.
fn db_config_path(state: &AppState) -> PathBuf {
    state.config_dir().join("db.yaml")
}

impl Register<AppState> for DbService {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        // An enclosing runner may have supplied its own typed boot options
        // before the server's built-in registrations.
        if state.registry_ref().resolve::<Self>().is_some() {
            return Ok(());
        }
        state.bind_config_environment::<Self>()?;
        state
            .registry_ref()
            .register(Arc::new(Self::empty(db_config_path(state))))?;
        Ok(())
    }
}

pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    <DbService as Register<AppState>>::register(state)
}

/// Register the existing DB schema as immutable process-local boot options.
/// No `db.yaml` is read; pool creation remains normal lifecycle work.
pub fn register_with_config(
    state: &AppState,
    config: DbFileConfig,
) -> claviron_runtime::registry::Result<()> {
    state.registry_ref().register(Arc::new(DbService::from_config(config)))?;
    Ok(())
}

fn build_state(
    cfg: &DbFileConfig,
    #[cfg_attr(
        not(any(feature = "postgres", feature = "sqlite")),
        allow(unused_variables)
    )]
    app: &AppState,
) -> Result<DbState> {
    let mut state = DbState::default();

    #[cfg(feature = "postgres")]
    for (name, entry) in &cfg.postgres.entries {
        // Each connection the pool opens is announced in this process, so a
        // listener that lost the database learns it is back without polling.
        let bus = app.events().bus().clone();
        let entry_name = name.clone();
        let on_connect: crate::postgres::ConnectHook = Arc::new(move || {
            bus.emit(PostgresConnected { entry: entry_name.clone() });
        });
        state.postgres.insert(
            name.clone(),
            crate::postgres::build_pool_notifying(
                name,
                entry,
                Some(on_connect),
            )?,
        );
        if let Some(timeout) = entry.pool.statement_timeout {
            state.postgres_statement_timeout.insert(name.clone(), timeout);
        }
    }
    #[cfg(feature = "mysql")]
    for (name, entry) in &cfg.mysql.entries {
        state
            .mysql
            .insert(name.clone(), crate::mysql::build_pool(name, entry)?);
    }

    #[cfg(feature = "sqlite")]
    {
        let checking = app
            .registry_ref()
            .resolve::<claviron_runtime::ConfigCheck>()
            .is_some();
        for (name, entry) in &cfg.sqlite {
            state.sqlite.insert(
                name.clone(),
                crate::sqlite::build_pool(name, entry, checking)?,
            );
        }
    }

    state.hosting = cfg.hosting.clone().unwrap_or_else(|| {
        if cfg!(feature = "postgres") {
            "postgres.default".to_owned()
        } else {
            "sqlite.default".to_owned()
        }
    });

    if app.is_worker() {
        tracing::debug!("DbService state built");
    } else {
        info!("DbService state built");
    }
    Ok(state)
}

#[cfg(test)]
#[path = "../tests/provider.rs"]
mod tests;
