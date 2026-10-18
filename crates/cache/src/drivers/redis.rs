use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron_helpers::de::{
    deserialize_duration, deserialize_optional_duration,
};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{Component, Reloadable, Runnable, priority};
use claviron_runtime::{AppState, Register, RunContext};
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};
use tracing::error;
use url::Url;

use crate::config::CacheStoreConfig;
use crate::{CacheDriver, CacheStore, Error};

/// The owner of `redis.yaml`: named connections, opened on first use from
/// the configuration Congif prepared. An absent file declares none; a
/// changed one drops the cached clients so the next use connects as declared.
pub struct RedisProvider {
    config_path: PathBuf,
    config: ArcSwap<RedisConfig>,
    clients: RwLock<HashMap<String, redis::Client>>,
    managers: RwLock<HashMap<String, redis::aio::ConnectionManager>>,
    connect_lock: Mutex<()>,
}

pub(crate) fn register(
    state: &AppState
) -> claviron_runtime::registry::Result<Arc<RedisProvider>> {
    if let Some(provider) = state.registry_ref().resolve::<RedisProvider>() {
        return Ok(provider);
    }
    state.bind_config_environment::<RedisProvider>()?;
    let provider =
        Arc::new(RedisProvider::new(state.config_dir().join("redis.yaml")));
    state.registry_ref().register(provider.clone())?;
    Ok(provider)
}

impl Register<AppState> for RedisProvider {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        register(state).map(|_| ())
    }
}

impl RedisProvider {
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            config_path,
            config: ArcSwap::from_pointee(RedisConfig::default()),
            clients: RwLock::new(HashMap::new()),
            managers: RwLock::new(HashMap::new()),
            connect_lock: Mutex::new(()),
        }
    }

    /// A provider with its connections in hand, for hosts and tests that own
    /// the configuration themselves.
    pub fn with_config(config: RedisConfig) -> Self {
        let provider = Self::new(PathBuf::new());
        provider.config.store(Arc::new(config));
        provider
    }

    async fn apply(
        &self,
        config: &RedisConfig,
    ) {
        let _guard = self.connect_lock.lock().await;
        self.config.store(Arc::new(config.clone()));
        self.clients.write().await.clear();
        self.managers.write().await.clear();
    }

    async fn build_client(
        name: &str,
        connection: &RedisConnConfig,
    ) -> crate::Result<redis::Client> {
        #[cfg(claviron_redis_tls_rustls_legacy)]
        if let Some(tls) = &connection.tls {
            return Self::build_tls_client(name, &connection.url, tls).await;
        }

        #[cfg(not(claviron_redis_tls_rustls_legacy))]
        if connection.tls.is_some()
            || matches!(connection.url.scheme(), "rediss" | "valkeys")
        {
            return Err(Error::Rejected(format!(
                "redis connection '{name}' requests TLS, but Redis TLS is disabled in this build"
            )));
        }

        redis::Client::open(connection.url.clone()).map_err(|err| {
            Error::wrap(format!("invalid redis connection '{name}'"), err)
        })
    }

    #[cfg(claviron_redis_tls_rustls_legacy)]
    async fn build_tls_client(
        name: &str,
        url: &url::Url,
        tls: &RedisTlsConfig,
    ) -> crate::Result<redis::Client> {
        let url = Self::tls_url(url)?;
        let root_cert =
            Self::read_tls_file(name, "ca certificate", &tls.ca_path).await?;
        let client_cert =
            Self::read_tls_file(name, "client certificate", &tls.cert_path)
                .await?;
        let client_key =
            Self::read_tls_file(name, "client key", &tls.key_path).await?;

        redis::Client::build_with_tls(
            url,
            redis::TlsCertificates {
                client_tls: Some(redis::ClientTlsConfig {
                    client_cert,
                    client_key,
                }),
                root_cert: Some(root_cert),
            },
        )
        .map_err(|err| {
            Error::wrap(format!("invalid redis tls connection '{name}'"), err)
        })
    }

    #[cfg(claviron_redis_tls_rustls_legacy)]
    fn tls_url(url: &url::Url) -> crate::Result<url::Url> {
        let mut tls_url = url.clone();

        match tls_url.scheme() {
            "redis" => {
                let _ = tls_url.set_scheme("rediss");
            }
            "rediss" => {}
            "valkey" => {
                let _ = tls_url.set_scheme("valkeys");
            }
            "valkeys" => {}
            other => {
                return Err(Error::Rejected(format!(
                    "redis tls requires a tcp url, unsupported scheme '{other}'"
                )));
            }
        }

        Ok(tls_url)
    }

    #[cfg(claviron_redis_tls_rustls_legacy)]
    async fn read_tls_file(
        name: &str,
        label: &str,
        path: &std::path::Path,
    ) -> crate::Result<Vec<u8>> {
        tokio::fs::read(path).await.map_err(|err| {
            Error::wrap(
                format!(
                    "redis tls {label} read failed for connection '{name}' at {}",
                    path.display()
                ),
                err
            )
        })
    }

    pub fn has_connection(
        &self,
        name: &str,
    ) -> bool {
        self.config.load().connections.contains_key(name)
    }

    async fn client(
        &self,
        name: &str,
        connection: &RedisConnConfig,
    ) -> crate::Result<redis::Client> {
        if let Some(client) = self.clients.read().await.get(name).cloned() {
            return Ok(client);
        }

        let client = Self::build_client(name, connection).await?;
        self.clients.write().await.insert(name.to_string(), client.clone());
        Ok(client)
    }

    pub fn connection_names(&self) -> Vec<String> {
        self.config.load().connections.keys().cloned().collect()
    }

    pub async fn connection_manager(
        &self,
        name: &str,
    ) -> crate::Result<redis::aio::ConnectionManager> {
        if let Some(manager) = self.managers.read().await.get(name).cloned() {
            return Ok(manager);
        }

        let _guard = self.connect_lock.lock().await;

        if let Some(manager) = self.managers.read().await.get(name).cloned() {
            return Ok(manager);
        }

        let config = self.config.load_full();
        let connection = config.connections.get(name).ok_or_else(|| {
            Error::Missing(format!(
                "redis connection '{name}' is not configured"
            ))
        })?;
        let client = self.client(name, connection).await?;

        let manager = client
            .get_connection_manager_with_config(
                redis::aio::ConnectionManagerConfig::new()
                    .set_exponent_base(connection.exponent_base)
                    .set_number_of_retries(connection.number_of_retries)
                    .set_min_delay(connection.min_delay)
                    .set_max_delay(connection.max_delay)
                    .set_connection_timeout(connection.connect_timeout)
                    .set_response_timeout(connection.response_timeout),
            )
            .await
            .map_err(Error::from)?;

        self.managers.write().await.insert(name.to_string(), manager.clone());
        Ok(manager)
    }

    pub async fn warmup_all(&self) {
        for name in self.connection_names() {
            if let Err(err) = self.connection_manager(&name).await {
                error!(
                    connection = %name,
                    error = %err,
                    "redis connection manager warmup failed; continuing in degraded mode"
                );
            }
        }
    }
}

#[derive(Clone)]
pub struct RedisCacheStore {
    store_name: String,
    connection: String,
    prefix: Option<String>,
    driver: Arc<RedisProvider>,
}

impl RedisCacheStore {
    pub fn new(
        store_name: impl Into<String>,
        connection: impl Into<String>,
        prefix: Option<String>,
        driver: Arc<RedisProvider>,
    ) -> Self {
        Self {
            store_name: store_name.into(),
            connection: connection.into(),
            prefix,
            driver,
        }
    }

    pub fn connection_name(&self) -> &str {
        &self.connection
    }

    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    fn scoped_key(
        &self,
        key: &str,
    ) -> String {
        match self.prefix() {
            Some(prefix) => format!("{prefix}{key}"),
            None => key.to_string(),
        }
    }

    pub async fn connection_manager(
        &self
    ) -> crate::Result<redis::aio::ConnectionManager> {
        self.driver.connection_manager(&self.connection).await
    }
}

#[async_trait]
impl CacheStore for RedisCacheStore {
    fn store_name(&self) -> &str {
        &self.store_name
    }

    fn driver_name(&self) -> &str {
        "redis"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn supports_ttl(&self) -> bool {
        true
    }

    async fn get(
        &self,
        key: &str,
    ) -> crate::Result<Option<bytes::Bytes>> {
        let mut manager = self.connection_manager().await?;
        redis::cmd("GET")
            .arg(self.scoped_key(key))
            .query_async::<Option<Vec<u8>>>(&mut manager)
            .await
            .map(|value| value.map(bytes::Bytes::from))
            .map_err(Error::from)
    }

    async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> crate::Result<()> {
        let mut manager = self.connection_manager().await?;
        let scoped_key = self.scoped_key(key);
        let mut cmd = redis::cmd("SET");
        cmd.arg(&scoped_key).arg(value);

        if let Some(ttl) = ttl {
            let ttl_ms =
                ttl.as_millis().max(1).min(u128::from(u64::MAX)) as u64;
            cmd.arg("PX").arg(ttl_ms);
        }

        cmd.query_async::<()>(&mut manager).await.map_err(Error::from)
    }

    async fn forget(
        &self,
        key: &str,
    ) -> crate::Result<()> {
        let mut manager = self.connection_manager().await?;
        redis::cmd("DEL")
            .arg(self.scoped_key(key))
            .query_async::<()>(&mut manager)
            .await
            .map_err(Error::from)
    }

    async fn clear(&self) -> crate::Result<()> {
        let prefix = self.prefix().filter(|prefix| !prefix.is_empty()).ok_or_else(|| {
            Error::Rejected(format!(
                "redis cache store '{}' requires a non-empty prefix for namespace clear",
                self.store_name
            ))
        })?;
        let prefix_bytes = prefix.as_bytes();
        let pattern = redis_prefix_pattern(prefix);
        let mut manager = self.connection_manager().await?;
        let mut cursor = 0u64;

        loop {
            let (next, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(1_000)
                .query_async(&mut manager)
                .await
                .map_err(Error::from)?;

            let keys = keys
                .into_iter()
                .filter(|key| key.starts_with(prefix_bytes))
                .collect::<Vec<_>>();
            if !keys.is_empty() {
                let mut unlink = redis::cmd("UNLINK");
                for key in keys {
                    unlink.arg(key);
                }
                unlink
                    .query_async::<u64>(&mut manager)
                    .await
                    .map_err(Error::from)?;
            }

            cursor = next;
            if cursor == 0 {
                return Ok(());
            }
        }
    }
}

fn redis_prefix_pattern(prefix: &str) -> String {
    let mut pattern = String::with_capacity(prefix.len() + 1);
    for character in prefix.chars() {
        if matches!(character, '*' | '?' | '[' | ']' | '\\') {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    pattern.push('*');
    pattern
}

pub struct RedisCacheDriver {
    provider: Arc<RedisProvider>,
}

impl RedisCacheDriver {
    pub fn new(provider: Arc<RedisProvider>) -> Self {
        Self { provider }
    }
}

impl CacheDriver for RedisCacheDriver {
    fn driver_name(&self) -> &'static str {
        "redis"
    }

    fn create_store(
        self: Arc<Self>,
        store_name: &str,
        store: &CacheStoreConfig,
    ) -> crate::Result<Arc<dyn CacheStore>> {
        let config: RedisCacheStoreConfig = store.deserialize_options()?;

        if !self.provider.has_connection(&config.connection) {
            return Err(Error::Missing(format!(
                "redis store '{store_name}' references unknown connection '{}'",
                config.connection
            )));
        }

        Ok(Arc::new(RedisCacheStore::new(
            store_name,
            config.connection,
            config.prefix,
            self.provider.clone(),
        )))
    }
}

impl Component<AppState> for RedisProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "RedisCacheProvider"
    }

    /// Connections open early: a store that uses one finds it warm.
    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

/// Connecting is owned work, not boot: the declared connections are warmed
/// when the process runs, and a failure leaves that connection to its first
/// use. The clients are dropped with the process.
#[async_trait]
impl Runnable<AppState> for RedisProvider {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        _state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        self.warmup_all().await;
        context.cancelled().await;
        self.clients.write().await.clear();
        self.managers.write().await.clear();
        Ok(())
    }
}

/// A reload takes the declaration Congif prepared for the batch; the next
/// use connects as declared.
#[async_trait]
impl Reloadable<AppState> for RedisProvider {
    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        self.apply(&config).await;
        Ok(())
    }
}

#[derive(Clone, Deserialize)]
pub struct RedisCacheStoreConfig {
    pub connection: String,
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Deserialize, Clone, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    #[serde(default)]
    pub connections: HashMap<String, RedisConnConfig>,
}

#[async_trait]
impl Configurable for RedisProvider {
    type Config = RedisConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    /// No file, no connections.
    fn absent_source(&self) -> Option<RedisConfig> {
        Some(RedisConfig::default())
    }

    async fn configure(
        &self,
        config: &RedisConfig,
    ) -> congif::Result<()> {
        self.apply(config).await;
        Ok(())
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for RedisProvider {
    async fn config_changed(
        &self,
        config: &RedisConfig,
    ) -> congif::Result<()> {
        self.apply(config).await;
        Ok(())
    }
}

#[derive(Deserialize, Clone, PartialEq)]
pub struct RedisConnConfig {
    pub url: Url,
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub connect_timeout: Option<Duration>,
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub response_timeout: Option<Duration>,
    #[serde(default = "default_redis_retry_exponent_base")]
    pub exponent_base: f32,
    #[serde(default = "default_redis_number_of_retries")]
    pub number_of_retries: usize,
    #[serde(deserialize_with = "deserialize_duration")]
    pub min_delay: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub max_delay: Duration,
    #[serde(default)]
    pub tls: Option<RedisTlsConfig>,
}

#[derive(Deserialize, Clone, PartialEq)]
pub struct RedisTlsConfig {
    pub ca_path: PathBuf,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

fn default_redis_retry_exponent_base() -> f32 {
    2.0
}

fn default_redis_number_of_retries() -> usize {
    6
}

#[cfg(test)]
#[path = "../../tests/drivers/redis.rs"]
mod tests;
