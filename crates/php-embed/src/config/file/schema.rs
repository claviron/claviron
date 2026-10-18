//! Desired configuration, independent of live threads and service generations.
//!
//! Parsing and resolution perform no environment-file I/O. Source paths and
//! inline overrides remain separate. Only `ResolvedConfig::build` allocates the runtime; registration
//! remains the publication boundary. No section currently implements reload.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use claviron_helpers::de::{deserialize_optional_duration, one_or_many};
use claviron_helpers::yaml::expand_env_with;
use claviron_runtime::registry::{Error, Result};
use serde::{Deserialize, Deserializer};

use crate::{ApplicationPool, PhpApplication, PhpEmbedRuntime, RequestPool};

pub(super) fn parse(
    raw: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<PhpConfig> {
    let yaml = expand_env_with(raw, lookup).map_err(|error| {
        Error::msg(format!("php.yaml environment: {error}"))
    })?;
    serde_yaml::from_str(&yaml)
        .map_err(|error| Error::msg(format!("php.yaml: {error}")))
}

/// Adapt a single named application's document to the one embed schema.
/// Environment expansion precedes parsing, as it does for `php.yaml`.
pub(super) fn parse_application(
    raw: &str,
    name: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<PhpConfig> {
    let yaml = expand_env_with(raw, lookup).map_err(|error| {
        Error::msg(format!("PHP configuration environment: {error}"))
    })?;
    let mut document: serde_yaml::Mapping = serde_yaml::from_str(&yaml)
        .map_err(|error| Error::msg(error.to_string()))?;
    if document.contains_key("applications") {
        return Err(Error::msg(
            "single-application configuration uses application, not applications",
        ));
    }
    let application = document.remove("application").ok_or_else(|| {
        Error::msg("PHP configuration is missing application")
    })?;
    let mut applications = serde_yaml::Mapping::new();
    applications.insert(name.into(), application);
    document.insert("applications".into(), applications.into());
    // A single persistent application does not also ask for a script pool.
    document.entry("sapi".into()).or_insert(serde_yaml::Value::Null);
    serde_yaml::from_value(document.into())
        .map_err(|error| Error::msg(format!("PHP configuration: {error}")))
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct PhpConfig {
    engine: EngineConfig,
    lifecycle: LifecycleConfig,
    sapi: Option<SapiConfig>,
    #[serde(deserialize_with = "unique_names")]
    applications: BTreeMap<String, ApplicationConfig>,
    management: crate::PhpManagementConfig,
    events: crate::PhpEventConfig,
    launch: crate::PhpLaunchConfig,
    metrics: MetricsConfig,
}

impl Default for PhpConfig {
    fn default() -> Self {
        Self {
            engine: EngineConfig::default(),
            lifecycle: LifecycleConfig::default(),
            // Keep configured registration's historical one-thread default.
            // Explicit `sapi: null` disables it; the Rust builder stays empty.
            sapi: Some(SapiConfig::default()),
            applications: BTreeMap::new(),
            management: crate::PhpManagementConfig::default(),
            events: crate::PhpEventConfig::default(),
            launch: crate::PhpLaunchConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

impl PhpConfig {
    pub(super) fn resolve(self) -> Result<ResolvedConfig> {
        self.events.validate().map_err(Error::msg)?;
        let cache_invalidation =
            self.sapi.as_ref().map_or_else(
                crate::config::default_cache_invalidation,
                |sapi| sapi.cache_invalidation,
            );
        let sapi = self.sapi.map(SapiConfig::resolve).transpose()?;
        let applications = self
            .applications
            .into_iter()
            .map(|(name, config)| {
                config.resolve(name.clone()).map_err(|error| {
                    Error::msg(format!("php.yaml applications.{name}: {error}"))
                })
            })
            .collect::<Result<_>>()?;
        Ok(ResolvedConfig {
            engine: self.engine,
            lifecycle: self.lifecycle,
            sapi,
            cache_invalidation,
            applications,
            management: self.management,
            events: self.events,
            launch: self.launch,
            metrics: self.metrics,
        })
    }
}

/// Resolved intent only: no engine, registry, channel, lease or thread handles.
/// Keep process settings separate from execution policy for future comparison
/// and selective apply. Constructing this candidate does not publish it.
#[derive(Debug)]
pub(super) struct ResolvedConfig {
    engine: EngineConfig,
    lifecycle: LifecycleConfig,
    sapi: Option<RequestPool>,
    cache_invalidation: bool,
    applications: Vec<PhpApplication>,
    management: crate::PhpManagementConfig,
    events: crate::PhpEventConfig,
    launch: crate::PhpLaunchConfig,
    metrics: MetricsConfig,
}

impl ResolvedConfig {
    pub(super) fn has_applications(&self) -> bool {
        !self.applications.is_empty()
    }

    pub(super) fn build_script(
        self,
        name: &str,
        description: claviron_app::ApplicationDescription,
    ) -> PhpEmbedRuntime {
        self.builder().script_application(name, description).build()
    }

    pub(super) fn describe(
        mut self,
        description: claviron_app::ApplicationDescription,
    ) -> Self {
        for application in &mut self.applications {
            if application.frameworks.is_none() {
                application.frameworks = Some(description.frameworks.clone());
                application.description.frameworks =
                    description.frameworks.clone();
            }
            if application.description.icon.is_none() {
                application.description.icon = description.icon.clone();
            }
            if description.runner.is_some() {
                application.description.runner = description.runner.clone();
            }
        }
        self
    }

    pub(super) fn build(self) -> PhpEmbedRuntime {
        self.builder().build()
    }

    fn builder(self) -> crate::PhpEmbedRuntimeBuilder {
        let mut builder = PhpEmbedRuntime::builder()
            .graceful_timeout(self.lifecycle.graceful_timeout)
            .execution_timeout(self.lifecycle.execution_timeout)
            .interruptible_sleep(self.lifecycle.interruptible_sleep)
            .cache_invalidation(self.cache_invalidation)
            .management(self.management)
            .events(self.events)
            .launch(self.launch);
        if let Some(path) = self.engine.ini_path {
            builder = builder.ini_path(path);
        }
        if let Some(path) = self.engine.ini_scan_dir {
            builder = builder.ini_scan_dir(path);
        }
        if let Some(pool) = self.sapi {
            builder = builder.request_pool(pool);
        }
        for application in self.applications {
            builder = builder.application(application);
        }
        #[cfg(feature = "metrics")]
        {
            builder = builder.metrics(crate::PhpEmbedMetricsConfig {
                enabled: self.metrics.enabled,
                publish_interval: self.metrics.publish_interval,
                max_age: self.metrics.max_age,
            });
        }
        #[cfg(not(feature = "metrics"))]
        let _ = self.metrics;
        builder
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct EngineConfig {
    ini_path: Option<PathBuf>,
    ini_scan_dir: Option<PathBuf>,
}

/// Shared default for the three independent service drains, not an engine kill
/// deadline. None is an intentional unbounded drain, never a missing fallback.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LifecycleConfig {
    #[serde(deserialize_with = "deserialize_optional_duration")]
    graceful_timeout: Option<Duration>,
    /// Budget for a single PHP execution. None leaves it unbounded.
    #[serde(deserialize_with = "deserialize_optional_duration")]
    execution_timeout: Option<Duration>,
    /// Let a cancelled program's `sleep()` return early, once, so it can see
    /// `claviron_is_cancelled()` without waiting the sleep out. Off by
    /// default: it changes a PHP builtin for every program on the engine.
    interruptible_sleep: bool,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SapiConfig {
    pool: PoolConfig,
    /// Removed; read only to say where it went. How a request is run —
    /// the wait for a free thread, a client that is not reading — is the
    /// route's (`requests:` / `responses:` on the site's `php:` block, its
    /// host or listener), not the engine's: php.yaml describes instances.
    requests: Option<serde::de::IgnoredAny>,
    #[serde(default = "crate::config::default_cache_invalidation")]
    cache_invalidation: bool,
}

impl Default for SapiConfig {
    fn default() -> Self {
        Self {
            pool: PoolConfig::default(),
            requests: None,
            cache_invalidation: crate::config::default_cache_invalidation(),
        }
    }
}

impl SapiConfig {
    fn resolve(self) -> Result<RequestPool> {
        if self.requests.is_some() {
            return Err(Error::msg(
                "php.yaml sapi.requests is gone: dispatch_queue_timeout is requests.queue_timeout and streaming_write_timeout is responses.write_timeout, written on the route (the site's php: block, its host or its listener)",
            ));
        }
        let pool = self.pool.resolve();
        pool.validate()
            .map_err(|error| Error::msg(format!("php.yaml sapi: {error}")))?;
        Ok(pool)
    }
}

/// Capacity and retirement only, shared vocabulary for both HTTP models.
/// Defaults come from the typed API; absent ceiling/idle target follow the floor.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PoolConfig {
    min_idle: Option<u8>,
    max_idle: Option<u8>,
    max_size: Option<u8>,
    #[serde(deserialize_with = "deserialize_optional_duration")]
    idle_timeout: Option<Duration>,
    #[serde(deserialize_with = "deserialize_optional_duration")]
    max_life: Option<Duration>,
    max_requests: Option<u64>,
}

impl PoolConfig {
    fn resolve(self) -> RequestPool {
        let min =
            self.min_idle.unwrap_or_else(crate::config::default_instances);
        let mut pool = RequestPool::elastic()
            .min_idle(min)
            .max_size(self.max_size.unwrap_or(min))
            .max_idle(self.max_idle.unwrap_or(min))
            .max_life(self.max_life)
            .max_requests(self.max_requests);
        if let Some(value) = self.idle_timeout {
            pool = pool.idle_timeout(value);
        }
        pool
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationConfig {
    script_path: PathBuf,
    #[serde(
        default,
        alias = "framework",
        deserialize_with = "claviron_helpers::de::opt_one_or_many"
    )]
    frameworks: Option<Vec<String>>,
    #[serde(default)]
    icon: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    /// One path, or several. Later files override earlier ones, so a
    /// per-host file listed after the shared one wins; inline `env` still
    /// wins over every file.
    #[serde(default, deserialize_with = "one_or_many")]
    env_file: Vec<PathBuf>,
    #[serde(default, alias = "environment")]
    env: BTreeMap<String, String>,
    #[serde(default)]
    pool: ApplicationPoolConfig,
    /// Budget for one request or event on an instance; unset takes
    /// `lifecycle.execution_timeout`. Running, not request handling: events
    /// arrive with no route.
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    execution_timeout: Option<Duration>,
    /// Removed; read only to say where each field went.
    #[serde(default)]
    requests: Option<serde::de::IgnoredAny>,
    #[serde(default)]
    retry: ApplicationRetryConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ApplicationPoolConfig {
    min_idle: Option<u8>,
    max_idle: Option<u8>,
    max_size: Option<u8>,
    #[serde(deserialize_with = "deserialize_optional_duration")]
    idle_timeout: Option<Duration>,
    #[serde(deserialize_with = "deserialize_optional_duration")]
    max_life: Option<Duration>,
    max_requests: Option<u64>,
    /// Callers admitted at once, running and waiting.
    max_waiting: Option<usize>,
    #[serde(deserialize_with = "deserialize_optional_duration")]
    maintenance_interval: Option<Duration>,
}

impl ApplicationPoolConfig {
    fn resolve(self) -> ApplicationPool {
        // Explicit fields keep strict unknown/duplicate rejection. Serde flatten
        // with deny_unknown_fields does not reliably preserve that contract.
        let capacity = PoolConfig {
            min_idle: self.min_idle,
            max_idle: self.max_idle,
            max_size: self.max_size,
            idle_timeout: self.idle_timeout,
            max_life: self.max_life,
            max_requests: self.max_requests,
        };
        let mut pool = ApplicationPool {
            capacity: capacity.resolve(),
            ..ApplicationPool::default()
        };
        if let Some(value) = self.maintenance_interval {
            pool = pool.maintenance_interval(value);
        }
        if let Some(value) = self.max_waiting {
            pool = pool.max_waiting(value);
        }
        pool
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ApplicationRetryConfig {
    max_attempts: Option<u32>,
    #[serde(deserialize_with = "deserialize_optional_duration")]
    backoff: Option<Duration>,
    #[serde(deserialize_with = "deserialize_optional_duration")]
    backoff_max: Option<Duration>,
}

impl ApplicationConfig {
    fn resolve(
        self,
        name: String,
    ) -> Result<PhpApplication> {
        if self.requests.is_some() {
            return Err(Error::msg(format!(
                "php.yaml applications.{name}.requests is gone: max_waiting goes under pool:, execution_timeout directly under the application; dispatch_queue_timeout is requests.queue_timeout and response_limit is responses.max_body_size, written on the route that selects it"
            )));
        }
        let mut pool = self
            .pool
            .resolve()
            .max_attempts(self.retry.max_attempts)
            .execution_timeout(self.execution_timeout);
        if let Some(value) = self.retry.backoff {
            pool = pool.backoff(value);
        }
        if let Some(value) = self.retry.backoff_max {
            pool = pool.backoff_max(value);
        }
        let mut application = PhpApplication::new(name, self.script_path)
            .args(self.args)
            .env_files(self.env_file)
            .environments(self.env)
            .pool(pool);
        if let Some(frameworks) = self.frameworks {
            application = application.frameworks(frameworks);
        }
        application.description.icon = self.icon;
        application.validate().map_err(Error::msg)?;
        Ok(application)
    }
}

fn unique_names<'de, D, T>(
    deserializer: D
) -> std::result::Result<BTreeMap<String, T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Names<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Names<T> {
        type Value = BTreeMap<String, T>;
        fn expecting(
            &self,
            formatter: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            formatter.write_str("a map of unique PHP program names")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut entries = BTreeMap::new();
            while let Some((name, value)) = map.next_entry::<String, T>()? {
                if entries.insert(name.clone(), value).is_some() {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate PHP program: {name}"
                    )));
                }
            }
            Ok(entries)
        }
    }
    deserializer.deserialize_map(Names(std::marker::PhantomData))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(not(feature = "metrics"), allow(dead_code))]
struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    enabled: bool,
    #[serde(
        default = "default_metrics_publish_interval",
        deserialize_with = "claviron_helpers::de::deserialize_duration"
    )]
    publish_interval: Duration,
    #[serde(
        default = "default_metrics_max_age",
        deserialize_with = "claviron_helpers::de::deserialize_duration"
    )]
    max_age: Duration,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            publish_interval: default_metrics_publish_interval(),
            max_age: default_metrics_max_age(),
        }
    }
}

fn default_metrics_enabled() -> bool {
    true
}
fn default_metrics_publish_interval() -> Duration {
    Duration::from_secs(1)
}
fn default_metrics_max_age() -> Duration {
    Duration::from_secs(5)
}

#[cfg(test)]
#[path = "../../../tests/config/file/schema.rs"]
mod tests;
