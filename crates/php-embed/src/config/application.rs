//! Persistent HTTP applications: local capacity only, never fleet slot leases.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use claviron_app::{ApplicationDescription, ApplicationRetryPolicy};

use super::{RequestPool, validate_program};

/// A boot script implementing the explicit application receive/respond protocol.
/// Every serving process owns an independent pool. No PHP objects cross threads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhpApplication {
    pub(crate) description: ApplicationDescription,
    /// Unresolved metadata intent; Some(empty) suppresses Composer discovery.
    pub(crate) frameworks: Option<Vec<String>>,
    pub(crate) name: String,
    pub(crate) script_path: PathBuf,
    pub(crate) args: Vec<String>,
    pub(crate) environment: BTreeMap<String, String>,
    pub(crate) env_file: Vec<PathBuf>,
    pub(crate) pool: ApplicationPool,
}

impl PhpApplication {
    pub fn new(
        name: impl Into<String>,
        script: impl Into<PathBuf>,
    ) -> Self {
        Self {
            description: ApplicationDescription::new("php", "application")
                .runner("claviron"),
            frameworks: None,
            name: name.into(),
            script_path: script.into(),
            args: Vec::new(),
            environment: BTreeMap::new(),
            env_file: Vec::new(),
            pool: ApplicationPool::default(),
        }
    }
    pub fn pool(
        mut self,
        pool: ApplicationPool,
    ) -> Self {
        self.pool = pool;
        self
    }
    /// Prepared management metadata; execution identity and mode stay engine-owned.
    pub fn framework(
        self,
        framework: impl Into<String>,
    ) -> Self {
        self.frameworks([framework.into()])
    }

    pub fn frameworks(
        mut self,
        frameworks: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let frameworks: Vec<String> =
            frameworks.into_iter().map(Into::into).collect();
        self.description.frameworks = frameworks.clone();
        self.frameworks = Some(frameworks);
        self
    }

    pub fn icon(
        mut self,
        icon: impl Into<String>,
    ) -> Self {
        self.description.icon = Some(icon.into());
        self
    }

    pub fn runner(
        mut self,
        runner: impl Into<String>,
    ) -> Self {
        self.description.runner = Some(runner.into());
        self
    }
    pub fn args(
        mut self,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }
    pub fn environment(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.environment.insert(key.into(), value.into());
        self
    }
    pub fn environments(
        mut self,
        values: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        self.environment.extend(values);
        self
    }
    /// Declare environment files without reading them during registration.
    /// Paths and inline overrides remain separate execution inputs.
    pub fn env_files(
        mut self,
        paths: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) -> Self {
        self.env_file.extend(paths.into_iter().map(Into::into));
        self
    }
    pub fn validate(&self) -> Result<(), String> {
        validate_program(
            "PHP application",
            &self.name,
            &self.script_path,
            &self.args,
            &self.environment,
        )?;
        self.pool.validate()
    }
}

/// How the application runs: its instances and what bounds them.
/// Retry replaces failed workers, NEVER replays an accepted request.
///
/// Running, not request handling: an application lives in two shapes — HTTP
/// requests, which arrive through a route and take the route's policy (the
/// wait for an instance, the reply's ceiling), and events, which arrive with
/// no route at all. Both run on the same instances, so what bounds an
/// instance is set here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationPool {
    pub(crate) capacity: RequestPool,
    /// Callers admitted at once, running and waiting: the application's
    /// admission capacity.
    pub(crate) max_waiting: usize,
    /// Budget for one unit of work on an instance — a request or an event —
    /// falling back to the runtime's `lifecycle.execution_timeout` when
    /// unset. Per application rather than per route: several hosts may share
    /// the instances, and an instance is what a runaway unit occupies.
    pub(crate) execution_timeout: Option<Duration>,
    pub(crate) maintenance_interval: Duration,
    /// When a failed worker may be replaced and when to stop replacing it.
    /// Shared with every other Claviron runtime so the curve cannot drift.
    pub(crate) retry: ApplicationRetryPolicy,
}

impl ApplicationPool {
    pub fn fixed(instances: u8) -> Self {
        Self {
            capacity: RequestPool::fixed(instances),
            max_waiting: default_max_waiting(),
            execution_timeout: None,
            maintenance_interval: default_maintenance_interval(),
            retry: ApplicationRetryPolicy::new(
                default_backoff(),
                default_backoff_max(),
                None,
            ),
        }
    }
    pub fn elastic() -> Self {
        Self::fixed(1)
    }
    pub fn min_idle(
        mut self,
        n: u8,
    ) -> Self {
        self.capacity = self.capacity.min_idle(n);
        self
    }
    pub fn max_size(
        mut self,
        n: u8,
    ) -> Self {
        self.capacity = self.capacity.max_size(n);
        self
    }
    pub fn max_idle(
        mut self,
        n: u8,
    ) -> Self {
        self.capacity = self.capacity.max_idle(n);
        self
    }
    pub fn idle_timeout(
        mut self,
        value: Duration,
    ) -> Self {
        self.capacity = self.capacity.idle_timeout(value);
        self
    }
    pub fn max_life(
        mut self,
        value: Option<Duration>,
    ) -> Self {
        self.capacity = self.capacity.max_life(value);
        self
    }
    pub fn max_requests(
        mut self,
        value: Option<u64>,
    ) -> Self {
        self.capacity = self.capacity.max_requests(value);
        self
    }
    /// Callers admitted at once, running and waiting.
    pub fn max_waiting(
        mut self,
        value: usize,
    ) -> Self {
        self.max_waiting = value;
        self
    }
    /// Budget for one request or event on an instance.
    pub fn execution_timeout(
        mut self,
        value: Option<Duration>,
    ) -> Self {
        self.execution_timeout = value;
        self
    }
    pub fn maintenance_interval(
        mut self,
        value: Duration,
    ) -> Self {
        self.maintenance_interval = value;
        self
    }
    pub fn backoff(
        mut self,
        value: Duration,
    ) -> Self {
        self.retry = ApplicationRetryPolicy::new(
            value,
            self.retry.backoff_max(),
            self.retry.max_attempts(),
        );
        self
    }
    pub fn backoff_max(
        mut self,
        value: Duration,
    ) -> Self {
        self.retry = ApplicationRetryPolicy::new(
            self.retry.backoff(),
            value,
            self.retry.max_attempts(),
        );
        self
    }
    /// Consecutive attempts without completed requests. None retries indefinitely;
    /// exhausted slots remain unavailable until the service is restarted.
    pub fn max_attempts(
        mut self,
        value: Option<u32>,
    ) -> Self {
        self.retry = ApplicationRetryPolicy::new(
            self.retry.backoff(),
            self.retry.backoff_max(),
            value,
        );
        self
    }
    pub fn validate(&self) -> Result<(), String> {
        self.capacity.validate()?;
        if self.max_waiting == 0
            || self.max_waiting > tokio::sync::Semaphore::MAX_PERMITS
            || self.maintenance_interval.is_zero()
        {
            return Err(
                "application limits and intervals must be positive".into()
            );
        }
        self.retry.validate("retry")?;
        Ok(())
    }
}
impl Default for ApplicationPool {
    fn default() -> Self {
        Self::fixed(1)
    }
}
fn default_max_waiting() -> usize {
    256
}
fn default_maintenance_interval() -> Duration {
    Duration::from_millis(100)
}
fn default_backoff() -> Duration {
    Duration::from_secs(1)
}
fn default_backoff_max() -> Duration {
    Duration::from_secs(60)
}
