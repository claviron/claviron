use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron_helpers::sync::{LockUnpoisoned, Unpoisoned};
use claviron_orbit::OrbitStateExt;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{
    Bootable, Component, ComponentOrder, Reloadable, Runnable,
};
use claviron_runtime::{AppState, RunContext, RunMode};
use futures_util::FutureExt;
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[cfg(feature = "metrics")]
use crate::config::TaskMetricsConfig;
use crate::config::{
    TaskConfig, TaskExecutor, TaskExecutorKind, TaskPoolConfig,
    TaskServiceConfig, TaskSnapshotConfig, TaskTable, TaskTiming,
};
use crate::executor::Executor;
use crate::manager::{
    TaskCommand, TaskCommandAction, TaskManagerError, TaskManagerResult,
};
use crate::process::{
    CommandSpec, ProcessSignal, ReloadAction, RestartPolicy, run_command,
};
use crate::registry::{TaskDispatch, TaskScope};
use crate::runner::{ExecutionPlacement, StopCause, StopContext, TaskRunner};
use crate::snapshot::{
    TaskKind, TaskLifecycle, TaskListSnapshot, TaskMember, TaskMemberRole,
    TaskPlacementStatus, TaskRuntimeState, TaskStatus, TaskTimingSummary,
};
use crate::{Error, TaskResult};

pub struct TaskService {
    pub(crate) inner: Arc<Inner>,
    reload_lock: Arc<AsyncMutex<()>>,
}

impl Clone for TaskService {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            reload_lock: Arc::clone(&self.reload_lock),
        }
    }
}

pub(crate) struct Inner {
    /// Statically registered built-in task bodies.
    pub(crate) registry: RwLock<HashMap<String, Arc<TaskRunner>>>,
    /// Command runners materialized from the active YAML task table.
    command_runners: RwLock<HashMap<String, Arc<TaskRunner>>>,
    /// Runners materialized from the same table for every task whose kind has
    /// an installed engine. Empty in a build without one, which is why such a
    /// task then reports a missing runner instead of being mistaken for
    /// something else.
    pub(crate) engine_runners: RwLock<HashMap<String, Arc<TaskRunner>>>,
    /// Supplied by the crates that own the engines. See `set_executor`.
    pub(crate) executors: RwLock<HashMap<TaskExecutorKind, Executor>>,
    pub(crate) tasks: ArcSwap<TaskTable>,
    /// Service-wide graceful-stop budget loaded from `tasks.yaml`'s
    /// top-level `graceful_timeout`. `None` = wait indefinitely.
    pub(crate) graceful_timeout: ArcSwap<Option<Duration>>,
    /// Always-available management snapshot policy.
    snapshot: ArcSwap<TaskSnapshotConfig>,
    #[cfg(feature = "metrics")]
    /// Runtime metrics policy loaded from `tasks.yaml::metrics`.
    pub(crate) metrics: ArcSwap<TaskMetricsConfig>,
    /// Coordination timing for `dispatch: pool` controllers.
    pub(crate) pool: ArcSwap<TaskPoolConfig>,
    /// Task-scoped pool controller generations and their reload lifecycle.
    pool_reloader: crate::pool::PoolReloader,
    pub(crate) scheduler: Arc<crate::scheduler::Scheduler>,
    /// Non-pool supervisors owned by this service, keyed by task name.
    /// Keeping the runner beside its join handle lets reload cancel and drain
    /// the exact active generation even after a candidate command runner has
    /// been built.
    supervisors: Mutex<HashMap<String, ActiveSupervisor>>,
    /// Explicit executions owned by the service. A run remains here during
    /// retry backoff so reload and shutdown can still cancel it.
    explicit_runs: Mutex<HashMap<String, HashMap<u64, ActiveExplicitRun>>>,
    /// Logical execution state shared by replacement runner generations.
    runtime_states: RwLock<HashMap<String, Arc<TaskRuntimeState>>>,
    pub(crate) config_path: PathBuf,
    /// Set once boot published the first table; a prepared change before
    /// that is boot's to take, with the builtins registered by then.
    loaded: AtomicBool,
    /// The application, for a prepared source change to reload against.
    state: OnceLock<AppState>,
}

struct ActiveSupervisor {
    phase: SupervisorPhase,
    runner: Arc<TaskRunner>,
    handle: SupervisorHandle,
}

enum SupervisorHandle {
    Execution(JoinHandle<()>),
    Schedule(crate::scheduler::ScheduleHandle),
}

impl SupervisorHandle {
    fn is_finished(&self) -> bool {
        match self {
            Self::Execution(handle) => handle.is_finished(),
            Self::Schedule(handle) => handle.is_finished(),
        }
    }

    fn stop_schedule(&self) {
        if let Self::Schedule(handle) = self {
            handle.stop();
        }
    }

    #[cfg(test)]
    fn abort(&self) {
        match self {
            Self::Execution(handle) => handle.abort(),
            Self::Schedule(handle) => handle.stop(),
        }
    }
}

struct ActiveExplicitRun {
    token: CancellationToken,
    stop: StopContext,
    completion: oneshot::Sender<TaskResult<()>>,
}

#[derive(Default)]
struct ReloadTargets {
    supervisors: BTreeSet<String>,
    pools: BTreeMap<String, ReloadAction>,
    /// Unchanged command generations to notify without draining.
    signals: BTreeMap<String, ProcessSignal>,
}

impl ReloadTargets {
    fn has_transitions(&self) -> bool {
        !self.supervisors.is_empty() || !self.pools.is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SupervisorPhase {
    Running,
    Stopping,
}

/// Runtime effect of one task's candidate configuration.
///
/// The reload planner uses this classification to select only the supervisor
/// generations that must drain before the candidate can become active.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReloadTaskChange {
    Unchanged,
    Start,
    Stop,
    Rebuild,
    Reschedule,
    Reconfigure,
}

impl ReloadTaskChange {
    const fn label(self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Rebuild => "rebuild",
            Self::Reschedule => "reschedule",
            Self::Reconfigure => "reconfigure",
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else {
        "non-string panic payload"
    }
}

fn classify_task_change(
    active: Option<&TaskConfig>,
    candidate: Option<&TaskConfig>,
    runner: Option<&TaskRunner>,
) -> ReloadTaskChange {
    let active =
        active.filter(|cfg| cfg.timing.trigger_enabled() != Some(false));
    let candidate =
        candidate.filter(|cfg| cfg.timing.trigger_enabled() != Some(false));

    let (active, candidate) = match (active, candidate) {
        (None, None) => return ReloadTaskChange::Unchanged,
        (None, Some(_)) => return ReloadTaskChange::Start,
        (Some(_), None) => return ReloadTaskChange::Stop,
        (Some(active), Some(candidate)) => (active, candidate),
    };

    let structure_changed = active.executor_kind() != candidate.executor_kind()
        || effective_scope(active, runner)
            != effective_scope(candidate, runner)
        || effective_dispatch(active, runner)
            != effective_dispatch(candidate, runner)
        || active.concurrency != candidate.concurrency
        || active.scheduler != candidate.scheduler
        || active.timing.overlap() != candidate.timing.overlap()
        || !same_timing_kind(&active.timing, &candidate.timing);
    if structure_changed {
        return ReloadTaskChange::Rebuild;
    }

    if active.timing != candidate.timing {
        return ReloadTaskChange::Reschedule;
    }

    if !same_command_runtime(&active.command, &candidate.command)
        || active.attempts != candidate.attempts
        || active.backoff != candidate.backoff
        || active.graceful_timeout != candidate.graceful_timeout
        || active.silent != candidate.silent
    {
        return ReloadTaskChange::Reconfigure;
    }

    ReloadTaskChange::Unchanged
}

fn same_command_runtime(
    active: &Option<CommandSpec>,
    candidate: &Option<CommandSpec>,
) -> bool {
    match (active, candidate) {
        (None, None) => true,
        (Some(active), Some(candidate)) => {
            active.program == candidate.program
                && active.args == candidate.args
                && active.env == candidate.env
                && active.env_file == candidate.env_file
                && active.cwd == candidate.cwd
                && active.restart == candidate.restart
                && active.exit_action == candidate.exit_action
                && active.log == candidate.log
        }
        _ => false,
    }
}

pub(crate) fn effective_scope(
    cfg: &TaskConfig,
    runner: Option<&TaskRunner>,
) -> TaskScope {
    cfg.scope.unwrap_or_else(|| {
        cfg.executor_kind().default_scope().unwrap_or_else(|| {
            runner.map(TaskRunner::task_scope).unwrap_or_default()
        })
    })
}

pub(crate) fn effective_dispatch(
    cfg: &TaskConfig,
    runner: Option<&TaskRunner>,
) -> TaskDispatch {
    cfg.dispatch.unwrap_or_else(|| {
        cfg.executor_kind().default_dispatch().unwrap_or_else(|| {
            runner.map(TaskRunner::task_dispatch).unwrap_or_default()
        })
    })
}

fn same_timing_kind(
    active: &TaskTiming,
    candidate: &TaskTiming,
) -> bool {
    matches!(
        (active, candidate),
        (TaskTiming::Manual { .. }, TaskTiming::Manual { .. })
            | (TaskTiming::Daemon { .. }, TaskTiming::Daemon { .. })
            | (TaskTiming::Interval { .. }, TaskTiming::Interval { .. })
            | (TaskTiming::Cron { .. }, TaskTiming::Cron { .. })
    )
}

/// Idiomatic builder for TaskService that returns Arc<TaskService>
pub struct TaskServiceBuilder {
    svc: TaskService,
}

impl TaskServiceBuilder {
    pub fn new(config_path: PathBuf) -> Self {
        Self { svc: TaskService::new(config_path) }
    }

    /// Register a task function via builder (fluent, non‑consuming of the service)
    pub fn register_fn<T>(
        &mut self,
        name: &'static str,
        func: T,
    ) -> &mut Self
    where
        T: Into<TaskRunner>,
    {
        self.svc.register_fn(name, func);
        self
    }

    /// Register a builtin task with the entry it carries for itself; see
    /// [`TaskService::register_builtin`].
    pub fn register_builtin<T>(
        &mut self,
        name: &'static str,
        config: TaskConfig,
        func: T,
    ) -> &mut Self
    where
        T: Into<TaskRunner>,
    {
        self.svc.register_builtin(name, config, func);
        self
    }

    /// Register all tasks discovered via inventory (annotated with `#[task]` macro)
    ///
    /// Placement is evaluated when supervisors start, after `tasks.yaml`
    /// overrides have been loaded. Every process registers the same static
    /// inventory so config can move a task between scopes without recompiling.
    pub fn register_discovered_tasks(&mut self) -> Result<&mut Self, String> {
        use crate::registry::TaskRegistration;

        for task in inventory::iter::<TaskRegistration> {
            let mut runner = (task.create_runner)();
            if let Some(yaml) = task.config {
                let config = TaskConfig::from_yaml(yaml).map_err(|error| {
                    format!(
                        "task '{}' declares an invalid config: {error}",
                        task.name
                    )
                })?;
                runner = runner.with_defaults(config);
            }
            runner.set_name(task.name);
            runner.set_task_scope(task.scope);
            runner.set_task_dispatch(task.dispatch);
            runner.set_runtime_state(self.svc.runtime_state(task.name));
            self.svc
                .inner
                .registry
                .write()
                .unwrap()
                .insert(task.name.to_string(), Arc::new(runner));
        }
        Ok(self)
    }

    /// Finalize and get an Arc to the service for spawning
    pub fn build(self) -> Arc<TaskService> {
        Arc::new(self.svc)
    }
}

impl TaskService {
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            inner: Arc::new(Inner {
                registry: RwLock::new(HashMap::new()),
                command_runners: RwLock::new(HashMap::new()),
                engine_runners: RwLock::new(HashMap::new()),
                executors: RwLock::new(HashMap::new()),
                tasks: ArcSwap::from_pointee(TaskTable::new()),
                graceful_timeout: ArcSwap::from_pointee(None),
                snapshot: ArcSwap::from_pointee(TaskSnapshotConfig::default()),
                #[cfg(feature = "metrics")]
                metrics: ArcSwap::from_pointee(TaskMetricsConfig::default()),
                pool: ArcSwap::from_pointee(TaskPoolConfig::default()),
                pool_reloader: crate::pool::PoolReloader::default(),
                scheduler: Arc::new(crate::scheduler::Scheduler::default()),
                supervisors: Mutex::new(HashMap::new()),
                explicit_runs: Mutex::new(HashMap::new()),
                runtime_states: RwLock::new(HashMap::new()),
                config_path,
                loaded: AtomicBool::new(false),
                state: OnceLock::new(),
            }),
            reload_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    /// The application this service was registered in, for the hooks that
    /// have no state of their own.
    pub(crate) fn attach_state(
        &self,
        state: &AppState,
    ) {
        let _ = self.inner.state.set(state.clone());
    }

    pub(crate) fn runtime_state(
        &self,
        name: &str,
    ) -> Arc<TaskRuntimeState> {
        if let Some(state) =
            self.inner.runtime_states.read_unpoisoned().get(name)
        {
            return Arc::clone(state);
        }
        let mut states = self.inner.runtime_states.write_unpoisoned();
        Arc::clone(states.entry(name.to_owned()).or_default())
    }

    pub(crate) fn snapshot_config(&self) -> TaskSnapshotConfig {
        (**self.inner.snapshot.load()).clone()
    }

    /// Current RSS sampler cadence — read each tick by
    /// `metrics::ExternalProcessSampler` so a SIGHUP picks up the
    /// new value on the next loop iteration.
    #[cfg(feature = "metrics")]
    pub fn metrics_interval(&self) -> Duration {
        self.inner.metrics.load().rss_sample_interval
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn metrics_config(&self) -> TaskMetricsConfig {
        (**self.inner.metrics.load()).clone()
    }

    /// Materialize the prepared `tasks.yaml` without publishing it: fold in
    /// the builtin entries registered by now, the scheduler and the service
    /// defaults, then validate the whole candidate. Executor env-file paths
    /// and inline values remain declarations, not evaluated environments.
    /// Congif prepared the document before boot or as the reload batch;
    /// nothing here opens the file, and an absent one is the empty document.
    async fn read_task_config(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<TaskServiceConfig> {
        let cfg = (*state.config_reader().snapshot::<Self>()?).clone();
        let mut cfg = cfg;
        if cfg.tasks.contains_key("scheduler") {
            return Err(claviron_runtime::registry::Error::msg(
                "scheduler is builtin; configure the top-level scheduler block, not tasks.scheduler",
            ));
        }
        self.merge_builtin_defaults(&mut cfg.tasks);
        Self::materialize_scheduler(&mut cfg)?;
        Self::apply_command_defaults(&mut cfg);

        // Validate the candidate before publishing any of its snapshots. A
        // failed boot/reload must never expose a partially accepted config.
        self.validate_task_policies_for(&cfg.tasks, &cfg.pool, state)?;
        if cfg.snapshot.publish_interval.is_zero() {
            return Err(claviron_runtime::registry::Error::msg(
                "tasks snapshot publish_interval must be greater than zero",
            ));
        }
        if cfg.snapshot.max_age.is_zero() {
            return Err(claviron_runtime::registry::Error::msg(
                "tasks snapshot max_age must be greater than zero",
            ));
        }
        crate::snapshot::validate_task_table(&cfg.tasks)
            .map_err(claviron_runtime::registry::Error::msg)?;

        Ok(cfg)
    }

    /// Stamp the service-level `command.path` onto every command spec.
    ///
    /// Resolved once here, against the candidate config, so the spawn path
    /// reads a single field rather than reaching back for service config it
    /// otherwise has no use for. It also means a published snapshot carries
    /// the path it was loaded under instead of picking one up later from
    /// whatever the service happens to hold by then — the same reason the
    /// rest of this config is validated before it is published.
    fn apply_command_defaults(cfg: &mut TaskServiceConfig) {
        let path = cfg.command.path.clone();
        for task in cfg.tasks.values_mut() {
            if task.command.is_none() {
                continue;
            }
            if let Some(command) = Arc::make_mut(task).command.as_mut() {
                command.default_path.clone_from(&path);
            }
        }
    }

    /// Publish all snapshots belonging to one validated config.
    /// Every registered task that carries its own entry gets it, unless the
    /// file already has one under that name: the file replaces, never merges.
    fn merge_builtin_defaults(
        &self,
        tasks: &mut TaskTable,
    ) {
        let registry = self.inner.registry.read_unpoisoned();
        for (name, runner) in registry.iter() {
            if tasks.contains_key(name) {
                continue;
            }
            if let Some(defaults) = runner.defaults() {
                // The table owns its entries: what follows edits them in
                // place, and a copy shared with the runner could not be.
                tasks.insert(name.clone(), Arc::new((*defaults).clone()));
            }
        }
    }

    fn materialize_scheduler(
        cfg: &mut TaskServiceConfig
    ) -> claviron_runtime::registry::Result<()> {
        if cfg.scheduler.poll_interval.is_zero()
            || tokio::time::Instant::now()
                .checked_add(cfg.scheduler.poll_interval)
                .is_none()
        {
            return Err(claviron_runtime::registry::Error::msg(
                "scheduler poll_interval must be greater than zero and representable by the runtime",
            ));
        }
        if let Some(task) = cfg.tasks.get_mut("scheduler") {
            Arc::make_mut(task).scheduler = Some(cfg.scheduler.clone());
        }
        Ok(())
    }

    fn publish_task_config(
        &self,
        cfg: TaskServiceConfig,
    ) {
        self.inner.tasks.store(Arc::new(cfg.tasks));
        self.inner.graceful_timeout.store(Arc::new(cfg.graceful_timeout));
        self.inner.snapshot.store(Arc::new(cfg.snapshot));
        self.inner.pool.store(Arc::new(cfg.pool));
        #[cfg(feature = "metrics")]
        self.inner.metrics.store(Arc::new(cfg.metrics));
    }

    /// Materialize and publish the prepared config during initial boot.
    async fn load_tasks(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let cfg = self.read_task_config(state).await?;
        self.publish_task_config(cfg);
        self.inner.loaded.store(true, Ordering::Release);
        Ok(())
    }

    fn classify_reload(
        &self,
        active: &TaskTable,
        candidate: &TaskTable,
    ) -> Vec<(String, ReloadTaskChange)> {
        let names = active
            .keys()
            .chain(candidate.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let registry = self.inner.registry.read_unpoisoned();

        names
            .into_iter()
            .map(|name| {
                let runner = registry.get(name.as_str()).map(Arc::as_ref);
                let change = classify_task_change(
                    active.get(&name).map(Arc::as_ref),
                    candidate.get(&name).map(Arc::as_ref),
                    runner,
                );
                (name, change)
            })
            .collect()
    }

    /// Service-wide graceful-stop budget from `tasks.yaml`.
    /// `None` is preserved and means wait indefinitely.
    fn graceful_timeout(&self) -> Option<Duration> {
        **self.inner.graceful_timeout.load()
    }

    pub(crate) fn task_snapshot(&self) -> Arc<TaskTable> {
        self.inner.tasks.load_full()
    }

    /// Publish one task's live trigger state without maintaining a parallel
    /// trigger-state overlay. YAML provides the initial value; fleet management
    /// commands replace it in the current task table.
    fn set_task_trigger_enabled(
        &self,
        name: &str,
        enabled: bool,
    ) -> TaskManagerResult<Arc<TaskConfig>> {
        let current = self.task_snapshot();
        let mut tasks = (*current).clone();
        let config = {
            let config = tasks.get_mut(name).ok_or_else(|| {
                TaskManagerError::TaskNotFound(name.to_owned())
            })?;
            if !Arc::make_mut(config).timing.set_trigger_enabled(enabled) {
                return Err(TaskManagerError::ManualTaskHasNoSchedule(
                    name.to_owned(),
                ));
            }
            Arc::clone(config)
        };
        self.inner.tasks.store(Arc::new(tasks));
        Ok(config)
    }

    pub(crate) fn local_snapshot(
        &self,
        state: &AppState,
    ) -> TaskListSnapshot {
        let captured_at = SystemTime::now();
        let node_id = state
            .fleet()
            .map(|fleet| fleet.node_id().get())
            .unwrap_or_else(|| state.fleet_node_id().raw());
        let role = match state.run_mode() {
            RunMode::Master => TaskMemberRole::Master,
            RunMode::Worker(_) => TaskMemberRole::Worker,
            RunMode::Standalone => TaskMemberRole::Standalone,
        };
        let member = TaskMember { node_id, role };
        let tasks = self.task_snapshot();
        let registry = self.inner.registry.read_unpoisoned();
        let commands = self.inner.command_runners.read_unpoisoned();
        let engines = self.inner.engine_runners.read_unpoisoned();
        let explicit_run_tasks = self
            .inner
            .explicit_runs
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(name, runs)| {
                (!runs.is_empty()).then_some(name.clone())
            })
            .collect::<BTreeSet<_>>();
        let supervisors = self.inner.supervisors.lock_unpoisoned();
        let mut statuses = Vec::with_capacity(tasks.len());

        for (name, config) in tasks.iter() {
            // Three kinds, three sources, and no `else`. A task whose block
            // names an executor that has nothing registered reports
            // `MissingRunner` below, where it used to be mistaken for a Rust
            // task that does not exist — SEMANTICS.md S14.
            let configured_runner = match config.executor() {
                TaskExecutor::Builtin => registry.get(name).cloned(),
                TaskExecutor::Command(_) => commands.get(name).cloned(),
                TaskExecutor::Php(_)
                | TaskExecutor::Js(_)
                | TaskExecutor::Lua(_) => engines.get(name).cloned(),
            };
            let scope = effective_scope(config, configured_runner.as_deref());
            let dispatch =
                effective_dispatch(config, configured_runner.as_deref());
            let (runtime_lifecycle, runtime_runner) = if dispatch
                .is_controller_driven()
            {
                self.inner
                    .pool_reloader
                    .snapshot(name)
                    .map(|(lifecycle, runner)| (Some(lifecycle), Some(runner)))
                    .unwrap_or((None, None))
            } else {
                supervisors
                    .get(name)
                    .map(|active| {
                        let lifecycle =
                            if active.phase == SupervisorPhase::Stopping {
                                TaskLifecycle::Stopping
                            } else if active.handle.is_finished()
                                && !active.runner.is_running()
                            {
                                TaskLifecycle::Finished
                            } else {
                                TaskLifecycle::Active
                            };
                        (Some(lifecycle), Some(Arc::clone(&active.runner)))
                    })
                    .unwrap_or((None, None))
            };
            let status_runner =
                runtime_runner.or_else(|| configured_runner.clone());
            let explicit_run_active = explicit_run_tasks.contains(name);
            let lifecycle =
                if runtime_lifecycle == Some(TaskLifecycle::Stopping) {
                    TaskLifecycle::Stopping
                } else if config.timing.trigger_enabled() == Some(false) {
                    TaskLifecycle::Disabled
                } else if !scope.includes(state.run_mode()) {
                    TaskLifecycle::Ineligible
                } else if configured_runner.is_none() {
                    TaskLifecycle::MissingRunner
                } else {
                    runtime_lifecycle.unwrap_or(if explicit_run_active {
                        TaskLifecycle::Active
                    } else {
                        TaskLifecycle::Inactive
                    })
                };
            let in_flight = status_runner
                .as_ref()
                .map_or(0, |runner| runner.running_count());
            let owned_slots =
                status_runner.as_ref().map_or(0, |runner| runner.owned_slots());
            let last_execution =
                status_runner.and_then(|runner| runner.last_execution());
            let generation = self
                .inner
                .runtime_states
                .read()
                .unwrap()
                .get(name)
                .map_or(0, |runtime| runtime.generation());

            statuses.push(TaskStatus {
                name: name.clone(),
                kind: match config.executor_kind() {
                    TaskExecutorKind::Builtin => TaskKind::Builtin,
                    TaskExecutorKind::Command => TaskKind::Command,
                    TaskExecutorKind::Php => TaskKind::Php,
                    TaskExecutorKind::Js => TaskKind::Js,
                    TaskExecutorKind::Lua => TaskKind::Lua,
                },
                scope,
                dispatch,
                timing: TaskTimingSummary::from(&config.timing),
                concurrency: config.concurrency,
                placements: vec![TaskPlacementStatus {
                    member,
                    lifecycle,
                    generation,
                    in_flight,
                    owned_slots,
                    last_execution,
                    observed_at: captured_at,
                }],
            });
        }
        statuses.sort_by(|left, right| left.name.cmp(&right.name));

        TaskListSnapshot {
            captured_at,
            // This is one local publication, not a fleet membership claim.
            // TaskManager fills the fleet expectation from the process owner.
            members_expected: 1,
            members_reporting: 1,
            tasks: statuses,
        }
    }

    pub(crate) fn publish_local_snapshot(
        &self,
        state: &AppState,
    ) -> Result<TaskListSnapshot, String> {
        let fleet = state.fleet().ok_or_else(|| {
            "Orbit fleet is unavailable for task snapshots".to_string()
        })?;
        let snapshot = self.local_snapshot(state);
        let role = match state.run_mode() {
            RunMode::Master => TaskMemberRole::Master,
            RunMode::Worker(_) => TaskMemberRole::Worker,
            RunMode::Standalone => TaskMemberRole::Standalone,
        };
        crate::snapshot::publish(&fleet, role, &snapshot)?;
        Ok(snapshot)
    }

    fn validate_task_policies(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let tasks = self.task_snapshot();
        let pool = self.inner.pool.load_full();
        self.validate_task_policies_for(&tasks, &pool, state)
    }

    fn validate_task_policies_for(
        &self,
        tasks: &TaskTable,
        pool: &TaskPoolConfig,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let registry = self.inner.registry.read_unpoisoned();
        let mut needs_fleet = false;

        if pool.lease_ttl.is_zero() {
            return Err(claviron_runtime::registry::Error::msg(
                "task pool lease_ttl must be greater than zero",
            ));
        }

        for (name, cfg) in tasks.iter() {
            if name != "scheduler" && cfg.scheduler.is_some() {
                return Err(claviron_runtime::registry::Error::msg(
                    "the scheduler block belongs only to the builtin scheduler task",
                ));
            }
            if name == "scheduler" {
                if cfg
                    .scheduler
                    .as_ref()
                    .is_some_and(|cfg| cfg.poll_interval.is_zero())
                {
                    return Err(claviron_runtime::registry::Error::msg(
                        "scheduler poll_interval must be greater than zero",
                    ));
                }
                if !matches!(cfg.timing, TaskTiming::Daemon { .. })
                    || cfg.scope.is_some_and(|scope| scope != TaskScope::Fleet)
                    || cfg
                        .dispatch
                        .is_some_and(|dispatch| dispatch != TaskDispatch::Each)
                    || cfg.concurrency != 1
                    || !matches!(cfg.executor(), TaskExecutor::Builtin)
                {
                    return Err(claviron_runtime::registry::Error::msg(
                        "scheduler requires builtin daemon timing, fleet scope, each dispatch and concurrency 1",
                    ));
                }
            }
            if cfg.has_conflicting_executors() {
                return Err(claviron_runtime::registry::Error::msg(format!(
                    "task '{name}' declares both 'command' and 'php'; a task has one \
                     executor, so keep the block that runs it and remove the other"
                )));
            }
            let runner = registry.get(name.as_str());
            let scope = cfg.scope.unwrap_or_else(|| {
                effective_scope(cfg, runner.map(Arc::as_ref))
            });
            let dispatch = cfg.dispatch.unwrap_or_else(|| {
                effective_dispatch(cfg, runner.map(Arc::as_ref))
            });

            if cfg.timing.execution_timeout().is_some_and(|execution_timeout| {
                execution_timeout.is_zero()
                    || tokio::time::Instant::now()
                        .checked_add(execution_timeout)
                        .is_none()
            }) {
                return Err(claviron_runtime::registry::Error::msg(format!(
                    "task '{name}' execution_timeout must be greater than zero and representable by the runtime"
                )));
            }

            match &cfg.timing {
                TaskTiming::Manual { .. } => {}
                TaskTiming::Daemon { .. } => {}
                TaskTiming::Interval { every, .. } if every.is_zero() => {
                    return Err(claviron_runtime::registry::Error::msg(
                        format!(
                            "task '{name}' interval must be greater than zero"
                        ),
                    ));
                }
                TaskTiming::Interval { .. } => {}
                TaskTiming::Cron { expression, .. } => {
                    cron::Schedule::from_str(expression).map_err(|err| {
                        claviron_runtime::registry::Error::msg(format!(
                            "task '{name}' has invalid cron expression '{expression}': {err}"
                        ))
                    })?;
                }
            }

            if cfg.concurrency == 0 {
                return Err(claviron_runtime::registry::Error::msg(format!(
                    "task '{name}' concurrency must be greater than zero"
                )));
            }

            if let Some(command) = &cfg.command {
                let actions = [
                    ("exit_action", Some(command.exit_action.is_terminating())),
                    (
                        "reload_action",
                        command
                            .reload_action
                            .map(|action| action.is_terminating()),
                    ),
                ];
                for (field, is_terminating) in
                    actions.into_iter().filter_map(|(field, value)| {
                        value.map(|value| (field, value))
                    })
                {
                    if is_terminating {
                        continue;
                    }
                    if cfg.timing.is_scheduled() {
                        return Err(claviron_runtime::registry::Error::msg(
                            format!(
                                "task '{name}' configures a non-terminating {field}; scheduled commands cannot keep running or receive in-place signals"
                            ),
                        ));
                    }
                    if dispatch.is_controller_driven() && field == "exit_action"
                    {
                        return Err(claviron_runtime::registry::Error::msg(
                            format!(
                                "task '{name}' uses coordinated dispatch with a non-terminating {field}; slot ownership cannot be relinquished while its command remains running"
                            ),
                        ));
                    }
                }
            }

            if let Some(command) = &cfg.command
                && command.restart == RestartPolicy::Always
                && cfg.timing.is_scheduled()
            {
                return Err(claviron_runtime::registry::Error::msg(format!(
                    "task '{name}' uses restart 'always' with a schedule"
                )));
            }

            if dispatch == TaskDispatch::Pool {
                if scope == TaskScope::Master {
                    return Err(claviron_runtime::registry::Error::msg(
                        format!(
                            "task '{name}' uses dispatch 'pool' with scope 'master'; pool requires scope 'workers' or 'fleet'"
                        ),
                    ));
                }
                if !cfg.timing.is_daemon() {
                    return Err(claviron_runtime::registry::Error::msg(
                        format!(
                            "task '{name}' uses dispatch 'pool' with a schedule; pool tasks must be long-running daemons"
                        ),
                    ));
                }

                if !matches!(state.run_mode(), RunMode::Standalone) {
                    needs_fleet = true;
                }
                continue;
            }

            if dispatch != TaskDispatch::One {
                continue;
            }
            if !matches!(state.run_mode(), RunMode::Standalone) {
                needs_fleet = true;
            }
            if cfg.concurrency != 1 {
                return Err(claviron_runtime::registry::Error::msg(format!(
                    "task '{name}' uses dispatch 'one' and must have concurrency: 1"
                )));
            }
        }

        if needs_fleet && state.fleet().is_none() {
            return Err(claviron_runtime::registry::Error::msg(
                "coordinated task dispatch requires a booted Orbit fleet",
            ));
        }

        Ok(())
    }

    /// Register a task that carries its own `tasks.yaml` entry: it exists
    /// without a line in the file, and a line under its name replaces the
    /// entry whole. This is how a builtin task registers.
    pub fn register_builtin<T>(
        &self,
        name: &'static str,
        config: TaskConfig,
        func: T,
    ) -> &Self
    where
        T: Into<TaskRunner>,
    {
        let runner: TaskRunner = func.into();
        self.register_fn(name, runner.with_defaults(config))
    }

    /// Register a task function. Returns `&Self` for chaining.
    ///
    /// # Example
    /// ```ignore
    /// task_service
    ///     .register_fn("caching_gc", cache_gc_task)
    ///     .register_fn("renew_certs", renew_certs);
    /// ```
    pub fn register_fn<T>(
        &self,
        name: &'static str,
        func: T,
    ) -> &Self
    where
        T: Into<TaskRunner>,
    {
        let mut runner: TaskRunner = func.into();
        runner.set_name(name);
        runner.set_runtime_state(self.runtime_state(name));
        let runner = Arc::new(runner);
        let mut guard = self.inner.registry.write_unpoisoned();
        guard.insert(name.to_string(), Arc::clone(&runner));
        drop(guard);
        self
    }

    /// Register all process-based tasks (those with `command:` in config) into the registry.
    ///
    /// Reads from this service's own `tasks` snapshot (loaded by `boot()` /
    /// `reload()` from `tasks.yaml`) and creates `TaskRunner`s that execute
    /// external processes via `process_fn`. Existing entries with the same
    /// name are replaced.
    pub fn register_commands(&self) -> &Self {
        let tasks = self.task_snapshot();
        let mut commands = self.inner.command_runners.write_unpoisoned();
        commands.clear();
        if tasks.is_empty() {
            debug!("TaskService: no tasks in config to register");
            return self;
        }

        for (name, cfg) in tasks.iter() {
            // Only register tasks that have a command spec
            if let Some(spec) = cfg.command.clone() {
                let spec = Arc::new(spec); // capture per-runner
                let graceful_timeout = cfg.graceful_timeout;
                let dispatch = cfg.dispatch.unwrap_or_default();
                let persistent_instance = cfg.timing.is_daemon();
                // TaskRunner expects a 'static name; leak the String safely
                let static_name: &'static str =
                    Box::leak(name.clone().into_boxed_str());

                let mut runner: TaskRunner = TaskRunner::from_with_stop_context(
                    move |state, token, runtime, stop| {
                        let spec = spec.clone();
                        async move {
                            run_command(
                                state,
                                token,
                                stop,
                                &spec,
                                runtime,
                                graceful_timeout,
                                persistent_instance,
                            )
                            .await
                        }
                    },
                );
                runner.set_name(static_name);
                runner.set_task_scope(cfg.scope.unwrap_or_default());
                runner.set_task_dispatch(dispatch);
                runner.set_runtime_state(self.runtime_state(name));

                commands.insert(name.clone(), Arc::new(runner));
            }

            // Self-info gauge: stamp every YAML-defined task (Rust or
            // command) with its `kind` + `schedule` labels so Grafana
            // can join into a self-describing table.
            #[cfg(feature = "metrics")]
            {
                let kind = cfg.executor_kind().as_str();
                let schedule = cfg.timing.label();
                crate::metrics::set_info(name, kind, schedule);
            }
        }

        drop(commands);

        // Service-level `registered` gauge — total registry size
        // after this round of command-task registration.
        #[cfg(feature = "metrics")]
        {
            let count = self.inner.registry.read_unpoisoned().len()
                + self.inner.command_runners.read_unpoisoned().len();
            let count = count as u64;
            crate::metrics::set_registered(count);
        }

        self
    }

    pub fn get(
        &self,
        name: &str,
    ) -> Option<Arc<TaskRunner>> {
        if let Some(active) = self.inner.supervisors.lock_unpoisoned().get(name)
        {
            return Some(Arc::clone(&active.runner));
        }
        if let Some(runner) = self.inner.pool_reloader.runner(name) {
            return Some(runner);
        }
        self.inner.command_runners.read().unwrap().get(name).cloned().or_else(
            || self.inner.registry.read_unpoisoned().get(name).cloned(),
        )
    }

    pub(crate) fn validate_manager_command(
        &self,
        name: &str,
        action: TaskCommandAction,
    ) -> TaskManagerResult<()> {
        let tasks = self.task_snapshot();
        let config = tasks
            .get(name)
            .ok_or_else(|| TaskManagerError::TaskNotFound(name.to_owned()))?;

        // `start` and `stop` move a schedule. Manual timing has none.
        if matches!(action, TaskCommandAction::Start | TaskCommandAction::Stop)
            && config.timing.trigger_enabled().is_none()
        {
            return Err(TaskManagerError::ManualTaskHasNoSchedule(
                name.to_owned(),
            ));
        }
        // `restart` replaces a daemon's generation, so it needs one to replace.
        if action == TaskCommandAction::Restart && !config.timing.is_daemon() {
            return Err(TaskManagerError::RestartRequiresDaemon(
                name.to_owned(),
            ));
        }
        // `reload` asks the task to re-read the configuration it owns — for a
        // command task, the child's own file, reached through `reload_action`.
        // A schedule that produces bounded occurrences owns nothing between
        // them and carries no reload action, so the honest answer is that there
        // was nothing to do. That is a success, not a refusal, and it is
        // applied as one: see the reload arm, which returns without acting.
        if matches!(
            action,
            TaskCommandAction::Restart | TaskCommandAction::Reload
        ) && config.timing.is_daemon()
            && config.timing.trigger_enabled() == Some(false)
        {
            return Err(TaskManagerError::TaskStopped(name.to_owned()));
        }
        Ok(())
    }

    pub(crate) async fn apply_manager_command(
        &self,
        state: &AppState,
        command: TaskCommand,
    ) -> claviron_runtime::registry::Result<()> {
        if state.is_shutting_down() {
            return Err(claviron_runtime::registry::Error::msg(
                TaskManagerError::ShuttingDown.to_string(),
            ));
        }
        let _guard = self.reload_lock.lock().await;
        self.validate_manager_command(&command.name, command.action).map_err(
            |error| claviron_runtime::registry::Error::msg(error.to_string()),
        )?;

        let tasks = self.task_snapshot();
        let config = tasks.get(&command.name).cloned().ok_or_else(|| {
            claviron_runtime::registry::Error::msg(format!(
                "task '{}' disappeared before its management command was applied",
                command.name
            ))
        })?;

        match command.action {
            TaskCommandAction::Start => {
                let config = self
                    .set_task_trigger_enabled(&command.name, true)
                    .map_err(|error| {
                        claviron_runtime::registry::Error::msg(
                            error.to_string(),
                        )
                    })?;
                let targets = self.management_targets(
                    &command.name,
                    config.as_ref(),
                    ReloadAction::Restart,
                );
                let tasks = self.task_snapshot();
                let started =
                    self.start_task_set(state, &tasks, Some(&targets));
                info!(
                    task = command.name,
                    started, "task manager start applied"
                );
            }
            TaskCommandAction::Stop => {
                let targets = self.management_targets(
                    &command.name,
                    &config,
                    ReloadAction::Restart,
                );
                let signalled = self.begin_stop_targets(&targets);
                self.set_task_trigger_enabled(&command.name, false).map_err(
                    |error| {
                        claviron_runtime::registry::Error::msg(
                            error.to_string(),
                        )
                    },
                )?;
                if !self
                    .wait_reload_targets(&targets, self.graceful_timeout())
                    .await
                {
                    return Err(claviron_runtime::registry::Error::msg(
                        format!("task '{}' stop timed out", command.name),
                    ));
                }
                info!(
                    task = command.name,
                    signalled, "task manager stop applied"
                );
            }
            TaskCommandAction::Reload => {
                // Nothing lives between two bounded occurrences, and a schedule
                // carries no reload action to deliver. Reporting that as done
                // is correct; falling back to a restart — which is what an
                // unset `reload_action` used to mean here — was not, because it
                // replaced work the operator had not asked to replace.
                if !config.timing.is_daemon() {
                    info!(
                        task = command.name,
                        "task manager reload had nothing to apply"
                    );
                    return Ok(());
                }
                let action = config
                    .command
                    .as_ref()
                    .and_then(|command| command.reload_action)
                    .unwrap_or(ReloadAction::Restart);

                if let Some(signal) = action.in_place_signal() {
                    let signals =
                        BTreeMap::from([(command.name.clone(), signal)]);
                    self.send_reload_signals(&signals);
                    info!(
                        task = command.name,
                        ?signal,
                        "task manager reload signal applied"
                    );
                    return Ok(());
                }

                let targets =
                    self.management_targets(&command.name, &config, action);
                let changes =
                    [(command.name.clone(), ReloadTaskChange::Unchanged)];
                let signalled =
                    self.begin_reload_targets(&targets, &tasks, &changes);
                if !self
                    .wait_reload_targets(&targets, self.graceful_timeout())
                    .await
                {
                    return Err(claviron_runtime::registry::Error::msg(
                        format!("task '{}' reload timed out", command.name),
                    ));
                }
                let started =
                    self.start_task_set(state, &tasks, Some(&targets));
                info!(
                    task = command.name,
                    signalled, started, "task manager reload applied"
                );
            }
            TaskCommandAction::Restart => {
                let targets = self.management_targets(
                    &command.name,
                    &config,
                    ReloadAction::Restart,
                );
                let signalled = self.begin_stop_targets(&targets);
                if !self
                    .wait_reload_targets(&targets, self.graceful_timeout())
                    .await
                {
                    return Err(claviron_runtime::registry::Error::msg(
                        format!("task '{}' restart timed out", command.name),
                    ));
                }
                let started =
                    self.start_task_set(state, &tasks, Some(&targets));
                info!(
                    task = command.name,
                    signalled, started, "task manager restart applied"
                );
            }
        }

        Ok(())
    }

    pub(crate) async fn run_explicit(
        &self,
        state: &AppState,
        name: &str,
        run_id: u64,
        execution_timeout_override: Option<Duration>,
    ) -> TaskManagerResult<TaskResult<()>> {
        if state.is_shutting_down() {
            return Err(TaskManagerError::ShuttingDown);
        }
        let tasks = self.task_snapshot();
        let config = tasks
            .get(name)
            .cloned()
            .ok_or_else(|| TaskManagerError::TaskNotFound(name.to_owned()))?;
        let Some(configured_timeout) = config.timing.execution_timeout() else {
            return Err(TaskManagerError::DaemonRequiresStart(name.to_owned()));
        };
        let execution_timeout =
            execution_timeout_override.unwrap_or(configured_timeout);
        let runner = match config.executor() {
            TaskExecutor::Builtin => {
                self.inner.registry.read_unpoisoned().get(name).cloned()
            }
            TaskExecutor::Command(_) => {
                self.inner.command_runners.read_unpoisoned().get(name).cloned()
            }
            TaskExecutor::Php(_)
            | TaskExecutor::Js(_)
            | TaskExecutor::Lua(_) => {
                self.inner.engine_runners.read_unpoisoned().get(name).cloned()
            }
        }
        .ok_or_else(|| {
            TaskManagerError::TaskRunnerUnavailable(name.to_owned())
        })?;

        let scope = effective_scope(&config, Some(&runner));
        let dispatch = effective_dispatch(&config, Some(&runner));
        if !scope.includes(state.run_mode()) {
            return Err(TaskManagerError::TaskIneligible(name.to_owned()));
        }

        let token = state.shutdown_token().child_token();
        let stop = StopContext::default();
        let completion = {
            // Register the request-scoped waiter before starting the run. The
            // same lock guards completion removal, so even a body that returns
            // immediately cannot finish before its waiter is visible.
            // Never retain this lock while awaiting the result.
            let mut explicit_runs = self.inner.explicit_runs.lock_unpoisoned();
            let (completion, waiter) = oneshot::channel();
            let task_runs = explicit_runs.entry(name.to_owned()).or_default();
            let previous = task_runs.insert(
                run_id,
                ActiveExplicitRun {
                    token: token.clone(),
                    stop: stop.clone(),
                    completion,
                },
            );
            debug_assert!(
                previous.is_none(),
                "local task run ids must be unique"
            );

            let service = self.clone();
            let task_name = name.to_owned();
            let run_state = state.clone();
            info!(task = task_name, run_id, "explicit task run started");
            tokio::spawn(async move {
                let result = std::panic::AssertUnwindSafe(runner.run_explicit(
                    run_state,
                    config,
                    ExecutionPlacement::new(scope, dispatch),
                    token,
                    stop,
                    execution_timeout,
                ))
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| {
                    Err(Error::other(format!(
                        "task execution panicked: {}",
                        panic_message(panic.as_ref())
                    )))
                });
                service.finish_explicit_run(&task_name, run_id, result);
            });
            waiter
        };

        match completion.await {
            Ok(result) => Ok(result),
            Err(_) => Ok(Err(Error::other(
                "task execution completion channel closed",
            ))),
        }
    }

    fn finish_explicit_run(
        &self,
        name: &str,
        run_id: u64,
        result: TaskResult<()>,
    ) {
        let mut explicit_runs = self.inner.explicit_runs.lock_unpoisoned();
        let (completion, remove_task) = match explicit_runs.get_mut(name) {
            Some(runs) => {
                let completion = runs.remove(&run_id).map(|run| run.completion);
                (completion, runs.is_empty())
            }
            None => (None, false),
        };
        if remove_task {
            explicit_runs.remove(name);
        }
        drop(explicit_runs);

        if let Some(completion) = completion {
            let _ = completion.send(result);
        }
    }

    fn cancel_explicit_runs(
        &self,
        name: &str,
    ) -> usize {
        let explicit_runs = self.inner.explicit_runs.lock_unpoisoned();
        let Some(runs) = explicit_runs.get(name) else {
            return 0;
        };
        for run in runs.values() {
            // Reload must reap a command even when normal process shutdown is
            // configured to keep it running.
            run.stop.set_reload_action(ReloadAction::Restart);
            run.token.cancel();
        }
        runs.len()
    }

    fn management_targets(
        &self,
        name: &str,
        config: &TaskConfig,
        action: ReloadAction,
    ) -> ReloadTargets {
        let registry = self.inner.registry.read_unpoisoned();
        let runner = registry.get(name).map(Arc::as_ref);
        let mut targets = ReloadTargets::default();
        if effective_dispatch(config, runner).is_controller_driven() {
            targets.pools.insert(name.to_owned(), action);
        } else {
            targets.supervisors.insert(name.to_owned());
        }
        targets
    }

    /// Spawn supervisors for all tasks in the current (ArcSwap) config and return how many were spawned.
    /// Non-blocking: only spawns; does not wait for supervisors.
    fn start_tasks(
        &self,
        state: &AppState,
    ) -> Option<usize> {
        let tasks = self.task_snapshot();
        if tasks.is_empty() {
            return None;
        }

        Some(self.start_task_set(state, &tasks, None))
    }

    /// Start every task at boot, or only the selected task generations after
    /// an incremental reload.
    fn start_task_set(
        &self,
        state: &AppState,
        tasks: &TaskTable,
        selected: Option<&ReloadTargets>,
    ) -> usize {
        // Read-lock registries for point lookups (no full clone).
        let registry = self.inner.registry.read_unpoisoned();
        let commands = self.inner.command_runners.read_unpoisoned();
        let engines = self.inner.engine_runners.read_unpoisoned();

        let mut spawned = 0usize;
        for (name, cfg) in tasks.iter() {
            // A disabled entry is not asked for a runner: a task this build
            // does not carry, kept in the file switched off, is not a fault.
            if cfg.timing.trigger_enabled() != Some(true) {
                continue;
            }
            // A scope the file states rules this process out before any
            // runner is asked for: the master carries no PHP engine, and a
            // worker-scoped PHP task is not its concern.
            if cfg.scope.is_some_and(|scope| !scope.includes(state.run_mode()))
            {
                continue;
            }
            let runner = match cfg.executor() {
                TaskExecutor::Builtin => registry.get(name.as_str()).cloned(),
                TaskExecutor::Command(_) => {
                    commands.get(name.as_str()).cloned()
                }
                TaskExecutor::Php(_)
                | TaskExecutor::Js(_)
                | TaskExecutor::Lua(_) => engines.get(name.as_str()).cloned(),
            };
            let Some(runner) = runner else {
                warn!("Unregistered Task '{}' — skipping", name);
                continue;
            };

            let scope = cfg.scope.unwrap_or_else(|| runner.task_scope());
            let dispatch =
                cfg.dispatch.unwrap_or_else(|| runner.task_dispatch());

            if !scope.includes(state.run_mode()) {
                continue;
            }

            let st = state.clone();
            let cf = cfg.clone();
            if dispatch.is_controller_driven() {
                if selected
                    .is_some_and(|targets| !targets.pools.contains_key(name))
                {
                    continue;
                }
                let task =
                    crate::pool::PoolTask::new(runner, cf, scope, dispatch);
                if self.inner.pool_reloader.start(
                    task,
                    st,
                    self.inner.pool.load().lease_ttl,
                ) {
                    spawned += 1;
                }
            } else {
                if selected
                    .is_some_and(|targets| !targets.supervisors.contains(name))
                {
                    continue;
                }
                let can_start = {
                    let mut supervisors =
                        self.inner.supervisors.lock_unpoisoned();
                    match supervisors.get(name) {
                        Some(active)
                            if !active.handle.is_finished()
                                || active.runner.is_running() =>
                        {
                            false
                        }
                        Some(_) => {
                            supervisors.remove(name);
                            true
                        }
                        None => true,
                    }
                };
                if !can_start {
                    warn!(
                        task = name,
                        "task supervisor is already active; skipping duplicate"
                    );
                    continue;
                }

                // Reset only after proving that no active generation still
                // owns the runner's previous cancellation token.
                runner.reset_token();
                runner.begin_generation();
                let active_runner = Arc::clone(&runner);
                let handle = if let TaskTiming::Cron { expression, .. } =
                    &cf.timing
                {
                    let context = crate::runner::ScheduleContext {
                        worker_number: 1,
                        state: st,
                        cfg: Arc::clone(&cf),
                        scope,
                        dispatch,
                        token: runner.token().child_token(),
                    };
                    match self.inner.scheduler.register(
                        expression,
                        Arc::clone(&runner),
                        context,
                    ) {
                        Ok(handle) => SupervisorHandle::Schedule(handle),
                        Err(error) => {
                            error!(task = name, %error, "cron registration failed");
                            continue;
                        }
                    }
                } else {
                    SupervisorHandle::Execution(tokio::spawn(
                        runner.run_supervisor(st, cf, scope, dispatch),
                    ))
                };
                self.inner.supervisors.lock_unpoisoned().insert(
                    name.clone(),
                    ActiveSupervisor {
                        phase: SupervisorPhase::Running,
                        runner: active_runner,
                        handle,
                    },
                );
                spawned += 1;
            }
        }

        debug!("🧩 Spawned {} supervisor(s)", spawned);
        spawned
    }

    /// Send cancel signal to **all** registered task runners.
    /// Returns the number of runners signaled (registered count).
    pub fn cancel_all(&self) -> usize {
        self.cancel_all_for(StopCause::Shutdown)
    }

    fn cancel_all_for(
        &self,
        cause: StopCause,
    ) -> usize {
        let mut supervisors = self.inner.supervisors.lock_unpoisoned();
        for active in supervisors.values_mut() {
            active.phase = SupervisorPhase::Stopping;
            active.runner.cancel_for(cause);
            active.handle.stop_schedule();
        }
        let mut count = supervisors.len();
        drop(supervisors);

        count += self.inner.pool_reloader.cancel_all(cause);

        let explicit_runs = self.inner.explicit_runs.lock_unpoisoned();
        for runs in explicit_runs.values() {
            for run in runs.values() {
                run.stop.set(cause);
                run.token.cancel();
                count += 1;
            }
        }
        drop(explicit_runs);

        if count == 0 {
            debug!("TaskService stop: no registered tasks");
        }

        count
    }

    fn begin_stop_targets(
        &self,
        targets: &ReloadTargets,
    ) -> usize {
        let mut supervisors = self.inner.supervisors.lock_unpoisoned();
        let mut count = 0;
        for name in &targets.supervisors {
            let Some(active) = supervisors.get_mut(name) else {
                continue;
            };
            active.phase = SupervisorPhase::Stopping;
            // Disabling a managed trigger must stop its automatic body even
            // when an external command's process-exit policy is `keep-running`.
            active.runner.cancel_for_reload(ReloadAction::Restart);
            active.handle.stop_schedule();
            count += 1;
        }
        drop(supervisors);

        count += self.inner.pool_reloader.begin_reload(&targets.pools);
        count
    }

    fn reload_targets(
        &self,
        active: &TaskTable,
        candidate: &TaskTable,
        changes: &[(String, ReloadTaskChange)],
        pool_changed: bool,
    ) -> ReloadTargets {
        let registry = self.inner.registry.read_unpoisoned();
        let mut targets = ReloadTargets::default();

        for (name, change) in changes {
            let runner = registry.get(name.as_str()).map(Arc::as_ref);
            let active = active
                .get(name)
                .map(Arc::as_ref)
                .filter(|cfg| cfg.timing.trigger_enabled() != Some(false));
            let candidate = candidate
                .get(name)
                .map(Arc::as_ref)
                .filter(|cfg| cfg.timing.trigger_enabled() != Some(false));
            let active_pool = active.is_some_and(|cfg| {
                effective_dispatch(cfg, runner).is_controller_driven()
            });
            let candidate_pool = candidate.is_some_and(|cfg| {
                effective_dispatch(cfg, runner).is_controller_driven()
            });
            let reload_action = candidate
                .and_then(|cfg| cfg.command.as_ref())
                .and_then(|command| command.reload_action);
            if *change == ReloadTaskChange::Unchanged
                && !(pool_changed && (active_pool || candidate_pool))
                && let Some(signal) =
                    reload_action.and_then(ReloadAction::in_place_signal)
            {
                targets.signals.insert(name.clone(), signal);
                continue;
            }
            let needs_transition = *change != ReloadTaskChange::Unchanged
                || reload_action.is_some();

            if (pool_changed || needs_transition)
                && (active_pool || candidate_pool)
            {
                let action = if !pool_changed
                    && *change == ReloadTaskChange::Unchanged
                {
                    candidate
                        .and_then(|cfg| cfg.command.as_ref())
                        .and_then(|command| command.reload_action)
                        .unwrap_or(ReloadAction::Restart)
                } else {
                    // A changed task plan must stop its current generation
                    // before the candidate can become active.
                    ReloadAction::Restart
                };
                targets.pools.insert(name.clone(), action);
            }
            if needs_transition && (!active_pool || !candidate_pool) {
                targets.supervisors.insert(name.clone());
            }
        }

        targets
    }

    fn send_reload_signals(
        &self,
        targets: &BTreeMap<String, ProcessSignal>,
    ) {
        for (name, signal) in targets {
            // The command catalog may have been rebuilt for other tasks.
            // Signal only the runner owned by this active generation.
            let runner = self
                .inner
                .supervisors
                .lock()
                .unwrap()
                .get(name)
                .filter(|active| active.phase == SupervisorPhase::Running)
                .map(|active| Arc::clone(&active.runner))
                .or_else(|| self.inner.pool_reloader.runner(name))
                .or_else(|| {
                    self.inner
                        .command_runners
                        .read_unpoisoned()
                        .get(name)
                        .cloned()
                })
                .or_else(|| {
                    self.inner.registry.read_unpoisoned().get(name).cloned()
                });
            if let Some(runner) = runner {
                runner.reload_signal(*signal);
            }
        }
    }

    fn begin_reload_targets(
        &self,
        targets: &ReloadTargets,
        candidate: &TaskTable,
        changes: &[(String, ReloadTaskChange)],
    ) -> usize {
        let mut supervisors = self.inner.supervisors.lock_unpoisoned();
        let mut count = 0;
        for name in &targets.supervisors {
            let Some(active) = supervisors.get_mut(name) else {
                continue;
            };
            let change = changes
                .iter()
                .find_map(|(task, change)| (task == name).then_some(*change))
                .unwrap_or(ReloadTaskChange::Stop);
            let action = if change == ReloadTaskChange::Unchanged {
                candidate
                    .get(name)
                    .and_then(|cfg| cfg.command.as_ref())
                    .and_then(|command| command.reload_action)
                    .unwrap_or(ReloadAction::Restart)
            } else {
                // A changed task plan must stop its current generation before
                // the candidate can become active, regardless of its normal
                // reload preference.
                ReloadAction::Restart
            };
            active.phase = SupervisorPhase::Stopping;
            active.runner.cancel_for_reload(action);
            active.handle.stop_schedule();
            count += 1;
        }
        drop(supervisors);

        count += self.inner.pool_reloader.begin_reload(&targets.pools);
        for name in targets.supervisors.iter().chain(targets.pools.keys()) {
            count += self.cancel_explicit_runs(name);
        }

        count
    }

    async fn wait_reload_targets(
        &self,
        targets: &ReloadTargets,
        timeout: Option<Duration>,
    ) -> bool {
        use tokio::time::{Instant, sleep};

        let started = Instant::now();
        loop {
            let pending_supervisors = {
                let supervisors = self.inner.supervisors.lock_unpoisoned();
                targets
                    .supervisors
                    .iter()
                    .filter_map(|name| supervisors.get(name))
                    .filter(|active| {
                        !active.handle.is_finished()
                            || active.runner.is_running()
                    })
                    .count()
            };
            let pending_pool =
                self.inner.pool_reloader.pending_reload(&targets.pools);
            let pending_manual = {
                let explicit_runs = self.inner.explicit_runs.lock_unpoisoned();
                targets
                    .supervisors
                    .iter()
                    .chain(targets.pools.keys())
                    .map(|name| explicit_runs.get(name).map_or(0, HashMap::len))
                    .sum::<usize>()
            };
            let pending = pending_supervisors + pending_pool + pending_manual;

            if pending == 0 {
                self.inner
                    .supervisors
                    .lock()
                    .unwrap()
                    .retain(|name, _| !targets.supervisors.contains(name));
                self.inner.pool_reloader.finish_reload(&targets.pools);
                return true;
            }

            if timeout.is_some_and(|timeout| started.elapsed() >= timeout) {
                error!(
                    pending,
                    "TaskService reload timed out while draining task state"
                );
                return false;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn running_tasks_count(&self) -> usize {
        let supervisors = self
            .inner
            .supervisors
            .lock()
            .unwrap()
            .values()
            .filter(|active| {
                !active.handle.is_finished() || active.runner.is_running()
            })
            .count();
        let pools = self.inner.pool_reloader.running_count();
        let explicit_runs = self
            .inner
            .explicit_runs
            .lock_unpoisoned()
            .values()
            .map(HashMap::len)
            .sum::<usize>();
        supervisors + pools + explicit_runs
    }

    /// Wait until all registered runners report `is_running() == false`.
    /// If `timeout` is Some(dur), log a warning and return early after the duration.
    async fn wait_all_stopped(
        &self,
        timeout: Option<Duration>,
    ) {
        use tokio::time::{Instant, sleep};
        let start = Instant::now();
        debug!(
            "⌛ TaskService stop: waiting for all running tasks to finish...(timeout :{timeout:?}"
        );
        loop {
            let running = self.running_tasks_count();
            if running == 0 {
                debug!("🔸 TaskService stop: all tasks stopped");
                break;
            }
            if let Some(t) = timeout
                && start.elapsed() > t
            {
                error!(
                    "⛔ TaskService stop: Shutdown forced,  {} task(s) still marked running",
                    running,
                );
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Run one task-service generation: spawn supervisors, wait for its
    /// [`RunContext`] cancellation, then stop and drain owned task work.
    pub async fn serve(
        &self,
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        let count = self.start_tasks(&state);

        if let Some(count) = count
            && count > 0
        {
            tracing::debug!(
                "TaskService: spawned {count} supervisor(s) — starting -"
            );
        } else {
            tracing::info!(
                "⚪ No background tasks active — task management remains available"
            );
        }

        let mut interval = self.snapshot_config().publish_interval;
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = context.cancelled() => break,
                _ = ticker.tick(), if !state.is_standalone() => {
                    let current = self.snapshot_config().publish_interval;
                    if current != interval {
                        interval = current;
                        ticker = tokio::time::interval(interval);
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                    if let Err(error) = self.publish_local_snapshot(&state) {
                        error!(%error, "failed to publish task management snapshot");
                    }
                }
            }
        }

        // Stop every task generation after this service generation is cancelled.
        #[cfg(debug_assertions)]
        debug!("🔸 shutting down TaskService supervisors");

        #[cfg(debug_assertions)]
        {
            let sent = self.cancel_all();
            debug!(
                "TaskService: sent cancel to all {sent} task(s) — shutting down"
            );
        }
        #[cfg(not(debug_assertions))]
        self.cancel_all();

        self.wait_all_stopped(self.graceful_timeout()).await;

        // Stopping has to say so, once, or nobody learns it happened. The
        // publish loop above ended with the cancellation that started this
        // stop, so everything the stop then did went unwritten, and a reader
        // keeps whatever landed last — which is whichever state a task was
        // in when the final tick fired.
        //
        // The supervisor entries are dropped first, and that order is the
        // whole point: `cancel_all` leaves them in `Stopping`, so publishing
        // over them would write `Stopping` as this member's permanent last
        // word. With them gone the snapshot reports each task by its
        // configuration alone — inactive, disabled, ineligible — which is
        // what "not running here" actually is.
        self.inner.supervisors.lock_unpoisoned().clear();
        if !state.is_standalone()
            && let Err(error) = self.publish_local_snapshot(&state)
        {
            error!(%error, "failed to publish the final task management snapshot");
        }

        info!("❎ TaskService stopped successfully");
        Ok(())
    }
}

impl Component<AppState> for TaskService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "TaskService"
    }

    /// Snapshots are published into this ring. The fleet itself is a
    /// use-time lookup: boot does not touch it, and a failed publish is
    /// logged, not fatal.
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<claviron_orbit::shm::ManagedShmProvider<
            claviron_orbit::shm::ReusableOrbitRing<
                crate::snapshot::TaskSnapshotRecord,
            >,
        >>()
    }

    fn validate(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.validate_task_policies(state)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    /// Tasks run wherever their own scope says — master, workers, or
    /// either — so the service that schedules and publishes them runs in
    /// every process.
    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_runnable(
        self: std::sync::Arc<Self>
    ) -> Option<std::sync::Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState> for TaskService {
    async fn boot(
        &self,
        _deps: (),
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.load_tasks(state).await?;
        self.register_commands();
        self.register_engine_tasks();
        Ok(())
    }
}

/// `tasks.yaml` is owned here. The document stays the reader's until boot:
/// other providers register their builtin tasks in their own `configure`, and
/// the table folds those in, so it is materialized once every configure has
/// run. An absent file is the empty document — the builtin table alone.
impl Scoped for TaskService {
    fn scope(&self) -> Scope {
        Scope::Any
    }
}

#[async_trait]
impl Configurable for TaskService {
    type Config = TaskServiceConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.inner.config_path)
    }

    fn absent_source(&self) -> Option<TaskServiceConfig> {
        Some(TaskServiceConfig::default())
    }

    async fn configure(
        &self,
        _config: &TaskServiceConfig,
    ) -> congif::Result<()> {
        Ok(())
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for TaskService {
    async fn config_changed(
        &self,
        _config: &TaskServiceConfig,
    ) -> congif::Result<()> {
        if !self.inner.loaded.load(Ordering::Acquire) {
            // Boot takes the newest preparation itself.
            return Ok(());
        }
        let state = self.inner.state.get().ok_or_else(|| {
            congif::Error::msg(
                "TaskService was not attached to its application",
            )
        })?;
        Reloadable::reload(self, state).await.map_err(congif::Error::msg)
    }
}

#[async_trait]
impl Reloadable<AppState> for TaskService {
    fn priority(&self) -> Option<u8> {
        Some(10)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        #[cfg(feature = "metrics")]
        crate::metrics::inc_reload();

        // Claim the whole reload transaction before reading config. Otherwise
        // a coalesced signal could publish a new snapshot without restarting
        // the supervisors that consume it.
        let Ok(guard) = Arc::clone(&self.reload_lock).try_lock_owned() else {
            // Already reloading; coalesce this signal
            debug!(
                "TaskService reload: coalesced (reload already in progress)"
            );
            return Ok(());
        };
        let _guard = guard;

        // Re-read and validate before the old runners are touched. Keep the
        // candidate private while the current runtime begins its reload
        // transition; config reads must not get ahead of that transition.
        let candidate = self.read_task_config(state).await?;
        let active = self.task_snapshot();
        let changes = self.classify_reload(&active, &candidate.tasks);
        let changed = changes
            .iter()
            .filter(|(_, change)| *change != ReloadTaskChange::Unchanged)
            .count();
        let pool_changed =
            self.inner.pool.load().lease_ttl != candidate.pool.lease_ttl;
        let explicit_actions = candidate
            .tasks
            .values()
            .filter(|cfg| cfg.timing.trigger_enabled() != Some(false))
            .filter_map(|cfg| cfg.command.as_ref())
            .filter(|command| command.reload_action.is_some())
            .count();
        debug!(
            tasks = changes.len(),
            changed,
            pool_changed,
            explicit_actions,
            "TaskService reload plan classified"
        );
        for (task, change) in changes
            .iter()
            .filter(|(_, change)| *change != ReloadTaskChange::Unchanged)
        {
            debug!(
                task,
                change = change.label(),
                "TaskService reload plan entry"
            );
        }

        let targets = self.reload_targets(
            &active,
            &candidate.tasks,
            &changes,
            pool_changed,
        );

        // Without transitions, preserve every active generation. Signals only
        // notifies its live children; absent reload actions remain a no-op.
        if !targets.has_transitions() {
            self.publish_task_config(candidate);
            self.send_reload_signals(&targets.signals);
            debug!("TaskService reload: active task plan unchanged");
            return Ok(());
        }

        let graceful_timeout = candidate.graceful_timeout;

        let sent =
            self.begin_reload_targets(&targets, &candidate.tasks, &changes);
        debug!(
            supervisors = targets.supervisors.len(),
            pools = targets.pools.len(),
            sent,
            "🔸 TaskService reload: draining selected task state"
        );

        if !self.wait_reload_targets(&targets, graceful_timeout).await {
            return Err(claviron_runtime::registry::Error::msg(
                "TaskService reload timed out; candidate config was not published",
            ));
        }

        self.publish_task_config(candidate);
        self.register_commands();
        self.register_engine_tasks();
        let tasks = self.task_snapshot();
        let count = self.start_task_set(state, &tasks, Some(&targets));
        self.send_reload_signals(&targets.signals);
        debug!(count, "🔁 TaskService: targeted reload completed");
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState> for TaskService {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        self.serve(state, context).await
    }
}

#[cfg(test)]
#[path = "../tests/service.rs"]
mod tests;
