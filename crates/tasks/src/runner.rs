use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use claviron_helpers::sync::LockUnpoisoned;
use claviron_runtime::{AppState, RunMode};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::process::{ProcessSignal, ReloadAction};
use crate::snapshot::{TaskExecutionResult, TaskRuntimeState};
use crate::{
    Error, Reason, TaskConfig, TaskDispatch, TaskResult, TaskScope, TaskTiming,
};

/// After this many seconds of continuous run, the next failure resets the
/// retry attempt counter back to 1 — a long-lived task that finally
/// crashes shouldn't get the harshest exponential delay applied.
const BACKOFF_RESET_SECS: u64 = 300;

/// Hard ceiling on the per-attempt exponential backoff delay. Without
/// this, attempt 8+ would pile on minutes of sleep between retries.
const MAX_BACKOFF_SECS: u64 = 60;

type TaskBody = dyn Fn(
        AppState,
        CancellationToken,
        TaskRuntime,
        StopContext,
    ) -> BoxFuture<'static, TaskResult<()>>
    + Send
    + Sync
    + 'static;

struct ExecutionGuard {
    runtime_state: Arc<TaskRuntimeState>,
    completed: bool,
}

impl ExecutionGuard {
    fn new(runtime_state: Arc<TaskRuntimeState>) -> Self {
        Self { runtime_state, completed: false }
    }

    fn complete(
        &mut self,
        result: TaskExecutionResult,
    ) {
        self.runtime_state.record(result);
        self.completed = true;
    }
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.runtime_state.record(TaskExecutionResult::Failed);
        }
    }
}

/// One unit of cancellable, retryable work managed by
/// [`TaskService`](crate::service::TaskService).
///
/// Wraps an `async` function (or external process) into a re-entrant
/// supervisor that the service can spawn under an automatic trigger
/// (cron / interval / daemon — see [`TaskRunner::run_supervisor`]) or invoke
/// explicitly when the timing policy is bounded (manual / cron / interval).
/// Holds enough state to drive cancellation, retries, and overlap
/// suppression without the caller writing any of it.
///
/// **Cancellation:** the runner owns a [`CancellationToken`] under a
/// `Mutex`. `cancel()` flips it; the supervisor and every spawned worker
/// observe it via `select!` against `token.cancelled()`. After a stop /
/// reload cycle, `reset_token()` re-arms a fresh token so the next
/// `run_supervisor` call doesn't see a stale cancelled flag.
///
/// **Liveness counting:** every attempt increments the logical task's
/// generation-independent `in_flight` state on entry and decrements on drop
/// (via an internal `RunningGuard`). The service uses this to drain on shutdown
/// and to build management snapshots.
///
/// **Overlap suppression:** `interval_limit` is a `Semaphore(1)`. When
/// a tick fires while the previous attempt is still in flight, the
/// permit is unavailable → the tick is logged and dropped, never
/// queued. Prevents pile-up if a task drifts longer than its schedule.
/// Explicit runs are independent one-shot invocations. They do not consult the
/// automatic supervisor's concurrency or overlap state; a task that needs
/// serialization owns that policy itself.
///
/// **Placement and dispatch:** registration supplies the default
/// [`TaskScope`] and [`TaskDispatch`]. `tasks.yaml` may override either
/// before the supervisor is spawned.
pub struct TaskRunner {
    name: Arc<str>,
    inner: Arc<TaskBody>,
    task_scope: TaskScope,
    task_dispatch: TaskDispatch,
    cancel: Mutex<CancellationToken>,
    stop: StopContext,
    runtime_state: Arc<TaskRuntimeState>,
    interval_limit: Arc<Semaphore>,
    /// The entry this task supplies for itself when `tasks.yaml` has none.
    /// A file entry replaces it whole; it is a default, not a floor.
    defaults: Option<Arc<TaskConfig>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum StopCause {
    #[default]
    Shutdown = 0,
    Reload = 1,
    ExecutionTimeout = 2,
}

/// Shared process-control state: cancellation metadata and in-place reloads.
struct StopState {
    cause: AtomicU8,
    reload_action: Mutex<Option<ReloadAction>>,
    reload_signal: watch::Sender<Option<ProcessSignal>>,
}

impl Default for StopState {
    fn default() -> Self {
        Self {
            cause: AtomicU8::new(StopCause::Shutdown as u8),
            reload_action: Mutex::new(None),
            reload_signal: watch::channel(None).0,
        }
    }
}

#[derive(Clone)]
pub struct StopContext {
    shared: Arc<StopState>,
    execution_timed_out: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
pub(crate) struct ExecutionPlacement {
    scope: TaskScope,
    dispatch: TaskDispatch,
}

impl ExecutionPlacement {
    pub(crate) const fn new(
        scope: TaskScope,
        dispatch: TaskDispatch,
    ) -> Self {
        Self { scope, dispatch }
    }
}

impl Default for StopContext {
    fn default() -> Self {
        Self {
            shared: Arc::new(StopState::default()),
            execution_timed_out: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl StopContext {
    /// Create per-occurrence timeout state while retaining this runner
    /// generation's reload and shutdown control plane.
    fn for_execution(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            execution_timed_out: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Subscribe for this live child only. A new subscriber does not replay a
    /// reload from before it started; pending reloads coalesce to the latest signal.
    pub(crate) fn subscribe_reload_signals(
        &self
    ) -> watch::Receiver<Option<ProcessSignal>> {
        self.shared.reload_signal.subscribe()
    }

    pub(crate) fn send_reload_signal(
        &self,
        signal: ProcessSignal,
    ) {
        // No receiver means there is no live child to notify (e.g. backoff).
        let _ = self.shared.reload_signal.send(Some(signal));
    }

    #[inline]
    pub(crate) fn cause(&self) -> StopCause {
        if self.execution_timed_out.load(Ordering::SeqCst) {
            return StopCause::ExecutionTimeout;
        }
        match self.shared.cause.load(Ordering::SeqCst) {
            value if value == StopCause::Reload as u8 => StopCause::Reload,
            value if value == StopCause::ExecutionTimeout as u8 => {
                StopCause::ExecutionTimeout
            }
            _ => StopCause::Shutdown,
        }
    }

    #[inline]
    pub(crate) fn set(
        &self,
        cause: StopCause,
    ) {
        if cause == StopCause::ExecutionTimeout {
            self.execution_timed_out.store(true, Ordering::SeqCst);
            return;
        }
        self.shared.cause.store(cause as u8, Ordering::SeqCst);
        if cause == StopCause::Shutdown {
            *self.shared.reload_action.lock_unpoisoned() = None;
        }
    }

    #[inline]
    pub(crate) fn set_reload_action(
        &self,
        action: ReloadAction,
    ) {
        *self.shared.reload_action.lock_unpoisoned() = Some(action);
        self.shared.cause.store(StopCause::Reload as u8, Ordering::SeqCst);
    }

    #[inline]
    pub(crate) fn reload_action(&self) -> Option<ReloadAction> {
        *self.shared.reload_action.lock_unpoisoned()
    }
}

/// Runtime context handed to a task on every attempt.
///
/// Tasks that need to identify themselves (logs, metrics, external
/// process env vars, audit trails) read from this struct instead of
/// re-resolving the same data from the registry. The task name shares owned
/// storage with its runner, keeping metadata valid after runner retirement.
///
/// Field meanings:
/// - `task_name` — the registered name (matches `tasks.yaml` key).
/// - `task_worker_number` — 1-indexed concurrency slot. A task with
///   `concurrency: 4` produces workers numbered 1..=4. Under
///   `dispatch: pool`, this is the stable fleet slot identity; ownership may
///   move to another runtime without changing the number.
/// - `attempt` — 1-indexed retry counter. If retry policy permits another
///   attempt after an execution lasting at least 300 seconds, that attempt
///   resets to 1; reaching the attempt limit still stops retries.
/// - `task_scope` — eligible runtime roles (`master` / `workers` / `fleet`).
/// - `task_dispatch` — execution distribution (`each` / `one` / `pool`).
/// - `run_mode` — the **process** role from the runtime
///   (`Master` / `Worker(id)` / `Standalone`). Distinct from
///   `task_scope` — a `TaskScope::Fleet` task running in a
///   `RunMode::Worker(2)` process sees both values.
/// - `worker_count` — this process's bootstrap worker count, not live membership
///   captured for this attempt.
/// - `process_id` — `std::process::id()`. Useful for external-process
///   tasks (passed through as `CLAVIRON_PROCESS_ID` env var).
#[derive(Debug, Clone)]
pub struct TaskRuntime {
    pub task_name: Arc<str>,
    pub task_worker_number: u8,
    pub attempt: u32,
    pub task_scope: TaskScope,
    pub task_dispatch: TaskDispatch,
    pub run_mode: RunMode,
    pub worker_count: u8,
    pub process_id: u32,
}

impl TaskRuntime {
    /// The identity a task's body is entitled to know, in the spelling every
    /// executor uses.
    ///
    /// Written once because it is read twice: a command task receives these as
    /// process environment, and a PHP task as program environment. They were
    /// the command executor's alone until the `php` kind arrived, and a program
    /// converted from `command:` to `php:` would otherwise have lost its
    /// identity without a word.
    pub fn identity_env(&self) -> Vec<(&'static str, String)> {
        let mut env = vec![
            ("CLAVIRON_TASK_NAME", self.task_name.to_string()),
            ("CLAVIRON_TASK_SLOT", self.slot().to_string()),
            ("CLAVIRON_TASK_ATTEMPT", self.attempt.to_string()),
            (
                "CLAVIRON_TASK_SCOPE",
                match self.task_scope {
                    TaskScope::Master => "master",
                    TaskScope::Workers => "workers",
                    TaskScope::Fleet => "fleet",
                }
                .to_owned(),
            ),
            (
                "CLAVIRON_TASK_DISPATCH",
                match self.task_dispatch {
                    TaskDispatch::Each => "each",
                    TaskDispatch::One => "one",
                    TaskDispatch::Pool => "pool",
                }
                .to_owned(),
            ),
            ("CLAVIRON_RUN_MODE", self.run_mode.as_env_value().to_owned()),
            ("CLAVIRON_PROCESS_ID", self.process_id.to_string()),
        ];
        if let RunMode::Worker(worker_id) = self.run_mode {
            env.push(("CLAVIRON_WORKER_NUMBER", worker_id.raw().to_string()));
        }
        if !matches!(self.run_mode, RunMode::Standalone) {
            env.push(("CLAVIRON_WORKER_COUNT", self.worker_count.to_string()));
        }
        env
    }

    /// The 1-indexed concurrency slot for this attempt. Under pool dispatch,
    /// this identity is stable across owner failover.
    pub const fn slot(&self) -> u8 {
        self.task_worker_number
    }
}

impl Clone for TaskRunner {
    fn clone(&self) -> Self {
        let token = {
            let g = self.cancel.lock_unpoisoned();
            g.clone()
        };
        Self {
            name: Arc::clone(&self.name),
            inner: self.inner.clone(),
            task_scope: self.task_scope,
            task_dispatch: self.task_dispatch,
            cancel: Mutex::new(token),
            stop: self.stop.clone(),
            runtime_state: self.runtime_state.clone(),
            interval_limit: self.interval_limit.clone(),
            defaults: self.defaults.clone(),
        }
    }
}

/// Everything a schedule loop needs that is not the schedule itself.
///
/// The sibling of [`RunContext`], and introduced for the same reason: the
/// loops had grown past the point where a reader could tell the arguments
/// apart. `token` is the one worth naming — it is the *slot's* token under the
/// controller and the runner's own under a supervisor, and the difference is
/// what lets a lost lease stop a schedule without ending the generation.
#[derive(Clone)]
pub(crate) struct ScheduleContext {
    pub(crate) worker_number: u8,
    pub(crate) state: AppState,
    pub(crate) cfg: Arc<TaskConfig>,
    pub(crate) scope: TaskScope,
    pub(crate) dispatch: TaskDispatch,
    pub(crate) token: CancellationToken,
}

impl TaskRunner {
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn cancel(&self) {
        self.cancel_for(StopCause::Shutdown);
    }

    #[inline]
    pub(crate) fn cancel_for(
        &self,
        cause: StopCause,
    ) {
        self.stop.set(cause);
        if let Ok(g) = self.cancel.lock() {
            g.cancel();
        }
    }

    #[inline]
    pub(crate) fn cancel_for_reload(
        &self,
        action: ReloadAction,
    ) {
        self.stop.set_reload_action(action);
        if let Ok(g) = self.cancel.lock() {
            g.cancel();
        }
    }

    /// Notify current command children without cancelling this generation.
    pub(crate) fn reload_signal(
        &self,
        signal: ProcessSignal,
    ) {
        if !self.token().is_cancelled() {
            self.stop.send_reload_signal(signal);
        }
    }

    #[inline]
    pub fn token(&self) -> CancellationToken {
        self.cancel.lock_unpoisoned().clone()
    }

    #[inline]
    pub fn reset_token(&self) {
        self.stop.set(StopCause::Shutdown);
        *self.cancel.lock_unpoisoned() = CancellationToken::new();
    }

    #[inline]
    pub(crate) fn set_name(
        &mut self,
        name: impl Into<Arc<str>>,
    ) {
        self.name = name.into();
    }

    #[inline]
    pub(crate) fn set_task_scope(
        &mut self,
        task_scope: TaskScope,
    ) {
        self.task_scope = task_scope;
    }

    #[inline]
    pub(crate) fn set_task_dispatch(
        &mut self,
        task_dispatch: TaskDispatch,
    ) {
        self.task_dispatch = task_dispatch;
    }

    pub(crate) fn set_runtime_state(
        &mut self,
        runtime_state: Arc<TaskRuntimeState>,
    ) {
        self.runtime_state = runtime_state;
    }

    pub(crate) fn begin_generation(&self) -> u64 {
        self.runtime_state.begin_generation()
    }

    #[inline]
    /// Give this task the `tasks.yaml` entry it runs with when the file has
    /// none for it, so a builtin needs no operator line to exist.
    pub fn with_defaults(
        mut self,
        config: TaskConfig,
    ) -> Self {
        self.defaults = Some(Arc::new(config));
        self
    }

    pub(crate) fn defaults(&self) -> Option<Arc<TaskConfig>> {
        self.defaults.clone()
    }

    pub(crate) fn task_scope(&self) -> TaskScope {
        self.task_scope
    }

    #[inline]
    pub(crate) fn task_dispatch(&self) -> TaskDispatch {
        self.task_dispatch
    }

    #[inline]
    fn call(
        &self,
        state: AppState,
        token: CancellationToken,
        runtime: TaskRuntime,
        stop: StopContext,
    ) -> BoxFuture<'static, TaskResult<()>> {
        // Always decrement `running` even if the future is cancelled/dropped.
        let inner = self.inner.clone();
        let runtime_state = Arc::clone(&self.runtime_state);
        async move {
            // increment when attempt starts
            runtime_state.begin_attempt();

            struct RunningGuard {
                runtime_state: Arc<TaskRuntimeState>,
            }
            impl Drop for RunningGuard {
                fn drop(&mut self) {
                    self.runtime_state.finish_attempt();
                }
            }
            let _guard =
                RunningGuard { runtime_state: Arc::clone(&runtime_state) };

            inner(state, token, runtime, stop).await
        }
        .boxed()
    }

    #[inline]
    pub fn is_running(&self) -> bool {
        self.running_count() > 0
    }

    #[inline]
    pub(crate) fn running_count(&self) -> usize {
        self.runtime_state.in_flight()
    }

    pub(crate) fn last_execution(&self) -> Option<crate::TaskExecutionOutcome> {
        self.runtime_state.last_execution()
    }

    /// Fleet-arbitrated slots this runtime holds for the task. Owning it and
    /// running it are different questions; see `TaskPlacementStatus`.
    pub(crate) fn owned_slots(&self) -> usize {
        self.runtime_state.owned_slots()
    }

    pub(crate) fn set_owned_slots(
        &self,
        slots: usize,
    ) {
        self.runtime_state.set_owned_slots(slots);
    }
}

impl<F, Fut> From<F> for TaskRunner
where
    F: Fn(AppState, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = TaskResult<()>> + Send + 'static,
{
    fn from(f: F) -> Self {
        TaskRunner {
            name: Arc::from("<unnamed>"),
            inner: Arc::new(
                move |state: AppState,
                      token: CancellationToken,
                      _runtime: TaskRuntime,
                      _stop: StopContext| {
                    f(state, token).boxed()
                },
            ),
            task_scope: TaskScope::default(),
            task_dispatch: TaskDispatch::Each,
            cancel: Mutex::new(CancellationToken::new()),
            stop: StopContext::default(),
            runtime_state: Arc::new(TaskRuntimeState::default()),
            interval_limit: Arc::new(Semaphore::new(1)),
            defaults: None,
        }
    }
}

impl TaskRunner {
    pub fn from_with_runtime<F, Fut>(f: F) -> Self
    where
        F: Fn(AppState, CancellationToken, TaskRuntime) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = TaskResult<()>> + Send + 'static,
    {
        Self {
            name: Arc::from("<unnamed>"),
            inner: Arc::new(
                move |state: AppState,
                      token: CancellationToken,
                      runtime: TaskRuntime,
                      _stop: StopContext| {
                    f(state, token, runtime).boxed()
                },
            ),
            task_scope: TaskScope::default(),
            task_dispatch: TaskDispatch::Each,
            cancel: Mutex::new(CancellationToken::new()),
            stop: StopContext::default(),
            runtime_state: Arc::new(TaskRuntimeState::default()),
            interval_limit: Arc::new(Semaphore::new(1)),
            defaults: None,
        }
    }

    pub(crate) fn from_with_stop_context<F, Fut>(f: F) -> Self
    where
        F: Fn(AppState, CancellationToken, TaskRuntime, StopContext) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = TaskResult<()>> + Send + 'static,
    {
        Self {
            name: Arc::from("<unnamed>"),
            inner: Arc::new(move |state, token, runtime, stop| {
                f(state, token, runtime, stop).boxed()
            }),
            task_scope: TaskScope::default(),
            task_dispatch: TaskDispatch::Each,
            cancel: Mutex::new(CancellationToken::new()),
            stop: StopContext::default(),
            runtime_state: Arc::new(TaskRuntimeState::default()),
            interval_limit: Arc::new(Semaphore::new(1)),
            defaults: None,
        }
    }
}

impl TaskRunner {
    /// Supervisor: spawns workers according to concurrency and interval.
    pub async fn run_supervisor(
        self: Arc<Self>,
        state: AppState,
        cfg: Arc<TaskConfig>,
        scope: TaskScope,
        dispatch: TaskDispatch,
    ) {
        // Both fleet-arbitrated dispatches are owned by the controller. `one`
        // is a pool of exactly one slot: the slot lease *is* the election, held
        // by one runtime and renewed, rather than re-run by every runtime on
        // every occurrence.
        if matches!(dispatch, TaskDispatch::Pool | TaskDispatch::One) {
            error!(
                task = self.name(),
                ?dispatch,
                "fleet-arbitrated dispatch must be driven by TaskService's pool controller"
            );
            return;
        }

        let concurrency = cfg.concurrency;
        debug!(
            "🧩 Task '{}' starting with {} worker(s) (scope {:?} dispatch {:?} timing {:?})",
            self.name(),
            concurrency,
            scope,
            dispatch,
            &cfg.timing
        );

        // Snapshot the worker-health gauges. `workers_total` is fixed
        // for the lifetime of this supervisor; `workers_healthy` starts
        // equal and decrements as workers exit (see join loop below).
        #[cfg(feature = "metrics")]
        let metrics_role =
            crate::metrics::role_label_from_run_mode(state.run_mode());
        #[cfg(feature = "metrics")]
        {
            crate::metrics::set_workers_total(
                self.name(),
                &metrics_role,
                concurrency as u64,
            );
            crate::metrics::set_workers_healthy(
                self.name(),
                &metrics_role,
                concurrency as u64,
            );
        }

        let mut workers = JoinSet::new();
        for i in 0..concurrency {
            let st = state.clone();
            let runner = self.clone();
            match &cfg.timing {
                TaskTiming::Manual { .. } => {
                    debug!(
                        task = self.name(),
                        "manual task has no automatic supervisor"
                    );
                }
                TaskTiming::Cron { expression, .. } => {
                    let token = runner.token();
                    let run = ScheduleContext {
                        worker_number: i + 1,
                        state: st,
                        cfg: cfg.clone(),
                        scope,
                        dispatch,
                        token,
                    };
                    workers
                        .spawn(runner.run_with_cron(expression.clone(), run));
                }
                TaskTiming::Interval { every, .. } => {
                    let token = runner.token();
                    let run = ScheduleContext {
                        worker_number: i + 1,
                        state: st,
                        cfg: cfg.clone(),
                        scope,
                        dispatch,
                        token,
                    };
                    workers.spawn(runner.run_with_interval(*every, run));
                }
                TaskTiming::Daemon { .. } => {
                    workers.spawn(runner.run_with_retries(
                        i + 1,
                        st,
                        cfg.clone(),
                        scope,
                        dispatch,
                    ));
                }
            }
        }

        #[cfg(feature = "metrics")]
        let mut alive: u64 = concurrency as u64;

        while let Some(joined) = workers.join_next().await {
            #[cfg(feature = "metrics")]
            {
                alive = alive.saturating_sub(1);
                crate::metrics::set_workers_healthy(
                    self.name(),
                    &metrics_role,
                    alive,
                );
            }
            if let Err(e) = joined {
                error!("worker join error for '{}': {e}", self.name());
            }
        }

        info!("🧩 Supervisor finished for '{}'", self.name())
    }

    /// Worker: periodic (interval) or service mode.
    pub(crate) async fn run_with_interval(
        self: Arc<Self>,
        interval: Duration,
        run: ScheduleContext,
    ) {
        if run.dispatch == TaskDispatch::One {
            self.run_with_aligned_interval(interval, run).await;
            return;
        }
        let ScheduleContext { worker_number, state, cfg, scope, token, .. } =
            run;

        let skip_on_boot = match &cfg.timing {
            TaskTiming::Interval { skip_on_boot, .. } => *skip_on_boot,
            _ => unreachable!("interval runner requires interval timing"),
        };

        let start = if skip_on_boot {
            Instant::now() + interval
        } else {
            Instant::now()
        };

        let mut ticker = tokio::time::interval_at(start, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut executions = JoinSet::new();

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("🔸 '{}' #{} stopped", self.name(), worker_number);
                    break;
                }
                _ = ticker.tick() => {
                    self.spawn_tick(ScheduleContext {
                        worker_number, state: state.clone(), cfg: cfg.clone(),
                        scope, dispatch: TaskDispatch::Each, token: token.clone(),
                    }, &mut executions);
                }
            }
        }
        self.drain_ticks(&mut executions).await;
    }

    /// Fleet-aligned interval scheduling for `dispatch: one`.
    ///
    /// Local `tokio::time::interval` starts at process boot, so two workers
    /// booted thirty seconds apart do not share a logical 60-second tick.
    /// Wall-clock buckets give every fleet member the same occurrence id.
    async fn run_with_aligned_interval(
        self: Arc<Self>,
        interval: Duration,
        run: ScheduleContext,
    ) {
        let ScheduleContext { worker_number, state, cfg, scope, token, .. } =
            run;
        let interval_ms = interval.as_millis().min(u128::from(u64::MAX)) as u64;

        if interval_ms == 0 {
            error!("❌ '{}' interval must be at least 1ms", self.name());
            return;
        }
        let skip_on_boot = match &cfg.timing {
            TaskTiming::Interval { skip_on_boot, .. } => *skip_on_boot,
            _ => {
                unreachable!("aligned interval runner requires interval timing")
            }
        };
        let mut run_on_boot = !skip_on_boot;
        let mut executions = JoinSet::new();

        loop {
            let now_ms = unix_now_ms();
            let current_start = interval_bucket(now_ms, interval_ms);
            let target_ms = if run_on_boot {
                current_start
            } else {
                current_start.saturating_add(interval_ms)
            };

            if target_ms > now_ms {
                let delay = Duration::from_millis(target_ms - now_ms);
                tokio::select! {
                    _ = token.cancelled() => {
                        debug!("🔸 '{}' #{} stopped", self.name(), worker_number);
                        break;
                    }
                    _ = sleep(delay) => {}
                }
            } else if token.is_cancelled() {
                debug!("🔸 '{}' #{} stopped", self.name(), worker_number);
                break;
            }

            if token.is_cancelled() {
                debug!("🔸 '{}' #{} stopped", self.name(), worker_number);
                break;
            }

            run_on_boot = false;

            self.spawn_tick(
                ScheduleContext {
                    worker_number,
                    state: state.clone(),
                    cfg: cfg.clone(),
                    scope,
                    dispatch: TaskDispatch::One,
                    token: token.clone(),
                },
                &mut executions,
            );
        }
        self.drain_ticks(&mut executions).await;
    }

    /// Worker: cron schedule.
    ///
    /// Semantics:
    /// - Cron does NOT use `skip_on_boot`. The first run happens at the next cron occurrence.
    ///
    /// Overlap is selected by the task's policy at occurrence admission.
    pub(crate) async fn run_with_cron(
        self: Arc<Self>,
        cron: String,
        run: ScheduleContext,
    ) {
        let Some(service) =
            run.state.registry_ref().resolve::<crate::service::TaskService>()
        else {
            error!(
                task = self.name(),
                "cron requires the registered task service"
            );
            return;
        };
        let token = run.token.clone();
        match service.inner.scheduler.register(&cron, self.clone(), run) {
            Ok(handle) => {
                token.cancelled().await;
                handle.stop();
                handle.stopped().await;
            }
            Err(error) => {
                error!(task = self.name(), %error, "cron registration failed")
            }
        }
    }

    /// Reserve a scheduled occurrence synchronously; no waiting backlog.
    pub(crate) fn scheduled_occurrence(
        self: &Arc<Self>,
        run: ScheduleContext,
    ) -> Option<BoxFuture<'static, ()>> {
        if run.token.is_cancelled() {
            return None;
        }
        let Some(overlap) = run.cfg.timing.overlap() else {
            error!(
                task = self.name(),
                "scheduled occurrence requires cron or interval timing"
            );
            return None;
        };
        let Ok(permit) = overlap.reserve(&self.interval_limit) else {
            warn!(
                task = self.name(),
                "scheduled occurrence skipped; previous run still active"
            );
            #[cfg(feature = "metrics")]
            crate::metrics::record_skipped(self.name());
            return None;
        };
        let runner = Arc::clone(self);
        Some(Box::pin(async move {
            let _permit = permit;
            runner
                .run_daemon_slot(
                    run.worker_number,
                    run.state,
                    run.cfg,
                    run.scope,
                    run.dispatch,
                    run.token.child_token(),
                )
                .await;
        }))
    }

    fn spawn_tick(
        self: &Arc<Self>,
        run: ScheduleContext,
        executions: &mut JoinSet<()>,
    ) {
        while let Some(result) = executions.try_join_next() {
            if let Err(error) = result {
                error!(task = self.name(), %error, "scheduled execution join failed");
            }
        }
        if let Some(execution) = self.scheduled_occurrence(run) {
            executions.spawn(execution);
        }
    }

    async fn drain_ticks(
        &self,
        executions: &mut JoinSet<()>,
    ) {
        while let Some(result) = executions.join_next().await {
            if let Err(error) = result {
                error!(task = self.name(), %error, "scheduled execution join failed");
            }
        }
    }

    /// One attempt with retries/backoff.
    pub async fn run_with_retries(
        self: Arc<Self>,
        worker_number: u8,
        state: AppState,
        cfg: Arc<TaskConfig>,
        scope: TaskScope,
        dispatch: TaskDispatch,
    ) {
        let parent = self.token();
        let stop = self.stop.for_execution();
        let execution_timeout = cfg.timing.execution_timeout();
        let run = RunContext {
            worker_number,
            state,
            cfg,
            scope,
            dispatch,
            parent,
            stop,
        };
        let _ = match execution_timeout {
            Some(execution_timeout) => {
                self.run_with_execution_timeout_on(run, execution_timeout).await
            }
            None => self.run_with_retries_on(run).await,
        };
    }

    /// Execute one service-owned daemon slot. Fleet coordination deliberately
    /// stays in `TaskService`; the runner only owns body/retry semantics.
    pub(crate) async fn run_daemon_slot(
        self: Arc<Self>,
        worker_number: u8,
        state: AppState,
        cfg: Arc<TaskConfig>,
        scope: TaskScope,
        dispatch: TaskDispatch,
        token: CancellationToken,
    ) {
        let stop = self.stop.for_execution();
        let execution_timeout = cfg.timing.execution_timeout();
        let run = RunContext {
            worker_number,
            state,
            cfg,
            scope,
            dispatch,
            parent: token,
            stop,
        };
        let _ = match execution_timeout {
            Some(execution_timeout) => {
                self.run_with_execution_timeout_on(run, execution_timeout).await
            }
            None => self.run_with_retries_on(run).await,
        };
    }

    /// Run one controller-owned schedule: the `one` dispatch.
    ///
    /// The counterpart of [`Self::run_daemon_slot`] for a task that fires rather
    /// than runs. The slot lease *is* the election, so nothing here arbitrates:
    /// the runtime holding the lease owns the schedule until it stops or the
    /// lease lapses, and no other runtime is awake for this task at all.
    ///
    /// That is the whole of what changed. Before, every runtime armed the same
    /// schedule and they raced for a per-occurrence lock — correct, but it made
    /// a task with one owner look like a task with several: two supervisors in
    /// the log, a winner that drifted between runtimes minute by minute, and a
    /// history that accumulated on every one of them.
    ///
    /// `token` is the slot's, not the runner's: losing the lease has to stop
    /// the schedule, and the runner's own token only ends the generation.
    pub(crate) async fn run_scheduled_slot(
        self: Arc<Self>,
        slot: u8,
        state: AppState,
        cfg: Arc<TaskConfig>,
        scope: TaskScope,
        token: CancellationToken,
    ) {
        let run = ScheduleContext {
            worker_number: slot,
            state,
            cfg: Arc::clone(&cfg),
            scope,
            dispatch: TaskDispatch::One,
            token,
        };
        match &cfg.timing {
            TaskTiming::Cron { expression, .. } => {
                self.run_with_cron(expression.clone(), run).await;
            }
            TaskTiming::Interval { every, .. } => {
                self.run_with_interval(*every, run).await;
            }
            // Neither reaches here: manual has no automatic trigger, while a
            // daemon is sent to `run_daemon_slot` by the controller.
            TaskTiming::Manual { .. } | TaskTiming::Daemon { .. } => {}
        }
    }

    /// Execute one management-requested occurrence and return its task result.
    pub(crate) async fn run_explicit(
        self: Arc<Self>,
        state: AppState,
        cfg: Arc<TaskConfig>,
        placement: ExecutionPlacement,
        token: CancellationToken,
        stop: StopContext,
        execution_timeout: Duration,
    ) -> TaskResult<()> {
        let run = RunContext {
            worker_number: 1,
            state,
            cfg,
            scope: placement.scope,
            dispatch: placement.dispatch,
            parent: token,
            stop,
        };
        self.run_with_execution_timeout_on(run, execution_timeout).await
    }

    /// Bound the complete logical run, including retries and backoff. Timeout
    /// cancellation is occurrence-local, while reload/shutdown state remains
    /// shared with the owning runner generation.
    async fn run_with_execution_timeout_on(
        self: Arc<Self>,
        run: RunContext,
        execution_timeout: Duration,
    ) -> TaskResult<()> {
        let deadline_token = run.parent.clone();
        let deadline_stop = run.stop.clone();
        let execution = self.run_with_retries_on(run);
        tokio::pin!(execution);

        tokio::select! {
            result = &mut execution => result,
            _ = sleep(execution_timeout) => {
                deadline_stop.set(StopCause::ExecutionTimeout);
                deadline_token.cancel();
                (&mut execution).await
            }
        }
    }

    async fn run_with_retries_on(
        self: Arc<Self>,
        run: RunContext,
    ) -> TaskResult<()> {
        let RunContext {
            worker_number,
            state,
            cfg,
            scope,
            dispatch,
            parent,
            stop,
        } = run;
        let mut execution =
            ExecutionGuard::new(Arc::clone(&self.runtime_state));
        // One scheduled execution = one `runs_total` increment,
        // regardless of how many retry attempts happen below.
        #[cfg(feature = "metrics")]
        crate::metrics::record_run(self.name());

        let mut attempt: u32 = 1;

        let graceful_timeout = cfg.graceful_timeout;
        // The executor, not the runner, owns the drain for anything that is
        // not a Rust future — see `TaskExecutorKind::owns_shutdown`.
        let executor_owns_shutdown = cfg.executor_kind().owns_shutdown();
        let attempt_limit = cfg.attempts;
        let backoff = cfg.backoff;

        loop {
            if !cfg.silent {
                debug!(
                    "🚀 Task '{}' #{} attempt {}",
                    self.name(),
                    worker_number,
                    attempt
                );
            }

            let started_at = Instant::now();
            let token = parent.child_token();
            let runtime = TaskRuntime {
                task_name: Arc::clone(&self.name),
                task_worker_number: worker_number,
                attempt,
                task_scope: scope,
                task_dispatch: dispatch,
                run_mode: state.run_mode(),
                worker_count: state.worker_count(),
                process_id: std::process::id(),
            };
            let fut =
                self.call(state.clone(), token.clone(), runtime, stop.clone());

            tokio::pin!(fut);

            // RAII: in_flight gauge inc here, dec + record_duration +
            // record_attempt(outcome) on drop. Outcome set explicitly
            // below; if not set (panic path) the guard reports `Error`.
            #[cfg(feature = "metrics")]
            let mut metrics_guard =
                crate::metrics::AttemptGuard::start(self.name());

            // Either the attempt finishes naturally, or we see cancellation.
            // On cancellation: if graceful_timeout is Some(d), wait up to d; else, await natural completion.
            let res = tokio::select! {
                // natural completion
                r = &mut fut => r,

                // cancellation observed
                _ = parent.cancelled() => {
                    if stop.cause() == StopCause::ExecutionTimeout && !executor_owns_shutdown {
                        Err(Error::stopped(Reason::ExecutionTimeout))
                    } else if let Some(gt) = graceful_timeout {
                        if executor_owns_shutdown {
                            (&mut fut).await
                        } else {
                            match timeout(gt, &mut fut).await {
                                Ok(r) => r, // finished within grace
                                Err(_) => Err(Error::stopped(Reason::GracefulTimeout))
                            }
                        }
                    } else {
                        // no grace: let the attempt finish naturally
                        (&mut fut).await
                    }
                }
            };

            // Classify outcome, finish the metrics guard, then drop it
            // BEFORE any backoff sleep so in_flight reflects "actually
            // running" not "between retries".
            #[cfg(feature = "metrics")]
            {
                let outcome = match &res {
                    Ok(()) => crate::metrics::AttemptOutcome::Success,
                    Err(Error::Exit(Reason::Shutdown)) => {
                        crate::metrics::AttemptOutcome::Shutdown
                    }
                    Err(Error::Exit(Reason::ExecutionTimeout)) => {
                        crate::metrics::AttemptOutcome::Error
                    }
                    Err(Error::Exit(Reason::GracefulTimeout)) => {
                        crate::metrics::AttemptOutcome::GracefulTimeout
                    }
                    Err(Error::Exit(Reason::Cancelled)) => {
                        crate::metrics::AttemptOutcome::Shutdown
                    }
                    Err(_) => crate::metrics::AttemptOutcome::Error,
                };
                metrics_guard.finish(outcome);
                drop(metrics_guard);
            }

            match res {
                Ok(()) => {
                    execution.complete(TaskExecutionResult::Succeeded);
                    return Ok(());
                }
                Err(Error::Exit(reason)) => {
                    let outcome = match &reason {
                        Reason::GracefulTimeout | Reason::ExecutionTimeout => {
                            TaskExecutionResult::TimedOut
                        }
                        Reason::Cancelled | Reason::Shutdown => {
                            TaskExecutionResult::Cancelled
                        }
                        Reason::Custom(_) => TaskExecutionResult::Failed,
                    };
                    execution.complete(outcome);
                    if !cfg.silent {
                        info!(
                            "🔸 Task '{}#{}' exits — reason: {}",
                            self.name(),
                            worker_number,
                            reason
                        );
                    }
                    return Err(Error::Exit(reason));
                }
                Err(Error::Io(err)) => {
                    execution.complete(TaskExecutionResult::Failed);
                    error!(
                        "‼️  '{}' #{} I/O error: {}",
                        self.name(),
                        worker_number,
                        err
                    );
                    return Err(Error::Io(err));
                }
                Err(e) => {
                    error!(
                        "❌ '{}' #{} failed attempt {}: {}",
                        self.name(),
                        worker_number,
                        attempt,
                        e
                    );

                    let alive = started_at.elapsed();

                    let reset =
                        alive >= Duration::from_secs(BACKOFF_RESET_SECS);

                    if attempt_limit.exhausted_by(attempt) {
                        execution.complete(TaskExecutionResult::GaveUp);
                        error!(
                            "❌ '{}' giving up after {} attempts",
                            self.name(),
                            attempt
                        );
                        // Last error attempt was already counted via the
                        // guard above; emit a separate `gave_up` signal
                        // so dashboards can distinguish "still retrying"
                        // from "abandoned".
                        #[cfg(feature = "metrics")]
                        crate::metrics::record_attempt(
                            self.name(),
                            crate::metrics::AttemptOutcome::GaveUp,
                        );
                        return Err(e);
                    }
                    let delay = exponential_delay(attempt, backoff);

                    tokio::select! {
                        _ = parent.cancelled() => {
                            let reason = if stop.cause() == StopCause::ExecutionTimeout {
                                Reason::ExecutionTimeout
                            } else {
                                Reason::Cancelled
                            };
                            execution.complete(if matches!(reason, Reason::ExecutionTimeout) {
                                TaskExecutionResult::TimedOut
                            } else {
                                TaskExecutionResult::Cancelled
                            });
                            info!("🔸 '{}' #{} cancelled during backoff", self.name(), worker_number);
                            return Err(Error::stopped(reason));
                        }
                        _ = sleep(delay) => {
                            if reset {
                                attempt = 1;
                            } else {
                                attempt = attempt.saturating_add(1);
                            }
                        }
                    }
                }
            }
        }
    }
}

struct RunContext {
    worker_number: u8,
    state: AppState,
    cfg: Arc<TaskConfig>,
    scope: TaskScope,
    dispatch: TaskDispatch,
    parent: CancellationToken,
    stop: StopContext,
}

#[inline]
fn exponential_delay(
    attempt: u32,
    backoff: Duration,
) -> Duration {
    if attempt == 1 {
        return backoff;
    }
    // exponential backoff with cap (MAX_BACKOFF_SECS) and jitter (±20%)
    let exp = 1u32 << attempt.saturating_sub(1).min(10);
    let mut delay = backoff.saturating_mul(exp);
    let cap = Duration::from_secs(MAX_BACKOFF_SECS);
    if delay > cap {
        delay = cap;
    }
    // jitter: 80%..120%
    let nanos = delay.as_nanos();
    let low = (nanos as f64 * 0.8) as u128;
    let high = (nanos as f64 * 1.2) as u128;
    let jittered = low + fastrand::u128(low..=high).saturating_sub(low);
    Duration::from_nanos(jittered as u64)
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

pub(crate) fn cron_search_after(
    now: DateTime<Utc>,
    last_occurrence: Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    match last_occurrence {
        Some(last_occurrence) if last_occurrence > now => last_occurrence,
        _ => now,
    }
}

#[cfg(test)]
fn cron_wait_delay(
    target: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<Duration> {
    target
        .signed_duration_since(now)
        .to_std()
        .ok()
        .filter(|delay| !delay.is_zero())
}

/// The wall-clock bucket an instant falls in.
///
/// Alignment outlived the lock it was written for: it is why two runtimes that
/// booted seconds apart still fire at the same instant, which is what makes a
/// schedule readable from outside the process.
fn interval_bucket(
    now_ms: u64,
    interval_ms: u64,
) -> u64 {
    now_ms - (now_ms % interval_ms)
}

#[cfg(test)]
#[path = "../tests/runner.rs"]
mod tests;
