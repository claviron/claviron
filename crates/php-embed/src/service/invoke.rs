//! The invocation bridge: a PHP program submitted through Orbit, once, on its
//! own thread.
//!
//! This is cron with the process replaced by a thread. Something inside the
//! engine -- a scheduler, a request, another program -- submits a PHP program
//! invocation with a script path and an argv, and the one bridge task in
//! the fleet answers by booting that script on a dedicated PHP thread and
//! letting it run to completion. Fire and forget: the publisher gets nothing
//! back, and does not wait.
//!
//! Every invocation is a full boot, and that is accepted on purpose. `crontab`
//! already pays it for every line; what this removes is the fork, the exec,
//! the php binary lookup and the process that lives outside the engine with no
//! cancellation, no budget and no line in `task-status`. A warm-slot
//! optimisation can live *inside* this bridge later without the publisher
//! noticing -- the contract is the invocation, not the thread.
//!
//! Concurrency is a semaphore over threads created on demand, not a pool of
//! pre-made ones. Ring consumption is decoupled from admission through a
//! bounded local queue, so waiting for PHP capacity does not stop the Orbit
//! cursor from draining a retained burst.
//!
//! The payload is deliberately framework-free. It names a script by absolute
//! path and an argv, because that is what every console PHP -- Laravel's
//! `artisan`, Symfony's `bin/console`, a bare file -- reduces itself to before
//! it hands anything to a shell. The bridge does not know what `artisan` is.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron_app::{EventArrayKey, EventValue};
use claviron_orbit::invoke::INVOCATION_RING_CAPACITY;
use claviron_orbit::{
    Invocation, InvocationCodec, InvocationId, InvocationProvider, OrbitEpoch,
};
use claviron_runtime::gate::Error as GateError;
use claviron_runtime::registry::{self, Component, Runnable, priority};
use claviron_runtime::{
    AppState, Gate, GateDrainOutcome, Permit, RunContext, ServiceStartPolicy,
};
use claviron_tasks::service::TaskService;
use claviron_tasks::{Error as TaskError, TaskConfig, TaskResult};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::TaskProgram;
use crate::engine::task::TaskExecutions;
use crate::service::PhpEmbedRuntime;

/// Runtime-neutral operation handled by the PHP program executor.
pub const OPERATION: &str = "claviron.php.program.invoke.v1";

/// The builtin task name, as `tasks.yaml` refers to it.
pub const TASK: &str = "job-bridge";

/// One PHP program invocation payload, checked.
///
/// `script` is absolute by contract. A relative path would be resolved against
/// the bridge's process cwd, which is nowhere the publisher can see -- so it is
/// refused rather than guessed.
pub(crate) struct PhpProgramInvocation {
    kind: String,
    script: PathBuf,
    argv: Vec<String>,
    cwd: Option<PathBuf>,
    env: Vec<(String, String)>,
}

impl PhpProgramInvocation {
    fn from_payload(payload: &EventValue) -> Result<Self, String> {
        let EventValue::Array(fields) = payload else {
            return Err(format!("{OPERATION} payload must be an array"));
        };
        let field = |name: &str| {
            fields.iter().find_map(|(key, value)| match key {
                EventArrayKey::String(key) if key == name => Some(value),
                _ => None,
            })
        };
        let kind = match field("kind") {
            Some(EventValue::String(bytes)) => string(bytes, "kind")?,
            _ => {
                return Err(format!(
                    "{OPERATION} payload needs a string `kind`"
                ));
            }
        };
        if kind.trim().is_empty() {
            return Err(format!("{OPERATION} `kind` cannot be empty"));
        }
        let script = match field("script") {
            Some(EventValue::String(bytes)) => {
                PathBuf::from(string(bytes, "script")?)
            }
            _ => {
                return Err(format!(
                    "{OPERATION} payload needs a string `script`"
                ));
            }
        };
        if !script.is_absolute() {
            return Err(format!(
                "{OPERATION} `script` must be an absolute path, got {}",
                script.display()
            ));
        }
        let argv = match field("argv") {
            None => Vec::new(),
            Some(EventValue::Array(items)) => items
                .iter()
                .map(|(_, value)| match value {
                    EventValue::String(bytes) => string(bytes, "argv"),
                    _ => Err(format!(
                        "{OPERATION} `argv` entries must be strings"
                    )),
                })
                .collect::<Result<_, _>>()?,
            Some(_) => {
                return Err(format!(
                    "{OPERATION} `argv` must be an array of strings"
                ));
            }
        };
        let cwd = match field("cwd") {
            None => None,
            Some(EventValue::String(bytes)) => {
                Some(PathBuf::from(string(bytes, "cwd")?))
            }
            Some(_) => {
                return Err(format!("{OPERATION} `cwd` must be a string"));
            }
        };
        let env = match field("env") {
            None => Vec::new(),
            Some(EventValue::Array(items)) => items
                .iter()
                .map(|(key, value)| {
                    let EventArrayKey::String(key) = key else {
                        return Err(format!(
                            "{OPERATION} `env` keys must be strings"
                        ));
                    };
                    let EventValue::String(bytes) = value else {
                        return Err(format!(
                            "{OPERATION} `env` values must be strings"
                        ));
                    };
                    Ok((key.clone(), string(bytes, "env")?))
                })
                .collect::<Result<_, _>>()?,
            Some(_) => {
                return Err(format!("{OPERATION} `env` must be an array"));
            }
        };
        Ok(Self { kind, script, argv, cwd, env })
    }

    fn program(
        &self,
        id: InvocationId,
        submitted_at_ms: u64,
        admitted_at_ms: u64,
        budget: Option<std::time::Duration>,
    ) -> TaskProgram {
        let mut program =
            TaskProgram::new(format!("invoke:{id}"), self.script.clone())
                .args(self.argv.iter().cloned());
        // Where the script lives, unless told otherwise. Sent explicitly this
        // was 34 bytes of a 512-byte frame saying what the path already said.
        match &self.cwd {
            Some(cwd) => program = program.cwd(cwd.clone()),
            None => {
                if let Some(dir) = self.script.parent() {
                    program = program.cwd(dir.to_path_buf());
                }
            }
        }
        program = program.execution_timeout(budget);
        // The budget, as a parameter: the program is told what it has, in
        // seconds, so it can size its own work rather than discover the limit
        // by being interrupted.
        if let Some(budget) = budget {
            program = program.environment(
                "CLAVIRON_INVOKE_TIMEOUT",
                budget.as_secs().to_string(),
            );
        }
        for (key, value) in &self.env {
            program = program.environment(key.clone(), value.clone());
        }
        // Runtime identity is authoritative and cannot be replaced by payload
        // environment values. The id was minted at publication, before the
        // executor waited for capacity.
        program = program
            .environment("CLAVIRON_INVOKE_KIND", self.kind.clone())
            .environment("CLAVIRON_INVOKE_ID", id.to_string())
            .environment("CLAVIRON_INVOKE_OPERATION", OPERATION)
            .environment(
                "CLAVIRON_INVOKE_SUBMITTED_AT",
                submitted_at_ms.to_string(),
            )
            .environment(
                "CLAVIRON_INVOKE_ADMITTED_AT",
                admitted_at_ms.to_string(),
            );
        program
    }
}

impl InvocationCodec for PhpProgramInvocation {
    const OPERATION: &'static str = OPERATION;

    fn encode_invocation(&self) -> Result<Vec<u8>, String> {
        let argv = self
            .argv
            .iter()
            .enumerate()
            .map(|(index, value)| {
                (
                    EventArrayKey::Int(index as i64),
                    EventValue::String(value.as_bytes().to_vec()),
                )
            })
            .collect();
        let env = self
            .env
            .iter()
            .map(|(key, value)| {
                (
                    EventArrayKey::String(key.clone()),
                    EventValue::String(value.as_bytes().to_vec()),
                )
            })
            .collect();
        let mut fields = vec![
            (
                EventArrayKey::String("kind".into()),
                EventValue::String(self.kind.as_bytes().to_vec()),
            ),
            (
                EventArrayKey::String("script".into()),
                EventValue::String(
                    self.script.as_os_str().as_encoded_bytes().to_vec(),
                ),
            ),
            (EventArrayKey::String("argv".into()), EventValue::Array(argv)),
            (EventArrayKey::String("env".into()), EventValue::Array(env)),
        ];
        if let Some(cwd) = &self.cwd {
            fields.push((
                EventArrayKey::String("cwd".into()),
                EventValue::String(cwd.as_os_str().as_encoded_bytes().to_vec()),
            ));
        }
        EventValue::Array(fields).to_bytes()
    }

    fn decode_invocation(payload: &[u8]) -> Result<Self, String> {
        Self::from_payload(&EventValue::from_bytes(payload)?)
    }
}

fn string(
    bytes: &[u8],
    field: &str,
) -> Result<String, String> {
    String::from_utf8(bytes.to_vec())
        .map_err(|_| format!("{OPERATION} `{field}` must be UTF-8"))
}

/// How long an invocation waits at the gate before asking again whether the
/// executor is still open. Not a deadline: a burst simply waits, the way it
/// would have waited in the bus. The only thing that ends the wait is
/// shutdown.
const ACQUIRE_TIMEOUT: Duration = Duration::from_millis(100);

/// Publishes generation gates. The bridge caches a gate and refreshes it only
/// after closure; ordinary admission uses Gate's existing atomic permit path.
struct InvocationGates {
    current: ArcSwap<Gate>,
}

impl InvocationGates {
    fn new(max_in_flight: usize) -> Self {
        let gate = Gate::new(Some(max_in_flight), ACQUIRE_TIMEOUT);
        gate.graceful_shutdown(None);
        Self { current: ArcSwap::from_pointee(gate) }
    }

    fn begin(
        &self,
        max_in_flight: usize,
    ) -> registry::Result<InvocationGeneration> {
        let previous = self.current.load_full();
        if !previous.is_shutting_down() || previous.count() != 0 {
            return Err(registry::Error::recoverable(
                "previous PHP invocation generation is still running or draining",
            ));
        }
        let gate = Arc::new(Gate::new(Some(max_in_flight), ACQUIRE_TIMEOUT));
        let observed =
            self.current.compare_and_swap(&previous, Arc::clone(&gate));
        if !Arc::ptr_eq(&observed, &previous) {
            return Err(registry::Error::recoverable(
                "PHP invocation generation is already starting",
            ));
        }
        Ok(InvocationGeneration { gate })
    }

    async fn enter(
        &self,
        cached: &mut Arc<Gate>,
    ) -> Result<Permit, GateError> {
        loop {
            match cached.enter().await {
                Ok(permit) => return Ok(permit),
                Err(GateError::AcquireTimeout(_) | GateError::AtCapacity) => {
                    continue;
                }
                Err(GateError::ShuttingDown) => {
                    let current = self.current.load_full();
                    if Arc::ptr_eq(cached, &current) {
                        return Err(GateError::ShuttingDown);
                    }
                    *cached = current;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

struct InvocationGeneration {
    gate: Arc<Gate>,
}

impl Drop for InvocationGeneration {
    fn drop(&mut self) {
        // Also close admission if the runnable future is dropped unexpectedly.
        // Existing permits retain their own gate until their work ends.
        if !self.gate.is_shutting_down() {
            self.gate.graceful_shutdown(None);
        }
    }
}

/// The executor: threads, admission, and the drain that ends them.
///
/// A runtime-owned service rather than a task, because those three are a
/// lifecycle and a lifecycle belongs to the registry: `run` is one generation,
/// its cancellation closes the gate to new invocations, and the generation
/// ends only when every invocation already admitted has finished or been
/// forced. It is present in every worker, and that is fine -- it invokes
/// nothing on its own. What decides *which* worker executes is the one
/// `job-bridge` task in the fleet, which subscribes to the bus and calls
/// [`PhpInvocationService::invoke`] on the executor beside it.
pub struct PhpInvocationService {
    runtime: Arc<PhpEmbedRuntime>,
    gates: InvocationGates,
}

impl PhpInvocationService {
    pub(crate) fn new(runtime: Arc<PhpEmbedRuntime>) -> Self {
        let max_in_flight = runtime.launch_config().max_in_flight;
        Self { runtime, gates: InvocationGates::new(max_in_flight) }
    }

    /// Admit and start one invocation. Returns once it has been handed to its
    /// own thread, or once the executor refused it because it is shutting down.
    ///
    /// Waits at the gate while every slot is taken. That wait is the whole
    /// difference from a channel with a fixed depth: nothing is refused for
    /// capacity, a burst is simply spread over the permits as they free.
    pub(crate) async fn invoke(
        &self,
        invocation: Invocation,
        gate: &mut Arc<Gate>,
    ) -> Result<(), String> {
        let program_invocation = invocation
            .decode::<PhpProgramInvocation>()
            .map_err(|error| error.to_string())?;
        let permit =
            self.gates.enter(gate).await.map_err(|error| {
                format!("invocation executor refused: {error}")
            })?;
        self.runtime.ensure_booted().map_err(|error| error.to_string())?;

        let config = self.runtime.launch_config();
        // Own value when set, the host's `lifecycle.execution_timeout` otherwise.
        let budget =
            crate::engine::interrupt::resolve(config.execution_timeout);
        let id = invocation.id;
        let admitted_at_ms = OrbitEpoch::now().as_unix_ms();
        let program = program_invocation
            .program(
                id,
                invocation.submitted_at.as_unix_ms(),
                admitted_at_ms,
                budget,
            )
            .log(config.log.clone());
        let drain = config.drain_timeout;

        tokio::spawn(async move {
            // Held for the life of the invocation; dropping it is what lets
            // `wait_all_done` finish.
            let _permit = permit;
            let mut executions = TaskExecutions::default();
            // `instance` reaches the script as CLAVIRON_PHP_INSTANCE; an
            // invocation has no instance in the pool sense, so it gets its id
            // modulo what the field can hold.
            let instance = (id.net_id().counter() % 256) as u8;
            if let Err(error) =
                executions.start_instance(&program, instance).await
            {
                tracing::error!(%error, invocation = %id, script = %program.script_path.display(), "invocation failed to start");
                return;
            }
            executions.wait().await;
            let failed = executions.failed();
            let deadline = tokio::time::Instant::now() + drain;
            if let Err(error) = executions.drain_until(Some(deadline)).await {
                tracing::error!(%error, invocation = %id, "invocation did not drain");
            }
            if failed {
                tracing::warn!(invocation = %id, script = %program.script_path.display(), "invocation ended in failure");
            }
        });
        Ok(())
    }
}

impl Component<AppState> for PhpInvocationService {
    type Deps = Arc<PhpEmbedRuntime>;

    fn name(&self) -> &'static str {
        "PhpInvocationService"
    }
    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }
    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState, Arc<PhpEmbedRuntime>> for PhpInvocationService {
    fn start_policy(
        &self,
        state: &AppState,
    ) -> ServiceStartPolicy {
        if state.is_master() {
            ServiceStartPolicy::Unavailable
        } else {
            ServiceStartPolicy::Automatic
        }
    }

    /// One generation: open until cancelled, then closed to new launches, then
    /// over once what was admitted has ended -- on its own within the grace, or
    /// forced at its end.
    async fn run(
        self: Arc<Self>,
        _runtime: Arc<PhpEmbedRuntime>,
        _state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let config = self.runtime.launch_config();
        let generation = self.gates.begin(config.max_in_flight)?;
        tracing::info!(max_in_flight = config.max_in_flight, execution_timeout = ?config.execution_timeout, "PHP invocation service started");
        context.cancellation_token().cancelled().await;
        generation.gate.graceful_shutdown(Some(config.drain_timeout));
        match generation.gate.wait_all_done().await {
            GateDrainOutcome::Drained => {
                tracing::info!("PHP invocation service stopped");
                Ok(())
            }
            GateDrainOutcome::Forced { remaining } => {
                tracing::warn!(
                    remaining,
                    "PHP invocation service forced after grace; invocations retained"
                );
                Err(registry::Error::recoverable(format!(
                    "{remaining} invocation(s) did not finish within grace"
                )))
            }
        }
    }
}

/// Install the bridge as a builtin task. Idempotent, like the executor above it.
/// The bridge's own `tasks.yaml` entry: one daemon in the fleet, on a
/// worker, brought back for as long as the runtime lives. A file entry under
/// the same name replaces it.
const DEFAULTS: &str = "
timing: { kind: daemon }
scope: workers
dispatch: one
attempts: unlimited
backoff: 1s
graceful_timeout: 15s
";

pub(crate) fn register(
    tasks: &TaskService,
    runtime: Arc<PhpEmbedRuntime>,
) -> Result<(), String> {
    let defaults = TaskConfig::from_yaml(DEFAULTS)
        .map_err(|error| format!("{TASK}: {error}"))?;
    tasks.register_builtin(
        TASK,
        defaults,
        move |state: AppState, cancellation: CancellationToken| {
            let runtime = Arc::clone(&runtime);
            run(state, runtime, cancellation)
        },
    );
    Ok(())
}

/// The bridge's life: drain the Orbit ring into a bounded local queue while an
/// independent admission loop waits for PHP capacity. Placement is the task's
/// -- one in the fleet -- and everything after admission is the executor's.
async fn run(
    state: AppState,
    runtime: Arc<PhpEmbedRuntime>,
    cancellation: CancellationToken,
) -> TaskResult<()> {
    runtime
        .ensure_booted()
        .map_err(|error| TaskError::other(error.to_string()))?;
    let executor = state
        .registry_ref()
        .resolve::<PhpInvocationService>()
        .ok_or_else(|| {
            TaskError::other(
                "no PhpInvocationService registered in this worker",
            )
        })?;
    let bus = state
        .registry_ref()
        .resolve::<InvocationProvider>()
        .and_then(|provider| provider.handle())
        .ok_or_else(|| {
            TaskError::other("Orbit invocation transport is unavailable")
        })?;
    let mut subscription = bus
        .subscribe(OPERATION)
        .map_err(|error| TaskError::other(error.to_string()))?;
    let (sender, receiver) = mpsc::channel(INVOCATION_RING_CAPACITY);

    tracing::info!(operation = OPERATION, "PHP invocation bridge listening");

    let receive = receive_invocations(&mut subscription, sender, &cancellation);
    let admit = admit_invocations(receiver, &executor, &cancellation);
    let (receive, admit) = tokio::join!(receive, admit);
    receive?;
    admit
}

async fn receive_invocations(
    subscription: &mut claviron_orbit::InvocationSubscription,
    sender: mpsc::Sender<Invocation>,
    cancellation: &CancellationToken,
) -> TaskResult<()> {
    loop {
        let invocation = tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            invocation = subscription.receive() => invocation
        };
        let invocation = match invocation {
            Ok(invocation) => invocation,
            Err(claviron_orbit::invoke::Error::Lagged(count)) => {
                // A lagged subscription is the one error that is not fatal: the
                // ring dropped invocations we never saw. Retained invocations
                // already decoded into the subscription's pending queue remain
                // available to the next receive.
                tracing::warn!(count, "invocation bridge subscription lagged");
                continue;
            }
            Err(error) => return Err(TaskError::other(error.to_string())),
        };

        if tokio::select! {
            biased;
            () = cancellation.cancelled() => false,
            sent = sender.send(invocation) => sent.is_ok()
        } {
            continue;
        }
        break;
    }
    Ok(())
}

async fn admit_invocations(
    mut receiver: mpsc::Receiver<Invocation>,
    executor: &Arc<PhpInvocationService>,
    cancellation: &CancellationToken,
) -> TaskResult<()> {
    let mut gate = executor.gates.current.load_full();
    loop {
        let invocation = tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            invocation = receiver.recv() => match invocation {
                Some(invocation) => invocation,
                None => break
            }
        };
        let admitted = tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            admitted = executor.invoke(invocation, &mut gate) => admitted
        };
        if let Err(error) = admitted {
            tracing::warn!(%error, "invocation not admitted");
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/launch.rs"]
mod tests;
