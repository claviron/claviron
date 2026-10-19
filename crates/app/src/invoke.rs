//! Rust handlers for Orbit invocations.
//!
//! An operation served here is a capability the runtime offers to whatever
//! runs on it -- PHP, JavaScript, another Rust service -- without a native
//! function per capability. The caller submits `{operation, payload}` on the
//! invocation ring and hears the answer on the application event bus as
//! `InvocationCompleted` / `InvocationFailed`, the same two events a PHP
//! handler answers with, carrying the invocation id it was given.
//!
//! One router serves the fleet: the ring is a broadcast with a cursor per
//! subscriber, so the `invoke-router` task runs with `dispatch: one` and
//! every handler registered in every process serves only where that task
//! lands. Concurrency inside is a `Gate`; a handler that outruns the drain
//! is reported, not abandoned silently.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use claviron_orbit::{
    Invocation, InvocationProvider, InvocationSubscription, OrbitEpoch,
};
use claviron_runtime::gate::Error as GateError;
use claviron_runtime::registry::{self};
use claviron_runtime::{AppState, Gate, GateDrainOutcome};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::event::{ApplicationEvent, EventArrayKey, EventValue};

/// The builtin task name, as `tasks.yaml` refers to it.
pub const TASK: &str = "invoke-router";

/// Outcome topics, as the Laravel bridge types them.
pub const COMPLETED_TOPIC: &str = "InvocationCompleted";
pub const FAILED_TOPIC: &str = "InvocationFailed";

/// What an outcome may carry as `output` before it is cut: the rest of the
/// frame is the envelope, the id and the operation name.
const OUTPUT_BUDGET: usize = 6_144;
const MAX_IN_FLIGHT: usize = 16;
const ACQUIRE_TIMEOUT: Duration = Duration::from_millis(100);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// One operation, served in Rust.
///
/// The request is the submitter's payload as JSON; the response goes back
/// as JSON in the outcome's `output`. A `String` error becomes an
/// `InvocationFailed` with that reason.
pub trait InvocationHandler: Send + Sync + 'static {
    const OPERATION: &'static str;
    type Request: DeserializeOwned + Send;
    type Response: Serialize + Send;

    fn handle(
        &self,
        request: Self::Request,
    ) -> impl Future<Output = Result<Self::Response, String>> + Send;
}

type Answer =
    Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send>>;

trait Erased: Send + Sync {
    fn call(
        &self,
        request: serde_json::Value,
    ) -> Answer;
}

struct Typed<H>(Arc<H>);

impl<H: InvocationHandler> Erased for Typed<H> {
    fn call(
        &self,
        request: serde_json::Value,
    ) -> Answer {
        let handler = Arc::clone(&self.0);
        Box::pin(async move {
            let request: H::Request =
                serde_json::from_value(request).map_err(|error| {
                    format!("{} request does not fit: {error}", H::OPERATION)
                })?;
            let response = handler.handle(request).await?;
            serde_json::to_value(response).map_err(|error| {
                format!("{} response is not JSON: {error}", H::OPERATION)
            })
        })
    }
}

/// The operations this process can serve.
///
/// The gate that bounds a run lives in `serve`, not here: a task is stopped
/// and started again on the same router, and a gate once drained stays
/// closed.
pub struct InvocationRouter {
    handlers: RwLock<HashMap<&'static str, Arc<dyn Erased>>>,
}

impl Default for InvocationRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl InvocationRouter {
    pub fn new() -> Self {
        Self { handlers: RwLock::new(HashMap::new()) }
    }

    /// Serve `H::OPERATION` with `handler`. An operation names one handler.
    pub fn register<H: InvocationHandler>(
        &self,
        handler: H,
    ) -> registry::Result<()> {
        let mut handlers =
            self.handlers.write().expect("invocation handlers lock poisoned");
        if handlers.contains_key(H::OPERATION) {
            return Err(registry::Error::msg(format!(
                "invocation operation {} already has a handler",
                H::OPERATION
            )));
        }
        handlers.insert(H::OPERATION, Arc::new(Typed(Arc::new(handler))));
        Ok(())
    }

    pub fn operations(&self) -> Vec<&'static str> {
        let mut operations: Vec<_> = self
            .handlers
            .read()
            .expect("invocation handlers lock poisoned")
            .keys()
            .copied()
            .collect();
        operations.sort_unstable();
        operations
    }

    /// Invoke a registered Rust capability directly in this process.
    ///
    /// Language runtimes hosted by Claviron use this path when the caller and
    /// handler share a process. Orbit transports the same operation contract
    /// across process boundaries; it is not required for a local call.
    pub async fn invoke(
        &self,
        operation: &str,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let handler = self
            .handlers
            .read()
            .expect("invocation handlers lock poisoned")
            .get(operation)
            .cloned()
            .ok_or_else(|| format!("no handler for {operation}"))?;
        handler.call(request).await
    }

    /// Take invocations for every registered operation until cancelled, then
    /// drain what is in flight. This is the body of the `invoke-router` task.
    pub async fn serve(
        self: Arc<Self>,
        state: AppState,
        cancellation: CancellationToken,
    ) -> Result<(), String> {
        let bus = state
            .registry_ref()
            .resolve::<InvocationProvider>()
            .and_then(|provider| provider.handle())
            .ok_or_else(|| {
                "Orbit invocation transport is unavailable".to_owned()
            })?;
        let operations = self.operations();
        if operations.is_empty() {
            if state.is_worker() {
                tracing::debug!(
                    "invocation router has no handlers; idling until cancelled"
                );
            } else {
                info!(
                    "invocation router has no handlers; idling until cancelled"
                );
            }
            cancellation.cancelled().await;
            return Ok(());
        }
        let mut subscriptions = operations
            .iter()
            .map(|operation| {
                bus.subscribe(*operation).map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        if state.is_worker() {
            tracing::debug!(operations = ?operations, "invocation router listening");
        } else {
            info!(operations = ?operations, "invocation router listening");
        }
        let gate = Arc::new(Gate::new(Some(MAX_IN_FLIGHT), ACQUIRE_TIMEOUT));

        loop {
            let invocation = tokio::select! {
                biased;
                () = cancellation.cancelled() => break,
                received = next(&mut subscriptions) => match received {
                    Ok(invocation) => invocation,
                    Err(claviron_orbit::invoke::Error::Lagged(count)) => {
                        warn!(count, "invocation router lagged; those invocations were never served");
                        continue;
                    }
                    Err(error) => return Err(error.to_string())
                }
            };
            let permit = loop {
                match gate.enter().await {
                    Ok(permit) => break Some(permit),
                    Err(
                        GateError::AcquireTimeout(_) | GateError::AtCapacity,
                    ) => {
                        if cancellation.is_cancelled() {
                            break None;
                        }
                    }
                    Err(error) => {
                        return Err(format!(
                            "invocation router refused: {error}"
                        ));
                    }
                }
            };
            let Some(permit) = permit else { break };
            let router = Arc::clone(&self);
            let state = state.clone();
            tokio::spawn(async move {
                let _permit = permit;
                router.answer(&state, invocation).await;
            });
        }

        gate.graceful_shutdown(Some(DRAIN_TIMEOUT));
        match gate.wait_all_done().await {
            GateDrainOutcome::Drained => Ok(()),
            GateDrainOutcome::Forced { remaining } => Err(format!(
                "{remaining} invocation(s) did not finish within grace"
            )),
        }
    }

    async fn answer(
        &self,
        state: &AppState,
        invocation: Invocation,
    ) {
        let started = OrbitEpoch::now().as_unix_ms();
        let result = match EventValue::from_bytes(&invocation.payload)
            .and_then(|value| value.to_json())
        {
            Ok(request) => {
                self.invoke(invocation.operation.as_str(), request).await
            }
            Err(error) => Err(format!(
                "{} payload unreadable: {error}",
                invocation.operation
            )),
        };
        let run_ms =
            OrbitEpoch::now().as_unix_ms().saturating_sub(started) as i64;
        let id = invocation.id.to_string();

        let (topic, exit, output, reason) = match result {
            Ok(value) => (COMPLETED_TOPIC, 0, value.to_string(), None),
            Err(reason) => {
                warn!(invocation = %id, operation = %invocation.operation, %reason, "invocation failed");
                (FAILED_TOPIC, 1, String::new(), Some(reason))
            }
        };
        let truncated = output.len() > OUTPUT_BUDGET;
        let output =
            if truncated { cut(&output, OUTPUT_BUDGET) } else { output };

        let mut fields = vec![
            field("invocation", EventValue::String(id.clone().into_bytes())),
            field(
                "command",
                EventValue::String(invocation.operation.clone().into_bytes()),
            ),
            field("exit", EventValue::Int(exit)),
            field("run_ms", EventValue::Int(run_ms)),
            field("output", EventValue::String(output.into_bytes())),
            field("truncated", EventValue::Bool(truncated)),
        ];
        if let Some(reason) = reason {
            fields.push(field(
                "reason",
                EventValue::String(cut(&reason, 200).into_bytes()),
            ));
        }
        match ApplicationEvent::new(topic, EventValue::Array(fields)) {
            Ok(event) => state.events().emit(event),
            Err(error) => {
                warn!(invocation = %id, %error, "invocation outcome could not be published")
            }
        }
    }
}

/// The next invocation across every subscription, whichever ring lane
/// delivers first.
async fn next(
    subscriptions: &mut [InvocationSubscription]
) -> Result<Invocation, claviron_orbit::invoke::Error> {
    let waits: Vec<_> =
        subscriptions.iter_mut().map(|s| Box::pin(s.receive())).collect();
    let (result, _, _) = futures_util::future::select_all(waits).await;
    result
}

fn field(
    name: &str,
    value: EventValue,
) -> (EventArrayKey, EventValue) {
    (EventArrayKey::String(name.to_owned()), value)
}

fn cut(
    text: &str,
    budget: usize,
) -> String {
    let mut end = budget.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

/// `claviron.ping.v1`: answers with where it ran and when. The smallest
/// proof that a call reached Rust and came back.
pub struct Ping {
    worker: Option<u8>,
}

#[derive(serde::Deserialize)]
pub struct PingRequest {
    #[serde(default)]
    pub echo: serde_json::Value,
}

#[derive(serde::Serialize)]
pub struct PingResponse {
    pub worker: Option<u8>,
    pub pid: u32,
    pub at_ms: u64,
    pub echo: serde_json::Value,
}

impl InvocationHandler for Ping {
    const OPERATION: &'static str = "claviron.ping.v1";
    type Request = PingRequest;
    type Response = PingResponse;

    async fn handle(
        &self,
        request: PingRequest,
    ) -> Result<PingResponse, String> {
        Ok(PingResponse {
            worker: self.worker,
            pid: std::process::id(),
            at_ms: OrbitEpoch::now().as_unix_ms(),
            echo: request.echo,
        })
    }
}

/// Insert the router with the runtime's own handlers. Other crates resolve
/// `InvocationRouter` and `register` theirs at boot.
pub fn register(state: &AppState) -> registry::Result<()> {
    let router = InvocationRouter::new();
    router.register(Ping { worker: state.worker_id() })?;
    state.registry_ref().insert(Arc::new(router)).map(|_| ())
}

#[cfg(test)]
#[path = "../tests/invoke.rs"]
mod tests;
