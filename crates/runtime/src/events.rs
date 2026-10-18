//! Process-local callbacks on Continuo's typed event primitive.
//!
//! Providers register listeners synchronously, before runnable submission.
//! Registration subscribes immediately but starts no work. AppState installs
//! one dispatcher provider; Runtime polls all its listener futures in one task.
//! Orbit codecs, publication, and remote listeners belong to claviron-orbit.

use std::any::type_name;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
pub use continuo::events::{Event, ProcessEventBus, ShutdownInitiated};
use continuo::fleet::{Scope, Scoped};
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::sync::broadcast::{self};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::registry::{self, Component, Runnable};
use crate::{AppState, RunContext};

type Listener = Box<
    dyn Fn(AppState, CancellationToken) -> BoxFuture<'static, ()> + Send + Sync,
>;

struct Listeners {
    registered: Vec<Listener>,
    sealed: bool,
}

/// AppState's local callback registry and its single managed runnable.
///
/// Each listener handles its own events serially. Different listeners progress
/// concurrently through FuturesUnordered, without a Tokio task per listener or
/// event. Callbacks must yield during long work; blocking one blocks the whole
/// dispatcher. There is no ordering guarantee between listeners/event types.
///
/// The underlying ProcessEventBus stays available through Deref: emit/subscribe,
/// channel capacities, lag, and no-replay semantics are unchanged. A clone of
/// that primitive shares publication, but does not own or run callbacks.
///
/// Errors returned by callbacks are logged and the listener continues. Panics
/// fail the dispatcher task and are reported by Runtime like runnable panics.
/// Shutdown finishes in-flight callbacks and drains each receiver's retained
/// backlog once; a callback that never completes requires Runtime abort/drain.
pub struct EventDispatcher {
    bus: ProcessEventBus,
    listeners: Mutex<Listeners>,
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self {
            bus: ProcessEventBus::new(),
            listeners: Mutex::new(Listeners {
                registered: Vec::new(),
                sealed: false,
            }),
        }
    }
}

impl EventDispatcher {
    /// The shared primitive, for APIs that explicitly store a ProcessEventBus.
    pub fn bus(&self) -> &ProcessEventBus {
        &self.bus
    }

    /// Register a callback before Runtime::spawn_all submits runnables.
    ///
    /// The supplied state is passed at execution time. Capture a provider Arc,
    /// rather than AppState itself, to avoid a state -> listener -> state cycle.
    /// Callbacks execute only after provider boot, when the runtime starts.
    /// Each registration is a separate listener, including repeated callbacks;
    /// register once during setup, not on configuration reload.
    ///
    /// ```
    /// use claviron_runtime::{AppState, ProcessStarted};
    ///
    /// fn register(state: &AppState) -> claviron_runtime::Result<()> {
    ///     state.events().listen::<ProcessStarted, _>(|state, _| async move {
    ///         tracing::info!(instance = state.name(), "process started");
    ///         Ok(())
    ///     })
    /// }
    /// ```
    pub fn listen<E, Fut>(
        &self,
        callback: impl Fn(AppState, E) -> Fut + Send + 'static,
    ) -> registry::Result<()>
    where
        E: Event,
        Fut: Future<Output = registry::Result<()>> + Send + 'static,
    {
        let mut listeners =
            self.listeners.lock().expect("event listeners lock poisoned");
        if listeners.sealed {
            return Err(registry::Error::msg(
                "event listeners must be registered before runnable submission",
            ));
        }
        let receiver =
            Arc::new(tokio::sync::Mutex::new(self.bus.subscribe::<E>()));
        let callback = Arc::new(tokio::sync::Mutex::new(callback));
        listeners.registered.push(Box::new(move |state, stopping| {
            Box::pin(listen(
                state,
                stopping,
                receiver.clone(),
                callback.clone(),
            ))
        }));
        Ok(())
    }

    /// Keep cancellation and its notification together relative to dispatcher
    /// shutdown. A runnable on another executor thread cannot begin its final
    /// receiver drain between cancel() and emit(ShutdownInitiated).
    pub(crate) fn initiate_shutdown(
        &self,
        token: &CancellationToken,
    ) {
        let _guard =
            self.listeners.lock().expect("event listeners lock poisoned");
        token.cancel();
        self.bus.emit(ShutdownInitiated);
    }
}

impl Deref for EventDispatcher {
    type Target = ProcessEventBus;

    fn deref(&self) -> &Self::Target {
        &self.bus
    }
}

impl Scoped for EventDispatcher {
    fn scope(&self) -> Scope {
        // Master callbacks drive worker startup and shutdown acknowledgements.
        Scope::Any
    }
}

impl Component<AppState> for EventDispatcher {
    type Deps = ();

    fn name(&self) -> &'static str {
        "EventDispatcher"
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        let has_listeners = {
            let mut listeners =
                self.listeners.lock().expect("event listeners lock poisoned");
            listeners.sealed = true;
            !listeners.registered.is_empty()
        };
        has_listeners.then_some(self)
    }
}

#[async_trait]
impl Runnable<AppState> for EventDispatcher {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let running = {
            let mut listeners =
                self.listeners.lock().expect("event listeners lock poisoned");
            listeners.sealed = true;
            let stopping = CancellationToken::new();
            let running = listeners
                .registered
                .iter()
                .map(|listener| listener(state.clone(), stopping.clone()))
                .collect::<FuturesUnordered<_>>();
            (running, stopping)
        };
        let (mut running, stopping) = running;

        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    // Pairs with initiate_shutdown: publish its event before
                    // allowing any listener to snapshot its final backlog.
                    let _guard = self.listeners.lock().expect("event listeners lock poisoned");
                    stopping.cancel();
                    break;
                }
                completed = running.next() => {
                    if completed.is_none() {
                        return Ok(());
                    }
                }
            }
        }
        while running.next().await.is_some() {}
        Ok(())
    }
}

async fn listen<E, F, Fut>(
    state: AppState,
    stopping: CancellationToken,
    receiver: Arc<tokio::sync::Mutex<broadcast::Receiver<E>>>,
    callback: Arc<tokio::sync::Mutex<F>>,
) where
    E: Event,
    F: Fn(AppState, E) -> Fut + Send + 'static,
    Fut: Future<Output = registry::Result<()>> + Send + 'static,
{
    loop {
        tokio::select! {
            biased;
            _ = stopping.cancelled() => break,
            event = async { receiver.lock().await.recv().await } => {
                match event {
                    Ok(event) => {
                        if let Err(error) = invoke_callback(&callback, state.clone(), event).await {
                            error!(event_type = type_name::<E>(), %error, "local event listener failed");
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        warn!(event_type = type_name::<E>(), skipped, "local event listener lagged");
                    }
                    Err(RecvError::Closed) => return
                }
            }
        }
    }

    // Snapshot only retained work. Keep the receiver registered so a stopped
    // dispatcher generation retains bounded events for its successor; the
    // fixed snapshot prevents callbacks from extending this drain indefinitely.
    let mut receiver = receiver.lock().await;
    let mut remaining = receiver.len();
    let mut pending = Vec::new();
    while remaining > 0 {
        match receiver.try_recv() {
            Ok(event) => {
                pending.push(event);
                remaining -= 1;
            }
            Err(TryRecvError::Lagged(skipped)) => {
                warn!(
                    event_type = type_name::<E>(),
                    skipped, "local event listener lagged during drain"
                );
                remaining = remaining.saturating_sub(
                    usize::try_from(skipped).unwrap_or(usize::MAX),
                );
            }
            Err(TryRecvError::Empty | TryRecvError::Closed) => break,
        }
    }
    drop(receiver);
    for event in pending {
        if let Err(error) =
            invoke_callback(&callback, state.clone(), event).await
        {
            error!(event_type = type_name::<E>(), %error, "local event listener failed during drain");
        }
    }
}

async fn invoke_callback<E, F, Fut>(
    callback: &tokio::sync::Mutex<F>,
    state: AppState,
    event: E,
) -> registry::Result<()>
where
    F: Fn(AppState, E) -> Fut,
    Fut: Future<Output = registry::Result<()>>,
{
    let future = {
        let callback = callback.lock().await;
        callback(state, event)
    };
    future.await
}

/// This process completed provider boot and submitted its runnable tasks.
///
/// Emitted once by Claviron's server bootstrap in master, worker, and
/// standalone mode. This local milestone carries no process identity and
/// does not assert readiness of individual runnables. Cross-process adapters
/// must use a different event type rather than forward this marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessStarted;

impl Event for ProcessStarted {}

/// Reopen the log files this process writes, because they were moved.
///
/// Emitted where the operator's signal arrives — the master, or a standalone
/// process — and carried to every worker over Orbit, so a worker never needs
/// to know which signal was chosen. Whatever writes a file listens and reopens
/// its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReopenLogs;

impl Event for ReopenLogs {}
