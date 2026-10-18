//! PHP cache invalidation over the existing notified fleet event transport.
//!
//! Intercepts publish one typed event synchronously after a successful local
//! operation. The managed worker service waits on its typed subscription and
//! queues peer operations for PHP request entry under the replay guard.
//! There is no private SHM resource, timer, or peer-ring scanning loop.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use claviron_orbit::{FleetEventCodec, FleetEventRegistry};
use claviron_runtime::registry::{self, Bootable, Component, Runnable};
use claviron_runtime::{AppState, RunContext};
use tokio::sync::Mutex;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, info, warn};

use super::ipc::{InvalEvent, OwnedInvalEvent, PhpCacheInvalidationObserved};

static BUS: OnceLock<Arc<PhpInvalidationBus>> = OnceLock::new();

/// Intercept publication handle; absent in standalone or before worker boot.
pub fn bus() -> Option<&'static Arc<PhpInvalidationBus>> {
    BUS.get()
}

/// Register the inbound-only codec before runnables, and a worker subscription.
/// The fleet event provider owns all SHM lifecycle and native readiness.
pub fn register(state: &AppState) -> registry::Result<()> {
    if state.is_standalone() || state.worker_capacity() < 2 {
        return Ok(());
    }
    let events = state
        .registry_ref()
        .resolve::<FleetEventRegistry>()
        .ok_or_else(|| {
            registry::Error::msg(
                "FleetEventRegistry must precede PHP invalidation registration",
            )
        })?;
    events.register_inbound::<PhpCacheInvalidationObserved>()?;
    if state.is_worker() {
        state.registry_ref().register(Arc::new(PhpInvalidationBusService {
            events,
            inbound: Mutex::new(
                state.events().subscribe::<PhpCacheInvalidationObserved>(),
            ),
        }))?;
    }
    Ok(())
}

/// Synchronous producer on Orbit's multi-thread-safe fleet event ring.
pub struct PhpInvalidationBus {
    events: Arc<FleetEventRegistry>,
    worker_id: u8,
}

impl PhpInvalidationBus {
    pub fn publish(
        &self,
        operation: InvalEvent<'_>,
    ) {
        let operation = match operation {
            #[cfg(feature = "apcu-intercept")]
            InvalEvent::Delete(key) => OwnedInvalEvent::Delete(key.into()),
            #[cfg(feature = "apcu-intercept")]
            InvalEvent::DeletePattern(pattern) => {
                OwnedInvalEvent::DeletePattern(pattern.into())
            }
            #[cfg(feature = "apcu-intercept")]
            InvalEvent::ApcuClearCache => OwnedInvalEvent::ApcuClearCache,
            #[cfg(feature = "opcache-intercept")]
            InvalEvent::OpcacheReset => OwnedInvalEvent::OpcacheReset,
            #[cfg(feature = "opcache-intercept")]
            InvalEvent::OpcacheInvalidate(file, force) => {
                OwnedInvalEvent::OpcacheInvalidate(file.into(), force)
            }
        };
        let event = PhpCacheInvalidationObserved {
            source_worker: self.worker_id,
            operation,
        };
        match self.events.publish_raw(
            PhpCacheInvalidationObserved::TOPIC,
            &event.encode_orbit(),
        ) {
            Ok(_) => {
                info!(
                    target: "claviron::intercept",
                    event = "PhpCacheInvalidationObserved",
                    command = invalidation_command(&event.operation),
                    sender_worker = self.worker_id,
                    operation = ?event.operation,
                    "PHP cache invalidation observed"
                );
            }
            Err(error) => warn!(
                target: "claviron::intercept",
                "PHP cache invalidation publish failed from worker {}: {:?}: {}",
                self.worker_id,
                event.operation,
                error
            ),
        }
    }
}

const fn invalidation_command(operation: &OwnedInvalEvent) -> &'static str {
    match operation {
        OwnedInvalEvent::Delete(_) | OwnedInvalEvent::DeletePattern(_) => {
            "apcu_delete"
        }
        OwnedInvalEvent::ApcuClearCache => "apcu_clear_cache",
        OwnedInvalEvent::OpcacheReset => "opcache_reset",
        OwnedInvalEvent::OpcacheInvalidate(_, _) => "opcache_invalidate",
    }
}

/// Stoppable worker consumer; its receiver survives service generations.
pub struct PhpInvalidationBusService {
    events: Arc<FleetEventRegistry>,
    inbound: Mutex<Receiver<PhpCacheInvalidationObserved>>,
}

impl Component<AppState> for PhpInvalidationBusService {
    type Deps = Arc<FleetEventRegistry>;

    fn name(&self) -> &'static str {
        "PhpInvalidationBusService"
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, Arc<FleetEventRegistry>> for PhpInvalidationBusService {
    async fn boot(
        &self,
        _events: Arc<FleetEventRegistry>,
        state: &AppState,
    ) -> registry::Result<()> {
        let worker_id = state.worker_id().ok_or_else(|| {
            registry::Error::msg("PHP invalidation consumer requires a worker")
        })?;
        BUS.set(Arc::new(PhpInvalidationBus {
            events: self.events.clone(),
            worker_id,
        }))
        .map_err(|_| {
            registry::Error::msg("PHP invalidation producer already booted")
        })?;
        info!(target: "claviron::intercept", "PHP cache invalidation event subscription ready for worker {worker_id}");
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState, Arc<FleetEventRegistry>> for PhpInvalidationBusService {
    async fn run(
        self: Arc<Self>,
        _events: Arc<FleetEventRegistry>,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let mut inbound = self.inbound.try_lock().map_err(|_| {
            registry::Error::msg(
                "PHP invalidation subscription already running",
            )
        })?;
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => break,
                received = inbound.recv() => match received {
                    Ok(event) => receive(&state, event),
                    Err(RecvError::Lagged(skipped)) => warn!(
                        target: "claviron::intercept",
                        "PHP cache invalidation subscription lagged: {skipped} events skipped"
                    ),
                    Err(RecvError::Closed) => return Ok(())
                }
            }
        }
        // Drain only the retained snapshot; cancellation cannot extend forever.
        let remaining = inbound.len();
        for _ in 0..remaining {
            if let Ok(event) = inbound.try_recv() {
                receive(&state, event);
            }
        }
        Ok(())
    }
}

fn receive(
    state: &AppState,
    event: PhpCacheInvalidationObserved,
) {
    if state.worker_id() == Some(event.source_worker) {
        return;
    }
    let enabled = match &event.operation {
        OwnedInvalEvent::Delete(_)
        | OwnedInvalEvent::DeletePattern(_)
        | OwnedInvalEvent::ApcuClearCache => cfg!(feature = "apcu-intercept"),
        OwnedInvalEvent::OpcacheReset
        | OwnedInvalEvent::OpcacheInvalidate(_, _) => {
            cfg!(feature = "opcache-intercept")
        }
    };
    if !enabled {
        return;
    }
    let source = event.source_worker;
    let event = event.operation;
    let summary = match &event {
        OwnedInvalEvent::Delete(key) => format!("apcu_delete key={key}"),
        OwnedInvalEvent::DeletePattern(pattern) => {
            format!("apcu_delete pattern={pattern}")
        }
        OwnedInvalEvent::ApcuClearCache => "apcu_clear_cache".to_string(),
        OwnedInvalEvent::OpcacheReset => "opcache_reset".to_string(),
        OwnedInvalEvent::OpcacheInvalidate(file, force) => {
            format!("opcache_invalidate file={file} force={force}")
        }
    };
    debug!(
        target: "claviron::intercept",
        kind = "received",
        source_worker = source,
        event = %summary,
        "PHP cache invalidation queued for apply from worker {source}: {summary}"
    );
    match event {
        OwnedInvalEvent::Delete(_) | OwnedInvalEvent::DeletePattern(_) =>
        {
            #[cfg(feature = "apcu-intercept")]
            crate::intercept::apcu_delete::enqueue_apply(event)
        }
        OwnedInvalEvent::ApcuClearCache =>
        {
            #[cfg(feature = "apcu-intercept")]
            crate::intercept::apcu_clear_cache::enqueue_apply(event)
        }
        OwnedInvalEvent::OpcacheReset =>
        {
            #[cfg(feature = "opcache-intercept")]
            crate::intercept::opcache_reset::enqueue_apply(event)
        }
        OwnedInvalEvent::OpcacheInvalidate(_, _) =>
        {
            #[cfg(feature = "opcache-intercept")]
            crate::intercept::opcache_invalidate::enqueue_apply(event)
        }
    }
}
