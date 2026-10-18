//! Independent subscriptions; startup/reload/lag replay reads current state.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_runtime::events::ProcessEventBus;
use claviron_runtime::registry::{Component, ComponentOrder, Runnable};
use claviron_runtime::{AppState, RunContext, ServiceStartPolicy, registry};
use tokio::sync::Notify;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;

use crate::health::event::{
    OriginHealthEvent, OriginServingEvent, RecoveryVacatedEvent,
};
use crate::provider::registry::Registry;
use crate::proxy::service::UpstreamService;
use crate::proxy::service::lifecycle::UpstreamLifecycle;
pub(crate) struct UpstreamHealthBridge {
    registry: Arc<Registry>,
}
impl UpstreamHealthBridge {
    pub(crate) fn new(registry: Arc<Registry>) -> Arc<Self> {
        Arc::new(Self { registry })
    }
}
impl Component<AppState> for UpstreamHealthBridge {
    type Deps = ();

    fn name(&self) -> &'static str {
        "UpstreamHealthBridge"
    }
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<crate::UpstreamProvider>()
    }
    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}
#[async_trait]
impl Runnable<AppState> for UpstreamHealthBridge {
    fn start_policy(
        &self,
        _: &AppState,
    ) -> ServiceStartPolicy {
        ServiceStartPolicy::Automatic
    }
    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        run_bridge(
            || self.registry.services(),
            self.registry.changed.clone(),
            state.events().bus().clone(),
            state.shutdown_token(),
            context,
        )
        .await;
        Ok(())
    }
}
async fn run_bridge(
    services: impl Fn() -> Vec<UpstreamService> + Send,
    changed: Arc<Notify>,
    events: ProcessEventBus,
    shutdown: CancellationToken,
    context: RunContext,
) {
    let mut health = events.subscribe::<OriginHealthEvent>();
    let mut serving = events.subscribe::<OriginServingEvent>();
    let mut vacancy = events.subscribe::<RecoveryVacatedEvent>();
    let mut first = true;
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        for service in services() {
            if service.lifecycle() == UpstreamLifecycle::Running
                && !service.is_shutting_down()
            {
                for index in 0..service.inner.profile.origins.len() {
                    service.refresh_origin_health(index);
                }
            }
        }
        // Wake recovery once at startup, including table changes made while
        // the bridge was absent. Never repeatedly wake ourselves for Down.
        if first {
            first = false;
            changed.notify_waiters();
        }
        tokio::select! {
            biased;
            _ = context.cancelled() => return,
            _ = shutdown.cancelled() => return,
            event = health.recv() => match event {
                Ok(event) => { for service in services() { service.apply_origin_health_event(event.clone()); } },
                Err(RecvError::Lagged(_)) => {}, Err(RecvError::Closed) => return,
            },
            event = serving.recv() => match event {
                Ok(event) => { for service in services() { service.apply_origin_serving_event(event.clone()); } },
                Err(RecvError::Lagged(_)) => {}, Err(RecvError::Closed) => return,
            },
            event = vacancy.recv() => match event {
                Ok(event) => { for service in services() { service.apply_recovery_vacated_event(event.clone()); } },
                Err(RecvError::Lagged(_)) => {}, Err(RecvError::Closed) => return,
            },
            _ = &mut notified => {},
        }
    }
}
