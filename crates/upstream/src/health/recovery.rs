//! One independently managed recovery generation over current slot health.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_runtime::registry::{Component, ComponentOrder, Runnable};
use claviron_runtime::{AppState, RunContext, ServiceStartPolicy, registry};

use super::tasks::{ProbeSchedule, run_rounds};
use crate::provider::registry::Registry;
pub(crate) struct UpstreamRecoveryProbe {
    registry: Arc<Registry>,
}
impl UpstreamRecoveryProbe {
    pub(crate) fn new(registry: Arc<Registry>) -> Arc<Self> {
        Arc::new(Self { registry })
    }
}
impl Component<AppState> for UpstreamRecoveryProbe {
    type Deps = ();

    fn name(&self) -> &'static str {
        "UpstreamRecoveryProbe"
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
impl Runnable<AppState> for UpstreamRecoveryProbe {
    fn start_policy(
        &self,
        _state: &AppState,
    ) -> ServiceStartPolicy {
        ServiceStartPolicy::Automatic
    }
    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        run_rounds(
            || self.registry.services(),
            self.registry.changed.clone(),
            state.shutdown_token(),
            context,
            ProbeSchedule::Recovery,
        )
        .await;
        Ok(())
    }
}
mod origin;
