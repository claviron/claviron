//! One independently managed active-health generation; no per-origin spawn.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_runtime::registry::{Component, ComponentOrder, Runnable};
use claviron_runtime::{AppState, RunContext, ServiceStartPolicy, registry};

use super::tasks::{ProbeSchedule, run_rounds};
use crate::provider::registry::Registry;

pub(crate) struct UpstreamActiveProbe {
    registry: Arc<Registry>,
}
impl UpstreamActiveProbe {
    pub(crate) fn new(registry: Arc<Registry>) -> Arc<Self> {
        Arc::new(Self { registry })
    }
}
impl Component<AppState> for UpstreamActiveProbe {
    type Deps = ();

    fn name(&self) -> &'static str {
        "UpstreamActiveProbe"
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
impl Runnable<AppState> for UpstreamActiveProbe {
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
            ProbeSchedule::Active,
        )
        .await;
        Ok(())
    }
}
pub(crate) mod l7;
mod origin;
