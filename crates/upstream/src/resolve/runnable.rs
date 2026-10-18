//! One managed generation polls profile owners and current-table consumers.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_runtime::registry::{Component, Runnable};
use claviron_runtime::{
    AppState, ComponentOrder, RunContext, ServiceStartPolicy, registry,
};
use futures_util::stream::{FuturesUnordered, StreamExt};

use super::ResolutionChanged;
use super::control::{Binding, Completion, Control};
use crate::provider::registry::Registry;

pub(crate) struct UpstreamResolver {
    control: Arc<Control>,
    registry: Arc<Registry>,
}
impl UpstreamResolver {
    pub(crate) fn new(
        control: Arc<Control>,
        registry: Arc<Registry>,
    ) -> Arc<Self> {
        Arc::new(Self { control, registry })
    }
}
impl Component<AppState> for UpstreamResolver {
    type Deps = ();

    fn name(&self) -> &'static str {
        "UpstreamResolver"
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
impl Runnable<AppState> for UpstreamResolver {
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
        let shutdown = state.shutdown_token();
        let mut jobs = FuturesUnordered::new();
        loop {
            let changed = self.control.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            for (binding, completion) in self.control.attach() {
                jobs.push(run_binding(
                    binding,
                    completion,
                    self.registry.clone(),
                    state.clone(),
                ));
            }
            tokio::select! { biased;
                _ = context.cancelled() => return Ok(()),
                _ = shutdown.cancelled() => return Ok(()),
                _ = &mut changed => {},
                result = jobs.next(), if !jobs.is_empty() => {
                    if let Some(result) = result { result?; }
                }
            }
        }
    }
}
pub(super) async fn run_binding(
    binding: Arc<Binding>,
    completion: Completion,
    registry: Arc<Registry>,
    state: AppState,
) -> registry::Result<()> {
    let _completion = completion;
    let context = RunContext::new(_completion.job.cancel.clone());
    let shutdown = state.shutdown_token();
    let events = state.events().bus().clone();
    // Consumer subscription precedes its first read. Local publication and
    // remote hints may coalesce; periodic retry and registry wake recover truth.
    let consumer = consume(&binding, &registry, &state);
    let owner = async {
        if state.is_master() {
            std::future::pending::<Result<(), super::ResolverError>>().await
        } else {
            binding.owner.run(&events, &context, &shutdown).await
        }
    };
    tokio::pin!(owner, consumer);
    tokio::select! { biased;
        _ = context.cancelled() => Ok(()),
        _ = shutdown.cancelled() => Ok(()),
        result = &mut owner => result.map_err(|error| registry::Error::msg(error.to_string())),
        result = &mut consumer => result,
    }
}
async fn consume(
    binding: &Binding,
    registry: &Arc<Registry>,
    state: &AppState,
) -> registry::Result<()> {
    let plan =
        binding.desired.resolver_plan().expect("named binding has a plan");
    let mut events = state.events().subscribe::<ResolutionChanged>();
    loop {
        let changed = registry.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if let Some(service) = registry.service(&plan.profile) {
            let target = service.membership_target();
            let candidate =
                binding.desired.materialize(&service, Some(&binding.owner));
            match candidate {
                Ok(candidate) => match registry.publish_resolved_membership(
                    &target,
                    candidate.profile,
                    &candidate.publications,
                ) {
                    Ok(current) => current.warm_pools(),
                    Err(
                        crate::proxy::service::MembershipError::Superseded
                        | crate::proxy::service::MembershipError::NotRunning,
                    ) => {}
                    Err(
                        crate::proxy::service::MembershipError::StateBinding(
                            error,
                        ),
                    ) => {
                        tracing::debug!(%error, "upstream membership preparation will retry");
                    }
                    Err(error) => {
                        return Err(registry::Error::msg(error.to_string()));
                    }
                },
                // A concurrent authoritative commit may supersede preparation;
                // retry from current truth, without sealing a serving view.
                Err(super::DesiredError::Publication(_)) => {}
                Err(error) => {
                    return Err(registry::Error::msg(error.to_string()));
                }
            }
        }
        tokio::select! {
            _ = &mut changed => {},
            _ = tokio::time::sleep(plan.config.retry_interval) => {},
            result = events.recv() => match result {
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {},
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(())
            }
        }
    }
}
