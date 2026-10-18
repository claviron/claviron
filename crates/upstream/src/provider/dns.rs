//! Desired config -> bounded boot truth -> atomic serving adoption.
use std::collections::BTreeMap;
use std::sync::Arc;

use claviron_orbit::lock::{LockOwner, OrbitLock};
use claviron_runtime::{AppState, RunContext, registry};
use tokio_util::sync::CancellationToken;

use super::{UpstreamDeps, UpstreamProvider, fleet_of};
use crate::compile::Validation;
use crate::config::UpstreamYaml;
use crate::resolve::control::Binding;
use crate::resolve::{
    DesiredUpstreams, ProfileResolver, ResolutionAuthority, ResolutionTable,
};

impl UpstreamProvider {
    fn resolution_table(
        &self,
        deps: &UpstreamDeps,
    ) -> registry::Result<Arc<ResolutionTable>> {
        if let Some(provider) = &deps.2 {
            return provider.handle().ok_or_else(|| {
                registry::Error::msg("UpstreamResolutionProvider is not booted")
            });
        }
        if fleet_of(deps).is_some() {
            return Err(registry::Error::msg(
                "UpstreamResolutionProvider is required for an Orbit fleet",
            ));
        }
        Ok(self
            .resolution_table
            .get_or_init(|| {
                ResolutionTable::memory(crate::resolve::table::layout::CAPACITY)
            })
            .clone())
    }
    pub(super) async fn rebuild_dns(
        &self,
        state: &AppState,
        deps: &UpstreamDeps,
        parsed: &UpstreamYaml,
        validation: &Validation,
    ) -> registry::Result<()> {
        let error = |error: crate::resolve::DesiredError| {
            registry::Error::msg(error.to_string())
        };
        // Certificates are the process certificate manager's; a profile that
        // names an identity or our own authority needs it registered.
        let material = crate::compile::tls::TlsMaterial::from_state(state);
        let desired = Arc::new(
            DesiredUpstreams::from_config(parsed, validation, &material)
                .map_err(error)?,
        );
        let runtime = self.runtime(state, deps)?;
        let table = self.resolution_table(deps)?;
        let fleet = fleet_of(deps);
        #[cfg(feature = "metrics")]
        let counters = if fleet.is_some() {
            Some(
                deps.3
                    .as_ref()
                    .and_then(|provider| provider.handle())
                    .ok_or_else(|| {
                        registry::Error::msg(
                            "CounterProvider is required for fleet DNS metrics",
                        )
                    })?,
            )
        } else {
            None
        };
        // Suspend only this process's polled query/lease futures. No helper
        // revokes another worker's live tenure. Cancellation/failure drops the
        // pause guard and restores the unchanged serving resolution bindings.
        // Which resolver answers is the application's decision; a profile
        // with DNS origins cannot boot without one.
        let factory = state
            .registry_ref()
            .resolve_dyn::<dyn claviron_resolver::ResolverFactory>();
        let pause = self.resolution.pause();
        pause.joined().await;
        let previous = self.resolution.bindings();
        let mut bindings = BTreeMap::new();
        let mut candidates = BTreeMap::new();
        let context = RunContext::new(CancellationToken::new());
        for (name, profile) in &desired.profiles {
            let binding = match profile.resolver_plan() {
                None => None,
                Some(plan) => {
                    let binding = match previous.get(name) {
                        Some(binding) if binding.desired == *profile => {
                            binding.clone()
                        }
                        _ => {
                            let driver = self
                                .resolution
                                .driver(factory.as_ref(), &plan.config.backend())
                                .map_err(|error| {
                                    registry::Error::msg(format!(
                                        "upstream profile `{name}` resolver: {error}"
                                    ))
                                })?;
                            let authority = match &fleet {
                                Some(fleet) => ResolutionAuthority::Fleet {
                                    table: table.clone(),
                                    locks: OrbitLock::new(fleet.clone())?,
                                    owner: LockOwner::new(format!(
                                        "upstream-resolver:{}:{}",
                                        fleet.node_id().get(),
                                        std::process::id()
                                    )),
                                },
                                None => ResolutionAuthority::Local {
                                    table: table.clone(),
                                },
                            };
                            #[allow(unused_mut)]
                            let mut owner = ProfileResolver::prepare(
                                plan, driver, authority,
                            )
                            .map_err(|error| {
                                registry::Error::msg(error.to_string())
                            })?;
                            #[cfg(feature = "metrics")]
                            owner
                                .attach_metrics(
                                    &self.resolution.metrics,
                                    counters.clone(),
                                )
                                .map_err(|error| {
                                    registry::Error::msg(error.to_string())
                                })?;
                            Arc::new(Binding {
                                desired: profile.clone(),
                                owner: Arc::new(owner),
                            })
                        }
                    };
                    // Identical config is live-state preserving, including an
                    // intentionally empty answer after successful required boot.
                    if self.registry.desired_profile(name).as_ref()
                        != Some(profile)
                    {
                        binding
                            .owner
                            .boot(
                                state.events().bus(),
                                &context,
                                &state.shutdown_token(),
                            )
                            .await
                            .map_err(|error| {
                                registry::Error::msg(error.to_string())
                            })?;
                    }
                    bindings.insert(name.clone(), binding.clone());
                    Some(binding)
                }
            };
            let candidate = profile
                .materialize_with(
                    &runtime,
                    binding.as_ref().map(|binding| binding.owner.as_ref()),
                )
                .map_err(error)?;
            candidates.insert(name.clone(), candidate);
        }
        let insecure = candidates
            .iter()
            .filter(|(name, candidate)| {
                candidate.profile.connector.is_some()
                    && candidate.profile.tls.insecure_skip_verify
                    && self.registry.desired_profile(name).as_ref()
                        != desired.profile(name).as_ref()
            })
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let retired = self
            .registry
            .adopt_resolved(desired, candidates, &table, runtime)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        self.resolution.publish(bindings);
        for name in insecure {
            tracing::warn!(profile = %name, "upstream TLS certificate verification is explicitly disabled");
        }
        #[cfg(feature = "metrics")]
        if let Ok(mut metrics) = self.metrics.write() {
            *metrics = parsed.metrics.clone();
        }
        drop(pause);
        for service in retired {
            service.wait_stopped().await;
            self.release_retired_l7(&service);
        }
        Ok(())
    }
}
