//! The supervisor's policy from the launch, not from a file of Claviron's own:
//! a runner's `supervisor:` section is the one written-down decision its
//! master and every worker are supervised by.

use std::sync::Arc;

use claviron_runtime::{AppState, registry};
use claviron_supervisor::{SupervisorConfig, SupervisorConfigSource};

struct SupervisorPolicy(SupervisorConfig);

impl SupervisorConfigSource for SupervisorPolicy {
    fn supervisor_config(&self) -> Result<SupervisorConfig, String> {
        Ok(self.0.clone())
    }
}

/// Register the launch's supervisor policy. Before the application's own
/// registrations: the supervisor boots early and must find it there.
pub fn register_policy(
    state: &AppState,
    policy: SupervisorConfig,
) -> registry::Result<()> {
    state.registry_ref().insert(Arc::new(SupervisorPolicy(policy)))?;
    state
        .registry_ref()
        .bind_dyn::<dyn SupervisorConfigSource, SupervisorPolicy>(|source| {
            source
        })?;
    Ok(())
}
