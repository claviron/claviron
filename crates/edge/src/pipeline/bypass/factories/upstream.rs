use std::sync::Arc;

use claviron_runtime::{AppState, Error, Result};
use claviron_types::Host;
use claviron_upstream::UpstreamProvider;

use crate::BypassService;
use crate::config::HostConfig;
use crate::pipeline::bypass::upstream::UpstreamBypass;

pub(crate) fn bypass_fn(
    state: AppState,
    canonical: Host,
    host_cfg: Arc<HostConfig>,
) -> Result<BypassService> {
    let upstream_name = host_cfg.upstream_name().ok_or_else(|| {
        Error::msg(format!("Missing upstream config for host {}", canonical))
    })?;

    let provider = state
        .registry_ref()
        .resolve::<UpstreamProvider>()
        .ok_or_else(|| Error::msg("UpstreamProvider not registered"))?;

    let app =
        provider.app_for_state(upstream_name, &state).ok_or_else(|| {
            Error::msg(format!(
                "Unknown upstream app/profile '{}' for host {}",
                upstream_name, canonical
            ))
        })?;

    let mut files = host_cfg.files.clone();
    files.root = host_cfg.root();
    let policy = claviron_upstream::proxy::service::UpstreamRequestPolicy {
        allow_h1_upgrade: host_cfg.transport.h1.upgrade,
        allow_connect_protocol: host_cfg.transport.h2.connect_protocol,
    };
    Ok(BypassService::Upstream(UpstreamBypass::new(
        app,
        state,
        Arc::new(files),
        policy,
    )))
}
