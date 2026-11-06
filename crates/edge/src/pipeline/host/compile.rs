//! Compile a virtual host's site. The compilation itself is
//! `claviron_pipeline::compile`; what a host adds to it is here: its registered
//! layers and providers, Link dispatch and its H1 upgrade endpoints.

use std::sync::Arc;

use claviron_http::config::HttpSnapshot;
use claviron_pipeline::compile::{CompileInput, NoHooks, compile};
use claviron_runtime::{AppState, registry::Result};
use claviron_types::Host;

#[cfg(feature = "websocket")]
use super::BackendHandler;
use super::HostExecution;
use crate::config::HostConfig;
use crate::pipeline::apps::{HostAppRegistry, HostContributions};
use crate::pipeline::{RouteProtocol, RustRouter};

/// One cold host compilation batch borrows one listener-policy publication.
pub(crate) struct HostCompiler<'a> {
    pub(crate) state: &'a AppState,
    pub(crate) http: Option<&'a HttpSnapshot>,
    pub(crate) protocol: Option<RouteProtocol>,
}

pub(crate) struct HostCompileTarget<'a> {
    pub(crate) host: &'a Host,
    pub(crate) listener: Option<&'a claviron_http::config::ListenerConfig>,
}

/// What a virtual host adds to each scope of its site.
impl HostCompiler<'_> {
    pub(crate) fn prepare(
        &self,
        host: &Host,
        config: Arc<HostConfig>,
        router: Option<RustRouter>,
        contributions: &HostContributions,
        #[cfg(feature = "rate-limit")] quotas: Option<
            &crate::pipeline::layers::HostQuotas,
        >,
        #[cfg(feature = "websocket")] websocket: Option<
            Arc<crate::pipeline::H1UpgradeRegistry>,
        >,
    ) -> Result<HostExecution> {
        self.prepare_target(
            HostCompileTarget { host, listener: None },
            config,
            router,
            contributions,
            #[cfg(feature = "rate-limit")]
            quotas,
            #[cfg(feature = "websocket")]
            websocket,
        )
    }

    pub(crate) fn prepare_target(
        &self,
        target: HostCompileTarget<'_>,
        host_config: Arc<HostConfig>,
        router: Option<RustRouter>,
        contributions: &HostContributions,
        #[cfg(feature = "rate-limit")] quotas: Option<
            &crate::pipeline::layers::HostQuotas,
        >,
        #[cfg(feature = "websocket")] websocket: Option<
            Arc<crate::pipeline::H1UpgradeRegistry>,
        >,
    ) -> Result<HostExecution> {
        let HostCompileTarget { host, listener } = target;
        let protocol = listener.map(RouteProtocol::from).or(self.protocol);
        let state = self.state;
        let registry = state.registry_ref().resolve::<HostAppRegistry>();
        // Only the upgrade decision reads the connection terms; everything
        // prepared below serves this host's site.
        #[cfg(feature = "websocket")]
        let upgrades = crate::pipeline::layers::WebSocketStage::prepare(
            state,
            host,
            &host_config.transport,
            listener,
            websocket,
        )
        .map(|stage| stage as Arc<dyn BackendHandler>);
        #[cfg(not(feature = "websocket"))]
        let upgrades = None;
        compile(CompileInput {
            state,
            host: Some(host),
            site: host_config.site().clone(),
            transport: &host_config.transport,
            listener,
            protocol: self.protocol,
            http: self.http,
            router,
            applications: &contributions.applications,
            layers: &contributions.layers,
            registry: registry.as_deref().map(HostAppRegistry::site),
            contributed: registry
                .as_deref()
                .map(|registry| {
                    registry.contributed_extensions(
                        host,
                        &host_config,
                        protocol,
                    )
                })
                .unwrap_or_default(),
            hooks: &NoHooks,
            #[cfg(feature = "rate-limit")]
            quotas,
            upgrades,
            policy: None,
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/compile.rs"]
mod tests;
