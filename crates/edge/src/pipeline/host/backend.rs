//! Cold native backend preparation, without host routing or Rust interception.

use std::sync::Arc;

use claviron_runtime::AppState;
use claviron_types::Host;

use super::DispatchStep;
use crate::pipeline::layers::prepare_fallback;
use crate::HostConfig;
use crate::pipeline::other::OtherService;

#[derive(Clone)]
pub(super) struct BackendPlan {
    pub terminal: OtherService,
    pub steps: Vec<Arc<dyn DispatchStep>>,
}

impl BackendPlan {
    pub(super) fn prepare(
        state: &AppState,
        host: &Host,
        config: &HostConfig,
    ) -> Self {
        let terminal = prepare_fallback(state, config);
        let steps = std::iter::empty::<Arc<dyn DispatchStep>>();
        #[cfg(feature = "orbit")]
        let steps = steps.chain(
            crate::pipeline::link::LinkBackend::new(state, config.orbit())
                .map(|owner| Arc::new(owner) as Arc<dyn DispatchStep>),
        );
        #[cfg(feature = "lua")]
        let steps = steps.chain(
            crate::pipeline::embed::lua::LuaBackend::new(
                state
                    .registry_ref()
                    .resolve::<claviron_lua::serve::LuaApplications>(),
                config.lua(),
            )
            .map(|owner| Arc::new(owner) as Arc<dyn DispatchStep>),
        );
        #[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
        let steps = steps.chain(
            crate::pipeline::embed::php::PhpBackend::new(
                state,
                config,
            )
            .map(|owner| Arc::new(owner) as Arc<dyn DispatchStep>),
        );
        #[cfg(feature = "upstream")]
        let steps = steps.chain(
            crate::pipeline::upstream::UpstreamStage::prepare(
                state,
                host,
                config.upstream.as_ref(),
                claviron_upstream::proxy::service::UpstreamRequestPolicy {
                    allow_h1_upgrade: config.transport.h1.upgrade,
                    allow_connect_protocol: config
                        .transport
                        .h2
                        .connect_protocol,
                },
            )
            .map(|owner| Arc::new(owner) as Arc<dyn DispatchStep>),
        );
        let _ = host;
        Self { terminal, steps: steps.collect() }
    }
}

