//! Cold native backend preparation, without host routing or Rust interception.

use std::sync::Arc;

use crate::pipeline::extension::RouteContext;

use super::DispatchStep;
use crate::pipeline::layers::prepare_fallback;

#[derive(Clone)]
pub(super) struct BackendPlan {
    pub steps: Vec<Arc<dyn DispatchStep>>,
}

impl BackendPlan {
    pub(super) fn prepare(context: &RouteContext<'_>) -> Self {
        let state = context.state;
        let host = context.host;
        let config = context.route;
        #[cfg(any(
            feature = "orbit",
            feature = "lua",
            feature = "php_embed",
            feature = "php_fcgi"
        ))]
        let policy = context.policy();
        let steps = std::iter::empty::<Arc<dyn DispatchStep>>();
        #[cfg(feature = "orbit")]
        let steps = steps.chain(
            crate::pipeline::link::LinkBackend::new(
                state,
                config.orbit(),
                Some(context.prefix),
                policy.requests,
            )
            .map(|owner| Arc::new(owner) as Arc<dyn DispatchStep>),
        );
        #[cfg(feature = "lua")]
        let steps = steps.chain(
            crate::pipeline::embed::lua::LuaBackend::new(
                state
                    .registry_ref()
                    .resolve::<claviron_lua::serve::LuaApplications>(),
                config.lua(),
                Some(context.prefix),
                policy,
            )
            .map(|owner| Arc::new(owner) as Arc<dyn DispatchStep>),
        );
        #[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
        let steps = steps.chain(
            crate::pipeline::embed::php::PhpBackend::new(state, config, policy)
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
        Self { steps: steps.chain(prepare_fallback(state, config)).collect() }
    }
}
