//! Cold native backend preparation, without host routing or Rust interception.

use std::sync::Arc;

use claviron_files::{FileSite, try_handle_files};
use claviron_http::config::files::FilesConfig;
use claviron_http::ext::RealIpExt;
use claviron_http::{
    RequestContext, RouteBody, RouteFuture, ServiceResult, err_status,
};
use claviron_runtime::AppState;
use claviron_types::Host;
use http::{Request, StatusCode};

use super::{DispatchStep, fallback_config};
use crate::HostConfig;
use crate::pipeline::other::{OtherHandler, OtherService};

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
        let files = Arc::new(fallback_config(config));
        let terminal = if files.spa.is_none() && !files.has_static_index() {
            OtherService::new(NotFound)
        } else {
            OtherService::new(FileFallback { state: state.clone(), files })
        };
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

struct NotFound;

impl OtherHandler for NotFound {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        drop(request);
        Box::pin(std::future::ready(Ok(err_status(StatusCode::NOT_FOUND))))
    }
}

#[derive(Clone)]
struct FileFallback {
    state: AppState,
    files: Arc<FilesConfig>,
}

impl OtherHandler for FileFallback {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let owner = self.clone();
        Box::pin(async move {
            let context = RequestContext {
                uri: request.uri(),
                headers: request.headers(),
                real_ip: request.extensions().get::<RealIpExt>().map(|ip| ip.0),
            };
            let response = match try_handle_files(
                &owner.state,
                owner.files.as_ref(),
                context,
            )
            .await
            {
                Ok(ServiceResult::Handled(response)) => response,
                Ok(ServiceResult::Unhandled(_)) => {
                    err_status(StatusCode::NOT_FOUND)
                }
                Err(status) => err_status(status),
            };
            // Retain the original body across file work and release it once.
            drop(request);
            Ok(response)
        })
    }
}
