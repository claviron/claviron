use std::sync::Arc;

use claviron_php::PhpRunner;
use claviron_runtime::{AppState, Error};
use claviron_types::Host;

use crate::OtherService;
use crate::config::HostConfig;

pub(crate) fn build(
    state: AppState,
    canonical: Host,
    host_cfg: Arc<HostConfig>,
) -> claviron_runtime::Result<OtherService> {
    let runtime = host_cfg
        .build_runtimes(&state)
        .into_iter()
        .next() // get first runtime
        .map(Arc::new)
        .ok_or_else(|| {
            Error::msg(format!("php is not configured for {}", canonical))
        })?;

    match runtime.php_runner {
        #[cfg(feature = "php_fcgi")]
        PhpRunner::Fcgi => {
            use claviron_php_fcgi::{PhpFcgiService, PhpFpmAdapter};

            let php_service = state
                .registry_ref()
                .resolve::<PhpFcgiService>()
                .ok_or_else(|| Error::msg("PhpFcgiService not registered"))?;

            Ok(OtherService::new(PhpFpmAdapter::new(
                php_service.clone(),
                runtime,
            )))
        }
        #[cfg(feature = "php_embed")]
        PhpRunner::Script => {
            use claviron_php_embed::{PhpScriptAdapter, PhpScriptService};

            let php_service = state
                .registry_ref()
                .resolve::<PhpScriptService>()
                .ok_or_else(|| Error::msg("PhpScriptService not registered"))?;

            Ok(OtherService::new(PhpScriptAdapter::new(
                php_service.clone(),
                runtime,
            )))
        }
        #[cfg(feature = "php_embed")]
        PhpRunner::Application => {
            use claviron_php_embed::{
                PhpApplicationAdapter, PhpApplicationService,
            };

            let php_service = state
                .registry_ref()
                .resolve::<PhpApplicationService>()
                .ok_or_else(|| {
                    Error::msg("PhpApplicationService not registered")
                })?;

            let app_name = runtime.application.as_deref().ok_or_else(|| {
                Error::msg("PHP application route has no application name")
            })?;

            let adapter = PhpApplicationAdapter::new(php_service, app_name)
                .environment(&state, &runtime)
                .map_err(|error| Error::msg(error.to_string()))?
                .route_policy(claviron_http::policy::HttpPolicyOverride {
                    requests: runtime.requests,
                    responses: runtime.responses,
                });
            Ok(OtherService::new(adapter))
        }
    }
}
