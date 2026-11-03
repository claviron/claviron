use super::files::fallback_config;
use crate::HostConfig;
use crate::pipeline::host::{DispatchCall, DispatchStep};
use claviron_files::{FileSite, try_handle_files};
use claviron_http::config::files::FilesConfig;
use claviron_http::ext::RealIpExt;
use claviron_http::{RequestContext, RouteBody, ServiceResult, err_status};
use claviron_runtime::AppState;
use http::{Request, StatusCode};
use std::sync::Arc;

pub(in crate::pipeline) fn prepare_fallback(
    state: &AppState,
    config: &HostConfig,
) -> Option<Arc<dyn DispatchStep>> {
    let files = fallback_config(config);
    if files.spa.is_none() && !files.has_static_index() {
        return None;
    }
    Some(Arc::new(FileFallback {
        state: state.clone(),
        files: Arc::new(files),
    }))
}

#[derive(Clone)]
struct FileFallback {
    state: AppState,
    files: Arc<FilesConfig>,
}

impl DispatchStep for FileFallback {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        let owner = self.clone();
        DispatchCall::new(async move {
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
            ServiceResult::Handled(response)
        })
    }
}
