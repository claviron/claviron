use std::sync::Arc;
use claviron_files::{FileSite, try_handle_files};
use claviron_http::config::files::FilesConfig;
use claviron_http::ext::RealIpExt;
use claviron_http::{RequestContext, RouteBody, RouteFuture, ServiceResult, err_status};
use claviron_runtime::AppState;
use http::{Request, StatusCode};
use crate::HostConfig;
use crate::pipeline::other::{OtherHandler, OtherService};
use super::files::fallback_config;

pub(in crate::pipeline) fn prepare_fallback(state: &AppState, config: &HostConfig) -> OtherService {
    let files = fallback_config(config);
    if files.spa.is_none() && !files.has_static_index() {
        OtherService::new(NotFound)
    } else {
        OtherService::new(FileFallback { state: state.clone(), files: Arc::new(files) })
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
