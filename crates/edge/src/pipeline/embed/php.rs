//! Prepare PHP runner ownership once; requests visit native, selected calls.

use std::sync::Arc;

use claviron_http::ext::PathPrefix;
use claviron_http::{RouteBody, RouteResponse, ServiceResult, bytes_body};
#[cfg(feature = "php_embed")]
use claviron_http::empty_body;
use claviron_php::{PhpRunner, PhpRuntime};
use claviron_runtime::AppState;
use http::{Request, Response, StatusCode};

use crate::pipeline::host::{DispatchStep, DispatchFuture};

#[cfg(feature = "php_embed")]
mod app;
#[cfg(feature = "php_embed")]
mod script;
#[cfg(feature = "php_fcgi")]
mod fcgi;

struct Mount {
    path: Option<PathPrefix>,
    owner: Arc<dyn DispatchStep>,
}

/// Native PHP mount dispatch. Runner selection is cold; each runner owns its
/// own future, so script resolution never enlarges a persistent call's state.
#[derive(Clone)]
pub struct PhpBackend {
    mounts: Arc<[Mount]>,
}

impl PhpBackend {
    pub fn new(state: &AppState, runtimes: Vec<PhpRuntime>) -> Option<Self> {
        let mounts = runtimes.into_iter().filter_map(|runtime| {
            let path = runtime.path.clone();
            let owner = match runtime.php_runner {
                #[cfg(feature = "php_embed")]
                PhpRunner::Script => script::prepare(state, Arc::new(runtime)),
                #[cfg(feature = "php_embed")]
                PhpRunner::Application => Some(app::prepare(state, runtime)),
                #[cfg(feature = "php_fcgi")]
                PhpRunner::Fcgi => fcgi::prepare(state, Arc::new(runtime)),
            }?;
            Some(Mount { path, owner })
        }).collect();
        Self::from_mounts(mounts)
    }

    fn from_mounts(mut mounts: Vec<Mount>) -> Option<Self> {
        if mounts.is_empty() {
            return None;
        }
        // This replaces stable ascending Layer insertion followed by LIFO
        // dispatch: most specific first, later equal-prefix declarations first.
        mounts.sort_by(|a, b| match (&a.path, &b.path) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a), Some(b)) => a.as_str().len().cmp(&b.as_str().len()),
        });
        mounts.reverse();
        Some(Self { mounts: mounts.into() })
    }
}

impl DispatchStep for PhpBackend {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        let backend = self.clone();
        DispatchFuture::new(async move {
            let mut request = request;
            for mount in backend.mounts.iter() {
                if mount.path.as_ref().is_some_and(|prefix| {
                    !prefix.has_path_segment(request.uri().path())
                }) {
                    continue;
                }
                request = match mount.owner.call(request).await {
                    ServiceResult::Handled(response) => return ServiceResult::Handled(response),
                    ServiceResult::Unhandled(request) => request,
                };
            }
            ServiceResult::Unhandled(request)
        })
    }
}

/// Preserve the former gateway error's status and text without an Axum conversion.
fn gateway_error(status: StatusCode, error: impl std::fmt::Display) -> RouteResponse {
    let mut response = Response::new(bytes_body(error.to_string().into()));
    *response.status_mut() = status;
    response.headers_mut().insert(http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain; charset=utf-8"));
    response
}

#[cfg(feature = "php_embed")]
struct UnavailableApplication;

#[cfg(feature = "php_embed")]
impl DispatchStep for UnavailableApplication {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        drop(request);
        let mut response = Response::new(empty_body());
        *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        response.headers_mut().insert(http::header::RETRY_AFTER,
            http::HeaderValue::from_static("1"));
        DispatchFuture::ready(ServiceResult::Handled(response))
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/embed/php.rs"]
mod tests;
