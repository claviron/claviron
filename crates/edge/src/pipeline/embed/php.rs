//! Prepare PHP runner ownership once; requests visit native, selected calls.

use std::sync::Arc;

#[cfg(feature = "php_embed")]
use claviron_http::empty_body;
use claviron_http::ext::PathPrefix;
use claviron_http::{RouteBody, RouteResponse, ServiceResult, bytes_body};
use claviron_php::{PhpRunner, PhpRuntime};
use claviron_runtime::AppState;
use http::{Request, Response, StatusCode};

use crate::HostConfig;
use crate::pipeline::host::{DispatchFuture, DispatchStep};

#[cfg(feature = "php_embed")]
mod app;
#[cfg(feature = "php_fcgi")]
mod fcgi;
#[cfg(feature = "php_embed")]
mod script;

// Resolve route mappings and PHP defaults before selecting runner owners.
fn prepare_runtimes(state: &AppState, config: &HostConfig) -> Vec<PhpRuntime> {
    config.php
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|template| {
            let mut runtime = PhpRuntime::from_config(
                state,
                template,
                config.root(),
                config.indexes(),
            );
            if let Some(mapping) = &config.files.mapping {
                if template.dir.is_none() {
                    runtime.dir = mapping.directory().to_path_buf();
                }
                runtime.mapping = Some(
                    claviron_http::config::mapping::RouteMapping::new(
                        mapping.prefix().clone(),
                        runtime.dir.clone(),
                    ),
                );
            }
            runtime
        })
        .collect()
}

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
    pub fn new(
        state: &AppState,
        config: &HostConfig,
    ) -> Option<Self> {
        let mounts = prepare_runtimes(state, config)
            .into_iter()
            .filter_map(|runtime| {
                let path = runtime.path.clone();
                let owner = match runtime.php_runner {
                    #[cfg(feature = "php_embed")]
                    PhpRunner::Script => {
                        script::prepare(state, Arc::new(runtime))
                    }
                    #[cfg(feature = "php_embed")]
                    PhpRunner::Application => {
                        Some(app::prepare(state, runtime))
                    }
                    #[cfg(feature = "php_fcgi")]
                    PhpRunner::Fcgi => fcgi::prepare(state, Arc::new(runtime)),
                }?;
                Some(Mount { path, owner })
            })
            .collect();
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
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchFuture {
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
                    ServiceResult::Handled(response) => {
                        return ServiceResult::Handled(response);
                    }
                    ServiceResult::Unhandled(request) => request,
                };
            }
            ServiceResult::Unhandled(request)
        })
    }
}

/// Preserve the former gateway error's status and text without an Axum conversion.
fn gateway_error(
    status: StatusCode,
    error: impl std::fmt::Display,
) -> RouteResponse {
    let mut response = Response::new(bytes_body(error.to_string().into()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

#[cfg(feature = "php_embed")]
struct UnavailableApplication;

#[cfg(feature = "php_embed")]
impl DispatchStep for UnavailableApplication {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchFuture {
        drop(request);
        let mut response = Response::new(empty_body());
        *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        response.headers_mut().insert(
            http::header::RETRY_AFTER,
            http::HeaderValue::from_static("1"),
        );
        DispatchFuture::ready(ServiceResult::Handled(response))
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/embed/php.rs"]
mod tests;
