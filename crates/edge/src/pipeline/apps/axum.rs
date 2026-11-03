//! Optional Axum adaptation for explicitly supplied Rust applications.

use claviron_runtime::AppState;
use std::sync::Arc;

use axum::Router;
use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use futures_util::FutureExt;
use http::Request;
use std::future::Future;
use tower::ServiceExt;

use crate::pipeline::host::{
    ApplicationHandler, HostHandler, HostHandlerFuture, HostRequest,
};

struct AxumApplication(Router);

impl ApplicationHandler for Router {
    fn call(
        self,
        request: Request<RouteBody>,
    ) -> impl Future<Output = RouteResponse> + Send {
        self.oneshot(request.map(axum::body::Body::new)).map(
            |reply| match reply {
                Ok(response) => claviron_http::from_axum_response(response),
                Err(never) => match never {},
            },
        )
    }
}

/// Adapt an application-owned Axum router to a native Rust contribution.
/// The router owns its response, including a 404. Status never means Unhandled.
pub fn application(router: Router) -> Arc<dyn HostHandler> {
    Arc::new(AxumApplication(router))
}

impl HostHandler for AxumApplication {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        Box::pin(
            ApplicationHandler::call(self.0.clone(), request.into_request())
                .map(ServiceResult::Handled),
        )
    }
}

/// Cold Axum router factory for a selected host.
pub type Factory = dyn Fn(AppState) -> Router + Send + Sync + 'static;

impl super::HostAppRegistry {
    /// Register a complete Axum router for this host.
    ///
    /// The factory runs at boot/reload. The router enters the same host scope
    /// compiler as configured backends, after the scope's active policy layers.
    /// Its response, including a 404, ends request dispatch.
    pub fn register_router<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState) -> Router + Send + Sync + 'static,
    {
        let router: Arc<Factory> = Arc::new(factory);

        self.inner
            .write()
            .expect("host app registry lock poisoned")
            .insert(vhost_key.to_string(), super::Binding::Axum(router));
    }
}
