//! Warp application adaptation and host binding.

use std::sync::Arc;

use claviron_http::warp::Router;
use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use claviron_runtime::AppState;
use futures_util::FutureExt;
use http::Request;

use crate::pipeline::host::{
    ApplicationHandler, HostHandler, HostHandlerFuture, HostRequest,
};

/// Cold Warp router factory for a selected host.
pub type Factory = dyn Fn(AppState) -> Router + Send + Sync + 'static;

impl ApplicationHandler for Router {
    fn call(
        self,
        request: Request<RouteBody>,
    ) -> impl std::future::Future<Output = RouteResponse> + Send {
        self.handle(request).map(move |response| {
            let _owner = self;
            let Ok(response) = response;
            response
        })
    }
}

struct WarpApplication(Router);

/// Adapt Warp to a native contribution; every Warp response is terminal.
pub fn application(router: Router) -> Arc<dyn HostHandler> {
    Arc::new(WarpApplication(router))
}

impl HostHandler for WarpApplication {
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

impl super::HostAppRegistry {
    /// Prepare this Warp router for a canonical host at boot/reload.
    pub fn register_warp<F>(
        &self,
        vhost_key: &'static str,
        factory: F,
    ) where
        F: Fn(AppState) -> Router + Send + Sync + 'static,
    {
        self.inner.write().expect("host app registry lock poisoned").insert(
            vhost_key.to_owned(),
            super::Binding::Warp(Arc::new(factory)),
        );
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/apps/warp.rs"]
mod tests;
