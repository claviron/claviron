//! Axum-free upstream dispatch with prepared file and tunnel policy.
//! Configured pipeline dispatch lives under `pipeline/upstream`.
//! The upstream itself — pools, balancing, health — is
//! `claviron-upstream`, which knows nothing of hosts.

use claviron_files::{FileSite, try_handle_files};
use claviron_http::ext::RealIpExt;
use claviron_http::{
    RequestContext, RouteBody, RouteFuture, RouteResponse, ServiceResult,
    err_status,
};
use claviron_runtime::AppState;
use claviron_upstream::binding::UpstreamApp;
use claviron_upstream::proxy::service::{
    UpstreamRequestPolicy, UpstreamService,
};
use http::{Request, StatusCode};
use std::convert::Infallible;
use std::sync::Arc;

use super::BypassHandler;

#[derive(Clone)]
pub struct UpstreamBypass {
    app: UpstreamApp,
    state: AppState,
    files: Arc<dyn FileSite>,
    policy: UpstreamRequestPolicy,
}

impl BypassHandler for UpstreamBypass {
    fn times_its_head(&self) -> bool {
        true
    }

    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let adapter = self.clone();
        Box::pin(async move { adapter.handle(request).await })
    }
}

impl UpstreamBypass {
    pub fn new(
        app: UpstreamApp,
        state: AppState,
        files: Arc<dyn FileSite>,
        policy: UpstreamRequestPolicy,
    ) -> Self {
        Self { app, state, files, policy }
    }

    pub async fn handle(
        &self,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse, Infallible> {
        Ok(self.handle_inner(req).await)
    }

    async fn handle_inner(
        &self,
        req: Request<RouteBody>,
    ) -> RouteResponse {
        let path = req.uri().path();
        if let Some(service) = self.app.select_explicit(path) {
            return call_bypass_service(
                service,
                req,
                self.policy.allow_h1_upgrade,
                self.policy.allow_connect_protocol,
            )
            .await;
        }

        let real_ip = req.extensions().get::<RealIpExt>().map(|ext| ext.0);
        let ctx =
            RequestContext { uri: req.uri(), headers: req.headers(), real_ip };

        let site = self.files.route_for(path).unwrap_or(self.files.as_ref());
        if site.try_files()
            || site.spa().is_some()
            || site.statics().is_some_and(|s| !s.is_empty())
            || (path.ends_with('/') && site.has_static_index())
        {
            match try_handle_files(&self.state, site, ctx).await {
                Ok(ServiceResult::Handled(resp)) => return resp,
                Ok(ServiceResult::Unhandled(_)) => {}
                Err(status) => return err_status(status),
            }
        }

        let Some(service) = self.app.select_root(path) else {
            return err_status(StatusCode::NOT_FOUND);
        };

        call_bypass_service(
            service,
            req,
            self.policy.allow_h1_upgrade,
            self.policy.allow_connect_protocol,
        )
        .await
    }
}

async fn call_bypass_service(
    service: UpstreamService,
    req: Request<RouteBody>,
    allow_h1_upgrade: bool,
    allow_connect_protocol: bool,
) -> RouteResponse {
    #[cfg(debug_assertions)]
    {
        let mut response = claviron_upstream::binding::call_upstream_service(
            service,
            req,
            allow_h1_upgrade,
            allow_connect_protocol,
        )
        .await;
        response.headers_mut().insert(
            http::HeaderName::from_static("x-claviron-bypass"),
            http::HeaderValue::from_static("http-upstream"),
        );
        response
    }

    #[cfg(not(debug_assertions))]
    {
        claviron_upstream::binding::call_upstream_service(
            service,
            req,
            allow_h1_upgrade,
            allow_connect_protocol,
        )
        .await
    }
}
