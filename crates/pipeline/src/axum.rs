//! Optional Axum adaptation for explicitly supplied Rust applications.

use std::sync::Arc;

use axum::Router;
use claviron_http::{RouteBody, RouteResponse, ServiceResult};
use futures_util::FutureExt;
use http::{Request, request::Parts};
use std::future::Future;
use tower::ServiceExt;

use crate::plan::{
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

/// Select an Axum contribution from the request head before handing it the body.
pub fn application_if<F>(
    router: Router,
    accepts: F,
) -> Arc<dyn HostHandler>
where
    F: Fn(&Parts) -> bool + Send + Sync + 'static,
{
    Arc::new(ScopedApplication { app: AxumApplication(router), accepts })
}

struct ScopedApplication<F> {
    app: AxumApplication,
    accepts: F,
}

impl<F> HostHandler for ScopedApplication<F>
where
    F: Fn(&Parts) -> bool + Send + Sync + 'static,
{
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        if (self.accepts)(request.head()) {
            self.app.call(request)
        } else {
            Box::pin(std::future::ready(ServiceResult::Unhandled(request)))
        }
    }
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

/// Adapt routes that stand in front of a host's own owner: a path or method
/// they do not declare is returned Unhandled, with its unread body, as a
/// runner hands it to its owner.
pub fn routes(router: Router) -> Arc<dyn HostHandler> {
    Arc::new(FrontRoutes(
        router.fallback(unmatched).method_not_allowed_fallback(unmatched),
    ))
}

struct FrontRoutes(Router);

/// The request no route declared, carried back out of the router.
#[derive(Clone)]
struct Unmatched(Arc<std::sync::Mutex<Option<axum::extract::Request>>>);

async fn unmatched(
    request: axum::extract::Request
) -> axum::response::Response {
    let mut response = axum::response::Response::default();
    response
        .extensions_mut()
        .insert(Unmatched(Arc::new(std::sync::Mutex::new(Some(request)))));
    response
}

impl HostHandler for FrontRoutes {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        let router = self.0.clone();
        Box::pin(async move {
            let response = match router
                .oneshot(request.into_request().map(axum::body::Body::new))
                .await
            {
                Ok(response) => response,
                Err(never) => match never {},
            };
            let unmatched = response.extensions().get::<Unmatched>().and_then(
                |Unmatched(slot)| {
                    slot.lock().expect("unmatched request lock poisoned").take()
                },
            );
            match unmatched {
                Some(request) => ServiceResult::Unhandled(HostRequest::new(
                    request.map(|body| {
                        use http_body_util::BodyExt;
                        body.map_err(|error| -> claviron_http::RouteBodyError {
                            Box::new(error)
                        })
                        .boxed_unsync()
                    }),
                )),
                None => ServiceResult::Handled(
                    claviron_http::from_axum_response(response),
                ),
            }
        })
    }
}
