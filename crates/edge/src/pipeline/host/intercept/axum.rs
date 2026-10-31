//! Temporary invocation bridge while configured owners still compile as routers.

use super::*;

pub(crate) fn compose_router(
    backend: ::axum::Router,
    applications: &[Arc<dyn ApplicationInterceptor>],
) -> ::axum::Router {
    if applications.is_empty() {
        return backend;
    }
    let pipeline = ApplicationStage::new(backend, applications.to_vec());
    ::axum::Router::new().fallback(move |request: Request<::axum::body::Body>| {
        pipeline.clone()
            .handle(request.map(claviron_http::boxed_body))
            .map(claviron_http::into_axum_response)
    })
}
